"""QueryAgent — answers user questions using Tier 1 (DuckDB) or Tier 2 (Haiku)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import anthropic
import structlog
from anthropic.types import MessageParam

from agents.base import BaseAgent
from agents.query.planner import QueryPlanner
from agents.query.router import (
    METRIC_KEYWORDS,
    QuerySkillRouter,
    detect_domains,
    get_series_for_domains,
)
from api.dependencies import query_gold_series
from api.models import (
    AgentResult,
    DataPoint,
    QueryResponse,
    QueryTier,
)
from api.series_config import SERIES_DISPLAY, get_display_label
from config import get_domain_config
from security.sanitize import PromptInjectionError, sanitize_for_prompt
from security.xml_fencing import build_query_prompt

logger = structlog.get_logger()

# All series we know about.
ALL_SERIES: list[str] = list(SERIES_DISPLAY.keys())

_MODEL = "claude-haiku-4-5-20251001"

# Max recent data points to include per relevant series.
# Daily series need ~60 points for YTD queries, ~250 for 1-year.
# Monthly series (IPCA, unemployment, GDP) need fewer.
_MAX_CONTEXT_POINTS = 90


class QueryAgent(BaseAgent):
    """Answer user questions about economic indicators.

    Tier 1 (DIRECT_LOOKUP): simple latest-value questions answered via DuckDB.
    Tier 2 (FULL_LLM): questions answered by Haiku with focused context.
    """

    def __init__(
        self,
        question: str,
        history: list[dict[str, str]] | None = None,
        language: str = "en",
    ) -> None:
        self._question = question
        self._history: list[dict[str, str]] = history or []
        self._language = language
        self._router = QuerySkillRouter()
        self._planner = QueryPlanner()
        self._query_response: QueryResponse | None = None
        self._last_system_prompt: str = ""

    # ------------------------------------------------------------------
    # BaseAgent interface
    # ------------------------------------------------------------------

    @property
    def agent_name(self) -> str:
        return "QueryAgent"

    @property
    def query_response(self) -> QueryResponse | None:
        """Expose the structured QueryResponse for the API layer."""
        return self._query_response

    @property
    def last_system_prompt(self) -> str:
        """The system prompt used in the last LLM call (for audit logging)."""
        return self._last_system_prompt

    async def _execute(self) -> AgentResult:
        """Orchestrate the full query flow."""

        # Step 1 — L1 sanitization
        try:
            sanitized = sanitize_for_prompt(self._question)
        except PromptInjectionError as exc:
            logger.warning("prompt_injection_blocked", error=str(exc))
            cfg = get_domain_config()
            lang = self._language if self._language in ("en", "pt") else "en"
            scope = getattr(cfg.ai.scope_description, lang)
            indicators = getattr(cfg.ai.example_indicators, lang)
            raw_msg: str = getattr(cfg.ai.safety_message, lang)
            safety_msg = raw_msg.format(
                scope=scope, example_indicators=indicators,
            )
            self._query_response = QueryResponse(
                answer=safety_msg,
                tier_used=QueryTier.DIRECT_LOOKUP,
                llm_tokens_used=0,
            )
            return AgentResult(
                success=False,
                agent_name=self.agent_name,
                duration_ms=0.0,
                errors=[str(exc)],
            )

        # Step 2 — Route
        tier, metric = self._router.route(sanitized)

        # Step 3 — Execute the appropriate tier
        if tier == QueryTier.DIRECT_LOOKUP:
            try:
                self._query_response = await self.handle_direct_lookup(metric)
            except Exception as exc:
                logger.warning(
                    "tier1_escalation",
                    metric=metric,
                    error=str(exc),
                )
                # Escalate to LLM on failure
                self._query_response = await self.handle_full_llm(
                    sanitized, self._history
                )
        else:
            self._query_response = await self.handle_full_llm(
                sanitized, self._history
            )

        return AgentResult(
            success=True,
            agent_name=self.agent_name,
            duration_ms=0.0,  # filled by BaseAgent.run()
        )

    # ------------------------------------------------------------------
    # Tier 1 — Direct DuckDB lookup ($0)
    # ------------------------------------------------------------------

    async def handle_direct_lookup(self, metric: str) -> QueryResponse:
        """Answer a simple latest-value question from gold parquet data."""
        rows: list[dict[str, Any]] = await query_gold_series(metric)

        if not rows:
            raise ValueError(f"No gold data found for series={metric}")

        latest = rows[-1]
        value: float = float(latest["value"])
        raw_date = latest["date"]

        if isinstance(raw_date, datetime):
            date = raw_date if raw_date.tzinfo else raw_date.replace(tzinfo=timezone.utc)
        else:
            date = datetime.fromisoformat(str(raw_date)).replace(tzinfo=timezone.utc)

        series_name = get_display_label(metric)

        source = self._source_name_for_series(metric)

        if self._language == "pt":
            answer = (
                f"O valor mais recente de {series_name} e {value} "
                f"(em {date.strftime('%d/%m/%Y')})."
            )
        else:
            answer = (
                f"The latest value for {series_name} is {value} "
                f"(as of {date.strftime('%Y-%m-%d')})."
            )

        return QueryResponse(
            answer=answer,
            data_points=[
                DataPoint(series=series_name, value=value, date=date),
            ],
            sources=[source],
            tier_used=QueryTier.DIRECT_LOOKUP,
            llm_tokens_used=0,
        )

    # ------------------------------------------------------------------
    # Tier 2 — Haiku with focused context
    # ------------------------------------------------------------------

    async def handle_full_llm(
        self,
        question: str,
        history: list[dict[str, str]],
    ) -> QueryResponse:
        """Answer a question using Haiku with only the relevant data.

        Uses QueryPlanner to parse intent and pre-aggregate data in DuckDB,
        reducing token usage by 60-80% compared to sending raw data.
        """

        # Use QueryPlanner for structured intent parsing and context building
        intent = self._planner.parse_intent(question)
        relevant = intent.series if intent.series else self._extract_relevant_series(question)

        # Build pre-aggregated context via the planner
        context_data = await self._planner.build_context(intent)

        # Build XML-fenced prompt
        system_prompt, user_message = build_query_prompt(context_data, question)

        # Add language instruction
        lang_names = {"pt": "Portuguese (pt-BR)", "en": "English"}
        lang_label = lang_names.get(self._language, "English")
        system_prompt += f"\n\nIMPORTANT: Always respond in {lang_label}."

        self._last_system_prompt = system_prompt

        logger.info(
            "query_agent.prompt_assembled",
            question=question[:120],
            tier="FULL_LLM",
            relevant_series=relevant,
            system_prompt_chars=len(system_prompt),
            context_chars=len(context_data),
            language=self._language,
        )

        # Build messages list (include conversation history)
        messages: list[MessageParam] = []
        for entry in history:
            role = entry["role"]
            if role in ("user", "assistant"):
                messages.append({"role": role, "content": entry["content"]})  # type: ignore[typeddict-item]
        messages.append({"role": "user", "content": user_message})

        # Call Haiku
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY is not set")
        client = anthropic.AsyncAnthropic(api_key=api_key)

        answer_parts: list[str] = []
        total_input_tokens = 0
        total_output_tokens = 0

        async with client.messages.stream(
            model=_MODEL,
            max_tokens=512,
            system=system_prompt,
            messages=messages,
        ) as stream:
            async for text in stream.text_stream:
                answer_parts.append(text)

            final_message = await stream.get_final_message()
            total_input_tokens = final_message.usage.input_tokens
            total_output_tokens = final_message.usage.output_tokens

        answer = "".join(answer_parts)
        tokens_used = total_input_tokens + total_output_tokens

        # Build data points only for relevant series
        data_points = await self._extract_data_points(relevant)
        sources = self._determine_sources(data_points)

        logger.info(
            "llm_query_completed",
            model=_MODEL,
            relevant_series=relevant,
            tokens_input=total_input_tokens,
            tokens_output=total_output_tokens,
            tokens_total=tokens_used,
        )

        return QueryResponse(
            answer=answer,
            data_points=data_points,
            sources=sources,
            tier_used=QueryTier.FULL_LLM,
            llm_tokens_used=tokens_used,
            llm_input_tokens=total_input_tokens,
            llm_output_tokens=total_output_tokens,
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_relevant_series(question: str) -> list[str]:
        """Identify which series the question is about using keyword + domain matching.

        Strategy:
        1. Keyword match → specific series found → return those + same-domain siblings
        2. No keyword match → domain detection → return all series in matched domains
        3. No domain match → return ALL_SERIES (safe fallback for ambiguous questions)
        """
        lowered = question.lower()
        found: set[str] = set()
        for keyword, series_id in METRIC_KEYWORDS.items():
            if keyword in lowered:
                found.add(series_id)

        if found:
            # Also include same-domain siblings for richer context
            domains = detect_domains(question)
            if domains:
                domain_series = get_series_for_domains(domains)
                found.update(domain_series)
            return list(found)

        # No specific keyword → try domain-level detection
        domains = detect_domains(question)
        if domains:
            return get_series_for_domains(domains)

        # Fully ambiguous → all series
        return ALL_SERIES

    async def _build_compact_context(self, relevant_series: list[str]) -> str:
        """Build a token-efficient context string.

        Relevant series get their last N data points.
        Other series get a one-line summary (latest value only).
        """
        lines: list[str] = []

        for series_id in ALL_SERIES:
            label = get_display_label(series_id)
            meta = SERIES_DISPLAY.get(series_id, {})
            unit = meta.get("unit", "")
            desc = meta.get("description", "")
            rows = await query_gold_series(series_id)

            if not rows:
                lines.append(f"{label} ({unit}): no data available")
                continue

            latest = rows[-1]
            latest_val = latest["value"]
            latest_date = (
                latest["date"].strftime("%Y-%m-%d")
                if isinstance(latest["date"], datetime)
                else str(latest["date"])
            )

            if series_id in relevant_series:
                # Detailed: description + last N data points
                header = f"{label} ({unit})"
                if desc:
                    header += f" — {desc}"
                recent = rows[-_MAX_CONTEXT_POINTS:]
                lines.append(f"{header}\nLast {len(recent)} values:")
                for row in recent:
                    d = (
                        row["date"].strftime("%Y-%m-%d")
                        if isinstance(row["date"], datetime)
                        else str(row["date"])
                    )
                    lines.append(f"  {d}: {row['value']}")
            else:
                # Compact: description + one-line summary
                summary = f"{label} ({unit})"
                if desc:
                    summary += f" [{desc}]"
                summary += f": latest {latest_val} on {latest_date}"
                lines.append(summary)

        return "\n".join(lines)

    async def _extract_data_points(self, relevant_series: list[str]) -> list[DataPoint]:
        """Get the latest DataPoint for relevant series only."""
        points: list[DataPoint] = []
        for series_id in relevant_series:
            rows = await query_gold_series(series_id)
            if not rows:
                continue
            latest = rows[-1]
            raw_date = latest["date"]
            if isinstance(raw_date, datetime):
                date = raw_date if raw_date.tzinfo else raw_date.replace(tzinfo=timezone.utc)
            else:
                date = datetime.fromisoformat(str(raw_date)).replace(tzinfo=timezone.utc)

            points.append(
                DataPoint(
                    series=get_display_label(series_id),
                    value=float(latest["value"]),
                    date=date,
                )
            )
        return points

    @staticmethod
    def _source_name_for_series(series_id: str) -> str:
        """Return the human-readable data source name for a series."""
        cfg = get_domain_config()
        series_cfg = cfg.series.get(series_id)
        if not series_cfg:
            return series_id
        source_id = series_cfg.source.lower()
        for ds in cfg.data_sources:
            if ds.id == source_id:
                return ds.name
        return series_cfg.source

    @staticmethod
    def _determine_sources(data_points: list[DataPoint]) -> list[str]:
        """Derive human-readable source names from the data points present."""
        cfg = get_domain_config()
        sources: set[str] = set()
        for dp in data_points:
            for sid, meta in SERIES_DISPLAY.items():
                if get_display_label(sid) == dp.series:
                    source_id = meta["source"].lower()
                    for ds in cfg.data_sources:
                        if ds.id == source_id:
                            sources.add(ds.name)
                            break
                    else:
                        sources.add(meta["source"])
                    break
        return sorted(sources)
