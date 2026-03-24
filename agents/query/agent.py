"""QueryAgent — answers user questions using Tier 1 (DuckDB) or Tier 3 (Claude)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import anthropic
import structlog
from anthropic.types import MessageParam

from agents.base import BaseAgent
from agents.query.router import QuerySkillRouter
from api.dependencies import query_gold_series
from api.models import (
    AgentResult,
    DataPoint,
    QueryResponse,
    QueryTier,
)
from api.series_config import SERIES_DISPLAY, get_display_label
from security.sanitize import PromptInjectionError, sanitize_for_prompt
from security.xml_fencing import build_query_prompt

logger = structlog.get_logger()

# Series that originate from BCB (used for source attribution).
BCB_SERIES: set[str] = {sid for sid, m in SERIES_DISPLAY.items() if m["source"] == "BCB"}

# All series we know about — queried for FULL_LLM context.
ALL_SERIES: list[str] = list(SERIES_DISPLAY.keys())

_MODEL = "claude-sonnet-4-20250514"


class QueryAgent(BaseAgent):
    """Answer user questions about Brazilian economic indicators.

    Tier 1 (DIRECT_LOOKUP): simple latest-value questions answered via DuckDB.
    Tier 3 (FULL_LLM): complex questions answered by Claude with XML fencing.
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
        self._query_response: QueryResponse | None = None

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

    async def _execute(self) -> AgentResult:
        """Orchestrate the full query flow."""

        # Step 1 — L1 sanitization
        try:
            sanitized = sanitize_for_prompt(self._question)
        except PromptInjectionError as exc:
            logger.warning("prompt_injection_blocked", error=str(exc))
            self._query_response = QueryResponse(
                answer="Your question was blocked by our safety filter. "
                "Please rephrase and try again.",
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
                # Escalate to Tier 3 on failure
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

        source = (
            "Banco Central do Brasil"
            if metric in BCB_SERIES
            else "IBGE"
        )

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
    # Tier 3 — Full LLM call via Claude
    # ------------------------------------------------------------------

    async def handle_full_llm(
        self,
        question: str,
        history: list[dict[str, str]],
    ) -> QueryResponse:
        """Answer a complex question using Claude with XML-fenced context."""

        # Gather context from all gold series
        context_data = await self._gather_context()

        # Build XML-fenced prompt
        system_prompt, user_message = build_query_prompt(context_data, question)

        # Add language instruction
        if self._language == "pt":
            system_prompt += "\n\nIMPORTANT: Always respond in Brazilian Portuguese (pt-BR)."
        else:
            system_prompt += "\n\nIMPORTANT: Always respond in English."

        # Build messages list (include conversation history)
        messages: list[MessageParam] = []
        for entry in history:
            role = entry["role"]
            if role in ("user", "assistant"):
                messages.append({"role": role, "content": entry["content"]})  # type: ignore[typeddict-item]
        messages.append({"role": "user", "content": user_message})

        # Stream Claude response
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY is not set")
        client = anthropic.AsyncAnthropic(api_key=api_key)

        answer_parts: list[str] = []
        total_input_tokens = 0
        total_output_tokens = 0

        async with client.messages.stream(
            model=_MODEL,
            max_tokens=1024,
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

        # Extract data points and sources from context
        data_points = await self._extract_latest_data_points()
        sources = self._determine_sources(data_points)

        logger.info(
            "llm_query_completed",
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
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    async def _gather_context(self) -> str:
        """Fetch latest records from all known gold series and format as text."""
        lines: list[str] = []
        for series_id in ALL_SERIES:
            rows = await query_gold_series(series_id)
            if not rows:
                continue
            # Include last 30 data points for context
            recent = rows[-30:]
            lines.append(f"--- {series_id} ---")
            for row in recent:
                date_str = (
                    row["date"].strftime("%Y-%m-%d")
                    if isinstance(row["date"], datetime)
                    else str(row["date"])
                )
                lines.append(f"  {date_str}: {row['value']}")
        return "\n".join(lines)

    async def _extract_latest_data_points(self) -> list[DataPoint]:
        """Get the latest DataPoint for each known series (for response metadata)."""
        points: list[DataPoint] = []
        for series_id in ALL_SERIES:
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
                    series=str(latest.get("series", series_id)),
                    value=float(latest["value"]),
                    date=date,
                )
            )
        return points

    @staticmethod
    def _determine_sources(data_points: list[DataPoint]) -> list[str]:
        """Derive human-readable source names from the data points present."""
        sources: set[str] = set()
        for dp in data_points:
            if dp.series.startswith("bcb_"):
                sources.add("Banco Central do Brasil")
            elif dp.series.startswith("ibge_"):
                sources.add("IBGE")
        return sorted(sources)
