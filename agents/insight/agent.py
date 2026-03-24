"""InsightAgent — calls Claude API to generate economic insight summaries."""

from __future__ import annotations

import hashlib
import os
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import anthropic
import anthropic.types as atypes
import asyncpg
import structlog

from agents.base import BaseAgent
from api.dependencies import query_gold_series
from api.models import AgentResult, InsightRecord
from api.series_config import SERIES_DISPLAY, get_all_series_ids
from security.sanitize import sanitize_for_prompt
from security.xml_fencing import build_anomaly_prompt, build_insight_prompt

logger = structlog.get_logger()

_MODEL_VERSION = "claude-sonnet-4-20250514"

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS insights (
    id SERIAL PRIMARY KEY,
    content TEXT NOT NULL,
    language VARCHAR(5) NOT NULL,
    metric_refs TEXT[] DEFAULT '{}',
    model_version VARCHAR(100) NOT NULL,
    run_id VARCHAR(100) NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL,
    confidence_flag BOOLEAN DEFAULT TRUE,
    insight_type VARCHAR(20) DEFAULT 'digest',
    anomaly_hash VARCHAR(64) DEFAULT NULL
);
"""

_MIGRATE_TABLE_SQL = """
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='insights' AND column_name='insight_type') THEN
        ALTER TABLE insights ADD COLUMN insight_type VARCHAR(20) DEFAULT 'digest';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='insights' AND column_name='anomaly_hash') THEN
        ALTER TABLE insights ADD COLUMN anomaly_hash VARCHAR(64) DEFAULT NULL;
    END IF;
END $$;
"""

_MAX_ANOMALIES_FOR_PROMPT = 30


def _compute_z_scores(values: list[float]) -> list[float]:
    """Compute z-scores for a list of numeric values.

    Returns an empty list when fewer than 2 values are provided
    or when standard deviation is zero.
    """
    n = len(values)
    if n < 2:
        return []
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / n
    std = variance**0.5
    if std == 0.0:
        return [0.0] * n
    return [(v - mean) / std for v in values]


def _detect_anomalies(
    series_data: dict[str, list[dict[str, Any]]],
) -> list[str]:
    """Flag data points with |z-score| > 2.0 across each series.

    Returns a list of human-readable anomaly descriptions.
    """
    anomalies: list[str] = []
    for series_name, rows in series_data.items():
        values = [float(r["value"]) for r in rows if r.get("value") is not None]
        if len(values) < 2:
            continue
        z_scores = _compute_z_scores(values)
        for idx, z in enumerate(z_scores):
            if abs(z) > 2.0:
                row = rows[idx]
                anomalies.append(
                    f"{series_name} on {row.get('date', '?')}: "
                    f"value={row['value']}, z-score={z:.2f}"
                )
    return anomalies


def _compute_anomaly_hash(anomalies: list[str]) -> str:
    """Compute a deterministic hash of the anomaly set for dedup."""
    canonical = "\n".join(sorted(anomalies))
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def _extract_z_score(anomaly: str) -> float:
    """Extract absolute z-score from an anomaly description string."""
    try:
        return abs(float(anomaly.split("z-score=")[1]))
    except (IndexError, ValueError):
        return 0.0


def _format_anomaly_prompt_data(anomalies: list[str]) -> str:
    """Format anomalies grouped by series for the prompt."""
    parts: list[str] = [f"Total anomalies detected: {len(anomalies)}", ""]

    by_series: dict[str, list[str]] = defaultdict(list)
    for a in anomalies:
        series_name = a.split(" on ")[0] if " on " in a else "unknown"
        by_series[series_name].append(a)

    for series_name, items in sorted(by_series.items()):
        parts.append(f"## {series_name}")
        for item in items:
            parts.append(f"  - {item}")
        parts.append("")

    return "\n".join(parts)


def _build_series_descriptions() -> str:
    """Build a formatted string of series descriptions for the anomaly prompt."""
    parts: list[str] = []
    for sid, meta in SERIES_DISPLAY.items():
        desc = meta.get("description", "")
        parts.append(f"{meta['label']} ({sid}): {desc}")
    return "\n".join(parts)


def _format_gold_summary(
    series_data: dict[str, list[dict[str, Any]]],
    anomalies: list[str],
) -> str:
    """Format gold-layer data into a summary string for the prompt."""
    parts: list[str] = []
    for series_name, rows in series_data.items():
        if not rows:
            parts.append(f"{series_name}: no data available")
            continue
        latest = rows[-1]
        earliest = rows[0]
        parts.append(
            f"{series_name}: {len(rows)} data points, "
            f"range {earliest.get('date', '?')} to {latest.get('date', '?')}, "
            f"latest value={latest.get('value', '?')}"
        )
    if anomalies:
        parts.append("\nAnomalies detected (z-score > 2.0):")
        for a in anomalies:
            parts.append(f"  - {a}")
    return "\n".join(parts)


def _parse_insight_sections(
    raw_text: str,
    run_id: str,
    generated_at: datetime,
    metric_refs: list[str],
    confidence_flag: bool,
) -> list[InsightRecord]:
    """Parse Claude response into PT and EN InsightRecord objects.

    Expected response format uses XML-like section markers:
      <pt>...</pt>
      <en>...</en>

    Falls back to splitting on double-newline if markers are absent.
    """
    records: list[InsightRecord] = []

    for lang, open_tag, close_tag in [
        ("pt", "<pt>", "</pt>"),
        ("en", "<en>", "</en>"),
    ]:
        start = raw_text.find(open_tag)
        end = raw_text.find(close_tag)
        if start != -1 and end != -1:
            content = raw_text[start + len(open_tag) : end].strip()
            if content:
                records.append(
                    InsightRecord(
                        content=content,
                        language=lang,
                        metric_refs=metric_refs,
                        model_version=_MODEL_VERSION,
                        run_id=run_id,
                        generated_at=generated_at,
                        confidence_flag=confidence_flag,
                    )
                )

    # Fallback: if no markers found, treat entire response as EN
    if not records:
        records.append(
            InsightRecord(
                content=raw_text.strip(),
                language="en",
                metric_refs=metric_refs,
                model_version=_MODEL_VERSION,
                run_id=run_id,
                generated_at=generated_at,
                confidence_flag=confidence_flag,
            )
        )

    return records


class InsightAgent(BaseAgent):
    """Generates AI-powered economic insight summaries from gold-layer data.

    Reads gold data for all tracked series, detects anomalies via z-scores,
    calls Claude to produce PT + EN summaries, and stores results in Postgres.
    """

    @property
    def agent_name(self) -> str:
        return "insight"

    async def _execute(self) -> AgentResult:
        run_id = uuid.uuid4().hex[:12]
        warnings: list[str] = []
        errors: list[str] = []

        # --- Step 1: Read gold data for all series ---
        series_data: dict[str, list[dict[str, Any]]] = {}
        total_rows = 0
        all_series = get_all_series_ids()
        for series in all_series:
            try:
                rows = await query_gold_series(series)
                series_data[series] = rows
                total_rows += len(rows)
            except Exception as exc:
                warnings.append(f"Failed to read {series}: {exc}")
                logger.warning("insight_gold_read_error", series=series, error=str(exc))
                series_data[series] = []

        if total_rows == 0:
            errors.append("No gold data available for any series")
            return AgentResult(
                success=False,
                agent_name=self.agent_name,
                duration_ms=0.0,
                rows_processed=0,
                errors=errors,
                warnings=warnings,
            )

        # --- Step 2: Anomaly detection ---
        anomalies = _detect_anomalies(series_data)
        if anomalies:
            logger.info("insight_anomalies_detected", count=len(anomalies))
            for a in anomalies:
                warnings.append(f"Anomaly: {a}")

        # --- Step 3: Format, sanitize (L1), and build XML-fenced prompt (L3) ---
        summary = _format_gold_summary(series_data, anomalies)
        sanitized_summary = sanitize_for_prompt(summary)
        system_prompt, user_message = build_insight_prompt(sanitized_summary)

        # Append bilingual instruction to user message
        user_message_full = (
            f"{user_message}\n\n"
            "Produce two sections: one in Brazilian Portuguese wrapped in "
            "<pt>...</pt> tags and one in English wrapped in <en>...</en> tags."
        )

        # --- Step 4: Call Claude API ---
        generated_at = datetime.now(timezone.utc)
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            errors.append("ANTHROPIC_API_KEY is not set — skipping insight generation")
            logger.warning("insight_skipped_no_api_key")
            return AgentResult(
                success=False,
                agent_name=self.agent_name,
                duration_ms=0.0,
                rows_processed=total_rows,
                errors=errors,
                warnings=warnings,
            )
        try:
            client = anthropic.AsyncAnthropic(api_key=api_key)
            response = await client.messages.create(
                model=_MODEL_VERSION,
                max_tokens=2048,
                system=system_prompt,
                messages=[{"role": "user", "content": user_message_full}],
            )
            first_block = response.content[0]
            if not isinstance(first_block, atypes.TextBlock):
                raise TypeError(
                    f"Expected TextBlock, got {type(first_block).__name__}"
                )
            raw_text = first_block.text
        except Exception as exc:
            errors.append(f"Claude API call failed: {exc}")
            logger.error("insight_claude_error", error=str(exc))
            return AgentResult(
                success=False,
                agent_name=self.agent_name,
                duration_ms=0.0,
                rows_processed=total_rows,
                errors=errors,
                warnings=warnings,
            )

        # --- Step 5: Parse response into InsightRecords ---
        metric_refs = [s for s in all_series if series_data.get(s)]
        confidence_flag = len(anomalies) == 0
        records = _parse_insight_sections(
            raw_text, run_id, generated_at, metric_refs, confidence_flag
        )

        logger.info(
            "insight_records_parsed",
            count=len(records),
            languages=[r.language for r in records],
        )

        # --- Step 5b: Generate anomaly analysis ---
        if anomalies:
            try:
                anomaly_records = await self._generate_anomaly_analysis(
                    anomalies, metric_refs, run_id, generated_at,
                )
                if anomaly_records:
                    records.extend(anomaly_records)
                    logger.info("anomaly_analysis_generated", count=len(anomaly_records))
            except Exception as exc:
                warnings.append(f"Anomaly analysis failed: {exc}")
                logger.warning("anomaly_analysis_error", error=str(exc))

        # --- Step 6: Store results in Postgres ---
        try:
            await self._store_insights(records)
        except Exception as exc:
            errors.append(f"Postgres storage failed: {exc}")
            logger.error("insight_storage_error", error=str(exc))
            return AgentResult(
                success=False,
                agent_name=self.agent_name,
                duration_ms=0.0,
                rows_processed=total_rows,
                errors=errors,
                warnings=warnings,
            )

        # --- Step 7: Return AgentResult ---
        return AgentResult(
            success=True,
            agent_name=self.agent_name,
            duration_ms=0.0,
            rows_processed=total_rows,
            errors=errors,
            warnings=warnings,
        )

    async def _store_insights(self, records: list[InsightRecord]) -> None:
        """Persist InsightRecord objects to the insights Postgres table."""
        database_url = os.environ.get("DATABASE_URL", "")
        if not database_url:
            raise ValueError("DATABASE_URL environment variable is not set")

        conn: asyncpg.Connection = await asyncpg.connect(database_url)
        try:
            await conn.execute(_CREATE_TABLE_SQL)
            await conn.execute(_MIGRATE_TABLE_SQL)
            for record in records:
                await conn.execute(
                    """
                    INSERT INTO insights
                        (content, language, metric_refs, model_version,
                         run_id, generated_at, confidence_flag,
                         insight_type, anomaly_hash)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    """,
                    record.content,
                    record.language,
                    record.metric_refs,
                    record.model_version,
                    record.run_id,
                    record.generated_at,
                    record.confidence_flag,
                    record.insight_type,
                    record.anomaly_hash,
                )
            logger.info("insights_stored", count=len(records))
        finally:
            await conn.close()

    async def _anomaly_hash_exists(self, anomaly_hash: str) -> bool:
        """Check if an anomaly insight with this hash already exists recently."""
        database_url = os.environ.get("DATABASE_URL", "")
        if not database_url:
            return False
        try:
            conn: asyncpg.Connection = await asyncpg.connect(database_url)
            try:
                row = await conn.fetchrow(
                    "SELECT id FROM insights "
                    "WHERE insight_type = 'anomaly' AND anomaly_hash = $1 "
                    "AND generated_at > NOW() - INTERVAL '7 days' "
                    "LIMIT 1",
                    anomaly_hash,
                )
                return row is not None
            finally:
                await conn.close()
        except Exception:
            return False

    async def _generate_anomaly_analysis(
        self,
        anomalies: list[str],
        metric_refs: list[str],
        run_id: str,
        generated_at: datetime,
    ) -> list[InsightRecord]:
        """Generate Claude-powered anomaly analysis with macro context."""
        # Cap at top N by z-score magnitude
        sorted_anomalies = sorted(
            anomalies, key=_extract_z_score, reverse=True
        )[:_MAX_ANOMALIES_FOR_PROMPT]

        anomaly_hash = _compute_anomaly_hash(sorted_anomalies)

        # Dedup: skip if same anomaly set was analyzed recently
        if await self._anomaly_hash_exists(anomaly_hash):
            logger.info("anomaly_analysis_skipped_dedup", hash=anomaly_hash)
            return []

        # Build prompt
        anomaly_data = _format_anomaly_prompt_data(sorted_anomalies)
        series_descriptions = _build_series_descriptions()
        system_prompt, user_message = build_anomaly_prompt(
            anomaly_data, series_descriptions
        )

        # Call Claude
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            return []

        client = anthropic.AsyncAnthropic(api_key=api_key)
        response = await client.messages.create(
            model=_MODEL_VERSION,
            max_tokens=2048,
            system=system_prompt,
            messages=[{"role": "user", "content": user_message}],
        )
        first_block = response.content[0]
        if not isinstance(first_block, atypes.TextBlock):
            raise TypeError(f"Expected TextBlock, got {type(first_block).__name__}")

        records = _parse_insight_sections(
            first_block.text,
            run_id=run_id,
            generated_at=generated_at,
            metric_refs=metric_refs,
            confidence_flag=True,
        )

        # Mark as anomaly type
        for record in records:
            record.insight_type = "anomaly"
            record.anomaly_hash = anomaly_hash

        return records

    async def health_check(self) -> bool:
        """Verify Postgres and Claude API dependencies are reachable."""
        try:
            database_url = os.environ.get("DATABASE_URL", "")
            if not database_url:
                return False
            conn: asyncpg.Connection = await asyncpg.connect(database_url)
            await conn.execute("SELECT 1")
            await conn.close()
            return True
        except Exception:
            return False
