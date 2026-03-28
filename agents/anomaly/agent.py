"""AnomalyAgent — event-driven agent that detects and analyzes statistical anomalies."""

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
from api.series_config import get_all_series_ids
from config import get_domain_config
from security.xml_fencing import build_anomaly_prompt

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

# Z-score threshold for anomaly detection
_ANOMALY_Z_THRESHOLD = 2.0


def _detect_anomalies_from_gold(
    series_data: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    """Scan gold series for data points with |z_score| > threshold.

    Gold data already has z_score computed by the TransformationTask.
    Returns structured anomaly records.
    """
    anomalies: list[dict[str, Any]] = []
    for series_name, rows in series_data.items():
        for row in rows:
            z_score = row.get("z_score")
            if z_score is not None and abs(float(z_score)) > _ANOMALY_Z_THRESHOLD:
                anomalies.append({
                    "series": series_name,
                    "date": row.get("date", "?"),
                    "value": row.get("value"),
                    "z_score": float(z_score),
                })
    return anomalies


def _format_anomaly_descriptions(anomalies: list[dict[str, Any]]) -> list[str]:
    """Convert structured anomalies to human-readable descriptions."""
    return [
        f"{a['series']} on {a['date']}: value={a['value']}, z-score={a['z_score']:.2f}"
        for a in anomalies
    ]


def _compute_anomaly_hash(descriptions: list[str]) -> str:
    """Compute a deterministic hash of the anomaly set for dedup."""
    canonical = "\n".join(sorted(descriptions))
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def _format_anomaly_prompt_data(anomalies: list[dict[str, Any]]) -> str:
    """Format anomalies grouped by series for the prompt."""
    parts: list[str] = [f"Total anomalies detected: {len(anomalies)}", ""]

    by_series: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for a in anomalies:
        by_series[a["series"]].append(a)

    for series_name, items in sorted(by_series.items()):
        parts.append(f"## {series_name}")
        for item in items:
            parts.append(
                f"  - date={item['date']}, value={item['value']}, "
                f"z-score={item['z_score']:.2f}"
            )
        parts.append("")

    return "\n".join(parts)


def _build_series_descriptions() -> str:
    """Build a formatted string of series descriptions for the anomaly prompt."""
    cfg = get_domain_config()
    parts: list[str] = []
    for sid, series in cfg.series.items():
        parts.append(f"{series.label} ({sid}): {series.description.en}")
    return "\n".join(parts)


def _parse_anomaly_sections(
    raw_text: str,
    run_id: str,
    generated_at: datetime,
    metric_refs: list[str],
    anomaly_hash: str,
) -> list[InsightRecord]:
    """Parse Claude response into InsightRecord objects with anomaly metadata."""
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
                        confidence_flag=True,
                        insight_type="anomaly",
                        anomaly_hash=anomaly_hash,
                    )
                )

    if not records:
        records.append(
            InsightRecord(
                content=raw_text.strip(),
                language="en",
                metric_refs=metric_refs,
                model_version=_MODEL_VERSION,
                run_id=run_id,
                generated_at=generated_at,
                confidence_flag=True,
                insight_type="anomaly",
                anomaly_hash=anomaly_hash,
            )
        )

    return records


class AnomalyAgent(BaseAgent):
    """Scans gold series for statistical anomalies and generates AI analysis.

    Only fires when anomalies (|z_score| > 2.0) are detected. Deduplicates
    by anomaly hash to avoid re-analyzing the same set within 7 days.
    """

    @property
    def agent_name(self) -> str:
        return "anomaly"

    async def _execute(self) -> AgentResult:
        run_id = uuid.uuid4().hex[:12]
        warnings: list[str] = []
        errors: list[str] = []

        # Step 1: Read gold data for all series
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
                logger.warning("anomaly_gold_read_error", series=series, error=str(exc))

        if total_rows == 0:
            return AgentResult(
                success=True,
                agent_name=self.agent_name,
                duration_ms=0.0,
                rows_processed=0,
                warnings=["No gold data available — skipping anomaly scan"],
            )

        # Step 2: Detect anomalies from gold z_scores
        anomalies = _detect_anomalies_from_gold(series_data)

        if not anomalies:
            logger.info("anomaly_none_detected")
            return AgentResult(
                success=True,
                agent_name=self.agent_name,
                duration_ms=0.0,
                rows_processed=total_rows,
                warnings=["No anomalies detected — skipping analysis"],
            )

        logger.info("anomaly_detected", count=len(anomalies))

        # Step 3: Sort by |z_score| and cap for prompt
        sorted_anomalies = sorted(
            anomalies, key=lambda a: abs(a["z_score"]), reverse=True,
        )[:_MAX_ANOMALIES_FOR_PROMPT]

        # Step 4: Dedup check
        descriptions = _format_anomaly_descriptions(sorted_anomalies)
        anomaly_hash = _compute_anomaly_hash(descriptions)

        if await self._anomaly_hash_exists(anomaly_hash):
            logger.info("anomaly_skipped_dedup", hash=anomaly_hash)
            return AgentResult(
                success=True,
                agent_name=self.agent_name,
                duration_ms=0.0,
                rows_processed=total_rows,
                warnings=[f"Anomaly set already analyzed (hash={anomaly_hash})"],
            )

        # Step 5: Build prompt and call Claude
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            errors.append("ANTHROPIC_API_KEY is not set — skipping anomaly analysis")
            logger.warning("anomaly_skipped_no_api_key")
            return AgentResult(
                success=False,
                agent_name=self.agent_name,
                duration_ms=0.0,
                rows_processed=total_rows,
                errors=errors,
                warnings=warnings,
            )

        anomaly_data = _format_anomaly_prompt_data(sorted_anomalies)
        series_descriptions = _build_series_descriptions()
        system_prompt, user_message = build_anomaly_prompt(
            anomaly_data, series_descriptions,
        )

        generated_at = datetime.now(timezone.utc)

        try:
            client = anthropic.AsyncAnthropic(api_key=api_key)
            response = await client.messages.create(
                model=_MODEL_VERSION,
                max_tokens=2048,
                system=system_prompt,
                messages=[{"role": "user", "content": user_message}],
            )
            first_block = response.content[0]
            if not isinstance(first_block, atypes.TextBlock):
                raise TypeError(
                    f"Expected TextBlock, got {type(first_block).__name__}"
                )
            raw_text = first_block.text
        except Exception as exc:
            errors.append(f"Claude API call failed: {exc}")
            logger.error("anomaly_claude_error", error=str(exc))
            return AgentResult(
                success=False,
                agent_name=self.agent_name,
                duration_ms=0.0,
                rows_processed=total_rows,
                errors=errors,
                warnings=warnings,
            )

        # Step 6: Parse response
        affected_series = list({a["series"] for a in sorted_anomalies})
        records = _parse_anomaly_sections(
            raw_text, run_id, generated_at, affected_series, anomaly_hash,
        )

        logger.info(
            "anomaly_records_parsed",
            count=len(records),
            languages=[r.language for r in records],
        )

        # Step 7: Store in Postgres
        try:
            await self._store_insights(records)
        except Exception as exc:
            errors.append(f"Postgres storage failed: {exc}")
            logger.error("anomaly_storage_error", error=str(exc))
            return AgentResult(
                success=False,
                agent_name=self.agent_name,
                duration_ms=0.0,
                rows_processed=total_rows,
                errors=errors,
                warnings=warnings,
            )

        return AgentResult(
            success=True,
            agent_name=self.agent_name,
            duration_ms=0.0,
            rows_processed=total_rows,
            warnings=warnings,
        )

    async def _anomaly_hash_exists(self, anomaly_hash: str) -> bool:
        """Check if an anomaly insight with this hash exists within 7 days."""
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

    async def _store_insights(self, records: list[InsightRecord]) -> None:
        """Persist anomaly InsightRecord objects to Postgres."""
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
            logger.info("anomaly_insights_stored", count=len(records))
        finally:
            await conn.close()
