"""AI usage audit log — full record of every LLM interaction for cost and quality monitoring."""

from __future__ import annotations

import os
from datetime import datetime, timezone

import asyncpg
import structlog

logger = structlog.get_logger()

# Haiku 4.5 pricing (per million tokens)
_HAIKU_INPUT_COST = 0.80  # $/1M input tokens
_HAIKU_OUTPUT_COST = 4.00  # $/1M output tokens

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ai_audit_log (
    id SERIAL PRIMARY KEY,
    question TEXT NOT NULL,
    system_prompt TEXT NOT NULL DEFAULT '',
    answer TEXT NOT NULL DEFAULT '',
    tier VARCHAR(20) NOT NULL,
    model VARCHAR(100) NOT NULL DEFAULT '',
    input_tokens INT NOT NULL DEFAULT 0,
    output_tokens INT NOT NULL DEFAULT 0,
    total_tokens INT NOT NULL DEFAULT 0,
    cost_usd DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    duration_ms DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    language VARCHAR(5) NOT NULL DEFAULT 'en',
    session_hash VARCHAR(32) NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


def estimate_cost(input_tokens: int, output_tokens: int) -> float:
    """Estimate USD cost for a Haiku query."""
    return (
        (input_tokens / 1_000_000) * _HAIKU_INPUT_COST
        + (output_tokens / 1_000_000) * _HAIKU_OUTPUT_COST
    )


async def log_query(
    question: str,
    system_prompt: str,
    answer: str,
    tier: str,
    model: str,
    input_tokens: int,
    output_tokens: int,
    duration_ms: float = 0.0,
    language: str = "en",
    session_hash: str = "",
) -> None:
    """Log a full AI interaction to Postgres. Fire-and-forget."""
    database_url = os.environ.get("DATABASE_URL", "")
    if not database_url:
        return

    total = input_tokens + output_tokens
    cost = estimate_cost(input_tokens, output_tokens) if model else 0.0

    try:
        conn: asyncpg.Connection = await asyncpg.connect(database_url)  # type: ignore[type-arg]
        try:
            await conn.execute(_CREATE_TABLE_SQL)
            await conn.execute(
                """
                INSERT INTO ai_audit_log
                    (question, system_prompt, answer, tier, model,
                     input_tokens, output_tokens, total_tokens, cost_usd,
                     duration_ms, language, session_hash, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                """,
                question[:500],
                system_prompt[:5000],
                answer[:5000],
                tier,
                model,
                input_tokens,
                output_tokens,
                total,
                cost,
                duration_ms,
                language,
                session_hash,
                datetime.now(timezone.utc),
            )
        finally:
            await conn.close()
    except Exception as exc:
        logger.warning("audit_log_failed", error=str(exc))


async def get_usage_summary() -> dict[str, object]:
    """Return aggregated usage stats from the audit log."""
    database_url = os.environ.get("DATABASE_URL", "")
    if not database_url:
        return {"error": "DATABASE_URL not set"}

    try:
        conn: asyncpg.Connection = await asyncpg.connect(database_url)  # type: ignore[type-arg]
        try:
            # Check which table exists (handle migration from old name)
            table = "ai_audit_log"
            exists = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = $1)",
                table,
            )
            if not exists:
                table = "query_usage"
                exists = await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = $1)",
                    table,
                )
                if not exists:
                    return {"total_queries": 0, "total_tokens": 0, "total_cost_usd": 0.0}

            row = await conn.fetchrow(f"""
                SELECT
                    COUNT(*) AS total_queries,
                    COALESCE(SUM(total_tokens), 0) AS total_tokens,
                    COALESCE(SUM(cost_usd), 0) AS total_cost_usd,
                    COALESCE(SUM(input_tokens), 0) AS total_input_tokens,
                    COALESCE(SUM(output_tokens), 0) AS total_output_tokens,
                    COALESCE(AVG(total_tokens), 0) AS avg_tokens_per_query,
                    COALESCE(AVG(duration_ms), 0) AS avg_duration_ms
                FROM {table}
            """)  # noqa: S608
            tier_rows = await conn.fetch(f"""
                SELECT tier, COUNT(*) AS count,
                       COALESCE(SUM(total_tokens), 0) AS tokens,
                       COALESCE(SUM(cost_usd), 0) AS cost
                FROM {table} GROUP BY tier ORDER BY tier
            """)  # noqa: S608
            today_row = await conn.fetchrow(f"""
                SELECT COUNT(*) AS queries_today, COALESCE(SUM(cost_usd), 0) AS cost_today
                FROM {table}
                WHERE created_at >= CURRENT_DATE
            """)  # noqa: S608
        finally:
            await conn.close()

        return {
            "total_queries": row["total_queries"] if row else 0,
            "total_tokens": row["total_tokens"] if row else 0,
            "total_cost_usd": round(float(row["total_cost_usd"]), 6) if row else 0.0,
            "total_input_tokens": row["total_input_tokens"] if row else 0,
            "total_output_tokens": row["total_output_tokens"] if row else 0,
            "avg_tokens_per_query": round(float(row["avg_tokens_per_query"]), 1) if row else 0.0,
            "avg_duration_ms": round(float(row["avg_duration_ms"]), 1) if row else 0.0,
            "by_tier": [
                {"tier": r["tier"], "count": r["count"], "tokens": r["tokens"],
                 "cost_usd": round(float(r["cost"]), 6)}
                for r in tier_rows
            ],
            "today": {
                "queries": today_row["queries_today"] if today_row else 0,
                "cost_usd": round(float(today_row["cost_today"]), 6) if today_row else 0.0,
            },
        }
    except Exception as exc:
        logger.warning("usage_summary_failed", error=str(exc))
        return {"error": str(exc)}
