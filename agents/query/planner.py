"""QueryPlanner — pre-aggregates data in DuckDB to minimize LLM token usage.

Principle: "DuckDB answers WHAT and HOW MUCH; Claude answers WHAT DOES IT MEAN."
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

import duckdb
import pyarrow as pa
import structlog

from agents.query.router import METRIC_KEYWORDS, detect_domains, get_series_for_domains
from api.dependencies import query_gold_series
from config import get_domain_config
from api.models import (
    AggregationLevel,
    ComparisonType,
    QueryIntent,
)
from api.series_config import SERIES_DISPLAY, get_display_label

logger = structlog.get_logger()

# All series we know about
ALL_SERIES: list[str] = list(SERIES_DISPLAY.keys())

# Time range patterns
_TIME_RANGE_PATTERNS: list[tuple[str, str]] = [
    (r"(?:last|past|previous|últimos?)\s+(\d+)\s*(?:day|dia)s?", "{n}d"),
    (r"(?:last|past|previous|últimos?)\s+(\d+)\s*(?:month|mês|meses)s?", "{n}m"),
    (r"(?:last|past|previous|últimos?)\s+(\d+)\s*(?:year|ano)s?", "{n}y"),
    (r"\b(?:ytd|year.to.date|acumulado.ano)\b", "ytd"),
    (r"\b(?:mtd|month.to.date)\b", "1m"),
    (r"\b(?:this\s+year|este\s+ano)\b", "ytd"),
    (r"\b(?:3\s*m(?:onths?)?|trimestre|quarter)\b", "3m"),
    (r"\b(?:6\s*m(?:onths?)?|semestre)\b", "6m"),
    (r"\b(?:1\s*y(?:ear)?|12\s*m(?:onths?)?|um\s+ano)\b", "1y"),
    (r"\b(?:2\s*y(?:ears?)?|dois\s+anos)\b", "2y"),
    (r"\b(?:5\s*y(?:ears?)?|cinco\s+anos)\b", "5y"),
    (r"\b(?:10\s*y(?:ears?)?|dez\s+anos)\b", "10y"),
    (r"\b(?:all\s+time|todo\s+período|histórico|historical)\b", "all"),
    # Explicit year ranges: "2015-2016", "2015 to 2016", "from 2015 to 2016"
    (r"(?:from\s+)?(\d{4})\s*[-–to]+\s*(\d{4})", "year_range"),
    # Single year reference: "in 2015", "since 2020", "em 2015"
    (r"(?:in|since|desde|em|of)\s+(\d{4})\b", "since_year"),
]

# Comparison type patterns
_COMPARISON_PATTERNS: list[tuple[str, ComparisonType]] = [
    (r"\bcompar[eai]\b|\bversus\b|\bvs\.?\b|\bagainst\b|\bcontra\b", ComparisonType.CROSS_SERIES),
    (r"\btrend\b|\btendência\b|\btrajectory\b|\btrajetória\b|\bevolução\b|\bevolution\b", ComparisonType.TREND),
    (r"\bover\s+time\b|\bao\s+longo\b|\bhistor[yia]\b|\bsince\b|\bdesde\b", ComparisonType.TIME_SERIES),
]

# Aggregation patterns
_AGGREGATION_PATTERNS: list[tuple[str, AggregationLevel]] = [
    (r"\bdail[yia]\b|\bdiári[ao]\b", AggregationLevel.DAILY),
    (r"\bmonth\b|\bmensal\b|\bmonthly\b", AggregationLevel.MONTHLY),
    (r"\bquarter\b|\btrimest\b|\bquarterly\b", AggregationLevel.QUARTERLY),
    (r"\byear\b|\banual\b|\bannual\b|\byearly\b", AggregationLevel.YEARLY),
    (r"\blatest\b|\batual\b|\bcurrent\b|\bhoje\b|\btoday\b|\brecent\b", AggregationLevel.LATEST),
]


def _time_range_to_days(time_range: str) -> int | None:
    """Convert a time range string to number of days, or None for 'all'."""
    if time_range == "all":
        return None
    if time_range == "ytd":
        now = datetime.now(timezone.utc)
        return (now - datetime(now.year, 1, 1, tzinfo=timezone.utc)).days

    # Explicit year range: "year_range:2015:2016"
    if time_range.startswith("year_range:"):
        parts = time_range.split(":")
        start_year = int(parts[1])
        now = datetime.now(timezone.utc)
        return (now - datetime(start_year, 1, 1, tzinfo=timezone.utc)).days

    # Since a specific year: "since_year:2015"
    if time_range.startswith("since_year:"):
        year = int(time_range.split(":")[1])
        now = datetime.now(timezone.utc)
        return (now - datetime(year, 1, 1, tzinfo=timezone.utc)).days

    match = re.match(r"(\d+)([dmy])", time_range)
    if not match:
        return None  # default: all data

    n = int(match.group(1))
    unit = match.group(2)
    if unit == "d":
        return n
    elif unit == "m":
        return n * 30
    else:  # "y"
        return n * 365


class QueryPlanner:
    """Parses questions into QueryIntent and pre-aggregates data via DuckDB.

    Reduces LLM token usage by:
    1. Identifying only relevant series (not sending all 50+)
    2. Pre-aggregating to monthly averages instead of daily rows
    3. Computing summary statistics in DuckDB
    4. Building a compact context string
    """

    def parse_intent(self, question: str) -> QueryIntent:
        """Parse a natural-language question into a structured QueryIntent."""
        lowered = question.lower()

        # Detect relevant series
        series = self._detect_series(lowered)

        # Detect time range
        time_range = self._detect_time_range(lowered)

        # Detect comparison type
        comparison = self._detect_comparison(lowered)

        # Detect aggregation level
        aggregation = self._detect_aggregation(lowered)

        # A comparison or historical query needs data over time, not just
        # the latest value.  Override LATEST when context demands a range.
        if aggregation == AggregationLevel.LATEST and (
            comparison != ComparisonType.NONE
            or time_range.startswith(("year_range:", "since_year:"))
        ):
            aggregation = AggregationLevel.MONTHLY

        intent = QueryIntent(
            series=series,
            time_range=time_range,
            comparison=comparison,
            aggregation=aggregation,
        )
        logger.info(
            "query_planner.intent_parsed",
            question=question[:120],
            series=series,
            time_range=time_range,
            comparison=comparison.value,
            aggregation=aggregation.value,
            days=_time_range_to_days(time_range),
        )
        return intent

    async def build_context(self, intent: QueryIntent) -> str:
        """Build a compact, pre-aggregated context string for the LLM.

        This is the core token-saving mechanism. Instead of sending raw
        data points, we send aggregated summaries.
        """
        lines: list[str] = []
        days = _time_range_to_days(intent.time_range)

        for series_id in ALL_SERIES:
            label = get_display_label(series_id)
            meta = SERIES_DISPLAY.get(series_id, {})
            unit = meta.get("unit", "")
            desc = meta.get("description", "")

            rows = await query_gold_series(series_id)
            if not rows:
                continue

            if series_id in intent.series:
                # Relevant series: provide aggregated data
                context = self._build_series_context(
                    rows, label, unit, desc, days, intent.aggregation,
                    time_range=intent.time_range,
                    series_id=series_id,
                )
                lines.append(context)
            else:
                # Non-relevant: one-line summary
                latest = rows[-1]
                latest_val = latest["value"]
                latest_date = (
                    latest["date"].strftime("%Y-%m-%d")
                    if isinstance(latest["date"], datetime)
                    else str(latest["date"])
                )
                lines.append(f"{label} ({unit}): latest {latest_val} on {latest_date}")

        context = "\n".join(lines)
        logger.info(
            "query_planner.context_built",
            relevant_series=len(intent.series),
            context_lines=len(lines),
            context_chars=len(context),
            time_range=intent.time_range,
            days=days,
        )
        return context

    def _detect_series(self, lowered: str) -> list[str]:
        """Identify relevant series from the question."""
        found: set[str] = set()

        # Keyword matching
        for keyword, series_id in METRIC_KEYWORDS.items():
            if keyword in lowered:
                found.add(series_id)

        if found:
            # Add domain siblings for context
            domains = detect_domains(lowered)
            if domains:
                domain_series = get_series_for_domains(domains)
                found.update(domain_series)
            return list(found)

        # Fallback: domain detection
        domains = detect_domains(lowered)
        if domains:
            return get_series_for_domains(domains)

        # Fully ambiguous: return all series
        return ALL_SERIES

    def _detect_time_range(self, lowered: str) -> str:
        """Extract time range from question text."""
        for pattern, template in _TIME_RANGE_PATTERNS:
            match = re.search(pattern, lowered)
            if match:
                if template == "year_range":
                    return f"year_range:{match.group(1)}:{match.group(2)}"
                if template == "since_year":
                    return f"since_year:{match.group(1)}"
                if "{n}" in template:
                    return template.format(n=match.group(1))
                return template
        return "all"  # default: send full history

    def _detect_comparison(self, lowered: str) -> ComparisonType:
        """Detect if the question involves a comparison."""
        for pattern, comp_type in _COMPARISON_PATTERNS:
            if re.search(pattern, lowered):
                return comp_type
        return ComparisonType.NONE

    def _detect_aggregation(self, lowered: str) -> AggregationLevel:
        """Detect desired aggregation level."""
        for pattern, agg_level in _AGGREGATION_PATTERNS:
            if re.search(pattern, lowered):
                return agg_level
        return AggregationLevel.MONTHLY  # default

    @staticmethod
    def _row_date_utc(r: dict[str, Any]) -> datetime:
        """Normalize a row's date to a timezone-aware UTC datetime."""
        d = r["date"]
        if isinstance(d, datetime):
            return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
        return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)

    def _filter_rows_by_time(
        self,
        rows: list[dict[str, Any]],
        days: int | None,
        time_range: str,
    ) -> list[dict[str, Any]]:
        """Filter rows to the relevant time windows.

        For year_range queries (e.g. year_range:2015:2016), returns only the
        specified window plus the most recent 12 months — skipping the gap in
        between to keep token usage low.
        """
        from datetime import timedelta

        if time_range.startswith("year_range:"):
            parts = time_range.split(":")
            start_year, end_year = int(parts[1]), int(parts[2])
            window_start = datetime(start_year, 1, 1, tzinfo=timezone.utc)
            window_end = datetime(end_year, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
            recent_cutoff = datetime.now(timezone.utc) - timedelta(days=365)

            filtered = [
                r for r in rows
                if (window_start <= self._row_date_utc(r) <= window_end)
                or (self._row_date_utc(r) >= recent_cutoff)
            ]
            return filtered if filtered else rows[-12:]

        if days is None:
            return rows

        cutoff = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0,
        ) - timedelta(days=days)

        filtered = [r for r in rows if self._row_date_utc(r) >= cutoff]
        return filtered if filtered else rows[-12:]

    def _build_series_context(
        self,
        rows: list[dict[str, Any]],
        label: str,
        unit: str,
        desc: str,
        days: int | None,
        aggregation: AggregationLevel,
        time_range: str = "all",
        series_id: str | None = None,
    ) -> str:
        """Build an aggregated context string for a relevant series."""
        rows = self._filter_rows_by_time(rows, days, time_range)

        if not rows:
            return f"{label} ({unit}): no data in requested period"

        if aggregation == AggregationLevel.LATEST:
            latest = rows[-1]
            d = latest["date"]
            date_str = d.strftime("%Y-%m-%d") if isinstance(d, datetime) else str(d)
            z = latest.get("z_score", "N/A")
            mom = latest.get("mom_delta", "N/A")

            # Check typical range from series config
            range_flag = ""
            if series_id:
                cfg = get_domain_config()
                scfg = cfg.series.get(series_id)
                if scfg and scfg.typical_range:
                    tr = scfg.typical_range
                    val = latest["value"]
                    if val is not None and (val < tr.min or val > tr.max):
                        range_flag = (
                            f" [WARNING: value {val} is outside typical "
                            f"range {tr.min} to {tr.max}]"
                        )

            return (
                f"{label} ({unit}) — {desc}\n"
                f"  Latest: {latest['value']} on {date_str} "
                f"(z-score={z}, MoM delta={mom}){range_flag}"
            )

        # Aggregate via DuckDB for efficiency
        return self._aggregate_with_duckdb(rows, label, unit, desc, aggregation)

    def _aggregate_with_duckdb(
        self,
        rows: list[dict[str, Any]],
        label: str,
        unit: str,
        desc: str,
        aggregation: AggregationLevel,
    ) -> str:
        """Use DuckDB to aggregate rows into a compact summary."""
        # Convert rows to Arrow table for DuckDB
        dates = []
        values = []
        z_scores = []
        for r in rows:
            d = r["date"]
            if isinstance(d, datetime):
                dates.append(d)
            else:
                dates.append(datetime(d.year, d.month, d.day, tzinfo=timezone.utc))
            values.append(float(r["value"]) if r["value"] is not None else None)
            z_scores.append(
                float(r["z_score"]) if r.get("z_score") is not None else None
            )

        table = pa.table({
            "date": pa.array(dates, type=pa.timestamp("us", tz="UTC")),
            "value": pa.array(values, type=pa.float64()),
            "z_score": pa.array(z_scores, type=pa.float64()),
        })

        conn = duckdb.connect()
        conn.register("series_data", table)

        if aggregation == AggregationLevel.MONTHLY:
            trunc = "month"
        elif aggregation == AggregationLevel.QUARTERLY:
            trunc = "quarter"
        elif aggregation == AggregationLevel.YEARLY:
            trunc = "year"
        else:
            trunc = "month"

        # Aggregate: period avg, min, max + anomaly count
        agg_sql = f"""
            SELECT
                DATE_TRUNC('{trunc}', date) AS period,
                ROUND(AVG(value), 4) AS avg_val,
                ROUND(MIN(value), 4) AS min_val,
                ROUND(MAX(value), 4) AS max_val,
                COUNT(*) AS n,
                SUM(CASE WHEN ABS(z_score) > 2.0 THEN 1 ELSE 0 END) AS anomalies
            FROM series_data
            WHERE value IS NOT NULL
            GROUP BY DATE_TRUNC('{trunc}', date)
            ORDER BY period
        """

        logger.debug(
            "query_planner.duckdb_sql",
            series=label,
            sql=agg_sql.strip(),
            input_rows=len(rows),
        )
        result = conn.execute(agg_sql).fetchall()
        conn.close()
        logger.debug(
            "query_planner.duckdb_result",
            series=label,
            output_rows=len(result),
        )

        header = f"{label} ({unit})"
        if desc:
            header += f" — {desc}"

        parts = [header]

        # Summary stats
        all_vals = [v for v in values if v is not None]
        if all_vals:
            parts.append(
                f"  Period: {len(result)} {trunc}s, "
                f"range {all_vals[0]:.2f} to {all_vals[-1]:.2f}, "
                f"overall avg={sum(all_vals)/len(all_vals):.2f}"
            )

        # Aggregated data points
        for row in result:
            period_str = row[0].strftime("%Y-%m") if trunc == "month" else (
                row[0].strftime("%Y-Q") + str((row[0].month - 1) // 3 + 1)
                if trunc == "quarter" else row[0].strftime("%Y")
            )
            anomaly_flag = " [!]" if row[5] and row[5] > 0 else ""
            parts.append(
                f"  {period_str}: avg={row[1]}, "
                f"range=[{row[2]}, {row[3]}]{anomaly_flag}"
            )

        return "\n".join(parts)
