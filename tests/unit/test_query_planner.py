"""Tests for QueryPlanner — agents/query/planner.py."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from agents.query.planner import QueryPlanner, _time_range_to_days
from api.models import AggregationLevel, ComparisonType


# ---------------------------------------------------------------------------
# QueryPlanner.parse_intent — series detection
# ---------------------------------------------------------------------------


class TestParseIntentSeries:
    def setup_method(self) -> None:
        self.planner = QueryPlanner()

    def test_detects_selic_keyword(self) -> None:
        intent = self.planner.parse_intent("What is the current SELIC rate?")
        assert "bcb_selic" in intent.series

    def test_detects_ipca_keyword(self) -> None:
        intent = self.planner.parse_intent("How has IPCA changed this year?")
        assert "bcb_ipca" in intent.series

    def test_detects_real_rate_keywords(self) -> None:
        intent = self.planner.parse_intent("What is the real interest rate?")
        assert "derived_real_rate" in intent.series

    def test_detects_yield_spread_keywords(self) -> None:
        intent = self.planner.parse_intent("How is the yield curve looking?")
        assert "derived_yield_spread" in intent.series

    def test_multiple_series_detected(self) -> None:
        intent = self.planner.parse_intent(
            "Compare SELIC and USD/BRL over the last year"
        )
        assert "bcb_selic" in intent.series
        assert "bcb_usd_brl" in intent.series

    def test_ambiguous_question_returns_all(self) -> None:
        intent = self.planner.parse_intent("How is the economy doing?")
        # Should fall back to all series
        assert len(intent.series) > 5


# ---------------------------------------------------------------------------
# QueryPlanner.parse_intent — time range
# ---------------------------------------------------------------------------


class TestParseIntentTimeRange:
    def setup_method(self) -> None:
        self.planner = QueryPlanner()

    def test_detects_explicit_1_year(self) -> None:
        intent = self.planner.parse_intent("Show SELIC for the last 1 year")
        assert intent.time_range == "1y"

    def test_detects_6_months(self) -> None:
        intent = self.planner.parse_intent("IPCA over the last 6 months")
        assert intent.time_range == "6m"

    def test_detects_ytd(self) -> None:
        intent = self.planner.parse_intent("What's the YTD performance?")
        assert intent.time_range == "ytd"

    def test_detects_5_years(self) -> None:
        intent = self.planner.parse_intent("Show 5 year trend for SELIC")
        assert intent.time_range == "5y"

    def test_default_all(self) -> None:
        intent = self.planner.parse_intent("What is the SELIC rate?")
        assert intent.time_range == "all"

    def test_detects_portuguese_time_range(self) -> None:
        intent = self.planner.parse_intent("SELIC nos últimos 3 meses")
        assert intent.time_range == "3m"

    def test_detects_year_range(self) -> None:
        intent = self.planner.parse_intent(
            "Compare SELIC with the 2015-2016 environment"
        )
        assert intent.time_range == "year_range:2015:2016"

    def test_detects_since_year(self) -> None:
        intent = self.planner.parse_intent("IPCA since 2010")
        assert intent.time_range == "since_year:2010"

    def test_detects_in_year(self) -> None:
        intent = self.planner.parse_intent("What was SELIC in 2015?")
        assert intent.time_range == "since_year:2015"


# ---------------------------------------------------------------------------
# QueryPlanner.parse_intent — comparison type
# ---------------------------------------------------------------------------


class TestParseIntentComparison:
    def setup_method(self) -> None:
        self.planner = QueryPlanner()

    def test_detects_compare_keyword(self) -> None:
        intent = self.planner.parse_intent("Compare SELIC and IPCA")
        assert intent.comparison == ComparisonType.CROSS_SERIES

    def test_detects_versus(self) -> None:
        intent = self.planner.parse_intent("SELIC vs USD/BRL")
        assert intent.comparison == ComparisonType.CROSS_SERIES

    def test_detects_trend(self) -> None:
        intent = self.planner.parse_intent("What is the trend in IPCA?")
        assert intent.comparison == ComparisonType.TREND

    def test_detects_over_time(self) -> None:
        intent = self.planner.parse_intent("How has SELIC changed over time?")
        assert intent.comparison == ComparisonType.TIME_SERIES

    def test_no_comparison(self) -> None:
        intent = self.planner.parse_intent("What is the current SELIC?")
        assert intent.comparison == ComparisonType.NONE


# ---------------------------------------------------------------------------
# QueryPlanner.parse_intent — aggregation level
# ---------------------------------------------------------------------------


class TestParseIntentAggregation:
    def setup_method(self) -> None:
        self.planner = QueryPlanner()

    def test_detects_monthly(self) -> None:
        intent = self.planner.parse_intent("Show monthly IPCA data")
        assert intent.aggregation == AggregationLevel.MONTHLY

    def test_detects_daily(self) -> None:
        intent = self.planner.parse_intent("Show daily SELIC rates")
        assert intent.aggregation == AggregationLevel.DAILY

    def test_detects_latest(self) -> None:
        intent = self.planner.parse_intent("What is the latest SELIC?")
        assert intent.aggregation == AggregationLevel.LATEST

    def test_default_monthly(self) -> None:
        intent = self.planner.parse_intent("How is SELIC doing?")
        assert intent.aggregation == AggregationLevel.MONTHLY

    def test_comparison_overrides_latest(self) -> None:
        """'current' triggers LATEST, but comparison should override to MONTHLY."""
        intent = self.planner.parse_intent(
            "Compare the current SELIC cycle with the 2015-2016 environment"
        )
        assert intent.aggregation == AggregationLevel.MONTHLY

    def test_historical_range_overrides_latest(self) -> None:
        """'current' triggers LATEST, but year range should override to MONTHLY."""
        intent = self.planner.parse_intent(
            "What was the current account balance in 2015?")
        assert intent.aggregation == AggregationLevel.MONTHLY


# ---------------------------------------------------------------------------
# _time_range_to_days
# ---------------------------------------------------------------------------


class TestTimeRangeToDays:
    def test_days(self) -> None:
        assert _time_range_to_days("30d") == 30

    def test_months(self) -> None:
        assert _time_range_to_days("6m") == 180

    def test_years(self) -> None:
        assert _time_range_to_days("2y") == 730

    def test_all(self) -> None:
        assert _time_range_to_days("all") is None

    def test_ytd(self) -> None:
        result = _time_range_to_days("ytd")
        assert result is not None
        assert 0 < result <= 366

    def test_invalid_defaults_to_none(self) -> None:
        assert _time_range_to_days("invalid") is None

    def test_year_range(self) -> None:
        result = _time_range_to_days("year_range:2015:2016")
        assert result is not None
        # Should cover from 2015 to now (~11 years)
        assert result > 3650

    def test_since_year(self) -> None:
        result = _time_range_to_days("since_year:2020")
        assert result is not None
        assert result > 1800


# ---------------------------------------------------------------------------
# QueryPlanner.build_context
# ---------------------------------------------------------------------------


def _make_gold_rows(
    series: str, n: int = 24, base_value: float = 10.0,
) -> list[dict[str, Any]]:
    """Create mock gold data rows."""
    rows: list[dict[str, Any]] = []
    for i in range(n):
        month = (i % 12) + 1
        year = 2024 + i // 12
        rows.append({
            "date": datetime(year, month, 1, tzinfo=timezone.utc),
            "value": base_value + i * 0.1,
            "series": series,
            "z_score": 0.5 if i % 6 == 0 else 0.1,
        })
    return rows


class TestBuildContext:
    @pytest.mark.asyncio()
    async def test_context_includes_relevant_series_detail(self) -> None:
        """Relevant series should have aggregated data, not just latest value."""
        planner = QueryPlanner()
        intent = planner.parse_intent("What is the SELIC rate?")

        selic_rows = _make_gold_rows("bcb_selic", 24, 13.75)

        with patch(
            "agents.query.planner.query_gold_series",
            new_callable=AsyncMock,
        ) as mock_query:
            # Return data only for SELIC
            async def _mock_query(series_id: str, after: str | None = None) -> list[dict[str, Any]]:
                if series_id == "bcb_selic":
                    return selic_rows
                return []

            mock_query.side_effect = _mock_query
            context = await planner.build_context(intent)

        # Context should contain SELIC label with detail
        assert "SELIC" in context
        # Should have aggregated data (period markers)
        assert "avg=" in context or "latest" in context.lower()

    @pytest.mark.asyncio()
    async def test_context_non_relevant_series_compact(self) -> None:
        """Non-relevant series should only have a one-line summary."""
        planner = QueryPlanner()
        intent = planner.parse_intent("What is the SELIC rate?")

        with patch(
            "agents.query.planner.query_gold_series",
            new_callable=AsyncMock,
        ) as mock_query:
            async def _mock_query(series_id: str, after: str | None = None) -> list[dict[str, Any]]:
                return _make_gold_rows(series_id, 24, 10.0)

            mock_query.side_effect = _mock_query
            context = await planner.build_context(intent)

        # Context should be present
        assert len(context) > 0
        lines = context.strip().split("\n")
        # Non-relevant series should be single-line summaries
        # The context should be more compact than sending all raw data
        assert len(lines) < 500  # sanity check

    @pytest.mark.asyncio()
    async def test_empty_data_handled_gracefully(self) -> None:
        """Empty gold data shouldn't crash."""
        planner = QueryPlanner()
        intent = planner.parse_intent("What is the SELIC rate?")

        with patch(
            "agents.query.planner.query_gold_series",
            new_callable=AsyncMock,
            return_value=[],
        ):
            context = await planner.build_context(intent)

        assert isinstance(context, str)
