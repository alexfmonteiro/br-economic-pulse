"""Tests for CrossSeriesTask — tasks/cross_series/task.py."""

from __future__ import annotations

import io
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from storage.local import LocalStorageBackend
from tasks.cross_series.task import (
    CrossSeriesTask,
    compute_ipca_gdp_divergence,
    compute_real_rate,
    compute_yield_spread,
)


@pytest.fixture()
def storage(tmp_path: Path) -> LocalStorageBackend:
    return LocalStorageBackend(tmp_path)


def _make_gold_table(
    series: str,
    dates: list[str],
    values: list[float],
    unit: str = "% a.a.",
) -> pa.Table:
    """Create a gold-format PyArrow table."""
    n = len(dates)
    now_str = datetime.now(timezone.utc).isoformat()
    return pa.table({
        "date": pa.array(
            [datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=timezone.utc) for d in dates],
            type=pa.timestamp("us", tz="UTC"),
        ),
        "value": pa.array(values, type=pa.float64()),
        "series": pa.array([series] * n, type=pa.string()),
        "unit": pa.array([unit] * n, type=pa.string()),
        "last_updated_at": pa.array([now_str] * n, type=pa.string()),
        "calculation_version": pa.array(["1.0.0"] * n, type=pa.string()),
        "mom_delta": pa.array([None] * n, type=pa.float64()),
        "yoy_delta": pa.array([None] * n, type=pa.float64()),
        "rolling_12m_avg": pa.array([None] * n, type=pa.float64()),
        "z_score": pa.array([None] * n, type=pa.float64()),
    })


async def _write_gold(storage: LocalStorageBackend, series: str, table: pa.Table) -> None:
    """Write a gold parquet file to storage."""
    buf = io.BytesIO()
    pq.write_table(table, buf)
    await storage.write(f"gold/{series}.parquet", buf.getvalue())


# Generate 24 months of dates for realistic test data
_MONTHLY_DATES = [f"2024-{m:02d}-01" for m in range(1, 13)] + [
    f"2025-{m:02d}-01" for m in range(1, 13)
]


# ---------------------------------------------------------------------------
# compute_real_rate
# ---------------------------------------------------------------------------


class TestComputeRealRate:
    def test_basic_real_rate_computation(self) -> None:
        """SELIC annualized minus trailing 12M compounded IPCA."""
        # 24 months of SELIC at 13.75% annualized
        selic = _make_gold_table(
            "bcb_selic", _MONTHLY_DATES, [13.75] * 24,
        )
        # 24 months of IPCA at 0.5% monthly => ~6.17% annualized
        ipca = _make_gold_table(
            "bcb_ipca", _MONTHLY_DATES, [0.5] * 24, unit="% m/m",
        )

        result = compute_real_rate(selic, ipca)
        assert result.num_rows > 0
        assert "value" in result.column_names
        assert "series" in result.column_names

        # The real rate should be roughly 13.75 - 6.17 = ~7.58
        values = result.column("value").to_pylist()
        non_null = [v for v in values if v is not None]
        assert len(non_null) > 0
        # Real rate should be positive (SELIC > IPCA)
        assert non_null[-1] > 0
        # Should be approximately 7.5-7.7 range
        assert 6.0 < non_null[-1] < 9.0

    def test_gold_columns_present(self) -> None:
        """Derived gold table has all standard gold columns."""
        selic = _make_gold_table("bcb_selic", _MONTHLY_DATES, [13.75] * 24)
        ipca = _make_gold_table("bcb_ipca", _MONTHLY_DATES, [0.5] * 24)

        result = compute_real_rate(selic, ipca)
        expected_cols = {
            "date", "value", "series", "unit", "last_updated_at",
            "calculation_version", "mom_delta", "yoy_delta",
            "rolling_12m_avg", "z_score",
        }
        assert expected_cols == set(result.column_names)

    def test_series_name_is_derived_real_rate(self) -> None:
        selic = _make_gold_table("bcb_selic", _MONTHLY_DATES, [13.75] * 24)
        ipca = _make_gold_table("bcb_ipca", _MONTHLY_DATES, [0.5] * 24)

        result = compute_real_rate(selic, ipca)
        series_vals = result.column("series").to_pylist()
        assert all(s == "derived_real_rate" for s in series_vals)

    def test_high_inflation_negative_real_rate(self) -> None:
        """When IPCA > SELIC, real rate should be negative."""
        selic = _make_gold_table("bcb_selic", _MONTHLY_DATES, [5.0] * 24)
        # 1.5% monthly IPCA => ~19.6% annualized
        ipca = _make_gold_table("bcb_ipca", _MONTHLY_DATES, [1.5] * 24)

        result = compute_real_rate(selic, ipca)
        values = result.column("value").to_pylist()
        non_null = [v for v in values if v is not None]
        assert len(non_null) > 0
        # Real rate should be negative
        assert non_null[-1] < 0


# ---------------------------------------------------------------------------
# compute_ipca_gdp_divergence
# ---------------------------------------------------------------------------


class TestComputeIpcaGdpDivergence:
    def test_basic_divergence(self) -> None:
        """Divergence should be computed as z(IPCA) - z(IBC-Br)."""
        ipca = _make_gold_table(
            "bcb_ipca", _MONTHLY_DATES, [0.5] * 24,
        )
        ibc = _make_gold_table(
            "bcb_ibc_br", _MONTHLY_DATES, [140.0] * 24, unit="index",
        )

        result = compute_ipca_gdp_divergence(ipca, ibc)
        assert result.num_rows > 0
        assert "value" in result.column_names

    def test_rising_inflation_positive_divergence(self) -> None:
        """Rising IPCA with flat GDP should produce positive divergence."""
        # IPCA rising over time
        ipca_values = [0.3 + i * 0.05 for i in range(24)]
        ipca = _make_gold_table("bcb_ipca", _MONTHLY_DATES, ipca_values)
        # Flat GDP proxy
        ibc = _make_gold_table("bcb_ibc_br", _MONTHLY_DATES, [140.0] * 24, unit="index")

        result = compute_ipca_gdp_divergence(ipca, ibc)
        values = result.column("value").to_pylist()
        non_null = [v for v in values if v is not None]
        # Should have some positive divergence values (inflation outpacing growth)
        assert any(v > 0 for v in non_null)

    def test_series_name(self) -> None:
        ipca = _make_gold_table("bcb_ipca", _MONTHLY_DATES, [0.5] * 24)
        ibc = _make_gold_table("bcb_ibc_br", _MONTHLY_DATES, [140.0] * 24)

        result = compute_ipca_gdp_divergence(ipca, ibc)
        series_vals = result.column("series").to_pylist()
        assert all(s == "derived_ipca_gdp_divergence" for s in series_vals)


# ---------------------------------------------------------------------------
# compute_yield_spread
# ---------------------------------------------------------------------------


class TestComputeYieldSpread:
    def test_normal_curve_positive_spread(self) -> None:
        """Long rate > short rate => positive spread (normal curve)."""
        long_rate = _make_gold_table(
            "tesouro_prefixado_longo", _MONTHLY_DATES, [12.0] * 24,
        )
        short_rate = _make_gold_table(
            "tesouro_prefixado_curto", _MONTHLY_DATES, [11.0] * 24,
        )

        result = compute_yield_spread(long_rate, short_rate)
        values = result.column("value").to_pylist()
        non_null = [v for v in values if v is not None]
        assert len(non_null) > 0
        # Spread should be ~1.0 (12 - 11)
        assert all(abs(v - 1.0) < 0.01 for v in non_null)

    def test_inverted_curve_negative_spread(self) -> None:
        """Short rate > long rate => negative spread (inverted curve)."""
        long_rate = _make_gold_table(
            "tesouro_prefixado_longo", _MONTHLY_DATES, [10.0] * 24,
        )
        short_rate = _make_gold_table(
            "tesouro_prefixado_curto", _MONTHLY_DATES, [12.0] * 24,
        )

        result = compute_yield_spread(long_rate, short_rate)
        values = result.column("value").to_pylist()
        non_null = [v for v in values if v is not None]
        assert all(v < 0 for v in non_null)

    def test_series_name(self) -> None:
        long_rate = _make_gold_table("tesouro_prefixado_longo", _MONTHLY_DATES, [12.0] * 24)
        short_rate = _make_gold_table("tesouro_prefixado_curto", _MONTHLY_DATES, [11.0] * 24)

        result = compute_yield_spread(long_rate, short_rate)
        series_vals = result.column("series").to_pylist()
        assert all(s == "derived_yield_spread" for s in series_vals)

    def test_gold_columns_present(self) -> None:
        long_rate = _make_gold_table("tesouro_prefixado_longo", _MONTHLY_DATES, [12.0] * 24)
        short_rate = _make_gold_table("tesouro_prefixado_curto", _MONTHLY_DATES, [11.0] * 24)

        result = compute_yield_spread(long_rate, short_rate)
        expected_cols = {
            "date", "value", "series", "unit", "last_updated_at",
            "calculation_version", "mom_delta", "yoy_delta",
            "rolling_12m_avg", "z_score",
        }
        assert expected_cols == set(result.column_names)


# ---------------------------------------------------------------------------
# CrossSeriesTask integration tests
# ---------------------------------------------------------------------------


class TestCrossSeriesTask:
    @pytest.mark.asyncio()
    async def test_computes_all_derived_series(self, storage: LocalStorageBackend) -> None:
        """Task computes and writes all three derived series."""
        # Write source gold data
        await _write_gold(storage, "bcb_selic", _make_gold_table(
            "bcb_selic", _MONTHLY_DATES, [13.75] * 24,
        ))
        await _write_gold(storage, "bcb_ipca", _make_gold_table(
            "bcb_ipca", _MONTHLY_DATES, [0.5] * 24,
        ))
        await _write_gold(storage, "bcb_ibc_br", _make_gold_table(
            "bcb_ibc_br", _MONTHLY_DATES, [140.0] * 24,
        ))
        await _write_gold(storage, "tesouro_prefixado_longo", _make_gold_table(
            "tesouro_prefixado_longo", _MONTHLY_DATES, [12.0] * 24,
        ))
        await _write_gold(storage, "tesouro_prefixado_curto", _make_gold_table(
            "tesouro_prefixado_curto", _MONTHLY_DATES, [11.0] * 24,
        ))

        task = CrossSeriesTask(storage=storage)
        result = await task.run()

        assert result.success
        assert result.task_name == "cross_series"
        assert result.rows_processed > 0
        assert len(result.series_reconciliation) == 3

        # Verify all three gold files exist
        assert await storage.exists("gold/derived_real_rate.parquet")
        assert await storage.exists("gold/derived_ipca_gdp_divergence.parquet")
        assert await storage.exists("gold/derived_yield_spread.parquet")

    @pytest.mark.asyncio()
    async def test_partial_success_missing_sources(self, storage: LocalStorageBackend) -> None:
        """Task succeeds partially when some source data is missing."""
        # Only provide data for yield spread
        await _write_gold(storage, "tesouro_prefixado_longo", _make_gold_table(
            "tesouro_prefixado_longo", _MONTHLY_DATES, [12.0] * 24,
        ))
        await _write_gold(storage, "tesouro_prefixado_curto", _make_gold_table(
            "tesouro_prefixado_curto", _MONTHLY_DATES, [11.0] * 24,
        ))

        task = CrossSeriesTask(storage=storage)
        result = await task.run()

        assert result.success  # partial success
        assert len(result.warnings) >= 2  # two derivations failed
        assert len(result.series_reconciliation) == 1  # only yield spread
        assert await storage.exists("gold/derived_yield_spread.parquet")

    @pytest.mark.asyncio()
    async def test_fails_when_no_sources(self, storage: LocalStorageBackend) -> None:
        """Task fails when no source data exists at all."""
        task = CrossSeriesTask(storage=storage)
        result = await task.run()

        assert not result.success
        assert len(result.errors) > 0

    @pytest.mark.asyncio()
    async def test_derived_series_in_gold_format(self, storage: LocalStorageBackend) -> None:
        """Derived gold files have the standard gold schema."""
        await _write_gold(storage, "tesouro_prefixado_longo", _make_gold_table(
            "tesouro_prefixado_longo", _MONTHLY_DATES, [12.0] * 24,
        ))
        await _write_gold(storage, "tesouro_prefixado_curto", _make_gold_table(
            "tesouro_prefixado_curto", _MONTHLY_DATES, [11.0] * 24,
        ))

        task = CrossSeriesTask(storage=storage)
        await task.run()

        # Read derived gold and verify format
        data = await storage.read("gold/derived_yield_spread.parquet")
        table = pq.read_table(io.BytesIO(data))

        expected_cols = {
            "date", "value", "series", "unit", "last_updated_at",
            "calculation_version", "mom_delta", "yoy_delta",
            "rolling_12m_avg", "z_score",
        }
        assert expected_cols == set(table.column_names)
        assert table.num_rows > 0

    @pytest.mark.asyncio()
    async def test_reconciliation_data(self, storage: LocalStorageBackend) -> None:
        """Reconciliation records are populated correctly."""
        await _write_gold(storage, "tesouro_prefixado_longo", _make_gold_table(
            "tesouro_prefixado_longo", _MONTHLY_DATES, [12.0] * 24,
        ))
        await _write_gold(storage, "tesouro_prefixado_curto", _make_gold_table(
            "tesouro_prefixado_curto", _MONTHLY_DATES, [11.0] * 24,
        ))

        task = CrossSeriesTask(storage=storage)
        result = await task.run()

        recon = result.series_reconciliation[0]
        assert recon.series_id == "derived_yield_spread"
        assert recon.rows_in == 48  # 24 + 24 source rows
        assert recon.rows_out > 0
