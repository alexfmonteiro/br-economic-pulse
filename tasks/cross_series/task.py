"""CrossSeriesTask — computes derived gold metrics from multiple gold series."""

from __future__ import annotations

import io
from datetime import datetime, timezone

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from api.models import SeriesReconciliation, TaskResult
from config import get_domain_config
from storage.protocol import StorageBackend
from tasks.base import BaseTask

logger = structlog.get_logger()

GOLD_CALCULATION_VERSION = "1.0.0"

# Derived series definitions: each maps a derived series ID to the
# source series it depends on and the SQL that computes it.
DERIVED_SERIES: dict[str, dict[str, str | list[str]]] = {
    "derived_real_rate": {
        "sources": ["bcb_selic", "bcb_ipca"],
        "unit": "% a.a.",
        "description": "Real interest rate: SELIC minus trailing 12M annualized IPCA",
    },
    "derived_ipca_gdp_divergence": {
        "sources": ["bcb_ipca", "bcb_ibc_br"],
        "unit": "index",
        "description": "IPCA vs GDP proxy normalized spread",
    },
    "derived_yield_spread": {
        "sources": ["tesouro_prefixado_longo", "tesouro_prefixado_curto"],
        "unit": "p.p.",
        "description": "Long minus short prefixado yield curve slope",
    },
}


def _read_gold_table(parquet_bytes: bytes) -> pa.Table:
    """Read a gold parquet file into a PyArrow table."""
    return pq.read_table(io.BytesIO(parquet_bytes))


def compute_real_rate(
    selic_table: pa.Table, ipca_table: pa.Table,
) -> pa.Table:
    """Compute real interest rate: SELIC annualized - trailing 12M annualized IPCA.

    SELIC is already annualized (% a.a.).
    IPCA is monthly MoM change (%). We annualize by compounding trailing 12 months.
    """
    conn = duckdb.connect()
    conn.register("selic", selic_table)
    conn.register("ipca", ipca_table)

    now_str = datetime.now(timezone.utc).isoformat()

    sql = f"""
        WITH ipca_annualized AS (
            SELECT
                date,
                -- Compound trailing 12 months of monthly IPCA to get annualized rate
                -- Formula: ((1 + m1/100) * (1 + m2/100) * ... * (1 + m12/100) - 1) * 100
                -- We use EXP(SUM(LN(1 + value/100))) over rolling 12 months
                CASE
                    WHEN COUNT(value) OVER (
                        ORDER BY date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                    ) >= 12
                    THEN (EXP(SUM(LN(1 + value / 100.0)) OVER (
                        ORDER BY date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                    )) - 1) * 100
                    ELSE NULL
                END AS ipca_12m
            FROM ipca
            WHERE value IS NOT NULL
        ),
        joined AS (
            SELECT
                s.date,
                s.value AS selic_rate,
                ia.ipca_12m
            FROM selic s
            INNER JOIN ipca_annualized ia ON s.date = ia.date
            WHERE ia.ipca_12m IS NOT NULL
        )
        SELECT
            date,
            ROUND(selic_rate - ipca_12m, 4) AS value,
            'derived_real_rate' AS series,
            '% a.a.' AS unit,
            '{now_str}' AS last_updated_at,
            '{GOLD_CALCULATION_VERSION}' AS calculation_version,
            value - LAG(value) OVER (ORDER BY date) AS mom_delta,
            CASE
                WHEN LAG(value, 12) OVER (ORDER BY date) IS NOT NULL
                    AND LAG(value, 12) OVER (ORDER BY date) != 0
                THEN ((value - LAG(value, 12) OVER (ORDER BY date))
                      / ABS(LAG(value, 12) OVER (ORDER BY date))) * 100
                ELSE NULL
            END AS yoy_delta,
            AVG(value) OVER (
                ORDER BY date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
            ) AS rolling_12m_avg,
            CASE
                WHEN COUNT(value) OVER (
                    ORDER BY date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                ) >= 2
                THEN (value - AVG(value) OVER (
                    ORDER BY date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                )) / NULLIF(STDDEV(value) OVER (
                    ORDER BY date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                ), 0)
                ELSE NULL
            END AS z_score
        FROM joined
        ORDER BY date
    """
    result = conn.execute(sql)
    table = result.to_arrow_table()
    conn.close()
    return table


def compute_ipca_gdp_divergence(
    ipca_table: pa.Table, ibc_table: pa.Table,
) -> pa.Table:
    """Compute IPCA vs GDP proxy (IBC-Br) normalized spread.

    Both series are z-score normalized over trailing 12 months,
    then the divergence is IPCA_z - GDP_z. Positive = inflation
    outpacing growth (stagflationary signal).
    """
    conn = duckdb.connect()
    conn.register("ipca", ipca_table)
    conn.register("ibc", ibc_table)

    now_str = datetime.now(timezone.utc).isoformat()

    sql = f"""
        WITH ipca_z AS (
            SELECT
                date,
                CASE
                    WHEN COUNT(value) OVER (
                        ORDER BY date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                    ) >= 2 AND STDDEV(value) OVER (
                        ORDER BY date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                    ) > 0
                    THEN (value - AVG(value) OVER (
                        ORDER BY date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                    )) / STDDEV(value) OVER (
                        ORDER BY date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                    )
                    WHEN COUNT(value) OVER (
                        ORDER BY date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                    ) >= 2
                    THEN 0.0
                    ELSE NULL
                END AS z_ipca
            FROM ipca
            WHERE value IS NOT NULL
        ),
        ibc_z AS (
            SELECT
                date,
                CASE
                    WHEN COUNT(value) OVER (
                        ORDER BY date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                    ) >= 2 AND STDDEV(value) OVER (
                        ORDER BY date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                    ) > 0
                    THEN (value - AVG(value) OVER (
                        ORDER BY date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                    )) / STDDEV(value) OVER (
                        ORDER BY date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                    )
                    WHEN COUNT(value) OVER (
                        ORDER BY date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                    ) >= 2
                    THEN 0.0
                    ELSE NULL
                END AS z_ibc
            FROM ibc
            WHERE value IS NOT NULL
        ),
        joined AS (
            SELECT
                i.date,
                i.z_ipca,
                g.z_ibc,
                ROUND(i.z_ipca - g.z_ibc, 4) AS divergence
            FROM ipca_z i
            INNER JOIN ibc_z g ON i.date = g.date
            WHERE i.z_ipca IS NOT NULL AND g.z_ibc IS NOT NULL
        )
        SELECT
            date,
            divergence AS value,
            'derived_ipca_gdp_divergence' AS series,
            'index' AS unit,
            '{now_str}' AS last_updated_at,
            '{GOLD_CALCULATION_VERSION}' AS calculation_version,
            value - LAG(value) OVER (ORDER BY date) AS mom_delta,
            CASE
                WHEN LAG(value, 12) OVER (ORDER BY date) IS NOT NULL
                    AND LAG(value, 12) OVER (ORDER BY date) != 0
                THEN ((value - LAG(value, 12) OVER (ORDER BY date))
                      / ABS(LAG(value, 12) OVER (ORDER BY date))) * 100
                ELSE NULL
            END AS yoy_delta,
            AVG(value) OVER (
                ORDER BY date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
            ) AS rolling_12m_avg,
            CASE
                WHEN COUNT(value) OVER (
                    ORDER BY date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                ) >= 2
                THEN (value - AVG(value) OVER (
                    ORDER BY date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                )) / NULLIF(STDDEV(value) OVER (
                    ORDER BY date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                ), 0)
                ELSE NULL
            END AS z_score
        FROM joined
        ORDER BY date
    """
    result = conn.execute(sql)
    table = result.to_arrow_table()
    conn.close()
    return table


def compute_yield_spread(
    long_table: pa.Table, short_table: pa.Table,
) -> pa.Table:
    """Compute yield curve slope: long-term prefixado minus short-term prefixado.

    Positive = normal yield curve. Negative = inverted (recession signal).
    """
    conn = duckdb.connect()
    conn.register("long_rate", long_table)
    conn.register("short_rate", short_table)

    now_str = datetime.now(timezone.utc).isoformat()

    sql = f"""
        WITH joined AS (
            SELECT
                l.date,
                l.value AS long_val,
                s.value AS short_val,
                ROUND(l.value - s.value, 4) AS spread
            FROM long_rate l
            INNER JOIN short_rate s ON l.date = s.date
            WHERE l.value IS NOT NULL AND s.value IS NOT NULL
        )
        SELECT
            date,
            spread AS value,
            'derived_yield_spread' AS series,
            'p.p.' AS unit,
            '{now_str}' AS last_updated_at,
            '{GOLD_CALCULATION_VERSION}' AS calculation_version,
            value - LAG(value) OVER (ORDER BY date) AS mom_delta,
            CASE
                WHEN LAG(value, 12) OVER (ORDER BY date) IS NOT NULL
                    AND LAG(value, 12) OVER (ORDER BY date) != 0
                THEN ((value - LAG(value, 12) OVER (ORDER BY date))
                      / ABS(LAG(value, 12) OVER (ORDER BY date))) * 100
                ELSE NULL
            END AS yoy_delta,
            AVG(value) OVER (
                ORDER BY date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
            ) AS rolling_12m_avg,
            CASE
                WHEN COUNT(value) OVER (
                    ORDER BY date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                ) >= 2
                THEN (value - AVG(value) OVER (
                    ORDER BY date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                )) / NULLIF(STDDEV(value) OVER (
                    ORDER BY date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                ), 0)
                ELSE NULL
            END AS z_score
        FROM joined
        ORDER BY date
    """
    result = conn.execute(sql)
    table = result.to_arrow_table()
    conn.close()
    return table


class CrossSeriesTask(BaseTask):
    """Computes cross-series derived metrics as new gold-layer outputs.

    Reads multiple gold series via storage, computes derived metrics using
    DuckDB, and writes the results as gold parquet files.
    """

    def __init__(self, storage: StorageBackend) -> None:
        self._storage = storage

    @property
    def task_name(self) -> str:
        return "cross_series"

    async def _read_gold(self, series: str) -> pa.Table | None:
        """Read a gold parquet file for the given series."""
        key = f"gold/{series}.parquet"
        if not await self._storage.exists(key):
            return None
        data = await self._storage.read(key)
        return _read_gold_table(data)

    async def _write_gold(self, series: str, table: pa.Table) -> None:
        """Write a derived gold parquet file."""
        buf = io.BytesIO()
        pq.write_table(table, buf)
        key = f"gold/{series}.parquet"
        await self._storage.write(key, buf.getvalue())

    @staticmethod
    def _validate_typical_range(
        series_id: str, table: pa.Table,
    ) -> list[str]:
        """Validate that computed values fall within the configured typical_range.

        Returns a list of warning messages for out-of-range values.
        """
        cfg = get_domain_config()
        series_cfg = cfg.series.get(series_id)
        if not series_cfg or not series_cfg.typical_range:
            return []

        tr = series_cfg.typical_range
        values = [v for v in table.column("value").to_pylist() if v is not None]
        if not values:
            return []

        violations: list[str] = []
        below = [v for v in values if v < tr.min]
        above = [v for v in values if v > tr.max]

        if below:
            worst = min(below)
            violations.append(
                f"{series_id}: {len(below)} values below typical_range.min "
                f"({tr.min}), worst={worst:.4f}"
            )
        if above:
            worst = max(above)
            violations.append(
                f"{series_id}: {len(above)} values above typical_range.max "
                f"({tr.max}), worst={worst:.4f}"
            )

        for v in violations:
            logger.error("cross_series.typical_range_violation", detail=v)

        return violations

    async def _compute_derived_real_rate(self) -> tuple[int, SeriesReconciliation]:
        """Compute and write derived_real_rate gold series."""
        selic = await self._read_gold("bcb_selic")
        ipca = await self._read_gold("bcb_ipca")

        if selic is None or ipca is None:
            missing = []
            if selic is None:
                missing.append("bcb_selic")
            if ipca is None:
                missing.append("bcb_ipca")
            raise ValueError(f"Missing source series: {', '.join(missing)}")

        # Validate source IPCA data before computing
        ipca_violations = self._validate_typical_range("bcb_ipca", ipca)
        if ipca_violations:
            raise ValueError(
                f"Source data failed sanity check: {'; '.join(ipca_violations)}"
            )

        result = compute_real_rate(selic, ipca)

        # Validate derived output
        violations = self._validate_typical_range("derived_real_rate", result)
        if violations:
            raise ValueError(
                f"Derived values outside typical range: {'; '.join(violations)}"
            )

        await self._write_gold("derived_real_rate", result)

        rows = result.num_rows
        recon = SeriesReconciliation(
            series_id="derived_real_rate",
            rows_in=selic.num_rows + ipca.num_rows,
            rows_out=rows,
        )
        return rows, recon

    async def _compute_ipca_gdp_divergence(self) -> tuple[int, SeriesReconciliation]:
        """Compute and write derived_ipca_gdp_divergence gold series."""
        ipca = await self._read_gold("bcb_ipca")
        ibc = await self._read_gold("bcb_ibc_br")

        if ipca is None or ibc is None:
            missing = []
            if ipca is None:
                missing.append("bcb_ipca")
            if ibc is None:
                missing.append("bcb_ibc_br")
            raise ValueError(f"Missing source series: {', '.join(missing)}")

        result = compute_ipca_gdp_divergence(ipca, ibc)

        violations = self._validate_typical_range(
            "derived_ipca_gdp_divergence", result,
        )
        if violations:
            raise ValueError(
                f"Derived values outside typical range: {'; '.join(violations)}"
            )

        await self._write_gold("derived_ipca_gdp_divergence", result)

        rows = result.num_rows
        recon = SeriesReconciliation(
            series_id="derived_ipca_gdp_divergence",
            rows_in=ipca.num_rows + ibc.num_rows,
            rows_out=rows,
        )
        return rows, recon

    async def _compute_yield_spread(self) -> tuple[int, SeriesReconciliation]:
        """Compute and write derived_yield_spread gold series."""
        long_rate = await self._read_gold("tesouro_prefixado_longo")
        short_rate = await self._read_gold("tesouro_prefixado_curto")

        if long_rate is None or short_rate is None:
            missing = []
            if long_rate is None:
                missing.append("tesouro_prefixado_longo")
            if short_rate is None:
                missing.append("tesouro_prefixado_curto")
            raise ValueError(f"Missing source series: {', '.join(missing)}")

        result = compute_yield_spread(long_rate, short_rate)

        violations = self._validate_typical_range("derived_yield_spread", result)
        if violations:
            raise ValueError(
                f"Derived values outside typical range: {'; '.join(violations)}"
            )

        await self._write_gold("derived_yield_spread", result)

        rows = result.num_rows
        recon = SeriesReconciliation(
            series_id="derived_yield_spread",
            rows_in=long_rate.num_rows + short_rate.num_rows,
            rows_out=rows,
        )
        return rows, recon

    async def _execute(self) -> TaskResult:
        total_rows = 0
        warnings: list[str] = []
        errors: list[str] = []
        reconciliation: list[SeriesReconciliation] = []
        any_success = False

        computations = [
            ("derived_real_rate", self._compute_derived_real_rate),
            ("derived_ipca_gdp_divergence", self._compute_ipca_gdp_divergence),
            ("derived_yield_spread", self._compute_yield_spread),
        ]

        for name, compute_fn in computations:
            try:
                rows, recon = await compute_fn()
                total_rows += rows
                reconciliation.append(recon)
                any_success = True
                logger.info("cross_series_computed", series=name, rows=rows)
            except Exception as exc:
                msg = f"Cross-series {name} failed: {exc}"
                warnings.append(msg)
                logger.warning("cross_series_failed", series=name, error=str(exc))

        if not any_success:
            errors.append("No derived series computed successfully")

        return TaskResult(
            success=any_success,
            task_name=self.task_name,
            duration_ms=0.0,
            rows_processed=total_rows,
            warnings=warnings,
            errors=errors,
            series_reconciliation=reconciliation,
        )
