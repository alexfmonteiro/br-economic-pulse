"""TransformationTask — config-driven bronze→silver→gold with watermark, merge, quarantine."""

from __future__ import annotations

import io
from datetime import datetime, timezone

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from api.models import FeedConfig, SeriesReconciliation, SilverProcessingType, SilverWatermark, TaskResult
from storage.protocol import StorageBackend
from tasks.base import BaseTask

logger = structlog.get_logger()

GOLD_CALCULATION_VERSION = "1.0.0"


class TransformationTask(BaseTask):
    """Reads bronze Parquet, applies config-driven transforms, writes silver + gold."""

    def __init__(
        self,
        storage: StorageBackend,
        feed_configs: dict[str, FeedConfig] | None = None,
    ) -> None:
        self._storage = storage
        self._feed_configs = feed_configs or {}

    @property
    def task_name(self) -> str:
        return "transformation"

    async def _discover_series(self) -> list[str]:
        """Discover all series with bronze data or a bronze_source reference."""
        all_keys = await self._storage.list_keys("bronze")
        bronze_set: set[str] = set()
        for key in all_keys:
            parts = key.split("/")
            if len(parts) >= 2:
                bronze_set.add(parts[1])

        series_set = set(bronze_set)
        # Include derived feeds whose bronze_source has data
        for feed_id, feed in self._feed_configs.items():
            if feed.bronze_source and feed.bronze_source in bronze_set:
                series_set.add(feed_id)

        # Exclude ingestion-only parents (feeds that serve as bronze_source for others)
        bronze_source_parents = {
            feed.bronze_source for feed in self._feed_configs.values() if feed.bronze_source
        }
        series_set -= bronze_source_parents

        return sorted(series_set)

    async def _execute(self) -> TaskResult:
        total_rows = 0
        warnings: list[str] = []
        errors: list[str] = []
        reconciliation: list[SeriesReconciliation] = []
        any_success = False

        series_list = await self._discover_series()
        if not series_list:
            return TaskResult(
                success=False,
                task_name=self.task_name,
                duration_ms=0.0,
                errors=["No bronze data found to transform"],
            )

        for series in series_list:
            try:
                rows, recon = await self._transform_series(series)
                total_rows += rows
                reconciliation.append(recon)
                any_success = True
            except Exception as exc:
                msg = f"Transform {series} failed: {exc}"
                warnings.append(msg)
                logger.warning("transformation_failed", series=series, error=str(exc))

        if not any_success:
            errors.append("No series transformed successfully")

        return TaskResult(
            success=any_success,
            task_name=self.task_name,
            duration_ms=0.0,
            rows_processed=total_rows,
            warnings=warnings,
            errors=errors,
            series_reconciliation=reconciliation,
        )

    async def _read_watermark(self, series: str) -> SilverWatermark | None:
        """Read the watermark for a series, if it exists."""
        wm_key = f"silver/{series}/_watermark.json"
        if not await self._storage.exists(wm_key):
            return None
        try:
            data = await self._storage.read(wm_key)
            return SilverWatermark.model_validate_json(data)
        except Exception:
            return None

    async def _write_watermark(self, series: str, last_key: str) -> None:
        """Write watermark after successful processing."""
        wm = SilverWatermark(
            last_processed_key=last_key,
            last_processed_at=datetime.now(timezone.utc),
        )
        wm_key = f"silver/{series}/_watermark.json"
        await self._storage.write(wm_key, wm.model_dump_json().encode())

    async def _get_bronze_keys_since_watermark(
        self, series: str, watermark: SilverWatermark | None
    ) -> list[str]:
        """Get bronze keys to process (all since last watermark, or all)."""
        all_keys = sorted(await self._storage.list_keys(f"bronze/{series}"))
        if watermark is None:
            return all_keys
        return [k for k in all_keys if k > watermark.last_processed_key]

    async def _read_existing_silver(self, series: str) -> pa.Table | None:
        """Read existing canonical silver file if it exists."""
        silver_key = f"silver/{series}.parquet"
        if not await self._storage.exists(silver_key):
            return None
        try:
            data = await self._storage.read(silver_key)
            return pq.read_table(io.BytesIO(data))
        except Exception:
            return None

    def _build_silver_sql(self, feed: FeedConfig, has_ingested_at: bool = True) -> str:
        """Generate silver SQL from feed config field definitions."""
        select_parts: list[str] = []
        where_parts: list[str] = []
        silver_aliases: list[str] = []

        for field in feed.schema_fields:
            if field.silver_expression and field.silver_type:
                # Use the configured expression for silver casting
                expr = field.silver_expression.replace("{col}", field.name)
                # Determine the silver column name
                if field.silver_type == "DATE":
                    select_parts.append(f'{expr} AS date')
                    silver_aliases.append("date")
                elif field.silver_type == "DOUBLE":
                    select_parts.append(f'{expr} AS value')
                    silver_aliases.append("value")
                # Skip fields without silver mapping
            # Track required fields for WHERE clause (bronze source not null)
            if field.required and field.silver_expression:
                where_parts.append(f'"{field.name}" IS NOT NULL')

        if not select_parts:
            raise ValueError(f"No silver columns defined for {feed.feed_id}")

        # Apply pre_filter from feed config (e.g. filter specific bond type)
        if feed.processing.silver.pre_filter:
            where_parts.append(feed.processing.silver.pre_filter)

        where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

        # Include _ingested_at if the bronze data has it, else use a placeholder
        ingested_at_col = '"_ingested_at"' if has_ingested_at else '? AS _ingested_at'

        inner_sql = f"""
            SELECT DISTINCT
                {', '.join(select_parts)},
                ? AS series,
                ? AS unit,
                ? AS _cleaned_at,
                ? AS _transformation_version,
                {ingested_at_col}
            FROM bronze_batch
            {where_clause}
        """

        # Wrap in outer query to filter out rows where TRY_CAST produced NULLs
        null_filters = " AND ".join(f"{alias} IS NOT NULL" for alias in silver_aliases)
        base_sql = f"""
            SELECT * FROM ({inner_sql}) _silver
            WHERE {null_filters}
        """

        # Optionally aggregate (e.g. AVG across maturities per date)
        if feed.processing.silver.aggregation == "avg":
            return f"""
                SELECT
                    date,
                    AVG(value) AS value,
                    ANY_VALUE(series) AS series,
                    ANY_VALUE(unit) AS unit,
                    MAX(_cleaned_at) AS _cleaned_at,
                    ANY_VALUE(_transformation_version) AS _transformation_version,
                    MAX(_ingested_at) AS _ingested_at
                FROM ({base_sql}) _agg
                GROUP BY date
                ORDER BY date
            """

        return base_sql

    async def _transform_series(self, series: str) -> tuple[int, SeriesReconciliation]:
        """Transform a single series from bronze -> silver -> gold."""
        feed = self._feed_configs.get(series)
        if feed is None:
            raise ValueError(f"No feed config for series {series}")

        # Read watermark and find new bronze files
        # Derived feeds read bronze from their parent feed's directory
        bronze_feed = feed.bronze_source or series
        watermark = await self._read_watermark(series)
        new_keys = await self._get_bronze_keys_since_watermark(bronze_feed, watermark)

        if not new_keys:
            logger.info("transformation_no_new_data", series=series)
            # Still produce gold from existing silver
            existing_silver = await self._read_existing_silver(series)
            if existing_silver is not None and existing_silver.num_rows > 0:
                await self._write_gold(series, existing_silver, feed)
                return existing_silver.num_rows
            raise ValueError(f"No new bronze data and no existing silver for {series}")

        # Read and concatenate all new bronze files
        bronze_tables: list[pa.Table] = []
        for key in new_keys:
            data = await self._storage.read(key)
            bronze_tables.append(pq.read_table(io.BytesIO(data)))

        bronze_batch = pa.concat_tables(bronze_tables, promote_options="permissive")

        conn = duckdb.connect()
        conn.register("bronze_batch", bronze_batch)

        # Check if bronze data has _ingested_at (old seed data may not)
        has_ingested_at = "_ingested_at" in bronze_batch.column_names

        # Generate and execute silver SQL
        silver_sql = self._build_silver_sql(feed, has_ingested_at=has_ingested_at)
        now_str = datetime.now(timezone.utc).isoformat()

        params: list[str] = [series, feed.metadata.unit, now_str, feed.version]
        if not has_ingested_at:
            params.append(now_str)  # placeholder for _ingested_at

        silver_result = conn.execute(silver_sql, params)
        new_silver = silver_result.to_arrow_table()

        # Track quarantined rows (bronze rows that didn't survive the transform)
        total_bronze_rows = bronze_batch.num_rows
        valid_silver_rows = new_silver.num_rows
        quarantined_count = total_bronze_rows - valid_silver_rows

        if quarantined_count > 0:
            logger.info(
                "quarantine_rows_detected",
                series=series,
                bronze_rows=total_bronze_rows,
                silver_rows=valid_silver_rows,
                quarantined=quarantined_count,
            )

        conn.close()

        if new_silver.num_rows == 0:
            raise ValueError(f"No valid rows after silver transform for {series}")

        # Merge with existing silver based on processing type
        existing_silver = await self._read_existing_silver(series)
        merged_silver = self._merge_silver(
            new_silver, existing_silver, feed
        )

        # Write canonical silver file
        silver_buf = io.BytesIO()
        pq.write_table(merged_silver, silver_buf)
        silver_key = f"silver/{series}.parquet"
        await self._storage.write(silver_key, silver_buf.getvalue())

        # Update watermark
        await self._write_watermark(series, new_keys[-1])

        # Write gold
        await self._write_gold(series, merged_silver, feed)

        row_count = merged_silver.num_rows
        logger.info(
            "transformation_complete",
            series=series,
            rows=row_count,
            new_bronze_files=len(new_keys),
            silver_key=silver_key,
        )

        recon = SeriesReconciliation(
            series_id=series,
            rows_in=total_bronze_rows,
            rows_out=valid_silver_rows,
            rows_quarantined=quarantined_count,
        )
        return row_count, recon

    def _merge_silver(
        self,
        new_silver: pa.Table,
        existing_silver: pa.Table | None,
        feed: FeedConfig,
    ) -> pa.Table:
        """Merge new silver data with existing based on processing type."""
        if existing_silver is None or existing_silver.num_rows == 0:
            return self._dedup_silver(new_silver, feed)

        processing_type = feed.processing.silver.processing_type

        if processing_type == SilverProcessingType.APPEND:
            combined = pa.concat_tables([existing_silver, new_silver])
            return combined

        # For latest_only and merge_by_key, combine then dedup
        # Ensure schemas match before concat
        combined = pa.concat_tables(
            [existing_silver, new_silver],
            promote_options="permissive",
        )
        return self._dedup_silver(combined, feed)

    def _dedup_silver(self, table: pa.Table, feed: FeedConfig) -> pa.Table:
        """Deduplicate silver table based on feed config."""
        processing_type = feed.processing.silver.processing_type
        if processing_type == SilverProcessingType.APPEND:
            return table

        conn = duckdb.connect()
        conn.register("silver_combined", table)

        # Dedup uses silver column names (post-transform), not bronze names.
        # Silver SQL always normalizes to "date" and "value", so dedup on "date".
        available_cols = set(table.column_names)
        dedup_cols = [
            c for c in feed.processing.silver.dedup_columns if c in available_cols
        ]
        if not dedup_cols:
            dedup_cols = ["date"]

        order_by = feed.processing.silver.dedup_order_by or "_ingested_at DESC"
        dedup_col_str = ", ".join(f'"{c}"' for c in dedup_cols)

        dedup_sql = f"""
            SELECT * FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY {dedup_col_str}
                        ORDER BY {order_by}
                    ) AS _rn
                FROM silver_combined
            ) WHERE _rn = 1
            ORDER BY date
        """

        result = conn.execute(dedup_sql)
        deduped = result.to_arrow_table()
        conn.close()

        # Remove the _rn helper column
        if "_rn" in deduped.column_names:
            idx = deduped.column_names.index("_rn")
            deduped = deduped.remove_column(idx)

        return deduped

    async def _write_gold(
        self, series: str, silver_table: pa.Table, feed: FeedConfig
    ) -> None:
        """Compute derived metrics and write gold Parquet."""
        conn = duckdb.connect()
        conn.register("silver_data", silver_table)

        now_str = datetime.now(timezone.utc).isoformat()

        gold_sql = f"""
            SELECT
                date,
                value,
                series,
                '{feed.metadata.unit}' AS unit,
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
                    ORDER BY date
                    ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
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
            FROM silver_data
            ORDER BY date
        """
        gold_result = conn.execute(gold_sql)
        gold_table = gold_result.to_arrow_table()
        conn.close()

        gold_buf = io.BytesIO()
        pq.write_table(gold_table, gold_buf)
        gold_key = f"gold/{series}.parquet"
        await self._storage.write(gold_key, gold_buf.getvalue())

    async def health_check(self) -> bool:
        try:
            test_key = "_health/transformation_check"
            await self._storage.write(test_key, b"ok")
            await self._storage.delete(test_key)
            return True
        except Exception:
            return False
