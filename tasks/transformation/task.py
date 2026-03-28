"""TransformationTask — config-driven bronze→silver→gold with watermark, merge, quarantine.

Uses DuckDB out-of-core processing via temp files to avoid OOM on large datasets.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from datetime import datetime, timezone

import duckdb
import structlog

from api.models import FeedConfig, SeriesReconciliation, SilverProcessingType, SilverWatermark, TaskResult
from storage.protocol import StorageBackend
from tasks.base import BaseTask

logger = structlog.get_logger()

GOLD_CALCULATION_VERSION = "1.0.0"


class TransformationTask(BaseTask):
    """Reads bronze Parquet, applies config-driven transforms, writes silver + gold.

    All heavy data processing uses DuckDB read_parquet() on temp files,
    keeping RAM usage constant regardless of dataset size.
    """

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

        # Only include bronze series that have a feed config
        series_set = bronze_set & set(self._feed_configs)
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

    async def _download_to_temp(
        self, key: str, temp_dir: str, filename: str
    ) -> str | None:
        """Download a storage object to a temp file. Returns path or None."""
        if not await self._storage.exists(key):
            return None
        try:
            data = await self._storage.read(key)
            path = os.path.join(temp_dir, filename)
            with open(path, "wb") as f:
                f.write(data)
            return path
        except Exception:
            return None

    async def _upload_from_temp(self, file_path: str, key: str) -> None:
        """Upload a temp file to storage."""
        with open(file_path, "rb") as f:
            await self._storage.write(key, f.read())

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

        # Optionally aggregate (e.g. AVG across maturities, SUM across subsystems)
        agg_func = feed.processing.silver.aggregation
        if agg_func in ("avg", "sum"):
            sql_agg = "AVG" if agg_func == "avg" else "SUM"
            return f"""
                SELECT
                    date,
                    {sql_agg}(value) AS value,
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

        temp_dir = tempfile.mkdtemp(prefix=f"veredas_{series}_")
        try:
            if not new_keys:
                return await self._rebuild_gold(series, feed, temp_dir)
            return await self._process_new_bronze(series, feed, new_keys, temp_dir)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    async def _rebuild_gold(
        self, series: str, feed: FeedConfig, temp_dir: str
    ) -> tuple[int, SeriesReconciliation]:
        """Rebuild gold from existing silver when no new bronze data."""
        logger.info("transformation_no_new_data", series=series)
        silver_path = await self._download_to_temp(
            f"silver/{series}.parquet", temp_dir, "existing_silver.parquet"
        )
        if silver_path is None:
            raise ValueError(f"No new bronze data and no existing silver for {series}")

        conn = duckdb.connect()
        try:
            result = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{silver_path}')"
            ).fetchone()
            assert result is not None
            row_count = result[0]
            if row_count == 0:
                raise ValueError(
                    f"No new bronze data and no existing silver for {series}"
                )

            gold_path = os.path.join(temp_dir, "gold.parquet")
            self._compute_gold(conn, silver_path, gold_path, feed)
            await self._upload_from_temp(gold_path, f"gold/{series}.parquet")
        finally:
            conn.close()

        return row_count, SeriesReconciliation(
            series_id=series, rows_in=0, rows_out=row_count, rows_quarantined=0,
        )

    async def _process_new_bronze(
        self,
        series: str,
        feed: FeedConfig,
        new_keys: list[str],
        temp_dir: str,
    ) -> tuple[int, SeriesReconciliation]:
        """Process new bronze files through silver and gold layers."""
        # Download bronze files to temp directory (one at a time, constant RAM)
        bronze_paths: list[str] = []
        for i, key in enumerate(new_keys):
            data = await self._storage.read(key)
            path = os.path.join(temp_dir, f"bronze_{i}.parquet")
            with open(path, "wb") as f:
                f.write(data)
            bronze_paths.append(path)

        conn = duckdb.connect()
        try:
            # Read bronze via DuckDB out-of-core streaming
            file_list = ", ".join(f"'{p}'" for p in bronze_paths)
            conn.execute(
                f"CREATE VIEW bronze_batch AS SELECT * FROM "
                f"read_parquet([{file_list}], union_by_name=true)"
            )

            # Check if bronze data has _ingested_at (old seed data may not)
            col_info = conn.execute(
                "DESCRIBE SELECT * FROM bronze_batch"
            ).fetchall()
            col_names = {row[0] for row in col_info}
            has_ingested_at = "_ingested_at" in col_names

            # Generate and execute silver SQL
            silver_sql = self._build_silver_sql(feed, has_ingested_at=has_ingested_at)
            now_str = datetime.now(timezone.utc).isoformat()

            params: list[str] = [series, feed.metadata.unit, now_str, feed.version]
            if not has_ingested_at:
                params.append(now_str)  # placeholder for _ingested_at

            result = conn.execute(
                "SELECT COUNT(*) FROM bronze_batch"
            ).fetchone()
            assert result is not None
            total_bronze_rows = result[0]

            # Execute silver transform — result is small after filtering
            new_silver = conn.execute(silver_sql, params).to_arrow_table()
            valid_silver_rows = new_silver.num_rows

            # Write new silver to temp file via DuckDB
            new_silver_path = os.path.join(temp_dir, "new_silver.parquet")
            conn.register("_new_silver", new_silver)
            conn.execute(
                f"COPY _new_silver TO '{new_silver_path}' (FORMAT PARQUET)"
            )
            conn.unregister("_new_silver")
            del new_silver  # Free PyArrow RAM

            quarantined_count = total_bronze_rows - valid_silver_rows

            # Track quarantined rows (bronze rows that didn't survive the transform)
            if quarantined_count > 0:
                logger.info(
                    "quarantine_rows_detected",
                    series=series,
                    bronze_rows=total_bronze_rows,
                    silver_rows=valid_silver_rows,
                    quarantined=quarantined_count,
                )

            if valid_silver_rows == 0:
                raise ValueError(
                    f"No valid rows after silver transform for {series}"
                )

            # Download existing silver to temp file (if exists)
            existing_silver_path = await self._download_to_temp(
                f"silver/{series}.parquet", temp_dir, "existing_silver.parquet"
            )

            # Merge using DuckDB out-of-core processing
            merged_path = os.path.join(temp_dir, "merged_silver.parquet")
            self._merge_silver_files(
                conn, new_silver_path, existing_silver_path, merged_path, feed
            )

            # Upload merged silver
            silver_key = f"silver/{series}.parquet"
            await self._upload_from_temp(merged_path, silver_key)

            # Update watermark
            await self._write_watermark(series, new_keys[-1])

            # Compute and upload gold
            gold_path = os.path.join(temp_dir, "gold.parquet")
            self._compute_gold(conn, merged_path, gold_path, feed)
            await self._upload_from_temp(gold_path, f"gold/{series}.parquet")

            result = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{merged_path}')"
            ).fetchone()
            assert result is not None
            row_count = result[0]
        finally:
            conn.close()

        logger.info(
            "transformation_complete",
            series=series,
            rows=row_count,
            new_bronze_files=len(new_keys),
            silver_key=silver_key,
        )

        return row_count, SeriesReconciliation(
            series_id=series,
            rows_in=total_bronze_rows,
            rows_out=valid_silver_rows,
            rows_quarantined=quarantined_count,
        )

    def _merge_silver_files(
        self,
        conn: duckdb.DuckDBPyConnection,
        new_silver_path: str,
        existing_silver_path: str | None,
        output_path: str,
        feed: FeedConfig,
    ) -> None:
        """Merge new and existing silver using DuckDB out-of-core processing."""
        if existing_silver_path is None:
            self._dedup_silver_file(conn, new_silver_path, output_path, feed)
            return

        processing_type = feed.processing.silver.processing_type

        if processing_type == SilverProcessingType.APPEND:
            conn.execute(f"""
                COPY (
                    SELECT * FROM read_parquet(
                        ['{existing_silver_path}', '{new_silver_path}'],
                        union_by_name=true
                    )
                ) TO '{output_path}' (FORMAT PARQUET)
            """)
            return

        # For latest_only and merge_by_key: combine then dedup
        conn.execute(f"""
            CREATE OR REPLACE VIEW silver_combined AS
            SELECT * FROM read_parquet(
                ['{existing_silver_path}', '{new_silver_path}'],
                union_by_name=true
            )
        """)
        self._dedup_from_view(conn, output_path, feed)

    def _dedup_silver_file(
        self,
        conn: duckdb.DuckDBPyConnection,
        input_path: str,
        output_path: str,
        feed: FeedConfig,
    ) -> None:
        """Deduplicate a single silver Parquet file."""
        processing_type = feed.processing.silver.processing_type
        if processing_type == SilverProcessingType.APPEND:
            conn.execute(
                f"COPY (SELECT * FROM read_parquet('{input_path}'))"
                f" TO '{output_path}' (FORMAT PARQUET)"
            )
            return

        conn.execute(
            f"CREATE OR REPLACE VIEW silver_combined AS"
            f" SELECT * FROM read_parquet('{input_path}')"
        )
        self._dedup_from_view(conn, output_path, feed)

    def _dedup_from_view(
        self,
        conn: duckdb.DuckDBPyConnection,
        output_path: str,
        feed: FeedConfig,
    ) -> None:
        """Deduplicate silver_combined view and write to output file."""
        col_info = conn.execute(
            "DESCRIBE SELECT * FROM silver_combined"
        ).fetchall()
        available_cols = {row[0] for row in col_info}

        # Dedup uses silver column names (post-transform), not bronze names.
        # Silver SQL always normalizes to "date" and "value", so dedup on "date".
        dedup_cols = [
            c for c in feed.processing.silver.dedup_columns if c in available_cols
        ]
        if not dedup_cols:
            dedup_cols = ["date"]

        order_by = feed.processing.silver.dedup_order_by or "_ingested_at DESC"
        dedup_col_str = ", ".join(f'"{c}"' for c in dedup_cols)

        conn.execute(f"""
            COPY (
                SELECT * EXCLUDE (_rn) FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY {dedup_col_str}
                            ORDER BY {order_by}
                        ) AS _rn
                    FROM silver_combined
                ) WHERE _rn = 1
                ORDER BY date
            ) TO '{output_path}' (FORMAT PARQUET)
        """)

    def _compute_gold(
        self,
        conn: duckdb.DuckDBPyConnection,
        silver_path: str,
        output_path: str,
        feed: FeedConfig,
    ) -> None:
        """Compute derived metrics and write gold Parquet from silver file."""
        now_str = datetime.now(timezone.utc).isoformat()

        conn.execute(f"""
            COPY (
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
                FROM read_parquet('{silver_path}')
                ORDER BY date
            ) TO '{output_path}' (FORMAT PARQUET)
        """)

    async def health_check(self) -> bool:
        try:
            test_key = "_health/transformation_check"
            await self._storage.write(test_key, b"ok")
            await self._storage.delete(test_key)
            return True
        except Exception:
            return False
