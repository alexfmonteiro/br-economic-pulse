"""QualityTask — validates data at each pipeline stage using feed config thresholds."""

from __future__ import annotations

import io
import uuid
from datetime import datetime, timezone

import pyarrow.parquet as pq
import structlog

from api.models import (
    FeedConfig,
    PipelineStage,
    QualityCheckResult,
    QualityLevel,
    QualityReport,
    SeriesReconciliation,
    TaskResult,
)
from storage.protocol import StorageBackend
from tasks.base import BaseTask

logger = structlog.get_logger()

# Default thresholds (used when no feed config is provided)
DEFAULT_MAX_NULL_RATE = 0.02
DEFAULT_MIN_ROW_COUNT = 1


class QualityTask(BaseTask):
    """Runs quality checks after ingestion or transformation."""

    def __init__(
        self,
        storage: StorageBackend,
        stage: PipelineStage = PipelineStage.POST_INGESTION,
        feed_configs: dict[str, FeedConfig] | None = None,
    ) -> None:
        self._storage = storage
        self._stage = stage
        self._feed_configs = feed_configs or {}

    @property
    def task_name(self) -> str:
        return "quality"

    def _get_bronze_thresholds(self, series: str) -> tuple[float, int, float | None]:
        """Get bronze quality thresholds for a series."""
        feed = self._feed_configs.get(series)
        if feed:
            q = feed.quality.bronze
            return q.max_null_rate, q.min_row_count, q.max_rescued_data_rate
        return DEFAULT_MAX_NULL_RATE, DEFAULT_MIN_ROW_COUNT, None

    def _get_gold_thresholds(
        self, series: str
    ) -> tuple[float, int, float | None, float | None]:
        """Get gold quality thresholds for a series."""
        feed = self._feed_configs.get(series)
        if feed and feed.quality.gold:
            q = feed.quality.gold
            return q.max_null_rate, q.min_row_count, q.value_range_min, q.value_range_max
        return DEFAULT_MAX_NULL_RATE, DEFAULT_MIN_ROW_COUNT, None, None

    async def _execute(self) -> TaskResult:
        run_id = f"quality-{uuid.uuid4().hex[:8]}"
        checks: list[QualityCheckResult] = []

        if self._stage == PipelineStage.POST_INGESTION:
            checks = await self._check_bronze()
        else:
            checks = await self._check_gold()

        if not checks:
            return TaskResult(
                success=False,
                task_name=self.task_name,
                duration_ms=0.0,
                errors=["No data found to validate"],
            )

        failed_checks = [c for c in checks if not c.passed]
        critical_failures = [
            c.message for c in failed_checks if c.check_name.startswith("critical_")
        ]

        if critical_failures:
            overall = QualityLevel.CRITICAL
        elif failed_checks:
            overall = QualityLevel.WARNING
        else:
            overall = QualityLevel.PASSED

        report = QualityReport(
            run_id=run_id,
            stage=self._stage,
            timestamp=datetime.now(timezone.utc),
            overall_status=overall,
            checks=checks,
            critical_failures=critical_failures,
        )

        report_key = f"quality/{run_id}/report.json"
        await self._storage.write(report_key, report.model_dump_json().encode())

        success = overall != QualityLevel.CRITICAL

        logger.info(
            "quality_check_complete",
            run_id=run_id,
            stage=self._stage.value,
            overall_status=overall.value,
            total_checks=len(checks),
            failed_checks=len(failed_checks),
        )

        # Build per-series reconciliation from check names
        series_checks: dict[str, tuple[int, int]] = {}  # series -> (total, passed)
        for c in checks:
            # Try to find a known series_id in the check name
            series_id = "unknown"
            for feed_id in (self._feed_configs or {}):
                if feed_id in c.check_name:
                    series_id = feed_id
                    break
            total, passed = series_checks.get(series_id, (0, 0))
            series_checks[series_id] = (total + 1, passed + (1 if c.passed else 0))

        reconciliation = [
            SeriesReconciliation(
                series_id=sid,
                rows_in=total,
                rows_out=passed,
            )
            for sid, (total, passed) in series_checks.items()
        ]

        return TaskResult(
            success=success,
            task_name=self.task_name,
            duration_ms=0.0,
            rows_processed=len(checks),
            warnings=[
                c.message
                for c in failed_checks
                if not c.check_name.startswith("critical_")
            ],
            errors=critical_failures,
            series_reconciliation=reconciliation,
        )

    async def _check_bronze(self) -> list[QualityCheckResult]:
        """Check bronze-layer data quality."""
        checks: list[QualityCheckResult] = []

        all_bronze_keys = await self._storage.list_keys("bronze")
        series_set: set[str] = set()
        for key in all_bronze_keys:
            parts = key.split("/")
            if len(parts) >= 2:
                series_set.add(parts[1])

        for series in sorted(series_set):
            max_null_rate, min_row_count, max_rescued_rate = (
                self._get_bronze_thresholds(series)
            )

            keys = await self._storage.list_keys(f"bronze/{series}")
            if not keys:
                continue

            latest_key = sorted(keys)[-1]
            try:
                data = await self._storage.read(latest_key)
                table = pq.read_table(io.BytesIO(data))
            except Exception as exc:
                checks.append(
                    QualityCheckResult(
                        check_name=f"critical_read_{series}",
                        passed=False,
                        message=f"Cannot read {latest_key}: {exc}",
                    )
                )
                continue

            # Null rate check (skip metadata columns)
            metadata_cols = {"_ingested_at", "_source", "_run_id", "_schema_hash", "_rescued_data"}
            for col_name in table.column_names:
                if col_name in metadata_cols:
                    continue
                col = table.column(col_name)
                null_count = col.null_count
                total = len(col)
                null_rate = null_count / total if total > 0 else 0.0

                checks.append(
                    QualityCheckResult(
                        check_name=f"null_rate_{series}_{col_name}",
                        passed=null_rate <= max_null_rate,
                        metric_value=round(null_rate, 4),
                        threshold=max_null_rate,
                        message=(
                            f"{series}.{col_name} null rate: {null_rate:.2%}"
                            if null_rate > max_null_rate
                            else ""
                        ),
                    )
                )

            # Row count check
            checks.append(
                QualityCheckResult(
                    check_name=f"row_count_{series}",
                    passed=table.num_rows >= min_row_count,
                    metric_value=float(table.num_rows),
                    message=f"{series} has {table.num_rows} rows",
                )
            )

            # Rescued data rate check
            if max_rescued_rate is not None and "_rescued_data" in table.column_names:
                rescued_col = table.column("_rescued_data").to_pylist()
                non_null_count = sum(1 for v in rescued_col if v is not None)
                rescued_rate = non_null_count / len(rescued_col) if rescued_col else 0.0

                checks.append(
                    QualityCheckResult(
                        check_name=f"rescued_data_rate_{series}",
                        passed=rescued_rate <= max_rescued_rate,
                        metric_value=round(rescued_rate, 4),
                        threshold=max_rescued_rate,
                        message=(
                            f"{series} rescued data rate: {rescued_rate:.2%}"
                            if rescued_rate > max_rescued_rate
                            else ""
                        ),
                    )
                )

            # Duplicate check on data column if it exists
            if "data" in table.column_names:
                data_col = table.column("data").to_pylist()
                non_null = [v for v in data_col if v is not None]
                has_dupes = len(non_null) != len(set(non_null))
                checks.append(
                    QualityCheckResult(
                        check_name=f"no_duplicates_{series}",
                        passed=not has_dupes,
                        message=f"{series} has duplicate dates" if has_dupes else "",
                    )
                )

        return checks

    async def _check_gold(self) -> list[QualityCheckResult]:
        """Check gold-layer data quality."""
        checks: list[QualityCheckResult] = []

        gold_keys = await self._storage.list_keys("gold")
        if not gold_keys:
            return checks

        for key in gold_keys:
            if not key.endswith(".parquet"):
                continue

            try:
                data = await self._storage.read(key)
                table = pq.read_table(io.BytesIO(data))
            except Exception as exc:
                checks.append(
                    QualityCheckResult(
                        check_name=f"critical_read_gold_{key}",
                        passed=False,
                        message=f"Cannot read {key}: {exc}",
                    )
                )
                continue

            series_name = key.split("/")[-1].replace(".parquet", "")
            max_null_rate, min_row_count, val_min, val_max = (
                self._get_gold_thresholds(series_name)
            )

            # Required columns check
            required = {"date", "value", "series"}
            missing = required - set(table.column_names)
            checks.append(
                QualityCheckResult(
                    check_name=f"schema_{series_name}",
                    passed=len(missing) == 0,
                    message=(
                        f"{series_name} missing columns: {missing}" if missing else ""
                    ),
                )
            )

            # Null rate on value column
            if "value" in table.column_names:
                val_col = table.column("value")
                null_rate = (
                    val_col.null_count / len(val_col) if len(val_col) > 0 else 0.0
                )
                checks.append(
                    QualityCheckResult(
                        check_name=f"null_rate_{series_name}_value",
                        passed=null_rate <= max_null_rate,
                        metric_value=round(null_rate, 4),
                        threshold=max_null_rate,
                        message=(
                            f"{series_name}.value null rate: {null_rate:.2%}"
                            if null_rate > max_null_rate
                            else ""
                        ),
                    )
                )

            # Row count
            checks.append(
                QualityCheckResult(
                    check_name=f"row_count_{series_name}",
                    passed=table.num_rows >= min_row_count,
                    metric_value=float(table.num_rows),
                    message=f"{series_name} has {table.num_rows} rows",
                )
            )

            # Value range check
            if (
                "value" in table.column_names
                and (val_min is not None or val_max is not None)
            ):
                values = [
                    v for v in table.column("value").to_pylist() if v is not None
                ]
                if values:
                    out_of_range = 0
                    for v in values:
                        if val_min is not None and v < val_min:
                            out_of_range += 1
                        if val_max is not None and v > val_max:
                            out_of_range += 1
                    checks.append(
                        QualityCheckResult(
                            check_name=f"value_range_{series_name}",
                            passed=out_of_range == 0,
                            metric_value=float(out_of_range),
                            message=(
                                f"{series_name} has {out_of_range} values out of range"
                                f" [{val_min}, {val_max}]"
                                if out_of_range > 0
                                else ""
                            ),
                        )
                    )

        return checks

    async def health_check(self) -> bool:
        try:
            test_key = "_health/quality_check"
            await self._storage.write(test_key, b"ok")
            await self._storage.delete(test_key)
            return True
        except Exception:
            return False
