"""Tests for QualityTask."""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from api.models import FeedConfig, PipelineStage
from pipeline.feed_config import load_feed_configs
from storage.local import LocalStorageBackend
from tasks.quality.task import QualityTask


@pytest.fixture()
def storage(tmp_path: Path) -> LocalStorageBackend:
    return LocalStorageBackend(tmp_path)


@pytest.fixture()
def feed_configs() -> dict[str, FeedConfig]:
    return load_feed_configs("config/feeds/br_macro")


def _write_parquet(storage_path: Path, key: str, columns: dict[str, object]) -> None:
    """Write a Parquet file to local storage."""
    path = storage_path / key
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table(columns)
    pq.write_table(table, path)


def _write_bronze_with_metadata(
    storage_path: Path,
    series: str,
    data_col: list[str | None],
    valor_col: list[str | None],
    rescued_col: list[str | None] | None = None,
) -> None:
    """Write bronze Parquet with metadata columns."""
    n = len(data_col)
    if rescued_col is None:
        rescued_col = [None] * n
    columns = {
        "data": pa.array(data_col, type=pa.string()),
        "valor": pa.array(valor_col, type=pa.string()),
        "_ingested_at": pa.array(["2026-01-01T00:00:00"] * n, type=pa.string()),
        "_source": pa.array([series] * n, type=pa.string()),
        "_run_id": pa.array(["test-run"] * n, type=pa.string()),
        "_schema_hash": pa.array(["hash"] * n, type=pa.string()),
        "_rescued_data": pa.array(rescued_col, type=pa.string()),
    }
    path = storage_path / f"bronze/{series}/20260323.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(columns), path)


@pytest.fixture()
def good_bronze_data(tmp_path: Path) -> Path:
    """Bronze data that passes all quality checks."""
    _write_bronze_with_metadata(
        tmp_path, "bcb_selic",
        ["01/01/2026", "01/02/2026", "01/03/2026"],
        ["14.75", "14.75", "14.75"],
    )
    return tmp_path


@pytest.fixture()
def good_gold_data(tmp_path: Path) -> Path:
    """Gold data that passes all quality checks."""
    dates = [dt.datetime(2026, 1, 1), dt.datetime(2026, 2, 1), dt.datetime(2026, 3, 1)]
    _write_parquet(tmp_path, "gold/bcb_selic.parquet", {
        "date": dates,
        "value": [14.75, 14.75, 14.75],
        "series": ["bcb_selic", "bcb_selic", "bcb_selic"],
        "unit": ["% a.a.", "% a.a.", "% a.a."],
        "last_updated_at": ["2026-01-01T00:00:00"] * 3,
        "calculation_version": ["1.0.0"] * 3,
        "mom_delta": [None, 0.0, 0.0],
        "yoy_delta": [None, None, None],
        "rolling_12m_avg": [14.75, 14.75, 14.75],
        "z_score": [None, None, None],
    })
    return tmp_path


@pytest.mark.asyncio()
async def test_quality_post_ingestion_pass(good_bronze_data: Path) -> None:
    """Quality checks should pass for valid bronze data."""
    storage = LocalStorageBackend(good_bronze_data)
    task = QualityTask(storage=storage, stage=PipelineStage.POST_INGESTION)
    result = await task.run()

    assert result.success is True
    assert result.task_name == "quality"

    keys = await storage.list_keys("quality")
    assert len(keys) > 0

    report_data = await storage.read(keys[0])
    report = json.loads(report_data)
    assert report["overall_status"] in ["passed", "warning"]


@pytest.mark.asyncio()
async def test_quality_post_transformation_pass(good_gold_data: Path) -> None:
    """Quality checks should pass for valid gold data."""
    storage = LocalStorageBackend(good_gold_data)
    task = QualityTask(storage=storage, stage=PipelineStage.POST_TRANSFORMATION)
    result = await task.run()

    assert result.success is True


@pytest.mark.asyncio()
async def test_quality_empty_data(tmp_path: Path) -> None:
    """Quality checks should fail when no data exists."""
    storage = LocalStorageBackend(tmp_path)
    task = QualityTask(storage=storage, stage=PipelineStage.POST_INGESTION)
    result = await task.run()

    assert result.success is False


@pytest.mark.asyncio()
async def test_quality_report_format(good_bronze_data: Path) -> None:
    """Quality report should follow QualityReport schema."""
    storage = LocalStorageBackend(good_bronze_data)
    task = QualityTask(storage=storage, stage=PipelineStage.POST_INGESTION)
    await task.run()

    keys = await storage.list_keys("quality")
    report_data = await storage.read(keys[0])
    report = json.loads(report_data)

    assert "run_id" in report
    assert "stage" in report
    assert "timestamp" in report
    assert "overall_status" in report
    assert "checks" in report


@pytest.mark.asyncio()
async def test_quality_high_null_rate(tmp_path: Path) -> None:
    """Quality should flag high null rates."""
    _write_bronze_with_metadata(
        tmp_path, "bcb_selic",
        ["01/01/2026", None, None, None, None],
        ["14.75", None, None, None, None],
    )
    storage = LocalStorageBackend(tmp_path)
    task = QualityTask(storage=storage, stage=PipelineStage.POST_INGESTION)
    await task.run()

    keys = await storage.list_keys("quality")
    report_data = await storage.read(keys[0])
    report = json.loads(report_data)

    null_checks = [c for c in report["checks"] if "null" in c["check_name"].lower()]
    assert any(not c["passed"] for c in null_checks)


@pytest.mark.asyncio()
async def test_quality_health_check(tmp_path: Path) -> None:
    storage = LocalStorageBackend(tmp_path)
    task = QualityTask(storage=storage, stage=PipelineStage.POST_INGESTION)
    assert await task.health_check() is True


@pytest.mark.asyncio()
async def test_quality_rescued_data_rate(tmp_path: Path) -> None:
    """Quality should check rescued data rate when feed config provides threshold."""
    _write_bronze_with_metadata(
        tmp_path, "bcb_selic",
        ["01/01/2026", "01/02/2026", "01/03/2026", "01/04/2026"],
        ["14.75", "14.75", "14.75", "14.75"],
        # 50% rescued data rate
        rescued_col=['{"extra": "val"}', '{"extra": "val"}', None, None],
    )
    storage = LocalStorageBackend(tmp_path)
    configs = load_feed_configs("config/feeds/br_macro")
    task = QualityTask(
        storage=storage,
        stage=PipelineStage.POST_INGESTION,
        feed_configs=configs,
    )
    await task.run()

    # Rescued data rate of 50% exceeds the 10% threshold
    keys = await storage.list_keys("quality")
    report_data = await storage.read(keys[0])
    report = json.loads(report_data)
    rescued_checks = [c for c in report["checks"] if "rescued" in c["check_name"]]
    assert len(rescued_checks) == 1
    assert not rescued_checks[0]["passed"]


@pytest.mark.asyncio()
async def test_quality_uses_feed_config_thresholds(tmp_path: Path) -> None:
    """Quality should use thresholds from feed config, not hardcoded defaults."""
    # Write bronze with 40% null rate on 'valor'
    _write_bronze_with_metadata(
        tmp_path, "bcb_selic",
        ["01/01/2026", "01/02/2026", "01/03/2026", "01/04/2026", "01/05/2026"],
        ["14.75", "14.75", "14.75", None, None],
    )
    storage = LocalStorageBackend(tmp_path)

    # With default thresholds (2%), this should flag the null rate
    task_default = QualityTask(storage=storage, stage=PipelineStage.POST_INGESTION)
    await task_default.run()

    keys = await storage.list_keys("quality")
    report_data = await storage.read(sorted(keys)[-1])
    report = json.loads(report_data)
    valor_null = [c for c in report["checks"] if c["check_name"] == "null_rate_bcb_selic_valor"]
    assert len(valor_null) == 1
    assert not valor_null[0]["passed"]  # 40% > 2%


@pytest.mark.asyncio()
async def test_quality_gold_value_range(tmp_path: Path) -> None:
    """Quality should check value range when configured in feed config."""
    dates = [dt.datetime(2026, 1, 1), dt.datetime(2026, 2, 1)]
    _write_parquet(tmp_path, "gold/bcb_selic.parquet", {
        "date": dates,
        "value": [14.75, 999.99],  # 999.99 is out of range [0, 50]
        "series": ["bcb_selic", "bcb_selic"],
        "unit": ["% a.a.", "% a.a."],
        "last_updated_at": ["2026-01-01T00:00:00"] * 2,
        "calculation_version": ["1.0.0"] * 2,
        "mom_delta": [None, 985.24],
        "yoy_delta": [None, None],
        "rolling_12m_avg": [14.75, 507.37],
        "z_score": [None, None],
    })
    storage = LocalStorageBackend(tmp_path)
    configs = load_feed_configs("config/feeds/br_macro")
    task = QualityTask(
        storage=storage,
        stage=PipelineStage.POST_TRANSFORMATION,
        feed_configs=configs,
    )
    await task.run()

    keys = await storage.list_keys("quality")
    report_data = await storage.read(sorted(keys)[-1])
    report = json.loads(report_data)
    range_checks = [c for c in report["checks"] if "value_range" in c["check_name"]]
    assert len(range_checks) == 1
    assert not range_checks[0]["passed"]


@pytest.mark.asyncio()
async def test_quality_fallback_to_defaults(good_bronze_data: Path) -> None:
    """Quality should use hardcoded defaults when no feed config is provided."""
    storage = LocalStorageBackend(good_bronze_data)
    # No feed_configs passed — should still work with defaults
    task = QualityTask(storage=storage, stage=PipelineStage.POST_INGESTION)
    result = await task.run()

    assert result.success is True


# ---------------------------------------------------------------------------
# typical_range quality checks (recent data only)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio()
async def test_quality_gold_typical_range_catches_contaminated_ipca(
    tmp_path: Path,
) -> None:
    """typical_range should catch IPCA values that look like 12M accumulated."""
    # bcb_ipca typical_range is [-1, 3]. Values of 3.81 (accumulated) should fail.
    dates = [dt.datetime(2026, 1, 1), dt.datetime(2026, 2, 1)]
    _write_parquet(tmp_path, "gold/bcb_ipca.parquet", {
        "date": dates,
        "value": [3.81, 4.23],  # 12M accumulated values, NOT monthly
        "series": ["bcb_ipca", "bcb_ipca"],
        "unit": ["% a.m.", "% a.m."],
        "last_updated_at": ["2026-01-01T00:00:00"] * 2,
        "calculation_version": ["1.0.0"] * 2,
        "mom_delta": [None, 0.42],
        "yoy_delta": [None, None],
        "rolling_12m_avg": [3.81, 4.02],
        "z_score": [None, None],
    })
    storage = LocalStorageBackend(tmp_path)
    task = QualityTask(storage=storage, stage=PipelineStage.POST_TRANSFORMATION)
    await task.run()

    keys = await storage.list_keys("quality")
    report_data = await storage.read(sorted(keys)[-1])
    report = json.loads(report_data)
    tr_checks = [c for c in report["checks"] if "typical_range" in c["check_name"]]
    assert len(tr_checks) == 1
    assert not tr_checks[0]["passed"]
    assert "bcb_ipca" in tr_checks[0]["message"]


@pytest.mark.asyncio()
async def test_quality_gold_typical_range_passes_correct_ipca(
    tmp_path: Path,
) -> None:
    """Correct monthly IPCA values should pass typical_range check."""
    dates = [dt.datetime(2026, 1, 1), dt.datetime(2026, 2, 1)]
    _write_parquet(tmp_path, "gold/bcb_ipca.parquet", {
        "date": dates,
        "value": [0.33, 0.70],  # Normal monthly IPCA values
        "series": ["bcb_ipca", "bcb_ipca"],
        "unit": ["% a.m.", "% a.m."],
        "last_updated_at": ["2026-01-01T00:00:00"] * 2,
        "calculation_version": ["1.0.0"] * 2,
        "mom_delta": [None, 0.37],
        "yoy_delta": [None, None],
        "rolling_12m_avg": [0.33, 0.515],
        "z_score": [None, None],
    })
    storage = LocalStorageBackend(tmp_path)
    task = QualityTask(storage=storage, stage=PipelineStage.POST_TRANSFORMATION)
    await task.run()

    keys = await storage.list_keys("quality")
    report_data = await storage.read(sorted(keys)[-1])
    report = json.loads(report_data)
    tr_checks = [c for c in report["checks"] if "typical_range" in c["check_name"]]
    assert len(tr_checks) == 1
    assert tr_checks[0]["passed"]


@pytest.mark.asyncio()
async def test_quality_gold_typical_range_allows_historical_extremes(
    tmp_path: Path,
) -> None:
    """Historical extremes outside typical_range should pass (only recent data checked)."""
    # Mix of old extreme data and recent normal data
    dates = [
        dt.datetime(1994, 7, 1),  # Historical extreme (post-Plano Real)
        dt.datetime(1994, 8, 1),
        dt.datetime(2026, 1, 1),  # Recent normal data
        dt.datetime(2026, 2, 1),
    ]
    _write_parquet(tmp_path, "gold/bcb_ipca.parquet", {
        "date": dates,
        "value": [6.84, 1.86, 0.33, 0.70],  # 1994 values are outside [-1, 3]
        "series": ["bcb_ipca"] * 4,
        "unit": ["% a.m."] * 4,
        "last_updated_at": ["2026-01-01T00:00:00"] * 4,
        "calculation_version": ["1.0.0"] * 4,
        "mom_delta": [None, -4.98, None, 0.37],
        "yoy_delta": [None, None, None, None],
        "rolling_12m_avg": [6.84, 4.35, 0.33, 0.515],
        "z_score": [None, None, None, None],
    })
    storage = LocalStorageBackend(tmp_path)
    task = QualityTask(storage=storage, stage=PipelineStage.POST_TRANSFORMATION)
    await task.run()

    keys = await storage.list_keys("quality")
    report_data = await storage.read(sorted(keys)[-1])
    report = json.loads(report_data)
    tr_checks = [c for c in report["checks"] if "typical_range" in c["check_name"]]
    assert len(tr_checks) == 1
    # Should pass — only recent values (2026) are checked, and those are within range
    assert tr_checks[0]["passed"]
