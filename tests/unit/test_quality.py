"""Tests for QualityTask."""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from api.models import FeedConfig, GoldSummary, PipelineStage
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
    # WARNING check (all data) + CRITICAL check (recent data, from critical_checks config)
    assert len(range_checks) == 2
    assert not range_checks[0]["passed"]  # WARNING: all-data check
    assert not range_checks[1]["passed"]  # CRITICAL: recent-data check


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


# ---------------------------------------------------------------------------
# Layer 1: critical_checks tests
# ---------------------------------------------------------------------------

def _make_gold_columns(
    dates: list[str], values: list[float]
) -> dict[str, object]:
    """Build a minimal gold table dict."""
    n = len(dates)
    return {
        "date": pa.array([dt.date.fromisoformat(d) for d in dates], type=pa.date32()),
        "value": pa.array(values, type=pa.float64()),
        "series": ["test_series"] * n,
        "unit": ["%"] * n,
        "last_updated_at": [dt.datetime.now(dt.timezone.utc).isoformat()] * n,
        "calculation_version": ["1.0.0"] * n,
        "mom_delta": [None] * n,
        "yoy_delta": [None] * n,
        "rolling_12m_avg": [None] * n,
        "z_score": [None] * n,
    }


@pytest.mark.asyncio()
async def test_critical_value_range_halts_pipeline(
    tmp_path: Path,
    feed_configs: dict[str, FeedConfig],
) -> None:
    """Recent out-of-range values with critical_checks should halt the pipeline."""
    # bcb_selic has value_range [0, 50] and critical_checks: [value_range, typical_range]
    _write_parquet(tmp_path, "gold/bcb_selic.parquet", _make_gold_columns(
        dates=["2026-01-01", "2026-02-01", "2026-03-01"],
        values=[14.75, 14.75, 999.0],  # 999 is way out of range
    ))
    storage = LocalStorageBackend(tmp_path)
    task = QualityTask(
        storage=storage,
        stage=PipelineStage.POST_TRANSFORMATION,
        feed_configs={"bcb_selic": feed_configs["bcb_selic"]},
    )
    result = await task.run()
    assert result.success is False
    assert any("outside value_range" in e for e in result.errors)


@pytest.mark.asyncio()
async def test_critical_value_range_allows_historical(
    tmp_path: Path,
    feed_configs: dict[str, FeedConfig],
) -> None:
    """Out-of-range values older than 24 months should NOT trigger critical check."""
    _write_parquet(tmp_path, "gold/bcb_selic.parquet", _make_gold_columns(
        dates=["2020-01-01", "2020-02-01"],  # >24 months ago
        values=[999.0, 888.0],  # Out of range but old
    ))
    storage = LocalStorageBackend(tmp_path)
    task = QualityTask(
        storage=storage,
        stage=PipelineStage.POST_TRANSFORMATION,
        feed_configs={"bcb_selic": feed_configs["bcb_selic"]},
    )
    result = await task.run()
    # Pipeline should succeed — critical check only applies to recent data
    assert result.success is True


@pytest.mark.asyncio()
async def test_no_critical_checks_is_warning_only(
    tmp_path: Path,
) -> None:
    """Without critical_checks configured, out-of-range is WARNING only."""
    from api.models import (
        FeedMetadataConfig,
        FeedProcessingConfig,
        FeedQualityConfig,
        FeedSourceConfig,
        FeedStatus,
        QualityRuleConfig,
        SourceFormat,
    )

    feed = FeedConfig(
        feed_id="test_feed",
        name="Test Feed",
        version="1.0.0",
        status=FeedStatus.ACTIVE,
        source=FeedSourceConfig(type="api", url="http://example.com", format=SourceFormat.JSON),
        processing=FeedProcessingConfig(),
        quality=FeedQualityConfig(
            gold=QualityRuleConfig(
                value_range_min=0.0,
                value_range_max=100.0,
                # No critical_checks configured
            )
        ),
        metadata=FeedMetadataConfig(unit="%"),
    )

    _write_parquet(tmp_path, "gold/test_feed.parquet", _make_gold_columns(
        dates=["2026-01-01"],
        values=[999.0],  # Out of range
    ))
    storage = LocalStorageBackend(tmp_path)
    task = QualityTask(
        storage=storage,
        stage=PipelineStage.POST_TRANSFORMATION,
        feed_configs={"test_feed": feed},
    )
    result = await task.run()
    # Should succeed (WARNING only, not CRITICAL)
    assert result.success is True
    assert len(result.warnings) > 0


# ---------------------------------------------------------------------------
# Layer 2: cross-run comparison tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio()
async def test_cross_run_catches_3x_value_jump(
    tmp_path: Path,
    feed_configs: dict[str, FeedConfig],
) -> None:
    """A 3x jump in max value should trigger critical_max_value_jump."""
    # Write previous summary with normal values
    prev_summary = GoldSummary(
        series_id="bcb_selic",
        row_count=2,  # Match current gold row count
        value_min=0.5,
        value_max=14.75,
        value_mean=10.0,
        latest_date="2026-01-01",
        computed_at=dt.datetime.now(dt.timezone.utc),
    )
    summary_path = tmp_path / "gold" / "bcb_selic.summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(prev_summary.model_dump_json())

    # Write gold with 3x inflated values
    _write_parquet(tmp_path, "gold/bcb_selic.parquet", _make_gold_columns(
        dates=["2026-01-01", "2026-02-01"],
        values=[10.0, 44.25],  # max=44.25, prev_max=14.75 -> 200% change
    ))
    storage = LocalStorageBackend(tmp_path)
    task = QualityTask(
        storage=storage,
        stage=PipelineStage.POST_TRANSFORMATION,
        feed_configs={"bcb_selic": feed_configs["bcb_selic"]},
    )
    result = await task.run()
    assert result.success is False
    assert any("max value changed" in e for e in result.errors)


@pytest.mark.asyncio()
async def test_cross_run_no_previous_summary(
    tmp_path: Path,
    feed_configs: dict[str, FeedConfig],
) -> None:
    """First run (no previous summary) should pass without errors."""
    _write_parquet(tmp_path, "gold/bcb_selic.parquet", _make_gold_columns(
        dates=["2026-01-01"],
        values=[14.75],
    ))
    storage = LocalStorageBackend(tmp_path)
    task = QualityTask(
        storage=storage,
        stage=PipelineStage.POST_TRANSFORMATION,
        feed_configs={"bcb_selic": feed_configs["bcb_selic"]},
    )
    result = await task.run()
    assert result.success is True


@pytest.mark.asyncio()
async def test_cross_run_normal_change_passes(
    tmp_path: Path,
    feed_configs: dict[str, FeedConfig],
) -> None:
    """A <50% change in max value should pass."""
    prev_summary = GoldSummary(
        series_id="bcb_selic",
        row_count=2,  # Match current gold row count to avoid row_count_drop
        value_min=0.5,
        value_max=14.75,
        value_mean=10.0,
        latest_date="2026-01-01",
        computed_at=dt.datetime.now(dt.timezone.utc),
    )
    summary_path = tmp_path / "gold" / "bcb_selic.summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(prev_summary.model_dump_json())

    _write_parquet(tmp_path, "gold/bcb_selic.parquet", _make_gold_columns(
        dates=["2026-01-01", "2026-02-01"],
        values=[10.0, 15.0],  # max=15.0, prev_max=14.75 -> ~2% change
    ))
    storage = LocalStorageBackend(tmp_path)
    task = QualityTask(
        storage=storage,
        stage=PipelineStage.POST_TRANSFORMATION,
        feed_configs={"bcb_selic": feed_configs["bcb_selic"]},
    )
    result = await task.run()
    assert result.success is True


@pytest.mark.asyncio()
async def test_cross_run_row_count_drop(
    tmp_path: Path,
    feed_configs: dict[str, FeedConfig],
) -> None:
    """Row count dropping >20% should trigger critical_row_count_drop."""
    prev_summary = GoldSummary(
        series_id="bcb_selic",
        row_count=1000,
        value_min=0.5,
        value_max=14.75,
        value_mean=10.0,
        latest_date="2026-01-01",
        computed_at=dt.datetime.now(dt.timezone.utc),
    )
    summary_path = tmp_path / "gold" / "bcb_selic.summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(prev_summary.model_dump_json())

    # Only 2 rows vs previous 1000 -> 99.8% drop
    _write_parquet(tmp_path, "gold/bcb_selic.parquet", _make_gold_columns(
        dates=["2026-01-01", "2026-02-01"],
        values=[10.0, 14.75],
    ))
    storage = LocalStorageBackend(tmp_path)
    task = QualityTask(
        storage=storage,
        stage=PipelineStage.POST_TRANSFORMATION,
        feed_configs={"bcb_selic": feed_configs["bcb_selic"]},
    )
    result = await task.run()
    assert result.success is False
    assert any("row count dropped" in e for e in result.errors)
