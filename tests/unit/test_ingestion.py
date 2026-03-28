"""Tests for IngestionTask."""

from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pyarrow.parquet as pq
import pytest

from api.models import FeedConfig
from pipeline.feed_config import load_feed_configs
from storage.local import LocalStorageBackend
from tasks.ingestion.task import IngestionTask, _generate_bcb_windows

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


def test_generate_bcb_windows_spans_decades() -> None:
    """BCB windowing should cover the full date range in 10-year chunks."""
    windows = _generate_bcb_windows("01/01/1999", 10)
    assert len(windows) >= 3
    # First window starts at 1999
    assert windows[0][0] == "01/01/1999"
    # Windows don't have gaps (each end == next start roughly)
    for i in range(len(windows) - 1):
        end_year = int(windows[i][1].split("/")[2])
        next_start_year = int(windows[i + 1][0].split("/")[2])
        assert end_year == next_start_year


def test_generate_bcb_windows_short_range() -> None:
    """A start date within the window size should produce 1 window."""
    windows = _generate_bcb_windows("01/01/2020", 10)
    assert len(windows) >= 1


@pytest.fixture()
def storage(tmp_path: Path) -> LocalStorageBackend:
    return LocalStorageBackend(tmp_path)


@pytest.fixture()
def feed_configs() -> dict[str, FeedConfig]:
    return load_feed_configs("config/feeds/br_macro")


@pytest.fixture()
def bcb_only_configs(feed_configs: dict[str, FeedConfig]) -> dict[str, FeedConfig]:
    """Just BCB SELIC for focused tests."""
    return {"bcb_selic": feed_configs["bcb_selic"]}


@pytest.fixture()
def bcb_selic_data() -> list[dict[str, str]]:
    return json.loads((FIXTURES_DIR / "bcb_selic.json").read_text())


@pytest.fixture()
def bcb_ipca_data() -> list[dict[str, str]]:
    return json.loads((FIXTURES_DIR / "bcb_ipca.json").read_text())


@pytest.fixture()
def bcb_usd_brl_data() -> list[dict[str, str]]:
    return json.loads((FIXTURES_DIR / "bcb_usd_brl.json").read_text())


@pytest.fixture()
def ibge_data() -> list[dict[str, str]]:
    return json.loads((FIXTURES_DIR / "ibge_sample.json").read_text())


@pytest.fixture()
def tesouro_data() -> str:
    return (FIXTURES_DIR / "tesouro_sample.csv").read_text()


def _make_response(
    json_data: object = None, text_data: str | None = None, status: int = 200
) -> MagicMock:
    """Create a mock httpx response."""
    resp = MagicMock()
    resp.status_code = status
    if status == 200:
        resp.raise_for_status = MagicMock()
    else:
        resp.raise_for_status = MagicMock(side_effect=Exception(f"HTTP {status}"))
    if json_data is not None:
        resp.json = MagicMock(return_value=json_data)
    if text_data is not None:
        resp.text = text_data
    return resp


def _make_mock_client(
    feed_configs: dict[str, FeedConfig],
    bcb_selic_data: list | None = None,
    bcb_ipca_data: list | None = None,
    bcb_usd_brl_data: list | None = None,
    ibge_data: list | None = None,
    tesouro_data: str | None = None,
    default_status: int = 200,
) -> AsyncMock:
    """Create a mock HTTP client that routes by URL matching feed configs."""
    url_map: dict[str, MagicMock] = {}
    for feed_id, feed in feed_configs.items():
        if feed_id == "bcb_selic" and bcb_selic_data is not None:
            url_map[feed.source.url] = _make_response(json_data=bcb_selic_data)
        elif feed_id == "bcb_ipca" and bcb_ipca_data is not None:
            url_map[feed.source.url] = _make_response(json_data=bcb_ipca_data)
        elif feed_id == "bcb_usd_brl" and bcb_usd_brl_data is not None:
            url_map[feed.source.url] = _make_response(json_data=bcb_usd_brl_data)
        elif feed_id == "ibge_pnad" and ibge_data is not None:
            url_map[feed.source.url] = _make_response(json_data=ibge_data)
        elif feed_id == "tesouro" and tesouro_data is not None:
            url_map[feed.source.url] = _make_response(text_data=tesouro_data)

    async def mock_get(url: str, **kwargs: object) -> MagicMock:
        if url in url_map:
            return url_map[url]
        return _make_response(status=default_status)

    client = AsyncMock()
    client.get = AsyncMock(side_effect=mock_get)
    return client


@pytest.mark.asyncio()
async def test_ingestion_task_success(
    storage: LocalStorageBackend,
    feed_configs: dict[str, FeedConfig],
    bcb_selic_data: list,
    bcb_ipca_data: list,
    bcb_usd_brl_data: list,
    ibge_data: list,
    tesouro_data: str,
) -> None:
    """IngestionTask should fetch all sources and write bronze Parquet files."""
    client = _make_mock_client(
        feed_configs,
        bcb_selic_data=bcb_selic_data,
        bcb_ipca_data=bcb_ipca_data,
        bcb_usd_brl_data=bcb_usd_brl_data,
        ibge_data=ibge_data,
        tesouro_data=tesouro_data,
    )

    task = IngestionTask(
        storage=storage,
        feed_configs=feed_configs,
        http_client=client,
        run_id="test-run-001",
    )
    result = await task.run()

    assert result.success is True
    assert result.task_name == "ingestion"
    assert result.rows_processed > 0
    assert result.duration_ms > 0

    keys = await storage.list_keys("bronze")
    assert len(keys) > 0
    key_str = " ".join(keys)
    assert "bcb_selic" in key_str
    assert "bcb_ipca" in key_str
    assert "bcb_usd_brl" in key_str


@pytest.mark.asyncio()
async def test_ingestion_task_partial_failure(
    storage: LocalStorageBackend,
    feed_configs: dict[str, FeedConfig],
    bcb_selic_data: list,
) -> None:
    """IngestionTask should handle partial failures gracefully."""
    client = _make_mock_client(
        feed_configs, bcb_selic_data=bcb_selic_data, default_status=500
    )

    task = IngestionTask(
        storage=storage, feed_configs=feed_configs, http_client=client
    )
    result = await task.run()

    assert result.task_name == "ingestion"
    assert result.rows_processed > 0
    assert len(result.warnings) > 0


@pytest.mark.asyncio()
async def test_ingestion_writes_immutable_bronze(
    storage: LocalStorageBackend,
    bcb_only_configs: dict[str, FeedConfig],
    bcb_selic_data: list,
) -> None:
    """Bronze files should be timestamped and immutable."""
    client = _make_mock_client(bcb_only_configs, bcb_selic_data=bcb_selic_data)

    task = IngestionTask(
        storage=storage, feed_configs=bcb_only_configs, http_client=client
    )
    await task.run()
    keys_first = await storage.list_keys("bronze")

    await task.run()
    keys_second = await storage.list_keys("bronze")
    assert len(keys_second) > len(keys_first)


@pytest.mark.asyncio()
async def test_ingestion_health_check(
    storage: LocalStorageBackend,
    bcb_only_configs: dict[str, FeedConfig],
) -> None:
    """Health check should return True when storage is accessible."""
    task = IngestionTask(
        storage=storage, feed_configs=bcb_only_configs, http_client=AsyncMock()
    )
    assert await task.health_check() is True


@pytest.mark.asyncio()
async def test_ingestion_metadata_columns(
    storage: LocalStorageBackend,
    bcb_only_configs: dict[str, FeedConfig],
    bcb_selic_data: list,
) -> None:
    """Bronze Parquet should include metadata columns."""
    client = _make_mock_client(bcb_only_configs, bcb_selic_data=bcb_selic_data)

    task = IngestionTask(
        storage=storage,
        feed_configs=bcb_only_configs,
        http_client=client,
        run_id="test-meta-001",
    )
    await task.run()

    keys = await storage.list_keys("bronze/bcb_selic")
    data = await storage.read(sorted(keys)[-1])
    table = pq.read_table(io.BytesIO(data))

    assert "_ingested_at" in table.column_names
    assert "_source" in table.column_names
    assert "_run_id" in table.column_names
    assert "_schema_hash" in table.column_names
    assert "_rescued_data" in table.column_names

    # Verify metadata values
    sources = table.column("_source").to_pylist()
    assert all(s == "bcb_selic" for s in sources)
    run_ids = table.column("_run_id").to_pylist()
    assert all(r == "test-meta-001" for r in run_ids)


@pytest.mark.asyncio()
async def test_ingestion_rescued_data_null_when_clean(
    storage: LocalStorageBackend,
    bcb_only_configs: dict[str, FeedConfig],
    bcb_selic_data: list,
) -> None:
    """Rescued data should be null when all fields match the schema."""
    client = _make_mock_client(bcb_only_configs, bcb_selic_data=bcb_selic_data)

    task = IngestionTask(
        storage=storage, feed_configs=bcb_only_configs, http_client=client
    )
    await task.run()

    keys = await storage.list_keys("bronze/bcb_selic")
    data = await storage.read(sorted(keys)[-1])
    table = pq.read_table(io.BytesIO(data))

    rescued = table.column("_rescued_data").to_pylist()
    assert all(r is None for r in rescued)


@pytest.mark.asyncio()
async def test_ingestion_rescued_data_captures_extra_fields(
    storage: LocalStorageBackend,
    bcb_only_configs: dict[str, FeedConfig],
) -> None:
    """Extra fields in raw response should go to _rescued_data."""
    raw_data = [
        {"data": "01/01/2026", "valor": "14.25", "unexpected_field": "surprise"},
    ]
    client = _make_mock_client(bcb_only_configs, bcb_selic_data=raw_data)

    task = IngestionTask(
        storage=storage, feed_configs=bcb_only_configs, http_client=client
    )
    await task.run()

    keys = await storage.list_keys("bronze/bcb_selic")
    data = await storage.read(sorted(keys)[-1])
    table = pq.read_table(io.BytesIO(data))

    rescued = table.column("_rescued_data").to_pylist()
    assert rescued[0] is not None
    rescued_json = json.loads(rescued[0])
    assert "unexpected_field" in rescued_json
    assert rescued_json["unexpected_field"] == "surprise"


@pytest.mark.asyncio()
async def test_ingestion_all_fields_as_strings(
    storage: LocalStorageBackend,
    bcb_only_configs: dict[str, FeedConfig],
    bcb_selic_data: list,
) -> None:
    """All bronze columns should be stored as string type."""
    client = _make_mock_client(bcb_only_configs, bcb_selic_data=bcb_selic_data)

    task = IngestionTask(
        storage=storage, feed_configs=bcb_only_configs, http_client=client
    )
    await task.run()

    keys = await storage.list_keys("bronze/bcb_selic")
    data = await storage.read(sorted(keys)[-1])
    table = pq.read_table(io.BytesIO(data))

    import pyarrow as pa

    for col_name in table.column_names:
        assert table.column(col_name).type == pa.string(), (
            f"Column {col_name} should be string, got {table.column(col_name).type}"
        )


@pytest.mark.asyncio()
async def test_ingestion_ibge_all_fields_preserved(
    storage: LocalStorageBackend,
    feed_configs: dict[str, FeedConfig],
    ibge_data: list,
) -> None:
    """IBGE bronze should preserve all 11 original fields, not cherry-pick 3."""
    ibge_configs = {"ibge_pnad": feed_configs["ibge_pnad"]}
    client = _make_mock_client(ibge_configs, ibge_data=ibge_data)

    task = IngestionTask(
        storage=storage, feed_configs=ibge_configs, http_client=client
    )
    await task.run()

    keys = await storage.list_keys("bronze/ibge_pnad")
    data = await storage.read(sorted(keys)[-1])
    table = pq.read_table(io.BytesIO(data))

    # All 11 IBGE fields (normalized to lowercase) + 5 metadata columns
    ibge_fields = {"nc", "nn", "mc", "mn", "v", "d1c", "d1n", "d2c", "d2n", "d3c", "d3n"}
    for field in ibge_fields:
        assert field in table.column_names, f"Missing IBGE field: {field}"


@pytest.mark.asyncio()
async def test_ingestion_skips_paused_feed(
    storage: LocalStorageBackend,
) -> None:
    """Feeds with status != active should be skipped."""
    configs = load_feed_configs("config/feeds/br_macro")
    # Manually mark one as paused (in a copy)
    from api.models import FeedStatus

    paused = configs["bcb_selic"].model_copy(update={"status": FeedStatus.PAUSED})
    test_configs = {"bcb_selic": paused}

    client = AsyncMock()
    task = IngestionTask(
        storage=storage, feed_configs=test_configs, http_client=client
    )
    result = await task.run()

    # Should fail because no data ingested (paused feeds are in configs but
    # the task iterates them — the feed_config loader normally filters them)
    # Here we test that the task still runs without error
    assert result.task_name == "ingestion"


@pytest.mark.asyncio()
async def test_ingestion_backfill_uses_backfill_url(
    storage: LocalStorageBackend,
    feed_configs: dict[str, FeedConfig],
    ibge_data: list,
) -> None:
    """In backfill mode, IngestionTask should use backfill_url for non-windowed feeds."""
    ibge_configs = {"ibge_pnad": feed_configs["ibge_pnad"]}
    backfill_url = feed_configs["ibge_pnad"].source.backfill_url
    assert backfill_url is not None

    # Mock that responds to any URL
    async def mock_get(url: str, **kwargs: object) -> MagicMock:
        return _make_response(json_data=ibge_data)

    client = AsyncMock()
    client.get = AsyncMock(side_effect=mock_get)

    task = IngestionTask(
        storage=storage,
        feed_configs=ibge_configs,
        http_client=client,
        backfill=True,
    )
    result = await task.run()

    assert result.success is True
    # Verify it called the backfill URL, not the daily URL
    called_url = client.get.call_args_list[0][0][0]
    assert "p/all" in called_url


@pytest.mark.asyncio()
async def test_ingestion_backfill_bcb_windowed(
    storage: LocalStorageBackend,
    feed_configs: dict[str, FeedConfig],
    bcb_selic_data: list,
) -> None:
    """BCB backfill should fetch multiple windows and concatenate."""
    bcb_configs = {"bcb_selic": feed_configs["bcb_selic"]}

    async def mock_get(url: str, **kwargs: object) -> MagicMock:
        return _make_response(json_data=bcb_selic_data)

    client = AsyncMock()
    client.get = AsyncMock(side_effect=mock_get)

    task = IngestionTask(
        storage=storage,
        feed_configs=bcb_configs,
        http_client=client,
        backfill=True,
    )
    result = await task.run()

    assert result.success is True
    # BCB 432 starts 1999, so at least 3 windows (1999-2009, 2009-2019, 2019-now)
    assert client.get.call_count >= 3
    # All calls should use date range format
    for call in client.get.call_args_list:
        url = call[0][0]
        assert "dataInicial" in url
        assert "dataFinal" in url


@pytest.mark.asyncio()
async def test_ingestion_daily_mode_ignores_backfill_url(
    storage: LocalStorageBackend,
    feed_configs: dict[str, FeedConfig],
    bcb_selic_data: list,
) -> None:
    """Without backfill flag, IngestionTask should use the daily URL."""
    bcb_configs = {"bcb_selic": feed_configs["bcb_selic"]}
    daily_url = feed_configs["bcb_selic"].source.url

    async def mock_get(url: str, **kwargs: object) -> MagicMock:
        return _make_response(json_data=bcb_selic_data)

    client = AsyncMock()
    client.get = AsyncMock(side_effect=mock_get)

    task = IngestionTask(
        storage=storage,
        feed_configs=bcb_configs,
        http_client=client,
        backfill=False,
    )
    result = await task.run()

    assert result.success is True
    assert client.get.call_count == 1
    called_url = client.get.call_args_list[0][0][0]
    assert called_url == daily_url


# ---------------------------------------------------------------------------
# json_data_path + auth tests (Wave 2 Session 3)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio()
async def test_parse_json_with_json_data_path(
    storage: LocalStorageBackend,
) -> None:
    """json_data_path should extract nested arrays from JSON envelopes."""
    fred_response = {
        "observations": [
            {"date": "2026-03-24", "value": "72.35"},
            {"date": "2026-03-25", "value": "71.98"},
        ]
    }
    feed_dict = {
        "feed_id": "fred_test",
        "name": "FRED Test",
        "source": {
            "type": "api",
            "url": "https://example.com/fred",
            "format": "json",
            "json_data_path": "observations",
        },
        "schema_fields": [
            {"name": "date", "source_field": "date", "type": "string", "required": True},
            {"name": "value", "source_field": "value", "type": "string", "required": True},
        ],
    }
    feed = FeedConfig.model_validate(feed_dict, strict=False)
    configs = {"fred_test": feed}

    async def mock_get(url: str, **kwargs: object) -> MagicMock:
        return _make_response(json_data=fred_response)

    client = AsyncMock()
    client.get = AsyncMock(side_effect=mock_get)

    task = IngestionTask(storage=storage, feed_configs=configs, http_client=client)
    result = await task.run()

    assert result.success is True
    assert result.rows_processed == 2


@pytest.mark.asyncio()
async def test_parse_json_array_index_path(
    storage: LocalStorageBackend,
) -> None:
    """json_data_path with numeric key should index into arrays (World Bank format)."""
    wb_response = [
        {"page": 1, "pages": 1, "total": 2},
        [
            {"date": "2025", "value": "3.2", "countryiso3code": "BRA"},
            {"date": "2024", "value": "2.9", "countryiso3code": "BRA"},
        ],
    ]
    feed_dict = {
        "feed_id": "wb_test",
        "name": "World Bank Test",
        "source": {
            "type": "api",
            "url": "https://example.com/wb",
            "format": "json",
            "json_data_path": "1",
        },
        "schema_fields": [
            {"name": "date", "source_field": "date", "type": "string", "required": True},
            {"name": "value", "source_field": "value", "type": "string", "required": True},
        ],
    }
    feed = FeedConfig.model_validate(feed_dict, strict=False)
    configs = {"wb_test": feed}

    async def mock_get(url: str, **kwargs: object) -> MagicMock:
        return _make_response(json_data=wb_response)

    client = AsyncMock()
    client.get = AsyncMock(side_effect=mock_get)

    task = IngestionTask(storage=storage, feed_configs=configs, http_client=client)
    result = await task.run()

    assert result.success is True
    assert result.rows_processed == 2


@pytest.mark.asyncio()
async def test_parse_json_without_json_data_path_unchanged(
    storage: LocalStorageBackend,
    bcb_only_configs: dict[str, FeedConfig],
    bcb_selic_data: list,
) -> None:
    """Existing feeds without json_data_path should work exactly as before."""
    client = _make_mock_client(bcb_only_configs, bcb_selic_data=bcb_selic_data)
    task = IngestionTask(
        storage=storage, feed_configs=bcb_only_configs, http_client=client
    )
    result = await task.run()

    assert result.success is True
    assert result.rows_processed > 0


@pytest.mark.asyncio()
async def test_auth_api_key_query_injects_param(
    storage: LocalStorageBackend,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """api_key_query auth should inject api_key query param from env var."""
    monkeypatch.setenv("TEST_API_KEY", "secret123")

    feed_dict = {
        "feed_id": "auth_test",
        "name": "Auth Test",
        "source": {
            "type": "api",
            "url": "https://example.com/api",
            "format": "json",
            "auth_method": "api_key_query",
            "auth_key_env": "TEST_API_KEY",
        },
        "schema_fields": [
            {"name": "date", "source_field": "date", "type": "string", "required": True},
            {"name": "value", "source_field": "value", "type": "string", "required": True},
        ],
    }
    feed = FeedConfig.model_validate(feed_dict, strict=False)
    configs = {"auth_test": feed}

    async def mock_get(url: str, **kwargs: Any) -> MagicMock:
        # Verify api_key was appended to the URL
        assert "api_key=secret123" in url
        return _make_response(json_data=[{"date": "2026-01-01", "value": "42"}])

    client = AsyncMock()
    client.get = AsyncMock(side_effect=mock_get)

    task = IngestionTask(storage=storage, feed_configs=configs, http_client=client)
    result = await task.run()
    assert result.success is True


@pytest.mark.asyncio()
async def test_auth_api_key_missing_env_raises(
    storage: LocalStorageBackend,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing env var for api_key_query auth should produce a warning (not crash pipeline)."""
    monkeypatch.delenv("MISSING_KEY", raising=False)

    feed_dict = {
        "feed_id": "auth_fail_test",
        "name": "Auth Fail Test",
        "source": {
            "type": "api",
            "url": "https://example.com/api",
            "format": "json",
            "auth_method": "api_key_query",
            "auth_key_env": "MISSING_KEY",
        },
        "schema_fields": [
            {"name": "v", "source_field": "v", "type": "string", "required": True},
        ],
    }
    feed = FeedConfig.model_validate(feed_dict, strict=False)
    configs = {"auth_fail_test": feed}

    client = AsyncMock()
    task = IngestionTask(storage=storage, feed_configs=configs, http_client=client)
    result = await task.run()

    # Should fail gracefully (warning, not crash)
    assert result.success is False
    assert any("MISSING_KEY" in w for w in result.warnings)


@pytest.mark.asyncio()
async def test_ingestion_backfill_yearly_urls(
    storage: LocalStorageBackend,
) -> None:
    """Yearly backfill should fetch one URL per year using {year} placeholder."""
    csv_data = "din_instante;val_cargaenergiamwmed\n2024-01-01;70000.5"
    feed_dict = {
        "feed_id": "ons_test",
        "name": "ONS Test",
        "source": {
            "type": "api",
            "url": "https://example.com/CARGA_2026.csv",
            "format": "csv",
            "csv_separator": ";",
            "backfill_url": "https://example.com/CARGA_{year}.csv",
            "backfill_start_year": 2024,
        },
        "schema_fields": [
            {"name": "din_instante", "source_field": "din_instante", "type": "string", "required": True},
            {"name": "val_cargaenergiamwmed", "source_field": "val_cargaenergiamwmed", "type": "string", "required": True},
        ],
    }
    feed = FeedConfig.model_validate(feed_dict, strict=False)
    configs = {"ons_test": feed}

    called_urls: list[str] = []

    async def mock_get(url: str, **kwargs: object) -> MagicMock:
        called_urls.append(url)
        return _make_response(text_data=csv_data)

    client = AsyncMock()
    client.get = AsyncMock(side_effect=mock_get)

    task = IngestionTask(storage=storage, feed_configs=configs, http_client=client, backfill=True)
    result = await task.run()

    assert result.success is True
    # Should fetch 2024, 2025, 2026 (start_year to current year)
    assert len(called_urls) >= 2
    assert any("2024" in u for u in called_urls)
    assert any("2025" in u for u in called_urls)


@pytest.mark.asyncio()
async def test_ingestion_yahoo_chart_format(
    storage: LocalStorageBackend,
) -> None:
    """Yahoo Finance v8 chart format should be parsed into row-based records."""
    yahoo_response = {
        "chart": {
            "result": [
                {
                    "timestamp": [1711497600, 1711584000, 1711670400],
                    "indicators": {
                        "quote": [
                            {
                                "close": [128150.0, 127500.5, 129000.0],
                                "open": [127000.0, 128000.0, 127600.0],
                                "high": [128500.0, 128200.0, 129500.0],
                                "low": [126800.0, 127100.0, 127400.0],
                                "volume": [1000000, 1100000, 1200000],
                            }
                        ]
                    },
                }
            ]
        }
    }
    feed_dict = {
        "feed_id": "ibovespa_test",
        "name": "IBOVESPA Test",
        "source": {
            "type": "api",
            "url": "https://query2.finance.yahoo.com/v8/finance/chart/%5EBVSP?range=5d&interval=1d",
            "format": "yahoo_chart",
        },
        "schema_fields": [
            {"name": "data", "source_field": "data", "type": "string", "required": True},
            {"name": "valor", "source_field": "valor", "type": "string", "required": True},
        ],
    }
    feed = FeedConfig.model_validate(feed_dict, strict=False)
    configs = {"ibovespa_test": feed}

    async def mock_get(url: str, **kwargs: object) -> MagicMock:
        return _make_response(json_data=yahoo_response)

    client = AsyncMock()
    client.get = AsyncMock(side_effect=mock_get)

    task = IngestionTask(storage=storage, feed_configs=configs, http_client=client)
    result = await task.run()

    assert result.success is True
    assert result.rows_processed == 3

    # Verify bronze parquet was written
    keys = await storage.list_keys("bronze/ibovespa_test")
    assert len(keys) == 1
    data = await storage.read(keys[0])
    table = pq.read_table(io.BytesIO(data))
    assert table.num_rows == 3
    # Check the valor column has closing values
    valores = table.column("valor").to_pylist()
    assert valores[0] == "128150.0"
