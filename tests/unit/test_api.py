"""Tests for FastAPI app."""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from httpx import ASGITransport, AsyncClient

from api.main import app


@pytest.fixture(autouse=True)
def env_vars(tmp_path: Path) -> None:  # type: ignore[misc]
    """Set required env vars for tests."""
    gold_dir = tmp_path / "gold"
    gold_dir.mkdir()

    # Write a sample gold parquet
    dates = [datetime(2026, 1, 1), datetime(2026, 2, 1), datetime(2026, 3, 1)]
    table = pa.table({
        "date": dates,
        "value": [14.75, 14.75, 14.75],
        "series": ["bcb_432", "bcb_432", "bcb_432"],
        "mom_delta": [None, 0.0, 0.0],
        "yoy_delta": [None, None, None],
        "rolling_12m_avg": [14.75, 14.75, 14.75],
        "z_score": [None, None, None],
    })
    pq.write_table(table, gold_dir / "bcb_432.parquet")

    # Write metadata
    metadata = {
        "last_sync_at": "2026-03-22T06:05:12Z",
        "run_id": "pipeline-20260322-060000",
        "files_synced": 1,
        "sync_duration_ms": 100.0,
        "source": "local",
    }
    (gold_dir / "metadata.json").write_text(json.dumps(metadata))

    with patch.dict(os.environ, {
        "GOLD_DATA_DIR": str(gold_dir),
        "SYNC_WEBHOOK_SECRET": "test-secret-token",
        "ALLOWED_ORIGINS": "http://localhost:5173",
        "STORAGE_BACKEND": "local",
        "LOCAL_DATA_DIR": str(tmp_path),
    }):
        yield


@pytest.mark.asyncio
async def test_health_endpoint() -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "healthy"
    assert "timestamp" in data


@pytest.mark.asyncio
async def test_metrics_endpoint() -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/metrics/bcb_432")
    assert resp.status_code == 200
    data = resp.json()
    assert data["series"] == "bcb_432"
    assert len(data["data_points"]) > 0


@pytest.mark.asyncio
async def test_metrics_unknown_series() -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/metrics/unknown_series")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_quality_latest() -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/quality/latest")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert "report" in data
    assert "series_freshness" in data


@pytest.mark.asyncio
async def test_quality_latest_with_report(tmp_path: Path) -> None:
    """When a quality report exists in storage, it should be returned."""
    from storage import get_storage_backend

    storage = get_storage_backend()
    report_data = {
        "run_id": "quality-abc12345",
        "stage": "post_ingestion",
        "timestamp": "2026-03-22T06:05:00+00:00",
        "overall_status": "passed",
        "checks": [
            {"check_name": "null_rate_bcb_432", "passed": True, "metric_value": 0.0, "threshold": 0.02, "message": ""}
        ],
        "series_freshness": [],
        "critical_failures": [],
    }
    await storage.write(
        "quality/quality-abc12345/report.json",
        json.dumps(report_data).encode(),
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/quality/latest")
    assert resp.status_code == 200
    data = resp.json()
    assert data["report"] is not None
    assert data["report"]["run_id"] == "quality-abc12345"
    assert data["status"] == "ok"


@pytest.mark.asyncio
async def test_quality_latest_most_recent_report(tmp_path: Path) -> None:
    """When multiple reports exist, the most recent should be returned."""
    from storage import get_storage_backend

    storage = get_storage_backend()
    for i, ts in enumerate(["2026-03-20T06:00:00+00:00", "2026-03-22T06:00:00+00:00"]):
        report_data = {
            "run_id": f"quality-run{i}",
            "stage": "post_ingestion",
            "timestamp": ts,
            "overall_status": "passed",
            "checks": [],
            "series_freshness": [],
            "critical_failures": [],
        }
        await storage.write(
            f"quality/quality-run{i}/report.json",
            json.dumps(report_data).encode(),
        )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/quality/latest")
    assert resp.status_code == 200
    data = resp.json()
    assert data["report"]["run_id"] == "quality-run1"  # most recent


@pytest.mark.asyncio
async def test_quality_latest_malformed_report_skipped(tmp_path: Path) -> None:
    """Malformed report JSON should be skipped without error."""
    from storage import get_storage_backend

    storage = get_storage_backend()
    await storage.write("quality/bad-run/report.json", b"not valid json")

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/quality/latest")
    assert resp.status_code == 200
    data = resp.json()
    assert data["report"] is None


@pytest.mark.asyncio
async def test_quality_history_empty() -> None:
    """Quality history with no reports returns empty list."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/quality/history")
    assert resp.status_code == 200
    data = resp.json()
    assert data["reports"] == []
    assert data["total"] == 0


@pytest.mark.asyncio
async def test_quality_history_returns_sorted(tmp_path: Path) -> None:
    """Quality history returns reports sorted by timestamp desc."""
    from storage import get_storage_backend

    storage = get_storage_backend()
    timestamps = [
        "2026-03-20T06:00:00+00:00",
        "2026-03-22T06:00:00+00:00",
        "2026-03-21T06:00:00+00:00",
    ]
    for i, ts in enumerate(timestamps):
        report_data = {
            "run_id": f"quality-hist{i}",
            "stage": "post_ingestion",
            "timestamp": ts,
            "overall_status": "passed",
            "checks": [],
            "series_freshness": [],
            "critical_failures": [],
        }
        await storage.write(
            f"quality/quality-hist{i}/report.json",
            json.dumps(report_data).encode(),
        )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/quality/history")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 3
    # Should be sorted desc by timestamp
    assert data["reports"][0]["run_id"] == "quality-hist1"  # 2026-03-22
    assert data["reports"][1]["run_id"] == "quality-hist2"  # 2026-03-21
    assert data["reports"][2]["run_id"] == "quality-hist0"  # 2026-03-20


@pytest.mark.asyncio
async def test_quality_history_respects_limit(tmp_path: Path) -> None:
    """Quality history respects limit parameter."""
    from storage import get_storage_backend

    storage = get_storage_backend()
    for i in range(3):
        report_data = {
            "run_id": f"quality-lim{i}",
            "stage": "post_ingestion",
            "timestamp": f"2026-03-{20+i}T06:00:00+00:00",
            "overall_status": "passed",
            "checks": [],
            "series_freshness": [],
            "critical_failures": [],
        }
        await storage.write(
            f"quality/quality-lim{i}/report.json",
            json.dumps(report_data).encode(),
        )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/quality/history?limit=1")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 1
    assert len(data["reports"]) == 1


@pytest.mark.asyncio
async def test_quality_latest_series_freshness() -> None:
    """The quality endpoint should include series_freshness data."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/quality/latest")
    assert resp.status_code == 200
    data = resp.json()
    freshness = data["series_freshness"]
    assert isinstance(freshness, list)
    assert len(freshness) == 8  # 8 tracked series
    # bcb_432 has gold data in fixture; the rest should be CRITICAL (no data)
    bcb_432 = next(f for f in freshness if f["series"] == "bcb_432")
    assert bcb_432["status"] in ("fresh", "stale", "critical")
    assert bcb_432["hours_since_update"] is not None
    # Series without data should be CRITICAL
    ibge_pnad = next(f for f in freshness if f["series"] == "ibge_pnad")
    assert ibge_pnad["status"] == "critical"
    assert ibge_pnad["last_updated"] is None


@pytest.mark.asyncio
async def test_runs_list_empty() -> None:
    """Run history with no manifests returns empty list."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/runs")
    assert resp.status_code == 200
    data = resp.json()
    assert data["runs"] == []
    assert data["total"] == 0


@pytest.mark.asyncio
async def test_runs_list_returns_sorted(tmp_path: Path) -> None:
    """Run history returns manifests sorted by started_at desc."""
    from storage import get_storage_backend

    storage = get_storage_backend()
    manifests = [
        {
            "run_id": "pipeline-20260320-060000",
            "started_at": "2026-03-20T06:00:00+00:00",
            "finished_at": "2026-03-20T06:01:00+00:00",
            "status": "success",
            "trigger": "cron",
            "stages": [],
        },
        {
            "run_id": "pipeline-20260322-060000",
            "started_at": "2026-03-22T06:00:00+00:00",
            "finished_at": "2026-03-22T06:01:00+00:00",
            "status": "success",
            "trigger": "cron",
            "stages": [],
        },
    ]
    for m in manifests:
        await storage.write(
            f"runs/{m['run_id']}/manifest.json",
            json.dumps(m).encode(),
        )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/runs")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 2
    assert data["runs"][0]["run_id"] == "pipeline-20260322-060000"  # most recent


@pytest.mark.asyncio
async def test_run_detail_found(tmp_path: Path) -> None:
    """Run detail returns manifest for valid run_id."""
    from storage import get_storage_backend

    storage = get_storage_backend()
    manifest = {
        "run_id": "pipeline-20260322-060000",
        "started_at": "2026-03-22T06:00:00+00:00",
        "finished_at": "2026-03-22T06:01:00+00:00",
        "status": "success",
        "trigger": "local",
        "stages": [],
    }
    await storage.write(
        "runs/pipeline-20260322-060000/manifest.json",
        json.dumps(manifest).encode(),
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/runs/pipeline-20260322-060000")
    assert resp.status_code == 200
    data = resp.json()
    assert data["run_id"] == "pipeline-20260322-060000"


@pytest.mark.asyncio
async def test_run_detail_not_found() -> None:
    """Run detail returns 404 for unknown run_id."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/runs/pipeline-nonexistent")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_sync_status() -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/quality/sync-status")
    assert resp.status_code == 200
    data = resp.json()
    assert "sync_health" in data
    assert "last_sync_at" in data


@pytest.mark.asyncio
async def test_sync_endpoint_requires_auth() -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/api/internal/sync")
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_sync_endpoint_rejects_bad_token() -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/internal/sync",
            headers={"Authorization": "Bearer wrong-token"},
        )
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_sync_endpoint_accepts_valid_token() -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/internal/sync",
            headers={"Authorization": "Bearer test-secret-token"},
        )
    # May succeed or fail depending on storage state, but should not be 401
    assert resp.status_code != 401


@pytest.mark.asyncio
async def test_security_headers() -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/health")
    assert resp.headers.get("X-Content-Type-Options") == "nosniff"
    assert resp.headers.get("X-Frame-Options") == "DENY"


@pytest.mark.asyncio
async def test_sentry_tunnel_rejects_invalid_envelope() -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/api/sentry-tunnel", content=b"not-json")
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_sentry_tunnel_rejects_disallowed_dsn() -> None:
    envelope = b'{"dsn":"https://key@evil.example.com/123"}\n{}'
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/api/sentry-tunnel", content=envelope)
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_debug_sentry_raises_in_dev() -> None:
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/debug-sentry")
    assert resp.status_code == 500


@pytest.mark.asyncio
async def test_debug_sentry_hidden_in_production() -> None:
    transport = ASGITransport(app=app)
    with patch.dict(os.environ, {"APP_ENV": "production"}):
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/debug-sentry")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_sync_not_in_openapi() -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/openapi.json")
    schema = resp.json()
    paths = schema.get("paths", {})
    assert "/api/internal/sync" not in paths
