"""FastAPI dependencies for auth, storage, and data access."""

from __future__ import annotations

import asyncio
import io
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pyarrow.parquet as pq
import structlog
from fastapi import Header, HTTPException

from api.models import FreshnessStatus, QualityReport, RunManifest, SeriesFreshness, SyncInfo

logger = structlog.get_logger()

# --- In-memory cache for R2 gold data ---

_gold_cache: dict[str, tuple[float, bytes]] = {}
_CACHE_TTL_SECONDS = 300  # 5 minutes

# --- Request coalescing: prevent thundering herd on cache miss ---
_inflight: dict[str, asyncio.Future[bytes | None]] = {}


def _is_r2_mode() -> bool:
    return os.environ.get("STORAGE_BACKEND", "local") == "r2"


def get_gold_dir() -> Path:
    return Path(os.environ.get("GOLD_DATA_DIR", "./data/local/gold"))


def verify_sync_token(authorization: str | None = Header(default=None)) -> str:
    """Verify Bearer token for /internal/sync endpoint."""
    if authorization is None:
        raise HTTPException(status_code=401, detail="Missing authorization header")
    expected = os.environ.get("SYNC_WEBHOOK_SECRET", "")
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization")
    token = authorization[len("Bearer "):]
    if token != expected:
        raise HTTPException(status_code=401, detail="Invalid token")
    return token


def read_sync_metadata() -> SyncInfo | None:
    """Read /data/gold/metadata.json for sync status."""
    meta_path = get_gold_dir() / "metadata.json"
    if not meta_path.exists():
        return None
    try:
        data = json.loads(meta_path.read_text())
        return SyncInfo(
            last_sync_at=datetime.fromisoformat(data["last_sync_at"]),
            run_id=data.get("run_id"),
            files_synced=data.get("files_synced", 0),
            sync_duration_ms=data.get("sync_duration_ms", 0.0),
            source=data.get("source", ""),
        )
    except Exception as exc:
        logger.warning("sync_metadata_read_error", error=str(exc))
        return None


def get_sync_health(sync_info: SyncInfo | None) -> FreshnessStatus:
    """Determine sync health from metadata."""
    if sync_info is None or sync_info.last_sync_at is None:
        return FreshnessStatus.CRITICAL

    now = datetime.now(timezone.utc)
    last = sync_info.last_sync_at.replace(tzinfo=timezone.utc) if sync_info.last_sync_at.tzinfo is None else sync_info.last_sync_at
    hours = (now - last).total_seconds() / 3600

    if hours < 26:
        return FreshnessStatus.FRESH
    elif hours < 120:  # 5 days
        return FreshnessStatus.STALE
    return FreshnessStatus.CRITICAL


async def _read_gold_bytes_inner(series: str) -> bytes | None:
    """Read gold parquet bytes from R2 (no coalescing)."""
    now = time.monotonic()

    # Check cache
    if series in _gold_cache:
        cached_at, data = _gold_cache[series]
        if now - cached_at < _CACHE_TTL_SECONDS:
            return data

    try:
        from storage.r2 import R2StorageBackend
        r2 = R2StorageBackend()
        key = f"gold/{series}.parquet"
        if not await r2.exists(key):
            return None
        data = await r2.read(key)
        _gold_cache[series] = (time.monotonic(), data)
        logger.debug("gold_r2_read", series=series, size=len(data))
        return data
    except Exception as exc:
        logger.warning("gold_r2_read_error", series=series, error=str(exc))
        return None


async def _read_gold_bytes(series: str) -> bytes | None:
    """Read gold parquet bytes — from R2 (coalesced + cached) or local disk.

    Coalescing: if multiple concurrent requests ask for the same series,
    only one reads from storage; others await the same Future.
    """
    if _is_r2_mode():
        # Request coalescing for R2 reads
        if series in _inflight:
            return await _inflight[series]

        loop = asyncio.get_running_loop()
        future: asyncio.Future[bytes | None] = loop.create_future()
        _inflight[series] = future
        try:
            result = await _read_gold_bytes_inner(series)
            future.set_result(result)
            return result
        except Exception as exc:
            future.set_exception(exc)
            raise
        finally:
            _inflight.pop(series, None)

    # Local mode
    parquet_path = get_gold_dir() / f"{series}.parquet"
    if not parquet_path.exists():
        return None
    return parquet_path.read_bytes()


def _query_parquet_bytes(parquet_bytes: bytes, after: str | None) -> list[dict[str, Any]]:
    """Query in-memory parquet bytes with DuckDB.

    When a date filter returns no rows but the dataset contains data
    (e.g. annual World Bank series filtered by a 1-year window),
    falls back to returning all available data points.
    """
    table = pq.read_table(io.BytesIO(parquet_bytes))
    conn = duckdb.connect()
    conn.register("gold", table)

    columns = ["date", "value", "series"]

    if after:
        result = conn.execute(
            "SELECT date, value, series FROM gold WHERE CAST(date AS DATE) >= CAST(? AS DATE) ORDER BY date",
            [after],
        ).fetchall()
        if not result:
            # Fallback: return all data when filter excludes everything
            result = conn.execute(
                "SELECT date, value, series FROM gold ORDER BY date"
            ).fetchall()
    else:
        result = conn.execute(
            "SELECT date, value, series FROM gold ORDER BY date"
        ).fetchall()

    conn.close()
    return [dict(zip(columns, row)) for row in result]


async def query_gold_series(series: str, after: str | None = None) -> list[dict[str, Any]]:
    """Query gold Parquet for a given series, optionally filtered by date."""
    data = await _read_gold_bytes(series)
    if data is None:
        return []
    return _query_parquet_bytes(data, after)


# --- Quality report reading ---


async def _read_all_quality_reports() -> list[QualityReport]:
    """Read all quality reports from storage."""
    from storage import get_storage_backend

    storage = get_storage_backend()
    try:
        keys = await storage.list_keys("quality")
    except Exception as exc:
        logger.warning("quality_list_keys_error", error=str(exc))
        return []

    reports: list[QualityReport] = []
    for key in keys:
        if not key.endswith("report.json"):
            continue
        try:
            data = await storage.read(key)
            report = QualityReport.model_validate_json(data)
            reports.append(report)
        except Exception as exc:
            logger.warning("quality_report_parse_error", key=key, error=str(exc))
            continue

    return reports


async def _read_watermark_timestamp(series_id: str) -> datetime | None:
    """Read the last_processed_at from a series watermark file."""
    from api.models import SilverWatermark
    from storage import get_storage_backend

    storage = get_storage_backend()
    wm_key = f"silver/{series_id}/_watermark.json"
    try:
        if not await storage.exists(wm_key):
            return None
        data = await storage.read(wm_key)
        wm = SilverWatermark.model_validate_json(data)
        return wm.last_processed_at
    except Exception:
        return None


async def compute_series_freshness() -> list[SeriesFreshness]:
    """Compute freshness for all tracked series based on gold data timestamps and ingestion watermarks."""
    from api.series_config import SERIES_DISPLAY

    results: list[SeriesFreshness] = []
    now = datetime.now(timezone.utc)

    for series_id, config in SERIES_DISPLAY.items():
        threshold_hours = float(config["freshness_hours"])
        rows = await query_gold_series(series_id)
        last_ingested_at = await _read_watermark_timestamp(series_id)

        if not rows:
            results.append(SeriesFreshness(
                series=series_id,
                last_updated=None,
                status=FreshnessStatus.CRITICAL,
                hours_since_update=None,
                last_ingested_at=last_ingested_at,
            ))
            continue

        # Rows are sorted ascending by date; last row is most recent
        last_row = rows[-1]
        last_date = last_row["date"]

        # Handle both datetime and date objects
        if isinstance(last_date, datetime):
            if last_date.tzinfo is None:
                last_date = last_date.replace(tzinfo=timezone.utc)
        else:
            # It's a date object — convert to datetime at midnight UTC
            last_date = datetime(last_date.year, last_date.month, last_date.day, tzinfo=timezone.utc)

        hours_since = (now - last_date).total_seconds() / 3600

        if hours_since < threshold_hours:
            status = FreshnessStatus.FRESH
        elif hours_since < threshold_hours * 2:
            status = FreshnessStatus.STALE
        else:
            status = FreshnessStatus.CRITICAL

        results.append(SeriesFreshness(
            series=series_id,
            last_updated=last_date,
            status=status,
            hours_since_update=round(hours_since, 1),
            last_ingested_at=last_ingested_at,
        ))

    return results


async def read_latest_quality_report() -> QualityReport | None:
    """Return the most recent quality report from storage, or None."""
    reports = await _read_all_quality_reports()
    if not reports:
        return None
    return max(reports, key=lambda r: r.timestamp)


async def read_quality_history(limit: int = 20) -> list[QualityReport]:
    """Return quality reports sorted by timestamp descending."""
    reports = await _read_all_quality_reports()
    reports.sort(key=lambda r: r.timestamp, reverse=True)
    return reports[:limit]


# --- Run history reading ---


async def read_run_history(limit: int = 20) -> list[RunManifest]:
    """Read all run manifests from storage, sorted by started_at desc."""
    from storage import get_storage_backend

    storage = get_storage_backend()
    try:
        keys = await storage.list_keys("runs")
    except Exception as exc:
        logger.warning("runs_list_keys_error", error=str(exc))
        return []

    manifests: list[RunManifest] = []
    for key in keys:
        if not key.endswith("manifest.json"):
            continue
        try:
            data = await storage.read(key)
            manifest = RunManifest.model_validate_json(data)
            manifests.append(manifest)
        except Exception as exc:
            logger.warning("run_manifest_parse_error", key=key, error=str(exc))
            continue

    manifests.sort(key=lambda m: m.started_at, reverse=True)
    return manifests[:limit]


async def read_run_manifest(run_id: str) -> RunManifest | None:
    """Read a specific run manifest by run_id."""
    from storage import get_storage_backend

    storage = get_storage_backend()
    key = f"runs/{run_id}/manifest.json"
    try:
        if not await storage.exists(key):
            return None
        data = await storage.read(key)
        return RunManifest.model_validate_json(data)
    except Exception as exc:
        logger.warning("run_manifest_read_error", run_id=run_id, error=str(exc))
        return None
