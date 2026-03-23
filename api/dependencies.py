"""FastAPI dependencies for auth, storage, and data access."""

from __future__ import annotations

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

from api.models import FreshnessStatus, SyncInfo

logger = structlog.get_logger()

# --- In-memory cache for R2 gold data ---

_gold_cache: dict[str, tuple[float, bytes]] = {}
_CACHE_TTL_SECONDS = 300  # 5 minutes


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


async def _read_gold_bytes(series: str) -> bytes | None:
    """Read gold parquet bytes — from R2 (cached) or local disk."""
    if _is_r2_mode():
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
            _gold_cache[series] = (now, data)
            logger.debug("gold_r2_read", series=series, size=len(data))
            return data
        except Exception as exc:
            logger.warning("gold_r2_read_error", series=series, error=str(exc))
            return None

    # Local mode
    parquet_path = get_gold_dir() / f"{series}.parquet"
    if not parquet_path.exists():
        return None
    return parquet_path.read_bytes()


def _query_parquet_bytes(parquet_bytes: bytes, after: str | None) -> list[dict[str, Any]]:
    """Query in-memory parquet bytes with DuckDB."""
    table = pq.read_table(io.BytesIO(parquet_bytes))
    conn = duckdb.connect()
    conn.register("gold", table)

    if after:
        result = conn.execute(
            "SELECT date, value, series FROM gold WHERE CAST(date AS DATE) >= CAST(? AS DATE) ORDER BY date",
            [after],
        ).fetchall()
    else:
        result = conn.execute(
            "SELECT date, value, series FROM gold ORDER BY date"
        ).fetchall()
    columns = ["date", "value", "series"]
    conn.close()

    return [dict(zip(columns, row)) for row in result]


async def query_gold_series(series: str, after: str | None = None) -> list[dict[str, Any]]:
    """Query gold Parquet for a given series, optionally filtered by date."""
    data = await _read_gold_bytes(series)
    if data is None:
        return []
    return _query_parquet_bytes(data, after)
