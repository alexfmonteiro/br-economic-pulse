"""FastAPI application — BR Economic Pulse API."""

from __future__ import annotations

import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import structlog
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware

from api.dependencies import (
    get_gold_dir,
    get_sync_health,
    query_gold_series,
    read_sync_metadata,
    verify_sync_token,
)
from api.models import (
    HealthResponse,
    MetricDataPoint,
    MetricsResponse,
    SyncResult,
    SyncStatusResponse,
)

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Load environment variables on startup."""
    load_dotenv(".env.local")
    load_dotenv()
    logger.info("app_started", env=os.environ.get("APP_ENV", "unknown"))
    yield


app = FastAPI(title="BR Economic Pulse API", version="0.5.0", lifespan=lifespan)

# --- Middleware ---


def _get_allowed_origins() -> list[str]:
    return os.environ.get("ALLOWED_ORIGINS", "http://localhost:5173").split(",")


app.add_middleware(
    CORSMiddleware,
    allow_origins=_get_allowed_origins(),
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
    allow_credentials=False,
)


@app.middleware("http")
async def security_headers(request: Request, call_next: Any) -> Response:
    """Add security headers to all responses."""
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    return response


# --- Routes ---


@app.get("/api/health")
async def health() -> HealthResponse:
    sync_info = read_sync_metadata()
    sync_health = get_sync_health(sync_info)

    return HealthResponse(
        status="healthy",
        timestamp=datetime.now(timezone.utc),
        sync=sync_info,
        data_freshness={"sync": sync_health.value},
    )


@app.get("/api/metrics/{series}")
async def get_metrics(series: str, after: str | None = None) -> MetricsResponse:
    rows = await query_gold_series(series, after=after)
    if not rows:
        raise HTTPException(status_code=404, detail=f"Series '{series}' not found")

    data_points = []
    for row in rows:
        d = row["date"]
        # DuckDB may return date or datetime depending on the SQL function used
        if not isinstance(d, datetime):
            d = datetime(d.year, d.month, d.day)
        data_points.append(MetricDataPoint(
            date=d,
            value=float(row["value"]),
            series=row["series"],
        ))

    return MetricsResponse(
        series=series,
        data_points=data_points,
        last_updated=data_points[-1].date if data_points else None,
    )


@app.get("/api/quality/latest")
async def quality_latest() -> dict[str, Any]:
    """Return the latest quality report if available."""
    sync_info = read_sync_metadata()
    sync_health = get_sync_health(sync_info)

    return {
        "status": "ok",
        "sync_health": sync_health.value,
        "last_sync": sync_info.model_dump() if sync_info else None,
    }


@app.get("/api/quality/sync-status")
async def sync_status() -> SyncStatusResponse:
    sync_info = read_sync_metadata()
    sync_health = get_sync_health(sync_info)

    seconds_since = None
    if sync_info and sync_info.last_sync_at:
        last = sync_info.last_sync_at
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        seconds_since = (datetime.now(timezone.utc) - last).total_seconds()

    return SyncStatusResponse(
        last_sync_at=sync_info.last_sync_at if sync_info else None,
        run_id=sync_info.run_id if sync_info else None,
        files_synced=sync_info.files_synced if sync_info else 0,
        sync_duration_ms=sync_info.sync_duration_ms if sync_info else 0.0,
        source=sync_info.source if sync_info else "",
        seconds_since_sync=seconds_since,
        sync_health=sync_health,
    )


@app.post(
    "/api/internal/sync",
    include_in_schema=False,
)
async def trigger_sync(
    _token: str = Depends(verify_sync_token),
) -> SyncResult:
    """Trigger gold data sync from R2. Protected by Bearer token. Not in OpenAPI docs."""
    logger.info("sync_triggered")

    gold_dir = get_gold_dir()

    # If using local storage (dev mode), just count existing files
    storage_backend = os.environ.get("STORAGE_BACKEND", "local")
    if storage_backend == "local":
        gold_dir.mkdir(parents=True, exist_ok=True)
        parquet_files = list(gold_dir.glob("*.parquet"))
        return SyncResult(
            success=True,
            files_synced=len(parquet_files),
            sync_duration_ms=0.0,
        )

    # Production: download gold files from R2 to persistent volume
    from api.sync import sync_gold_from_r2

    files_synced, duration_ms, errors = await sync_gold_from_r2(gold_dir)

    return SyncResult(
        success=len(errors) == 0,
        files_synced=files_synced,
        sync_duration_ms=duration_ms,
        errors=errors,
    )
