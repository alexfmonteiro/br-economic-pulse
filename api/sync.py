"""Data sync — downloads gold Parquet, quality reports, and run manifests from R2."""

from __future__ import annotations

import json
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import structlog

logger = structlog.get_logger()


async def _sync_prefix(
    r2: object,
    prefix: str,
    target_dir: Path,
    extension: str | None = None,
) -> tuple[int, list[str]]:
    """Download all files under an R2 prefix to a local directory.

    Preserves the key structure relative to the prefix.
    Returns (files_synced, errors).
    """
    from storage.r2 import R2StorageBackend

    assert isinstance(r2, R2StorageBackend)
    errors: list[str] = []

    try:
        keys = await r2.list_keys(prefix)
    except Exception as exc:
        return 0, [f"Cannot list R2 keys for {prefix}: {exc}"]

    if extension:
        keys = [k for k in keys if k.endswith(extension)]

    if not keys:
        return 0, []

    files_synced = 0
    for key in keys:
        # Preserve directory structure: quality/run-123/report.json → target_dir/quality/run-123/report.json
        rel_path = key
        target = target_dir / rel_path
        target.parent.mkdir(parents=True, exist_ok=True)

        try:
            data = await r2.read(key)

            fd, tmp_path = tempfile.mkstemp(
                dir=str(target.parent), suffix=".tmp", prefix=".sync_"
            )
            try:
                os.write(fd, data)
                os.close(fd)
                os.replace(tmp_path, str(target))
                files_synced += 1
                logger.debug("sync_file_written", key=key, size=len(data))
            except Exception:
                try:
                    os.close(fd)
                except OSError:
                    pass
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
                raise
        except Exception as exc:
            errors.append(f"Failed to sync {key}: {exc}")
            logger.warning("sync_file_failed", key=key, error=str(exc))

    return files_synced, errors


async def sync_gold_from_r2(gold_dir: Path) -> tuple[int, float, list[str]]:
    """Download gold Parquet, quality reports, and run manifests from R2.

    Gold parquet → gold_dir (GOLD_DATA_DIR, persistent volume).
    Quality reports + run manifests → LOCAL_DATA_DIR (where LocalStorageBackend reads).

    Uses atomic os.replace() so readers never see partial writes.
    Returns (files_synced, duration_ms, errors).
    """
    start = time.perf_counter()

    try:
        from storage.r2 import R2StorageBackend
        r2 = R2StorageBackend()
    except Exception as exc:
        return 0, 0.0, [f"Cannot initialize R2 backend: {exc}"]

    gold_dir.mkdir(parents=True, exist_ok=True)
    local_data_dir = Path(os.environ.get("LOCAL_DATA_DIR", "./data/local"))

    all_errors: list[str] = []
    total_synced = 0

    # 1. Gold parquet → gold_dir (persistent volume for DuckDB)
    gold_synced, gold_errors = await _sync_prefix(
        r2, "gold/", gold_dir.parent, extension=".parquet"
    )
    total_synced += gold_synced
    all_errors.extend(gold_errors)

    # 2. Quality reports → LOCAL_DATA_DIR/quality/
    quality_synced, quality_errors = await _sync_prefix(
        r2, "quality/", local_data_dir, extension=".json"
    )
    total_synced += quality_synced
    all_errors.extend(quality_errors)

    # 3. Run manifests → LOCAL_DATA_DIR/runs/
    runs_synced, runs_errors = await _sync_prefix(
        r2, "runs/", local_data_dir, extension=".json"
    )
    total_synced += runs_synced
    all_errors.extend(runs_errors)

    duration_ms = round((time.perf_counter() - start) * 1000, 2)

    # Write metadata.json
    metadata = {
        "last_sync_at": datetime.now(timezone.utc).isoformat(),
        "run_id": f"sync-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}",
        "files_synced": total_synced,
        "sync_duration_ms": duration_ms,
        "source": "r2",
    }
    (gold_dir / "metadata.json").write_text(json.dumps(metadata, indent=2))

    logger.info(
        "sync_complete",
        files_synced=total_synced,
        gold=gold_synced,
        quality=quality_synced,
        runs=runs_synced,
        duration_ms=duration_ms,
        errors=len(all_errors),
    )

    return total_synced, duration_ms, all_errors
