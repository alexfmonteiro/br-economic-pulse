"""Full data refresh: wipe local + R2, backfill from sources, upload to R2, trigger API sync.

Usage:
    uv run python scripts/full_refresh.py
    uv run python scripts/full_refresh.py --skip-r2-cleanup   # keep R2 files before upload
    uv run python scripts/full_refresh.py --dry-run            # show what would happen
"""

from __future__ import annotations

import argparse
import asyncio
import shutil
import subprocess
import sys
import time
from pathlib import Path

# Ensure project root is on sys.path so `from storage.r2 import ...` works
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

import structlog  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

# Load env before any project imports
load_dotenv(_PROJECT_ROOT / ".env.local")
load_dotenv()

logger = structlog.get_logger()

PROJECT_ROOT = _PROJECT_ROOT
LOCAL_DATA = PROJECT_ROOT / "data" / "local"
R2_PREFIXES = ["bronze/", "silver/", "gold/", "quality/"]


def _step(msg: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {msg}")
    print(f"{'='*60}")


# ── Step 1: Clean local data ──────────────────────────────────────────────


def cleanup_local(dry_run: bool = False) -> None:
    _step("Step 1/5 — Cleaning local data")
    for subdir in ("bronze", "silver", "gold", "quality"):
        path = LOCAL_DATA / subdir
        if path.exists():
            if dry_run:
                print(f"  [dry-run] Would delete {path}")
            else:
                shutil.rmtree(path)
                print(f"  Deleted {path}")
        else:
            print(f"  {path} does not exist, skipping")


# ── Step 2: Clean R2 ─────────────────────────────────────────────────────


async def cleanup_r2(dry_run: bool = False) -> None:
    _step("Step 2/5 — Cleaning R2 bucket")
    try:
        from storage.r2 import R2StorageBackend

        r2 = R2StorageBackend()
    except Exception as exc:
        print(f"  R2 not configured ({exc}), skipping R2 cleanup")
        return

    total_deleted = 0
    for prefix in R2_PREFIXES:
        keys = await r2.list_keys(prefix)
        if not keys:
            print(f"  {prefix}: empty")
            continue
        print(f"  {prefix}: {len(keys)} files")
        for key in keys:
            if dry_run:
                print(f"    [dry-run] Would delete {key}")
            else:
                await r2.delete(key)
            total_deleted += 1

    action = "Would delete" if dry_run else "Deleted"
    print(f"  {action} {total_deleted} files from R2")


# ── Step 3: Run backfill pipeline ─────────────────────────────────────────


def run_backfill(dry_run: bool = False) -> None:
    _step("Step 3/5 — Running full backfill pipeline")
    cmd = [sys.executable, "-m", "pipeline.flow", "--backfill"]
    if dry_run:
        print(f"  [dry-run] Would run: {' '.join(cmd)}")
        return

    start = time.perf_counter()
    result = subprocess.run(cmd, cwd=str(PROJECT_ROOT))
    elapsed = time.perf_counter() - start
    print(f"  Pipeline finished in {elapsed:.1f}s (exit code {result.returncode})")
    if result.returncode != 0:
        print("  ERROR: Pipeline failed. Stopping.")
        sys.exit(1)


# ── Step 4: Upload pipeline artifacts to R2 ──────────────────────────────


async def upload_pipeline_to_r2(dry_run: bool = False) -> None:
    _step("Step 4/5 — Uploading gold + silver + watermarks to R2")
    gold_dir = LOCAL_DATA / "gold"
    silver_dir = LOCAL_DATA / "silver"
    if not gold_dir.exists():
        print("  ERROR: No gold directory found after pipeline run")
        sys.exit(1)

    try:
        from storage.r2 import R2StorageBackend

        r2 = R2StorageBackend()
    except Exception as exc:
        print(f"  R2 not configured ({exc}), skipping upload")
        return

    total_files = 0
    total_bytes = 0

    # Upload gold parquet files
    parquet_files = sorted(gold_dir.glob("*.parquet"))
    if not parquet_files:
        print("  ERROR: No .parquet files in gold/")
        sys.exit(1)

    for path in parquet_files:
        r2_key = f"gold/{path.name}"
        size = path.stat().st_size
        total_bytes += size
        total_files += 1
        if dry_run:
            print(f"  [dry-run] Would upload {r2_key} ({size / 1024:.1f} KB)")
        else:
            await r2.write(r2_key, path.read_bytes())
            print(f"  Uploaded {r2_key} ({size / 1024:.1f} KB)")

    # Upload silver parquet files and watermarks so daily pipeline
    # can merge new data with backfill history instead of starting fresh.
    if silver_dir.exists():
        # Silver parquet files (e.g. silver/bcb_1.parquet)
        for path in sorted(silver_dir.glob("*.parquet")):
            r2_key = f"silver/{path.name}"
            size = path.stat().st_size
            total_bytes += size
            total_files += 1
            if dry_run:
                print(f"  [dry-run] Would upload {r2_key} ({size / 1024:.1f} KB)")
            else:
                await r2.write(r2_key, path.read_bytes())
                print(f"  Uploaded {r2_key} ({size / 1024:.1f} KB)")

        # Watermark files (e.g. silver/bcb_1/_watermark.json)
        for wm_path in sorted(silver_dir.glob("*/_watermark.json")):
            series_id = wm_path.parent.name
            r2_key = f"silver/{series_id}/_watermark.json"
            size = wm_path.stat().st_size
            total_bytes += size
            total_files += 1
            if dry_run:
                print(f"  [dry-run] Would upload {r2_key} ({size / 1024:.0f} B)")
            else:
                await r2.write(r2_key, wm_path.read_bytes())
                print(f"  Uploaded {r2_key} ({size / 1024:.0f} B)")
    else:
        print("  WARNING: No silver directory found, skipping silver upload")

    action = "Would upload" if dry_run else "Uploaded"
    print(f"  {action} {total_files} files ({total_bytes / 1024:.1f} KB total)")


# ── Step 5: Trigger API sync ─────────────────────────────────────────────


def trigger_sync(dry_run: bool = False) -> None:
    import os

    _step("Step 5/5 — Triggering Railway API sync")
    api_url = os.environ.get("RAILWAY_API_URL", "")
    secret = os.environ.get("SYNC_WEBHOOK_SECRET", "")

    if not api_url or not secret:
        print("  RAILWAY_API_URL or SYNC_WEBHOOK_SECRET not set, skipping sync trigger")
        return

    sync_url = f"{api_url}/api/internal/sync"
    if dry_run:
        print(f"  [dry-run] Would POST {sync_url}")
        return

    import urllib.request

    req = urllib.request.Request(
        sync_url,
        method="POST",
        headers={"Authorization": f"Bearer {secret}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = resp.read().decode()
            print(f"  HTTP {resp.status}: {body}")
    except Exception as exc:
        print(f"  Sync trigger failed: {exc}")


# ── Main ──────────────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Full data refresh for Veredas")
    parser.add_argument("--dry-run", action="store_true", help="Show what would happen without doing it")
    parser.add_argument("--skip-r2-cleanup", action="store_true", help="Skip deleting R2 files before upload")
    return parser.parse_args()


async def _async_main(args: argparse.Namespace) -> None:
    print("Veredas — Full Data Refresh")
    if args.dry_run:
        print("(DRY RUN — no changes will be made)")

    cleanup_local(dry_run=args.dry_run)

    if not args.skip_r2_cleanup:
        await cleanup_r2(dry_run=args.dry_run)
    else:
        print("\n  Skipping R2 cleanup (--skip-r2-cleanup)")

    run_backfill(dry_run=args.dry_run)
    await upload_pipeline_to_r2(dry_run=args.dry_run)
    trigger_sync(dry_run=args.dry_run)

    print(f"\n{'='*60}")
    print("  Done!")
    print(f"{'='*60}")


def main() -> None:
    args = parse_args()
    asyncio.run(_async_main(args))


if __name__ == "__main__":
    main()
