"""Re-transform a series from bronze, resetting the watermark to force full reprocessing.

Use this to repair silver/gold data after fixing transformation logic.

Usage:
    # Preview (dry-run) — shows what would happen:
    uv run python scripts/retransform_series.py ons_electricity

    # Actually re-process:
    uv run python scripts/retransform_series.py ons_electricity --execute
"""

from __future__ import annotations

import argparse
import asyncio
import io
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(_PROJECT_ROOT / ".env.local")
load_dotenv()

import duckdb  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402


async def retransform(
    series_ids: list[str], *, dry_run: bool = True
) -> None:
    """Reset watermark and re-run transformation for the given series."""
    from config import get_domain_config
    from pipeline.feed_config import load_feed_configs
    from storage import get_storage_backend
    from tasks.transformation.task import TransformationTask

    storage = get_storage_backend()
    all_feeds = load_feed_configs()
    domain_cfg = get_domain_config()

    # Filter to requested series (include derived feeds whose bronze_source matches)
    filtered = {
        k: v
        for k, v in all_feeds.items()
        if k in series_ids or (v.bronze_source and v.bronze_source in series_ids)
    }

    missing = set(series_ids) - set(filtered)
    if missing:
        print(f"ERROR: Series not found in feed configs: {', '.join(missing)}")
        sys.exit(1)

    for sid in filtered:
        wm_key = f"silver/{sid}/_watermark.json"
        bronze_keys = sorted(await storage.list_keys(f"bronze/{sid}"))
        print(f"\n--- {sid} ---")
        print(f"  Bronze files: {len(bronze_keys)}")

        wm_exists = await storage.exists(wm_key)
        print(f"  Watermark exists: {wm_exists}")

        if dry_run:
            print("  [dry-run] Would delete watermark and re-transform")
            # Show current gold stats for comparison
            gold_key = f"gold/{sid}.parquet"
            if await storage.exists(gold_key):
                gold_bytes = await storage.read(gold_key)
                table = pq.read_table(io.BytesIO(gold_bytes))
                conn = duckdb.connect()
                conn.register("gold", table)
                stats = conn.execute(
                    "SELECT COUNT(*) AS n, "
                    "MIN(value) AS vmin, MAX(value) AS vmax, "
                    "AVG(value) AS vavg, "
                    "MIN(date) AS dmin, MAX(date) AS dmax "
                    "FROM gold WHERE value IS NOT NULL"
                ).fetchone()
                conn.close()
                assert stats is not None
                n, vmin, vmax, vavg, dmin, dmax = stats
                print(f"  Current gold: {n} rows, "
                      f"range [{vmin:.2f}, {vmax:.2f}], avg {vavg:.2f}")
                print(f"  Date range: {dmin} to {dmax}")

                # Check against typical_range
                series_cfg = domain_cfg.series.get(sid)
                if series_cfg and series_cfg.typical_range:
                    tr = series_cfg.typical_range
                    if vmax > tr.max * 1.5:
                        print(f"  WARNING: max value {vmax:.2f} is >"
                              f" 1.5x typical_range.max ({tr.max})"
                              f" — likely inflated by duplicate bronze data")
            continue

        # Execute: delete watermark AND existing silver to force clean re-processing.
        # Existing silver from a pre-fix run may have inflated values, and the
        # merge dedup could keep the old rows when _ingested_at ties.
        if wm_exists:
            await storage.delete(wm_key)
            print("  Deleted watermark")
        silver_key = f"silver/{sid}.parquet"
        if await storage.exists(silver_key):
            await storage.delete(silver_key)
            print(f"  Deleted existing silver ({silver_key})")

        # Run transformation
        task = TransformationTask(storage=storage, feed_configs={sid: filtered[sid]})
        result = await task.run()
        print(f"  Transform: success={result.success}, "
              f"rows={result.rows_processed}, "
              f"duration={result.duration_ms}ms")

        if result.errors:
            print(f"  Errors: {result.errors}")
            continue

        # Show repaired gold stats
        gold_key = f"gold/{sid}.parquet"
        gold_bytes = await storage.read(gold_key)
        table = pq.read_table(io.BytesIO(gold_bytes))
        conn = duckdb.connect()
        conn.register("gold", table)
        stats = conn.execute(
            "SELECT COUNT(*) AS n, "
            "MIN(value) AS vmin, MAX(value) AS vmax, "
            "AVG(value) AS vavg "
            "FROM gold WHERE value IS NOT NULL"
        ).fetchone()
        conn.close()
        assert stats is not None
        n, vmin, vmax, vavg = stats
        print(f"  Repaired gold: {n} rows, "
              f"range [{vmin:.2f}, {vmax:.2f}], avg {vavg:.2f}")

        # Validate against typical_range
        series_cfg = domain_cfg.series.get(sid)
        if series_cfg and series_cfg.typical_range:
            tr = series_cfg.typical_range
            if vmin >= tr.min and vmax <= tr.max:
                print(f"  Validation: PASS (within typical_range "
                      f"[{tr.min}, {tr.max}])")
            else:
                print(f"  Validation: WARN — some values outside "
                      f"typical_range [{tr.min}, {tr.max}]")

    if dry_run:
        print("\nThis was a dry run. Add --execute to actually re-transform.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Re-transform series from bronze with watermark reset.",
    )
    parser.add_argument(
        "series",
        nargs="+",
        help="Series IDs to re-transform (e.g. ons_electricity)",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually re-transform (default is dry-run)",
    )
    args = parser.parse_args()

    print(f"Series: {', '.join(args.series)}")
    print(f"Mode: {'EXECUTE' if args.execute else 'DRY RUN'}")

    asyncio.run(retransform(args.series, dry_run=not args.execute))


if __name__ == "__main__":
    main()
