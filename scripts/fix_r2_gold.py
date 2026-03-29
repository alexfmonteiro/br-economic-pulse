"""Fix R2 gold data by uploading verified-correct local gold files.

Usage:
    # Preview what would be uploaded (dry-run):
    uv run python scripts/fix_r2_gold.py bcb_ipca derived_real_rate

    # Actually upload:
    uv run python scripts/fix_r2_gold.py bcb_ipca derived_real_rate --execute

    # Upload ALL local gold files:
    uv run python scripts/fix_r2_gold.py --all --execute

    # Validate local data against typical_range before uploading:
    uv run python scripts/fix_r2_gold.py bcb_ipca --validate --execute
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

# Ensure project root is on sys.path
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(_PROJECT_ROOT / ".env.local")
load_dotenv()

import duckdb  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

LOCAL_GOLD = _PROJECT_ROOT / "data" / "local" / "gold"


def _validate_local_gold(series_id: str) -> list[str]:
    """Validate local gold data against typical_range from domain config."""
    from config import get_domain_config

    cfg = get_domain_config()
    series_cfg = cfg.series.get(series_id)
    if not series_cfg or not series_cfg.typical_range:
        return []

    path = LOCAL_GOLD / f"{series_id}.parquet"
    if not path.exists():
        return [f"Local gold file not found: {path}"]

    tr = series_cfg.typical_range
    table = pq.read_table(path)
    conn = duckdb.connect()
    conn.register("gold", table)

    # Check only recent 24 months
    result = conn.execute(
        "SELECT COUNT(*) FILTER (WHERE value IS NOT NULL) AS total, "
        "COUNT(*) FILTER (WHERE value IS NOT NULL AND "
        f"  (value < {tr.min} OR value > {tr.max})) AS oor, "
        "MIN(value) FILTER (WHERE value IS NOT NULL) AS vmin, "
        "MAX(value) FILTER (WHERE value IS NOT NULL) AS vmax "
        "FROM gold "
        "WHERE date >= CURRENT_DATE - INTERVAL '24 months'"
    ).fetchone()
    conn.close()
    assert result is not None

    total, oor, vmin, vmax = result
    violations: list[str] = []
    if oor and oor > 0:
        violations.append(
            f"{series_id}: {oor}/{total} recent values outside "
            f"typical_range [{tr.min}, {tr.max}] "
            f"(actual range: [{vmin:.4f}, {vmax:.4f}])"
        )
    return violations


async def upload_gold_files(
    series_ids: list[str],
    *,
    dry_run: bool = True,
    validate: bool = False,
) -> None:
    """Upload local gold files to R2 for the given series."""
    from storage.r2 import R2StorageBackend

    if not LOCAL_GOLD.exists():
        print(f"ERROR: Local gold directory not found: {LOCAL_GOLD}")
        sys.exit(1)

    # Validate first if requested
    if validate:
        print("Validating local gold data against typical_range...")
        all_violations: list[str] = []
        for sid in series_ids:
            violations = _validate_local_gold(sid)
            all_violations.extend(violations)
            if violations:
                for v in violations:
                    print(f"  FAIL: {v}")
            else:
                print(f"  OK: {sid}")

        if all_violations:
            print("\nERROR: Local data failed validation. Aborting upload.")
            sys.exit(1)
        print("All validations passed.\n")

    r2 = R2StorageBackend()
    total_files = 0
    total_bytes = 0

    for sid in series_ids:
        path = LOCAL_GOLD / f"{sid}.parquet"
        if not path.exists():
            print(f"  SKIP: {path} not found locally")
            continue

        r2_key = f"gold/{sid}.parquet"
        size = path.stat().st_size
        total_bytes += size
        total_files += 1

        if dry_run:
            print(f"  [dry-run] Would upload {r2_key} ({size / 1024:.1f} KB)")
        else:
            await r2.write(r2_key, path.read_bytes())
            print(f"  Uploaded {r2_key} ({size / 1024:.1f} KB)")

    action = "Would upload" if dry_run else "Uploaded"
    print(f"\n{action} {total_files} files ({total_bytes / 1024:.1f} KB total)")

    if dry_run:
        print("\nThis was a dry run. Add --execute to actually upload.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fix R2 gold data by uploading verified-correct local gold files.",
    )
    parser.add_argument(
        "series",
        nargs="*",
        help="Series IDs to upload (e.g. bcb_ipca derived_real_rate)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Upload ALL local gold files",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually upload (default is dry-run)",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate local data against typical_range before uploading",
    )
    args = parser.parse_args()

    if args.all:
        series_ids = [p.stem for p in sorted(LOCAL_GOLD.glob("*.parquet"))]
    elif args.series:
        series_ids = args.series
    else:
        parser.error("Provide series IDs or use --all")

    print(f"Series to upload: {', '.join(series_ids)}")
    print(f"Mode: {'EXECUTE' if args.execute else 'DRY RUN'}\n")

    asyncio.run(upload_gold_files(
        series_ids,
        dry_run=not args.execute,
        validate=args.validate,
    ))


if __name__ == "__main__":
    main()
