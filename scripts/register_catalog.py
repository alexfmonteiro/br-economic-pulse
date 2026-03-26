"""Register gold and silver parquet files as Iceberg tables in R2 Data Catalog.

Usage:
    uv run python scripts/register_catalog.py

Discovers series dynamically from feed configs — no hardcoded list.
Idempotent: drops and recreates tables, safe to re-run.

Requires env vars:
    R2_CATALOG_TOKEN, R2_CATALOG_WAREHOUSE, R2_CATALOG_URI
    R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ACCOUNT_ID, R2_BUCKET_NAME
"""

from __future__ import annotations

import os
import sys

import duckdb
import structlog

from pipeline.feed_config import load_feed_configs

logger = structlog.get_logger()


def _discover_series() -> tuple[list[str], list[str]]:
    """Discover gold and silver series from feed configs."""
    feeds = load_feed_configs()

    gold_series: list[str] = []
    silver_series: list[str] = []

    for feed_id, feed in feeds.items():
        # All active feeds produce silver
        silver_series.append(feed_id)

        # Feeds that are NOT raw parents (i.e., have no children using them
        # as bronze_source, or are themselves leaf feeds) produce gold.
        # In practice: all feeds produce gold except the raw tesouro feed
        # which only produces silver (its derived feeds produce gold).
        has_children = any(
            f.bronze_source == feed_id for f in feeds.values()
        )
        if not has_children:
            gold_series.append(feed_id)

    return sorted(gold_series), sorted(silver_series)


def main() -> None:
    token = os.environ.get("R2_CATALOG_TOKEN", "")
    warehouse = os.environ.get("R2_CATALOG_WAREHOUSE", "")
    catalog_uri = os.environ.get("R2_CATALOG_URI", "")
    bucket = os.environ.get("R2_BUCKET_NAME", "br-economic-pulse-data")
    account_id = os.environ.get("R2_ACCOUNT_ID", "")
    key_id = os.environ.get("R2_ACCESS_KEY_ID", "")
    secret = os.environ.get("R2_SECRET_ACCESS_KEY", "")

    missing = [
        name
        for name, val in [
            ("R2_CATALOG_TOKEN", token),
            ("R2_CATALOG_WAREHOUSE", warehouse),
            ("R2_CATALOG_URI", catalog_uri),
            ("R2_ACCESS_KEY_ID", key_id),
            ("R2_SECRET_ACCESS_KEY", secret),
        ]
        if not val
    ]
    if missing:
        logger.error("missing_env_vars", vars=missing)
        sys.exit(1)

    gold_series, silver_series = _discover_series()
    logger.info(
        "discovered_series",
        gold=len(gold_series),
        silver=len(silver_series),
    )

    conn = duckdb.connect()

    logger.info("installing_extensions")
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    conn.execute("INSTALL httpfs; LOAD httpfs;")

    conn.execute(f"""
        CREATE SECRET r2_catalog_secret (
            TYPE ICEBERG,
            TOKEN '{token}'
        );
    """)
    conn.execute(f"""
        CREATE SECRET r2_storage (
            TYPE R2,
            KEY_ID '{key_id}',
            SECRET '{secret}',
            ACCOUNT_ID '{account_id}'
        );
    """)

    logger.info("attaching_catalog", warehouse=warehouse)
    conn.execute(f"""
        ATTACH '{warehouse}' AS catalog (
            TYPE ICEBERG,
            ENDPOINT '{catalog_uri}'
        );
    """)

    for schema in ["gold", "silver"]:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS catalog.{schema};")

    registered = 0
    failed = 0

    for series in gold_series:
        parquet_url = f"r2://{bucket}/gold/{series}.parquet"
        logger.info("registering", layer="gold", series=series)
        try:
            try:
                conn.execute(f"DROP TABLE catalog.gold.{series};")
            except Exception:
                pass
            conn.execute(f"""
                CREATE TABLE catalog.gold.{series} AS
                SELECT * FROM read_parquet('{parquet_url}');
            """)
            registered += 1
        except Exception as e:
            logger.error("failed", layer="gold", series=series, error=str(e))
            failed += 1

    for series in silver_series:
        parquet_url = f"r2://{bucket}/silver/{series}.parquet"
        logger.info("registering", layer="silver", series=series)
        try:
            try:
                conn.execute(f"DROP TABLE catalog.silver.{series};")
            except Exception:
                pass
            conn.execute(f"""
                CREATE TABLE catalog.silver.{series} AS
                SELECT * FROM read_parquet('{parquet_url}');
            """)
            registered += 1
        except Exception as e:
            logger.error("failed", layer="silver", series=series, error=str(e))
            failed += 1

    logger.info("registration_complete", registered=registered, failed=failed)

    tables = conn.execute("SHOW ALL TABLES;").df()
    print("\n" + tables.to_string(index=False))
    conn.close()


if __name__ == "__main__":
    main()
