"""Shared R2 connection helper for all Veredas notebooks."""

import marimo as mo
import duckdb
import os


def connect_r2() -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with R2 credentials configured.

    Reads credentials from environment variables (same ones used by
    the API and pipeline services). Returns a connection ready to
    query parquet files via ``read_parquet('r2://bucket/key')``.
    """
    conn = duckdb.connect()

    account_id = os.environ.get("R2_ACCOUNT_ID", "")
    key_id = os.environ.get("R2_ACCESS_KEY_ID", "")
    secret = os.environ.get("R2_SECRET_ACCESS_KEY", "")
    bucket = os.environ.get("R2_BUCKET_NAME", "br-economic-pulse-data")

    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute(f"""
        CREATE SECRET r2_secret (
            TYPE R2,
            KEY_ID '{key_id}',
            SECRET '{secret}',
            ACCOUNT_ID '{account_id}'
        );
    """)

    return conn


def bucket_url(key: str = "") -> str:
    """Return the r2:// URL for a given key inside the data bucket."""
    bucket = os.environ.get("R2_BUCKET_NAME", "br-economic-pulse-data")
    return f"r2://{bucket}/{key}" if key else f"r2://{bucket}"
