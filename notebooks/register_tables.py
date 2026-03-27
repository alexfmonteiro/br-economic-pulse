import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        """
        # Register Tables in Iceberg Catalog

        Registers existing gold and silver parquet files from R2 as
        Iceberg tables in the R2 Data Catalog. Idempotent — safe to re-run.

        Click **Run Registration** to register all tables.
        """
    )
    return


@app.cell
def _():
    import marimo as mo
    import duckdb
    import os
    return duckdb, mo, os


@app.cell
def _(duckdb, os):
    _token = os.environ.get("R2_CATALOG_TOKEN", "")
    _warehouse = os.environ.get("R2_CATALOG_WAREHOUSE", "")
    _catalog_uri = os.environ.get("R2_CATALOG_URI", "")
    _bucket = os.environ.get("R2_BUCKET_NAME", "veredas-data")
    _key_id = os.environ.get("R2_ACCESS_KEY_ID", "")
    _secret = os.environ.get("R2_SECRET_ACCESS_KEY", "")
    _account_id = os.environ.get("R2_ACCOUNT_ID", "")

    conn = duckdb.connect()
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute(f"CREATE SECRET (TYPE ICEBERG, TOKEN '{_token}');")
    conn.execute(f"""
        CREATE SECRET r2_storage (
            TYPE R2, KEY_ID '{_key_id}',
            SECRET '{_secret}', ACCOUNT_ID '{_account_id}'
        );
    """)
    conn.execute(f"""
        ATTACH '{_warehouse}' AS catalog (
            TYPE ICEBERG, ENDPOINT '{_catalog_uri}'
        );
    """)

    bucket = _bucket
    return bucket, conn


@app.cell
def _(mo):
    GOLD_SERIES = [
        "bcb_1", "bcb_432", "bcb_433",
        "ibge_gdp", "ibge_pnad",
        "tesouro_ipca", "tesouro_prefixado_curto", "tesouro_prefixado_longo",
    ]
    SILVER_SERIES = GOLD_SERIES + ["tesouro"]

    run_btn = mo.ui.run_button(label="Run Registration")
    run_btn
    return GOLD_SERIES, SILVER_SERIES, run_btn


@app.cell
def _(GOLD_SERIES, SILVER_SERIES, bucket, conn, mo, run_btn):
    mo.stop(not run_btn.value)

    _results = []

    for _schema in ["gold", "silver"]:
        try:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS catalog.{_schema};")
        except Exception:
            pass

    for _series in GOLD_SERIES:
        _url = f"r2://{bucket}/gold/{_series}.parquet"
        try:
            try:
                conn.execute(f"DROP TABLE catalog.gold.{_series};")
            except Exception:
                pass
            conn.execute(f"""
                CREATE TABLE catalog.gold.{_series} AS
                SELECT * FROM read_parquet('{_url}');
            """)
            _count = conn.execute(f"SELECT count(*) FROM catalog.gold.{_series}").fetchone()[0]
            _results.append({"layer": "gold", "series": _series, "rows": _count, "status": "ok"})
        except Exception as _e:
            _results.append({"layer": "gold", "series": _series, "rows": 0, "status": str(_e)[:80]})

    for _series in SILVER_SERIES:
        _url = f"r2://{bucket}/silver/{_series}.parquet"
        try:
            try:
                conn.execute(f"DROP TABLE catalog.silver.{_series};")
            except Exception:
                pass
            conn.execute(f"""
                CREATE TABLE catalog.silver.{_series} AS
                SELECT * FROM read_parquet('{_url}');
            """)
            _count = conn.execute(f"SELECT count(*) FROM catalog.silver.{_series}").fetchone()[0]
            _results.append({"layer": "silver", "series": _series, "rows": _count, "status": "ok"})
        except Exception as _e:
            _results.append({"layer": "silver", "series": _series, "rows": 0, "status": str(_e)[:80]})

    mo.md(f"**Registered {len([_r for _r in _results if _r['status'] == 'ok'])} / {len(_results)} tables.**")
    mo.ui.table(_results)
    return


@app.cell
def _(conn, mo):
    mo.md("## Current Catalog Contents")
    _tables = conn.execute("SHOW ALL TABLES;").df()
    mo.ui.table(_tables)
    return


if __name__ == "__main__":
    app.run()
