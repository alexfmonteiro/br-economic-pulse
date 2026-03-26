import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        """
        # Data Catalog

        Browse all series available in R2, with row counts
        per medallion layer.
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
    conn = duckdb.connect()
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute(f"""
        CREATE SECRET r2_secret (
            TYPE R2,
            KEY_ID '{os.environ.get("R2_ACCESS_KEY_ID", "")}',
            SECRET '{os.environ.get("R2_SECRET_ACCESS_KEY", "")}',
            ACCOUNT_ID '{os.environ.get("R2_ACCOUNT_ID", "")}'
        );
    """)
    bucket = os.environ.get("R2_BUCKET_NAME", "br-economic-pulse-data")
    return bucket, conn


@app.cell
def _(conn, bucket, mo):
    mo.md("### Gold Layer Files")

    gold_info = conn.execute(f"""
        SELECT
            replace(split_part(filename, '/', -1), '.parquet', '') AS series,
            count(*) AS rows,
            min(date) AS first_date,
            max(date) AS last_date,
            count(*) - count(value) AS null_values
        FROM read_parquet('r2://{bucket}/gold/*.parquet', filename=true)
        GROUP BY filename
        ORDER BY series
    """).df()

    mo.ui.table(gold_info)
    return


@app.cell
def _(conn, bucket, mo):
    mo.md("### Silver Layer Files")

    try:
        silver_info = conn.execute(f"""
            SELECT
                replace(split_part(filename, '/', -1), '.parquet', '') AS series,
                count(*) AS rows,
                min(date) AS first_date,
                max(date) AS last_date
            FROM read_parquet('r2://{bucket}/silver/*.parquet', filename=true)
            GROUP BY filename
            ORDER BY series
        """).df()
        mo.ui.table(silver_info)
    except Exception as e:
        mo.md(f"Could not read silver layer: `{e}`")
    return


if __name__ == "__main__":
    app.run()
