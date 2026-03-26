import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        """
        # Data Catalog

        Browse all series available in R2, with file sizes and row counts
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
            replace(split_part(file, '/', -1), '.parquet', '') AS series,
            count(*) AS rows,
            min(date) AS first_date,
            max(date) AS last_date,
            round(count(*) - count(value), 0) AS null_values
        FROM read_parquet('r2://{bucket}/gold/*.parquet', filename=true)
        GROUP BY file
        ORDER BY series
    """).df()

    mo.ui.table(gold_info)
    return


@app.cell
def _(conn, bucket, mo):
    mo.md("### Bronze Layer Files")

    try:
        bronze_files = conn.execute(f"""
            SELECT file FROM glob('r2://{bucket}/bronze/**/*.parquet')
        """).df()
        mo.ui.table(bronze_files)
    except Exception as e:
        mo.md(f"Could not list bronze files: `{e}`")
    return


@app.cell
def _(conn, bucket, mo):
    mo.md("### Silver Layer Files")

    try:
        silver_files = conn.execute(f"""
            SELECT file FROM glob('r2://{bucket}/silver/*.parquet')
        """).df()
        mo.ui.table(silver_files)
    except Exception as e:
        mo.md(f"Could not list silver files: `{e}`")
    return


if __name__ == "__main__":
    app.run()
