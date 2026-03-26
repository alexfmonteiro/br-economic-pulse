import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        """
        # Gold Layer Explorer

        Query the gold (analytical) layer directly from R2 via DuckDB.
        Pick a series or write your own SQL.
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
def _(conn, bucket):
    # List available gold parquet files
    gold_files_df = conn.execute(f"""
        SELECT file FROM glob('r2://{bucket}/gold/*.parquet')
    """).df()

    series_list = sorted(
        f.split("/")[-1].replace(".parquet", "")
        for f in gold_files_df["file"].tolist()
    )
    return series_list,


@app.cell
def _(mo, series_list):
    series_picker = mo.ui.dropdown(
        options=series_list,
        value=series_list[0] if series_list else None,
        label="Series",
    )
    series_picker
    return (series_picker,)


@app.cell
def _(conn, bucket, mo, series_picker):
    if series_picker.value:
        url = f"r2://{bucket}/gold/{series_picker.value}.parquet"
        preview = conn.execute(f"""
            SELECT * FROM read_parquet('{url}') ORDER BY date DESC LIMIT 50
        """).df()
        row_count = conn.execute(f"""
            SELECT count(*) AS rows FROM read_parquet('{url}')
        """).fetchone()[0]

        mo.md(f"**{series_picker.value}** — {row_count:,} rows total")
        mo.ui.table(preview)
    return


@app.cell
def _(mo):
    mo.md("## Ad-Hoc SQL")
    return


@app.cell
def _(mo):
    sql_input = mo.ui.text_area(
        value="SELECT * FROM read_parquet('r2://br-economic-pulse-data/gold/bcb_432.parquet')\nORDER BY date DESC\nLIMIT 20",
        label="DuckDB SQL",
        full_width=True,
    )
    sql_input
    return (sql_input,)


@app.cell
def _(conn, mo, sql_input):
    if sql_input.value.strip():
        try:
            result = conn.execute(sql_input.value).df()
            mo.ui.table(result)
        except Exception as e:
            mo.md(f"**Error:** `{e}`")
    return


if __name__ == "__main__":
    app.run()
