import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        """
        # Layer Comparison

        Compare the same series across bronze, silver, and gold layers.
        Useful for debugging transformations, dedup, and aggregation.
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
def _(mo):
    series_input = mo.ui.text(value="bcb_432", label="Series ID")
    series_input
    return (series_input,)


@app.cell
def _(conn, bucket, mo, series_input):
    sid = series_input.value.strip()
    if not sid:
        mo.stop(True, mo.md("Enter a series ID above."))

    layers = {}
    for layer in ["bronze", "silver", "gold"]:
        try:
            pattern = f"r2://{bucket}/{layer}/{sid}*.parquet"
            df = conn.execute(f"""
                SELECT '{layer}' AS layer,
                       count(*) AS rows,
                       min(date) AS min_date,
                       max(date) AS max_date,
                       count(*) - count(value) AS null_values
                FROM read_parquet('{pattern}')
            """).df()
            layers[layer] = df
        except Exception:
            layers[layer] = None

    rows = [df for df in layers.values() if df is not None]
    if rows:
        import pandas as pd
        summary = pd.concat(rows, ignore_index=True)
        mo.md("### Layer Summary")
        mo.ui.table(summary)
    else:
        mo.md(f"No parquet files found for `{sid}` in any layer.")
    return


@app.cell
def _(conn, bucket, mo, series_input):
    sid = series_input.value.strip()
    if not sid:
        mo.stop(True)

    mo.md("### Gold Layer — Latest 20 Rows")
    try:
        gold = conn.execute(f"""
            SELECT * FROM read_parquet('r2://{bucket}/gold/{sid}.parquet')
            ORDER BY date DESC LIMIT 20
        """).df()
        mo.ui.table(gold)
    except Exception as e:
        mo.md(f"Could not read gold layer: `{e}`")
    return


if __name__ == "__main__":
    app.run()
