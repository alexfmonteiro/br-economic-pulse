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
    bucket = os.environ.get("R2_BUCKET_NAME", "veredas-data")
    return bucket, conn


@app.cell
def _(mo):
    series_input = mo.ui.text(value="bcb_selic", label="Series ID")
    series_input
    return (series_input,)


@app.cell
def _(conn, bucket, mo, series_input):
    _sid = series_input.value.strip()
    if not _sid:
        mo.stop(True, mo.md("Enter a series ID above."))

    _layers = {}

    # Bronze: files are at bronze/{series_id}/{timestamp}.parquet
    try:
        _df = conn.execute(f"""
            SELECT 'bronze' AS layer,
                   count(*) AS rows,
                   min(date) AS min_date,
                   max(date) AS max_date,
                   count(*) - count(value) AS null_values
            FROM read_parquet('r2://{bucket}/bronze/{_sid}/*.parquet', union_by_name=true)
        """).df()
        _layers["bronze"] = _df
    except Exception:
        _layers["bronze"] = None

    # Silver and gold: files are at {layer}/{series_id}.parquet
    for _layer in ["silver", "gold"]:
        try:
            _df = conn.execute(f"""
                SELECT '{_layer}' AS layer,
                       count(*) AS rows,
                       min(date) AS min_date,
                       max(date) AS max_date,
                       count(*) - count(value) AS null_values
                FROM read_parquet('r2://{bucket}/{_layer}/{_sid}.parquet')
            """).df()
            _layers[_layer] = _df
        except Exception:
            _layers[_layer] = None

    _rows = [_v for _v in _layers.values() if _v is not None]
    if _rows:
        import pandas as pd
        _summary = pd.concat(_rows, ignore_index=True)
        mo.md("### Layer Summary")
        mo.ui.table(_summary)
    else:
        mo.md(f"No parquet files found for `{_sid}` in any layer.")
    return


@app.cell
def _(conn, bucket, mo, series_input):
    _sid = series_input.value.strip()
    if not _sid:
        mo.stop(True)

    mo.md("### Gold Layer — Latest 20 Rows")
    try:
        _gold = conn.execute(f"""
            SELECT * FROM read_parquet('r2://{bucket}/gold/{_sid}.parquet')
            ORDER BY date DESC LIMIT 20
        """).df()
        mo.ui.table(_gold)
    except Exception as e:
        mo.md(f"Could not read gold layer: `{e}`")
    return


if __name__ == "__main__":
    app.run()
