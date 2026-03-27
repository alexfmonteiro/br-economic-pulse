import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        """
        # Gold Layer Explorer

        Query the gold (analytical) layer via the Iceberg catalog or
        directly from R2 parquet. Pick a series or write your own SQL.
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
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    conn.execute("INSTALL httpfs; LOAD httpfs;")

    bucket = os.environ.get("R2_BUCKET_NAME", "veredas-data")

    # R2 storage secret (for raw parquet fallback)
    key_id = os.environ.get("R2_ACCESS_KEY_ID", "")
    secret = os.environ.get("R2_SECRET_ACCESS_KEY", "")
    account_id = os.environ.get("R2_ACCOUNT_ID", "")
    if key_id and secret:
        conn.execute(f"""
            CREATE SECRET r2_storage (
                TYPE R2, KEY_ID '{key_id}',
                SECRET '{secret}', ACCOUNT_ID '{account_id}'
            );
        """)

    # Try attaching Iceberg catalog
    token = os.environ.get("R2_CATALOG_TOKEN", "")
    warehouse = os.environ.get("R2_CATALOG_WAREHOUSE", "")
    catalog_uri = os.environ.get("R2_CATALOG_URI", "")
    use_catalog = False

    if all([token, warehouse, catalog_uri]):
        try:
            conn.execute(f"CREATE SECRET (TYPE ICEBERG, TOKEN '{token}');")
            conn.execute(f"""
                ATTACH '{warehouse}' AS catalog (
                    TYPE ICEBERG, ENDPOINT '{catalog_uri}'
                );
            """)
            use_catalog = True
        except Exception:
            pass

    return bucket, conn, use_catalog


@app.cell
def _(bucket, conn, use_catalog):
    if use_catalog:
        tables_df = conn.execute("""
            SELECT name FROM (SHOW ALL TABLES)
            WHERE schema = 'gold'
        """).df()
        series_list = sorted(tables_df["name"].tolist())
    else:
        gold_files_df = conn.execute(f"""
            SELECT DISTINCT filename
            FROM read_parquet('r2://{bucket}/gold/*.parquet', filename=true)
        """).df()
        series_list = sorted(
            f.split("/")[-1].replace(".parquet", "")
            for f in gold_files_df["filename"].tolist()
        )
    return (series_list,)


@app.cell
def _(mo, series_list, use_catalog):
    mode = "Iceberg catalog" if use_catalog else "raw parquet"
    mo.md(f"_Source: {mode}_")

    series_picker = mo.ui.dropdown(
        options=series_list,
        value=series_list[0] if series_list else None,
        label="Series",
    )
    series_picker
    return (series_picker,)


@app.cell
def _(bucket, series_picker, use_catalog):
    # Build the table reference used by all downstream cells
    sid = series_picker.value
    if use_catalog:
        tbl_ref = f"catalog.gold.{sid}" if sid else ""
    else:
        tbl_ref = f"read_parquet('r2://{bucket}/gold/{sid}.parquet')" if sid else ""
    return (tbl_ref,)


# --- Overview ---


@app.cell
def _(conn, mo, series_picker, tbl_ref):
    mo.stop(not series_picker.value)

    stats = conn.execute(f"""
        SELECT
            count(*) AS total_rows,
            min(date) AS first_date,
            max(date) AS last_date,
            round(avg(value), 4) AS avg_value,
            round(min(value), 4) AS min_value,
            round(max(value), 4) AS max_value,
            count(*) - count(value) AS null_count
        FROM {tbl_ref}
    """).df()

    mo.md(f"## {series_picker.value}")
    mo.ui.table(stats)
    return


# --- Latest values ---


@app.cell
def _(conn, mo, tbl_ref):
    mo.md("### Latest 30 Rows")

    latest = conn.execute(f"""
        SELECT date, value, unit, mom_delta, yoy_delta, rolling_12m_avg, z_score
        FROM {tbl_ref}
        ORDER BY date DESC
        LIMIT 30
    """).df()

    mo.ui.table(latest)
    return


# --- Time series chart ---


@app.cell
def _(conn, mo, series_picker, tbl_ref):
    mo.md("### Time Series")

    chart_df = conn.execute(f"""
        SELECT date, value
        FROM {tbl_ref}
        WHERE value IS NOT NULL
        ORDER BY date
    """).df()

    mo.ui.altair_chart(
        _make_line_chart(chart_df, series_picker.value)
    )
    return


@app.cell
def _():
    import altair as alt

    def _make_line_chart(df, title):
        return (
            alt.Chart(df)
            .mark_line(strokeWidth=1.5)
            .encode(
                x=alt.X("date:T", title="Date"),
                y=alt.Y("value:Q", title="Value"),
                tooltip=["date:T", "value:Q"],
            )
            .properties(width="container", height=300, title=title)
        )

    return (_make_line_chart,)


# --- Z-score anomalies ---


@app.cell
def _(conn, mo, tbl_ref):
    mo.md("### Z-Score Anomalies (|z| > 2)")

    anomalies = conn.execute(f"""
        SELECT date, value, round(z_score, 3) AS z_score, mom_delta, yoy_delta
        FROM {tbl_ref}
        WHERE abs(z_score) > 2
        ORDER BY date DESC
        LIMIT 20
    """).df()

    if len(anomalies) == 0:
        mo.md("_No anomalies found._")
    else:
        mo.ui.table(anomalies)
    return


# --- Month-over-month changes ---


@app.cell
def _(conn, mo, tbl_ref):
    mo.md("### Largest Month-over-Month Changes")

    big_moves = conn.execute(f"""
        SELECT date, value, round(mom_delta, 4) AS mom_delta
        FROM {tbl_ref}
        WHERE mom_delta IS NOT NULL
        ORDER BY abs(mom_delta) DESC
        LIMIT 15
    """).df()

    mo.ui.table(big_moves)
    return


# --- Year-over-year comparison ---


@app.cell
def _(conn, mo, tbl_ref):
    mo.md("### Year-over-Year Comparison (Latest 24 Observations)")

    yoy = conn.execute(f"""
        SELECT date, value, round(yoy_delta, 4) AS yoy_delta,
               round(rolling_12m_avg, 4) AS rolling_12m_avg
        FROM {tbl_ref}
        WHERE yoy_delta IS NOT NULL
        ORDER BY date DESC
        LIMIT 24
    """).df()

    mo.ui.table(yoy)
    return


# --- Schema ---


@app.cell
def _(conn, mo, tbl_ref):
    mo.md("### Schema")

    schema = conn.execute(f"DESCRIBE SELECT * FROM {tbl_ref}").df()
    mo.ui.table(schema)
    return


# --- Ad-hoc SQL ---


@app.cell
def _(mo):
    mo.md("## Ad-Hoc SQL")
    return


@app.cell
def _(mo):
    sql_input = mo.ui.text_area(
        value="SELECT * FROM catalog.gold.bcb_selic\nORDER BY date DESC\nLIMIT 20",
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
