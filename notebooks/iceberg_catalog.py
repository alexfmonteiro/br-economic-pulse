import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        """
        # Iceberg Data Catalog

        Connects to the R2 Data Catalog via PyIceberg. Once the cell below
        runs, the catalog appears in the **Datasources panel** (left sidebar).

        You can browse namespaces and tables there. To query a table,
        use `catalog.load_table()` below.
        """
    )
    return


@app.cell
def _():
    import marimo as mo
    import os
    return mo, os


@app.cell
def _(os):
    from pyiceberg.catalog.rest import RestCatalog

    catalog = RestCatalog(
        "veredas",
        **{
            "warehouse": os.environ.get("R2_CATALOG_WAREHOUSE", ""),
            "uri": os.environ.get("R2_CATALOG_URI", ""),
            "token": os.environ.get("R2_CATALOG_TOKEN", ""),
        },
    )
    return (catalog,)


@app.cell
def _(catalog, mo):
    mo.md("## Namespaces")

    namespaces = catalog.list_namespaces()
    mo.ui.table(
        [{"namespace": ".".join(ns)} for ns in namespaces]
    )
    return (namespaces,)


@app.cell
def _(catalog, mo, namespaces):
    mo.md("## Tables")

    all_tables = []
    for _ns in namespaces:
        for _tbl in catalog.list_tables(_ns):
            all_tables.append({
                "namespace": ".".join(_ns),
                "table": _tbl[1],
                "full_name": f"{'.'.join(_ns)}.{_tbl[1]}",
            })

    mo.ui.table(all_tables)
    return (all_tables,)


@app.cell
def _(all_tables, mo):
    table_names = [t["full_name"] for t in all_tables]
    table_picker = mo.ui.dropdown(
        options=table_names,
        value=table_names[0] if table_names else None,
        label="Table",
    )
    table_picker
    return (table_picker,)


@app.cell
def _(catalog, mo, table_picker):
    mo.stop(not table_picker.value)

    _parts = table_picker.value.split(".")
    _ns = tuple(_parts[:-1])
    _tbl_name = _parts[-1]

    iceberg_table = catalog.load_table((*_ns, _tbl_name))

    mo.md(f"### `{table_picker.value}`")
    mo.md(f"**Schema:**\n```\n{iceberg_table.schema()}\n```")
    return (iceberg_table,)


@app.cell
def _(iceberg_table, mo):
    mo.md("### Preview (first 50 rows)")

    df = iceberg_table.scan().to_pandas()
    preview = df.sort_values("date", ascending=False).head(50) if "date" in df.columns else df.head(50)
    mo.ui.table(preview)
    return


@app.cell
def _(iceberg_table, mo):
    mo.md("### Snapshots")

    snapshots = [
        {
            "snapshot_id": s.snapshot_id,
            "timestamp_ms": s.timestamp_ms,
            "operation": s.summary.operation if s.summary else "—",
        }
        for s in iceberg_table.metadata.snapshots
    ]

    if snapshots:
        mo.ui.table(snapshots)
    else:
        mo.md("_No snapshots._")
    return


if __name__ == "__main__":
    app.run()
