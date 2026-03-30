import marimo

__generated_with = "0.13.0"
app = marimo.App(width="full")


@app.cell
def _(mo):
    mo.md(
        """
        # Range Validator

        Compare actual gold data against configured `value_range` (feed config)
        and `typical_range` (domain config) for every series. Identify ranges
        that are too tight (false alarms) or too loose (missed anomalies).
        """
    )
    return


@app.cell
def _():
    import marimo as mo
    import duckdb
    import os
    import yaml
    from pathlib import Path
    return Path, duckdb, mo, os, yaml


@app.cell
def _(Path, yaml):
    # ── Load feed configs (value_range) ──
    feeds_dir = Path(__file__).resolve().parent.parent / "config" / "feeds" / "br_macro"
    feed_ranges: dict[str, dict] = {}

    for f in sorted(feeds_dir.glob("*.yaml")):
        cfg = yaml.safe_load(f.read_text())
        gold_q = (cfg.get("quality") or {}).get("gold") or {}
        feed_ranges[cfg["feed_id"]] = {
            "vr_min": gold_q.get("value_range_min"),
            "vr_max": gold_q.get("value_range_max"),
            "unit": (cfg.get("metadata") or {}).get("unit", ""),
        }

    # ── Load domain config (typical_range) ──
    domain_path = Path(__file__).resolve().parent.parent / "config" / "domains" / "br_macro.yaml"
    domain_cfg = yaml.safe_load(domain_path.read_text())
    typical_ranges: dict[str, dict] = {}

    for sid, scfg in (domain_cfg.get("series") or {}).items():
        tr = scfg.get("typical_range")
        if tr:
            typical_ranges[sid] = {"tr_min": tr["min"], "tr_max": tr["max"]}

    return domain_cfg, feed_ranges, typical_ranges


@app.cell
def _(duckdb, os):
    conn = duckdb.connect()
    conn.execute("INSTALL httpfs; LOAD httpfs;")

    bucket = os.environ.get("R2_BUCKET_NAME", "veredas-data")
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

    # Try local gold first, fall back to R2
    use_local = os.path.isdir("../data/local/gold")
    data_source = "local" if use_local else "R2"

    return bucket, conn, data_source, use_local


# ── Overview: all series at a glance ──


@app.cell
def _(bucket, conn, data_source, feed_ranges, mo, typical_ranges, use_local):
    mo.md(f"## All Series Overview\n_Data source: **{data_source}**_")

    rows = []
    for sid, fr in sorted(feed_ranges.items()):
        if use_local:
            path = f"../data/local/gold/{sid}.parquet"
            if not __import__("os").path.exists(path):
                continue
            ref = f"read_parquet('{path}')"
        else:
            ref = f"read_parquet('r2://{bucket}/gold/{sid}.parquet')"

        try:
            r = conn.execute(f"""
                SELECT
                    COUNT(*) AS n,
                    MIN(value) AS vmin, MAX(value) AS vmax,
                    AVG(value) AS vavg,
                    STDDEV(value) AS vstd,
                    PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY value) AS p1,
                    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY value) AS p99,
                    MIN(date) AS dmin, MAX(date) AS dmax
                FROM {ref}
                WHERE value IS NOT NULL
            """).fetchone()
        except Exception:
            continue

        n, vmin, vmax, vavg, vstd, p1, p99, dmin, dmax = r
        tr = typical_ranges.get(sid, {})

        rows.append({
            "series": sid,
            "unit": fr["unit"],
            "rows": n,
            "actual_min": round(vmin, 2) if vmin is not None else None,
            "actual_max": round(vmax, 2) if vmax is not None else None,
            "actual_avg": round(vavg, 2) if vavg is not None else None,
            "P1": round(p1, 2) if p1 is not None else None,
            "P99": round(p99, 2) if p99 is not None else None,
            "vr_min": fr["vr_min"],
            "vr_max": fr["vr_max"],
            "tr_min": tr.get("tr_min"),
            "tr_max": tr.get("tr_max"),
            "date_range": f"{dmin} to {dmax}",
        })

    overview_data = rows
    mo.ui.table(rows, label="All series — actual vs configured ranges")
    return (overview_data,)


# ── Flagged series ──


@app.cell
def _(mo, overview_data):
    too_tight = []
    too_loose = []
    missing = []

    for r in overview_data:
        sid = r["series"]

        # Check typical_range issues
        if r["tr_min"] is not None and r["tr_max"] is not None:
            tr_width = r["tr_max"] - r["tr_min"]
            actual_width = (r["actual_max"] or 0) - (r["actual_min"] or 0)

            # Too tight: actual data exceeds the range
            if (r["actual_min"] is not None and r["actual_min"] < r["tr_min"]) or \
               (r["actual_max"] is not None and r["actual_max"] > r["tr_max"]):
                too_tight.append({
                    "series": sid,
                    "issue": "typical_range",
                    "configured": f"[{r['tr_min']}, {r['tr_max']}]",
                    "actual": f"[{r['actual_min']}, {r['actual_max']}]",
                    "suggestion": f"[{r['P1']}, {r['P99']}]",
                })

            # Too loose: range is >5x the actual data spread
            if actual_width > 0 and tr_width > actual_width * 5:
                too_loose.append({
                    "series": sid,
                    "issue": "typical_range",
                    "configured": f"[{r['tr_min']}, {r['tr_max']}]",
                    "actual_spread": f"[{r['actual_min']}, {r['actual_max']}]",
                    "ratio": f"{tr_width / actual_width:.1f}x",
                })
        else:
            missing.append({"series": sid, "missing": "typical_range"})

        # Check value_range issues
        if r["vr_min"] is not None and r["vr_max"] is not None:
            vr_width = r["vr_max"] - r["vr_min"]
            actual_width = (r["actual_max"] or 0) - (r["actual_min"] or 0)

            if (r["actual_min"] is not None and r["actual_min"] < r["vr_min"]) or \
               (r["actual_max"] is not None and r["actual_max"] > r["vr_max"]):
                too_tight.append({
                    "series": sid,
                    "issue": "value_range",
                    "configured": f"[{r['vr_min']}, {r['vr_max']}]",
                    "actual": f"[{r['actual_min']}, {r['actual_max']}]",
                    "suggestion": f"[{r['P1']}, {r['P99']}]",
                })

            if actual_width > 0 and vr_width > actual_width * 10:
                too_loose.append({
                    "series": sid,
                    "issue": "value_range",
                    "configured": f"[{r['vr_min']}, {r['vr_max']}]",
                    "actual_spread": f"[{r['actual_min']}, {r['actual_max']}]",
                    "ratio": f"{vr_width / actual_width:.1f}x",
                })

    flagged_tight = too_tight
    flagged_loose = too_loose
    flagged_missing = missing

    md_parts = []
    if too_tight:
        md_parts.append(f"### Too Tight ({len(too_tight)} issues)\nActual data falls outside the configured range. These would trigger critical alerts.")
    if too_loose:
        md_parts.append(f"### Too Loose ({len(too_loose)} issues)\nConfigured range is >5x (or >10x) wider than the actual data spread. Anomalies could hide.")
    if missing:
        md_parts.append(f"### Missing ({len(missing)} series)\nNo `typical_range` configured.")
    if not too_tight and not too_loose:
        md_parts.append("### All ranges look good!")

    mo.md("## Flagged Issues\n\n" + "\n\n".join(md_parts))
    return flagged_loose, flagged_missing, flagged_tight


@app.cell
def _(flagged_tight, mo):
    mo.stop(not flagged_tight)
    mo.md("#### Ranges too tight — actual data exceeds configured limits")
    mo.ui.table(flagged_tight)
    return


@app.cell
def _(flagged_loose, mo):
    mo.stop(not flagged_loose)
    mo.md("#### Ranges too loose — configured range is much wider than actual data")
    mo.ui.table(flagged_loose)
    return


@app.cell
def _(flagged_missing, mo):
    mo.stop(not flagged_missing)
    mo.md("#### Missing typical_range")
    mo.ui.table(flagged_missing)
    return


# ── Per-series deep dive ──


@app.cell
def _(mo, overview_data):
    series_list = [r["series"] for r in overview_data]
    series_picker = mo.ui.dropdown(
        options=series_list,
        value=series_list[0] if series_list else None,
        label="Deep dive into series",
    )
    mo.md("## Series Deep Dive")
    series_picker
    return (series_picker,)


@app.cell
def _(bucket, conn, feed_ranges, mo, series_picker, typical_ranges, use_local):
    mo.stop(not series_picker.value)
    sid = series_picker.value

    if use_local:
        ref = f"read_parquet('../data/local/gold/{sid}.parquet')"
    else:
        ref = f"read_parquet('r2://{bucket}/gold/{sid}.parquet')"

    fr = feed_ranges.get(sid, {})
    tr = typical_ranges.get(sid, {})

    # Detailed statistics
    stats = conn.execute(f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(value) AS non_null,
            MIN(date) AS first_date,
            MAX(date) AS last_date,
            ROUND(MIN(value), 4) AS min,
            ROUND(MAX(value), 4) AS max,
            ROUND(AVG(value), 4) AS mean,
            ROUND(MEDIAN(value), 4) AS median,
            ROUND(STDDEV(value), 4) AS stddev,
            ROUND(PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY value), 4) AS P1,
            ROUND(PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY value), 4) AS P5,
            ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY value), 4) AS P25,
            ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY value), 4) AS P75,
            ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY value), 4) AS P95,
            ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY value), 4) AS P99,
        FROM {ref}
        WHERE value IS NOT NULL
    """).df()

    config_md = f"""
**value_range** (feed config): `[{fr.get('vr_min', '—')}, {fr.get('vr_max', '—')}]`
**typical_range** (domain config): `[{tr.get('tr_min', '—')}, {tr.get('tr_max', '—')}]`
**Unit**: {fr.get('unit', '—')}

**Suggested value_range** (P1–P99): `[{stats['P1'].iloc[0]}, {stats['P99'].iloc[0]}]`
**Suggested typical_range** (P5–P95): `[{stats['P5'].iloc[0]}, {stats['P95'].iloc[0]}]`
"""

    mo.md(f"### {sid}\n\n{config_md}")
    mo.ui.table(stats, label="Full statistics")
    return ref, sid


# ── Value distribution histogram ──


@app.cell
def _(conn, mo, ref, sid):
    import altair as alt

    df = conn.execute(f"""
        SELECT value FROM {ref}
        WHERE value IS NOT NULL
    """).df()

    chart = alt.Chart(df).mark_bar().encode(
        alt.X("value:Q", bin=alt.Bin(maxbins=60), title=f"{sid} — value"),
        alt.Y("count()", title="Frequency"),
    ).properties(width="container", height=300, title=f"{sid} — Value Distribution")

    mo.ui.altair_chart(chart)
    return (alt,)


# ── Time series with range bands ──


@app.cell
def _(alt, conn, feed_ranges, mo, ref, sid, typical_ranges):
    df = conn.execute(f"""
        SELECT date, value FROM {ref}
        WHERE value IS NOT NULL
        ORDER BY date
    """).df()

    base = alt.Chart(df).encode(alt.X("date:T", title="Date"))

    line = base.mark_line(color="#eab308", strokeWidth=1).encode(
        alt.Y("value:Q", title="Value"),
    )

    layers = [line]

    # Add typical_range band
    tr = typical_ranges.get(sid, {})
    if tr.get("tr_min") is not None:
        band = base.mark_rect(opacity=0.15, color="green").encode(
            y=alt.value(0), y2=alt.value(0),
        )
        rule_min = alt.Chart().mark_rule(color="green", strokeDash=[4, 4]).encode(
            y=alt.datum(tr["tr_min"]),
        )
        rule_max = alt.Chart().mark_rule(color="green", strokeDash=[4, 4]).encode(
            y=alt.datum(tr["tr_max"]),
        )
        layers.extend([rule_min, rule_max])

    # Add value_range band
    fr = feed_ranges.get(sid, {})
    if fr.get("vr_min") is not None:
        vr_rule_min = alt.Chart().mark_rule(color="red", strokeDash=[8, 4]).encode(
            y=alt.datum(fr["vr_min"]),
        )
        vr_rule_max = alt.Chart().mark_rule(color="red", strokeDash=[8, 4]).encode(
            y=alt.datum(fr["vr_max"]),
        )
        layers.extend([vr_rule_min, vr_rule_max])

    chart = alt.layer(*layers).properties(
        width="container", height=400,
        title=f"{sid} — Time series (green=typical_range, red=value_range)"
    )

    mo.ui.altair_chart(chart)
    return


# ── Recent data (last 24 months) — what the critical checks see ──


@app.cell
def _(conn, feed_ranges, mo, ref, sid, typical_ranges):
    recent = conn.execute(f"""
        SELECT
            COUNT(*) AS rows,
            ROUND(MIN(value), 4) AS min,
            ROUND(MAX(value), 4) AS max,
            ROUND(AVG(value), 4) AS mean,
            COUNT(*) FILTER (
                WHERE value < {feed_ranges.get(sid, {}).get('vr_min', 'NULL')}
                   OR value > {feed_ranges.get(sid, {}).get('vr_max', 'NULL')}
            ) AS out_of_value_range,
            COUNT(*) FILTER (
                WHERE value < {typical_ranges.get(sid, {}).get('tr_min', 'NULL')}
                   OR value > {typical_ranges.get(sid, {}).get('tr_max', 'NULL')}
            ) AS out_of_typical_range
        FROM {ref}
        WHERE value IS NOT NULL
          AND date >= CURRENT_DATE - INTERVAL '24 months'
    """).df()

    vr_oor = recent["out_of_value_range"].iloc[0]
    tr_oor = recent["out_of_typical_range"].iloc[0]
    total = recent["rows"].iloc[0]

    status = "PASS" if vr_oor == 0 and tr_oor == 0 else "FAIL"
    color = "green" if status == "PASS" else "red"

    mo.md(f"""
### Recent Data (last 24 months) — Critical Check Preview

**Status: <span style='color:{color}'>{status}</span>**

- Rows: {total}
- Out of **value_range**: {vr_oor}/{total}
- Out of **typical_range**: {tr_oor}/{total}
""")
    mo.ui.table(recent)
    return


# ── Z-score outliers ──


@app.cell
def _(conn, mo, ref, sid):
    outliers = conn.execute(f"""
        SELECT date, value, ROUND(z_score, 2) AS z_score
        FROM {ref}
        WHERE z_score IS NOT NULL AND ABS(z_score) > 2.0
        ORDER BY ABS(z_score) DESC
        LIMIT 20
    """).df()

    mo.md(f"### Z-Score Outliers (|z| > 2.0)\n{len(outliers)} outliers found")
    mo.ui.table(outliers)
    return


if __name__ == "__main__":
    app.run()
