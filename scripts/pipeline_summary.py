"""Generate pipeline run summary for GitHub Actions step summary.

Reads the latest quality report and gold summaries from storage,
then outputs a markdown table showing per-series status.

Usage:
    # Output to stdout (pipe to $GITHUB_STEP_SUMMARY in CI):
    uv run python scripts/pipeline_summary.py

    # Also works locally for previewing:
    STORAGE_BACKEND=local uv run python scripts/pipeline_summary.py
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(_PROJECT_ROOT / ".env.local")
load_dotenv()

from api.models import GoldSummary, QualityReport  # noqa: E402
from storage import get_storage_backend  # noqa: E402


async def generate_summary() -> str:
    """Generate a markdown summary of the latest pipeline run."""
    storage = get_storage_backend()
    lines: list[str] = []

    # --- Quality report ---
    quality_keys = sorted(await storage.list_keys("quality/"))
    report_keys = [k for k in quality_keys if k.endswith("report.json")]

    report: QualityReport | None = None
    if report_keys:
        # Find the most recent report by timestamp (keys are random hex UUIDs)
        latest_report: QualityReport | None = None
        for key in report_keys:
            data = await storage.read(key)
            r = QualityReport.model_validate_json(data)
            if latest_report is None or r.timestamp > latest_report.timestamp:
                latest_report = r
        report = latest_report

    # --- Gold summaries ---
    gold_keys = await storage.list_keys("gold/")
    summary_keys = sorted(k for k in gold_keys if k.endswith(".summary.json"))

    summaries: dict[str, GoldSummary] = {}
    for key in summary_keys:
        data = await storage.read(key)
        s = GoldSummary.model_validate_json(data)
        summaries[s.series_id] = s

    # --- Build failed checks index ---
    failed_by_series: dict[str, list[str]] = {}
    critical_by_series: dict[str, list[str]] = {}
    if report:
        for check in report.checks:
            if check.passed:
                continue
            series_id = "unknown"
            for sid in summaries:
                if sid in check.check_name:
                    series_id = sid
                    break
            bucket = (
                critical_by_series
                if check.check_name.startswith("critical_")
                else failed_by_series
            )
            bucket.setdefault(series_id, []).append(check.check_name)

    # --- Header ---
    if report and report.critical_failures:
        lines.append(
            f"## Pipeline Run Summary — CRITICAL ({len(report.critical_failures)} failures)"
        )
    elif report and report.overall_status.value == "warning":
        lines.append("## Pipeline Run Summary — WARNING")
    else:
        lines.append("## Pipeline Run Summary — OK")

    if report:
        lines.append(f"Quality status: **{report.overall_status.value.upper()}** | "
                      f"Checks: {len(report.checks)} | "
                      f"Stage: {report.stage.value}")
    lines.append("")

    # --- Series table ---
    if summaries:
        lines.append("| Series | Rows | Value Range | Avg | Flagged |")
        lines.append("|--------|------|-------------|-----|---------|")

        for sid in sorted(summaries):
            s = summaries[sid]
            vmin = f"{s.value_min:.2f}" if s.value_min is not None else "?"
            vmax = f"{s.value_max:.2f}" if s.value_max is not None else "?"
            vavg = f"{s.value_mean:.2f}" if s.value_mean is not None else "?"

            flags: list[str] = []
            if sid in critical_by_series:
                flags.extend(f"CRITICAL: {c}" for c in critical_by_series[sid])
            if sid in failed_by_series:
                flags.extend(failed_by_series[sid])
            flag_str = ", ".join(flags) if flags else ""

            lines.append(
                f"| {sid} | {s.row_count} | [{vmin}, {vmax}] | {vavg} | {flag_str} |"
            )
    else:
        lines.append("No gold summaries found.")

    # --- Critical failures detail ---
    if report and report.critical_failures:
        lines.append("")
        lines.append("### Critical Failures")
        for msg in report.critical_failures:
            lines.append(f"- {msg}")

    lines.append("")
    return "\n".join(lines)


def main() -> None:
    output = asyncio.run(generate_summary())
    print(output)


if __name__ == "__main__":
    main()
