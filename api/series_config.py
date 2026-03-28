"""Series display configuration — maps internal IDs to user-friendly names.

Built dynamically from DomainConfig at import time.
"""

from __future__ import annotations

from config import get_domain_config


def _build_series_display() -> dict[str, dict[str, str]]:
    """Build SERIES_DISPLAY dict from DomainConfig."""
    cfg = get_domain_config()
    result: dict[str, dict[str, str]] = {}
    for sid, series in cfg.series.items():
        result[sid] = {
            "label": series.label.en,
            "unit": series.unit,
            "source": series.source,
            "color": series.color,
            "freshness_hours": str(series.freshness_hours),
            "description": series.description.en,
        }
    return result


SERIES_DISPLAY: dict[str, dict[str, str]] = _build_series_display()


def get_display_label(series_id: str) -> str:
    """Return human-readable label for a series ID."""
    meta = SERIES_DISPLAY.get(series_id)
    return meta["label"] if meta else series_id


def get_all_series_ids() -> list[str]:
    """Return all tracked series IDs."""
    return list(SERIES_DISPLAY.keys())
