"""Series display configuration — maps internal IDs to user-friendly names."""

from __future__ import annotations

# Display metadata for each tracked series.
# Internal IDs (bcb_432, etc.) should never appear in the UI.
SERIES_DISPLAY: dict[str, dict[str, str]] = {
    "bcb_432": {"label": "SELIC", "unit": "% a.a.", "source": "BCB", "color": "#3b82f6"},
    "bcb_433": {"label": "IPCA", "unit": "% a.m.", "source": "BCB", "color": "#8b5cf6"},
    "bcb_1": {"label": "USD/BRL", "unit": "R$", "source": "BCB", "color": "#22c55e"},
    "ibge_pnad": {"label": "Unemployment", "unit": "%", "source": "IBGE", "color": "#f59e0b"},
    "ibge_gdp": {"label": "GDP", "unit": "R$ bi", "source": "IBGE", "color": "#06b6d4"},
    "tesouro": {"label": "Tesouro Direto", "unit": "% a.a.", "source": "Tesouro", "color": "#ec4899"},
}


def get_display_label(series_id: str) -> str:
    """Return human-readable label for a series ID."""
    meta = SERIES_DISPLAY.get(series_id)
    return meta["label"] if meta else series_id


def get_all_series_ids() -> list[str]:
    """Return all tracked series IDs."""
    return list(SERIES_DISPLAY.keys())
