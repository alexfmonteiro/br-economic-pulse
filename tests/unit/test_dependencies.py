"""Tests for api.dependencies query functions."""

from __future__ import annotations

import io
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq

from api.dependencies import _query_parquet_bytes


def _make_parquet(rows: list[dict]) -> bytes:
    """Build an in-memory parquet from a list of dicts."""
    table = pa.table(
        {
            "date": [r["date"] for r in rows],
            "value": [r["value"] for r in rows],
            "series": [r["series"] for r in rows],
        }
    )
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


ANNUAL_ROWS = [
    {"date": date(2020, 1, 1), "value": 1.0, "series": "wb_test"},
    {"date": date(2021, 1, 1), "value": 2.0, "series": "wb_test"},
    {"date": date(2022, 1, 1), "value": 3.0, "series": "wb_test"},
    {"date": date(2023, 1, 1), "value": 4.0, "series": "wb_test"},
    {"date": date(2024, 1, 1), "value": 5.0, "series": "wb_test"},
]


def test_query_no_filter_returns_all() -> None:
    data = _make_parquet(ANNUAL_ROWS)
    result = _query_parquet_bytes(data, after=None)
    assert len(result) == 5


def test_query_filter_within_range() -> None:
    data = _make_parquet(ANNUAL_ROWS)
    result = _query_parquet_bytes(data, after="2023-01-01")
    assert len(result) == 2
    assert result[0]["value"] == 4.0
    assert result[1]["value"] == 5.0


def test_query_filter_fallback_when_empty() -> None:
    """When date filter excludes all rows, fall back to full dataset."""
    data = _make_parquet(ANNUAL_ROWS)
    result = _query_parquet_bytes(data, after="2025-03-27")
    # Should return all rows as fallback instead of empty
    assert len(result) == 5
    assert result[0]["value"] == 1.0


def test_query_empty_parquet_returns_empty() -> None:
    """Truly empty parquet returns empty list, no fallback."""
    data = _make_parquet([])
    result = _query_parquet_bytes(data, after="2025-01-01")
    assert result == []
