"""IngestionTask — config-driven ingestion with rescued data and metadata."""

from __future__ import annotations

import asyncio
import io
import json
import os
from datetime import datetime, timezone
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from api.models import FeedConfig, SeriesReconciliation, SourceFormat, TaskResult
from pipeline.feed_config import compute_schema_hash
from storage.protocol import StorageBackend
from tasks.base import BaseTask

logger = structlog.get_logger()

_YAHOO_USER_AGENT = "Mozilla/5.0 (compatible; Veredas/1.0)"


def _generate_bcb_windows(
    start_date: str, window_years: int
) -> list[tuple[str, str]]:
    """Generate date windows for BCB API's max-range constraint.

    Args:
        start_date: DD/MM/YYYY format earliest date.
        window_years: Max years per request (BCB enforces 10).

    Returns:
        List of (start, end) tuples in DD/MM/YYYY format.
    """
    day, month, year = (int(x) for x in start_date.split("/"))
    now = datetime.now(timezone.utc)
    windows: list[tuple[str, str]] = []

    current_year = year
    while current_year <= now.year:
        end_year = min(current_year + window_years, now.year + 1)
        w_start = f"{day:02d}/{month:02d}/{current_year}"
        w_end = f"{now.day:02d}/{now.month:02d}/{end_year}" if end_year > now.year else f"{day:02d}/{month:02d}/{end_year}"
        windows.append((w_start, w_end))
        current_year = end_year

    return windows


class IngestionTask(BaseTask):
    """Fetches raw data from sources defined in feed configs. Writes bronze Parquet."""

    def __init__(
        self,
        storage: StorageBackend,
        feed_configs: dict[str, FeedConfig],
        http_client: Any = None,
        run_id: str | None = None,
        backfill: bool = False,
    ) -> None:
        self._storage = storage
        self._feed_configs = feed_configs
        self._http_client = http_client
        self._run_id = run_id or "unknown"
        self._backfill = backfill

    @property
    def task_name(self) -> str:
        return "ingestion"

    async def _execute(self) -> TaskResult:
        total_rows = 0
        warnings: list[str] = []
        errors: list[str] = []
        reconciliation: list[SeriesReconciliation] = []
        owns_client = self._http_client is None
        timeout = 120.0 if self._backfill else 60.0
        client = self._http_client or httpx.AsyncClient(
            timeout=timeout, follow_redirects=True
        )

        try:
            feeds = [f for f in self._feed_configs.values() if not f.bronze_source]
            semaphore = asyncio.Semaphore(10)

            async def _fetch_one(feed: FeedConfig) -> tuple[int, SeriesReconciliation] | None:
                async with semaphore:
                    try:
                        return await self._fetch_feed(client, feed)
                    except Exception as exc:
                        msg = f"{feed.feed_id} failed: {exc}"
                        warnings.append(msg)
                        logger.warning(
                            "ingestion_source_failed",
                            source=feed.feed_id,
                            error=str(exc),
                        )
                        return None

            results = await asyncio.gather(*[_fetch_one(f) for f in feeds])
            for result in results:
                if result is not None:
                    rows, recon = result
                    total_rows += rows
                    reconciliation.append(recon)
        finally:
            if owns_client:
                await client.aclose()

        success = total_rows > 0
        if not success:
            errors.append("No data ingested from any source")

        return TaskResult(
            success=success,
            task_name=self.task_name,
            duration_ms=0.0,
            rows_processed=total_rows,
            warnings=warnings,
            errors=errors,
            series_reconciliation=reconciliation,
        )

    async def _fetch_feed(self, client: Any, feed: FeedConfig) -> tuple[int, SeriesReconciliation]:
        """Fetch data from a single feed source and write to bronze."""
        if self._backfill and feed.source.backfill_url and feed.source.backfill_window_years and feed.source.backfill_start_date:
            return await self._fetch_windowed(client, feed)

        if self._backfill and feed.source.backfill_url and feed.source.backfill_start_year and "{year}" in feed.source.backfill_url:
            return await self._fetch_yearly(client, feed)

        url = feed.source.url
        if self._backfill and feed.source.backfill_url:
            url = feed.source.backfill_url

        if feed.source.auth_method == "api_key_query" and feed.source.auth_key_env:
            api_key = os.environ.get(feed.source.auth_key_env, "")
            if not api_key:
                raise ValueError(
                    f"Missing env var {feed.source.auth_key_env} for feed {feed.feed_id}"
                )
            separator = "&" if "?" in url else "?"
            url = f"{url}{separator}api_key={api_key}"

        headers = {}
        if feed.source.format == SourceFormat.YAHOO_CHART:
            headers["User-Agent"] = _YAHOO_USER_AGENT

        response = await client.get(url, headers=headers)
        response.raise_for_status()

        raw_records = self._parse_response(response, feed)
        if not raw_records:
            raise ValueError(f"Empty response from {feed.feed_id}")

        return await self._write_bronze(raw_records, feed)

    async def _fetch_windowed(self, client: Any, feed: FeedConfig) -> tuple[int, SeriesReconciliation]:
        """Fetch data in date windows for APIs with range limits (BCB)."""
        assert feed.source.backfill_url is not None
        assert feed.source.backfill_start_date is not None
        assert feed.source.backfill_window_years is not None

        windows = _generate_bcb_windows(
            feed.source.backfill_start_date,
            feed.source.backfill_window_years,
        )

        max_retries = feed.schedule.retry_attempts
        retry_delay = feed.schedule.retry_delay_seconds
        all_records: list[dict[str, Any]] = []
        failed_windows: list[str] = []
        for w_start, w_end in windows:
            url = feed.source.backfill_url.format(start=w_start, end=w_end)
            logger.info(
                "backfill_window_fetch",
                feed_id=feed.feed_id,
                window_start=w_start,
                window_end=w_end,
            )
            last_exc: Exception | None = None
            for attempt in range(1, max_retries + 1):
                try:
                    response = await client.get(url)
                    response.raise_for_status()
                    records = self._parse_response(response, feed)
                    all_records.extend(records)
                    last_exc = None
                    break
                except Exception as exc:
                    last_exc = exc
                    if attempt < max_retries:
                        logger.info(
                            "backfill_window_retry",
                            feed_id=feed.feed_id,
                            window_start=w_start,
                            window_end=w_end,
                            attempt=attempt,
                            max_retries=max_retries,
                            error=str(exc),
                        )
                        await asyncio.sleep(retry_delay)
            if last_exc is not None:
                logger.warning(
                    "backfill_window_failed",
                    feed_id=feed.feed_id,
                    window_start=w_start,
                    window_end=w_end,
                    error=str(last_exc),
                    attempts=max_retries,
                )
                failed_windows.append(f"{w_start}-{w_end}")

        if not all_records:
            raise ValueError(f"Empty backfill response from {feed.feed_id}")

        # Deduplicate by all field values (windows may overlap at boundaries)
        seen: set[str] = set()
        unique_records: list[dict[str, Any]] = []
        for record in all_records:
            key = json.dumps(record, sort_keys=True, default=str)
            if key not in seen:
                seen.add(key)
                unique_records.append(record)

        logger.info(
            "backfill_fetch_complete",
            feed_id=feed.feed_id,
            windows=len(windows),
            total_records=len(unique_records),
            duplicates_removed=len(all_records) - len(unique_records),
        )

        return await self._write_bronze(unique_records, feed)

    async def _fetch_yearly(self, client: Any, feed: FeedConfig) -> tuple[int, SeriesReconciliation]:
        """Fetch data from yearly URL templates (e.g. ONS per-year CSVs)."""
        assert feed.source.backfill_url is not None
        assert feed.source.backfill_start_year is not None

        now = datetime.now(timezone.utc)
        max_retries = feed.schedule.retry_attempts
        retry_delay = feed.schedule.retry_delay_seconds
        all_records: list[dict[str, Any]] = []
        failed_years: list[int] = []

        for year in range(feed.source.backfill_start_year, now.year + 1):
            url = feed.source.backfill_url.format(year=year)
            logger.info("backfill_year_fetch", feed_id=feed.feed_id, year=year)
            last_exc: Exception | None = None
            for attempt in range(1, max_retries + 1):
                try:
                    response = await client.get(url)
                    response.raise_for_status()
                    records = self._parse_response(response, feed)
                    all_records.extend(records)
                    last_exc = None
                    break
                except Exception as exc:
                    last_exc = exc
                    if attempt < max_retries:
                        logger.info(
                            "backfill_year_retry",
                            feed_id=feed.feed_id,
                            year=year,
                            attempt=attempt,
                            error=str(last_exc),
                        )
                        await asyncio.sleep(retry_delay)
            if last_exc is not None:
                logger.warning(
                    "backfill_year_failed",
                    feed_id=feed.feed_id,
                    year=year,
                    error=str(last_exc),
                )
                failed_years.append(year)

        if not all_records:
            raise ValueError(f"Empty yearly backfill from {feed.feed_id}")

        # Deduplicate by all field values
        seen: set[str] = set()
        unique_records: list[dict[str, Any]] = []
        for record in all_records:
            key = json.dumps(record, sort_keys=True, default=str)
            if key not in seen:
                seen.add(key)
                unique_records.append(record)

        logger.info(
            "backfill_yearly_complete",
            feed_id=feed.feed_id,
            years_fetched=now.year + 1 - feed.source.backfill_start_year - len(failed_years),
            years_failed=len(failed_years),
            total_records=len(unique_records),
        )

        return await self._write_bronze(unique_records, feed)

    async def _write_bronze(
        self, raw_records: list[dict[str, Any]], feed: FeedConfig
    ) -> tuple[int, SeriesReconciliation]:
        """Build bronze records and write to storage."""
        bronze_records = self._build_bronze_records(raw_records, feed)
        parquet_bytes = self._to_parquet(bronze_records, feed.feed_id)

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        key = f"bronze/{feed.feed_id}/{timestamp}.parquet"
        await self._storage.write(key, parquet_bytes)

        # Count rescued records
        rescued_col = feed.processing.bronze.rescued_data_column
        rows_rescued = sum(
            1 for r in bronze_records if r.get(rescued_col) is not None
        )

        logger.info(
            "ingestion_feed_complete",
            feed_id=feed.feed_id,
            rows_fetched=len(bronze_records),
            rows_rescued=rows_rescued,
            key=key,
            backfill=self._backfill,
        )

        recon = SeriesReconciliation(
            series_id=feed.feed_id,
            rows_in=len(raw_records),
            rows_out=len(bronze_records),
            rows_rescued=rows_rescued,
        )
        return len(bronze_records), recon

    def _parse_response(
        self, response: Any, feed: FeedConfig
    ) -> list[dict[str, Any]]:
        """Parse HTTP response based on feed source format."""
        if feed.source.format == SourceFormat.CSV:
            return self._parse_csv(response.text, feed)
        if feed.source.format == SourceFormat.YAHOO_CHART:
            return self._parse_yahoo_chart(response, feed)
        return self._parse_json(response, feed)

    def _parse_json(
        self, response: Any, feed: FeedConfig
    ) -> list[dict[str, Any]]:
        """Parse JSON response, optionally extracting nested data via json_data_path."""
        data = response.json()

        if feed.source.json_data_path:
            for key in feed.source.json_data_path.split("."):
                if isinstance(data, dict):
                    data = data.get(key, [])
                elif isinstance(data, list) and key.isdigit():
                    idx = int(key)
                    data = data[idx] if idx < len(data) else []
                else:
                    return []

        if not isinstance(data, list):
            return []
        if feed.source.skip_rows > 0:
            data = data[feed.source.skip_rows :]
        return data

    def _parse_csv(self, text: str, feed: FeedConfig) -> list[dict[str, str]]:
        """Parse CSV text using feed config separator."""
        lines = text.strip().split("\n")
        if len(lines) < 2:
            return []

        header_idx = feed.source.csv_header_row
        headers = lines[header_idx].split(feed.source.csv_separator)
        records: list[dict[str, str]] = []

        for line in lines[header_idx + 1 :]:
            values = line.split(feed.source.csv_separator)
            if len(values) == len(headers):
                records.append(dict(zip(headers, values)))

        return records

    def _parse_yahoo_chart(
        self, response: Any, feed: FeedConfig
    ) -> list[dict[str, Any]]:
        """Parse Yahoo Finance v8 chart API columnar response into rows.

        Yahoo returns timestamps and quote values as parallel arrays:
        {chart: {result: [{timestamp: [...], indicators: {quote: [{close: [...], ...}]}}]}}
        """
        data = response.json()
        result = data.get("chart", {}).get("result", [])
        if not result:
            return []

        entry = result[0]
        timestamps = entry.get("timestamp", [])
        quote = entry.get("indicators", {}).get("quote", [{}])[0]

        records: list[dict[str, Any]] = []
        for i, ts in enumerate(timestamps):
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            date_str = dt.strftime("%d/%m/%Y")
            close = quote.get("close", [None] * (i + 1))[i]
            if close is not None:
                records.append({"data": date_str, "valor": str(close)})

        return records

    def _build_bronze_records(
        self,
        raw_records: list[dict[str, Any]],
        feed: FeedConfig,
    ) -> list[dict[str, str | None]]:
        """Map raw records to bronze schema with rescued data and metadata."""
        schema_hash = compute_schema_hash(feed)
        ingested_at = datetime.now(timezone.utc).isoformat()

        # Build source_field -> name mapping
        field_map: dict[str, str] = {
            f.source_field: f.name for f in feed.schema_fields
        }
        expected_source_fields = set(field_map.keys())

        bronze_records: list[dict[str, str | None]] = []

        for raw in raw_records:
            record: dict[str, str | None] = {}

            # Map known fields (all stored as strings)
            for source_field, target_name in field_map.items():
                value = raw.get(source_field)
                record[target_name] = str(value) if value is not None else None

            # Collect rescued data: extra fields not in schema
            rescued: dict[str, Any] = {}
            extra_keys = set(raw.keys()) - expected_source_fields
            for extra_key in extra_keys:
                rescued[extra_key] = raw[extra_key]

            # Check for missing required fields
            for field_def in feed.schema_fields:
                if field_def.required and record.get(field_def.name) is None:
                    rescued[f"_missing_{field_def.name}"] = True

            record[feed.processing.bronze.rescued_data_column] = (
                json.dumps(rescued, default=str) if rescued else None
            )

            # Metadata columns
            record["_ingested_at"] = ingested_at
            record["_source"] = feed.feed_id
            record["_run_id"] = self._run_id
            record["_schema_hash"] = schema_hash

            bronze_records.append(record)

        return bronze_records

    def _to_parquet(
        self, records: list[dict[str, str | None]], source: str
    ) -> bytes:
        """Convert list of dicts to Parquet bytes via PyArrow. All values as strings."""
        if not records:
            raise ValueError(f"No records to convert for {source}")

        columns: dict[str, list[str | None]] = {}
        for key in records[0]:
            columns[key] = [r.get(key) for r in records]

        # Force all columns to string type for bronze
        arrays = {k: pa.array(v, type=pa.string()) for k, v in columns.items()}
        table = pa.table(arrays)
        buf = io.BytesIO()
        pq.write_table(table, buf)
        return buf.getvalue()

    async def health_check(self) -> bool:
        """Verify storage is accessible."""
        try:
            test_key = "_health/ingestion_check"
            await self._storage.write(test_key, b"ok")
            await self._storage.delete(test_key)
            return True
        except Exception:
            return False
