"""Tests for LLM response cache (api/query_cache.py)."""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from api.query_cache import _cache_key, get_cached_response, set_cached_response
from api.models import QueryResponse, QueryTier


# --- _cache_key tests ---


def test_cache_key_deterministic() -> None:
    """Same inputs produce the same key."""
    key1 = _cache_key("What is SELIC?", "en", "2026-03-22T06:05:12")
    key2 = _cache_key("What is SELIC?", "en", "2026-03-22T06:05:12")
    assert key1 == key2
    assert key1.startswith("qcache:")


def test_cache_key_varies_with_language() -> None:
    """Different language produces different key."""
    key_en = _cache_key("What is SELIC?", "en", "gen1")
    key_pt = _cache_key("What is SELIC?", "pt", "gen1")
    assert key_en != key_pt


def test_cache_key_varies_with_generation() -> None:
    """Different gold generation produces different key (cache invalidation)."""
    key1 = _cache_key("What is SELIC?", "en", "gen1")
    key2 = _cache_key("What is SELIC?", "en", "gen2")
    assert key1 != key2


def test_cache_key_normalizes_question() -> None:
    """Question is stripped and lowercased for normalization."""
    key1 = _cache_key("  What is SELIC?  ", "en", "gen1")
    key2 = _cache_key("what is selic?", "en", "gen1")
    assert key1 == key2


# --- Fail-open tests ---


@pytest.fixture
def _mock_gold_dir(tmp_path: Path) -> None:  # type: ignore[misc]
    """Set up gold dir with metadata for cache generation."""
    gold_dir = tmp_path / "gold"
    gold_dir.mkdir()
    metadata = {
        "last_sync_at": "2026-03-22T06:05:12Z",
        "run_id": "pipeline-20260322-060000",
        "files_synced": 1,
    }
    (gold_dir / "metadata.json").write_text(json.dumps(metadata))
    with patch.dict(os.environ, {
        "GOLD_DATA_DIR": str(gold_dir),
        "STORAGE_BACKEND": "local",
        "LOCAL_DATA_DIR": str(tmp_path),
    }):
        yield


@pytest.mark.asyncio
async def test_get_cached_response_no_redis(_mock_gold_dir: None) -> None:
    """Returns None when Redis env vars are not set (fail-open)."""
    with patch.dict(os.environ, {"UPSTASH_REDIS_URL": "", "UPSTASH_REDIS_TOKEN": ""}):
        result = await get_cached_response("test question", "en")
    assert result is None


@pytest.mark.asyncio
async def test_set_cached_response_no_redis(_mock_gold_dir: None) -> None:
    """Does not raise when Redis is unavailable (fail-open)."""
    response = QueryResponse(answer="test answer")
    with patch.dict(os.environ, {"UPSTASH_REDIS_URL": "", "UPSTASH_REDIS_TOKEN": ""}):
        # Should not raise
        await set_cached_response("test question", "en", response)


# --- Mocked Redis round-trip ---


@pytest.mark.asyncio
async def test_cache_roundtrip_with_mocked_redis(_mock_gold_dir: None) -> None:
    """Set then get returns the same QueryResponse via mocked Redis."""
    response = QueryResponse(
        answer="The SELIC rate is 14.75% per year.",
        data_points=[],
        sources=["BCB"],
        tier_used=QueryTier.FULL_LLM,
        llm_tokens_used=150,
        llm_input_tokens=100,
        llm_output_tokens=50,
    )

    # Storage for the mock
    store: dict[str, str] = {}

    async def mock_post(url: str, **kwargs: object) -> MagicMock:
        mock_resp = MagicMock()
        # Parse the URL to determine command
        if "/get/" in url:
            key = url.split("/get/")[1]
            value = store.get(key)
            mock_resp.json.return_value = {"result": value}
        else:
            # SET command via body
            body = kwargs.get("json", [])
            if isinstance(body, list) and len(body) >= 3 and body[0] == "SET":
                store[body[1]] = body[2]
            mock_resp.json.return_value = {"result": "OK"}
        return mock_resp

    mock_client = AsyncMock()
    mock_client.post = mock_post
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)

    with patch.dict(os.environ, {
        "UPSTASH_REDIS_URL": "https://fake-redis.upstash.io",
        "UPSTASH_REDIS_TOKEN": "fake-token",
    }):
        with patch("api.query_cache.httpx.AsyncClient", return_value=mock_client):
            # SET
            await set_cached_response("What is SELIC?", "en", response)
            # GET
            cached = await get_cached_response("What is SELIC?", "en")

    assert cached is not None
    assert cached.answer == response.answer
    assert cached.tier_used == QueryTier.FULL_LLM
    assert cached.llm_tokens_used == 150
