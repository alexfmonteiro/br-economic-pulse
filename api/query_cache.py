"""LLM response cache backed by Upstash Redis.

Uses generation-based keys so the cache auto-invalidates when gold data
refreshes (new sync = new generation = new key namespace). Fails open:
all functions return None / do nothing if Redis is unavailable.
"""

from __future__ import annotations

import hashlib
import os

import httpx
import structlog

from api.dependencies import read_sync_metadata
from api.models import QueryResponse

logger = structlog.get_logger()

_CACHE_TTL_SECONDS = 86400  # 24 hours


def _get_gold_generation() -> str:
    """Return a string that changes whenever gold data is refreshed."""
    sync_info = read_sync_metadata()
    if sync_info is None or sync_info.last_sync_at is None:
        return "unknown"
    return sync_info.last_sync_at.isoformat()


def _cache_key(question: str, language: str, gold_generation: str) -> str:
    """Build a deterministic cache key from question + language + generation."""
    raw = f"{question.strip().lower()}|{language}|{gold_generation}"
    digest = hashlib.sha256(raw.encode()).hexdigest()[:24]
    return f"qcache:{digest}"


async def get_cached_response(question: str, language: str) -> QueryResponse | None:
    """Return cached QueryResponse for this question, or None on miss/error."""
    redis_url = os.environ.get("UPSTASH_REDIS_URL", "")
    redis_token = os.environ.get("UPSTASH_REDIS_TOKEN", "")

    if not redis_url or not redis_token:
        return None

    try:
        generation = _get_gold_generation()
        key = _cache_key(question, language, generation)

        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{redis_url}/get/{key}",
                headers={"Authorization": f"Bearer {redis_token}"},
                timeout=5.0,
            )
            result = resp.json().get("result")
            if result is None:
                return None
            return QueryResponse.model_validate_json(result)
    except Exception as exc:
        logger.warning("query_cache_get_error", error=str(exc))
        return None


async def set_cached_response(
    question: str, language: str, response: QueryResponse
) -> None:
    """Cache a QueryResponse in Redis with TTL. Fire-and-forget, never raises."""
    redis_url = os.environ.get("UPSTASH_REDIS_URL", "")
    redis_token = os.environ.get("UPSTASH_REDIS_TOKEN", "")

    if not redis_url or not redis_token:
        return

    try:
        generation = _get_gold_generation()
        key = _cache_key(question, language, generation)
        value = response.model_dump_json()

        async with httpx.AsyncClient() as client:
            # Use Upstash REST body format for large values
            await client.post(
                redis_url,
                headers={"Authorization": f"Bearer {redis_token}"},
                json=["SET", key, value, "EX", str(_CACHE_TTL_SECONDS)],
                timeout=5.0,
            )
        logger.debug("query_cache_set", key=key)
    except Exception as exc:
        logger.warning("query_cache_set_error", error=str(exc))
