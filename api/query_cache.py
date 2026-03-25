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


def _upstash_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


async def get_cached_response(question: str, language: str) -> QueryResponse | None:
    """Return cached QueryResponse for this question, or None on miss/error."""
    redis_url = os.environ.get("UPSTASH_REDIS_URL", "")
    redis_token = os.environ.get("UPSTASH_REDIS_TOKEN", "")

    if not redis_url or not redis_token:
        logger.warning("query_cache_skip_no_redis", op="get")
        return None

    try:
        generation = _get_gold_generation()
        key = _cache_key(question, language, generation)

        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{redis_url}/get/{key}",
                headers=_upstash_headers(redis_token),
                timeout=5.0,
            )
            body = resp.json()
            if "error" in body:
                logger.warning("query_cache_get_rejected", error=body["error"])
                return None
            result = body.get("result")
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
        logger.warning("query_cache_skip_no_redis", op="set")
        return

    try:
        generation = _get_gold_generation()
        key = _cache_key(question, language, generation)
        value = response.model_dump_json()

        async with httpx.AsyncClient() as client:
            resp = await client.post(
                redis_url,
                headers=_upstash_headers(redis_token),
                json=["SET", key, value, "EX", _CACHE_TTL_SECONDS],
                timeout=5.0,
            )
            body = resp.json()
            if "error" in body:
                logger.warning(
                    "query_cache_set_rejected", error=body["error"], key=key
                )
            else:
                logger.info("query_cache_set", key=key, generation=generation)
    except Exception as exc:
        logger.warning("query_cache_set_error", error=str(exc))
