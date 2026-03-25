"""Session-based rate limiting backed by Upstash Redis.

Uses a secure session token (cookie) instead of IP to prevent
circumvention via IP rotation. Falls open if Redis is unavailable.
"""

from __future__ import annotations

import hashlib
import os
import secrets

import httpx
import structlog
from fastapi import Request, Response

logger = structlog.get_logger()

_DAILY_QUERY_LIMIT = 10
_SESSION_COOKIE_NAME = "br_ep_session"
_SESSION_TOKEN_BYTES = 32


def _get_or_create_session(request: Request, response: Response) -> str:
    """Return existing session token from cookie, or generate a new one.

    Sets the cookie on the response if a new token is created.
    """
    existing = request.cookies.get(_SESSION_COOKIE_NAME)
    if existing and len(existing) >= 32:
        return existing

    token = secrets.token_urlsafe(_SESSION_TOKEN_BYTES)
    response.set_cookie(
        key=_SESSION_COOKIE_NAME,
        value=token,
        max_age=86400,  # 24 hours
        httponly=True,
        samesite="lax",
        secure=request.url.scheme == "https",
    )
    return token


def _session_key(session_token: str) -> str:
    """Derive a Redis key from the session token (hashed for privacy)."""
    hashed = hashlib.sha256(session_token.encode()).hexdigest()[:16]
    return f"ratelimit:session:{hashed}"


async def check_rate_limit(
    request: Request,
    response: Response,
    limit: int = _DAILY_QUERY_LIMIT,
) -> tuple[bool, int]:
    """Check if session is within daily rate limit.

    Returns (allowed: bool, current_count: int).
    """
    session_token = _get_or_create_session(request, response)
    redis_url = os.environ.get("UPSTASH_REDIS_URL", "")
    redis_token = os.environ.get("UPSTASH_REDIS_TOKEN", "")

    if not redis_url or not redis_token:
        logger.warning("rate_limit_skip_no_redis")
        return True, 0  # fail-open

    key = _session_key(session_token)
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{redis_url}/incr/{key}",
                headers={"Authorization": f"Bearer {redis_token}"},
                timeout=5.0,
            )
            count = int(resp.json().get("result", 0))

            if count == 1:
                await client.post(
                    f"{redis_url}/expire/{key}/86400",
                    headers={"Authorization": f"Bearer {redis_token}"},
                    timeout=5.0,
                )

            return count <= limit, count
    except Exception as exc:
        logger.warning("rate_limit_check_failed", error=str(exc))
        return True, 0  # fail-open


async def get_remaining_queries(request: Request) -> int:
    """Return how many queries the session has left today."""
    session_token = request.cookies.get(_SESSION_COOKIE_NAME)
    if not session_token:
        return _DAILY_QUERY_LIMIT

    redis_url = os.environ.get("UPSTASH_REDIS_URL", "")
    redis_token = os.environ.get("UPSTASH_REDIS_TOKEN", "")

    if not redis_url or not redis_token:
        return _DAILY_QUERY_LIMIT

    key = _session_key(session_token)
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{redis_url}/get/{key}",
                headers={"Authorization": f"Bearer {redis_token}"},
                timeout=5.0,
            )
            count = int(resp.json().get("result") or 0)
            return max(0, _DAILY_QUERY_LIMIT - count)
    except Exception:
        return _DAILY_QUERY_LIMIT
