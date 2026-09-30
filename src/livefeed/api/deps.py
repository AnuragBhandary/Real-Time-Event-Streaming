"""Shared state plus producer authentication, rate limiting and load shedding."""

from __future__ import annotations

import logging
import math
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

import asyncpg
import redis.asyncio as aioredis
from fastapi import Depends, Header, HTTPException, Request, Response, status
from redis.exceptions import RedisError

from livefeed import metrics
from livefeed.auth import Authenticator, Principal
from livefeed.config import Settings
from livefeed.hub import Hub
from livefeed.ratelimit import RateLimiter

log = logging.getLogger("livefeed.api")


@dataclass
class AppState:
    settings: Settings
    pool: asyncpg.Pool
    redis: aioredis.Redis
    hub: Hub
    authenticator: Authenticator
    limiter: RateLimiter
    inflight_publishes: int = field(default=0)


def get_state(request: Request) -> AppState:
    state: AppState = request.app.state.livefeed
    return state


async def authenticate(
    state: AppState = Depends(get_state),
    x_api_key: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
) -> Principal:
    raw = x_api_key or None
    if raw is None and authorization and authorization.lower().startswith("bearer "):
        raw = authorization[7:].strip()
    principal = await state.authenticator.authenticate(raw) if raw else None
    if principal is None:
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            detail="missing or invalid API key",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return principal


async def producer(
    response: Response,
    state: AppState = Depends(get_state),
    principal: Principal = Depends(authenticate),
) -> Principal:
    """Authenticated + within its token bucket (fails open if Redis is down)."""
    try:
        decision = await state.limiter.acquire(principal.id, principal.rate_per_s, principal.burst)
    except RedisError:
        log.warning("rate limiter unavailable; allowing request")
        return principal
    response.headers["X-RateLimit-Limit"] = str(principal.burst)
    response.headers["X-RateLimit-Remaining"] = str(math.floor(decision.remaining))
    if not decision.allowed:
        metrics.RATE_LIMITED.inc()
        raise HTTPException(
            status.HTTP_429_TOO_MANY_REQUESTS,
            detail="rate limit exceeded",
            headers={"Retry-After": str(max(1, math.ceil(decision.retry_after_s)))},
        )
    return principal


async def publish_slot(state: AppState = Depends(get_state)) -> AsyncIterator[None]:
    """Load shedding: past ``max_inflight_publishes`` concurrent appends on this instance,
    reject immediately with 503 instead of queueing without bound (which would only grow
    latency for everyone). Producers retry with backoff, ideally on another instance."""
    if state.inflight_publishes >= state.settings.max_inflight_publishes:
        metrics.PUBLISH_SHED.inc()
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="overloaded; retry with backoff",
            headers={"Retry-After": "1"},
        )
    state.inflight_publishes += 1
    try:
        yield
    finally:
        state.inflight_publishes -= 1
