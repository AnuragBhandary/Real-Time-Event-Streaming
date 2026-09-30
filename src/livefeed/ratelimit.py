"""Per-API-key token-bucket rate limiting, executed atomically inside Redis.

A Lua script makes read-refill-consume a single atomic step, so many API replicas can share one
bucket without races, and it uses Redis's clock so API hosts with skewed clocks agree.
"""

from __future__ import annotations

from dataclasses import dataclass

import redis.asyncio as aioredis

_TOKEN_BUCKET = """
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
local state = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts = tonumber(state[2])
if tokens == nil then
    tokens = burst
    ts = now
end
tokens = math.min(burst, tokens + math.max(0, now - ts) * rate)
local allowed = 0
local retry_after = 0
if tokens >= cost then
    tokens = tokens - cost
    allowed = 1
else
    retry_after = (cost - tokens) / rate
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], math.ceil(burst / rate * 1000) + 1000)
return {allowed, tostring(tokens), tostring(retry_after)}
"""


@dataclass(frozen=True, slots=True)
class RateDecision:
    allowed: bool
    remaining: float
    retry_after_s: float


class RateLimiter:
    def __init__(self, redis: aioredis.Redis, prefix: str = "lf:rl:") -> None:
        self._script = redis.register_script(_TOKEN_BUCKET)
        self._prefix = prefix

    async def acquire(
        self, key: str | int, rate_per_s: float, burst: int, cost: float = 1.0
    ) -> RateDecision:
        allowed, remaining, retry_after = await self._script(
            keys=[f"{self._prefix}{key}"], args=[rate_per_s, burst, cost]
        )
        return RateDecision(bool(int(allowed)), float(remaining), float(retry_after))
