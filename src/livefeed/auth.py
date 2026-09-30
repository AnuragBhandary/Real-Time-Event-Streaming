"""API keys: ``lf_<prefix>_<secret>``.

The prefix is public and indexed for lookup; only sha256(secret) is stored. A fast hash is the
right choice here (unlike passwords): the secret is 256 random bits, so brute force is
infeasible and a slow KDF would only add latency to every request. Verified keys are cached
in-process for ``ttl_s``, which bounds how long a revoked key keeps working.
"""

from __future__ import annotations

import hashlib
import hmac
import re
import secrets
import time
from dataclasses import dataclass

import asyncpg

_KEY_RE = re.compile(r"^lf_([0-9a-f]{12})_([A-Za-z0-9_-]{20,128})$")


@dataclass(frozen=True, slots=True)
class Principal:
    id: int
    name: str
    rate_per_s: float
    burst: int


def hash_secret(secret: str) -> bytes:
    return hashlib.sha256(secret.encode()).digest()


def generate_key() -> str:
    return f"lf_{secrets.token_hex(6)}_{secrets.token_urlsafe(32)}"


def parse_key(raw: str) -> tuple[str, str] | None:
    match = _KEY_RE.match(raw)
    return (match.group(1), match.group(2)) if match else None


async def create_api_key(
    db: asyncpg.Pool | asyncpg.Connection,
    name: str,
    *,
    rate_per_s: float,
    burst: int,
    raw_key: str | None = None,
) -> tuple[int, str]:
    """Create a key (or register ``raw_key`` if given; idempotent). Returns (id, raw key)."""
    raw_key = raw_key or generate_key()
    parsed = parse_key(raw_key)
    if parsed is None:
        raise ValueError("API keys must look like lf_<12 hex chars>_<20+ url-safe chars>")
    prefix, secret = parsed
    row = await db.fetchrow(
        """
        INSERT INTO api_keys (name, prefix, secret_hash, rate_per_s, burst)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (prefix) DO UPDATE SET prefix = EXCLUDED.prefix
        RETURNING id
        """,
        name,
        prefix,
        hash_secret(secret),
        rate_per_s,
        burst,
    )
    assert row is not None
    return row["id"], raw_key


async def revoke_api_key(db: asyncpg.Pool | asyncpg.Connection, key_id: int) -> None:
    await db.execute("UPDATE api_keys SET revoked_at = now() WHERE id = $1", key_id)


class Authenticator:
    def __init__(self, pool: asyncpg.Pool, ttl_s: float = 30.0) -> None:
        self._pool = pool
        self._ttl_s = ttl_s
        self._cache: dict[str, tuple[float, bytes, Principal]] = {}

    async def authenticate(self, raw_key: str) -> Principal | None:
        parsed = parse_key(raw_key)
        if parsed is None:
            return None
        prefix, secret = parsed
        now = time.monotonic()
        cached = self._cache.get(prefix)
        if cached is None or cached[0] < now:
            row = await self._pool.fetchrow(
                """
                SELECT id, name, secret_hash, rate_per_s, burst FROM api_keys
                WHERE prefix = $1 AND revoked_at IS NULL
                """,
                prefix,
            )
            if row is None:
                self._cache.pop(prefix, None)
                return None
            principal = Principal(row["id"], row["name"], row["rate_per_s"], row["burst"])
            cached = (now + self._ttl_s, bytes(row["secret_hash"]), principal)
            self._cache[prefix] = cached
        _, secret_hash, principal = cached
        # Constant-time comparison: no timing side channel on the hash.
        if not hmac.compare_digest(secret_hash, hash_secret(secret)):
            return None
        return principal
