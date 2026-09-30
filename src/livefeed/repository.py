"""All SQL. Appends take a row lock on the stream, so sequence numbers per stream are gap-free,
strictly increasing, and assigned in commit order."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, TypeAlias
from uuid import UUID

import asyncpg

from livefeed.reducers import REDUCERS

Executor: TypeAlias = asyncpg.Pool | asyncpg.Connection


class StreamNotFound(Exception):
    pass


class NotOwner(Exception):
    pass


@dataclass(frozen=True, slots=True)
class NewEvent:
    event_id: UUID
    type: str
    data: dict[str, Any]


@dataclass(frozen=True, slots=True)
class AppendResult:
    event_id: UUID
    seq: int
    duplicate: bool


async def create_stream(
    db: Executor, stream_id: str, kind: str, owner_key_id: int
) -> tuple[asyncpg.Record, bool]:
    """Create a stream. Returns (row, created); an existing row is returned unchanged."""
    row = await db.fetchrow(
        """
        INSERT INTO streams (id, kind, owner_key_id) VALUES ($1, $2, $3)
        ON CONFLICT (id) DO NOTHING
        RETURNING id, kind, owner_key_id, last_seq, state, created_at, updated_at
        """,
        stream_id,
        kind,
        owner_key_id,
    )
    if row is not None:
        return row, True
    existing = await get_stream(db, stream_id)
    assert existing is not None
    return existing, False


async def get_stream(db: Executor, stream_id: str) -> asyncpg.Record | None:
    return await db.fetchrow(
        """
        SELECT id, kind, owner_key_id, last_seq, state, created_at, updated_at
        FROM streams WHERE id = $1
        """,
        stream_id,
    )


async def append_events(
    pool: asyncpg.Pool, stream_id: str, owner_key_id: int, events: Sequence[NewEvent]
) -> tuple[list[AppendResult], list[asyncpg.Record]]:
    """Append a batch atomically. Returns per-event results (in request order) and the newly
    inserted rows (in seq order) for fan-out.

    ``SELECT ... FOR UPDATE`` on the stream row serialises appends to the same stream, so the
    next seq is simply ``last_seq + 1`` and seq order equals commit order. The reducer folds each
    event into the snapshot in the same transaction; if it rejects an event (ValueError), the
    whole batch rolls back.
    """
    async with pool.acquire() as conn, conn.transaction():
        stream = await conn.fetchrow(
            "SELECT kind, owner_key_id, last_seq, state FROM streams WHERE id = $1 FOR UPDATE",
            stream_id,
        )
        if stream is None:
            raise StreamNotFound(stream_id)
        if stream["owner_key_id"] != owner_key_id:
            raise NotOwner(stream_id)
        known: dict[UUID, int] = {
            r["event_id"]: r["seq"]
            for r in await conn.fetch(
                "SELECT event_id, seq FROM events "
                "WHERE stream_id = $1 AND event_id = ANY($2::uuid[])",
                stream_id,
                [e.event_id for e in events],
            )
        }
        reducer = REDUCERS[stream["kind"]]
        seq, state = stream["last_seq"], stream["state"]
        results: list[AppendResult] = []
        fresh: list[tuple[int, NewEvent]] = []
        for event in events:
            if event.event_id in known:  # retried publish (or repeated within this batch)
                results.append(AppendResult(event.event_id, known[event.event_id], True))
                continue
            state = reducer(state, event.type, event.data)
            seq += 1
            known[event.event_id] = seq
            fresh.append((seq, event))
            results.append(AppendResult(event.event_id, seq, False))
        rows: list[asyncpg.Record] = []
        if fresh:
            rows = await conn.fetch(
                """
                INSERT INTO events (stream_id, seq, event_id, type, data)
                SELECT $1, * FROM unnest($2::bigint[], $3::uuid[], $4::text[], $5::jsonb[])
                RETURNING seq, event_id, type, data, created_at
                """,
                stream_id,
                [s for s, _ in fresh],
                [e.event_id for _, e in fresh],
                [e.type for _, e in fresh],
                [e.data for _, e in fresh],
            )
            await conn.execute(
                "UPDATE streams SET last_seq = $2, state = $3, updated_at = now() WHERE id = $1",
                stream_id,
                seq,
                state,
            )
        return results, sorted(rows, key=lambda r: r["seq"])


async def read_events(
    db: Executor, stream_id: str, after: int, limit: int, upto: int | None = None
) -> list[asyncpg.Record]:
    """Events with ``after < seq [<= upto]`` in order: an index range scan on the primary key."""
    return await db.fetch(
        """
        SELECT seq, event_id, type, data, created_at FROM events
        WHERE stream_id = $1 AND seq > $2 AND ($3::bigint IS NULL OR seq <= $3)
        ORDER BY seq
        LIMIT $4
        """,
        stream_id,
        after,
        upto,
        limit,
    )


async def last_seq(db: Executor, stream_id: str) -> int | None:
    value = await db.fetchval("SELECT last_seq FROM streams WHERE id = $1", stream_id)
    return int(value) if value is not None else None


async def last_seqs(db: Executor, stream_ids: Sequence[str]) -> dict[str, int]:
    rows = await db.fetch(
        "SELECT id, last_seq FROM streams WHERE id = ANY($1::text[])", list(stream_ids)
    )
    return {r["id"]: r["last_seq"] for r in rows}
