"""REST API: streams, publishing (ordering, idempotency, validation), history, auth, limits."""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import uuid4

import asyncpg
import httpx

from livefeed.auth import create_api_key


async def _create(http: httpx.AsyncClient, stream_id: str = "m1", kind: str = "generic") -> Any:
    return await http.post("/v1/streams", json={"id": stream_id, "kind": kind})


async def _publish(http: httpx.AsyncClient, stream_id: str, *events: dict[str, Any]) -> Any:
    return await http.post(f"/v1/streams/{stream_id}/events", json={"events": list(events)})


async def test_create_stream_is_idempotent_for_owner(
    http: httpx.AsyncClient, pool: asyncpg.Pool
) -> None:
    first = await _create(http)
    assert first.status_code == 201 and first.json()["seq"] == 0
    again = await _create(http)
    assert again.status_code == 200
    assert (await _create(http, kind="match")).status_code == 409
    _, other = await create_api_key(pool, "other", rate_per_s=100, burst=100)
    resp = await http.post("/v1/streams", json={"id": "m1"}, headers={"X-API-Key": other})
    assert resp.status_code == 409
    assert (await _create(http, stream_id="bad id!")).status_code == 422


async def test_publish_assigns_consecutive_seqs_and_updates_snapshot(
    http: httpx.AsyncClient,
) -> None:
    await _create(http, kind="match")
    resp = await _publish(
        http, "m1",
        {"type": "match_started", "data": {"teams": ["MI", "CSK"]}},
        {"type": "score", "data": {"team": "MI", "points": 4}},
    )  # fmt: skip
    body = resp.json()
    assert resp.status_code == 200 and [r["seq"] for r in body["results"]] == [1, 2]
    assert body["last_seq"] == 2
    snap = (await http.get("/v1/streams/m1")).json()
    assert snap["seq"] == 2 and snap["state"]["score"] == {"MI": 4, "CSK": 0}


async def test_publish_is_idempotent_by_event_id(
    http: httpx.AsyncClient, pool: asyncpg.Pool
) -> None:
    await _create(http)
    eid = str(uuid4())
    first = (await _publish(http, "m1", {"event_id": eid, "type": "a"})).json()
    again = (await _publish(http, "m1", {"event_id": eid, "type": "a"}, {"type": "b"})).json()
    assert first["results"][0] == {"event_id": eid, "seq": 1, "duplicate": False}
    assert again["results"] == [
        {"event_id": eid, "seq": 1, "duplicate": True},
        {"event_id": again["results"][1]["event_id"], "seq": 2, "duplicate": False},
    ]
    same_batch = await _publish(
        http, "m1", {"event_id": eid, "type": "a"}, {"event_id": eid, "type": "a"}
    )
    assert [r["duplicate"] for r in same_batch.json()["results"]] == [True, True]
    assert await pool.fetchval("SELECT count(*) FROM events") == 2


async def test_concurrent_publishers_get_gap_free_order(
    http: httpx.AsyncClient, pool: asyncpg.Pool
) -> None:
    await _create(http)
    await asyncio.gather(
        *(_publish(http, "m1", {"type": "t", "data": {"i": i}}) for i in range(60))
    )
    seqs = [r["seq"] for r in await pool.fetch("SELECT seq FROM events ORDER BY seq")]
    assert seqs == list(range(1, 61))
    assert (await http.get("/v1/streams/m1")).json()["state"]["events"] == 60


async def test_invalid_event_rolls_back_whole_batch(
    http: httpx.AsyncClient, pool: asyncpg.Pool
) -> None:
    await _create(http, kind="match")
    resp = await _publish(
        http, "m1",
        {"type": "match_started", "data": {"teams": ["A", "B"]}},
        {"type": "score", "data": {"team": "Z", "points": 1}},
    )  # fmt: skip
    assert resp.status_code == 422 and "unknown team" in resp.json()["detail"]
    assert await pool.fetchval("SELECT count(*) FROM events") == 0
    assert (await http.get("/v1/streams/m1")).json()["seq"] == 0


async def test_publish_errors(http: httpx.AsyncClient, pool: asyncpg.Pool) -> None:
    assert (await _publish(http, "nope", {"type": "a"})).status_code == 404
    await _create(http)
    _, other = await create_api_key(pool, "other", rate_per_s=100, burst=100)
    resp = await http.post(
        "/v1/streams/m1/events", json={"events": [{"type": "a"}]}, headers={"X-API-Key": other}
    )
    assert resp.status_code == 404  # not the owner: indistinguishable from missing
    assert (await http.post("/v1/streams/m1/events", json={"events": []})).status_code == 422
    too_many = {"events": [{"type": "a"}] * 501}
    assert (await http.post("/v1/streams/m1/events", json=too_many)).status_code == 422
    huge = {"events": [{"type": "a", "data": {"x": "y" * 1_100_000}}]}
    assert (await http.post("/v1/streams/m1/events", json=huge)).status_code == 413


async def test_read_events_pages(http: httpx.AsyncClient) -> None:
    await _create(http)
    await _publish(http, "m1", *({"type": "t", "data": {"i": i}} for i in range(5)))
    page = (await http.get("/v1/streams/m1/events", params={"after": 0, "limit": 3})).json()
    assert [e["seq"] for e in page["items"]] == [1, 2, 3] and page["next_after"] == 3
    rest = (await http.get("/v1/streams/m1/events", params={"after": 3, "limit": 3})).json()
    assert [e["seq"] for e in rest["items"]] == [4, 5] and rest["next_after"] is None
    assert rest["items"][0]["data"] == {"i": 3}
    assert (await http.get("/v1/streams/zzz/events")).status_code == 404
    assert (await http.get("/v1/streams/zzz")).status_code == 404


async def test_auth(http: httpx.AsyncClient, api_key: Any) -> None:
    assert (
        await http.post("/v1/streams", json={"id": "x"}, headers={"X-API-Key": ""})
    ).status_code == 401
    bearer = {"X-API-Key": "", "Authorization": f"Bearer {api_key[1]}"}
    assert (await http.post("/v1/streams", json={"id": "x"}, headers=bearer)).status_code == 201


async def test_rate_limit(http: httpx.AsyncClient, pool: asyncpg.Pool) -> None:
    _, key = await create_api_key(pool, "tiny", rate_per_s=0.5, burst=2)
    headers = {"X-API-Key": key}
    codes = [
        (await http.post("/v1/streams", json={"id": f"r{i}"}, headers=headers)).status_code
        for i in range(3)
    ]
    assert codes == [201, 201, 429]


async def test_load_shedding(http: httpx.AsyncClient) -> None:
    await _create(http)
    state = http.server.app.state.livefeed  # type: ignore[attr-defined]
    state.inflight_publishes = state.settings.max_inflight_publishes
    resp = await _publish(http, "m1", {"type": "a"})
    assert resp.status_code == 503 and resp.headers["retry-after"] == "1"
    state.inflight_publishes = 0
    assert (await _publish(http, "m1", {"type": "a"})).status_code == 200


async def test_redis_outage_degrades_gracefully(
    http: httpx.AsyncClient, pool: asyncpg.Pool
) -> None:
    import redis.asyncio as aioredis

    from livefeed.ratelimit import RateLimiter

    await _create(http)
    state = http.server.app.state.livefeed  # type: ignore[attr-defined]
    dead = aioredis.from_url("redis://127.0.0.1:1/0", socket_connect_timeout=0.2)
    state.redis, state.limiter = dead, RateLimiter(dead)
    resp = await _publish(http, "m1", {"type": "a"})
    assert resp.status_code == 200  # committed; resync delivers it to subscribers later
    assert await pool.fetchval("SELECT count(*) FROM events") == 1
    ready = await http.get("/readyz")
    assert ready.status_code == 503 and ready.json()["postgres"] == "ok"


async def test_ops_endpoints(http: httpx.AsyncClient) -> None:
    assert (await http.get("/healthz")).json() == {"status": "ok"}
    assert (await http.get("/readyz")).json() == {"postgres": "ok", "redis": "ok"}
    assert "livefeed_ws_connections" in (await http.get("/metrics")).text
    assert (await http.get("/v1/instance")).json()["instance_id"] == "test-0"
    assert "livefeed viewer" in (await http.get("/")).text
