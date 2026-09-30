"""WebSocket streaming against real instances: fan-out across instances, cursor resume, bounded
replay, gap repair, pub/sub loss, slow consumers, heartbeats, shutdown."""

from __future__ import annotations

import asyncio
import base64
import os
import random
from typing import Any

import asyncpg
import pytest

from livefeed import bus
from livefeed.client import Event, Producer, Snapshot, StreamNotFoundError, Subscription
from livefeed.protocol import CloseCode, bus_payload, encode_event
from tests.conftest import LiveServer, wait_until


class Collector:
    """Consumes a subscription in the background and records everything it yields."""

    def __init__(self, sub: Subscription) -> None:
        self.sub = sub
        self.items: list[Event | Snapshot] = []
        self.task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        async for item in self.sub:
            self.items.append(item)

    @property
    def seqs(self) -> list[int]:
        return [i.seq for i in self.items if isinstance(i, Event)]

    async def wait_for_seq(self, seq: int, timeout: float = 10) -> None:
        await wait_until(lambda: (self.sub.cursor or 0) >= seq, timeout)

    async def stop(self) -> None:
        await self.sub.close()
        self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)


async def _publish_n(producer: Producer, stream: str, n: int, start: int = 0) -> None:
    for i in range(start, start + n):
        await producer.publish(stream, "tick", {"i": i})


def _assert_violation_free(*collectors: Collector) -> None:
    for c in collectors:
        assert c.sub.stats.duplicates_received == 0
        assert c.sub.stats.out_of_order_received == 0


async def test_snapshot_then_live_events(start_servers: Any, api_key: Any) -> None:
    (server,) = start_servers(1)
    async with Producer(server.url, api_key[1]) as producer:
        await producer.create_stream("s1")
        await _publish_n(producer, "s1", 3)
        c = Collector(Subscription(server.url, "s1"))
        await c.wait_for_seq(3)
        await _publish_n(producer, "s1", 5, start=3)
        await c.wait_for_seq(8)
    first = c.items[0]
    assert isinstance(first, Snapshot) and first.seq == 3 and first.state["events"] == 3
    assert c.seqs == [4, 5, 6, 7, 8]
    assert all(not e.replay for e in c.items[1:] if isinstance(e, Event))
    await c.stop()
    _assert_violation_free(c)


async def test_fan_out_across_three_instances(start_servers: Any, api_key: Any) -> None:
    servers = start_servers(3)
    async with Producer(servers[0].url, api_key[1]) as producer:
        await producer.create_stream("s1")
        collectors = [Collector(Subscription(servers[i % 3].url, "s1")) for i in range(9)]
        await wait_until(lambda: all(c.sub.cursor is not None for c in collectors))
        # Publish through every instance, concurrently: pub/sub order != commit order.
        producers = [Producer(s.url, api_key[1]) for s in servers]
        await asyncio.gather(
            *(_publish_n(p, "s1", 20, start=20 * k) for k, p in enumerate(producers))
        )
        for p in producers:
            await p.aclose()
    for c in collectors:
        await c.wait_for_seq(60)
        assert c.seqs[-1] == 60 and c.seqs == list(range(c.seqs[0], 61))
    assert all(len(s.hub.sessions) == 3 for s in servers)
    for c in collectors:
        await c.stop()
    _assert_violation_free(*collectors)


async def test_resume_from_cursor_replays_missed_events(start_servers: Any, api_key: Any) -> None:
    (server,) = start_servers(1)
    async with Producer(server.url, api_key[1]) as producer:
        await producer.create_stream("s1")
        await _publish_n(producer, "s1", 10)
        c = Collector(Subscription(server.url, "s1", cursor=4))
        await c.wait_for_seq(10)
        assert c.seqs == [5, 6, 7, 8, 9, 10]
        assert all(e.replay for e in c.items if isinstance(e, Event))
        c.sub.drop()  # abrupt network failure
        await _publish_n(producer, "s1", 5, start=10)
        await c.wait_for_seq(15)
    assert c.seqs == list(range(5, 16)) and c.sub.stats.reconnects >= 1
    await c.stop()
    _assert_violation_free(c)


async def test_old_or_future_cursor_gets_snapshot(start_servers: Any, api_key: Any) -> None:
    (server,) = start_servers(1, max_replay=5)
    async with Producer(server.url, api_key[1]) as producer:
        await producer.create_stream("s1")
        await _publish_n(producer, "s1", 12)
    old = Collector(Subscription(server.url, "s1", cursor=2))
    ahead = Collector(Subscription(server.url, "s1", cursor=99))
    await wait_until(lambda: bool(old.items) and bool(ahead.items))
    assert isinstance(old.items[0], Snapshot) and old.items[0].reason == "cursor_expired"
    assert isinstance(ahead.items[0], Snapshot) and ahead.items[0].reason == "cursor_ahead"
    assert old.sub.cursor == 12
    await old.stop()
    await ahead.stop()


async def test_long_replay_is_paged_and_flow_controlled(start_servers: Any, api_key: Any) -> None:
    (server,) = start_servers(1, replay_page=50, client_queue_max=20)
    async with Producer(server.url, api_key[1]) as producer:
        await producer.create_stream("s1")
        for batch in range(10):
            await producer.publish_many("s1", [{"type": "t", "data": {"i": batch}}] * 30)
        c = Collector(Subscription(server.url, "s1", cursor=0))
        await c.wait_for_seq(300)
    assert c.seqs == list(range(1, 301))  # 300 events through a 20-slot queue, no 4008
    await c.stop()
    _assert_violation_free(c)


async def test_join_while_publishing_has_no_gaps_or_duplicates(
    start_servers: Any, api_key: Any
) -> None:
    servers = start_servers(2)
    async with Producer(servers[0].url, api_key[1]) as producer:
        await producer.create_stream("s1")
        publishing = asyncio.create_task(_publish_n(producer, "s1", 150))
        collectors = []
        for i in range(15):  # join at random moments, from random cursors, mid-stream
            await asyncio.sleep(random.uniform(0, 0.05))
            cursor = random.choice([None, 0, max(0, i * 5 - 3)])
            collectors.append(Collector(Subscription(servers[i % 2].url, "s1", cursor=cursor)))
        await publishing
    for c in collectors:
        await c.wait_for_seq(150)
        assert c.seqs == list(range(c.seqs[0], 151))
        await c.stop()
    _assert_violation_free(*collectors)


async def test_lost_and_reordered_pubsub_messages_are_repaired(
    start_servers: Any, api_key: Any, pool: asyncpg.Pool, redis: Any
) -> None:
    (server,) = start_servers(1, resync_interval_s=30)  # isolate gap repair from resync
    async with Producer(server.url, api_key[1]) as producer:
        await producer.create_stream("s1")
        await _publish_n(producer, "s1", 3)
    c = Collector(Subscription(server.url, "s1"))
    await c.wait_for_seq(3)
    # Commit 4..6 directly (as if their pub/sub messages were lost), then deliver only 6's
    # message, followed by stale duplicates and an out-of-order copy of 5.
    await pool.execute(
        """INSERT INTO events (stream_id, seq, event_id, type, data)
           SELECT 's1', s, gen_random_uuid(), 'tick', '{}' FROM generate_series(4, 6) s"""
    )
    await pool.execute("UPDATE streams SET last_seq = 6 WHERE id = 's1'")
    rows = {r["seq"]: r for r in await pool.fetch("SELECT * FROM events WHERE stream_id = 's1'")}
    for seq in (6, 2, 5, 6):
        payload = bus_payload(seq, encode_event("s1", rows[seq]))
        await redis.publish(bus.channel_name("s1"), payload)
    await c.wait_for_seq(6)
    assert c.seqs == [4, 5, 6]
    await c.stop()
    _assert_violation_free(c)


async def test_resync_loop_delivers_a_lost_final_message(
    start_servers: Any, api_key: Any, pool: asyncpg.Pool
) -> None:
    (server,) = start_servers(1)
    async with Producer(server.url, api_key[1]) as producer:
        await producer.create_stream("s1")
    c = Collector(Subscription(server.url, "s1"))
    await wait_until(lambda: c.sub.cursor is not None)
    await pool.execute(
        "INSERT INTO events (stream_id, seq, event_id, type, data) "
        "VALUES ('s1', 1, gen_random_uuid(), 't', '{}')"
    )
    await pool.execute("UPDATE streams SET last_seq = 1 WHERE id = 's1'")
    await c.wait_for_seq(1, timeout=5)  # no later message will ever reveal this gap
    await c.stop()


async def test_pubsub_connection_loss_is_healed(
    start_servers: Any, api_key: Any, redis: Any
) -> None:
    servers = start_servers(2)
    async with Producer(servers[0].url, api_key[1]) as producer:
        await producer.create_stream("s1")
        c = Collector(Subscription(servers[1].url, "s1"))
        await _publish_n(producer, "s1", 5)
        await c.wait_for_seq(5)
        for _ in range(3):
            await redis.execute_command("CLIENT", "KILL", "TYPE", "pubsub")
            await _publish_n(producer, "s1", 5, start=c.sub.cursor or 0)
        await c.wait_for_seq(20)
    assert c.seqs == list(range(1, 21))
    await c.stop()
    _assert_violation_free(c)


async def _raw_ws_that_never_reads(port: int, stream: str) -> asyncio.StreamWriter:
    """A WebSocket client that completes the handshake and then never reads a byte."""
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    key = base64.b64encode(os.urandom(16)).decode()
    writer.write(
        f"GET /v1/streams/{stream}/ws HTTP/1.1\r\nHost: t\r\nUpgrade: websocket\r\n"
        f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n"
        "Sec-WebSocket-Version: 13\r\n\r\n".encode()
    )
    await writer.drain()
    assert b"101 Switching Protocols" in await reader.readuntil(b"\r\n\r\n")
    return writer


async def test_slow_consumer_is_cut_off_without_affecting_others(
    start_servers: Any, api_key: Any
) -> None:
    (server,) = start_servers(1, client_queue_max=20, frame_max_events=5)
    async with Producer(server.url, api_key[1]) as producer:
        await producer.create_stream("s1")
        stuck = await _raw_ws_that_never_reads(server.port, "s1")
        await wait_until(lambda: len(server.hub.sessions) == 1)
        (stuck_session,) = server.hub.sessions
        fast = Collector(Subscription(server.url, "s1"))
        await wait_until(lambda: fast.sub.cursor is not None)
        padding = "x" * 10_000
        for _ in range(40):  # ~20 MB
            await producer.publish_many("s1", [{"type": "t", "data": {"p": padding}}] * 50)
        await fast.wait_for_seq(2000)
    assert stuck_session.close_code == CloseCode.SLOW_CONSUMER
    assert fast.sub.stats.reconnects == 0  # one stuck client never slows the others down
    assert fast.seqs == list(range(1, 2001))
    stuck.close()
    await fast.stop()
    _assert_violation_free(fast)


async def test_client_resumes_after_server_side_close(start_servers: Any, api_key: Any) -> None:
    (server,) = start_servers(1)
    async with Producer(server.url, api_key[1]) as producer:
        await producer.create_stream("s1")
        c = Collector(Subscription(server.url, "s1"))
        await _publish_n(producer, "s1", 10)
        await c.wait_for_seq(10)
        for code in (CloseCode.SLOW_CONSUMER, CloseCode.RESYNC_REQUIRED):
            await wait_until(lambda: len(server.hub.sessions) == 1)
            (session,) = server.hub.sessions
            session.fail(code, "test")
            await _publish_n(producer, "s1", 10, start=c.sub.cursor or 0)
            await wait_until(lambda session=session: session not in server.hub.sessions)
        await c.wait_for_seq(30)
    assert c.seqs == list(range(1, 31))
    assert c.sub.stats.close_codes[CloseCode.SLOW_CONSUMER] == 1
    assert c.sub.stats.close_codes[CloseCode.RESYNC_REQUIRED] == 1
    await c.stop()
    _assert_violation_free(c)


async def test_heartbeat_and_ping(start_servers: Any, api_key: Any) -> None:
    from websockets.asyncio.client import connect

    (server,) = start_servers(1)
    async with Producer(server.url, api_key[1]) as producer:
        await producer.create_stream("s1")
    async with connect(f"{server.ws_url}/v1/streams/s1/ws") as ws:
        assert '"snapshot"' in await ws.recv()
        await ws.send('{"type":"ping"}')
        await ws.send("not json")
        frames = [await asyncio.wait_for(ws.recv(), 2) for _ in range(2)]
        assert any('"pong"' in f for f in frames) and any('"heartbeat"' in f for f in frames)


async def test_unknown_stream_closes_with_4004(start_servers: Any) -> None:
    (server,) = start_servers(1)
    with pytest.raises(StreamNotFoundError):
        async for _ in Subscription(server.url, "missing"):
            pass


async def test_idle_channels_are_reaped(start_servers: Any, api_key: Any) -> None:
    (server,) = start_servers(1)
    async with Producer(server.url, api_key[1]) as producer:
        await producer.create_stream("s1")
    c = Collector(Subscription(server.url, "s1"))
    await wait_until(lambda: "s1" in server.hub.channels and c.sub.cursor is not None)
    await c.stop()
    await wait_until(lambda: "s1" not in server.hub.channels, timeout=5)


async def test_shutdown_sends_1012_and_clients_move(start_servers: Any, api_key: Any) -> None:
    doomed, survivor = start_servers(2)
    sub = Subscription([doomed.url], "s1", backoff_base_s=0.05)
    async with Producer(survivor.url, api_key[1]) as producer:
        await producer.create_stream("s1")
        c = Collector(sub)
        await wait_until(lambda: sub.cursor is not None)
        sub.urls = [doomed.ws_url, survivor.ws_url]
        await asyncio.to_thread(doomed.stop)
        sub.urls = [survivor.ws_url]
        await _publish_n(producer, "s1", 5)
        await c.wait_for_seq(5)
    assert sub.stats.close_codes[CloseCode.SERVICE_RESTART] == 1
    await c.stop()
    _assert_violation_free(c)


def test_server_type_hint() -> None:
    assert LiveServer.__name__ == "LiveServer"
