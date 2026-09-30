"""Per-instance fan-out: channels (one per stream with local clients) and sessions (one per
WebSocket).

Ordering and gap repair
-----------------------
Pub/sub messages can be lost (subscriber reconnects, inbox overflow) and can arrive out of order
(two instances commit seq 5 and 6, but publish them in the opposite order). Each channel keeps
``last_seq``, the highest seq it has delivered, and a single pump task processes its inbox:

* ``seq <= last_seq``     → duplicate, dropped;
* ``seq == last_seq + 1`` → delivered;
* ``seq >  last_seq + 1`` → the missing range is read from PostgreSQL first. Seq N committed
  implies every seq < N is committed (appends hold the stream's row lock), so the fill always
  succeeds.

A resync loop compares each channel with ``streams.last_seq`` every second, which also repairs a
lost *final* message that no later message would reveal.

Joining without a race
----------------------
A connecting client is registered on the channel *before* its replay query runs, and live events
that arrive meanwhile are buffered. After the replay, the buffer is flushed, skipping anything the
replay already covered. The replay starts after registration, so replay + buffer is contiguous;
if a gap is ever detected anyway, the session is closed with 4009 and the client resumes from its
cursor. Disconnect-and-resume is the one recovery path for every failure.

Backpressure
------------
Channels never block on clients. Each session has a bounded queue drained by its own writer task,
which batches consecutive events into one frame. A client whose queue fills is closed with 4008
and resumes from its cursor. Server memory is bounded and fast clients are never slowed down by
slow ones.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections import defaultdict
from typing import Any, Protocol
from uuid import uuid4

import asyncpg
import redis.asyncio as aioredis

from livefeed import metrics
from livefeed import repository as repo
from livefeed.bus import Subscriber
from livefeed.config import Settings
from livefeed.protocol import (
    PONG_FRAME,
    CloseCode,
    encode_event,
    events_frame,
    heartbeat_frame,
    snapshot_frame,
)

log = logging.getLogger("livefeed.hub")

_LIVE, _REPLAY, _TEXT = 0, 1, 2
_RESYNC = object()


class Transport(Protocol):
    async def send_text(self, data: str) -> None: ...

    async def close(self, code: int = 1000, reason: str | None = None) -> None: ...


class SessionClosed(Exception):
    pass


class Session:
    def __init__(self, transport: Transport, stream_id: str, settings: Settings) -> None:
        self.id = uuid4().hex[:10]
        self.stream_id = stream_id
        self.transport = transport
        self.sent_seq: int | None = None  # last seq handed to this client's send pipeline
        self.live = False
        self.close_code: int | None = None
        self._queue: asyncio.Queue[tuple[int, Any]] = asyncio.Queue(settings.client_queue_max)
        self._frame_max = settings.frame_max_events
        self._join_buffer_max = settings.join_buffer_max
        self._buffer: list[tuple[int, bytes]] = []
        self._writer = asyncio.create_task(self._write_loop(), name=f"livefeed-writer-{self.id}")
        self._closer: asyncio.Task[None] | None = None

    @property
    def closed(self) -> bool:
        return self.close_code is not None

    # -- called by the channel (synchronous, must never block) --------------------------------

    def deliver(self, seq: int, event: bytes) -> None:
        if self.closed:
            return
        if not self.live:
            if self.sent_seq is not None and seq <= self.sent_seq:
                return
            self._buffer.append((seq, event))
            if len(self._buffer) > self._join_buffer_max:
                self.fail(CloseCode.SLOW_CONSUMER, "fell behind while joining; resume with cursor")
            return
        self._offer(seq, event)

    def _offer(self, seq: int, event: bytes) -> None:
        assert self.sent_seq is not None
        if seq <= self.sent_seq:
            return  # already covered by replay
        if seq != self.sent_seq + 1:
            self.fail(CloseCode.RESYNC_REQUIRED, "gap detected; resume with cursor")
            return
        try:
            self._queue.put_nowait((_LIVE, event))
        except asyncio.QueueFull:
            self.fail(CloseCode.SLOW_CONSUMER, "slow consumer; resume with cursor")
            return
        self.sent_seq = seq
        metrics.DELIVERED.inc()

    def go_live(self) -> None:
        """Replay finished: flush events buffered during the join, then deliver directly."""
        buffered, self._buffer = self._buffer, []
        self.live = True
        for seq, event in buffered:
            self._offer(seq, event)

    def heartbeat(self, head: int) -> None:
        if self.live and not self.closed and self.sent_seq is not None:
            self._put_text_nowait(heartbeat_frame(self.sent_seq, head))

    def pong(self) -> None:
        self._put_text_nowait(PONG_FRAME)

    def _put_text_nowait(self, frame: str) -> None:
        try:
            self._queue.put_nowait((_TEXT, frame))
        except asyncio.QueueFull:
            self.fail(CloseCode.SLOW_CONSUMER, "slow consumer; resume with cursor")

    # -- called during the join (may wait for queue space: replay is flow-controlled) ---------

    async def send_snapshot(self, seq: int, state: Any, reason: str | None) -> None:
        await self._put((_TEXT, snapshot_frame(self.stream_id, seq, state, reason)))
        self.sent_seq = seq

    async def send_replay(self, events: list[tuple[int, bytes]]) -> None:
        for seq, event in events:
            await self._put((_REPLAY, event))
            self.sent_seq = seq

    async def _put(self, item: tuple[int, Any]) -> None:
        if self.closed:
            raise SessionClosed
        await self._queue.put(item)

    # -- writing and closing ------------------------------------------------------------------

    async def _write_loop(self) -> None:
        pending: tuple[int, Any] | None = None
        try:
            while True:
                kind, payload = pending or await self._queue.get()
                pending = None
                if kind == _TEXT:
                    await self.transport.send_text(payload)
                    continue
                items = [payload]
                while len(items) < self._frame_max:
                    try:
                        nxt = self._queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    if nxt[0] != kind:
                        pending = nxt
                        break
                    items.append(nxt[1])
                await self.transport.send_text(events_frame(items, replay=kind == _REPLAY))
                metrics.FRAMES_SENT.inc()
        except asyncio.CancelledError:
            raise
        except Exception:
            # The peer went away mid-send; the endpoint's receive loop will see the disconnect.
            if self.close_code is None:
                self.close_code = 1006

    def fail(self, code: int, reason: str) -> None:
        """Close this session from synchronous code (never blocks the caller)."""
        if self.closed:
            return
        self.close_code = code
        metrics.DISCONNECTS.labels(str(code)).inc()
        self._closer = asyncio.create_task(self._close(code, reason))

    async def _close(self, code: int, reason: str) -> None:
        self._writer.cancel()
        with contextlib.suppress(Exception, asyncio.CancelledError):
            await asyncio.wait_for(self.transport.close(code, reason), timeout=2)

    async def shutdown(self) -> None:
        if self.close_code is None:
            self.close_code = 1000
        self._writer.cancel()
        await asyncio.gather(self._writer, return_exceptions=True)
        if self._closer is not None:
            await asyncio.gather(self._closer, return_exceptions=True)


class Channel:
    """Everything this instance knows about one stream: its subscribers and the last seq it has
    delivered to them."""

    def __init__(self, hub: Hub, stream_id: str, last_seq: int) -> None:
        self.hub = hub
        self.stream_id = stream_id
        self.last_seq = last_seq
        self.sessions: set[Session] = set()
        self.idle_since: float | None = None
        self._inbox: asyncio.Queue[Any] = asyncio.Queue(hub.settings.channel_inbox_max)
        self._resync_pending = False
        self.task = asyncio.create_task(self._pump(), name=f"livefeed-channel-{stream_id}")

    def on_message(self, seq: int, event: bytes) -> None:
        if seq <= self.last_seq and self._inbox.empty():
            metrics.BUS_DUPLICATES.inc()
            return
        try:
            self._inbox.put_nowait((seq, event))
        except asyncio.QueueFull:
            metrics.INBOX_OVERFLOWS.inc()  # dropped on purpose: the next seq reveals the gap

    def request_resync(self) -> None:
        if self._resync_pending:
            return
        try:
            self._inbox.put_nowait(_RESYNC)
            self._resync_pending = True
        except asyncio.QueueFull:
            pass  # a full inbox will surface the gap by itself

    async def _pump(self) -> None:
        while True:
            item = await self._inbox.get()
            try:
                if item is _RESYNC:
                    self._resync_pending = False
                    head = await repo.last_seq(self.hub.pool, self.stream_id)
                    if head is not None and head > self.last_seq:
                        await self._fill(head)
                    continue
                seq, event = item
                if seq <= self.last_seq:
                    metrics.BUS_DUPLICATES.inc()
                    continue
                if seq > self.last_seq + 1:
                    await self._fill(seq - 1)
                if seq == self.last_seq + 1:
                    self._deliver(seq, event)
                # Yield after every message so session writers drain between deliveries;
                # otherwise a burst would fill every client's queue before any of them sends.
                await asyncio.sleep(0)
            except asyncio.CancelledError:
                raise
            except Exception:
                # e.g. PostgreSQL briefly unavailable: nothing is lost, the resync loop retries.
                log.exception("channel pump error", extra={"stream": self.stream_id})
                await asyncio.sleep(0.2)

    async def _fill(self, upto: int) -> None:
        page = self.hub.settings.replay_page
        while self.last_seq < upto:
            rows = await repo.read_events(self.hub.pool, self.stream_id, self.last_seq, page, upto)
            if not rows:
                return
            for row in rows:
                if row["seq"] != self.last_seq + 1:  # pragma: no cover - impossible by design
                    raise RuntimeError(f"non-contiguous events in {self.stream_id}")
                self._deliver(row["seq"], encode_event(self.stream_id, row))
                await asyncio.sleep(0)
            metrics.GAP_REPAIRS.inc(len(rows))

    def _deliver(self, seq: int, event: bytes) -> None:
        self.last_seq = seq
        for session in list(self.sessions):
            session.deliver(seq, event)


class Hub:
    def __init__(self, settings: Settings, pool: asyncpg.Pool, redis: aioredis.Redis) -> None:
        self.settings = settings
        self.pool = pool
        self.channels: dict[str, Channel] = {}
        self.sessions: set[Session] = set()
        self.subscriber = Subscriber(redis, self._on_bus_message, self._resync_all)
        self._locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._tasks: list[asyncio.Task[None]] = []

    async def start(self) -> None:
        await self.subscriber.start()
        self._tasks = [
            asyncio.create_task(self._every(self.settings.resync_interval_s, self._resync_active)),
            asyncio.create_task(self._every(self.settings.heartbeat_interval_s, self._heartbeat)),
            asyncio.create_task(
                self._every(max(0.5, self.settings.channel_linger_s / 3), self._reap_idle)
            ),
        ]

    async def stop(self) -> None:
        for session in list(self.sessions):
            session.fail(CloseCode.SERVICE_RESTART, "server restarting; reconnect with cursor")
        await asyncio.gather(*(s.shutdown() for s in list(self.sessions)), return_exceptions=True)
        for task in self._tasks + [c.task for c in self.channels.values()]:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.subscriber.stop()

    # -- joining and leaving -------------------------------------------------------------------

    async def attach(self, session: Session, cursor: int | None) -> None:
        """Register, catch the client up from its cursor (or a snapshot), then go live.
        Raises repo.StreamNotFound or SessionClosed."""
        channel = await self._channel(session.stream_id)
        channel.sessions.add(session)
        self.sessions.add(session)
        stream = await repo.get_stream(self.pool, session.stream_id)
        assert stream is not None
        head = stream["last_seq"]
        reason: str | None = None
        if cursor is None:
            reason = "initial"
        elif cursor > head:
            reason = "cursor_ahead"
        elif head - cursor > self.settings.max_replay:
            reason = "cursor_expired"
        if reason is not None:
            await session.send_snapshot(
                head, stream["state"], None if reason == "initial" else reason
            )
            metrics.SNAPSHOTS.labels(reason).inc()
        else:
            assert cursor is not None
            session.sent_seq = after = cursor
            page, replayed = self.settings.replay_page, 0
            while True:
                rows = await repo.read_events(self.pool, session.stream_id, after, page)
                if not rows:
                    break
                await session.send_replay(
                    [(r["seq"], encode_event(session.stream_id, r)) for r in rows]
                )
                after = rows[-1]["seq"]
                replayed += len(rows)
                if len(rows) < page or replayed >= self.settings.max_replay:
                    break
            metrics.REPLAYED.inc(replayed)
        session.go_live()

    def detach(self, session: Session) -> None:
        self.sessions.discard(session)
        channel = self.channels.get(session.stream_id)
        if channel is not None:
            channel.sessions.discard(session)
            if not channel.sessions:
                channel.idle_since = time.monotonic()

    async def _channel(self, stream_id: str) -> Channel:
        channel = self.channels.get(stream_id)
        if channel is None:
            async with self._locks[stream_id]:
                channel = self.channels.get(stream_id)
                if channel is None:
                    # Subscribe first, then read the head: anything committed after the read is
                    # guaranteed to reach us through pub/sub or gap repair.
                    await self.subscriber.subscribe(stream_id)
                    head = await repo.last_seq(self.pool, stream_id)
                    if head is None:
                        await self.subscriber.unsubscribe(stream_id)
                        raise repo.StreamNotFound(stream_id)
                    channel = Channel(self, stream_id, head)
                    self.channels[stream_id] = channel
                    metrics.CHANNELS.set(len(self.channels))
        channel.idle_since = None
        return channel

    # -- bus callbacks and background loops ---------------------------------------------------

    def _on_bus_message(self, stream_id: str, seq: int, event: bytes) -> None:
        channel = self.channels.get(stream_id)
        if channel is not None:
            channel.on_message(seq, event)

    async def _resync_all(self) -> None:
        for channel in self.channels.values():
            channel.request_resync()

    async def _resync_active(self) -> None:
        active = [c for c in self.channels.values() if c.sessions]
        if not active:
            return
        heads = await repo.last_seqs(self.pool, [c.stream_id for c in active])
        for channel in active:
            if heads.get(channel.stream_id, 0) > channel.last_seq:
                channel.request_resync()

    async def _heartbeat(self) -> None:
        for channel in self.channels.values():
            for session in list(channel.sessions):
                session.heartbeat(channel.last_seq)

    async def _reap_idle(self) -> None:
        now = time.monotonic()
        for stream_id, channel in list(self.channels.items()):
            if channel.sessions or channel.idle_since is None:
                continue
            if now - channel.idle_since < self.settings.channel_linger_s:
                continue
            async with self._locks[stream_id]:
                if channel.sessions or self.channels.get(stream_id) is not channel:
                    continue
                del self.channels[stream_id]
                channel.task.cancel()
                await self.subscriber.unsubscribe(stream_id)
            metrics.CHANNELS.set(len(self.channels))

    @staticmethod
    async def _every(interval_s: float, step: Any) -> None:
        while True:
            await asyncio.sleep(interval_s)
            try:
                await step()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("hub background loop failed")
