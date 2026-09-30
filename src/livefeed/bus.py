"""Redis pub/sub: best-effort, low-latency broadcast of committed events to every instance.

Pub/sub is fire-and-forget: a subscriber that is disconnected, slow, or not yet subscribed
simply misses messages, and messages from different publishers can arrive in any order. That is
acceptable because every message carries its sequence number and each instance repairs gaps from
PostgreSQL (see ``hub.Channel``). Redis gives speed; PostgreSQL gives correctness.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable, Iterable

import redis.asyncio as aioredis
from redis.exceptions import RedisError

from livefeed import metrics
from livefeed.protocol import bus_payload, parse_bus_payload

log = logging.getLogger("livefeed.bus")

CHANNEL_PREFIX = "lf:s:"


def channel_name(stream_id: str) -> str:
    return f"{CHANNEL_PREFIX}{stream_id}"


def create_redis(url: str) -> aioredis.Redis:
    return aioredis.from_url(url, decode_responses=False)


async def publish(
    redis: aioredis.Redis, stream_id: str, events: Iterable[tuple[int, bytes]]
) -> None:
    """PUBLISH each committed event, pipelined into one round trip."""
    pipe = redis.pipeline(transaction=False)
    count = 0
    for seq, event in events:
        pipe.publish(channel_name(stream_id), bus_payload(seq, event))
        count += 1
    if count:
        await pipe.execute()


MessageHandler = Callable[[str, int, bytes], None]


class Subscriber:
    """One pub/sub connection per instance, subscribed to exactly the streams that have local
    WebSocket clients. Reconnects forever; after every (re)connect it calls ``on_resync`` so the
    hub can fetch whatever was published while it was not listening."""

    def __init__(
        self,
        redis: aioredis.Redis,
        on_message: MessageHandler,
        on_resync: Callable[[], Awaitable[None]],
    ) -> None:
        self._redis = redis
        self._on_message = on_message
        self._on_resync = on_resync
        self._pubsub: aioredis.client.PubSub | None = None
        self._channels: set[str] = set()
        self._connected = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="livefeed-subscriber")
        await asyncio.wait_for(self._connected.wait(), timeout=10)

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        if self._pubsub is not None:
            await self._pubsub.aclose()

    async def subscribe(self, stream_id: str) -> None:
        name = channel_name(stream_id)
        self._channels.add(name)
        await self._connected.wait()
        if self._pubsub is not None:
            try:
                await self._pubsub.subscribe(name)
            except RedisError:
                # Connection is dropping; _run() resubscribes every channel after reconnecting.
                log.warning("subscribe failed; will resubscribe", extra={"stream": stream_id})

    async def unsubscribe(self, stream_id: str) -> None:
        name = channel_name(stream_id)
        self._channels.discard(name)
        if self._pubsub is not None:
            with contextlib.suppress(RedisError):
                await self._pubsub.unsubscribe(name)

    async def _run(self) -> None:
        failures = 0
        while True:
            pubsub = self._redis.pubsub(ignore_subscribe_messages=True)
            try:
                # A throwaway subscription keeps the connection in pub/sub mode even when no
                # stream has local clients, so get_message() always has a connection to read.
                await pubsub.subscribe("lf:control", *self._channels)
                self._pubsub = pubsub
                self._connected.set()
                if failures:
                    metrics.BUS_RECONNECTS.inc()
                    log.warning("pub/sub reconnected; resyncing channels")
                failures = 0
                await self._on_resync()
                while True:
                    message = await pubsub.get_message(timeout=1.0)
                    if message is None or message["type"] != "message":
                        continue
                    stream_id = message["channel"].decode()[len(CHANNEL_PREFIX) :]
                    seq, event = parse_bus_payload(message["data"])
                    self._on_message(stream_id, seq, event)
            except asyncio.CancelledError:
                raise
            except (RedisError, OSError) as exc:
                self._connected.clear()
                self._pubsub = None
                failures += 1
                delay = min(5.0, 0.05 * 2**failures)
                log.warning("pub/sub connection lost", extra={"error": str(exc), "retry_s": delay})
                await asyncio.sleep(delay)
            finally:
                if self._pubsub is not pubsub:
                    await pubsub.aclose()
