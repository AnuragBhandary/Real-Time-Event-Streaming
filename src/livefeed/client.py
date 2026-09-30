"""Async Python SDK.

Subscriber::

    async with Subscription(["ws://localhost:8080"], "match-42") as sub:
        async for item in sub:          # Snapshot first (or replay from a cursor), then Events
            print(item)

A subscription never gives up: on any disconnect (network drop, server restart, slow-consumer
close) it reconnects with its cursor, so the consumer sees every event exactly once, in order.
Its ``stats`` also count *protocol violations* the server would have to commit for that promise
to break (duplicate or out-of-order seq), which is what the load test audits.

Producer::

    async with Producer("http://localhost:8080", api_key) as producer:
        await producer.create_stream("match-42", kind="match")
        await producer.publish("match-42", "score", {"team": "MI", "points": 4})

Every event gets an ``event_id`` before the first attempt, so retries after timeouts, 429 or
503 can never publish it twice.
"""

from __future__ import annotations

import asyncio
import random
from collections import Counter
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

import httpx
import orjson
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake, InvalidURI

from livefeed.protocol import CloseCode

_FAST_RECONNECT = {CloseCode.SLOW_CONSUMER, CloseCode.RESYNC_REQUIRED, CloseCode.SERVICE_RESTART}


@dataclass(frozen=True, slots=True)
class Event:
    stream: str
    seq: int
    id: str
    type: str
    data: dict[str, Any]
    ts: float  # commit time (ms since epoch)
    replay: bool  # True if delivered from history on (re)connect rather than live


@dataclass(frozen=True, slots=True)
class Snapshot:
    stream: str
    seq: int
    state: dict[str, Any]
    reason: str | None  # None on first connect; "cursor_expired" / "cursor_ahead" on reset


@dataclass
class SubscriptionStats:
    connects: int = 0
    reconnects: int = 0
    connect_failures: int = 0
    events: int = 0
    snapshots: int = 0
    heartbeats: int = 0
    duplicates_received: int = 0  # protocol violation: server sent a seq we already had
    out_of_order_received: int = 0  # protocol violation: server skipped or reordered a seq
    close_codes: Counter[int] = field(default_factory=Counter)


class StreamNotFoundError(Exception):
    pass


class _Resync(Exception):
    pass


def _ws_base(url: str) -> str:
    url = url.rstrip("/")
    if url.startswith("http"):
        return "ws" + url[4:]
    return url


class Subscription:
    def __init__(
        self,
        urls: str | Sequence[str],
        stream_id: str,
        *,
        cursor: int | None = None,
        backoff_base_s: float = 0.1,
        backoff_max_s: float = 5.0,
        open_timeout_s: float = 10.0,
        rng: random.Random | None = None,
    ) -> None:
        self.urls = [_ws_base(u) for u in ([urls] if isinstance(urls, str) else urls)]
        self.stream_id = stream_id
        self.cursor = cursor
        self.head: int | None = None
        self.stats = SubscriptionStats()
        self._backoff_base_s = backoff_base_s
        self._backoff_max_s = backoff_max_s
        self._open_timeout_s = open_timeout_s
        self._rng = rng or random.Random()
        self._ws: ClientConnection | None = None
        self._closed = False

    async def __aenter__(self) -> Subscription:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    def __aiter__(self) -> AsyncIterator[Event | Snapshot]:
        return self.items()

    def _url(self, base: str) -> str:
        url = f"{base}/v1/streams/{self.stream_id}/ws"
        return url if self.cursor is None else f"{url}?cursor={self.cursor}"

    async def items(self) -> AsyncIterator[Event | Snapshot]:
        attempt = 0
        index = self._rng.randrange(len(self.urls))
        while not self._closed:
            base = self.urls[index % len(self.urls)]
            index += 1
            code: int | None = None
            try:
                async with connect(
                    self._url(base), open_timeout=self._open_timeout_s, max_size=2**24
                ) as ws:
                    self._ws = ws
                    self.stats.connects += 1
                    if self.stats.connects > 1:
                        self.stats.reconnects += 1
                    attempt = 0
                    async for raw in ws:
                        for item in self._handle(orjson.loads(raw)):
                            yield item
            except ConnectionClosed as exc:
                code = exc.rcvd.code if exc.rcvd is not None else 1006
                self.stats.close_codes[code] += 1
                if code == CloseCode.STREAM_NOT_FOUND:
                    raise StreamNotFoundError(self.stream_id) from exc
            except _Resync:
                pass
            except (OSError, TimeoutError, InvalidHandshake, InvalidURI):
                self.stats.connect_failures += 1
            finally:
                self._ws = None
            if self._closed:
                break
            if code in _FAST_RECONNECT:
                delay = self._rng.uniform(0, self._backoff_base_s)
            else:
                delay = self._rng.uniform(
                    0, min(self._backoff_max_s, self._backoff_base_s * 2**attempt)
                )
                attempt += 1
            await asyncio.sleep(delay)

    def _handle(self, msg: dict[str, Any]) -> list[Event | Snapshot]:
        kind = msg.get("type")
        if kind == "events":
            out: list[Event | Snapshot] = []
            for item in msg["items"]:
                seq = item["seq"]
                if self.cursor is not None and seq <= self.cursor:
                    self.stats.duplicates_received += 1
                    continue
                if self.cursor is not None and seq != self.cursor + 1:
                    self.stats.out_of_order_received += 1
                    raise _Resync  # reconnect from our cursor; the server replays the gap
                self.cursor = seq
                self.stats.events += 1
                out.append(
                    Event(
                        item["stream"],
                        seq,
                        item["id"],
                        item["type"],
                        item["data"],
                        item["ts"],
                        msg["replay"],
                    )
                )
            return out
        if kind == "snapshot":
            self.cursor = msg["seq"]
            self.stats.snapshots += 1
            return [Snapshot(msg["stream"], msg["seq"], msg["state"], msg["reason"])]
        if kind == "heartbeat":
            self.stats.heartbeats += 1
            self.head = msg["head"]
        return []

    def drop(self) -> None:
        """Abruptly kill the TCP connection (no close frame), as a network failure would.
        The subscription reconnects with its cursor. Used by the chaos tests."""
        if self._ws is not None:
            self._ws.transport.abort()

    async def close(self) -> None:
        self._closed = True
        if self._ws is not None:
            await self._ws.close()


class Producer:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        *,
        timeout_s: float = 10.0,
        max_retries: int = 6,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._http = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            headers={"X-API-Key": api_key},
            timeout=timeout_s,
            transport=transport,
        )
        self.max_retries = max_retries
        self.retries = 0

    async def __aenter__(self) -> Producer:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._http.aclose()

    async def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        for attempt in range(self.max_retries + 1):
            last = attempt == self.max_retries
            try:
                response = await self._http.request(method, path, **kwargs)
            except httpx.TransportError:
                if last:
                    raise
                self.retries += 1
                await asyncio.sleep(random.uniform(0, min(5.0, 0.05 * 2**attempt)))
                continue
            if response.status_code in (429, 502, 503, 504) and not last:
                self.retries += 1
                retry_after = float(response.headers.get("Retry-After", 0) or 0)
                await asyncio.sleep(retry_after or random.uniform(0, min(5.0, 0.05 * 2**attempt)))
                continue
            response.raise_for_status()
            return response.json()
        raise AssertionError("unreachable")

    async def create_stream(self, stream_id: str, kind: str = "generic") -> dict[str, Any]:
        result: dict[str, Any] = await self._request(
            "POST", "/v1/streams", json={"id": stream_id, "kind": kind}
        )
        return result

    async def publish_many(
        self, stream_id: str, events: Sequence[dict[str, Any]]
    ) -> dict[str, Any]:
        body = {"events": [{"event_id": str(uuid4()), **e} for e in events]}
        result: dict[str, Any] = await self._request(
            "POST", f"/v1/streams/{stream_id}/events", json=body
        )
        return result

    async def publish(
        self,
        stream_id: str,
        type_: str,
        data: dict[str, Any] | None = None,
        event_id: str | None = None,
    ) -> dict[str, Any]:
        event = {"type": type_, "data": data or {}, "event_id": event_id or str(uuid4())}
        response = await self.publish_many(stream_id, [event])
        result: dict[str, Any] = response["results"][0]
        return result

    async def snapshot(self, stream_id: str) -> dict[str, Any]:
        result: dict[str, Any] = await self._request("GET", f"/v1/streams/{stream_id}")
        return result
