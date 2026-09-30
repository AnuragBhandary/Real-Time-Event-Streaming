"""Failure tests with real server *processes*: SIGKILL an instance while events are flowing,
restart it, and check that every subscriber still saw every event exactly once, in order."""

from __future__ import annotations

import asyncio
import os
import signal
import socket
import subprocess
import sys
from collections.abc import Iterator
from typing import Any

import httpx
import pytest

from livefeed.client import Producer, Subscription
from tests.conftest import DATABASE_URL, REDIS_URL, wait_until
from tests.test_streaming import Collector

pytestmark = pytest.mark.chaos


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class Instance:
    def __init__(self, index: int) -> None:
        self.index = index
        self.port = _free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        self.proc: subprocess.Popen[bytes] | None = None

    def start(self) -> None:
        env = {
            **os.environ,
            "LIVEFEED_DATABASE_URL": DATABASE_URL,
            "LIVEFEED_REDIS_URL": REDIS_URL,
            "LIVEFEED_INSTANCE_ID": f"chaos-{self.index}",
            "LIVEFEED_RESYNC_INTERVAL_S": "0.2",
            "LIVEFEED_LOG_LEVEL": "WARNING",
        }
        self.proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "livefeed",
                "serve",
                "--host",
                "127.0.0.1",
                "--port",
                str(self.port),
            ],
            env=env,
            stdout=subprocess.DEVNULL,
        )

    async def wait_ready(self) -> None:
        async def ready() -> bool:
            try:
                async with httpx.AsyncClient() as client:
                    return (await client.get(f"{self.url}/readyz")).status_code == 200
            except httpx.HTTPError:
                return False

        await wait_until(ready, timeout=20, interval=0.1)

    def kill(self) -> None:
        assert self.proc is not None
        self.proc.send_signal(signal.SIGKILL)
        self.proc.wait(timeout=10)


@pytest.fixture
def instances(pool: Any, redis: Any) -> Iterator[list[Instance]]:
    group = [Instance(i) for i in range(3)]
    yield group
    for inst in group:
        if inst.proc is not None and inst.proc.poll() is None:
            inst.proc.kill()
            inst.proc.wait(timeout=10)


async def _publish_everywhere(instances: list[Instance], key: str, stream: str, n: int) -> None:
    """Publish n events, rotating over instances; skip dead ones (a real producer behind a
    balancer would be routed elsewhere). Event ids are fixed before the first attempt, so a
    retry after an ambiguous failure cannot duplicate an event."""
    producers = [Producer(i.url, key, max_retries=0, timeout_s=2) for i in instances]
    try:
        for k in range(n):
            event = {"type": "tick", "data": {"k": k}, "event_id": None}
            from uuid import uuid4

            event["event_id"] = str(uuid4())
            for attempt in range(30):
                producer = producers[(k + attempt) % len(producers)]
                try:
                    await producer.publish_many(stream, [event])
                    break
                except httpx.HTTPError:
                    await asyncio.sleep(0.05)
            else:
                raise AssertionError("no instance accepted the publish")
            await asyncio.sleep(0.005)
    finally:
        for producer in producers:
            await producer.aclose()


async def test_sigkill_an_instance_mid_stream(instances: list[Instance], api_key: Any) -> None:
    for inst in instances:
        inst.start()
    await asyncio.gather(*(inst.wait_ready() for inst in instances))
    async with Producer(instances[0].url, api_key[1]) as producer:
        await producer.create_stream("chaos")
    urls = [i.url for i in instances]
    collectors = [
        Collector(Subscription(urls[k % 3 :] + urls[: k % 3], "chaos", backoff_base_s=0.05))
        for k in range(30)
    ]
    await wait_until(lambda: all(c.sub.cursor is not None for c in collectors), timeout=20)

    publishing = asyncio.create_task(_publish_everywhere(instances, api_key[1], "chaos", 600))
    await asyncio.sleep(1.0)
    victim = instances[0]
    victim.kill()  # no close frames, no cleanup: clients see a dead TCP connection
    await asyncio.sleep(1.0)
    victim.start()  # comes back on the same port; clients may land on it again
    await victim.wait_ready()
    await asyncio.sleep(0.5)
    instances[1].kill()
    await publishing

    for c in collectors:
        await c.wait_for_seq(600, timeout=30)
        assert c.seqs == list(range(c.seqs[0], 601))
        assert c.sub.stats.duplicates_received == 0
        assert c.sub.stats.out_of_order_received == 0
    moved = sum(c.sub.stats.reconnects > 0 for c in collectors)
    assert moved >= 10  # the clients of both killed instances failed over and caught up
    for c in collectors:
        await c.stop()
