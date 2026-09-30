"""Fixtures. Tests run against real PostgreSQL and Redis (``docker compose up -d postgres
redis``), and real uvicorn servers in background threads, so WebSocket, pub/sub and locking
behaviour is the real thing."""

from __future__ import annotations

import asyncio
import os
import socket
import threading
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from typing import Any

import asyncpg
import httpx
import pytest
import uvicorn

from livefeed.api.app import create_app
from livefeed.auth import create_api_key
from livefeed.bus import create_redis
from livefeed.config import Settings
from livefeed.db import create_pool, migrate

DATABASE_URL = os.environ.get(
    "LIVEFEED_TEST_DATABASE_URL", "postgresql://livefeed:livefeed@localhost:5432/livefeed_test"
)
REDIS_URL = os.environ.get("LIVEFEED_TEST_REDIS_URL", "redis://localhost:6379/14")


def make_settings(**overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "database_url": DATABASE_URL,
        "redis_url": REDIS_URL,
        "db_pool_min": 1,
        "db_pool_max": 10,
        "auth_cache_ttl_s": 0.0,
        "resync_interval_s": 0.2,
        "heartbeat_interval_s": 0.3,
        "channel_linger_s": 0.6,
        "log_json": False,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)  # type: ignore[call-arg]


async def _prepare_database() -> None:
    base, _, name = DATABASE_URL.rpartition("/")
    admin = await asyncpg.connect(f"{base}/postgres")
    try:
        if not await admin.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", name):
            await admin.execute(f'CREATE DATABASE "{name}"')
    finally:
        await admin.close()
    await migrate(DATABASE_URL)


@pytest.fixture(scope="session")
def database() -> str:
    asyncio.run(_prepare_database())
    return DATABASE_URL


@pytest.fixture
def settings(database: str) -> Settings:
    return make_settings()


@pytest.fixture
async def pool(settings: Settings) -> AsyncIterator[asyncpg.Pool]:
    pool = await create_pool(settings)
    await pool.execute("TRUNCATE api_keys, streams, events RESTART IDENTITY CASCADE")
    yield pool
    await pool.close()


@pytest.fixture
async def redis(settings: Settings) -> AsyncIterator[Any]:
    client = create_redis(settings.redis_url)
    await client.flushdb()
    yield client
    await client.aclose()


@pytest.fixture
async def api_key(pool: asyncpg.Pool) -> tuple[int, str]:
    return await create_api_key(pool, "test", rate_per_s=10_000, burst=10_000)


class LiveServer:
    """A real livefeed instance (uvicorn + app + hub) in a background thread."""

    def __init__(self, settings: Settings) -> None:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        self.app = create_app(settings)
        config = uvicorn.Config(
            self.app, host="127.0.0.1", port=self.port, log_level="warning", ws_ping_interval=None
        )
        self.server = uvicorn.Server(config)
        self.thread = threading.Thread(target=self.server.run, daemon=True)
        self.url = f"http://127.0.0.1:{self.port}"
        self.ws_url = f"ws://127.0.0.1:{self.port}"

    @property
    def hub(self) -> Any:
        return self.app.state.livefeed.hub

    def start(self) -> LiveServer:
        self.thread.start()
        deadline = time.monotonic() + 15
        while not self.server.started:
            if time.monotonic() > deadline:
                raise RuntimeError("live server did not start")
            time.sleep(0.02)
        return self

    def stop(self) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=15)


@pytest.fixture
def start_servers(pool: asyncpg.Pool, redis: Any) -> Iterator[Callable[..., list[LiveServer]]]:
    started: list[LiveServer] = []

    def start(n: int = 1, **overrides: Any) -> list[LiveServer]:
        servers = [
            LiveServer(make_settings(instance_id=f"test-{i}", **overrides)) for i in range(n)
        ]
        for server in servers:
            server.start()
        started.extend(servers)
        return servers

    yield start
    for server in started:
        server.stop()


@pytest.fixture
async def http(api_key: tuple[int, str], start_servers: Any) -> AsyncIterator[httpx.AsyncClient]:
    (server,) = start_servers(1)
    async with httpx.AsyncClient(base_url=server.url, headers={"X-API-Key": api_key[1]}) as client:
        client.server = server  # type: ignore[attr-defined]
        yield client


async def wait_until(
    predicate: Callable[[], Awaitable[bool] | bool], timeout: float = 10.0, interval: float = 0.02
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if isinstance(result, Awaitable):
            result = await result
        if result:
            return
        await asyncio.sleep(interval)
    raise AssertionError(f"condition not met within {timeout}s")
