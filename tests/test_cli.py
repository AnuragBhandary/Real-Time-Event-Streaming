"""CLI commands, migrations and the match simulator."""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import time
from typing import Any

import asyncpg
import pytest

from livefeed import cli
from livefeed.auth import parse_key
from livefeed.db import migrate, migration_files
from livefeed.simulator import match_events, simulate
from tests.conftest import DATABASE_URL, REDIS_URL

ENV = {
    "LIVEFEED_DATABASE_URL": DATABASE_URL,
    "LIVEFEED_REDIS_URL": REDIS_URL,
    "LIVEFEED_LOG_JSON": "false",
}


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch, database: str) -> None:
    for name, value in ENV.items():
        monkeypatch.setenv(name, value)


def test_migrations_idempotent(capsys: pytest.CaptureFixture[str]) -> None:
    assert [v for v, _ in migration_files()] == ["0001_init"]
    assert asyncio.run(migrate(DATABASE_URL)) == []
    assert cli.main(["migrate"]) == 0
    assert "up to date" in capsys.readouterr().out


def test_create_key(capsys: pytest.CaptureFixture[str]) -> None:
    assert cli.main(["create-key", "--name", "cli", "--rate", "5", "--burst", "7"]) == 0
    assert parse_key(capsys.readouterr().out.strip()) is not None


def test_simulate_requires_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LIVEFEED_API_KEY", raising=False)
    assert cli.main(["simulate", "--api-key", ""]) == 2


def test_match_events_are_valid() -> None:
    import random

    from livefeed.reducers import match

    state: dict[str, Any] = {}
    events = match_events(random.Random(3), balls=30)
    for type_, data in events:
        state = match(state, type_, data)
    assert state["status"] == "finished" and state["events"] == len(events)


async def test_simulator_against_live_server(
    start_servers: Any, api_key: Any, pool: asyncpg.Pool
) -> None:
    (server,) = start_servers(1)
    await simulate(server.url, api_key[1], matches=2, events_per_s=5000, seed=7)
    rows = await pool.fetch("SELECT id, last_seq, state FROM streams ORDER BY id")
    assert len(rows) == 2
    for row in rows:
        assert row["state"]["status"] == "finished" and row["last_seq"] == row["state"]["events"]


def test_serve_runs_and_stops_on_sigterm() -> None:
    import socket
    import urllib.request

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    proc = subprocess.Popen(
        [sys.executable, "-m", "livefeed", "serve", "--host", "127.0.0.1", "--port", str(port)],
        env={**os.environ, **ENV},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 20
        while True:
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/readyz") as resp:
                    assert resp.status == 200
                    break
            except OSError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.2)
    finally:
        proc.send_signal(signal.SIGTERM)
        # uvicorn shuts down gracefully, then re-raises the signal: -SIGTERM is the clean exit
        assert proc.wait(timeout=15) in (0, -signal.SIGTERM)
