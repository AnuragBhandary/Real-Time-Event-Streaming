"""Contract between the server reducers and their TypeScript port in web/src/feed/reducers.ts.

The browser folds live events into the snapshot it was sent, so its reducer must produce exactly
what the server's does. This file writes a fixture (simulated matches folded by the Python
reducers, with the state after every event) that web/src/feed/reducers.test.ts replays. The test
here fails if the fixture is stale; regenerate it with:

    uv run python tests/test_web_contract.py
"""

from __future__ import annotations

import json
import random
from pathlib import Path
from typing import Any

from livefeed.reducers import REDUCERS
from livefeed.simulator import match_events

FIXTURE = Path(__file__).parent.parent / "web" / "src" / "feed" / "reducer-fixture.json"
SEEDS = (1, 7, 2026)


def build() -> dict[str, Any]:
    cases = []
    for seed in SEEDS:
        events = match_events(random.Random(seed), balls=30)
        cases.append({"kind": "match", "seed": seed, "steps": _fold("match", events)})
    generic_events = [("a", {"x": 1}), ("b", {}), ("a", {"x": 2, "y": "z"})]
    cases.append({"kind": "generic", "seed": None, "steps": _fold("generic", generic_events)})
    return {"cases": cases}


def _fold(kind: str, events: list[tuple[str, dict[str, Any]]]) -> list[dict[str, Any]]:
    state: dict[str, Any] = {}
    steps = []
    for type_, data in events:
        state = REDUCERS[kind](state, type_, data)
        steps.append({"type": type_, "data": data, "state": state})
    return steps


def render() -> str:
    return json.dumps(build(), indent=1, sort_keys=True) + "\n"


def test_web_reducer_fixture_is_current() -> None:
    assert FIXTURE.read_text() == render(), "stale fixture: run python tests/test_web_contract.py"


if __name__ == "__main__":
    FIXTURE.write_text(render())
    print(f"wrote {FIXTURE}")
