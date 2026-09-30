"""Simulated live cricket matches: a realistic producer for demos."""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any

from livefeed.client import Producer

log = logging.getLogger("livefeed.simulator")

TEAMS = ["MI", "CSK", "RCB", "KKR", "SRH", "RR", "DC", "PBKS", "GT", "LSG"]
LINES = [
    "Driven through the covers!",
    "Short ball, pulled away to deep midwicket.",
    "Beaten outside off, great delivery.",
    "Dropped at slip! That could be costly.",
    "Timeout called on the field.",
]


def match_events(rng: random.Random, balls: int = 120) -> list[tuple[str, dict[str, Any]]]:
    home, away = rng.sample(TEAMS, 2)
    events: list[tuple[str, dict[str, Any]]] = [("match_started", {"teams": [home, away]})]
    for innings, team in enumerate((home, away)):
        wickets = 0
        for ball in range(balls):
            if wickets == 10:
                break
            roll = rng.random()
            if roll < 0.05:
                wickets += 1
                events.append(("wicket", {"team": team, "ball": innings * balls + ball}))
            else:
                points = rng.choices([0, 1, 2, 3, 4, 6], weights=[35, 35, 10, 2, 12, 6])[0]
                events.append(("score", {"team": team, "points": points}))
            if rng.random() < 0.1:
                events.append(("commentary", {"text": rng.choice(LINES)}))
    events.append(("match_ended", {}))
    return events


async def run_match(
    producer: Producer, stream_id: str, events_per_s: float, rng: random.Random
) -> None:
    await producer.create_stream(stream_id, kind="match")
    for type_, data in match_events(rng):
        await producer.publish(stream_id, type_, data)
        await asyncio.sleep(rng.expovariate(events_per_s))
    log.info("match finished", extra={"stream": stream_id})


async def simulate(
    base_url: str, api_key: str, matches: int, events_per_s: float, seed: int | None
) -> None:
    rng = random.Random(seed)
    async with Producer(base_url, api_key) as producer:
        run_id = rng.randrange(10_000)
        await asyncio.gather(
            *(
                run_match(
                    producer, f"match-{run_id}-{i}", events_per_s, random.Random(rng.random())
                )
                for i in range(matches)
            )
        )
