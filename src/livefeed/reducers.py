"""State reducers: fold one event into a stream's snapshot state.

A reducer is a pure function ``(state, type, data) -> new_state``. It runs inside the append
transaction, so the stored snapshot always corresponds exactly to ``last_seq``. A reducer raises
``ValueError`` to reject an invalid event (the whole batch is rolled back and the API returns 422).
"""

from __future__ import annotations

import copy
from collections.abc import Callable
from typing import Any

State = dict[str, Any]
Reducer = Callable[[State, str, dict[str, Any]], State]


def generic(state: State, type_: str, data: dict[str, Any]) -> State:
    """Any event type is accepted; the snapshot counts events and keeps the latest one."""
    counts = dict(state.get("counts", {}))
    counts[type_] = counts.get(type_, 0) + 1
    return {"events": state.get("events", 0) + 1, "counts": counts, "last": {"type": type_, **data}}


def _team(state: State, data: dict[str, Any]) -> str:
    team = data.get("team")
    if team not in state.get("teams", []):
        raise ValueError(f"unknown team {team!r}")
    return str(team)


def match(state: State, type_: str, data: dict[str, Any]) -> State:
    """A live sports match (cricket-style): score, wickets, status and latest commentary."""
    s: State = copy.deepcopy(state) if state else {"status": "scheduled", "events": 0}
    if type_ == "match_started":
        teams = data.get("teams")
        if not (
            isinstance(teams, list) and len(teams) == 2 and all(isinstance(t, str) for t in teams)
        ):
            raise ValueError("match_started needs two team names")
        s.update(
            status="live", teams=teams, score={t: 0 for t in teams}, wickets={t: 0 for t in teams}
        )
    elif type_ in ("score", "wicket"):
        if s.get("status") != "live":
            raise ValueError(f"{type_} before match_started or after match_ended")
        team = _team(s, data)
        if type_ == "score":
            points = data.get("points")
            if not isinstance(points, int) or not 0 <= points <= 10:
                raise ValueError("score needs integer points between 0 and 10")
            s["score"][team] += points
        else:
            s["wickets"][team] += 1
    elif type_ == "commentary":
        text = data.get("text")
        if not isinstance(text, str) or not text:
            raise ValueError("commentary needs text")
        s["commentary"] = text
    elif type_ == "match_ended":
        if s.get("status") != "live":
            raise ValueError("match_ended before match_started")
        s["status"] = "finished"
        scores = s["score"]
        best = max(scores.values())
        leaders = [t for t, v in scores.items() if v == best]
        s["winner"] = leaders[0] if len(leaders) == 1 else None
    else:
        raise ValueError(f"unknown match event type {type_!r}")
    s["events"] = s.get("events", 0) + 1
    return s


REDUCERS: dict[str, Reducer] = {"generic": generic, "match": match}
