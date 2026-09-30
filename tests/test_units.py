"""Pure unit tests: reducers, wire formats, client-side ordering checks, session mechanics."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import orjson
import pytest

from livefeed.client import Event, Snapshot, Subscription, _Resync, _ws_base
from livefeed.config import Settings
from livefeed.hub import Session, SessionClosed
from livefeed.protocol import (
    CloseCode,
    bus_payload,
    encode_event,
    events_frame,
    heartbeat_frame,
    parse_bus_payload,
    snapshot_frame,
)
from livefeed.reducers import generic, match


class TestReducers:
    def test_generic_counts(self) -> None:
        state = generic({}, "a", {"x": 1})
        state = generic(state, "b", {})
        state = generic(state, "a", {"y": 2})
        assert state == {"events": 3, "counts": {"a": 2, "b": 1}, "last": {"type": "a", "y": 2}}

    def test_match_lifecycle(self) -> None:
        s = match({}, "match_started", {"teams": ["MI", "CSK"]})
        s = match(s, "score", {"team": "MI", "points": 6})
        s = match(s, "score", {"team": "CSK", "points": 4})
        s = match(s, "wicket", {"team": "CSK"})
        s = match(s, "commentary", {"text": "What a shot"})
        s = match(s, "match_ended", {})
        assert s["score"] == {"MI": 6, "CSK": 4} and s["wickets"] == {"MI": 0, "CSK": 1}
        assert s["status"] == "finished" and s["winner"] == "MI" and s["events"] == 6
        assert s["commentary"] == "What a shot"

    def test_match_tie_has_no_winner(self) -> None:
        s = match({}, "match_started", {"teams": ["A", "B"]})
        assert match(s, "match_ended", {})["winner"] is None

    def test_reducer_is_pure(self) -> None:
        s = match({}, "match_started", {"teams": ["A", "B"]})
        match(s, "score", {"team": "A", "points": 1})
        assert s["score"]["A"] == 0

    @pytest.mark.parametrize(
        ("type_", "data"),
        [
            ("score", {"team": "A", "points": 1}),  # before start
            ("match_ended", {}),
            ("match_started", {"teams": ["only-one"]}),
            ("commentary", {"text": ""}),
            ("teleport", {}),
        ],
    )
    def test_match_rejects_invalid(self, type_: str, data: dict[str, object]) -> None:
        with pytest.raises(ValueError):
            match({}, type_, data)

    @pytest.mark.parametrize(
        "data", [{"team": "X", "points": 1}, {"team": "A", "points": 11}, {"team": "A"}]
    )
    def test_match_rejects_bad_scores(self, data: dict[str, object]) -> None:
        s = match({}, "match_started", {"teams": ["A", "B"]})
        with pytest.raises(ValueError):
            match(s, "score", data)


class TestProtocol:
    def test_event_encoding_is_deterministic(self) -> None:
        row = {
            "seq": 7,
            "event_id": uuid4(),
            "type": "score",
            "data": {"team": "MI", "points": 4},
            "created_at": datetime(2026, 1, 1, tzinfo=UTC),
        }
        raw = encode_event("m1", row)
        assert raw == encode_event("m1", dict(row))
        decoded = orjson.loads(raw)
        assert (
            decoded["seq"] == 7 and decoded["stream"] == "m1" and decoded["ts"] == 1767225600000.0
        )

    def test_bus_payload_roundtrip(self) -> None:
        assert parse_bus_payload(bus_payload(42, b'{"a":"b:c"}')) == (42, b'{"a":"b:c"}')

    def test_frames(self) -> None:
        frame = orjson.loads(events_frame([b'{"seq":1}', b'{"seq":2}'], replay=True))
        assert frame == {"type": "events", "replay": True, "items": [{"seq": 1}, {"seq": 2}]}
        assert orjson.loads(events_frame([b"{}"], replay=False))["replay"] is False
        assert orjson.loads(snapshot_frame("s", 3, {"x": 1}, None))["seq"] == 3
        assert orjson.loads(heartbeat_frame(3, 5)) == {"type": "heartbeat", "seq": 3, "head": 5}


def _item(seq: int) -> dict[str, object]:
    return {"stream": "s", "seq": seq, "id": "x", "type": "t", "data": {}, "ts": 0.0}


class TestClientOrdering:
    def test_snapshot_then_contiguous_events(self) -> None:
        sub = Subscription("http://h", "s")
        (snap,) = sub._handle(
            {"type": "snapshot", "stream": "s", "seq": 5, "state": {}, "reason": None}
        )
        assert isinstance(snap, Snapshot) and sub.cursor == 5
        out = sub._handle({"type": "events", "replay": False, "items": [_item(6), _item(7)]})
        assert [e.seq for e in out if isinstance(e, Event)] == [6, 7] and sub.cursor == 7

    def test_duplicates_are_counted_and_dropped(self) -> None:
        sub = Subscription("http://h", "s", cursor=7)
        out = sub._handle(
            {"type": "events", "replay": True, "items": [_item(6), _item(7), _item(8)]}
        )
        assert [e.seq for e in out] == [8] and sub.stats.duplicates_received == 2

    def test_gap_forces_resync(self) -> None:
        sub = Subscription("http://h", "s", cursor=7)
        with pytest.raises(_Resync):
            sub._handle({"type": "events", "replay": False, "items": [_item(9)]})
        assert sub.stats.out_of_order_received == 1 and sub.cursor == 7

    def test_heartbeat_and_unknown(self) -> None:
        sub = Subscription("http://h", "s")
        assert sub._handle({"type": "heartbeat", "seq": 1, "head": 9}) == []
        assert sub.head == 9 and sub._handle({"type": "pong"}) == []

    def test_url_handling(self) -> None:
        assert _ws_base("https://x/") == "wss://x" and _ws_base("ws://x") == "ws://x"
        assert Subscription("http://h", "s", cursor=3)._url("ws://h").endswith("ws?cursor=3")


class FakeTransport:
    def __init__(self, block: bool = False) -> None:
        self.frames: list[dict[str, object]] = []
        self.closed: tuple[int, str | None] | None = None
        self.block = block
        self.unblock = asyncio.Event()

    async def send_text(self, data: str) -> None:
        if self.block:
            await self.unblock.wait()
        self.frames.append(orjson.loads(data))

    async def close(self, code: int = 1000, reason: str | None = None) -> None:
        self.closed = (code, reason)


def _settings(**kw: object) -> Settings:
    return Settings(_env_file=None, **kw)  # type: ignore[arg-type]


class TestSession:
    async def test_batches_consecutive_events_into_frames(self) -> None:
        transport = FakeTransport(block=True)
        session = Session(transport, "s", _settings(frame_max_events=3))
        await session.send_snapshot(0, {}, None)
        session.go_live()
        for seq in range(1, 8):
            session.deliver(seq, b'{"seq":%d}' % seq)
        transport.unblock.set()
        await asyncio.sleep(0.05)
        kinds = [(f["type"], len(f.get("items", []))) for f in transport.frames]  # type: ignore[arg-type]
        assert kinds == [("snapshot", 0), ("events", 3), ("events", 3), ("events", 1)]
        await session.shutdown()

    async def test_join_buffer_flushes_without_duplicates(self) -> None:
        transport = FakeTransport()
        session = Session(transport, "s", _settings())
        for seq in (3, 4, 5):  # live events arriving while the replay (1..4) runs
            session.deliver(seq, b'{"seq":%d}' % seq)
        session.sent_seq = 0
        await session.send_replay([(s, b'{"seq":%d}' % s) for s in (1, 2, 3, 4)])
        session.go_live()
        await asyncio.sleep(0.05)
        seqs = [i["seq"] for f in transport.frames for i in f["items"]]  # type: ignore[union-attr]
        assert seqs == [1, 2, 3, 4, 5]
        await session.shutdown()

    async def test_slow_consumer_is_closed_with_4008(self) -> None:
        transport = FakeTransport(block=True)
        session = Session(transport, "s", _settings(client_queue_max=5))
        await session.send_snapshot(0, {}, None)
        session.go_live()
        for seq in range(1, 20):
            session.deliver(seq, b"{}")
        await asyncio.sleep(0.05)
        assert session.close_code == CloseCode.SLOW_CONSUMER
        assert transport.closed is not None and transport.closed[0] == CloseCode.SLOW_CONSUMER
        with pytest.raises(SessionClosed):
            await session.send_replay([(99, b"{}")])
        await session.shutdown()

    async def test_gap_closes_with_4009(self) -> None:
        session = Session(FakeTransport(), "s", _settings())
        await session.send_snapshot(10, {}, None)
        session.go_live()
        session.deliver(12, b"{}")
        await asyncio.sleep(0.01)
        assert session.close_code == CloseCode.RESYNC_REQUIRED
        await session.shutdown()

    async def test_join_buffer_overflow_is_slow_consumer(self) -> None:
        session = Session(FakeTransport(), "s", _settings(join_buffer_max=3))
        for seq in range(1, 6):
            session.deliver(seq, b"{}")
        await asyncio.sleep(0.01)
        assert session.close_code == CloseCode.SLOW_CONSUMER
        session.deliver(6, b"{}")  # ignored once closed
        await session.shutdown()

    async def test_heartbeat_and_pong(self) -> None:
        transport = FakeTransport()
        session = Session(transport, "s", _settings())
        session.heartbeat(5)  # not live yet: ignored
        await session.send_snapshot(4, {}, None)
        session.go_live()
        session.heartbeat(5)
        session.pong()
        await asyncio.sleep(0.02)
        assert [f["type"] for f in transport.frames] == ["snapshot", "heartbeat", "pong"]
        await session.shutdown()

    async def test_send_failure_marks_session_closed(self) -> None:
        class Broken(FakeTransport):
            async def send_text(self, data: str) -> None:
                raise ConnectionResetError

        session = Session(Broken(), "s", _settings())
        await session.send_snapshot(0, {}, None)
        await asyncio.sleep(0.01)
        assert session.close_code == 1006
        await session.shutdown()
