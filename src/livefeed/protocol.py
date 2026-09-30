"""Wire formats: the event JSON, the Redis pub/sub payload, and WebSocket frames.

An event is serialised to JSON exactly once, right after it is committed (or when it is read
back for replay/gap repair, producing identical bytes), and those bytes are reused for every
subscriber on every instance. Frames are assembled by concatenating pre-serialised events.

Server → client frames::

    {"type": "snapshot",  "stream": id, "seq": n, "state": {...}, "reason": null | "cursor_expired"}
    {"type": "events",    "replay": bool, "items": [event, ...]}     # items have seq, strictly +1
    {"type": "heartbeat", "seq": last_seq_sent_to_you, "head": latest_seq_known_to_server}
    {"type": "pong"}

Client → server: ``{"type": "ping"}``. Close codes are listed in :class:`CloseCode`.
"""

from __future__ import annotations

from datetime import datetime
from enum import IntEnum
from typing import Any
from uuid import UUID

import orjson


class CloseCode(IntEnum):
    SERVICE_RESTART = 1012  # instance shutting down: reconnect (to another instance)
    STREAM_NOT_FOUND = 4004
    SLOW_CONSUMER = 4008  # fell too far behind: reconnect with your cursor
    RESYNC_REQUIRED = 4009  # server detected a gap for this client: reconnect with your cursor


def event_dict(
    stream_id: str, seq: int, event_id: UUID, type_: str, data: Any, created_at: datetime
) -> dict[str, Any]:
    return {
        "stream": stream_id,
        "seq": seq,
        "id": str(event_id),
        "type": type_,
        "data": data,
        "ts": round(created_at.timestamp() * 1000, 3),
    }


def encode_event(stream_id: str, row: Any) -> bytes:
    """Serialise an ``events`` row. Same bytes whether live-published or read back later."""
    return orjson.dumps(
        event_dict(
            stream_id, row["seq"], row["event_id"], row["type"], row["data"], row["created_at"]
        )
    )


def bus_payload(seq: int, event: bytes) -> bytes:
    """Pub/sub payload: ``<seq>:<event json>`` so receivers read the seq without parsing JSON."""
    return b"%d:%s" % (seq, event)


def parse_bus_payload(payload: bytes) -> tuple[int, bytes]:
    seq, _, event = payload.partition(b":")
    return int(seq), event


def events_frame(items: list[bytes], replay: bool) -> str:
    head = (
        b'{"type":"events","replay":true,"items":['
        if replay
        else (b'{"type":"events","replay":false,"items":[')
    )
    return (head + b",".join(items) + b"]}").decode()


def snapshot_frame(stream_id: str, seq: int, state: Any, reason: str | None) -> str:
    return orjson.dumps(
        {"type": "snapshot", "stream": stream_id, "seq": seq, "state": state, "reason": reason}
    ).decode()


def heartbeat_frame(seq: int, head: int) -> str:
    return orjson.dumps({"type": "heartbeat", "seq": seq, "head": head}).decode()


PONG_FRAME = '{"type":"pong"}'
