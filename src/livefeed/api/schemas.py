from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

STREAM_ID_PATTERN = r"^[A-Za-z0-9_.:-]{1,128}$"


class StreamCreate(BaseModel):
    id: str = Field(pattern=STREAM_ID_PATTERN)
    kind: Literal["generic", "match"] = "generic"


class StreamOut(BaseModel):
    id: str
    kind: str
    seq: int = Field(description="Sequence number the state corresponds to")
    state: dict[str, Any]
    created_at: datetime
    updated_at: datetime


class EventIn(BaseModel):
    event_id: UUID = Field(
        default_factory=uuid4,
        description="Idempotency key. Set it so retries of the same publish are deduplicated.",
    )
    type: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_.:-]{0,63}$")
    data: dict[str, Any] = Field(default_factory=dict)


class PublishRequest(BaseModel):
    events: list[EventIn] = Field(min_length=1)


class PublishResult(BaseModel):
    event_id: UUID
    seq: int
    duplicate: bool


class PublishResponse(BaseModel):
    stream: str
    last_seq: int
    results: list[PublishResult]


class EventOut(BaseModel):
    stream: str
    seq: int
    id: UUID
    type: str
    data: dict[str, Any]
    ts: float


class EventPage(BaseModel):
    items: list[EventOut]
    next_after: int | None
