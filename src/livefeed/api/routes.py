"""REST endpoints for producers and readers, and the WebSocket subscription endpoint."""

from __future__ import annotations

import logging
import time

import asyncpg
import orjson
from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Path,
    Query,
    Request,
    Response,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from redis.exceptions import RedisError

from livefeed import bus, metrics
from livefeed import repository as repo
from livefeed.api.deps import AppState, get_state, producer, publish_slot
from livefeed.api.schemas import (
    STREAM_ID_PATTERN,
    EventOut,
    EventPage,
    PublishRequest,
    PublishResponse,
    PublishResult,
    StreamCreate,
    StreamOut,
)
from livefeed.auth import Principal
from livefeed.hub import Session, SessionClosed
from livefeed.protocol import CloseCode, encode_event, event_dict
from livefeed.repository import NewEvent

log = logging.getLogger("livefeed.api")

router = APIRouter(prefix="/v1")
StreamId = Path(pattern=STREAM_ID_PATTERN)


def _stream_out(row: asyncpg.Record) -> StreamOut:
    return StreamOut(
        id=row["id"],
        kind=row["kind"],
        seq=row["last_seq"],
        state=row["state"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


@router.post("/streams", response_model=StreamOut, status_code=201, tags=["streams"])
async def create_stream(
    body: StreamCreate,
    response: Response,
    principal: Principal = Depends(producer),
    state: AppState = Depends(get_state),
) -> StreamOut:
    """Create a stream (idempotent for its owner: 200 if it already exists)."""
    row, created = await repo.create_stream(state.pool, body.id, body.kind, principal.id)
    if not created:
        if row["owner_key_id"] != principal.id or row["kind"] != body.kind:
            raise HTTPException(status.HTTP_409_CONFLICT, "stream id already taken")
        response.status_code = status.HTTP_200_OK
    return _stream_out(row)


@router.get("/streams/{stream_id}", response_model=StreamOut, tags=["streams"])
async def get_snapshot(
    stream_id: str = StreamId, state: AppState = Depends(get_state)
) -> StreamOut:
    """Current state snapshot and the seq it corresponds to (public)."""
    row = await repo.get_stream(state.pool, stream_id)
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "stream not found")
    return _stream_out(row)


async def _check_body_size(request: Request, state: AppState = Depends(get_state)) -> None:
    length = request.headers.get("content-length")
    if length is not None and length.isdigit() and int(length) > state.settings.max_body_bytes:
        raise HTTPException(status.HTTP_413_CONTENT_TOO_LARGE, "body too large")


@router.post(
    "/streams/{stream_id}/events",
    response_model=PublishResponse,
    tags=["streams"],
    dependencies=[Depends(_check_body_size), Depends(publish_slot)],
)
async def publish(
    body: PublishRequest,
    stream_id: str = StreamId,
    principal: Principal = Depends(producer),
    state: AppState = Depends(get_state),
) -> PublishResponse:
    """Append events (atomically, in order) and fan them out. Retrying with the same
    ``event_id`` values returns the original sequence numbers with ``duplicate: true``."""
    if len(body.events) > state.settings.max_batch:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "batch too large")
    started = time.perf_counter()
    events = [NewEvent(e.event_id, e.type, e.data) for e in body.events]
    try:
        results, rows = await repo.append_events(state.pool, stream_id, principal.id, events)
    except (repo.StreamNotFound, repo.NotOwner) as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "stream not found") from exc
    except ValueError as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(exc)) from exc
    if rows:
        try:
            await bus.publish(
                state.redis, stream_id, ((r["seq"], encode_event(stream_id, r)) for r in rows)
            )
        except RedisError:
            # Committed already: instances pick the events up through their resync loop.
            metrics.BUS_PUBLISH_ERRORS.inc()
            log.warning("pub/sub publish failed; resync will deliver", extra={"stream": stream_id})
        metrics.EVENTS_PUBLISHED.labels("event").inc(len(rows))
    duplicates = sum(r.duplicate for r in results)
    if duplicates:
        metrics.DUPLICATE_PUBLISHES.inc(duplicates)
    metrics.PUBLISH_SECONDS.observe(time.perf_counter() - started)
    return PublishResponse(
        stream=stream_id,
        last_seq=max(r.seq for r in results),
        results=[
            PublishResult(event_id=r.event_id, seq=r.seq, duplicate=r.duplicate) for r in results
        ],
    )


@router.get("/streams/{stream_id}/events", response_model=EventPage, tags=["streams"])
async def read_events(
    stream_id: str = StreamId,
    after: int = Query(default=0, ge=0, description="Return events with seq > after"),
    limit: int = Query(default=100, ge=1, le=1000),
    state: AppState = Depends(get_state),
) -> EventPage:
    """HTTP catch-up / history (public). Page with ``after = next_after``."""
    if await repo.last_seq(state.pool, stream_id) is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "stream not found")
    rows = await repo.read_events(state.pool, stream_id, after, limit)
    items = [
        EventOut.model_validate(
            event_dict(stream_id, r["seq"], r["event_id"], r["type"], r["data"], r["created_at"])
        )
        for r in rows
    ]
    return EventPage(items=items, next_after=rows[-1]["seq"] if len(rows) == limit else None)


@router.websocket("/streams/{stream_id}/ws")
async def subscribe(
    websocket: WebSocket,
    stream_id: str = StreamId,
    cursor: int | None = Query(default=None, ge=0, description="Last seq you have processed"),
) -> None:
    """Live subscription. Without ``cursor`` you get a snapshot, then every later event. With
    ``cursor`` you get every event after it (or a snapshot if it is too old), then live events.
    Items always arrive with consecutive seq numbers."""
    state: AppState = websocket.app.state.livefeed
    await websocket.accept()
    session = Session(websocket, stream_id, state.settings)
    metrics.CONNECTIONS.inc()
    try:
        await state.hub.attach(session, cursor)
        while True:
            message = await websocket.receive_text()
            try:
                if orjson.loads(message).get("type") == "ping":
                    session.pong()
            except (orjson.JSONDecodeError, AttributeError):
                pass
    except repo.StreamNotFound:
        session.fail(CloseCode.STREAM_NOT_FOUND, "stream not found")
    except (WebSocketDisconnect, SessionClosed, RuntimeError):
        pass  # client left, or we closed it (slow consumer / resync / shutdown)
    finally:
        state.hub.detach(session)
        await session.shutdown()
        metrics.CONNECTIONS.dec()
