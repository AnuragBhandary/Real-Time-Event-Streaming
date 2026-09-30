# livefeed: Design

A service that takes ordered events from producers (e.g. a live match: scores, wickets,
commentary) and pushes them to many WebSocket subscribers across several server instances. Each
section states the problem, the decision, and the alternatives.

## 1. Requirements

**Functional**
- Producers append events to a stream; each gets a per-stream sequence number (`seq`).
- Subscribers get live events over WebSocket; a new subscriber gets a **snapshot** of the
  current state plus everything after it.
- A reconnecting subscriber resumes from its **cursor** (last seq it processed).
- History over HTTP; per-stream materialised state.

**Guarantees**
- **Per-stream total order**: every subscriber sees seq 1, 2, 3, … with no gaps, duplicates or
  reordering, including across reconnects, instance crashes and Redis failures.
- **Durability**: an acknowledged publish is committed and will reach every subscriber.
- **Isolation**: a slow or stuck client cannot slow others down or exhaust server memory.
- **Scale-out**: any number of stateless instances behind a load balancer.

**Targets**: 1,000 concurrent clients across 3 instances, 100,000 events, p95 live delivery
< 150 ms, zero duplicate or out-of-order events across 10,000 reconnects.

## 2. Architecture

```
producers ── POST /v1/streams/{id}/events ──┐
                                            ▼
             ┌──────────── nginx (least_conn, WebSocket upgrade) ────────────┐
             ▼                             ▼                                 ▼
      ┌─────────────┐               ┌─────────────┐                   ┌─────────────┐
      │ instance 1  │               │ instance 2  │                   │ instance 3  │
      │ FastAPI     │               │             │                   │             │
      │ Hub         │ ◀─ SUBSCRIBE ─┤    Redis    ├─ SUBSCRIBE ─▶     │             │
      │  Channel/   │   (per stream │   pub/sub   │                   │             │
      │  stream     │    with local │ (best effort│                   │             │
      │  Session/   │    clients)   │   fan-out)  │                   │             │
      │  client     │ ── PUBLISH ─▶ └─────────────┘                   │             │
      └──────┬──────┘                                                 └──────┬──────┘
             │ append (row lock per stream) · replay · gap fill · snapshot   │
             └────────────────────────▶ PostgreSQL ◀─────────────────────────┘
                                   streams(last_seq, state) · events(stream_id, seq)
```

The same principle as my job-queue project: **PostgreSQL is the source of truth; Redis
pub/sub is a fast, lossy copy.** Every correctness property comes from the sequence numbers in
PostgreSQL; Redis only makes the common case fast.

## 3. Ordering: assigning sequence numbers

```sql
BEGIN;
SELECT kind, last_seq, state FROM streams WHERE id = $1 FOR UPDATE;   -- per-stream lock
-- dedupe by event_id, fold events into state with the reducer (Python)
INSERT INTO events (stream_id, seq, ...) SELECT ... unnest(...);      -- seq = last_seq + 1 ...
UPDATE streams SET last_seq = $n, state = $state WHERE id = $1;
COMMIT;
-- then: PUBLISH each committed event on Redis (pipelined)
```

- The row lock serialises appends **per stream** only; different streams append in parallel.
- Because seq is assigned under the lock and committed before the lock is released, **seq order
  equals commit order**. That gives the property everything else relies on: *if seq N is
  visible, every seq < N is visible.*
- Why not a PostgreSQL `SEQUENCE`? Sequences are gap-y (a rolled-back transaction burns a
  number) and not ordered by commit. Clients could not tell "lost" from "never existed".
- Why not a Redis `INCR`? The counter and the data would live in different systems. A crash
  between them creates holes or duplicates.
- **Snapshot consistency**: the reducer updates `streams.state` in the same transaction, so
  `(state, last_seq)` is always a consistent pair. A snapshot is one row read.
- **Idempotent publish**: `UNIQUE (stream_id, event_id)`. The SDK assigns `event_id` before the
  first attempt, so a retry after a timeout returns the original seq with `duplicate: true`.
- **Throughput per stream** is bounded by one transaction at a time (~1-2 ms locally). Batch
  publish (up to 500 events per request) amortises it. Different streams scale independently.

## 4. Fan-out across instances

After commit, the API `PUBLISH`es `"<seq>:<event json>"` to `lf:s:<stream>`. Each instance keeps
**one** pub/sub connection, subscribed only to streams that have local clients (reference-counted
channels, unsubscribed after a linger period).

The event is serialised **once** (orjson) and the same bytes are forwarded to every subscriber
on every instance. WebSocket frames are built by concatenating pre-serialised events.

### Why Redis pub/sub is allowed to be unreliable
Pub/sub is fire-and-forget: a subscriber that is reconnecting misses messages, and messages from
*different publishers* (instances) can arrive out of order. For example, instance A commits seq 5
and instance B commits seq 6, but B's PUBLISH lands first. Instead of fighting that, each
instance treats pub/sub as a hint and repairs it:

**Per-stream channel pump** (one task per stream per instance, processing its inbox in order):

| Message seq vs channel `last_seq` | Action |
|---|---|
| `seq <= last_seq` | duplicate → drop |
| `seq == last_seq + 1` | deliver to local sessions |
| `seq > last_seq + 1` | **gap** → read `(last_seq, seq)` from PostgreSQL, deliver, then this one |

The gap fill always succeeds because of the ordering property in §3. A **resync loop** (every 1 s)
compares each active channel with `streams.last_seq` in one query. That catches a lost *final*
message, which no later message would reveal. After a pub/sub reconnect, every channel is
resynced immediately. If the channel's inbox overflows, messages are dropped on purpose and the
next seq reveals the gap.

**Why not Redis Streams or Kafka for fan-out?** Either would give a replayable buffer, but
PostgreSQL already is the replayable log (primary-key range scans on `(stream_id, seq)`). A
second durable log would be a second source of truth to keep consistent. Pub/sub gives the
lowest latency for the broadcast and costs nothing when it fails.

## 5. Joining and resuming without a race

The classic bug: a client asks for history, then subscribes to live events, and anything
published between the two is lost (or, done in the other order, duplicated). livefeed avoids it:

1. **Register first**: the session joins the channel in *buffering* mode. Live events for it are
   buffered, not sent.
2. **Catch up**: no cursor → send snapshot `(state, seq)`. With a cursor → replay `seq > cursor`
   from PostgreSQL in pages of 500, flow-controlled by the session's queue. If the cursor is more
   than `max_replay` (10,000) behind, or ahead of the stream, send a snapshot with a `reason`
   instead: **bounded replay**.
3. **Go live**: flush the buffer, skipping anything the replay already covered, then deliver
   directly.

The replay starts *after* registration, so replay + buffer covers every seq. If a session ever
sees a gap anyway, it is closed with **4009** and the client reconnects with its cursor. The whole
system has **one recovery path**: disconnect and resume from the cursor. The same path handles
network drops, instance crashes, slow consumers and deploys.

When an instance creates a channel, it subscribes to Redis *before* reading `last_seq` from
PostgreSQL, so nothing committed after the read can be missed.

## 6. Backpressure and slow consumers

- The channel pump **never blocks on a client**: `Session.deliver()` is synchronous and uses
  `put_nowait` on a bounded queue (default 2,000 events).
- Each session has its own **writer task** that batches consecutive events (up to 200) into one
  frame and awaits the socket. The ASGI server's transport flow control makes that await block
  when the kernel buffer is full, so a stuck client stops draining its queue.
- When the queue is full, the client is **closed with 4008**. It loses nothing: it reconnects and
  resumes from its cursor via replay, which is itself flow-controlled. The server spends
  O(queue size) memory per client, never unbounded. Fast clients are unaffected (tested: a client
  that stops reading is cut off after ~1 MB while another client receives all 2,000 events without
  a single reconnect).
- The pump yields to the event loop after every message, so writers drain in between. Without
  that, a burst would fill every queue before any writer ran.
- **Producer side**: per-key token bucket (429) and a per-instance cap on in-flight publishes
  (503 + `Retry-After`). Past capacity the instance sheds load immediately instead of queueing
  until everyone times out.

Alternatives: dropping events for slow clients (breaks the ordering guarantee), or buffering
without bound (one slow phone can take down the server). Disconnect-and-resume keeps both
guarantees.

## 7. Liveness: heartbeats

- WebSocket protocol pings (uvicorn, every 20 s, 20 s timeout) detect dead TCP peers that never
  sent a FIN (e.g. a phone that lost signal).
- An application-level heartbeat every 15 s carries `seq` (last sent to you) and `head` (latest
  known). A client can detect staleness even through proxies that answer pings themselves.
- Clients may send `{"type":"ping"}` and get a pong.

## 8. Failure modes

| Failure | Effect | Recovery |
|---|---|---|
| Client network drop | TCP dies (1006) | SDK reconnects with jittered backoff, resumes from cursor |
| Instance SIGKILL | Its clients drop; nginx routes new connections elsewhere | Clients resume from their cursors on other instances; no state is lost (instances are stateless) |
| Instance deploy / SIGTERM | Clients get **1012** | Immediate resume elsewhere |
| Redis pub/sub connection killed | Messages missed | Subscriber reconnects, resubscribes, resyncs all channels from PostgreSQL |
| Redis down | Publishes still commit (PUBLISH failure is logged); rate limiter fails open | Resync loop delivers from PostgreSQL within ~1 s |
| Out-of-order pub/sub delivery | Gap then duplicate | Gap fill from PostgreSQL, duplicate dropped |
| Slow / stuck client | Queue fills | **4008**, resume from cursor |
| Very old cursor | Replay would be huge | Snapshot with `reason: cursor_expired` |
| Producer retry after timeout | Might double-publish | `event_id` dedupe returns the original seq |
| PostgreSQL down | Publishes fail (5xx); live fan-out of already-committed events continues | Producers retry with the same event ids |

## 9. Scaling path

- **Instances**: stateless; add more behind the balancer. Each instance subscribes only to the
  streams its clients watch.
- **Hot stream** (one match, 1M viewers): fan-out work is per instance, not per stream, so spread
  its viewers across instances. Past that, add a tier of edge relays that subscribe once
  upstream.
- **Many streams**: pub/sub channels are cheap; Redis Cluster shards pub/sub (sharded pub/sub in
  Redis 7, `SPUBLISH`).
- **PostgreSQL**: appends are per-stream serial and parallel across streams. Partition `events`
  by time for retention; archive old streams. Replays are primary-key range scans.

## 10. Limitations (deliberate)

- One stream per WebSocket connection (multiplexing many streams is a straightforward extension).
- Subscribers are unauthenticated (public live feeds). Private streams would need signed,
  expiring subscribe tokens.
- No event retention job yet; bounded replay plus snapshots already make old events unnecessary
  for clients.
