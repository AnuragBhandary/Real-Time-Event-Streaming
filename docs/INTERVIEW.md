# Interview guide

How to learn this codebase, pitch it, and defend it in a system-design or behavioural deep
dive. Numbers are in [BENCHMARKS.md](BENCHMARKS.md); reasoning is in [DESIGN.md](DESIGN.md).

## 1. Learning path (read in this order)

| # | File | Be able to explain afterwards |
|---|---|---|
| 1 | `src/livefeed/migrations/0001_init.sql` | Why `(stream_id, seq)` is the key, why `last_seq` + `state` live on the stream row |
| 2 | `src/livefeed/repository.py` → `append_events` | Row lock → seq = commit order; idempotency; reducer in the transaction |
| 3 | `src/livefeed/reducers.py` | Snapshots as a fold over events |
| 4 | `src/livefeed/protocol.py` | Serialise once; frame format; close codes |
| 5 | `src/livefeed/bus.py` | Why pub/sub may lose/reorder and why that is fine; resubscribe + resync |
| 6 | `src/livefeed/hub.py` → `Channel` | The pump: duplicate / deliver / gap-fill; resync loop |
| 7 | `src/livefeed/hub.py` → `Hub.attach`, `Session` | Register → replay → go live; bounded queue; 4008 / 4009 |
| 8 | `src/livefeed/api/routes.py` | Publish path, WebSocket endpoint, load shedding |
| 9 | `src/livefeed/client.py` | Cursor resume, jittered reconnect, violation counters |
| 10 | `tests/test_streaming.py`, `tests/test_chaos.py` | What each failure test proves |
| 11 | `bench/loadtest.py` | How latency and "zero violations" are measured |

## 2. The 60-second pitch

> "I built a real-time event streaming service, a live sports feed, in Python with asyncio,
> FastAPI WebSockets, Redis pub/sub and PostgreSQL, running as three instances behind nginx.
> Every event gets a per-stream sequence number under a row lock, so sequence order equals commit
> order. Redis pub/sub fans events out between instances, but it's treated as lossy: each instance
> tracks the last sequence it delivered, drops duplicates, and fills any gap from PostgreSQL.
> Clients reconnect with a cursor; the server registers them before replaying history, so there's
> no race between catch-up and live. Slow clients get a bounded queue and are disconnected to
> resume from their cursor instead of eating memory. I load-tested it with 1,000 WebSocket
> clients and 100,000 events while killing instances, Redis connections and client sockets, over
> 10,000 reconnects, and every client received every event exactly once, in order."

## 3. Deep-dive questions and answers

**How do you guarantee order?**
Per stream, not globally. The append transaction locks the stream row (`SELECT … FOR UPDATE`),
sets `seq = last_seq + 1`, inserts and commits. The lock makes seq order equal commit order, so
seeing seq N means everything before N is committed and readable. Different streams don't
contend.

**Why not a database sequence or Redis INCR for seq?**
Sequences leave gaps on rollback and aren't commit-ordered, so a client couldn't distinguish a
lost event from one that never existed. INCR puts the counter in a different system from the
data; a crash between the two creates holes.

**Redis pub/sub loses messages. Why use it?**
Latency and simplicity for the broadcast. It's only an optimisation: every instance repairs
losses from PostgreSQL. A missing seq shows up as a gap when the next one arrives, and the
one-second resync loop catches a lost *last* message.

**Can pub/sub deliver out of order?**
Yes, across publishers. Instance A commits 5 and instance B commits 6, but B publishes first.
The channel sees 6 with `last_seq = 4`, fills 5 from PostgreSQL, delivers 5 then 6, and later
drops A's late 5 as a duplicate. `test_lost_and_reordered_pubsub_messages_are_repaired` injects
exactly this.

**Walk me through a reconnect.**
Client connects with `?cursor=N`. The server registers the session in buffering mode *first*,
then replays `seq > N` from PostgreSQL in pages, then flushes buffered live events, skipping
those the replay covered. Registering before the replay closes the race window. If the cursor
is more than 10,000 behind (or ahead of the stream), it gets a snapshot instead: bounded replay.

**What happens with a slow client?**
Delivery into its queue is non-blocking. Its own writer task drains the queue, and that blocks
when the TCP buffer is full. When the queue (2,000 events) overflows, the server closes with
4008; the client resumes from its cursor. Memory per client is bounded, and the others never
wait. I verified it with a raw client that never reads: it's cut off after ~1 MB while another
client gets all 2,000 events with zero reconnects.

**Why disconnect instead of dropping messages for slow clients?**
Dropping breaks the ordering guarantee; buffering forever lets one bad client exhaust memory.
Disconnect-and-resume keeps both guarantees and reuses the same code path as any network failure.

**How do you prevent a burst from disconnecting everyone?**
The channel pump yields to the event loop after each message so writers can drain, writers batch
up to 200 events per frame, and queues are sized for bursts (max batch is 500 events).

**What if an instance dies?**
Instances are stateless: all state is in PostgreSQL. Its clients see a dead socket, reconnect
through nginx to another instance with their cursors, and replay what they missed. The load test
SIGKILLs a random instance every 45 seconds.

**What does the heartbeat add over TCP keepalive or WebSocket ping?**
Protocol pings catch dead peers. The app heartbeat carries `seq` and `head`, so a client can see
it's behind even if a proxy answers pings on the server's behalf.

**How do producers avoid duplicates?**
Each event has an `event_id`, set by the SDK before the first attempt, and there's a unique
constraint on `(stream_id, event_id)`. A retry returns the original seq with `duplicate: true`.

**How is latency measured?**
The producer stamps `sent_at` in the event; each subscriber computes receive time minus
`sent_at`. Both run in containers on the same host clock, so the measurement is end to end:
HTTP publish, PostgreSQL commit, Redis, the instance, nginx, and the WebSocket. Live and replayed
deliveries are reported separately, because replayed ones include the time the client was
disconnected.

**How would you scale to a million viewers of one match?**
Fan-out cost is per instance, so spread viewers over many instances; each subscribes once to the
stream. Beyond that, add a relay tier (edge nodes subscribe upstream and fan out locally), and use
Redis sharded pub/sub for many streams. PostgreSQL work is per event, not per viewer.

**What would you change for production?**
Signed subscribe tokens for private streams, multiplexing several streams per socket, retention
and partitioning for `events`, OpenTelemetry tracing across publish → deliver, and autoscaling on
connection count.

## 4. Amazon Leadership Principles stories

- **Dive Deep**: *The slow-consumer test that wouldn't fail.* My backpressure test kept passing
  without ever triggering a disconnect. I dug into it: the server's flow control worked with a raw
  TCP client that never reads, but the WebSocket client library was buffering everything in
  memory. So I rewrote the test around a client that truly stops reading, and documented the
  behaviour.
- **Insist on the Highest Standards**: Zero violations was measured *at the protocol level*,
  before the SDK's own de-duplication, so the SDK couldn't hide server bugs. Every client's final
  cursor was also checked against PostgreSQL.
- **Invent and Simplify**: One recovery path for every failure (disconnect, resume from cursor)
  instead of separate logic for slow clients, deploys, gaps and crashes.
- **Frugality**: Redis pub/sub instead of a second durable log; PostgreSQL already was the log.
- **Bias for Action / Ownership**: Built the load and chaos harness early and let its results
  drive fixes: yielding in the pump, and sizing queues for bursts.

## 5. Whiteboard version

1. Producer → instance → PostgreSQL (`streams` row lock, `events` table) → "seq = commit order".
2. Add Redis pub/sub between instances; state that it's lossy and reorders.
3. Channel pump table: duplicate / deliver / gap-fill; resync loop.
4. Client join sequence: register → replay → go live; the race it avoids.
5. Session queue + writer; 4008; resume.
6. Failure table (DESIGN.md §8) and scaling path (§9).
