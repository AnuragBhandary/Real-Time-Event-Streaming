# Benchmarks

Produced by `bench/loadtest.py`; raw results are in [`bench/results/`](../bench/results).
Subscribers and the producer run as containers on the compose network, so traffic goes through
nginx exactly as external clients' would, without the macOS host's port forwarding in the path.

## Environment

| | |
|---|---|
| Machine | MacBook Pro, Apple M5 Pro, 24 GB RAM |
| Containers | Colima VM with 8 vCPU / 8 GiB |
| Stack | 3 livefeed instances (1 uvicorn process each) behind nginx (`least_conn`), PostgreSQL 16, Redis 7 |
| Settings | defaults from `config.py`: client queue 2,000 events, frames of up to 200 events, resync every 1 s, max replay 10,000 |
| Load generators | 4 subscriber processes × 250 WebSocket clients, 1 producer process |

Everything shares one laptop, including the load generators.

## 1. 1,000 clients, 100,000 events, continuous chaos

```bash
uv run python bench/loadtest.py --in-docker --clients 1000 --streams 20 --events 100000 \
    --rate 400 --subscriber-procs 4 --drop-mean-s 20 --kill-interval 45 \
    --pubsub-kill-interval 30 --label full-1000-clients-chaos
```

**Workload.** 20 live-match streams with 50 subscribers each. 100,000 `score` events at 400/s
(open loop) for 250 s, which is **5,000,000 deliveries**.

**Chaos, all at the same time:**
- every client kills its own TCP connection at random (exponential, mean 20 s) and reconnects
  with its cursor;
- every 45 s a random instance is **SIGKILLed** and restarted 3 s later (5 kills), so all of its
  clients fail over through nginx;
- every 30 s all Redis pub/sub connections are killed (8 times, 3 connections each).

| Metric | Result |
|---|---|
| Clients that ended with every event | **1,000 / 1,000** |
| Deliveries / expected | **5,000,000 / 5,000,000** |
| Duplicate seqs received (protocol level, before SDK dedupe) | **0** |
| Out-of-order or missing seqs received | **0** |
| Snapshot resets (cursor too old) | 0 |
| Reconnects | **14,069** |
| **Live delivery latency** (producer send → subscriber receive) | **p50 3.0 ms · p95 5.0 ms · p99 14.5 ms** · p99.9 112 ms |
| Replayed delivery latency (events caught up after a reconnect) | p50 160 ms · p95 3.3 s |
| Publish latency (HTTP, through nginx) | p50 2.5 ms · p95 4.5 ms · p99 10 ms |
| Producer errors / retries | 0 / 10 (retries during instance kills; idempotent) |
| PostgreSQL | 100,020 events (100,000 + 20 `match_started`), 0 streams with gaps |

Live latency counts events pushed while the client was connected. Replayed latency counts events
the client missed while disconnected: they wait for the reconnect (backoff + handshake + replay),
so their tail reflects outage length, not steady-state speed.

"Zero duplicates / out-of-order" is measured on what the server actually sent. The SDK counts a
violation *before* it would drop or repair anything, so its own safety net cannot hide a server
bug. Every client's final cursor was also compared with `streams.last_seq` in PostgreSQL.

## 2. Headroom (no chaos)

```bash
uv run python bench/loadtest.py --in-docker --clients 1000 --streams 20 --events 100000 \
    --rate 1600 --subscriber-procs 4 --label headroom-no-chaos
```

| Metric | Result |
|---|---|
| Achieved publish rate | ≈ **1,000 events/s** → **≈ 50,000 deliveries/s** |
| Live delivery latency | p50 7.5 ms · **p95 70.5 ms** · p99 130 ms |
| Deliveries / duplicates / out-of-order | 5,000,000 / 0 / 0 |

The run requested 1,600 events/s. The single producer process reached ~1,000/s, because each
stream publishes sequentially (to keep the match reducer's order meaningful) and each publish is
one HTTP round trip plus one PostgreSQL transaction. So this is a floor on capacity, not a
ceiling. Even at 2.5× the target load, p95 stays under 150 ms.

## Reproduce

```bash
make up
make load-smoke   # about a minute, small, with every kind of chaos
make load         # run 1 above (about 5 minutes)
```
