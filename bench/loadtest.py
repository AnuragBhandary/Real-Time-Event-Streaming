"""Load + chaos test against the docker compose stack (3 instances behind nginx).

    docker compose up -d --build --wait
    uv run python bench/loadtest.py --clients 1000 --streams 20 --events 100000 --rate 400 \\
        --drop-mean-s 25 --kill-interval 45 --pubsub-kill-interval 30 --label full

Roles (one file, so every process runs the same code):

* orchestrator (default): creates a key and streams, starts subscriber and producer processes,
  injects chaos, collects results and audits PostgreSQL.
* subscriber: holds ``clients / subscriber-procs`` WebSocket subscriptions (livefeed SDK),
  records delivery latency (receive time - producer's send time), randomly kills its own TCP
  connections, and checks every client ends with every seq exactly once, in order.
* producer: publishes ``--events`` events at ``--rate``/s (open loop) round-robin over streams.

With ``--in-docker`` the subscriber/producer processes run as containers on the compose network
(no host port-forwarding in the measured path); the orchestrator stays on the host to drive
``docker`` for chaos.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import platform
import random
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from livefeed.client import Event, Producer, Snapshot, Subscription

BUCKET_MS = 0.5  # latency histogram resolution


def bucket(ms: float) -> int:
    return int(ms / BUCKET_MS)


def summarize(hist: dict[int, int]) -> dict[str, float]:
    total = sum(hist.values())
    if not total:
        return {"count": 0}
    keys = sorted(hist)
    out: dict[str, float] = {"count": total}
    for p in (50, 90, 95, 99, 99.9):
        target, running = math.ceil(total * p / 100), 0
        for k in keys:
            running += hist[k]
            if running >= target:
                out[f"p{p}"] = round((k + 1) * BUCKET_MS, 1)
                break
    out["max"] = round((keys[-1] + 1) * BUCKET_MS, 1)
    out["mean"] = round(sum((k + 0.5) * BUCKET_MS * v for k, v in hist.items()) / total, 2)
    return out


def merge(hists: list[dict[str, int]]) -> dict[int, int]:
    out: Counter[int] = Counter()
    for h in hists:
        for k, v in h.items():
            out[int(k)] += v
    return dict(out)


def emit(tag: str, payload: Any) -> None:
    sys.stdout.write(f"{tag} {json.dumps(payload)}\n")
    sys.stdout.flush()


# --------------------------------------------------------------------------------------------
# subscriber role
# --------------------------------------------------------------------------------------------


@dataclass
class ClientResult:
    stream: str
    events: int = 0
    first_seq: int | None = None
    resets: int = 0
    live_hist: Counter[int] = field(default_factory=Counter)
    replay_hist: Counter[int] = field(default_factory=Counter)


async def run_subscriber(cfg: dict[str, Any]) -> None:
    rng = random.Random(cfg["seed"])
    subs: list[tuple[Subscription, ClientResult]] = []
    for i, stream in enumerate(cfg["streams"]):
        sub = Subscription(
            cfg["ws_url"], stream, rng=random.Random(rng.random()), backoff_base_s=0.2
        )
        subs.append((sub, ClientResult(stream)))
        if i % 50 == 0:
            await asyncio.sleep(0.05)  # don't stampede the balancer on start-up

    async def consume(sub: Subscription, res: ClientResult) -> None:
        async for item in sub:
            now_ms = time.time() * 1000
            if isinstance(item, Event):
                res.events += 1
                if res.first_seq is None:
                    res.first_seq = item.seq
                sent = item.data.get("sent_at")
                if sent is not None:
                    hist = res.replay_hist if item.replay else res.live_hist
                    hist[bucket(now_ms - sent * 1000)] += 1
            elif isinstance(item, Snapshot) and item.reason is not None:
                res.resets += 1

    async def chaos(sub: Subscription) -> None:
        mean = cfg["drop_mean_s"]
        if not mean:
            return
        while True:
            await asyncio.sleep(rng.expovariate(1 / mean))
            sub.drop()

    consumers = [asyncio.create_task(consume(s, r)) for s, r in subs]
    await _wait(lambda: all(s.cursor is not None for s, _ in subs), cfg["connect_timeout_s"])
    emit("READY", {"clients": len(subs)})
    chaos_tasks = [asyncio.create_task(chaos(s)) for s, _ in subs]

    line = await asyncio.get_running_loop().run_in_executor(None, sys.stdin.readline)
    final: dict[str, int] = json.loads(line.split(" ", 1)[1])
    for t in chaos_tasks:
        t.cancel()
    try:
        await _wait(
            lambda: all((s.cursor or 0) >= final[r.stream] for s, r in subs), cfg["drain_timeout_s"]
        )
        drained = True
    except TimeoutError:
        drained = False

    per_client = []
    for sub, res in subs:
        st = sub.stats
        per_client.append(
            {
                "stream": res.stream,
                "cursor": sub.cursor,
                "final": final[res.stream],
                "events": res.events,
                "first_seq": res.first_seq,
                "resets": res.resets,
                "reconnects": st.reconnects,
                "duplicates_received": st.duplicates_received,
                "out_of_order_received": st.out_of_order_received,
                "close_codes": dict(st.close_codes),
            }
        )
        await sub.close()
    for t in consumers:
        t.cancel()
    emit(
        "RESULT",
        {
            "drained": drained,
            "clients": per_client,
            "live_hist": merge([dict(r.live_hist) for _, r in subs]),
            "replay_hist": merge([dict(r.replay_hist) for _, r in subs]),
        },
    )


async def _wait(predicate: Any, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise TimeoutError
        await asyncio.sleep(0.2)


# --------------------------------------------------------------------------------------------
# producer role
# --------------------------------------------------------------------------------------------


async def run_producer(cfg: dict[str, Any]) -> None:
    streams: list[str] = cfg["streams"]
    total, rate = cfg["events"], cfg["rate"]
    latencies: Counter[int] = Counter()
    errors: Counter[str] = Counter()
    semaphore = asyncio.Semaphore(cfg["concurrency"])
    last_seq: dict[str, int] = {}
    rng = random.Random(cfg["seed"])
    # Per-stream order must follow send order for the match reducer, so each stream publishes
    # sequentially; streams run in parallel and the global rate is paced open-loop.
    queues: dict[str, asyncio.Queue[tuple[int, float] | None]] = {
        s: asyncio.Queue() for s in streams
    }

    async with Producer(cfg["http_url"], cfg["api_key"], max_retries=20, timeout_s=10) as producer:

        async def stream_worker(stream: str) -> None:
            q = queues[stream]
            while (item := await q.get()) is not None:
                k, _ = item
                data = {"team": "A" if k % 2 else "B", "points": rng.choice([0, 1, 2, 4, 6])}
                async with semaphore:
                    started = time.perf_counter()
                    try:
                        data["sent_at"] = time.time()
                        result = await producer.publish(stream, "score", data)
                        last_seq[stream] = max(last_seq.get(stream, 0), result["seq"])
                        latencies[bucket((time.perf_counter() - started) * 1000)] += 1
                    except Exception as exc:
                        errors[type(exc).__name__] += 1

        workers = [asyncio.create_task(stream_worker(s)) for s in streams]
        t0 = time.perf_counter()
        for k in range(total):
            due = t0 + k / rate
            delay = due - time.perf_counter()
            if delay > 0:
                await asyncio.sleep(delay)
            queues[streams[k % len(streams)]].put_nowait((k, due))
            if k and k % 20000 == 0:
                print(f"producer: {k} queued in {time.perf_counter() - t0:.0f}s", file=sys.stderr)
        for s in streams:
            queues[s].put_nowait(None)
        await asyncio.gather(*workers)
        elapsed = time.perf_counter() - t0
        emit(
            "RESULT",
            {
                "published": sum(latencies.values()),
                "errors": dict(errors),
                "retries": producer.retries,
                "duration_s": round(elapsed, 1),
                "rate_per_s": round(sum(latencies.values()) / elapsed, 1),
                "publish_hist": dict(latencies),
            },
        )


# --------------------------------------------------------------------------------------------
# orchestrator
# --------------------------------------------------------------------------------------------


class Proc:
    """A subscriber/producer process (local, or a container on the compose network)."""

    def __init__(self, name: str, argv: list[str]) -> None:
        self.name = name
        self.argv = argv
        self.proc: asyncio.subprocess.Process | None = None

    async def start(self) -> None:
        self.proc = await asyncio.create_subprocess_exec(
            *self.argv, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            limit=2**26,
        )  # fmt: skip

    async def expect(self, tag: str, timeout: float) -> Any:
        assert self.proc is not None and self.proc.stdout is not None
        while True:
            line = await asyncio.wait_for(self.proc.stdout.readline(), timeout)
            if not line:
                raise RuntimeError(f"{self.name} exited before {tag}")
            text = line.decode()
            if text.startswith(tag + " "):
                return json.loads(text[len(tag) + 1 :])

    def send(self, line: str) -> None:
        assert self.proc is not None and self.proc.stdin is not None
        self.proc.stdin.write(line.encode() + b"\n")


async def sh(*argv: str) -> str:
    proc = await asyncio.create_subprocess_exec(
        *argv, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL
    )
    out, _ = await proc.communicate()
    return out.decode()


async def chaos_loop(
    args: argparse.Namespace, events: list[dict[str, Any]], stop: asyncio.Event
) -> None:
    rng = random.Random(args.seed + 7)
    next_kill = time.monotonic() + args.kill_interval if args.kill_interval else math.inf
    next_pubsub = (
        time.monotonic() + args.pubsub_kill_interval if args.pubsub_kill_interval else math.inf
    )
    while not stop.is_set():
        await asyncio.sleep(0.5)
        now = time.monotonic()
        if now >= next_kill:
            victim = rng.choice(["livefeed1", "livefeed2", "livefeed3"])
            cid = (await sh("docker", "compose", "ps", "-q", victim)).strip()
            await sh("docker", "kill", "-s", "KILL", cid)
            events.append(
                {"t": datetime.now(UTC).isoformat(), "action": "SIGKILL", "target": victim}
            )
            print(f"chaos: SIGKILL {victim}", flush=True)
            await asyncio.sleep(args.restart_after)
            await sh("docker", "start", cid)
            events.append(
                {"t": datetime.now(UTC).isoformat(), "action": "restart", "target": victim}
            )
            next_kill = time.monotonic() + args.kill_interval
        if now >= next_pubsub:
            out = await sh(
                "docker", "compose", "exec", "-T", "redis", "redis-cli", "CLIENT", "KILL",
                "TYPE", "pubsub",
            )  # fmt: skip
            events.append(
                {"t": datetime.now(UTC).isoformat(), "action": "kill pubsub", "killed": out.strip()}
            )
            print(f"chaos: killed {out.strip()} pub/sub connections", flush=True)
            next_pubsub = time.monotonic() + args.pubsub_kill_interval


def role_argv(args: argparse.Namespace, role: str, cfg: dict[str, Any]) -> list[str]:
    script = ["--role", role, "--config", json.dumps(cfg)]
    if args.in_docker:
        return [
            "docker", "run", "-i", "--rm", "--network", args.network,
            "-v", f"{Path(__file__).resolve().parent}:/bench:ro",
            "--entrypoint", "python", "livefeed:latest", "/bench/loadtest.py", *script,
        ]  # fmt: skip
    return [sys.executable, str(Path(__file__).resolve()), *script]


async def orchestrate(args: argparse.Namespace) -> int:
    import asyncpg

    from livefeed.auth import create_api_key

    conn = await asyncpg.connect(args.dsn)
    _, key = await create_api_key(conn, f"load-{args.label}", rate_per_s=100_000, burst=100_000)
    run = f"{args.label}-{int(time.time())}"
    streams = [f"{run}-m{i}" for i in range(args.streams)]
    async with Producer(args.host_url, key) as producer:
        for s in streams:
            await producer.create_stream(s, kind="match")
            await producer.publish(s, "match_started", {"teams": ["A", "B"]})

    inner_http = args.docker_url if args.in_docker else args.host_url
    per_proc = [streams[i % len(streams)] for i in range(args.clients)]
    subscribers = []
    for p in range(args.subscriber_procs):
        cfg = {
            "ws_url": inner_http,
            "streams": per_proc[p :: args.subscriber_procs],
            "seed": args.seed + p,
            "drop_mean_s": args.drop_mean_s,
            "connect_timeout_s": 120,
            "drain_timeout_s": args.drain_timeout_s,
        }
        subscribers.append(Proc(f"subscriber-{p}", role_argv(args, "subscriber", cfg)))
    for s in subscribers:
        await s.start()
    ready = await asyncio.gather(*(s.expect("READY", 180) for s in subscribers))
    print(f"{sum(r['clients'] for r in ready)} clients connected", flush=True)

    producer_cfg = {
        "http_url": inner_http, "api_key": key, "streams": streams, "events": args.events,
        "rate": args.rate, "concurrency": args.producer_concurrency, "seed": args.seed,
    }  # fmt: skip
    producer_proc = Proc("producer", role_argv(args, "producer", producer_cfg))
    stop = asyncio.Event()
    chaos_events: list[dict[str, Any]] = []
    chaos = asyncio.create_task(chaos_loop(args, chaos_events, stop))
    await producer_proc.start()
    produced = await producer_proc.expect("RESULT", args.events / args.rate * 4 + 600)
    stop.set()
    await chaos
    await sh("docker", "compose", "up", "-d", "livefeed1", "livefeed2", "livefeed3")

    final = {
        r["id"]: r["last_seq"]
        for r in await conn.fetch("SELECT id, last_seq FROM streams WHERE id = ANY($1)", streams)
    }
    for s in subscribers:
        s.send("FINAL " + json.dumps(final))
    results = await asyncio.gather(
        *(s.expect("RESULT", args.drain_timeout_s + 120) for s in subscribers)
    )
    clients = [c for r in results for c in r["clients"]]

    db_events = await conn.fetchval(
        "SELECT count(*) FROM events WHERE stream_id = ANY($1)", streams
    )
    gaps = await conn.fetchval(
        """
        SELECT count(*) FROM streams s WHERE s.id = ANY($1)
          AND s.last_seq <> (SELECT count(*) FROM events e WHERE e.stream_id = s.id)
        """,
        streams,
    )
    await conn.close()

    close_codes: Counter[str] = Counter()
    for c in clients:
        close_codes.update({str(k): v for k, v in c["close_codes"].items()})
    expected_deliveries = sum(c["final"] - (c["first_seq"] or 1) + 1 for c in clients)
    report = {
        "label": args.label,
        "timestamp": datetime.now(UTC).isoformat(),
        "machine": {"platform": platform.platform(), "python": sys.version.split()[0]},
        "config": {k: v for k, v in vars(args).items() if k not in ("dsn",)},
        "producer": {k: v for k, v in produced.items() if k != "publish_hist"}
        | {
            "publish_latency_ms": summarize(
                {int(k): v for k, v in produced["publish_hist"].items()}
            )
        },
        "subscribers": {
            "clients": len(clients),
            "all_drained": all(r["drained"] for r in results),
            "clients_complete": sum(c["cursor"] == c["final"] for c in clients),
            "deliveries": sum(c["events"] for c in clients),
            "expected_deliveries": expected_deliveries,
            "duplicates_received": sum(c["duplicates_received"] for c in clients),
            "out_of_order_received": sum(c["out_of_order_received"] for c in clients),
            "snapshot_resets": sum(c["resets"] for c in clients),
            "reconnects": sum(c["reconnects"] for c in clients),
            "close_codes": dict(close_codes),
            "live_latency_ms": summarize(merge([r["live_hist"] for r in results])),
            "replay_latency_ms": summarize(merge([r["replay_hist"] for r in results])),
        },
        "database": {"events": db_events, "streams_with_gaps": gaps},
        "chaos_events": chaos_events,
    }
    args.out.mkdir(parents=True, exist_ok=True)
    path = args.out / f"{args.label}.json"
    path.write_text(json.dumps(report, indent=2, default=str))
    print(json.dumps({k: report[k] for k in ("producer", "subscribers", "database")}, indent=2))
    print(f"wrote {path}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--role", choices=["orchestrator", "subscriber", "producer"], default="orchestrator"
    )
    parser.add_argument("--config", help="JSON config (subscriber/producer roles)")
    parser.add_argument("--host-url", default="http://localhost:8080")
    parser.add_argument("--docker-url", default="http://nginx:80")
    parser.add_argument("--dsn", default="postgresql://livefeed:livefeed@localhost:5432/livefeed")
    parser.add_argument("--in-docker", action="store_true")
    parser.add_argument("--network", default="real-time-event-streaming_default")
    parser.add_argument("--clients", type=int, default=1000)
    parser.add_argument("--streams", type=int, default=20)
    parser.add_argument("--events", type=int, default=100_000)
    parser.add_argument("--rate", type=float, default=400, help="events per second")
    parser.add_argument("--subscriber-procs", type=int, default=4)
    parser.add_argument("--producer-concurrency", type=int, default=64)
    parser.add_argument("--drop-mean-s", type=float, default=0, help="mean s between client drops")
    parser.add_argument("--kill-interval", type=float, default=0, help="s between instance kills")
    parser.add_argument("--restart-after", type=float, default=3)
    parser.add_argument("--pubsub-kill-interval", type=float, default=0)
    parser.add_argument("--drain-timeout-s", type=float, default=180)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--label", default="run")
    parser.add_argument("--out", type=Path, default=Path(__file__).resolve().parent / "results")
    args = parser.parse_args()
    if args.role == "subscriber":
        asyncio.run(run_subscriber(json.loads(args.config)))
        return 0
    if args.role == "producer":
        asyncio.run(run_producer(json.loads(args.config)))
        return 0
    return asyncio.run(orchestrate(args))


if __name__ == "__main__":
    sys.exit(main())
