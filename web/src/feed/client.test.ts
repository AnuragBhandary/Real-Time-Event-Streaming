import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { ev, FakeSocket, matchEvents } from "../test/fakeSocket";
import { FeedClient, streamUrl, type FeedClientOptions } from "./client";
import { CloseCode } from "./protocol";
import { match } from "./reducers";

class FakeNetwork extends EventTarget {
  navigator = { onLine: true };
  go(online: boolean) {
    this.navigator.onLine = online;
    this.dispatchEvent(new Event(online ? "online" : "offline"));
  }
}

let network: FakeNetwork;

function makeClient(overrides: Partial<FeedClientOptions> = {}) {
  const client = new FeedClient({
    url: streamUrl("m1", { protocol: "http:", host: "test" }),
    reducer: match,
    createSocket: FakeSocket.create,
    network,
    random: () => 0.5,
    now: () => Date.now(),
    ...overrides,
  });
  client.start();
  return client;
}

beforeEach(() => {
  vi.useFakeTimers();
  FakeSocket.reset();
  network = new FakeNetwork();
});

afterEach(() => {
  vi.useRealTimers();
});

describe("FeedClient", () => {
  it("starts from a snapshot, then folds each following event exactly once", () => {
    const client = makeClient();
    expect(FakeSocket.last.url).toBe("ws://test/v1/streams/m1/ws");
    expect(client.getSnapshot().status).toBe("connecting");

    FakeSocket.last.open().snapshot(0, {}).events(...matchEvents(1, 3));
    const s = client.getSnapshot();
    expect(s.status).toBe("live");
    expect(s.cursor).toBe(3);
    expect(s.baseSeq).toBe(0);
    expect(s.state).toMatchObject({ status: "live", score: { A: 2, B: 0 } });
    expect(s.stats).toMatchObject({ applied: 3, snapshots: 1, duplicatesDropped: 0 });
    expect(s.recent.map((e) => e.seq)).toEqual([3, 2, 1]); // newest first
  });

  it("drops replayed duplicates instead of applying them twice", () => {
    const client = makeClient();
    FakeSocket.last.open().snapshot(0, {}).events(...matchEvents(1, 4)).events(...matchEvents(3, 5));
    expect(client.getSnapshot().stats).toMatchObject({ applied: 5, duplicatesDropped: 2 });
    expect(client.getSnapshot().state).toMatchObject({ score: { A: 4 } });
  });

  it("never applies past a gap: it resyncs from its cursor", () => {
    const client = makeClient();
    const first = FakeSocket.last.open().snapshot(0, {});
    first.events(...matchEvents(1, 2), ...matchEvents(4, 5)); // seq 3 is missing

    expect(client.getSnapshot().cursor).toBe(2); // stopped before the hole
    expect(client.getSnapshot().stats.gapsDetected).toBe(1);
    expect(first.closedWith).toBe(CloseCode.CLIENT_RESYNC);

    vi.advanceTimersByTime(250); // immediate reconnect (jitter only)
    expect(FakeSocket.last.url).toBe("ws://test/v1/streams/m1/ws?cursor=2");
    FakeSocket.last.open().events(...matchEvents(3, 5));
    expect(client.getSnapshot().state).toMatchObject({ score: { A: 4 } });
    expect(client.getSnapshot().stats).toMatchObject({ applied: 5, reconnects: 1 });
  });

  it("ignores frames from a socket it has abandoned", () => {
    const client = makeClient();
    const old = FakeSocket.last.open().snapshot(0, {});
    client.dropConnection();
    old.events(...matchEvents(1, 3)).drop();
    expect(client.getSnapshot().stats.applied).toBe(0);
    expect(client.getSnapshot().stats.lastCloseCode).toBe(CloseCode.USER_DROP);
  });

  it("backs off exponentially with jitter after network failures, and resets once stable", () => {
    makeClient();
    FakeSocket.last.drop(); // never opened
    const delays: number[] = [];
    for (let i = 0; i < 6; i++) {
      const before = FakeSocket.all.length;
      let waited = 0;
      while (FakeSocket.all.length === before) {
        vi.advanceTimersByTime(50);
        waited += 50;
      }
      delays.push(waited);
      FakeSocket.last.drop();
    }
    // random() = 0.5, so half of min(10 s, 500 ms * 2^attempt), rounded up to the 50 ms tick
    expect(delays).toEqual([250, 500, 1000, 2000, 4000, 5000]);

    vi.advanceTimersByTime(5000);
    FakeSocket.last.open();
    vi.advanceTimersByTime(5000); // stable for 5 s
    FakeSocket.last.drop();
    vi.advanceTimersByTime(249);
    const count = FakeSocket.all.length;
    vi.advanceTimersByTime(1);
    expect(FakeSocket.all.length).toBe(count + 1); // back to the first step
  });

  it.each([
    CloseCode.SERVICE_RESTART,
    CloseCode.SLOW_CONSUMER,
    CloseCode.RESYNC_REQUIRED,
  ])("reconnects at once with its cursor after close code %i", (code) => {
    const client = makeClient();
    FakeSocket.last.open().snapshot(0, {}).events(...matchEvents(1, 2)).drop(code);
    expect(client.getSnapshot().status).toBe("reconnecting");
    vi.advanceTimersByTime(125);
    expect(FakeSocket.last.url).toMatch(/\?cursor=2$/);
  });

  it("stops for good when the stream does not exist", () => {
    const client = makeClient();
    FakeSocket.last.open().drop(CloseCode.STREAM_NOT_FOUND);
    vi.advanceTimersByTime(60_000);
    expect(client.getSnapshot().status).toBe("not_found");
    expect(FakeSocket.all).toHaveLength(1);
  });

  it("treats silence as a dead connection", () => {
    const client = makeClient({ heartbeatTimeoutMs: 40_000 });
    const socket = FakeSocket.last.open().snapshot(0, {});
    vi.advanceTimersByTime(30_000);
    socket.frame({ type: "heartbeat", seq: 0, head: 0 }); // any frame re-arms the watchdog
    vi.advanceTimersByTime(39_000);
    expect(socket.closedWith).toBeNull();
    vi.advanceTimersByTime(1_000);
    expect(socket.closedWith).toBe(CloseCode.HEARTBEAT_TIMEOUT);
    expect(client.getSnapshot().status).toBe("reconnecting");
  });

  it("measures ping round trips and tracks the server head from heartbeats", () => {
    const client = makeClient({ pingIntervalMs: 10_000 });
    const socket = FakeSocket.last.open().snapshot(5, { status: "live" });
    vi.advanceTimersByTime(10_000);
    expect(socket.sent).toEqual(['{"type":"ping"}']);
    vi.advanceTimersByTime(42);
    socket.frame({ type: "pong" }).frame({ type: "heartbeat", seq: 5, head: 9 });
    expect(client.getSnapshot().stats).toMatchObject({ rttMs: 42, head: 9 });
  });

  it("waits for the network to come back instead of retrying while offline", () => {
    const client = makeClient();
    FakeSocket.last.open().snapshot(0, {}).events(...matchEvents(1, 2));
    network.go(false);
    expect(client.getSnapshot().status).toBe("offline");
    vi.advanceTimersByTime(60_000);
    expect(FakeSocket.all).toHaveLength(1);

    network.go(true);
    expect(FakeSocket.all).toHaveLength(2);
    expect(FakeSocket.last.url).toMatch(/\?cursor=2$/);
  });

  it("replaces its state when the server sends a newer snapshot (cursor too old)", () => {
    const client = makeClient();
    FakeSocket.last.open().snapshot(0, {}).events(...matchEvents(1, 2)).drop();
    vi.advanceTimersByTime(250);
    FakeSocket.last
      .open()
      .snapshot(50_000, { status: "live", teams: ["A", "B"], score: { A: 300, B: 0 }, events: 50_000 }, "cursor_expired")
      .events(ev(50_001, "score", { team: "A", points: 6 }));
    const s = client.getSnapshot();
    expect(s).toMatchObject({ cursor: 50_001, baseSeq: 0, state: { score: { A: 306 } } });
    expect(s.stats.snapshots).toBe(2);
  });

  it("keeps only the most recent events for display", () => {
    const client = makeClient({ recentLimit: 3 });
    FakeSocket.last.open().snapshot(0, {}).events(...matchEvents(1, 10));
    expect(client.getSnapshot().recent.map((e) => e.seq)).toEqual([10, 9, 8]);
  });

  it("can be stopped and started again, resuming from its cursor", () => {
    const client = makeClient();
    const listener = vi.fn();
    const unsubscribe = client.subscribe(listener);
    const socket = FakeSocket.last.open().snapshot(0, {});
    socket.events(...matchEvents(1, 3));
    expect(listener).toHaveBeenCalled();

    client.stop();
    expect(socket.closedWith).toBe(CloseCode.NORMAL);
    expect(client.getSnapshot().status).toBe("stopped");
    vi.advanceTimersByTime(60_000);
    expect(FakeSocket.all).toHaveLength(1);

    client.start();
    expect(FakeSocket.last.url).toMatch(/\?cursor=3$/);
    unsubscribe();
  });

  it("ignores frames it does not understand", () => {
    const client = makeClient();
    const socket = FakeSocket.last.open();
    socket.onmessage?.(new MessageEvent("message", { data: "not json" }));
    socket.frame({ type: "mystery" });
    expect(client.getSnapshot().state).toBeNull();
  });

  it("treats events before any snapshot as a gap", () => {
    const client = makeClient();
    FakeSocket.last.open().events(...matchEvents(1, 1));
    expect(client.getSnapshot().stats.gapsDetected).toBe(1);
  });
});

describe("streamUrl", () => {
  it("uses wss on https and escapes the stream id", () => {
    const url = streamUrl("a b", { protocol: "https:", host: "example.com" });
    expect(url(null)).toBe("wss://example.com/v1/streams/a%20b/ws");
    expect(url(7)).toBe("wss://example.com/v1/streams/a%20b/ws?cursor=7");
  });
});
