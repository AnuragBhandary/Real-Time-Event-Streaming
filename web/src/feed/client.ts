// A resumable WebSocket subscription to one stream, for the browser.
//
// It keeps a cursor (the last seq folded into state) and holds the server's guarantee to account:
// every item must be exactly cursor + 1. An older seq is a duplicate and is dropped; a newer one is
// a gap, so the client abandons the socket and reconnects from its cursor. Every failure (network
// drop, server restart, slow consumer, missed heartbeats, a detected gap) ends in the same place:
// reconnect with ?cursor=N, and the server replays exactly what was missed. Snapshot and events are
// held in one immutable object that changes identity on every update, so React can subscribe to it
// with useSyncExternalStore.

import {
  CloseCode,
  IMMEDIATE_RECONNECT,
  parseFrame,
  type FeedEvent,
  type ServerFrame,
} from "./protocol";
import type { Reducer, State } from "./reducers";

export type ConnectionStatus =
  | "idle"
  | "connecting"
  | "live"
  | "reconnecting"
  | "offline"
  | "not_found"
  | "stopped";

export interface FeedStats {
  applied: number; // events folded into state; each seq exactly once
  duplicatesDropped: number;
  gapsDetected: number;
  snapshots: number;
  reconnects: number;
  lastCloseCode: number | null;
  head: number | null; // latest seq the server knows about (from heartbeats)
  rttMs: number | null; // application-level ping round trip
}

export interface FeedSnapshot {
  status: ConnectionStatus;
  cursor: number | null;
  /** Seq of the first snapshot this client received; events after it are in `recent`. */
  baseSeq: number | null;
  state: State | null;
  recent: FeedEvent[]; // newest first
  stats: FeedStats;
}

type SocketLike = Pick<WebSocket, "close" | "send" | "readyState"> & {
  onopen: ((ev: Event) => void) | null;
  onmessage: ((ev: MessageEvent) => void) | null;
  onclose: ((ev: CloseEvent) => void) | null;
  onerror: ((ev: Event) => void) | null;
};

export interface FeedClientOptions {
  /** Builds the subscription URL; cursor is null on the very first connect. */
  url: (cursor: number | null) => string;
  reducer: Reducer;
  createSocket?: (url: string) => SocketLike;
  /** Window-like target for online/offline events; null disables it (tests, SSR). */
  network?: Pick<Window, "addEventListener" | "removeEventListener"> & {
    navigator?: { onLine: boolean };
  } | null;
  random?: () => number;
  now?: () => number;
  recentLimit?: number;
  backoffBaseMs?: number;
  backoffMaxMs?: number;
  /** A connection that stays up this long resets the backoff. */
  stableAfterMs?: number;
  /** No frame for this long means the connection is dead. The server heartbeats every 15 s. */
  heartbeatTimeoutMs?: number;
  pingIntervalMs?: number;
}

const OPEN = 1;

export class FeedClient {
  private readonly opts: Required<Omit<FeedClientOptions, "network">> & {
    network: FeedClientOptions["network"];
  };
  private snapshot: FeedSnapshot;
  private readonly listeners = new Set<() => void>();
  private socket: SocketLike | null = null;
  private generation = 0; // bumps whenever a socket is abandoned, so its late events are ignored
  private attempts = 0;
  private stopped = true;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private watchdog: ReturnType<typeof setTimeout> | null = null;
  private stableTimer: ReturnType<typeof setTimeout> | null = null;
  private pingTimer: ReturnType<typeof setInterval> | null = null;
  private pingSentAt: number | null = null;

  constructor(options: FeedClientOptions) {
    this.opts = {
      createSocket: (url) => new WebSocket(url),
      network: typeof window === "undefined" ? null : window,
      random: Math.random,
      now: () => performance.now(),
      recentLimit: 60,
      backoffBaseMs: 500,
      backoffMaxMs: 10_000,
      stableAfterMs: 5_000,
      heartbeatTimeoutMs: 40_000,
      pingIntervalMs: 10_000,
      ...options,
    };
    this.snapshot = {
      status: "idle",
      cursor: null,
      baseSeq: null,
      state: null,
      recent: [],
      stats: {
        applied: 0,
        duplicatesDropped: 0,
        gapsDetected: 0,
        snapshots: 0,
        reconnects: 0,
        lastCloseCode: null,
        head: null,
        rttMs: null,
      },
    };
  }

  // --- the store interface React uses ------------------------------------------------------

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  getSnapshot = (): FeedSnapshot => this.snapshot;

  // --- lifecycle -----------------------------------------------------------------------------

  start(): void {
    if (!this.stopped) return;
    this.stopped = false;
    this.opts.network?.addEventListener("online", this.onOnline);
    this.opts.network?.addEventListener("offline", this.onOffline);
    this.connect();
  }

  /** Close for good (the component unmounted). The cursor is kept, so start() resumes. */
  stop(): void {
    if (this.stopped) return;
    this.stopped = true;
    this.opts.network?.removeEventListener("online", this.onOnline);
    this.opts.network?.removeEventListener("offline", this.onOffline);
    this.clearTimers();
    this.abandonSocket(CloseCode.NORMAL);
    this.update({ status: "stopped" });
  }

  /** Demo control: kill the connection as a network failure would, and watch it resume. */
  dropConnection(): void {
    if (this.socket) this.lose(CloseCode.USER_DROP);
  }

  // --- connecting ------------------------------------------------------------------------------

  private connect(): void {
    this.reconnectTimer = null;
    if (this.stopped) return;
    const { cursor, stats } = this.snapshot;
    const first = cursor === null && stats.lastCloseCode === null;
    const generation = ++this.generation;
    const socket = this.opts.createSocket(this.opts.url(cursor));
    this.socket = socket;
    this.update({
      status: first ? "connecting" : "reconnecting",
      stats: first ? stats : { ...stats, reconnects: stats.reconnects + 1 },
    });

    socket.onopen = () => {
      if (generation !== this.generation) return;
      this.update({ status: "live" });
      this.armWatchdog();
      this.stableTimer = setTimeout(() => (this.attempts = 0), this.opts.stableAfterMs);
      this.pingTimer = setInterval(this.ping, this.opts.pingIntervalMs);
    };
    socket.onmessage = (ev) => {
      if (generation !== this.generation) return;
      this.armWatchdog();
      const frame = parseFrame(String(ev.data));
      if (frame) this.handle(frame);
    };
    socket.onclose = (ev) => {
      if (generation !== this.generation) return;
      this.closed(ev.code);
    };
    socket.onerror = () => {
      // Always followed by a close event, which does the work.
    };
  }

  /** Give up on the current socket now, as if it had closed with `code`. */
  private lose(code: number): void {
    this.abandonSocket(code);
    this.closed(code);
  }

  private abandonSocket(code: number): void {
    const socket = this.socket;
    if (!socket) return;
    this.generation++; // ignore anything the old socket still delivers
    this.socket = null;
    try {
      socket.close(code);
    } catch {
      // already closing
    }
  }

  private closed(code: number): void {
    this.socket = null;
    this.clearTimers();
    this.update({ stats: { ...this.snapshot.stats, lastCloseCode: code } });
    if (this.stopped) return;
    if (code === CloseCode.STREAM_NOT_FOUND) {
      this.update({ status: "not_found" });
      return;
    }
    if (this.opts.network?.navigator?.onLine === false) {
      this.update({ status: "offline" }); // the online event reconnects
      return;
    }
    this.update({ status: "reconnecting" });
    this.reconnectTimer = setTimeout(() => this.connect(), this.delayAfter(code));
  }

  /** Straight back for server restarts and resyncs (small jitter so a restarted instance's
   * clients do not all arrive at once); exponential backoff with full jitter otherwise. */
  private delayAfter(code: number): number {
    if (IMMEDIATE_RECONNECT.has(code)) return this.opts.random() * 250;
    const cap = Math.min(this.opts.backoffMaxMs, this.opts.backoffBaseMs * 2 ** this.attempts);
    this.attempts++;
    return this.opts.random() * cap;
  }

  private onOnline = (): void => {
    if (this.stopped || this.socket) return;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.attempts = 0;
    this.connect();
  };

  private onOffline = (): void => {
    if (this.stopped) return;
    if (this.socket) this.lose(CloseCode.ABNORMAL);
    else this.update({ status: "offline" });
  };

  // --- frames ----------------------------------------------------------------------------------

  private handle(frame: ServerFrame): void {
    switch (frame.type) {
      case "snapshot": {
        const { stats, baseSeq } = this.snapshot;
        this.update({
          state: frame.state,
          cursor: frame.seq,
          baseSeq: baseSeq ?? frame.seq,
          stats: { ...stats, snapshots: stats.snapshots + 1 },
        });
        return;
      }
      case "events":
        this.applyEvents(frame.items);
        return;
      case "heartbeat":
        this.update({ stats: { ...this.snapshot.stats, head: frame.head } });
        return;
      case "pong":
        if (this.pingSentAt !== null) {
          const rttMs = Math.round(this.opts.now() - this.pingSentAt);
          this.pingSentAt = null;
          this.update({ stats: { ...this.snapshot.stats, rttMs } });
        }
        return;
    }
  }

  private applyEvents(items: FeedEvent[]): void {
    let { cursor, state } = this.snapshot;
    const stats = { ...this.snapshot.stats };
    const fresh: FeedEvent[] = [];
    let gap = false;
    for (const item of items) {
      if (cursor !== null && item.seq <= cursor) {
        stats.duplicatesDropped++;
        continue;
      }
      if (cursor === null || state === null || item.seq !== cursor + 1) {
        gap = true; // never apply past a hole: resync from the cursor instead
        stats.gapsDetected++;
        break;
      }
      state = this.opts.reducer(state, item.type, item.data);
      cursor = item.seq;
      stats.applied++;
      fresh.push(item);
    }
    const recent = fresh.length
      ? [...fresh.reverse(), ...this.snapshot.recent].slice(0, this.opts.recentLimit)
      : this.snapshot.recent;
    this.update({ cursor, state, recent, stats });
    if (gap) this.lose(CloseCode.CLIENT_RESYNC);
  }

  private ping = (): void => {
    if (this.socket?.readyState !== OPEN) return;
    this.pingSentAt = this.opts.now();
    this.socket.send('{"type":"ping"}');
  };

  // --- timers ----------------------------------------------------------------------------------

  private armWatchdog(): void {
    if (this.watchdog) clearTimeout(this.watchdog);
    this.watchdog = setTimeout(
      () => this.lose(CloseCode.HEARTBEAT_TIMEOUT),
      this.opts.heartbeatTimeoutMs,
    );
  }

  private clearTimers(): void {
    for (const timer of [this.reconnectTimer, this.watchdog, this.stableTimer]) {
      if (timer) clearTimeout(timer);
    }
    if (this.pingTimer) clearInterval(this.pingTimer);
    this.reconnectTimer = this.watchdog = this.stableTimer = this.pingTimer = null;
    this.pingSentAt = null;
  }

  private update(patch: Partial<FeedSnapshot>): void {
    this.snapshot = { ...this.snapshot, ...patch };
    for (const listener of this.listeners) listener();
  }
}

/** ws(s)://<this host>/v1/streams/<id>/ws[?cursor=N] */
export function streamUrl(streamId: string, location: Pick<Location, "protocol" | "host">) {
  const scheme = location.protocol === "https:" ? "wss" : "ws";
  const base = `${scheme}://${location.host}/v1/streams/${encodeURIComponent(streamId)}/ws`;
  return (cursor: number | null) => (cursor === null ? base : `${base}?cursor=${cursor}`);
}
