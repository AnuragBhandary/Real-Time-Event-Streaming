// A scriptable stand-in for the browser WebSocket: tests play the server's part.

import type { FeedEvent } from "../feed/protocol";

export class FakeSocket {
  static all: FakeSocket[] = [];
  readyState: 0 | 1 | 2 | 3 = 0;
  sent: string[] = [];
  closedWith: number | null = null;
  onopen: ((ev: Event) => void) | null = null;
  onmessage: ((ev: MessageEvent) => void) | null = null;
  onclose: ((ev: CloseEvent) => void) | null = null;
  onerror: ((ev: Event) => void) | null = null;

  constructor(readonly url: string) {
    FakeSocket.all.push(this);
  }

  static reset(): void {
    FakeSocket.all = [];
  }

  static get last(): FakeSocket {
    const socket = FakeSocket.all.at(-1);
    if (!socket) throw new Error("no socket was opened");
    return socket;
  }

  static create = (url: string) => new FakeSocket(url);

  send(data: string): void {
    this.sent.push(data);
  }

  close(code = 1000): void {
    this.closedWith = code;
    this.readyState = 3;
  }

  // --- the server's side ---
  open(): this {
    this.readyState = 1;
    this.onopen?.(new Event("open"));
    return this;
  }

  frame(payload: unknown): this {
    this.onmessage?.(new MessageEvent("message", { data: JSON.stringify(payload) }));
    return this;
  }

  snapshot(seq: number, state: Record<string, unknown>, reason: string | null = null): this {
    return this.frame({ type: "snapshot", stream: "m1", seq, state, reason });
  }

  events(...items: FeedEvent[]): this {
    return this.frame({ type: "events", replay: false, items });
  }

  drop(code = 1006): void {
    this.readyState = 3;
    this.onclose?.(new CloseEvent("close", { code }));
  }
}

let nextId = 0;

export function ev(seq: number, type: string, data: Record<string, unknown> = {}): FeedEvent {
  return { stream: "m1", seq, id: `e${++nextId}`, type, data, ts: 0 };
}

/** A started match between A and B, then n single runs for A: seqs 1..n+1. */
export function matchEvents(from: number, to: number): FeedEvent[] {
  const out: FeedEvent[] = [];
  for (let seq = from; seq <= to; seq++) {
    out.push(
      seq === 1 ? ev(1, "match_started", { teams: ["A", "B"] }) : ev(seq, "score", { team: "A", points: 1 }),
    );
  }
  return out;
}
