// Wire formats, mirroring src/livefeed/protocol.py.
//
// Server -> client frames:
//   {"type": "snapshot",  "stream": id, "seq": n, "state": {...}, "reason": null | "cursor_expired"}
//   {"type": "events",    "replay": bool, "items": [event, ...]}   items have seq, strictly +1
//   {"type": "heartbeat", "seq": last_seq_sent_to_you, "head": latest_seq_known_to_server}
//   {"type": "pong"}
// Client -> server: {"type": "ping"}.

export interface FeedEvent {
  stream: string;
  seq: number;
  id: string;
  type: string;
  data: Record<string, unknown>;
  ts: number; // server commit time, ms since the epoch
}

export interface SnapshotFrame {
  type: "snapshot";
  stream: string;
  seq: number;
  state: Record<string, unknown>;
  reason: "cursor_expired" | null;
}

export interface EventsFrame {
  type: "events";
  replay: boolean;
  items: FeedEvent[];
}

export interface HeartbeatFrame {
  type: "heartbeat";
  seq: number;
  head: number;
}

export interface PongFrame {
  type: "pong";
}

export type ServerFrame = SnapshotFrame | EventsFrame | HeartbeatFrame | PongFrame;

/** Close codes. 1xxx come from the WebSocket spec, 4000-4003 are this client's own. */
export const CloseCode = {
  NORMAL: 1000,
  ABNORMAL: 1006, // connection dropped without a close frame (network, crash)
  SERVICE_RESTART: 1012, // instance shutting down: reconnect to another one
  CLIENT_RESYNC: 4000, // this client saw a seq gap and is reconnecting from its cursor
  HEARTBEAT_TIMEOUT: 4001, // this client heard nothing for too long
  USER_DROP: 4002, // "simulate network drop" button
  STREAM_NOT_FOUND: 4004,
  SLOW_CONSUMER: 4008, // fell too far behind: reconnect with the cursor
  RESYNC_REQUIRED: 4009, // the server saw a gap for this client
} as const;

/** Codes after which reconnecting straight away is right: nothing is wrong with the network. */
export const IMMEDIATE_RECONNECT: ReadonlySet<number> = new Set([
  CloseCode.SERVICE_RESTART,
  CloseCode.CLIENT_RESYNC,
  CloseCode.HEARTBEAT_TIMEOUT,
  CloseCode.USER_DROP,
  CloseCode.SLOW_CONSUMER,
  CloseCode.RESYNC_REQUIRED,
]);

export function describeClose(code: number | null): string {
  switch (code) {
    case null:
      return "—";
    case CloseCode.NORMAL:
      return "closed normally";
    case CloseCode.ABNORMAL:
      return "connection lost";
    case CloseCode.SERVICE_RESTART:
      return "server restarting";
    case CloseCode.CLIENT_RESYNC:
      return "gap detected, resyncing";
    case CloseCode.HEARTBEAT_TIMEOUT:
      return "heartbeat timeout";
    case CloseCode.USER_DROP:
      return "dropped on purpose";
    case CloseCode.STREAM_NOT_FOUND:
      return "stream not found";
    case CloseCode.SLOW_CONSUMER:
      return "too slow, resuming";
    case CloseCode.RESYNC_REQUIRED:
      return "server asked to resync";
    default:
      return `code ${code}`;
  }
}

export function parseFrame(raw: string): ServerFrame | null {
  try {
    const frame = JSON.parse(raw) as { type?: unknown };
    switch (frame.type) {
      case "snapshot":
      case "events":
      case "heartbeat":
      case "pong":
        return frame as ServerFrame;
      default:
        return null;
    }
  } catch {
    return null;
  }
}
