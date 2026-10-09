// REST calls to the livefeed API (same origin; Vite proxies /v1 in development).

import type { FeedEvent } from "./feed/protocol";

export interface StreamInfo {
  id: string;
  kind: string;
  seq: number;
  state: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export class NotFoundError extends Error {}

async function getJson<T>(path: string): Promise<T> {
  const response = await fetch(path, { headers: { Accept: "application/json" } });
  if (response.status === 404) throw new NotFoundError(path);
  if (!response.ok) throw new Error(`${response.status} from ${path}`);
  return (await response.json()) as T;
}

export const listMatches = (limit = 24) =>
  getJson<StreamInfo[]>(`/v1/streams?kind=match&limit=${limit}`);

export const getStream = (id: string) =>
  getJson<StreamInfo>(`/v1/streams/${encodeURIComponent(id)}`);

/** Events with after < seq, oldest first (used to show recent balls when joining mid-match). */
export const getEvents = (id: string, after: number, limit: number) =>
  getJson<{ items: FeedEvent[]; next_after: number | null }>(
    `/v1/streams/${encodeURIComponent(id)}/events?after=${after}&limit=${limit}`,
  ).then((page) => page.items);
