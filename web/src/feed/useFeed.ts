import { useEffect, useMemo, useSyncExternalStore } from "react";

import { FeedClient, streamUrl, type FeedClientOptions, type FeedSnapshot } from "./client";
import type { Reducer } from "./reducers";

type Overrides = Omit<FeedClientOptions, "url" | "reducer">;

/** Subscribe to a stream for as long as the component is mounted. Pass a null reducer to wait
 * (for example until the stream's kind is known). */
export function useFeed(
  streamId: string,
  reducer: Reducer | null,
  overrides?: Overrides,
): { feed: FeedSnapshot | null; client: FeedClient | null } {
  const client = useMemo(
    () =>
      reducer
        ? new FeedClient({ url: streamUrl(streamId, window.location), reducer, ...overrides })
        : null,
    // overrides are for tests and are fixed for a component's lifetime
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [streamId, reducer],
  );
  useEffect(() => {
    client?.start();
    return () => client?.stop();
  }, [client]);
  const feed = useSyncExternalStore(
    client ? client.subscribe : noSubscribe,
    client ? client.getSnapshot : noSnapshot,
  );
  return { feed, client };
}

const noSubscribe = () => () => {};
const noSnapshot = () => null;
