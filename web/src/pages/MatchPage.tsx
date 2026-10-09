import { useQuery } from "@tanstack/react-query";
import { useMemo } from "react";
import { Link, useParams } from "react-router";

import { getEvents, getStream, NotFoundError } from "../api";
import { BallFeed } from "../components/BallFeed";
import { ConnectionPanel } from "../components/ConnectionPanel";
import { Scoreboard } from "../components/Scoreboard";
import type { FeedClientOptions } from "../feed/client";
import { reducerFor, type MatchState } from "../feed/reducers";
import { useFeed } from "../feed/useFeed";

const BACKFILL = 30;

type FeedOverrides = Omit<FeedClientOptions, "url" | "reducer">;

export function MatchPage({ feedOptions }: { feedOptions?: FeedOverrides }) {
  const { streamId = "" } = useParams();
  const info = useQuery({
    queryKey: ["stream", streamId],
    queryFn: () => getStream(streamId),
    staleTime: Infinity,
    retry: (count, error) => !(error instanceof NotFoundError) && count < 1,
  });
  const reducer = info.data ? reducerFor(info.data.kind) : null;
  const { feed, client } = useFeed(streamId, reducer, feedOptions);

  // Joining mid-match gives a snapshot, not history: fetch the last few balls to show.
  const base = feed?.baseSeq ?? null;
  const history = useQuery({
    queryKey: ["history", streamId, base],
    queryFn: () => getEvents(streamId, Math.max(0, base! - BACKFILL), BACKFILL),
    enabled: base !== null && base > 0,
    staleTime: Infinity,
  });
  const events = useMemo(() => {
    const recent = feed?.recent ?? [];
    // Only append the backfill while the live list still reaches back to it; once older live
    // events have been trimmed, adding it would leave a hole in the list.
    const oldest = recent.at(-1)?.seq ?? (base ?? 0) + 1;
    if (base === null || oldest !== base + 1) return recent;
    const older = (history.data ?? []).filter((e) => e.seq <= base).reverse();
    return [...recent, ...older];
  }, [feed?.recent, history.data, base]);

  if (info.error instanceof NotFoundError || feed?.status === "not_found") {
    return (
      <div className="text-center">
        <h1 className="mb-2 text-2xl font-bold">Match not found</h1>
        <p className="mb-4 text-slate-500">
          There is no stream called <code className="font-mono">{streamId}</code>.
        </p>
        <Link to="/" className="text-pitch underline">
          Back to matches
        </Link>
      </div>
    );
  }
  if (info.error) {
    return (
      <p role="alert" className="text-red-600">
        Could not load the match ({info.error.message}).
      </p>
    );
  }

  return (
    <div className="space-y-4">
      <div className="flex items-baseline justify-between gap-2">
        <h1 className="truncate text-2xl font-bold tracking-tight">{streamId}</h1>
        <Link to="/" className="shrink-0 text-sm text-slate-500 hover:underline">
          All matches
        </Link>
      </div>
      {!feed || feed.state === null ? (
        <p className="text-slate-500">Connecting…</p>
      ) : info.data?.kind === "match" ? (
        <Scoreboard match={feed.state as unknown as MatchState} />
      ) : (
        <pre className="overflow-x-auto rounded-2xl bg-white p-4 text-xs ring-1 ring-slate-200 dark:bg-slate-900 dark:ring-slate-800">
          {JSON.stringify(feed.state, null, 2)}
        </pre>
      )}
      <div className="grid gap-4 lg:grid-cols-[1fr_1.1fr]">
        <BallFeed events={events} />
        {feed && client && <ConnectionPanel feed={feed} onDrop={() => client.dropConnection()} />}
      </div>
    </div>
  );
}
