import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router";

import { listMatches, type StreamInfo } from "../api";
import { StatusBadge } from "../components/StatusBadge";
import type { MatchState } from "../feed/reducers";

function MatchCard({ stream }: { stream: StreamInfo }) {
  const match = stream.state as unknown as Partial<MatchState>;
  const teams = match.teams ?? [];
  return (
    <Link
      to={`/m/${encodeURIComponent(stream.id)}`}
      className="block rounded-2xl bg-white p-4 shadow-sm ring-1 ring-slate-200 transition hover:-translate-y-0.5 hover:shadow-md dark:bg-slate-900 dark:ring-slate-800"
    >
      <div className="mb-3 flex items-center justify-between">
        <StatusBadge status={match.status ?? "scheduled"} />
        <span className="font-mono text-xs text-slate-400">{stream.id}</span>
      </div>
      {teams.length === 0 ? (
        <p className="text-sm text-slate-500">Not started</p>
      ) : (
        <ul className="space-y-1">
          {teams.map((team) => (
            <li key={team} className="flex justify-between font-medium">
              <span>{team}</span>
              <span className="tabular-nums">
                {match.score?.[team] ?? 0}/{match.wickets?.[team] ?? 0}
              </span>
            </li>
          ))}
        </ul>
      )}
      {match.status === "finished" && (
        <p className="mt-2 text-xs text-slate-500">{match.winner ? `${match.winner} won` : "Tied"}</p>
      )}
    </Link>
  );
}

export function MatchListPage() {
  const { data, error, isPending } = useQuery({
    queryKey: ["matches"],
    queryFn: () => listMatches(),
    refetchInterval: 2_000,
  });
  return (
    <>
      <h1 className="mb-4 text-2xl font-bold tracking-tight">Matches</h1>
      {isPending ? (
        <p className="text-slate-500">Loading…</p>
      ) : error ? (
        <p role="alert" className="text-red-600">
          Could not load matches ({error.message}).
        </p>
      ) : data.length === 0 ? (
        <div className="rounded-2xl border border-dashed border-slate-300 p-8 text-center text-slate-500 dark:border-slate-700">
          No matches yet. Start some with <code className="font-mono">make demo</code>.
        </div>
      ) : (
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
          {data.map((stream) => (
            <MatchCard key={stream.id} stream={stream} />
          ))}
        </div>
      )}
    </>
  );
}
