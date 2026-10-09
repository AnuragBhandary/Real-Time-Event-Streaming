import type { MatchState } from "../feed/reducers";
import { StatusBadge } from "./StatusBadge";

export function Scoreboard({ match }: { match: MatchState }) {
  const teams = match.teams ?? [];
  return (
    <section
      aria-label="Scoreboard"
      className="rounded-2xl bg-white p-5 shadow-sm ring-1 ring-slate-200 dark:bg-slate-900 dark:ring-slate-800"
    >
      <div className="mb-4 flex items-center justify-between">
        <StatusBadge status={match.status} />
        {match.status === "finished" && (
          <p className="text-sm font-medium" data-testid="result">
            {match.winner ? `${match.winner} won` : "Tied"}
          </p>
        )}
      </div>
      {teams.length === 0 ? (
        <p className="text-slate-500">Waiting for the toss…</p>
      ) : (
        <div className="grid grid-cols-2 gap-4" aria-live="polite">
          {teams.map((team) => (
            <div key={team} className="text-center" data-testid={`team-${team}`}>
              <p className="text-sm font-medium text-slate-500 dark:text-slate-400">{team}</p>
              <p className="text-4xl font-bold tabular-nums sm:text-5xl">
                <span data-testid={`runs-${team}`}>{match.score?.[team] ?? 0}</span>
                <span className="text-slate-400">/</span>
                <span data-testid={`wickets-${team}`}>{match.wickets?.[team] ?? 0}</span>
              </p>
            </div>
          ))}
        </div>
      )}
      {match.commentary && (
        <p className="mt-4 border-t border-slate-100 pt-3 text-sm italic text-slate-600 dark:border-slate-800 dark:text-slate-300">
          “{match.commentary}”
        </p>
      )}
    </section>
  );
}
