import type { FeedSnapshot } from "../feed/client";
import { describeClose } from "../feed/protocol";

const dot: Record<string, string> = {
  live: "bg-emerald-500",
  connecting: "bg-amber-400 animate-pulse",
  reconnecting: "bg-amber-400 animate-pulse",
  offline: "bg-slate-400",
  not_found: "bg-red-500",
  stopped: "bg-slate-400",
  idle: "bg-slate-400",
};

function Stat({ label, value, id }: { label: string; value: string | number; id: string }) {
  return (
    <div>
      <dt className="text-xs text-slate-500 dark:text-slate-400">{label}</dt>
      <dd className="font-mono text-sm tabular-nums" data-testid={id}>
        {value}
      </dd>
    </div>
  );
}

/** The guarantees, made visible: what the client applied, what it dropped, how it recovered. */
export function ConnectionPanel({ feed, onDrop }: { feed: FeedSnapshot; onDrop: () => void }) {
  const { stats, cursor } = feed;
  const lag = stats.head !== null && cursor !== null ? Math.max(0, stats.head - cursor) : null;
  return (
    <section
      aria-label="Connection"
      className="rounded-2xl bg-white p-5 shadow-sm ring-1 ring-slate-200 dark:bg-slate-900 dark:ring-slate-800"
    >
      <div className="mb-4 flex items-center justify-between gap-2">
        <p className="flex items-center gap-2 text-sm font-medium">
          <span aria-hidden className={`size-2.5 rounded-full ${dot[feed.status]}`} />
          <span data-testid="conn-status">{feed.status.replace("_", " ")}</span>
        </p>
        <button
          type="button"
          onClick={onDrop}
          disabled={feed.status !== "live"}
          className="rounded-lg border border-slate-300 px-3 py-1.5 text-sm font-medium hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-50 dark:border-slate-700 dark:hover:bg-slate-800"
        >
          Simulate network drop
        </button>
      </div>
      <dl className="grid grid-cols-2 gap-x-4 gap-y-3 sm:grid-cols-4">
        <Stat label="Cursor (seq)" value={cursor ?? "—"} id="stat-cursor" />
        <Stat label="Events applied" value={stats.applied} id="stat-applied" />
        <Stat label="Duplicates dropped" value={stats.duplicatesDropped} id="stat-duplicates" />
        <Stat label="Gaps resynced" value={stats.gapsDetected} id="stat-gaps" />
        <Stat label="Reconnects" value={stats.reconnects} id="stat-reconnects" />
        <Stat label="Snapshots" value={stats.snapshots} id="stat-snapshots" />
        <Stat label="Behind server" value={lag ?? "—"} id="stat-lag" />
        <Stat label="Ping" value={stats.rttMs === null ? "—" : `${stats.rttMs} ms`} id="stat-rtt" />
      </dl>
      <p className="mt-3 text-xs text-slate-500 dark:text-slate-400">
        Last close: <span data-testid="stat-close">{describeClose(stats.lastCloseCode)}</span>
      </p>
    </section>
  );
}
