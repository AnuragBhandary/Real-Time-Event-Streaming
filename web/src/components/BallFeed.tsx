import type { FeedEvent } from "../feed/protocol";

function Ball({ event }: { event: FeedEvent }) {
  const d = event.data;
  switch (event.type) {
    case "score": {
      const points = d.points as number;
      const tone =
        points === 6
          ? "bg-violet-600 text-white"
          : points === 4
            ? "bg-sky-600 text-white"
            : "bg-slate-100 text-slate-700 dark:bg-slate-800 dark:text-slate-200";
      return (
        <>
          <span className={`grid size-7 place-items-center rounded-full text-sm font-semibold ${tone}`}>
            {points === 0 ? "•" : points}
          </span>
          <span>
            {String(d.team)} {points === 0 ? "dot ball" : `${points} run${points === 1 ? "" : "s"}`}
          </span>
        </>
      );
    }
    case "wicket":
      return (
        <>
          <span className="grid size-7 place-items-center rounded-full bg-red-600 text-sm font-semibold text-white">
            W
          </span>
          <span>{String(d.team)} lose a wicket</span>
        </>
      );
    case "commentary":
      return <span className="italic text-slate-600 dark:text-slate-300">{String(d.text)}</span>;
    case "match_started":
      return <span className="font-medium">Match started: {(d.teams as string[]).join(" vs ")}</span>;
    case "match_ended":
      return <span className="font-medium">Match ended</span>;
    default:
      return <span>{event.type}</span>;
  }
}

export function BallFeed({ events }: { events: FeedEvent[] }) {
  return (
    <section
      aria-label="Ball by ball"
      className="rounded-2xl bg-white p-5 shadow-sm ring-1 ring-slate-200 dark:bg-slate-900 dark:ring-slate-800"
    >
      <h2 className="mb-3 font-semibold">Ball by ball</h2>
      {events.length === 0 ? (
        <p className="text-sm text-slate-500">Nothing yet.</p>
      ) : (
        <ol className="max-h-[28rem] space-y-2 overflow-y-auto pr-1 text-sm">
          {events.map((event) => (
            <li key={event.seq} className="flex items-center gap-3" data-seq={event.seq}>
              <span className="w-10 shrink-0 text-right font-mono text-xs text-slate-400">#{event.seq}</span>
              <Ball event={event} />
            </li>
          ))}
        </ol>
      )}
    </section>
  );
}
