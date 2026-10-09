const styles: Record<string, string> = {
  live: "bg-red-100 text-red-700 dark:bg-red-950 dark:text-red-300",
  finished: "bg-slate-200 text-slate-700 dark:bg-slate-800 dark:text-slate-300",
  scheduled: "bg-amber-100 text-amber-800 dark:bg-amber-950 dark:text-amber-300",
};

export function StatusBadge({ status }: { status: string }) {
  return (
    <span
      className={`inline-flex items-center gap-1.5 rounded-full px-2 py-0.5 text-xs font-medium uppercase tracking-wide ${styles[status] ?? styles.scheduled}`}
    >
      {status === "live" && <span aria-hidden className="size-1.5 animate-pulse rounded-full bg-current" />}
      {status}
    </span>
  );
}
