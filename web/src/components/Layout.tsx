import { Link, Outlet } from "react-router";

export function Layout() {
  return (
    <div className="min-h-dvh">
      <header className="border-b border-slate-200 bg-white/80 backdrop-blur dark:border-slate-800 dark:bg-slate-900/80">
        <div className="mx-auto flex max-w-5xl items-center justify-between px-4 py-3">
          <Link to="/" className="flex items-center gap-2 font-semibold tracking-tight">
            <span aria-hidden className="inline-block size-2.5 rounded-full bg-pitch" />
            livefeed
          </Link>
          <span className="text-sm text-slate-500 dark:text-slate-400">live cricket, exactly once</span>
        </div>
      </header>
      <main className="mx-auto max-w-5xl px-4 py-6">
        <Outlet />
      </main>
    </div>
  );
}
