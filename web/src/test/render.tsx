import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render } from "@testing-library/react";
import type { ReactElement } from "react";
import { createMemoryRouter, RouterProvider } from "react-router";
import { vi } from "vitest";

/** Render `element` at `path` inside a fresh query client and an in-memory router. */
export function renderAt(path: string, pattern: string, element: ReactElement) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const router = createMemoryRouter(
    [
      { path: pattern, element },
      { path: "*", element: <p>elsewhere</p> },
    ],
    { initialEntries: [path] },
  );
  return render(
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>,
  );
}

/** Stub fetch with a handler keyed by URL path (+ query). Unknown URLs get a 500. */
export function mockApi(routes: Record<string, unknown | ((url: URL) => unknown)>) {
  const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
    const url = new URL(String(input), "http://test");
    const key = url.pathname + url.search;
    const match = routes[key] ?? routes[url.pathname];
    if (match === undefined) return new Response("no route", { status: 500 });
    const body = typeof match === "function" ? (match as (u: URL) => unknown)(url) : match;
    if (body instanceof Response) return body;
    return Response.json(body);
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}
