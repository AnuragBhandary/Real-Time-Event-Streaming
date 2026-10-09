import { screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { mockApi, renderAt } from "../test/render";
import { MatchListPage } from "./MatchListPage";

const stream = (id: string, state: Record<string, unknown>) => ({
  id,
  kind: "match",
  seq: 10,
  state,
  created_at: "2026-10-09T00:00:00Z",
  updated_at: "2026-10-09T00:00:00Z",
});

afterEach(() => vi.unstubAllGlobals());

describe("MatchListPage", () => {
  it("lists matches with their score and status, linking to each", async () => {
    mockApi({
      "/v1/streams": [
        stream("final", { status: "live", teams: ["MI", "CSK"], score: { MI: 87, CSK: 0 }, wickets: { MI: 2, CSK: 0 } }),
        stream("semi", { status: "finished", teams: ["RR", "GT"], score: { RR: 150, GT: 151 }, wickets: { RR: 9, GT: 4 }, winner: "GT" }),
        stream("later", {}),
      ],
    });
    renderAt("/", "/", <MatchListPage />);
    expect(await screen.findByText("87/2")).toBeInTheDocument();
    expect(screen.getByText("GT won")).toBeInTheDocument();
    expect(screen.getByText("Not started")).toBeInTheDocument();
    expect(screen.getByText("final").closest("a")).toHaveAttribute("href", "/m/final");
  });

  it("explains how to start matches when there are none", async () => {
    mockApi({ "/v1/streams": [] });
    renderAt("/", "/", <MatchListPage />);
    expect(await screen.findByText(/No matches yet/)).toBeInTheDocument();
  });

  it("shows an error when the API fails", async () => {
    mockApi({});
    renderAt("/", "/", <MatchListPage />);
    expect(await screen.findByRole("alert")).toHaveTextContent("Could not load matches");
  });
});
