import { act, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { CloseCode } from "../feed/protocol";
import { ev, FakeSocket, matchEvents } from "../test/fakeSocket";
import { mockApi, renderAt } from "../test/render";
import { MatchPage } from "./MatchPage";

const info = { id: "m1", kind: "match", seq: 0, state: {}, created_at: "", updated_at: "" };
const options = { createSocket: FakeSocket.create, network: null };

function renderMatch() {
  return renderAt("/m/m1", "/m/:streamId", <MatchPage feedOptions={options} />);
}

async function socket(): Promise<FakeSocket> {
  await vi.waitFor(() => expect(FakeSocket.all.length).toBeGreaterThan(0));
  return FakeSocket.last;
}

beforeEach(() => FakeSocket.reset());
afterEach(() => vi.unstubAllGlobals());

describe("MatchPage", () => {
  it("shows the live score, ball by ball, and the delivery guarantees", async () => {
    mockApi({ "/v1/streams/m1": info });
    renderMatch();
    const ws = await socket();
    act(() => {
      ws.open().snapshot(0, {});
      ws.events(
        ...matchEvents(1, 2),
        ev(3, "score", { team: "A", points: 6 }),
        ev(4, "wicket", { team: "A" }),
        ev(5, "commentary", { text: "Huge hit!" }),
      );
    });
    expect(screen.getByTestId("runs-A")).toHaveTextContent("7");
    expect(screen.getByTestId("wickets-A")).toHaveTextContent("1");
    expect(screen.getByText("“Huge hit!”")).toBeInTheDocument();
    const feed = screen.getByRole("region", { name: "Ball by ball" });
    expect(within(feed).getAllByRole("listitem").map((li) => li.dataset.seq)).toEqual(["5", "4", "3", "2", "1"]);
    expect(screen.getByTestId("conn-status")).toHaveTextContent("live");
    expect(screen.getByTestId("stat-applied")).toHaveTextContent("5");

    act(() => ws.events(...matchEvents(4, 5), ev(6, "match_ended")));
    expect(screen.getByTestId("stat-duplicates")).toHaveTextContent("2");
    expect(screen.getByTestId("result")).toHaveTextContent("A won");
  });

  it("drops the connection on request and resumes from the cursor", async () => {
    mockApi({ "/v1/streams/m1": info });
    renderMatch();
    const ws = await socket();
    act(() => void ws.open().snapshot(0, {}).events(...matchEvents(1, 3)));

    await userEvent.click(screen.getByRole("button", { name: "Simulate network drop" }));
    expect(ws.closedWith).toBe(CloseCode.USER_DROP);
    await vi.waitFor(() => expect(FakeSocket.all).toHaveLength(2));
    expect(FakeSocket.last.url).toMatch(/\?cursor=3$/);
    act(() => void FakeSocket.last.open().events(...matchEvents(2, 4)));
    expect(screen.getByTestId("stat-reconnects")).toHaveTextContent("1");
    expect(screen.getByTestId("stat-applied")).toHaveTextContent("4");
    expect(screen.getByTestId("stat-close")).toHaveTextContent("dropped on purpose");
  });

  it("joining mid-match shows the snapshot and backfills recent balls over REST", async () => {
    const fetchMock = mockApi({
      "/v1/streams/m1": { ...info, seq: 40 },
      "/v1/streams/m1/events": (url: URL) => {
        const after = Number(url.searchParams.get("after"));
        return { items: matchEvents(after + 1, 40), next_after: null };
      },
    });
    renderMatch();
    const ws = await socket();
    act(() => {
      ws.open().snapshot(40, {
        status: "live", teams: ["A", "B"], score: { A: 39, B: 0 }, wickets: { A: 0, B: 0 }, events: 40,
      });
      ws.events(...matchEvents(41, 41));
    });
    expect(screen.getByTestId("runs-A")).toHaveTextContent("40");
    await vi.waitFor(() => expect(screen.getAllByRole("listitem")).toHaveLength(31));
    const seqs = screen.getAllByRole("listitem").map((li) => Number(li.dataset.seq));
    expect(seqs[0]).toBe(41);
    expect(seqs.at(-1)).toBe(11);
    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining("/v1/streams/m1/events?after=10&limit=30"),
      expect.anything(),
    );
  });

  it("drops the backfill once live events no longer reach back to it, so the list has no hole", async () => {
    mockApi({
      "/v1/streams/m1": { ...info, seq: 40 },
      "/v1/streams/m1/events": { items: matchEvents(11, 40), next_after: null },
    });
    renderAt("/m/m1", "/m/:streamId", <MatchPage feedOptions={{ ...options, recentLimit: 5 }} />);
    const ws = await socket();
    act(() => void ws.open().snapshot(40, { status: "live", teams: ["A", "B"], score: { A: 39, B: 0 }, wickets: { A: 0, B: 0 }, events: 40 }));
    act(() => void ws.events(...matchEvents(41, 43)));
    await vi.waitFor(() => expect(screen.getAllByRole("listitem")).toHaveLength(33));
    act(() => void ws.events(...matchEvents(44, 50)));
    const seqs = screen.getAllByRole("listitem").map((li) => Number(li.dataset.seq));
    expect(seqs).toEqual([50, 49, 48, 47, 46]);
  });

  it("says so when the match does not exist", async () => {
    mockApi({ "/v1/streams/m1": new Response("{}", { status: 404 }) });
    renderMatch();
    expect(await screen.findByText("Match not found")).toBeInTheDocument();
    expect(FakeSocket.all).toHaveLength(0);
  });

  it("shows other stream kinds as raw state", async () => {
    mockApi({ "/v1/streams/m1": { ...info, kind: "generic" } });
    renderMatch();
    const ws = await socket();
    act(() => void ws.open().snapshot(0, {}).events(ev(1, "ping", { n: 1 })));
    expect(screen.getByText(/"events": 1/)).toBeInTheDocument();
  });
});
