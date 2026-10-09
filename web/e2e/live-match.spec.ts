// The browser's delivery guarantee, tested end to end against the real stack.
//
// A producer publishes a simulated match while three browser tabs watch it. During the match:
// one tab's connection is dropped over and over, every server instance is restarted (graceful,
// close 1012), one is SIGKILLed (abrupt, no close frame), and a third tab joins half way through.
// At the end every tab must show exactly the server's final state, and the two tabs that watched
// from the start must have applied every event exactly once: applied == last seq.

import { exec } from "node:child_process";
import { randomUUID } from "node:crypto";
import { resolve } from "node:path";
import { promisify } from "node:util";

import { expect, test, type APIRequestContext, type Page } from "@playwright/test";

const run = promisify(exec);
const REPO = resolve(import.meta.dirname, "..", "..");
const KEY = process.env.E2E_API_KEY ?? "lf_0000000000de_local-dev-key-not-for-production";
const EVENTS = Number(process.env.E2E_EVENTS ?? 600);
const CHAOS = process.env.E2E_CHAOS !== "0";
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Deterministic pseudo-random numbers, so a failing run can be replayed. */
function prng(seed: number) {
  return () => {
    seed = (seed * 1103515245 + 12345) % 2 ** 31;
    return seed / 2 ** 31;
  };
}

/** Publish one event, retrying through restarts. The event_id makes retries safe: the server
 * returns the original seq instead of appending twice. */
async function publish(api: APIRequestContext, stream: string, type: string, data: object) {
  const body = { events: [{ event_id: randomUUID(), type, data }] };
  for (let attempt = 0; ; attempt++) {
    try {
      const res = await api.post(`/v1/streams/${stream}/events`, {
        data: body,
        headers: { "X-API-Key": KEY },
        timeout: 5_000,
      });
      if (res.ok()) return;
      if (res.status() < 500 && res.status() !== 429) throw new Error(`${res.status()} ${await res.text()}`);
    } catch (error) {
      if (attempt > 60) throw error;
    }
    await sleep(100);
  }
}

async function stat(page: Page, id: string) {
  return Number(await page.getByTestId(id).textContent());
}

async function compose(args: string) {
  await run(`docker compose ${args}`, { cwd: REPO });
}

test("every tab ends with the server's exact state through drops, restarts and a crash", async ({
  browser,
  request,
}, testInfo) => {
  const stream = `e2e-${Date.now()}`;
  const created = await request.post("/v1/streams", {
    data: { id: stream, kind: "match" },
    headers: { "X-API-Key": KEY },
  });
  expect(created.status()).toBe(201);

  const context = await browser.newContext();
  const [steady, dropped] = [await context.newPage(), await context.newPage()];
  for (const page of [steady, dropped]) {
    await page.goto(`/m/${stream}`);
    await expect(page.getByTestId("conn-status")).toHaveText("live");
  }

  const random = prng(2026);
  const teams = ["MI", "CSK"];
  let published = 0;
  const producer = (async () => {
    await publish(request, stream, "match_started", { teams });
    for (let i = 0; i < EVENTS; i++) {
      const team = teams[i < EVENTS / 2 ? 0 : 1]!;
      const roll = random();
      if (roll < 0.04) await publish(request, stream, "wicket", { team });
      else if (roll < 0.1) await publish(request, stream, "commentary", { text: `ball ${i}` });
      else await publish(request, stream, "score", { team, points: [0, 1, 1, 2, 4, 6][Math.floor(random() * 6)] });
      published++;
      await sleep(15);
    }
    await publish(request, stream, "match_ended", {});
  })();

  let joined: Page | null = null;
  const chaos = (async () => {
    const until = (n: number) => expect.poll(() => published, { timeout: 120_000 }).toBeGreaterThanOrEqual(n);
    for (let i = 1; i <= 6; i++) {
      await until((EVENTS * i) / 8);
      await dropped.getByRole("button", { name: "Simulate network drop" }).click();
      if (i === 3) {
        joined = await context.newPage(); // joins mid-match: snapshot + REST backfill
        await joined.goto(`/m/${stream}`);
      }
      if (!CHAOS) continue;
      if (i <= 3) await compose(`restart livefeed${i}`); // graceful: clients get 1012
      if (i === 5) await compose("kill -s SIGKILL livefeed2"); // abrupt: no close frame
      if (i === 6) await compose("up -d --wait livefeed2");
    }
  })();

  await Promise.all([producer, chaos]);
  const server = await (await request.get(`/v1/streams/${stream}`)).json();
  expect(server.state.status).toBe("finished");

  const summary: Record<string, unknown> = { stream, lastSeq: server.seq, chaos: CHAOS };
  for (const [name, page] of [["steady", steady], ["dropped", dropped], ["joined", joined!]] as const) {
    await expect(page.getByTestId("stat-cursor")).toHaveText(String(server.seq));
    for (const team of teams) {
      await expect(page.getByTestId(`runs-${team}`)).toHaveText(String(server.state.score[team]));
      await expect(page.getByTestId(`wickets-${team}`)).toHaveText(String(server.state.wickets[team]));
    }
    await expect(page.getByTestId("result")).toHaveText(server.state.winner ? `${server.state.winner} won` : "Tied");
    // The ball-by-ball list: newest first, consecutive, no repeats.
    const seqs = (await page.locator("li[data-seq]").evaluateAll((els) =>
      els.map((el) => Number((el as HTMLElement).dataset.seq)),
    )) as number[];
    expect(seqs).toEqual(seqs.map((_, i) => server.seq - i));
    summary[name] = {
      applied: await stat(page, "stat-applied"),
      reconnects: await stat(page, "stat-reconnects"),
      duplicatesDropped: await stat(page, "stat-duplicates"),
      gapsResynced: await stat(page, "stat-gaps"),
      snapshots: await stat(page, "stat-snapshots"),
    };
  }
  // Watching from seq 0, each event must have been applied exactly once.
  expect(await stat(steady, "stat-applied")).toBe(server.seq);
  expect(await stat(dropped, "stat-applied")).toBe(server.seq);
  expect(await stat(dropped, "stat-reconnects")).toBeGreaterThanOrEqual(6);

  await testInfo.attach("summary.json", { body: JSON.stringify(summary, null, 2), contentType: "application/json" });
  console.log(JSON.stringify(summary));
  await context.close();
});

test("the match list links to a match", async ({ page, request }) => {
  const stream = `e2e-list-${Date.now()}`;
  await request.post("/v1/streams", { data: { id: stream, kind: "match" }, headers: { "X-API-Key": KEY } });
  await publish(request, stream, "match_started", { teams: ["RR", "GT"] });
  await page.goto("/");
  await page.getByRole("link", { name: new RegExp(stream) }).click();
  await expect(page).toHaveURL(new RegExp(`/m/${stream}$`));
  await expect(page.getByTestId("runs-RR")).toHaveText("0");
});
