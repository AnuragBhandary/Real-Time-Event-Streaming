import { defineConfig, devices } from "@playwright/test";

// End-to-end tests run against the real stack: `docker compose up -d --build --wait` from the
// repository root (3 instances behind nginx on :8080), then `npm run e2e`.
export default defineConfig({
  testDir: "e2e",
  timeout: 240_000,
  expect: { timeout: 30_000 },
  workers: 1,
  reporter: [["list"], ["html", { open: "never" }]],
  use: {
    baseURL: process.env.E2E_BASE_URL ?? "http://localhost:8080",
    trace: "retain-on-failure",
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
});
