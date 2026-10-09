import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

// `npm run dev` proxies the API and WebSockets to the docker compose stack on :8080.
// `npm run build` writes into the Python package, which FastAPI serves at / and /m/<id>.
export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    proxy: {
      "/v1": { target: "http://localhost:8080", ws: true },
    },
  },
  build: {
    outDir: "../src/livefeed/static/web",
    emptyOutDir: true,
  },
  test: {
    environment: "jsdom",
    setupFiles: ["./src/test/setup.ts"],
    include: ["src/**/*.test.{ts,tsx}"],
    coverage: {
      provider: "v8",
      include: ["src/**/*.{ts,tsx}"],
      exclude: ["src/**/*.test.{ts,tsx}", "src/test/**", "src/main.tsx"],
      thresholds: { lines: 95, branches: 80 },
    },
  },
});
