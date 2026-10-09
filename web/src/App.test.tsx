import { screen } from "@testing-library/react";
import { render } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import { App } from "./App";
import { mockApi } from "./test/render";

afterEach(() => vi.unstubAllGlobals());

it("renders the layout and the match list at /", async () => {
  mockApi({ "/v1/streams": [] });
  render(<App />);
  expect(screen.getByRole("link", { name: /livefeed/ })).toHaveAttribute("href", "/");
  expect(await screen.findByText(/No matches yet/)).toBeInTheDocument();
});
