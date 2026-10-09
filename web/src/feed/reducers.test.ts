import { describe, expect, it } from "vitest";

import fixture from "./reducer-fixture.json";
import { generic, match, reducerFor, type State } from "./reducers";

describe("reducers match the Python server reducers", () => {
  for (const c of fixture.cases) {
    it(`${c.kind} (seed ${c.seed}): identical state after each of ${c.steps.length} events`, () => {
      const reduce = reducerFor(c.kind);
      let state: State = {};
      c.steps.forEach((step, i) => {
        state = reduce(state, step.type, step.data as Record<string, unknown>);
        expect(state, `after event ${i + 1} (${step.type})`).toEqual(step.state);
      });
    });
  }

  it("does not mutate the state it is given", () => {
    const before = match({}, "match_started", { teams: ["A", "B"] });
    const copy = structuredClone(before);
    match(before, "score", { team: "A", points: 4 });
    expect(before).toEqual(copy);
  });

  it("reports a tie with a null winner", () => {
    let s = match({}, "match_started", { teams: ["A", "B"] });
    s = match(s, "match_ended", {});
    expect(s).toMatchObject({ status: "finished", winner: null });
  });

  it("falls back to the generic reducer for unknown kinds", () => {
    expect(reducerFor("something-new")).toBe(generic);
  });
});
