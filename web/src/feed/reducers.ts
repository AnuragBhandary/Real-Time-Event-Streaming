// Client-side state reducers, a port of src/livefeed/reducers.py.
//
// The server folds every event into a snapshot inside the append transaction. The browser starts
// from that snapshot and folds the events that follow with these functions, so both sides must
// agree exactly. reducers.test.ts checks that, step by step, against states produced by the Python
// reducer (tests/test_web_contract.py writes the fixture). The server has already validated each
// event, so these functions do not reject anything.

export type State = Record<string, unknown>;
export type Reducer = (state: State, type: string, data: Record<string, unknown>) => State;

export interface MatchState {
  status: "scheduled" | "live" | "finished";
  events: number;
  teams?: [string, string];
  score?: Record<string, number>;
  wickets?: Record<string, number>;
  commentary?: string;
  winner?: string | null;
}

export const generic: Reducer = (state, type, data) => {
  const counts = { ...((state.counts as Record<string, number> | undefined) ?? {}) };
  counts[type] = (counts[type] ?? 0) + 1;
  return { events: ((state.events as number | undefined) ?? 0) + 1, counts, last: { type, ...data } };
};

export const match: Reducer = (state, type, data) => {
  const s = (
    Object.keys(state).length ? structuredClone(state) : { status: "scheduled", events: 0 }
  ) as unknown as MatchState;
  const team = data.team as string;
  switch (type) {
    case "match_started": {
      const teams = data.teams as [string, string];
      s.status = "live";
      s.teams = teams;
      s.score = Object.fromEntries(teams.map((t) => [t, 0]));
      s.wickets = Object.fromEntries(teams.map((t) => [t, 0]));
      break;
    }
    case "score":
      s.score![team] = (s.score![team] ?? 0) + (data.points as number);
      break;
    case "wicket":
      s.wickets![team] = (s.wickets![team] ?? 0) + 1;
      break;
    case "commentary":
      s.commentary = data.text as string;
      break;
    case "match_ended": {
      s.status = "finished";
      const scores = s.score ?? {};
      const best = Math.max(...Object.values(scores));
      const leaders = Object.keys(scores).filter((t) => scores[t] === best);
      s.winner = leaders.length === 1 ? leaders[0] : null;
      break;
    }
  }
  s.events = (s.events ?? 0) + 1;
  return s as unknown as State;
};

export const REDUCERS: Record<string, Reducer> = { generic, match };

export function reducerFor(kind: string): Reducer {
  return REDUCERS[kind] ?? generic;
}
