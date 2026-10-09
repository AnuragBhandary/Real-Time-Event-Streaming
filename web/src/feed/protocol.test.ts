import { describe, expect, it } from "vitest";

import { CloseCode, describeClose, parseFrame } from "./protocol";

describe("protocol", () => {
  it("parses the four server frames and rejects anything else", () => {
    for (const type of ["snapshot", "events", "heartbeat", "pong"]) {
      expect(parseFrame(JSON.stringify({ type }))?.type).toBe(type);
    }
    expect(parseFrame('{"type":"other"}')).toBeNull();
    expect(parseFrame("{not json")).toBeNull();
  });

  it("describes every close code it can see", () => {
    const codes = [null, ...Object.values(CloseCode), 4999];
    const texts = codes.map(describeClose);
    expect(new Set(texts).size).toBe(codes.length);
    expect(describeClose(4999)).toBe("code 4999");
  });
});
