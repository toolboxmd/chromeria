import { describe, expect, it } from "vite-plus/test";

import { parseModeration, spectrumShape } from "./freeMode.ts";
import { makeState } from "./testFixtures.ts";

describe("parseModeration", () => {
  const state = makeState({ mode: "free", limit: 6 });

  it("reads only the last non-empty line as the control line", () => {
    expect(parseModeration(state, "Over to Red.\nNEXT: Red\n \n")).toEqual({
      type: "next",
      participant: 1,
      message: "Over to Red.",
    });
    expect(parseModeration(state, "DONE")).toEqual({ type: "done", message: "" });
    // A control line that is not last is ordinary text.
    expect(parseModeration(state, "NEXT: Red\nThat is all.")).toEqual({ type: "malformed" });
  });

  it("rejects unknown labels and decorated control lines", () => {
    for (const text of ["NEXT: Purple", "NEXT: red", "**DONE**", "Done", "", "NEXT:"])
      expect(parseModeration(state, text)).toEqual({ type: "malformed" });
  });
});

describe("spectrumShape", () => {
  const labels = ["Blue", "Red", "Green"];

  it("defaults free mode to three turns per Color with the first Color as White", () => {
    expect(spectrumShape({ mode: "free", labels })).toEqual({ limit: 9, moderator: 0 });
    expect(spectrumShape({ mode: "free", labels, turnsPerColor: 2, white: "Green" })).toEqual({
      limit: 6,
      moderator: 2,
    });
    expect(spectrumShape({ mode: "council", labels })).toEqual({ limit: 2, moderator: 0 });
  });

  it("rejects a cap below the Color count and contradictory input", () => {
    expect(spectrumShape({ mode: "free", labels, limit: 2 })).toBeTypeOf("string");
    expect(spectrumShape({ mode: "free", labels, white: "Purple" })).toBeTypeOf("string");
    expect(spectrumShape({ mode: "free", labels, white: "Red", moderator: 0 })).toBeTypeOf(
      "string",
    );
    expect(spectrumShape({ mode: "free", labels, limit: 6, turnsPerColor: 2 })).toBeTypeOf(
      "string",
    );
  });
});
