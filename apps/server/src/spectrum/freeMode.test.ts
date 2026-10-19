import { describe, expect, it } from "vite-plus/test";

import { parseModeration } from "./freeMode.ts";
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
