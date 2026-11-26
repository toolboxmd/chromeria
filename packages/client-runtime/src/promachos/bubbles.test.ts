import { describe, expect, it } from "vite-plus/test";

import { groupPromachosBubbles, type PromachosBlockRole } from "./bubbles.ts";

const ROLES: Record<string, PromachosBlockRole> = {
  h: "heading",
  p: "block",
  code: "block",
  hr: "between",
  ws: "invisible",
};

/** Bubbles as `[blocks]`, blocks outside them bare. */
function group(blocks: string[]) {
  return groupPromachosBubbles(blocks, (block) => ROLES[block.replace(/\d+$/, "")]!).map((item) =>
    item.kind === "bubble" ? item.blocks : item.block,
  );
}

describe("groupPromachosBubbles", () => {
  it("gives each block its own bubble and joins headings to the block after them", () => {
    expect(group(["p1", "h1", "h2", "code1", "p2"])).toEqual([
      ["p1"],
      ["h1", "h2", "code1"],
      ["p2"],
    ]);
  });

  it("keeps invisible blocks out of bubbles without splitting a heading from its block", () => {
    expect(group(["ws1", "h1", "ws2", "p1", "ws3", "p2"])).toEqual([
      "ws1",
      ["h1", "p1"],
      "ws2",
      "ws3",
      ["p2"],
    ]);
  });

  it("closes the bubble at a break, even after a lone heading", () => {
    expect(group(["h1", "hr1", "p1", "hr2"])).toEqual([["h1"], "hr1", ["p1"], "hr2"]);
  });
});
