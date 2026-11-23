import { describe, expect, it } from "vite-plus/test";

import { promachosBubbleFragments } from "./promachosBubbles";

function fragments(markdown: string) {
  return promachosBubbleFragments(markdown).map((fragment) =>
    fragment.kind === "break"
      ? "---"
      : fragment.kind === "outside"
        ? `outside:${fragment.markdown}`
        : fragment.markdown,
  );
}

// The same replies the desktop grouping test renders, cut on the phone's own parse.
describe("promachosBubbleFragments", () => {
  it("gives each paragraph its own bubble", () => {
    expect(
      fragments("Found it.\n\nA reseller lists it at 21.90 EUR.\n\n\nGardazon looks fine."),
    ).toEqual(["Found it.", "A reseller lists it at 21.90 EUR.", "Gardazon looks fine."]);
  });

  it("keeps indented and fenced code whole across blank lines", () => {
    expect(fragments("Run:\n\n    first\n\n    second\n\nThen:\n\n```sh\na\n\nb\n```")).toEqual([
      "Run:",
      "    first\n\n    second",
      "Then:",
      "```sh\na\n\nb\n```",
    ]);
  });

  it("keeps a loose list and a table whole, with a heading joined to its block", () => {
    expect(
      fragments("1. First\n\n2. Second\n\n   more\n\n## Prices\n\n| a | b |\n| - | - |\n| 1 | 2 |"),
    ).toEqual(["1. First\n\n2. Second\n\n   more", "## Prices\n\n| a | b |\n| - | - |\n| 1 | 2 |"]);
  });

  it("gives reference definitions no bubble", () => {
    expect(
      fragments("See [the docs][d] now\n\nMore later.\n\n[d]: https://example.com/docs"),
    ).toEqual(["See [the docs][d] now", "More later."]);
  });

  it("marks a table alone as a card", () => {
    const cards = promachosBubbleFragments("Prices:\n\n| a | b |\n| - | - |\n| 1 | 2 |").map(
      (fragment) => fragment.kind === "bubble" && fragment.card,
    );
    expect(cards).toEqual([false, true]);
  });

  it("leaves a thematic break between bubbles", () => {
    expect(fragments("Before.\n\n---\n\nAfter.")).toEqual(["Before.", "---", "After."]);
  });
});
