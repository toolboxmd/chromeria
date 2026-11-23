import { toHtml } from "hast-util-to-html";
import { describe, expect, it } from "vite-plus/test";

import { wordsHast } from "~/markdown/wordsHast";
import { PROMACHOS_BUBBLE_MARKDOWN } from "./promachosBubbles";

// The real parser and sanitizer ChatMarkdown uses, so the checks cover what
// the parser makes of each block, not only the grouping.
function bubbles(text: string): string {
  return toHtml(
    wordsHast(text, {
      math: false,
      breaks: false,
      escapeHtml: false,
      insights: false,
      extensions: PROMACHOS_BUBBLE_MARKDOWN.extensions,
    }),
  )
    .replaceAll("\n", "")
    .replaceAll(' dir="ltr"', "");
}

describe("groupPromachosBubbles", () => {
  it("gives each paragraph its own bubble", () => {
    expect(
      bubbles("Found it.\n\nA reseller lists it at 21.90 EUR.\n\n\nGardazon looks fine."),
    ).toBe(
      "<div><p>Found it.</p></div><div><p>A reseller lists it at 21.90 EUR.</p></div><div><p>Gardazon looks fine.</p></div>",
    );
  });

  it("keeps indented and fenced code whole across blank lines", () => {
    const html = bubbles("Run:\n\n    first\n\n    second\n\nThen:\n\n```sh\na\n\nb\n```");
    expect(html.match(/<div>/g)).toHaveLength(4);
    expect(html).toMatch(/^<div><p>Run:<\/p><\/div><div><pre[^>]*><code[^>]*>first\s*second/);
    expect(html).toMatch(
      /<div><p>Then:<\/p><\/div><div><pre[^>]*><code[^>]*>a\s*b\s*<\/code><\/pre><\/div>$/,
    );
  });

  it("keeps a loose list and a table whole, with a heading joined to its block", () => {
    const html = bubbles(
      "1. First\n\n2. Second\n\n   more\n\n## Prices\n\n| a | b |\n| - | - |\n| 1 | 2 |",
    );
    expect(html).toMatch(
      /^<div><ol><li><p>First<\/p><\/li><li><p>Second<\/p><p>more<\/p><\/li><\/ol><\/div>/,
    );
    expect(html).toMatch(/<div><h2[^>]*>Prices<\/h2><table>/);
    expect(html.match(/<div>/g)).toHaveLength(2);
  });

  it("resolves reference links across bubbles and gives definitions no bubble", () => {
    expect(bubbles("See [the docs][d] now\n\nMore later.\n\n[d]: https://example.com/docs")).toBe(
      '<div><p>See <a href="https://example.com/docs">the docs</a> now</p></div><div><p>More later.</p></div>',
    );
  });

  it("marks a table alone as a card", () => {
    expect(bubbles("Prices:\n\n| a | b |\n| - | - |\n| 1 | 2 |")).toMatch(
      /^<div><p>Prices:<\/p><\/div><div data-promachos-card=""><table>/,
    );
  });

  it("leaves a thematic break between bubbles", () => {
    expect(bubbles("Before.\n\n---\n\nAfter.")).toBe(
      "<div><p>Before.</p></div><hr><div><p>After.</p></div>",
    );
  });
});
