import { renderToStaticMarkup } from "react-dom/server";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { describe, expect, it } from "vite-plus/test";

import { remarkPromachosBubbles } from "./promachosBubbles";

// The real parser and Markdown-to-HTML step ChatMarkdown uses, so the checks
// cover what the parser makes of each block, not only the grouping.
function bubbles(text: string): string {
  return renderToStaticMarkup(
    <ReactMarkdown remarkPlugins={[remarkGfm, remarkPromachosBubbles]}>{text}</ReactMarkdown>,
  ).replaceAll("\n", "");
}

describe("remarkPromachosBubbles", () => {
  it("gives each paragraph its own bubble", () => {
    expect(
      bubbles("Found it.\n\nA reseller lists it at 21.90 EUR.\n\n\nGardazon looks fine."),
    ).toBe(
      "<div><p>Found it.</p></div><div><p>A reseller lists it at 21.90 EUR.</p></div><div><p>Gardazon looks fine.</p></div>",
    );
  });

  it("keeps indented and fenced code whole across blank lines", () => {
    expect(bubbles("Run:\n\n    first\n\n    second\n\nThen:\n\n```sh\na\n\nb\n```")).toBe(
      '<div><p>Run:</p></div><div><pre><code>first\nsecond\n</code></pre></div><div><p>Then:</p></div><div><pre><code class="language-sh">a\n\nb\n</code></pre></div>'.replaceAll(
        "\n",
        "",
      ),
    );
  });

  it("keeps a loose list and a table whole, with a heading joined to its block", () => {
    const html = bubbles(
      "1. First\n\n2. Second\n\n   more\n\n## Prices\n\n| a | b |\n| - | - |\n| 1 | 2 |",
    );
    expect(
      html.match(
        /^<div><ol><li><p>First<\/p><\/li><li><p>Second<\/p><p>more<\/p><\/li><\/ol><\/div>/,
      ),
    ).not.toBeNull();
    expect(html).toContain("<div><h2>Prices</h2><table>");
    expect(html.match(/<div>/g)).toHaveLength(2);
  });

  it("resolves reference links across bubbles and gives definitions no bubble", () => {
    expect(bubbles("See [the docs][d].\n\nMore later.\n\n[d]: https://example.com/docs")).toBe(
      '<div><p>See <a href="https://example.com/docs">the docs</a>.</p></div><div><p>More later.</p></div>',
    );
  });

  it("leaves a thematic break between bubbles", () => {
    expect(bubbles("Before.\n\n---\n\nAfter.")).toBe(
      "<div><p>Before.</p></div><hr/><div><p>After.</p></div>",
    );
  });
});
