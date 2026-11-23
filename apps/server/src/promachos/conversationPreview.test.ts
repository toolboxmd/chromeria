import { describe, expect, it } from "vite-plus/test";

import { serializeComposerFileLink } from "@t3tools/shared/composerTrigger";

import { CONVERSATION_PREVIEW_MAX_CHARS, conversationPreviewText } from "./conversationPreview.ts";

describe("conversationPreviewText", () => {
  it.each([
    ["plain text", "Hello there", "Hello there"],
    [
      "headings and paragraphs",
      "# Plan\n\nFirst step.\nSecond step.",
      "Plan First step. Second step.",
    ],
    [
      "emphasis and inline code",
      "**Bold**, _soft_, ~~gone~~ and `vp i`",
      "Bold, soft, gone and vp i",
    ],
    ["snake_case words", "Set my_var_name in config_file", "Set my_var_name in config_file"],
    ["links and images", "See [the docs](https://x.dev) ![logo](a.png)", "See the docs logo"],
    [
      "a link target with parentheses",
      'See [the docs](https://x.dev/a_(b) "Docs")',
      "See the docs",
    ],
    ["code spans as written", "Run `foo__bar` and ``a*b*c``", "Run foo__bar and a*b*c"],
    ["a link inside a code span", "Write `[x](url)` here", "Write [x](url) here"],
    ["closing heading hashes", "## Done ##", "Done"],
    ["reference links", "See [the docs][ref].\n\n[ref]: https://example.com", "See the docs."],
    ["fenced code as written", "```sh\n# install\n**vp** i\n```", "# install **vp** i"],
    ["a fence inside a quote", "> ```sh\n> # install\n> **vp** i\n> ```", "# install **vp** i"],
    ["a comment inside a fence", "```html\n<!-- keep -->\n```", "<!-- keep -->"],
    ["a code span holding a shorter backtick run", "Use ``a`b__c`` here", "Use a`b__c here"],
    ["escaped characters", "\\*not emphasis\\* and 1\\. item", "*not emphasis* and 1. item"],
    [
      "a composer file mention",
      `Open ${serializeComposerFileLink("pkg/__init__.py")}`,
      "Open __init__.py",
    ],
    ["a comment across lines", "Before <!-- one\ntwo --> after", "Before after"],
    [
      "formatted text before a comment that spans lines",
      "**Ready**: run `vp i` <!-- draft note\nfor later -->",
      "Ready: run vp i",
    ],
    [
      "a comment opener inside a code span",
      "Use `<!--` to start a comment.",
      "Use <!-- to start a comment.",
    ],
    [
      "a fence inside a comment",
      ["<!--", "```md", "Hidden draft", "```", "-->", "Visible summary"].join("\n"),
      "Visible summary",
    ],
    [
      "a quoted fence keeps deeper markers",
      ["> ```sh", "> > out.txt", "> ```"].join("\n"),
      "> out.txt",
    ],
    [
      "a quoted fence ends with its quote",
      ["> ```sh", "> vp i", "", "**Setup complete.**"].join("\n"),
      "vp i Setup complete.",
    ],
    ["private-use characters", "Before \uE00042\uE001 after", "Before 42 after"],
    ["lists and quotes", "> Quoted\n- [ ] todo\n1. first\n* second", "Quoted todo first second"],
    ["code fences keep their code", "```ts\nconst a = 1;\n```", "const a = 1;"],
    ["tables and rules", "| a | b |\n| --- | --- |\n| 1 | 2 |\n\n---", "| a | b | | 1 | 2 |"],
    ["comments", "<!-- hidden -->Shown", "Shown"],
  ])("strips Markdown from %s", (_name, input, expected) => {
    expect(conversationPreviewText(input)).toBe(expected);
  });

  it.each([
    ["empty text", ""],
    ["only whitespace", " \n\t "],
    ["only a fence and a rule", "```\n```\n***"],
  ])("has no preview for %s", (_name, input) => {
    expect(conversationPreviewText(input)).toBeNull();
  });

  it("caps the line by characters, not UTF-16 units", () => {
    const exact = "a".repeat(CONVERSATION_PREVIEW_MAX_CHARS);
    expect(conversationPreviewText(exact)).toBe(exact);
    const long = "😀".repeat(CONVERSATION_PREVIEW_MAX_CHARS + 5);
    expect(conversationPreviewText(long)).toBe(
      `${"😀".repeat(CONVERSATION_PREVIEW_MAX_CHARS - 1)}…`,
    );
  });
});
