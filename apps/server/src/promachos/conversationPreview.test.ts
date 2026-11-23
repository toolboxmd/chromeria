import { describe, expect, it } from "vite-plus/test";

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
