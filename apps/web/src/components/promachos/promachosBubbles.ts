/**
 * Groups an assistant reply's top-level Markdown blocks into chat bubbles by the shared Promachos
 * rule. It runs on the reply's single parse, so a code block, list or table is always one bubble,
 * and reference links still resolve against definitions anywhere in the reply.
 */
import {
  groupPromachosBubbles as groupBlocks,
  type PromachosBlockRole,
} from "@t3tools/client-runtime/promachos/bubbles";
import type { Element, Root, RootContent } from "hast";

import type { MarkdownExtensions } from "~/markdown/wordsHast";

const HEADINGS = new Set(["h1", "h2", "h3", "h4", "h5", "h6"]);

function roleOf(node: RootContent): PromachosBlockRole {
  // Whitespace between blocks and comments render nowhere.
  if (node.type !== "element") return "invisible";
  // Footnotes span the reply.
  if (node.tagName === "hr" || node.tagName === "section") return "between";
  return HEADINGS.has(node.tagName) ? "heading" : "block";
}

export function groupPromachosBubbles(tree: Root): void {
  tree.children = groupBlocks(tree.children, roleOf).map((item) =>
    item.kind === "bubble"
      ? {
          type: "element",
          tagName: "div",
          // A table alone is its own card rather than a bubble.
          properties: item.blocks.every(
            (node) => node.type === "element" && node.tagName === "table",
          )
            ? { dataPromachosCard: "" }
            : {},
          children: item.blocks.filter((node): node is Element => node.type === "element"),
        }
      : item.block,
  );
}

/** One stable object: ChatMarkdown keys its parse cache by the extensions object. */
const PROMACHOS_BUBBLE_EXTENSIONS: MarkdownExtensions = { promachosBubbles: true };

/** ChatMarkdown props for a bubbled reply; the bubbles are styled as the root's direct `div`s. */
export const PROMACHOS_BUBBLE_MARKDOWN = {
  extensions: PROMACHOS_BUBBLE_EXTENSIONS,
  className:
    "flex flex-col items-start gap-2 [&>div]:min-w-0 [&>div]:max-w-[82%] [&>div]:rounded-[18px] [&>div]:bg-message dark:[&>div]:bg-[color-mix(in_srgb,var(--foreground)_20%,var(--background))] [&>div]:px-3.5 [&>div]:py-2.5 [&>div]:text-message-foreground [&>div[data-promachos-card]]:border [&>div[data-promachos-card]]:bg-card dark:[&>div[data-promachos-card]]:bg-card [&>div[data-promachos-card]]:px-3 [&>div[data-promachos-card]]:py-1 [&>div>:first-child]:mt-0 [&>div>:last-child]:mb-0 [&>div:empty]:hidden [&>hr]:self-stretch [&>section]:self-stretch",
};
