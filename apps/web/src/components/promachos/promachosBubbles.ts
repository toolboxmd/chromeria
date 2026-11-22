/**
 * Groups an assistant reply's top-level Markdown blocks into chat bubbles,
 * one per paragraph. It runs on the reply's single parse, so a code block,
 * list or table is always one bubble, and reference links still resolve
 * against definitions anywhere in the reply.
 */
import type { Element, Root, RootContent } from "hast";

import type { MarkdownExtensions } from "~/markdown/wordsHast";

const HEADINGS = new Set(["h1", "h2", "h3", "h4", "h5", "h6"]);

const isHeading = (node: RootContent) => node.type === "element" && HEADINGS.has(node.tagName);

export function groupPromachosBubbles(tree: Root): void {
  const children: RootContent[] = [];
  let bubble: Element | null = null;
  // A heading introduces the block after it, so they share a bubble.
  let headingOnly = false;
  for (const node of tree.children) {
    if (node.type !== "element") {
      // Whitespace between blocks and comments render nowhere, so never an empty bubble.
      children.push(node);
      continue;
    }
    if (node.tagName === "hr" || node.tagName === "section") {
      // A thematic break stands between bubbles; footnotes span the reply.
      children.push(node);
      bubble = null;
      continue;
    }
    if (bubble === null || !headingOnly) {
      bubble = { type: "element", tagName: "div", properties: {}, children: [] };
      children.push(bubble);
    }
    bubble.children.push(node);
    headingOnly = bubble.children.every(isHeading);
  }
  tree.children = children;
}

/** One stable object: ChatMarkdown keys its parse cache by the extensions object. */
const PROMACHOS_BUBBLE_EXTENSIONS: MarkdownExtensions = { promachosBubbles: true };

/** ChatMarkdown props for a bubbled reply; the bubbles are styled as the root's direct `div`s. */
export const PROMACHOS_BUBBLE_MARKDOWN = {
  extensions: PROMACHOS_BUBBLE_EXTENSIONS,
  className:
    "flex flex-col items-start gap-1 [&>div]:min-w-0 [&>div]:max-w-[85%] [&>div]:rounded-2xl [&>div]:bg-message [&>div]:px-4 [&>div]:py-2.5 [&>div]:text-message-foreground [&>div>:first-child]:mt-0 [&>div>:last-child]:mb-0 [&>div:empty]:hidden [&>hr]:self-stretch [&>section]:self-stretch",
};
