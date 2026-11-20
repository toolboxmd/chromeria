/**
 * Groups an assistant reply's top-level Markdown blocks into chat bubbles,
 * one per paragraph. It runs inside the reply's single Markdown parse, so a
 * code block, list or table is always one bubble, and reference links still
 * resolve against definitions anywhere in the reply.
 */
import type { Parent, Root, RootContent } from "mdast";

declare module "mdast" {
  interface RootContentMap {
    promachosBubble: PromachosBubble;
  }
}

interface PromachosBubble extends Parent {
  type: "promachosBubble";
  children: RootContent[];
}

export function remarkPromachosBubbles() {
  return (tree: Root) => {
    const children: RootContent[] = [];
    let bubble: PromachosBubble | null = null;
    // A heading introduces the block after it, so they share a bubble.
    let headingOnly = false;
    for (const node of tree.children) {
      if (node.type === "definition" || node.type === "footnoteDefinition") {
        // Rendered nowhere, so never an empty bubble.
        children.push(node);
        continue;
      }
      if (node.type === "thematicBreak") {
        children.push(node);
        bubble = null;
        continue;
      }
      if (bubble === null || (!headingOnly && node.type !== "html")) {
        bubble = { type: "promachosBubble", data: { hName: "div" }, children: [] };
        children.push(bubble);
      }
      bubble.children.push(node);
      headingOnly = bubble.children.every((child) => child.type === "heading");
    }
    tree.children = children;
  };
}

/**
 * ChatMarkdown props for a bubbled reply. Sanitizing drops class names from
 * Markdown output, so the bubbles are styled as the root's direct `div`s.
 */
export const PROMACHOS_BUBBLE_MARKDOWN = {
  extraRemarkPlugins: [remarkPromachosBubbles],
  className:
    "flex flex-col items-start gap-1 [&>div]:min-w-0 [&>div]:max-w-[85%] [&>div]:rounded-2xl [&>div]:bg-message [&>div]:px-4 [&>div]:py-2.5 [&>div]:text-message-foreground [&>div>:first-child]:mt-0 [&>div>:last-child]:mb-0 [&>div:empty]:hidden [&>hr]:self-stretch [&>section]:self-stretch",
};
