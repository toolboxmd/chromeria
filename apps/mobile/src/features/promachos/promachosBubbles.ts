import {
  groupPromachosBubbles,
  type PromachosBlockRole,
} from "@t3tools/client-runtime/promachos/bubbles";
import type { RootContent } from "mdast";
import remarkGfm from "remark-gfm";
import remarkParse from "remark-parse";
import { unified } from "unified";

const parser = unified().use(remarkParse).use(remarkGfm);

function roleOf(node: RootContent): PromachosBlockRole {
  switch (node.type) {
    case "heading":
      return "heading";
    case "thematicBreak":
    case "footnoteDefinition":
      return "between";
    case "definition":
      return "invisible";
    case "html":
      return /^\s*<!--[\s\S]*-->\s*$/.test(node.value) ? "invisible" : "block";
    default:
      return "block";
  }
}

export type PromachosFragment =
  | { readonly kind: "bubble"; readonly markdown: string; readonly offset: number }
  | { readonly kind: "break"; readonly offset: number }
  | { readonly kind: "outside"; readonly markdown: string; readonly offset: number };

/**
 * A reply's Markdown cut into the source of each Promachos bubble, by the rule desktop uses.
 * Reference definitions belong to no bubble; the caller adds the reply's definitions to each one.
 */
export function promachosBubbleFragments(markdown: string): PromachosFragment[] {
  const start = (node: RootContent) => node.position?.start.offset ?? 0;
  const end = (node: RootContent) => node.position?.end.offset ?? markdown.length;
  return groupPromachosBubbles(parser.parse(markdown).children, roleOf).flatMap(
    (item): PromachosFragment[] => {
      if (item.kind === "bubble") {
        const first = item.blocks[0]!;
        const offset = start(first);
        return [
          { kind: "bubble", markdown: markdown.slice(offset, end(item.blocks.at(-1)!)), offset },
        ];
      }
      const offset = start(item.block);
      if (item.block.type === "thematicBreak") return [{ kind: "break", offset }];
      if (roleOf(item.block) === "invisible") return [];
      return [{ kind: "outside", markdown: markdown.slice(offset, end(item.block)), offset }];
    },
  );
}
