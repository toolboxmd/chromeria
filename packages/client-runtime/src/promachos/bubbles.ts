/**
 * Promachos bubble grouping, shared by every client: a heading shares the bubble of the block after
 * it, every other block is one bubble, `between` blocks (a break, footnotes) close the bubble, and
 * `invisible` ones (whitespace, comments, definitions) neither open nor close one.
 */
export type PromachosBlockRole = "heading" | "block" | "between" | "invisible";

export type PromachosBubbleItem<Block> =
  | { readonly kind: "bubble"; readonly blocks: Block[] }
  | { readonly kind: "outside"; readonly block: Block };

export function groupPromachosBubbles<Block>(
  blocks: ReadonlyArray<Block>,
  roleOf: (block: Block) => PromachosBlockRole,
): PromachosBubbleItem<Block>[] {
  const items: PromachosBubbleItem<Block>[] = [];
  let bubble: Block[] | null = null;
  let headingOnly = false;
  for (const block of blocks) {
    const role = roleOf(block);
    if (role === "invisible" || role === "between") {
      items.push({ kind: "outside", block });
      if (role === "between") bubble = null;
      continue;
    }
    if (bubble === null || !headingOnly) {
      bubble = [];
      items.push({ kind: "bubble", blocks: bubble });
      headingOnly = true;
    }
    bubble.push(block);
    headingOnly &&= role === "heading";
  }
  return items;
}
