import type * as Tool from "effect/ai/Tool";

export type McpToolEffect = "read" | "owned" | "external-unknown";

export type McpToolEffectClassification = McpToolEffect | ((input: unknown) => McpToolEffect);
export const classifyToolEffect = (
  classification: McpToolEffectClassification,
  input: unknown,
): McpToolEffect => (typeof classification === "function" ? classification(input) : classification);

// The registered declaration owns effect classification. MCP's readOnlyHint
// also permits app artifacts (HTML/PNG), so it cannot classify durable effects.
const effects = new WeakMap<Tool.Any, McpToolEffectClassification>();
export const registerToolEffect = (tool: Tool.Any, effect: McpToolEffectClassification) => {
  const previous = effects.get(tool);
  if (previous !== undefined && previous !== effect)
    throw new Error(`Conflicting effect owners for ${tool.name}.`);
  effects.set(tool, effect);
};
export const toolEffect = (tool: Tool.Any, input: unknown) => {
  const classification = effects.get(tool);
  return classification === undefined ? undefined : classifyToolEffect(classification, input);
};
