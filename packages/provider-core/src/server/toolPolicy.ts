import type { ProviderDriverKind, ThreadTools } from "@t3tools/contracts";
import * as Schema from "effect/Schema";

export class ToolPolicyError extends Schema.TaggedError<ToolPolicyError>()("ToolPolicyError", {
  reason: Schema.String,
}) {
  override get message() {
    return this.reason;
  }
}

export function toolPolicyRejection(
  driver: ProviderDriverKind,
  tools: ThreadTools,
): string | undefined {
  if (driver !== "claudeAgent")
    return `Tool allowlists are not supported on ${driver}: no proven native exact-tool enforcement interface.`;
  if (tools.allow.some((name) => name.startsWith("mcp__")))
    return "MCP tools must use mcp:server:tool names.";
  return undefined;
}

export function toolAllowlistsEqual(
  left: ThreadTools | undefined,
  right: ThreadTools | undefined,
): boolean {
  if (left === undefined || right === undefined) return left === right;
  return (
    new Set(left.allow).size === new Set(right.allow).size &&
    left.allow.every((name) => right.allow.includes(name))
  );
}
