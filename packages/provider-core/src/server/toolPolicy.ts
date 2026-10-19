import type {
  ProviderDriverKind,
  ThreadTools,
  RuntimeMode,
  ProviderInteractionMode,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

export class ToolPolicyError extends Schema.TaggedError<ToolPolicyError>()("ToolPolicyError", {
  reason: Schema.String,
}) {
  override get message() {
    return this.reason;
  }
}

export const TOOL_POLICY_MODE_ERROR =
  "Tool allowlists require full-access runtime and default interaction mode.";

export function toolPolicyModeRejection(policy: {
  readonly runtimeMode: RuntimeMode;
  readonly interactionMode: ProviderInteractionMode;
}): string | undefined {
  return policy.runtimeMode === "full-access" && policy.interactionMode === "default"
    ? undefined
    : TOOL_POLICY_MODE_ERROR;
}

export function toolPolicyRejection(
  driver: ProviderDriverKind,
  tools: ThreadTools,
  modes: { readonly runtimeMode: RuntimeMode; readonly interactionMode: ProviderInteractionMode },
): string | undefined {
  if (driver !== "claudeAgent")
    return `Tool allowlists are not supported on ${driver}: no proven native exact-tool enforcement interface.`;
  const modeError = toolPolicyModeRejection(modes);
  if (modeError !== undefined) return modeError;
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
