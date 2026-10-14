/**
 * What differs per harness: how its hook events read, which moments reach the
 * model before its next action, and how a reply is shaped. Each delivering seam
 * is one the per-harness canary (ContextMD #5) proved, rechecked live in
 * Chromeria on 2026-10-12; change an entry only with new canary evidence.
 */
import type { MomentKind } from "./Selection.ts";

/** A dotted field path into the raw event, or the first present of several. */
type Field = string | ReadonlyArray<string>;

export interface Harness {
  readonly name: string;
  readonly events: Readonly<Record<string, MomentKind>>;
  readonly fields: {
    readonly event: Field;
    readonly session: Field;
    readonly toolUseId: Field;
    readonly tool: Field;
    readonly toolInput: Field;
    readonly cwd: Field;
    readonly source?: Field;
    /** The failure text a failure event carries (Claude Code `error`). */
    readonly error?: Field;
    readonly exitCode?: Field;
    /** The tool's output; error keys and a printed non-zero exit are read from it. */
    readonly output: Field;
  };
  /** How the harness reports a failed call: its own event, an exit code, or not at all. */
  readonly failureBy: "event" | "exit_code" | "none";
  readonly newEpochSources: ReadonlyArray<string>;
  readonly shellTools: ReadonlyArray<string>;
  readonly delivering: ReadonlyArray<MomentKind>;
  /** Grok: text selected where the harness discards hook output waits for this moment. */
  readonly carryTo: MomentKind | null;
  /** UTF-8 bytes one hook output may carry before the harness spills or clips it. */
  readonly limitBytes: number;
  /** How long a reply may wait for its settle: the hook settles at once, OpenCode when the model reads it. */
  readonly settleWithinMs: number;
  readonly sessionStartLimitBytes: number;
  readonly templates: { readonly context: unknown; readonly deny: unknown };
  /** Where the module can see what the model was shown for a denied call. */
  readonly denyEvidence: "projection" | "none";
  /** A deny reason the harness shows only up to this many bytes (Grok cuts at 256, #229). */
  readonly denyLimitBytes?: number;
  /** A shorter deny head where the reason is that short. */
  readonly denyHead?: string;
}

const HOOK_OUTPUT = {
  context: { hookSpecificOutput: { hookEventName: "{{event}}", additionalContext: "{{text}}" } },
  deny: {
    hookSpecificOutput: {
      hookEventName: "{{event}}",
      permissionDecision: "deny",
      permissionDecisionReason: "{{text}}",
    },
  },
};

const COMMAND_HOOK_FIELDS = {
  event: "hook_event_name",
  session: "session_id",
  toolUseId: "tool_use_id",
  tool: "tool_name",
  toolInput: "tool_input",
  cwd: "cwd",
  source: "source",
};

export const HARNESSES: Readonly<Record<string, Harness>> = {
  // Every seam delivered before the next action; an 11.7 KB context spilled to a 2 KB preview.
  claude: {
    name: "claude",
    events: {
      SessionStart: "session_start",
      UserPromptSubmit: "user_message",
      PreToolUse: "pre_tool",
      PostToolUse: "tool_result",
      PostToolUseFailure: "tool_failure",
      Stop: "before_done",
    },
    fields: { ...COMMAND_HOOK_FIELDS, error: "error", output: "tool_response" },
    failureBy: "event",
    newEpochSources: ["compact", "clear"],
    shellTools: ["Bash"],
    delivering: [
      "session_start",
      "user_message",
      "pre_tool",
      "tool_result",
      "tool_failure",
      "before_done",
    ],
    carryTo: null,
    limitBytes: 2000,
    settleWithinMs: 2000,
    sessionStartLimitBytes: 9500,
    templates: HOOK_OUTPUT,
    denyEvidence: "projection",
  },
  // A failed command fires PostToolUse with the output only, no exit code; context over
  // 2,500 tokens is cut in the middle. A denied call leaves no projected item.
  codex: {
    name: "codex",
    events: {
      SessionStart: "session_start",
      UserPromptSubmit: "user_message",
      PreToolUse: "pre_tool",
      PostToolUse: "tool_result",
      PostCompact: "compaction",
      Stop: "before_done",
    },
    fields: { ...COMMAND_HOOK_FIELDS, output: "tool_response" },
    failureBy: "none",
    newEpochSources: [],
    shellTools: ["Bash", "shell", "exec_command", "local_shell", "unified_exec"],
    delivering: ["user_message", "pre_tool", "tool_result", "tool_failure", "before_done"],
    carryTo: null,
    limitBytes: 2000,
    settleWithinMs: 2000,
    sessionStartLimitBytes: 0,
    templates: HOOK_OUTPUT,
    denyEvidence: "none",
  },
  // Prompt-hook and post-tool output are discarded; PreToolUse context arrives with the
  // tool result; context is clipped at 10,000 characters; hooks fail open at 5 s.
  // Stdin carries both names: `hookEventName` is snake_case and `hook_event_name` is
  // Claude's PascalCase. The reply echoes `hook_event_name`, the name the canary proved.
  grok: {
    name: "grok",
    events: {
      session_start: "session_start",
      SessionStart: "session_start",
      user_prompt_submit: "user_message",
      UserPromptSubmit: "user_message",
      pre_tool_use: "pre_tool",
      PreToolUse: "pre_tool",
      post_tool_use: "tool_result",
      PostToolUse: "tool_result",
      post_tool_use_failure: "tool_failure",
      PostToolUseFailure: "tool_failure",
      post_compact: "compaction",
      PostCompact: "compaction",
      stop: "before_done",
      Stop: "before_done",
    },
    fields: {
      ...COMMAND_HOOK_FIELDS,
      exitCode: "toolResult.exit_code",
      output: "toolResult.output_for_prompt",
    },
    failureBy: "exit_code",
    newEpochSources: [],
    shellTools: ["run_terminal_command"],
    delivering: ["pre_tool", "before_done"],
    carryTo: "pre_tool",
    limitBytes: 2000,
    settleWithinMs: 2000,
    sessionStartLimitBytes: 0,
    templates: HOOK_OUTPUT,
    denyEvidence: "projection",
    denyLimitBytes: 256,
    denyHead: "The user's own ContextMD rule stopped this call:",
  },
  // The plugin adds served text to the user message (rule text in tool output was doubted,
  // obedience test 2026-10-12) and throws to deny; OpenCode has no before-done hook.
  opencode: {
    name: "opencode",
    events: {
      "chat.message": "user_message",
      "tool.execute.before": "pre_tool",
      "tool.execute.after": "tool_result",
      "session.compacted": "compaction",
    },
    fields: {
      event: "hook",
      session: "input.sessionID",
      toolUseId: "input.callID",
      tool: "input.tool",
      toolInput: ["output.args", "input.args"],
      cwd: "directory",
      exitCode: "output.metadata.exit",
      output: "output.output",
    },
    failureBy: "exit_code",
    newEpochSources: [],
    shellTools: ["bash"],
    delivering: ["user_message", "pre_tool", "tool_result", "tool_failure"],
    carryTo: null,
    limitBytes: 2000,
    settleWithinMs: 120_000,
    sessionStartLimitBytes: 0,
    templates: { context: { context: "{{text}}" }, deny: { deny: "{{text}}" } },
    denyEvidence: "none",
  },
};
