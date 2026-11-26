import * as Schema from "effect/Schema";

import {
  IsoDateTime,
  NodeId,
  NonNegativeInt,
  RunId,
  ThreadId,
  TrimmedNonEmptyString,
} from "./baseSchemas.ts";
import { ExecutionPurpose } from "./executionPurpose.ts";

/** Native evidence is separate from display cursors and inferred turn references. */
export const NativeInputEvidence = Schema.Struct({
  version: Schema.Literal(1),
  nativeId: TrimmedNonEmptyString,
  kind: Schema.Literals(["turn", "message"]),
  validation: Schema.Literals([
    "codex_turn_started",
    "claude_prompt_echo",
    "opencode_admitted_message",
  ]),
});
export type NativeInputEvidence = typeof NativeInputEvidence.Type;

/** Why a provider turn's main-agent counters may not be all of its model usage. */
export const UsageClosureOpenReason = Schema.Literals([
  "turn_not_completed",
  "usage_incomplete",
  "native_child_observed",
  "native_child_running",
  "ledger_baseline_unproven",
  "ledger_growth_outside_root_model",
  "ledger_mismatch",
  "usage_stream_broken",
  "usage_unattributed",
  "prompt_suggestion_sampled",
]);
export type UsageClosureOpenReason = typeof UsageClosureOpenReason.Type;

/** Whether a provider turn's main-agent counters are all of the model usage the turn caused. */
export const UsageClosure = Schema.Union([
  Schema.Struct({
    version: Schema.Literal(1),
    state: Schema.Literal("main_agent_only"),
    witness: Schema.Literals(["process_ledger", "event_stream"]),
    /** In-turn compactions whose usage turnTokenUsage leaves out. */
    uncountedCompactions: NonNegativeInt,
  }),
  Schema.Struct({
    version: Schema.Literal(1),
    state: Schema.Literal("open"),
    reason: UsageClosureOpenReason,
  }),
]);
export type UsageClosure = typeof UsageClosure.Type;

export const NativeExecutionSegment = Schema.Struct({
  version: Schema.Literal(1),
  /** Native session, child and initiating call identify a segment, never just a reusable task id. */
  id: TrimmedNonEmptyString,
  /** Session carrying the initiating call; nativeChildId names a distinct child session/task. */
  nativeSessionId: TrimmedNonEmptyString,
  nativeOperationId: TrimmedNonEmptyString,
  nativeChildId: Schema.optional(TrimmedNonEmptyString),
  kind: Schema.Literals(["start", "resume", "workflow_member", "background"]),
  source: Schema.Union([
    Schema.Struct({ type: Schema.Literal("app_run"), threadId: ThreadId, runId: RunId }),
    Schema.Struct({
      type: Schema.Literal("native_segment"),
      threadId: ThreadId,
      subagentId: NodeId,
      segmentId: TrimmedNonEmptyString,
    }),
    Schema.Struct({ type: Schema.Literal("unresolved"), reason: TrimmedNonEmptyString }),
  ]),
  purpose: ExecutionPurpose,
  /** A known initiating call does not by itself join child responses to this segment. */
  boundary: Schema.Union([
    Schema.Struct({
      state: Schema.Literal("native_turns"),
      nativeTurnIds: Schema.NonEmptyArray(TrimmedNonEmptyString),
    }),
    Schema.Struct({ state: Schema.Literal("native_input"), nativeInputId: TrimmedNonEmptyString }),
    Schema.Struct({ state: Schema.Literal("unresolved"), reason: TrimmedNonEmptyString }),
  ]),
  status: Schema.Literals(["running", "ended", "roster_absent", "interrupted", "unknown"]),
  /** Neither task termination nor roster removal proves all native usage was collected. */
  usageFinality: Schema.Literal("unreconciled"),
  noticeIds: Schema.Array(TrimmedNonEmptyString),
  wakeDispositions: Schema.Array(
    Schema.Literals([
      "offered",
      "consumed_in_active",
      "dropped_disabled",
      "dropped_restart",
      "duplicate",
    ]),
  ),
  observedAt: IsoDateTime,
});
export type NativeExecutionSegment = typeof NativeExecutionSegment.Type;
