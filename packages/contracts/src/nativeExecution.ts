import * as Schema from "effect/Schema";

import { IsoDateTime, NodeId, RunId, ThreadId, TrimmedNonEmptyString } from "./baseSchemas.ts";
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
