import * as Schema from "effect/Schema";
import {
  CommandId,
  EnvironmentId,
  ThreadId,
  RunId,
  EventId,
  NodeId,
  NonNegativeInt,
  IsoDateTime,
  RuntimeRequestId,
} from "./baseSchemas.ts";
import { PeerReturnPair } from "./peerAuthority.ts";

/** Qualified provenance never occupies a local child/transfer lookup field. */
export const PeerTaskResult = Schema.Struct({
  environmentId: EnvironmentId,
  threadId: ThreadId,
  runId: RunId,
  eventId: EventId,
  sequence: NonNegativeInt,
  status: Schema.Literals(["completed", "failed", "cancelled", "interrupted"]),
  text: Schema.String,
  truncated: Schema.Boolean,
});
export type PeerTaskResult = typeof PeerTaskResult.Type;
const PeerTaskStopExecutionState = Schema.Literals(["pending", "failed", "settled", "unknown"]);
export const PeerTaskSnapshot = Schema.Struct({
  sequence: NonNegativeInt,
  observedAt: IsoDateTime,
  title: Schema.String,
  workState: Schema.Literals(["working", "waiting_for_children", "result_available"]),
  pendingChildren: NonNegativeInt,
  queuedRuns: NonNegativeInt,
  recoveryWaiting: Schema.Boolean,
  incomplete: Schema.Boolean,
  executionSettled: Schema.Boolean,
  stopExecution: Schema.optional(
    Schema.Struct({
      commandId: CommandId,
      state: PeerTaskStopExecutionState,
    }),
  ),
  pendingQuestions: NonNegativeInt,
  requestCount: NonNegativeInt,
  requests: Schema.Array(
    Schema.Struct({
      environmentId: EnvironmentId,
      threadId: ThreadId,
      requestId: RuntimeRequestId,
      revision: NonNegativeInt,
      kind: Schema.String,
      responseType: Schema.String,
    }),
  ),
});
export type PeerTaskSnapshot = typeof PeerTaskSnapshot.Type;
export const PeerTaskObservation = Schema.Struct({
  cursor: NonNegativeInt,
  snapshot: PeerTaskSnapshot,
  result: Schema.optional(PeerTaskResult),
});
export type PeerTaskObservation = typeof PeerTaskObservation.Type;
export const PeerTaskStop = Schema.Struct({
  requestId: Schema.String,
  destinationCommandId: Schema.optional(CommandId),
  reason: Schema.optional(Schema.String),
  state: Schema.Literals(["pending", "unconfirmed", "acknowledged", "confirmed", "not_started"]),
});
export const PeerTaskResource = Schema.Struct({ environmentId: EnvironmentId, threadId: ThreadId });
export type PeerTaskResource = typeof PeerTaskResource.Type;
export const PeerTaskLifecycleRequest = Schema.Struct({
  environmentId: EnvironmentId,
  generation: Schema.String,
  pair: PeerReturnPair,
  operation: Schema.Union([
    Schema.Struct({
      type: Schema.Literal("observe"),
      stopCommandId: Schema.optional(CommandId),
      afterStopExecution: Schema.optional(PeerTaskStopExecutionState),
      after: NonNegativeInt,
      afterSnapshot: Schema.optional(NonNegativeInt),
      wait: Schema.Boolean,
    }),
    Schema.Struct({
      type: Schema.Literal("resultText"),
      eventId: EventId,
      runId: RunId,
      sequence: NonNegativeInt,
      textOffset: NonNegativeInt,
    }),
    Schema.Struct({ type: Schema.Literal("acknowledge"), through: NonNegativeInt }),
    Schema.Struct({
      type: Schema.Literal("stop"),
      requestId: Schema.String,
      reason: Schema.optional(Schema.String),
    }),
    Schema.Struct({
      type: Schema.Literal("snapshot"),
      stopCommandId: Schema.optional(CommandId),
      afterStopExecution: Schema.optional(PeerTaskStopExecutionState),
      after: NonNegativeInt,
      wait: Schema.Boolean,
    }),
    Schema.Struct({
      type: Schema.Literal("view"),
      resource: Schema.optional(PeerTaskResource),
      after: NonNegativeInt,
      textOffset: Schema.optional(NonNegativeInt),
    }),
  ]),
});
export type PeerTaskLifecycleRequest = typeof PeerTaskLifecycleRequest.Type;
export const PeerTaskView = Schema.Struct({
  taskId: NodeId,
  environmentId: EnvironmentId,
  threadId: ThreadId,
  snapshot: PeerTaskSnapshot,
  childrenTruncated: Schema.Boolean,
  children: Schema.Array(
    Schema.Struct({
      taskId: NodeId,
      environmentId: EnvironmentId,
      threadId: Schema.NullOr(ThreadId),
      title: Schema.NullOr(Schema.String),
      status: Schema.String,
    }),
  ),
  messages: Schema.Array(
    Schema.Struct({
      id: Schema.String,
      runId: Schema.NullOr(RunId),
      role: Schema.Literals(["user", "assistant", "system"]),
      text: Schema.String,
    }),
  ),
  nextCursor: Schema.NullOr(NonNegativeInt),
  nextTextOffset: Schema.NullOr(NonNegativeInt),
});
export type PeerTaskView = typeof PeerTaskView.Type;
export const PeerTaskLifecycleResult = Schema.Union([
  Schema.Struct({ type: Schema.Literal("stop"), commandId: CommandId }),
  Schema.Struct({ type: Schema.Literal("observation"), observation: PeerTaskObservation }),
  Schema.Struct({
    type: Schema.Literal("resultText"),
    text: Schema.String,
    nextTextOffset: Schema.NullOr(NonNegativeInt),
  }),
  Schema.Struct({ type: Schema.Literal("acknowledged"), through: NonNegativeInt }),
  Schema.Struct({ type: Schema.Literal("snapshot"), snapshot: PeerTaskSnapshot }),
  Schema.Struct({ type: Schema.Literal("view"), view: PeerTaskView }),
]);
export type PeerTaskLifecycleResult = typeof PeerTaskLifecycleResult.Type;
