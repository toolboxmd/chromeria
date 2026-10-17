import {
  AuthSessionId,
  EnvironmentId,
  NodeId,
  PeerAuthorityEnvelope,
  PeerCeiling,
  PeerReturnPair,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as Invocation from "../mcp/McpInvocationContext.ts";

export const PeerSession = Schema.Struct({
  environmentId: EnvironmentId,
  generation: Schema.String,
});
export const Authority = Schema.Struct({
  id: Schema.String,
  peer: PeerSession,
  sessionId: Schema.NullOr(AuthSessionId),
  /** Resource owner of imported work, distinct from a new verified contributor. */
  resourceSessionId: Schema.optionalKey(AuthSessionId),
  localThreadId: Schema.NullOr(ThreadId),
  originClientId: Schema.NullOr(AuthSessionId),
  parentId: Schema.NullOr(Schema.String),
  ceiling: PeerCeiling,
  envelope: PeerAuthorityEnvelope,
  tool: Schema.String,
  readOnly: Schema.Boolean,
  originRunId: Schema.NullOr(RunId),
  pair: Schema.optionalKey(Schema.Struct({ parentThreadId: ThreadId, taskId: NodeId })),
  /** Local constraints, never accepted from a public tool or peer envelope. */
  context: Schema.optionalKey(
    Schema.Union([
      Schema.Struct({
        type: Schema.Literal("task-stop"),
        pair: PeerReturnPair,
        requestId: Schema.String,
        reason: Schema.optional(Schema.String),
        recordIds: Schema.Array(Schema.String),
        targets: Schema.Array(
          Schema.Struct({ threadId: ThreadId, recordIds: Schema.Array(Schema.String) }),
        ),
      }),
      Schema.Struct({
        type: Schema.Literal("run-inputs"),
        threadId: ThreadId,
        recordIds: Schema.NonEmptyArray(Schema.String),
      }),
      Schema.Struct({
        type: Schema.Literal("owned-thread"),
        threadId: ThreadId,
        creationRecordId: Schema.String,
        runRecordId: Schema.String,
      }),
      Schema.Struct({
        type: Schema.Literal("legacy-thread"),
        threadId: ThreadId,
        runRecordId: Schema.String,
      }),
    ]),
  ),
});
export type Authority = typeof Authority.Type;

export const intersect = (a: PeerCeiling, b: PeerCeiling): PeerCeiling => ({
  ...Invocation.intersectModes(a, b),
  resourceScope:
    a.resourceScope === "created-work" || b.resourceScope === "created-work"
      ? "created-work"
      : "environment-resources",
  onward: a.onward && b.onward,
  capabilities: a.capabilities.filter((capability) => b.capabilities.includes(capability)),
});
