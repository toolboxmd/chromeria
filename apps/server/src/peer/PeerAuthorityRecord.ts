import {
  AuthSessionId,
  EnvironmentId,
  NodeId,
  PeerAuthorityEnvelope,
  PeerCeiling,
  PeerReturnPair,
  ProjectId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as Invocation from "../mcp/McpInvocationContext.ts";

export const PeerSession = Schema.Struct({
  environmentId: EnvironmentId,
  generation: Schema.String,
  automationVersion: Schema.optionalKey(Schema.Literal(1)),
});
export const Constraint = Schema.Union([
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
]);

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
  requestActorThreadId: Schema.optionalKey(Schema.NullOr(ThreadId)),
  requestClientSessionId: Schema.optionalKey(AuthSessionId),
  readOnly: Schema.Boolean,
  originRunId: Schema.NullOr(RunId),
  pair: Schema.optionalKey(Schema.Struct({ parentThreadId: ThreadId, taskId: NodeId })),
  /** Local constraints, never accepted from a public tool or peer envelope. */
  context: Schema.optionalKey(Constraint),
});
export type Authority = typeof Authority.Type;

export const AutomationExecution = Schema.Struct({
  kind: Schema.Literal("automation-execution"),
  id: Schema.String,
  ceiling: PeerCeiling,
  certificateId: Schema.String,
  actionId: Schema.String,
  generation: Schema.String,
  projectId: ProjectId,
  context: Schema.optionalKey(Schema.Never),
  pair: Schema.optionalKey(Schema.Never),
});
export type AutomationExecution = typeof AutomationExecution.Type;

const AutomationBirth = Schema.Struct({
  ...AutomationExecution.fields,
  kind: Schema.Literal("automation-birth"),
});

export const Conjunction = Schema.Struct({
  kind: Schema.Literal("conjunction"),
  id: Schema.String,
  ceiling: PeerCeiling,
  generation: Schema.String,
  pair: Schema.optionalKey(Schema.Never),
  resourceSessionId: Schema.optionalKey(AuthSessionId),
  context: Constraint,
});
export type Conjunction = typeof Conjunction.Type;

export const ExecutionRecord = Schema.Union([
  Authority,
  AutomationExecution,
  AutomationBirth,
  Conjunction,
]);
export type ExecutionRecord = typeof ExecutionRecord.Type;

export const interactiveAuthority = (record: ExecutionRecord | undefined): Authority | undefined =>
  record !== undefined && "envelope" in record ? record : undefined;

export const intersect = (a: PeerCeiling, b: PeerCeiling): PeerCeiling => ({
  ...Invocation.intersectModes(a, b),
  resourceScope:
    a.resourceScope === "created-work" || b.resourceScope === "created-work"
      ? "created-work"
      : "environment-resources",
  onward: a.onward && b.onward,
  capabilities: a.capabilities.filter((capability) => b.capabilities.includes(capability)),
});

export function conjunction(
  id: string,
  context: typeof Constraint.Type,
  records: readonly [ExecutionRecord, ...ExecutionRecord[]],
  resourceSessionId?: AuthSessionId,
): ExecutionRecord {
  const base = records[0];
  const ceiling = records.reduce(
    (ceiling, record) => intersect(ceiling, record.ceiling),
    base.ceiling,
  );
  if ("envelope" in base && records.every((record) => "envelope" in record))
    return {
      ...base,
      id,
      context,
      ceiling,
      ...(resourceSessionId === undefined ? {} : { resourceSessionId }),
      envelope: { ...base.envelope, recordId: id, actionId: id, digest: id },
    };
  const native = records.find((record) => "generation" in record);
  if (native === undefined) throw new Error("A native conjunction requires an execution owner");
  return {
    kind: "conjunction",
    id,
    context,
    ceiling,
    generation: native.generation,
    ...(resourceSessionId === undefined ? {} : { resourceSessionId }),
  };
}
