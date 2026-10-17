import * as Schema from "effect/Schema";
import { AuthSessionId, EnvironmentId, ThreadId, RunId, NodeId } from "./baseSchemas.ts";
import { RuntimeMode, ProviderInteractionMode } from "./providerPolicy.ts";

/** Explicit human consent, separate from runtime mode. Missing legacy fields grant nothing. */
export const PeerGrant = Schema.Struct({
  sessionId: AuthSessionId,
  resourceScope: Schema.Literals(["created-work", "environment-resources"]),
  onward: Schema.Boolean,
  preview: Schema.Boolean,
  device: Schema.Boolean,
});
export type PeerGrant = typeof PeerGrant.Type;
export const PeerGrantSummary = Schema.Struct({
  ...PeerGrant.fields,
  environmentId: EnvironmentId,
  label: Schema.String,
});
export type PeerGrantSummary = typeof PeerGrantSummary.Type;

export const PeerCeiling = Schema.Struct({
  runtimeMode: RuntimeMode,
  interactionMode: ProviderInteractionMode,
  resourceScope: PeerGrant.fields.resourceScope,
  onward: Schema.Boolean,
  capabilities: Schema.Array(
    Schema.Literals(["preview", "orchestration", "worktree", "device", "pull-requests"]),
  ),
});
export type PeerCeiling = typeof PeerCeiling.Type;

/** Server transport metadata. Public tool inputs cannot construct an authority. */
export const PeerAuthorityEnvelope = Schema.Struct({
  version: Schema.Literal(1),
  environmentId: EnvironmentId,
  generation: Schema.String,
  recordId: Schema.String,
  actionId: Schema.String,
  digest: Schema.String,
  route: Schema.Array(EnvironmentId),
  descendant: Schema.Boolean,
  pair: Schema.optionalKey(Schema.Struct({ parentThreadId: ThreadId, taskId: NodeId })),
});
export type PeerAuthorityEnvelope = typeof PeerAuthorityEnvelope.Type;

export const PeerReturnPair = Schema.Struct({
  parentEnvironmentId: EnvironmentId,
  parentThreadId: ThreadId,
  taskId: NodeId,
  childEnvironmentId: EnvironmentId,
  childThreadId: ThreadId,
  childRunId: Schema.NullOr(RunId),
});
export type PeerReturnPair = typeof PeerReturnPair.Type;
