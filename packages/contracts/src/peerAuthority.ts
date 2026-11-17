import * as Effect from "effect/Effect";
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
  /** "Share browser logins": copying this environment's cookies out, separate from driving its browser. */
  browserExport: Schema.Boolean.pipe(Schema.withDecodingDefault(Effect.succeed(false))),
});
export type PeerGrant = typeof PeerGrant.Type;
/** Every grant on ("Full access") or every grant off, without the session. */
export const uniformPeerGrant = (enabled: boolean) =>
  ({
    resourceScope: enabled ? "environment-resources" : "created-work",
    onward: enabled,
    preview: enabled,
    device: enabled,
    browserExport: enabled,
  }) satisfies Omit<PeerGrant, "sessionId">;
export const isFullPeerGrant = (grant: PeerGrant) =>
  grant.resourceScope === "environment-resources" &&
  grant.onward &&
  grant.preview &&
  grant.device &&
  grant.browserExport;
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
  /** Bound to the complete authenticated envelope; absent on pre-request-identity peers. */
  requestOwner: Schema.optionalKey(
    Schema.Struct({
      namespace: Schema.String.check(Schema.isMaxLength(2048)),
      generation: Schema.String.check(Schema.isMaxLength(2048)),
    }),
  ),
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
