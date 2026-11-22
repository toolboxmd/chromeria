import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SchemaGetter from "effect/SchemaGetter";
import { AuthSessionId, EnvironmentId, ThreadId, RunId, NodeId } from "./baseSchemas.ts";
import { RuntimeMode, ProviderInteractionMode } from "./providerPolicy.ts";

const peerGrantFields = {
  sessionId: AuthSessionId,
  resourceScope: Schema.Literals(["created-work", "environment-resources"]),
  onward: Schema.Boolean,
  preview: Schema.Boolean,
  device: Schema.Boolean,
  /** "Share browser logins": copying this environment's cookies out, separate from driving its browser. */
  browserExport: Schema.Boolean.pipe(Schema.withDecodingDefault(Effect.succeed(false))),
  /** "Use credential vault": filling logins from this environment's vault into its own browser. */
  vault: Schema.Boolean,
};
/** A grant stored or sent before "Use credential vault" existed follows its Browser access. */
const vaultFollowsPreview = <G extends { readonly preview: boolean; readonly vault?: boolean }>(
  grant: G,
) => ({ ...grant, vault: grant.vault ?? grant.preview });
const peerGrantSummaryFields = {
  ...peerGrantFields,
  environmentId: EnvironmentId,
  label: Schema.String,
};

/** A grant change. A client that predates `vault` omits it, and the stored choice stays. */
export const PeerGrantUpdate = Schema.Struct({
  ...peerGrantFields,
  vault: Schema.optionalKey(Schema.Boolean),
});
export type PeerGrantUpdate = typeof PeerGrantUpdate.Type;
/** Explicit human consent, separate from runtime mode. Missing legacy fields grant nothing. */
export const PeerGrant = PeerGrantUpdate.pipe(
  Schema.decodeTo(Schema.toType(Schema.Struct(peerGrantFields)), {
    decode: SchemaGetter.transform(vaultFollowsPreview),
    encode: SchemaGetter.passthroughSubtype(),
  }),
);
export type PeerGrant = typeof PeerGrant.Type;
/** Every grant on ("Full access") or every grant off, without the session. */
export const uniformPeerGrant = (enabled: boolean) =>
  ({
    resourceScope: enabled ? "environment-resources" : "created-work",
    onward: enabled,
    preview: enabled,
    device: enabled,
    browserExport: enabled,
    vault: enabled,
  }) satisfies Omit<PeerGrant, "sessionId">;
export const isFullPeerGrant = (grant: PeerGrant) =>
  grant.resourceScope === "environment-resources" &&
  grant.onward &&
  grant.preview &&
  grant.device &&
  grant.browserExport &&
  grant.vault;
export const PeerGrantSummary = Schema.Struct({
  ...peerGrantSummaryFields,
  vault: Schema.optionalKey(Schema.Boolean),
}).pipe(
  Schema.decodeTo(Schema.toType(Schema.Struct(peerGrantSummaryFields)), {
    decode: SchemaGetter.transform(vaultFollowsPreview),
    encode: SchemaGetter.passthroughSubtype(),
  }),
);
export type PeerGrantSummary = typeof PeerGrantSummary.Type;

/** Capabilities that cross environments. Older peers decode exactly this set, so local-only
 * capabilities (such as `issues`) are filtered out before a list leaves the server. */
export const PeerCapability = Schema.Literals([
  "preview",
  "orchestration",
  "worktree",
  "device",
  "pull-requests",
]);
export type PeerCapability = typeof PeerCapability.Type;
export const isPeerCapability = Schema.is(PeerCapability);
export const peerCapabilities = (capabilities: Iterable<string>): Array<PeerCapability> =>
  [...capabilities].filter(isPeerCapability);

export const PeerCeiling = Schema.Struct({
  runtimeMode: RuntimeMode,
  interactionMode: ProviderInteractionMode,
  resourceScope: peerGrantFields.resourceScope,
  onward: Schema.Boolean,
  capabilities: Schema.Array(PeerCapability),
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
