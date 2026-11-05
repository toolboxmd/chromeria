import { SecretRequestField } from "./secretRequest.ts";
import { PeerAuthorityEnvelope } from "./peerAuthority.ts";
import * as Schema from "effect/Schema";
import { EnvironmentId, ProjectId, RunId, ThreadId, TrimmedNonEmptyString } from "./baseSchemas.ts";

export const RemoteSecretRequestId = Schema.String.check(Schema.isPattern(/^rsr_[0-9a-f]{32}$/));
export const RemoteSecretHandle = Schema.Struct({
  environmentId: EnvironmentId,
  requestId: RemoteSecretRequestId,
});
export type RemoteSecretHandle = typeof RemoteSecretHandle.Type;
export const RemoteSecretFamily = Schema.Literal("schedule.webhook-signature");
export const RemoteSecretSupport = Schema.Struct({
  version: Schema.Literal(1),
  families: Schema.Array(RemoteSecretFamily),
  consumers: Schema.Array(Schema.Literal("schedule_task")),
});
export const RemoteSecretOrigin = Schema.Struct({
  threadId: ThreadId,
  runId: RunId,
  namespace: Schema.String,
  generation: Schema.String,
});
export type RemoteSecretOrigin = typeof RemoteSecretOrigin.Type;
export const RemoteSecretCreate = Schema.Struct({
  type: Schema.Literal("create"),
  projectId: ProjectId,
  key: TrimmedNonEmptyString,
  family: RemoteSecretFamily,
  label: TrimmedNonEmptyString,
  reason: TrimmedNonEmptyString,
  placeholder: Schema.optional(TrimmedNonEmptyString),
  field: Schema.optional(SecretRequestField),
});
export type RemoteSecretCreate = typeof RemoteSecretCreate.Type;
export const RemoteSecretControl = Schema.Union([
  RemoteSecretCreate,
  Schema.Struct({ type: Schema.Literal("status"), requestId: RemoteSecretRequestId }),
  Schema.Struct({
    type: Schema.Literal("answer"),
    requestId: RemoteSecretRequestId,
    attemptId: Schema.String.check(Schema.isPattern(/^[0-9a-f]{32}$/)),
    answer: Schema.Literals(["save", "decline"]),
    value: Schema.optional(Schema.String.check(Schema.isMaxLength(65536))),
  }),
]);
export type RemoteSecretControl = typeof RemoteSecretControl.Type;
export const RemoteSecretStatus = Schema.Literals([
  "pending",
  "saving",
  "saved",
  "reserved",
  "consumed",
  "declined",
  "retired",
]);
export const RemoteSecretResult = Schema.Struct({
  handle: RemoteSecretHandle,
  status: RemoteSecretStatus,
});
export type RemoteSecretResult = typeof RemoteSecretResult.Type;
export const RemoteSecretRetirement = Schema.Struct({
  version: Schema.Literal(1),
  environmentId: EnvironmentId,
  generation: Schema.String,
  requestId: RemoteSecretRequestId,
  origin: RemoteSecretOrigin,
});
export type RemoteSecretRetirement = typeof RemoteSecretRetirement.Type;
export const RemoteSecretRetirementAck = Schema.Struct({
  ...RemoteSecretRetirement.fields,
  retired: Schema.Literal(true),
});
/** Private values never enter generic authority identifiers or request digests. */
export const remoteSecretIntent = (input: RemoteSecretControl) => {
  if (input.type !== "answer") return input;
  const { value: _value, ...intent } = input;
  return intent;
};
export const RemoteSecretDestination = Schema.Struct({
  ...RemoteSecretHandle.fields,
  environmentLabel: Schema.String,
  projectId: ProjectId,
});

export const RemoteSecretReceiptIdentity = Schema.Struct({
  id: Schema.String,
  generation: Schema.String,
  inputDigest: Schema.String,
  clientRequestId: Schema.String,
});

export const RemoteSecretReceiptQuery = Schema.Struct({
  handle: RemoteSecretHandle,
  projectId: ProjectId,
  envelope: PeerAuthorityEnvelope,
  request: RemoteSecretReceiptIdentity,
});
export type RemoteSecretReceiptQuery = typeof RemoteSecretReceiptQuery.Type;

export type RemoteSecretDestination = typeof RemoteSecretDestination.Type;
