import { EnvironmentId, ScheduledTaskId, ThreadId } from "./baseSchemas.ts";
import * as Schema from "effect/Schema";

/** A historical mutation confirmation, not a projection of the current resource. */
export const McpConfirmedOperation = Schema.Struct({
  type: Schema.Literal("confirmed-operation"),
  applied: Schema.Literal(true),
  requestId: Schema.String.check(Schema.isMaxLength(512)),
  operationId: Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/)),
  stage: Schema.Literal("confirmed"),
});
export type McpConfirmedOperation = typeof McpConfirmedOperation.Type;

/** Confirms scheduler admission or native command acceptance, never provider completion. */
export const McpManualRunReceipt = Schema.Struct({
  requestId: Schema.String.check(Schema.isMaxLength(512)),
  operationId: Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/)),
  stage: Schema.Literals(["native-accepted", "admitted", "skipped"]),
});
export type McpManualRunReceipt = typeof McpManualRunReceipt.Type;

export const McpScheduledMutationReceipt = Schema.Struct({
  ...McpConfirmedOperation.fields,
  scheduledTaskId: ScheduledTaskId,
});
export type McpScheduledMutationReceipt = typeof McpScheduledMutationReceipt.Type;

/** Original saved PNG acceptance; the file may be changed independently later. */
export const McpSavedSnapshotReceipt = Schema.Struct({
  ...McpConfirmedOperation.fields,
  artifact: Schema.Struct({
    environmentId: EnvironmentId,
    owner: Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/)),
    screenshotPath: Schema.String.check(Schema.isMaxLength(4096)),
    mimeType: Schema.Literal("image/png"),
    sizeBytes: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
    digest: Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/)),
  }),
});
export type McpSavedSnapshotReceipt = typeof McpSavedSnapshotReceipt.Type;

/** Renderer-free authority for this one historical preview purpose, never a tool proxy. */
export const PeerSavedPreviewAuthorizationSupport = Schema.Struct({
  version: Schema.Literal(1),
  purpose: Schema.Literal("saved-preview-authorization"),
});
export const PeerSavedPreviewAuthorizationRequest = Schema.Struct({
  ...PeerSavedPreviewAuthorizationSupport.fields,
  environmentId: EnvironmentId.check(Schema.isMaxLength(2048)),
  generation: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(2048)),
  contextThreadId: ThreadId.check(Schema.isMaxLength(4096)),
  operationId: McpConfirmedOperation.fields.operationId,
});
export type PeerSavedPreviewAuthorizationRequest = typeof PeerSavedPreviewAuthorizationRequest.Type;
export const PeerSavedPreviewAuthorizationResult = Schema.Struct({
  ...PeerSavedPreviewAuthorizationRequest.fields,
  authorized: Schema.Literal(true),
});
export type PeerSavedPreviewAuthorizationResult = typeof PeerSavedPreviewAuthorizationResult.Type;
