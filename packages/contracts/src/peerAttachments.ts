import * as Schema from "effect/Schema";
import { AttachmentCreateUploadUrlResult } from "./assets.ts";
import { EnvironmentId } from "./baseSchemas.ts";
import { ChatAttachmentId, ChatFileAttachment } from "./chatAttachment.ts";

export const AttachmentDigest = Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/));
export const PeerRequestProtocol = Schema.Struct({
  version: Schema.Literal(1),
  attachments: Schema.Boolean,
});
export type PeerRequestProtocol = typeof PeerRequestProtocol.Type;
/** An address and verified manifest, never a URL, path or bearer capability. */
export const PeerAttachmentReference = Schema.Struct({
  environmentId: EnvironmentId,
  attachmentId: ChatAttachmentId,
  owner: AttachmentDigest,
  name: ChatFileAttachment.fields.name,
  digest: AttachmentDigest,
  mimeType: Schema.String.check(Schema.isMaxLength(100)),
  sizeBytes: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 50 * 1024 * 1024 })),
});
export type PeerAttachmentReference = typeof PeerAttachmentReference.Type;

/** Staged bytes are recomputed; native acceptance attests the original consumed manifest. */
export const PeerAttachmentTransferState = Schema.Union([
  Schema.Struct({ stage: Schema.Literal("awaiting-bytes") }),
  Schema.Struct({
    stage: Schema.Literals(["staged", "native-accepted"]),
    digest: AttachmentDigest,
  }),
]);
export type PeerAttachmentTransferState = typeof PeerAttachmentTransferState.Type;

export const PeerAttachmentUploadResult = Schema.Struct({
  ...AttachmentCreateUploadUrlResult.fields,
  transferState: Schema.optional(PeerAttachmentTransferState),
});
export type PeerAttachmentUploadResult = typeof PeerAttachmentUploadResult.Type;
