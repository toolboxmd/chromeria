import * as Schema from "effect/Schema";
import {
  AttachmentCreateUploadUrlResult,
  AttachmentCreateUploadUrlInput,
  PeerAttachmentAssetResource,
  NativeBrowserDownloadAssetResource,
} from "./assets.ts";
import { TrimmedNonEmptyString } from "./baseSchemas.ts";
import { AttachmentDigest, PeerAttachmentReference } from "./peerAttachmentReference.ts";
import { ChatAttachmentId, ChatFileAttachment } from "./chatAttachment.ts";

export { AttachmentDigest, PeerAttachmentReference };

export const PeerRequestProtocol = Schema.Struct({
  version: Schema.Literal(1),
  attachments: Schema.Boolean,
});
export type PeerRequestProtocol = typeof PeerRequestProtocol.Type;

/** Staged bytes are recomputed; native acceptance attests the original consumed manifest. */
export const PeerAttachmentTransferState = Schema.Union([
  Schema.Struct({ stage: Schema.Literal("awaiting-bytes") }),
  Schema.Struct({
    stage: Schema.Literals(["staged", "native-accepted"]),
    digest: AttachmentDigest,
  }),
]);
export type PeerAttachmentTransferState = typeof PeerAttachmentTransferState.Type;

// Asset resources reuse PeerAttachmentReference; defer the upload-result edge
// so both existing public schemas can initialize without duplicating either.
export const PeerAttachmentUploadResult = Schema.suspend(() =>
  Schema.Struct({
    ...AttachmentCreateUploadUrlResult.fields,
    transferState: Schema.optional(PeerAttachmentTransferState),
    uploadUrl: Schema.optionalKey(Schema.String),
  }),
);
export type PeerAttachmentUploadResult = typeof PeerAttachmentUploadResult.Type;

// New attachment operations are closed at the public boundary. Reuse the
// canonical manifest/reference schemas, including their byte and field bounds.
export const closedAttachmentInput = <F extends Schema.Struct.Fields>(fields: F) =>
  Schema.StructWithRest(Schema.Struct(fields), [Schema.Record(Schema.String, Schema.Unknown)])
    .check(
      Schema.makeFilter(
        (value) =>
          Object.keys(value).every((key) => Object.hasOwn(fields, key)) ||
          "Unknown attachment option.",
      ),
    )
    .pipe(Schema.decodeTo(Schema.Struct(fields)));
export const PeerAttachmentInspectSource = Schema.Union([
  closedAttachmentInput({ type: Schema.Literal("capabilities") }),
  closedAttachmentInput({
    type: Schema.Literal("upload"),
    attachmentId: ChatAttachmentId,
    upload: Schema.suspend(() =>
      Schema.Union(
        AttachmentCreateUploadUrlInput.members.map((member) =>
          closedAttachmentInput(member.fields),
        ),
      ).pipe(Schema.decodeTo(AttachmentCreateUploadUrlInput)),
    ),
  }),
]);
export const PeerAttachmentUploadInspection = Schema.Union([
  Schema.Struct({ stage: Schema.Literal("awaiting-bytes") }),
  Schema.Struct({ stage: Schema.Literal("reserved") }),
  Schema.Struct({ stage: Schema.Literal("native-accepted"), digest: AttachmentDigest }),
  Schema.Struct({ stage: Schema.Literal("staged"), reference: PeerAttachmentReference }),
]);
export type PeerAttachmentUploadInspection = typeof PeerAttachmentUploadInspection.Type;
export const PeerAttachmentInspectResult = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("capabilities"),
    version: Schema.Literal(1),
    maxBytes: Schema.Literal(50 * 1024 * 1024),
    previewInputArtifacts: Schema.optionalKey(Schema.Literal(1)),
  }),
  Schema.Struct({ type: Schema.Literal("upload"), inspection: PeerAttachmentUploadInspection }),
]);
export type PeerAttachmentInspectResult = typeof PeerAttachmentInspectResult.Type;
export const PeerAttachmentAcquireInput = Schema.Struct({
  resource: Schema.suspend(() =>
    Schema.Union([
      closedAttachmentInput({
        ...PeerAttachmentAssetResource.fields,
        reference: closedAttachmentInput(PeerAttachmentReference.fields),
      }),
      NativeBrowserDownloadAssetResource,
    ]),
  ),
  clientRequestId: TrimmedNonEmptyString.check(Schema.isMaxLength(512)),
});
export const PeerAttachmentAcquisitionResult = Schema.suspend(() =>
  Schema.Struct({
    acceptance: Schema.Literal("native-accepted"),
    resource: PeerAttachmentAssetResource,
    attachment: ChatFileAttachment,
    path: TrimmedNonEmptyString.check(Schema.isMaxLength(4096)),
  }),
);
export type PeerAttachmentAcquisitionResult = typeof PeerAttachmentAcquisitionResult.Type;
