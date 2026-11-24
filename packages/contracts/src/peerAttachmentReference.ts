import * as Schema from "effect/Schema";

import { EnvironmentId, ThreadId } from "./baseSchemas.ts";
import { ChatAttachmentId, ChatFileAttachment } from "./chatAttachment.ts";

// A leaf module: device and settings contracts import these without reaching assets.ts,
// which imports server.ts and would close a cycle through settings.ts.
export const AttachmentDigest = Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/));
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

export const PeerAttachmentAssetResource = Schema.TaggedStruct("peer-attachment", {
  threadId: ThreadId,
  reference: PeerAttachmentReference,
  disposition: Schema.optionalKey(Schema.Literals(["inline", "attachment"])),
});
export type PeerAttachmentAssetResource = typeof PeerAttachmentAssetResource.Type;
