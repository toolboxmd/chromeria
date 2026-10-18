import { ChatFileAttachment, ChatImageAttachment, EnvironmentId } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as Struct from "effect/Struct";

// Screenshot provenance is app-owned. Existing attachments recover it from stored
// metadata; exposing its recursive accessibility tree breaks model tool schemas.
export const McpAttachmentInput = Schema.Union([
  Schema.Struct({
    ...Struct.omit(ChatImageAttachment.fields, ["source"]),
    environmentId: Schema.optional(EnvironmentId),
  }),
  Schema.Struct({ ...ChatFileAttachment.fields, environmentId: Schema.optional(EnvironmentId) }),
]);
