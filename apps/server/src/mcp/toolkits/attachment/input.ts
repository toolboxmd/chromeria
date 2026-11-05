import { ChatFileAttachment, ChatImageAttachment, EnvironmentId } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as Struct from "effect/Struct";

// Keep screenshot provenance in app metadata; its recursive tree breaks model tool schemas.
export const McpAttachmentInput = Schema.Union([
  Schema.Struct({
    ...Struct.omit(ChatImageAttachment.fields, ["source"]),
    environmentId: Schema.optional(EnvironmentId),
  }),
  Schema.Struct({ ...ChatFileAttachment.fields, environmentId: Schema.optional(EnvironmentId) }),
]);
