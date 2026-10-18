import * as McpRouting from "../../McpRouting.ts";
import { McpAttachmentInput } from "./input.ts";
import {
  AttachmentCreateUploadUrlInput,
  PeerAttachmentUploadResult,
  AttachmentDeleteInput,
  AttachmentDigest,
  MessageId,
  RunId,
  ThreadId,
  OrchestrationV2RunStatus,
  OrchestratorMcpFailure,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/ai";
import * as ServerSecretStore from "../../../auth/ServerSecretStore.ts";
import * as ServerConfig from "../../../config.ts";
import * as ThreadManagementService from "../../../orchestration-v2/ThreadManagementService.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";

const shared = {
  failure: OrchestratorMcpFailure,
  failureMode: "return" as const,
  dependencies: [
    McpInvocationContext.McpInvocationContext,
    ThreadManagementService.ThreadManagementService,
    ServerConfig.ServerConfig,
    ServerSecretStore.ServerSecretStore,
    FileSystem.FileSystem,
    Crypto.Crypto,
  ],
};
const AttachmentUploadTool = Tool.make("t3_attachment_prepare_upload", {
  ...shared,
  description:
    "Create the app's signed upload URL. POST the exact bytes to relativeUrl on uploadOrigin when returned, otherwise this MCP server's HTTP origin, then pass the attachment metadata and returned ID to t3_thread_send_attachments. Upload is separate from sending; provider attachment support is decided by its adapter.",
  parameters: McpRouting.parameters(
    Schema.Struct({
      upload: AttachmentCreateUploadUrlInput,
      digest: Schema.optional(AttachmentDigest),
    }),
  ),
  success: PeerAttachmentUploadResult,
}).annotate(Tool.Destructive, true);
const AttachmentDiscardTool = Tool.make("t3_attachment_discard", {
  ...shared,
  description:
    "Discard a pending upload. clientRequestId recovers the original discard acceptance and invalidates the upload without deleting its pending bytes; success does not assert current file absence. Already-delivered thread attachments are never deleted by this operation.",
  parameters: McpRouting.parameters(AttachmentDeleteInput),
  success: Schema.Struct({}),
}).annotate(Tool.Destructive, true);
const AttachmentSendTool = Tool.make("t3_thread_send_attachments", {
  ...shared,
  description:
    "Send uploaded attachments to a permitted thread. clientRequestId retains one message across retries and credential renewal. A descendant's ancestor notice queues behind active work; Stopped ancestors stay stopped. Acceptance records the message, not provider consumption or completion. The target must remain within the caller's authority; uncertain dispatch retains its attachment files.",
  parameters: McpRouting.parameters(
    Schema.Struct({
      threadId: Schema.optional(ThreadId),
      message: Schema.optional(Schema.String.check(Schema.isMaxLength(120000))),
      attachments: Schema.Array(McpAttachmentInput).check(
        Schema.isMinLength(1),
        Schema.isMaxLength(8),
      ),
    }),
  ),
  success: Schema.Struct({
    threadId: ThreadId,
    messageId: MessageId,
    runId: RunId,
    status: OrchestrationV2RunStatus,
  }),
})
  .annotate(Tool.Destructive, true)
  .annotate(Tool.OpenWorld, true);
export const AttachmentToolkit = Toolkit.make(
  AttachmentUploadTool,
  AttachmentDiscardTool,
  AttachmentSendTool,
);
