import * as McpRouting from "../../McpRouting.ts";
import { McpAttachmentInput } from "./input.ts";
import {
  AttachmentCreateUploadUrlInput,
  PeerAttachmentInspectSource,
  PeerAttachmentInspectResult,
  PeerAttachmentAcquireInput,
  PeerAttachmentAcquisitionResult,
  closedAttachmentInput,
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
import * as DocumentText from "../../../provider/DocumentText.ts";
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
    "Get an upload URL. POST the file's exact bytes to `relativeUrl` on the returned `uploadOrigin`, or on this MCP server's HTTP origin when none is returned, then send via `t3_thread_send_attachments` with the returned `attachmentId`.",
  parameters: McpRouting.parameters(
    Schema.Struct({
      upload: AttachmentCreateUploadUrlInput,
      digest: Schema.optional(AttachmentDigest),
    }),
  ),
  success: PeerAttachmentUploadResult,
}).annotate(Tool.Destructive, true);
const AttachmentInspectTool = Tool.make("t3_attachment_inspect", {
  ...shared,
  description:
    "Inspect attachment transfer support or an upload owned by this caller. Awaiting bytes and reserved uploads have no reusable reference. A staged reference describes verified current bytes; native-accepted records historical consumption and does not promise current file readiness.",
  parameters: closedAttachmentInput(
    McpRouting.parameters(Schema.Struct({ source: PeerAttachmentInspectSource })).fields,
  ),
  success: PeerAttachmentInspectResult,
})
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, false);
const AttachmentAcquireTool = Tool.make("t3_attachment_acquire", {
  ...shared,
  description:
    "Acquire a qualified permitted device/browser artifact into this caller's native attachment owner. Requires a durable clientRequestId. Native acceptance and its source-local path survive retries without replacing accepted files; acceptance is historical and does not promise current file readiness. No destination path or signed URL is accepted.",
  parameters: closedAttachmentInput({
    ...McpRouting.parameters(PeerAttachmentAcquireInput).fields,
    clientRequestId: PeerAttachmentAcquireInput.fields.clientRequestId,
  }),
  success: PeerAttachmentAcquisitionResult,
})
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, false);
const AttachmentDiscardTool = Tool.make("t3_attachment_discard", {
  ...shared,
  description:
    "Discard a pending upload (not a sent attachment). `clientRequestId` recovers the original discard acceptance and invalidates the upload without deleting its pending bytes; success does not assert the file is gone.",
  parameters: McpRouting.parameters(AttachmentDeleteInput),
  success: Schema.Struct({}),
}).annotate(Tool.Destructive, true);
const AttachmentSendTool = Tool.make("t3_thread_send_attachments", {
  ...shared,
  dependencies: [...shared.dependencies, DocumentText.DocumentText],
  description:
    "Send uploaded attachments as a new message to a permitted thread. `clientRequestId` keeps one message across retries and credential renewal. A descendant agent's send to an ordinary ancestor is a child notice queued behind active work; Stopped ancestors stay stopped. Acceptance records the message, not provider consumption. The target must stay within the caller's authority; uncertain dispatch keeps its attachment files.",
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
  AttachmentInspectTool,
  AttachmentAcquireTool,
  AttachmentDiscardTool,
  AttachmentSendTool,
);
