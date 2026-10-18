import {
  ATTACHMENT_UPLOAD_URL_TTL_MS,
  type ChatAttachment,
  MessageId,
  OrchestratorMcpFailure,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as Upload from "../../../assets/AttachmentUpload.ts";
import * as Claims from "../../../orchestration-v2/AttachmentClaims.ts";
import * as ThreadMessageIntake from "../../../orchestration-v2/ThreadMessageIntake.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import { newCommandId, readThread, unavailable } from "../../threadAccess.ts";
import { AttachmentToolkit } from "./tools.ts";
import * as Staging from "../../../assets/AttachmentStaging.ts";
import { CurrentRequest, requestDigest } from "../../McpRequestReceipts.ts";
import { requestNamespace } from "../../requestIdentity.ts";
import * as Option from "effect/Option";
import { CommandId } from "@t3tools/contracts";

export function resolveAttachmentReferences(
  requested: ReadonlyArray<ChatAttachment>,
  stored: ReadonlyArray<ChatAttachment>,
) {
  const owned = new Map(stored.map((attachment) => [attachment.id, attachment]));
  return Effect.forEach(requested, (attachment) =>
    Effect.gen(function* () {
      const canonical = Claims.attachmentIsPendingUpload(attachment)
        ? attachment
        : owned.get(attachment.id);
      if (canonical !== undefined) return canonical;
      const staging = yield* Effect.serviceOption(Staging.AttachmentStaging);
      const scope = yield* Effect.serviceOption(McpInvocationContext.McpInvocationContext);
      if (Option.isSome(staging) && Option.isSome(scope)) {
        yield* staging.value.verify(scope.value, attachment);
        return attachment;
      }
      return yield* new OrchestratorMcpFailure({
        code: "invalid_request",
        message:
          "Attachments must be ready app-owned uploads or already belong to an authorized thread.",
      });
    }),
  );
}

/**
 * The sweep counts its 24 hours from when the upload finishes, which can be up
 * to the upload URL's lifetime after issue.
 */
const UPLOAD_OWNER_TTL_MS = 24 * 60 * 60 * 1000 + ATTACHMENT_UPLOAD_URL_TTL_MS;

const uploadOwner = McpInvocationContext.McpInvocationContext.pipe(
  Effect.map((scope) => scope.thread?.threadId ?? scope.requestNamespace),
);

export const layer = McpToolAccess.toLayer(
  AttachmentToolkit,
  Effect.sync(() => {
    const uploadOwners = new Map<string, { readonly owner: string; readonly issuedAt: number }>();
    return {
      // A pending upload belongs to its caller until a send claims it, so
      // linked callers may stage the uploads their own sends need.
      t3_attachment_prepare_upload: McpToolAccess.writes(
        (input) =>
          Effect.gen(function* () {
            const scope = yield* McpInvocationContext.McpInvocationContext;
            const staging = yield* Effect.serviceOption(Staging.AttachmentStaging);
            const binding = Option.isSome(staging)
              ? yield* staging.value.prepare(
                  scope,
                  input.upload,
                  input.clientRequestId ?? String(yield* newCommandId()),
                  input.digest,
                )
              : undefined;
            if (binding === undefined && input.digest !== undefined)
              return yield* new OrchestratorMcpFailure({
                code: "invalid_request",
                message: "The destination does not support durable attachment staging.",
              });
            const result = yield* Upload.issueAttachmentUploadUrl(
              input.upload,
              binding === undefined
                ? undefined
                : { ...binding, ...(input.digest === undefined ? {} : { digest: input.digest }) },
            ).pipe(Effect.mapError(unavailable));
            const now = yield* Clock.currentTimeMillis;
            for (const [id, entry] of uploadOwners) {
              if (now - entry.issuedAt > UPLOAD_OWNER_TTL_MS) uploadOwners.delete(id);
            }
            uploadOwners.set(result.attachmentId, { owner: yield* uploadOwner, issuedAt: now });
            const transferState =
              binding === undefined || Option.isNone(staging) || input.clientRequestId === undefined
                ? undefined
                : yield* staging.value.inspectTransfer(scope, binding.attachmentId, input.upload);
            return { ...result, ...(transferState === undefined ? {} : { transferState }) };
          }),
        { linkedCallers: "allowed" },
      ),
      t3_attachment_discard: McpToolAccess.writes(
        (input) =>
          Effect.gen(function* () {
            const staging = yield* Effect.serviceOption(Staging.AttachmentStaging);
            const request = yield* CurrentRequest;
            if (Option.isSome(staging))
              yield* staging.value.discard(
                yield* McpInvocationContext.McpInvocationContext,
                input.attachmentId,
                request,
              );
            else if (uploadOwners.get(input.attachmentId)?.owner !== (yield* uploadOwner)) {
              return yield* new OrchestratorMcpFailure({
                code: "invalid_request",
                message: "Only the caller that prepared a pending upload can discard it.",
              });
            }
            if (Option.isNone(staging) || request === undefined)
              yield* Upload.deletePendingAttachment(input.attachmentId);
            uploadOwners.delete(input.attachmentId);
            return {};
          }),
        { linkedCallers: "allowed" },
      ),
      t3_thread_send_attachments: McpToolAccess.writesThreads(
        (input) => [input.threadId],
        (input) =>
          Effect.gen(function* () {
            const { caller, projection } = yield* readThread(input.threadId, ["messages"]);
            if (projection.thread.archivedAt !== null)
              return yield* new OrchestratorMcpFailure({
                code: "invalid_request",
                message: "Unarchive the target thread before sending attachments.",
              });
            const attachments = yield* resolveAttachmentReferences(
              input.attachments,
              projection.messages.flatMap((message) => message.attachments),
            );
            const scope = yield* McpInvocationContext.McpInvocationContext;
            const commandId =
              input.clientRequestId === undefined
                ? yield* newCommandId()
                : CommandId.make(
                    `mcp:attachment-send:${requestDigest([requestNamespace(scope), input.clientRequestId])}`,
                  );
            const messageId = MessageId.make(commandId);
            const result = yield* ThreadMessageIntake.sendToThread({
              projectId: projection.thread.projectId,
              threadId: projection.thread.id,
              commandId,
              messageId,
              ...(caller === undefined ? {} : { senderThreadId: caller.id }),
              text: input.message ?? "",
              attachments,
              mode: "auto",
              createdBy: "agent",
              creationSource: "mcp",
            }).pipe(
              Effect.mapError((error) =>
                error._tag === "AttachmentClaimError"
                  ? new OrchestratorMcpFailure({
                      code: "orchestration_error",
                      message: error.message,
                    })
                  : unavailable(),
              ),
            );
            return {
              threadId: projection.thread.id,
              messageId,
              runId: result.run.id,
              status: result.run.status,
            };
          }),
      ),
    };
  }),
);
