import * as FileSystem from "effect/FileSystem";
import {
  ChatAttachmentId,
  CommandId,
  getProviderAttachmentLimitError,
  type ChatAttachment,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { isSvgAttachment } from "@t3tools/shared/image";

import {
  parseThreadSegmentFromAttachmentId,
  PENDING_ATTACHMENT_THREAD_SEGMENT,
  planAttachmentClaim,
  resolveAttachmentPath,
} from "../attachmentStore.ts";
import * as ServerConfig from "../config.ts";
import * as DocumentText from "../provider/DocumentText.ts";
import * as Staging from "../assets/AttachmentStaging.ts";
import * as Invocation from "../mcp/McpInvocationContext.ts";
import * as Option from "effect/Option";
import * as Receipts from "./CommandReceiptStore.ts";
import { sha256 } from "@noble/hashes/sha2";
import * as Hex from "effect/encoding/Hex";

export class AttachmentClaimError extends Schema.TaggedError<AttachmentClaimError>()(
  "AttachmentClaimError",
  {
    message: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export const validateAttachmentLimits = Effect.fn("AttachmentClaims.validateAttachmentLimits")(
  function* (attachments: ReadonlyArray<ChatAttachment>) {
    const error = getProviderAttachmentLimitError(attachments);
    if (error) return yield* new AttachmentClaimError({ message: error });
  },
);

export interface ClaimedAttachments {
  readonly attachments: ReadonlyArray<ChatAttachment>;
  readonly claimedPaths: ReadonlyArray<string>;
}

export function attachmentIsPendingUpload(attachment: ChatAttachment): boolean {
  return parseThreadSegmentFromAttachmentId(attachment.id) === PENDING_ATTACHMENT_THREAD_SEGMENT;
}

/** Remove partial claims only before dispatch, or after proving they were not accepted. */
export const releaseClaimedAttachments = Effect.fn("AttachmentClaims.releaseClaimedAttachments")(
  function* (claimedPaths: ReadonlyArray<string>) {
    if (claimedPaths.length === 0) return;
    const fileSystem = yield* FileSystem.FileSystem;
    yield* Effect.forEach(
      claimedPaths,
      (path) =>
        fileSystem
          .remove(path)
          .pipe(
            Effect.ignore,
            Effect.andThen(fileSystem.remove(`${path}.txt`, { force: true }).pipe(Effect.ignore)),
          ),
      {
        concurrency: 1,
        discard: true,
      },
    ).pipe(Effect.uninterruptible);
  },
);

/** Copies preserve the pending retry source after failed bootstrap; the pending sweep reclaims it. */
export const claimPendingAttachments = Effect.fn("AttachmentClaims.claimPendingAttachments")(
  function* (input: {
    readonly threadId: string;
    readonly attachments: ReadonlyArray<ChatAttachment>;
    readonly commandId?: string;
  }) {
    yield* validateAttachmentLimits(input.attachments);
    if (
      new Set(input.attachments.map((attachment) => attachment.id)).size !==
      input.attachments.length
    ) {
      return yield* new AttachmentClaimError({
        message: "Duplicate attachment ids are not allowed.",
      });
    }
    const staging = yield* Effect.serviceOption(Staging.AttachmentStaging);
    if (!input.attachments.some(attachmentIsPendingUpload) && Option.isNone(staging)) {
      return { attachments: input.attachments, claimedPaths: [] } satisfies ClaimedAttachments;
    }
    const serverConfig = yield* ServerConfig.ServerConfig;
    const fileSystem = yield* FileSystem.FileSystem;
    const scope = yield* Effect.serviceOption(Invocation.McpInvocationContext);
    if (Option.isSome(staging) && input.commandId !== undefined)
      yield* staging.value
        .bindConsumption(
          Option.getOrUndefined(scope),
          input.commandId,
          input.threadId,
          input.attachments,
        )
        .pipe(
          Effect.mapError((cause) => new AttachmentClaimError({ message: cause.message, cause })),
        );
    const receipts = yield* Effect.serviceOption(Receipts.CommandReceiptStoreV2);
    const accepted = Effect.gen(function* () {
      if (input.commandId === undefined || Option.isNone(receipts)) return false;
      const receipt = yield* receipts.value.getByCommandId(CommandId.make(input.commandId)).pipe(
        Effect.mapError(
          (cause) =>
            new AttachmentClaimError({
              message: "The consuming command receipt cannot be verified.",
              cause,
            }),
        ),
      );
      return (
        Option.isSome(receipt) &&
        receipt.value.status === "accepted" &&
        receipt.value.commandType === "message.dispatch" &&
        receipt.value.threadId === input.threadId
      );
    });
    const claimedPaths: string[] = [];
    const documents = yield* DocumentText.DocumentText;
    const attachments = yield* Effect.forEach(
      input.attachments,
      (attachment) =>
        Effect.gen(function* () {
          const reservation =
            Option.isSome(staging) && input.commandId !== undefined
              ? yield* staging.value
                  .reserve(
                    Option.getOrUndefined(scope),
                    input.commandId,
                    input.threadId,
                    attachment,
                  )
                  .pipe(
                    Effect.mapError(
                      (cause) => new AttachmentClaimError({ message: cause.message, cause }),
                    ),
                  )
              : undefined;
          if (!attachmentIsPendingUpload(attachment) && reservation === undefined)
            return attachment;
          const claim =
            reservation ??
            planAttachmentClaim({
              attachmentsDir: serverConfig.attachmentsDir,
              threadId: input.threadId,
              attachmentId: attachment.id,
            });
          if (!claim.ok) {
            return yield* new AttachmentClaimError({
              message: `Attachment '${attachment.name}' cannot be sent: ${claim.reason}.`,
            });
          }
          const normalized: ChatAttachment = {
            ...attachment,
            id: ChatAttachmentId.make(claim.finalId),
            name:
              attachment.type === "file" &&
              isSvgAttachment(attachment) &&
              !/\.svg$/i.test(attachment.name)
                ? `${attachment.name}.svg`
                : attachment.name,
            mimeType: attachment.mimeType.toLowerCase(),
          };
          const expectedPath = resolveAttachmentPath({
            attachmentsDir: serverConfig.attachmentsDir,
            attachment: normalized,
          });
          if (expectedPath !== claim.finalPath) {
            return yield* new AttachmentClaimError({
              message: `Attachment '${attachment.name}' cannot be sent: attachment type does not match the upload.`,
            });
          }
          if (reservation !== undefined) {
            // An accepted native receipt owns the delivered file, including later
            // user edits. Never overwrite it or register it for rollback.
            if (yield* accepted) return normalized;
            if (Option.isNone(staging) || Option.isNone(scope))
              return yield* new AttachmentClaimError({
                message: "The attachment owner cannot be verified.",
              });
            const verified = yield* staging.value
              .verify(scope.value, attachment)
              .pipe(
                Effect.mapError(
                  (cause) => new AttachmentClaimError({ message: cause.message, cause }),
                ),
              );
            yield* Effect.scoped(
              Effect.gen(function* () {
                const temporary = yield* fileSystem.makeTempFileScoped({
                  directory: serverConfig.attachmentsDir,
                });
                yield* fileSystem.copyFile(claim.currentPath, temporary);
                // Link the completed COPY, never the mutable pending source. A crash
                // exposes either no final file or the whole file. Same-command
                // contenders cannot overwrite the first publication.
                yield* fileSystem.link(temporary, claim.finalPath).pipe(
                  Effect.catchTags({
                    PlatformError: (error) =>
                      error.reason._tag === "AlreadyExists" ? Effect.void : Effect.fail(error),
                  }),
                );
                if (yield* accepted) return;
                const bytes = yield* fileSystem.readFile(claim.finalPath);
                if (
                  bytes.byteLength !== attachment.sizeBytes ||
                  Hex.encode(sha256(bytes)) !== verified.digest
                )
                  return yield* new AttachmentClaimError({
                    message: "The claimed attachment bytes changed before acceptance.",
                  });
              }),
            ).pipe(
              Effect.mapError((cause) =>
                cause._tag === "AttachmentClaimError"
                  ? cause
                  : new AttachmentClaimError({
                      message: "Failed to publish the attachment claim.",
                      cause,
                    }),
              ),
            );
            // The durable reservation owns crash leftovers. Transient rollback
            // must not remove a file concurrently accepted by this same command.
            return normalized;
          }
          const info = yield* fileSystem.stat(claim.currentPath).pipe(
            Effect.mapError(
              (cause) =>
                new AttachmentClaimError({
                  message: `Attachment '${attachment.name}' cannot be sent: attachment not found.`,
                  cause,
                }),
            ),
          );
          if (Number(info.size) !== attachment.sizeBytes) {
            return yield* new AttachmentClaimError({
              message: `Attachment '${attachment.name}' cannot be sent: stored size does not match.`,
            });
          }
          // Copy, not hard link: an agent editing the delivered file must not mutate the retry source.
          // fs.copyFile cannot be cancelled, so the copy and its rollback registration share one
          // uninterruptible region and an interrupt still records the path before cleanup.
          yield* fileSystem.copyFile(claim.currentPath, claim.finalPath).pipe(
            Effect.mapError(
              (cause) =>
                new AttachmentClaimError({
                  message: `Failed to claim attachment '${attachment.name}' for this thread.`,
                  cause,
                }),
            ),
            Effect.andThen(Effect.sync(() => claimedPaths.push(claim.finalPath))),
            Effect.uninterruptible,
          );
          const sidecar = `${claim.currentPath}.txt`;
          if (yield* fileSystem.exists(sidecar).pipe(Effect.orElseSucceed(() => false))) {
            yield* fileSystem.copyFile(sidecar, `${claim.finalPath}.txt`).pipe(
              Effect.andThen(Effect.sync(() => claimedPaths.push(`${claim.finalPath}.txt`))),
              Effect.onError(() =>
                fileSystem.remove(`${claim.finalPath}.txt`, { force: true }).pipe(Effect.ignore),
              ),
              Effect.uninterruptible,
              Effect.ignore,
            );
          } else {
            const extracted = yield* documents.ensure(claim.finalPath);
            if (extracted) claimedPaths.push(extracted);
          }
          return normalized;
        }),
      { concurrency: 1 },
    ).pipe(Effect.onError(() => releaseClaimedAttachments(claimedPaths)));
    return { attachments, claimedPaths } satisfies ClaimedAttachments;
  },
);
