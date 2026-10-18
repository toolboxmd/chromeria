import * as CommandReceipts from "../orchestration-v2/CommandReceiptStore.ts";
import {
  type AttachmentCreateUploadUrlInput,
  type ChatAttachment,
  OrchestratorMcpFailure,
  type PeerAttachmentReference,
  type PeerAttachmentTransferState,
  CommandId,
  AttachmentDigest,
  PROVIDER_SEND_TURN_MAX_FILE_BYTES,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import { sha256 } from "@noble/hashes/sha2";
import * as Hex from "effect/encoding/Hex";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Semaphore from "effect/Semaphore";
import {
  createPendingAttachmentId,
  createAttachmentId,
  parseAttachmentFileExtension,
  resolveAttachmentPathById,
  resolveAttachmentPath,
  type AttachmentClaimPlan,
  attachmentFileExtension,
  toSafeThreadAttachmentSegment,
  parseAttachmentUuid,
  PENDING_ATTACHMENT_THREAD_SEGMENT,
} from "../attachmentStore.ts";
import * as Invocation from "../mcp/McpInvocationContext.ts";
import { requestGeneration, requestNamespace } from "../mcp/requestIdentity.ts";
import { CurrentRequest, requestDigest, type RequestIdentity } from "../mcp/McpRequestReceipts.ts";
import * as Config from "../config.ts";
import * as PeerAuthority from "../peer/PeerAuthority.ts";
import * as Option from "effect/Option";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";

const decodeAttachmentDigest = Schema.decodeUnknownEffect(AttachmentDigest);

const encode = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const denied = () =>
  new OrchestratorMcpFailure({
    code: "invalid_request",
    message:
      "The attachment is not ready, belongs to another caller, or is reserved for another command.",
  });
const unavailable = (_cause: unknown) =>
  new OrchestratorMcpFailure({
    code: "orchestration_error",
    message: "The durable attachment owner is unavailable.",
  });

export class AttachmentStaging extends Context.Service<
  AttachmentStaging,
  {
    readonly prepare: (
      scope: Invocation.McpInvocationScope,
      input: AttachmentCreateUploadUrlInput,
      key: string,
      digest?: string,
    ) => Effect.Effect<{ attachmentId: string; owner: string }, OrchestratorMcpFailure>;
    readonly transferDigest: (
      scope: Invocation.McpInvocationScope,
      consumer: string,
      destination: string,
      attachment: ChatAttachment,
    ) => Effect.Effect<string, OrchestratorMcpFailure>;
    readonly inspectTransfer: (
      scope: Invocation.McpInvocationScope,
      id: string,
      input: AttachmentCreateUploadUrlInput,
    ) => Effect.Effect<PeerAttachmentTransferState, OrchestratorMcpFailure>;
    readonly publishReference: (
      scope: Invocation.McpInvocationScope,
      attachment: ChatAttachment,
    ) => Effect.Effect<PeerAttachmentReference, OrchestratorMcpFailure>;
    readonly claimRecording: (
      scope: Invocation.McpInvocationScope,
      pendingId: string,
      attachment: ChatAttachment,
    ) => Effect.Effect<PeerAttachmentReference, OrchestratorMcpFailure>;
    readonly verify: (
      scope: Invocation.McpInvocationScope,
      attachment: ChatAttachment,
    ) => Effect.Effect<
      { path: string; digest: string; reference: PeerAttachmentReference },
      OrchestratorMcpFailure
    >;
    readonly complete: (
      id: string,
      owner: string,
      digest: string,
    ) => Effect.Effect<void, OrchestratorMcpFailure>;
    readonly authorizeUpload: (
      id: string,
      owner: string,
    ) => Effect.Effect<void, OrchestratorMcpFailure>;
    readonly reserveTransfer: (
      scope: Invocation.McpInvocationScope,
      consumer: string,
      destination: string,
      attachments: ReadonlyArray<ChatAttachment>,
    ) => Effect.Effect<void, OrchestratorMcpFailure>;
    readonly bindConsumption: (
      scope: Invocation.McpInvocationScope | undefined,
      commandId: string,
      threadId: string,
      attachments: ReadonlyArray<ChatAttachment>,
    ) => Effect.Effect<void, OrchestratorMcpFailure>;
    readonly reserve: (
      scope: Invocation.McpInvocationScope | undefined,
      commandId: string,
      threadId: string,
      attachment: ChatAttachment,
    ) => Effect.Effect<AttachmentClaimPlan | undefined, OrchestratorMcpFailure>;
    readonly discard: (
      scope: Invocation.McpInvocationScope,
      id: string,
      request?: RequestIdentity,
    ) => Effect.Effect<void, OrchestratorMcpFailure>;
  }
>()("t3/assets/AttachmentStaging") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const fs = yield* FileSystem.FileSystem;
  const config = yield* Config.ServerConfig;
  const authority = yield* Effect.serviceOption(PeerAuthority.PeerAuthority);
  const receipts = yield* Effect.serviceOption(CommandReceipts.CommandReceiptStoreV2);
  const recordingClaims = yield* Semaphore.make(1);
  yield* sql`CREATE TABLE IF NOT EXISTS attachment_staging (
    id TEXT PRIMARY KEY, request_id TEXT NOT NULL UNIQUE, owner TEXT NOT NULL, generation TEXT NOT NULL,
    manifest TEXT NOT NULL, digest TEXT, expected_digest TEXT, consumer TEXT, target TEXT, claimed_id TEXT)`.pipe(
    Effect.orDie,
  );
  type Row = {
    id: string;
    owner: string;
    generation: string;
    manifest: string;
    digest: string | null;
    expected_digest: string | null;
    consumer: string | null;
    target: string | null;
    claimed_id: string | null;
    discard_receipt: string | null;
    discard_cleanup: string | null;
  };
  const columns = new Set(
    (yield* sql<{ name: string }>`PRAGMA table_info(attachment_staging)`).map((row) => row.name),
  );
  if (!columns.has("discard_receipt"))
    yield* sql`ALTER TABLE attachment_staging ADD COLUMN discard_receipt TEXT`.pipe(Effect.orDie);
  if (!columns.has("discard_cleanup"))
    yield* sql`ALTER TABLE attachment_staging ADD COLUMN discard_cleanup TEXT`.pipe(Effect.orDie);
  yield* sql`CREATE TABLE IF NOT EXISTS attachment_staging_authority (attachment_id TEXT PRIMARY KEY, record_id TEXT NOT NULL)`.pipe(
    Effect.orDie,
  );
  yield* sql`CREATE TABLE IF NOT EXISTS attachment_consumptions (
    command_id TEXT PRIMARY KEY, manifest TEXT NOT NULL)`.pipe(Effect.orDie);
  yield* sql`CREATE TABLE IF NOT EXISTS attachment_transfers (consumer TEXT NOT NULL, attachment_id TEXT NOT NULL, binding TEXT NOT NULL, digest TEXT NOT NULL, PRIMARY KEY(consumer,attachment_id))`.pipe(
    Effect.orDie,
  );
  const owner = (scope: Invocation.McpInvocationScope) => requestDigest(requestNamespace(scope));
  const manifest = (a: { type?: string; name: string; mimeType: string; sizeBytes: number }) =>
    encode({
      type: a.type ?? "image",
      name: a.name,
      mimeType: a.mimeType.toLowerCase(),
      sizeBytes: a.sizeBytes,
    });
  const read = (id: string) =>
    sql<Row>`SELECT * FROM attachment_staging WHERE id=${id}`.pipe(
      Effect.map((rows) => rows[0]),
      Effect.mapError(unavailable),
    );
  const owned = (scope: Invocation.McpInvocationScope, row: Row | undefined) =>
    row !== undefined && row.owner === owner(scope) && row.generation === requestGeneration(scope);
  const reserve: AttachmentStaging["Service"]["reserve"] = (
    scope,
    commandId,
    threadId,
    attachment,
  ) =>
    Effect.gen(function* () {
      const row = yield* read(attachment.id);
      if (row === undefined) return undefined; // Pre-protocol native uploads retain their existing owner.
      if (
        scope === undefined ||
        !owned(scope, row) ||
        row.digest === null ||
        row.manifest !== manifest(attachment)
      )
        return yield* denied();
      const extension = parseAttachmentFileExtension(attachment.id) ?? undefined;
      const claimedId = createAttachmentId(threadId, extension);
      if (claimedId === null) return yield* denied();
      yield* sql`UPDATE attachment_staging SET consumer=${commandId}, target=${threadId}, claimed_id=${claimedId}
        WHERE id=${attachment.id} AND consumer IS NULL`.pipe(Effect.mapError(unavailable));
      const reserved = yield* read(attachment.id);
      if (
        reserved?.consumer !== commandId ||
        reserved.target !== threadId ||
        reserved.claimed_id === null
      )
        return yield* denied();
      const currentPath = resolveAttachmentPath({
        attachmentsDir: config.attachmentsDir,
        attachment,
      });
      const finalPath = resolveAttachmentPath({
        attachmentsDir: config.attachmentsDir,
        attachment: { ...attachment, id: reserved.claimed_id },
      });
      if (currentPath === null || finalPath === null) return yield* denied();
      return { ok: true, finalId: reserved.claimed_id, currentPath, finalPath };
    });
  const bindIntent = (
    scope: Invocation.McpInvocationScope | undefined,
    commandId: string,
    threadId: string,
    attachments: ReadonlyArray<ChatAttachment>,
  ) =>
    Effect.gen(function* () {
      const rows = yield* Effect.forEach(attachments, (attachment) => read(attachment.id));
      if (!rows.some((row) => row !== undefined)) return;
      if (scope === undefined) return yield* denied();
      const binding = requestDigest({
        owner: owner(scope),
        generation: requestGeneration(scope),
        threadId,
        attachments,
      });
      yield* sql`INSERT INTO attachment_consumptions (command_id,manifest) VALUES (${commandId},${binding}) ON CONFLICT DO NOTHING`.pipe(
        Effect.mapError(unavailable),
      );
      const existing = yield* sql<{
        manifest: string;
      }>`SELECT manifest FROM attachment_consumptions WHERE command_id=${commandId}`.pipe(
        Effect.mapError(unavailable),
      );
      if (existing[0]?.manifest !== binding) return yield* denied();
    });
  const verify: AttachmentStaging["Service"]["verify"] = (scope, attachment) =>
    Effect.gen(function* () {
      const row = yield* read(attachment.id);
      let expectedDigest: string | undefined;
      if (row !== undefined) {
        if (!owned(scope, row) || row.digest === null || row.manifest !== manifest(attachment))
          return yield* denied();
        expectedDigest = row.digest;
      } else {
        // Existing attachments are transferable only when the authenticated
        // calling thread actually owns a stored reference to these bytes.
        const threads = yield* Effect.serviceOption(Threads.ThreadManagementService);
        if (scope.thread === undefined || Option.isNone(threads)) return yield* denied();
        const records = yield* threads.value
          .getThreadRecords(scope.thread.threadId, ["messages"])
          .pipe(Effect.mapError(unavailable));
        if (
          !records.messages.some((message) =>
            message.attachments.some(
              (stored) => stored.id === attachment.id && manifest(stored) === manifest(attachment),
            ),
          )
        )
          return yield* denied();
      }
      if (attachment.sizeBytes > 50 * 1024 * 1024) return yield* denied();
      const path = resolveAttachmentPathById({
        attachmentsDir: config.attachmentsDir,
        attachmentId: attachment.id,
      });
      if (path === null) return yield* denied();
      const bytes = yield* fs.readFile(path).pipe(Effect.mapError(unavailable));
      // Byte digest is independent of the JSON request fingerprint.
      const digest = Hex.encode(sha256(bytes));
      if (
        bytes.byteLength !== attachment.sizeBytes ||
        (expectedDigest !== undefined && digest !== expectedDigest)
      )
        return yield* denied();
      return {
        path,
        digest,
        reference: {
          environmentId: scope.environmentId,
          attachmentId: attachment.id,
          owner: owner(scope),
          name: attachment.name,
          digest,
          mimeType: attachment.mimeType,
          sizeBytes: attachment.sizeBytes,
        },
      };
    });
  const transferBinding = (
    scope: Invocation.McpInvocationScope,
    destination: string,
    attachment: ChatAttachment,
  ) =>
    requestDigest([
      owner(scope),
      requestGeneration(scope),
      destination,
      attachment.id,
      manifest(attachment),
    ]);
  return AttachmentStaging.of({
    claimRecording: (scope, pendingId, attachment) =>
      recordingClaims.withPermit(
        Effect.gen(function* () {
          // Current authority is checked outside SQL, even for accepted references.
          if (scope.authority !== undefined) {
            if (Option.isNone(authority) || scope.thread === undefined)
              return yield* unavailable(undefined);
            yield* authority.value.checkTarget(scope, {
              type: "thread",
              threadId: scope.thread.threadId,
            });
          }
          const threadId = scope.thread?.threadId;
          const segment = threadId === undefined ? null : toSafeThreadAttachmentSegment(threadId);
          const uuid = parseAttachmentUuid(pendingId);
          const extension = parseAttachmentFileExtension(pendingId);
          if (
            threadId === undefined ||
            segment === null ||
            !uuid ||
            !extension ||
            pendingId !== `${PENDING_ATTACHMENT_THREAD_SEGMENT}-${uuid}-${extension}` ||
            attachment.id !== `${segment}-${uuid}-${extension}` ||
            attachment.type !== "file" ||
            attachment.sizeBytes < 1 ||
            attachment.sizeBytes > PROVIDER_SEND_TURN_MAX_FILE_BYTES
          )
            return yield* denied();
          const reference = (digest: string): PeerAttachmentReference => ({
            environmentId: scope.environmentId,
            attachmentId: attachment.id,
            name: attachment.name,
            owner: owner(scope),
            digest,
            mimeType: attachment.mimeType,
            sizeBytes: attachment.sizeBytes,
          });
          const request = yield* CurrentRequest;
          const uncertain = () =>
            new OrchestratorMcpFailure({
              code: "outcome_unknown",
              ...(request === undefined ? {} : { requestId: request.clientRequestId }),
              lastConfirmedStage: "native_binding_accepted",
              message:
                "The recording claim may have been published. Its native acceptance is unproved; no file was replaced or cleaned up.",
            });
          const previous = yield* read(attachment.id);
          if (previous !== undefined) {
            if (
              !owned(scope, previous) ||
              previous.manifest !== manifest(attachment) ||
              previous.consumer === "discarded"
            )
              return yield* denied();
            if (previous.digest === null || previous.expected_digest !== previous.digest)
              return yield* uncertain();
            // Native acceptance, not current path existence/size, owns replay.
            return reference(previous.digest);
          }
          const currentPath = resolveAttachmentPath({
            attachmentsDir: config.attachmentsDir,
            attachment: { ...attachment, id: pendingId },
          });
          const finalPath = resolveAttachmentPath({
            attachmentsDir: config.attachmentsDir,
            attachment,
          });
          if (currentPath === null || finalPath === null) return yield* denied();
          const staged = yield* read(pendingId);
          if (
            staged !== undefined &&
            (!owned(scope, staged) ||
              staged.manifest !== manifest(attachment) ||
              staged.digest === null ||
              staged.consumer !== null)
          )
            return yield* denied();
          const bytes = yield* fs.readFile(currentPath).pipe(Effect.mapError(unavailable));
          if (bytes.byteLength !== attachment.sizeBytes) return yield* denied();
          const digest = Hex.encode(sha256(bytes));
          if (staged !== undefined && staged.digest !== digest) return yield* denied();
          // Reserve the exact upload before publication. A different caller/thread
          // cannot claim the same upload, and reconstruction never retries a write.
          yield* sql
            .withTransaction(
              Effect.gen(function* () {
                const won =
                  yield* sql`INSERT INTO attachment_staging (id,request_id,owner,generation,manifest,expected_digest)
          VALUES (${attachment.id},${requestDigest(["recording-claim", pendingId])},${owner(scope)},${requestGeneration(scope)},${manifest(attachment)},${digest})
          ON CONFLICT DO NOTHING RETURNING id`;
                if (won.length !== 1) return yield* uncertain();
                if (staged !== undefined) {
                  const reserved =
                    yield* sql`UPDATE attachment_staging SET consumer=${`recording:${attachment.id}`},target=${threadId},claimed_id=${attachment.id}
              WHERE id=${pendingId} AND owner=${owner(scope)} AND generation=${requestGeneration(scope)}
              AND consumer IS NULL AND digest=${digest} RETURNING id`;
                  if (reserved.length !== 1) return yield* denied();
                }
              }),
            )
            .pipe(
              Effect.mapError((error) =>
                error._tag === "OrchestratorMcpFailure" ? error : unavailable(error),
              ),
            );
          // An exclusive copy from captured bytes never aliases the upload inode or
          // replaces an existing artifact. Partial publication remains uncertain.
          yield* fs.writeFile(finalPath, bytes, { flag: "wx" }).pipe(Effect.mapError(uncertain));
          const accepted = yield* sql`UPDATE attachment_staging SET digest=${digest}
          WHERE id=${attachment.id} AND owner=${owner(scope)} AND generation=${requestGeneration(scope)}
          AND manifest=${manifest(attachment)} AND digest IS NULL AND expected_digest=${digest} RETURNING id`.pipe(
            Effect.mapError(uncertain),
          );
          if (accepted.length !== 1) return yield* uncertain();
          return reference(digest);
        }),
      ),
    prepare: (scope, input, key, digest) =>
      Effect.gen(function* () {
        const requestId = requestDigest([requestNamespace(scope), "attachment-upload", key]);
        const id = createPendingAttachmentId(
          input.type === "file" ? attachmentFileExtension(input.name) : undefined,
        );
        yield* sql`INSERT INTO attachment_staging (id,request_id,owner,generation,manifest,expected_digest)
        VALUES (${id},${requestId},${owner(scope)},${requestGeneration(scope)},${manifest(input)},${digest ?? null}) ON CONFLICT DO NOTHING`;
        const rows =
          yield* sql<Row>`SELECT * FROM attachment_staging WHERE request_id=${requestId}`;
        const row = rows[0];
        if (
          !owned(scope, row) ||
          row!.manifest !== manifest(input) ||
          row!.expected_digest !== (digest ?? null)
        )
          return yield* denied();
        if (scope.authority !== undefined) {
          if (Option.isNone(authority)) return yield* unavailable(undefined);
          yield* authority.value.checkRecord(scope.authority.id);
          yield* sql`INSERT INTO attachment_staging_authority (attachment_id,record_id) VALUES (${row!.id},${scope.authority.id})
          ON CONFLICT(attachment_id) DO UPDATE SET record_id=excluded.record_id`;
        }
        return { attachmentId: row!.id, owner: row!.owner };
      }).pipe(Effect.mapError((e) => (e._tag === "OrchestratorMcpFailure" ? e : unavailable(e)))),
    publishReference: (scope, attachment) =>
      Effect.gen(function* () {
        if (scope.authority !== undefined) {
          if (Option.isNone(authority) || scope.thread === undefined)
            return yield* unavailable(undefined);
          yield* authority.value.checkTarget(scope, {
            type: "thread",
            threadId: scope.thread.threadId,
          });
        }
        const segment =
          scope.thread === undefined ? null : toSafeThreadAttachmentSegment(scope.thread.threadId);
        if (segment === null || !attachment.id.startsWith(`${segment}-`)) return yield* denied();
        const previous = yield* read(attachment.id);
        if (previous !== undefined) {
          if (
            !owned(scope, previous) ||
            previous.manifest !== manifest(attachment) ||
            previous.digest === null ||
            previous.consumer === "discarded"
          )
            return yield* denied();
          return {
            environmentId: scope.environmentId,
            attachmentId: attachment.id,
            name: attachment.name,
            owner: owner(scope),
            digest: previous.digest,
            mimeType: attachment.mimeType,
            sizeBytes: attachment.sizeBytes,
          };
        }
        const path = resolveAttachmentPathById({
          attachmentsDir: config.attachmentsDir,
          attachmentId: attachment.id,
        });
        if (path === null || attachment.sizeBytes < 1 || attachment.sizeBytes > 50 * 1024 * 1024)
          return yield* denied();
        const bytes = yield* fs.readFile(path).pipe(Effect.mapError(unavailable));
        if (bytes.byteLength !== attachment.sizeBytes) return yield* denied();
        const digest = Hex.encode(sha256(bytes));
        yield* sql`INSERT INTO attachment_staging (id,request_id,owner,generation,manifest,digest,expected_digest)
        VALUES (${attachment.id},${requestDigest([owner(scope), "published", attachment.id])},${owner(scope)},${requestGeneration(scope)},${manifest(attachment)},${digest},${digest}) ON CONFLICT DO NOTHING`.pipe(
          Effect.mapError(unavailable),
        );
        const row = yield* read(attachment.id);
        if (
          !owned(scope, row) ||
          row!.manifest !== manifest(attachment) ||
          row!.digest !== digest ||
          row!.expected_digest !== digest
        )
          return yield* denied();
        return {
          environmentId: scope.environmentId,
          attachmentId: attachment.id,
          name: attachment.name,
          owner: owner(scope),
          digest,
          mimeType: attachment.mimeType,
          sizeBytes: attachment.sizeBytes,
        };
      }),
    verify,
    complete: (id, expectedOwner, digest) =>
      Effect.gen(function* () {
        const changed = yield* sql`UPDATE attachment_staging SET digest=${digest}
        WHERE id=${id} AND owner=${expectedOwner} AND (digest IS NULL OR digest=${digest})
          AND (consumer IS NULL OR (consumer<>'discarded' AND digest=${digest}))
          AND (expected_digest IS NULL OR expected_digest=${digest}) RETURNING id`.pipe(
          Effect.mapError(unavailable),
        );
        if (changed.length !== 1) return yield* denied();
      }),
    authorizeUpload: (id, expectedOwner) =>
      Effect.gen(function* () {
        const row = yield* read(id);
        if (row === undefined || row.owner !== expectedOwner || row.consumer === "discarded")
          return yield* denied();
        const owners = yield* sql<{
          record_id: string;
        }>`SELECT record_id FROM attachment_staging_authority WHERE attachment_id=${id}`.pipe(
          Effect.mapError(unavailable),
        );
        if (owners[0] !== undefined) {
          if (Option.isNone(authority)) return yield* unavailable(undefined);
          yield* authority.value.checkRecord(owners[0].record_id);
        }
      }),
    transferDigest: (scope, consumer, destination, attachment) =>
      Effect.gen(function* () {
        const row = (yield* sql<{
          binding: string;
          digest: string;
        }>`SELECT binding,digest FROM attachment_transfers WHERE consumer=${consumer} AND attachment_id=${attachment.id}`.pipe(
          Effect.mapError(unavailable),
        ))[0];
        if (row?.binding !== transferBinding(scope, destination, attachment))
          return yield* denied();
        return yield* decodeAttachmentDigest(row.digest).pipe(Effect.mapError(unavailable));
      }),
    inspectTransfer: (scope, id, input) =>
      Effect.gen(function* () {
        const row = yield* read(id);
        if (
          !owned(scope, row) ||
          row!.manifest !== manifest(input) ||
          row!.consumer === "discarded"
        )
          return yield* denied();
        if (row!.digest === null) return { stage: "awaiting-bytes" as const };
        if (
          row!.consumer !== null &&
          row!.claimed_id !== null &&
          row!.target !== null &&
          Option.isSome(receipts)
        ) {
          const receipt = yield* receipts.value
            .getByCommandId(CommandId.make(row!.consumer))
            .pipe(Effect.mapError(unavailable));
          if (
            Option.isSome(receipt) &&
            receipt.value.status === "accepted" &&
            receipt.value.commandType === "message.dispatch" &&
            receipt.value.threadId === row!.target
          )
            return { stage: "native-accepted" as const, digest: row!.digest };
        }
        const path = resolveAttachmentPathById({
          attachmentsDir: config.attachmentsDir,
          attachmentId: id,
        });
        if (path === null) return { stage: "awaiting-bytes" as const };
        const bytes = yield* fs.readFile(path).pipe(Effect.mapError(unavailable));
        return bytes.byteLength === input.sizeBytes && Hex.encode(sha256(bytes)) === row!.digest
          ? { stage: "staged" as const, digest: row!.digest }
          : { stage: "awaiting-bytes" as const };
      }),
    reserveTransfer: (scope, consumer, destination, attachments) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            yield* bindIntent(scope, consumer, destination, attachments);
            for (const attachment of attachments) {
              const binding = transferBinding(scope, destination, attachment);
              const prior = (yield* sql<{
                binding: string;
              }>`SELECT binding FROM attachment_transfers WHERE consumer=${consumer} AND attachment_id=${attachment.id}`.pipe(
                Effect.mapError(unavailable),
              ))[0];
              if (prior !== undefined) {
                if (prior.binding !== binding) return yield* denied();
                continue;
              }
              const row = yield* read(attachment.id);
              if (
                row !== undefined &&
                (!owned(scope, row) ||
                  row.digest === null ||
                  row.manifest !== manifest(attachment) ||
                  (row.consumer !== null &&
                    (row.consumer !== consumer || row.target !== destination)))
              )
                return yield* denied();
              // This exact source access/byte proof is retained before destination staging.
              // A retry can compare it with the destination's real staged/native owner,
              // even after an ephemeral pending source has been swept.
              const source = yield* verify(scope, attachment);
              if (row !== undefined) {
                yield* sql`UPDATE attachment_staging SET consumer=${consumer},target=${destination} WHERE id=${attachment.id} AND consumer IS NULL`.pipe(
                  Effect.mapError(unavailable),
                );
                const reserved = yield* read(attachment.id);
                if (reserved?.consumer !== consumer || reserved.target !== destination)
                  return yield* denied();
              }
              yield* sql`INSERT INTO attachment_transfers VALUES (${consumer},${attachment.id},${binding},${source.digest})`.pipe(
                Effect.mapError(unavailable),
              );
            }
          }),
        )
        .pipe(
          Effect.mapError((error) =>
            error._tag === "OrchestratorMcpFailure" ? error : unavailable(error),
          ),
        ),
    bindConsumption: (scope, commandId, threadId, attachments) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            yield* bindIntent(scope, commandId, threadId, attachments);
            // Reserve the complete manifest atomically before any filesystem work.
            yield* Effect.forEach(
              attachments,
              (attachment) => reserve(scope, commandId, threadId, attachment),
              { discard: true },
            );
          }),
        )
        .pipe(
          Effect.mapError((error) =>
            error._tag === "OrchestratorMcpFailure" ? error : unavailable(error),
          ),
        ),
    reserve,
    discard: (scope, id, request) =>
      Effect.gen(function* () {
        if (request !== undefined && request.generation !== requestGeneration(scope))
          return yield* denied();
        const binding =
          request === undefined
            ? null
            : encode({
                id: request.id,
                generation: request.generation,
                inputDigest: request.inputDigest,
              });
        // Ownership and the one-use transition are one conditional write.
        // A stale pre-read cannot authorize deletion after a rival reserves it.
        yield* sql
          .withTransaction(
            Effect.gen(function* () {
              const won = yield* sql<Row>`UPDATE attachment_staging
                SET consumer='discarded',discard_receipt=${binding},discard_cleanup=${request === undefined ? "pending" : "retained"}
                WHERE id=${id} AND owner=${owner(scope)} AND generation=${requestGeneration(scope)} AND consumer IS NULL RETURNING *`;
              const row = won[0] ?? (request === undefined ? undefined : yield* read(id));
              if (
                row === undefined ||
                !owned(scope, row) ||
                row.consumer !== "discarded" ||
                row.discard_receipt !== binding ||
                (row.discard_cleanup !== "pending" &&
                  row.discard_cleanup !== "started" &&
                  row.discard_cleanup !== "retained")
              )
                return yield* denied();
            }),
          )
          .pipe(
            Effect.mapError((error) =>
              error._tag === "OrchestratorMcpFailure" ? error : unavailable(error),
            ),
          );
        // Native acceptance does not prove ownership of the current path contents.
        // A read/digest followed by unlink can delete an independent replacement.
        // Keyed calls therefore retain bytes, including historical pending cleanup;
        // legacy callers keep their existing separate best-effort cleanup.
      }),
  });
});

export const layer = Layer.effect(AttachmentStaging, make);
