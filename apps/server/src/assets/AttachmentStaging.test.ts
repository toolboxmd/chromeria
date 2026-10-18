import * as Fiber from "effect/Fiber";
import { resolveAttachmentPath } from "../attachmentStore.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  type ChatAttachment,
  EnvironmentId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import { sha256 } from "@noble/hashes/sha2";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Hex from "effect/encoding/Hex";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Config from "../config.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as Secrets from "../auth/ServerSecretStore.ts";
import * as Invocation from "../mcp/McpInvocationContext.ts";
import * as Claims from "../orchestration-v2/AttachmentClaims.ts";
import * as Staging from "./AttachmentStaging.ts";
import * as Upload from "./AttachmentUpload.ts";
import * as Requests from "../mcp/McpRequestReceipts.ts";

const scope: Invocation.McpInvocationScope = {
  environmentId: EnvironmentId.make("staging-a"),
  requestNamespace: "credential",
  issuedAt: 0,
  thread: {
    threadId: ThreadId.make("caller"),
    providerSessionId: "credential",
    providerInstanceId: ProviderInstanceId.make("codex"),
  },
  client: undefined,
  capabilities: new Set(["orchestration"]),
};
const database = Sqlite.layerMemory.pipe(
  Layer.provideMerge(Secrets.layer),
  Layer.provideMerge(Config.layerTest(process.cwd(), { prefix: "t3-staging-" })),
  Layer.provideMerge(NodeServices.layer),
);
const layer = Staging.layer.pipe(Layer.provideMerge(database));
const keyedLayer = Requests.layer.pipe(Layer.provideMerge(layer));
const recordingClaimsLayer = Staging.layer.pipe(
  Layer.provideMerge(
    Sqlite.layerConfig.pipe(
      Layer.provideMerge(Secrets.layer),
      Layer.provideMerge(Config.layerTest(process.cwd(), { prefix: "t3-recording-claim-" })),
      Layer.provideMerge(NodeServices.layer),
    ),
  ),
);
const bytes = new Uint8Array([1, 2, 3, 4]);
const digest = Hex.encode(sha256(bytes));
const metadata = {
  type: "file" as const,
  name: "document.pdf",
  mimeType: "application/pdf",
  sizeBytes: 4,
};
const upload = Effect.gen(function* () {
  const staging = yield* Staging.AttachmentStaging;
  const binding = yield* staging.prepare(scope, metadata, "upload-once", digest);
  const issued = yield* Upload.issueAttachmentUploadUrl(metadata, { ...binding, digest });
  const claims = yield* Upload.validateAttachmentUploadToken(issued.relativeUrl.split("/").at(-1)!);
  if (!claims) return yield* Effect.die("Invalid fixture token");
  return { claims, attachment: { ...metadata, id: issued.attachmentId } satisfies ChatAttachment };
});

it.effect(
  "verifies actual bytes and retains preparation/ownership across service reconstruction",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const staging = yield* Staging.AttachmentStaging;
        const { claims, attachment } = yield* upload;
        expect(yield* Upload.storeAttachmentUpload(claims, bytes)).toEqual({ ok: true });
        const rebuilt = Context.get(yield* Layer.build(Staging.layer), Staging.AttachmentStaging);
        expect(
          (yield* rebuilt.prepare(
            {
              ...scope,
              requestNamespace: "renewed",
              thread: { ...scope.thread!, providerSessionId: "renewed" },
            },
            metadata,
            "upload-once",
            digest,
          )).attachmentId,
        ).toBe(attachment.id);
        const verified = yield* rebuilt.verify(scope, attachment);
        expect(verified.digest).toBe(
          "9f64a747e1b97f131fabb6b447296c9b6f0201e79fb3c5356e6c77e89b6a806a",
        );
        const fs = yield* FileSystem.FileSystem;
        expect(Array.from(yield* fs.readFile(verified.path))).toEqual([1, 2, 3, 4]);
        const wrongOwner = {
          ...scope,
          thread: { ...scope.thread!, threadId: ThreadId.make("other") },
        };
        expect((yield* Effect.result(rebuilt.verify(wrongOwner, attachment)))._tag).toBe("Failure");
        expect(
          (yield* Effect.result(
            staging.prepare(scope, { ...metadata, mimeType: "audio/wav" }, "upload-once", digest),
          ))._tag,
        ).toBe("Failure");
      }),
    ).pipe(Effect.provide(layer)),
);

it.effect("rejects digest mismatch before publication and converges an identical upload race", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { claims, attachment } = yield* upload;
      const wrong = yield* Upload.storeAttachmentUpload(claims, new Uint8Array([4, 3, 2, 1]));
      expect(wrong).toMatchObject({ ok: false, status: 400 });
      const results = yield* Effect.all(
        [Upload.storeAttachmentUpload(claims, bytes), Upload.storeAttachmentUpload(claims, bytes)],
        { concurrency: "unbounded" },
      );
      expect(results).toEqual([{ ok: true }, { ok: true }]);
      expect((yield* (yield* Staging.AttachmentStaging).verify(scope, attachment)).digest).toBe(
        digest,
      );
      const fs = yield* FileSystem.FileSystem;
      const config = yield* Config.ServerConfig;
      expect(
        (yield* fs.readDirectory(config.attachmentsDir)).filter((name) => name.endsWith(".part")),
      ).toEqual([]);
    }),
  ).pipe(Effect.provide(layer)),
);

it.effect("reserves exactly one consuming command and recovers its exact claim mapping", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { claims, attachment } = yield* upload;
      yield* Upload.storeAttachmentUpload(claims, bytes);
      const claim = (commandId: string) =>
        Claims.claimPendingAttachments({
          commandId,
          threadId: "destination",
          attachments: [attachment],
        }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope));
      const results = yield* Effect.all(
        [Effect.result(claim("command-a")), Effect.result(claim("command-b"))],
        { concurrency: "unbounded" },
      );
      expect(results.filter((r) => r._tag === "Success")).toHaveLength(1);
      expect(results.filter((r) => r._tag === "Failure")).toHaveLength(1);
      const successful = results.find((r) => r._tag === "Success");
      if (successful?._tag !== "Success") return yield* Effect.die("No winning consumer");
      const sql = yield* SqlClient.SqlClient;
      const row = (yield* sql<{
        consumer: string;
      }>`SELECT consumer FROM attachment_staging WHERE id=${attachment.id}`)[0]!;
      const rebuilt = Context.get(yield* Layer.build(Staging.layer), Staging.AttachmentStaging);
      const replay = yield* claim(row.consumer).pipe(
        Effect.provideService(Staging.AttachmentStaging, rebuilt),
      );
      expect(replay.attachments).toEqual(successful.success.attachments);
      expect(replay.claimedPaths).toEqual(successful.success.claimedPaths);
      expect(
        (yield* Effect.result(rebuilt.reserve(scope, row.consumer, "different-target", attachment)))
          ._tag,
      ).toBe("Failure");
      // Retrying byte acknowledgement after consumption is not a second consumption.
      expect(yield* Upload.storeAttachmentUpload(claims, bytes)).toEqual({ ok: true });
    }),
  ).pipe(Effect.provide(layer)),
);

it.effect(
  "recovers bytes published before a staging-ack crash without changing their identity",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { claims, attachment } = yield* upload;
        const fs = yield* FileSystem.FileSystem;
        const crashingFs = FileSystem.FileSystem.of({
          ...fs,
          link: (from, to) => fs.link(from, to).pipe(Effect.andThen(Effect.interrupt)),
        });
        // Real publication succeeds, then its process loses control before the SQL
        // digest acknowledgement. The deterministic platform fault replaces no intake.
        const crashed = yield* Upload.storeAttachmentUpload(claims, bytes).pipe(
          Effect.provideService(FileSystem.FileSystem, crashingFs),
          Effect.forkScoped,
        );
        yield* Fiber.await(crashed);
        const sql = yield* SqlClient.SqlClient;
        expect(
          (yield* sql<{
            digest: string | null;
          }>`SELECT digest FROM attachment_staging WHERE id=${attachment.id}`)[0]?.digest,
        ).toBe(null);
        const rebuilt = Context.get(yield* Layer.build(Staging.layer), Staging.AttachmentStaging);
        expect(
          yield* Upload.storeAttachmentUpload(claims, bytes).pipe(
            Effect.provideService(Staging.AttachmentStaging, rebuilt),
          ),
        ).toEqual({ ok: true });
        expect((yield* rebuilt.verify(scope, attachment)).digest).toBe(digest);
      }),
    ).pipe(Effect.provide(layer)),
);

it.effect.each([false, true])(
  "recovers the reserved claim mapping after a publication=%s crash",
  (afterPublish) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { claims, attachment } = yield* upload;
        yield* Upload.storeAttachmentUpload(claims, bytes);
        const fs = yield* FileSystem.FileSystem;
        const crashingFs = FileSystem.FileSystem.of({
          ...fs,
          link: (from, to) =>
            (afterPublish ? fs.link(from, to) : Effect.void).pipe(Effect.andThen(Effect.interrupt)),
        });
        const claim = Claims.claimPendingAttachments({
          commandId: "crash-command",
          threadId: "destination",
          attachments: [attachment],
        }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope));
        const crashed = yield* claim.pipe(
          Effect.provideService(FileSystem.FileSystem, crashingFs),
          Effect.forkScoped,
        );
        yield* Fiber.await(crashed);
        const sql = yield* SqlClient.SqlClient;
        const reserved = (yield* sql<{
          claimed_id: string;
          consumer: string;
        }>`SELECT claimed_id,consumer FROM attachment_staging WHERE id=${attachment.id}`)[0]!;
        expect(reserved.consumer).toBe("crash-command");
        const rebuilt = Context.get(yield* Layer.build(Staging.layer), Staging.AttachmentStaging);
        const recovered = yield* claim.pipe(
          Effect.provideService(Staging.AttachmentStaging, rebuilt),
        );
        expect(recovered.attachments[0]?.id).toBe(reserved.claimed_id);
        expect(recovered.claimedPaths).toEqual([]);
        const path = resolveAttachmentPath({
          attachmentsDir: (yield* Config.ServerConfig).attachmentsDir,
          attachment: recovered.attachments[0]!,
        });
        expect(Array.from(yield* fs.readFile(path!))).toEqual([1, 2, 3, 4]);
        const changed = yield* Effect.result(
          Claims.claimPendingAttachments({
            commandId: "crash-command",
            threadId: "destination",
            attachments: [attachment, { ...attachment, id: "other" }],
          }).pipe(Effect.provideService(Invocation.McpInvocationContext, scope)),
        );
        expect(changed._tag).toBe("Failure");
        expect(Array.from(yield* fs.readFile(path!))).toEqual([1, 2, 3, 4]);
      }),
    ).pipe(Effect.provide(layer)),
);

it.effect(
  "refuses unscoped consumption and discarded-token resurrection without publishing bytes",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { claims, attachment } = yield* upload;
        yield* Upload.storeAttachmentUpload(claims, bytes);
        const unscoped = yield* Effect.result(
          Claims.claimPendingAttachments({
            commandId: "unverified",
            threadId: "destination",
            attachments: [attachment],
          }),
        );
        expect(unscoped._tag).toBe("Failure");
        const staging = yield* Staging.AttachmentStaging;
        yield* staging.discard(scope, attachment.id);
        yield* Upload.deletePendingAttachment(attachment.id);
        expect(yield* Upload.storeAttachmentUpload(claims, bytes)).toMatchObject({
          ok: false,
          status: 403,
        });
        expect((yield* Effect.result(staging.verify(scope, attachment)))._tag).toBe("Failure");
      }),
    ).pipe(Effect.provide(layer)),
);

it.effect("reserves a complete manifest atomically against an opposite-order rival", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const staging = yield* Staging.AttachmentStaging;
      const first = yield* upload;
      expect(yield* Upload.storeAttachmentUpload(first.claims, bytes)).toEqual({ ok: true });
      const secondBinding = yield* staging.prepare(
        scope,
        { ...metadata, name: "second.pdf" },
        "second-upload",
        digest,
      );
      const issued = yield* Upload.issueAttachmentUploadUrl(
        { ...metadata, name: "second.pdf" },
        { ...secondBinding, digest },
      );
      const claims = yield* Upload.validateAttachmentUploadToken(
        issued.relativeUrl.split("/").at(-1)!,
      );
      expect(claims).not.toBeNull();
      expect(yield* Upload.storeAttachmentUpload(claims!, bytes)).toEqual({ ok: true });
      const second = { ...metadata, name: "second.pdf", id: issued.attachmentId };
      const raced = yield* Effect.all(
        [
          Effect.result(
            staging.bindConsumption(scope, "command-a", "target-a", [first.attachment, second]),
          ),
          Effect.result(
            staging.bindConsumption(scope, "command-b", "target-b", [second, first.attachment]),
          ),
        ],
        { concurrency: "unbounded" },
      );
      expect(raced.filter((result) => result._tag === "Success")).toHaveLength(1);
      const sql = yield* SqlClient.SqlClient;
      const rows = yield* sql<{
        consumer: string;
        target: string;
      }>`SELECT consumer,target FROM attachment_staging`;
      expect(rows).toHaveLength(2);
      expect(new Set(rows.map((row) => row.consumer)).size).toBe(1);
      expect(new Set(rows.map((row) => row.target)).size).toBe(1);
      expect(yield* sql`SELECT command_id FROM attachment_consumptions`).toHaveLength(1);
    }),
  ).pipe(Effect.provide(layer)),
);

it.effect("Grok R1: a concurrent discard deletes bytes only if it wins against reservation", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const staging = yield* Staging.AttachmentStaging;
      const { claims, attachment } = yield* upload;
      yield* Upload.storeAttachmentUpload(claims, bytes);
      const results = yield* Effect.all(
        [
          Effect.result(staging.reserve(scope, "reserved-command", "destination", attachment)),
          Effect.result(
            staging
              .discard(scope, attachment.id)
              .pipe(Effect.andThen(Upload.deletePendingAttachment(attachment.id))),
          ),
        ],
        { concurrency: "unbounded" },
      );
      expect(results.filter((result) => result._tag === "Success")).toHaveLength(1);
      const sql = yield* SqlClient.SqlClient;
      const row = (yield* sql<{
        consumer: string;
      }>`SELECT consumer FROM attachment_staging WHERE id=${attachment.id}`)[0]!;
      const file = resolveAttachmentPath({
        attachmentsDir: (yield* Config.ServerConfig).attachmentsDir,
        attachment,
      })!;
      const fs = yield* FileSystem.FileSystem;
      if (row.consumer === "reserved-command") {
        expect(results[1]._tag).toBe("Failure");
        expect(Array.from(yield* fs.readFile(file))).toEqual([1, 2, 3, 4]);
      } else {
        expect(row.consumer).toBe("discarded");
        expect(results[0]._tag).toBe("Failure");
        expect(yield* fs.exists(file)).toBe(false);
      }
    }),
  ).pipe(Effect.provide(layer)),
);

it.effect.each(["fresh", "pending", "started", "removed"] as const)(
  "S7: keyed discard retains bytes and exact acceptance across %s cleanup history",
  (history) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { claims, attachment } = yield* upload;
        yield* Upload.storeAttachmentUpload(claims, bytes);
        const requests = yield* Requests.McpRequestReceipts;
        const intent = { attachmentId: attachment.id, clientRequestId: "native-discard" };
        const request = yield* requests.identify(
          scope,
          "t3_attachment_discard",
          intent,
          intent.clientRequestId,
        );
        const fs = yield* FileSystem.FileSystem;
        const path = resolveAttachmentPath({
          attachmentsDir: (yield* Config.ServerConfig).attachmentsDir,
          attachment,
        })!;
        // A changed file before the first attempt also remains independent work.
        if (history === "fresh") yield* fs.writeFile(path, new Uint8Array([9, 8, 7, 6]));
        const retained = history === "fresh" ? [9, 8, 7, 6] : [1, 2, 3, 4];
        let reads = 0;
        let removals = 0;
        const observed = FileSystem.FileSystem.of({
          ...fs,
          readFile: (file) =>
            fs.readFile(file).pipe(
              Effect.flatMap((snapshot) => {
                if (file !== path) return Effect.succeed(snapshot);
                reads++;
                // The original S7 interleaving: an independent write after the real read.
                return fs.writeFile(path, new Uint8Array([9, 8, 7, 6])).pipe(Effect.as(snapshot));
              }),
            ),
          remove: (file, options) => {
            if (file === path) removals++;
            return fs.remove(file, options);
          },
        });
        const native = Context.get(
          yield* Layer.build(
            Staging.layer.pipe(
              Layer.provide(Layer.succeed(FileSystem.FileSystem, observed)),
              Layer.fresh,
            ),
          ),
          Staging.AttachmentStaging,
        );
        // A refusal before native commit leaves the upload available.
        expect(
          (yield* Effect.result(
            native.discard(scope, attachment.id, {
              ...request,
              generation: "replacement",
            }),
          ))._tag,
        ).toBe("Failure");
        const sql = yield* SqlClient.SqlClient;
        expect(
          yield* sql`SELECT consumer,discard_receipt FROM attachment_staging WHERE id=${attachment.id}`,
        ).toEqual([{ consumer: null, discard_receipt: null }]);
        // Lose acknowledgement after the actual owner commits, then reconstruct.
        const lost = yield* native
          .discard(scope, attachment.id, request)
          .pipe(Effect.andThen(Effect.interrupt), Effect.forkScoped);
        expect((yield* Fiber.await(lost))._tag).toBe("Failure");
        expect(reads).toBe(0);
        expect(removals).toBe(0);
        expect(Array.from(yield* fs.readFile(path))).toEqual(retained);
        expect(
          (yield* sql`SELECT discard_cleanup FROM attachment_staging WHERE id=${attachment.id}`)[0]
            ?.discard_cleanup,
        ).toBe("retained");
        // Mixed-version fixture: change only the cleanup stage of a real verified
        // native acceptance. Never invent its owner, generation, intent or receipt.
        if (history !== "fresh") {
          yield* sql`UPDATE attachment_staging SET discard_cleanup=${history === "removed" ? "started" : history} WHERE id=${attachment.id}`;
          if (history === "removed") yield* fs.remove(path);
        }
        const accepted =
          yield* sql`SELECT consumer,discard_receipt,discard_cleanup FROM attachment_staging WHERE id=${attachment.id}`;
        expect(accepted[0]?.consumer).toBe("discarded");
        expect(JSON.parse(String(accepted[0]?.discard_receipt))).toEqual({
          id: request.id,
          generation: request.generation,
          inputDigest: request.inputDigest,
        });
        const rebuiltContext = yield* Layer.build(
          Requests.layer.pipe(
            Layer.provideMerge(Staging.layer),
            Layer.provide(Layer.succeed(FileSystem.FileSystem, observed)),
            Layer.fresh,
          ),
        );
        const rebuilt = Context.get(rebuiltContext, Staging.AttachmentStaging);
        const renewed = {
          ...scope,
          thread: { ...scope.thread!, providerSessionId: "renewed-discard" },
        };
        const same = yield* Context.get(rebuiltContext, Requests.McpRequestReceipts).identify(
          renewed,
          "t3_attachment_discard",
          intent,
          intent.clientRequestId,
        );
        expect(same).toEqual(request);
        yield* rebuilt.discard(renewed, attachment.id, same);
        expect(yield* fs.exists(path)).toBe(history !== "removed");
        if (history !== "removed") {
          // An independent reader sees original bytes; its later writer changes
          // them before replay. No destructive check can race that replacement.
          expect(Array.from(yield* fs.readFile(path))).toEqual(retained);
        }
        yield* fs.writeFile(path, new Uint8Array([9, 8, 7, 6]));
        yield* rebuilt.discard(renewed, attachment.id, same);
        expect(Array.from(yield* fs.readFile(path))).toEqual([9, 8, 7, 6]);
        // A later removal/replacement also cannot be cleaned by historical replay.
        yield* fs.remove(path);
        yield* fs.writeFile(path, new Uint8Array([6, 7, 8, 9]));
        yield* rebuilt.discard(renewed, attachment.id, same);
        expect(Array.from(yield* fs.readFile(path))).toEqual([6, 7, 8, 9]);
        expect(reads).toBe(0);
        expect(removals).toBe(0);
        expect(
          yield* sql`SELECT consumer,discard_receipt,discard_cleanup FROM attachment_staging WHERE id=${attachment.id}`,
        ).toEqual(accepted);
        expect(
          (yield* Effect.result(
            rebuilt.discard(renewed, attachment.id, { ...same, inputDigest: "changed" }),
          ))._tag,
        ).toBe("Failure");
        expect(
          (yield* Effect.result(
            rebuilt.discard(renewed, attachment.id, { ...same, generation: "replacement" }),
          ))._tag,
        ).toBe("Failure");
        expect(
          (yield* Effect.result(
            rebuilt.discard(
              { ...renewed, thread: { ...renewed.thread!, threadId: ThreadId.make("rival") } },
              attachment.id,
              same,
            ),
          ))._tag,
        ).toBe("Failure");
      }),
    ).pipe(
      Effect.provide(
        Requests.layer.pipe(
          Layer.provideMerge(
            Staging.layer.pipe(
              Layer.provideMerge(
                Sqlite.layerConfig.pipe(
                  Layer.provideMerge(Secrets.layer),
                  Layer.provideMerge(Config.layerTest(process.cwd(), { prefix: "t3-discard-s7-" })),
                  Layer.provideMerge(NodeServices.layer),
                ),
              ),
            ),
          ),
        ),
      ),
    ),
);

it.effect("S6: imported/unkeyed tombstones cannot acquire a keyed receipt", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { claims, attachment } = yield* upload;
      yield* Upload.storeAttachmentUpload(claims, bytes);
      const staging = yield* Staging.AttachmentStaging;
      yield* staging.discard(scope, attachment.id);
      const request = yield* (yield* Requests.McpRequestReceipts).identify(
        scope,
        "t3_attachment_discard",
        { attachmentId: attachment.id },
        "imported-discard",
      );
      expect((yield* Effect.result(staging.discard(scope, attachment.id, request)))._tag).toBe(
        "Failure",
      );
      const sql = yield* SqlClient.SqlClient;
      expect(
        yield* sql`SELECT discard_receipt FROM attachment_staging WHERE id=${attachment.id}`,
      ).toEqual([{ discard_receipt: null }]);
    }),
  ).pipe(Effect.provide(keyedLayer)),
);

it.effect.each([false, true])(
  "S6: concurrent discard rival keys=%s retain one native acceptance without destructive cleanup",
  (rival) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { claims, attachment } = yield* upload;
        yield* Upload.storeAttachmentUpload(claims, bytes);
        const requests = yield* Requests.McpRequestReceipts;
        const request = (key: string) =>
          requests.identify(
            scope,
            "t3_attachment_discard",
            { attachmentId: attachment.id, clientRequestId: key },
            key,
          );
        const one = yield* request("first");
        const two = yield* request(rival ? "rival" : "first");
        const fs = yield* FileSystem.FileSystem;
        const path = resolveAttachmentPath({
          attachmentsDir: (yield* Config.ServerConfig).attachmentsDir,
          attachment,
        })!;
        let removals = 0;
        const observed = FileSystem.FileSystem.of({
          ...fs,
          remove: (file, options) => {
            if (file === path) removals++;
            return fs.remove(file, options);
          },
        });
        const native = Context.get(
          yield* Layer.build(
            Staging.layer.pipe(
              Layer.provide(Layer.succeed(FileSystem.FileSystem, observed)),
              Layer.fresh,
            ),
          ),
          Staging.AttachmentStaging,
        );
        const results = yield* Effect.all(
          [one, two].map((identity) =>
            Effect.result(native.discard(scope, attachment.id, identity)),
          ),
          { concurrency: "unbounded" },
        );
        expect(results.filter((result) => result._tag === "Success")).toHaveLength(rival ? 1 : 2);
        expect(results.filter((result) => result._tag === "Failure")).toHaveLength(rival ? 1 : 0);
        expect(removals).toBe(0);
        expect(Array.from(yield* fs.readFile(path))).toEqual([1, 2, 3, 4]);
        const sql = yield* SqlClient.SqlClient;
        const rows = yield* sql<{
          discard_receipt: string;
          consumer: string;
        }>`SELECT discard_receipt,consumer FROM attachment_staging WHERE id=${attachment.id}`;
        expect(rows).toHaveLength(1);
        expect(rows[0]?.consumer).toBe("discarded");
        expect([one.id, two.id]).toContain(JSON.parse(rows[0]!.discard_receipt).id);
      }),
    ).pipe(Effect.provide(keyedLayer)),
);

it.effect.each(["before-write", "after-write", "after-acceptance"] as const)(
  "S8: recording claim %s reconstruction never repeats publication",
  (stage) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { claims, attachment } = yield* upload;
        yield* Upload.storeAttachmentUpload(claims, bytes);
        // The same native file owner handles recordings; the test uses bounded PDF
        // bytes so no renderer/codec can substitute for the publication oracle.
        const final = { ...attachment, id: attachment.id.replace(/^pending-/, "caller-") };
        const fs = yield* FileSystem.FileSystem;
        const path = resolveAttachmentPath({
          attachmentsDir: (yield* Config.ServerConfig).attachmentsDir,
          attachment: final,
        })!;
        let writes = 0;
        const interruptedFs = FileSystem.FileSystem.of({
          ...fs,
          writeFile: (file, data, options) =>
            file !== path
              ? fs.writeFile(file, data, options)
              : Effect.gen(function* () {
                  writes++;
                  if (stage === "before-write") return yield* Effect.interrupt;
                  yield* fs.writeFile(file, data, options);
                  if (stage === "after-write") return yield* Effect.interrupt;
                }),
        });
        const first = Context.get(
          yield* Layer.build(
            Staging.layer.pipe(
              Layer.provide(Layer.succeed(FileSystem.FileSystem, interruptedFs)),
              Layer.fresh,
            ),
          ),
          Staging.AttachmentStaging,
        );
        const lost = yield* first
          .claimRecording(scope, attachment.id, final)
          .pipe(Effect.andThen(Effect.interrupt), Effect.forkScoped);
        expect((yield* Fiber.await(lost))._tag).toBe("Failure");
        const sql = yield* SqlClient.SqlClient;
        const original =
          yield* sql`SELECT owner,generation,manifest,digest,expected_digest FROM attachment_staging WHERE id=${final.id}`;
        expect(original).toHaveLength(1);
        expect(original[0]?.expected_digest).toBe(digest);
        expect(original[0]?.digest).toBe(stage === "after-acceptance" ? digest : null);
        expect(yield* fs.exists(path)).toBe(stage !== "before-write");
        if (stage !== "before-write")
          expect(Array.from(yield* fs.readFile(path))).toEqual([1, 2, 3, 4]);
        yield* fs.writeFile(path, new Uint8Array([9, 8, 7, 6]));
        const rebuilt = Context.get(
          yield* Layer.build(
            Staging.layer.pipe(
              Layer.provide(Layer.succeed(FileSystem.FileSystem, interruptedFs)),
              Layer.fresh,
            ),
          ),
          Staging.AttachmentStaging,
        );
        const renewed = {
          ...scope,
          thread: { ...scope.thread!, providerSessionId: "renewed-recording" },
        };
        const replay = yield* Effect.result(rebuilt.claimRecording(renewed, attachment.id, final));
        if (stage === "after-acceptance") {
          expect(replay._tag).toBe("Success");
          if (replay._tag === "Success") expect(replay.success.digest).toBe(digest);
        } else {
          expect(replay._tag).toBe("Failure");
          if (replay._tag === "Failure")
            expect(replay.failure).toMatchObject({
              code: "outcome_unknown",
              lastConfirmedStage: "native_binding_accepted",
            });
        }
        expect(writes).toBe(1);
        expect(Array.from(yield* fs.readFile(path))).toEqual([9, 8, 7, 6]);
        expect(
          yield* sql`SELECT owner,generation,manifest,digest,expected_digest FROM attachment_staging WHERE id=${final.id}`,
        ).toEqual(original);
        expect(
          (yield* Effect.result(
            rebuilt.claimRecording(renewed, attachment.id, { ...final, name: "changed.pdf" }),
          ))._tag,
        ).toBe("Failure");
        expect(
          (yield* Effect.result(
            rebuilt.claimRecording(
              { ...renewed, thread: { ...renewed.thread!, threadId: ThreadId.make("other") } },
              attachment.id,
              final,
            ),
          ))._tag,
        ).toBe("Failure");
      }),
    ).pipe(Effect.provide(recordingClaimsLayer)),
);

it.effect("S8: an unproved existing recording path is never accepted or overwritten", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { claims, attachment } = yield* upload;
      yield* Upload.storeAttachmentUpload(claims, bytes);
      const final = { ...attachment, id: attachment.id.replace(/^pending-/, "caller-") };
      const fs = yield* FileSystem.FileSystem;
      const path = resolveAttachmentPath({
        attachmentsDir: (yield* Config.ServerConfig).attachmentsDir,
        attachment: final,
      })!;
      yield* fs.writeFile(path, new Uint8Array([9, 8, 7, 6]));
      const native = yield* Staging.AttachmentStaging;
      const refused = yield* Effect.result(native.claimRecording(scope, attachment.id, final));
      expect(refused._tag).toBe("Failure");
      if (refused._tag === "Failure") expect(refused.failure.code).toBe("outcome_unknown");
      expect(Array.from(yield* fs.readFile(path))).toEqual([9, 8, 7, 6]);
      const sql = yield* SqlClient.SqlClient;
      expect(
        (yield* sql`SELECT digest FROM attachment_staging WHERE id=${final.id}`)[0]?.digest,
      ).toBe(null);
    }),
  ).pipe(Effect.provide(recordingClaimsLayer)),
);
