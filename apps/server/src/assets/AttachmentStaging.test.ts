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
