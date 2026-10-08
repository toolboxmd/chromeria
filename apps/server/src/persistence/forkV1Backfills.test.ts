// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";

import * as LegacyV1ThreadImporter from "../orchestration-v2/legacy/LegacyV1ThreadImporter.ts";
import { runMigrations } from "./Migrations.ts";
import {
  runForkV1Backfills,
  ForkV1BackfillStepError,
  type ForkV1Backfill,
} from "./forkV1Backfills.ts";
import { forkV1SnapshotLayer, vacuumForkV1Snapshot } from "./forkV1Snapshot.testFixtures.ts";

it.effect(
  "recovers partial backfill failure and repeats after importing shells without transcripts",
  () =>
    Effect.gen(function* () {
      const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "chromeria-backfill-"));
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => NodeFS.rmSync(directory, { recursive: true, force: true })),
      );
      const sourcePath = NodePath.join(directory, "state.sqlite");
      const snapshotPath = NodePath.join(directory, "chromeria-v2.sqlite");
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations({ toMigrationInclusive: 52 });
        yield* sql`INSERT INTO projection_projects (project_id, title, workspace_root, scripts_json, created_at, updated_at)
        VALUES ('project', 'Project', '/tmp/project', '[]', '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z')`;
        yield* sql`INSERT INTO projection_threads (thread_id, project_id, title, model_selection_json, runtime_mode, interaction_mode, created_at, updated_at)
        VALUES ('legacy', 'project', 'Legacy', '{"instanceId":"codex","model":"gpt-5.4"}', 'full-access', 'default', '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z')`;
        yield* sql`INSERT INTO projection_thread_messages (message_id, thread_id, role, text, is_streaming, created_at, updated_at)
        VALUES ('message', 'legacy', 'user', 'Private transcript', 0, '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z')`;
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: sourcePath })));
      const original = NodeFS.readFileSync(sourcePath);
      yield* Effect.promise(() =>
        vacuumForkV1Snapshot({ sourcePath, destinationPath: snapshotPath }),
      );
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
        yield* importer.reconcileShells;
        yield* sql`CREATE TABLE fork_test_backfill (id TEXT PRIMARY KEY)`;
        let fail = true;
        const backfills: ReadonlyArray<ForkV1Backfill> = [
          {
            id: "first",
            run: sql`INSERT OR IGNORE INTO fork_test_backfill (id)
            SELECT thread_id FROM orchestration_v2_legacy_imports`.pipe(Effect.asVoid),
          },
          {
            id: "second",
            run: Effect.gen(function* () {
              if (fail) return yield* new ForkV1BackfillStepError({ backfillId: "second" });
              yield* sql`INSERT OR IGNORE INTO fork_test_backfill (id) VALUES ('second')`;
            }),
          },
          {
            id: "last",
            run: sql`INSERT OR IGNORE INTO fork_test_backfill (id) VALUES ('last')`.pipe(
              Effect.asVoid,
            ),
          },
        ];
        const failed = yield* runForkV1Backfills(backfills).pipe(Effect.exit);
        assert.isTrue(Exit.isFailure(failed));
        if (Exit.isFailure(failed)) {
          assert.include(String(failed.cause), "second");
          assert.notInclude(String(failed.cause), "partial failure");
        }
        assert.deepEqual(yield* sql`SELECT id FROM fork_test_backfill ORDER BY id`, [
          { id: "legacy" },
        ]);
        assert.deepEqual(
          yield* sql`SELECT transcript_imported_at FROM orchestration_v2_legacy_imports`,
          [{ transcript_imported_at: null }],
        );
        fail = false;
        yield* runForkV1Backfills(backfills);
        yield* importer.reconcileShells;
        yield* runForkV1Backfills(backfills);
        assert.deepEqual(yield* sql`SELECT id FROM fork_test_backfill ORDER BY id`, [
          { id: "last" },
          { id: "legacy" },
          { id: "second" },
        ]);
        yield* importer.importPendingTranscripts;
        assert.deepEqual(
          yield* sql`SELECT imported_message_count FROM orchestration_v2_legacy_imports`,
          [{ imported_message_count: 1 }],
        );
      }).pipe(Effect.provide(forkV1SnapshotLayer(snapshotPath)));
      assert.deepEqual(NodeFS.readFileSync(sourcePath), original);
      const overwrite = yield* Effect.tryPromise(() =>
        vacuumForkV1Snapshot({ sourcePath, destinationPath: snapshotPath }),
      ).pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(overwrite));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

const realSource = process.env.CHROMERIA_V1_SNAPSHOT_SOURCE;
(realSource ? it.effect : it.effect.skip)(
  "imports a read-only VACUUM INTO snapshot of real V1 data",
  () =>
    Effect.gen(function* () {
      const directory = NodeFS.mkdtempSync(
        NodePath.join(NodeOS.tmpdir(), "chromeria-real-import-"),
      );
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => NodeFS.rmSync(directory, { recursive: true, force: true })),
      );
      const snapshotPath = NodePath.join(directory, "chromeria-v2.sqlite");
      yield* Effect.promise(() =>
        vacuumForkV1Snapshot({ sourcePath: realSource!, destinationPath: snapshotPath }),
      );
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
        const pending = yield* importer.pendingThreadCount;
        assert.isAbove(pending, 0);
        const result = yield* importer.reconcileShells;
        assert.equal(result.importedThreadCount, pending);
        yield* runForkV1Backfills([
          {
            id: "snapshot-shell-proof",
            run: Effect.gen(function* () {
              const missingShells = yield* sql`SELECT thread_id FROM projection_threads AS thread
              WHERE NOT EXISTS (
                SELECT 1 FROM orchestration_events AS event
                WHERE event.application_event_version = 2
                  AND event.aggregate_kind = 'thread'
                  AND event.stream_id = thread.thread_id
                  AND event.event_type = 'thread.created'
              )`;
              assert.lengthOf(missingShells, 0);
              const hydrated = yield* sql`SELECT thread_id FROM orchestration_v2_legacy_imports
              WHERE transcript_imported_at IS NOT NULL`;
              assert.lengthOf(hydrated, 0);
              const shells = yield* sql<{ count: number }>`SELECT COUNT(*) AS count
              FROM orchestration_v2_legacy_imports WHERE transcript_imported_at IS NULL`;
              assert.equal(shells[0]?.count, pending);
            }),
          },
        ]);
        yield* importer.importPendingTranscripts;
        assert.equal(yield* importer.pendingThreadCount, 0);
        const failures = yield* sql`SELECT thread_id FROM orchestration_v2_legacy_imports
        WHERE transcript_imported_at IS NULL OR last_error IS NOT NULL`;
        assert.lengthOf(failures, 0);
        const before = yield* sql`SELECT COUNT(*) AS count FROM orchestration_events`;
        assert.equal((yield* importer.reconcileShells).importedThreadCount, 0);
        yield* runForkV1Backfills();
        assert.equal((yield* importer.importPendingTranscripts).importedMessageCount, 0);
        assert.deepEqual(yield* sql`SELECT COUNT(*) AS count FROM orchestration_events`, before);
      }).pipe(Effect.provide(forkV1SnapshotLayer(snapshotPath)));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
