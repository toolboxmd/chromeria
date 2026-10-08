// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";

import { ProjectionStoreV2 } from "../orchestration-v2/ProjectionStore.ts";
import * as LegacyV1ThreadImporter from "../orchestration-v2/legacy/LegacyV1ThreadImporter.ts";
import { runMigrations } from "../persistence/Migrations.ts";
import { runForkV1Backfills } from "../persistence/forkV1Backfills.ts";
import {
  forkV1SnapshotLayer,
  vacuumForkV1Snapshot,
} from "../persistence/forkV1Snapshot.testFixtures.ts";
import { importedLineageRepairs } from "./lineageBackfill.ts";

it.effect(
  "repairs imported descendants after partial failure without hydrating transcripts or linking orphans",
  () =>
    Effect.gen(function* () {
      const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "chromeria-lineage-"));
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
        yield* Effect.forEach(
          [
            "root",
            "sub.root.child",
            "sub.sub.root.child.grandchild",
            "sub.missing.orphan",
            "ordinary",
          ],
          (id) =>
            sql`INSERT INTO projection_threads (thread_id, project_id, title, model_selection_json, runtime_mode, interaction_mode, created_at, updated_at)
          VALUES (${id}, 'project', 'Imported', '{"instanceId":"codex","model":"gpt-5.4"}', 'full-access', 'default', '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z')`,
        );
        yield* sql`INSERT INTO projection_thread_messages (message_id, thread_id, role, text, is_streaming, created_at, updated_at)
        VALUES ('message', 'sub.root.child', 'user', 'Transcript fixture', 0, '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z')`;
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: sourcePath })));
      const original = NodeFS.readFileSync(sourcePath);
      yield* Effect.promise(() =>
        vacuumForkV1Snapshot({ sourcePath, destinationPath: snapshotPath }),
      );
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
        const projections = yield* ProjectionStoreV2;
        yield* importer.reconcileShells;
        yield* sql`CREATE TRIGGER fail_lineage BEFORE INSERT ON orchestration_events
        WHEN NEW.event_id = 'migration:fork-v1:child-lineage:sub.sub.root.child.grandchild'
        BEGIN SELECT RAISE(ABORT, 'lineage fixture failure'); END`;
        assert.isTrue(Exit.isFailure(yield* runForkV1Backfills().pipe(Effect.exit)));
        assert.equal(
          (yield* projections.getThread(ThreadId.make("sub.root.child"))).lineage.parentThreadId,
          "root",
        );
        assert.isNull(
          (yield* projections.getThread(ThreadId.make("sub.sub.root.child.grandchild"))).lineage
            .parentThreadId,
        );
        yield* sql`DROP TRIGGER fail_lineage`;
        yield* runForkV1Backfills();
        const child = yield* projections.getThread(ThreadId.make("sub.root.child"));
        const grandchild = yield* projections.getThread(
          ThreadId.make("sub.sub.root.child.grandchild"),
        );
        assert.deepEqual(child.lineage, {
          parentThreadId: ThreadId.make("root"),
          relationshipToParent: "subagent",
          rootThreadId: ThreadId.make("root"),
        });
        assert.deepEqual(grandchild.lineage, {
          parentThreadId: ThreadId.make("sub.root.child"),
          relationshipToParent: "subagent",
          rootThreadId: ThreadId.make("root"),
        });
        assert.isNull(
          (yield* projections.getThread(ThreadId.make("sub.missing.orphan"))).lineage
            .parentThreadId,
        );
        assert.isNull(
          (yield* projections.getThread(ThreadId.make("ordinary"))).lineage.parentThreadId,
        );
        assert.lengthOf(
          yield* sql`SELECT thread_id FROM orchestration_v2_legacy_imports WHERE transcript_imported_at IS NOT NULL`,
          0,
        );
        const events = yield* sql`SELECT COUNT(*) AS count FROM orchestration_events`;
        yield* importer.reconcileShells;
        yield* runForkV1Backfills();
        assert.deepEqual(yield* sql`SELECT COUNT(*) AS count FROM orchestration_events`, events);
        yield* importer.importPendingTranscripts;
        yield* importer.reconcileShells;
        yield* runForkV1Backfills();
        assert.deepEqual((yield* projections.getThread(child.id)).lineage, child.lineage);
        assert.deepEqual((yield* projections.getThread(grandchild.id)).lineage, grandchild.lineage);
        assert.deepEqual(
          yield* sql`SELECT imported_message_count FROM orchestration_v2_legacy_imports WHERE thread_id = 'sub.root.child'`,
          [{ imported_message_count: 1 }],
        );

        // A proposed imported link must not close an existing native cycle.
        const root = yield* projections.getThread(ThreadId.make("root"));
        assert.lengthOf(
          importedLineageRepairs([
            {
              ...root,
              lineage: {
                parentThreadId: child.id,
                relationshipToParent: "subagent",
                rootThreadId: root.id,
              },
            },
            {
              ...child,
              lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: child.id },
            },
          ]),
          0,
        );
      }).pipe(Effect.provide(forkV1SnapshotLayer(snapshotPath)));
      assert.deepEqual(NodeFS.readFileSync(sourcePath), original);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
