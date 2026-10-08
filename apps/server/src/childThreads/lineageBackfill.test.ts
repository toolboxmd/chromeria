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

const realSource = process.env.CHROMERIA_V1_SNAPSHOT_SOURCE;
(realSource ? it.effect : it.effect.skip)(
  "repairs actual V1 sub ancestry from a read-only snapshot without changing transcripts",
  () => {
    let phase = "snapshot";
    return Effect.gen(function* () {
      const directory = NodeFS.mkdtempSync(
        NodePath.join(NodeOS.tmpdir(), "chromeria-real-lineage-"),
      );
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => NodeFS.rmSync(directory, { recursive: true, force: true })),
      );
      const snapshotPath = NodePath.join(directory, "state.sqlite");
      yield* Effect.promise(() =>
        vacuumForkV1Snapshot({ sourcePath: realSource!, destinationPath: snapshotPath }),
      );
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
        const projections = yield* ProjectionStoreV2;
        phase = "frozen ancestry inventory";
        const source = yield* sql<{ thread_id: string }>`SELECT thread_id FROM projection_threads`;
        const ids = new Set(source.map((row) => row.thread_id));
        // Independent V1 naming oracle, not the production repair helper.
        const expected = new Map<string, string>();
        let subCount = 0;
        for (const id of ids) {
          if (!id.startsWith("sub.")) continue;
          subCount++;
          const components = id.slice(4).split(".");
          components.pop();
          const parent = components.join(".");
          if (ids.has(parent)) expected.set(id, parent);
        }
        assert.isAbove(expected.size, 0);
        yield* importer.reconcileShells;
        const transcriptCounts = Effect.gen(function* () {
          const legacy = yield* sql<{
            count: number;
          }>`SELECT COUNT(*) AS count FROM projection_thread_messages`;
          const native = yield* sql<{
            count: number;
          }>`SELECT COUNT(*) AS count FROM orchestration_v2_projection_messages`;
          const ledger = yield* sql<{
            count: number;
            messages: number;
          }>`SELECT COUNT(transcript_imported_at) AS count,
            COALESCE(SUM(imported_message_count), 0) AS messages FROM orchestration_v2_legacy_imports`;
          return { legacy: legacy[0]?.count, native: native[0]?.count, ledger: ledger[0] };
        });
        const before = yield* transcriptCounts;
        phase = "repair coverage and graph integrity";
        yield* runForkV1Backfills();
        const threads = yield* Effect.forEach(source, (row) =>
          projections.getThread(ThreadId.make(row.thread_id)),
        );
        const byId = new Map(threads.map((thread) => [String(thread.id), thread]));
        let covered = 0;
        let dangling = 0;
        let cycles = 0;
        let rootMismatch = 0;
        let unexpectedLinks = 0;
        for (const thread of threads) {
          const expectedParent = expected.get(thread.id);
          if (
            expectedParent !== undefined &&
            thread.lineage.parentThreadId === expectedParent &&
            thread.lineage.relationshipToParent === "subagent"
          )
            covered++;
          if (expectedParent === undefined && thread.lineage.parentThreadId !== null)
            unexpectedLinks++;
          const seen = new Set<string>();
          let cursor = thread;
          while (cursor.lineage.parentThreadId !== null) {
            if (seen.has(cursor.id)) {
              cycles++;
              break;
            }
            seen.add(cursor.id);
            const parent = byId.get(cursor.lineage.parentThreadId);
            if (parent === undefined) {
              dangling++;
              break;
            }
            cursor = parent;
          }
          if (thread.lineage.rootThreadId !== cursor.id) rootMismatch++;
        }
        assert.equal(covered, expected.size);
        assert.equal(dangling, 0);
        assert.equal(cycles, 0);
        assert.equal(rootMismatch, 0);
        assert.equal(unexpectedLinks, 0);
        const repairs = yield* sql<{
          count: number;
        }>`SELECT COUNT(*) AS count FROM orchestration_events
          WHERE event_id LIKE 'migration:fork-v1:child-lineage:%'`;
        assert.equal(repairs[0]?.count, expected.size);
        assert.deepEqual(yield* transcriptCounts, before);
        phase = "idempotent startup replay";
        const events = yield* sql<{
          count: number;
        }>`SELECT COUNT(*) AS count FROM orchestration_events`;
        yield* importer.reconcileShells;
        yield* runForkV1Backfills();
        assert.deepEqual(yield* sql`SELECT COUNT(*) AS count FROM orchestration_events`, events);
        assert.deepEqual(yield* transcriptCounts, before);
        phase = "hydrated transcript preservation";
        yield* importer.importPendingTranscripts;
        const hydrated = yield* transcriptCounts;
        assert.isAbove(hydrated.native ?? 0, 0);
        assert.equal(hydrated.legacy, before.legacy);
        const hydratedEvents = yield* sql<{
          count: number;
        }>`SELECT COUNT(*) AS count FROM orchestration_events`;
        yield* importer.reconcileShells;
        yield* runForkV1Backfills();
        assert.equal((yield* importer.importPendingTranscripts).importedMessageCount, 0);
        assert.deepEqual(yield* transcriptCounts, hydrated);
        assert.deepEqual(
          yield* sql`SELECT COUNT(*) AS count FROM orchestration_events`,
          hydratedEvents,
        );
        const summary = {
          sourceThreads: ids.size,
          subThreads: subCount,
          existingParentLinks: expected.size,
          orphanSubThreads: subCount - expected.size,
          repairedLinks: covered,
          dangling,
          cycles,
          rootMismatch,
          before,
          hydrated,
        };
        // Optional aggregate-only evidence outside the temporary snapshot and repository.
        const reportPath = process.env.CHROMERIA_V1_SNAPSHOT_REPORT;
        if (reportPath !== undefined)
          yield* Effect.sync(() =>
            NodeFS.writeFileSync(reportPath, JSON.stringify(summary, null, 2)),
          );
      }).pipe(Effect.provide(forkV1SnapshotLayer(snapshotPath)));
    }).pipe(
      Effect.scoped,
      Effect.provide(NodeServices.layer),
      // Database errors and assertions can carry private IDs or transcript values.
      Effect.catchCause(() =>
        Effect.die(
          new Error(`Real V1 lineage proof failed during ${phase}; private details suppressed`),
        ),
      ),
    );
  },
);
