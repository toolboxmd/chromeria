// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as LegacyV1ThreadImporter from "../orchestration-v2/legacy/LegacyV1ThreadImporter.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import { forkV1Backfills, runForkV1Backfills } from "../persistence/forkV1Backfills.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { CheckState } from "./state.ts";

/**
 * Chromeria v1 scheduled tasks through the real v2 startup path, on an
 * ACTUAL-DATA SUBSET of a v1 database (toolboxmd/chromeria#174): every
 * scheduler event plus the exact projects and threads they reference, copied
 * read-only into a private file. Never a full snapshot. Runs only when
 * `CHROMERIA_V1_SUBSET_DB` names such a file; it works on a copy and records
 * counts only, never ids or text, in `CHROMERIA_V1_SUBSET_SUMMARY` when set.
 * Nothing imported is ever run.
 */
const subset = process.env.CHROMERIA_V1_SUBSET_DB;
/** Where to write the sanitized counts, when set. */
const summaryPath = process.env.CHROMERIA_V1_SUBSET_SUMMARY;
const toJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

const boot = (dbPath: string) => {
  const database = SqlitePersistence.layerFromPath(dbPath).pipe(Layer.provide(NodeServices.layer));
  const stores = Layer.mergeAll(
    database,
    EventStore.layer.pipe(Layer.provideMerge(database)),
    ProjectionStore.layer.pipe(Layer.provideMerge(database)),
  );
  const sink = EventSink.layer.pipe(Layer.provide(stores));
  return Layer.mergeAll(
    stores,
    sink,
    LegacyV1ThreadImporter.layer.pipe(Layer.provide(Layer.mergeAll(stores, sink))),
  );
};

/** Production's first startup phase on v1 data: thread shells, then the fork backfills. */
const startup = Effect.gen(function* () {
  const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
  const pending = yield* importer.pendingThreadCount;
  const shells = yield* importer.reconcileShells;
  yield* runForkV1Backfills(forkV1Backfills);
  return { pending, shells };
});

const decodeState = Schema.decodeUnknownSync(Schema.fromJsonString(CheckState));

/** Everything the import wrote, for comparing runs; only counts leave the test. */
const imported = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const streams = yield* sql<{ readonly stream_id: string }>`
    SELECT DISTINCT stream_id FROM orchestration_events WHERE event_type = 'scheduler.state-set'
    ORDER BY stream_id`;
  const markers = yield* sql<{
    readonly task_id: string;
    readonly outcome: string;
    readonly kind: string;
    readonly reason: string | null;
    readonly has_payload: number;
  }>`SELECT task_id, outcome, kind, reason, payload_json IS NOT NULL AS has_payload
     FROM fork_scheduled_task_v1_imports ORDER BY task_id`;
  const tasks = yield* sql<{
    readonly task_id: string;
    readonly enabled: number;
    readonly next_run_at: string | null;
    readonly last_run_status: string;
    readonly run_count: number;
    readonly thread_id: string | null;
    readonly bound_thread_in_v2: number;
  }>`SELECT task_id, enabled, next_run_at, last_run_status, run_count, thread_id,
       thread_id IS NOT NULL AND EXISTS (
         SELECT 1 FROM orchestration_v2_projection_threads AS thread
         WHERE thread.thread_id = scheduled_tasks.thread_id
       ) AS bound_thread_in_v2
     FROM scheduled_tasks ORDER BY task_id`;
  const states = yield* sql<{ readonly task_id: string; readonly state_json: string }>`
    SELECT task_id, state_json FROM fork_scheduled_task_checks ORDER BY task_id`;
  // A COUNT query always returns one row.
  const v2 = (yield* sql<{ readonly threads: number; readonly events: number }>`
    SELECT (SELECT COUNT(*) FROM orchestration_v2_projection_threads) AS threads,
           (SELECT COUNT(*) FROM orchestration_v2_events) AS events`)[0]!;
  return { streams, markers, tasks, states, v2 };
});

it.effect.skipIf(subset === undefined)(
  "an ACTUAL-DATA SUBSET of v1 scheduled tasks imports through the real startup path disabled and inert, and converges",
  () => {
    const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "chromeria-v1-subset-"));
    const dbPath = NodePath.join(directory, "state.sqlite");
    NodeFS.copyFileSync(subset!, dbPath);
    return Effect.gen(function* () {
      const first = yield* Effect.gen(function* () {
        const ran = yield* startup;
        const after = yield* imported;
        // A rerun in the same process changes nothing.
        const again = yield* startup;
        assert.equal(again.shells.importedThreadCount, 0);
        assert.deepEqual(yield* imported, after);
        return { ran, after };
      }).pipe(Effect.provide(boot(dbPath)));
      const { after } = first;

      // Every v1 task has exactly one marker.
      assert.deepEqual(
        after.markers.map((marker) => marker.task_id),
        after.streams.map((stream) => stream.stream_id),
      );
      const importedIds = new Set(
        after.markers.filter((marker) => marker.outcome === "imported").map((m) => m.task_id),
      );
      assert.deepEqual(
        after.tasks.filter((task) => importedIds.has(task.task_id)).map((task) => task.task_id),
        [...importedIds].toSorted(),
      );
      // Only tasks left behind keep their payload, privately.
      for (const marker of after.markers)
        assert.equal(marker.has_payload === 1, marker.outcome === "unsupported");
      // Imported tasks never fire until the user enables them, and bound ones keep their thread.
      for (const task of after.tasks.filter((entry) => importedIds.has(entry.task_id))) {
        assert.equal(task.enabled, 0);
        assert.isNull(task.next_run_at);
        assert.equal(task.last_run_status, "never");
        assert.equal(task.run_count, 0);
        if (task.thread_id !== null) assert.equal(task.bound_thread_in_v2, 1);
      }
      // Their v1 runs are inert history: marked imported, with no send to repeat.
      const runs = after.states.flatMap((row) => decodeState(row.state_json).runs);
      for (const run of runs) {
        assert.isDefined(run.imported);
        assert.deepEqual(run.sends, []);
      }

      // A restart on the same file converges too.
      const restarted = yield* Effect.gen(function* () {
        const ran = yield* startup;
        return { ran, after: yield* imported };
      }).pipe(Effect.provide(boot(dbPath)));
      assert.equal(restarted.ran.shells.importedThreadCount, 0);
      assert.deepEqual(restarted.after, after);

      const tally = (key: (marker: (typeof after.markers)[number]) => string) =>
        Object.fromEntries(
          [...new Set(after.markers.map(key))].map((value) => [
            value,
            after.markers.filter((marker) => key(marker) === value).length,
          ]),
        );
      // Counts only: the proof's sanitized record.
      const summary = {
        v1Tasks: after.streams.length,
        outcomes: tally((marker) => marker.outcome),
        kinds: tally((marker) => marker.kind),
        unsupportedReasons: tally((marker) => marker.reason ?? "none"),
        importedDisabled: after.tasks.filter(
          (task) => importedIds.has(task.task_id) && task.enabled === 0,
        ).length,
        boundToImportedThread: after.tasks.filter((task) => task.bound_thread_in_v2 === 1).length,
        inertRuns: runs.length,
        pendingThreadsAtStart: first.ran.pending,
        threadShellsImported: first.ran.shells.importedThreadCount,
        v2Threads: after.v2.threads,
        rerunAndRestartChanges: 0,
      };
      yield* Effect.logInfo("ACTUAL-DATA SUBSET v1 scheduled-task import").pipe(
        Effect.annotateLogs(summary),
      );
      if (summaryPath !== undefined) NodeFS.writeFileSync(summaryPath, toJson(summary));
    }).pipe(
      Effect.ensuring(
        Effect.sync(() => NodeFS.rmSync(directory, { recursive: true, force: true })),
      ),
    );
  },
);
