// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { EventId, ThreadId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Logger from "effect/Logger";
import * as References from "effect/References";
import * as SqlClient from "effect/sql/SqlClient";

import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as LegacyV1ThreadImporter from "../orchestration-v2/legacy/LegacyV1ThreadImporter.ts";
import { runMigrations } from "./Migrations.ts";
import { runForkV1Backfills } from "./forkV1Backfills.ts";
import { backfillThreadPeople } from "./forkThreadPeopleBackfill.ts";
import { forkV1SnapshotLayer, vacuumForkV1Snapshot } from "./forkV1Snapshot.testFixtures.ts";

interface V1Thread {
  readonly id: string;
  readonly owner: string | null;
  readonly coOwnersJson: string;
}

/** Builds a v1 database, with the v1 people columns unless told otherwise, and snapshots it. */
const v1Snapshot = (threads: ReadonlyArray<V1Thread>, options?: { readonly people: boolean }) =>
  Effect.gen(function* () {
    const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "chromeria-people-"));
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => NodeFS.rmSync(directory, { recursive: true, force: true })),
    );
    const sourcePath = NodePath.join(directory, "state.sqlite");
    const snapshotPath = NodePath.join(directory, "chromeria-v2.sqlite");
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 52 });
      if (options?.people !== false) {
        yield* sql`ALTER TABLE projection_threads ADD COLUMN owner TEXT`;
        yield* sql`ALTER TABLE projection_threads ADD COLUMN co_owners_json TEXT NOT NULL DEFAULT '[]'`;
      }
      yield* sql`INSERT INTO projection_projects (project_id, title, workspace_root, scripts_json, created_at, updated_at)
      VALUES ('project', 'Project', '/tmp/project', '[]', '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z')`;
      for (const [index, thread] of threads.entries()) {
        const createdAt = `2026-01-01T00:00:0${index}.000Z`;
        yield* sql`INSERT INTO projection_threads (thread_id, project_id, title, model_selection_json, runtime_mode, interaction_mode, created_at, updated_at)
        VALUES (${thread.id}, 'project', 'Thread', '{"instanceId":"codex","model":"gpt-5.4"}', 'full-access', 'default', ${createdAt}, ${createdAt})`;
        if (options?.people !== false) {
          yield* sql`UPDATE projection_threads SET owner = ${thread.owner}, co_owners_json = ${thread.coOwnersJson}
          WHERE thread_id = ${thread.id}`;
        }
      }
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: sourcePath })));
    yield* Effect.promise(() =>
      vacuumForkV1Snapshot({ sourcePath, destinationPath: snapshotPath }),
    );
    return snapshotPath;
  });

const peopleRows = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql<{
    readonly thread_id: string;
    readonly owner_type: string | null;
    readonly owner: string | null;
    readonly co_owners: string | null;
  }>`
    SELECT
      thread_id,
      json_type(payload_json, '$.owner') AS owner_type,
      json_extract(payload_json, '$.owner') AS owner,
      json_extract(payload_json, '$.coOwners') AS co_owners
    FROM orchestration_v2_projection_threads
    ORDER BY thread_id
  `;
});

const markerCount = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const [row] = yield* sql<{ readonly count: number }>`
    SELECT COUNT(*) AS count FROM orchestration_events
    WHERE event_id LIKE 'migration:fork-v1:people:%'
  `;
  return row?.count ?? 0;
});

it.effect(
  "carries frozen owners and co-owners onto imported threads, recording malformed ones",
  () =>
    Effect.gen(function* () {
      const snapshotPath = yield* v1Snapshot([
        { id: "pauli-owned", owner: "Pauli", coOwnersJson: "[]" },
        { id: "luke-shared", owner: "Luke", coOwnersJson: '["Pauli"]' },
        { id: "bad-co-owners", owner: "Pauli", coOwnersJson: "not json" },
        { id: "bad-owner", owner: "", coOwnersJson: "[]" },
        { id: "unowned", owner: null, coOwnersJson: "[]" },
        { id: "pauli-shared", owner: "Pauli", coOwnersJson: '["Luke"]' },
        { id: "v2-wins", owner: "Pauli", coOwnersJson: "garbage" },
        { id: "bad-both", owner: " ", coOwnersJson: '[""]' },
      ]);
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const eventSink = yield* EventSink.EventSinkV2;
        yield* importer.reconcileShells;

        // A co-owner set in v2 before the backfill wins, and its garbage frozen value is never read.
        const v2Thread = yield* projections.getThread(ThreadId.make("v2-wins"));
        yield* eventSink.write({
          events: [
            {
              id: EventId.make("test:v2-share"),
              type: "thread.metadata-updated",
              threadId: v2Thread.id,
              providerInstanceId: v2Thread.providerInstanceId,
              occurredAt: yield* DateTime.now,
              payload: { ...v2Thread, coOwners: ["Luke"] },
            },
          ],
        });

        // The third thread fails after its event is written: the first two stay done, and
        // that thread's event and diagnostic roll back together.
        let writes = 0;
        const flakySink = EventSink.EventSinkV2.of({
          ...eventSink,
          write: (input) =>
            ++writes === 3
              ? eventSink
                  .write(input)
                  .pipe(
                    Effect.andThen(
                      Effect.fail(
                        new EventSink.EventSinkWriteError({ eventCount: input.events.length }),
                      ),
                    ),
                  )
              : eventSink.write(input),
        });
        const failed = yield* backfillThreadPeople.pipe(
          Effect.provideService(EventSink.EventSinkV2, flakySink),
          Effect.exit,
        );
        assert.isTrue(Exit.isFailure(failed));
        assert.equal(writes, 3);
        assert.equal(yield* markerCount, 2);
        assert.deepEqual(
          yield* sql`SELECT thread_id, reason_kind FROM fork_thread_people_backfill_issues`,
          [],
        );

        const logged: Array<unknown> = [];
        yield* backfillThreadPeople.pipe(
          Effect.provide(
            Logger.layer(
              [
                Logger.make(({ fiber }) => {
                  logged.push(fiber.getRef(References.CurrentLogAnnotations));
                }),
              ],
              { mergeWithExisting: false },
            ),
          ),
        );
        // One count-only line: threads backfilled in this run, distinct threads left unresolved.
        assert.deepEqual(logged, [{ backfilledThreadCount: 6, unresolvedThreadCount: 3 }]);
        const expected = [
          { thread_id: "bad-both", owner_type: "null", owner: null, co_owners: "[]" },
          { thread_id: "bad-co-owners", owner_type: "text", owner: "Pauli", co_owners: "[]" },
          { thread_id: "bad-owner", owner_type: "null", owner: null, co_owners: "[]" },
          { thread_id: "luke-shared", owner_type: "text", owner: "Luke", co_owners: '["Pauli"]' },
          { thread_id: "pauli-owned", owner_type: "text", owner: "Pauli", co_owners: "[]" },
          { thread_id: "pauli-shared", owner_type: "text", owner: "Pauli", co_owners: '["Luke"]' },
          { thread_id: "unowned", owner_type: "null", owner: null, co_owners: "[]" },
          { thread_id: "v2-wins", owner_type: "text", owner: "Pauli", co_owners: '["Luke"]' },
        ];
        assert.deepEqual(yield* peopleRows, expected);
        const shell = yield* projections.getThreadShell(ThreadId.make("pauli-shared"));
        assert.equal(shell?.owner, "Pauli");
        assert.deepEqual(shell?.coOwners, ["Luke"]);
        assert.deepEqual(
          yield* sql`SELECT thread_id, reason_kind FROM fork_thread_people_backfill_issues
          ORDER BY thread_id, reason_kind`,
          [
            { thread_id: "bad-both", reason_kind: "invalid_co_owners" },
            { thread_id: "bad-both", reason_kind: "invalid_owner" },
            { thread_id: "bad-co-owners", reason_kind: "invalid_co_owners" },
            { thread_id: "bad-owner", reason_kind: "invalid_owner" },
          ],
        );
        assert.equal(yield* markerCount, 8);
        // The hook runs before transcripts hydrate.
        assert.deepEqual(
          yield* sql`SELECT COUNT(*) AS count FROM orchestration_v2_legacy_imports
          WHERE transcript_imported_at IS NOT NULL`,
          [{ count: 0 }],
        );

        // Reruns change nothing, and recorded rows stay.
        const [before] = yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM orchestration_events`;
        yield* runForkV1Backfills();
        yield* importer.reconcileShells;
        yield* runForkV1Backfills();
        const [after] = yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM orchestration_events`;
        assert.equal(after?.count, before?.count);
        assert.deepEqual(yield* peopleRows, expected);
        assert.lengthOf(yield* sql`SELECT * FROM fork_thread_people_backfill_issues`, 4);
      }).pipe(Effect.provide(forkV1SnapshotLayer(snapshotPath)));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect.each([
  { payload: "{}", typedError: true },
  { payload: "not json", typedError: false },
])("stops startup when an imported thread payload is $payload", ({ payload, typedError }) =>
  Effect.gen(function* () {
    const snapshotPath = yield* v1Snapshot([{ id: "broken", owner: "Pauli", coOwnersJson: "[]" }]);
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
      yield* importer.reconcileShells;
      yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json = ${payload}
      WHERE thread_id = 'broken'`;
      // Valid JSON of the wrong shape is the feature's own error; malformed JSON fails in SQL.
      const direct = yield* backfillThreadPeople.pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(direct));
      if (Exit.isFailure(direct)) {
        assert.equal(String(direct.cause).includes("ThreadPeopleBackfillPayloadError"), typedError);
      }
      const failed = yield* runForkV1Backfills().pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(failed));
      if (Exit.isFailure(failed)) {
        assert.include(String(failed.cause), "ForkV1BackfillError");
        assert.include(String(failed.cause), "thread-people");
        assert.notInclude(String(failed.cause), payload);
      }
      assert.equal(yield* markerCount, 0);
      assert.lengthOf(yield* sql`SELECT * FROM fork_thread_people_backfill_issues`, 0);
    }).pipe(Effect.provide(forkV1SnapshotLayer(snapshotPath)));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("leaves threads untouched when the v1 database never had people", () =>
  Effect.gen(function* () {
    const snapshotPath = yield* v1Snapshot([{ id: "plain", owner: null, coOwnersJson: "[]" }], {
      people: false,
    });
    yield* Effect.gen(function* () {
      const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
      yield* importer.reconcileShells;
      yield* runForkV1Backfills();
      assert.equal(yield* markerCount, 0);
      assert.deepEqual(yield* peopleRows, [
        { thread_id: "plain", owner_type: null, owner: null, co_owners: null },
      ]);
    }).pipe(Effect.provide(forkV1SnapshotLayer(snapshotPath)));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

const realSource = process.env.CHROMERIA_V1_SNAPSHOT_SOURCE;
(realSource ? it.effect : it.effect.skip)(
  "carries every frozen owner of a read-only real V1 snapshot",
  () =>
    Effect.gen(function* () {
      const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "chromeria-people-"));
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
        yield* importer.reconcileShells;
        yield* runForkV1Backfills();
        const count = (rows: ReadonlyArray<{ readonly count: number }>) => rows[0]?.count ?? 0;
        const imported = count(
          yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count FROM orchestration_v2_legacy_imports`,
        );
        const missing = count(
          yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count FROM orchestration_v2_legacy_imports AS legacy_import
            INNER JOIN orchestration_v2_projection_threads AS projection
              ON projection.thread_id = legacy_import.thread_id
            WHERE json_type(projection.payload_json, '$.owner') IS NULL
               OR json_type(projection.payload_json, '$.coOwners') IS NULL`,
        );
        // Independent oracle: the payload equals the frozen v1 columns for every recorded-clean thread.
        const mismatched = count(
          yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count FROM orchestration_v2_legacy_imports AS legacy_import
            INNER JOIN projection_threads AS thread ON thread.thread_id = legacy_import.thread_id
            INNER JOIN orchestration_v2_projection_threads AS projection
              ON projection.thread_id = legacy_import.thread_id
            WHERE NOT EXISTS (
                SELECT 1 FROM fork_thread_people_backfill_issues AS issue
                WHERE issue.thread_id = thread.thread_id
              )
              AND (
                json_extract(projection.payload_json, '$.owner') IS NOT thread.owner
                OR json(json_extract(projection.payload_json, '$.coOwners'))
                  IS NOT json(thread.co_owners_json)
              )`,
        );
        const unresolved = count(
          yield* sql<{ readonly count: number }>`
            SELECT COUNT(DISTINCT thread_id) AS count FROM fork_thread_people_backfill_issues`,
        );
        const hydrated = count(
          yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count FROM orchestration_v2_legacy_imports
            WHERE transcript_imported_at IS NOT NULL`,
        );
        const before = count(
          yield* sql<{
            readonly count: number;
          }>`SELECT COUNT(*) AS count FROM orchestration_events`,
        );
        yield* runForkV1Backfills();
        const after = count(
          yield* sql<{
            readonly count: number;
          }>`SELECT COUNT(*) AS count FROM orchestration_events`,
        );
        // Counts only: no thread ids, titles or payloads leave the snapshot.
        console.info(
          JSON.stringify({
            imported,
            missing,
            mismatched,
            unresolved,
            hydrated,
            rerunEvents: after - before,
          }),
        );
        assert.isAbove(imported, 0);
        assert.equal(yield* markerCount, imported);
        assert.equal(missing, 0);
        assert.equal(mismatched, 0);
        assert.equal(hydrated, 0);
        assert.equal(after, before);
      }).pipe(Effect.provide(forkV1SnapshotLayer(snapshotPath)));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
