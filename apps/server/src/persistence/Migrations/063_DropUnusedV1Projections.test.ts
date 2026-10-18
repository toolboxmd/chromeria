import { assert, describe, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";

const legacyTables = [
  "projection_thread_activities",
  "projection_turns",
  "projection_thread_proposed_plans",
  "projection_pending_approvals",
  "projection_state",
  "checkpoint_diff_blobs",
] as const;

describe("063_DropUnusedV1Projections", () => {
  it.effect(
    "retains Chromeria's legacy rows and schemas while advancing the migration ledger",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations({ toMigrationInclusive: 62 });
        yield* sql`INSERT INTO projection_thread_activities
        (activity_id, thread_id, tone, kind, summary, payload_json, created_at)
        VALUES ('activity-1', 'thread-1', 'info', 'tool', 'retained', '{}', '2026-01-01')`;
        yield* sql`INSERT INTO projection_turns
        (thread_id, turn_id, state, requested_at, checkpoint_files_json)
        VALUES ('thread-1', 'turn-1', 'completed', '2026-01-01', '[]')`;
        yield* sql`INSERT INTO projection_thread_proposed_plans
        (plan_id, thread_id, plan_markdown, created_at, updated_at)
        VALUES ('plan-1', 'thread-1', 'Retained plan', '2026-01-01', '2026-01-01')`;
        yield* sql`INSERT INTO projection_pending_approvals
        (request_id, thread_id, status, created_at)
        VALUES ('request-1', 'thread-1', 'pending', '2026-01-01')`;
        yield* sql`INSERT OR REPLACE INTO projection_state
        (projector, last_applied_sequence, updated_at)
        VALUES ('retained-projector', 42, '2026-01-01')`;
        yield* sql`INSERT INTO checkpoint_diff_blobs
        (thread_id, from_turn_count, to_turn_count, diff, created_at)
        VALUES ('thread-1', 0, 1, 'retained diff', '2026-01-01')`;
        yield* sql`INSERT INTO projection_threads
        (thread_id, project_id, title, model_selection_json, runtime_mode, created_at, updated_at)
        VALUES ('thread-1', 'project-1', 'Retained thread', '{}', 'full-access', '2026-01-01', '2026-01-01')`;
        yield* sql`
        INSERT INTO projection_thread_messages
          (message_id, thread_id, role, text, is_streaming, created_at, updated_at)
        VALUES ('message-1', 'thread-1', 'user', 'hello', 0, '2026-01-01', '2026-01-01')
      `;
        yield* sql`INSERT INTO projection_thread_pull_requests
        (thread_id, host, repository, number, url, source, linked_at)
        VALUES ('thread-1', 'github.com', 'toolboxmd/chromeria', 411,
          'https://github.com/toolboxmd/chromeria/pull/411', 'manual', '2026-01-01')`;

        const retained = [
          ...legacyTables,
          "projection_threads",
          "projection_thread_messages",
          "projection_thread_pull_requests",
        ];
        const snapshot = Effect.gen(function* () {
          const schemas = yield* sql`SELECT type, name, tbl_name, sql FROM sqlite_master
          WHERE ${sql.in("tbl_name", retained)} ORDER BY type, name`;
          const rows = yield* Effect.forEach(retained, (name) =>
            sql.unsafe(`SELECT * FROM ${name} ORDER BY rowid`),
          );
          return { schemas, rows };
        });
        const before = yield* snapshot;

        assert.deepEqual(yield* runMigrations({ toMigrationInclusive: 63 }), [
          [63, "DropUnusedV1Projections"],
        ]);
        assert.deepEqual(yield* snapshot, before);
        assert.deepEqual(yield* runMigrations(), []);
        assert.deepEqual(yield* snapshot, before);
        assert.deepEqual(
          yield* sql`SELECT migration_id, name FROM effect_sql_migrations WHERE migration_id = 63`,
          [{ migration_id: 63, name: "DropUnusedV1Projections" }],
        );
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect("leaves already absent legacy tables absent on upgrade and rerun", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 62 });
      for (const name of legacyTables) yield* sql.unsafe(`DROP TABLE ${name}`);

      yield* runMigrations();
      assert.deepEqual(yield* runMigrations(), []);

      const rows = yield* sql<{ readonly name: string }>`
        SELECT name FROM sqlite_master WHERE type = 'table'
      `;
      const tables = new Set(rows.map((row) => row.name));
      for (const dropped of legacyTables) {
        assert.isFalse(tables.has(dropped), dropped);
      }
      for (const kept of [
        "projection_threads",
        "projection_thread_messages",
        "projection_thread_pull_requests",
      ]) {
        assert.isTrue(tables.has(kept), kept);
      }
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );
});
