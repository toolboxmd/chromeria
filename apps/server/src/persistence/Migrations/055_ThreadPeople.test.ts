import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { runMigrations } from "../Migrations.ts";
import migrate from "./055_ThreadPeople.ts";

it.layer(NodeSqliteClient.layer({ filename: ":memory:" }))("055_ThreadPeople", (it) => {
  it.effect("keeps legacy owners null and sharing empty; reruns preserve labels", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 54 });
      yield* sql`INSERT INTO projection_threads (thread_id, project_id, title, model_selection_json, runtime_mode, created_at, updated_at) VALUES ('thread', 'project', 'Legacy', '{"instanceId":"codex","model":"gpt-5"}', 'full-access', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`;
      yield* sql`INSERT INTO auth_sessions (session_id, subject, scopes, method, issued_at, expires_at) VALUES ('session', 'device', '[]', 'bearer-access-token', '2026-01-01T00:00:00Z', '2027-01-01T00:00:00Z')`;
      yield* runMigrations({ toMigrationInclusive: 55 });
      assert.deepEqual(yield* sql`SELECT owner, co_owners_json FROM projection_threads`, [
        { owner: null, co_owners_json: "[]" },
      ]);
      assert.deepEqual(yield* sql`SELECT person FROM auth_sessions`, [{ person: null }]);
      yield* sql`UPDATE auth_sessions SET person = 'Pauli'`;
      yield* sql`UPDATE projection_threads SET owner = 'Pauli', co_owners_json = '["Luke"]'`;
      yield* migrate;
      assert.deepEqual(yield* sql`SELECT owner, co_owners_json FROM projection_threads`, [
        { owner: "Pauli", co_owners_json: '["Luke"]' },
      ]);
      assert.deepEqual(yield* sql`SELECT person FROM auth_sessions`, [{ person: "Pauli" }]);
    }),
  );
});
