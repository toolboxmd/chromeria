import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { runMigrations } from "./Migrations.ts";
import { ensureScheduledTaskOriginsSchema } from "./forkScheduledTaskOriginsSchema.ts";

it.effect("adds nullable scheduled origin once while preserving existing task history", () =>
  Effect.gen(function* () {
    yield* runMigrations();
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO scheduled_tasks ${sql.insert({
      task_id: "legacy-timer",
      title: "Legacy",
      prompt: "Keep this work",
      enabled: 0,
      schedule_json: '{"type":"interval","everyMs":60000}',
      project_id: "project",
      thread_id: null,
      workspace_strategy_json: '{"type":"root"}',
      model_selection_json: '{"instanceId":"codex","model":"test"}',
      runtime_mode: "auto",
      interaction_mode: "default",
      created_by: "user",
      creation_source: "web",
      created_at: "2026-10-14T00:00:00Z",
      updated_at: "2026-10-14T00:00:00Z",
      next_run_at: null,
      last_run_at: null,
      last_run_status: "succeeded",
      last_run_error: null,
      run_count: 7,
    })}`;
    yield* ensureScheduledTaskOriginsSchema;
    yield* ensureScheduledTaskOriginsSchema;
    const rows = yield* sql<{
      prompt: string;
      run_count: number;
      link_origin_json: string | null;
    }>`SELECT prompt, run_count, link_origin_json FROM scheduled_tasks WHERE task_id = 'legacy-timer'`;
    assert.deepStrictEqual(rows, [
      { prompt: "Keep this work", run_count: 7, link_origin_json: null },
    ]);
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);
