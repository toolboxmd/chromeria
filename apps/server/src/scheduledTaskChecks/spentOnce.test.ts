import { assert, it } from "@effect/vitest";
import { ScheduledTaskId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { deleteSpentOnceTask, pruneSpentForkOnceTasks } from "./spentOnce.ts";
import { ensureCheckSchema } from "./store.ts";

const RAN = "2026-10-08T10:00:05.000Z";
const at = (value: string) => JSON.stringify({ type: "once", at: value });
const wrap = (schedule: string) =>
  JSON.stringify({
    type: "destination-automation-v1",
    certificateId: "c",
    schedule: JSON.parse(schedule),
  });

// Each row: whether completion removes it, and whether the startup prune does.
const rows = [
  { id: "spent-at", schedule: at("2026-10-08T10:00:00Z"), onCompletion: true, pruned: true },
  {
    id: "spent-offset",
    schedule: at("2026-10-08T12:00:00+02:00"),
    onCompletion: true,
    pruned: true,
  },
  {
    id: "spent-wrapped",
    schedule: wrap(at("2026-10-08T10:00:00Z")),
    onCompletion: true,
    pruned: true,
  },
  {
    id: "spent-run-at",
    schedule: JSON.stringify({ type: "once", runAt: "2026-10-08T10:00:00.000Z" }),
    onCompletion: true,
    pruned: false,
  },
  {
    id: "failed",
    schedule: at("2026-10-08T10:00:00Z"),
    status: "failed",
    onCompletion: false,
    pruned: false,
  },
  {
    id: "never-run",
    schedule: at("2026-10-08T10:00:00Z"),
    status: "never",
    lastRunAt: null,
    onCompletion: false,
    pruned: false,
  },
  {
    id: "paused",
    schedule: at("2026-10-08T10:00:00Z"),
    enabled: 0,
    onCompletion: true,
    pruned: false,
  },
  {
    id: "due",
    schedule: at("2026-10-09T10:00:00Z"),
    next: "2026-10-09T10:00:00.000Z",
    onCompletion: false,
    pruned: false,
  },
  { id: "run-early", schedule: at("2026-10-08T11:00:00Z"), onCompletion: false, pruned: false },
  {
    id: "checked",
    schedule: at("2026-10-08T10:00:00Z"),
    checked: true,
    onCompletion: false,
    pruned: false,
  },
  {
    id: "interval",
    schedule: '{"type":"interval","everyMs":60000}',
    onCompletion: false,
    pruned: false,
  },
  { id: "corrupt", schedule: "broken", onCompletion: false, pruned: false },
] as const satisfies ReadonlyArray<{
  id: string;
  schedule: string;
  status?: string;
  lastRunAt?: null;
  enabled?: number;
  next?: string;
  checked?: boolean;
  onCompletion: boolean;
  pruned: boolean;
}>;

const seed = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* ensureCheckSchema;
  for (const row of rows) {
    yield* sql`INSERT INTO scheduled_tasks ${sql.insert({
      task_id: row.id,
      title: "task",
      prompt: "Run task",
      enabled: "enabled" in row ? row.enabled : 1,
      schedule_json: row.schedule,
      project_id: "project:test",
      thread_id: null,
      workspace_strategy_json: '{"type":"root"}',
      model_selection_json: '{"instanceId":"codex","model":"gpt-5"}',
      runtime_mode: "full-access",
      interaction_mode: "default",
      created_by: "user",
      creation_source: "web",
      created_at: "2026-10-01T00:00:00.000Z",
      updated_at: RAN,
      next_run_at: "next" in row ? row.next : null,
      last_run_at: "lastRunAt" in row ? row.lastRunAt : RAN,
      last_run_status: "status" in row ? row.status : "succeeded",
      last_run_error: null,
      run_count: 1,
    })}`;
    if ("checked" in row)
      yield* sql`INSERT INTO fork_scheduled_task_checks (task_id, revision, state_json) VALUES (${row.id}, 1, '{}')`;
  }
});

const remaining = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const left = yield* sql<{
    task_id: string;
  }>`SELECT task_id FROM scheduled_tasks ORDER BY task_id`;
  return left.map((row) => row.task_id);
});

const expected = (key: "onCompletion" | "pruned") =>
  rows
    .filter((row) => !row[key])
    .map((row) => row.id)
    .toSorted();

it.effect("completion removes exactly the spent one-time tasks of every form", () =>
  Effect.gen(function* () {
    yield* seed;
    for (const row of rows) {
      const removed = yield* deleteSpentOnceTask(ScheduledTaskId.make(row.id), RAN);
      assert.equal(removed, row.onCompletion, row.id);
    }
    assert.deepEqual(yield* remaining, expected("onCompletion"));
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("the startup prune removes only enabled spent fork one-time tasks", () =>
  Effect.gen(function* () {
    yield* seed;
    yield* pruneSpentForkOnceTasks();
    assert.deepEqual(yield* remaining, expected("pruned"));
    yield* pruneSpentForkOnceTasks();
    assert.deepEqual(yield* remaining, expected("pruned"));
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);
