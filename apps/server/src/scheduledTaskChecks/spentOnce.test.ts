import { assert, it } from "@effect/vitest";
import { ScheduledTaskId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { deleteSpentOnceTask } from "./spentOnce.ts";
import { ensureCheckSchema } from "./store.ts";

const RAN = "2026-10-08T10:00:05.000Z";
const spentAt = "2026-10-08T10:00:00Z";
const at = (value: string) => JSON.stringify({ type: "once", at: value });
const wrap = (schedule: string) =>
  JSON.stringify({
    type: "destination-automation-v1",
    certificateId: "c",
    schedule: JSON.parse(schedule),
  });

// Each row: whether the completion of the run started at RAN removes it.
const rows: ReadonlyArray<{
  readonly id: string;
  readonly schedule: string;
  readonly status?: string;
  readonly lastRunAt?: string | null;
  readonly enabled?: number;
  readonly next?: string;
  readonly check?: "agent" | "command";
  readonly removed: boolean;
}> = [
  { id: "spent-at", schedule: at(spentAt), removed: true },
  { id: "spent-offset", schedule: at("2026-10-08T12:00:00+02:00"), removed: true },
  { id: "spent-wrapped", schedule: wrap(at(spentAt)), removed: true },
  {
    id: "spent-run-at",
    schedule: JSON.stringify({ type: "once", runAt: "2026-10-08T10:00:00.000Z" }),
    removed: true,
  },
  {
    id: "spent-wrapped-run-at",
    schedule: wrap(JSON.stringify({ type: "once", runAt: "2026-10-08T10:00:00.000Z" })),
    removed: true,
  },
  // Fired at its own millisecond; SQLite's julianday rounds the sub-millisecond time up.
  { id: "spent-at-boundary", schedule: at("2026-10-08T12:00:05.000999+02:00"), removed: true },
  // Upstream's existing rule: a paused task run at or after its time is spent too.
  { id: "paused", schedule: at(spentAt), enabled: 0, removed: true },
  { id: "failed", schedule: at(spentAt), status: "failed", removed: false },
  { id: "never-run", schedule: at(spentAt), status: "never", lastRunAt: null, removed: false },
  {
    id: "other-start",
    schedule: at(spentAt),
    lastRunAt: "2026-10-08T10:00:06.000Z",
    removed: false,
  },
  { id: "due", schedule: at(spentAt), next: "2026-10-09T10:00:00.000Z", removed: false },
  { id: "run-early", schedule: at("2026-10-08T11:00:00Z"), removed: false },
  ...(["agent", "command"] as const).map((check) => ({
    id: `checked-${check}`,
    schedule: at(spentAt),
    check,
    removed: false,
  })),
  // Shapes the server never writes are not one-time tasks.
  {
    id: "interval",
    schedule: JSON.stringify({
      type: "interval",
      everyMs: 60_000,
      schedule: JSON.parse(at(spentAt)),
    }),
    removed: false,
  },
  { id: "numeric-at", schedule: JSON.stringify({ type: "once", at: 2_461_321 }), removed: false },
  { id: "corrupt", schedule: "broken", removed: false },
];

it.effect("completion removes exactly the spent one-time tasks of every form", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* ensureCheckSchema;
    for (const row of rows) {
      yield* sql`INSERT INTO scheduled_tasks ${sql.insert({
        task_id: row.id,
        title: "task",
        prompt: "Run task",
        enabled: row.enabled ?? 1,
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
        next_run_at: row.next ?? null,
        last_run_at: row.lastRunAt === undefined ? RAN : row.lastRunAt,
        last_run_status: row.status ?? "succeeded",
        last_run_error: null,
        run_count: 1,
      })}`;
      if (row.check !== undefined)
        yield* sql`INSERT INTO fork_scheduled_task_checks (task_id, revision, state_json)
          VALUES (${row.id}, 1, ${JSON.stringify({ taskId: row.id, kind: row.check })})`;
    }
    for (const row of rows) {
      const removed = yield* deleteSpentOnceTask(ScheduledTaskId.make(row.id), RAN);
      assert.equal(removed, row.removed, row.id);
    }
    const left = yield* sql<{ task_id: string }>`SELECT task_id FROM scheduled_tasks`;
    assert.deepEqual(
      left.map((row) => row.task_id).toSorted(),
      rows
        .filter((row) => !row.removed)
        .map((row) => row.id)
        .toSorted(),
    );
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);
