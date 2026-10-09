import type { ScheduledTask, ScheduledTaskId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import { nextScheduledRunAt } from "../scheduledTasks/Schedule.ts";
import { CheckState } from "./state.ts";

export class CheckStateConflict extends Schema.TaggedError<CheckStateConflict>()(
  "CheckStateConflict",
  { taskId: Schema.String },
) {
  override get message() {
    return `Outcome check state for ${this.taskId} changed concurrently.`;
  }
}

const StateJson = Schema.fromJsonString(CheckState);
const decode = Schema.decodeUnknownEffect(StateJson);
const encode = Schema.encodeEffect(StateJson);

/** One fork row per checked task, keyed by upstream's `scheduled_tasks.task_id`. */
export const ensureCheckSchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    CREATE TABLE IF NOT EXISTS fork_scheduled_task_checks (
      task_id TEXT PRIMARY KEY,
      revision INTEGER NOT NULL,
      state_json TEXT NOT NULL
    )
  `;
});

export const readCheckState = Effect.fn("ScheduledTaskChecks.read")(function* (
  taskId: ScheduledTaskId,
) {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ state_json: string }>`
    SELECT state_json FROM fork_scheduled_task_checks WHERE task_id = ${taskId}
  `;
  return rows[0] === undefined ? null : yield* decode(rows[0].state_json);
});

export const listCheckStates = Effect.fn("ScheduledTaskChecks.list")(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ state_json: string }>`
    SELECT state_json FROM fork_scheduled_task_checks ORDER BY task_id
  `;
  return yield* Effect.forEach(rows, (row) => decode(row.state_json));
});

/** Compare-and-set on the stored revision; `previous` null inserts a new row. */
export const writeCheckState = Effect.fn("ScheduledTaskChecks.write")(function* (
  previous: CheckState | null,
  next: CheckState,
) {
  const sql = yield* SqlClient.SqlClient;
  const stored: CheckState = { ...next, revision: (previous?.revision ?? 0) + 1 };
  const json = yield* encode(stored);
  const written =
    previous === null
      ? yield* sql<{ task_id: string }>`
          INSERT INTO fork_scheduled_task_checks (task_id, revision, state_json)
          VALUES (${stored.taskId}, ${stored.revision}, ${json})
          ON CONFLICT (task_id) DO NOTHING
          RETURNING task_id
        `
      : yield* sql<{ task_id: string }>`
          UPDATE fork_scheduled_task_checks
          SET revision = ${stored.revision}, state_json = ${json}
          WHERE task_id = ${stored.taskId} AND revision = ${previous.revision}
          RETURNING task_id
        `;
  if (written.length !== 1) return yield* new CheckStateConflict({ taskId: stored.taskId });
  return stored;
});

export const deleteCheckState = Effect.fn("ScheduledTaskChecks.delete")(function* (
  taskId: ScheduledTaskId,
) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`DELETE FROM fork_scheduled_task_checks WHERE task_id = ${taskId}`;
});

/**
 * Consumes a scheduled slot without a run: moves upstream's next run on, but
 * only for the incarnation and slot this fire saw, so an edit or a delete and
 * recreate since then keeps its own next run.
 */
export const consumeSlot = Effect.fn("ScheduledTaskChecks.consumeSlot")(function* (
  task: ScheduledTask,
  now: DateTime.DateTime,
) {
  const sql = yield* SqlClient.SqlClient;
  let next: string | null = null;
  if (task.enabled) {
    try {
      const at = nextScheduledRunAt(task.schedule, now);
      next =
        at !== null && Number.isFinite(DateTime.toEpochMillis(at))
          ? DateTime.formatIso(DateTime.toUtc(at))
          : null;
    } catch {
      next = null;
    }
  }
  yield* sql`
    UPDATE scheduled_tasks
    SET next_run_at = ${next}, updated_at = ${DateTime.formatIso(DateTime.toUtc(now))}
    WHERE task_id = ${task.id}
      AND created_at = ${task.createdAt}
      AND next_run_at IS ${task.nextRunAt}
  `;
});

/** Whether upstream's row still matches the snapshot a fire read, so it is not acted on stale. */
export const isCurrentTask = Effect.fn("ScheduledTaskChecks.isCurrentTask")(function* (
  task: Pick<ScheduledTask, "id" | "createdAt" | "updatedAt">,
) {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ readonly task_id: string }>`
    SELECT task_id FROM scheduled_tasks
    WHERE task_id = ${task.id} AND created_at = ${task.createdAt} AND updated_at = ${task.updatedAt}
  `;
  return rows.length === 1;
});
