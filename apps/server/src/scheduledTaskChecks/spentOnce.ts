import type { ScheduledTaskId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

import { ensureCheckSchema } from "./store.ts";

// The schedule, unwrapped from an accepted automation stamp when it carries one. A corrupt row
// reads as NULL: json_extract throws on malformed JSON.
const json = "CASE WHEN json_valid(schedule_json) THEN schedule_json END";
const scheduleField = (field: string) =>
  `COALESCE(json_extract(${json}, '$.schedule.${field}'), json_extract(${json}, '$.${field}'))`;

/**
 * A one-time task whose last dispatch succeeded at or after its time, upstream's `runAt` or the
 * fork's `at`. Outcome-checked and command tasks are kept: their check state owns the outcome.
 */
const SPENT_ONCE = `
  last_run_status = 'succeeded' AND next_run_at IS NULL
  AND ${scheduleField("type")} = 'once'
  AND julianday(last_run_at) >= julianday(COALESCE(${scheduleField("runAt")}, ${scheduleField("at")}))
  AND NOT EXISTS (
    SELECT 1 FROM fork_scheduled_task_checks AS checks
    WHERE checks.task_id = scheduled_tasks.task_id
  )
`;

/** Removes a one-time task once the run that `startedAtIso` dispatched completed. */
export const deleteSpentOnceTask = Effect.fn("ScheduledTaskChecks.deleteSpentOnceTask")(function* (
  taskId: ScheduledTaskId,
  startedAtIso: string,
) {
  const sql = yield* SqlClient.SqlClient;
  const removed = yield* sql`
    DELETE FROM scheduled_tasks
    WHERE task_id = ${taskId} AND last_run_at = ${startedAtIso} AND ${sql.literal(SPENT_ONCE)}
    RETURNING task_id
  `;
  return removed.length > 0;
});

/**
 * Fork `at` tasks were once kept after completion; this clears the
 * enabled ones already spent. Paused, failed, never-run and still-due tasks stay.
 */
export const pruneSpentForkOnceTasks = Effect.fn("ScheduledTaskChecks.pruneSpentForkOnceTasks")(
  function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* ensureCheckSchema;
    const removed = yield* sql`
      DELETE FROM scheduled_tasks
      WHERE enabled = 1 AND ${sql.literal(`${scheduleField("at")} IS NOT NULL`)}
        AND ${sql.literal(SPENT_ONCE)}
      RETURNING task_id
    `;
    if (removed.length > 0)
      yield* Effect.logInfo("Removed spent one-time schedule tasks", { removed: removed.length });
  },
);
