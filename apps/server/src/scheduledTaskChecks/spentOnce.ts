import { ScheduledTask } from "@t3tools/contracts";
import type { ScheduledTaskId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import { ensureCheckSchema } from "./store.ts";

// The stored forms upstream's row decoder accepts: a schedule, or one wrapped by accepted automation.
const decodeStored = Schema.decodeUnknownOption(
  Schema.fromJsonString(
    Schema.Union([
      ScheduledTask.fields.schedule,
      Schema.Struct({
        type: Schema.Literal("destination-automation-v1"),
        certificateId: Schema.String,
        schedule: ScheduledTask.fields.schedule,
      }),
    ]),
  ),
);

/** Whether `json` holds a one-time schedule that the run started at `lastRunAt` used up. */
const spentOnce = (json: string, lastRunAt: string) => {
  const stored = Option.getOrUndefined(decodeStored(json));
  if (stored === undefined) return false;
  const schedule = stored.type === "destination-automation-v1" ? stored.schedule : stored;
  if (schedule.type !== "once") return false;
  const at = Date.parse("at" in schedule ? schedule.at : schedule.runAt);
  return Date.parse(lastRunAt) >= at;
};

/**
 * Removes a one-time task, upstream's `runAt` or the fork's `at`, once the run that `startedAtIso`
 * dispatched completed. Outcome-checked and command tasks are kept: their check state owns the outcome.
 */
export const deleteSpentOnceTask = Effect.fn("ScheduledTaskChecks.deleteSpentOnceTask")(function* (
  taskId: ScheduledTaskId,
  startedAtIso: string,
) {
  const sql = yield* SqlClient.SqlClient;
  yield* ensureCheckSchema;
  const [row] = yield* sql<{ schedule_json: string }>`
    SELECT schedule_json FROM scheduled_tasks
    WHERE task_id = ${taskId} AND last_run_at = ${startedAtIso}
      AND last_run_status = 'succeeded' AND next_run_at IS NULL
  `;
  if (row === undefined || !spentOnce(row.schedule_json, startedAtIso)) return false;
  const removed = yield* sql`
    DELETE FROM scheduled_tasks
    WHERE task_id = ${taskId} AND last_run_at = ${startedAtIso}
      AND last_run_status = 'succeeded' AND next_run_at IS NULL
      AND schedule_json = ${row.schedule_json}
      AND NOT EXISTS (SELECT 1 FROM fork_scheduled_task_checks WHERE task_id = ${taskId})
    RETURNING task_id
  `;
  return removed.length > 0;
});
