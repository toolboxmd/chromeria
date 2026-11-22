import { isForkScheduledTaskSchedule, ScheduledTask, ScheduledTaskId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import { forkFireKey } from "./schedules.ts";
import { ensureCheckSchema } from "./store.ts";

const encodeIdsJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Array(Schema.String)));
// Strings always encode; a failure here is a defect.
const encodeIds = (ids: ReadonlyArray<string>) => encodeIdsJson(ids).pipe(Effect.orDie);
const decodeTaskId = Schema.decodeUnknownOption(ScheduledTaskId);

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

/** The one-time schedule stored in `json`, when the run started at `lastRunAt` used it up. */
const spentOnce = (json: string, lastRunAt: string) => {
  const stored = Option.getOrUndefined(decodeStored(json));
  if (stored === undefined) return undefined;
  const schedule = stored.type === "destination-automation-v1" ? stored.schedule : stored;
  if (schedule.type !== "once") return undefined;
  const at = Date.parse("at" in schedule ? schedule.at : schedule.runAt);
  return Date.parse(lastRunAt) >= at ? schedule : undefined;
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
  const [row] = yield* sql<{ schedule_json: string }>`
    SELECT schedule_json FROM scheduled_tasks
    WHERE task_id = ${taskId} AND last_run_at = ${startedAtIso}
      AND last_run_status = 'succeeded' AND next_run_at IS NULL
  `;
  if (row === undefined || spentOnce(row.schedule_json, startedAtIso) === undefined) return false;
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

interface Candidate {
  readonly task_id: string;
  readonly schedule_json: string;
  readonly last_run_at: string;
}

const MESSAGE_PREFIX = "scheduled-task-message:";
// What follows `<task id>:` in a fire's message id; a longer task id that shares the prefix leaves
// a remainder that matches neither form.
const FIRE_SUFFIX =
  /^(?:(?:once|weekly|monthly):\d{4}-\d{2}-\d{2}T[\d:.]+Z|\d+:(?:scheduled|manual))$/;

/** Whether `messageId` is one `runTask` gave a fire of `taskId`, under any slot. */
const isTaskFireMessage = (messageId: string, taskId: string) => {
  const prefix = `${MESSAGE_PREFIX}${taskId}:`;
  return messageId.startsWith(prefix) && FIRE_SUFFIX.test(messageId.slice(prefix.length));
};

/** The local Run Now id of the fire that started at `lastRunAt`, unique to that start. */
const manualMessageId = (row: Candidate, task: Parameters<typeof forkFireKey>[0]) => {
  const startedAt = DateTime.makeUnsafe(row.last_run_at);
  return `${MESSAGE_PREFIX}${
    forkFireKey(task, "manual", startedAt) ??
    `${row.task_id}:${DateTime.toEpochMillis(startedAt)}:manual`
  }`;
};

/**
 * Fork `at` tasks were once kept after completion; this clears the enabled ones already spent whose
 * last fire's run completed. A dispatch alone proves nothing: its turn can still fail. Failed,
 * never-run, paused, still-due and checked tasks stay.
 */
export const pruneSpentForkOnceTasks = Effect.fn("ScheduledTaskChecks.pruneSpentForkOnceTasks")(
  function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* ensureCheckSchema;
    const rows = yield* sql<Candidate>`
      SELECT task_id, schedule_json, last_run_at FROM scheduled_tasks AS tasks
      WHERE enabled = 1 AND last_run_status = 'succeeded' AND next_run_at IS NULL
        AND last_run_at IS NOT NULL AND schedule_json LIKE '%"once"%'
        AND NOT EXISTS (
          SELECT 1 FROM fork_scheduled_task_checks AS checks WHERE checks.task_id = tasks.task_id
        )
    `;
    const spentRows = rows.flatMap((row) => {
      const schedule = spentOnce(row.schedule_json, row.last_run_at);
      const id = Option.getOrUndefined(decodeTaskId(row.task_id));
      return id !== undefined && schedule !== undefined && isForkScheduledTaskSchedule(schedule)
        ? [{ row, task: { id, schedule, nextRunAt: null } }]
        : [];
    });
    if (spentRows.length === 0) return;
    // One JSON parameter, so a pile of any size stays under SQLite's variable limit.
    const taskIds = yield* encodeIds(spentRows.map(({ row }) => row.task_id));
    const keyed = yield* sql<{ task_id: string; request_id: string; started_at: string }>`
      SELECT task_id, request_id, started_at FROM scheduled_manual_requests
      WHERE task_id IN (SELECT value FROM json_each(${taskIds}))
    `;
    const keyedByTask = Map.groupBy(keyed, (request) => request.task_id);
    const runs = yield* sql<{ message_id: string; status: string; requested_at: string }>`
      SELECT message_id, status, requested_at FROM (
        SELECT json_extract(payload_json, '$.userMessageId') AS message_id, status, requested_at
        FROM orchestration_v2_projection_runs
        WHERE json_valid(payload_json)
      )
      WHERE message_id LIKE 'scheduled-task-message:%'
    `;
    const runsByMessage = Map.groupBy(runs, (run) => run.message_id);
    // Which fire a run belonged to cannot be told apart in every history (requeues, edits, clock
    // steps, restarts), so every run of the task's fires, under any slot, must have completed. The
    // last fire's own run must also be there: under its Run Now id, which is unique to the start, or
    // any run requested from the start on. Any other history keeps the task.
    const spent = spentRows.filter(({ row, task }) => {
      const requests = keyedByTask.get(row.task_id) ?? [];
      const atStart = requests.filter((request) => request.started_at === row.last_run_at);
      // Two keyed requests at one start leave the fire unknown even when one has finished.
      if (atStart.length > 1) return false;
      const ownIds = new Set([
        manualMessageId(row, task),
        ...atStart.map((request) => `${MESSAGE_PREFIX}manual:${request.request_id}`),
      ]);
      const keyedIds = new Set(
        requests.map((request) => `${MESSAGE_PREFIX}manual:${request.request_id}`),
      );
      const taskRuns = [...runsByMessage].flatMap(([messageId, messageRuns]) =>
        keyedIds.has(messageId) || isTaskFireMessage(messageId, row.task_id)
          ? messageRuns.map((run) => ({ ...run, own: ownIds.has(messageId) }))
          : [],
      );
      return (
        taskRuns.every((run) => run.status === "completed") &&
        taskRuns.some(
          (run) => run.own || Date.parse(run.requested_at) >= Date.parse(row.last_run_at),
        )
      );
    });
    const removed = yield* sql.withTransaction(
      Effect.forEach(spent, ({ row }) =>
        sql`
          DELETE FROM scheduled_tasks
          WHERE task_id = ${row.task_id} AND schedule_json = ${row.schedule_json}
            AND last_run_at = ${row.last_run_at} AND enabled = 1
            AND last_run_status = 'succeeded' AND next_run_at IS NULL
            AND NOT EXISTS (SELECT 1 FROM fork_scheduled_task_checks WHERE task_id = ${row.task_id})
          RETURNING task_id
        `.pipe(Effect.map((deleted) => deleted.length)),
      ),
    );
    yield* Effect.logInfo("Removed spent one-time schedule tasks", {
      removed: removed.reduce((sum, count) => sum + count, 0),
      kept: spentRows.length - spent.length,
    });
  },
);
