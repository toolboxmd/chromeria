import { isCommandTask, ScheduledTask, type OrchestrationReadModel } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Struct from "effect/Struct";
import type * as SqlClient from "effect/unstable/sql/SqlClient";

/**
 * Command tasks keep their latest full state, runs and output tails included, in one fork-owned
 * `fork_scheduler_task_state` row per task, deleted tasks too. Routine admissions, results and
 * health write only that row. Configuration changes also append a `scheduler.state-set` event
 * without runs as their audit. The engine validates every write with the decider and commits the
 * row, any event and the command receipt in one transaction. Agent tasks keep full events and no
 * row.
 */
const TaskJson = Schema.fromJsonString(ScheduledTask);
const decodeTask = Schema.decodeUnknownEffect(TaskJson);
const encodeTask = Schema.encodeSync(TaskJson);
const sameConfig = Schema.toEquivalence(
  ScheduledTask.mapFields(
    Struct.omit([
      "revision",
      "updatedAt",
      "consumedSlot",
      "runs",
      "failureStreak",
      "lastError",
      "lastSuccessfulRunId",
    ]),
  ),
);

/** A command task write that changes no configuration. It appends no event. */
export const isRuntimeWrite = (previous: ScheduledTask | undefined, next: ScheduledTask) =>
  previous !== undefined && isCommandTask(next.definition) && sameConfig(previous, next);

/** What a task's state event records: a command task's audit leaves its runs to the row. */
export const auditedTask = (task: ScheduledTask): ScheduledTask =>
  isCommandTask(task.definition) ? { ...task, runs: [] } : task;

/** The revision a write must expect: the newer of the task's latest event and its row. */
export const durableRevision = (sql: SqlClient.SqlClient, taskId: string) =>
  sql<{ revision: number | null }>`
    SELECT MAX(revision) AS revision FROM (
      SELECT * FROM (
        SELECT json_extract(payload_json, '$.revision') AS revision FROM orchestration_events
        WHERE event_type = 'scheduler.state-set' AND stream_id = ${taskId}
        ORDER BY sequence DESC LIMIT 1
      )
      UNION ALL
      SELECT revision FROM fork_scheduler_task_state WHERE task_id = ${taskId}
    )
  `.pipe(Effect.map((rows) => rows[0]?.revision ?? 0));

/** Inside the engine's transaction: stores a command task's row and returns the read model. */
export const storeTaskState = (
  sql: SqlClient.SqlClient,
  model: OrchestrationReadModel,
  task: ScheduledTask,
) =>
  isCommandTask(task.definition)
    ? sql`
        INSERT INTO fork_scheduler_task_state (task_id, revision, state_json)
        VALUES (${task.id}, ${task.revision}, ${encodeTask(task)})
        ON CONFLICT (task_id) DO UPDATE SET
          revision = excluded.revision,
          state_json = excluded.state_json
      `.pipe(
        Effect.as({
          ...model,
          scheduledTasks: [
            ...(model.scheduledTasks ?? []).filter((entry) => entry.id !== task.id),
            task,
          ],
        }),
      )
    : Effect.succeed(model);

/**
 * Every task's latest state at startup. A row wins a tie, since the audit event written with it
 * has no runs. An event newer than the row, written by a build without rows, wins.
 */
export const loadScheduledTasks = Effect.fnUntraced(function* (sql: SqlClient.SqlClient) {
  yield* sql`
    CREATE TABLE IF NOT EXISTS fork_scheduler_task_state (
      task_id TEXT PRIMARY KEY,
      revision INTEGER NOT NULL,
      state_json TEXT NOT NULL
    )
  `;
  const events = yield* sql<{ payload: string }>`
    SELECT payload_json AS payload FROM orchestration_events
    WHERE event_type = 'scheduler.state-set' AND sequence IN (
      SELECT MAX(sequence) FROM orchestration_events WHERE event_type = 'scheduler.state-set' GROUP BY stream_id
    )
  `;
  const rows = yield* sql<{ payload: string }>`
    SELECT state_json AS payload FROM fork_scheduler_task_state
  `;
  const latest = new Map<string, ScheduledTask>();
  for (const event of events) {
    const task = yield* decodeTask(event.payload);
    latest.set(task.id, task);
  }
  for (const row of rows) {
    const task = yield* decodeTask(row.payload);
    if (task.revision >= (latest.get(task.id)?.revision ?? 0)) latest.set(task.id, task);
  }
  return [...latest.values()];
});
