import {
  SCHEDULED_TASK_RUNS_LIMIT,
  ScheduledTaskError,
  ThreadId,
  type MessageId,
  type ScheduledTaskId,
  type ScheduledTaskListRunsResult,
  type ScheduledTaskRunRecord,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

import { conversationPreviewText } from "../promachos/conversationPreview.ts";
import type { CheckedRun, CheckState } from "./state.ts";
import { readCheckState } from "./store.ts";

/** A run before its thread and summary are read. */
interface RunDraft extends Omit<ScheduledTaskRunRecord, "thread" | "summary"> {
  readonly threadId: ThreadId | null;
  /** The message whose provider run wrote the summary. */
  readonly messageId: MessageId | null;
  /** Shown when that run left no reply: a command's report, or a run's error. */
  readonly fallback: string | null;
}

const lastLine = (text: string | undefined) => text?.trimEnd().split("\n").at(-1)?.trim() || null;

function checkedRunStatus(state: CheckState, run: CheckedRun): ScheduledTaskRunRecord["status"] {
  if (run.imported !== undefined) return "failed";
  if (run.stage === "done") return "succeeded";
  if (run.stage === "needs-you") return "failed";
  // A command run that is not running ended; only `done` passed.
  return state.kind === "command" && run.stage !== "running" ? "failed" : "running";
}

/** The kept runs of a checked or command task, newest first. */
function checkedRunDrafts(state: CheckState): ReadonlyArray<RunDraft> {
  return state.runs
    .slice(-SCHEDULED_TASK_RUNS_LIMIT)
    .toReversed()
    .map((run) => {
      const status = checkedRunStatus(state, run);
      return {
        id: run.id,
        startedAt: run.sends[0]?.createdAt ?? run.slot,
        // A command run records its slot, not when a catch-up actually started it, so its
        // end would turn scheduler downtime into run time.
        endedAt:
          status === "running" || run.sends.length === 0 ? null : (run.check?.checkedAt ?? null),
        status,
        threadId: run.threadId,
        messageId: run.sends.at(-1)?.messageId ?? null,
        fallback:
          state.kind === "command" ? (run.error ?? lastLine(run.commandResult?.output)) : run.error,
      };
    });
}

const LIVE_RUN_STATUSES = new Set(["queued", "preparing", "starting", "running", "waiting"]);

/**
 * A task without fork state keeps no run records, so its runs are the messages the scheduler
 * sent for it: their ids start with the task's id, and each names the provider run it started.
 */
const messageRunDrafts = Effect.fn("ScheduledTaskRuns.messageRunDrafts")(function* (
  taskId: ScheduledTaskId,
  createdAt: string,
) {
  const sql = yield* SqlClient.SqlClient;
  const prefix = `scheduled-task-message:${taskId}:`;
  // `;` follows `:`, so the range is every id with the prefix, read through the primary key.
  const end = `${prefix.slice(0, -1)};`;
  const rows = yield* sql<{
    readonly message_id: string;
    readonly thread_id: string;
    readonly status: string;
    readonly started_at: string;
    readonly completed_at: string | null;
  }>`
    SELECT message.message_id, message.thread_id, run.status, run.completed_at,
      -- A prompt queued behind a busy thread waits before it starts; the wait is not run time.
      coalesce(json_extract(run.payload_json, '$.startedAt'), run.requested_at) AS started_at
    FROM orchestration_v2_projection_messages message
    JOIN orchestration_v2_projection_runs run ON run.run_id = message.run_id
    WHERE (
        (message.message_id >= ${prefix} AND message.message_id < ${end})
        -- A keyed manual run's id holds its request, not its task.
        OR (
          message.message_id >= 'scheduled-task-message:manual:'
          AND message.message_id < 'scheduled-task-message:manual;'
        )
      )
      AND json_extract(message.payload_json, '$.scheduledTaskId') = ${taskId}
      AND run.status <> 'rolled_back'
      -- A task deleted and created again under the same id starts without the old one's runs.
      AND julianday(run.requested_at) >= julianday(${createdAt})
    ORDER BY run.requested_at DESC, message.message_id DESC
    LIMIT ${SCHEDULED_TASK_RUNS_LIMIT}
  `;
  return rows.map((row): RunDraft => ({
    id: row.message_id,
    startedAt: row.started_at,
    endedAt: row.completed_at,
    status: LIVE_RUN_STATUSES.has(row.status)
      ? "running"
      : row.status === "completed"
        ? "succeeded"
        : "failed",
    threadId: ThreadId.make(row.thread_id),
    messageId: row.message_id as MessageId,
    fallback: null,
  }));
});

const hasCheckStates = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql`
    SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'fork_scheduled_task_checks'
  `;
  return rows.length > 0;
});

/**
 * A task's newest runs with each thread's state and a one-line summary. Read on demand: the
 * task list every client subscribes to carries no runs.
 */
export const listTaskRuns = Effect.fn("ScheduledTaskRuns.list")(
  function* (taskId: ScheduledTaskId) {
    const sql = yield* SqlClient.SqlClient;
    const [task] = yield* sql<{ readonly created_at: string }>`
      SELECT created_at FROM scheduled_tasks WHERE task_id = ${taskId}
    `;
    if (task === undefined) return { runs: [] } satisfies ScheduledTaskListRunsResult;
    const state = (yield* hasCheckStates) ? yield* readCheckState(taskId) : null;
    const drafts =
      state === null ? yield* messageRunDrafts(taskId, task.created_at) : checkedRunDrafts(state);
    const threadIds = [...new Set(drafts.flatMap((run) => run.threadId ?? []))];
    const threads =
      threadIds.length === 0
        ? []
        : yield* sql<{
            readonly thread_id: string;
            readonly deleted_at: string | null;
            readonly archived_at: string | null;
            readonly settled: string | null;
          }>`
            SELECT thread_id, deleted_at, archived_at, json_extract(payload_json, '$.settledOverride') AS settled
            FROM orchestration_v2_projection_threads
            WHERE thread_id IN ${sql.in(threadIds)}
          `;
    const threadStates = new Map(
      threads.map((row) => [
        row.thread_id,
        row.deleted_at !== null
          ? ("missing" as const)
          : // An archived thread is out of the user's way, like a settled one.
            row.settled === "settled" || row.archived_at !== null
            ? ("settled" as const)
            : ("open" as const),
      ]),
    );
    const messageIds = drafts.flatMap((run) => run.messageId ?? []);
    // The newest finished assistant message of the provider run each message started.
    const replies =
      messageIds.length === 0
        ? []
        : yield* sql<{ readonly message_id: string; readonly text: string | null }>`
            SELECT sent.message_id, (
              SELECT substr(json_extract(reply.payload_json, '$.text'), 1, 1000)
              FROM orchestration_v2_projection_messages reply
              WHERE reply.run_id = sent.run_id AND reply.role = 'assistant' AND reply.streaming = 0
              ORDER BY reply.created_at DESC, reply.message_id DESC
              LIMIT 1
            ) AS text
            FROM orchestration_v2_projection_messages sent
            WHERE sent.message_id IN ${sql.in(messageIds)}
          `;
    const summaries = new Map(
      replies.map((row) => [
        row.message_id,
        row.text === null ? null : conversationPreviewText(row.text),
      ]),
    );
    const result: ScheduledTaskListRunsResult = {
      runs: drafts.map(({ threadId, messageId, fallback, ...run }) => ({
        ...run,
        thread:
          threadId === null
            ? null
            : { id: threadId, state: threadStates.get(threadId) ?? "missing" },
        summary: (messageId === null ? null : (summaries.get(messageId) ?? null)) ?? fallback,
      })),
    };
    return result;
  },
  (effect, taskId) =>
    effect.pipe(
      Effect.mapError(
        (cause) =>
          new ScheduledTaskError({ message: "Could not read the task's runs.", taskId, cause }),
      ),
    ),
);
