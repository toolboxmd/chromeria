import { assert, it } from "@effect/vitest";
import { CommandId, MessageId, ScheduledTaskId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { listTaskRuns } from "./runs.ts";
import type { CheckedRun, CheckState } from "./state.ts";
import { ensureCheckSchema, writeCheckState } from "./store.ts";

const taskId = ScheduledTaskId.make("scheduled-task:daily");

const insertThread = (
  id: string,
  payload: { readonly settledOverride: string | null; readonly archived?: boolean },
  deletedAt: string | null = null,
) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`
      INSERT INTO orchestration_v2_projection_threads (
        thread_id, project_id, title, default_provider, runtime_mode, interaction_mode,
        created_at, updated_at, archived_at, deleted_at, payload_json
      ) VALUES (
        ${id}, 'project', 'Thread', 'codex', 'full-access', 'default',
        '2026-11-01T00:00:00.000Z', '2026-11-01T00:00:00.000Z',
        ${payload.archived ? "2026-11-23T00:00:00.000Z" : null}, ${deletedAt},
        ${JSON.stringify(payload)}
      )
    `;
  });

const insertTask = (createdAt: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`
      INSERT INTO scheduled_tasks (
        task_id, title, prompt, enabled, schedule_json, project_id, workspace_strategy_json,
        model_selection_json, runtime_mode, interaction_mode, created_by, creation_source,
        created_at, updated_at, last_run_status, run_count
      ) VALUES (
        ${taskId}, 'Daily', 'Do the work', 1, '{}', 'project', '{}', '{}', 'full-access',
        'default', 'user', 'web', ${createdAt}, ${createdAt}, 'succeeded', 1
      )
    `;
  });

let ordinal = 0;
/** One scheduled send: its user message, the provider run it started and that run's replies. */
const insertSend = (input: {
  readonly messageId: string;
  readonly threadId: string;
  readonly task: string;
  readonly status: string;
  readonly requestedAt: string;
  /** Defaults to `requestedAt`; null is a run that never started. */
  readonly startedAt?: string | null;
  readonly completedAt: string | null;
  readonly replies?: ReadonlyArray<string>;
}) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const runId = `run:${input.messageId}`;
    ordinal += 1;
    yield* sql`
      INSERT INTO orchestration_v2_projection_runs (
        run_id, thread_id, ordinal, provider, status, requested_at, completed_at, payload_json
      ) VALUES (
        ${runId}, ${input.threadId}, ${ordinal}, 'codex', ${input.status}, ${input.requestedAt},
        ${input.completedAt},
        ${JSON.stringify({ startedAt: input.startedAt === undefined ? input.requestedAt : input.startedAt })}
      )
    `;
    const message = (id: string, role: string, at: string, payload: object) => sql`
      INSERT INTO orchestration_v2_projection_messages (
        message_id, thread_id, run_id, role, streaming, created_at, updated_at, payload_json
      ) VALUES (${id}, ${input.threadId}, ${runId}, ${role}, 0, ${at}, ${at}, ${JSON.stringify(payload)})
    `;
    yield* message(input.messageId, "user", input.requestedAt, {
      text: "Do the work",
      scheduledTaskId: input.task,
    });
    for (const [index, text] of (input.replies ?? []).entries())
      yield* message(
        `${input.messageId}:reply:${index}`,
        "assistant",
        `${input.requestedAt.slice(0, -2)}${index + 1}Z`,
        { text },
      );
  });

it.effect("lists a plain task's runs from the messages the scheduler sent for it", () =>
  Effect.gen(function* () {
    yield* insertTask("2026-11-20T12:00:00.000Z");
    yield* insertThread("thread:open", { settledOverride: null });
    // A task deleted and created again under the same id: the old one's run is not this one's.
    yield* insertSend({
      messageId: `scheduled-task-message:${taskId}:0:scheduled`,
      threadId: "thread:open",
      task: taskId,
      status: "completed",
      requestedAt: "2026-11-19T08:00:00.000Z",
      completedAt: "2026-11-19T08:01:00.000Z",
    });
    yield* insertThread("thread:settled", { settledOverride: "settled" });
    yield* insertThread("thread:deleted", { settledOverride: null }, "2026-11-20T00:00:00.000Z");
    yield* insertSend({
      messageId: `scheduled-task-message:${taskId}:1:scheduled`,
      threadId: "thread:deleted",
      task: taskId,
      status: "failed",
      // Failed while still queued: the wait is not a duration.
      requestedAt: "2026-11-21T08:00:00.000Z",
      startedAt: null,
      completedAt: "2026-11-21T08:00:05.000Z",
    });
    yield* insertSend({
      messageId: `scheduled-task-message:${taskId}:2:scheduled`,
      threadId: "thread:settled",
      task: taskId,
      status: "completed",
      // Queued behind a busy thread for a minute: the wait is not run time.
      requestedAt: "2026-11-22T08:00:00.000Z",
      startedAt: "2026-11-22T08:01:00.000Z",
      completedAt: "2026-11-22T08:02:00.000Z",
      replies: ["Looking.", "## Done\n\nNothing **new** upstream."],
    });
    // A keyed manual run's message id names its request, not its task.
    yield* insertSend({
      messageId: "scheduled-task-message:manual:abc",
      threadId: "thread:open",
      task: taskId,
      status: "running",
      requestedAt: "2026-11-23T08:00:00.000Z",
      completedAt: null,
    });
    // Another task whose id starts with this one's must not leak in.
    yield* insertSend({
      messageId: `scheduled-task-message:${taskId}:other:1:scheduled`,
      threadId: "thread:open",
      task: `${taskId}:other`,
      status: "completed",
      requestedAt: "2026-11-24T08:00:00.000Z",
      completedAt: "2026-11-24T08:01:00.000Z",
    });

    assert.deepStrictEqual((yield* listTaskRuns(taskId)).runs, [
      {
        id: "scheduled-task-message:manual:abc",
        startedAt: "2026-11-23T08:00:00.000Z",
        endedAt: null,
        status: "running",
        thread: { id: ThreadId.make("thread:open"), state: "open" },
        summary: null,
      },
      {
        id: `scheduled-task-message:${taskId}:2:scheduled`,
        startedAt: "2026-11-22T08:01:00.000Z",
        endedAt: "2026-11-22T08:02:00.000Z",
        status: "succeeded",
        thread: { id: ThreadId.make("thread:settled"), state: "settled" },
        summary: "Done Nothing new upstream.",
      },
      {
        id: `scheduled-task-message:${taskId}:1:scheduled`,
        startedAt: "2026-11-21T08:00:00.000Z",
        endedAt: null,
        status: "failed",
        thread: { id: ThreadId.make("thread:deleted"), state: "missing" },
        summary: null,
      },
    ]);
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

const run = (overrides: Partial<CheckedRun>): CheckedRun => ({
  id: "run",
  slot: "2026-11-22T08:00:00.000Z",
  checkVersion: null,
  threadId: null,
  checkCwd: null,
  stage: "done",
  attempt: 0,
  retryAt: null,
  hasWork: true,
  sends: [],
  error: null,
  check: null,
  ...overrides,
});

const state = (overrides: Partial<CheckState>): CheckState => ({
  version: 1,
  taskId,
  revision: 0,
  kind: "agent",
  command: null,
  role: null,
  lane: null,
  checks: [],
  runs: [],
  failureStreak: 0,
  lastError: null,
  lastSuccessfulRunId: null,
  ...overrides,
});

it.effect("lists a checked task's kept runs with the thread and reply of each", () =>
  Effect.gen(function* () {
    yield* ensureCheckSchema;
    yield* insertTask("2026-11-20T12:00:00.000Z");
    // Archived without being settled: out of the user's way all the same.
    yield* insertThread("thread:run", { settledOverride: null, archived: true });
    const messageId = "scheduled-task-message:checked:continue";
    yield* insertSend({
      messageId: "scheduled-task-message:checked:start",
      threadId: "thread:run",
      task: taskId,
      status: "completed",
      requestedAt: "2026-11-22T08:00:03.000Z",
      completedAt: "2026-11-22T08:01:00.000Z",
      replies: ["The check passes now."],
    });
    // The last send got no reply: the run's newest reply is the earlier send's.
    yield* insertSend({
      messageId,
      threadId: "thread:run",
      task: taskId,
      status: "failed",
      requestedAt: "2026-11-22T08:05:00.000Z",
      completedAt: "2026-11-22T08:06:00.000Z",
    });
    yield* writeCheckState(
      null,
      state({
        runs: [
          run({
            id: "passed",
            threadId: ThreadId.make("thread:run"),
            sends: [
              {
                index: 0,
                commandId: CommandId.make("command:start"),
                messageId: MessageId.make("scheduled-task-message:checked:start"),
                kind: "start",
                createdAt: "2026-11-22T08:00:03.000Z",
              },
              {
                index: 1,
                commandId: CommandId.make("command:continue"),
                messageId: MessageId.make(messageId),
                kind: "continue",
                createdAt: "2026-11-22T08:05:00.000Z",
              },
            ],
            // A reply outranks an error as the summary: the badge already says it failed.
            error: "An earlier check failed.",
            check: { version: 1, passed: true, output: "", checkedAt: "2026-11-22T08:06:10.000Z" },
          }),
          run({
            id: "stuck",
            slot: "2026-11-23T08:00:00.000Z",
            stage: "needs-you",
            error: "No model.",
          }),
          run({ id: "retrying", slot: "2026-11-24T08:00:00.000Z", stage: "retry" }),
        ],
      }),
    );

    assert.deepStrictEqual((yield* listTaskRuns(taskId)).runs, [
      {
        id: "retrying",
        startedAt: "2026-11-24T08:00:00.000Z",
        endedAt: null,
        status: "running",
        thread: null,
        summary: null,
      },
      {
        id: "stuck",
        startedAt: "2026-11-23T08:00:00.000Z",
        endedAt: null,
        status: "failed",
        thread: null,
        summary: "No model.",
      },
      {
        id: "passed",
        startedAt: "2026-11-22T08:00:03.000Z",
        endedAt: "2026-11-22T08:06:10.000Z",
        status: "succeeded",
        thread: { id: ThreadId.make("thread:run"), state: "settled" },
        summary: "The check passes now.",
      },
    ]);
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("lists a command task's runs without a thread, with what each reported", () =>
  Effect.gen(function* () {
    yield* ensureCheckSchema;
    yield* insertTask("2026-11-20T12:00:00.000Z");
    yield* writeCheckState(
      null,
      state({
        kind: "command",
        command: "make check",
        runs: [
          run({
            id: "failed",
            stage: "needs-you",
            error: "exited with 2",
            commandResult: { exitCode: 2, timedOut: false, endedAt: "2026-11-22T08:00:09.000Z" },
          }),
          run({
            id: "passed",
            slot: "2026-11-23T08:00:00.000Z",
            commandResult: {
              exitCode: 0,
              timedOut: false,
              endedAt: "2026-11-23T08:00:04.000Z",
              output: "building\nall good\n",
            },
          }),
        ],
      }),
    );

    assert.deepStrictEqual((yield* listTaskRuns(taskId)).runs, [
      {
        id: "passed",
        startedAt: "2026-11-23T08:00:00.000Z",
        // Only the slot is recorded, so a catch-up hours later must not read as run time.
        endedAt: null,
        status: "succeeded",
        thread: null,
        summary: "all good",
      },
      {
        id: "failed",
        startedAt: "2026-11-22T08:00:00.000Z",
        endedAt: null,
        status: "failed",
        thread: null,
        summary: "exited with 2",
      },
    ]);
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);
