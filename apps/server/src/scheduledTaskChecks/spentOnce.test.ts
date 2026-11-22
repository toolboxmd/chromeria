import { assert, it } from "@effect/vitest";
import { ScheduledTaskId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
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
// `runTask`'s message id for a scheduled fork `once` fire and for a manual fire at RAN.
const scheduledMessage = (id: string, instant: string) =>
  `scheduled-task-message:${id}:once:${DateTime.formatIso(DateTime.makeUnsafe(instant))}`;
const manualMessage = (id: string) => `scheduled-task-message:${id}:${Date.parse(RAN)}:manual`;

interface Row {
  readonly id: string;
  readonly schedule: string;
  readonly status?: string;
  readonly lastRunAt?: string | null;
  readonly enabled?: number;
  readonly next?: string;
  readonly check?: "agent" | "command";
  /** The fire's run in the thread projection, when it reached one. */
  readonly run?: { readonly messageId: string; readonly status: string };
  readonly onCompletion: boolean;
  readonly pruned: boolean;
}

const spentAt = "2026-10-08T10:00:00Z";
const rows: ReadonlyArray<Row> = [
  {
    id: "spent-at",
    schedule: at(spentAt),
    run: { messageId: scheduledMessage("spent-at", spentAt), status: "completed" },
    onCompletion: true,
    pruned: true,
  },
  {
    id: "spent-offset",
    schedule: at("2026-10-08T12:00:00+02:00"),
    run: { messageId: scheduledMessage("spent-offset", spentAt), status: "completed" },
    onCompletion: true,
    pruned: true,
  },
  {
    id: "spent-wrapped",
    schedule: wrap(at(spentAt)),
    run: { messageId: scheduledMessage("spent-wrapped", spentAt), status: "completed" },
    onCompletion: true,
    pruned: true,
  },
  {
    id: "spent-by-hand",
    schedule: at(spentAt),
    run: { messageId: manualMessage("spent-by-hand"), status: "completed" },
    onCompletion: true,
    pruned: true,
  },
  // Fired at its own millisecond; SQLite's julianday rounds the sub-millisecond time up.
  {
    id: "spent-at-boundary",
    schedule: at("2026-10-08T12:00:05.000999+02:00"),
    run: {
      messageId: scheduledMessage("spent-at-boundary", "2026-10-08T10:00:05.000Z"),
      status: "completed",
    },
    onCompletion: true,
    pruned: true,
  },
  // Dispatch was accepted, but the turn failed, never finished, or left no run.
  {
    id: "turn-failed",
    schedule: at(spentAt),
    run: { messageId: scheduledMessage("turn-failed", spentAt), status: "failed" },
    onCompletion: true,
    pruned: false,
  },
  {
    id: "turn-running",
    schedule: at(spentAt),
    run: { messageId: scheduledMessage("turn-running", spentAt), status: "running" },
    onCompletion: true,
    pruned: false,
  },
  { id: "turn-unknown", schedule: at(spentAt), onCompletion: true, pruned: false },
  {
    id: "spent-run-at",
    schedule: JSON.stringify({ type: "once", runAt: "2026-10-08T10:00:00.000Z" }),
    onCompletion: true,
    pruned: false,
  },
  {
    id: "failed",
    schedule: at(spentAt),
    status: "failed",
    run: { messageId: scheduledMessage("failed", spentAt), status: "completed" },
    onCompletion: false,
    pruned: false,
  },
  {
    id: "never-run",
    schedule: at(spentAt),
    status: "never",
    lastRunAt: null,
    onCompletion: false,
    pruned: false,
  },
  {
    id: "paused",
    schedule: at(spentAt),
    enabled: 0,
    run: { messageId: scheduledMessage("paused", spentAt), status: "completed" },
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
  ...(["agent", "command"] as const).map((check) => ({
    id: `checked-${check}`,
    schedule: at(spentAt),
    check,
    run: { messageId: scheduledMessage(`checked-${check}`, spentAt), status: "completed" },
    onCompletion: false,
    pruned: false,
  })),
  // Shapes the server never writes are not one-time tasks.
  {
    id: "interval",
    schedule: JSON.stringify({
      type: "interval",
      everyMs: 60_000,
      schedule: JSON.parse(at(spentAt)),
    }),
    onCompletion: false,
    pruned: false,
  },
  {
    id: "numeric-at",
    schedule: JSON.stringify({ type: "once", at: 2_461_321 }),
    onCompletion: false,
    pruned: false,
  },
  { id: "corrupt", schedule: "broken", onCompletion: false, pruned: false },
];

const seed = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* ensureCheckSchema;
  for (const [ordinal, row] of rows.entries()) {
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
    if (row.run !== undefined)
      yield* sql`INSERT INTO orchestration_v2_projection_runs ${sql.insert({
        run_id: `run:${row.id}`,
        thread_id: "thread:runs",
        ordinal,
        provider: "codex",
        status: row.run.status,
        requested_at: RAN,
        payload_json: JSON.stringify({ userMessageId: row.run.messageId }),
      })}`;
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

// Completion runs only after the watcher saw the turn complete, so the run row is not consulted.
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

it.effect("the startup prune removes only spent fork one-time tasks whose turn completed", () =>
  Effect.gen(function* () {
    yield* seed;
    yield* pruneSpentForkOnceTasks();
    assert.deepEqual(yield* remaining, expected("pruned"));
    yield* pruneSpentForkOnceTasks();
    assert.deepEqual(yield* remaining, expected("pruned"));
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);
