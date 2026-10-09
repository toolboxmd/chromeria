import {
  DEFAULT_RUNTIME_MODE,
  ModelSelection,
  OrchestrationV2AppThreadJson,
  ScheduledTask,
  ScheduledTaskId,
  type ScheduledTaskSchedule,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import { CheckState, type CheckedRun } from "./state.ts";
import { ensureCheckSchema } from "./store.ts";

export const scheduledTasksBackfillId = "scheduled-tasks";

/** The frozen v1 `scheduler.state-set` task, as far as the import reads it. */
const V1CheckResult = Schema.Struct({
  version: Schema.Number,
  passed: Schema.Boolean,
  output: Schema.String,
  checkedAt: Schema.String,
});
const V1Task = Schema.Struct({
  id: Schema.String,
  revision: Schema.Number,
  createdBy: Schema.optionalKey(Schema.String),
  createdAt: Schema.String,
  updatedAt: Schema.String,
  deleted: Schema.Boolean,
  definition: Schema.Struct({
    kind: Schema.optionalKey(Schema.String),
    title: Schema.String,
    prompt: Schema.optionalKey(Schema.String),
    command: Schema.optionalKey(Schema.String),
    projectId: Schema.optionalKey(Schema.String),
    role: Schema.optionalKey(Schema.String),
    lane: Schema.optionalKey(Schema.Literals(["easy", "medium", "hard"])),
    target: Schema.optionalKey(
      Schema.Union([
        Schema.Struct({ kind: Schema.Literal("new-thread"), projectId: Schema.String }),
        Schema.Struct({ kind: Schema.Literal("thread"), threadId: Schema.String }),
      ]),
    ),
    schedule: Schema.Struct({
      kind: Schema.String,
      minutes: Schema.optionalKey(Schema.Number),
      at: Schema.optionalKey(Schema.String),
      weekdays: Schema.optionalKey(Schema.Array(Schema.Number)),
      times: Schema.optionalKey(Schema.Array(Schema.String)),
      timeZone: Schema.optionalKey(Schema.String),
      windowMinutes: Schema.optionalKey(Schema.Number),
    }),
  }),
  choices: Schema.optionalKey(
    Schema.Array(Schema.Struct({ requested: Schema.String, offsetMinutes: Schema.Number })),
  ),
  checks: Schema.Array(
    Schema.Struct({
      version: Schema.Number,
      command: Schema.String,
      actor: Schema.String,
      reason: Schema.String,
      createdAt: Schema.String,
      revertedFrom: Schema.NullOr(Schema.Number),
    }),
  ),
  runs: Schema.Array(
    Schema.Struct({
      id: Schema.String,
      slot: Schema.String,
      checkVersion: Schema.optionalKey(Schema.Number),
      threadId: Schema.NullOr(Schema.String),
      checkCwd: Schema.String,
      status: Schema.String,
      attempt: Schema.Number,
      hasWork: Schema.Boolean,
      error: Schema.NullOr(Schema.String),
      check: Schema.NullOr(V1CheckResult),
      commandResult: Schema.optionalKey(
        Schema.Struct({
          exitCode: Schema.NullOr(Schema.Number),
          output: Schema.optionalKey(Schema.String),
          timedOut: Schema.Boolean,
          endedAt: Schema.String,
        }),
      ),
    }),
  ),
  failureStreak: Schema.Number,
  lastError: Schema.NullOr(Schema.String),
  lastSuccessfulRunId: Schema.optionalKey(Schema.String),
});
type V1Task = typeof V1Task.Type;

const decodeV1 = Schema.decodeUnknownOption(Schema.fromJsonString(V1Task));
const decodeThread = Schema.decodeUnknownOption(
  Schema.fromJsonString(OrchestrationV2AppThreadJson),
);
const decodeSelection = Schema.decodeUnknownOption(Schema.fromJsonString(ModelSelection));
const decodeTask = Schema.decodeUnknownEffect(ScheduledTask);
const decodeState = Schema.decodeUnknownEffect(CheckState);
const encodeState = Schema.encodeEffect(Schema.fromJsonString(CheckState));
const encodeSchedule = Schema.encodeEffect(Schema.fromJsonString(ScheduledTask.fields.schedule));
const encodeStrategy = Schema.encodeEffect(
  Schema.fromJsonString(ScheduledTask.fields.workspaceStrategy),
);
const encodeSelection = Schema.encodeEffect(Schema.fromJsonString(ModelSelection));
const idPattern = /^[A-Za-z0-9._:-]{1,200}$/;
const SCHEDULE_KINDS = new Set(["interval", "once", "weekly"]);

/** A bounded kind safe to log: arbitrary stored strings are never echoed. */
const kindOf = (task: V1Task) => {
  const schedule = SCHEDULE_KINDS.has(task.definition.schedule.kind)
    ? task.definition.schedule.kind
    : "unknown";
  if (task.definition.kind === "command") return `command:${schedule}`;
  if (task.definition.kind !== undefined && task.definition.kind !== "agent") return "unknown";
  return `agent:${schedule}`;
};

type Outcome =
  | { readonly outcome: "imported"; readonly kind: string }
  | { readonly outcome: "deleted"; readonly kind: string }
  | { readonly outcome: "unsupported"; readonly kind: string; readonly reason: string };

/**
 * Copies Chromeria v1 scheduled tasks into upstream's `scheduled_tasks` and
 * the fork's task state (toolboxmd/chromeria#174). V1 keeps running them until
 * the user switches, so every imported task is disabled and its runs are inert
 * history. A task that cannot be mapped keeps its full v1 payload privately in
 * `fork_scheduled_task_v1_imports`; logs carry only its id and a bounded kind.
 * One marker per v1 task makes reruns and partial failures converge.
 */
export const backfillScheduledTasks = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const tables = yield* sql<{ readonly name: string }>`
    SELECT name FROM sqlite_master
    WHERE type = 'table' AND name IN ('orchestration_events', 'fork_scheduler_task_state')
  `;
  if (!tables.some((table) => table.name === "orchestration_events")) return;
  yield* ensureCheckSchema;
  yield* sql`
    CREATE TABLE IF NOT EXISTS fork_scheduled_task_v1_imports (
      task_id TEXT PRIMARY KEY,
      outcome TEXT NOT NULL,
      kind TEXT NOT NULL,
      reason TEXT,
      payload_json TEXT
    )
  `;
  const events = yield* sql<{ readonly stream_id: string; readonly payload: string }>`
    SELECT stream_id, payload_json AS payload FROM orchestration_events
    WHERE event_type = 'scheduler.state-set' AND sequence IN (
      SELECT MAX(sequence) FROM orchestration_events
      WHERE event_type = 'scheduler.state-set' GROUP BY stream_id
    )
  `;
  // Command tasks kept their newest state in a fork row; a row wins a revision tie.
  const rows = tables.some((table) => table.name === "fork_scheduler_task_state")
    ? yield* sql<{ readonly stream_id: string; readonly payload: string }>`
        SELECT task_id AS stream_id, state_json AS payload FROM fork_scheduler_task_state
      `
    : [];
  const latest = new Map<string, { readonly payload: string; readonly revision: number }>();
  for (const entry of [...events, ...rows]) {
    const revision = Option.match(decodeV1(entry.payload), {
      onNone: () => -1,
      onSome: (task) => task.revision,
    });
    const previous = latest.get(entry.stream_id);
    if (previous === undefined || revision >= previous.revision)
      latest.set(entry.stream_id, { payload: entry.payload, revision });
  }
  const done = new Set(
    (yield* sql<{
      readonly task_id: string;
    }>`SELECT task_id FROM fork_scheduled_task_v1_imports`).map((row) => row.task_id),
  );
  const tally = {
    imported: 0,
    deleted: 0,
    unsupported: [] as Array<{ id: string; kind: string }>,
  };
  for (const [streamId, entry] of latest) {
    if (done.has(streamId)) continue;
    const decoded = decodeV1(entry.payload);
    const outcome = yield* sql.withTransaction(
      Option.isNone(decoded)
        ? record(
            sql,
            streamId,
            { outcome: "unsupported", kind: "unknown", reason: "unreadable" },
            entry.payload,
          )
        : importTask(sql, decoded.value, entry.payload),
    );
    if (outcome.outcome === "imported") tally.imported += 1;
    else if (outcome.outcome === "deleted") tally.deleted += 1;
    // Ids are server-generated; anything else is withheld from the log.
    else
      tally.unsupported.push({
        id: idPattern.test(streamId) ? streamId : "(withheld)",
        kind: outcome.kind,
      });
  }
  if (tally.imported + tally.deleted + tally.unsupported.length > 0)
    yield* Effect.logInfo("Scheduled task v1 import finished").pipe(
      Effect.annotateLogs({
        importedTaskCount: tally.imported,
        deletedTaskCount: tally.deleted,
        unsupportedTasks: tally.unsupported,
      }),
    );
});

const record = (
  sql: SqlClient.SqlClient,
  taskId: string,
  outcome: Outcome,
  payload: string | null,
) =>
  sql`
    INSERT INTO fork_scheduled_task_v1_imports (task_id, outcome, kind, reason, payload_json)
    VALUES (
      ${taskId}, ${outcome.outcome}, ${outcome.kind},
      ${outcome.outcome === "unsupported" ? outcome.reason : null},
      ${outcome.outcome === "unsupported" ? payload : null}
    )
    ON CONFLICT (task_id) DO NOTHING
  `.pipe(Effect.as(outcome));

/** The v1 schedule as a v2 trigger, validated by the task schema; weekly keeps its minutes. */
const scheduleOf = (task: V1Task): unknown => {
  const schedule = task.definition.schedule;
  if (schedule.kind === "interval" && schedule.minutes !== undefined)
    return { type: "interval", everyMs: Math.round(schedule.minutes * 60_000) };
  if (
    schedule.kind === "once" &&
    schedule.at !== undefined &&
    Number.isFinite(Date.parse(schedule.at))
  )
    return { type: "once", at: DateTime.formatIso(DateTime.makeUnsafe(Date.parse(schedule.at))) };
  if (schedule.kind === "weekly")
    return {
      type: "weekly",
      weekdays: schedule.weekdays,
      times: schedule.times,
      timeZone: schedule.timeZone,
      ...(schedule.windowMinutes === undefined ? {} : { windowMinutes: schedule.windowMinutes }),
      ...(task.choices === undefined
        ? {}
        : {
            chosen: task.choices.map((choice) => ({
              requested: choice.requested,
              offsetMinutes: choice.offsetMinutes,
            })),
          }),
    };
  return null;
};

const v1Stage = (status: string): CheckedRun["stage"] =>
  status === "done" || status === "retry" || status === "usage-limit" || status === "running"
    ? status
    : "needs-you";

function importTask(sql: SqlClient.SqlClient, task: V1Task, payload: string) {
  return Effect.gen(function* () {
    const kind = kindOf(task);
    const unsupported = (reason: string) =>
      record(sql, task.id, { outcome: "unsupported", kind, reason }, payload);
    if (task.deleted) return yield* record(sql, task.id, { outcome: "deleted", kind }, null);
    if (kind === "unknown") return yield* unsupported("task kind is not recognized");
    const command = task.definition.kind === "command";
    const definition = task.definition;
    const complete = command
      ? definition.command !== undefined && definition.projectId !== undefined
      : definition.prompt !== undefined &&
        definition.target !== undefined &&
        task.checks.length > 0;
    if (!complete) return yield* unsupported("task definition is incomplete");
    let projectId = definition.projectId ?? "";
    let threadId: string | null = null;
    let selection: ModelSelection | null = null;
    let runtimeMode: ScheduledTask["runtimeMode"] = DEFAULT_RUNTIME_MODE;
    let interactionMode: ScheduledTask["interactionMode"] = "default";
    const target = command ? undefined : definition.target;
    if (target?.kind === "thread") {
      const threads = yield* sql<{ readonly payload_json: string }>`
        SELECT payload_json FROM orchestration_v2_projection_threads
        WHERE thread_id = ${target.threadId}
      `;
      const thread =
        threads[0] === undefined ? Option.none() : decodeThread(threads[0].payload_json);
      if (Option.isNone(thread) || thread.value.deletedAt !== null)
        return yield* unsupported("its thread was not imported");
      projectId = thread.value.projectId;
      threadId = thread.value.id;
      selection = thread.value.modelSelection;
      runtimeMode = thread.value.runtimeMode;
      interactionMode = thread.value.interactionMode;
    } else {
      if (target !== undefined) projectId = target.projectId;
      const projects = yield* sql<{ readonly default_model_selection_json: string | null }>`
        SELECT default_model_selection_json FROM projection_projects
        WHERE project_id = ${projectId} AND deleted_at IS NULL
      `;
      if (projects[0] === undefined) return yield* unsupported("its project was not imported");
      const stored = projects[0].default_model_selection_json;
      selection = stored === null ? null : Option.getOrNull(decodeSelection(stored));
    }
    if (selection === null) return yield* unsupported("no model selection to store");
    const upstream = yield* decodeTask({
      id: task.id,
      title: definition.title,
      // A command task's prompt only describes it; the command runs from the fork state.
      prompt: command ? definition.title : definition.prompt,
      enabled: false,
      schedule: scheduleOf(task),
      projectId,
      threadId,
      workspaceStrategy: { type: "root" },
      modelSelection: selection,
      runtimeMode,
      interactionMode,
      createdBy: (task.createdBy ?? task.checks[0]?.actor ?? "").startsWith("user:")
        ? "user"
        : "agent",
      creationSource: "server",
      createdAt: task.createdAt,
      updatedAt: task.updatedAt,
      nextRunAt: null,
      lastRunAt: null,
      lastRunStatus: "never",
      lastRunError: null,
      runCount: 0,
    }).pipe(Effect.option);
    if (Option.isNone(upstream)) return yield* unsupported("its fields do not fit a v2 task");
    const state = yield* decodeState({
      version: 1,
      taskId: ScheduledTaskId.make(task.id),
      revision: 1,
      kind: command ? "command" : "agent",
      command: command ? definition.command : null,
      role: command ? null : (definition.role ?? null),
      lane: command ? null : (definition.lane ?? null),
      checks: command ? [] : task.checks,
      runs: task.runs.map((run) => ({
        id: run.id,
        slot: run.slot,
        checkVersion: command ? null : (run.checkVersion ?? task.checks.at(-1)!.version),
        threadId: run.threadId,
        checkCwd: run.checkCwd,
        stage: v1Stage(run.status),
        attempt: run.attempt,
        retryAt: null,
        hasWork: run.hasWork,
        sends: [],
        error: run.error,
        check: run.check,
        ...(run.commandResult === undefined ? {} : { commandResult: run.commandResult }),
        imported: { from: "v1", status: run.status },
      })),
      failureStreak: task.failureStreak,
      lastError: task.lastError,
      lastSuccessfulRunId: task.lastSuccessfulRunId ?? null,
    }).pipe(Effect.option);
    if (Option.isNone(state)) return yield* unsupported("its run history does not fit v2");
    const row = upstream.value;
    const schedule: ScheduledTaskSchedule = row.schedule;
    const inserted = yield* sql<{ readonly task_id: string }>`
      INSERT INTO scheduled_tasks (
        task_id, title, prompt, enabled, schedule_json, project_id, thread_id,
        workspace_strategy_json, model_selection_json, runtime_mode, interaction_mode,
        created_by, creation_source, created_at, updated_at, next_run_at, last_run_at,
        last_run_status, last_run_error, run_count, webhook_token, webhook_secret
      ) VALUES (
        ${row.id}, ${row.title}, ${row.prompt}, 0, ${yield* encodeSchedule(schedule)},
        ${row.projectId}, ${row.threadId}, ${yield* encodeStrategy(row.workspaceStrategy)},
        ${yield* encodeSelection(row.modelSelection)}, ${row.runtimeMode}, ${row.interactionMode},
        ${row.createdBy}, ${row.creationSource}, ${row.createdAt}, ${row.updatedAt}, NULL, NULL,
        'never', NULL, 0, NULL, NULL
      )
      ON CONFLICT (task_id) DO NOTHING
      RETURNING task_id
    `;
    // A v2 task already using this id wins; the v1 task stays recoverable instead.
    if (inserted.length === 0) return yield* unsupported("a v2 task already uses its id");
    yield* sql`
      INSERT INTO fork_scheduled_task_checks (task_id, revision, state_json)
      VALUES (${row.id}, 1, ${yield* encodeState(state.value)})
    `;
    return yield* record(sql, task.id, { outcome: "imported", kind }, null);
  });
}
