import { assert, it } from "@effect/vitest";
import { CommandId, EventId, ScheduledTaskId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as References from "effect/References";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import * as EventSink from "../orchestration-v2/EventSink.ts";
import { recoveryRun, recoveryTestLayer, threadFor } from "../prism/recovery.testkit.ts";
import { checkedRunStatus } from "./state.ts";
import { readCheckState } from "./store.ts";
import { backfillScheduledTasks } from "./v1Import.ts";

const toJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const fromJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const thread = threadFor(recoveryRun);
const PROMPT = "SECRET PROMPT TEXT";
const selection = { instanceId: "codex", model: "project-default" };

/** A Chromeria v1 `scheduler.state-set` task, as v1 stored it. */
const v1Task = (
  id: string,
  definition: Record<string, unknown>,
  extra: Record<string, unknown> = {},
) => ({
  id,
  revision: 1,
  createdBy: "user:me",
  createdAt: "2026-09-01T00:00:00.000Z",
  updatedAt: "2026-09-02T00:00:00.000Z",
  deleted: false,
  definition: {
    kind: "agent",
    title: "Nightly review",
    prompt: PROMPT,
    target: { kind: "thread", threadId: thread.id },
    schedule: { kind: "interval", minutes: 60 },
    ...definition,
  },
  checks: [
    {
      version: 1,
      command: "test -f secret-check",
      actor: "user:me",
      reason: "the review leaves a file",
      createdAt: "2026-09-01T00:00:00.000Z",
      revertedFrom: null,
    },
  ],
  runs: [
    {
      id: `${id}:run`,
      slot: "2026-09-02T00:00:00.000Z",
      checkVersion: 1,
      threadId: thread.id,
      checkCwd: "/repo",
      status: "running",
      attempt: 1,
      hasWork: true,
      error: null,
      check: null,
    },
  ],
  failureStreak: 0,
  lastError: null,
  ...extra,
});

let sequence = 0;
const v1Event = (streamId: string, payload: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    sequence += 1;
    yield* sql`INSERT INTO orchestration_events ${sql.insert({
      event_id: `v1:${sequence}`,
      aggregate_kind: "scheduler",
      stream_id: streamId,
      stream_version: sequence,
      event_type: "scheduler.state-set",
      occurred_at: "2026-09-02T00:00:00.000Z",
      command_id: null,
      causation_event_id: null,
      correlation_id: null,
      actor_kind: "server",
      payload_json: payload,
      metadata_json: "{}",
    })}`;
  });

const seed = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  // The v2 thread a bound v1 task targets, and the project a new-thread task targets.
  yield* (yield* EventSink.EventSinkV2).commitCommand({
    commandId: CommandId.make("v1:thread"),
    commandType: "fixture",
    threadId: thread.id,
    acceptedAt: recoveryRun.requestedAt,
    events: [
      {
        id: EventId.make("event:v1:thread"),
        type: "thread.created",
        threadId: thread.id,
        occurredAt: recoveryRun.requestedAt,
        payload: thread,
      },
    ],
    effects: [],
  });
  yield* sql`INSERT INTO projection_projects ${sql.insert({
    project_id: "project:v1",
    title: "V1 project",
    workspace_root: "/repo",
    scripts_json: "[]",
    created_at: "2026-09-01T00:00:00.000Z",
    updated_at: "2026-09-01T00:00:00.000Z",
    deleted_at: null,
    default_model_selection_json: toJson(selection),
  })}`;
  yield* v1Event("task:interval", toJson(v1Task("task:interval", {})));
  yield* v1Event(
    "task:once",
    toJson(
      v1Task("task:once", {
        target: { kind: "new-thread", projectId: "project:v1" },
        schedule: { kind: "once", at: "2026-10-09T10:00:00+02:00" },
      }),
    ),
  );
  yield* v1Event(
    "task:weekly",
    toJson(
      v1Task(
        "task:weekly",
        {
          schedule: {
            kind: "weekly",
            weekdays: [1, 4],
            times: ["09:00"],
            timeZone: "Europe/Warsaw",
            windowMinutes: 20,
          },
        },
        { choices: [{ requested: "09:00", offsetMinutes: -3 }] },
      ),
    ),
  );
  // A command task: its newest state is the fork row, which wins the revision tie.
  const command = (text: string, revision: number) =>
    toJson(
      v1Task(
        "task:command",
        { kind: "command", command: text, projectId: "project:v1", prompt: undefined },
        { revision, checks: [] },
      ),
    );
  yield* v1Event("task:command", command("echo secret-old", 1));
  yield* sql`CREATE TABLE fork_scheduler_task_state (task_id TEXT PRIMARY KEY, state_json TEXT NOT NULL)`;
  yield* sql`INSERT INTO fork_scheduler_task_state ${sql.insert({
    task_id: "task:command",
    state_json: command("echo secret-new", 2),
  })}`;
  yield* v1Event("task:deleted", toJson(v1Task("task:deleted", {}, { deleted: true })));
  yield* v1Event("task with spaces", "{not json");
  yield* v1Event("task:browser", toJson(v1Task("task:browser", { kind: "browser" })));
  yield* v1Event(
    "task:lost-thread",
    toJson(v1Task("task:lost-thread", { target: { kind: "thread", threadId: "thread:gone" } })),
  );
  // A v2 task already uses this id: it wins, and the v1 task stays recoverable.
  yield* v1Event("task:taken", toJson(v1Task("task:taken", {})));
  yield* sql`INSERT INTO scheduled_tasks ${sql.insert({
    task_id: "task:taken",
    title: "Upstream task",
    prompt: "Upstream prompt",
    enabled: 1,
    schedule_json: toJson({ type: "interval", everyMs: 3_600_000 }),
    project_id: thread.projectId,
    thread_id: null,
    workspace_strategy_json: '{"type":"root"}',
    model_selection_json: toJson(selection),
    runtime_mode: "full-access",
    interaction_mode: "default",
    created_by: "user",
    creation_source: "web",
    created_at: "2026-09-01T00:00:00.000Z",
    updated_at: "2026-09-01T00:00:00.000Z",
    next_run_at: null,
    last_run_at: null,
    last_run_status: "never",
    last_run_error: null,
    run_count: 0,
  })}`;
});

it.effect(
  "imports v1 tasks disabled with inert history, keeps the rest privately, and never logs their text",
  () => {
    const logs: Array<unknown> = [];
    const logger = Logger.make(({ fiber, message }) => {
      logs.push({ message, annotations: fiber.getRef(References.CurrentLogAnnotations) });
    });
    return Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* seed;
      yield* backfillScheduledTasks;

      const tasks = yield* sql<{
        readonly task_id: string;
        readonly enabled: number;
        readonly schedule_json: string;
        readonly thread_id: string | null;
        readonly project_id: string;
        readonly prompt: string;
        readonly model_selection_json: string;
        readonly next_run_at: string | null;
      }>`SELECT task_id, enabled, schedule_json, thread_id, project_id, prompt,
           model_selection_json, next_run_at
         FROM scheduled_tasks WHERE task_id <> 'task:taken' ORDER BY task_id`;
      assert.deepEqual(
        tasks.map((task) => task.task_id),
        ["task:command", "task:interval", "task:once", "task:weekly"],
      );
      // Imported tasks never fire until the user enables them.
      for (const task of tasks) {
        assert.equal(task.enabled, 0);
        assert.isNull(task.next_run_at);
      }
      const byId = new Map(tasks.map((task) => [task.task_id, task]));
      assert.deepEqual(fromJson(byId.get("task:interval")!.schedule_json), {
        type: "interval",
        everyMs: 3_600_000,
      });
      // A bound task takes its thread's project and model.
      assert.equal(byId.get("task:interval")!.thread_id, thread.id);
      assert.equal(byId.get("task:interval")!.project_id, thread.projectId);
      assert.deepEqual(
        fromJson(byId.get("task:interval")!.model_selection_json),
        fromJson(toJson(thread.modelSelection)),
      );
      // A one-shot keeps its instant; a new-thread task takes its project's default model.
      assert.deepEqual(fromJson(byId.get("task:once")!.schedule_json), {
        type: "once",
        at: "2026-10-09T08:00:00.000Z",
      });
      assert.deepEqual(fromJson(byId.get("task:once")!.model_selection_json), selection);
      // A weekly task keeps its window and v1's minute picks.
      assert.deepEqual(fromJson(byId.get("task:weekly")!.schedule_json), {
        type: "weekly",
        weekdays: [1, 4],
        times: ["09:00"],
        timeZone: "Europe/Warsaw",
        windowMinutes: 20,
        chosen: [{ requested: "09:00", offsetMinutes: -3 }],
      });
      // A command task's prompt only describes it; its command is the newest v1 state.
      assert.equal(byId.get("task:command")!.prompt, "Nightly review");
      const command = (yield* readCheckState(ScheduledTaskId.make("task:command")))!;
      assert.equal(command.kind, "command");
      assert.equal(command.command, "echo secret-new");
      // Imported runs are inert history and report as such.
      const interval = (yield* readCheckState(ScheduledTaskId.make("task:interval")))!;
      assert.deepEqual(interval.runs[0]?.imported, { from: "v1", status: "running" });
      assert.deepEqual(interval.runs[0]?.sends, []);
      assert.equal(checkedRunStatus(interval)?.status, "failed");

      const markers = yield* sql<{
        readonly task_id: string;
        readonly outcome: string;
        readonly kind: string;
        readonly reason: string | null;
        readonly payload_json: string | null;
      }>`SELECT task_id, outcome, kind, reason, payload_json
         FROM fork_scheduled_task_v1_imports ORDER BY task_id`;
      assert.deepEqual(
        markers.map(({ task_id, outcome, kind, reason }) => [task_id, outcome, kind, reason]),
        [
          ["task with spaces", "unsupported", "unknown", "unreadable"],
          ["task:browser", "unsupported", "unknown", "task kind is not recognized"],
          ["task:command", "imported", "command:interval", null],
          ["task:deleted", "deleted", "agent:interval", null],
          ["task:interval", "imported", "agent:interval", null],
          ["task:lost-thread", "unsupported", "agent:interval", "its thread was not imported"],
          ["task:once", "imported", "agent:once", null],
          ["task:taken", "unsupported", "agent:interval", "a v2 task already uses its id"],
          ["task:weekly", "imported", "agent:weekly", null],
        ],
      );
      // Only tasks left behind keep their payload, privately.
      for (const marker of markers)
        assert.equal(marker.payload_json !== null, marker.outcome === "unsupported");
      assert.equal(
        (yield* sql<{ readonly prompt: string }>`
          SELECT prompt FROM scheduled_tasks WHERE task_id = 'task:taken'`)[0]?.prompt,
        "Upstream prompt",
      );

      // A rerun converges: nothing is imported, written or logged again.
      const logged = logs.length;
      yield* backfillScheduledTasks;
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM scheduled_tasks`)[0]?.count,
        5,
      );
      assert.equal(logs.length, logged);

      // The log carries ids and bounded kinds only.
      const text = toJson(logs);
      for (const secret of [PROMPT, "secret-check", "secret-old", "secret-new", "task with spaces"])
        assert.notInclude(text, secret);
      assert.include(text, "(withheld)");
    }).pipe(
      Effect.provide(
        Layer.merge(recoveryTestLayer, Logger.layer([logger], { mergeWithExisting: false })),
      ),
    );
  },
);
