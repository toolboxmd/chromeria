// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ScheduledTaskId,
  ThreadId,
  type ScheduledTask,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import type * as PlatformError from "effect/PlatformError";
import type { MigrationError } from "effect/sql/Migrator";
import type { SqlError } from "effect/sql/SqlError";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";

import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import type { ProviderAdapterV2Shape } from "../orchestration-v2/ProviderAdapter.ts";
import * as Registry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as ThreadLaunchService from "../orchestration-v2/ThreadLaunchService.ts";
import * as Harness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { ProcessRunner } from "../processRunner.ts";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as ScheduledTaskChecks from "./ScheduledTaskChecks.ts";
import type { CheckState } from "./state.ts";
import { readCheckState, writeCheckState } from "./store.ts";

const toJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const instanceId = ProviderInstanceId.make("codex");
const projectId = ProjectId.make("project:checks");
const threadId = ThreadId.make("thread:checks");
const selection = {
  instanceId,
  model: "fixed",
  options: [{ id: "reasoningEffort", value: "high" }],
};
// Committed dispatch only: no provider process ever starts.
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("These tests prove committed dispatch without a provider process"),
} as ProviderAdapterV2Shape;

/** Production wiring of upstream's scheduler with outcome checks, on a real orchestrator. */
type Database = Layer.Layer<
  SqlClient.SqlClient,
  MigrationError | PlatformError.PlatformError | SqlError
>;

const runtime = (database: Database) => {
  const orchestration = Harness.layerWithRegistry(
    { name: "scheduled-task-checks" },
    Registry.layerFromAdapters([adapter]),
    { databaseLayer: database, runEffectWorker: false },
  );
  const dependencies = Layer.mergeAll(
    database,
    orchestration,
    ProjectStore.layer.pipe(Layer.provide(database)),
    Layer.mock(ThreadLaunchService.ThreadLaunchService)({
      launch: () => Effect.die("These tasks post to a bound thread"),
    }),
    Layer.mock(SecretRequests.SecretRequests)({}),
    Layer.succeed(ProcessRunner, {
      run: () =>
        Effect.succeed({
          stdout: "",
          stderr: "not yet",
          code: 1 as never,
          timedOut: false,
          stdoutTruncated: false,
          stderrTruncated: false,
          stdoutInvalidUtf8: false,
          stderrInvalidUtf8: false,
        }),
    }),
    NodeCrypto.layer,
    NodeServices.layer,
    Scheduler.layer,
  );
  return Layer.mergeAll(
    dependencies,
    ScheduledTaskChecks.withOutcomeChecks(ScheduledTaskService.layer).pipe(
      Layer.provide(dependencies),
    ),
  );
};

const createThread = Effect.gen(function* () {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  yield* orchestrator.dispatch({
    type: "thread.create",
    commandId: CommandId.make("checks:create"),
    threadId,
    projectId,
    title: "Checked work",
    modelSelection: selection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdBy: "user",
    creationSource: "web",
  });
});

const insertTask = (input: {
  readonly id: string;
  readonly schedule: unknown;
  readonly next: string | null;
  readonly status?: string;
  readonly createdAt?: string;
}) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO scheduled_tasks ${sql.insert({
      task_id: input.id,
      title: "Checked work",
      prompt: "Do the scheduled work",
      enabled: 1,
      schedule_json: toJson(input.schedule),
      project_id: projectId,
      thread_id: threadId,
      workspace_strategy_json: '{"type":"root"}',
      model_selection_json: toJson(selection),
      runtime_mode: "full-access",
      interaction_mode: "default",
      created_by: "agent",
      creation_source: "mcp",
      created_at: input.createdAt ?? "2026-10-08T00:00:00.000Z",
      updated_at: "2026-10-08T00:00:00.000Z",
      next_run_at: input.next,
      last_run_at: input.status === "running" ? "2026-10-08T12:00:00.000Z" : null,
      last_run_status: input.status ?? "never",
      last_run_error: null,
      run_count: 0,
    })}`;
  });

const messagesOf = Effect.gen(function* () {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const projection = yield* orchestrator.getThreadProjection(threadId);
  return projection.messages.map((message) => message.id as string);
});

/** Waits on the service's own change stream, never on time. */
const awaitTask = (id: string, predicate: (task: ScheduledTask) => boolean) =>
  Effect.gen(function* () {
    const service = yield* ScheduledTaskService.ScheduledTaskService;
    return yield* service.subscribeList().pipe(
      Stream.map((result) => result.tasks.find((entry) => entry.id === id)),
      Stream.filter((task): task is ScheduledTask => task !== undefined && predicate(task)),
      Stream.runHead,
    );
  });

const checkState = (id: string, runs: CheckState["runs"] = []): CheckState => ({
  version: 1,
  taskId: ScheduledTaskId.make(id),
  revision: 0,
  kind: "agent",
  command: null,
  role: null,
  lane: null,
  checks: [
    {
      version: 1,
      command: "test -f done",
      actor: "agent",
      reason: "the work leaves done",
      createdAt: "2026-10-08T00:00:00.000Z",
      revertedFrom: null,
    },
  ],
  runs,
  failureStreak: 0,
  lastError: null,
  lastSuccessfulRunId: null,
});

const tempDatabase = Effect.gen(function* () {
  const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "chromeria-checks-"));
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => NodeFS.rmSync(directory, { recursive: true, force: true })),
  );
  return SqlitePersistence.layerFromPath(NodePath.join(directory, "chromeria-v2.sqlite")).pipe(
    Layer.provide(NodeServices.layer),
  );
});

/** A fire has settled once its run status left never and running. */
const settled = (task: ScheduledTask) =>
  task.lastRunStatus === "succeeded" || task.lastRunStatus === "failed";

const ONCE_ID = "scheduled-task:once";
const CANONICAL = "2026-10-08T12:00:00.000Z";

it.effect(
  "an overdue one-shot fires once through real dispatch; a replay with another offset and a restart send nothing more",
  () =>
    Effect.gen(function* () {
      const database = yield* tempDatabase;
      yield* TestClock.setTime(Date.parse(CANONICAL) + 2 * 3_600_000);
      // First server: the slot passed while it was off; it fires once on its first poll.
      yield* Effect.gen(function* () {
        yield* createThread;
        yield* insertTask({
          id: ONCE_ID,
          schedule: { type: "once", at: "2026-10-08T14:00:00+02:00" },
          next: CANONICAL,
        });
        const fired = yield* awaitTask(ONCE_ID, settled).pipe(Effect.forkScoped);
        yield* TestClock.adjust("6 seconds");
        const task = yield* Fiber.join(fired);
        assert.deepEqual(
          Option.map(task, (entry) => [entry.lastRunStatus, entry.lastRunError]),
          Option.some(["succeeded", null]),
        );
        assert.deepEqual(yield* messagesOf, [
          `scheduled-task-message:${ONCE_ID}:once:${CANONICAL}`,
        ]);
        const sql = yield* SqlClient.SqlClient;
        const [row] = yield* sql<{ next_run_at: string | null }>`
          SELECT next_run_at FROM scheduled_tasks WHERE task_id = ${ONCE_ID}`;
        assert.isNull(row?.next_run_at ?? null);
        // A lost completion replays the same slot, written with an equivalent offset.
        yield* sql`UPDATE scheduled_tasks
          SET next_run_at = ${CANONICAL}, last_run_status = 'never',
              schedule_json = ${toJson({ type: "once", at: "2026-10-08T12:00:00Z" })}
          WHERE task_id = ${ONCE_ID}`;
        const replayed = yield* awaitTask(ONCE_ID, (entry) => entry.runCount === 2).pipe(
          Effect.forkScoped,
        );
        yield* TestClock.adjust("6 seconds");
        yield* Fiber.join(replayed);
        // The same instant is the same command: the orchestrator's receipt keeps it to one message.
        assert.deepEqual(yield* messagesOf, [
          `scheduled-task-message:${ONCE_ID}:once:${CANONICAL}`,
        ]);
      }).pipe(Effect.provide(runtime(database)), Effect.scoped);
      // Second server on the same database: nothing is due and nothing is sent.
      yield* Effect.gen(function* () {
        yield* insertTask({
          id: "scheduled-task:control",
          schedule: { type: "interval", everyMs: 3_600_000 },
          next: CANONICAL,
        });
        const control = yield* awaitTask("scheduled-task:control", settled).pipe(Effect.forkScoped);
        yield* TestClock.adjust("6 seconds");
        assert.deepEqual(
          Option.map(yield* Fiber.join(control), (entry) => [
            entry.lastRunStatus,
            entry.lastRunError,
          ]),
          Option.some(["succeeded", null]),
        );
        const messages = yield* messagesOf;
        assert.equal(
          messages.filter((id) => id.includes(":once:")).length,
          1,
          "the one-shot never fires again",
        );
      }).pipe(Effect.provide(runtime(database)), Effect.scoped);
    }).pipe(Effect.scoped),
);

it.effect(
  "a checked fire records its send before upstream marks it running; after a crash the recorded send lands once",
  () =>
    Effect.gen(function* () {
      const database = yield* tempDatabase;
      yield* TestClock.setTime(Date.parse(CANONICAL));
      const id = "scheduled-task:checked";
      const runId = `${id}:${CANONICAL}`;
      const commandId = `scheduled-task-check:${runId}:0`;
      const messageId = `scheduled-task-check-message:${runId}:0`;
      // What the policy records before dispatch, with the server dying right after.
      yield* Effect.gen(function* () {
        yield* createThread;
        yield* insertTask({
          id,
          schedule: { type: "interval", everyMs: 3_600_000 },
          next: CANONICAL,
          status: "running",
        });
        yield* writeCheckState(
          null,
          checkState(id, [
            {
              id: runId,
              slot: CANONICAL,
              checkVersion: 1,
              threadId,
              checkCwd: null,
              stage: "running",
              attempt: 0,
              retryAt: null,
              hasWork: false,
              sends: [
                {
                  index: 0,
                  commandId: CommandId.make(commandId),
                  messageId: messageId as never,
                  kind: "start",
                  createdAt: CANONICAL,
                  payload: {
                    text: "Pinned text from before the crash",
                    modelSelection: null,
                    launch: null,
                    projectId,
                  },
                },
              ],
              error: null,
              check: null,
            },
          ]),
        );
      }).pipe(Effect.provide(runtime(database)), Effect.scoped);
      // Restart: upstream releases its stuck row; the checked run resends its own send.
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        // The task row is edited after the crash; the resend still carries the pinned text.
        yield* sql`UPDATE scheduled_tasks SET prompt = 'An edited prompt' WHERE task_id = ${id}`;
        const checks = yield* ScheduledTaskChecks.ScheduledTaskChecks;
        yield* checks.reconcile;
        yield* checks.reconcile;
        yield* checks.reconcile;
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const projection = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(
          projection.messages.map((message) => message.id as string),
          [messageId],
        );
        assert.equal(projection.messages[0]?.text, "Pinned text from before the crash");
        assert.equal(projection.messages[0]?.scheduledTaskId, id);
        assert.equal(projection.messages[0]?.creationSource, "server");
        // The run keeps the thread's stored model and effort.
        assert.deepEqual(projection.runs[0]?.modelSelection, selection);
        const [row] = yield* sql<{ last_run_status: string; last_run_error: string | null }>`
          SELECT last_run_status, last_run_error FROM scheduled_tasks WHERE task_id = ${id}`;
        assert.equal(row?.last_run_status, "failed");
        // The read model reports the checked run, not upstream's interrupted dispatch.
        const service = yield* ScheduledTaskService.ScheduledTaskService;
        const listed = (yield* service.list()).tasks.find((task) => task.id === id);
        assert.equal(listed?.lastRunStatus, "running");
        assert.equal(listed?.outcomeCheck?.run?.stage, "running");
      }).pipe(Effect.provide(runtime(database)), Effect.scoped);
    }).pipe(Effect.scoped),
);

it.effect(
  "scheduled fires skip an unfinished checked run and Run now does not queue behind a busy thread",
  () =>
    Effect.gen(function* () {
      const database = yield* tempDatabase;
      yield* TestClock.setTime(Date.parse(CANONICAL));
      yield* Effect.gen(function* () {
        const id = "scheduled-task:busy";
        yield* createThread;
        yield* insertTask({ id, schedule: { type: "interval", everyMs: 3_600_000 }, next: null });
        yield* writeCheckState(null, checkState(id));
        const service = yield* ScheduledTaskService.ScheduledTaskService;
        // Run now starts a checked run; its message is the thread's active turn.
        const started = yield* service.runNow({ id: ScheduledTaskId.make(id) });
        assert.equal(started.task.lastRunStatus, "running");
        assert.equal((yield* messagesOf).length, 1);
        // A due slot meanwhile is consumed without a run or a message.
        const sql = yield* SqlClient.SqlClient;
        yield* sql`UPDATE scheduled_tasks SET next_run_at = ${CANONICAL} WHERE task_id = ${id}`;
        const skipped = yield* awaitTask(
          id,
          (task) => task.nextRunAt !== null && task.nextRunAt > CANONICAL,
        ).pipe(Effect.forkScoped);
        yield* TestClock.adjust("6 seconds");
        yield* Fiber.join(skipped);
        assert.equal((yield* messagesOf).length, 1);
        const refused = yield* Effect.exit(service.runNow({ id: ScheduledTaskId.make(id) }));
        assert.isTrue(Exit.isFailure(refused));
        assert.include(String(refused), "unfinished work");
        // Even a needs-you run is not resumed while its thread still has an active turn.
        const state = (yield* readCheckState(ScheduledTaskId.make(id)))!;
        yield* writeCheckState(state, {
          ...state,
          runs: state.runs.map((run) => ({ ...run, stage: "needs-you", error: "gave up" })),
        });
        const busy = yield* Effect.exit(service.runNow({ id: ScheduledTaskId.make(id) }));
        assert.isTrue(Exit.isFailure(busy));
        assert.include(String(busy), "busy or waiting on you");
        assert.equal((yield* messagesOf).length, 1);
      }).pipe(Effect.provide(runtime(database)), Effect.scoped);
    }).pipe(Effect.scoped),
);
