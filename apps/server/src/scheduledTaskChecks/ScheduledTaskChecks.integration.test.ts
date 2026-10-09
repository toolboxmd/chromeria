// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  RunId,
  ScheduledTaskId,
  ThreadId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
  type OrchestrationV2ServerCommand,
  type ScheduledTask,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
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
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import type { ProviderAdapterV2Shape } from "../orchestration-v2/ProviderAdapter.ts";
import * as Registry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import { continuationAdmission } from "../prism/continuationAdmission.ts";
import * as Prism from "../prism/PrismService.ts";
import { errorFor, recoveryRun } from "../prism/recovery.testkit.ts";
import * as History from "../prism/RecoveryHistory.ts";
import { continuationRunFields } from "../prism/RecoveryHooks.ts";
import * as RecoveryStore from "../prism/RecoveryStore.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as ThreadLaunchService from "../orchestration-v2/ThreadLaunchService.ts";
import * as Harness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { ProcessRunner } from "../processRunner.ts";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import {
  ScheduledTaskDispatchPolicy,
  type ScheduledTaskDispatchPolicyShape,
} from "./DispatchPolicy.ts";
import { ScheduledTaskSpectra } from "./handoff.ts";
import * as ScheduledTaskChecks from "./ScheduledTaskChecks.ts";
import {
  bindSpectrum,
  ensureSpectraFixture,
  fixtureSpectra,
  reportReceipt,
} from "./spectra.testkit.ts";
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

const checkResult = (code: number) => ({
  stdout: "",
  stderr: code === 0 ? "" : "not yet",
  code: code as never,
  timedOut: false,
  stdoutTruncated: false,
  stderrTruncated: false,
  stdoutInvalidUtf8: false,
  stderrInvalidUtf8: false,
});

/** Where a fork dispatch waits once admitted, until the test releases it. */
interface DispatchBarrier {
  readonly admitted: Deferred.Deferred<void>;
  readonly proceed: Deferred.Deferred<void>;
}

/** Upstream's service with the fork's policy, its admitted dispatches held at a barrier. */
const withDispatchBarrier = (barrier: DispatchBarrier) =>
  Layer.unwrap(
    Effect.gen(function* () {
      const policy = yield* ScheduledTaskDispatchPolicy;
      const held: ScheduledTaskDispatchPolicyShape = {
        decide: (input) =>
          policy.decide(input).pipe(
            Effect.map((decision) =>
              decision._tag === "fork"
                ? {
                    _tag: "fork" as const,
                    dispatch: Deferred.succeed(barrier.admitted, undefined).pipe(
                      Effect.andThen(Deferred.await(barrier.proceed)),
                      Effect.andThen(decision.dispatch),
                    ),
                  }
                : decision,
            ),
          ),
      };
      return ScheduledTaskService.layer.pipe(
        Layer.provide(Layer.succeed(ScheduledTaskDispatchPolicy, held)),
      );
    }),
  );

const runtime = (
  database: Database,
  options: {
    /** Each outcome check's exit code; checks fail by default. */
    readonly check?: Effect.Effect<number>;
    readonly spectra?: ReturnType<typeof fixtureSpectra>;
    /** A thread launch built on the real orchestrator; launches die without one. */
    readonly launch?: (services: {
      readonly sink: EventSink.EventSinkV2["Service"];
      readonly orchestrator: Orchestrator.OrchestratorV2["Service"];
    }) => ThreadLaunchService.ThreadLaunchService["Service"]["launch"];
    readonly dispatchBarrier?: DispatchBarrier;
  } = {},
) => {
  const orchestration = Harness.layerWithRegistry(
    { name: "scheduled-task-checks" },
    Registry.layerFromAdapters([adapter]),
    { databaseLayer: database, runEffectWorker: false },
  );
  const dependencies = Layer.mergeAll(
    database,
    orchestration,
    ProjectStore.layer.pipe(Layer.provide(database)),
    options.launch === undefined
      ? Layer.mock(ThreadLaunchService.ThreadLaunchService)({
          launch: () => Effect.die("These tasks post to a bound thread"),
        })
      : Layer.unwrap(
          Effect.gen(function* () {
            const sink = yield* EventSink.EventSinkV2;
            const orchestrator = yield* Orchestrator.OrchestratorV2;
            return Layer.mock(ThreadLaunchService.ThreadLaunchService)({
              launch: options.launch!({ sink, orchestrator }),
            });
          }),
        ).pipe(Layer.provide(orchestration)),
    Layer.mock(SecretRequests.SecretRequests)({}),
    Layer.succeed(ProcessRunner, {
      run: () => (options.check ?? Effect.succeed(1)).pipe(Effect.map(checkResult)),
    }),
    // #169's recovery history and controller tables, as production starts them.
    RecoveryStore.layer.pipe(Layer.provide(database)),
    ...(options.spectra === undefined
      ? []
      : [Layer.succeed(ScheduledTaskSpectra, options.spectra)]),
    NodeCrypto.layer,
    NodeServices.layer,
    Scheduler.layer,
    // These tasks have no Prism role, so routing is never consulted.
    Layer.mock(Prism.PrismService)({}),
    Layer.mock(ProviderRegistry.ProviderRegistry)({}),
    Registry.layerFromAdapters([adapter]),
  );
  return Layer.mergeAll(
    dependencies,
    ScheduledTaskChecks.withOutcomeChecks(
      options.dispatchBarrier === undefined
        ? ScheduledTaskService.layer
        : withDispatchBarrier(options.dispatchBarrier),
    ).pipe(Layer.provide(dependencies)),
  );
};

const createThreadIn = (worktreePath: string | null) =>
  Effect.gen(function* () {
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
      worktreePath,
      createdBy: "user",
      creationSource: "web",
    });
  });
const createThread = createThreadIn(null);

const insertTask = (input: {
  readonly id: string;
  readonly schedule: unknown;
  readonly next: string | null;
  readonly status?: string;
  readonly createdAt?: string;
  /** Null for a task that launches a new thread per run. */
  readonly threadId?: string | null;
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
      thread_id: input.threadId === undefined ? threadId : input.threadId,
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

const messagesOfThread = (id: ThreadId) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projection = yield* orchestrator.getThreadProjection(id);
    return projection.messages.map((message) => message.id as string);
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

const runEvent = (
  type: "run.created" | "run.updated",
  payload: OrchestrationV2Run,
  suffix: string,
): OrchestrationV2DomainEvent => ({
  id: EventId.make(`event:${suffix}`),
  type,
  threadId: payload.threadId,
  occurredAt: payload.requestedAt,
  payload,
});

const persist = (id: string, events: ReadonlyArray<OrchestrationV2DomainEvent>) =>
  Effect.gen(function* () {
    yield* (yield* EventSink.EventSinkV2).commitCommand({
      commandId: CommandId.make(id),
      commandType: "fixture",
      threadId,
      acceptedAt: recoveryRun.requestedAt,
      events,
      effects: [],
    });
  });

/** A run on the checked thread, recorded as the orchestrator records it. */
const threadRun = (
  id: string,
  userMessageId: string,
  status: OrchestrationV2Run["status"],
  ordinal = 1,
) =>
  ({
    ...recoveryRun,
    id: RunId.make(id),
    ordinal,
    threadId,
    providerInstanceId: instanceId,
    modelSelection: selection,
    userMessageId: MessageId.make(userMessageId),
    status,
  }) satisfies OrchestrationV2Run;

/** A checked run whose one send started the given run. */
const sentRun = (id: string, messageId: string): CheckState["runs"][number] => ({
  id,
  slot: CANONICAL,
  checkVersion: 1,
  threadId,
  checkCwd: null,
  stage: "running",
  attempt: 0,
  retryAt: null,
  hasWork: true,
  sends: [
    {
      index: 0,
      commandId: CommandId.make(`scheduled-task-check:${id}:0`),
      messageId: MessageId.make(messageId),
      kind: "start",
      createdAt: CANONICAL,
    },
  ],
  error: null,
  check: null,
});

const runOf = (id: string) =>
  readCheckState(ScheduledTaskId.make(id)).pipe(Effect.map((state) => state!.runs[0]!));

it.effect(
  "a pass never settles when recovery resumes the run's work during its check, and settles once that work completes",
  () =>
    Effect.gen(function* () {
      const database = yield* tempDatabase;
      const worktree = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "chromeria-checks-wt-"));
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => NodeFS.rmSync(worktree, { recursive: true, force: true })),
      );
      yield* TestClock.setTime(Date.parse(CANONICAL));
      const id = "scheduled-task:race";
      const runId = `${id}:${CANONICAL}`;
      const sentMessage = `scheduled-task-check-message:${runId}:0`;
      // What happens while a check runs; the check passes either way.
      let duringCheck: Effect.Effect<void> = Effect.void;
      const check = Effect.suspend(() => duringCheck).pipe(Effect.as(0));
      yield* Effect.gen(function* () {
        yield* createThreadIn(worktree);
        yield* insertTask({ id, schedule: { type: "interval", everyMs: 3_600_000 }, next: null });
        // The scheduler's send started a run that failed, and recovery concluded.
        const source = threadRun("run:race:source", sentMessage, "failed");
        yield* persist("race:source", [
          runEvent("run.created", source, "race:source"),
          {
            id: EventId.make("event:race:failure"),
            type: "turn-item.updated",
            threadId,
            occurredAt: source.requestedAt,
            payload: errorFor(source),
          },
        ]);
        yield* History.writeRecoveryOutcome({
          sourceRunId: source.id,
          threadId,
          status: "decided",
          outcome: "not_retryable",
          reason: "non_mcp",
        });
        yield* writeCheckState(null, checkState(id, [sentRun(runId, sentMessage)]));
        // While the check runs, recovery admits a continuation of that failed run.
        const command: OrchestrationV2ServerCommand = {
          type: "message.dispatch",
          commandId: CommandId.make("race:retry"),
          messageId: MessageId.make("race:retry:message"),
          threadId,
          forkPrismRetryOfRunId: source.id,
          text: "Continue",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "system",
          creationSource: "server",
        };
        const successor: OrchestrationV2Run = {
          ...source,
          ...continuationRunFields(command),
          id: RunId.make("run:race:successor"),
          ordinal: 2,
          userMessageId: command.messageId,
          status: "running",
          completedAt: null,
        };
        const admitted = [runEvent("run.created", successor, "race:successor")];
        const sink = yield* EventSink.EventSinkV2;
        duringCheck = sink
          .commitCommand({
            commandId: command.commandId,
            commandType: command.type,
            threadId,
            acceptedAt: source.requestedAt,
            events: admitted,
            effects: [],
            forkPlans: [continuationAdmission(command, admitted)!],
          })
          .pipe(
            Effect.andThen(
              Effect.sync(() => {
                duringCheck = Effect.void;
              }),
            ),
            Effect.orDie,
          );
        const checks = yield* ScheduledTaskChecks.ScheduledTaskChecks;
        yield* checks.reconcile;
        // The settle transaction saw the resumed work and wrote nothing.
        const resumed = yield* runOf(id);
        assert.equal(resumed.stage, "running");
        assert.isNull(resumed.check);
        assert.notEqual(resumed.awaitingReports, true);
        // While the continuation runs, the check is not run again.
        yield* checks.reconcile;
        assert.equal((yield* runOf(id)).stage, "running");
        yield* persist("race:successor:completed", [
          runEvent(
            "run.updated",
            { ...successor, status: "completed", completedAt: source.completedAt },
            "race:successor:completed",
          ),
        ]);
        yield* checks.reconcile;
        const done = yield* runOf(id);
        assert.equal(done.stage, "done");
        assert.isTrue(done.check?.passed === true);
      }).pipe(Effect.provide(runtime(database, { check })), Effect.scoped);
    }).pipe(Effect.scoped),
);

it.effect(
  "a passed check waits for its bound report, a report that needs you makes the run need you, and the user's abandonment lets the check decide",
  () =>
    Effect.gen(function* () {
      const database = yield* tempDatabase;
      const worktree = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "chromeria-checks-wt-"));
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => NodeFS.rmSync(worktree, { recursive: true, force: true })),
      );
      yield* TestClock.setTime(Date.parse(CANONICAL));
      const id = "scheduled-task:reports";
      const runId = `${id}:${CANONICAL}`;
      const sentMessage = `scheduled-task-check-message:${runId}:0`;
      const reason = "Spectrum could not deliver its report after 3 attempts";
      const needsYou = new Map<string, string>();
      yield* Effect.gen(function* () {
        yield* ensureSpectraFixture;
        yield* createThreadIn(worktree);
        yield* insertTask({ id, schedule: { type: "interval", everyMs: 3_600_000 }, next: null });
        // The scheduler's own work completed.
        yield* persist("reports:work", [
          runEvent("run.created", threadRun("run:reports:work", sentMessage, "completed"), "work"),
        ]);
        yield* writeCheckState(null, checkState(id, [sentRun(runId, sentMessage)]));
        // A Spectrum the run started is still deliberating.
        const spectrum = yield* bindSpectrum({
          callerThreadId: threadId,
          schedulerRunId: runId,
          status: "active",
        });
        const checks = yield* ScheduledTaskChecks.ScheduledTaskChecks;
        const service = yield* ScheduledTaskService.ScheduledTaskService;
        yield* checks.reconcile;
        const waiting = yield* runOf(id);
        assert.equal(waiting.stage, "running");
        assert.isTrue(waiting.awaitingReports === true);
        // Its report's last attempt failed and recovery concluded: Spectrum will not try again.
        yield* bindSpectrum({
          callerThreadId: threadId,
          schedulerRunId: runId,
          status: "settled",
          report: "a3",
          spectrumThreadId: spectrum,
        });
        yield* reportReceipt(threadId, "a3", "accepted");
        const report = threadRun("run:reports:a3", "report-message:a3", "failed", 2);
        yield* persist("reports:a3", [runEvent("run.created", report, "a3")]);
        yield* History.writeRecoveryOutcome({
          sourceRunId: report.id,
          threadId,
          status: "decided",
          outcome: "not_retryable",
          reason: "non_mcp",
        });
        needsYou.set("report:a3", reason);
        yield* checks.reconcile;
        const needs = yield* runOf(id);
        assert.equal(needs.stage, "needs-you");
        assert.equal(needs.error, `Spectrum report: ${reason}`);
        const listed = (yield* service.list()).tasks.find((task) => task.id === id);
        assert.equal(listed?.outcomeCheck?.run?.stage, "needs-you");
        assert.equal(listed?.lastRunError, `Needs you: Spectrum report: ${reason}`);
        // Run now while the report still needs you is refused, and nothing is sent.
        const before = (yield* messagesOf).length;
        const refused = yield* Effect.exit(service.runNow({ id: ScheduledTaskId.make(id) }));
        assert.isTrue(Exit.isFailure(refused));
        assert.include(String(refused), "The Spectrum report still needs you");
        // The user abandons that report; Run now resumes without messaging the agent.
        yield* bindSpectrum({
          callerThreadId: threadId,
          schedulerRunId: runId,
          status: "settled",
          report: "a3",
          abandoned: "a3",
          spectrumThreadId: spectrum,
        });
        yield* service.runNow({ id: ScheduledTaskId.make(id) });
        assert.equal((yield* runOf(id)).stage, "running");
        yield* checks.reconcile;
        assert.equal((yield* runOf(id)).stage, "done");
        assert.equal((yield* messagesOf).length, before);
      }).pipe(
        Effect.provide(
          runtime(database, { check: Effect.succeed(0), spectra: fixtureSpectra(needsYou) }),
        ),
        Effect.scoped,
      );
    }).pipe(Effect.scoped),
);

it.effect(
  "a report that needs you before any passing check makes the run need you without running the failing check",
  () =>
    Effect.gen(function* () {
      const database = yield* tempDatabase;
      const worktree = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "chromeria-checks-wt-"));
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => NodeFS.rmSync(worktree, { recursive: true, force: true })),
      );
      yield* TestClock.setTime(Date.parse(CANONICAL));
      const id = "scheduled-task:early-report";
      const runId = `${id}:${CANONICAL}`;
      const sentMessage = `scheduled-task-check-message:${runId}:0`;
      const reason = "The Spectrum report run failed and will not be retried";
      let checksRun = 0;
      // The check would fail, so a check would lead to a retry.
      const check = Effect.sync(() => {
        checksRun += 1;
        return 1;
      });
      yield* Effect.gen(function* () {
        yield* ensureSpectraFixture;
        yield* createThreadIn(worktree);
        yield* insertTask({ id, schedule: { type: "interval", everyMs: 3_600_000 }, next: null });
        // The scheduler's own work ended idle, with no check run yet.
        yield* persist("early:work", [
          runEvent("run.created", threadRun("run:early:work", sentMessage, "completed"), "work"),
        ]);
        yield* writeCheckState(null, checkState(id, [sentRun(runId, sentMessage)]));
        yield* bindSpectrum({
          callerThreadId: threadId,
          schedulerRunId: runId,
          status: "settled",
          report: "early",
        });
        yield* reportReceipt(threadId, "early", "accepted");
        yield* persist("early:report", [
          runEvent(
            "run.created",
            threadRun("run:early:report", "report-message:early", "failed", 2),
            "report",
          ),
        ]);
        const before = (yield* messagesOf).length;
        const checks = yield* ScheduledTaskChecks.ScheduledTaskChecks;
        yield* checks.reconcile;
        const needs = (yield* readCheckState(ScheduledTaskId.make(id)))!.runs[0]!;
        assert.equal(needs.stage, "needs-you");
        assert.equal(needs.error, `Spectrum report: ${reason}`);
        assert.equal(needs.attempt, 0);
        assert.equal(checksRun, 0, "the failing check never ran");
        yield* checks.reconcile;
        assert.equal(checksRun, 0);
        assert.equal((yield* messagesOf).length, before, "nothing was resent");
      }).pipe(
        Effect.provide(
          runtime(database, {
            check,
            spectra: fixtureSpectra(new Map([["report:early", reason]])),
          }),
        ),
        Effect.scoped,
      );
    }).pipe(Effect.scoped),
);

it.effect(
  "after a crash, a recorded retry send is never redelivered over a report that needs you",
  () =>
    Effect.gen(function* () {
      const database = yield* tempDatabase;
      const worktree = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "chromeria-checks-wt-"));
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => NodeFS.rmSync(worktree, { recursive: true, force: true })),
      );
      yield* TestClock.setTime(Date.parse(CANONICAL));
      const id = "scheduled-task:crash-report";
      const runId = `${id}:${CANONICAL}`;
      const firstMessage = `scheduled-task-check-message:${runId}:0`;
      const retryMessage = `scheduled-task-check-message:${runId}:1`;
      const reason = "Spectrum could not deliver its report after 3 attempts";
      let checksRun = 0;
      const check = Effect.sync(() => {
        checksRun += 1;
        return 1;
      });
      const spectra = fixtureSpectra(new Map([["report:crash", reason]]));
      // Before the crash: the first send's run started a Spectrum and ended; a failed
      // check recorded a retry send that was never submitted.
      yield* Effect.gen(function* () {
        yield* ensureSpectraFixture;
        yield* createThreadIn(worktree);
        yield* insertTask({ id, schedule: { type: "interval", everyMs: 3_600_000 }, next: null });
        yield* persist("crash:work", [
          runEvent("run.created", threadRun("run:crash:work", firstMessage, "completed"), "work"),
        ]);
        const first = sentRun(runId, firstMessage);
        yield* writeCheckState(
          null,
          checkState(id, [
            {
              ...first,
              attempt: 1,
              sends: [
                ...first.sends,
                {
                  index: 1,
                  commandId: CommandId.make(`scheduled-task-check:${runId}:1`),
                  messageId: MessageId.make(retryMessage),
                  kind: "continue",
                  createdAt: CANONICAL,
                  payload: {
                    text: "Keep working: the check still fails.",
                    modelSelection: null,
                    launch: null,
                    projectId,
                  },
                },
              ],
            },
          ]),
        );
        yield* bindSpectrum({
          callerThreadId: threadId,
          schedulerRunId: runId,
          status: "settled",
          report: "crash",
        });
        yield* reportReceipt(threadId, "crash", "accepted");
        yield* persist("crash:report", [
          runEvent(
            "run.created",
            threadRun("run:crash:report", "report-message:crash", "failed", 2),
            "report",
          ),
        ]);
      }).pipe(Effect.provide(runtime(database, { check, spectra })), Effect.scoped);
      // Restart: the report needs you, so the run does; the recorded send stays unsent.
      yield* Effect.gen(function* () {
        const before = yield* messagesOf;
        const checks = yield* ScheduledTaskChecks.ScheduledTaskChecks;
        yield* checks.reconcile;
        yield* checks.reconcile;
        const needs = (yield* readCheckState(ScheduledTaskId.make(id)))!.runs[0]!;
        assert.equal(needs.stage, "needs-you");
        assert.equal(needs.error, `Spectrum report: ${reason}`);
        assert.equal(checksRun, 0, "no check ran");
        const after = yield* messagesOf;
        assert.deepEqual(after, before);
        assert.notInclude(after, retryMessage);
      }).pipe(Effect.provide(runtime(database, { check, spectra })), Effect.scoped);
    }).pipe(Effect.scoped),
);

it.effect(
  "a direct upsert never turns a checked task into a webhook, refuses a past one-shot, and places weekly times",
  () =>
    Effect.gen(function* () {
      const database = yield* tempDatabase;
      yield* TestClock.setTime(Date.parse(CANONICAL));
      const id = ScheduledTaskId.make("scheduled-task:rpc");
      yield* Effect.gen(function* () {
        yield* createThread;
        yield* insertTask({ id, schedule: { type: "interval", everyMs: 3_600_000 }, next: null });
        yield* writeCheckState(null, checkState(id));
        const service = yield* ScheduledTaskService.ScheduledTaskService;
        const input = {
          id,
          title: "Checked work",
          prompt: "Do the scheduled work",
          enabled: true,
          projectId,
          threadId,
          workspaceStrategy: { type: "root" as const },
          modelSelection: selection,
          runtimeMode: "full-access" as const,
          interactionMode: "default" as const,
        };
        const sql = yield* SqlClient.SqlClient;
        const scheduleOf = sql<{ readonly schedule_json: string }>`
          SELECT schedule_json FROM scheduled_tasks WHERE task_id = ${id}`.pipe(
          Effect.map((rows) => rows[0]?.schedule_json),
        );
        const before = yield* scheduleOf;
        // The RPC path cannot strip a task's check by making it a webhook.
        const webhook = yield* Effect.exit(
          service.upsert({ ...input, schedule: { type: "webhook", signature: null } }),
        );
        assert.isTrue(Exit.isFailure(webhook));
        assert.include(String(webhook), "Webhook tasks cannot have an outcome check");
        assert.equal(yield* scheduleOf, before);
        const past = yield* Effect.exit(
          service.upsert({ ...input, schedule: { type: "once", at: "2026-10-08T11:00:00Z" } }),
        );
        assert.isTrue(Exit.isFailure(past));
        assert.include(String(past), "The one-shot time is in the past.");
        assert.equal(yield* scheduleOf, before);
        const future = yield* service.upsert({
          ...input,
          schedule: { type: "once", at: "2026-10-09T09:00:00+02:00" },
        });
        assert.equal(future.task.schedule.type, "once");
        assert.equal(future.task.nextRunAt, "2026-10-09T07:00:00.000Z");
        // The server places each weekly time and reports its pick.
        const weekly = yield* service.upsert({
          ...input,
          schedule: { type: "weekly", weekdays: [1], times: ["09:00"], timeZone: "UTC" },
        });
        assert.deepEqual(
          weekly.task.schedule.type === "weekly" ? weekly.task.schedule.chosen : undefined,
          [{ requested: "09:00", offsetMinutes: 0 }],
        );
      }).pipe(Effect.provide(runtime(database)), Effect.scoped);
    }).pipe(Effect.scoped),
);

it.effect(
  "a new-thread launch the orchestrator rejected before its thread existed is launched again after a restart, once",
  () =>
    Effect.gen(function* () {
      const database = yield* tempDatabase;
      yield* TestClock.setTime(Date.parse(CANONICAL));
      const id = ScheduledTaskId.make("scheduled-task:relaunch");
      const runId = `${id}:${CANONICAL}`;
      const runThread = ThreadId.make(`scheduled-run:${runId}`);
      const attempts: Array<{
        readonly commandId: string;
        readonly threadId: string | undefined;
        readonly launch: unknown;
        readonly message: unknown;
      }> = [];
      // The real launch's receipt contract on the real orchestrator: its first
      // `thread.create` is refused before any shell exists, the next succeeds.
      const launch =
        ({
          sink,
          orchestrator,
        }: {
          readonly sink: EventSink.EventSinkV2["Service"];
          readonly orchestrator: Orchestrator.OrchestratorV2["Service"];
        }): ThreadLaunchService.ThreadLaunchService["Service"]["launch"] =>
        (input) =>
          Effect.gen(function* () {
            const threadId = input.threadId!;
            attempts.push({
              commandId: input.commandId,
              threadId: input.threadId,
              launch: {
                projectId: input.projectId,
                title: input.title,
                modelSelection: input.modelSelection,
                runtimeMode: input.runtimeMode,
                interactionMode: input.interactionMode,
                workspaceStrategy: input.workspaceStrategy,
              },
              message: {
                text: input.initialMessage?.text,
                messageId: input.initialMessage?.messageId,
              },
            });
            if (attempts.length === 1) {
              yield* sink
                .commitRejectedCommand({
                  commandId: input.commandId,
                  threadId,
                  commandType: "thread.create",
                  rejectedAt: yield* DateTime.now,
                  error: "The orchestrator refused the thread.",
                })
                .pipe(Effect.orDie);
              return yield* new ThreadLaunchService.ThreadLaunchError({
                operation: "create-thread",
                commandId: input.commandId,
                projectId: input.projectId,
                threadId,
                cause: "The orchestrator refused the thread.",
              });
            }
            yield* orchestrator
              .dispatch({
                type: "thread.create",
                commandId: input.commandId,
                threadId,
                projectId: input.projectId,
                title: input.title,
                modelSelection: input.modelSelection,
                runtimeMode: input.runtimeMode,
                interactionMode: input.interactionMode,
                branch: null,
                worktreePath: null,
                createdBy: input.createdBy,
                creationSource: input.creationSource,
              })
              .pipe(Effect.orDie);
            yield* orchestrator
              .dispatch({
                type: "message.dispatch",
                commandId: CommandId.make(`${input.commandId}:initial-message`),
                threadId,
                messageId: input.initialMessage!.messageId!,
                text: input.initialMessage!.text,
                ...(input.initialMessage!.scheduledTaskId === undefined
                  ? {}
                  : { scheduledTaskId: input.initialMessage!.scheduledTaskId }),
                attachments: [],
                modelSelection: input.modelSelection,
                dispatchMode: { type: "defer_start" },
                createdBy: input.createdBy,
                creationSource: input.creationSource,
              })
              .pipe(Effect.orDie);
            return {
              threadId,
              projection: yield* orchestrator.getThreadProjection(threadId).pipe(Effect.orDie),
              resumed: false,
            };
          });
      const receiptOf = (commandId: string) =>
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const rows = yield* sql<{ readonly status: string }>`
            SELECT status FROM orchestration_command_receipts WHERE command_id = ${commandId}`;
          return rows[0]?.status ?? null;
        });
      const shellExists = Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const rows = yield* sql`
          SELECT 1 FROM orchestration_v2_projection_threads WHERE thread_id = ${runThread}`;
        return rows.length > 0;
      });
      // First server: Run now launches a new thread, and the orchestrator refuses it.
      yield* Effect.gen(function* () {
        yield* insertTask({
          id,
          schedule: { type: "interval", everyMs: 3_600_000 },
          next: null,
          threadId: null,
        });
        yield* writeCheckState(null, checkState(id));
        const service = yield* ScheduledTaskService.ScheduledTaskService;
        yield* Effect.exit(service.runNow({ id }));
        const refused = yield* runOf(id);
        assert.equal(refused.stage, "retry");
        assert.equal(refused.threadId, runThread);
        assert.equal(refused.sends.length, 1);
        assert.equal(yield* receiptOf(refused.sends[0]!.commandId), "rejected");
        assert.isFalse(yield* shellExists);
      }).pipe(Effect.provide(runtime(database, { launch })), Effect.scoped);
      // Restart after the retry delay: the thread was never created, so it is launched again.
      yield* TestClock.adjust("31 seconds");
      yield* Effect.gen(function* () {
        const checks = yield* ScheduledTaskChecks.ScheduledTaskChecks;
        yield* checks.reconcile;
        const relaunched = yield* runOf(id);
        assert.equal(relaunched.stage, "running", "a never-created thread is not unavailable");
        assert.equal(relaunched.sends.length, 2);
        const [first, second] = relaunched.sends;
        assert.notEqual(second!.commandId, first!.commandId);
        assert.notEqual(second!.messageId, first!.messageId);
        assert.equal(yield* receiptOf(first!.commandId), "rejected");
        assert.equal(yield* receiptOf(second!.commandId), "accepted");
        assert.isTrue(yield* shellExists);
        // Both attempts launched the same thread with the same launch fields.
        assert.equal(attempts.length, 2);
        assert.deepEqual(
          attempts.map((attempt) => attempt.threadId),
          [runThread, runThread],
        );
        assert.deepEqual(attempts[1]!.launch, attempts[0]!.launch);
        assert.deepEqual(
          attempts.map((attempt) => attempt.commandId),
          [first!.commandId, second!.commandId],
        );
        assert.deepEqual(yield* messagesOfThread(runThread), [second!.messageId]);
      }).pipe(Effect.provide(runtime(database, { launch })), Effect.scoped);
      // Another restart replays nothing: no second launch and no duplicate message.
      yield* Effect.gen(function* () {
        const checks = yield* ScheduledTaskChecks.ScheduledTaskChecks;
        yield* checks.reconcile;
        yield* checks.reconcile;
        assert.equal(attempts.length, 2);
        assert.equal((yield* runOf(id)).sends.length, 2);
        assert.equal((yield* messagesOfThread(runThread)).length, 1);
        // Once the thread existed, losing it is not a missing launch: the run needs you.
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        yield* orchestrator.dispatch({
          type: "thread.delete",
          commandId: CommandId.make("relaunch:delete"),
          threadId: runThread,
        });
        yield* checks.reconcile;
        const lost = yield* runOf(id);
        assert.equal(lost.stage, "needs-you");
        assert.equal(lost.sends.length, 2);
        assert.equal(attempts.length, 2);
      }).pipe(Effect.provide(runtime(database, { launch })), Effect.scoped);
    }).pipe(Effect.scoped),
);

it.effect(
  "a report whose real receipt is accepted and whose turn completed releases the run; absent or rejected holds",
  () =>
    Effect.gen(function* () {
      const database = yield* tempDatabase;
      const worktree = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "chromeria-checks-wt-"));
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => NodeFS.rmSync(worktree, { recursive: true, force: true })),
      );
      yield* TestClock.setTime(Date.parse(CANONICAL));
      const id = "scheduled-task:receipts";
      const runId = `${id}:${CANONICAL}`;
      const sentMessage = `scheduled-task-check-message:${runId}:0`;
      yield* Effect.gen(function* () {
        yield* ensureSpectraFixture;
        yield* createThreadIn(worktree);
        yield* insertTask({ id, schedule: { type: "interval", everyMs: 3_600_000 }, next: null });
        yield* persist("receipts:work", [
          runEvent("run.created", threadRun("run:receipts:work", sentMessage, "completed"), "work"),
        ]);
        yield* writeCheckState(null, checkState(id, [sentRun(runId, sentMessage)]));
        const spectrum = yield* bindSpectrum({
          callerThreadId: threadId,
          schedulerRunId: runId,
          status: "settled",
          report: "first",
        });
        // The report's turn completed, but its dispatch has no receipt yet: hold.
        yield* persist("receipts:report", [
          runEvent(
            "run.created",
            threadRun("run:receipts:report", "report-message:first", "completed", 2),
            "report",
          ),
        ]);
        const checks = yield* ScheduledTaskChecks.ScheduledTaskChecks;
        yield* checks.reconcile;
        assert.equal((yield* runOf(id)).stage, "running");
        // A rejected attempt holds too, until Spectrum's next attempt delivers.
        yield* reportReceipt(threadId, "first", "rejected");
        yield* checks.reconcile;
        assert.equal((yield* runOf(id)).stage, "running");
        yield* bindSpectrum({
          callerThreadId: threadId,
          schedulerRunId: runId,
          status: "settled",
          report: "second",
          spectrumThreadId: spectrum,
        });
        yield* reportReceipt(threadId, "second", "accepted");
        yield* persist("receipts:second", [
          runEvent(
            "run.created",
            threadRun("run:receipts:second", "report-message:second", "completed", 3),
            "second",
          ),
        ]);
        yield* checks.reconcile;
        assert.equal((yield* runOf(id)).stage, "done");
      }).pipe(
        Effect.provide(runtime(database, { check: Effect.succeed(0), spectra: fixtureSpectra() })),
        Effect.scoped,
      );
    }).pipe(Effect.scoped),
);

it.effect(
  "the live task subscription carries no command output, while the list and the agent tools keep it",
  () =>
    Effect.gen(function* () {
      const database = yield* tempDatabase;
      yield* TestClock.setTime(Date.parse(CANONICAL));
      const id = ScheduledTaskId.make("scheduled-task:output");
      const runId = `${id}:${CANONICAL}`;
      const marker = "UNIQUE-OUTPUT-MARKER-174";
      yield* Effect.gen(function* () {
        yield* insertTask({ id, schedule: { type: "interval", everyMs: 3_600_000 }, next: null });
        yield* writeCheckState(null, {
          ...checkState(id, [
            {
              id: runId,
              slot: CANONICAL,
              checkVersion: null,
              threadId: null,
              checkCwd: null,
              stage: "done",
              attempt: 0,
              retryAt: null,
              hasWork: false,
              sends: [],
              error: null,
              check: null,
              commandResult: { exitCode: 0, timedOut: false, endedAt: CANONICAL, output: marker },
            },
          ]),
          kind: "command",
          command: "make backup",
          checks: [],
          lastSuccessfulRunId: runId,
        });
        const service = yield* ScheduledTaskService.ScheduledTaskService;
        const live = yield* service.subscribeList().pipe(Stream.runHead);
        const liveTask = Option.getOrThrow(live).tasks.find((task) => task.id === id)!;
        // The compact state every client watches stays whole; only the output is gone.
        assert.equal(liveTask.command?.run?.id, runId);
        assert.equal(liveTask.command?.run?.exitCode, 0);
        assert.equal(liveTask.command?.lastSuccessfulRunId, runId);
        assert.notInclude(toJson(Option.getOrThrow(live)), marker);
        // The full list, which Settings fetches on demand, and the agent tools keep it.
        const listed = (yield* service.list()).tasks.find((task) => task.id === id)!;
        assert.equal(listed.command?.run?.output, marker);
        assert.include(toJson(ScheduledTaskChecks.forkSummaryFields(listed)), marker);
      }).pipe(Effect.provide(runtime(database)), Effect.scoped);
    }).pipe(Effect.scoped),
);

it.effect(
  "a task deleted between admitting a run and sending it sends nothing, and its fork state goes with it",
  () =>
    Effect.gen(function* () {
      const database = yield* tempDatabase;
      yield* TestClock.setTime(Date.parse(CANONICAL));
      const id = ScheduledTaskId.make("scheduled-task:deleted");
      const barrier = {
        admitted: yield* Deferred.make<void>(),
        proceed: yield* Deferred.make<void>(),
      };
      yield* Effect.gen(function* () {
        yield* createThread;
        yield* insertTask({ id, schedule: { type: "interval", everyMs: 3_600_000 }, next: null });
        yield* writeCheckState(null, checkState(id));
        const service = yield* ScheduledTaskService.ScheduledTaskService;
        const starting = yield* service.runNow({ id }).pipe(Effect.exit, Effect.forkScoped);
        // Admitted and recorded with its send, which has not gone out.
        yield* Deferred.await(barrier.admitted);
        assert.equal((yield* readCheckState(id))?.runs[0]?.sends.length, 1);
        yield* service.delete({ id });
        yield* Deferred.succeed(barrier.proceed, undefined);
        yield* Fiber.join(starting);
        assert.deepEqual(yield* messagesOf, []);
        assert.isNull(yield* readCheckState(id));
        const sql = yield* SqlClient.SqlClient;
        assert.deepEqual(yield* sql`SELECT task_id FROM scheduled_tasks WHERE task_id = ${id}`, []);
      }).pipe(Effect.provide(runtime(database, { dispatchBarrier: barrier })), Effect.scoped);
    }).pipe(Effect.scoped),
);

it.effect(
  "a judged thread's delete that races its run's start is refused, and anyone else's removes the task with its fork state",
  () =>
    Effect.gen(function* () {
      const database = yield* tempDatabase;
      yield* TestClock.setTime(Date.parse(CANONICAL));
      const id = ScheduledTaskId.make("scheduled-task:guarded");
      yield* Effect.gen(function* () {
        yield* createThread;
        yield* insertTask({ id, schedule: { type: "interval", everyMs: 3_600_000 }, next: null });
        yield* writeCheckState(null, checkState(id));
        const service = yield* ScheduledTaskService.ScheduledTaskService;
        const checks = yield* ScheduledTaskChecks.ScheduledTaskChecks;
        const existing = (yield* service.list()).tasks.find((task) => task.id === id)!;
        const reached = yield* Deferred.make<void>();
        const proceed = yield* Deferred.make<void>();
        // The tool's delete, held after its hook began and before upstream's delete.
        const remove = (input: { readonly id: ScheduledTaskId }) =>
          Deferred.succeed(reached, undefined).pipe(
            Effect.andThen(Deferred.await(proceed)),
            Effect.andThen(service.delete(input)),
          );
        const deleting = yield* checks
          .delete(
            { existing, parent: { thread: { id: threadId } } },
            remove,
          )({ id })
          .pipe(Effect.exit, Effect.forkScoped);
        yield* Deferred.await(reached);
        // Meanwhile the task's run starts on that same thread.
        yield* service.runNow({ id });
        assert.equal((yield* readCheckState(id))?.runs[0]?.threadId, threadId);
        yield* Deferred.succeed(proceed, undefined);
        const refused = yield* Fiber.join(deleting);
        assert.isTrue(Exit.isFailure(refused));
        assert.include(String(refused), "A judged thread cannot change its own scheduled task");
        assert.isNotNull(yield* readCheckState(id));
        assert.isDefined((yield* service.list()).tasks.find((task) => task.id === id));
        yield* checks.delete(
          { existing, parent: { thread: { id: ThreadId.make("thread:other") } } },
          service.delete,
        )({ id });
        assert.isNull(yield* readCheckState(id));
        assert.isUndefined((yield* service.list()).tasks.find((task) => task.id === id));
      }).pipe(Effect.provide(runtime(database)), Effect.scoped);
    }).pipe(Effect.scoped),
);

it.effect(
  "a report that comes to need you after Run now admits a send holds that send, and the run needs you",
  () =>
    Effect.gen(function* () {
      const database = yield* tempDatabase;
      yield* TestClock.setTime(Date.parse(CANONICAL));
      const id = "scheduled-task:boundary";
      const runId = `${id}:${CANONICAL}`;
      const sentMessage = `scheduled-task-check-message:${runId}:0`;
      const reason = "The Spectrum report run failed and will not be retried";
      const barrier = {
        admitted: yield* Deferred.make<void>(),
        proceed: yield* Deferred.make<void>(),
      };
      const bind = (report: string) =>
        bindSpectrum({
          callerThreadId: threadId,
          schedulerRunId: runId,
          status: "active",
          report,
          spectrumThreadId: "spectrum:boundary",
        });
      yield* Effect.gen(function* () {
        yield* ensureSpectraFixture;
        yield* createThread;
        yield* insertTask({ id, schedule: { type: "interval", everyMs: 3_600_000 }, next: null });
        // The run's work ended and its checks gave out, so it needs you.
        yield* persist("boundary:work", [
          runEvent("run.created", threadRun("run:boundary:work", sentMessage, "completed"), "bw"),
        ]);
        yield* writeCheckState(
          null,
          checkState(id, [
            { ...sentRun(runId, sentMessage), stage: "needs-you", error: "Outcome check failed" },
          ]),
        );
        // A bound Spectrum is still working on its first report.
        yield* bind("first");
        const before = yield* messagesOf;
        const service = yield* ScheduledTaskService.ScheduledTaskService;
        const resuming = yield* service
          .runNow({ id: ScheduledTaskId.make(id) })
          .pipe(Effect.exit, Effect.forkScoped);
        yield* Deferred.await(barrier.admitted);
        assert.equal((yield* runOf(id)).sends.length, 2);
        // Its next report needs you before the admitted send goes out.
        yield* bind("second");
        yield* Deferred.succeed(barrier.proceed, undefined);
        yield* Fiber.join(resuming);
        assert.deepEqual(yield* messagesOf, before);
        const held = yield* runOf(id);
        assert.equal(held.stage, "needs-you");
        assert.equal(held.error, `Spectrum report: ${reason}`);
        assert.equal(held.sends.length, 2, "the recorded send is kept");
      }).pipe(
        Effect.provide(
          runtime(database, {
            spectra: fixtureSpectra(new Map([["report:second", reason]])),
            dispatchBarrier: barrier,
          }),
        ),
        Effect.scoped,
      );
    }).pipe(Effect.scoped),
);

it.effect(
  "a send held for a waiting report stays unsent through reconcile and a restart, then lands once with its recorded id and text",
  () =>
    Effect.gen(function* () {
      const database = yield* tempDatabase;
      yield* TestClock.setTime(Date.parse(CANONICAL));
      const id = "scheduled-task:waiting";
      const runId = `${id}:${CANONICAL}`;
      const sentMessage = `scheduled-task-check-message:${runId}:0`;
      const heldMessage = `scheduled-task-check-message:${runId}:1`;
      const barrier = {
        admitted: yield* Deferred.make<void>(),
        proceed: yield* Deferred.make<void>(),
      };
      const spectrumThreadId = "spectrum:waiting";
      // Before the restart: Run now admits a send while a bound Spectrum still works on its report.
      yield* Effect.gen(function* () {
        yield* ensureSpectraFixture;
        yield* createThread;
        yield* insertTask({ id, schedule: { type: "interval", everyMs: 3_600_000 }, next: null });
        yield* persist("waiting:work", [
          runEvent("run.created", threadRun("run:waiting:work", sentMessage, "completed"), "ww"),
        ]);
        yield* writeCheckState(
          null,
          checkState(id, [
            { ...sentRun(runId, sentMessage), stage: "needs-you", error: "Outcome check failed" },
          ]),
        );
        yield* bindSpectrum({
          callerThreadId: threadId,
          schedulerRunId: runId,
          status: "active",
          report: "first",
          spectrumThreadId,
        });
        const service = yield* ScheduledTaskService.ScheduledTaskService;
        const resuming = yield* service
          .runNow({ id: ScheduledTaskId.make(id) })
          .pipe(Effect.exit, Effect.forkScoped);
        yield* Deferred.await(barrier.admitted);
        yield* Deferred.succeed(barrier.proceed, undefined);
        yield* Fiber.join(resuming);
        const checks = yield* ScheduledTaskChecks.ScheduledTaskChecks;
        yield* checks.reconcile;
        assert.notInclude(yield* messagesOf, heldMessage);
      }).pipe(
        Effect.provide(runtime(database, { spectra: fixtureSpectra(), dispatchBarrier: barrier })),
        Effect.scoped,
      );
      // After the restart: still held until the report's turn completes, then sent once.
      yield* Effect.gen(function* () {
        const checks = yield* ScheduledTaskChecks.ScheduledTaskChecks;
        yield* checks.reconcile;
        assert.notInclude(yield* messagesOf, heldMessage);
        const held = yield* runOf(id);
        assert.equal(held.stage, "running");
        assert.equal(held.sends.length, 2);
        const recorded = held.sends[1]!;
        assert.equal(recorded.messageId, heldMessage);
        yield* bindSpectrum({
          callerThreadId: threadId,
          schedulerRunId: runId,
          status: "settled",
          report: "first",
          spectrumThreadId,
        });
        yield* reportReceipt(threadId, "first", "accepted");
        yield* persist("waiting:report", [
          runEvent(
            "run.created",
            threadRun("run:waiting:report", "report-message:first", "completed", 2),
            "wr",
          ),
        ]);
        yield* checks.reconcile;
        yield* checks.reconcile;
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const projection = yield* orchestrator.getThreadProjection(threadId);
        const landed = projection.messages.filter((message) => message.id === heldMessage);
        assert.equal(landed.length, 1);
        assert.equal(landed[0]?.text, recorded.payload?.text);
        assert.deepEqual((yield* runOf(id)).sends, held.sends);
      }).pipe(Effect.provide(runtime(database, { spectra: fixtureSpectra() })), Effect.scoped);
    }).pipe(Effect.scoped),
);

it.effect(
  "the schedule tool's retried request id reaches the same task after a delete, and Run now at the same instant the same command run id",
  () =>
    Effect.gen(function* () {
      const database = yield* tempDatabase;
      const workspace = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "chromeria-checks-cmd-"));
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => NodeFS.rmSync(workspace, { recursive: true, force: true })),
      );
      yield* TestClock.setTime(Date.parse(CANONICAL));
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`INSERT INTO projection_projects ${sql.insert({
          project_id: projectId,
          title: "Checked project",
          workspace_root: workspace,
          default_model_selection_json: null,
          default_thread_env_mode: null,
          auto_pull: 0,
          favicon_path: null,
          project_icon_json: null,
          scripts_json: "[]",
          created_at: CANONICAL,
          updated_at: CANONICAL,
          deleted_at: null,
        })}`;
        const checks = yield* ScheduledTaskChecks.ScheduledTaskChecks;
        const service = yield* ScheduledTaskService.ScheduledTaskService;
        // The schedule tool's own call under one stable request key.
        const create = (command: string) =>
          checks.schedule(
            {
              input: { command, schedule: { type: "interval" }, clientRequestId: "nightly" },
              projectId,
              parent: undefined,
              bindToCurrentThread: false,
              scope: { requestNamespace: "caller" },
            },
            service.upsert,
          )({
            title: "Nightly backup",
            prompt: "Nightly backup",
            enabled: false,
            projectId,
            threadId: null,
            workspaceStrategy: { type: "root" },
            modelSelection: selection,
            runtimeMode: "full-access",
            interactionMode: "default",
            schedule: { type: "interval", everyMs: 3_600_000 },
          });
        const runToEnd = (id: ScheduledTaskId) =>
          Effect.gen(function* () {
            const ended = yield* awaitTask(id, (task) => task.command?.run?.stage === "done").pipe(
              Effect.forkScoped,
            );
            yield* service.runNow({ id });
            yield* Fiber.join(ended);
            return (yield* readCheckState(id))!.runs.at(-1)!;
          });
        const first = yield* create("printf first");
        const firstRun = yield* runToEnd(first.task.id);
        assert.equal(firstRun.commandResult?.output, "first");
        yield* service.delete({ id: first.task.id });
        assert.isNull(yield* readCheckState(first.task.id));
        const second = yield* create("printf second");
        assert.equal(second.task.id, first.task.id);
        // The clock has not moved: the new run takes the old run's id.
        const secondRun = yield* runToEnd(second.task.id);
        assert.equal(secondRun.id, firstRun.id);
        assert.equal(secondRun.commandResult?.output, "second");
      }).pipe(Effect.provide(runtime(database)), Effect.scoped);
    }).pipe(Effect.scoped),
);

it.effect(
  "an agent task recreated through the schedule tool with the same request id gets, at the same instant, the same run and send ids",
  () =>
    Effect.gen(function* () {
      const database = yield* tempDatabase;
      const worktree = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "chromeria-checks-wt-"));
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => NodeFS.rmSync(worktree, { recursive: true, force: true })),
      );
      yield* TestClock.setTime(Date.parse(CANONICAL));
      yield* Effect.gen(function* () {
        yield* createThreadIn(worktree);
        const checks = yield* ScheduledTaskChecks.ScheduledTaskChecks;
        const service = yield* ScheduledTaskService.ScheduledTaskService;
        // The schedule tool's own call from the bound thread, under one stable request key.
        const create = (prompt: string) =>
          checks.schedule(
            {
              input: {
                checkCommand: "test -f done",
                checkReason: "the work leaves done",
                schedule: { type: "interval" },
                clientRequestId: "nightly",
              },
              projectId,
              parent: { thread: { id: threadId } },
              bindToCurrentThread: true,
              scope: { requestNamespace: "caller" },
            },
            service.upsert,
          )({
            title: "Nightly work",
            prompt,
            enabled: false,
            projectId,
            threadId,
            workspaceStrategy: { type: "root" },
            modelSelection: selection,
            runtimeMode: "full-access",
            interactionMode: "default",
            schedule: { type: "interval", everyMs: 3_600_000 },
          });
        const first = yield* create("Old work");
        yield* service.runNow({ id: first.task.id });
        const old = (yield* readCheckState(first.task.id))!.runs[0]!;
        yield* service.delete({ id: first.task.id });
        const second = yield* create("New work");
        assert.equal(second.task.id, first.task.id);
        yield* service.runNow({ id: second.task.id });
        const replacement = (yield* readCheckState(second.task.id))!.runs[0]!;
        assert.equal(replacement.id, old.id);
        assert.equal(replacement.sends[0]!.commandId, old.sends[0]!.commandId);
        assert.equal(replacement.sends[0]!.messageId, old.sends[0]!.messageId);
      }).pipe(Effect.provide(runtime(database)), Effect.scoped);
    }).pipe(Effect.scoped),
);
