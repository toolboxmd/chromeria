import * as BareLaunch from "../orchestration-v2/BareLaunch.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  EventId,
  MessageId,
  ProviderDriverKind,
  ProviderInstanceId,
  RunId,
  ScheduledTaskId,
  ThreadId,
  type NodeId,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as Events from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as Projection from "../orchestration-v2/ProjectionStore.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as ThreadLaunchService from "../orchestration-v2/ThreadLaunchService.ts";
import { layer as serialization } from "../orchestration-v2/ThreadCommandExecutor.ts";
import type { McpInvocationScope } from "../mcp/McpInvocationContext.ts";
import * as Mcp from "../mcp/OrchestratorMcpService.ts";
import * as RemoteDelegation from "../peer/RemoteDelegation.ts";
import { initializeRecoveryHistory } from "../prism/RecoveryHistory.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import { reportsFence } from "../scheduledTaskChecks/handoff.ts";
import { ensureCheckSchema, writeCheckState } from "../scheduledTaskChecks/store.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as Settings from "../serverSettings.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as CommandPlans from "./commandPlan.ts";
import * as Controller from "./Controller.ts";
import { base, input, setup, spareInstanceId } from "./controllerTestkit.ts";
import * as Launch from "./LaunchService.ts";
import * as McpInterrupt from "./mcpInterrupt.ts";
import * as Reports from "./ReportService.ts";
import * as Round from "./RoundService.ts";
import * as Adapter from "./SchedulerAdapter.ts";
import { readSpectrum } from "./store.ts";
import { makeRun, NOW } from "./testFixtures.ts";
import * as Transcript from "./TranscriptService.ts";

const dependencies = Layer.mergeAll(base, serialization);
const services = Layer.mergeAll(
  Launch.layer.pipe(Layer.provide(Layer.mock(BareLaunch.BareLaunch)({}))),
  Round.layer,
  Transcript.layer,
  Reports.layer,
).pipe(Layer.provideMerge(dependencies));
const spectrum = Layer.mergeAll(
  CommandPlans.layer.pipe(Layer.provide(dependencies)),
  McpInterrupt.layer.pipe(Layer.provide(dependencies)),
  Adapter.layer,
  Controller.layer.pipe(Layer.provideMerge(services)),
);
const runtime = Layer.fresh(
  Mcp.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(BareLaunch.BareLaunch)({}),
        Layer.mock(RemoteDelegation.RemoteDelegation)({}),
        Layer.mock(ThreadLaunchService.ThreadLaunchService)({}),
        Layer.mock(GitVcsDriver.GitVcsDriver)({}),
        Layer.mock(ProjectService.ProjectService)({}),
        Layer.mock(SecretRequests.SecretRequests)({}),
        Layer.mock(ScheduledTaskService.ScheduledTaskService)({}),
        Settings.layerTest(),
        NodeServices.layer,
      ),
    ),
    Layer.provideMerge(spectrum),
  ),
);

const scopeOf = (threadId: ThreadId): McpInvocationScope => ({
  environmentId: EnvironmentId.make("environment:spectrum-task"),
  requestNamespace: `provider-session:${threadId}`,
  thread: {
    threadId,
    providerSessionId: `provider-session:${threadId}`,
    providerInstanceId: ProviderInstanceId.make("codex"),
  },
  client: undefined,
  capabilities: new Set(["orchestration"]),
  issuedAt: 1,
});
const callerScope = scopeOf(input.callerThreadId);
const agentStart = { ...input, delegatedTask: true };
const read = (id: ThreadId) => readSpectrum(id).pipe(Effect.map(Option.getOrThrow));

const ended = Effect.fn("test.SpectrumTask.ended")(function* (
  run: OrchestrationV2Run,
  status: "completed",
  text?: string,
) {
  yield* (yield* Events.EventSinkV2).write({
    events: [
      {
        id: EventId.make(`${run.id}:${status}`),
        type: "run.updated",
        threadId: run.threadId,
        occurredAt: NOW,
        payload: { ...run, status, completedAt: NOW },
      },
      ...(text === undefined
        ? []
        : [
            {
              id: EventId.make(`${run.id}:reply`),
              type: "message.updated" as const,
              threadId: run.threadId,
              occurredAt: NOW,
              payload: {
                id: MessageId.make(`${run.id}:reply`),
                threadId: run.threadId,
                runId: run.id,
                nodeId: null,
                createdBy: "agent" as const,
                creationSource: "provider" as const,
                role: "assistant" as const,
                text,
                attachments: [],
                streaming: false,
                createdAt: NOW,
                updatedAt: NOW,
              },
            },
          ]),
    ],
  });
});

/** A Color run that lost its work and whose native restart declined ends the Spectrum as failed. */
const declinedRestart = Effect.fn("test.SpectrumTask.declinedRestart")(function* (
  run: OrchestrationV2Run,
) {
  const commandId = CommandId.make(`command:runtime-reconcile:fixture:${run.id}`);
  const effectId = `effect:restart-continuation:${run.id}`;
  yield* (yield* Events.EventSinkV2).commitCommand({
    commandId,
    threadId: run.threadId,
    commandType: "fixture",
    acceptedAt: NOW,
    events: [
      {
        id: EventId.make(`${run.id}:lost-work`),
        type: "run.updated",
        threadId: run.threadId,
        occurredAt: NOW,
        payload: {
          ...run,
          status: "cancelled",
          completedAt: NOW,
          restartCancelledBackgroundWork: [{ kind: "monitor", id: "lost", label: "Awaited work" }],
        },
      },
    ],
    effects: [
      {
        id: effectId,
        commandId,
        threadId: run.threadId,
        request: { type: "provider-runtime.continue", sourceRunId: run.id },
      },
    ],
  });
  const sql = yield* SqlClient.SqlClient;
  yield* sql`UPDATE orchestration_v2_effect_outbox SET status='succeeded' WHERE effect_id=${effectId}`;
});

/** Answers every open slot until the Spectrum leaves the active state. */
const runToReport = Effect.fn("test.SpectrumTask.runToReport")(function* (threadId: ThreadId) {
  const controller = yield* Controller.SpectrumController;
  const projections = yield* Projection.ProjectionStoreV2;
  let current = yield* controller.resume(threadId);
  while (current.status === "active") {
    const slot = current.round!.slots.find((candidate) => candidate.replyIds === null)!;
    const child = yield* projections.getThreadRecords(slot.threadId, ["runs"]);
    yield* ended(
      child.runs.find((run) => run.id === slot.runId)!,
      "completed",
      "reply",
    );
    current = yield* controller.resume(threadId);
  }
  return current;
});

const taskEvents = Effect.fn("test.SpectrumTask.events")(function* (taskId: NodeId) {
  const stored = yield* (yield* EventStore.EventStoreV2)
    .read({ afterSequence: 0, limit: 10_000 })
    .pipe(Stream.runCollect);
  return Array.from(stored).filter(
    (row) => row.event.type === "subagent.updated" && row.event.payload.id === taskId,
  );
});

const resumeCaller = Effect.fn("test.SpectrumTask.resumeCaller")(function* (
  caller: Effect.Success<typeof setup>,
) {
  const token = CommandId.make("caller:restart:stop");
  yield* (yield* Events.EventSinkV2).write({
    events: [
      {
        id: EventId.make("caller:resumed"),
        type: "thread.metadata-updated",
        threadId: caller.id,
        occurredAt: NOW,
        payload: { ...caller, forkRetirement: { token }, forkResumedRetirements: [token] },
      },
    ],
  });
});

it.effect(
  "an agent-started council from a resumed caller is its delegated task until the report, which is the only wake",
  () =>
    Effect.gen(function* () {
      yield* resumeCaller(yield* setup);
      yield* initializeRecoveryHistory;
      const launch = yield* Launch.SpectrumLaunchService;
      const service = yield* Mcp.OrchestratorMcpService;
      const projections = yield* Projection.ProjectionStoreV2;
      const state = yield* launch.register(agentStart);
      const taskId = state.delegatedTaskId!;
      assert.isNotNull(state.delegatedTaskId);
      assert.deepStrictEqual(yield* launch.register(agentStart), state);
      assert.strictEqual((yield* taskEvents(taskId)).length, 1);
      const rows = (yield* projections.getThreadRecords(input.callerThreadId, ["subagents"]))
        .subagents;
      assert.deepStrictEqual(
        rows.map((task) => [task.id, task.origin, task.childThreadId, task.status]),
        [[taskId, "app_owned", state.threadId, "running"]],
      );

      const working = yield* service.taskStatus(callerScope, taskId);
      assert.strictEqual(working.status, "running");
      assert.strictEqual(working.childThreadId, state.threadId);
      assert.isNull(working.summary);

      const other = ThreadId.make("unrelated");
      const caller = yield* projections.getThread(input.callerThreadId);
      yield* (yield* Events.EventSinkV2).write({
        events: [
          {
            id: EventId.make("unrelated:create"),
            type: "thread.created",
            threadId: other,
            occurredAt: NOW,
            payload: {
              ...caller,
              id: other,
              lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: other },
            },
          },
        ],
      });
      const denied = yield* service.taskStatus(scopeOf(other), taskId).pipe(Effect.flip);
      assert.strictEqual(denied.code, "task_not_found");

      const settled = yield* runToReport(state.threadId);
      assert.strictEqual(settled.status, "settled");
      const done = yield* service.taskStatus(callerScope, taskId);
      assert.strictEqual(done.status, "completed");
      assert.strictEqual(done.summary, settled.report!.text);
      assert.isTrue(done.summary!.startsWith("Spectrum completed."));

      const records = yield* projections.getThreadRecords(input.callerThreadId, [
        "runs",
        "messages",
      ]);
      const reportRun = records.runs.find(
        (run) => run.userMessageId === settled.report!.messageId,
      )!;
      yield* ended(reportRun, "completed", "Report acknowledged.");
      yield* service.taskStatus(callerScope, taskId);
      const after = yield* projections.getThreadRecords(input.callerThreadId, ["messages", "runs"]);
      assert.deepStrictEqual(
        after.messages.filter((message) => message.role === "user").map((message) => message.id),
        [settled.report!.messageId],
      );
      assert.strictEqual(after.runs.length, 1);
    }).pipe(Effect.provide(runtime)),
);

it.effect("a user-started Spectrum has no delegated task row", () =>
  Effect.gen(function* () {
    yield* setup;
    const state = yield* (yield* Launch.SpectrumLaunchService).register(input);
    assert.isNull(state.delegatedTaskId);
    const projections = yield* Projection.ProjectionStoreV2;
    assert.deepStrictEqual(
      (yield* projections.getThreadRecords(input.callerThreadId, ["subagents"])).subagents,
      [],
    );
  }).pipe(Effect.provide(runtime)),
);

it.effect("task_cancel stops a Spectrum task mid-round and cancels every Color run", () =>
  Effect.gen(function* () {
    yield* setup;
    yield* initializeRecoveryHistory;
    const state = yield* (yield* Launch.SpectrumLaunchService).register(agentStart);
    const taskId = state.delegatedTaskId!;
    const controller = yield* Controller.SpectrumController;
    const running = yield* controller.resume(state.threadId);
    assert.isTrue(running.round!.slots.every((slot) => slot.runId !== null));
    const service = yield* Mcp.OrchestratorMcpService;
    const cancelled = yield* service.cancelTask(callerScope, { taskId });
    assert.strictEqual(cancelled.status, "cancel_requested");
    const retired = yield* controller.resume(state.threadId);
    assert.strictEqual(retired.status, "retired");
    const projections = yield* Projection.ProjectionStoreV2;
    for (const color of retired.participants) {
      const child = yield* projections.getThreadRecords(color.threadId, ["runs"]);
      assert.isNotEmpty(child.runs);
      assert.isTrue(
        child.runs.every((run) =>
          ["completed", "failed", "interrupted", "cancelled", "rolled_back"].includes(run.status),
        ),
      );
    }
    const status = yield* service.taskStatus(callerScope, taskId);
    assert.strictEqual(status.status, "cancelled");
    assert.strictEqual(status.summary, "Spectrum stopped.");
  }).pipe(Effect.provide(runtime)),
);

it.effect("task_cancel stops a Spectrum task that has no Color run between rounds", () =>
  Effect.gen(function* () {
    yield* setup;
    const state = yield* (yield* Launch.SpectrumLaunchService).register(agentStart);
    const service = yield* Mcp.OrchestratorMcpService;
    const cancelled = yield* service.cancelTask(callerScope, { taskId: state.delegatedTaskId! });
    assert.strictEqual(cancelled.status, "cancel_requested");
    assert.strictEqual((yield* read(state.threadId)).status, "retired");
    const status = yield* service.taskStatus(callerScope, state.delegatedTaskId!);
    assert.strictEqual(status.status, "cancelled");
    assert.strictEqual(status.summary, "Spectrum stopped.");
  }).pipe(Effect.provide(runtime)),
);

it.effect("a failed Color and a retired caller each leave a terminal row with a result", () =>
  Effect.gen(function* () {
    const caller = yield* setup;
    yield* initializeRecoveryHistory;
    const launch = yield* Launch.SpectrumLaunchService;
    const service = yield* Mcp.OrchestratorMcpService;
    const controller = yield* Controller.SpectrumController;
    const projections = yield* Projection.ProjectionStoreV2;

    const failing = yield* launch.register(agentStart);
    const slot = (yield* controller.resume(failing.threadId)).round!.slots[0]!;
    const child = yield* projections.getThreadRecords(slot.threadId, ["runs"]);
    yield* declinedRestart(child.runs.find((run) => run.id === slot.runId)!);
    assert.strictEqual((yield* controller.resume(failing.threadId)).status, "retired");
    const failed = yield* service.taskStatus(callerScope, failing.delegatedTaskId!);
    assert.strictEqual(failed.status, "failed");
    assert.strictEqual(failed.summary, "Spectrum could not finish: run-ended.");

    const retiring = yield* launch.register({
      ...agentStart,
      commandId: CommandId.make("register:retiring"),
      threadId: ThreadId.make("spectrum:retiring"),
    });
    yield* (yield* Events.EventSinkV2).write({
      events: [
        {
          id: EventId.make("caller:retire"),
          type: "thread.metadata-updated",
          threadId: caller.id,
          occurredAt: NOW,
          payload: {
            ...(yield* projections.getThread(caller.id)),
            forkRetirement: { token: CommandId.make("caller:stop") },
          },
        },
      ],
    });
    assert.strictEqual((yield* controller.resume(retiring.threadId)).status, "retired");
    const retired = yield* service.taskStatus(callerScope, retiring.delegatedTaskId!);
    assert.strictEqual(retired.status, "cancelled");
    assert.strictEqual(retired.summary, "Spectrum stopped.");
  }).pipe(Effect.provide(runtime)),
);

it.effect(
  "a Color refused before its run commits starts on its slot's fallback, never retrying the refused one",
  () =>
    Effect.gen(function* () {
      yield* setup;
      yield* initializeRecoveryHistory;
      const codex = ProviderInstanceId.make("codex");
      const spare = { instanceId: spareInstanceId, model: "test-model" };
      const state = yield* (yield* Launch.SpectrumLaunchService).register({
        ...agentStart,
        colors: [
          {
            label: "Blue",
            selection: { instanceId: codex, model: "test-model" },
            fallbacks: [{ selection: spare }],
          },
          {
            label: "Red",
            // Unavailable at registration: the slot starts on its fallback instead.
            selection: { instanceId: ProviderInstanceId.make("missing"), model: "test-model" },
            fallbacks: [{ selection: spare }],
          },
        ],
      });
      assert.deepStrictEqual(state.participants[1]!.selection, spare);
      assert.deepStrictEqual(state.participants[1]!.attempted, [
        { instanceId: ProviderInstanceId.make("missing"), model: "test-model" },
      ]);
      const projections = yield* Projection.ProjectionStoreV2;
      const projectId = (yield* projections.getThread(state.threadId)).projectId;
      const gate = Layer.fresh(Threads.layer).pipe(
        Layer.provideMerge(
          Settings.layerTest({
            providerInstances: {
              [codex]: { driver: ProviderDriverKind.make("codex"), enabled: true },
              [spareInstanceId]: { driver: ProviderDriverKind.make("codex"), enabled: true },
            },
            projectSettingsOverrides: {
              [projectId]: { providerInstances: { [codex]: { enabled: false } } },
            },
          }),
        ),
      );
      const blue = state.participants[0]!.threadId;
      yield* Effect.gen(function* () {
        const rounds = yield* Round.SpectrumRoundService;
        const prepared = yield* rounds.prepare(state.threadId);
        const refused = prepared.outbox.find(
          (command) => command.type === "message.dispatch" && command.threadId === blue,
        )!;
        const started = yield* rounds.dispatch(state.threadId, refused.commandId);
        const color = started.participants[0]!;
        assert.deepStrictEqual(color.selection, spare);
        assert.deepStrictEqual(color.attempted, [{ instanceId: codex, model: "test-model" }]);
        assert.deepStrictEqual(color.fallbacks, []);
        const slot = started.round!.slots.find((candidate) => candidate.threadId === blue)!;
        assert.notStrictEqual(slot.commandId, refused.commandId);
        assert.isNotNull(slot.runId);
        // A restarted controller reads the persisted choice and never re-sends the refused start.
        yield* Effect.gen(function* () {
          yield* (yield* Controller.SpectrumController).resume(state.threadId);
        }).pipe(Effect.provide(Layer.fresh(Controller.layer)));
        const runs = (yield* projections.getThreadRecords(blue, ["runs"])).runs;
        assert.deepStrictEqual(
          runs.map((run) => run.modelSelection.instanceId),
          [spareInstanceId],
        );
        assert.deepStrictEqual((yield* read(state.threadId)).participants[0]!.attempted, [
          { instanceId: codex, model: "test-model" },
        ]);
      }).pipe(Effect.provide(Layer.fresh(Round.layer).pipe(Layer.provideMerge(gate))));
    }).pipe(Effect.provide(runtime)),
);

it.effect("a Spectrum task of a scheduled run still holds that run until its report lands", () =>
  Effect.gen(function* () {
    yield* setup;
    yield* initializeRecoveryHistory;
    yield* ensureCheckSchema;
    const originMessage = MessageId.make("scheduler:origin");
    const origin = makeRun(
      {
        threadId: input.callerThreadId,
        commandId: CommandId.make("scheduler:send"),
        messageId: originMessage,
        runId: null,
        replyIds: null,
      },
      { id: RunId.make("scheduler:caller-run"), status: "completed", completedAt: NOW },
    );
    yield* (yield* Events.EventSinkV2).write({
      events: [
        {
          id: EventId.make("scheduler:origin"),
          type: "run.created",
          threadId: origin.threadId,
          occurredAt: NOW,
          payload: origin,
        },
      ],
    });
    yield* writeCheckState(null, {
      version: 1,
      taskId: ScheduledTaskId.make("scheduler:task"),
      revision: 0,
      kind: "agent",
      command: null,
      role: null,
      lane: null,
      checks: [],
      runs: [
        {
          id: "scheduler:run",
          slot: DateTime.formatIso(NOW),
          checkVersion: null,
          threadId: origin.threadId,
          checkCwd: null,
          stage: "running",
          attempt: 0,
          retryAt: null,
          hasWork: true,
          sends: [
            {
              index: 0,
              commandId: CommandId.make("scheduler:send"),
              messageId: originMessage,
              kind: "start",
              createdAt: DateTime.formatIso(NOW),
            },
          ],
          error: null,
          check: null,
        },
      ],
      failureStreak: 0,
      lastError: null,
      lastSuccessfulRunId: null,
    });
    const state = yield* (yield* Launch.SpectrumLaunchService).register({
      ...agentStart,
      callerRunId: origin.id,
    });
    assert.strictEqual(state.schedulerRunId, "scheduler:run");
    const projections = yield* Projection.ProjectionStoreV2;
    const row = (yield* projections.getThreadRecords(input.callerThreadId, ["subagents"]))
      .subagents[0]!;
    assert.strictEqual(row.runId, origin.id);
    assert.deepStrictEqual(yield* reportsFence("scheduler:run"), { kind: "waiting" });
    const settled = yield* runToReport(state.threadId);
    // The row already reads done; the scheduler still waits on the report itself.
    assert.strictEqual(
      (yield* (yield* Mcp.OrchestratorMcpService).taskStatus(callerScope, state.delegatedTaskId!))
        .status,
      "completed",
    );
    assert.deepStrictEqual(yield* reportsFence("scheduler:run"), { kind: "waiting" });
    const caller = yield* projections.getThreadRecords(origin.threadId, ["runs"]);
    const reportRun = caller.runs.find((run) => run.userMessageId === settled.report!.messageId)!;
    yield* ended(reportRun, "completed", "Report acknowledged.");
    assert.deepStrictEqual(yield* reportsFence("scheduler:run"), { kind: "released" });
  }).pipe(Effect.provide(runtime)),
);

const reportReachedCaller = Effect.fn("test.SpectrumTask.reportReachedCaller")(function* (
  threadId: ThreadId,
) {
  const delivered = yield* (yield* Reports.SpectrumReportService).deliver(threadId);
  const caller = yield* (yield* Projection.ProjectionStoreV2).getThreadRecords(
    input.callerThreadId,
    ["runs"],
  );
  return caller.runs.some((run) => run.userMessageId === delivered.report!.messageId);
});

it.effect("task_cancel after the report settles stops nothing and keeps the report's wake", () =>
  Effect.gen(function* () {
    yield* setup;
    yield* initializeRecoveryHistory;
    const state = yield* (yield* Launch.SpectrumLaunchService).register(agentStart);
    const settled = yield* (yield* Reports.SpectrumReportService).settle(
      state.threadId,
      "Spectrum completed.\n\nThe answer.",
    );
    const service = yield* Mcp.OrchestratorMcpService;
    const cancelled = yield* service.cancelTask(callerScope, { taskId: state.delegatedTaskId! });
    assert.strictEqual(cancelled.status, "completed");
    const after = yield* read(state.threadId);
    assert.strictEqual(after.status, "settled");
    assert.isTrue(after.outbox.some((command) => command.commandId === settled.report!.commandId));
    assert.isTrue(yield* reportReachedCaller(state.threadId));
  }).pipe(Effect.provide(runtime)),
);

it.effect("a Stop planned after the report settles still delivers that report", () =>
  Effect.gen(function* () {
    yield* setup;
    yield* initializeRecoveryHistory;
    const state = yield* (yield* Launch.SpectrumLaunchService).register(agentStart);
    yield* (yield* Reports.SpectrumReportService).settle(
      state.threadId,
      "Spectrum completed.\n\nThe answer.",
    );
    yield* (yield* Orchestrator.OrchestratorV2).dispatch({
      type: "thread.stop",
      threadId: state.threadId,
      commandId: CommandId.make("late:stop"),
    });
    assert.strictEqual((yield* read(state.threadId)).status, "retired");
    assert.isTrue(yield* reportReachedCaller(state.threadId));
    const status = yield* (yield* Mcp.OrchestratorMcpService).taskStatus(
      callerScope,
      state.delegatedTaskId!,
    );
    assert.strictEqual(status.status, "completed");
  }).pipe(Effect.provide(runtime)),
);

it.effect(
  "task_cancel racing the report's settle returns the settled status and stops nothing",
  () =>
    Effect.gen(function* () {
      yield* setup;
      yield* initializeRecoveryHistory;
      const state = yield* (yield* Launch.SpectrumLaunchService).register(agentStart);
      const reports = yield* Reports.SpectrumReportService;
      const hook = yield* McpInterrupt.SpectrumMcpInterrupt;
      // Settle commits after cancelTask has read the task as running, before the hook runs.
      const cancelled = yield* (yield* Mcp.OrchestratorMcpService)
        .cancelTask(callerScope, { taskId: state.delegatedTaskId! })
        .pipe(
          Effect.provideService(McpInterrupt.SpectrumMcpInterrupt, {
            ...hook,
            stop: (threads, request, commandId) =>
              reports
                .settle(state.threadId, "Spectrum completed.\n\nThe answer.")
                .pipe(Effect.orDie, Effect.andThen(hook.stop(threads, request, commandId))),
          }),
        );
      assert.strictEqual(cancelled.status, "completed");
      assert.strictEqual((yield* read(state.threadId)).status, "settled");
      assert.isTrue(yield* reportReachedCaller(state.threadId));
    }).pipe(Effect.provide(runtime)),
);
