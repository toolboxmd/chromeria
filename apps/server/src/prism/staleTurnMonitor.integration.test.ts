import { expect, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  RunAttemptId,
  RuntimeRequestId,
  ThreadId,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import * as Registry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import type { ProviderAdapterV2Shape } from "../orchestration-v2/ProviderAdapter.ts";
import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as Harness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as Persistence from "../persistence/Sqlite.ts";
import { ServerActivation } from "../serverActivation.ts";
import * as Monitor from "./staleTurnMonitor.ts";
import * as StreamClock from "./streamClock.ts";
import * as Stats from "./StreamStatsStore.ts";

const instanceId = ProviderInstanceId.make("codex");
const modelSelection = {
  instanceId,
  model: "fixed",
  options: [{ id: "reasoningEffort", value: "high" }],
};
const parentId = ThreadId.make("silence:parent");
const database = Persistence.layerMemory;
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("No live provider in this durable dispatch proof"),
} as ProviderAdapterV2Shape;
const replay = Harness.layerWithRegistry(
  { name: "silence" },
  Registry.layerFromAdapters([adapter]),
  { databaseLayer: database, runEffectWorker: false },
);
const clock = StreamClock.layer.pipe(Layer.provide(Stats.layer), Layer.provide(database));
const dependencies = Layer.mergeAll(
  replay,
  clock,
  Threads.layer.pipe(Layer.provide(replay)),
  Layer.succeed(ServerActivation, Effect.never),
  Layer.succeed(Monitor.StaleTurnMonotonicClock, Clock.currentTimeMillis),
);
const layer = Layer.mergeAll(dependencies, Monitor.layer.pipe(Layer.provide(dependencies)));

const setup = Effect.gen(function* () {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  yield* orchestrator.dispatch({
    type: "thread.create",
    commandId: CommandId.make("silence:create"),
    threadId: parentId,
    projectId: ProjectId.make("silence:project"),
    title: "Parent",
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdBy: "user",
    creationSource: "web",
  });
  yield* orchestrator.dispatch({
    type: "message.dispatch",
    commandId: CommandId.make("silence:start"),
    threadId: parentId,
    messageId: MessageId.make("silence:message"),
    text: "Delegate",
    attachments: [],
    dispatchMode: { type: "start_immediately" },
    createdBy: "user",
    creationSource: "web",
  });
  const parent = (yield* orchestrator.getThreadProjection(parentId)).runs[0]!;
  yield* orchestrator.dispatch({
    type: "delegated_task.request",
    commandId: CommandId.make("silence:delegate"),
    parentThreadId: parentId,
    parentRunId: parent.id,
    parentNodeId: parent.rootNodeId!,
    task: "Work",
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    completionWake: "always",
    createdBy: "agent",
    creationSource: "mcp",
  });
  const childId = (yield* orchestrator.getThreadProjection(parentId)).subagents[0]!.childThreadId!;
  const run = (yield* orchestrator.getThreadProjection(childId)).runs[0]!;
  const activeAttemptId = RunAttemptId.make("silence:attempt");
  const now = yield* DateTime.now;
  yield* (yield* EventSink.EventSinkV2).write({
    events: [
      {
        id: EventId.make("silence:running"),
        type: "run.updated",
        threadId: childId,
        occurredAt: now,
        payload: { ...run, status: "running", activeAttemptId, startedAt: now },
      },
    ],
  });
  yield* (yield* StreamClock.StreamClock).beginAttempt({
    threadId: childId,
    runId: run.id,
    runOrdinal: run.ordinal,
    attemptId: activeAttemptId,
    attemptOrdinal: 1,
    providerThreadId: ProviderThreadId.make("silence:native"),
    provider: adapter.driver,
    model: modelSelection.model,
  });
  return { orchestrator, childId, run, activeAttemptId };
});
const advanceSilence = (epoch: number) =>
  Effect.gen(function* () {
    const monitor = yield* Monitor.StaleTurnMonitor;
    // Drive real sweep drains at its cadence; a jump would correctly be host sleep.
    for (let second = 0; second <= 151; second += 1) {
      yield* TestClock.setTime(epoch + second * 1_000);
      yield* monitor.sweep;
    }
  });

it.effect("queues one parent notice with stored effort and leaves the silent child running", () =>
  Effect.gen(function* () {
    const epoch = yield* Clock.currentTimeMillis;
    const { orchestrator, childId, run } = yield* setup;
    yield* advanceSilence(epoch);
    yield* (yield* Monitor.StaleTurnMonitor).sweep;
    const parent = yield* orchestrator.getThreadProjection(parentId);
    const notices = parent.messages.filter((message) => message.senderThreadId === childId);
    expect(notices).toHaveLength(1);
    expect(notices[0]!.text).toContain("Reason: silence.");
    expect(notices[0]!.text).toContain(run.id);
    expect(notices[0]!.creationSource).toBe("server");
    expect(
      parent.runs.find((candidate) => candidate.userMessageId === notices[0]!.id),
    ).toMatchObject({ status: "queued", modelSelection });
    expect((yield* orchestrator.getThreadProjection(childId)).runs[0]).toMatchObject({
      id: run.id,
      status: "running",
    });
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "does not notify after a propagated Stop and preserves its original retirement token",
  () =>
    Effect.gen(function* () {
      const epoch = yield* Clock.currentTimeMillis;
      const { orchestrator, childId } = yield* setup;
      const token = CommandId.make("silence:stop");
      yield* orchestrator.dispatch({ type: "thread.stop", commandId: token, threadId: parentId });
      yield* advanceSilence(epoch);
      const parent = yield* orchestrator.getThreadProjection(parentId);
      expect(parent.thread.forkRetirement?.token).toBe(token);
      expect(parent.messages.filter((message) => message.senderThreadId === childId)).toEqual([]);
      expect(
        (yield* orchestrator.getThreadProjection(childId)).thread.forkResumedRetirements ?? [],
      ).not.toContain(token);
    }).pipe(Effect.provide(layer)),
);

it.effect("pending input and a replaced attempt cannot produce a silence notice", () =>
  Effect.gen(function* () {
    const epoch = yield* Clock.currentTimeMillis;
    const { orchestrator, childId, run } = yield* setup;
    const sink = yield* EventSink.EventSinkV2;
    const now = yield* DateTime.now;
    const request = {
      id: RuntimeRequestId.make("silence:input"),
      nodeId: NodeId.make("silence:input-node"),
      providerTurnId: null,
      nativeRequestRef: null,
      kind: "user_input" as const,
      status: "pending" as const,
      responseCapability: { type: "message" as const },
      createdAt: now,
      resolvedAt: null,
    };
    yield* sink.write({
      events: [
        {
          id: EventId.make("silence:input-event"),
          type: "runtime-request.updated",
          threadId: childId,
          occurredAt: now,
          payload: request,
        },
      ],
    });
    yield* advanceSilence(epoch);
    expect(
      (yield* orchestrator.getThreadProjection(parentId)).messages.filter(
        (message) => message.senderThreadId === childId,
      ),
    ).toEqual([]);
    const active = (yield* orchestrator.getThreadProjection(childId)).runs.find(
      (candidate) => candidate.id === run.id,
    )!;
    yield* sink.write({
      events: [
        {
          id: EventId.make("silence:resolved"),
          type: "runtime-request.updated",
          threadId: childId,
          occurredAt: now,
          payload: { ...request, status: "resolved", resolvedAt: now },
        },
        {
          id: EventId.make("silence:replacement"),
          type: "run.updated",
          threadId: childId,
          occurredAt: now,
          payload: { ...active, activeAttemptId: RunAttemptId.make("silence:replacement-attempt") },
        },
      ],
    });
    yield* advanceSilence(epoch + 152_000);
    expect(
      (yield* orchestrator.getThreadProjection(parentId)).messages.filter(
        (message) => message.senderThreadId === childId,
      ),
    ).toEqual([]);
  }).pipe(Effect.provide(layer)),
);
