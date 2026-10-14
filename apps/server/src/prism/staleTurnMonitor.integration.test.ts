import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RuntimeRequestId,
  ThreadId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as Registry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import type {
  ProviderAdapterV2,
  ProviderAdapterV2Event,
  ProviderAdapterV2TurnInput,
} from "@t3tools/provider-core/server/ProviderAdapter";
import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as Harness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import * as EffectWorker from "../orchestration-v2/EffectWorker.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as CommandReceipts from "../orchestration-v2/CommandReceiptStore.ts";
import * as Settings from "../serverSettings.ts";
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
} satisfies ProviderAdapterV2["Service"];
const replay = Harness.layerWithRegistry(
  { name: "silence" },
  Registry.layerFromAdapters([adapter]),
  { databaseLayer: database, runEffectWorker: false },
);
const clock = StreamClock.layer.pipe(Layer.provide(Stats.layer), Layer.provide(database));
const threadDependencies = Layer.mergeAll(
  replay,
  Settings.layerTest(),
  CommandReceipts.layer.pipe(Layer.provide(database)),
);
const dependencies = Layer.mergeAll(
  threadDependencies,
  clock,
  EventStore.layer.pipe(Layer.provide(database)),
  Threads.layer.pipe(Layer.provide(threadDependencies)),
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
    expect(notices[0]!.notification).toMatchObject({
      source: { kind: "subagent", childThreadId: childId },
      outcome: "updated",
      detail: notices[0]!.text,
    });
    expect(notices[0]!.notification?.summary).toContain("Silence observed:");
    expect(notices[0]!.text).toContain(DateTime.formatIso(DateTime.makeUnsafe(epoch + 151_000)));
    expect(notices[0]!.text).toContain("t3_thread_read");
    expect(notices[0]!.text).toContain("task_status");
    expect(notices[0]!.text).toContain(run.id);
    expect(notices[0]!.createdBy).toBe("agent");
    expect(notices[0]!.creationSource).toBe("server");
    expect(notices[0]!.delegatedCompletion).toBeUndefined();
    const events = yield* (yield* EventStore.EventStoreV2)
      .readByCommandId({
        commandId: CommandId.make(notices[0]!.id),
      })
      .pipe(Stream.runCollect);
    expect(
      events.some(
        ({ event }) =>
          event.type === "message.updated" &&
          event.payload.notification?.source.kind === "subagent",
      ),
    ).toBe(true);
    expect(
      parent.runs.find((candidate) => candidate.userMessageId === notices[0]!.id),
    ).toMatchObject({ status: "queued", modelSelection });
    expect((yield* orchestrator.getThreadProjection(childId)).runs[0]).toMatchObject({
      id: run.id,
      status: "running",
    });
  }).pipe(Effect.provide(layer)),
);

it.effect("an idle parent receives a persisted child notice instead of a user-message item", () =>
  Effect.gen(function* () {
    const epoch = yield* Clock.currentTimeMillis;
    const { orchestrator, childId } = yield* setup;
    const parent = yield* orchestrator.getThreadProjection(parentId);
    const now = yield* DateTime.now;
    yield* (yield* EventSink.EventSinkV2).write({
      events: [
        {
          id: EventId.make("silence:parent-finished"),
          type: "run.updated",
          threadId: parentId,
          occurredAt: now,
          payload: { ...parent.runs[0]!, status: "completed", completedAt: now },
        },
      ],
    });
    yield* advanceSilence(epoch);
    const after = yield* orchestrator.getThreadProjection(parentId);
    const message = after.messages.find((message) => message.senderThreadId === childId)!;
    const items = after.turnItems.filter((item) => item.runId === message.runId);
    expect(items.some((item) => item.type === "notification")).toBe(true);
    expect(items.some((item) => item.type === "user_message")).toBe(false);
    expect(after.subagents).toEqual(parent.subagents);
  }).pipe(Effect.provide(layer)),
);

it.effect("a fresh monitor replays the receipt without duplicating the notice", () =>
  Effect.gen(function* () {
    const epoch = yield* Clock.currentTimeMillis;
    const { orchestrator, childId } = yield* setup;
    yield* advanceSilence(epoch);
    yield* Monitor.StaleTurnMonitor.use((monitor) => monitor.sweep).pipe(
      Effect.provide(Monitor.layer),
    );
    const parent = yield* orchestrator.getThreadProjection(parentId);
    const notices = parent.messages.filter((message) => message.senderThreadId === childId);
    expect(notices).toHaveLength(1);
    expect(parent.runs.filter((run) => run.userMessageId === notices[0]!.id)).toHaveLength(1);
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "settling a held notice cancels it, while a notice can wake an already-settled parent",
  () =>
    Effect.gen(function* () {
      const epoch = yield* Clock.currentTimeMillis;
      const { orchestrator, childId } = yield* setup;
      yield* advanceSilence(epoch);
      const before = yield* orchestrator.getThreadProjection(parentId);
      const noticeMessage = before.messages.find((message) => message.senderThreadId === childId)!;
      const noticeRun = before.runs.find((run) => run.userMessageId === noticeMessage.id)!;
      const now = yield* DateTime.now;
      const sink = yield* EventSink.EventSinkV2;
      yield* sink.write({
        events: [
          {
            id: EventId.make("silence:finished-for-settle"),
            type: "run.updated",
            threadId: parentId,
            occurredAt: now,
            payload: { ...before.runs[0]!, status: "completed", completedAt: now },
          },
          {
            id: EventId.make("silence:held"),
            type: "run.updated",
            threadId: parentId,
            occurredAt: now,
            payload: { ...noticeRun, queueHeld: true },
          },
        ],
      });
      yield* orchestrator.dispatch({
        type: "thread.settle",
        commandId: CommandId.make("silence:settle"),
        threadId: parentId,
      });
      const settled = yield* orchestrator.getThreadProjection(parentId);
      expect(settled.thread.settledOverride).toBe("settled");
      expect(settled.runs.find((run) => run.id === noticeRun.id)?.status).toBe("cancelled");

      // A different provider turn gets its own notice identity, just as a later child turn does.
      yield* (yield* StreamClock.StreamClock).observe(
        {
          threadId: childId,
          runId: (yield* orchestrator.getThreadProjection(childId)).runs[0]!.id,
          providerSessionId: ProviderSessionId.make("silence:session"),
          providerInstanceId: instanceId,
          event: {
            type: "provider_turn.updated",
            driver: adapter.driver,
            providerTurn: {
              id: ProviderTurnId.make("silence:next-turn"),
              providerThreadId: ProviderThreadId.make("silence:native"),
              nodeId: NodeId.make("silence:node"),
              runAttemptId: RunAttemptId.make("silence:attempt"),
              nativeTurnRef: null,
              ordinal: 1,
              status: "running",
              startedAt: now,
              completedAt: null,
            },
          },
        },
        RunAttemptId.make("silence:attempt"),
      );
      yield* advanceSilence(epoch + 152_000);
      const after = yield* orchestrator.getThreadProjection(parentId);
      expect(after.thread.settledOverride).toBeNull();
      expect(after.messages.filter((message) => message.senderThreadId === childId)).toHaveLength(
        2,
      );
      const events = yield* (yield* EventStore.EventStoreV2)
        .read({ threadId: parentId })
        .pipe(Stream.runCollect);
      expect(events.some(({ event }) => event.type === "thread.unsettled")).toBe(true);
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "delivers a queued historical notice to the adapter after the child's terminal result",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const cwd = yield* checkpointWorkspace("stale-notice-delivery");
        const started = yield* Queue.unbounded<ProviderAdapterV2TurnInput>();
        const streams = new Map<string, Queue.Queue<ProviderAdapterV2Event>>();
        const capabilities = {
          ...CodexProviderCapabilitiesV2,
          turns: {
            ...CodexProviderCapabilitiesV2.turns,
            supportsActiveSteering: false,
            supportsSteeringByInterruptRestart: false,
          },
        };
        const liveAdapter: ProviderAdapterV2["Service"] = {
          instanceId,
          driver: adapter.driver,
          getCapabilities: () => Effect.succeed(capabilities),
          planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
          openSession: (input) =>
            Effect.gen(function* () {
              const now = yield* DateTime.now;
              const events = yield* Queue.unbounded<ProviderAdapterV2Event>();
              streams.set(input.providerSessionId, events);
              return {
                instanceId,
                driver: adapter.driver,
                providerSessionId: input.providerSessionId,
                providerSession: {
                  id: input.providerSessionId,
                  driver: adapter.driver,
                  providerInstanceId: instanceId,
                  status: "ready",
                  cwd,
                  model: modelSelection.model,
                  capabilities,
                  createdAt: now,
                  updatedAt: now,
                  lastError: null,
                },
                events: Stream.fromQueue(events),
                ensureThread: ({ threadId }) =>
                  Effect.succeed({
                    id: ProviderThreadId.make(`silence:native:${threadId}`),
                    driver: adapter.driver,
                    providerInstanceId: instanceId,
                    providerSessionId: input.providerSessionId,
                    appThreadId: threadId,
                    ownerNodeId: null,
                    nativeThreadRef: {
                      driver: adapter.driver,
                      nativeId: threadId,
                      strength: "strong",
                    },
                    nativeConversationHeadRef: null,
                    status: "idle",
                    firstRunOrdinal: null,
                    lastRunOrdinal: null,
                    handoffIds: [],
                    forkedFrom: null,
                    createdAt: now,
                    updatedAt: now,
                  }),
                resumeThread: ({ providerThread }) => Effect.succeed(providerThread),
                startTurn: (turn) =>
                  Effect.gen(function* () {
                    const at = yield* DateTime.now;
                    yield* Queue.offer(started, turn);
                    yield* Queue.offer(events, {
                      type: "provider_turn.updated",
                      driver: adapter.driver,
                      providerTurn: {
                        id: ProviderTurnId.make(`silence:turn:${turn.attemptId}`),
                        providerThreadId: turn.providerThread.id,
                        nodeId: turn.rootNodeId,
                        runAttemptId: turn.attemptId,
                        nativeTurnRef: null,
                        ordinal: turn.providerTurnOrdinal,
                        status: "running",
                        startedAt: at,
                        completedAt: null,
                      },
                    });
                  }),
                steerTurn: () => Effect.die("These notices queue instead of steering"),
                interruptTurn: () => Effect.void,
                respondToRuntimeRequest: () => Effect.die("unused"),
                readThreadSnapshot: () => Effect.die("unused"),
                rollbackThread: () => Effect.die("unused"),
                forkThread: () => Effect.die("unused"),
              };
            }),
        };
        const liveReplay = Harness.layerWithRegistry(
          { name: "silence-delivery" },
          Registry.layerFromAdapters([liveAdapter]),
          { databaseLayer: database, runContinuationWorker: true },
        );
        const liveDependencies = Layer.mergeAll(
          liveReplay,
          clock,
          EventStore.layer.pipe(Layer.provide(database)),
          Layer.succeed(ServerActivation, Effect.void),
          Layer.succeed(Monitor.StaleTurnMonotonicClock, Clock.currentTimeMillis),
        );
        const liveLayer = Layer.merge(
          liveDependencies,
          Monitor.layer.pipe(Layer.provide(liveDependencies)),
        );
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
            orchestrator.streamDomainEvents.pipe(
              Stream.filter(predicate),
              Stream.take(1),
              Stream.runDrain,
              Effect.forkScoped,
            );
          const running = (threadId: ThreadId) =>
            watch(
              (event) =>
                event.threadId === threadId &&
                event.type === "provider-turn.updated" &&
                event.payload.status === "running",
            );
          const finish = (turn: ProviderAdapterV2TurnInput) =>
            Effect.gen(function* () {
              const waiting = yield* watch(
                (event) =>
                  event.type === "run.updated" &&
                  event.payload.id === turn.runId &&
                  event.payload.status === "waiting",
              );
              const completed = yield* watch(
                (event) =>
                  event.type === "run.updated" &&
                  event.payload.id === turn.runId &&
                  event.payload.status === "completed",
              );
              yield* Queue.offer(streams.get(turn.providerThread.providerSessionId!)!, {
                type: "turn.terminal",
                driver: adapter.driver,
                providerThreadId: turn.providerThread.id,
                providerTurnId: ProviderTurnId.make(`silence:turn:${turn.attemptId}`),
                runOrdinal: turn.runOrdinal,
                status: "completed",
                failure: null,
                threadDisposition: "reusable",
              });
              yield* Fiber.join(waiting);
              yield* worker.drain();
              yield* Fiber.join(completed);
            });
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("silence:live-create"),
            threadId: parentId,
            projectId: ProjectId.make("silence:project"),
            title: "Parent",
            modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: cwd,
            createdBy: "user",
            creationSource: "web",
          });
          const parentRunning = yield* running(parentId);
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("silence:live-start"),
            threadId: parentId,
            messageId: MessageId.make("silence:live-message"),
            text: "Delegate",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* worker.drain();
          const parentInput = yield* Queue.take(started);
          yield* Fiber.join(parentRunning);
          yield* orchestrator.dispatch({
            type: "delegated_task.request",
            commandId: CommandId.make("silence:live-delegate"),
            parentThreadId: parentId,
            parentRunId: parentInput.runId,
            parentNodeId: parentInput.rootNodeId,
            task: "Work",
            modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            completionWake: "always",
            createdBy: "agent",
            creationSource: "mcp",
          });
          const childId = (yield* orchestrator.getThreadProjection(parentId)).subagents[0]!
            .childThreadId!;
          const childRunning = yield* running(childId);
          yield* worker.drain();
          const childInput = yield* Queue.take(started);
          yield* Fiber.join(childRunning);
          const childRun = (yield* orchestrator.getThreadProjection(childId)).runs[0]!;
          yield* (yield* StreamClock.StreamClock).beginAttempt({
            threadId: childId,
            runId: childInput.runId,
            runOrdinal: childRun.ordinal,
            attemptId: childInput.attemptId,
            attemptOrdinal: 1,
            providerThreadId: childInput.providerThread.id,
            provider: adapter.driver,
            model: modelSelection.model,
          });
          yield* advanceSilence(yield* Clock.currentTimeMillis);
          const queued = yield* orchestrator.getThreadProjection(parentId);
          const noticeMessage = queued.messages.find(
            (message) => message.senderThreadId === childId,
          )!;
          expect(queued.runs.find((run) => run.userMessageId === noticeMessage.id)?.status).toBe(
            "queued",
          );
          expect((yield* orchestrator.getThreadProjection(childId)).runs[0]?.status).toBe(
            "running",
          );

          const completionQueued = yield* watch(
            (event) =>
              event.threadId === parentId &&
              event.type === "message.updated" &&
              event.payload.delegatedCompletion !== undefined,
          );
          const at = yield* DateTime.now;
          yield* Queue.offer(streams.get(childInput.providerThread.providerSessionId!)!, {
            type: "message.updated",
            driver: adapter.driver,
            message: {
              id: MessageId.make("silence:child-result"),
              threadId: childId,
              runId: childInput.runId,
              nodeId: childInput.rootNodeId,
              role: "assistant",
              text: "Work finished successfully",
              attachments: [],
              streaming: false,
              createdAt: at,
              updatedAt: at,
              createdBy: "agent",
              creationSource: "provider",
            },
          });
          yield* finish(childInput);
          yield* Fiber.join(completionQueued);
          yield* finish(parentInput);
          yield* worker.drain();
          const completionInput = yield* Queue.take(started);
          const beforeNotice = yield* orchestrator.getThreadProjection(parentId);
          expect(
            beforeNotice.messages.find(
              (message) => message.id === completionInput.message.messageId,
            )?.delegatedCompletion,
          ).toBeDefined();
          yield* finish(completionInput);
          yield* worker.drain();
          const noticeInput = yield* Queue.take(started);
          expect(noticeInput.message.messageId).toBe(noticeMessage.id);
          expect(noticeInput.message.text).toBe(noticeMessage.text);
          expect(noticeInput.message.text).toContain("may have resumed or finished since");
          const after = yield* orchestrator.getThreadProjection(parentId);
          const task = after.subagents[0]!;
          expect(task.status).toBe("completed");
          expect(task.result).toContain("Work finished successfully");
          expect(
            after.messages.find((message) => message.id === noticeMessage.id)?.delegatedCompletion,
          ).toBeUndefined();
          const noticeItems = after.turnItems.filter((item) => item.runId === noticeInput.runId);
          expect(noticeItems.some((item) => item.type === "notification")).toBe(true);
          expect(noticeItems.some((item) => item.type === "user_message")).toBe(false);
          yield* finish(noticeInput);
          const final = yield* orchestrator.getThreadProjection(parentId);
          expect(final.subagents[0]).toEqual(task);
          expect(
            final.messages.find((message) => message.id === completionInput.message.messageId),
          ).toEqual(
            after.messages.find((message) => message.id === completionInput.message.messageId),
          );
        }).pipe(Effect.provide(liveLayer));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
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
