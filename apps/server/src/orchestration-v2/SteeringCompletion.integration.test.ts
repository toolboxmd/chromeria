import { ProviderTurnAnalytics } from "../telemetry/ProviderUsageAnalytics.ts";
import { assert, it } from "@effect/vitest";
import {
  ChatAttachmentId,
  CommandId,
  EnvironmentId,
  MessageId,
  EventId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RunId,
  RunAttemptId,
  ThreadId,
  TurnItemId,
  type ModelSelection,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2Subagent,
  type OrchestrationV2TurnItem,
  type OrchestrationV2DomainEvent,
  executionPurposeOf,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as StreamClock from "../prism/streamClock.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as EventSink from "./EventSink.ts";
import * as Orchestrator from "./Orchestrator.ts";
import {
  ProviderAdapterInterruptError,
  type ProviderAdapterV2Event,
  type ProviderAdapterV2SessionRuntime,
} from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";

const driver = ProviderDriverKind.make("codex");
const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "test-model" };

it.effect.each(
  (
    [
      [false, "during delivery"],
      [false, "before dispatch"],
      [true, "after delivery"],
      [true, "without native steering"],
      [true, "with interrupting native steering"],
      [true, "settled only"],
      [false, "during continuation"],
      ["question", "during delivery"],
    ] as const
  ).map(([kind, timing]) => ({
    mailbox: kind === true,
    question: kind === "question",
    timing,
    label:
      kind === "question" ? "delegated task question" : kind ? "mailbox notification" : "steering",
  })),
)("delivers $label when completion wins $timing", ({ mailbox, question, timing }) =>
  Effect.scoped(
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace(`steering-completion-${timing.replaceAll(" ", "-")}`);
      const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
      const started: ProviderAdapter.ProviderAdapterV2TurnInput[] = [];
      const steerEntered = yield* Deferred.make<void>();
      const rejectSteer = yield* Deferred.make<void>();
      let steerCalls = 0;
      const capabilities = {
        ...CodexProviderCapabilitiesV2,
        turns: {
          ...CodexProviderCapabilitiesV2.turns,
          supportsActiveSteering: timing !== "without native steering",
          activeSteeringInterruptsTools: timing === "with interrupting native steering",
        },
      };
      const adapter: ProviderAdapter.ProviderAdapterV2["Service"] = {
        instanceId,
        driver,
        getCapabilities: () => Effect.succeed(capabilities),
        planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
        openSession: (input) =>
          Effect.gen(function* () {
            const now = yield* DateTime.now;
            return {
              instanceId,
              driver,
              providerSessionId: input.providerSessionId,
              providerSession: {
                id: input.providerSessionId,
                driver,
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
                  id: ProviderThreadId.make(`provider-thread:${threadId}`),
                  driver,
                  providerInstanceId: instanceId,
                  providerSessionId: input.providerSessionId,
                  appThreadId: threadId,
                  ownerNodeId: null,
                  nativeThreadRef: { driver, nativeId: "native-thread", strength: "strong" },
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
                  started.push(turn);
                  yield* Queue.offer(events, {
                    type: "provider_turn.updated",
                    driver,
                    providerTurn: {
                      id: ProviderTurnId.make(`provider-turn:${turn.attemptId}`),
                      providerThreadId: turn.providerThread.id,
                      nodeId: turn.rootNodeId,
                      runAttemptId: turn.attemptId,
                      nativeTurnRef: {
                        driver,
                        nativeId: `native:${turn.attemptId}`,
                        strength: "strong",
                      },
                      ordinal: turn.providerTurnOrdinal,
                      status: "running",
                      startedAt: now,
                      completedAt: null,
                    },
                  });
                }),
              steerTurn: (turn) =>
                Effect.gen(function* () {
                  steerCalls += 1;
                  if (timing === "after delivery" || timing === "during continuation") return;
                  yield* Deferred.succeed(steerEntered, undefined);
                  yield* Deferred.await(rejectSteer);
                  return yield* new ProviderAdapter.ProviderAdapterSteerRunError({
                    driver,
                    providerThreadId: turn.providerThread.id,
                    providerTurnId: turn.providerTurnId,
                    cause: "turn already completed",
                  });
                }),
              interruptTurn: () => Effect.void,
              respondToRuntimeRequest: () => Effect.void,
              readThreadSnapshot: () => Effect.die("unused"),
              rollbackThread: () => Effect.die("unused"),
              forkThread: () => Effect.die("unused"),
            };
          }),
      };
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = ThreadId.make("thread:steering-completion");
        const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
          orchestrator.streamDomainEvents.pipe(
            Stream.filter(predicate),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make("create"),
          threadId,
          projectId: ProjectId.make("project:steering-completion"),
          title: "Steering race",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: cwd,
          createdBy: "user",
          creationSource: "web",
        });
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("first"),
          threadId,
          messageId: MessageId.make("message:first"),
          text: "first",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        const running = yield* watch(
          (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
        );
        yield* worker.drain();
        yield* Fiber.join(running);
        const first = started[0]!;
        const messageId = MessageId.make("message:steering");
        const taskId = NodeId.make("task:mailbox");
        if (mailbox) {
          const sink = yield* EventSink.EventSinkV2;
          const current = yield* orchestrator.getThreadProjection(threadId);
          const parentRun = current.runs.find((run) => run.id === first.runId)!;
          const completionSource = {
            runId: RunId.make("run:mailbox-child"),
            purpose: executionPurposeOf(parentRun),
          };
          const now = yield* DateTime.now;
          yield* sink.write({
            events: [
              {
                id: EventId.make("mailbox:cohort"),
                type: "run.updated",
                threadId,
                runId: first.runId,
                occurredAt: now,
                payload: {
                  ...parentRun,
                  delegatedCompletion: {
                    disposition: "open",
                    nextGeneration: 2,
                    delivery: {
                      generation: 1,
                      messageId,
                      taskIds: [taskId],
                      purposeCauses: [{ taskId, ...completionSource }],
                    },
                  },
                },
              },
              {
                id: EventId.make("mailbox:task"),
                type: "subagent.updated",
                threadId,
                runId: first.runId,
                nodeId: taskId,
                occurredAt: now,
                payload: {
                  id: taskId,
                  threadId,
                  runId: first.runId,
                  parentNodeId: first.rootNodeId,
                  origin: "app_owned",
                  createdBy: "agent",
                  driver,
                  providerInstanceId: instanceId,
                  providerThreadId: null,
                  childThreadId: null,
                  nativeTaskRef: null,
                  prompt: "Do background work",
                  title: "Background test",
                  model: null,
                  completionWake: timing === "settled only" ? "settled_only" : "always",
                  completionDelivery: { state: "claimed", observedByRunId: null },
                  completionSource,
                  status: "completed",
                  result: "done",
                  startedAt: now,
                  completedAt: now,
                  updatedAt: now,
                },
              },
            ],
          });
        }
        if (question) {
          const { thread } = yield* orchestrator.getThreadProjection(threadId);
          yield* (yield* EventSink.EventSinkV2).write({
            events: [
              {
                id: EventId.make("event:composer-selection"),
                type: "thread.model-selection-updated",
                threadId,
                providerInstanceId: instanceId,
                occurredAt: yield* DateTime.now,
                payload: { ...thread, modelSelection: composerSelection },
              },
            ],
          });
        }
        const dispatchSteer = orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("steer"),
          threadId,
          messageId,
          text: "fix the popover",
          attachments: [
            {
              type: "image",
              id: "steering-screenshot",
              name: "image.png",
              mimeType: "image/png",
              sizeBytes: 10,
            },
          ],
          dispatchMode:
            mailbox || question
              ? { type: "queue_after_active" }
              : { type: "steer_active", targetRunId: first.runId },
          createdBy: mailbox || question ? "agent" : "user",
          creationSource: mailbox || question ? "server" : "web",
          ...(question
            ? {
                notification: {
                  source: { kind: "delegated_task", taskIds: [taskId] },
                  outcome: "updated",
                  summary: "Delegated task has a question",
                },
              }
            : {}),
          ...(mailbox
            ? {
                delegatedCompletion: {
                  parentRunId: first.runId,
                  generation: 1,
                  taskIds: [taskId],
                },
              }
            : {}),
        });
        if (timing !== "before dispatch") yield* dispatchSteer;
        if (timing === "during continuation") {
          const projection = yield* orchestrator.getThreadProjection(threadId);
          const turn = projection.providerTurns[0]!;
          const failed = yield* watch(
            (event) => event.type === "provider-turn.updated" && event.payload.status === "failed",
          );
          yield* Queue.offer(events, {
            type: "provider_turn.updated",
            driver,
            providerTurn: { ...turn, status: "failed", completedAt: yield* DateTime.now },
          });
          yield* Fiber.join(failed);
          yield* worker.drain();
          assert.equal(steerCalls, 1);
          assert.lengthOf(started, 1);
          const continued = yield* orchestrator.getThreadProjection(threadId);
          assert.equal(continued.runs[0]?.status, "running");
          assert.equal(
            continued.messages.find((message) => message.id === messageId)?.runId,
            first.runId,
          );
          return;
        }
        if (timing === "after delivery") yield* worker.drain();
        const delivery =
          timing === "during delivery" ? yield* worker.runOnce.pipe(Effect.forkScoped) : null;
        if (delivery !== null) yield* Deferred.await(steerEntered);
        const completed = yield* watch(
          (event) =>
            event.type === "run.updated" &&
            event.payload.id === first.runId &&
            event.payload.status === "waiting",
        );
        const projection = yield* orchestrator.getThreadProjection(threadId);
        const turn = projection.providerTurns[0]!;
        yield* Queue.offer(events, {
          type: "provider_turn.updated",
          driver,
          providerTurn: { ...turn, status: "completed", completedAt: yield* DateTime.now },
        });
        yield* Queue.offer(events, {
          type: "turn.terminal",
          driver,
          providerThreadId: turn.providerThreadId,
          providerTurnId: turn.id,
          runOrdinal: first.runOrdinal,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* Fiber.join(completed);
        if (delivery !== null) {
          yield* Deferred.succeed(rejectSteer, undefined);
          yield* Fiber.join(delivery);
        }
        if (delivery !== null && !question) {
          const requeued = yield* orchestrator.getThreadProjection(threadId);
          const item = requeued.turnItems.find(
            (item) => item.type === "user_message" && item.messageId === messageId,
          );
          assert.equal(item?.status, "pending");
          assert.isNull(item?.startedAt);
          assert.isNull(item?.completedAt);
        }
        if (timing === "before dispatch") yield* dispatchSteer;
        yield* worker.drain();
        yield* orchestrator.resumeQueuedRuns;
        yield* worker.drain();
        if (timing === "after delivery") {
          assert.equal(steerCalls, 1);
          assert.equal(started.length, 1);
          if (mailbox) {
            const delivered = yield* orchestrator.getThreadProjection(threadId);
            assert.equal(delivered.subagents[0]?.completionDelivery?.state, "delivered");
            assert.equal(delivered.subagents[0]?.completionDelivery?.observedByRunId, null);
            assert.equal(delivered.runs[0]?.delegatedCompletion?.delivery, null);
            assert.equal(
              delivered.turnItems.filter((item) => item.type === "notification").length,
              1,
            );
            yield* orchestrator.dispatch({
              type: "notification.delivery.accept",
              commandId: CommandId.make("duplicate-acceptance"),
              threadId,
              messageId,
            });
            yield* worker.drain();
            assert.equal(steerCalls, 1);
            yield* orchestrator.dispatch({
              type: "delegated_task.completion-delivery.acknowledge",
              commandId: CommandId.make("read-result"),
              parentThreadId: threadId,
              taskId,
              observedByRunId: first.runId,
            });
            const acknowledged = yield* orchestrator.getThreadProjection(threadId);
            assert.equal(acknowledged.subagents[0]?.completionDelivery?.state, "acknowledged");
          }
          return;
        }
        assert.equal(started.length, 2);
        assert.equal(started[1]?.message.messageId, messageId);
        if (mailbox) assert.include(started[1]?.message.text ?? "", String(taskId));
        else assert.equal(started[1]?.message.text, "fix the popover");
        assert.deepEqual(started[1]?.message.attachments, [
          {
            type: "image",
            id: "steering-screenshot",
            name: "image.png",
            mimeType: "image/png",
            sizeBytes: 10,
          },
        ]);
        assert.equal(steerCalls, timing === "during delivery" ? 1 : 0);
        const final = yield* orchestrator.getThreadProjection(threadId);
        if (question) {
          assert.deepEqual(final.thread.modelSelection, composerSelection);
          assert.deepEqual(started[1]?.modelSelection, composerSelection);
        }
        if (!mailbox && !question) {
          assert.equal(
            final.turnItems.find(
              (item) => item.type === "user_message" && item.messageId === messageId,
            )?.status,
            "completed",
          );
        }
        assert.equal(final.messages.filter((message) => message.id === messageId).length, 1);
        assert.equal(
          final.messages.find((message) => message.id === messageId)?.runId,
          started[1]?.runId,
        );
        assert.equal(
          final.turnItems.filter((item) =>
            mailbox || question
              ? item.type === "notification"
              : item.type === "user_message" && item.messageId === messageId,
          ).length,
          1,
        );
        yield* worker.drain();
        assert.equal(started.length, 2);
      }).pipe(
        Effect.provide(
          ProviderReplayHarness.layerWithRegistry(
            { name: `steering-completion-${timing}` },
            ProviderAdapterRegistry.layerSingle(adapter),
            { runEffectWorker: false },
          ),
        ),
      );
    }),
  ),
);

const runSelection = {
  instanceId,
  model: "test-model",
  options: [{ id: "effort", value: "xhigh" }],
};
const composerSelection = {
  ...runSelection,
  options: [...runSelection.options, { id: "fastMode", value: false }],
};

const nextTurnSelectionHarness = Effect.fn("nextTurnSelectionHarness")(function* (
  name: string,
  supportsRestart = false,
  interruptTurn?: ProviderAdapterV2SessionRuntime["interruptTurn"],
  options: {
    readonly planSelectionTransition?: ProviderAdapter.ProviderAdapterV2["Service"]["planSelectionTransition"];
    readonly endedTurnHold?: ProviderAdapterV2SessionRuntime["endedTurnHold"];
    readonly supportsActiveSteering?: boolean;
    readonly activeSteeringAcceptsAttachments?: boolean;
    readonly supportsNativeGoals?: boolean;
  } = {},
) {
  const driver = ProviderDriverKind.make(
    options.supportsNativeGoals === undefined ? "codex" : "opencode",
  );
  const cwd = yield* checkpointWorkspace(name);
  const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
  const started: ProviderAdapter.ProviderAdapterV2TurnInput[] = [];
  const steered: string[] = [];
  const interrupted: Parameters<ProviderAdapterV2SessionRuntime["interruptTurn"]>[0][] = [];
  const capabilities = {
    ...CodexProviderCapabilitiesV2,
    turns: {
      ...CodexProviderCapabilitiesV2.turns,
      supportsActiveSteering: options.supportsActiveSteering ?? true,
      activeSteeringAcceptsAttachments: options.activeSteeringAcceptsAttachments ?? true,
      supportsSteeringByInterruptRestart: supportsRestart,
      ...(options.supportsNativeGoals === undefined
        ? {}
        : { supportsNativeGoals: options.supportsNativeGoals }),
    },
  };
  const adapter: ProviderAdapter.ProviderAdapterV2["Service"] = {
    instanceId,
    driver,
    getCapabilities: () => Effect.succeed(capabilities),
    planSelectionTransition:
      options.planSelectionTransition ?? (() => Effect.succeed({ type: "apply_on_next_turn" })),
    openSession: (input) =>
      Effect.gen(function* () {
        const now = yield* DateTime.now;
        const turnUpdated = (
          turn: ProviderAdapter.ProviderAdapterV2TurnInput,
          status: "running" | "interrupted",
        ) =>
          Queue.offer(events, {
            type: "provider_turn.updated",
            driver,
            providerTurn: {
              id: ProviderTurnId.make(`provider-turn:${turn.attemptId}`),
              providerThreadId: turn.providerThread.id,
              nodeId: turn.rootNodeId,
              runAttemptId: turn.attemptId,
              nativeTurnRef: {
                driver,
                nativeId: `native:${turn.attemptId}`,
                strength: "strong",
              },
              ordinal: turn.providerTurnOrdinal,
              status,
              startedAt: now,
              completedAt: status === "running" ? null : now,
            },
          });
        const startTurn = (turn: ProviderAdapter.ProviderAdapterV2TurnInput) =>
          Effect.gen(function* () {
            started.push(turn);
            yield* turnUpdated(turn, "running");
          });
        return {
          instanceId,
          driver,
          providerSessionId: input.providerSessionId,
          providerSession: {
            id: input.providerSessionId,
            driver,
            providerInstanceId: instanceId,
            status: "ready",
            cwd,
            model: runSelection.model,
            capabilities,
            createdAt: now,
            updatedAt: now,
            lastError: null,
          },
          events: Stream.fromQueue(events),
          ...(options.endedTurnHold === undefined ? {} : { endedTurnHold: options.endedTurnHold }),
          ensureThread: ({ threadId }) =>
            Effect.succeed({
              id: ProviderThreadId.make(`provider-thread:${threadId}`),
              driver,
              providerInstanceId: instanceId,
              providerSessionId: input.providerSessionId,
              appThreadId: threadId,
              ownerNodeId: null,
              nativeThreadRef: { driver, nativeId: "native-thread", strength: "strong" },
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
          startTurn,
          compactThread: startTurn,
          steerTurn: (turn) =>
            Effect.sync(() => {
              steered.push(turn.message.text);
            }),
          interruptTurn: (input) =>
            Effect.sync(() => {
              interrupted.push(input);
            }).pipe(
              Effect.andThen(() => interruptTurn?.(input) ?? Effect.void),
              Effect.andThen(
                Effect.forEach(
                  started.filter(
                    (turn) =>
                      ProviderTurnId.make(`provider-turn:${turn.attemptId}`) ===
                      input.providerTurnId,
                  ),
                  (turn) => turnUpdated(turn, "interrupted"),
                  { discard: true },
                ),
              ),
            ),
          respondToRuntimeRequest: () => Effect.void,
          readThreadSnapshot: () => Effect.die("unused"),
          rollbackThread: () => Effect.die("unused"),
          forkThread: () => Effect.die("unused"),
        };
      }),
  };
  const layer = ProviderReplayHarness.layerWithRegistry(
    { name },
    ProviderAdapterRegistry.layerSingle(adapter),
    { runEffectWorker: false },
  );
  const startFirstTurn = Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
    const threadId = ThreadId.make(`thread:${name}`);
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make("create"),
      threadId,
      projectId: ProjectId.make(`project:${name}`),
      title: "Steer with changed options",
      modelSelection: runSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: cwd,
      createdBy: "user",
      creationSource: "web",
    });
    const running = yield* orchestrator.streamDomainEvents.pipe(
      Stream.filter(
        (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
      ),
      Stream.take(1),
      Stream.runDrain,
      Effect.forkScoped,
    );
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make("first"),
      threadId,
      messageId: MessageId.make("message:first"),
      text: "first",
      attachments: [],
      modelSelection: runSelection,
      dispatchMode: { type: "start_immediately" },
      createdBy: "user",
      creationSource: "web",
    });
    yield* worker.drain();
    yield* Fiber.join(running);
    return threadId;
  });
  return { cwd, events, started, steered, interrupted, layer, startFirstTurn };
});

it.effect.each([
  { notice: "delegated task question", goal: null, steers: true },
  { notice: "background command", goal: "active", steers: true },
  { notice: "background command", goal: null, steers: false },
] as const)(
  "a $notice notice steers into the running turn: $steers (goal $goal)",
  ({ notice, goal, steers }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
          `notice-steer-${notice.replaceAll(" ", "-")}-${goal ?? "none"}`,
        );
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const threadId = yield* startFirstTurn;
          const { thread } = yield* orchestrator.getThreadProjection(threadId);
          yield* (yield* EventSink.EventSinkV2).write({
            events: [
              {
                id: EventId.make("event:composer-selection"),
                type: "thread.model-selection-updated",
                threadId,
                providerInstanceId: instanceId,
                occurredAt: yield* DateTime.now,
                payload: { ...thread, modelSelection: composerSelection },
              },
            ],
          });
          if (goal !== null) {
            const providerThread = (yield* orchestrator.getThreadProjection(threadId))
              .providerThreads[0]!;
            yield* (yield* EventSink.EventSinkV2).write({
              events: [
                {
                  id: EventId.make("event:goal-active"),
                  type: "provider-thread.updated",
                  threadId,
                  occurredAt: yield* DateTime.now,
                  payload: { ...providerThread, goal: { objective: "Ship it", status: goal } },
                },
              ],
            });
          }
          const messageId = MessageId.make("message:notice");
          const notification =
            notice === "delegated task question"
              ? {
                  source: {
                    kind: "delegated_task" as const,
                    taskIds: [NodeId.make("task:question")],
                    childThreadId: ThreadId.make("thread:question-child"),
                  },
                  outcome: "updated" as const,
                  summary: "Delegated task has a question",
                }
              : {
                  source: { kind: "command" as const },
                  outcome: "completed" as const,
                  summary: "Background command finished",
                };
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("notice-wake"),
            threadId,
            messageId,
            text: `${notification.summary}\nexit 0: built 12 files`,
            notification,
            attachments: [],
            dispatchMode: { type: "queue_after_active" },
            createdBy: "agent",
            creationSource: "server",
          });
          yield* worker.drain();
          const after = yield* orchestrator.getThreadProjection(threadId);
          assert.deepEqual(
            steered,
            steers ? [`${notification.summary}\nexit 0: built 12 files`] : [],
          );
          assert.lengthOf(started, 1);
          assert.equal(
            after.runs.some((run) => run.status === "queued"),
            !steers,
          );
          const message = after.messages.find((candidate) => candidate.id === messageId);
          assert.deepEqual(message?.notification, notification);
          if (steers) assert.equal(message?.runId, started[0]?.runId);
          assert.deepEqual(after.thread.modelSelection, composerSelection);
        }).pipe(Effect.provide(layer));
      }),
    ).pipe(Effect.provide(TestClock.layer())),
);

it.effect.each([
  { cutIn: false, rootStatus: "completed", publishTerminal: true },
  { cutIn: true, rootStatus: "completed", publishTerminal: true },
  { cutIn: true, rootStatus: "failed", publishTerminal: false },
] as const)(
  "persists one root writer across restart or next turn while earlier native work drains: %j",
  ({ cutIn, rootStatus, publishTerminal }) =>
    Effect.scoped(
      Effect.gen(function* () {
        let oldBackground:
          | Extract<ProviderAdapterV2Event, { type: "turn_item.updated" }>
          | undefined;
        const cancellations: Array<
          Parameters<ProviderAdapterV2SessionRuntime["interruptTurn"]>[0]
        > = [];
        const { events, started, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
          `completed-restart-root-owner-${rootStatus}-${cutIn}`,
          true,
          (input): Effect.Effect<void, ProviderAdapterInterruptError> =>
            Effect.gen(function* () {
              cancellations.push(input);
              if (oldBackground?.turnItem.type !== "command_execution")
                return yield* Effect.die("missing old tool");
              const now = yield* DateTime.now;
              yield* Queue.offer(events, {
                ...oldBackground,
                turnItem: {
                  ...oldBackground.turnItem,
                  status: "interrupted",
                  output: "OLD_TOOL_STOPPED",
                  completedAt: now,
                },
              });
            }),
          { endedTurnHold: () => Effect.succeed("descendants" as const) },
        );
        const oldIngestionEnded = yield* Deferred.make<void>();
        const replacementIngestionEnded = yield* Deferred.make<void>();
        const terminalHandlerBlocked = yield* Deferred.make<void>();
        const releaseTerminalHandler = yield* Deferred.make<void>();
        let terminalHandlerWasGated = false;
        let oldAttemptId: RunAttemptId | undefined;
        let replacementAttemptId: RunAttemptId | undefined;
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const threadId = yield* startFirstTurn;
          const first = started[0]!;
          oldAttemptId = first.attemptId;
          const now = yield* DateTime.now;
          const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
            orchestrator.streamDomainEvents.pipe(
              Stream.filter(predicate),
              Stream.take(1),
              Stream.runDrain,
              Effect.forkScoped({ startImmediately: true }),
            );
          const before = yield* orchestrator.getThreadProjection(threadId);
          const oldTurn = before.providerTurns[0]!;
          const background = {
            type: "turn_item.updated",
            driver,
            turnItem: {
              id: TurnItemId.make("tool:old-completed-attempt"),
              type: "command_execution",
              threadId,
              runId: first.runId,
              nodeId: first.rootNodeId,
              providerThreadId: first.providerThread.id,
              providerTurnId: oldTurn.id,
              nativeItemRef: { driver, nativeId: "old-background", strength: "strong" },
              parentItemId: null,
              ordinal: 1,
              status: "running",
              title: "Old native work",
              input: "old command",
              startedAt: now,
              completedAt: null,
              updatedAt: now,
            },
          } satisfies ProviderAdapterV2Event;
          oldBackground = background;
          const backgroundRunning = yield* watch(
            (event) =>
              event.type === "turn-item.updated" && event.payload.id === background.turnItem.id,
          );
          yield* Queue.offer(events, background);
          yield* Fiber.join(backgroundRunning);
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("restart-before-completion"),
            threadId,
            messageId: MessageId.make("message:restart-before-completion"),
            text: "Change course",
            attachments: [],
            ...(cutIn ? {} : { modelSelection: composerSelection }),
            dispatchMode: {
              type: cutIn ? "restart_active" : "steer_active",
              targetRunId: first.runId,
            },
            createdBy: cutIn ? "user" : "agent",
            creationSource: cutIn ? "web" : "mcp",
          });
          // The native turn settles before its restart effect. A retained old
          // background artifact must not make that attempt a second root writer.
          yield* Queue.offer(events, {
            type: "provider_turn.updated",
            driver,
            providerTurn: { ...oldTurn, status: rootStatus, completedAt: now },
          });
          const rootTerminal = {
            type: "turn.terminal",
            driver,
            providerThreadId: first.providerThread.id,
            providerTurnId: oldTurn.id,
            runOrdinal: first.runOrdinal,
            ...(rootStatus === "failed"
              ? {
                  status: "failed" as const,
                  failureItemOrdinal: 2,
                  failure: makeProviderFailure({ message: "Native root failed" }),
                }
              : { status: "completed" as const, failure: null }),
            threadDisposition: "reusable",
          } satisfies ProviderAdapterV2Event;
          if (publishTerminal) yield* Queue.offer(events, rootTerminal);
          if (cutIn && publishTerminal) {
            // The projection is terminal, but the old ingestor has not handled
            // turn.terminal yet. Stop outcomes are queued before replacement.
            yield* Deferred.await(terminalHandlerBlocked);
          } else {
            const pastTerminal = yield* watch(
              (event) =>
                event.type === "turn-item.updated" &&
                event.payload.id === background.turnItem.id &&
                event.payload.type === "command_execution" &&
                event.payload.output === "ROOT_TERMINAL_PROCESSED",
            );
            yield* Queue.offer(events, {
              ...background,
              turnItem: { ...background.turnItem, output: "ROOT_TERMINAL_PROCESSED" },
            });
            yield* Fiber.join(pastTerminal);
          }
          const replacementRunning = yield* watch(
            (event) =>
              event.type === "provider-turn.updated" &&
              event.payload.id !== oldTurn.id &&
              event.payload.status === "running",
          );
          yield* worker.drain();
          yield* Fiber.join(replacementRunning);
          assert.lengthOf(started, 2);
          assert.lengthOf(cancellations, cutIn ? 1 : 0);
          if (cutIn) {
            assert.isTrue(cancellations[0]?.requestRuntimeRestart);
            assert.isTrue(cancellations[0]?.replacementFollows);
            if (publishTerminal) assert.isFalse(yield* Deferred.isDone(releaseTerminalHandler));
          }
          const replacement = started[1]!;
          replacementAttemptId = replacement.attemptId;
          if (cutIn) assert.equal(replacement.runId, first.runId);
          else
            assert.notEqual(
              replacement.runId,
              first.runId,
              "routine selection waits for the next run",
            );
          assert.notEqual(replacement.attemptId, first.attemptId);
          const current = yield* orchestrator.getThreadProjection(threadId);
          const root = current.nodes.find((node) => node.id === replacement.rootNodeId)!;
          const messageId = MessageId.make("message:replacement-root-once");
          const message = {
            id: messageId,
            threadId,
            runId: replacement.runId,
            nodeId: replacement.rootNodeId,
            role: "assistant",
            text: "NEW_ROOT_ONCE",
            attachments: [],
            streaming: false,
            createdBy: "agent",
            creationSource: "provider",
            createdAt: now,
            updatedAt: now,
          } as const;
          yield* Queue.offer(events, { type: "message.updated", driver, message });
          yield* Queue.offer(events, {
            type: "node.updated",
            driver,
            node: {
              ...root,
              nativeItemRef: { driver, nativeId: "NEW_ROOT_ONCE", strength: "strong" },
            },
          });
          const rootToolId = TurnItemId.make("tool:replacement-root-once");
          const nextTurn = current.providerTurns.find(
            (turn) => turn.runAttemptId === replacement.attemptId,
          )!;
          yield* Queue.offer(events, {
            ...background,
            turnItem: {
              ...background.turnItem,
              id: rootToolId,
              runId: replacement.runId,
              nodeId: replacement.rootNodeId,
              providerTurnId: nextTurn.id,
              input: "NEW_ROOT_ONCE",
              status: "completed",
              completedAt: now,
            },
          });
          yield* Queue.offer(events, {
            type: "provider_turn.updated",
            driver,
            providerTurn: { ...nextTurn, status: "completed", completedAt: now },
          });
          yield* Queue.offer(events, {
            type: "turn.terminal",
            driver,
            providerThreadId: replacement.providerThread.id,
            providerTurnId: nextTurn.id,
            runOrdinal: replacement.runOrdinal,
            status: "completed",
            failure: null,
            threadDisposition: "reusable",
          });
          yield* Deferred.await(replacementIngestionEnded);
          if (cutIn && publishTerminal) {
            yield* Deferred.succeed(releaseTerminalHandler, undefined);
          } else if (!cutIn) {
            yield* Queue.offer(events, {
              ...background,
              turnItem: {
                ...background.turnItem,
                status: "completed",
                output: "OLD_TOOL_TAIL",
                completedAt: now,
              },
            });
          }
          if (!publishTerminal) yield* Queue.offer(events, rootTerminal);
          yield* Deferred.await(oldIngestionEnded);
          const sequence = yield* orchestrator.getThreadEventSequence(threadId);
          const stored = yield* orchestrator
            .streamStoredEventsFrom({ threadId, afterSequence: 0 })
            .pipe(
              Stream.takeUntil((event) => event.sequence === sequence),
              Stream.map((event) => event.event),
              Stream.runCollect,
            );
          assert.deepEqual(
            {
              messages: stored.filter(
                (event) => event.type === "message.updated" && event.payload.id === messageId,
              ).length,
              nodes: stored.filter(
                (event) =>
                  event.type === "node.updated" &&
                  event.payload.nativeItemRef?.nativeId === "NEW_ROOT_ONCE",
              ).length,
              tools: stored.filter(
                (event) => event.type === "turn-item.updated" && event.payload.id === rootToolId,
              ).length,
            },
            { messages: 1, nodes: 1, tools: 1 },
          );
          const oldEnds = stored.filter(
            (event) =>
              event.type === "turn-item.updated" &&
              event.payload.id === background.turnItem.id &&
              (event.payload.status === "completed" || event.payload.status === "interrupted"),
          );
          assert.lengthOf(oldEnds, 1);
          const oldEnd = oldEnds[0];
          assert.equal(oldEnd?.type, "turn-item.updated");
          if (oldEnd?.type !== "turn-item.updated") return;
          assert.equal(oldEnd.payload.nodeId, first.rootNodeId);
          assert.equal(oldEnd.payload.status, cutIn ? "interrupted" : "completed");
        }).pipe(
          Effect.provide(
            layer.pipe(
              Layer.provide(
                Layer.mergeAll(
                  Layer.succeed(StreamClock.StreamClockHooks, {
                    beginAttempt: () => Effect.void,
                    observe: () => Effect.void,
                    endAttempt: (_threadId, _runId, attemptId) =>
                      Effect.gen(function* () {
                        if (attemptId === oldAttemptId)
                          yield* Deferred.succeed(oldIngestionEnded, undefined);
                        if (attemptId === replacementAttemptId)
                          yield* Deferred.succeed(replacementIngestionEnded, undefined);
                      }),
                  }),
                  Layer.mock(ProviderTurnAnalytics, {
                    runStarted: () => Effect.void,
                    runSettled: () => Effect.void,
                    turnCompleted: (properties) =>
                      Effect.gen(function* () {
                        if (
                          !cutIn ||
                          !publishTerminal ||
                          terminalHandlerWasGated ||
                          properties.providerTurn.status !== "completed"
                        )
                          return;
                        terminalHandlerWasGated = true;
                        yield* Deferred.succeed(terminalHandlerBlocked, undefined);
                        yield* Deferred.await(releaseTerminalHandler);
                      }),
                  }),
                ),
              ),
            ),
          ),
        );
      }),
    ),
);

it.effect.each([
  "in the restart gap",
  "after the replacement turn",
  "after a mid-turn update",
] as const)(
  "keeps a soft-restarted attempt's surviving subagent to its terminal: completes %s",
  (completes) =>
    Effect.scoped(
      Effect.gen(function* () {
        let oldTurn: OrchestrationV2ProviderTurn | undefined;
        let survivor: Extract<ProviderAdapterV2Event, { type: "turn_item.updated" }> | undefined;
        let survivorRow: OrchestrationV2Subagent | undefined;
        const midTurn = completes === "after a mid-turn update";
        const survivorCompleted = () =>
          Effect.gen(function* () {
            const now = yield* DateTime.now;
            if (midTurn)
              yield* Queue.offer(events, {
                type: "subagent.updated",
                driver,
                subagent: {
                  ...survivorRow!,
                  status: "completed",
                  result: "SURVIVOR_DONE",
                  completedAt: now,
                  updatedAt: now,
                },
              });
            yield* Queue.offer(events, {
              ...survivor!,
              turnItem: {
                ...survivor!.turnItem,
                status: "completed",
                result: "SURVIVOR_DONE",
                completedAt: now,
              } as OrchestrationV2TurnItem,
            });
          });
        const { events, started, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
          `soft-restart-surviving-subagent-${completes.replaceAll(" ", "-")}`,
          true,
          // ACP without native steering restarts the turn on a routine steer.
          // Its soft interrupt cancels the foreground and carries live
          // subagents over instead of terminalizing them.
          (input): Effect.Effect<void, ProviderAdapterInterruptError> =>
            Effect.gen(function* () {
              assert.notEqual(input.requestRuntimeRestart, true);
              const now = yield* DateTime.now;
              yield* Queue.offer(events, {
                type: "provider_turn.updated",
                driver,
                providerTurn: { ...oldTurn!, status: "interrupted", completedAt: now },
              });
              yield* Queue.offer(events, {
                type: "turn.terminal",
                driver,
                providerThreadId: oldTurn!.providerThreadId,
                providerTurnId: oldTurn!.id,
                runOrdinal: started[0]!.runOrdinal,
                status: "interrupted",
                failure: null,
                threadDisposition: "reusable",
              });
              // Published before the replacement attempt has subscribed.
              if (completes === "in the restart gap") yield* survivorCompleted();
            }),
          { supportsActiveSteering: false },
        );
        const oldIngestionEnded = yield* Deferred.make<void>();
        const replacementIngestionEnded = yield* Deferred.make<void>();
        let oldAttemptId: RunAttemptId | undefined;
        let replacementAttemptId: RunAttemptId | undefined;
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const threadId = yield* startFirstTurn;
          const first = started[0]!;
          oldAttemptId = first.attemptId;
          const now = yield* DateTime.now;
          const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
            orchestrator.streamDomainEvents.pipe(
              Stream.filter(predicate),
              Stream.take(1),
              Stream.runDrain,
              Effect.forkScoped({ startImmediately: true }),
            );
          oldTurn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
          survivor = {
            type: "turn_item.updated",
            driver,
            turnItem: {
              id: TurnItemId.make("subagent:surviving"),
              type: "subagent",
              threadId,
              runId: first.runId,
              nodeId: first.rootNodeId,
              providerThreadId: first.providerThread.id,
              providerTurnId: oldTurn.id,
              nativeItemRef: { driver, nativeId: "surviving-task", strength: "strong" },
              parentItemId: null,
              ordinal: 1,
              status: "running",
              title: "Surviving subagent",
              startedAt: now,
              completedAt: null,
              updatedAt: now,
              subagentId: NodeId.make("node:surviving-subagent"),
              origin: "provider_native",
              driver,
              providerInstanceId: instanceId,
              childThreadId: null,
              prompt: "keep working",
              result: null,
            },
          };
          const survivorRunning = yield* watch(
            (event) =>
              event.type === "turn-item.updated" && event.payload.id === survivor!.turnItem.id,
          );
          yield* Queue.offer(events, survivor);
          yield* Fiber.join(survivorRunning);

          const replacementRunning = yield* watch(
            (event) =>
              event.type === "provider-turn.updated" &&
              event.payload.id !== oldTurn!.id &&
              event.payload.status === "running",
          );
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("soft-restart"),
            threadId,
            messageId: MessageId.make("message:soft-restart"),
            text: "Change course",
            attachments: [],
            dispatchMode: { type: "steer_active", targetRunId: first.runId },
            createdBy: "agent",
            creationSource: "mcp",
          });
          yield* worker.drain();
          yield* Fiber.join(replacementRunning);
          assert.lengthOf(started, 2);
          const replacement = started[1]!;
          assert.equal(replacement.runId, first.runId);
          replacementAttemptId = replacement.attemptId;
          const nextTurn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns.find(
            (turn) => turn.runAttemptId === replacement.attemptId,
          )!;
          if (midTurn) {
            // A progress update while the replacement root is open must not
            // make the replacement wait for a terminal it will not receive.
            survivorRow = {
              id:
                survivor!.turnItem.type === "subagent"
                  ? survivor!.turnItem.subagentId
                  : NodeId.make("unused"),
              threadId,
              runId: first.runId,
              parentNodeId: first.rootNodeId,
              origin: "provider_native",
              createdBy: "agent",
              driver,
              providerInstanceId: instanceId,
              providerThreadId: null,
              childThreadId: null,
              nativeTaskRef: { driver, nativeId: "surviving-task", strength: "strong" },
              prompt: "keep working",
              title: "Surviving subagent",
              model: null,
              status: "running",
              result: null,
              startedAt: now,
              completedAt: null,
              updatedAt: now,
            };
            const progressStored = yield* watch(
              (event) =>
                event.type === "turn-item.updated" &&
                event.payload.id === survivor!.turnItem.id &&
                event.payload.title === "Surviving subagent: still working",
            );
            yield* Queue.offer(events, { type: "subagent.updated", driver, subagent: survivorRow });
            yield* Queue.offer(events, {
              ...survivor!,
              turnItem: { ...survivor!.turnItem, title: "Surviving subagent: still working" },
            });
            yield* Fiber.join(progressStored);
          }
          const replacementRootSettled = yield* watch(
            (event) =>
              event.type === "run.updated" &&
              event.payload.activeAttemptId === replacement.attemptId &&
              event.payload.status === "waiting",
          );
          yield* Queue.offer(events, {
            type: "provider_turn.updated",
            driver,
            providerTurn: { ...nextTurn, status: "completed", completedAt: now },
          });
          yield* Queue.offer(events, {
            type: "turn.terminal",
            driver,
            providerThreadId: replacement.providerThread.id,
            providerTurnId: nextTurn.id,
            runOrdinal: replacement.runOrdinal,
            status: "completed",
            failure: null,
            threadDisposition: "reusable",
          });
          // The replacement root has settled before the survivor reports.
          yield* Fiber.join(replacementRootSettled);
          if (completes !== "in the restart gap") yield* survivorCompleted();
          yield* Deferred.await(oldIngestionEnded);
          yield* Deferred.await(replacementIngestionEnded);
          yield* worker.drain();

          const sequence = yield* orchestrator.getThreadEventSequence(threadId);
          const stored = yield* orchestrator
            .streamStoredEventsFrom({ threadId, afterSequence: 0 })
            .pipe(
              Stream.takeUntil((event) => event.sequence === sequence),
              Stream.map((event) => event.event),
              Stream.runCollect,
            );
          assert.deepEqual(
            stored.flatMap((event) =>
              event.type === "turn-item.updated" && event.payload.id === survivor!.turnItem.id
                ? [event.payload.status]
                : [],
            ),
            midTurn ? ["running", "running", "completed"] : ["running", "completed"],
          );
          const projected = yield* orchestrator.getThreadProjection(threadId);
          if (midTurn)
            assert.equal(
              projected.subagents.find((row) => row.id === survivorRow!.id)?.status,
              "completed",
            );
          assert.equal(projected.runs[0]?.status, "completed");
        }).pipe(
          Effect.provide(
            layer.pipe(
              Layer.provide(
                Layer.succeed(StreamClock.StreamClockHooks, {
                  beginAttempt: () => Effect.void,
                  observe: () => Effect.void,
                  endAttempt: (_threadId, _runId, attemptId) =>
                    Effect.gen(function* () {
                      if (attemptId === oldAttemptId)
                        yield* Deferred.succeed(oldIngestionEnded, undefined);
                      if (attemptId === replacementAttemptId)
                        yield* Deferred.succeed(replacementIngestionEnded, undefined);
                    }),
                }),
              ),
            ),
          ),
        );
      }),
    ),
);

it.effect("sends a selected held message once and preserves the remaining queue order", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { started, steered, layer, startFirstTurn } =
        yield* nextTurnSelectionHarness("send-held-message");
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        yield* Effect.forEach(["queued-first", "selected", "queued-last"], (text) =>
          orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(text),
            threadId,
            messageId: MessageId.make(`message:${text}`),
            text,
            attachments: [],
            modelSelection: runSelection,
            dispatchMode: { type: "queue_after_active" },
            createdBy: "user",
            creationSource: "web",
          }),
        );
        const before = yield* orchestrator.getThreadProjection(threadId);
        const selected = before.runs.find(
          (run) => run.userMessageId === MessageId.make("message:selected"),
        )!;
        const sink = yield* EventSink.EventSinkV2;
        const now = yield* DateTime.now;
        // Restart recovery cancels the active run and holds every queued message.
        yield* sink.write({
          events: [
            ...before.runs.map((run) => ({
              id: EventId.make(`restart:${run.id}`),
              type: "run.updated" as const,
              threadId,
              runId: run.id,
              occurredAt: now,
              payload:
                run.status === "queued"
                  ? { ...run, queueHeld: true }
                  : { ...run, status: "cancelled" as const, completedAt: now },
            })),
            ...before.providerTurns.map((turn) => ({
              id: EventId.make(`restart:${turn.id}`),
              type: "provider-turn.updated" as const,
              threadId,
              occurredAt: now,
              payload: { ...turn, status: "cancelled" as const, completedAt: now },
            })),
          ],
        });
        const command = {
          type: "queued-run.start" as const,
          commandId: CommandId.make("send-selected"),
          threadId,
          runId: selected.id,
        };
        yield* orchestrator.dispatch(command);
        yield* worker.drain();
        yield* orchestrator.dispatch(command);
        yield* worker.drain();
        const after = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(after.runs.find((run) => run.id === selected.id)?.status, "running");
        assert.deepEqual(
          started.map((input) => input.message.text),
          ["first", "selected"],
        );
        assert.equal(started[1]?.runId, selected.id);
        assert.equal(started[1]?.message.messageId, selected.userMessageId);
        assert.deepEqual(started[1]?.modelSelection, runSelection);
        assert.deepEqual(steered, []);
        assert.lengthOf(
          after.messages.filter((message) => message.id === selected.userMessageId),
          1,
        );
        assert.deepEqual(
          after.runs.filter((run) => run.status === "queued").map((run) => run.userMessageId),
          [MessageId.make("message:queued-first"), MessageId.make("message:queued-last")],
        );
        assert.isFalse(after.runs.some((run) => run.status === "queued" && run.queueHeld));
        const duplicate = yield* orchestrator
          .dispatch({
            ...command,
            commandId: CommandId.make("send-selected-again"),
          })
          .pipe(Effect.flip);
        assert.equal(duplicate._tag, "OrchestratorDispatchError");
      }).pipe(Effect.provide(layer));
    }),
  ),
);

it.effect("force sends multiple queued messages and can force send a requeued message again", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
        "multiple-promoted-messages",
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        const queue = (text: string, commandId: string) =>
          orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(commandId),
            threadId,
            messageId: MessageId.make(`message:${text}`),
            text,
            attachments: [],
            dispatchMode: { type: "queue_after_active" },
            createdBy: "user",
            creationSource: "web",
          });
        for (const text of ["one", "two", "remaining"]) yield* queue(text, `queue-${text}`);
        const promote = Effect.fn("test.promote")(function* (text: string, commandId: string) {
          const projection = yield* orchestrator.getThreadProjection(threadId);
          const queued = projection.runs.find(
            (run) =>
              run.status === "queued" && run.userMessageId === MessageId.make(`message:${text}`),
          )!;
          yield* orchestrator.dispatch({
            type: "queued-message.promote-to-steer",
            commandId: CommandId.make(commandId),
            threadId,
            queuedRunId: queued.id,
            targetRunId: started[0]!.runId,
          });
        });
        yield* promote("one", "promote-one");
        yield* promote("two", "promote-two");
        yield* worker.drain();
        assert.deepEqual(steered, ["one", "two"]);

        // The completion-race fallback redispatches the same message into the queue.
        yield* queue("one", "requeue-one");
        const requeued = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(
          requeued.turnItems.find(
            (item) => item.type === "user_message" && item.messageId === "message:one",
          )?.status,
          "pending",
        );
        yield* promote("one", "promote-one-again");
        yield* worker.drain();
        assert.deepEqual(steered, ["one", "two", "one"]);
        assert.lengthOf(started, 1);
        const final = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(
          final.runs.filter((run) => run.status === "queued").map((run) => run.userMessageId),
          [MessageId.make("message:remaining")],
        );
        for (const text of ["one", "two"]) {
          const items = final.turnItems.filter(
            (item) => item.type === "user_message" && item.messageId === `message:${text}`,
          );
          assert.lengthOf(items, 1);
          assert.equal(items[0]?.status, "completed");
        }
      }).pipe(Effect.provide(layer));
    }),
  ),
);

it.effect("keeps a message queued when send now races with a running turn", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { started, layer, startFirstTurn } =
        yield* nextTurnSelectionHarness("send-queued-race");
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("queue"),
          threadId,
          messageId: MessageId.make("message:queued"),
          text: "queued",
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "user",
          creationSource: "web",
        });
        const before = yield* orchestrator.getThreadProjection(threadId);
        const queued = before.runs.find((run) => run.status === "queued")!;
        const error = yield* orchestrator
          .dispatch({
            type: "queued-run.start",
            commandId: CommandId.make("send-now"),
            threadId,
            runId: queued.id,
          })
          .pipe(Effect.flip);
        yield* worker.drain();
        assert.equal(error._tag, "OrchestratorDispatchError");
        assert.equal(started.length, 1);
        const after = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(after.runs, before.runs);
      }).pipe(Effect.provide(layer));
    }),
  ),
);

// regression: #19210 a provider that can restart the turn must still steer, since restarting ends its native subagents
it.effect(
  "steers a changed turn-scoped selection into a running turn the provider could restart",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
          "steering-selection-change",
          true,
        );
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const threadId = yield* startFirstTurn;
          const steer = (id: string, selection: ModelSelection) =>
            orchestrator
              .dispatch({
                type: "message.dispatch",
                commandId: CommandId.make(id),
                threadId,
                messageId: MessageId.make(`message:${id}`),
                text: id,
                attachments: [],
                modelSelection: selection,
                dispatchMode: { type: "steer_active", targetRunId: started[0]!.runId },
                createdBy: "user",
                creationSource: "web",
              })
              .pipe(Effect.andThen(worker.drain()));

          yield* steer("steer-changed", composerSelection);
          const changed = yield* orchestrator.getThreadProjection(threadId);
          assert.deepEqual(steered, ["steer-changed"]);
          assert.equal(started.length, 1);
          assert.lengthOf(changed.attempts, 1);
          assert.equal(changed.runs[0]?.status, "running");
          assert.deepEqual(changed.runs[0]?.modelSelection, runSelection);
          assert.deepEqual(changed.thread.modelSelection, composerSelection);

          yield* steer("steer-reverted", runSelection);
          const reverted = yield* orchestrator.getThreadProjection(threadId);
          assert.deepEqual(steered, ["steer-changed", "steer-reverted"]);
          assert.lengthOf(reverted.attempts, 1);
          assert.deepEqual(reverted.thread.modelSelection, runSelection);

          const otherSelection = {
            instanceId: ProviderInstanceId.make("codex-work"),
            model: "other",
          };
          const sink = yield* EventSink.EventSinkV2;
          yield* sink.write({
            events: [
              {
                id: EventId.make("switched-away"),
                type: "thread.provider-switched",
                threadId,
                providerInstanceId: otherSelection.instanceId,
                occurredAt: yield* DateTime.now,
                payload: {
                  ...reverted.thread,
                  providerInstanceId: otherSelection.instanceId,
                  modelSelection: otherSelection,
                },
              },
            ],
          });
          yield* steer("steer-back", runSelection);
          const back = yield* orchestrator.getThreadProjection(threadId);
          assert.deepEqual(steered, ["steer-changed", "steer-reverted", "steer-back"]);
          assert.equal(back.thread.providerInstanceId, instanceId);
          assert.deepEqual(back.thread.modelSelection, runSelection);
        }).pipe(Effect.provide(layer));
      }),
    ),
);

it.effect("restarts the turn for an image when native steering takes text only", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
        "steering-text-only-attachment",
        true,
        undefined,
        { activeSteeringAcceptsAttachments: false },
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("steer-image"),
          threadId,
          messageId: MessageId.make("message:steer-image"),
          text: "look at this",
          attachments: [
            {
              type: "image",
              id: ChatAttachmentId.make("steer-image"),
              name: "screenshot.png",
              mimeType: "image/png",
              sizeBytes: 4,
            },
          ],
          modelSelection: runSelection,
          dispatchMode: { type: "steer_active", targetRunId: started[0]!.runId },
          createdBy: "user",
          creationSource: "web",
        });
        yield* worker.drain();
        assert.deepEqual(steered, []);
        assert.deepEqual(
          started.map((turn) => [turn.message.text, turn.message.attachments.length]),
          [
            ["first", 0],
            ["look at this", 1],
          ],
        );
      }).pipe(Effect.provide(layer));
    }),
  ),
);

it.effect("starts a steer that missed the turn on the saved next-turn selection", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { events, started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
        "steering-selection-follow-up",
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        const first = started[0]!;
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("steer"),
          threadId,
          messageId: MessageId.make("message:steer"),
          text: "late steer",
          attachments: [],
          modelSelection: composerSelection,
          dispatchMode: { type: "steer_active", targetRunId: first.runId },
          createdBy: "user",
          creationSource: "web",
        });
        const settled = yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) =>
              event.type === "run.updated" &&
              event.payload.id === first.runId &&
              event.payload.status === "waiting",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        const turn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
        yield* Queue.offer(events, {
          type: "provider_turn.updated",
          driver,
          providerTurn: { ...turn, status: "completed", completedAt: yield* DateTime.now },
        });
        yield* Queue.offer(events, {
          type: "turn.terminal",
          driver,
          providerThreadId: turn.providerThreadId,
          providerTurnId: turn.id,
          runOrdinal: first.runOrdinal,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* Fiber.join(settled);
        yield* worker.drain();
        yield* orchestrator.resumeQueuedRuns;
        yield* worker.drain();

        const projection = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(steered, []);
        assert.equal(started.length, 2);
        assert.equal(started[1]?.message.messageId, MessageId.make("message:steer"));
        assert.deepEqual(started[1]?.modelSelection, composerSelection);
        assert.deepEqual(projection.thread.modelSelection, composerSelection);
      }).pipe(Effect.provide(layer));
    }),
  ),
);

// regression: #26695 cancelling a Copilot prompt to steer it kills its background agents
it.effect("queues a steer until the turn ends when the provider cannot steer it", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { events, started, steered, interrupted, layer, startFirstTurn } =
        yield* nextTurnSelectionHarness("steering-unsupported", false, undefined, {
          supportsActiveSteering: false,
        });
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        const first = started[0]!;
        const messageId = MessageId.make("message:steer");
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("steer"),
          threadId,
          messageId,
          text: "follow up",
          attachments: [],
          modelSelection: runSelection,
          dispatchMode: { type: "start_immediately" },
          deliveryIntent: "steer",
          createdBy: "user",
          creationSource: "web",
        });
        yield* worker.drain();

        const held = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(steered, []);
        assert.deepEqual(interrupted, []);
        assert.equal(started.length, 1);
        assert.equal(held.runs.find((run) => run.id === first.runId)?.status, "running");
        const queued = held.runs.find((run) => run.userMessageId === messageId);
        assert.equal(queued?.status, "queued");

        const settled = yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) =>
              event.type === "run.updated" &&
              event.payload.id === first.runId &&
              event.payload.status === "waiting",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        const turn = held.providerTurns[0]!;
        yield* Queue.offer(events, {
          type: "provider_turn.updated",
          driver,
          providerTurn: { ...turn, status: "completed", completedAt: yield* DateTime.now },
        });
        yield* Queue.offer(events, {
          type: "turn.terminal",
          driver,
          providerThreadId: turn.providerThreadId,
          providerTurnId: turn.id,
          runOrdinal: first.runOrdinal,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* Fiber.join(settled);
        yield* worker.drain();
        yield* orchestrator.resumeQueuedRuns;
        yield* worker.drain();

        assert.deepEqual(interrupted, []);
        assert.equal(started.length, 2);
        assert.equal(started[1]?.runId, queued?.id);
        assert.equal(started[1]?.message.text, "follow up");
      }).pipe(Effect.provide(layer));
    }),
  ),
);

// regression: #33454 a steer that restarts a completion wake run left its cohort claimed forever
const restartCompletionWake = Effect.fn("restartCompletionWake")(function* (
  harness: Pick<
    Effect.Success<ReturnType<typeof nextTurnSelectionHarness>>,
    "events" | "started" | "startFirstTurn"
  >,
  siblingFinishesFirst: boolean,
) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
  const sink = yield* EventSink.EventSinkV2;
  const threadId = yield* harness.startFirstTurn;
  const watchRun = (runId: string, status: string) =>
    orchestrator.streamDomainEvents.pipe(
      Stream.filter(
        (event) =>
          event.type === "run.updated" &&
          event.payload.id === runId &&
          event.payload.status === status,
      ),
      Stream.take(1),
      Stream.runDrain,
      Effect.forkScoped,
    );
  const watchTurnRunning = (ordinal: number) =>
    orchestrator.streamDomainEvents.pipe(
      Stream.filter(
        (event) =>
          event.type === "provider-turn.updated" &&
          event.payload.status === "running" &&
          event.payload.ordinal === ordinal,
      ),
      Stream.take(1),
      Stream.runDrain,
      Effect.forkScoped,
    );
  const finishTurn = (turn: ProviderAdapter.ProviderAdapterV2TurnInput) =>
    Effect.gen(function* () {
      const providerTurn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns.find(
        (candidate) => candidate.runAttemptId === turn.attemptId,
      )!;
      const settled = yield* watchRun(turn.runId, "waiting");
      yield* Queue.offer(harness.events, {
        type: "provider_turn.updated",
        driver,
        providerTurn: {
          ...providerTurn,
          status: "completed",
          completedAt: yield* DateTime.now,
        },
      });
      yield* Queue.offer(harness.events, {
        type: "turn.terminal",
        driver,
        providerThreadId: providerTurn.providerThreadId,
        providerTurnId: providerTurn.id,
        runOrdinal: turn.runOrdinal,
        status: "completed",
        failure: null,
        threadDisposition: "reusable",
      });
      yield* Fiber.join(settled);
      yield* worker.drain();
    });

  const origin = harness.started[0]!;
  const firstTaskId = NodeId.make("task:steered-wake-first");
  const siblingTaskId = NodeId.make("task:steered-wake-sibling");
  const now = yield* DateTime.now;
  yield* sink.write({
    events: [firstTaskId, siblingTaskId].map((taskId) => ({
      id: EventId.make(`event:seed:${taskId}`),
      type: "subagent.updated" as const,
      threadId,
      runId: origin.runId,
      nodeId: taskId,
      occurredAt: now,
      payload: {
        id: taskId,
        threadId,
        runId: origin.runId,
        parentNodeId: origin.rootNodeId,
        origin: "app_owned" as const,
        createdBy: "agent" as const,
        driver,
        providerInstanceId: instanceId,
        providerThreadId: null,
        childThreadId: null,
        remoteChild: {
          environmentId: EnvironmentId.make("environment:box"),
          threadId: ThreadId.make(`thread:remote:${taskId}`),
          label: "box",
        },
        nativeTaskRef: null,
        prompt: "Do background work",
        title: null,
        model: null,
        completionWake: "always" as const,
        completionDelivery: { state: "pending" as const, observedByRunId: null },
        status: "running" as const,
        result: null,
        startedAt: now,
        completedAt: null,
        updatedAt: now,
      },
    })),
  });
  yield* finishTurn(origin);

  const completeTask = (taskId: NodeId) =>
    orchestrator.dispatch({
      type: "delegated_task.remote.complete",
      commandId: CommandId.make(`command:remote-complete:${taskId}`),
      parentThreadId: threadId,
      taskId,
      status: "completed",
      result: `${taskId} finished`,
      stoppedThere: "ran-out",
    });
  yield* completeTask(firstTaskId);
  const firstDelivery = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
    (run) => run.id === origin.runId,
  )!.delegatedCompletion!.delivery!;
  assert.deepEqual(firstDelivery.taskIds, [firstTaskId]);
  yield* orchestrator.dispatch({
    type: "message.dispatch",
    commandId: CommandId.make("command:first-wake"),
    threadId,
    messageId: firstDelivery.messageId,
    text: "Delegated task completed",
    attachments: [],
    modelSelection: runSelection,
    dispatchMode: { type: "queue_after_active" },
    delegatedCompletion: {
      parentRunId: origin.runId,
      generation: firstDelivery.generation,
      taskIds: firstDelivery.taskIds,
    },
    createdBy: "agent",
    creationSource: "server",
  });
  const wakeRunning = yield* watchTurnRunning(2);
  yield* orchestrator.resumeQueuedRuns;
  yield* worker.drain();
  yield* Fiber.join(wakeRunning);
  const wake = harness.started[1]!;
  assert.equal(wake.message.messageId, firstDelivery.messageId);

  if (siblingFinishesFirst) {
    yield* completeTask(siblingTaskId);
  }

  // The provider cannot steer natively, so the steer restarts the wake run.
  const steeredRunning = yield* watchTurnRunning(3);
  yield* orchestrator.dispatch({
    type: "message.dispatch",
    commandId: CommandId.make("command:steer-wake"),
    threadId,
    messageId: MessageId.make("message:steer-wake"),
    text: "also check the logs",
    attachments: [],
    modelSelection: runSelection,
    dispatchMode: { type: "steer_active", targetRunId: wake.runId },
    createdBy: "user",
    creationSource: "web",
  });
  yield* worker.drain();
  yield* Fiber.join(steeredRunning);
  const steered = harness.started[2]!;
  assert.equal(steered.runId, wake.runId);
  assert.equal(
    (yield* orchestrator.getThreadProjection(threadId)).attempts.find(
      (attempt) => attempt.id === steered.attemptId,
    )?.reason,
    "steering_restart",
  );
  return {
    threadId,
    origin,
    wake,
    steered,
    firstTaskId,
    siblingTaskId,
    watchRun,
    finishTurn,
    completeTask,
  };
});

it.effect("wakes the parent for a sibling result after a steer restarts the completion wake", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const harness = yield* nextTurnSelectionHarness("steered-completion-wake", true, undefined, {
        supportsActiveSteering: false,
      });
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const {
          threadId,
          origin,
          wake,
          steered,
          firstTaskId,
          siblingTaskId,
          watchRun,
          finishTurn,
          completeTask,
        } = yield* restartCompletionWake(harness, false);

        // A follow-up queued behind the wake starts once the terminal-run listener has settled it.
        const followUp = MessageId.make("message:follow-up");
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("command:follow-up"),
          threadId,
          messageId: followUp,
          text: "follow up",
          attachments: [],
          modelSelection: runSelection,
          dispatchMode: { type: "queue_after_active" },
          createdBy: "user",
          creationSource: "web",
        });
        const followUpRun = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (run) => run.userMessageId === followUp,
        )!;
        const followUpStarted = yield* watchRun(followUpRun.id, "starting");
        yield* finishTurn(steered);
        yield* Fiber.join(followUpStarted);
        assert.equal(
          (yield* orchestrator.getThreadProjection(threadId)).runs.find(
            (run) => run.id === wake.runId,
          )?.status,
          "completed",
        );

        yield* completeTask(siblingTaskId);
        const final = yield* orchestrator.getThreadProjection(threadId);
        const tasks = new Map(
          final.subagents.map((task) => [task.id, task.completionDelivery?.state]),
        );
        assert.deepInclude(
          final.runs.find((run) => run.id === origin.runId)?.delegatedCompletion?.delivery ?? {},
          { generation: 2, taskIds: [siblingTaskId] },
          "the sibling finished after the steered wake but no new completion wake was reserved",
        );
        assert.equal(tasks.get(siblingTaskId), "claimed");
        assert.equal(
          tasks.get(firstTaskId),
          "delivered",
          "the steered wake run's own task stays claimed after that run completed",
        );
      }).pipe(Effect.provide(harness.layer));
    }),
  ),
);

it.effect(
  "offers the next completion wake when a steer restarts a wake a sibling finished behind",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* nextTurnSelectionHarness(
          "steered-completion-wake-behind",
          true,
          undefined,
          { supportsActiveSteering: false },
        );
        const offers: ProviderContinuationRequests.ProviderContinuationRequest[] = [];
        const requests = Layer.succeed(ProviderContinuationRequests.ProviderContinuationRequests, {
          offer: (request) => Effect.sync(() => void offers.push(request)),
          take: Effect.never,
        });
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const { threadId, origin, firstTaskId, siblingTaskId } = yield* restartCompletionWake(
            harness,
            true,
          );
          const projection = yield* orchestrator.getThreadProjection(threadId);
          const delivery = projection.runs.find((run) => run.id === origin.runId)
            ?.delegatedCompletion?.delivery;
          assert.deepInclude(delivery ?? {}, { generation: 2, taskIds: [siblingTaskId] });
          const tasks = new Map(
            projection.subagents.map((task) => [task.id, task.completionDelivery?.state]),
          );
          assert.equal(tasks.get(firstTaskId), "delivered");
          assert.equal(tasks.get(siblingTaskId), "claimed");
          assert.deepEqual(offers.at(-1)?.delegatedCompletion, {
            parentRunId: origin.runId,
            generation: 2,
            messageId: delivery!.messageId,
          });
        }).pipe(Effect.provide(harness.layer.pipe(Layer.provide(requests))));
      }),
    ),
);

// Codex flushes held drafts after the final answer; reserve their pre-steer position before turn end.
it.effect("keeps a held commentary draft before the steer when turn end flushes it", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { events, started, layer, startFirstTurn } =
        yield* nextTurnSelectionHarness("steering-held-draft");
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        const first = started[0]!;
        const turn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
        const now = yield* DateTime.now;
        const assistantItem = (id: string, text: string, streaming: boolean) =>
          ({
            type: "turn_item.updated",
            driver,
            turnItem: {
              id: TurnItemId.make(id),
              threadId,
              runId: first.runId,
              nodeId: first.rootNodeId,
              providerThreadId: turn.providerThreadId,
              providerTurnId: turn.id,
              nativeItemRef: null,
              parentItemId: null,
              ordinal: 0,
              status: streaming ? "running" : "completed",
              title: null,
              startedAt: now,
              completedAt: streaming ? null : now,
              updatedAt: now,
              type: "assistant_message",
              messageId: MessageId.make(`message:${id}`),
              text,
              streaming,
            },
          }) satisfies ProviderAdapter.ProviderAdapterV2Event;
        const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
          orchestrator.streamDomainEvents.pipe(
            Stream.filter(predicate),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );

        yield* Queue.offer(events, assistantItem("draft", "Let me look at the", true));
        const commentaryWritten = yield* watch(
          (event) => event.type === "turn-item.updated" && event.payload.id === "commentary",
        );
        yield* Queue.offer(events, assistantItem("commentary", "Checking the popover.", false));
        yield* Fiber.join(commentaryWritten);

        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("steer"),
          threadId,
          messageId: MessageId.make("message:steer"),
          text: "fix the popover",
          attachments: [],
          dispatchMode: { type: "steer_active", targetRunId: first.runId },
          createdBy: "user",
          creationSource: "web",
        });
        yield* worker.drain();

        const settled = yield* watch(
          (event) =>
            event.type === "run.updated" &&
            event.payload.id === first.runId &&
            event.payload.status === "waiting",
        );
        yield* Queue.offer(events, assistantItem("answer", "All fixed.", false));
        yield* Queue.offer(events, assistantItem("draft", "Let me look at the", false));
        yield* Queue.offer(events, {
          type: "provider_turn.updated",
          driver,
          providerTurn: { ...turn, status: "completed", completedAt: yield* DateTime.now },
        });
        yield* Queue.offer(events, {
          type: "turn.terminal",
          driver,
          providerThreadId: turn.providerThreadId,
          providerTurnId: turn.id,
          runOrdinal: first.runOrdinal,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* Fiber.join(settled);
        yield* worker.drain();

        const items = (yield* orchestrator.getThreadProjection(threadId)).turnItems
          .toSorted((left, right) => left.ordinal - right.ordinal)
          .flatMap((item) =>
            item.type === "user_message"
              ? [String(item.messageId)]
              : item.type === "assistant_message"
                ? [String(item.id)]
                : [],
          );
        assert.deepEqual(items, [
          "message:first",
          "draft",
          "commentary",
          "message:steer",
          "answer",
        ]);
      }).pipe(Effect.provide(layer));
    }),
  ),
);

it.effect("steers a promoted queued message with the selection it was queued with", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
        "promote-queued-selection",
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("queued"),
          threadId,
          messageId: MessageId.make("message:queued"),
          text: "queued",
          attachments: [],
          modelSelection: composerSelection,
          dispatchMode: { type: "queue_after_active" },
          createdBy: "user",
          creationSource: "web",
        });
        const queuedRun = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (run) => run.status === "queued",
        )!;
        yield* orchestrator.dispatch({
          type: "queued-message.promote-to-steer",
          commandId: CommandId.make("promote"),
          threadId,
          queuedRunId: queuedRun.id,
          targetRunId: started[0]!.runId,
        });
        yield* worker.drain();

        const projection = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(steered, ["queued"]);
        assert.deepEqual(projection.thread.modelSelection, composerSelection);
      }).pipe(Effect.provide(layer));
    }),
  ),
);

it.effect("marks the model switch on a promoted message that restarts the turn", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { started, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
        "promote-restart-switch",
        true,
        undefined,
        { supportsActiveSteering: false },
      );
      const otherSelection = { ...runSelection, model: "other-model" };
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("queued"),
          threadId,
          messageId: MessageId.make("message:queued"),
          text: "queued",
          attachments: [],
          modelSelection: otherSelection,
          dispatchMode: { type: "queue_after_active" },
          createdBy: "user",
          creationSource: "web",
        });
        const queuedRun = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (run) => run.status === "queued",
        )!;
        yield* orchestrator.dispatch({
          type: "queued-message.promote-to-steer",
          commandId: CommandId.make("promote"),
          threadId,
          queuedRunId: queuedRun.id,
          targetRunId: started[0]!.runId,
        });
        yield* worker.drain();

        const promoted = (yield* orchestrator.getThreadProjection(threadId)).turnItems.find(
          (item) => item.type === "user_message" && item.messageId === "message:queued",
        );
        assert.deepEqual(promoted?.type === "user_message" ? promoted.modelSwitch : undefined, {
          from: runSelection,
          to: otherSelection,
        });
      }).pipe(Effect.provide(layer));
    }),
  ),
);

it.effect("keeps a queued message queued when promoted into a run that Stop reached", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { started, steered, layer, startFirstTurn } =
        yield* nextTurnSelectionHarness("steering-after-stop");
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        const first = started[0]!;
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("queued"),
          threadId,
          messageId: MessageId.make("message:queued"),
          text: "queued",
          attachments: [],
          modelSelection: runSelection,
          dispatchMode: { type: "queue_after_active" },
          createdBy: "user",
          creationSource: "web",
        });
        const queuedRun = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (run) => run.status === "queued",
        )!;
        // The worker has not delivered the interrupt yet, so the run still reads as running.
        yield* orchestrator.dispatch({
          type: "run.interrupt",
          commandId: CommandId.make("stop"),
          threadId,
          runId: first.runId,
          holdQueue: true,
        });
        const promoteError = yield* orchestrator
          .dispatch({
            type: "queued-message.promote-to-steer",
            commandId: CommandId.make("promote"),
            threadId,
            queuedRunId: queuedRun.id,
            targetRunId: first.runId,
          })
          .pipe(Effect.flip);
        yield* worker.drain();

        const projection = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(promoteError._tag, "OrchestratorDispatchError");
        assert.deepEqual(steered, []);
        const queued = projection.runs.find((run) => run.id === queuedRun.id);
        assert.equal(queued?.status, "queued");
        assert.isTrue(queued?.queueHeld);
      }).pipe(Effect.provide(layer));
    }),
  ),
);

// regression: #32891 a restart that Stop already reached is lost
it.effect("refuses a steer but starts a restart sent into a run that Stop reached", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { started, steered, layer, startFirstTurn } =
        yield* nextTurnSelectionHarness("restart-after-stop");
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        const first = started[0]!;
        // The worker has not delivered the interrupt yet, so the run still reads as running.
        yield* orchestrator.dispatch({
          type: "run.interrupt",
          commandId: CommandId.make("stop"),
          threadId,
          runId: first.runId,
        });
        const steerError = yield* orchestrator
          .dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("steer"),
            threadId,
            messageId: MessageId.make("message:steer"),
            text: "steer",
            attachments: [],
            modelSelection: runSelection,
            dispatchMode: { type: "steer_active", targetRunId: first.runId },
            createdBy: "user",
            creationSource: "web",
          })
          .pipe(Effect.flip);
        assert.equal(steerError._tag, "OrchestratorDispatchError");
        assert.include(String(steerError.cause), "is stopping and cannot be steered");
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("restart"),
          threadId,
          messageId: MessageId.make("message:restart"),
          text: "restart",
          attachments: [],
          modelSelection: runSelection,
          dispatchMode: { type: "restart_active", targetRunId: first.runId },
          createdBy: "user",
          creationSource: "web",
        });
        yield* worker.drain();

        assert.deepEqual(steered, []);
        assert.deepEqual(
          started.map((turn) => turn.message.text),
          ["first", "restart"],
        );
      }).pipe(Effect.provide(layer));
    }),
  ),
);

// regression: #32891 a restart that arrives after the turn ended is lost
it.effect.each([
  { status: "completed", settled: true },
  { status: "completed", settled: false },
  { status: "interrupted", settled: true },
  { status: "interrupted", settled: false },
  { status: "cancelled", settled: false },
] as const)(
  "starts a restart that missed the turn ($status, run settled: $settled) as a new turn",
  ({ status, settled: runSettled }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { events, started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
          `restart-after-${status}-${runSettled ? "settled" : "unsettled"}`,
        );
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const threadId = yield* startFirstTurn;
          const first = started[0]!;
          const turn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
          const turnEnded = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "provider-turn.updated" &&
                event.payload.id === turn.id &&
                event.payload.status === status,
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          yield* Queue.offer(events, {
            type: "provider_turn.updated",
            driver,
            providerTurn: { ...turn, status, completedAt: yield* DateTime.now },
          });
          yield* Fiber.join(turnEnded);
          const settleRun = Effect.gen(function* () {
            const settled = yield* orchestrator.streamDomainEvents.pipe(
              Stream.filter(
                (event) =>
                  event.type === "run.updated" &&
                  event.payload.id === first.runId &&
                  event.payload.status !== "running",
              ),
              Stream.take(1),
              Stream.runDrain,
              Effect.forkScoped,
            );
            yield* Queue.offer(events, {
              type: "turn.terminal",
              driver,
              providerThreadId: turn.providerThreadId,
              providerTurnId: turn.id,
              runOrdinal: first.runOrdinal,
              status,
              failure: null,
              threadDisposition: "reusable",
            });
            yield* Fiber.join(settled);
            yield* worker.drain();
          });
          if (runSettled) yield* settleRun;

          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("restart"),
            threadId,
            messageId: MessageId.make("message:restart"),
            text: "restart",
            attachments: [],
            modelSelection: runSelection,
            dispatchMode: { type: "restart_active", targetRunId: first.runId },
            createdBy: "user",
            creationSource: "web",
          });
          yield* worker.drain();
          if (!runSettled) {
            const restartLeftQueue = yield* orchestrator.streamDomainEvents.pipe(
              Stream.filter(
                (event) =>
                  event.type === "run.updated" &&
                  event.payload.userMessageId === "message:restart" &&
                  event.payload.status !== "queued",
              ),
              Stream.take(1),
              Stream.runDrain,
              Effect.forkScoped,
            );
            yield* settleRun;
            yield* Fiber.join(restartLeftQueue);
            yield* worker.drain();
          }

          assert.deepEqual(steered, []);
          assert.deepEqual(
            started.map((turn) => turn.message.text),
            ["first", "restart"],
          );
        }).pipe(Effect.provide(layer));
      }),
    ),
);

// regression: #32891 a restart into a failed provider turn is lost
it.effect("queues a restart sent into a provider turn that failed before its run settled", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { events, started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
        "restart-after-failed-unsettled",
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        const first = started[0]!;
        const turn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
        const turnFailed = yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) =>
              event.type === "provider-turn.updated" &&
              event.payload.id === turn.id &&
              event.payload.status === "failed",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        yield* Queue.offer(events, {
          type: "provider_turn.updated",
          driver,
          providerTurn: { ...turn, status: "failed", completedAt: yield* DateTime.now },
        });
        yield* Fiber.join(turnFailed);

        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("restart"),
          threadId,
          messageId: MessageId.make("message:restart"),
          text: "restart",
          attachments: [],
          modelSelection: runSelection,
          dispatchMode: { type: "restart_active", targetRunId: first.runId },
          createdBy: "user",
          creationSource: "web",
        });
        yield* worker.drain();

        assert.deepEqual(steered, []);
        const projection = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(
          projection.runs.find((run) => run.userMessageId === "message:restart")?.status,
          "queued",
        );
      }).pipe(Effect.provide(layer));
    }),
  ),
);

it.effect("queues a plain send behind the stopped queue and releases its hold", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const interruptReached = yield* Deferred.make<void>();
      const releaseInterrupt = yield* Deferred.make<void>();
      const { started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
        "plain-send-after-stop",
        false,
        () =>
          Deferred.succeed(interruptReached, undefined).pipe(
            Effect.andThen(Deferred.await(releaseInterrupt)),
          ),
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        const send = (id: string, delivery: "queue" | "auto") =>
          orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(id),
            threadId,
            messageId: MessageId.make(`message:${id}`),
            text: id,
            attachments: [],
            modelSelection: runSelection,
            ...(delivery === "queue"
              ? { dispatchMode: { type: "queue_after_active" } }
              : { dispatchMode: { type: "start_immediately" }, deliveryIntent: "auto" }),
            createdBy: "user",
            creationSource: "web",
          });
        yield* send("queued", "queue");
        yield* orchestrator.dispatch({
          type: "run.interrupt",
          commandId: CommandId.make("stop"),
          threadId,
          runId: started[0]!.runId,
          holdQueue: true,
        });
        const held = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (run) => run.userMessageId === MessageId.make("message:queued"),
        )!;
        assert.isTrue(held.queueHeld);
        // The send releases the held queue, but must not steer the interrupted run.
        yield* send("sent", "auto");
        // Once the stop settles, the run-update reactor promotes the queue on
        // its own fiber, so read the queue while the provider is still stopping.
        const draining = yield* worker.drain().pipe(Effect.forkScoped);
        yield* Deferred.await(interruptReached);

        const projection = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(started.length, 1);
        const queued = projection.runs
          .filter((run) => run.status === "queued")
          .sort((left, right) => (left.queuePosition ?? 0) - (right.queuePosition ?? 0));
        assert.deepEqual(
          queued.map(
            (run) => projection.messages.find((message) => message.id === run.userMessageId)?.text,
          ),
          ["queued", "sent"],
        );
        // The user sent it, so the queue they stopped runs again once the provider stops.
        assert.isFalse(queued.some((run) => run.queueHeld === true));
        const watchQueued = (status: "starting" | "running") =>
          orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.payload.id === held.id &&
                event.payload.status === status,
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped({ startImmediately: true }),
          );
        const promoted = yield* watchQueued("starting");
        const running = yield* watchQueued("running");
        yield* Deferred.succeed(releaseInterrupt, undefined);
        yield* Fiber.join(draining);
        yield* Fiber.join(promoted);
        yield* worker.drain();
        yield* Fiber.join(running);
        assert.deepEqual(
          started.map((turn) => turn.message.text),
          ["first", "queued"],
        );
        const resumed = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(
          resumed.runs
            .filter((run) => run.status === "queued")
            .map(
              (run) => resumed.messages.find((message) => message.id === run.userMessageId)?.text,
            ),
          ["sent"],
        );
        assert.deepEqual(steered, []);
      }).pipe(Effect.provide(layer));
    }),
  ),
);

const goalHarness = Effect.fn("goalHarness")(function* (name: string, nativeGoals?: boolean) {
  const harness = yield* nextTurnSelectionHarness(
    name,
    false,
    undefined,
    nativeGoals === undefined ? {} : { supportsNativeGoals: nativeGoals },
  );
  const send = (id: string, text: string, delivery: "queue" | "auto", freshContext = false) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(id),
        threadId: ThreadId.make(`thread:${name}`),
        messageId: MessageId.make(`message:${id}`),
        text,
        attachments: [],
        modelSelection: runSelection,
        ...(freshContext ? { freshContext } : {}),
        ...(delivery === "queue"
          ? { dispatchMode: { type: "queue_after_active" } }
          : { dispatchMode: { type: "start_immediately" }, deliveryIntent: "auto" }),
        createdBy: "user",
        creationSource: "web",
      });
    });
  const read = (threadId: ThreadId) =>
    Effect.gen(function* () {
      const projection = yield* (yield* Orchestrator.OrchestratorV2).getThreadProjection(threadId);
      const textOf = (run: (typeof projection.runs)[number]) =>
        projection.messages.find((message) => message.id === run.userMessageId)?.text;
      return {
        queue: projection.runs
          .filter((run) => run.status === "queued")
          .toSorted((left, right) => (left.queuePosition ?? 0) - (right.queuePosition ?? 0))
          .map(textOf),
        started: harness.started.map((turn) => turn.message.text),
        stopRequested: projection.turnItems.some(
          (item) =>
            item.type === "run_interrupt_request" && item.runId === harness.started[0]?.runId,
        ),
      };
    });
  return { ...harness, send, read };
});

it.effect.each([undefined, false, true])(
  "goal dispatch stops the active turn only with native support: %s",
  (nativeGoals) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { steered, layer, startFirstTurn, send, read } = yield* goalHarness(
          "goal-steer",
          nativeGoals,
        );
        yield* Effect.gen(function* () {
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const threadId = yield* startFirstTurn;
          yield* send("queued", "queued", "queue");
          const sending = send("goal", "/goal all tests pass", "auto");
          if (nativeGoals === false) {
            yield* Effect.flip(sending);
          } else yield* sending;
          yield* worker.drain();

          assert.deepEqual(steered, []);
          assert.deepEqual(yield* read(threadId), {
            queue: ["queued"],
            started: nativeGoals === false ? ["first"] : ["first", "/goal all tests pass"],
            stopRequested: nativeGoals !== false,
          });
        }).pipe(Effect.provide(layer));
      }),
    ),
);

it.effect("starts a goal that names no model on the thread's model ahead of a queued pick", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { started, layer, startFirstTurn, read } = yield* goalHarness("goal-ahead-of-pick");
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        for (const [id, text, modelSelection] of [
          ["picked", "picked", composerSelection],
          ["goal", "/goal all tests pass", undefined],
        ] as const) {
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(id),
            threadId,
            messageId: MessageId.make(`message:${id}`),
            text,
            attachments: [],
            ...(modelSelection === undefined ? {} : { modelSelection }),
            dispatchMode:
              id === "picked" ? { type: "queue_after_active" } : { type: "start_immediately" },
            ...(id === "picked" ? {} : { deliveryIntent: "auto" as const }),
            createdBy: "user",
            creationSource: "web",
          });
        }
        yield* worker.drain();

        assert.deepEqual(yield* read(threadId), {
          queue: ["picked"],
          started: ["first", "/goal all tests pass"],
          stopRequested: true,
        });
        assert.deepEqual(started[1]?.modelSelection, runSelection);
      }).pipe(Effect.provide(layer));
    }),
  ),
);

it.effect("queues a goal clear without stopping work when no goal drives it", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { steered, layer, startFirstTurn, send, read } = yield* goalHarness("goal-clear");
      yield* Effect.gen(function* () {
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        yield* send("queued", "queued", "queue");
        yield* send("clear", "/goal clear", "auto");
        yield* worker.drain();

        assert.deepEqual(steered, []);
        assert.deepEqual(yield* read(threadId), {
          queue: ["queued", "/goal clear"],
          started: ["first"],
          stopRequested: false,
        });
      }).pipe(Effect.provide(layer));
    }),
  ),
);

it.effect("promotes a queued goal by stopping the running turn", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { started, steered, layer, startFirstTurn, send, read } =
        yield* goalHarness("goal-promote");
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        yield* send("queued", "queued", "queue");
        yield* send("goal", "/goal ship it", "queue");
        const goalRun = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (run) => run.userMessageId === MessageId.make("message:goal"),
        )!;
        yield* orchestrator.dispatch({
          type: "queued-message.promote-to-steer",
          commandId: CommandId.make("promote"),
          threadId,
          queuedRunId: goalRun.id,
          targetRunId: started[0]!.runId,
        });
        yield* worker.drain();

        assert.deepEqual(steered, []);
        assert.deepEqual(yield* read(threadId), {
          queue: ["queued"],
          started: ["first", "/goal ship it"],
          stopRequested: true,
        });
      }).pipe(Effect.provide(layer));
    }),
  ),
);

// A fresh message skips compaction as its own turn, which steering a running turn would not.
it.effect.each(["next", "/goal ship it", "fresh next"])(
  "promoting %s into a compaction stops it and sends it next",
  (promoted) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { events, layer, send, read, started, steered, startFirstTurn } = yield* goalHarness(
          `compaction-promote-${promoted.length}`,
        );
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const threadId = yield* startFirstTurn;
          yield* send("compact", "/compact", "queue");
          yield* send("queued", "queued", "queue");
          yield* send("next", promoted, "queue", promoted.startsWith("fresh"));
          const settled = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.payload.id === started[0]!.runId &&
                event.payload.status === "waiting",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          const compacting = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "provider-turn.updated" &&
                event.payload.status === "running" &&
                event.payload.runAttemptId !== started[0]!.attemptId,
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          const turn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
          yield* Queue.offer(events, {
            type: "provider_turn.updated",
            driver,
            providerTurn: { ...turn, status: "completed", completedAt: yield* DateTime.now },
          });
          yield* Queue.offer(events, {
            type: "turn.terminal",
            driver,
            providerThreadId: turn.providerThreadId,
            providerTurnId: turn.id,
            runOrdinal: started[0]!.runOrdinal,
            status: "completed",
            failure: null,
            threadDisposition: "reusable",
          });
          yield* Fiber.join(settled);
          yield* worker.drain();
          yield* orchestrator.resumeQueuedRuns;
          yield* worker.drain();
          yield* Fiber.join(compacting);
          const compactRun = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
            (run) => run.userMessageId === MessageId.make("message:compact"),
          )!;
          const nextRun = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
            (run) => run.userMessageId === MessageId.make("message:next"),
          )!;
          yield* orchestrator.dispatch({
            type: "queued-message.promote-to-steer",
            commandId: CommandId.make("promote"),
            threadId,
            queuedRunId: nextRun.id,
            targetRunId: compactRun.id,
          });
          yield* worker.drain();

          const projection = yield* orchestrator.getThreadProjection(threadId);
          assert.deepEqual(steered, []);
          assert.isTrue(
            projection.turnItems.some(
              (item) => item.type === "run_interrupt_request" && item.runId === compactRun.id,
            ),
          );
          assert.deepEqual((yield* read(threadId)).queue, ["queued"]);
          assert.deepEqual(
            started.map((turn) => turn.message.text),
            ["first", "/compact", promoted],
          );
        }).pipe(Effect.provide(layer));
      }),
    ),
);

it.effect("stops a turn that is still starting when a goal is sent into it", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { cwd, layer, send, started } = yield* goalHarness("goal-during-start");
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = ThreadId.make("thread:goal-during-start");
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make("create"),
          threadId,
          projectId: ProjectId.make("project:goal-during-start"),
          title: "Goal during start",
          modelSelection: runSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: cwd,
          createdBy: "user",
          creationSource: "web",
        });
        // The stopped run's end starts the queue, which then needs one more drain.
        const goalStarting = yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) => event.type === "run.updated" && event.payload.status === "starting",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped({ startImmediately: true }),
        );
        yield* send("first", "first", "auto");
        yield* send("goal", "/goal all tests pass", "auto");
        yield* worker.drain();
        yield* Fiber.join(goalStarting);
        yield* worker.drain();

        const projection = yield* orchestrator.getThreadProjection(threadId);
        const statusOf = (text: string) =>
          projection.runs.find(
            (run) =>
              projection.messages.find((message) => message.id === run.userMessageId)?.text ===
              text,
          )?.status;
        assert.equal(statusOf("first"), "interrupted");
        assert.deepEqual(
          started.map((turn) => turn.message.text),
          ["/goal all tests pass"],
        );
      }).pipe(Effect.provide(layer));
    }),
  ),
);

it.effect("stops the running turn when a goal clear ends the goal driving it", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { events, layer, startFirstTurn, send, read, steered } =
        yield* goalHarness("goal-clear-active");
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        const providerThread = (yield* orchestrator.getThreadProjection(threadId))
          .providerThreads[0]!;
        const goalSeen = yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) =>
              event.type === "provider-thread.updated" && event.payload.goal?.status === "active",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped({ startImmediately: true }),
        );
        yield* Queue.offer(events, {
          type: "provider_thread.updated",
          driver,
          providerThread: {
            ...providerThread,
            goal: { objective: "all tests pass", status: "active" },
          },
        });
        yield* Fiber.join(goalSeen);
        yield* send("clear", "/goal clear", "auto");
        yield* worker.drain();

        assert.deepEqual(steered, []);
        assert.deepEqual(yield* read(threadId), {
          queue: [],
          started: ["first", "/goal clear"],
          stopRequested: true,
        });
      }).pipe(Effect.provide(layer));
    }),
  ),
);

it.effect("a side chat that opens with /goal gets its context on the next question", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { events, layer, started, startFirstTurn } = yield* goalHarness("side-chat-goal");
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const parentId = yield* startFirstTurn;
        const sideChatId = ThreadId.make("thread:side-chat-goal-child");
        yield* orchestrator.dispatch({
          type: "thread.fork",
          commandId: CommandId.make("side-chat"),
          sourceThreadId: parentId,
          targetThreadId: sideChatId,
          sourcePoint: { type: "latest_stable" },
          sideChat: true,
          createdBy: "user",
          creationSource: "web",
        });
        const ask = (id: string, text: string) =>
          orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(id),
            threadId: sideChatId,
            messageId: MessageId.make(`message:${id}`),
            text,
            attachments: [],
            modelSelection: runSelection,
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
        const sideChatEvent = (matches: (event: OrchestrationV2DomainEvent) => boolean) =>
          orchestrator.streamDomainEvents.pipe(
            Stream.filter((event) => event.threadId === sideChatId && matches(event)),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped({ startImmediately: true }),
          );
        const running = yield* sideChatEvent(
          (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
        );
        const settled = yield* sideChatEvent(
          (event) => event.type === "run.updated" && event.payload.status === "waiting",
        );
        yield* ask("goal", "/goal clear");
        yield* worker.drain();
        yield* Fiber.join(running);
        const turn = (yield* orchestrator.getThreadProjection(sideChatId)).providerTurns[0]!;
        yield* Queue.offer(events, {
          type: "provider_turn.updated",
          driver,
          providerTurn: { ...turn, status: "completed", completedAt: yield* DateTime.now },
        });
        yield* Queue.offer(events, {
          type: "turn.terminal",
          driver,
          providerThreadId: turn.providerThreadId,
          providerTurnId: turn.id,
          runOrdinal: started[1]!.runOrdinal,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* Fiber.join(settled);
        yield* worker.drain();
        yield* ask("question", "What is the main thread doing?");
        yield* worker.drain();

        const [first, goal, question] = started.map((turn) => turn.message.text);
        assert.deepEqual([first, goal], ["first", "/goal clear"]);
        assert.include(question, "You are in a side chat beside the main thread.");
        assert.include(question, "- User: first");
        assert.isTrue(question?.endsWith("What is the main thread doing?"));
      }).pipe(Effect.provide(layer));
    }),
  ),
);
