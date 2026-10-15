import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  EventId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  ThreadId,
  TurnItemId,
  type ModelSelection,
  type OrchestrationV2DomainEvent,
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
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EventSink from "./EventSink.ts";
import * as Orchestrator from "./Orchestrator.ts";
import {
  ProviderAdapterInterruptError,
  type ProviderAdapterV2Event,
  type ProviderAdapterV2SessionRuntime,
} from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderEventIngestor from "./ProviderEventIngestor.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";

const driver = ProviderDriverKind.make("codex");
const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "test-model" };

it.effect.each(
  [false, true]
    .flatMap((mailbox) =>
      (
        [
          "before delivery",
          "during delivery",
          "before dispatch",
          "after delivery",
          "without native steering",
          "with interrupting native steering",
          "settled only",
        ] as const
      ).map((timing) => ({
        mailbox,
        timing,
        label: mailbox ? "mailbox notification" : "steering",
      })),
    )
    .filter(
      ({ mailbox, timing }) =>
        mailbox ||
        (timing !== "without native steering" &&
          timing !== "with interrupting native steering" &&
          timing !== "settled only"),
    ),
)("delivers $label when completion wins $timing", ({ mailbox, timing }) =>
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
                  if (timing === "after delivery") return;
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
                    delivery: { generation: 1, messageId, taskIds: [taskId] },
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
          dispatchMode: mailbox
            ? { type: "queue_after_active" }
            : { type: "steer_active", targetRunId: first.runId },
          createdBy: mailbox ? "agent" : "user",
          creationSource: mailbox ? "server" : "web",
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
        assert.equal(final.messages.filter((message) => message.id === messageId).length, 1);
        assert.equal(
          final.messages.find((message) => message.id === messageId)?.runId,
          started[1]?.runId,
        );
        assert.equal(
          final.turnItems.filter((item) =>
            mailbox
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

// Providers without interrupt-and-restart keep a changed selection for the next turn.
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
  } = {},
) {
  const cwd = yield* checkpointWorkspace(name);
  const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
  const started: ProviderAdapter.ProviderAdapterV2TurnInput[] = [];
  const steered: string[] = [];
  const capabilities = {
    ...CodexProviderCapabilitiesV2,
    turns: {
      ...CodexProviderCapabilitiesV2.turns,
      supportsActiveSteering: true,
      supportsSteeringByInterruptRestart: supportsRestart,
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
            Effect.sync(() => {
              steered.push(turn.message.text);
            }),
          interruptTurn: interruptTurn ?? (() => Effect.void),
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
  // Creates the thread and starts its first turn on `runSelection`.
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
  return { events, started, steered, layer, startFirstTurn };
});

it.effect.each([
  { timing: "during retry", transition: "apply_on_next_turn" },
  { timing: "after final failure", transition: "apply_on_next_turn" },
  { timing: "during retry", transition: "restart_session" },
  { timing: "after final failure", transition: "restart_session" },
  { timing: "during retry", transition: "create_with_handoff" },
  { timing: "after final failure", transition: "create_with_handoff" },
] as const)("Stop reaches the old turn held under a replacement: %j", ({ timing, transition }) =>
  Effect.scoped(
    Effect.gen(function* () {
      let rejectCutIn = true;
      const interrupts: Array<Parameters<ProviderAdapterV2SessionRuntime["interruptTurn"]>[0]> = [];
      const { events, started, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
        `held-replacement-${timing.replaceAll(" ", "-")}-${transition}`,
        true,
        (input): Effect.Effect<void, ProviderAdapterInterruptError> =>
          Effect.gen(function* () {
            interrupts.push(input);
            if (rejectCutIn) {
              return yield* new ProviderAdapterInterruptError({
                driver,
                providerThreadId: input.providerThread.id,
                providerTurnId: input.providerTurnId,
                cause: "native cancellation was not acknowledged",
              });
            }
            const first = started[0]!;
            const now = yield* DateTime.now;
            yield* Queue.offer(events, {
              type: "provider_turn.updated",
              driver,
              providerTurn: {
                id: input.providerTurnId,
                providerThreadId: input.providerThread.id,
                nodeId: first.rootNodeId,
                runAttemptId: first.attemptId,
                nativeTurnRef: { driver, nativeId: "held-native-turn", strength: "strong" },
                ordinal: first.providerTurnOrdinal,
                status: "interrupted",
                startedAt: now,
                completedAt: now,
              },
            });
            yield* Queue.offer(events, {
              type: "turn.terminal",
              driver,
              providerThreadId: input.providerThread.id,
              providerTurnId: input.providerTurnId,
              runOrdinal: first.runOrdinal,
              status: "interrupted",
              failure: null,
              threadDisposition: "reusable",
            });
          }),
        { planSelectionTransition: () => Effect.succeed({ type: transition }) },
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        const threadId = yield* startFirstTurn;
        const first = started[0]!;
        const oldTurn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
        const restartCommand = CommandId.make("hold-urgent-replacement");
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: restartCommand,
          threadId,
          messageId: MessageId.make("message:held-urgent"),
          text: "Stop the unwanted action and change course",
          attachments: [],
          ...(transition === "apply_on_next_turn" ? {} : { modelSelection: composerSelection }),
          dispatchMode: { type: "restart_active", targetRunId: first.runId },
          createdBy: "user",
          creationSource: "web",
        });
        yield* worker.drain();
        assert.lengthOf(interrupts, 1);
        assert.isTrue(interrupts[0]?.replacementFollows);
        assert.lengthOf(started, 1, "a rejected cancellation cannot deliver the replacement");
        assert.equal(
          (yield* orchestrator.getThreadProjection(threadId)).runs[0]?.status,
          "starting",
        );
        if (timing === "after final failure") {
          for (const delay of [100, 200, 400, 800]) {
            yield* TestClock.adjust(`${delay} millis`);
            yield* worker.drain();
          }
          assert.lengthOf(interrupts, 5);
          const failed = yield* orchestrator.getThreadProjection(threadId);
          assert.equal(
            failed.runs[0]?.status,
            "failed",
            "exhausted cancellation must not spin starting",
          );
          const error = failed.turnItems.find((item) => item.type === "error");
          assert.equal(error?.title, "Message not delivered");
          assert.include(
            error?.type === "error" ? error.failure.message : "",
            "could not stop the running work",
          );
          assert.equal(
            failed.providerTurns.find((turn) => turn.id === oldTurn.id)?.status,
            "running",
          );
        }
        rejectCutIn = false;
        const stopped = yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) =>
              event.type === "provider-turn.updated" &&
              event.payload.id === oldTurn.id &&
              event.payload.status === "interrupted",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped({ startImmediately: true }),
        );
        const stopCommand = CommandId.make("stop-held-replacement");
        yield* orchestrator.dispatch({
          type: "run.interrupt",
          commandId: stopCommand,
          threadId,
          runId: first.runId,
          holdQueue: true,
        });
        const stopEffects = yield* outbox.listByCommandId(stopCommand);
        assert.isTrue(
          stopEffects.some(
            (effect) =>
              effect.request.type === "provider-turn.interrupt" &&
              effect.request.providerTurnId === oldTurn.id &&
              effect.request.providerSessionId === first.providerThread.providerSessionId,
          ),
          "Stop must reach the captured old turn",
        );
        if (timing === "during retry") {
          assert.equal(
            (yield* orchestrator.getThreadProjection(threadId)).runs[0]?.status,
            "interrupted",
          );
          const restarts = yield* outbox.listByCommandId(restartCommand);
          assert.equal(
            restarts.find((effect) => effect.request.type === "provider-turn.restart")?.status,
            "cancelled",
          );
        }
        yield* worker.drain();
        yield* Fiber.join(stopped);
        assert.lengthOf(started, 1);
        assert.isUndefined(
          interrupts.at(-1)?.replacementFollows,
          "ordinary Stop releases the hold",
        );
      }).pipe(Effect.provide(layer));
    }),
  ).pipe(Effect.provide(TestClock.layer())),
);

it.effect.each([
  { cutIn: false, rootStatus: "completed", publishTerminal: true },
  { cutIn: true, rootStatus: "completed", publishTerminal: true },
  { cutIn: false, rootStatus: "failed", publishTerminal: false },
  { cutIn: true, rootStatus: "failed", publishTerminal: false },
] as const)(
  "persists one replacement root writer while its earlier attempt drains: %j",
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
          assert.equal(replacement.runId, first.runId);
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
                  Layer.succeed(ProviderEventIngestor.ProviderTurnAnalytics, {
                    record: (properties) =>
                      Effect.gen(function* () {
                        if (
                          !cutIn ||
                          !publishTerminal ||
                          terminalHandlerWasGated ||
                          properties.terminalStatus !== "completed"
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

it.effect("steers a changed turn-scoped selection into a provider that cannot restart", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
        "steering-selection-change",
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

        // Choosing the running run's selection again replaces the saved choice.
        yield* steer("steer-reverted", runSelection);
        const reverted = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(steered, ["steer-changed", "steer-reverted"]);
        assert.lengthOf(reverted.attempts, 1);
        assert.deepEqual(reverted.thread.modelSelection, runSelection);

        // The saved choice moves to another instance while the run keeps going.
        // Steering with the run's selection brings the thread's instance back too.
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
        // The turn ends before the worker delivers the steer, so the steer
        // becomes a follow-up turn.
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
