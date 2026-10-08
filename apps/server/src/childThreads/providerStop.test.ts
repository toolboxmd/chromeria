import { assert, it } from "@effect/vitest";
import {
  EventId,
  PositiveInt,
  TrimmedNonEmptyString,
  CommandId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import { EventSinkV2 } from "../orchestration-v2/EventSink.ts";
import { EffectOutboxV2 } from "../orchestration-v2/EffectOutbox.ts";
import * as EffectWorker from "../orchestration-v2/EffectWorker.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import type {
  ProviderAdapterV2Shape,
  ProviderAdapterV2SessionRuntime,
} from "../orchestration-v2/ProviderAdapter.ts";
import * as Registry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Sessions from "../orchestration-v2/ProviderSessionManager.ts";
import { ThreadManagementService } from "../orchestration-v2/ThreadManagementService.ts";
import { isThreadRetired } from "./retirement.ts";
import * as Harness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";

it.effect.each([
  ["openSession", "self", "none"],
  ["startTurn", "self", "none"],
  ["openSession", "ancestor", "none"],
  ["startTurn", "ancestor", "none"],
  ["startTurn", "self", "newer-owner"],
  ["startTurn", "self", "background"],
  ["startTurn", "resumed-grandchild", "none"],
] as const)(
  "normal Stop with adapter %s blocked, target %s, protection %s",
  ([blocked, stopTarget, protection]) =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const released = yield* Deferred.make<void>();
      const cancelled = yield* Deferred.make<void>();
      const openSessions = yield* Ref.make(0);
      const inFlightStarts = yield* Ref.make(0);
      const acceptedTurns = yield* Ref.make(0);
      const instanceId = ProviderInstanceId.make("codex");
      const driver = ProviderDriverKind.make("codex");
      const adapter: ProviderAdapterV2Shape = {
        instanceId,
        driver,
        getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
        planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
        openSession: (input) =>
          Effect.gen(function* () {
            yield* Effect.acquireRelease(
              Ref.update(openSessions, (count) => count + 1),
              () => Ref.update(openSessions, (count) => count - 1),
            );
            if (blocked === "openSession") {
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(released).pipe(
                Effect.onInterrupt(() => Deferred.succeed(cancelled, undefined)),
              );
            }
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
                cwd: input.runtimePolicy.cwd ?? process.cwd(),
                model: input.modelSelection.model,
                capabilities: CodexProviderCapabilitiesV2,
                createdAt: now,
                updatedAt: now,
                lastError: null,
              },
              events: Stream.never,
              ensureThread: (request) =>
                request.existingProviderThread === undefined
                  ? Effect.die("Missing allocated provider thread")
                  : Effect.succeed({
                      ...request.existingProviderThread,
                      status: "idle",
                      nativeThreadRef: {
                        driver,
                        nativeId: "blocked-start-native",
                        strength: "strong",
                      },
                    }),
              resumeThread: ({ providerThread }) => Effect.succeed(providerThread),
              startTurn: () =>
                Effect.gen(function* () {
                  yield* Ref.update(inFlightStarts, (count) => count + 1);
                  yield* Deferred.succeed(entered, undefined);
                  yield* Deferred.await(released);
                  yield* Ref.update(acceptedTurns, (count) => count + 1);
                }).pipe(
                  Effect.onInterrupt(() => Deferred.succeed(cancelled, undefined)),
                  Effect.ensuring(Ref.update(inFlightStarts, (count) => count - 1)),
                ),
              steerTurn: () => Effect.die("No steer"),
              interruptTurn: () => Effect.die("No accepted provider turn"),
              respondToRuntimeRequest: () => Effect.die("No request"),
              readThreadSnapshot: () => Effect.die("No snapshot"),
              rollbackThread: () => Effect.die("No rollback"),
              forkThread: () => Effect.die("No fork"),
            } satisfies ProviderAdapterV2SessionRuntime;
          }),
      };
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const sessions = yield* Sessions.ProviderSessionManagerV2;
        const threadId = ThreadId.make(`thread:production-stop:${blocked}`);
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make(`create:${blocked}`),
          threadId,
          projectId: ProjectId.make("project:production-stop"),
          title: "Stop proof",
          modelSelection: { instanceId, model: "gpt-5.1-codex" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          createdBy: "user",
          creationSource: "web",
        });
        const sink = yield* EventSinkV2;
        const parentId = ThreadId.make(`parent:${blocked}`);
        const revivedGrandchild = stopTarget === "resumed-grandchild";
        if (stopTarget === "ancestor" || revivedGrandchild) {
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make(`create-parent:${blocked}`),
            threadId: parentId,
            projectId: ProjectId.make("project:production-stop"),
            title: "Native parent",
            modelSelection: { instanceId, model: "gpt-5.1-codex" },
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            createdBy: "user",
            creationSource: "web",
          });
          let directParentId = parentId;
          if (revivedGrandchild) {
            directParentId = ThreadId.make("unresumed-native-intermediate");
            yield* orchestrator.dispatch({
              type: "thread.create",
              commandId: CommandId.make("create-native-intermediate"),
              threadId: directParentId,
              projectId: ProjectId.make("project:production-stop"),
              title: "Intermediate",
              modelSelection: { instanceId, model: "gpt-5.1-codex" },
              runtimeMode: "full-access",
              interactionMode: "default",
              branch: null,
              worktreePath: null,
              createdBy: "user",
              creationSource: "web",
            });
            const middle = (yield* orchestrator.getThreadProjection(directParentId)).thread;
            yield* sink.write({
              events: [
                {
                  id: EventId.make("native-intermediate-lineage"),
                  type: "thread.metadata-updated",
                  threadId: directParentId,
                  providerInstanceId: instanceId,
                  occurredAt: yield* DateTime.now,
                  payload: {
                    ...middle,
                    lineage: {
                      parentThreadId: parentId,
                      rootThreadId: parentId,
                      relationshipToParent: "subagent",
                    },
                  },
                },
              ],
            });
          }
          const child = yield* orchestrator.getThreadProjection(threadId);
          yield* sink.write({
            events: [
              {
                id: EventId.make(`native-lineage:${blocked}`),
                type: "thread.metadata-updated",
                threadId,
                providerInstanceId: instanceId,
                occurredAt: yield* DateTime.now,
                payload: {
                  ...child.thread,
                  lineage: {
                    parentThreadId: directParentId,
                    rootThreadId: parentId,
                    relationshipToParent: "subagent",
                  },
                },
              },
            ],
          });
          assert.lengthOf((yield* orchestrator.getThreadProjection(parentId)).subagents, 0);
        }
        const outbox = yield* EffectOutboxV2;
        let delayedEffectId: string | undefined;
        const rootStopId = CommandId.make("stop-before-grandchild-revival");
        if (revivedGrandchild) {
          yield* orchestrator.dispatch({
            type: "thread.stop",
            commandId: rootStopId,
            threadId: parentId,
          });
          // Hold the real durable root effect while another worker starts the explicit turn.
          const delayed = yield* outbox.claimNext({
            workerId: "delayed-root-stop",
            leaseDurationMs: 60_000,
          });
          assert.isTrue(Option.isSome(delayed));
          if (Option.isNone(delayed)) return;
          assert.equal(delayed.value.request.type, "delegated-tasks.stop");
          delayedEffectId = delayed.value.id;
        }
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(`start:${blocked}`),
          threadId,
          messageId: MessageId.make(`start:${blocked}`),
          text: "Work",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        const execution = yield* worker.runOnce.pipe(Effect.forkChild);
        yield* Deferred.await(entered);
        const before = yield* orchestrator.getThreadProjection(threadId);
        assert.lengthOf(before.runs, 1);
        assert.lengthOf(before.attempts, 1);
        assert.include(["starting", "running"], before.runs[0]!.status);
        if (revivedGrandchild) {
          const threads = yield* ThreadManagementService;
          yield* threads.stopDelegatedTasks({ threadId: parentId, commandId: rootStopId });
          assert.isFalse(yield* Deferred.isDone(cancelled));
          assert.isFalse(yield* isThreadRetired(threadId));
          assert.equal(
            (yield* orchestrator.getThreadProjection(threadId)).runs[0]!.status,
            "running",
          );
          const launch = (yield* outbox.listByCommandId(CommandId.make(`start:${blocked}`))).find(
            (effect) => effect.request.type === "provider-turn.start",
          );
          assert.equal(launch?.status, "running");
          const intermediate = yield* orchestrator.getThreadProjection(
            ThreadId.make("unresumed-native-intermediate"),
          );
          assert.equal(intermediate.thread.forkRetirement?.token, rootStopId);
          assert.lengthOf(intermediate.subagents, 0);
          assert.isTrue(yield* isThreadRetired(intermediate.thread.id));
          assert.lengthOf(
            yield* outbox.listByCommandId(
              CommandId.make(`${rootStopId}:stop:${intermediate.thread.id}`),
            ),
            0,
          );
          yield* Deferred.succeed(released, undefined);
          yield* Fiber.join(execution);
          assert.equal(yield* Ref.get(acceptedTurns), 1);
          assert.equal(
            (yield* orchestrator.getThreadProjection(threadId)).runs[0]!.status,
            "running",
          );
          assert.isFalse(yield* isThreadRetired(threadId));
          assert.isTrue(
            yield* outbox.succeed({ effectId: delayedEffectId!, workerId: "delayed-root-stop" }),
          );
          return;
        }
        if (protection !== "none") {
          const providerThread = before.providerThreads[0]!;
          yield* sink.write({
            events: [
              {
                id: EventId.make(`protected-provider:${protection}`),
                type: "provider-thread.updated",
                threadId,
                providerInstanceId: instanceId,
                occurredAt: yield* DateTime.now,
                payload: {
                  ...providerThread,
                  ...(protection === "newer-owner"
                    ? { lastRunOrdinal: PositiveInt.make(before.runs[0]!.ordinal + 1) }
                    : {
                        pendingBackgroundTasks: [
                          {
                            taskId: TrimmedNonEmptyString.make("still-running"),
                            kind: "command" as const,
                          },
                        ],
                      }),
                },
              },
            ],
          });
        }
        const stopped = yield* orchestrator
          .dispatch({
            type: "thread.stop",
            commandId: CommandId.make(`stop:${blocked}`),
            threadId: stopTarget === "ancestor" ? parentId : threadId,
          })
          .pipe(Effect.forkChild);
        yield* Deferred.await(cancelled);
        yield* Fiber.join(stopped);
        yield* Fiber.join(execution);
        if (stopTarget === "ancestor") yield* worker.drain();
        const after = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(after.runs[0]!.status, "interrupted");
        assert.equal(after.attempts[0]!.status, "interrupted");
        assert.equal(
          after.nodes.find((node) => node.id === after.runs[0]!.rootNodeId)?.status,
          "interrupted",
        );
        assert.isDefined(after.thread.forkRetirement);
        assert.equal(yield* Ref.get(inFlightStarts), 0);
        assert.equal(yield* Ref.get(acceptedTurns), 0);
        // Releasing the adapter's gate after Stop cannot resurrect a cancelled launch.
        yield* Deferred.succeed(released, undefined);
        assert.equal(yield* Ref.get(acceptedTurns), 0);
        assert.lengthOf(after.providerTurns, 0);
        if (blocked === "openSession") {
          assert.equal(yield* Ref.get(openSessions), 0);
          const sessionId = after.providerThreads[0]?.providerSessionId;
          if (sessionId !== null && sessionId !== undefined)
            assert.isTrue(Option.isNone(yield* sessions.get(sessionId)));
        } else {
          assert.equal(
            after.providerThreads.some((thread) => thread.status === "active"),
            protection !== "none",
          );
          assert.equal(yield* Ref.get(openSessions), 1);
        }
      }).pipe(
        Effect.provide(
          Harness.layerWithRegistry(
            { name: `production-stop-${blocked}` },
            Registry.layerFromAdapters([adapter]),
            { runEffectWorker: false },
          ),
        ),
      );
      assert.equal(yield* Ref.get(openSessions), 0);
    }),
);

it.effect(
  "concurrent ancestor Stop and automatic child admission leave no provider launch after the worker drains",
  () =>
    Effect.gen(function* () {
      const providerLaunches = yield* Ref.make(0);
      const instanceId = ProviderInstanceId.make("codex");
      const adapter: ProviderAdapterV2Shape = {
        instanceId,
        driver: ProviderDriverKind.make("codex"),
        getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
        planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
        openSession: () =>
          Ref.update(providerLaunches, (count) => count + 1).pipe(
            Effect.andThen(Effect.die("Retired child must never reach the adapter")),
          ),
      };
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const sink = yield* EventSinkV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const parentId = ThreadId.make("race-parent");
        const childId = ThreadId.make("race-child");
        for (const threadId of [parentId, childId]) {
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make(`create:${threadId}`),
            threadId,
            projectId: ProjectId.make("project:admission-race"),
            title: threadId,
            modelSelection: { instanceId, model: "gpt-5.1-codex" },
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            createdBy: "user",
            creationSource: "web",
          });
        }
        const child = yield* orchestrator.getThreadProjection(childId);
        yield* sink.write({
          events: [
            {
              id: EventId.make("race-lineage"),
              type: "thread.metadata-updated",
              threadId: childId,
              providerInstanceId: instanceId,
              occurredAt: yield* DateTime.now,
              payload: {
                ...child.thread,
                lineage: {
                  parentThreadId: parentId,
                  rootThreadId: parentId,
                  relationshipToParent: "subagent",
                },
              },
            },
          ],
        });
        yield* Effect.all(
          [
            orchestrator.dispatch({
              type: "thread.stop",
              commandId: CommandId.make("race-stop"),
              threadId: parentId,
            }),
            orchestrator
              .dispatch({
                type: "message.dispatch",
                commandId: CommandId.make("race-start"),
                threadId: childId,
                messageId: MessageId.make("race-start"),
                text: "Automatic continuation",
                attachments: [],
                dispatchMode: { type: "start_immediately" },
                createdBy: "user",
                creationSource: "server",
              })
              .pipe(Effect.result),
          ],
          { concurrency: "unbounded" },
        );
        yield* worker.drain();
        const after = yield* orchestrator.getThreadProjection(childId);
        assert.isDefined((yield* orchestrator.getThreadProjection(parentId)).thread.forkRetirement);
        assert.isDefined(after.thread.forkRetirement);
        assert.isFalse(
          after.runs.some(
            (run) =>
              run.status === "preparing" || run.status === "starting" || run.status === "running",
          ),
        );
        assert.equal(yield* Ref.get(providerLaunches), 0);
        assert.deepEqual(after.thread.forkResumedRetirements ?? [], []);
      }).pipe(
        Effect.provide(
          Harness.layerWithRegistry(
            { name: "admission-race" },
            Registry.layerFromAdapters([adapter]),
            { runEffectWorker: false },
          ),
        ),
      );
    }),
);
