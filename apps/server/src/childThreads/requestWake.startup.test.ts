// @effect-diagnostics nodeBuiltinImport:off -- File-backed runtimes model process loss without a server.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderTurnId,
  RuntimeRequestId,
  ThreadId,
} from "@t3tools/contracts";
import type { ProviderAdapterV2 } from "@t3tools/provider-core/server/ProviderAdapter";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";

import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as EffectOutbox from "../orchestration-v2/EffectOutbox.ts";
import { EventSinkV2 } from "../orchestration-v2/EventSink.ts";
import { OrchestratorV2 } from "../orchestration-v2/Orchestrator.ts";
import { ProjectionStoreV2 } from "../orchestration-v2/ProjectionStore.ts";
import * as ProviderRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderRuntimeRecovery from "../orchestration-v2/ProviderRuntimeRecoveryService.ts";
import * as Harness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as ThreadCommandExecutor from "../orchestration-v2/ThreadCommandExecutor.ts";
import { forkV1SnapshotLayer } from "../persistence/forkV1Snapshot.testFixtures.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { runOrderedV2StartupPhases } from "../serverRuntimeStartup.ts";
import * as ServerSettings from "../serverSettings.ts";

const instanceId = ProviderInstanceId.make("codex");
const driver = ProviderDriverKind.make("codex");
const modelSelection = { instanceId, model: "gpt-5.1-codex" };
const adapter = {
  instanceId,
  driver,
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("No provider process in persisted startup proof"),
} satisfies ProviderAdapterV2["Service"];

it.effect.each([
  { name: "ordered startup", type: "startup", continueAfterRestart: false, liveApproval: false },
  {
    name: "pending continuation",
    type: "startup",
    continueAfterRestart: true,
    liveApproval: false,
  },
  {
    name: "request listener during recovery",
    type: "startup",
    continueAfterRestart: false,
    liveApproval: true,
  },
  {
    name: "result transfer before cancelled question dismissal",
    type: "transferred",
    ending: "cancelled",
  },
  {
    name: "result transfer before interrupted question dismissal",
    type: "transferred",
    ending: "interrupted",
  },
  {
    name: "result transfer preserves completed questions",
    type: "transferred",
    ending: "completed",
  },
  { name: "result transfer preserves failed questions", type: "transferred", ending: "failed" },
] as const)("persisted delegated question lifetime: $name", (scenario) =>
  Effect.gen(function* () {
    const continueAfterRestart = scenario.type === "startup" && scenario.continueAfterRestart;
    const liveApproval = scenario.type === "startup" && scenario.liveApproval;
    const initialStatus = scenario.type === "transferred" ? scenario.ending : "running";
    const preservesQuestion =
      continueAfterRestart || initialStatus === "completed" || initialStatus === "failed";
    const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "question-startup-"));
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => NodeFS.rmSync(directory, { recursive: true, force: true })),
    );
    const filename = NodePath.join(directory, "chromeria-v2.sqlite");
    const runtime = () =>
      Layer.merge(
        Harness.layerWithRegistry(
          { name: "question-startup" },
          ProviderRegistry.layerFromAdapters([adapter]),
          {
            databaseLayer: SqlitePersistence.layerFromPath(filename).pipe(
              Layer.provide(NodeServices.layer),
            ),
            runEffectWorker: false,
          },
        ),
        ThreadCommandExecutor.layer,
      );
    const parentId = ThreadId.make("question-startup-parent");
    const markerChildId = ThreadId.make("question-startup-listener-marker-child");
    const approvalId = RuntimeRequestId.make("question-startup-live-approval");
    const requestId = RuntimeRequestId.make("question-startup-request");
    const questionCommandId = CommandId.make(
      `server:delegated-task-question:${parentId}:${requestId}`,
    );
    const questionMessageId = MessageId.make(
      `message:delegated-task-question:${parentId}:${requestId}`,
    );
    const now = yield* DateTime.now;
    const { parentRun, child, childRun } = yield* Effect.gen(function* () {
      const engine = yield* OrchestratorV2;
      if (liveApproval) {
        yield* engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make("question-startup-marker-create"),
          threadId: markerChildId,
          projectId: ProjectId.make("question-startup-project"),
          title: "Listener marker",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          createdBy: "user",
          creationSource: "web",
        });
      }
      yield* engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("question-startup-create"),
        threadId: parentId,
        projectId: ProjectId.make("question-startup-project"),
        title: "Parent",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      });
      yield* engine.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("question-startup-parent-run"),
        threadId: parentId,
        messageId: MessageId.make("question-startup-parent-message"),
        text: "Delegate the research",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        createdBy: "user",
        creationSource: "web",
      });
      const parentRun = (yield* engine.getThreadProjection(parentId)).runs[0]!;
      yield* engine.dispatch({
        type: "delegated_task.request",
        commandId: CommandId.make("question-startup-delegate"),
        parentThreadId: parentId,
        parentRunId: parentRun.id,
        parentNodeId: parentRun.rootNodeId!,
        task: "Research the sources",
        completionWake: "settled_only",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        createdBy: "agent",
        creationSource: "mcp",
      });
      const task = (yield* engine.getThreadProjection(parentId)).subagents[0]!;
      const child = yield* engine.getThreadProjection(task.childThreadId!);
      return { parentRun, child, childRun: child.runs[0]! };
    }).pipe(Effect.provide(runtime()));

    // Persist an idle parent and a real resumable running child with a durable
    // question. No Orchestrator listener can deliver it before the next boot.
    const sessionId = ProviderSessionId.make("question-startup-session");
    const providerThread = child.providerThreads.find(
      (thread) => thread.id === childRun.providerThreadId,
    )!;
    yield* Effect.gen(function* () {
      const sink = yield* EventSinkV2;
      yield* sink.write({
        events: [
          {
            id: EventId.make("question-startup-parent-completed"),
            type: "run.updated",
            threadId: parentId,
            occurredAt: now,
            payload: { ...parentRun, status: "completed", startedAt: now, completedAt: now },
          },
          {
            id: EventId.make("question-startup-child-running"),
            type: "run.updated",
            threadId: child.thread.id,
            occurredAt: now,
            payload: {
              ...childRun,
              status: initialStatus,
              startedAt: now,
              completedAt: initialStatus === "running" ? null : now,
            },
          },
          {
            id: EventId.make("question-startup-session-attached"),
            type: "provider-session.attached",
            threadId: child.thread.id,
            occurredAt: now,
            payload: {
              id: sessionId,
              driver,
              providerInstanceId: instanceId,
              status: "ready",
              cwd: directory,
              model: modelSelection.model,
              capabilities: CodexProviderCapabilitiesV2,
              createdAt: now,
              updatedAt: now,
              lastError: null,
            },
          },
          {
            id: EventId.make("question-startup-provider-thread"),
            type: "provider-thread.updated",
            threadId: child.thread.id,
            occurredAt: now,
            payload: {
              ...providerThread,
              providerSessionId: sessionId,
              nativeThreadRef: {
                driver,
                nativeId: "question-startup-native",
                strength: "strong",
              },
              status: "active",
            },
          },
          {
            id: EventId.make("question-startup-turn"),
            type: "provider-turn.updated",
            threadId: child.thread.id,
            runId: childRun.id,
            occurredAt: now,
            payload: {
              id: ProviderTurnId.make("question-startup-turn"),
              providerThreadId: providerThread.id,
              nodeId: childRun.rootNodeId!,
              runAttemptId: childRun.activeAttemptId!,
              nativeTurnRef: {
                driver,
                nativeId: "question-startup-native-turn",
                strength: "strong",
              },
              ordinal: 1,
              status: initialStatus === "interrupted" ? "cancelled" : initialStatus,
              startedAt: now,
              completedAt: initialStatus === "running" ? null : now,
            },
          },
          {
            id: EventId.make("question-startup-pending"),
            type: "runtime-request.updated",
            threadId: child.thread.id,
            runId: childRun.id,
            occurredAt: now,
            payload: {
              id: requestId,
              nodeId: childRun.rootNodeId!,
              providerTurnId: null,
              nativeRequestRef: null,
              kind: "user_input",
              status: "pending",
              responseCapability: { type: "message" },
              createdAt: now,
              resolvedAt: null,
            },
          },
          ...(liveApproval
            ? [
                {
                  id: EventId.make("question-startup-live-approval"),
                  type: "runtime-request.updated" as const,
                  threadId: child.thread.id,
                  runId: childRun.id,
                  occurredAt: now,
                  payload: {
                    id: approvalId,
                    nodeId: childRun.rootNodeId!,
                    providerTurnId: null,
                    nativeRequestRef: null,
                    kind: "command" as const,
                    status: "pending" as const,
                    responseCapability: { type: "live" as const, providerSessionId: sessionId },
                    createdAt: now,
                    resolvedAt: null,
                  },
                },
              ]
            : []),
        ],
      });
      if (liveApproval) {
        const projections = yield* ProjectionStoreV2;
        const marker = yield* projections.getThread(markerChildId);
        yield* sink.write({
          events: [
            {
              id: EventId.make("question-startup-marker-lineage"),
              type: "thread.metadata-updated",
              threadId: markerChildId,
              occurredAt: now,
              payload: {
                ...marker,
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
    }).pipe(Effect.provide(forkV1SnapshotLayer(filename)));

    if (scenario.type === "transferred") {
      // Stop the real finalizer after its actual result transfer commits, while
      // the captured child lock still prevents its separate dismissal command.
      yield* Effect.gen(function* () {
        const engine = yield* OrchestratorV2;
        const sink = yield* EventSinkV2;
        const executor = yield* ThreadCommandExecutor.ThreadCommandExecutor;
        const locked = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const holder = yield* executor
          .withLock(
            child.thread.id,
            Deferred.succeed(locked, undefined).pipe(Effect.andThen(Deferred.await(release))),
          )
          .pipe(Effect.forkChild);
        yield* Deferred.await(locked);
        const observed = yield* sink.stream({ afterSequence: yield* sink.latestSequence() }).pipe(
          Stream.filter(
            (stored) =>
              stored.event.type === "context-transfer.created" &&
              stored.event.payload.type === "subagent_result" &&
              stored.event.payload.sourceThreadId === child.thread.id,
          ),
          Stream.runHead,
          Effect.forkChild,
        );
        const finalizing = yield* engine
          .recoverDelegatedTask(child.thread.id, childRun.id)
          .pipe(Effect.forkChild);
        assert.isTrue(Option.isSome(yield* Fiber.join(observed)));
        yield* Fiber.interrupt(finalizing);
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(holder);
        assert.equal(
          (yield* engine.getThreadProjection(child.thread.id)).runtimeRequests[0]!.status,
          "pending",
        );
        const parent = yield* engine.getThreadProjection(parentId);
        assert.lengthOf(
          parent.contextTransfers.filter((transfer) => transfer.type === "subagent_result"),
          1,
        );
        assert.notEqual(parent.subagents[0]!.completionDelivery?.state, "disposed");
      }).pipe(Effect.provide(runtime()));
    }

    yield* Effect.gen(function* () {
      const engine = yield* OrchestratorV2;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const sink = yield* EventSinkV2;
      const projections = yield* ProjectionStoreV2;
      if (scenario.type === "transferred") {
        assert.deepEqual(yield* projections.getRecoveryThreadIds("subagent-results"), []);
      }
      assert.equal(
        (yield* engine.getThreadProjection(child.thread.id)).runtimeRequests.find(
          (request) => request.id === requestId,
        )?.status,
        "pending",
      );
      assert.isFalse(
        (yield* engine.getThreadProjection(parentId)).messages.some(
          (message) => message.id === questionMessageId,
        ),
      );
      const recover = ProviderRuntimeRecovery.ProviderRuntimeRecoveryService.use(
        (service) => service.recover,
      ).pipe(
        Effect.provide(
          ProviderRuntimeRecovery.layer.pipe(
            Layer.provide(
              Layer.mergeAll(
                IdAllocator.layer,
                ServerSettings.layerTest({
                  continueThreadsAfterServerUpdate: continueAfterRestart,
                }),
              ),
            ),
          ),
        ),
      );
      const recoverAndObserveListener = Effect.gen(function* () {
        yield* recover;
        if (!liveApproval) return;
        const recovered = yield* engine.getThreadProjection(child.thread.id);
        assert.equal(
          recovered.runtimeRequests.find((request) => request.id === approvalId)?.status,
          "expired",
        );
        // The one global request listener handles events sequentially. A later
        // persisted manual approval notice proves it finished the earlier live
        // request expiry before delegated recovery, without sleeps or polling.
        const markerId = RuntimeRequestId.make("question-startup-listener-marker");
        const markerCommand = CommandId.make(
          `command:child-request:${markerChildId}:${markerId}:${parentId}:0`,
        );
        const observed = yield* sink.stream({ afterSequence: yield* sink.latestSequence() }).pipe(
          Stream.filter(
            (stored) =>
              stored.commandId === markerCommand && stored.event.type === "message.updated",
          ),
          Stream.runHead,
          Effect.forkChild,
        );
        yield* sink.write({
          events: [
            {
              id: EventId.make("question-startup-listener-marker"),
              type: "runtime-request.updated",
              threadId: markerChildId,
              occurredAt: now,
              payload: {
                id: markerId,
                nodeId: NodeId.make("question-startup-listener-marker"),
                providerTurnId: null,
                nativeRequestRef: null,
                kind: "command",
                status: "pending",
                responseCapability: { type: "message" },
                createdAt: now,
                resolvedAt: null,
              },
            },
          ],
        });
        assert.isTrue(Option.isSome(yield* Fiber.join(observed)));
        if (!continueAfterRestart) {
          assert.isFalse(
            (yield* engine.getThreadProjection(parentId)).runs.some(
              (run) => run.userMessageId === questionMessageId,
            ),
            "the active listener must settle question lifetime before admitting a recovery wake",
          );
        }
      });
      yield* runOrderedV2StartupPhases({
        importLegacyShells: Effect.void,
        recover: recoverAndObserveListener,
        recoverDelegatedTasks: engine.recoverDelegatedTasks,
        startEffectWorker: Effect.void,
        autoBootstrap: Effect.void,
      });
      yield* engine.recoverDelegatedTasks;
      const recovered = yield* engine.getThreadProjection(child.thread.id);
      assert.equal(
        recovered.runs.find((run) => run.id === childRun.id)?.status,
        initialStatus === "running" ? "cancelled" : initialStatus,
      );
      const question = recovered.runtimeRequests.find((request) => request.id === requestId)!;
      const continuation = yield* outbox.get(`effect:restart-continuation:${childRun.id}`);
      const parent = yield* engine.getThreadProjection(parentId);
      const questionRun = parent.runs.find((run) => run.userMessageId === questionMessageId);
      const questionEffects = yield* outbox.listByCommandId(questionCommandId);
      if (preservesQuestion) {
        assert.equal(question.status, "pending");
        if (continueAfterRestart) {
          assert.isTrue(Option.isSome(continuation));
          if (Option.isSome(continuation)) assert.equal(continuation.value.status, "pending");
          assert.isNull(parent.subagents[0]!.result);
        }
        assert.isDefined(questionRun);
        assert.lengthOf(
          parent.runs.filter((run) => run.userMessageId === questionMessageId),
          1,
        );
        assert.isTrue(
          questionEffects.some((effect) => effect.request.type === "provider-turn.start"),
        );
        assert.lengthOf(
          parent.messages.filter((message) => message.id === questionMessageId),
          1,
        );
      } else {
        assert.equal(question.status, "resolved");
        assert.equal(question.decision, "cancel");
        assert.isTrue(Option.isNone(continuation));
        assert.isUndefined(questionRun, "a question invalidated by finalization must never start");
        assert.isFalse(parent.messages.some((message) => message.id === questionMessageId));
        assert.lengthOf(questionEffects, 0);
      }
      if (scenario.type === "transferred") {
        assert.lengthOf(
          parent.contextTransfers.filter((transfer) => transfer.type === "subagent_result"),
          1,
        );
        assert.deepEqual(yield* projections.getRecoveryThreadIds("subagent-results"), []);
      }
      if (scenario.name === "ordered startup") {
        // After startup, a real requested follow-up owns a fresh question. The
        // active listener must keep delivering valid work on this same child.
        yield* engine.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("question-startup-followup"),
          threadId: child.thread.id,
          messageId: MessageId.make("question-startup-followup"),
          text: "Continue with a new source",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        const followup = (yield* engine.getThreadProjection(child.thread.id)).runs.toSorted(
          (a, b) => b.ordinal - a.ordinal,
        )[0]!;
        const followupId = RuntimeRequestId.make("question-startup-followup-question");
        const observed = yield* sink.stream({ afterSequence: yield* sink.latestSequence() }).pipe(
          Stream.filter(
            (stored) =>
              stored.commandId ===
                CommandId.make(`server:delegated-task-question:${parentId}:${followupId}`) &&
              stored.event.type === "message.updated",
          ),
          Stream.runHead,
          Effect.forkChild,
        );
        yield* sink.write({
          events: [
            {
              id: EventId.make("question-startup-followup-question"),
              type: "runtime-request.updated",
              threadId: child.thread.id,
              runId: followup.id,
              occurredAt: now,
              payload: {
                id: followupId,
                nodeId: followup.rootNodeId!,
                providerTurnId: null,
                nativeRequestRef: null,
                kind: "user_input",
                status: "pending",
                responseCapability: { type: "message" },
                createdAt: now,
                resolvedAt: null,
              },
            },
          ],
        });
        assert.isTrue(Option.isSome(yield* Fiber.join(observed)));
        assert.equal(
          (yield* engine.getThreadProjection(child.thread.id)).runtimeRequests.find(
            (request) => request.id === followupId,
          )?.status,
          "pending",
        );
        assert.lengthOf(
          (yield* engine.getThreadProjection(parentId)).messages.filter(
            (message) =>
              message.id ===
              MessageId.make(`message:delegated-task-question:${parentId}:${followupId}`),
          ),
          1,
        );
      }
    }).pipe(Effect.provide(runtime()));

    // A second reconstructed process must converge independently of whether
    // result delivery already transferred in the first one.
    yield* Effect.gen(function* () {
      const engine = yield* OrchestratorV2;
      yield* engine.recoverDelegatedTasks;
      yield* engine.recoverDelegatedTasks;
      const childState = yield* engine.getThreadProjection(child.thread.id);
      assert.equal(
        childState.runtimeRequests.find((request) => request.id === requestId)?.status,
        preservesQuestion ? "pending" : "resolved",
      );
      const parent = yield* engine.getThreadProjection(parentId);
      assert.lengthOf(
        parent.messages.filter((message) => message.id === questionMessageId),
        preservesQuestion ? 1 : 0,
      );
      assert.lengthOf(
        parent.runs.filter((run) => run.userMessageId === questionMessageId),
        preservesQuestion ? 1 : 0,
      );
    }).pipe(Effect.provide(runtime()));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
