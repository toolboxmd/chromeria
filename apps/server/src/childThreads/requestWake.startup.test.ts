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
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as EffectOutbox from "../orchestration-v2/EffectOutbox.ts";
import { EventSinkV2 } from "../orchestration-v2/EventSink.ts";
import { OrchestratorV2 } from "../orchestration-v2/Orchestrator.ts";
import * as ProviderRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderRuntimeRecovery from "../orchestration-v2/ProviderRuntimeRecoveryService.ts";
import * as Harness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
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

it.effect.each([false, true])(
  "ordered startup settles cancelled questions before wakes, preserving pending continuation=$0",
  (continueAfterRestart) =>
    Effect.gen(function* () {
      const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "question-startup-"));
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => NodeFS.rmSync(directory, { recursive: true, force: true })),
      );
      const filename = NodePath.join(directory, "chromeria-v2.sqlite");
      const runtime = () =>
        Harness.layerWithRegistry(
          { name: "question-startup" },
          ProviderRegistry.layerFromAdapters([adapter]),
          {
            databaseLayer: SqlitePersistence.layerFromPath(filename).pipe(
              Layer.provide(NodeServices.layer),
            ),
            runEffectWorker: false,
          },
        );
      const parentId = ThreadId.make("question-startup-parent");
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
              payload: { ...childRun, status: "running", startedAt: now },
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
                status: "running",
                startedAt: now,
                completedAt: null,
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
          ],
        });
      }).pipe(Effect.provide(forkV1SnapshotLayer(filename)));

      yield* Effect.gen(function* () {
        const engine = yield* OrchestratorV2;
        const outbox = yield* EffectOutbox.EffectOutboxV2;
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
        yield* runOrderedV2StartupPhases({
          importLegacyShells: Effect.void,
          recover,
          recoverDelegatedTasks: engine.recoverDelegatedTasks,
          startEffectWorker: Effect.void,
          autoBootstrap: Effect.void,
        });
        yield* engine.recoverDelegatedTasks;
        const recovered = yield* engine.getThreadProjection(child.thread.id);
        assert.equal(recovered.runs.find((run) => run.id === childRun.id)?.status, "cancelled");
        const question = recovered.runtimeRequests.find((request) => request.id === requestId)!;
        const continuation = yield* outbox.get(`effect:restart-continuation:${childRun.id}`);
        const parent = yield* engine.getThreadProjection(parentId);
        const questionRun = parent.runs.find((run) => run.userMessageId === questionMessageId);
        const questionEffects = yield* outbox.listByCommandId(questionCommandId);
        if (continueAfterRestart) {
          assert.equal(question.status, "pending");
          assert.isTrue(Option.isSome(continuation));
          if (Option.isSome(continuation)) assert.equal(continuation.value.status, "pending");
          assert.isNull(parent.subagents[0]!.result);
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
          assert.isUndefined(
            questionRun,
            "a question invalidated by finalization must never start",
          );
          assert.isFalse(parent.messages.some((message) => message.id === questionMessageId));
          assert.lengthOf(questionEffects, 0);
        }
      }).pipe(Effect.provide(runtime()));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
