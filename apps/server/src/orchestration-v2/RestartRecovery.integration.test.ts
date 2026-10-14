import * as NodeSqlite from "node:sqlite";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  ThreadId,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as OrchestratorMcpService from "../mcp/OrchestratorMcpService.ts";
import type { McpThreadInvocationScope } from "../mcp/McpInvocationContext.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as RemoteDelegation from "../peer/RemoteDelegation.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import * as ThreadLaunch from "./ThreadLaunchService.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import { ClaudeProviderCapabilitiesV2 } from "./Adapters/ClaudeAdapterV2.ts";
import { GrokProviderCapabilitiesV2 } from "@t3tools/provider-grok/testing";
import * as EffectWorker from "./EffectWorker.ts";
import * as EventSink from "./EventSink.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Orchestrator from "./Orchestrator.ts";
import {
  ProviderAdapterOpenSessionError,
  type ProviderAdapterV2Event,
  type ProviderAdapterV2,
} from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderRuntimeRecovery from "./ProviderRuntimeRecoveryService.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import { RESTART_CUT_REASONS } from "@t3tools/shared/restartCut";
import { subagentResultForRun } from "@t3tools/provider-core/server/subagentProjection";

const providers = [
  ["codex", CodexProviderCapabilitiesV2],
  ["claudeAgent", ClaudeProviderCapabilitiesV2],
  ["grok", GrokProviderCapabilitiesV2],
] as const;

// Only the external harness is replaced. Commands, turn admission, SQLite,
// recovery, outbox execution, session ownership and event ingestion are real.
function unfinishedAdapter(
  driver: ProviderDriverKind,
  capabilities: OrchestrationV2ProviderCapabilities,
  failOpen = false,
): ProviderAdapterV2["Service"] {
  const instanceId = ProviderInstanceId.make(driver);
  return {
    instanceId,
    driver,
    getCapabilities: () => Effect.succeed(capabilities),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
    openSession: (input) =>
      Effect.gen(function* () {
        if (failOpen)
          return yield* new ProviderAdapterOpenSessionError({
            driver,
            providerSessionId: input.providerSessionId,
            cause: "simulated unavailable harness",
          });
        const events = yield* Queue.unbounded<ProviderAdapterV2Event>();
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
            cwd: input.runtimePolicy.cwd ?? "/unused",
            model: input.modelSelection.model,
            capabilities,
            createdAt: now,
            updatedAt: now,
            lastError: null,
          },
          events: Stream.fromQueue(events),
          ensureThread: (thread) =>
            Effect.gen(function* () {
              const now = yield* DateTime.now;
              return {
                id: ProviderThreadId.make(`provider-thread:${thread.threadId}`),
                driver,
                providerInstanceId: instanceId,
                providerSessionId: input.providerSessionId,
                appThreadId: thread.threadId,
                ownerNodeId: null,
                nativeThreadRef: {
                  driver,
                  nativeId: `native:${thread.threadId}`,
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
              } satisfies OrchestrationV2ProviderThread;
            }),
          resumeThread: ({ providerThread }) => Effect.succeed(providerThread),
          startTurn: (turn) =>
            Effect.gen(function* () {
              const now = yield* DateTime.now;
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
                    nativeId: `native-turn:${turn.attemptId}`,
                    strength: "strong",
                  },
                  ordinal: turn.providerTurnOrdinal,
                  status: "running",
                  startedAt: now,
                  completedAt: null,
                },
              });
            }),
          steerTurn: () => Effect.void,
          interruptTurn: () => Effect.void,
          respondToRuntimeRequest: () => Effect.void,
          readThreadSnapshot: () => Effect.die("unused snapshot"),
          rollbackThread: () => Effect.die("unused rollback"),
          forkThread: () => Effect.die("unused fork"),
        };
      }),
  };
}

const recovery = ProviderRuntimeRecovery.ProviderRuntimeRecoveryService.use((service) =>
  Effect.succeed(service),
).pipe(
  Effect.provide(
    ProviderRuntimeRecovery.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          IdAllocator.layer,
          ServerSettings.layerTest({ continueThreadsAfterServerUpdate: true }),
        ),
      ),
    ),
  ),
);

it.live(
  "rediscovering a cut follow-up tells its parent and MCP while keeping the first result",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const instanceId = ProviderInstanceId.make("codex");
        const modelSelection = { instanceId, model: "parent-restart-proof" };
        const parentId = ThreadId.make("thread:restart-result-parent");
        const registry = ProviderAdapterRegistry.layerSingle(
          unfinishedAdapter(ProviderDriverKind.make("codex"), CodexProviderCapabilitiesV2),
        );
        const runtime = ProviderReplayHarness.layerWithRegistry(
          { name: "restart-result-parent" },
          registry,
          { runEffectWorker: false, runContinuationWorker: true },
        );
        const services = Layer.mergeAll(
          OrchestratorMcpService.layer.pipe(
            Layer.provideMerge(runtime),
            Layer.provide(registry),
            Layer.provide(ServerSettings.layerTest()),
            Layer.provide(NodeCrypto.layer),
            Layer.provide(
              Layer.mergeAll(
                Layer.mock(ProviderRegistry.ProviderRegistry)({ getProviders: Effect.succeed([]) }),
                Layer.mock(RemoteDelegation.RemoteDelegation)({}),
                Layer.mock(ProjectService.ProjectService)({}),
                Layer.mock(SecretRequests.SecretRequests)({}),
                Layer.mock(ScheduledTaskService.ScheduledTaskService)({}),
                Layer.mock(ThreadLaunch.ThreadLaunchService)({}),
                Layer.mock(GitVcsDriver.GitVcsDriver)({}),
              ),
            ),
          ),
          runtime,
        );
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const sink = yield* EventSink.EventSinkV2;
          const mcp = yield* OrchestratorMcpService.OrchestratorMcpService;
          const scope: McpThreadInvocationScope = {
            environmentId: EnvironmentId.make("environment:restart-proof"),
            requestNamespace: "restart-proof",
            thread: {
              threadId: parentId,
              providerSessionId: "session:restart-proof",
              providerInstanceId: instanceId,
            },
            client: undefined,
            capabilities: new Set(["orchestration"]),
            issuedAt: 1,
          };
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("parent:create"),
            threadId: parentId,
            projectId: ProjectId.make("project:restart-result"),
            title: "Restart result",
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
            commandId: CommandId.make("parent:start"),
            threadId: parentId,
            messageId: MessageId.make("parent:message"),
            text: "Delegate the work.",
            attachments: [],
            modelSelection,
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* EffectWorker.OrchestrationEffectWorkerV2.use((worker) => worker.drain());
          const parentRun = (yield* orchestrator.getThreadProjection(parentId)).runs[0];
          if (parentRun?.rootNodeId == null) throw new Error("The parent run has no root node.");
          yield* orchestrator.dispatch({
            type: "delegated_task.request",
            commandId: CommandId.make("parent:delegate"),
            parentThreadId: parentId,
            parentRunId: parentRun.id,
            parentNodeId: parentRun.rootNodeId,
            task: "Finish the work and then accept a follow-up.",
            modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            completionWake: "always",
            createdBy: "agent",
            creationSource: "mcp",
          });
          const task = (yield* orchestrator.getThreadProjection(parentId)).subagents[0];
          if (task?.childThreadId == null) throw new Error("The delegated child was not created.");
          const childId = task.childThreadId;
          const first = (yield* orchestrator.getThreadProjection(childId)).runs[0];
          if (first === undefined) throw new Error("The child run was not created.");
          const now = yield* DateTime.now;
          yield* sink.write({
            events: [
              {
                id: EventId.make("child:first-message"),
                type: "message.updated",
                threadId: childId,
                runId: first.id,
                occurredAt: now,
                payload: {
                  id: MessageId.make("child:first-result"),
                  threadId: childId,
                  runId: first.id,
                  nodeId: first.rootNodeId,
                  role: "assistant",
                  text: "Original result.",
                  attachments: [],
                  createdBy: "agent",
                  creationSource: "provider",
                  streaming: false,
                  createdAt: now,
                  updatedAt: now,
                },
              },
              {
                id: EventId.make("child:first-completed"),
                type: "run.updated",
                threadId: childId,
                runId: first.id,
                occurredAt: now,
                payload: { ...first, status: "completed", startedAt: now, completedAt: now },
              },
            ],
          });
          yield* orchestrator.recoverDelegatedTask(childId, first.id);
          const originalParent = yield* orchestrator.getThreadProjection(parentId);
          const originalTransfer = originalParent.contextTransfers.find(
            (transfer) => transfer.type === "subagent_result",
          );
          if (originalTransfer === undefined) throw new Error("The first result did not transfer.");
          assert.equal(originalParent.subagents[0]?.result, "Original result.");
          assert.equal((yield* mcp.taskStatus(scope, task.id)).summary, "Original result.");
          // Old versions had no source run identity. A future timestamp must not
          // hide a new cut when wall clocks move backwards or tie.
          const { runId: _runId, ...legacyPoint } = originalTransfer.sourcePoint;
          yield* sink.write({
            events: [
              {
                id: EventId.make("parent:legacy-transfer"),
                type: "context-transfer.updated",
                threadId: parentId,
                occurredAt: now,
                payload: {
                  ...originalTransfer,
                  sourcePoint: legacyPoint,
                  createdAt: DateTime.makeUnsafe("2099-01-01T00:00:00Z"),
                },
              },
            ],
          });
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("child:follow-up"),
            threadId: childId,
            senderThreadId: parentId,
            messageId: MessageId.make("child:follow-up-prompt"),
            text: "Do the next part.",
            attachments: [],
            modelSelection,
            dispatchMode: { type: "start_immediately" },
            createdBy: "agent",
            creationSource: "mcp",
          });
          const followUp = (yield* orchestrator.getThreadProjection(childId)).runs.find(
            (run) => run.ordinal === 2,
          );
          if (followUp === undefined) throw new Error("The follow-up did not start.");
          yield* sink.write({
            events: [
              {
                id: EventId.make("child:partial"),
                type: "message.updated",
                threadId: childId,
                runId: followUp.id,
                occurredAt: now,
                payload: {
                  id: MessageId.make("child:partial-output"),
                  threadId: childId,
                  runId: followUp.id,
                  nodeId: followUp.rootNodeId,
                  role: "assistant",
                  text: "Inspected the second file.",
                  attachments: [],
                  createdBy: "agent",
                  creationSource: "provider",
                  streaming: true,
                  createdAt: now,
                  updatedAt: now,
                },
              },
              {
                id: EventId.make("child:running"),
                type: "run.updated",
                threadId: childId,
                runId: followUp.id,
                occurredAt: now,
                payload: { ...followUp, status: "running", startedAt: now },
              },
            ],
          });
          const cutCursor = yield* sink.latestSequence({ threadId: parentId });
          yield* (yield* recovery).recover;
          yield* orchestrator.recoverDelegatedTasks;
          yield* EffectWorker.OrchestrationEffectWorkerV2.use((worker) => worker.drain());
          yield* sink.stream({ threadId: parentId, afterSequence: cutCursor }).pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "message.updated" &&
                stored.event.payload.text.includes("A server restart cut the work"),
            ),
            Stream.take(1),
            Stream.runDrain,
          );
          const status = yield* mcp.taskStatus(scope, task.id);
          assert.equal(status.summary, "Original result.");
          assert.equal(status.latestTerminalRunId, followUp.id);
          assert.equal(status.latestTerminalStatus, "cancelled");
          assert.include(status.latestTerminalSummary ?? "", "server restart");
          assert.include(status.latestTerminalSummary ?? "", RESTART_CUT_REASONS.NOT_RESUMABLE);
          assert.include(status.latestTerminalSummary ?? "", "Inspected the second file.");
          const read = yield* mcp.readThread(scope, { threadId: childId });
          assert.deepEqual(read.recentRuns.find((run) => run.runId === followUp.id)?.restartCut, {
            state: "declined",
            reason: RESTART_CUT_REASONS.NOT_RESUMABLE,
          });
          const after = yield* orchestrator.getThreadProjection(parentId);
          assert.isTrue(
            after.messages.some((message) =>
              message.text.includes("A server restart cut the work"),
            ),
          );
          assert.lengthOf(
            after.contextTransfers.filter(
              (transfer) =>
                transfer.type === "subagent_result" && transfer.sourcePoint.runId === followUp.id,
            ),
            1,
          );
          yield* orchestrator.recoverDelegatedTasks;
          assert.lengthOf(
            (yield* orchestrator.getThreadProjection(parentId)).contextTransfers.filter(
              (transfer) =>
                transfer.type === "subagent_result" && transfer.sourcePoint.runId === followUp.id,
            ),
            1,
          );
        }).pipe(Effect.provide(services));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

const snapshot = (sourcePath: string, targetPath: string) =>
  Effect.acquireUseRelease(
    Effect.sync(() => new NodeSqlite.DatabaseSync(sourcePath, { readOnly: true })),
    (source) => Effect.promise(() => NodeSqlite.backup(source, targetPath)),
    (source) => Effect.sync(() => source.close()),
  );

it.live.each(
  providers
    .flatMap(([driver, capabilities]) =>
      ["shutdown", "crash"].map((loss) => ({ driver, capabilities, loss, failOpen: false })),
    )
    .concat([
      { driver: "codex", capabilities: CodexProviderCapabilitiesV2, loss: "crash", failOpen: true },
    ]),
)(
  "records $driver's outcome after $loss (failed open: $failOpen)",
  ({ driver, capabilities, loss, failOpen }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const name = `restart-${driver}-${loss}`;
        const cwd = yield* checkpointWorkspace(name);
        const stateDir = yield* FileSystem.FileSystem.use((fs) =>
          fs.makeTempDirectoryScoped({ prefix: `${name}-state-` }),
        );
        const originalPath = `${stateDir}/original.sqlite`;
        const restartPath = `${stateDir}/restart.sqlite`;
        const threadId = ThreadId.make(`thread:${name}`);
        const modelSelection = {
          instanceId: ProviderInstanceId.make(driver),
          model: "restart-proof",
        };
        const layer = (path: string, failOpen = false) =>
          ProviderReplayHarness.layerWithRegistry(
            { name, runtimePolicyOverride: { cwd } },
            ProviderAdapterRegistry.layerSingle(
              unfinishedAdapter(ProviderDriverKind.make(driver), capabilities, failOpen),
            ),
            {
              databaseLayer: SqlitePersistence.layerFromPath(path).pipe(
                Layer.provide(NodeServices.layer),
              ),
              runEffectWorker: false,
              continueThreadsAfterServerUpdate: true,
              // Exercise the real last-attempt failure path without waiting for retry backoff.
              effectWorkerOptions: { maxAttempts: 1 },
            },
          );
        const sourceRunId = yield* Effect.scoped(
          Effect.gen(function* () {
            const orchestrator = yield* Orchestrator.OrchestratorV2;
            const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
            const sink = yield* EventSink.EventSinkV2;
            const ids = yield* IdAllocator.IdAllocatorV2;
            yield* orchestrator.dispatch({
              type: "thread.create",
              commandId: CommandId.make(`${name}:create`),
              threadId,
              projectId: ProjectId.make(`project:${name}`),
              title: name,
              modelSelection,
              runtimeMode: "full-access",
              interactionMode: "default",
              branch: null,
              worktreePath: cwd,
              createdBy: "user",
              creationSource: "web",
            });
            const sourceId = ids.derive.run({ threadId, ordinal: 1 });
            const cursor = yield* sink.latestSequence({ threadId });
            yield* orchestrator.dispatch({
              type: "message.dispatch",
              commandId: CommandId.make(`${name}:start`),
              threadId,
              messageId: MessageId.make(`${name}:message`),
              text: "Work until the next instruction.",
              attachments: [],
              modelSelection,
              dispatchMode: { type: "start_immediately" },
              createdBy: "user",
              creationSource: "web",
            });
            yield* worker.drain();
            yield* sink.stream({ threadId, afterSequence: cursor }).pipe(
              Stream.filter(
                (stored) =>
                  stored.event.type === "provider-turn.updated" &&
                  stored.event.runId === sourceId &&
                  stored.event.payload.status === "running",
              ),
              Stream.take(1),
              Stream.runDrain,
            );
            const before = yield* orchestrator.getThreadProjection(threadId);
            assert.equal(before.runs[0]?.status, "running");
            assert.isNotNull(before.runs[0]?.startedAt);
            const source = before.runs.find((run) => run.id === sourceId);
            if (source === undefined) throw new Error("The live source run disappeared.");
            const now = yield* DateTime.now;
            yield* sink.write({
              events: [
                {
                  id: EventId.make(`${name}:partial`),
                  type: "message.updated",
                  threadId,
                  runId: sourceId,
                  occurredAt: now,
                  payload: {
                    id: MessageId.make(`${name}:partial`),
                    threadId,
                    runId: sourceId,
                    nodeId: source.rootNodeId,
                    role: "assistant",
                    text: "Partial work before the restart.",
                    attachments: [],
                    streaming: true,
                    createdBy: "agent",
                    creationSource: "provider",
                    createdAt: now,
                    updatedAt: now,
                  },
                },
              ],
            });
            if (loss === "shutdown") {
              const service = yield* recovery;
              yield* service.prepareForShutdown;
              yield* ProviderSessionManager.ProviderSessionManagerV2.use(
                (sessions) => sessions.shutdown,
              );
              yield* service.reconcile("shutdown");
            }
            // The crash copy predates scope cleanup, preserving the dead process's
            // exact durable state instead of turning a crash into graceful shutdown.
            yield* snapshot(originalPath, restartPath);
            return sourceId;
          }).pipe(
            Effect.provide(
              Layer.mergeAll(layer(originalPath), NodeServices.layer, IdAllocator.layer),
            ),
          ),
        );

        yield* Effect.scoped(
          Effect.gen(function* () {
            const orchestrator = yield* Orchestrator.OrchestratorV2;
            const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
            const sink = yield* EventSink.EventSinkV2;
            const service = yield* recovery;
            yield* service.recover;
            const cut = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
              (run) => run.id === sourceRunId,
            );
            assert.equal(cut?.status, "cancelled");
            assert.equal(cut?.restartCut?.continuation, "scheduled");
            yield* service.recover;
            const cursor = yield* sink.latestSequence({ threadId });
            yield* worker.drain();
            yield* sink.stream({ threadId, afterSequence: cursor }).pipe(
              Stream.filter(
                (stored) =>
                  stored.event.type === "run.updated" &&
                  stored.event.payload.restartContinuationOfRunId === sourceRunId &&
                  stored.event.payload.status === (failOpen ? "failed" : "running"),
              ),
              Stream.take(1),
              Stream.runDrain,
            );
            const after = yield* orchestrator.getThreadProjection(threadId);
            const continued = after.runs.filter(
              (run) => run.restartContinuationOfRunId === sourceRunId,
            );
            assert.lengthOf(continued, 1);
            const source = after.runs.find((run) => run.id === sourceRunId);
            if (source === undefined) throw new Error("The restart source run disappeared.");
            const view = yield* orchestrator.readRestartCutView(source, after.runs);
            if (failOpen) {
              assert.isNull(continued[0]?.startedAt);
              assert.equal(view?.state, "declined");
              if (view?.state === "declined")
                assert.equal(view.reason, RESTART_CUT_REASONS.EFFECT_FAILED);
              const replacement = continued[0];
              if (replacement === undefined) throw new Error("The failed replacement disappeared.");
              const result = subagentResultForRun(after, replacement).text;
              assert.include(result, "before starting");
              assert.include(result, "Partial work before the restart.");
            } else {
              assert.isNotNull(continued[0]?.startedAt);
              assert.equal(view?.state, "continued");
            }
          }).pipe(Effect.provide(Layer.mergeAll(layer(restartPath, failOpen), NodeServices.layer))),
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "two restarts do not replay an old cut after the delegated task already returned a result",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const name = "restart-old-result";
        const cwd = yield* checkpointWorkspace(name);
        const stateDir = yield* FileSystem.FileSystem.use((fs) =>
          fs.makeTempDirectoryScoped({ prefix: name }),
        );
        const originalPath = `${stateDir}/original.sqlite`;
        const restartPath = `${stateDir}/restart.sqlite`;
        const secondRestartPath = `${stateDir}/second-restart.sqlite`;
        const parentId = ThreadId.make("thread:old-result-parent");
        const modelSelection = {
          instanceId: ProviderInstanceId.make("codex"),
          model: "restart-proof",
        };
        const layer = (path: string) =>
          ProviderReplayHarness.layerWithRegistry(
            { name, runtimePolicyOverride: { cwd } },
            ProviderAdapterRegistry.layerSingle(
              unfinishedAdapter(ProviderDriverKind.make("codex"), CodexProviderCapabilitiesV2),
            ),
            {
              databaseLayer: SqlitePersistence.layerFromPath(path).pipe(
                Layer.provide(NodeServices.layer),
              ),
              runEffectWorker: false,
              continueThreadsAfterServerUpdate: true,
            },
          );
        const runningRun = Effect.fnUntraced(function* (threadId: ThreadId, ordinal: number) {
          const sink = yield* EventSink.EventSinkV2;
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const run = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
            (run) => run.ordinal === ordinal,
          );
          if (run === undefined) throw new Error("The requested run was not created.");
          yield* sink.stream({ threadId, afterSequence: 0 }).pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "provider-turn.updated" &&
                stored.event.runId === run.id &&
                stored.event.payload.status === "running",
            ),
            Stream.take(1),
            Stream.runDrain,
          );
          const projection = yield* orchestrator.getThreadProjection(threadId);
          const fresh = projection.runs.find((candidate) => candidate.id === run.id);
          const turn = projection.providerTurns.find(
            (turn) => turn.runAttemptId === fresh?.activeAttemptId,
          );
          if (fresh === undefined || turn === undefined)
            throw new Error("The provider did not start the requested run.");
          return { run: fresh, turn };
        });
        const childId = yield* Effect.scoped(
          Effect.gen(function* () {
            const orchestrator = yield* Orchestrator.OrchestratorV2;
            yield* orchestrator.dispatch({
              type: "thread.create",
              commandId: CommandId.make(`${name}:create`),
              threadId: parentId,
              projectId: ProjectId.make(`project:${name}`),
              title: name,
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
              commandId: CommandId.make(`${name}:start`),
              threadId: parentId,
              messageId: MessageId.make(`${name}:prompt`),
              text: "Delegate work.",
              attachments: [],
              modelSelection,
              dispatchMode: { type: "start_immediately" },
              createdBy: "user",
              creationSource: "web",
            });
            yield* EffectWorker.OrchestrationEffectWorkerV2.use((worker) => worker.drain());
            const parentRun = (yield* orchestrator.getThreadProjection(parentId)).runs[0]!;
            yield* orchestrator.dispatch({
              type: "delegated_task.request",
              commandId: CommandId.make(`${name}:delegate`),
              parentThreadId: parentId,
              parentRunId: parentRun.id,
              parentNodeId: parentRun.rootNodeId!,
              task: "Finish the work.",
              modelSelection,
              runtimeMode: "full-access",
              interactionMode: "default",
              completionWake: "always",
              createdBy: "agent",
              creationSource: "mcp",
            });
            yield* EffectWorker.OrchestrationEffectWorkerV2.use((worker) => worker.drain());
            const task = (yield* orchestrator.getThreadProjection(parentId)).subagents[0]!;
            assert.isNotNull(task.childThreadId);
            const { run: initial } = yield* runningRun(task.childThreadId!, 1);
            const { delegatedTaskId: _taskId, ...legacyRun } = initial;
            const sink = yield* EventSink.EventSinkV2;
            yield* sink.write({
              events: [
                {
                  id: EventId.make(`${name}:legacy-binding`),
                  type: "run.updated",
                  threadId: task.childThreadId!,
                  runId: initial.id,
                  occurredAt: yield* DateTime.now,
                  payload: legacyRun,
                },
              ],
            });
            yield* snapshot(originalPath, restartPath);
            return task.childThreadId!;
          }).pipe(Effect.provide(Layer.mergeAll(layer(originalPath), NodeServices.layer))),
        );

        const oldCutId = yield* Effect.scoped(
          Effect.gen(function* () {
            const orchestrator = yield* Orchestrator.OrchestratorV2;
            const sink = yield* EventSink.EventSinkV2;
            const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
            yield* (yield* recovery).recover; // First restart.
            yield* worker.drain();
            const { run: current, turn: completedTurn } = yield* runningRun(childId, 2);
            const now = yield* DateTime.now;
            yield* sink.write({
              events: [
                {
                  id: EventId.make(`${name}:turn-done`),
                  type: "provider-turn.updated",
                  threadId: childId,
                  runId: current.id,
                  occurredAt: now,
                  payload: { ...completedTurn, status: "completed", completedAt: now },
                },
                {
                  id: EventId.make(`${name}:done`),
                  type: "run.updated",
                  threadId: childId,
                  runId: current.id,
                  occurredAt: now,
                  payload: { ...current, status: "completed", completedAt: now },
                },
              ],
            });
            yield* orchestrator.recoverDelegatedTask(childId, current.id);
            assert.isNotNull(
              (yield* orchestrator.getThreadProjection(parentId)).subagents[0]?.result,
            );
            yield* orchestrator.dispatch({
              type: "message.dispatch",
              commandId: CommandId.make(`${name}:followup`),
              threadId: childId,
              messageId: MessageId.make(`${name}:followup-prompt`),
              text: "Follow up.",
              attachments: [],
              modelSelection,
              dispatchMode: { type: "start_immediately" },
              createdBy: "agent",
              creationSource: "mcp",
            });
            yield* worker.drain();
            const { run: followup, turn } = yield* runningRun(childId, 3);
            // Replay the persisted event shape of the pre-#301 restart: it cut the
            // follow-up without metadata or an intent, after the first task result.
            const cutAt = yield* DateTime.now;
            yield* sink.commitCommand({
              commandId: CommandId.make(`command:runtime-reconcile:${name}:legacy-cut`),
              threadId: childId,
              commandType: "provider-runtime.reconcile",
              acceptedAt: cutAt,
              effects: [],
              events: [
                {
                  id: EventId.make(`${name}:legacy-turn-cut`),
                  type: "provider-turn.updated",
                  threadId: childId,
                  runId: followup.id,
                  occurredAt: cutAt,
                  payload: { ...turn, status: "cancelled", completedAt: cutAt },
                },
                {
                  id: EventId.make(`${name}:legacy-cut`),
                  type: "run.updated",
                  threadId: childId,
                  runId: followup.id,
                  occurredAt: cutAt,
                  payload: { ...followup, status: "cancelled", completedAt: cutAt },
                },
              ],
            });
            yield* snapshot(restartPath, secondRestartPath);
            return followup.id;
          }).pipe(Effect.provide(Layer.mergeAll(layer(restartPath), NodeServices.layer))),
        );

        yield* Effect.scoped(
          Effect.gen(function* () {
            const orchestrator = yield* Orchestrator.OrchestratorV2;
            yield* (yield* recovery).recover; // Second restart, fresh SQLite/runtime.
            yield* orchestrator.recoverDelegatedTasks;
            const parent = yield* orchestrator.getThreadProjection(parentId);
            assert.isFalse(
              parent.contextTransfers.some(
                (transfer) =>
                  transfer.type === "subagent_result" && transfer.sourcePoint.runId === oldCutId,
              ),
            );
            const cut = (yield* orchestrator.getThreadProjection(childId)).runs.find(
              (run) => run.id === oldCutId,
            );
            assert.equal(cut?.restartCut?.continuation, "declined");
            if (cut?.restartCut?.continuation === "declined")
              assert.equal(cut.restartCut.reason, RESTART_CUT_REASONS.TASK_SETTLED);
            const projections = yield* ProjectionStore.ProjectionStoreV2;
            assert.notInclude(yield* projections.getRecoveryThreadIds("subagent-results"), childId);
            yield* orchestrator.recoverDelegatedTasks;
            assert.isFalse(
              (yield* orchestrator.getThreadProjection(parentId)).contextTransfers.some(
                (transfer) => transfer.sourcePoint.runId === oldCutId,
              ),
            );
          }).pipe(Effect.provide(Layer.mergeAll(layer(secondRestartPath), NodeServices.layer))),
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);
