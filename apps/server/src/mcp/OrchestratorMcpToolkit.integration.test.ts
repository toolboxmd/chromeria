import * as NodeServices from "@effect/platform-node/NodeServices";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as PrismServerSettings from "../serverSettings.ts";
import * as ThreadLaunch from "../orchestration-v2/ThreadLaunchService.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as ProviderAdapterRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import { describe, expect, it } from "@effect/vitest";
import {
  CommandId,
  ChatAttachmentId,
  EventId,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  ProviderThreadId,
  ProviderTurnId,
  RuntimeRequestId,
  TurnItemId,
  EnvironmentId,
  isProviderNativeSubagentThread,
  MessageId,
  NodeId,
  type ModelSelection,
  OrchestratorMcpDelegateTaskResult,
  OrchestratorMcpThreadInterruptResult,
  OrchestratorMcpThreadSendResult,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderOptionDescriptor,
  RunId,
  type ServerProvider,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { McpSchema, McpServer } from "effect/ai";

import { CodexOrchestratorReplayHarness } from "../orchestration-v2/Adapters/CodexAdapterV2.testkit.ts";
import * as CommandReceiptStore from "../orchestration-v2/CommandReceiptStore.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ThreadLaunchService from "../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import * as ProviderReplayHarness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import {
  decodeProviderReplayNdjson,
  materializeReplayTranscriptWorkspace,
} from "@t3tools/provider-testing/replayTranscript";
import * as ProviderRegistryMock from "../provider/testUtils/providerRegistryMock.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as RemoteDelegation from "../peer/RemoteDelegation.ts";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerConfig from "../config.ts";
import { createPendingAttachmentId, resolveAttachmentPath } from "../attachmentStore.ts";
import * as EffectWorker from "../orchestration-v2/EffectWorker.ts";
import { ServerActivation } from "../serverActivation.ts";
import * as AttachmentHandlers from "./toolkits/attachment/handlers.ts";
import { AttachmentToolkit } from "./toolkits/attachment/tools.ts";
import * as McpHttpServer from "./McpHttpServer.ts";
import * as McpInvocationContext from "./McpInvocationContext.ts";
import * as PeerForwarding from "../peer/PeerForwarding.ts";
import * as PeerLinkRequests from "../peer/PeerLinkRequests.ts";
import * as PeerLinks from "../peer/PeerLinks.ts";
import { delegatedTaskRun, hasPendingChildRuns } from "./OrchestratorMcpService.ts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as BareLaunch from "../orchestration-v2/BareLaunch.ts";

const layerOrchestratorToolkit = McpHttpServer.layerOrchestratorToolkit.pipe(
  Layer.provide(Layer.mock(BareLaunch.BareLaunch)({})),
  Layer.provide(PrismServerSettings.layerTest()),
  Layer.provide(Layer.mock(RemoteDelegation.RemoteDelegation)({})),
  Layer.provide(Layer.mock(PeerForwarding.PeerForwarding)({})),
  Layer.provide(Layer.mock(ThreadLaunchService.ThreadLaunchService)({})),
  Layer.provide(Layer.mock(GitVcsDriver.GitVcsDriver)({})),
);

/** A task delegated here always has a local child thread. */
const localChild = (childThreadId: ThreadId | null): ThreadId => {
  if (childThreadId === null) throw new Error("expected a local delegated child");
  return childThreadId;
};

// Effect returns a declared tool failure as `isError` with its encoded payload
// as JSON text, never as `structuredContent`.

const parentThreadId = ThreadId.make("thread:mcp-orchestrator-parent");
const projectId = ProjectId.make("project:mcp-orchestrator");
const codexInstanceId = ProviderInstanceId.make("codex");
const codexModel = "gpt-5.4";
const parentPrompt = "Keep this parent turn active while orchestration tools are tested.";
const delegatedPrompt = "Inspect the delegated API boundary and return the result.";
const delegatedResult = "Delegated API boundary inspected.";
const cancellationPrompt = "Remain active until the parent cancels this delegated task.";
const queuedFollowupPrompt = "Complete the queued follow-up and return the final result.";
const queuedFollowupResult = "Queued delegated follow-up completed.";

const decodeDelegateTaskResult = Schema.decodeUnknownEffect(OrchestratorMcpDelegateTaskResult);
const decodeThreadInterruptResult = Schema.decodeUnknownEffect(
  OrchestratorMcpThreadInterruptResult,
);
const decodeThreadSendResult = Schema.decodeUnknownEffect(OrchestratorMcpThreadSendResult);

const codexSelection = {
  instanceId: codexInstanceId,
  model: codexModel,
} satisfies ModelSelection;

const delegatedTaskStatusTranscriptFile = new URL(
  "../orchestration-v2/testkit/fixtures/delegated_task_status/codex_transcript.ndjson",
  import.meta.url,
);

const readDelegatedTaskStatusTranscript = Effect.fn("readDelegatedTaskStatusTranscript")(function* (
  followupResult: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const text = yield* fs.readFileString(
    decodeURIComponent(delegatedTaskStatusTranscriptFile.pathname),
  );
  const transcript = yield* decodeProviderReplayNdjson(
    text.replaceAll(queuedFollowupResult, followupResult),
  );
  // The recording ends when the recorder closes the app-server while the parent
  // turn is still open. This test keeps acting as that parent afterwards, and
  // replaying the exit would kill the shared process and fail the parent run.
  return {
    ...transcript,
    entries: transcript.entries.filter((entry) => entry.type !== "runtime_exit"),
  };
}, Effect.provide(NodeServices.layer));

function makeProviderSnapshot(input: {
  readonly instanceId: ProviderInstanceId;
  readonly driver: ProviderDriverKind;
  readonly model: string;
  readonly optionDescriptors?: ReadonlyArray<ProviderOptionDescriptor>;
}): ServerProvider {
  return {
    instanceId: input.instanceId,
    driver: input.driver,
    enabled: true,
    installed: true,
    version: "test",
    status: "ready",
    auth: { status: "authenticated" },
    checkedAt: "2026-06-17T00:00:00.000Z",
    models: [
      {
        slug: input.model,
        name: input.model,
        isCustom: false,
        capabilities:
          input.optionDescriptors === undefined
            ? null
            : { optionDescriptors: input.optionDescriptors },
      },
    ],
    slashCommands: [],
    skills: [],
  };
}

const client = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  clientCapabilities: {},
  clientInfo: { name: "orchestrator-mcp-test", version: "1.0.0" },
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "orchestrator-mcp-test", version: "1.0.0" },
  },
  getClient: Effect.die("unused"),
});

const layerMemorySecretStore = Layer.sync(ServerSecretStore.ServerSecretStore, () => {
  const stored = new Map<string, Uint8Array>();
  return ServerSecretStore.ServerSecretStore.of({
    get: (name) => Effect.succeed(Option.fromNullishOr(stored.get(name))),
    set: (name, value) => Effect.sync(() => void stored.set(name, value)),
    create: (name, value) => Effect.sync(() => void stored.set(name, value)),
    getOrCreateRandom: (name, bytes) =>
      Effect.sync(() => {
        const existing = stored.get(name);
        if (existing) return existing;
        const value = new Uint8Array(bytes).fill(7);
        stored.set(name, value);
        return value;
      }),
    remove: (name) => Effect.sync(() => void stored.delete(name)),
  });
});

const layerUnusedScheduledTaskStub = Layer.succeed(
  ScheduledTaskService.ScheduledTaskService,
  ScheduledTaskService.ScheduledTaskService.of({
    list: () => Effect.succeed({ tasks: [] }),
    subscribeList: () => Stream.succeed({ tasks: [] }),
    upsert: () => Effect.die("ScheduledTaskService.upsert is unused in this test"),
    setEnabled: () => Effect.die("ScheduledTaskService.setEnabled is unused in this test"),
    delete: () => Effect.die("ScheduledTaskService.delete is unused in this test"),
    deleteForThread: () => Effect.die("unused in this test"),
    runNow: () => Effect.die("ScheduledTaskService.runNow is unused in this test"),
    rotateWebhookToken: () => Effect.die("unused in this test"),
    listWebhookDeliveries: () => Effect.die("unused in this test"),
    getWebhookDelivery: () => Effect.die("unused in this test"),
    triggerWebhook: () => Effect.die("unused in this test"),
  }),
);

// Fork: helpers for the descendant notice test (toolboxmd/chromeria#304).
// Effect returns a declared tool failure as `isError` with its encoded payload
// as JSON text, never as `structuredContent`.
const declaredFailure = (result: McpSchema.CallToolResult) => {
  const text = result.content[0];
  return result.isError === true && text?.type === "text" ? JSON.parse(text.text) : undefined;
};

interface CapturedTurn {
  readonly instanceId: ProviderInstanceId;
  readonly threadId: ThreadId;
  readonly text: string;
}

function unsupported(driver: ProviderDriverKind, detail: string) {
  return Effect.fail(new ProviderAdapter.ProviderAdapterProtocolError({ driver, detail }));
}

function makeDeterministicAdapter(input: {
  readonly instanceId: ProviderInstanceId;
  readonly driver: ProviderDriverKind;
  readonly capabilities: OrchestrationV2ProviderCapabilities;
  readonly capturedTurns: Ref.Ref<ReadonlyArray<CapturedTurn>>;
  readonly shouldComplete: (turn: ProviderAdapter.ProviderAdapterV2TurnInput) => boolean;
  readonly terminalGate?: (
    turn: ProviderAdapter.ProviderAdapterV2TurnInput,
  ) => Deferred.Deferred<void> | undefined;
  readonly response: (turn: ProviderAdapter.ProviderAdapterV2TurnInput) => string;
}): ProviderAdapter.ProviderAdapterV2["Service"] {
  return {
    instanceId: input.instanceId,
    driver: input.driver,
    getCapabilities: () => Effect.succeed(input.capabilities),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
    openSession: (sessionInput) =>
      Effect.gen(function* () {
        const events = yield* PubSub.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
        const now = yield* DateTime.now;
        const providerSession: OrchestrationV2ProviderSession = {
          id: sessionInput.providerSessionId,
          driver: input.driver,
          providerInstanceId: input.instanceId,
          status: "ready",
          cwd: sessionInput.runtimePolicy.cwd ?? process.cwd(),
          model: sessionInput.modelSelection.model,
          capabilities: input.capabilities,
          createdAt: now,
          updatedAt: now,
          lastError: null,
        };

        const publish = (providerEvents: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>) =>
          Effect.forEach(providerEvents, (event) => PubSub.publish(events, event), {
            discard: true,
          });
        const runOrdinals = new Map<ProviderTurnId, number>();
        const turnInputs = new Map<ProviderTurnId, ProviderAdapter.ProviderAdapterV2TurnInput>();

        return {
          instanceId: input.instanceId,
          driver: input.driver,
          providerSessionId: sessionInput.providerSessionId,
          providerSession,
          events: Stream.fromPubSub(events),
          ensureThread: (threadInput) =>
            Effect.gen(function* () {
              const createdAt = yield* DateTime.now;
              const nativeThreadId = `${input.driver}:${threadInput.threadId}`;
              return {
                id: ProviderThreadId.make(`provider-thread:${nativeThreadId}`),
                driver: input.driver,
                providerInstanceId: input.instanceId,
                providerSessionId: sessionInput.providerSessionId,
                appThreadId: threadInput.threadId,
                ownerNodeId: null,
                nativeThreadRef: {
                  driver: input.driver,
                  nativeId: nativeThreadId,
                  strength: "strong",
                },
                nativeConversationHeadRef: null,
                status: "idle",
                firstRunOrdinal: null,
                lastRunOrdinal: null,
                handoffIds: [],
                forkedFrom: null,
                createdAt,
                updatedAt: createdAt,
              } satisfies OrchestrationV2ProviderThread;
            }),
          resumeThread: ({ providerThread }) => Effect.succeed(providerThread),
          startTurn: (turnInput) =>
            Effect.gen(function* () {
              yield* Ref.update(input.capturedTurns, (turns) => [
                ...turns,
                {
                  instanceId: input.instanceId,
                  threadId: turnInput.threadId,
                  text: turnInput.message.text,
                },
              ]);
              const eventTime = yield* DateTime.now;
              const providerTurnId = ProviderTurnId.make(
                `provider-turn:${input.instanceId}:${turnInput.threadId}:${turnInput.runOrdinal}`,
              );
              runOrdinals.set(providerTurnId, turnInput.runOrdinal);
              turnInputs.set(providerTurnId, turnInput);
              yield* publish([
                {
                  type: "provider_turn.updated",
                  driver: input.driver,
                  providerTurn: {
                    id: providerTurnId,
                    providerThreadId: turnInput.providerThread.id,
                    nodeId: turnInput.rootNodeId,
                    runAttemptId: turnInput.attemptId,
                    nativeTurnRef: {
                      driver: input.driver,
                      nativeId: `native-turn:${turnInput.threadId}:${turnInput.runOrdinal}`,
                      strength: "strong",
                    },
                    ordinal: turnInput.providerTurnOrdinal,
                    status: "running",
                    startedAt: eventTime,
                    completedAt: null,
                  },
                },
              ]);
              const terminalGate = input.terminalGate?.(turnInput);
              if (terminalGate !== undefined) {
                yield* Deferred.await(terminalGate);
              } else if (!input.shouldComplete(turnInput)) {
                return;
              }
              const response = input.response(turnInput);
              yield* publish([
                {
                  type: "provider_turn.updated",
                  driver: input.driver,
                  providerTurn: {
                    id: providerTurnId,
                    providerThreadId: turnInput.providerThread.id,
                    nodeId: turnInput.rootNodeId,
                    runAttemptId: turnInput.attemptId,
                    nativeTurnRef: {
                      driver: input.driver,
                      nativeId: `native-turn:${turnInput.threadId}:${turnInput.runOrdinal}`,
                      strength: "strong",
                    },
                    ordinal: turnInput.providerTurnOrdinal,
                    status: "completed",
                    startedAt: eventTime,
                    completedAt: eventTime,
                  },
                },
                {
                  type: "turn_item.updated",
                  driver: input.driver,
                  turnItem: {
                    id: TurnItemId.make(
                      `turn-item:${input.instanceId}:${turnInput.threadId}:${turnInput.runOrdinal}:assistant`,
                    ),
                    threadId: turnInput.threadId,
                    runId: turnInput.runId,
                    nodeId: turnInput.rootNodeId,
                    providerThreadId: turnInput.providerThread.id,
                    providerTurnId,
                    nativeItemRef: null,
                    parentItemId: null,
                    ordinal: turnInput.runOrdinal * 100 + 1,
                    status: "completed",
                    title: null,
                    startedAt: eventTime,
                    completedAt: eventTime,
                    updatedAt: eventTime,
                    type: "assistant_message",
                    messageId: MessageId.make(
                      `message:${input.instanceId}:${turnInput.threadId}:${turnInput.runOrdinal}:assistant`,
                    ),
                    text: response,
                    streaming: false,
                  },
                },
                {
                  type: "turn.terminal",
                  driver: input.driver,
                  providerThreadId: turnInput.providerThread.id,
                  providerTurnId,
                  runOrdinal: turnInput.runOrdinal,
                  status: "completed",
                  failure: null,
                  threadDisposition: "reusable",
                },
              ]);
            }),
          steerTurn: () => Effect.void,
          interruptTurn: ({ providerThread, providerTurnId }) =>
            Effect.gen(function* () {
              const turnInput = turnInputs.get(providerTurnId);
              const completedAt = yield* DateTime.now;
              if (turnInput !== undefined) {
                yield* publish([
                  {
                    type: "provider_turn.updated",
                    driver: input.driver,
                    providerTurn: {
                      id: providerTurnId,
                      providerThreadId: providerThread.id,
                      nodeId: turnInput.rootNodeId,
                      runAttemptId: turnInput.attemptId,
                      nativeTurnRef: {
                        driver: input.driver,
                        nativeId: `native-turn:${turnInput.threadId}:${turnInput.runOrdinal}`,
                        strength: "strong",
                      },
                      ordinal: turnInput.providerTurnOrdinal,
                      status: "interrupted",
                      startedAt: completedAt,
                      completedAt,
                    },
                  },
                ]);
              }
              yield* publish([
                {
                  type: "turn.terminal",
                  driver: input.driver,
                  providerThreadId: providerThread.id,
                  providerTurnId,
                  runOrdinal: runOrdinals.get(providerTurnId) ?? 1,
                  status: "interrupted",
                  failure: null,
                  threadDisposition: "reusable",
                },
              ]);
            }),
          respondToRuntimeRequest: () => Effect.void,
          readThreadSnapshot: () =>
            unsupported(input.driver, "readThreadSnapshot is unused in this test"),
          rollbackThread: () => unsupported(input.driver, "rollbackThread is unused in this test"),
          forkThread: () => unsupported(input.driver, "forkThread is unused in this test"),
        };
      }),
  };
}

describe("orchestrator MCP toolkit", () => {
  it.live("child text and attachment updates queue as notices on a steerable user turn", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const cwd = yield* checkpointWorkspace("ancestor-mcp-notices");
        const capturedTurns = yield* Ref.make<ReadonlyArray<CapturedTurn>>([]);
        const started = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2TurnInput>();
        const parentGate = yield* Deferred.make<void>();
        const childGate = yield* Deferred.make<void>();
        const calls: string[] = [];
        const base = makeDeterministicAdapter({
          instanceId: codexInstanceId,
          driver: ProviderDriverKind.make("codex"),
          capabilities: CodexProviderCapabilitiesV2,
          capturedTurns,
          shouldComplete: () => true,
          terminalGate: (turn) =>
            turn.runOrdinal === 1
              ? turn.threadId === parentThreadId
                ? parentGate
                : childGate
              : undefined,
          response: () => "Work complete.",
        });
        const adapter: ProviderAdapter.ProviderAdapterV2["Service"] = {
          ...base,
          openSession: (input) =>
            Effect.gen(function* () {
              const scope = yield* Effect.scope;
              const runtime = yield* base.openSession(input);
              return {
                ...runtime,
                startTurn: (turn) =>
                  Queue.offer(started, turn).pipe(
                    Effect.andThen(runtime.startTurn(turn).pipe(Effect.forkIn(scope))),
                    Effect.asVoid,
                  ),
                steerTurn: (turn) =>
                  Effect.sync(() => {
                    calls.push("steer");
                  }).pipe(Effect.andThen(runtime.steerTurn(turn))),
                interruptTurn: (turn) =>
                  Effect.sync(() => {
                    calls.push("interrupt");
                  }).pipe(Effect.andThen(runtime.interruptTurn(turn))),
              };
            }),
        };
        const registry = ProviderAdapterRegistry.layerFromAdapters([adapter]);
        const replay = ProviderReplayHarness.layerWithRegistry(
          { name: "ancestor-mcp-notices" },
          registry,
        );
        const layers = Layer.merge(
          McpHttpServer.layerOrchestratorToolkit.pipe(
            Layer.provide(Layer.mock(BareLaunch.BareLaunch)({})),
          ),
          McpHttpServer.toolkitRegistration(AttachmentToolkit, AttachmentHandlers.layer),
        ).pipe(
          Layer.provide(PrismServerSettings.layerTest()),
          Layer.provide(Layer.mock(PeerForwarding.PeerForwarding)({})),
          Layer.provide(Layer.mock(RemoteDelegation.RemoteDelegation)({})),
          Layer.provideMerge(McpServer.McpServer.layer),
          Layer.provideMerge(replay),
          Layer.provide(registry),
          Layer.provide(
            ProviderRegistryMock.layer([
              makeProviderSnapshot({
                instanceId: codexInstanceId,
                driver: ProviderDriverKind.make("codex"),
                model: codexModel,
              }),
            ]),
          ),
          Layer.provide(layerUnusedScheduledTaskStub),
          Layer.provideMerge(
            SecretRequests.layer.pipe(Layer.provide(layerMemorySecretStore), Layer.provide(replay)),
          ),
          Layer.provide(layerMemorySecretStore),
          Layer.provideMerge(
            ServerConfig.layerTest(cwd, { prefix: "t3-ancestor-attachments-" }).pipe(
              Layer.provide(NodeServices.layer),
            ),
          ),
          Layer.provide(
            Layer.mergeAll(
              Layer.mock(ProjectService.ProjectService)({}),
              Layer.mock(ThreadLaunch.ThreadLaunchService)({}),
              Layer.mock(GitVcsDriver.GitVcsDriver)({}),
              Layer.succeed(ServerActivation, Effect.void),
            ),
          ),
          Layer.provideMerge(NodeServices.layer),
        );
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const server = yield* McpServer.McpServer;
          const create = yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("ancestor:mcp-create"),
            threadId: parentThreadId,
            projectId,
            title: "User planner",
            modelSelection: codexSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: cwd,
            createdBy: "user",
            creationSource: "web",
          });
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("ancestor:mcp-start"),
            threadId: parentThreadId,
            messageId: MessageId.make("ancestor:mcp-user"),
            text: parentPrompt,
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          const waitForRun = (
            threadId: ThreadId,
            afterSequence: number,
            status: "running" | "completed",
            runId?: RunId,
          ) =>
            orchestrator.streamStoredEventsFrom({ threadId, afterSequence }).pipe(
              Stream.filter(
                ({ event }) =>
                  event.type === "run.updated" &&
                  event.payload.status === status &&
                  (runId === undefined || event.payload.id === runId),
              ),
              Stream.take(1),
              Stream.runDrain,
            );
          yield* waitForRun(parentThreadId, create.sequence, "running");
          const parentInput = yield* Queue.take(started);
          const delegated = yield* orchestrator.dispatch({
            type: "delegated_task.request",
            commandId: CommandId.make("ancestor:mcp-delegate"),
            parentThreadId,
            parentRunId: parentInput.runId,
            parentNodeId: parentInput.rootNodeId,
            task: "Build and report",
            modelSelection: codexSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            completionWake: "settled_only",
            createdBy: "agent",
            creationSource: "mcp",
          });
          const childId = (yield* orchestrator.getThreadProjection(parentThreadId)).subagents[0]!
            .childThreadId!;
          yield* waitForRun(childId, delegated.sequence, "running");
          yield* Queue.take(started);
          yield* worker.drain();
          const invocation = (threadId: ThreadId): McpInvocationContext.McpInvocationScope => ({
            environmentId: EnvironmentId.make("ancestor:mcp-environment"),
            requestNamespace: `ancestor:${threadId}`,
            thread: {
              threadId,
              providerSessionId: `ancestor:${threadId}`,
              providerInstanceId: codexInstanceId,
            },
            client: undefined,
            capabilities: new Set(["orchestration"]),
            issuedAt: 1,
          });
          const invoke = (threadId: ThreadId, name: string, args: Record<string, unknown>) =>
            server
              .callTool({ name, arguments: args })
              .pipe(
                Effect.provideService(
                  McpInvocationContext.McpInvocationContext,
                  invocation(threadId),
                ),
                Effect.provideService(McpSchema.McpServerClient, client),
              );
          const sink = yield* EventSink.EventSinkV2;
          const now = yield* DateTime.now;
          const active = yield* orchestrator.getThreadProjection(parentThreadId);
          const request = {
            id: RuntimeRequestId.make("ancestor:pending-question"),
            nodeId: parentInput.rootNodeId,
            providerTurnId: active.providerTurns[0]!.id,
            nativeRequestRef: null,
            kind: "user_input" as const,
            status: "pending" as const,
            responseCapability: { type: "message" as const },
            createdAt: now,
            resolvedAt: null,
          };
          const tool = {
            id: TurnItemId.make("ancestor:open-tool"),
            threadId: parentThreadId,
            runId: parentInput.runId,
            nodeId: parentInput.rootNodeId,
            providerThreadId: parentInput.providerThread.id,
            providerTurnId: active.providerTurns[0]!.id,
            nativeItemRef: null,
            parentItemId: null,
            ordinal: 1000,
            status: "running" as const,
            title: "User's pending check",
            startedAt: now,
            completedAt: null,
            updatedAt: now,
            type: "dynamic_tool" as const,
            toolName: "check",
            input: { check: "pending" },
          };
          yield* sink.write({
            events: [
              {
                id: EventId.make("ancestor:tool-open"),
                threadId: parentThreadId,
                occurredAt: now,
                type: "turn-item.updated",
                payload: tool,
              },
              {
                id: EventId.make("ancestor:question-pending"),
                threadId: parentThreadId,
                occurredAt: now,
                type: "runtime-request.updated",
                payload: request,
              },
            ],
          });
          const before = yield* orchestrator.getThreadProjection(parentThreadId);
          expect(before.runs[0]!.status).toBe("running");
          expect(before.providerSessions[0]!.capabilities?.turns.supportsActiveSteering).toBe(true);
          const text = "The implementation and UI proof are ready.\nReview the detailed result.";
          const sentCall = yield* invoke(childId, "t3_thread_send", {
            threadId: parentThreadId,
            message: text,
            clientRequestId: "ancestor-update",
          });
          expect(sentCall.isError).not.toBe(true);
          const sent = yield* decodeThreadSendResult(sentCall.structuredContent);
          expect(sent.delivery).toBe("queued");
          for (const mode of ["steer", "restart"]) {
            const refusal = yield* invoke(childId, "t3_thread_send", {
              threadId: parentThreadId,
              message: text,
              mode,
              clientRequestId: "ancestor-update",
            });
            expect(declaredFailure(refusal)).toMatchObject({ code: "invalid_request" });
          }
          const fileSystem = yield* FileSystem.FileSystem;
          const config = yield* ServerConfig.ServerConfig;
          const id = ChatAttachmentId.make(createPendingAttachmentId()!);
          yield* fileSystem.writeFile(
            `${config.attachmentsDir}/${id}.png`,
            new Uint8Array([1, 2, 3]),
          );
          const attachmentCall = yield* invoke(childId, "t3_thread_send_attachments", {
            threadId: parentThreadId,
            attachments: [
              { type: "image", id, name: "proof.png", mimeType: "image/png", sizeBytes: 3 },
            ],
          });
          expect(attachmentCall.isError).not.toBe(true);
          const after = yield* orchestrator.getThreadProjection(parentThreadId);
          const notices = after.messages.filter((message) => message.senderThreadId === childId);
          expect(notices).toHaveLength(2);
          expect(
            notices.every(
              (message) =>
                message.notification !== undefined &&
                message.delegatedCompletion === undefined &&
                message.creationSource === "server",
            ),
          ).toBe(true);
          const imageMessage = notices.find((message) => message.attachments.length > 0)!;
          expect(imageMessage.notification?.summary).toMatch(/^1 attachment.*proof\.png/);
          const claimed = resolveAttachmentPath({
            attachmentsDir: config.attachmentsDir,
            attachment: imageMessage.attachments[0]!,
          });
          expect(claimed).not.toBeNull();
          expect(Array.from(yield* fileSystem.readFile(claimed!))).toEqual([1, 2, 3]);
          expect(after.runs.find((run) => run.id === parentInput.runId)).toEqual(before.runs[0]);
          expect(after.attempts.filter((attempt) => attempt.runId === parentInput.runId)).toEqual(
            before.attempts.filter((attempt) => attempt.runId === parentInput.runId),
          );
          expect(after.providerTurns).toEqual(before.providerTurns);
          expect(after.runtimeRequests).toEqual(before.runtimeRequests);
          expect(after.turnItems.filter((item) => item.runId === parentInput.runId)).toEqual(
            before.turnItems.filter((item) => item.runId === parentInput.runId),
          );
          yield* worker.drain();
          expect(calls).toEqual([]);
          const repeat = yield* invoke(childId, "t3_thread_send", {
            threadId: parentThreadId,
            message: "A replacement must not run",
            clientRequestId: "ancestor-update",
          });
          expect((yield* decodeThreadSendResult(repeat.structuredContent)).messageId).toBe(
            sent.messageId,
          );
          expect(
            (yield* orchestrator.getThreadProjection(parentThreadId)).messages.filter(
              (message) => message.senderThreadId === childId,
            ),
          ).toHaveLength(2);

          // A positive control proves this fake can steer an ordinary active target.
          const ordinary = yield* invoke(parentThreadId, "t3_thread_send", {
            threadId: childId,
            message: "Parent guidance",
            clientRequestId: "ordinary-control",
          });
          expect((yield* decodeThreadSendResult(ordinary.structuredContent)).delivery).toBe(
            "steered",
          );
          yield* worker.drain();
          expect(calls).toEqual(["steer"]);
          yield* orchestrator.dispatch({
            type: "thread.runtime-mode.set",
            commandId: CommandId.make("ancestor:lower-caller"),
            threadId: childId,
            runtimeMode: "approval-required",
          });
          const limited = yield* invoke(childId, "t3_thread_send", {
            threadId: parentThreadId,
            message: "Must be refused",
            clientRequestId: "ancestor-limited",
          });
          expect(limited.isError).toBe(true);
          expect(
            (yield* orchestrator.getThreadProjection(parentThreadId)).messages.filter(
              (message) => message.senderThreadId === childId,
            ),
          ).toHaveLength(2);
          yield* sink.write({
            events: [
              {
                id: EventId.make("ancestor:tool-complete"),
                threadId: parentThreadId,
                occurredAt: now,
                type: "turn-item.updated",
                payload: { ...tool, status: "completed", completedAt: now },
              },
              {
                id: EventId.make("ancestor:question-resolved"),
                threadId: parentThreadId,
                occurredAt: now,
                type: "runtime-request.updated",
                payload: { ...request, status: "resolved", resolvedAt: now },
              },
            ],
          });
          yield* Deferred.succeed(childGate, undefined);
          yield* waitForRun(childId, delegated.sequence, "completed");
          yield* Deferred.succeed(parentGate, undefined);
          yield* waitForRun(parentThreadId, create.sequence, "completed");
          yield* worker.drain();
          const noticeInputs: ProviderAdapter.ProviderAdapterV2TurnInput[] = [];
          while (noticeInputs.length < notices.length) {
            const next = yield* Queue.take(started);
            if (notices.some((message) => message.id === next.message.messageId))
              noticeInputs.push(next);
          }
          for (const message of notices) {
            const providerInput = noticeInputs.find(
              (input) => input.message.messageId === message.id,
            )!;
            expect(providerInput.message.text).toBe(message.text);
            expect(providerInput.message.attachments).toEqual(message.attachments);
            expect(providerInput.message.createdBy).toBe("agent");
            expect(providerInput.message.creationSource).toBe("server");
            yield* waitForRun(parentThreadId, create.sequence, "completed", providerInput.runId);
          }
          expect(
            noticeInputs.find((input) => input.message.messageId === sent.messageId)?.message.text,
          ).toContain(text);
          const final = yield* orchestrator.getThreadProjection(parentThreadId);
          for (const message of notices) {
            expect(
              final.turnItems.some(
                (item) => item.type === "notification" && item.runId === message.runId,
              ),
            ).toBe(true);
            expect(
              final.turnItems.some(
                (item) => item.type === "user_message" && item.messageId === message.id,
              ),
            ).toBe(false);
          }
          const oldCaller = yield* invoke(childId, "t3_thread_send", {
            threadId: parentThreadId,
            message: text,
            clientRequestId: "ancestor-update",
          });
          expect(declaredFailure(oldCaller)).toMatchObject({ code: "parent_not_active" });
        }).pipe(Effect.provide(layers));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live.each([
    { label: "distinct", result: queuedFollowupResult, latestSummary: queuedFollowupResult },
    { label: "identical", result: delegatedResult, latestSummary: null },
  ])("reports $label child results", ({ result, latestSummary }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const cwd = yield* checkpointWorkspace("delegated-task-status");
        const rawTranscript = yield* readDelegatedTaskStatusTranscript(result);
        const transcript = yield* CodexOrchestratorReplayHarness.decodeTranscript(
          materializeReplayTranscriptWorkspace(rawTranscript, cwd),
        );
        const layerOrchestrator = ProviderReplayHarness.layerProviderReplay(
          {
            name: "delegated-task-status/codex",
            transcript,
            commands: [],
            runtimePolicyOverride: { cwd },
          },
          CodexOrchestratorReplayHarness,
        );
        const layerOrchestration = Layer.merge(
          layerOrchestrator,
          ThreadManagementService.layer.pipe(
            Layer.provide(layerOrchestrator),
            Layer.provide(ServerSettings.layerTest()),
            Layer.provide(
              CommandReceiptStore.layer.pipe(Layer.provide(SqlitePersistence.layerMemory)),
            ),
          ),
        );
        const layerProviderRegistry = ProviderRegistryMock.layer([
          makeProviderSnapshot({
            instanceId: codexInstanceId,
            driver: ProviderDriverKind.make("codex"),
            model: codexModel,
          }),
        ]);
        const layerTest = layerOrchestratorToolkit.pipe(
          Layer.provide(PrismServerSettings.layerTest()),
          Layer.provideMerge(McpServer.McpServer.layer),
          Layer.provideMerge(layerOrchestration),
          Layer.provide(
            CodexOrchestratorReplayHarness.makeProviderAdapterRegistryLayer(transcript).pipe(
              Layer.provide(McpProviderSessions.layer),
            ),
          ),
          Layer.provide(layerProviderRegistry),
          Layer.provide(ServerSettings.layerTest()),
          Layer.provide(layerUnusedScheduledTaskStub),
          Layer.provide(Layer.mock(PeerForwarding.PeerForwarding)({})),
          Layer.provide(Layer.mock(PeerLinkRequests.PeerLinkRequests)({})),
          Layer.provide(Layer.mock(PeerLinks.PeerLinks)({})),
          Layer.provide(Layer.mock(ProjectService.ProjectService)({})),
          Layer.provideMerge(
            SecretRequests.layer.pipe(
              Layer.provide(layerMemorySecretStore),
              Layer.provide(layerOrchestration),
            ),
          ),
          Layer.provide(Layer.mock(RemoteDelegation.RemoteDelegation)({})),
          Layer.provide(NodeServices.layer),
        );

        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const server = yield* McpServer.McpServer;
          const parentCreate = yield* orchestrator.dispatch({
            type: "thread.create",
            createdBy: "user",
            creationSource: "web",
            commandId: CommandId.make("command:mcp-replay-parent:create"),
            threadId: parentThreadId,
            projectId,
            title: "MCP replay parent",
            modelSelection: codexSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: cwd,
          });
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            createdBy: "user",
            creationSource: "web",
            commandId: CommandId.make("command:mcp-replay-parent:start"),
            threadId: parentThreadId,
            messageId: MessageId.make("message:mcp-replay-parent:start"),
            text: parentPrompt,
            attachments: [],
            modelSelection: codexSelection,
            dispatchMode: { type: "start_immediately" },
          });
          yield* orchestrator
            .streamStoredEventsFrom({
              threadId: parentThreadId,
              afterSequence: parentCreate.sequence,
            })
            .pipe(
              Stream.filter(
                (stored) =>
                  stored.event.type === "provider-turn.updated" &&
                  stored.event.payload.status === "running",
              ),
              Stream.runHead,
              Effect.flatMap(
                Option.match({
                  onNone: () => Effect.die("Parent provider turn did not start."),
                  onSome: () => Effect.void,
                }),
              ),
            );

          const invocation: McpInvocationContext.McpInvocationScope = {
            environmentId: EnvironmentId.make("environment:mcp-replay"),
            requestNamespace: "mcp-provider-session-replay-parent",
            thread: {
              threadId: parentThreadId,
              providerSessionId: "mcp-provider-session-replay-parent",
              providerInstanceId: codexInstanceId,
            },
            client: undefined,
            capabilities: new Set(["orchestration"]),
            issuedAt: 1,
          };
          const invoke = (name: string, args: Record<string, unknown>) =>
            server
              .callTool({ name, arguments: args })
              .pipe(
                Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
                Effect.provideService(McpSchema.McpServerClient, client),
              );

          const delegationStartSequence =
            yield* orchestrator.getThreadEventSequence(parentThreadId);
          const delegatedCall = yield* invoke("delegate_task", {
            task: delegatedPrompt,
            target: {
              providerInstanceId: codexInstanceId,
              model: codexModel,
            },
            mode: "async",
            clientRequestId: "delegate-codex-replay-1",
          });
          const delegatedStart = yield* decodeDelegateTaskResult(
            delegatedCall.structuredContent,
          ).pipe(Effect.orDie);
          expect(delegatedStart.childRunId).not.toBeNull();
          yield* orchestrator
            .streamStoredEventsFrom({
              threadId: parentThreadId,
              afterSequence: delegationStartSequence,
            })
            .pipe(
              Stream.filter(
                (stored) =>
                  stored.event.type === "context-transfer.created" &&
                  stored.event.payload.type === "subagent_result" &&
                  stored.event.payload.sourceThreadId === delegatedStart.childThreadId &&
                  stored.event.payload.sourcePoint.runId === delegatedStart.childRunId,
              ),
              Stream.runHead,
              Effect.flatMap(
                Option.match({
                  onNone: () => Effect.die("Delegated result transfer was not created."),
                  onSome: () => Effect.void,
                }),
              ),
            );
          const delegatedStatusCall = yield* invoke("task_status", {
            taskId: delegatedStart.taskId,
          });
          const delegated = yield* decodeDelegateTaskResult(
            delegatedStatusCall.structuredContent,
          ).pipe(Effect.orDie);
          expect(delegated).toMatchObject({
            status: "completed",
            summary: delegatedResult,
            providerInstanceId: codexInstanceId,
            hasPendingChildRuns: false,
            latestTerminalRunId: delegated.childRunId,
            latestTerminalStatus: "completed",
            latestTerminalSummary: null,
            latestTerminalResultContextTransferId: null,
          });
          expect(delegated.resultContextTransferId).not.toBeNull();
          const originalTask = (yield* orchestrator.getThreadProjection(
            parentThreadId,
          )).subagents.find((task) => task.id === delegated.taskId)!;

          // Delegated children are subagent threads too, but T3 owns them, so
          // they keep taking follow-ups (provider-native children do not).
          const delegatedChild = yield* orchestrator.getThreadProjection(
            localChild(delegated.childThreadId),
          );
          expect(delegatedChild.thread.lineage.relationshipToParent).toBe("subagent");
          expect(isProviderNativeSubagentThread(delegatedChild.thread)).toBe(false);
          expect(
            delegatedChild.runs.find((run) => run.id === delegated.childRunId)?.delegatedTaskId,
          ).toBe(delegated.taskId);
          const followupStartSequence = yield* orchestrator.getThreadEventSequence(
            localChild(delegated.childThreadId),
          );
          const runningFollowupCall = yield* invoke("t3_thread_send", {
            threadId: localChild(delegated.childThreadId),
            message: cancellationPrompt,
            clientRequestId: "delegated-child-replay-running-1",
          });
          const runningFollowup = yield* decodeThreadSendResult(
            runningFollowupCall.structuredContent,
          ).pipe(Effect.orDie);
          yield* orchestrator
            .streamStoredEventsFrom({
              threadId: localChild(delegated.childThreadId),
              afterSequence: followupStartSequence,
            })
            .pipe(
              Stream.filter(
                (stored) =>
                  stored.event.type === "provider-turn.updated" &&
                  stored.event.payload.status === "running",
              ),
              Stream.runHead,
              Effect.flatMap(
                Option.match({
                  onNone: () => Effect.die("Follow-up provider turn did not start."),
                  onSome: () => Effect.void,
                }),
              ),
            );

          const queuedFollowupCall = yield* invoke("t3_thread_send", {
            threadId: localChild(delegated.childThreadId),
            message: queuedFollowupPrompt,
            mode: "queue",
            clientRequestId: "delegated-child-replay-queued-1",
          });
          const queuedFollowup = yield* decodeThreadSendResult(
            queuedFollowupCall.structuredContent,
          ).pipe(Effect.orDie);
          expect(queuedFollowup).toMatchObject({
            status: "queued",
            delivery: "queued",
          });

          const pendingProjection = yield* orchestrator.getThreadProjection(
            localChild(delegated.childThreadId),
          );
          const followupTasks = (yield* orchestrator.getThreadProjection(
            parentThreadId,
          )).subagents.filter(
            (task) =>
              task.childThreadId === delegated.childThreadId && task.id !== delegated.taskId,
          );
          // Fork: requested turns belong to the same job, so no sibling task is minted.
          expect(followupTasks).toHaveLength(0);
          expect(
            pendingProjection.runs.find((run) => run.id === runningFollowup.runId)?.delegatedTaskId,
          ).toBe(delegated.taskId);
          expect(
            pendingProjection.runs.find((run) => run.id === queuedFollowup.runId)?.delegatedTaskId,
          ).toBe(delegated.taskId);
          expect(
            pendingProjection.runs.find((run) => run.id === delegated.childRunId)?.status,
          ).toBe("completed");
          expect(
            pendingProjection.runs.find((run) => run.id === runningFollowup.runId)?.status,
          ).toBe("running");
          expect(
            pendingProjection.runs.find((run) => run.id === queuedFollowup.runId)?.status,
          ).toBe("queued");
          expect(
            (yield* orchestrator.getThreadProjection(parentThreadId)).runs.some(
              (run) => run.status === "running",
            ),
          ).toBe(true);

          const pendingStatusCall = yield* invoke("task_status", {
            taskId: delegated.taskId,
          });
          const pendingStatus = yield* decodeDelegateTaskResult(
            pendingStatusCall.structuredContent,
          ).pipe(Effect.orDie);
          expect(pendingStatus).toMatchObject({
            childRunId: delegated.childRunId,
            status: "completed",
            summary: delegatedResult,
            resultContextTransferId: delegated.resultContextTransferId,
            hasPendingChildRuns: true,
            latestTerminalRunId: delegated.childRunId,
            latestTerminalStatus: "completed",
            latestTerminalSummary: null,
            latestTerminalResultContextTransferId: null,
          });

          const finalSequence = yield* orchestrator.getThreadEventSequence(
            localChild(delegated.childThreadId),
          );
          const finalParentSequence = yield* orchestrator.getThreadEventSequence(parentThreadId);
          const interruptCall = yield* invoke("t3_thread_interrupt", {
            threadId: localChild(delegated.childThreadId),
            runId: runningFollowup.runId,
            reason: "Allow the queued replay follow-up to run.",
            clientRequestId: "interrupt-delegated-child-replay-1",
          });
          const interrupt = yield* decodeThreadInterruptResult(
            interruptCall.structuredContent,
          ).pipe(Effect.orDie);
          expect(interrupt).toMatchObject({
            runId: runningFollowup.runId,
            status: "interrupt_requested",
          });
          yield* orchestrator
            .streamStoredEventsFrom({
              threadId: localChild(delegated.childThreadId),
              afterSequence: finalSequence,
            })
            .pipe(
              Stream.filter(
                (stored) =>
                  stored.event.type === "run.updated" &&
                  stored.event.payload.id === queuedFollowup.runId &&
                  stored.event.payload.status === "completed",
              ),
              Stream.runHead,
              Effect.flatMap(
                Option.match({
                  onNone: () => Effect.die("Queued follow-up did not complete."),
                  onSome: () => Effect.void,
                }),
              ),
            );

          const finalProjection = yield* orchestrator.getThreadProjection(
            localChild(delegated.childThreadId),
          );
          expect(finalProjection.runs.find((run) => run.id === runningFollowup.runId)?.status).toBe(
            "interrupted",
          );
          expect(finalProjection.runs.find((run) => run.id === queuedFollowup.runId)?.status).toBe(
            "completed",
          );
          expect(
            finalProjection.turnItems.find(
              (item) =>
                item.runId === runningFollowup.runId && item.type === "run_interrupt_result",
            ),
          ).toMatchObject({
            message: "Stopped by an agent: Allow the queued replay follow-up to run.",
          });
          yield* orchestrator
            .streamStoredEventsFrom({
              threadId: parentThreadId,
              afterSequence: finalParentSequence,
            })
            .pipe(
              Stream.filter(
                (stored) =>
                  stored.event.type === "context-transfer.created" &&
                  stored.event.payload.type === "subagent_result" &&
                  stored.event.payload.sourceThreadId === delegated.childThreadId &&
                  stored.event.payload.sourcePoint.runId === queuedFollowup.runId,
              ),
              Stream.runHead,
              Effect.flatMap(
                Option.match({
                  onNone: () => Effect.die("Queued follow-up result did not reach the parent."),
                  onSome: () => Effect.void,
                }),
              ),
            );
          const finishedParent = yield* orchestrator.getThreadProjection(parentThreadId);
          expect(
            finishedParent.contextTransfers.some(
              (transfer) =>
                transfer.type === "subagent_result" &&
                transfer.sourceThreadId === delegated.childThreadId &&
                transfer.sourcePoint.runId === runningFollowup.runId,
            ),
          ).toBe(true);
          const finishedTask = finishedParent.subagents.find(
            (task) => task.id === delegated.taskId,
          )!;
          const {
            completionDelivery: _firstDelivery,
            completionSource: _firstCompletionSource,
            updatedAt: _firstUpdatedAt,
            ...firstResult
          } = originalTask;
          const {
            completionDelivery: _delivery,
            completionSource: _completionSource,
            updatedAt: _updatedAt,
            ...stableResult
          } = finishedTask;
          expect(stableResult).toEqual(firstResult);
          expect(finishedTask.completionSource).toEqual({
            runId: queuedFollowup.runId,
            purpose: finalProjection.runs.find((run) => run.id === queuedFollowup.runId)?.purpose,
          });
          expect(
            finishedParent.subagents.filter(
              (task) => task.childThreadId === delegated.childThreadId,
            ),
          ).toHaveLength(1);
          expect(finishedTask).toMatchObject({
            status: "completed",
            result,
          });
          expect(
            finishedParent.contextTransfers.filter(
              (transfer) =>
                transfer.type === "subagent_result" &&
                transfer.sourceThreadId === delegated.childThreadId,
            ),
          ).toHaveLength(3);
          expect(finishedTask.completionDelivery?.state).toBe("claimed");
          const finalStatusCall = yield* invoke("task_status", {
            taskId: delegated.taskId,
          });
          const finalStatus = yield* decodeDelegateTaskResult(
            finalStatusCall.structuredContent,
          ).pipe(Effect.orDie);
          expect(finalStatus).toMatchObject({
            childRunId: delegated.childRunId,
            status: "completed",
            summary: delegatedResult,
            resultContextTransferId: delegated.resultContextTransferId,
            hasPendingChildRuns: false,
            latestTerminalRunId: queuedFollowup.runId,
            latestTerminalStatus: "completed",
            latestTerminalSummary: latestSummary,
          });
          // Fork (pingdotgg/t3code#13490): the follow-up result reaches the parent as its own transfer.
          const followupTransfer = finishedParent.contextTransfers.find(
            (transfer) =>
              transfer.type === "subagent_result" &&
              transfer.sourcePoint.runId === queuedFollowup.runId,
          );
          expect(followupTransfer).toBeDefined();
          expect(finalStatus.latestTerminalResultContextTransferId).toBe(followupTransfer?.id);
          expect(finalStatus.latestTerminalResultContextTransferId).not.toBe(
            delegated.resultContextTransferId,
          );
          const read = yield* invoke("t3_thread_read", {
            threadId: localChild(delegated.childThreadId),
            limit: 100,
          });
          expect(read.isError).not.toBe(true);
          const afterRead = yield* orchestrator.getThreadProjection(parentThreadId);
          expect(
            afterRead.subagents.find((task) => task.id === delegated.taskId)?.completionDelivery
              ?.state,
          ).toBe("acknowledged");
        }).pipe(Effect.provide(layerTest));
      }),
    ),
  );
});
