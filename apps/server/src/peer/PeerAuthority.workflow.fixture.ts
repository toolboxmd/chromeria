import * as Clock from "effect/Clock";
import * as OrchestratorService from "../mcp/OrchestratorMcpService.ts";
import * as OrchestratorHandlers from "../mcp/toolkits/orchestrator/handlers.ts";
import * as ThreadMetadataService from "../mcp/ThreadMetadataMcpService.ts";
import * as RemoteSecretRequests from "../secrets/RemoteSecretRequests.ts";
import * as RemoteSecretRelay from "../secrets/RemoteSecretRelay.ts";
import { vi } from "vite-plus/test";
import * as PeerLinkEvents from "./PeerLinkEvents.ts";
import { DeviceStandardToolkit as LegacyDeviceToolkit } from "../device/testing/LegacyDeviceTools.fixture.ts";
import * as LegacyDeviceHandlers from "../device/testing/LegacyDeviceHandlers.fixture.ts";
import * as Crypto from "effect/Crypto";
import * as Hex from "effect/encoding/Hex";
import {
  RemotePendingRequestResult,
  RuntimeRequestId,
  ProviderThreadId,
  PeerAttachmentUploadResult,
  LOCAL_DEVICE_HOST_ID,
  McpSavedSnapshotReceipt,
  McpScheduledMutationReceipt,
  SecretRequestError,
  PeerAttachmentReference,
  DeviceCommandTarget,
  DeviceCommandResult,
  ProviderTurnId,
  CommandId,
  OrchestratorMcpFailure,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  RunId,
  ThreadId,
  type Project,
  type OrchestrationV2LinkOrigin,
  type ExecutionEnvironmentDescriptor,
  TurnItemId,
  EnvironmentHttpApi,
  DeviceViewerAccess,
  ORCHESTRATION_V2_WS_METHODS,
  PreviewStreamInput,
  WS_METHODS,
  SourceControlProviderError,
  SourceControlProviderKind,
} from "@t3tools/contracts";
import * as PendingRequests from "../childThreads/PendingRequestService.ts";
import * as AttachmentStaging from "../assets/AttachmentStaging.ts";
import { type McpThreadInvocationScope } from "../mcp/McpInvocationContext.ts";
import * as Invocation from "../mcp/McpInvocationContext.ts";
import * as Queue from "effect/Queue";
import * as Path from "effect/Path";
import * as NativeDevices from "../device/DeviceService.ts";
import * as DeviceHost from "../device/DeviceHost.ts";
import { makeDeviceHubOperations } from "../device/DeviceHubOperations.ts";
import { agentDeviceConfigPath } from "../device/AgentDeviceTarget.ts";
import { AGENT_DEVICE_VERSION, DEVICE_HUB_VERSION } from "../device/DeviceToolchain.ts";
import * as Net from "@t3tools/shared/Net";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import * as PullRequestReviewGuideService from "../pullRequest/PullRequestReviewGuideService.ts";
import { IssuesToolkit } from "../mcp/toolkits/issues/tools.ts";
import * as IssueHandlers from "../mcp/toolkits/issues/handlers.ts";
import * as IssueLinks from "../issueLinks/IssueLinks.ts";
import * as ClosingReferences from "../issueLinks/closingReferences.ts";
import * as WorktreeMcp from "../mcp/WorktreeMcpService.ts";
import { WorktreeToolkit } from "../mcp/toolkits/worktree/tools.ts";
import * as WorktreeHandlers from "../mcp/toolkits/worktree/handlers.ts";
import * as VcsStatus from "../vcs/VcsStatusBroadcaster.ts";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as ScheduledChecks from "../scheduledTaskChecks/ScheduledTaskChecks.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as SpectrumLaunch from "../spectrum/LaunchService.ts";
import * as SpectrumMcp from "../spectrum/mcpToolkit.ts";
import * as SpectrumStore from "../spectrum/store.ts";
import * as AutomationAuthority from "../scheduledTasks/DestinationAutomationAuthority.ts";
import * as CheckStore from "../scheduledTaskChecks/store.ts";
import * as Prism from "../prism/PrismService.ts";
import * as HtmlRender from "../htmlRender/HtmlRender.ts";
import * as PreviewBrowser from "../preview/PreviewBrowser.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import * as ProjectEnrichment from "../project/ProjectEnrichmentService.ts";
import * as ProjectFavicon from "../project/ProjectFaviconResolver.ts";
import * as ProjectFaviconResolver from "../project/ProjectFaviconResolver.ts";
import * as GitManager from "../git/GitManager.ts";
import * as VcsRegistry from "../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as ThreadSearch from "../orchestration-v2/ThreadSearch.ts";
import { ThreadToolkit } from "../mcp/toolkits/thread/tools.ts";
import * as RequestReceipts from "../mcp/McpRequestReceipts.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as SourceProviders from "../sourceControl/SourceControlProviderRegistry.ts";
import * as ProductionRuntime from "../orchestration-v2/runtimeLayer.ts";
import * as RequestRuntime from "../mcp/McpRequestRuntime.ts";
import * as AttachmentHandlers from "../mcp/toolkits/attachment/handlers.ts";
import { AttachmentToolkit } from "../mcp/toolkits/attachment/tools.ts";
import * as ServerHttp from "../http.ts";
import * as FileSystem from "effect/FileSystem";
import * as PlatformError from "effect/PlatformError";
import * as BrowserBroker from "../mcp/PreviewAutomationBroker.ts";
import * as Instances from "../provider/ProviderInstanceRegistry.ts";
import { type ProviderInstance } from "@t3tools/provider-core/server/driver";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as Checkpoints from "../checkpointing/CheckpointStore.ts";
import * as ProviderCatalog from "../provider/ProviderRegistry.ts";
import * as RecoveryStore from "../prism/RecoveryStore.ts";
import * as PeerTaskLifecycle from "./PeerTaskLifecycle.ts";
import { RecoveryWaitState } from "./recoveryWait.ts";
import * as RecoveryHistory from "../prism/RecoveryHistory.ts";
import * as RecoverySignals from "./recoveryWaitSignals.ts";
import * as ProviderSessionManager from "../orchestration-v2/ProviderSessionManager.ts";
import * as Deferred from "effect/Deferred";
import * as Ref from "effect/Ref";
import * as EffectWorker from "../orchestration-v2/EffectWorker.ts";
import * as EffectOutbox from "../orchestration-v2/EffectOutbox.ts";
import * as McpSessionRegistry from "../mcp/McpSessionRegistry.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import * as ScheduledTasks from "../scheduledTasks/ScheduledTaskService.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as HttpBody from "effect/http/HttpBody";
import * as Schema from "effect/Schema";

import { expect } from "@effect/vitest";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as SqlClient from "effect/sql/SqlClient";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import type * as Adapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as PeerAuthority from "./PeerAuthority.ts";
import * as PeerNoticeDelivery from "./PeerNoticeDelivery.ts";
import * as PeerForwarding from "./PeerForwarding.ts";
import * as PeerMcpClient from "./PeerMcpClient.ts";
import * as PeerLinks from "./PeerLinks.ts";
import * as RemoteDelegation from "./RemoteDelegation.ts";
import { descriptorOf, layerLaunchCatalog, linkTo, servePeer } from "./PeerLinks.testkit.ts";
import * as Config from "../config.ts";
import * as Environment from "../environment/ServerEnvironment.ts";
import * as Secrets from "../auth/ServerSecretStore.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as Runtime from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as Adapters from "../orchestration-v2/ProviderAdapterRegistry.ts";
import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as Projects from "../project/ProjectService.ts";
import * as RepositoryIdentity from "../project/RepositoryIdentityResolver.ts";
import * as BuiltInDrivers from "../sourceControl/builtInDrivers.ts";
import * as McpHttpServer from "../mcp/McpHttpServer.ts";
import { ProjectToolkit } from "../mcp/toolkits/project/tools.ts";
import { OrchestratorToolkit } from "../mcp/toolkits/orchestrator/tools.ts";
import * as ThreadLaunch from "../orchestration-v2/ThreadLaunchService.ts";
import * as Receipts from "../orchestration-v2/CommandReceiptStore.ts";
import * as WorktreeSetup from "../project/WorktreeSetupTracker.ts";
import * as ProjectClone from "../project/ProjectCloneTracker.ts";
import * as Terminals from "../terminal/Manager.ts";
import * as GitWorkflow from "../git/GitWorkflowService.ts";
import * as TextGeneration from "../textGeneration/TextGeneration.ts";
import * as SetupScripts from "../project/ProjectSetupScriptRunner.ts";
import * as Folders from "../project/ManagedProjectFolders.ts";
import * as Settings from "../serverSettings.ts";
import * as BareLaunch from "../orchestration-v2/BareLaunch.ts";
import * as Git from "../vcs/GitVcsDriver.ts";
import * as Repositories from "../sourceControl/SourceControlRepositoryService.ts";
import * as Cause from "effect/Cause";

// @effect-diagnostics nodeBuiltinImport:off
// The external WebSocket fixture requires an actual Node HTTP server.
import * as NodeHttp from "node:http";

import * as ResourceWebSocketServer from "../http/ResourceWebSocketServer.ts";
import { guardHttpResponseWriteErrors } from "../httpResponseErrorGuard.ts";

import { NodeWS } from "@effect/platform-node/NodeSocket";
import * as DeviceHubProxy from "../device/DeviceHubProxy.ts";
import * as AuthHttp from "../auth/http.ts";

import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";

import * as RpcAuthorization from "../auth/RpcAuthorization.ts";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServer from "effect/http/HttpServer";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import { RpcGroup, RpcServer, RpcSerialization } from "effect/rpc";
import { CoreServerWsRpcGroup, OrchestrationServerWsRpcGroup, ServerWsRpcGroup } from "../ws.ts";
import * as ForkWsRpc from "../forkWsRpc.ts";
import { rpcInstrumentationLayer } from "../observability/RpcInstrumentation.ts";
import * as CommandOutputHub from "../orchestration-v2/CommandOutputHub.ts";

import * as AuthSessions from "../persistence/AuthSessions.ts";
import * as ViewerSessions from "../auth/SessionStore.ts";
import * as AssetAccess from "../assets/AssetAccess.ts";
import * as NativeAppIconResolver from "../assets/NativeAppIconResolver.ts";
import * as T3ProjectFileLoader from "../project/T3ProjectFileLoader.ts";
import * as GitHubCredentials from "@t3tools/source-control-github/server/GitHubCredentials";

import { PreviewControlsToolkit } from "../mcp/toolkits/previewControls/tools.ts";
import * as PreviewControlHandlers from "../mcp/toolkits/previewControls/handlers.ts";
import * as NativeBrowser from "../preview/ServerBrowser.ts";
import * as CredentialVault from "../preview/CredentialVault.ts";
import * as NativePreviewManager from "../preview/Manager.ts";
import * as NativeDesktopChannel from "../preview/DesktopBrowserChannel.ts";
import { makeExternalBrowserContext } from "../preview/testing/BrowserExternal.fixture.ts";

import * as NodeHttpPlatform from "@effect/platform-node/NodeHttpPlatform";
import * as BrowserStream from "../preview/ServerBrowserStream.ts";

// Keep production group access behind this fixture so native mocks initialize first.
export const native383RpcGroups = {
  CoreServerWsRpcGroup,
  OrchestrationServerWsRpcGroup,
  ServerWsRpcGroup,
  ForkWsRpc,
};

export const instanceId = ProviderInstanceId.make("codex");

export const driver = ProviderDriverKind.make("codex");

export const selection = { instanceId, model: "gpt-5.4" };

export const decodeWorktreeBinding = Schema.decodeUnknownEffect(
  Schema.Struct({ worktreePath: Schema.String }),
);

export const decodeSavedPngReceipt = Schema.decodeUnknownEffect(McpSavedSnapshotReceipt);

export const decodeScheduledTaskList = Schema.decodeUnknownEffect(
  OrchestratorToolkit.tools.list_scheduled_tasks.successSchema,
);

export const adapter = {
  instanceId,
  driver,
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("The isolated workflow never starts a provider process"),
} as Adapter.ProviderAdapterV2["Service"];

const decodeFixturePeerSession = Schema.decodeUnknownSync(
  Schema.fromJsonString(PeerAuthority.PeerSession),
);

const decodeFixtureControl = Schema.decodeUnknownSync(
  Schema.fromJsonString(PeerAuthority.Exchange),
);

const decodeFixtureCaptureGraph = Schema.decodeUnknownSync(AutomationAuthority.AcceptedGraph);

export const decodeFixtureDisable = Schema.decodeUnknownSync(
  Schema.fromJsonString(AutomationAuthority.AutomationDisable),
);

export const decodeFixtureDependency = Schema.decodeUnknownSync(
  Schema.fromJsonString(AutomationAuthority.AutomationDependency),
);

export const decodeManualRun = Schema.decodeUnknownEffect(
  ThreadToolkit.tools.run_scheduled_task_now.successSchema,
);

export const decodeAutomationScheduleReceipt = Schema.decodeUnknownEffect(
  McpScheduledMutationReceipt,
);

export const decodeAutomationSpectrumResult = Schema.decodeUnknownEffect(
  SpectrumMcp.StartSpectrumResult,
);

export const decodeAutomationAck = Schema.decodeUnknownEffect(
  Schema.fromJsonString(AutomationAuthority.AutomationDisableAck),
);

export const decodeAutomationCertificate = Schema.decodeUnknownEffect(
  Schema.fromJsonString(AutomationAuthority.AutomationCertificate),
);

export const automationAdapter = (
  started: Array<Adapter.ProviderAdapterV2TurnInput>,
  opened: Array<Adapter.ProviderAdapterV2OpenSessionInput>,
  onStart?: (
    turn: Adapter.ProviderAdapterV2TurnInput,
    events: Queue.Queue<Adapter.ProviderAdapterV2Event>,
  ) => Effect.Effect<void>,
  onSteer?: () => void,
): Adapter.ProviderAdapterV2["Service"] => ({
  instanceId,
  driver,
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: Effect.fn("PeerAuthority.openSession")(function* (input) {
    opened.push(input);
    const events = yield* Queue.unbounded<Adapter.ProviderAdapterV2Event>();
    const now = yield* DateTime.now;
    const providerSession = {
      id: input.providerSessionId,
      driver,
      providerInstanceId: instanceId,
      status: "ready" as const,
      cwd: input.runtimePolicy.cwd ?? process.cwd(),
      model: selection.model,
      capabilities: CodexProviderCapabilitiesV2,
      createdAt: now,
      updatedAt: now,
      lastError: null,
    };
    return {
      instanceId,
      driver,
      providerSessionId: input.providerSessionId,
      providerSession,
      events: Stream.fromQueue(events),
      ensureThread: (thread) =>
        Effect.succeed({
          id: ProviderThreadId.make(`external-384:${thread.threadId}`),
          driver,
          providerInstanceId: instanceId,
          providerSessionId: input.providerSessionId,
          appThreadId: thread.threadId,
          ownerNodeId: null,
          nativeThreadRef: {
            driver,
            nativeId: `external-384:${thread.threadId}`,
            strength: "strong" as const,
          },
          nativeConversationHeadRef: null,
          status: "idle" as const,
          firstRunOrdinal: null,
          lastRunOrdinal: null,
          handoffIds: [],
          forkedFrom: null,
          createdAt: now,
          updatedAt: now,
        }),
      resumeThread: (thread) => Effect.succeed(thread.providerThread),
      startTurn: Effect.fn("PeerAuthority.startTurn")(function* (turn) {
        started.push(turn);
        const now = yield* DateTime.now;
        yield* Queue.offer(events, {
          type: "provider_turn.updated",
          driver,
          threadId: turn.threadId,
          providerTurn: {
            id: ProviderTurnId.make(`external-384:${turn.runId}`),
            providerThreadId: turn.providerThread.id,
            nodeId: turn.rootNodeId,
            runAttemptId: turn.attemptId,
            nativeTurnRef: {
              driver,
              nativeId: `external-384:${turn.runId}`,
              strength: "strong",
            },
            ordinal: turn.providerTurnOrdinal,
            status: "running",
            startedAt: now,
            completedAt: null,
          },
        });
        if (onStart !== undefined) yield* onStart(turn, events);
      }),
      steerTurn: () =>
        Effect.sync(() => {
          onSteer?.();
        }),
      interruptTurn: () => Effect.void,
      respondToRuntimeRequest: () => Effect.void,
      readThreadSnapshot: () => Effect.die("No external snapshot in this proof"),
      rollbackThread: () => Effect.die("No external rollback in this proof"),
      forkThread: () => Effect.die("No external fork in this proof"),
    } satisfies Adapter.ProviderAdapterV2SessionRuntime;
  }),
});

export const automationReply = Effect.fn("PeerAuthority.automationReply")(function* (
  turn: Adapter.ProviderAdapterV2TurnInput,
  events: Queue.Queue<Adapter.ProviderAdapterV2Event>,
  text: string,
) {
  const now = yield* DateTime.now;
  const providerTurnId = ProviderTurnId.make(`external-384:${turn.runId}`);
  yield* Queue.offerAll(events, [
    {
      type: "provider_turn.updated",
      driver,
      threadId: turn.threadId,
      providerTurn: {
        id: providerTurnId,
        providerThreadId: turn.providerThread.id,
        nodeId: turn.rootNodeId,
        runAttemptId: turn.attemptId,
        nativeTurnRef: { driver, nativeId: `external-384:${turn.runId}`, strength: "strong" },
        ordinal: turn.providerTurnOrdinal,
        status: "completed",
        startedAt: now,
        completedAt: now,
      },
    },
    {
      type: "message.updated",
      driver,
      message: {
        id: MessageId.make(`external-384:${turn.runId}:answer`),
        threadId: turn.threadId,
        runId: turn.runId,
        nodeId: turn.rootNodeId,
        createdBy: "agent",
        creationSource: "provider",
        role: "assistant",
        text,
        attachments: [],
        streaming: false,
        createdAt: now,
        updatedAt: now,
      },
    },
    {
      type: "turn_item.updated",
      driver,
      turnItem: {
        id: TurnItemId.make(`external-384:${turn.runId}:answer`),
        threadId: turn.threadId,
        runId: turn.runId,
        nodeId: turn.rootNodeId,
        providerThreadId: turn.providerThread.id,
        providerTurnId,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: turn.runOrdinal * 100 + 1,
        status: "completed",
        title: null,
        startedAt: now,
        completedAt: now,
        updatedAt: now,
        type: "assistant_message",
        messageId: MessageId.make(`external-384:${turn.runId}:answer`),
        text,
        streaming: false,
      },
    },
    {
      type: "turn.terminal",
      driver,
      providerThreadId: turn.providerThread.id,
      providerTurnId,
      runOrdinal: turn.runOrdinal,
      status: "completed",
      failure: null,
      threadDisposition: "reusable",
    },
  ]);
});

const graphConnections = new WeakMap<Scope.Scope, Set<Scope.Closeable>>();

const layerFixtureHostServices = Layer.merge(
  McpProviderSessions.layer,
  // Fixture repositories have no forge; pull request reads are mocked per test.
  Layer.succeed(BuiltInDrivers.SourceControlDrivers, []),
);

export const makeNode = Effect.fn("PeerAuthority.makeNode")(
  function* (
    descriptor: ExecutionEnvironmentDescriptor,
    storage: "memory" | "disk" = "memory",
    {
      productionKernel = false,
      attachmentRequests = false,
      remoteSecrets = false,
      preferencesOwner = false,
      nativeOwners = false,
      deviceOwner = false,
      viewerOwner = false,
      browserOwner = false,
      previewControlsOwner = false,
      viewerSocketOwner = false,
      legacyDeviceDomain = false,
      legacyAutomationPeer = false,
      captureReplyTransform,
      captureReplyGate,
      htmlImageReadGate,
      commandSpawner,
      ownerWorkspace,
      providerAdapter = adapter,
      manualScheduler,
    }: {
      readonly productionKernel?: boolean;
      readonly attachmentRequests?: boolean;
      readonly remoteSecrets?: boolean;
      readonly preferencesOwner?: boolean;
      readonly nativeOwners?: boolean;
      readonly deviceOwner?: boolean;
      readonly viewerOwner?: boolean;
      readonly browserOwner?: boolean;
      readonly previewControlsOwner?: boolean;
      readonly viewerSocketOwner?: boolean;
      readonly legacyDeviceDomain?: boolean;
      readonly legacyAutomationPeer?: boolean;
      readonly captureReplyGate?: Effect.Effect<void>;
      readonly htmlImageReadGate?: Effect.Effect<void>;
      readonly captureReplyTransform?: (
        graph: AutomationAuthority.AcceptedGraph,
      ) => AutomationAuthority.AcceptedGraph;
      readonly commandSpawner?: ChildProcessSpawner.ChildProcessSpawner["Service"];
      readonly ownerWorkspace?: string;
      readonly providerAdapter?: Adapter.ProviderAdapterV2["Service"];
      readonly manualScheduler?: Scheduler.Scheduler["Service"];
    } = {},
  ) {
    // Closures rebuilt later reuse this node's instances; provider sessions keep per-thread state.
    // Pick only these two: the ambient context also holds an HttpClient that would shadow peerHttp.
    const hostServices = Layer.succeedContext(
      Context.pick(
        McpProviderSessions.McpProviderSessions,
        BuiltInDrivers.SourceControlDrivers,
      )(
        yield* Effect.context<
          McpProviderSessions.McpProviderSessions | BuiltInDrivers.SourceControlDrivers
        >(),
      ),
    );
    const graphScope = yield* Scope.Scope;
    const connectionScopes = graphConnections.get(graphScope) ?? new Set<Scope.Closeable>();
    graphConnections.set(graphScope, connectionScopes);
    const databaseConfig = yield* Config.layerTest(process.cwd(), {
      prefix: `peer-authority-storage-${descriptor.label}-`,
    }).pipe(Layer.fresh, Layer.build);
    const databaseLayer =
      storage === "disk"
        ? Sqlite.layerConfig.pipe(Layer.provide(Layer.succeedContext(databaseConfig)))
        : Sqlite.layerMemory;
    const database = yield* databaseLayer.pipe(Layer.fresh, Layer.build);
    const reopenDatabase = databaseLayer.pipe(Layer.fresh, Layer.build);
    const db = Layer.succeedContext(database);
    yield* RecoveryStore.layer.pipe(Layer.provide(db), Layer.build);
    const signalContext = yield* RecoverySignals.layerSignals.pipe(Layer.fresh, Layer.build);
    const signals = Context.get(signalContext, RecoverySignals.RecoveryWaitSignals);
    const lifecycleRecovery = {
      subscribe: signals.subscribe,
      read: (_threadId: ThreadId, runId: RunId) =>
        RecoveryHistory.readRecoveryState(runId).pipe(Effect.provide(db)),
    };
    const project: Project = {
      id: ProjectId.make(`project:${descriptor.environmentId}`),
      title: descriptor.label,
      workspaceRoot: ownerWorkspace ?? process.cwd(),
      repositoryIdentity: null,
      defaultModelSelection: selection,
      scripts: [],
      createdAt: "2026-10-16T00:00:00Z",
      updatedAt: "2026-10-16T00:00:00Z",
      deletedAt: null,
    };
    const environment = Layer.succeed(Environment.ServerEnvironment, {
      getEnvironmentId: Effect.succeed(descriptor.environmentId),
      getDescriptor: Effect.succeed(descriptor),
    });
    const nodeConfigContext = yield* Config.layerTest(process.cwd(), {
      prefix: `peer-authority-${descriptor.label}-`,
    }).pipe(Layer.fresh, Layer.build);
    const nodeConfig = Layer.succeedContext(nodeConfigContext);
    const settingsWrites = yield* Ref.make(0);
    const loseSettingsConfirmation = yield* Ref.make(false);
    const actualFs = yield* FileSystem.FileSystem;
    const settingsPath = Context.get(nodeConfigContext, Config.ServerConfig).settingsPath;
    const settingsFs = {
      ...actualFs,
      rename: (from: string, to: string) =>
        actualFs.rename(from, to).pipe(
          Effect.tap(() =>
            Effect.gen(function* () {
              if (to !== settingsPath) return;
              yield* Ref.update(settingsWrites, (n) => n + 1);
              if (yield* Ref.getAndSet(loseSettingsConfirmation, false))
                return yield* PlatformError.systemError({
                  _tag: "Unknown",
                  module: "FileSystem",
                  method: "rename",
                  description: "Isolated confirmation loss after settings replacement",
                });
            }),
          ),
        ),
    };
    const realSettingsLayer = Settings.layer.pipe(
      Layer.provide(Secrets.layer.pipe(Layer.provide(nodeConfig))),
      Layer.provide(db),
      Layer.provide(nodeConfig),
      Layer.provide(Layer.succeed(FileSystem.FileSystem, settingsFs)),
    );
    const settingsContext = preferencesOwner
      ? yield* realSettingsLayer.pipe(Layer.fresh, Layer.build)
      : yield* Settings.layerTest({
          providerInstances: { [instanceId]: { driver, enabled: true } },
          enableAgentBrowserAccess: true,
          enableAgentDeviceAccess: true,
        }).pipe(Layer.fresh, Layer.build);
    const settings = Layer.succeedContext(settingsContext);
    const enrichment = ProjectEnrichment.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          RepositoryIdentity.layer,
          Layer.succeed(ProjectFavicon.ProjectFaviconResolver, {
            resolvePath: () => Effect.succeed(null),
            invalidate: () => Effect.void,
          }),
        ),
      ),
    );
    const projectLayer = nativeOwners
      ? ProductionRuntime.layerProjectService.pipe(
          Layer.provide(enrichment),
          Layer.provide(WorkspacePaths.layer),
          Layer.provide(nodeConfig),
          Layer.provide(settings),
          Layer.provide(db),
        )
      : Layer.mock(Projects.ProjectService)({
          getById: () => Effect.succeed(Option.some(project)),
          listPage: () => Effect.succeed({ projects: [project], nextCursor: null }),
        });
    const actualGit = Git.layer.pipe(Layer.provide(VcsProcess.layer));
    const namedGitWorkflow = GitWorkflow.layer.pipe(
      Layer.provide(VcsRegistry.layer.pipe(Layer.provide(VcsProcess.layer))),
      // Named folders only use repository detection; no GitManager method runs.
      Layer.provide(Layer.mock(GitManager.GitManager)({})),
      Layer.provide(actualGit),
    );
    const automationInfrastructure = (database: Context.Context<SqlClient.SqlClient>) =>
      PeerAuthority.layer.pipe(
        Layer.provideMerge(PeerLinks.layer),
        Layer.provideMerge(PeerLinkEvents.layer),
        Layer.provideMerge(Secrets.layer),
        Layer.provide(ProjectionStore.layer),
        Layer.provide(settings),
        Layer.provide(Layer.succeedContext(database)),
        Layer.provide(environment),
        Layer.provide(FetchHttpClient.layer),
        Layer.provide(nodeConfig),
        Layer.fresh,
      );
    const authorityInfrastructure = yield* automationInfrastructure(database).pipe(Layer.build);
    const reconstructAutomationWith = Effect.fn("PeerAuthority.reconstructAutomationWith")(
      function* (httpLayer: Layer.Layer<HttpClient.HttpClient>) {
        const reopened = yield* reopenDatabase;
        const infrastructure = yield* automationInfrastructure(reopened).pipe(Layer.build);
        const client = yield* PeerMcpClient.layer.pipe(
          Layer.provide(Layer.succeedContext(reopened)),
          Layer.provide(Layer.succeedContext(infrastructure)),
          Layer.provide(environment),
          Layer.provide(httpLayer),
          Layer.provide(nodeConfig),
          Layer.fresh,
          Layer.build,
        );
        return {
          authority: Context.get(infrastructure, PeerAuthority.PeerAuthority),
          links: Context.get(infrastructure, PeerLinks.PeerLinks),
          peers: Context.get(client, PeerMcpClient.PeerMcpClient),
          linkEvents: Context.get(infrastructure, PeerLinkEvents.PeerLinkEvents),
          sql: Context.get(reopened, SqlClient.SqlClient),
        };
      },
    );
    const sourceHttp = HttpServer.layerTestClient.pipe(
      Layer.provide(
        FetchHttpClient.layer.pipe(
          Layer.provide(Layer.succeed(FetchHttpClient.RequestInit)({ keepalive: false })),
        ),
      ),
      Layer.provideMerge(
        ResourceWebSocketServer.layer(() => guardHttpResponseWriteErrors(NodeHttp.createServer()), {
          host: "127.0.0.1",
          port: 0,
          gracefulShutdownTimeout: 0,
          websocket: { perMessageDeflate: true },
        }),
      ),
    );
    const serverContext = yield* sourceHttp.pipe(Layer.fresh, Layer.build);
    const registry = yield* McpSessionRegistry.__testing
      .make()
      .pipe(
        Effect.provide(
          Layer.mergeAll(
            Layer.succeedContext(authorityInfrastructure),
            environment,
            Layer.succeedContext(serverContext),
          ),
        ),
      );
    const testInstance = {
      instanceId,
      driverKind: driver,
      displayName: "Deterministic native boundary",
      enabled: true,
      continuationIdentity: { driverKind: driver, continuationKey: "production-binding" },
      snapshot: { getSnapshot: Effect.succeed({}) } as unknown as ProviderInstance["snapshot"],
      textGeneration: {} as ProviderInstance["textGeneration"],
      orchestrationAdapter: providerAdapter,
    } satisfies ProviderInstance;
    // This branch builds the actual kernel exported by layerProduction, without
    // supplying an outbox/store to the transport. Only external adapters are replaced.
    const production = Layer.mergeAll(
      ProductionRuntime.layer,
      ProductionRuntime.layerEventSink,
      ProjectionStore.layer,
    ).pipe(
      Layer.provide(
        Layer.succeed(Instances.ProviderInstanceRegistry, {
          getInstance: (id) => Effect.succeed(id === instanceId ? testInstance : undefined),
          listInstances: Effect.succeed([testInstance]),
          listUnavailable: Effect.succeed([]),
          streamChanges: Stream.empty,
          subscribeChanges: Effect.never,
        }),
      ),
      Layer.provide(Layer.succeed(McpSessionRegistry.McpSessionRegistry, registry)),
      Layer.provide(Layer.succeedContext(authorityInfrastructure)),
      Layer.provide(McpProviderSessions.layer),
      Layer.provide(
        ownerWorkspace === undefined
          ? Layer.mock(Checkpoints.CheckpointStore)({})
          : Checkpoints.layer.pipe(
              Layer.provide(VcsRegistry.layer.pipe(Layer.provide(VcsProcess.layer))),
            ),
      ),
      Layer.provide(Layer.mock(ProviderCatalog.ProviderRegistry)({})),
      Layer.provide(Layer.mock(GitWorkflow.GitWorkflowService)({})),
      Layer.provide(
        Layer.mock(SourceProviders.SourceControlProviderRegistry)({
          resolveLink: () => Effect.die("No external repository host in this fixture"),
          // Upstream git workflows look up a forge and tolerate none.
          resolve: ({ cwd }) =>
            Effect.fail(
              new SourceControlProviderError({
                provider: SourceControlProviderKind.make("unknown"),
                operation: "resolve",
                cwd,
                detail: "No external repository host in this fixture.",
              }),
            ),
        }),
      ),
      Layer.provide(projectLayer),
      Layer.provide(RepositoryIdentity.layer),
      Layer.provide(nodeConfig),
      Layer.provide(settings),
      Layer.provide(db),
    );
    // Both kernels retain their full context; consumers require this shared native surface.
    const runtime: Context.Context<
      | EffectOutbox.EffectOutboxV2
      | EventSink.EventSinkV2
      | EffectWorker.OrchestrationEffectWorkerV2
      | Orchestrator.OrchestratorV2
      | ProjectionStore.ProjectionStoreV2
      | ProviderSessionManager.ProviderSessionManagerV2
      | Threads.ThreadManagementService
    > = yield* productionKernel
      ? production.pipe(Layer.fresh, Layer.build)
      : Runtime.layerWithRegistry(
          { name: `peer-authority-${descriptor.label}` },
          Adapters.layerFromAdapters([providerAdapter]),
          { databaseLayer: db, runEffectWorker: false },
        ).pipe(Layer.fresh, Layer.build);
    const commandOutput = Context.getOption(runtime, CommandOutputHub.CommandOutputHub);
    if (productionKernel)
      yield* Effect.flatMap(ProjectStore.ProjectStoreV2, (projects) =>
        projects.apply({
          sequence: 0,
          eventId: EventId.make(`seed:${project.id}`),
          aggregateKind: "project",
          aggregateId: project.id,
          occurredAt: project.createdAt,
          commandId: null,
          causationEventId: null,
          correlationId: null,
          metadata: {},
          type: "project.created",
          payload: {
            projectId: project.id,
            title: project.title,
            workspaceRoot: project.workspaceRoot,
            defaultModelSelection: selection,
            scripts: [],
            createdAt: project.createdAt,
            updatedAt: project.updatedAt,
          },
        }),
      ).pipe(Effect.provide(ProjectStore.layer.pipe(Layer.provide(db))));
    const folders = nativeOwners
      ? Folders.layer.pipe(
          Layer.provide(projectLayer),
          Layer.provide(Receipts.layer),
          Layer.provide(namedGitWorkflow),
          Layer.provide(actualGit),
          Layer.provide(WorkspacePaths.layer),
          Layer.provide(nodeConfig),
          Layer.provide(settings),
          Layer.provide(db),
        )
      : Layer.mock(Folders.ManagedProjectFolders)({
          namedProjectsRoot: "/unused",
          folderForThread: () => Effect.succeed(Option.none()),
          isScratchProject: () => Effect.succeed(false),
        });
    const noticeFailure = yield* Deferred.make<unknown>();
    const noticeDelivery = Layer.effect(
      PeerNoticeDelivery.PeerNoticeDelivery,
      Effect.gen(function* () {
        const delivery = yield* PeerNoticeDelivery.PeerNoticeDelivery;
        return {
          deliver: (...args: Parameters<typeof delivery.deliver>) =>
            delivery
              .deliver(...args)
              .pipe(Effect.tapError((error) => Deferred.succeed(noticeFailure, error))),
        };
      }),
    ).pipe(Layer.provide(PeerNoticeDelivery.layer));
    const baseServices = yield* Layer.mergeAll(
      noticeDelivery,
      Layer.succeedContext(authorityInfrastructure),
      RepositoryIdentity.layer,
    ).pipe(
      Layer.provide(Layer.succeedContext(runtime)),
      Layer.provide(db),
      Layer.provide(environment),
      Layer.provide(FetchHttpClient.layer),
      Layer.provide(nodeConfig),
      Layer.fresh,
      Layer.build,
    );
    const requestContext =
      attachmentRequests || remoteSecrets
        ? yield* RequestRuntime.layer.pipe(
            Layer.provide(db),
            Layer.provide(nodeConfig),
            Layer.provide(Layer.succeedContext(baseServices)),
            Layer.build,
          )
        : Context.empty();
    const holdLaunchReply = yield* Ref.make(false);
    const dropLaunchResponses = yield* Ref.make(0);
    const dropCommittedToolReply = yield* Ref.make<string | undefined>(undefined);
    const launchReplyHeld = yield* Deferred.make<void>();
    const releaseLaunchReply = yield* Deferred.make<void>();
    const dropAckResponses = yield* Ref.make(0);
    const ackResponseLost = yield* Deferred.make<void>();
    const holdResultPage = yield* Ref.make(false);
    /** Tasks whose waiting observations hang, as a destination with nothing new does. */
    const quietWaits = yield* Ref.make<ReadonlySet<string>>(new Set());
    /** Each task whose waiting observation started to hang. */
    const quietWaitsHeld = yield* Queue.unbounded<string>();
    const resultPageStarted = yield* Deferred.make<void>();
    const releaseResultPage = yield* Deferred.make<void>();
    const faultablePeers = Layer.effect(
      PeerMcpClient.PeerMcpClient,
      Effect.gen(function* () {
        const peers = yield* PeerMcpClient.PeerMcpClient;
        const call: typeof peers.call = (input) =>
          peers.call(input).pipe(
            Effect.tap(() =>
              Effect.gen(function* () {
                if ((yield* Ref.get(dropCommittedToolReply)) === input.tool) {
                  yield* Ref.set(dropCommittedToolReply, undefined);
                  return yield* new OrchestratorMcpFailure({
                    code: "orchestration_error",
                    message: "Isolated committed tool reply loss",
                  });
                }
                if (
                  input.tool === "t3_thread_launch" &&
                  (yield* Ref.get(dropLaunchResponses)) > 0
                ) {
                  yield* Ref.update(dropLaunchResponses, (count) => count - 1);
                  return yield* new OrchestratorMcpFailure({
                    code: "orchestration_error",
                    message: "Fixture lost accepted launch reply.",
                  });
                }
                if (input.tool === "t3_thread_launch" && (yield* Ref.get(holdLaunchReply))) {
                  yield* Deferred.succeed(launchReplyHeld, undefined);
                  yield* Deferred.await(releaseLaunchReply);
                }
              }),
            ),
          );
        return {
          ...peers,
          call,
          taskLifecycle: Effect.fn("PeerAuthorityFixture.taskLifecycle")(function* (
            ...args: Parameters<NonNullable<typeof peers.taskLifecycle>>
          ) {
            const operation = args[2].operation;
            if (
              (operation.type === "observe" || operation.type === "snapshot") &&
              operation.wait === true &&
              (yield* Ref.get(quietWaits)).has(args[2].pair.taskId)
            ) {
              yield* Queue.offer(quietWaitsHeld, args[2].pair.taskId);
              return yield* Effect.never;
            }
            const result = yield* peers.taskLifecycle!(...args);
            if (args[2].operation.type === "resultText" && (yield* Ref.get(holdResultPage))) {
              yield* Deferred.succeed(resultPageStarted, undefined);
              yield* Deferred.await(releaseResultPage);
            }
            if (
              (args[2].operation.type === "acknowledge" || args[2].operation.type === "stop") &&
              (yield* Ref.get(dropAckResponses)) > 0
            ) {
              yield* Ref.update(dropAckResponses, (count) => count - 1);
              yield* Deferred.succeed(ackResponseLost, undefined);
              return yield* new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "Fixture dropped the acknowledgement response after destination commit.",
              });
            }
            return result;
          }),
        };
      }),
    ).pipe(Layer.provide(PeerMcpClient.layer));
    const secretControlReplyGate = yield* Ref.make<
      (ids: ReadonlyArray<string>) => Effect.Effect<void>
    >(() => Effect.void);
    const peerHttp =
      remoteSecrets ||
      legacyAutomationPeer ||
      captureReplyTransform !== undefined ||
      captureReplyGate !== undefined
        ? Layer.effect(
            HttpClient.HttpClient,
            Effect.map(HttpClient.HttpClient, (client) =>
              HttpClient.transform(
                HttpClient.mapRequest(client, (request) => {
                  if (
                    captureReplyTransform !== undefined &&
                    request.url.endsWith("/mcp/peer-control") &&
                    request.body._tag === "Uint8Array"
                  ) {
                    const control = decodeFixtureControl(
                      new TextDecoder().decode(request.body.body),
                    );
                    const answers = control.answers.map((answer) =>
                      answer.id.startsWith("capture:") && answer.result.ok
                        ? {
                            ...answer,
                            result: {
                              ok: true,
                              value: captureReplyTransform(
                                decodeFixtureCaptureGraph(answer.result.value),
                              ),
                            },
                          }
                        : answer,
                    );
                    request = HttpClientRequest.setBody(
                      request,
                      HttpBody.text(json({ ...control, answers }), "application/json"),
                    );
                  }
                  if (!legacyAutomationPeer) return request;
                  const header = request.headers[PeerAuthority.LINK_HEADER];
                  if (header === undefined) return request;
                  const { automationVersion: _version, ...legacy } =
                    decodeFixturePeerSession(header);
                  return HttpClientRequest.setHeader(
                    request,
                    PeerAuthority.LINK_HEADER,
                    json(legacy),
                  );
                }),
                (effect, request) => {
                  if (
                    !request.url.endsWith("/mcp/peer-control") ||
                    request.body._tag !== "Uint8Array"
                  )
                    return effect;
                  const control = decodeFixtureControl(new TextDecoder().decode(request.body.body));
                  return Effect.flatMap(Ref.get(secretControlReplyGate), (gate) =>
                    gate(control.answers.map((answer) => answer.id)),
                  ).pipe(
                    Effect.andThen(
                      captureReplyGate !== undefined &&
                        control.answers.some(
                          (answer) => answer.id.startsWith("capture:") && answer.result.ok,
                        )
                        ? captureReplyGate
                        : Effect.void,
                    ),
                    Effect.andThen(effect),
                  );
                },
              ),
            ),
          ).pipe(Layer.provide(FetchHttpClient.layer))
        : FetchHttpClient.layer;
    const connect = Effect.fn("PeerAuthority.connect")(function* () {
      const lifetime = yield* Scope.make();
      connectionScopes.add(lifetime);
      yield* Effect.addFinalizer(() =>
        Scope.close(lifetime, Exit.void).pipe(
          Effect.ensuring(Effect.sync(() => connectionScopes.delete(lifetime))),
        ),
      );
      const services = yield* RemoteDelegation.layer.pipe(
        Layer.provideMerge(
          attachmentRequests ? RequestRuntime.layerForwarding : PeerForwarding.layer,
        ),
        Layer.provideMerge(faultablePeers),
        Layer.provideMerge(Layer.succeedContext(baseServices)),
        Layer.provideMerge(Layer.succeedContext(requestContext)),
        Layer.provide(Layer.succeedContext(runtime)),
        Layer.provide(db),
        Layer.provide(nodeConfig),
        Layer.provide(projectLayer),
        Layer.provide(RepositoryIdentity.layer),
        Layer.provide(hostServices),
        Layer.provide(environment),
        Layer.provide(peerHttp),
        Layer.fresh,
        Layer.buildWithScope(lifetime),
      );
      return {
        services,
        stop: Scope.close(lifetime, Exit.void),
        peers: Context.get(services, PeerMcpClient.PeerMcpClient),
      };
    });
    const connection = yield* connect();
    const services = connection.services;
    const remoteSecretFault = yield* Ref.make<"after-save" | "hold-save" | "cleanup" | undefined>(
      undefined,
    );
    const secretPublished = yield* Deferred.make<void>();
    const releaseSecretPublication = yield* Deferred.make<void>();
    const actualSecretStore = Context.get(services, Secrets.ServerSecretStore);
    const secretRequestClock = yield* Ref.make<Clock.Clock | undefined>(undefined);
    const secretRequestAbort = yield* Ref.make<Deferred.Deferred<void> | undefined>(undefined);
    const secretRequestExits = yield* Queue.unbounded<void>();
    const secretValidationClock = yield* Ref.make<Clock.Clock | undefined>(undefined);
    const secretValidationErrors = yield* Queue.unbounded<{ id: string; code: string }>();
    const secretRemovals = yield* Queue.unbounded<{ name: string; failed: boolean }>();
    const secretCleanupName = yield* Ref.make<string | undefined>(undefined);
    const secretCleanupClock = yield* Ref.make<Clock.Clock | undefined>(undefined);
    const initialSecretClock = yield* Clock.Clock;
    const cleanupClock = () => Ref.getUnsafe(secretCleanupClock) ?? initialSecretClock;
    const secretOwnerClock: Clock.Clock = {
      currentTimeMillis: Effect.suspend(() => cleanupClock().currentTimeMillis),
      currentTimeMillisUnsafe: () => cleanupClock().currentTimeMillisUnsafe(),
      currentTimeNanos: Effect.suspend(() => cleanupClock().currentTimeNanos),
      currentTimeNanosUnsafe: () => cleanupClock().currentTimeNanosUnsafe(),
      monotonicTimeNanos: Effect.suspend(() => cleanupClock().monotonicTimeNanos),
      monotonicTimeNanosUnsafe: () => cleanupClock().monotonicTimeNanosUnsafe(),
      sleep: (duration) => Effect.suspend(() => cleanupClock().sleep(duration)),
    };
    const secretAuthority = Layer.effect(
      PeerAuthority.PeerAuthority,
      Effect.gen(function* () {
        const actual = yield* PeerAuthority.PeerAuthority;
        return {
          ...actual,
          checkInteractiveRecord: Effect.fn("PeerAuthority.checkInteractiveRecord")(function* (
            id: string,
          ) {
            const clock = yield* Ref.get(secretValidationClock);
            return yield* actual.checkInteractiveRecord(id).pipe(
              (effect) =>
                clock === undefined ? effect : Effect.provideService(effect, Clock.Clock, clock),
              Effect.tapError((error) =>
                Queue.offer(secretValidationErrors, { id, code: error.code }),
              ),
            );
          }),
        };
      }),
    ).pipe(Layer.provide(Layer.succeedContext(baseServices)));
    const secretOwnerScope = yield* Scope.make();
    yield* Effect.addFinalizer(() => Scope.close(secretOwnerScope, Exit.void));
    const faultSecretStore = Layer.succeed(Secrets.ServerSecretStore, {
      ...actualSecretStore,
      create: (name, bytes) =>
        actualSecretStore.create(name, bytes).pipe(
          Effect.andThen(
            Effect.gen(function* () {
              if (
                name.startsWith("secret-remote-") &&
                (yield* Ref.get(remoteSecretFault)) === "hold-save"
              ) {
                yield* Deferred.succeed(secretPublished, undefined);
                yield* Deferred.await(releaseSecretPublication);
              }
              if (
                name.startsWith("secret-remote-") &&
                (yield* Ref.get(remoteSecretFault)) === "after-save"
              ) {
                yield* Ref.set(remoteSecretFault, undefined);
                return yield* new Secrets.SecretStorePersistError({
                  resource: "isolated remote publication",
                  cause: new Error("Fixture interruption after protected publication"),
                });
              }
            }),
          ),
        ),
      remove: Effect.fn("PeerAuthority.remove")(
        function* (name) {
          if (
            name.startsWith("secret-remote-") &&
            (yield* Ref.get(remoteSecretFault)) === "cleanup" &&
            ((yield* Ref.get(secretCleanupName)) === undefined ||
              (yield* Ref.get(secretCleanupName)) === name)
          ) {
            return yield* new Secrets.SecretStoreRemoveError({
              resource: "isolated redundant copy",
              cause: new Error("Fixture interruption after outer commit"),
            });
          }
          return yield* actualSecretStore.remove(name);
        },
        (effect, name) =>
          effect.pipe(
            Effect.tapError(() => Queue.offer(secretRemovals, { name, failed: true })),
            Effect.tap(() => Queue.offer(secretRemovals, { name, failed: false })),
          ),
      ),
    });
    const secretOwners = remoteSecrets
      ? yield* Layer.mergeAll(RemoteSecretRequests.layer, RemoteSecretRelay.layer).pipe(
          Layer.provide(Layer.succeed(Clock.Clock, secretOwnerClock)),
          Layer.provide(faultSecretStore),
          Layer.provide(secretAuthority),
          Layer.provide(Layer.succeedContext(services)),
          Layer.provide(Layer.succeedContext(baseServices)),
          Layer.provide(Layer.succeedContext(runtime)),
          Layer.provide(db),
          Layer.provide(environment),
          Layer.provide(nodeConfig),
          Layer.buildWithScope(secretOwnerScope),
        )
      : Context.empty();
    const handoffGitManager = GitManager.layer.pipe(
      Layer.provide(actualGit),
      Layer.provide(
        Layer.mock(SourceProviders.SourceControlProviderRegistry)({
          resolveLink: () => Effect.die("No external repository host in this fixture"),
          // Upstream git workflows look up a forge and tolerate none.
          resolve: ({ cwd }) =>
            Effect.fail(
              new SourceControlProviderError({
                provider: SourceControlProviderKind.make("unknown"),
                operation: "resolve",
                cwd,
                detail: "No external repository host in this fixture.",
              }),
            ),
        }),
      ),
      Layer.provide(Layer.mock(TextGeneration.TextGeneration)({})),
      Layer.provide(layerLaunchCatalog),
      Layer.provide(Layer.succeedContext(runtime)),
      Layer.provide(ProjectStore.layer.pipe(Layer.provide(db))),
      Layer.provide(settings),
      Layer.provide(Layer.mock(SetupScripts.ProjectSetupScriptRunner)({})),
    );
    const handoffGit = GitWorkflow.layer.pipe(
      Layer.provide(handoffGitManager),
      Layer.provide(actualGit),
      Layer.provide(VcsRegistry.layer.pipe(Layer.provide(VcsProcess.layer))),
    );
    const launchDependencies = Layer.mergeAll(
      Layer.succeedContext(runtime),
      db,
      projectLayer,
      folders,
      settings,
      layerLaunchCatalog,
      Receipts.layer.pipe(Layer.provide(db)),
      IdAllocator.layer,
      WorktreeSetup.layer,
      Layer.mock(ProjectClone.ProjectCloneTracker)({ get: () => Effect.succeed(null) }),
      Layer.mock(Terminals.TerminalManager)({ close: () => Effect.void }),
      ownerWorkspace === undefined
        ? Layer.mock(GitWorkflow.GitWorkflowService)({})
        : handoffGit.pipe(Layer.provide(nodeConfig)),
      Layer.mock(TextGeneration.TextGeneration)({}),
      Layer.mock(SetupScripts.ProjectSetupScriptRunner)({
        runForThread: () => Effect.succeed({ status: "no-script" as const }),
      }),
      Layer.mock(BareLaunch.BareLaunch)({}),
    );
    const launches = yield* ThreadLaunch.layer.pipe(
      Layer.provide(launchDependencies),
      Layer.fresh,
      Layer.build,
    );
    const holdManualLaunchReply = yield* Ref.make(false);
    const manualLaunchReplyHeld = yield* Deferred.make<void>();
    const actualLaunch = Context.get(launches, ThreadLaunch.ThreadLaunchService);
    const scheduledLaunch = Layer.succeed(ThreadLaunch.ThreadLaunchService, {
      ...actualLaunch,
      launch: (input) =>
        actualLaunch.launch(input).pipe(
          Effect.tap(() =>
            Effect.gen(function* () {
              if (
                !input.commandId.startsWith("scheduled-task:manual:") ||
                !(yield* Ref.get(holdManualLaunchReply))
              )
                return;
              yield* Deferred.succeed(manualLaunchReplyHeld, undefined);
              return yield* Effect.never;
            }),
          ),
        ),
    });
    const holdManualSend = yield* Ref.make(false);
    const manualSendHeld = yield* Deferred.make<void>();
    const releaseManualSend = yield* Deferred.make<void>();
    const actualThreads = Context.get(runtime, Threads.ThreadManagementService);
    // Holds a keyed manual send to an existing thread before native acceptance.
    const scheduledThreads = Layer.succeed(Threads.ThreadManagementService, {
      ...actualThreads,
      sendToThread: Effect.fn("PeerAuthority.sendToThread")(function* (input) {
        if (
          input.commandId.startsWith("scheduled-task:manual:") &&
          (yield* Ref.get(holdManualSend))
        ) {
          yield* Deferred.succeed(manualSendHeld, undefined);
          yield* Deferred.await(releaseManualSend);
        }
        return yield* actualThreads.sendToThread(input);
      }),
    });
    const secretConsumes = yield* Ref.make(0);
    const loseSecretConfirmation = yield* Ref.make(false);
    const nativeSecrets = SecretRequests.layer.pipe(
      Layer.provide(nodeConfig),
      Layer.provide(
        Layer.succeed(Secrets.ServerSecretStore, Context.get(services, Secrets.ServerSecretStore)),
      ),
      Layer.provide(Layer.succeedContext(runtime)),
    );
    const scheduleSecrets = attachmentRequests
      ? Layer.effect(
          SecretRequests.SecretRequests,
          Effect.gen(function* () {
            const actual = yield* SecretRequests.SecretRequests;
            return {
              ...actual,
              consume: Effect.fn("PeerAuthority.consume")(function* (
                input: Parameters<typeof actual.consume>[0],
              ) {
                const rows = yield* Context.get(
                  database,
                  SqlClient.SqlClient,
                )`SELECT request_id FROM scheduled_mutation_receipts WHERE stage='secret-started'`.pipe(
                  Effect.orDie,
                );
                expect(rows.length).toBeGreaterThan(0);
                yield* Ref.update(secretConsumes, (n) => n + 1);
                const secret = yield* actual.consume(input);
                if (yield* Ref.getAndSet(loseSecretConfirmation, false))
                  return yield* new SecretRequestError({ reason: "consume_failed" });
                return secret;
              }),
            };
          }),
        ).pipe(Layer.provide(nativeSecrets))
      : Layer.mock(SecretRequests.SecretRequests)({});
    const scheduleLayer = (
      attachmentRequests
        ? ScheduledChecks.withOutcomeChecks(ScheduledTasks.layer)
        : ScheduledTasks.layer
    ).pipe(
      Layer.provide(
        commandSpawner === undefined
          ? ProcessRunner.layer
          : ProcessRunner.layer.pipe(
              Layer.provide(Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, commandSpawner)),
              Layer.fresh,
            ),
      ),
      Layer.provide(Layer.succeedContext(secretOwners)),
      Layer.provide(Layer.succeedContext(baseServices)),
      Layer.provide(
        Layer.succeed(
          ScheduledTasks.ScheduledTaskWebhookOrigin,
          Effect.succeed({ relayHookBaseUrl: "https://hooks.example.invalid/v1/hooks/fixture" }),
        ),
      ),
      Layer.provide(
        manualScheduler === undefined
          ? Scheduler.layer
          : Layer.succeed(Scheduler.Scheduler, manualScheduler),
      ),
      Layer.provide(scheduleSecrets),
      Layer.provide(scheduledLaunch),
      Layer.provide(scheduledThreads),
      Layer.provide(Prism.layer.pipe(Layer.provide(settings), Layer.provide(layerLaunchCatalog))),
      Layer.provide(
        commandSpawner === undefined
          ? Layer.empty
          : Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, commandSpawner),
      ),
      Layer.provide(ProjectStore.layer.pipe(Layer.provide(db))),
      Layer.provide(launchDependencies),
      Layer.provide(hostServices),
    );
    const scheduleScope = yield* Scope.make();
    yield* Effect.addFinalizer(() => Scope.close(scheduleScope, Exit.void));
    const schedules = yield* scheduleLayer.pipe(Layer.fresh, Layer.buildWithScope(scheduleScope));
    if (attachmentRequests) {
      yield* SpectrumStore.ensureSpectrumSchema.pipe(Effect.provide(db));
      yield* CheckStore.ensureCheckSchema.pipe(Effect.provide(db));
    }
    const spectrumLayer = SpectrumLaunch.layer.pipe(
      Layer.provide(Layer.succeedContext(baseServices)),
      Layer.provide(Adapters.layerFromAdapters([providerAdapter])),
      Layer.provide(Prism.layer.pipe(Layer.provide(settings), Layer.provide(layerLaunchCatalog))),
      Layer.provide(ProjectStore.layer.pipe(Layer.provide(db))),
      Layer.provide(launchDependencies),
    );
    const loseWorktreeReply = yield* Ref.make(false);
    const worktreeThreads = {
      ...Context.get(runtime, Threads.ThreadManagementService),
      dispatch: (input: Parameters<Threads.ThreadManagementService["Service"]["dispatch"]>[0]) =>
        Context.get(runtime, Threads.ThreadManagementService)
          .dispatch(input)
          .pipe(
            Effect.flatMap((result) =>
              Effect.gen(function* () {
                if (
                  input.type === "thread.metadata.update" &&
                  input.commandId.startsWith("worktree-handoff:") &&
                  (yield* Ref.getAndSet(loseWorktreeReply, false))
                )
                  return yield* new Orchestrator.OrchestratorDispatchError({
                    commandId: input.commandId,
                    commandType: input.type,
                    cause: "Isolated committed native binding reply loss",
                  });
                return result;
              }),
            ),
          ),
    };
    const handoff =
      ownerWorkspace === undefined
        ? Layer.empty
        : yield* Effect.gen(function* () {
            const gitContext = yield* handoffGit.pipe(
              Layer.provide(nodeConfig),
              Layer.fresh,
              Layer.build,
            );
            const service = WorktreeMcp.layer.pipe(
              Layer.provide(Layer.succeed(Threads.ThreadManagementService, worktreeThreads)),
              Layer.provide(Layer.succeedContext(gitContext)),
              Layer.provide(
                Layer.mock(VcsStatus.VcsStatusBroadcaster)({
                  refreshStatus: (cwd) =>
                    Context.get(gitContext, GitWorkflow.GitWorkflowService).status({ cwd }),
                }),
              ),
              Layer.provide(launchDependencies),
            );
            return McpHttpServer.toolkitRegistration(WorktreeToolkit, WorktreeHandlers.layer).pipe(
              Layer.provide(service),
              Layer.provide(Layer.succeedContext(gitContext)),
            );
          });
    const browser = yield* BrowserBroker.make.pipe(
      Effect.provideService(
        PeerAuthority.PeerAuthority,
        Context.get(baseServices, PeerAuthority.PeerAuthority),
      ),
    );
    const previewBrowser = Layer.succeed(PreviewBrowser.PreviewBrowser, {
      installed: Effect.succeedNone,
      executable: Effect.die("This isolated fixture cannot launch a browser"),
    });
    const nativeBrowser = browserOwner
      ? yield* NativeBrowser.layer.pipe(
          Layer.provide(Layer.succeedContext(runtime)),
          Layer.provide(
            Layer.mock(CredentialVault.CredentialVault)({
              uris: () => Effect.die("No real vault access in the peer authority fixture"),
              read: () => Effect.die("No real vault access in the peer authority fixture"),
            }),
          ),
          Layer.provideMerge(NativePreviewManager.layer),
          Layer.provide(NativeDesktopChannel.layer),
          Layer.provide(Layer.succeed(BrowserBroker.PreviewAutomationBroker, browser)),
          Layer.provide(previewBrowser),
          Layer.provide(nodeConfig),
          Layer.provide(environment),
          Layer.build,
        )
      : undefined;
    const previewControls =
      nativeBrowser ??
      (previewControlsOwner ? yield* NativePreviewManager.layer.pipe(Layer.build) : undefined);
    const htmlLayer = HtmlRender.layer.pipe(
      Layer.provide(
        Layer.succeed(FileSystem.FileSystem, {
          ...actualFs,
          stream: (path, options) =>
            htmlImageReadGate === undefined
              ? actualFs.stream(path, options)
              : Stream.unwrap(htmlImageReadGate.pipe(Effect.as(actualFs.stream(path, options)))),
        }),
      ),
      Layer.provide(nodeConfig),
      Layer.provide(previewBrowser),
    );

    const deviceHostStarts = yield* Ref.make(0);
    const devicePlatformAvailable = yield* Ref.make(true);
    const deviceAgentStarts = yield* Ref.make(0);
    const deviceConfigurations = yield* Ref.make(0);
    const loseDeviceConfiguration = yield* Ref.make(false);
    const deviceProcessCalls: ProcessRunner.ProcessRunInput[] = [];
    const deviceProcessHook = yield* Ref.make<
      (input: ProcessRunner.ProcessRunInput) => Effect.Effect<void>
    >(() => Effect.void);
    const deviceSummaryHook = yield* Ref.make<Effect.Effect<void>>(Effect.void);
    const deviceReadinessHook = yield* Ref.make<Effect.Effect<void>>(Effect.void);
    const deviceObservedScope = yield* Ref.make<Option.Option<Invocation.McpThreadInvocationScope>>(
      Option.none(),
    );
    const deviceMediaPaths = new Map<string, string>();
    const deviceMediaOutside = yield* Ref.make(false);
    const deviceScreenshotCalls = yield* Ref.make(0);
    const deviceShutdownCalls = yield* Ref.make(0);
    const deviceHubPlatform = yield* Ref.make<"android" | "ios">("android");
    const deviceHubBooted = yield* Ref.make(true);
    const deviceViewerRequests: string[] = [];
    const deviceViewerCurrentHook = yield* Ref.make<Effect.Effect<void>>(Effect.void);
    const deviceViewerFrame = Uint8Array.of(0, 0, 0, 4, 0, 1, 100, 0);
    const deviceViewerInputs: string[] = [];
    const deviceViewerHttpInputs: Array<{
      path: string;
      method: string;
      contentType: string;
      body: string;
    }> = [];
    const deviceViewerOutput: { bytes: string | Uint8Array } = {
      bytes: json({ ok: true }),
    };
    let viewerHubOrigin = "http://device382.invalid";
    if (viewerSocketOwner) {
      const hub = yield* Effect.acquireRelease(
        Effect.sync(() => new NodeWS.WebSocketServer({ port: 0 })),
        (server) =>
          Effect.promise(
            () =>
              new Promise<void>((resolve) => {
                for (const client of server.clients) client.terminate();
                server.close(() => resolve());
              }),
          ),
      );
      if (!hub.address())
        yield* Effect.promise(() => new Promise<void>((resolve) => hub.once("listening", resolve)));
      const address = hub.address();
      if (typeof address !== "object" || address === null)
        throw new Error("Missing external hub port");
      viewerHubOrigin = `http://127.0.0.1:${address.port}`;
      hub.on("connection", (socket) => {
        socket.on("message", (data) => {
          deviceViewerInputs.push(data.toString());
          socket.send(deviceViewerOutput.bytes);
        });
      });
    }

    const deviceOldVersion = yield* Ref.make(false);
    const deviceCapabilities = yield* Ref.make({
      device: { id: "fixture-device382", platform: "android", target: "mobile", kind: "emulator" },
      availableCommands: [
        "open",
        "snapshot",
        "click",
        "fill",
        "screenshot",
        "close",
        "record",
        "trace",
        "install",
      ],
    });
    const continuousDeviceMedia = yield* Ref.make(false);
    const deviceMediaChunks = yield* Queue.unbounded<Uint8Array>();
    let fixtureDevice: NativeDevices.DeviceService["Service"] | undefined;
    let fixtureViewers: DeviceHubProxy.DeviceViewers["Service"] | undefined;
    let fixtureBrowserViewers: BrowserStream.PreviewViewers["Service"] | undefined;
    let fixtureBrowserViewerSessions: ViewerSessions.SessionStore["Service"] | undefined;
    let fixtureViewerSessions: ViewerSessions.SessionStore["Service"] | undefined;
    const deviceLayer = deviceOwner
      ? yield* Effect.gen(function* () {
          const settingsService = Context.get(settingsContext, Settings.ServerSettingsService);
          yield* settingsService.updateSettings({
            enableDeviceSupport: true,
            enableAgentDeviceAccess: true,
          });
          const config = Context.get(nodeConfigContext, Config.ServerConfig);
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const file = yield* agentDeviceConfigPath(config.stateDir, LOCAL_DEVICE_HOST_ID, path);
          const agentInstall = `${config.baseDir}/tools/agent-device/${AGENT_DEVICE_VERSION}`;
          const hubInstall = `${config.baseDir}/tools/expo-device-hub/${DEVICE_HUB_VERSION}`;
          const entryPath = `${agentInstall}/node_modules/agent-device/bin/agent-device.mjs`;
          const hubEntry = `${hubInstall}/node_modules/expo-device-hub/dist/server/cli.mjs`;
          for (const [entry, install, version] of [
            [entryPath, agentInstall, AGENT_DEVICE_VERSION],
            [hubEntry, hubInstall, DEVICE_HUB_VERSION],
          ] as const) {
            yield* fs.makeDirectory(path.dirname(entry), { recursive: true });
            yield* fs.writeFileString(
              entry,
              "// Isolated external process leaf, never executed.\n",
            );
            yield* fs.writeFileString(`${install}/.install-complete`, version);
          }
          const assertStarted = Effect.gen(function* () {
            const sql = Context.get(database, SqlClient.SqlClient);
            expect(
              (yield* sql`SELECT id FROM mcp_request_receipts WHERE state='started'`.pipe(
                Effect.orDie,
              )).length,
            ).toBeGreaterThan(0);
          });
          const deviceFs: FileSystem.FileSystem = {
            ...fs,
            rename: (from, to) => {
              if (to !== file && !to.startsWith(`${config.stateDir}/device/sessions/`))
                return fs.rename(from, to);
              return assertStarted.pipe(
                Effect.andThen(fs.rename(from, to)),
                Effect.tap(() =>
                  Effect.gen(function* () {
                    yield* Ref.update(deviceConfigurations, (n) => n + 1);
                    if (yield* Ref.getAndSet(loseDeviceConfiguration, false))
                      return yield* PlatformError.systemError({
                        _tag: "Unknown",
                        module: "FileSystem",
                        method: "rename",
                        description: "Isolated reply loss after actual native config persistence",
                      });
                  }),
                ),
              );
            },
          };
          const runner = ProcessRunner.ProcessRunner.of({
            run: Effect.fn("PeerAuthority.run")(function* (input) {
              yield* assertStarted;
              const observed = yield* Effect.serviceOption(Invocation.McpInvocationContext);
              if (Option.isSome(observed) && observed.value.thread !== undefined)
                yield* Ref.set(
                  deviceObservedScope,
                  Option.some({ ...observed.value, thread: observed.value.thread }),
                );
              deviceProcessCalls.push(input);
              yield* (yield* Ref.get(deviceProcessHook))(input);
              const family = input.args[1];
              let data: unknown =
                family === "capabilities"
                  ? yield* Ref.get(deviceCapabilities)
                  : family === "snapshot"
                    ? {
                        nodes: [
                          {
                            index: 1,
                            ref: "e1",
                            role: "button",
                            label: "Continue",
                            privatePath: "/host/private",
                          },
                        ],
                        config: "/host/private",
                        token: "ISOLATED_DEVICE_TOKEN_382",
                      }
                    : {};
              if (family === "record" || family === "trace") {
                const flags = input.args.slice(0, input.args.indexOf("--"));
                const positionals = input.args.slice(input.args.indexOf("--") + 1);
                const key = `${flags[flags.indexOf("--session") + 1]}:${family}`;
                if (positionals[0] === "start") deviceMediaPaths.set(key, positionals[1]!);
                const outPath = deviceMediaPaths.get(key)!;
                if (positionals[0] === "stop") {
                  if (family === "record") {
                    const bytes = new Uint8Array(24);
                    new DataView(bytes.buffer).setUint32(4, 0x66747970);
                    yield* fs.writeFile(outPath, bytes).pipe(Effect.orDie);
                  } else
                    yield* fs
                      .writeFileString(
                        outPath,
                        "trace ISOLATED_DEVICE_TOKEN_382 http://device-agent382.invalid",
                      )
                      .pipe(Effect.orDie);
                }
                data = {
                  [family === "record" ? "recording" : "trace"]:
                    positionals[0] === "start" ? "started" : "stopped",
                  outPath,
                  artifacts: [
                    {
                      field: "outPath",
                      localPath: (yield* Ref.get(deviceMediaOutside)) ? entryPath : outPath,
                    },
                  ],
                  durationMs: 200,
                  capturedDurationMs: 150,
                  recorder: "unconfirmed",
                  nativePathDisposition: "pending",
                };
              }
              return {
                code: ChildProcessSpawner.ExitCode(0),
                stdout: json({ success: true, data }),
                stderr: "",
                timedOut: false,
                stdoutTruncated: false,
                stderrTruncated: false,
                stdoutInvalidUtf8: false,
                stderrInvalidUtf8: false,
              };
            }),
          });
          const currentHub = yield* Ref.make<DeviceHost.DeviceHostReady | null>(null);
          // Opening an Android device probes the host's ffmpeg encoders before streaming.
          const run: DeviceHost.DeviceHostReady["run"] = (_command, args) =>
            Effect.succeed({
              code: 0,
              stdout: args[0] === "-e" ? " V....D libx264  H.264\n" : "",
              stderr: "",
            });
          const ready: DeviceHost.DeviceHostReady = {
            nodePath: "/remote-host/node-must-never-execute",
            hub: { origin: viewerHubOrigin },
            devices: makeDeviceHubOperations({
              hostId: LOCAL_DEVICE_HOST_ID,
              origin: viewerHubOrigin,
              // Resolved per request: the fixture hub client below serves these calls.
              http: HttpClient.make((request) => hub.execute(request)),
              run,
              androidAvailable: Ref.get(devicePlatformAvailable),
            }),
            helpers: { serveSimAxSettings: null, serveSimCli: null },
            run,
          };
          const host: DeviceHost.DeviceHost["Service"] = {
            id: LOCAL_DEVICE_HOST_ID,
            label: "Isolated fixture",
            summary: Ref.get(devicePlatformAvailable).pipe(
              Effect.tap(() => Ref.get(deviceSummaryHook).pipe(Effect.flatten)),
              Effect.flatMap((available) =>
                Ref.get(deviceOldVersion).pipe(
                  Effect.map((oldVersion) => ({
                    id: LOCAL_DEVICE_HOST_ID,
                    kind: "local" as const,
                    label: "Isolated fixture",
                    platforms: [{ platform: "android" as const, available }],
                    hubInstalled: true,
                    agentDeviceInstalled: true,
                    tools: {
                      agent: {
                        requiredVersion: AGENT_DEVICE_VERSION,
                        installedVersions: [oldVersion ? "0.21.12" : AGENT_DEVICE_VERSION],
                        runningVersion: oldVersion ? "0.21.12" : AGENT_DEVICE_VERSION,
                      },
                      hub: {
                        requiredVersion: DEVICE_HUB_VERSION,
                        installedVersions: [DEVICE_HUB_VERSION],
                        runningVersion: DEVICE_HUB_VERSION,
                      },
                    },
                  })),
                ),
              ),
            ),
            platformAvailability: (platform) => Effect.succeed({ platform, available: true }),
            ensureReady: Effect.fn("PeerAuthority.ensureReady")(
              function* () {
                yield* yield* Ref.get(deviceReadinessHook);
                // A direct native UI setup call has no MCP request identity.
                // Every registered action must be started before preparation.
                if ((yield* RequestReceipts.CurrentRequest) !== undefined) yield* assertStarted;
              },
              Effect.andThen(Ref.update(deviceHostStarts, (n) => n + 1)),
              Effect.andThen(
                fs
                  .writeFileString(
                    `${config.stateDir}/fixture-device-host-started`,
                    "Isolated native preparation footprint",
                  )
                  .pipe(Effect.orDie),
              ),
              Effect.tap(() => Ref.set(currentHub, ready)),
              Effect.as(ready),
            ),
            ensureAgentReady: () =>
              assertStarted.pipe(
                Effect.andThen(Ref.update(deviceAgentStarts, (n) => n + 1)),
                Effect.as({
                  ...ready,
                  agentDevice: {
                    baseUrl: "http://device-agent382.invalid",
                    token: "ISOLATED_DEVICE_TOKEN_382",
                    entryPath,
                  },
                }),
              ),
            current: Ref.get(currentHub).pipe(
              Effect.tap(() => Ref.get(deviceViewerCurrentHook).pipe(Effect.flatten)),
            ),
            stopAgent: Effect.void,
            stop: Ref.set(currentHub, null),
          };
          const actualHttp = yield* Effect.serviceOption(HttpClient.HttpClient);
          const hub = HttpClient.make((request) =>
            Effect.gen(function* () {
              if (!request.url.startsWith(`${viewerHubOrigin}/`)) {
                if (Option.isNone(actualHttp))
                  return yield* Effect.die("Native HTTP transport is unavailable in this fixture.");
                return yield* actualHttp.value.execute(request);
              }
              const nativePath = new URL(request.url).pathname;
              if (
                request.method !== "GET" &&
                request.method !== "HEAD" &&
                /\/api\/(fold|stream-mode|stream-settings)$/.test(nativePath)
              ) {
                if (request.headers["content-type"] !== "application/json")
                  return HttpClientResponse.fromWeb(request, new Response(null, { status: 415 }));
                const body =
                  request.body._tag === "Stream"
                    ? Buffer.concat(
                        yield* request.body.stream.pipe(Stream.orDie, Stream.runCollect),
                      )
                    : request.body._tag === "Uint8Array"
                      ? request.body.body
                      : new Uint8Array();
                deviceViewerHttpInputs.push({
                  path: nativePath,
                  method: request.method,
                  contentType: request.headers["content-type"],
                  body: new TextDecoder().decode(body),
                });
                return HttpClientResponse.fromWeb(request, Response.json({ ok: true }));
              }
              if (request.url.endsWith("/stream.avcc")) {
                deviceViewerRequests.push(request.url);
                return HttpClientResponse.fromWeb(
                  request,
                  new Response(
                    (yield* Ref.get(continuousDeviceMedia))
                      ? yield* Stream.toReadableStreamEffect(Stream.fromQueue(deviceMediaChunks))
                      : deviceViewerFrame,
                    { headers: { "content-type": "application/octet-stream" } },
                  ),
                );
              }
              if (request.url.endsWith("/grid/api/start")) {
                yield* assertStarted;
                return HttpClientResponse.fromWeb(request, Response.json({ ok: true }));
              }
              if (request.url.endsWith("/shutdown")) {
                yield* assertStarted;
                yield* Ref.update(deviceShutdownCalls, (n) => n + 1);
                return HttpClientResponse.fromWeb(request, Response.json({ ok: true }));
              }
              if (request.url.includes("screenshot")) {
                if ((yield* RequestReceipts.CurrentRequest) !== undefined) yield* assertStarted;
                yield* Ref.update(deviceScreenshotCalls, (n) => n + 1);
                return HttpClientResponse.fromWeb(
                  request,
                  new Response(
                    Buffer.from(
                      "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScLbtAAAAABJRU5ErkJggg==",
                      "base64",
                    ),
                    { headers: { "content-type": "image/png" } },
                  ),
                );
              }
              return HttpClientResponse.fromWeb(
                request,
                Response.json({
                  simulators:
                    (yield* Ref.get(deviceHubPlatform)) === "ios"
                      ? [
                          {
                            id: "fixture-device382",
                            name: "Isolated iOS",
                            platform: "ios",
                            version: "fixture",
                            booted: yield* Ref.get(deviceHubBooted),
                            physical: false,
                          },
                        ]
                      : [],
                  emulators:
                    (yield* Ref.get(deviceHubPlatform)) === "android"
                      ? [
                          {
                            id: "fixture-device382",
                            name: "Isolated Android",
                            platform: "android",
                            version: "fixture",
                            booted: yield* Ref.get(deviceHubBooted),
                            physical: false,
                          },
                        ]
                      : [],
                }),
              );
            }),
          );
          const service = yield* NativeDevices.make.pipe(
            Effect.provideService(DeviceHost.DeviceHost, host),
            Effect.provideService(FileSystem.FileSystem, deviceFs),
            Effect.provideService(ProcessRunner.ProcessRunner, runner),
            Effect.provideService(HttpClient.HttpClient, hub),
            Effect.provideService(Settings.ServerSettingsService, settingsService),
            Effect.provide(Layer.merge(nodeConfig, Net.layer)),
          );
          fixtureDevice = service;
          const deviceServiceLayer = Layer.succeed(NativeDevices.DeviceService, service);
          if (!viewerOwner) return deviceServiceLayer;
          return Layer.mergeAll(
            HttpApiBuilder.layer(DeviceViewerTestApi).pipe(
              Layer.provide(DeviceHubProxy.layerApi),
              Layer.provide(AuthHttp.layerAuthenticatedAuth),
            ),
            DeviceHubProxy.layer,
            Layer.effectDiscard(
              Effect.gen(function* () {
                fixtureViewers = yield* DeviceHubProxy.DeviceViewers;
                fixtureViewerSessions = yield* ViewerSessions.SessionStore;
              }),
            ),
          ).pipe(
            Layer.provideMerge(DeviceHubProxy.layerViewers),
            Layer.provideMerge(deviceServiceLayer),
            Layer.provide(Layer.succeed(HttpClient.HttpClient, hub)),
          );
        })
      : Layer.mock(NativeDevices.DeviceService)({});

    const artifactResponseHook = yield* Ref.make<Effect.Effect<void, OrchestratorMcpFailure>>(
      Effect.void,
    );
    const artifactRequestHook = yield* Ref.make<
      (checkOnly: boolean) => Effect.Effect<void, OrchestratorMcpFailure>
    >(() => Effect.void);
    const previewUploadContentTypes: string[] = [];
    const artifactResponseBoundary = HttpRouter.middleware(
      (effect) =>
        Effect.gen(function* () {
          const request = yield* HttpServerRequest.HttpServerRequest;
          if (request.method === "POST" && request.url.startsWith("/api/preview-stream/upload?"))
            previewUploadContentTypes.push(request.headers["content-type"] ?? "");
          if (request.method === "POST" && request.url === "/mcp/peer-control/attachment")
            yield* (yield* Ref.get(artifactRequestHook))(
              request.headers["t3-attachment-check-only"] === "1",
            ).pipe(Effect.interruptible);
          const response = yield* effect;
          if (
            request.method === "POST" &&
            request.url === "/mcp/peer-control/attachment" &&
            response.status === 200
          )
            yield* yield* Ref.get(artifactResponseHook);
          return response;
        }),
      { global: true },
    );
    const browserMediaLayer =
      previewControls === undefined
        ? Layer.empty
        : Layer.mergeAll(
            BrowserStream.routeLayer.pipe(Layer.provide(NodeHttpPlatform.layer)),
            Layer.effectDiscard(
              Effect.gen(function* () {
                fixtureBrowserViewers = yield* BrowserStream.PreviewViewers;
                fixtureBrowserViewerSessions = yield* ViewerSessions.SessionStore;
              }),
            ),
          ).pipe(
            Layer.provideMerge(BrowserStream.layerViewers),
            Layer.provide(
              nativeBrowser === undefined ? Layer.empty : Layer.succeedContext(nativeBrowser),
            ),
            Layer.provide(AuthSessions.layer.pipe(Layer.provide(db))),
            Layer.provide(FetchHttpClient.layer),
            Layer.provide(environment),
          );
    const timedSecretService = Layer.effect(
      OrchestratorService.OrchestratorMcpService,
      Effect.gen(function* () {
        const actual = yield* OrchestratorService.OrchestratorMcpService;
        return {
          ...actual,
          requestSecret: Effect.fn("PeerAuthority.requestSecret")(function* (
            ...args: Parameters<typeof actual.requestSecret>
          ) {
            const clock = yield* Ref.get(secretRequestClock);
            const abort = yield* Ref.get(secretRequestAbort);
            const actualCall = actual.requestSecret(...args);
            const effect =
              abort === undefined
                ? actualCall
                : Effect.raceFirst(
                    actualCall,
                    Deferred.await(abort).pipe(Effect.andThen(Effect.interrupt)),
                  );
            return yield* (
              clock === undefined ? effect : Effect.provideService(effect, Clock.Clock, clock)
            ).pipe(Effect.onExit(() => Queue.offer(secretRequestExits, undefined)));
          }),
        };
      }),
    ).pipe(Layer.provide(OrchestratorService.layer));
    const secretToolkit = McpHttpServer.toolkitRegistration(
      OrchestratorToolkit,
      OrchestratorHandlers.layer,
    ).pipe(Layer.provide(timedSecretService), Layer.provide(ThreadMetadataService.layer));
    const toolsFor = (
      scheduleServices: typeof schedules,
      requests: typeof requestContext = requestContext,
      nativeServices = services,
    ) =>
      Layer.mergeAll(
        artifactResponseBoundary,
        McpHttpServer.layerProjectRegistration,
        remoteSecrets ? secretToolkit : McpHttpServer.layerOrchestratorToolkit,
        ...(previewControls === undefined
          ? []
          : [
              McpHttpServer.toolkitRegistration(
                PreviewControlsToolkit,
                PreviewControlHandlers.layer,
              ).pipe(Layer.provide(Layer.succeedContext(previewControls))),
            ]),
        ...(preferencesOwner ? [McpHttpServer.layerEnvironmentToolkit] : []),
        ...(deviceOwner
          ? [
              legacyDeviceDomain
                ? McpHttpServer.toolkitRegistration(
                    LegacyDeviceToolkit,
                    LegacyDeviceHandlers.layerStandard,
                  )
                : McpHttpServer.layerDeviceToolkit,
            ]
          : []),
        ...(attachmentRequests
          ? [
              McpHttpServer.toolkitRegistration(IssuesToolkit, IssueHandlers.layer).pipe(
                Layer.provide(
                  IssueLinks.layer.pipe(
                    Layer.provide(
                      Layer.succeed(ClosingReferences.IssueClosingReferences, {
                        issuesClosedBy: () => Effect.succeed([]),
                      }),
                    ),
                  ),
                ),
              ),
              McpHttpServer.layerPullRequestsToolkit,
              McpHttpServer.toolkitRegistration(AttachmentToolkit, AttachmentHandlers.layer),
              ServerHttp.layerAttachmentUploadRoute,
              ServerHttp.layerAssetRoute.pipe(
                HttpRouter.provideRequest(
                  Layer.mergeAll(
                    NodeHttpPlatform.layer,
                    WorkspacePaths.layer,
                    NativeAppIconResolver.layer.pipe(Layer.provide(nodeConfig)),
                    Layer.succeedContext(runtime),
                    // This fixture has no GitHub resource. Never access a real account.
                    Layer.mock(GitHubCredentials.GitHubCredentials)({
                      get: () =>
                        Effect.die("No GitHub credential access in the native artifact fixture"),
                    }),
                  ),
                ),
                Layer.provide(environment),
              ),
              McpHttpServer.layerPreviewToolkit,
              McpHttpServer.layerThreadToolkit,
              McpHttpServer.layerHtmlToolkit,
              handoff,
              McpHttpServer.toolkitRegistration(SpectrumMcp.SpectrumToolkit, SpectrumMcp.layer),
            ]
          : []),
      ).pipe(
        Layer.provide(
          Layer.mergeAll(
            spectrumLayer,
            htmlLayer,
            previewBrowser,
            nativeBrowser === undefined ? Layer.empty : Layer.succeedContext(nativeBrowser),
          ),
        ),
        Layer.provide(
          Layer.mergeAll(
            Layer.mock(PullRequestReviewGuideService.PullRequestReviewGuideService)({}),
            deviceLayer,
            browserMediaLayer,
          ),
        ),
        Layer.provide(ThreadSearch.layer.pipe(Layer.provide(db))),
        Layer.provide(Layer.succeed(BrowserBroker.PreviewAutomationBroker, browser)),
        Layer.provide(Layer.succeedContext(requests)),
        Layer.provide(nodeConfig),
        Layer.provide(Layer.succeedContext(baseServices)),
        Layer.provide(Layer.succeedContext(scheduleServices)),
        Layer.provide(
          Layer.mergeAll(
            remoteSecrets ? nativeSecrets : Layer.mock(SecretRequests.SecretRequests)({}),
            Layer.succeedContext(secretOwners),
          ),
        ),
        Layer.provide(Layer.mock(BareLaunch.BareLaunch)({})),
        Layer.provide(Layer.succeedContext(launches)),
        Layer.provide(Layer.succeedContext(runtime)),
        Layer.provide(Layer.succeedContext(nativeServices)),
        Layer.provide(projectLayer),
        Layer.provide(folders),
        Layer.provide(settings),
        Layer.provide(
          attachmentRequests ? Adapters.layerFromAdapters([providerAdapter]) : Layer.empty,
        ),
        Layer.provide(layerLaunchCatalog),
        Layer.provide(Layer.mock(Git.GitVcsDriver)({})),
        Layer.provide(Layer.mock(Repositories.SourceControlRepositoryService)({})),
      );
    const tools = toolsFor(schedules);
    const lifecycleContext = Context.mergeAll(
      services,
      runtime,
      database,
      productionKernel ? Context.empty() : Context.make(RecoveryWaitState, lifecycleRecovery),
    );
    const lifecycle = yield* PeerTaskLifecycle.layer.pipe(
      Layer.provide(Layer.succeedContext(lifecycleContext)),
      Layer.build,
    );
    const pendingRequests = yield* PendingRequests.layer.pipe(
      Layer.provide(
        Layer.succeedContext(
          Context.mergeAll(
            lifecycleContext,
            baseServices,
            requestContext,
            Context.make(Environment.ServerEnvironment, {
              getEnvironmentId: Effect.succeed(descriptor.environmentId),
              getDescriptor: Effect.succeed(descriptor),
            }),
          ),
        ),
      ),
      Layer.build,
    );
    const peer = yield* servePeer(descriptor, tools, {
      ...(attachmentRequests
        ? { serverConfig: Context.get(nodeConfigContext, Config.ServerConfig) }
        : {}),
      database: db,
      registry,
      secretStore: Context.get(services, Secrets.ServerSecretStore),
      authorityServices: Context.mergeAll(
        lifecycleContext,
        lifecycle,
        pendingRequests,
        secretOwners,
        nativeBrowser ?? Context.empty(),
      ),
    }).pipe(Effect.provide(Layer.succeedContext(serverContext)));
    let nativeViewerRpcUrl: string | undefined;
    const nativeCommandOutputFinalized = yield* Queue.unbounded<ThreadId>();
    if (previewControls !== undefined) {
      const http = yield* sourceHttp.pipe(Layer.fresh, Layer.build);
      const address = Context.get(http, HttpServer.HttpServer).address;
      if (address._tag === "UnixPathAddress")
        return yield* Effect.die("Expected native RPC fixture socket");
      nativeViewerRpcUrl = `ws://127.0.0.1:${address.port}`;
      const viewerHandlers = HttpRouter.use((router) =>
        Effect.gen(function* () {
          yield* router.add(
            "GET",
            "/ws",
            Effect.gen(function* () {
              const request = yield* HttpServerRequest.HttpServerRequest;
              const session = yield* peer.auth.authenticateWebSocketUpgrade(request, "rpc");
              const { protocol, httpEffect } = yield* RpcServer.makeProtocolWithHttpEffectWebsocket;
              yield* RpcServer.make(nativeOwnerRpcGroup, {
                disableTracing: true,
                disableFatalDefects: true,
              }).pipe(
                Effect.provideService(RpcServer.Protocol, protocol),
                Effect.provide(
                  Layer.mergeAll(
                    nativeOwnerRpcGroup.toLayer({
                      ...BrowserStream.clientHandlers(session, fixtureBrowserViewers!),
                      [WS_METHODS.peerLinksList]: () =>
                        Context.get(services, PeerLinks.PeerLinks).list.pipe(
                          Effect.map((links) => ({ links })),
                        ),
                      [ORCHESTRATION_V2_WS_METHODS.subscribeCommandOutput]: (input) =>
                        Stream.unwrap(
                          Effect.annotateCurrentSpan({
                            "orchestration_v2.thread_id": input.threadId,
                          }).pipe(
                            Effect.as(
                              Option.getOrThrow(commandOutput)
                                .subscribe(input)
                                .pipe(
                                  Stream.ensuring(
                                    Queue.offer(nativeCommandOutputFinalized, input.threadId),
                                  ),
                                ),
                            ),
                          ),
                        ),
                    }),
                    RpcAuthorization.layer(session.scopes),
                    rpcInstrumentationLayer,
                  ),
                ),
                Effect.forkScoped,
              );
              return yield* Effect.acquireUseRelease(
                fixtureBrowserViewerSessions!.markConnected(session.sessionId),
                () =>
                  Effect.raceFirst(
                    httpEffect,
                    fixtureBrowserViewerSessions!
                      .awaitInvalidation(session.sessionId)
                      .pipe(Effect.as(HttpServerResponse.empty())),
                  ),
                () => fixtureBrowserViewerSessions!.markDisconnected(session.sessionId),
              );
            }),
          );
        }),
      );
      yield* HttpRouter.serve(viewerHandlers, { disableListenLog: true, disableLogger: true }).pipe(
        Layer.provide(Layer.succeedContext(http)),
        Layer.provide(RpcSerialization.layerJson),
        Layer.fresh,
        Layer.build,
      );
    }
    yield* Effect.addFinalizer(() =>
      Effect.forEach([...connectionScopes], (lifetime) => Scope.close(lifetime, Exit.void), {
        concurrency: "unbounded",
        discard: true,
      }),
    );
    const rebuildScheduler = Scope.close(scheduleScope, Exit.void).pipe(
      Effect.andThen(scheduleLayer.pipe(Layer.fresh, Layer.build)),
    );
    const restartSchedulerTransportWith = Effect.fn("PeerAuthority.restartSchedulerTransportWith")(
      function* (durableRequests: boolean) {
        const rebuilt = yield* rebuildScheduler;
        const requests = durableRequests
          ? yield* RequestRuntime.layer.pipe(
              Layer.provide(db),
              Layer.provide(nodeConfig),
              Layer.provide(Layer.succeedContext(baseServices)),
              Layer.fresh,
              Layer.build,
            )
          : Context.empty();
        const http = yield* sourceHttp.pipe(Layer.fresh, Layer.build);
        const renewed = yield* McpSessionRegistry.__testing
          .make()
          .pipe(
            Effect.provideService(
              PeerAuthority.PeerAuthority,
              Context.get(baseServices, PeerAuthority.PeerAuthority),
            ),
            Effect.provide(Layer.merge(environment, Layer.succeedContext(http))),
          );
        const nativeServices = durableRequests
          ? services
          : Context.omit(
              RequestReceipts.McpRequestReceipts,
              AttachmentStaging.AttachmentStaging,
            )(services);
        const currentAuthority = Context.mergeAll(
          lifecycleContext,
          lifecycle,
          pendingRequests,
          nativeBrowser ?? Context.empty(),
          secretOwners,
        );
        const authorityServices = durableRequests
          ? currentAuthority
          : Context.omit(
              RequestReceipts.McpRequestReceipts,
              AttachmentStaging.AttachmentStaging,
            )(currentAuthority);
        const restarted = yield* servePeer(
          descriptor,
          toolsFor(rebuilt, requests, nativeServices),
          {
            ...(attachmentRequests
              ? { serverConfig: Context.get(nodeConfigContext, Config.ServerConfig) }
              : {}),
            database: db,
            registry: renewed,
            secretStore: Context.get(services, Secrets.ServerSecretStore),
            authorityServices,
          },
        ).pipe(Effect.provide(Layer.succeedContext(http)));
        return {
          ...restarted,
          registry: renewed,
          scheduled: Context.get(rebuilt, ScheduledTasks.ScheduledTaskService),
          requestContext: requests,
        };
      },
      Effect.provide(hostServices),
    );
    const restartSchedulerTransport = restartSchedulerTransportWith(attachmentRequests);
    const launchValidation = yield* layerLaunchCatalog.pipe(Layer.build);
    return {
      ...peer,
      pendingRequests: Context.get(pendingRequests, PendingRequests.PendingRequests),
      noticeFailure,
      holdLaunchReply,
      dropLaunchResponses,
      dropCommittedToolReply,
      secretConsumes,
      loseSecretConfirmation,
      launchReplyHeld,
      releaseLaunchReply,
      dropAckResponses,
      ackResponseLost,
      holdResultPage,
      quietWaits,
      quietWaitsHeld,
      resultPageStarted,
      releaseResultPage,
      reopenDatabase,
      reconstructAutomation: reconstructAutomationWith(FetchHttpClient.layer),
      reconstructAutomationWith,
      environmentId: descriptor.environmentId,
      project,
      database,
      databasePath: Context.get(databaseConfig, Config.ServerConfig).dbPath,
      lifecycleRecovery,
      signals,
      runtime: Context.merge(runtime, launchValidation),
      requestContext,
      secretOwners,
      secretRequestClock,
      secretRequestAbort,
      secretRequestExits,
      secretValidationClock,
      secretValidationErrors,
      secretControlReplyGate,
      secretRemovals,
      secretCleanupName,
      secretCleanupClock,
      stopSecretOwner: Scope.close(secretOwnerScope, Exit.void),
      remoteSecretFault,
      secretPublished,
      releaseSecretPublication,
      rebuildSecretOwner: RemoteSecretRequests.layer.pipe(
        Layer.provide(faultSecretStore),
        Layer.provide(secretAuthority),
        Layer.provide(Layer.succeedContext(baseServices)),
        Layer.provide(db),
        Layer.provide(environment),
        Layer.provide(nodeConfig),
        Layer.fresh,
        Layer.build,
      ),
      nativeSecrets: remoteSecrets ? yield* nativeSecrets.pipe(Layer.build) : Context.empty(),
      config: Context.get(nodeConfigContext, Config.ServerConfig),
      browser,
      nativeBrowser:
        nativeBrowser === undefined
          ? undefined
          : Context.get(nativeBrowser, NativeBrowser.ServerBrowser),
      nativePreviewManager:
        nativeBrowser === undefined
          ? undefined
          : Context.get(nativeBrowser, NativePreviewManager.PreviewManager),
      fixtureDevice,
      fixtureViewers,
      nativeViewerRpcUrl,
      nativeCommandOutputFinalized,
      commandOutput: Option.getOrUndefined(commandOutput),
      fixtureBrowserViewers,
      fixtureBrowserViewerSessions,
      fixtureViewerSessions,
      deviceViewerRequests,
      deviceViewerCurrentHook,
      deviceViewerFrame,
      continuousDeviceMedia,
      deviceMediaChunks,
      deviceViewerInputs,
      deviceViewerHttpInputs,
      deviceViewerOutput,
      deviceProcessCalls,
      deviceProcessHook,
      deviceSummaryHook,
      deviceReadinessHook,
      deviceObservedScope,
      deviceMediaPaths,
      deviceMediaOutside,
      deviceScreenshotCalls,
      artifactResponseHook,
      artifactRequestHook,
      previewUploadContentTypes,
      deviceShutdownCalls,
      deviceHubPlatform,
      deviceHubBooted,
      deviceOldVersion,
      deviceCapabilities,
      deviceHostStarts,
      devicePlatformAvailable,
      deviceAgentStarts,
      deviceConfigurations,
      loseDeviceConfiguration,
      issueAssetUrl: (input: Parameters<typeof AssetAccess.issueAssetUrl>[0]) =>
        AssetAccess.issueAssetUrl(input).pipe(
          Effect.provide(
            Layer.mergeAll(
              WorkspacePaths.layer,
              ProjectFaviconResolver.layer.pipe(
                Layer.provide(Layer.mergeAll(WorkspacePaths.layer, T3ProjectFileLoader.layer)),
              ),
              AuthSessions.layer.pipe(Layer.provide(db)),
              Layer.succeedContext(services),
              Layer.succeedContext(runtime),
              Layer.succeedContext(requestContext),
              nativeBrowser === undefined ? Layer.empty : Layer.succeedContext(nativeBrowser),
              nodeConfig,
              environment,
            ),
          ),
        ),
      fixtureSettings: Context.get(settingsContext, Settings.ServerSettingsService),
      settingsWrites,
      loseSettingsConfirmation,
      rebuildSettings: realSettingsLayer.pipe(Layer.fresh, Layer.build),
      services,
      registry,
      scheduled: Context.get(schedules, ScheduledTasks.ScheduledTaskService),
      rebuildScheduler,
      restartSchedulerTransport,
      restartWithoutRequestsTransport: restartSchedulerTransportWith(false),
      scheduledChecks: Context.getOption(schedules, ScheduledChecks.ScheduledTaskChecks),
      holdManualLaunchReply,
      manualLaunchReplyHeld,
      holdManualSend,
      manualSendHeld,
      releaseManualSend,
      loseWorktreeReply,
      stopConnections: connection.stop,
      reconnect: connect,
      serverContext,
      threads: Context.get(runtime, Threads.ThreadManagementService),
      orchestrator: Context.get(runtime, Orchestrator.OrchestratorV2),
      sink: Context.get(runtime, EventSink.EventSinkV2),
      projections: Context.get(runtime, ProjectionStore.ProjectionStoreV2),
      links: Context.get(services, PeerLinks.PeerLinks),
      authority: Context.get(services, PeerAuthority.PeerAuthority),
      forwarding: Context.get(services, PeerForwarding.PeerForwarding),
      remote: Context.get(services, RemoteDelegation.RemoteDelegation),
    };
  },
  Effect.provide(Layer.merge(RepositoryIdentity.layer, layerFixtureHostServices)),
);

export type Node = Effect.Success<ReturnType<typeof makeNode>>;

export const traceAutomationValidation = Effect.fn("PeerAuthority.traceAutomationValidation")(
  function* (node: Node) {
    const sql = Context.get(node.database, SqlClient.SqlClient);
    yield* sql`CREATE TABLE fixture_automation_validation_attempts (frame_type TEXT NOT NULL)`;
    yield* sql`CREATE TRIGGER fixture_automation_validation_trace AFTER INSERT ON peer_control_frames
    WHEN json_extract(NEW.frame_json,'$.body.type') IN ('validate','capture-automation','capture-legacy-automation')
    BEGIN INSERT INTO fixture_automation_validation_attempts VALUES (json_extract(NEW.frame_json,'$.body.type')); END`;
    return sql`SELECT * FROM fixture_automation_validation_attempts`;
  },
);

export const automationWorkspace = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "automation384-workspace-" });
  const git = (args: ReadonlyArray<string>) =>
    ProcessRunner.ProcessRunner.pipe(
      Effect.flatMap((runner) => runner.run({ command: "git", args: ["-C", root, ...args] })),
      Effect.provide(ProcessRunner.layer),
    );
  expect((yield* git(["init", "--quiet", "--initial-branch=main"])).code).toBe(0);
  yield* fs.writeFileString(`${root}/README.md`, "Isolated native authority fixture");
  expect((yield* git(["add", "README.md"])).code).toBe(0);
  expect(
    (yield* git([
      "-c",
      "user.name=Fixture",
      "-c",
      "user.email=fixture@example.invalid",
      "commit",
      "--quiet",
      "-m",
      "Fixture baseline",
    ])).code,
  ).toBe(0);
  return root;
});

export const finishAutomationRun = Effect.fn("PeerAuthority.finishAutomationRun")(function* (
  node: Node,
  threadId: ThreadId,
  runId?: RunId,
) {
  const milestone = (statuses: ReadonlyArray<string>) =>
    node.threads
      .streamStoredEventsFrom({ threadId, afterSequence: 0, eventType: "run.updated" })
      .pipe(
        Stream.filter(
          (stored) =>
            stored.event.type === "run.updated" &&
            (runId === undefined || stored.event.payload.id === runId) &&
            statuses.includes(stored.event.payload.status),
        ),
        Stream.runHead,
        Effect.flatMap(Effect.fromOption),
      );
  const ended = yield* milestone(["waiting", "completed", "failed", "cancelled", "interrupted"]);
  if (ended.event.type === "run.updated" && ended.event.payload.status === "waiting")
    yield* Context.get(node.runtime, EffectWorker.OrchestrationEffectWorkerV2).drain();
  return yield* milestone(["completed", "failed", "cancelled", "interrupted"]);
});

export const awaitAutomationPrepared = Effect.fn("PeerAuthority.awaitAutomationPrepared")(
  function* (node: Node, threadId: ThreadId) {
    const run = (yield* node.threads.getThreadRecords(threadId, ["runs"])).runs.at(-1);
    if (run?.status === "preparing")
      yield* node.threads
        .streamStoredEventsFrom({ threadId, afterSequence: 0, eventType: "run.updated" })
        .pipe(
          Stream.filter(
            (stored) =>
              stored.event.type === "run.updated" &&
              stored.event.payload.id === run.id &&
              stored.event.payload.status !== "preparing",
          ),
          Stream.runHead,
          Effect.flatMap(Effect.fromOption),
        );
  },
);

export const awaitAutomationRunning = Effect.fn("PeerAuthority.awaitAutomationRunning")(function* (
  node: Node,
  threadId: ThreadId,
) {
  yield* awaitAutomationPrepared(node, threadId);
  yield* Context.get(node.runtime, EffectWorker.OrchestrationEffectWorkerV2).drain();
  yield* node.threads
    .streamStoredEventsFrom({ threadId, afterSequence: 0, eventType: "provider-turn.updated" })
    .pipe(
      Stream.filter(
        (stored) =>
          stored.event.type === "provider-turn.updated" &&
          stored.event.payload.status === "running",
      ),
      Stream.runHead,
      Effect.flatMap(Effect.fromOption),
    );
});

export const drainAutomationRun = Effect.fn("PeerAuthority.drainAutomationRun")(function* (
  node: Node,
  threadId: ThreadId,
  run: { readonly id: RunId; readonly status: string },
) {
  if (run.status === "preparing")
    yield* node.threads
      .streamStoredEventsFrom({ threadId, afterSequence: 0, eventType: "run.updated" })
      .pipe(
        Stream.filter(
          (event) =>
            event.event.type === "run.updated" &&
            event.event.payload.id === run.id &&
            event.event.payload.status !== "preparing",
        ),
        Stream.runHead,
        Effect.flatMap(Effect.fromOption),
      );
  yield* Context.get(node.runtime, EffectWorker.OrchestrationEffectWorkerV2).drain();
  return yield* finishAutomationRun(node, threadId, run.id);
});

export const decodeTyped383Open = Schema.decodeUnknownEffect(
  Schema.Struct({
    target: Schema.Struct({
      hostId: Schema.String,
      deviceId: Schema.String,
      toolSessionId: Schema.String,
    }),
    quickStart: Schema.String,
  }),
);

export const typed383Digest = Effect.fn("PeerAuthority.typed383Digest")(function* (
  bytes: Uint8Array,
) {
  const crypto = yield* Crypto.Crypto;
  return Hex.encode(yield* crypto.digest("SHA-256", bytes));
}, Effect.orDie);

export const decodeTyped383Command = Schema.decodeUnknownEffect(DeviceCommandResult);

export const decodeTyped383Upload = Schema.decodeUnknownEffect(PeerAttachmentUploadResult);

export const decodeTyped383Target = Schema.decodeUnknownEffect(
  Schema.Struct({ target: DeviceCommandTarget }),
);

export const decodeTyped383Artifact = Schema.decodeUnknownEffect(
  Schema.Struct({
    type: Schema.Literal("artifact"),
    artifact: PeerAttachmentReference,
  }),
);

export const assertTyped383SourceIdle = Effect.fn("PeerAuthority.assertTyped383SourceIdle")(
  function* (source: Node) {
    expect(yield* Ref.get(source.deviceHostStarts)).toBe(0);
    expect(yield* Ref.get(source.deviceAgentStarts)).toBe(0);
    expect(yield* Ref.get(source.deviceConfigurations)).toBe(0);
    expect(yield* Ref.get(source.deviceScreenshotCalls)).toBe(0);
    expect(yield* Ref.get(source.deviceShutdownCalls)).toBe(0);
    expect(source.deviceProcessCalls).toHaveLength(0);
  },
);

export const typed383Pair = Effect.fn("PeerAuthority.typed383Pair")(function* (
  id: string,
  legacyDeviceDomain = false,
  sourcePreferencesOwner = false,
  viewerOwner:
    | boolean
    | {
        readonly source: boolean;
        readonly destination: boolean;
        readonly destinationSocket?: boolean;
        readonly destinationBrowser?: boolean;
      } = false,
) {
  const a = yield* makeNode(descriptorOf(`${id}-a`, "A", { peerAuthority: 1 }), "disk", {
    productionKernel: true,
    attachmentRequests: true,
    preferencesOwner: sourcePreferencesOwner,
    deviceOwner: true,
    viewerOwner: typeof viewerOwner === "boolean" ? viewerOwner : viewerOwner.source,
    previewControlsOwner:
      typeof viewerOwner !== "boolean" && viewerOwner.destinationBrowser === true,
  });
  yield* Effect.addFinalizer(() => assertTyped383SourceIdle(a));
  const b = yield* makeNode(descriptorOf(`${id}-b`, "B", { peerAuthority: 1 }), "disk", {
    productionKernel: true,
    attachmentRequests: true,
    preferencesOwner: true,
    legacyDeviceDomain,
    deviceOwner: true,
    viewerOwner: typeof viewerOwner === "boolean" ? viewerOwner : viewerOwner.destination,
    viewerSocketOwner: typeof viewerOwner !== "boolean" && viewerOwner.destinationSocket === true,
    browserOwner: typeof viewerOwner !== "boolean" && viewerOwner.destinationBrowser === true,
  });
  yield* linkTo(a.links, b, "full-access");
  const parent = ThreadId.make(`${id}-parent`);
  yield* seedParent(a, parent);
  const launched = yield* nativeCall(a, parent, "t3_thread_launch", {
    environmentId: b.environmentId,
    projectId: b.project.id,
    title: id,
    message: "Wait",
    modelSelection: selection,
    clientRequestId: `${id}-child`,
  });
  expect(launched.isError, json(launched)).not.toBe(true);
  const child = (yield* decodeLaunchReceipt(launched.structuredContent)).threadId;
  yield* activate(b, child);
  const grant = (yield* b.authority.grants)[0]!;
  yield* b.authority.setGrant({ ...grant, device: true });
  const route = { environmentId: b.environmentId, contextThreadId: child };
  const open = (key: string) =>
    nativeCall(a, parent, "device_open", {
      ...route,
      deviceId: "fixture-device382",
      platform: "android",
      clientRequestId: key,
    });
  const footprint = Effect.fn("PeerAuthority.footprint")(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const file = yield* agentDeviceConfigPath(b.config.stateDir, LOCAL_DEVICE_HOST_ID, path);
    return {
      host: yield* Ref.get(b.deviceHostStarts),
      agent: yield* Ref.get(b.deviceAgentStarts),
      config: yield* Ref.get(b.deviceConfigurations),
      process: b.deviceProcessCalls.length,
      screenshots: yield* Ref.get(b.deviceScreenshotCalls),
      sessions: yield* b.fixtureDevice!.sessionsForThread(child),
      configuration: (yield* fs.exists(file)) ? yield* fs.readFileString(file) : null,
    };
  });
  return { a, b, parent, child, route, open, footprint };
});

export const decodeTyped383Example = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
);

export const drainStops = (node: Node) =>
  EffectWorker.OrchestrationEffectWorkerV2.pipe(
    Effect.flatMap((worker) => worker.drain()),
    Effect.provide(
      EffectWorker.layerWithOptions({ maxAttempts: 1 }).pipe(
        Layer.provide(
          Layer.succeed(EffectWorker.OrchestrationEffectExecutorV2, {
            execute: (effect) =>
              effect.request.type === "delegated-tasks.stop"
                ? node.threads
                    .stopDelegatedTasks({
                      threadId: effect.threadId,
                      commandId: effect.commandId,
                      reason: effect.request.reason,
                      forkRetirementStop: effect.request.forkRetirementStop,
                    })
                    .pipe(
                      Effect.mapError(
                        (cause) =>
                          new EffectWorker.OrchestrationEffectExecutionError({
                            effectId: effect.id,
                            effectType: effect.request.type,
                            cause,
                          }),
                      ),
                    )
                : Effect.void,
          }),
        ),
        Layer.provide(Layer.succeed(PeerAuthority.PeerAuthority, node.authority)),
        Layer.provide(Layer.succeedContext(node.runtime)),
      ),
    ),
  );

export const restoreAuthority = Effect.fn("PeerAuthority.restoreAuthority")(function* (node: Node) {
  const reopened = yield* node.reopenDatabase;
  return yield* PeerAuthority.PeerAuthority.pipe(
    Effect.provide(
      PeerAuthority.layer.pipe(
        Layer.provide(Layer.succeed(PeerLinks.PeerLinks, node.links)),
        Layer.provide(Layer.succeedContext(node.runtime)),
        Layer.provide(Layer.succeedContext(reopened)),
        Layer.provide(
          Layer.succeed(Environment.ServerEnvironment, {
            getEnvironmentId: Effect.succeed(node.environmentId),
            getDescriptor: Ref.get(node.descriptor),
          }),
        ),
        Layer.fresh,
      ),
    ),
  );
});

export const activate = Effect.fn("PeerAuthority.activate")(function* (
  node: Node,
  threadId: ThreadId,
) {
  const records = yield* node.threads.getThreadRecords(threadId, ["runs"]);
  const run = records.runs.at(-1);
  if (!run) return yield* Effect.die("Expected a persisted launch run");
  const now = yield* DateTime.now;
  const root = NodeId.make(`fixture-root:${run.id}`);
  yield* node.sink.write({
    events: [
      {
        id: EventId.make(`fixture-node:${run.id}`),
        type: "node.updated",
        threadId,
        runId: run.id,
        nodeId: root,
        driver,
        providerInstanceId: instanceId,
        occurredAt: now,
        payload: {
          id: root,
          threadId,
          runId: run.id,
          parentNodeId: null,
          rootNodeId: root,
          kind: "root_turn",
          status: "running",
          countsForRun: true,
          providerThreadId: null,
          providerTurnId: null,
          nativeItemRef: null,
          runtimeRequestId: null,
          checkpointScopeId: null,
          startedAt: now,
          completedAt: null,
        },
      },
      {
        id: EventId.make(`fixture-run:${run.id}`),
        type: "run.updated",
        threadId,
        runId: run.id,
        nodeId: root,
        providerInstanceId: instanceId,
        occurredAt: now,
        payload: { ...run, rootNodeId: root, status: "running", startedAt: now },
      },
    ],
  });
  return run.id;
});

export const seedParent = Effect.fn("PeerAuthority.seedParent")(function* (
  node: Node,
  threadId: ThreadId,
  linkOrigin?: OrchestrationV2LinkOrigin,
  prismRole?: "promachos",
) {
  yield* node.threads.dispatch({
    type: "thread.create",
    commandId: CommandId.make(`create:${threadId}`),
    threadId,
    projectId: node.project.id,
    title: threadId,
    modelSelection: selection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdBy: "user",
    creationSource: "web",
    ...(linkOrigin === undefined ? {} : { linkOrigin }),
    ...(prismRole === undefined ? {} : { prismRole }),
  });
  yield* node.threads.dispatch({
    type: "message.dispatch",
    commandId: CommandId.make(`send:${threadId}`),
    threadId,
    messageId: MessageId.make(`message:${threadId}`),
    text: "Work",
    attachments: [],
    dispatchMode: { type: "start_immediately" },
    createdBy: "user",
    creationSource: "web",
  });
  yield* activate(node, threadId);
});

export const caller = (node: Node, threadId: ThreadId): McpThreadInvocationScope => ({
  environmentId: node.environmentId,
  requestNamespace: `fixture:${threadId}`,
  issuedAt: 0,
  thread: { threadId, providerSessionId: `fixture:${threadId}`, providerInstanceId: instanceId },
  client: undefined,
  capabilities: new Set(["orchestration"]),
});

export const sendResult = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Struct({
      result: Schema.Struct({
        structuredContent: Schema.optionalKey(Schema.Unknown),
        isError: Schema.optionalKey(Schema.Boolean),
        content: Schema.Array(
          Schema.Struct({ type: Schema.String, text: Schema.optionalKey(Schema.String) }),
        ),
      }),
    }),
  ),
);

export const json = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

export const decodeTaskReceipt = Schema.decodeUnknownEffect(Schema.Struct({ taskId: NodeId }));

export const decodeCreatedThreads = Schema.decodeUnknownEffect(
  OrchestratorToolkit.tools.create_threads.successSchema,
);

export const decodeLaunchReceipt = Schema.decodeUnknownEffect(
  ProjectToolkit.tools.t3_thread_launch.successSchema,
);

export const nativeRpcCall = Effect.fn("PeerAuthority.nativeRpcCall")(function* (
  node: Node,
  threadId: ThreadId,
  tool: string,
  args: Record<string, unknown>,
  capabilities: McpThreadInvocationScope["capabilities"] = new Set([
    "orchestration",
    "preview",
    "device",
  ]),
  credential?: Effect.Success<ReturnType<Node["registry"]["issue"]>>,
) {
  const issued =
    credential ??
    (yield* node.registry.issue({
      threadId,
      providerInstanceId: instanceId,
      capabilities,
    }));
  const http = yield* HttpClient.HttpClient;
  const post = (body: unknown, sessionId?: string) =>
    http.execute(
      HttpClientRequest.post(issued.config.endpoint).pipe(
        HttpClientRequest.setHeaders({
          authorization: issued.config.authorizationHeader,
          accept: "application/json, text/event-stream",
          ...(sessionId === undefined
            ? {}
            : { "mcp-session-id": sessionId, "mcp-protocol-version": "2025-06-18" }),
        }),
        HttpClientRequest.setBody(HttpBody.text(json(body), "application/json")),
      ),
    );
  const initialized = yield* post({
    jsonrpc: "2.0",
    id: 0,
    method: "initialize",
    params: {
      protocolVersion: "2025-06-18",
      capabilities: {},
      clientInfo: { name: "isolated-native-agent", version: "1" },
    },
  });
  const sessionId = initialized.headers["mcp-session-id"];
  if (sessionId === undefined)
    return yield* Effect.die(
      `Native initialization failed: ${initialized.status}; credential origin=${new URL(issued.config.endpoint).origin}; bound origin=${node.url}`,
    );
  yield* post({ jsonrpc: "2.0", method: "notifications/initialized" }, sessionId);
  const result = yield* post(
    { jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: tool, arguments: args } },
    sessionId,
  );
  const text = yield* result.text;
  return text.startsWith("data:") ? text.slice(5).trim() : text;
}, Effect.provide(FetchHttpClient.layer));

export const nativeCall = (...input: Parameters<typeof nativeRpcCall>) =>
  nativeRpcCall(...input).pipe(
    Effect.flatMap(sendResult),
    Effect.map((reply) => reply.result),
  );

const decodeNativeInvalidParams = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Struct({
      jsonrpc: Schema.Literal("2.0"),
      id: Schema.Literal(1),
      error: Schema.Struct({
        code: Schema.Literal(-32602),
        message: Schema.String,
        data: Schema.optionalKey(Schema.Unknown),
      }),
    }),
  ),
);

export const nativeInvalidParams = (...input: Parameters<typeof nativeRpcCall>) =>
  nativeRpcCall(...input).pipe(Effect.flatMap(decodeNativeInvalidParams));

export const decodeSend = Schema.decodeUnknownEffect(
  OrchestratorToolkit.tools.t3_thread_send.successSchema,
);

export const sendNotice = Effect.fn("PeerAuthority.sendNotice")(function* (
  node: Node,
  threadId: ThreadId,
  environmentId: Node["environmentId"],
  parent: ThreadId,
  message: string,
  key: string,
) {
  const result = yield* nativeCall(node, threadId, "t3_thread_send", {
    environmentId,
    threadId: parent,
    message,
    mode: "auto",
    clientRequestId: key,
  });
  expect(result.isError, json(result.content)).not.toBe(true);
  return yield* decodeSend(result.structuredContent);
});

export const awaitNotice = (node: Node, threadId: ThreadId, text: string) =>
  node.orchestrator
    .streamStoredEventsFrom({ threadId, afterSequence: 0, eventType: "message.updated" })
    .pipe(
      Stream.filter(
        (stored) => stored.event.type === "message.updated" && stored.event.payload.text === text,
      ),
      Stream.runHead,
      Effect.flatMap(Effect.fromOption),
    );

export const attachedChild = (node: Node, parentThreadId: ThreadId, taskId: NodeId) =>
  node.orchestrator
    .streamStoredEventsFrom({
      threadId: parentThreadId,
      afterSequence: 0,
      eventType: "subagent.updated",
    })
    .pipe(
      Stream.filter(
        (stored) =>
          stored.event.type === "subagent.updated" &&
          stored.event.payload.id === taskId &&
          stored.event.payload.remoteChild?.threadId !== undefined,
      ),
      Stream.runHead,
      Effect.flatMap(Effect.fromOption),
      Effect.flatMap((stored) =>
        stored.event.type === "subagent.updated" &&
        stored.event.payload.remoteChild?.threadId !== undefined
          ? Effect.succeed(stored.event.payload.remoteChild.threadId)
          : Effect.die("Missing attached child"),
      ),
    );

export const delegate = Effect.fn("PeerAuthority.delegate")(function* (
  source: Node,
  destination: Node,
  parentId: ThreadId,
  key: string,
) {
  const { taskId } = yield* source.remote.delegate(caller(source, parentId), {
    task: "Return evidence",
    target: {
      environmentId: destination.environmentId,
      projectId: destination.project.id,
      providerInstanceId: instanceId,
      model: "gpt-5.4",
    },
    mode: "async",
    clientRequestId: key,
  });
  return { taskId, childId: yield* attachedChild(source, parentId, taskId) };
});

export const activeProvider = Effect.fn("PeerAuthority.activeProvider")(function* (
  node: Node,
  target: ThreadId,
  capabilities: Adapter.ProviderAdapterV2SessionRuntime["providerSession"]["capabilities"] = CodexProviderCapabilitiesV2,
) {
  const projection = yield* node.orchestrator.getThreadProjection(target);
  const run = projection.runs[0]!;
  const providerThread = projection.providerThreads[0]!;
  const now = yield* DateTime.now;
  const providerSession = {
    id: providerThread.providerSessionId!,
    driver,
    providerInstanceId: instanceId,
    status: "running" as const,
    cwd: process.cwd(),
    model: selection.model,
    capabilities,
    createdAt: now,
    updatedAt: now,
    lastError: null,
  };
  yield* node.sink.write({
    events: [
      {
        id: EventId.make(`${target}-${run.activeAttemptId}-attached`),
        type: "provider-session.attached",
        threadId: target,
        occurredAt: now,
        payload: providerSession,
      },
      {
        id: EventId.make(`${target}-${run.activeAttemptId}-turn`),
        type: "provider-turn.updated",
        threadId: target,
        runId: run.id,
        occurredAt: now,
        payload: {
          id: ProviderTurnId.make(`${target}-${run.activeAttemptId}-turn`),
          providerThreadId: providerThread.id,
          nodeId: run.rootNodeId!,
          runAttemptId: run.activeAttemptId,
          nativeTurnRef: null,
          ordinal: 1,
          status: "running",
          startedAt: now,
          completedAt: null,
        },
      },
    ],
  });
  const sessions = Context.get(node.runtime, ProviderSessionManager.ProviderSessionManagerV2);
  const spy = vi
    .spyOn(sessions, "get")
    .mockReturnValue(
      Effect.succeed(Option.some({ providerSession } as Adapter.ProviderAdapterV2SessionRuntime)),
    );
  yield* Effect.addFinalizer(() => Effect.sync(() => spy.mockRestore()));

  return run;
});

export const finishLifecycleRun = Effect.fn("PeerAuthority.finishLifecycleRun")(function* (
  node: Node,
  childId: ThreadId,
  label: string,
  status: "completed" | "failed" = "completed",
  text = label,
) {
  const runId = yield* activate(node, childId);
  const projection = yield* node.threads.getThreadRecords(childId, ["runs", "messages"]);
  const run = projection.runs.find((run) => run.id === runId)!;
  const now = yield* DateTime.now;
  yield* node.sink.write({
    commandId: CommandId.make(`fixture:finish:${label}`),
    events: [
      {
        id: EventId.make(`fixture:reply:${label}`),
        type: "message.updated",
        threadId: childId,
        runId,
        occurredAt: now,
        payload: {
          ...projection.messages[0]!,
          id: MessageId.make(`message:${label}`),
          runId,
          role: "assistant",
          text,
          streaming: false,
          createdAt: now,
          updatedAt: now,
        },
      },
      {
        id: EventId.make(`fixture:item:${label}`),
        type: "turn-item.updated",
        threadId: childId,
        runId,
        occurredAt: now,
        payload: {
          id: TurnItemId.make(`item:${label}`),
          type: "assistant_message",
          threadId: childId,
          runId,
          nodeId: run.rootNodeId,
          providerThreadId: null,
          providerTurnId: null,
          nativeItemRef: null,
          parentItemId: null,
          ordinal: run.ordinal * 100,
          status: "completed",
          title: null,
          startedAt: now,
          completedAt: now,
          updatedAt: now,
          messageId: MessageId.make(`message:${label}`),
          text,
          streaming: false,
        },
      },
      {
        id: EventId.make(`fixture:terminal:${label}`),
        type: "run.updated",
        threadId: childId,
        runId,
        occurredAt: now,
        payload: { ...run, status, completedAt: now },
      },
    ],
  });
  if (status === "failed")
    yield* RecoveryHistory.writeRecoveryOutcome({
      sourceRunId: runId,
      threadId: childId,
      status: "decided",
      outcome: "not_retryable",
      reason: "non_retryable",
    }).pipe(
      Effect.provide(Layer.succeedContext(node.database)),
      Effect.provideService(RecoverySignals.RecoveryWaitSignals, node.signals),
    );
  return runId;
});

export const decodeDiscardUpload = Schema.decodeUnknownEffect(PeerAttachmentUploadResult);

export const decodePending369Content = Schema.decodeUnknownEffect(
  ThreadToolkit.tools.t3_pending_request_read.successSchema,
);

export const decodePending369ClientResult = Schema.decodeUnknownEffect(RemotePendingRequestResult);

export const decodePending369LocalTask = Schema.decodeUnknownEffect(
  OrchestratorToolkit.tools.delegate_task.successSchema,
);

export const seedPending369 = Effect.fn("PeerAuthority.seedPending369")(function* (
  node: Node,
  threadId: ThreadId,
  suffix = "question",
) {
  const now = yield* DateTime.now;
  const requestId = RuntimeRequestId.make(`pending369:${threadId}:${suffix}`);
  const nodeId = NodeId.make(`node:${requestId}`);
  const common = { threadId, nodeId, occurredAt: now };
  yield* node.sink.write({
    events: [
      {
        ...common,
        id: EventId.make(`node:${requestId}`),
        type: "node.updated",
        payload: {
          id: nodeId,
          threadId,
          runId: null,
          parentNodeId: null,
          rootNodeId: nodeId,
          kind: "user_input_request",
          status: "waiting",
          countsForRun: false,
          providerThreadId: null,
          providerTurnId: null,
          nativeItemRef: null,
          runtimeRequestId: requestId,
          checkpointScopeId: null,
          startedAt: now,
          completedAt: null,
        },
      },
      {
        ...common,
        id: EventId.make(`request:${requestId}`),
        type: "runtime-request.updated",
        payload: {
          id: requestId,
          nodeId,
          providerTurnId: null,
          nativeRequestRef: null,
          kind: "user_input",
          status: "pending",
          responseCapability: { type: "message" },
          createdAt: now,
          resolvedAt: null,
        },
      },
      {
        ...common,
        id: EventId.make(`item:${requestId}`),
        type: "turn-item.updated",
        payload: {
          id: TurnItemId.make(`item:${requestId}`),
          type: "user_input_request",
          threadId,
          runId: null,
          nodeId,
          providerThreadId: null,
          providerTurnId: null,
          nativeItemRef: null,
          parentItemId: null,
          ordinal: 0,
          status: "waiting",
          title: null,
          startedAt: now,
          completedAt: null,
          updatedAt: now,
          requestId,
          responseMode: "message",
          questions: [{ id: "color", header: "Color", question: "Which color?", options: [] }],
        },
      },
    ],
  });
  return requestId;
});

export const decodePendingWsReply = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Struct({
      _tag: Schema.String,
      requestId: Schema.optional(Schema.String),
      exit: Schema.optional(
        Schema.Struct({
          _tag: Schema.String,
          value: Schema.optional(Schema.Unknown),
          cause: Schema.optional(Schema.Unknown),
        }),
      ),
    }),
  ),
);

class DeviceViewerTestApi extends HttpApi.make("environment").add(
  EnvironmentHttpApi.groups.devices,
) {}

export const decodeDeviceViewerAccess = Schema.decodeUnknownEffect(DeviceViewerAccess);

export const decodeProducerCounter = Schema.decodeUnknownEffect(
  Schema.declare<(time: number) => unknown>(
    (value): value is (time: number) => unknown => typeof value === "function",
  ),
);

export const decodeProducerClickCount = Schema.decodeUnknownEffect(Schema.Int);

export const decodeProducerInput = Schema.decodeUnknownEffect(PreviewStreamInput);

export type NativeTestBrowserSender = (
  target: {
    threadId: string;
    tabId: string;
    maxWidth: number;
    maxHeight: number;
    access: {
      httpBase: string;
      wsBase: string;
      query: Record<string, string>;
      credentials: boolean;
      canOperate?: boolean;
    };
  },
  events: {
    onFrame: (jpeg: ArrayBuffer) => void;
    onViewport: (size: { width: number; height: number }) => void;
    onControl: (control: {
      controller: string;
      generation: number;
      dialog?: { message: string } | null;
    }) => void;
    onConnectedChange: (connected: boolean) => void;
    onUnauthorized: () => void;
    onInputRejected?: (message: string) => void;
  },
) => {
  send: (input: PreviewStreamInput) => boolean;
  stop: () => void;
};

const decodeBrowserSender = Schema.decodeUnknownSync(
  Schema.Struct({
    createPreviewStreamClient: Schema.declare<NativeTestBrowserSender>(
      (value): value is NativeTestBrowserSender => typeof value === "function",
    ),
  }),
);

export const { createPreviewStreamClient } = decodeBrowserSender(
  await import(
    /* @vite-ignore */ new URL(
      "../../../../packages/client-runtime/src/preview/serverBrowserStream.ts",
      import.meta.url,
    ).href
  ),
);

export const browserExternalContexts: ReturnType<typeof makeExternalBrowserContext>[] = [];

export const browserExternalBoundary = { beforeContextReturn: async () => {} };

vi.mock("../preview/ServerBrowserContexts.ts", () => ({
  presentAsChrome: async () => {},
  ServerBrowserContexts: class {
    private readonly options: {
      onContextClose?: (context: import("playwright-core").BrowserContext) => void;
    };
    constructor(options: {
      onContextClose?: (context: import("playwright-core").BrowserContext) => void;
    }) {
      this.options = options;
    }
    async contextFor() {
      const context = makeExternalBrowserContext(this.options.onContextClose);
      browserExternalContexts.push(context);
      await browserExternalBoundary.beforeContextReturn();
      return context;
    }
    async close() {}
  },
}));

vi.mock("effect/Queue", { spy: true });

const nativeOwnerRpcGroup = RpcGroup.make(
  ...Array.from(CoreServerWsRpcGroup.requests.values()).filter(
    (
      rpc,
    ): rpc is Extract<
      RpcGroup.Rpcs<typeof CoreServerWsRpcGroup>,
      { readonly _tag: typeof WS_METHODS.peerLinksList }
    > => rpc._tag === WS_METHODS.peerLinksList,
  ),
  ...Array.from(OrchestrationServerWsRpcGroup.requests.values()).filter(
    (
      rpc,
    ): rpc is Extract<
      RpcGroup.Rpcs<typeof OrchestrationServerWsRpcGroup>,
      { readonly _tag: typeof ORCHESTRATION_V2_WS_METHODS.subscribeCommandOutput }
    > => rpc._tag === ORCHESTRATION_V2_WS_METHODS.subscribeCommandOutput,
  ),
  ...Array.from(ForkWsRpc.group.requests.values()).filter(
    (
      rpc,
    ): rpc is Extract<
      RpcGroup.Rpcs<typeof ForkWsRpc.group>,
      { readonly _tag: typeof WS_METHODS.previewViewerOpen | typeof WS_METHODS.previewViewerClose }
    > => rpc._tag === WS_METHODS.previewViewerOpen || rpc._tag === WS_METHODS.previewViewerClose,
  ),
);

export const makeNativePromiseBridge = Effect.gen(function* () {
  const jobs = yield* Queue.unbounded<Effect.Effect<void>>();
  yield* Effect.forever(Queue.take(jobs).pipe(Effect.flatMap((job) => job))).pipe(
    Effect.forkScoped,
  );
  return <A, E>(effect: Effect.Effect<A, E>) =>
    new Promise<A>((resolve, reject) => {
      const job = Effect.matchCauseEffect(effect, {
        onFailure: (cause) => Effect.sync(() => reject(Cause.squash(cause))),
        onSuccess: (value) => Effect.sync(() => resolve(value)),
      });
      if (!Queue.offerUnsafe(jobs, job)) reject(new Error("Native callback scope is closed"));
    });
});

export const idleResource383Pair = Effect.fn("PeerAuthority.idleResource383Pair")(function* (
  id: string,
  destinationInstance = ProviderInstanceId.make("idle-resource-destination-provider"),
) {
  const a = yield* makeNode(descriptorOf(`${id}-a`, "A", { peerAuthority: 1 }), "disk", {
    productionKernel: true,
    attachmentRequests: true,
    deviceOwner: true,
    previewControlsOwner: true,
  });
  yield* Effect.addFinalizer(() => assertTyped383SourceIdle(a));
  const b = yield* makeNode(descriptorOf(`${id}-b`, "B", { peerAuthority: 1 }), "disk", {
    productionKernel: true,
    attachmentRequests: true,
    preferencesOwner: true,
    browserOwner: true,
    deviceOwner: true,
    previewControlsOwner: true,
  });
  yield* linkTo(a.links, b, "full-access");
  const parent = ThreadId.make(`${id}-source`);
  const child = ThreadId.make(`${id}-existing-idle`);
  yield* seedParent(a, parent);
  yield* b.threads.dispatch({
    type: "thread.create",
    commandId: CommandId.make(`${id}-create`),
    threadId: child,
    projectId: b.project.id,
    title: "Existing idle resource context",
    modelSelection: {
      ...selection,
      instanceId: destinationInstance,
    },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdBy: "user",
    creationSource: "web",
  });
  const enrolled = yield* nativeCall(a, parent, "orchestrator_capabilities", {
    environmentId: b.environmentId,
  });
  expect(enrolled.isError, json(enrolled)).not.toBe(true);
  const grant = (yield* b.authority.grants)[0]!;
  yield* b.authority.setGrant({
    ...grant,
    resourceScope: "environment-resources",
    preview: true,
    device: true,
    onward: false,
  });
  const caps = new Set(["orchestration", "preview", "device"] as const);
  const credential = yield* a.registry.issue({
    threadId: parent,
    providerInstanceId: instanceId,
    capabilities: caps,
  });
  const route = { environmentId: b.environmentId, contextThreadId: child };
  const call = (name: string, input: Record<string, unknown> = {}, token = credential) =>
    nativeCall(a, parent, name, { ...route, ...input }, caps, token);
  const assertIdle = Effect.gen(function* () {
    expect((yield* b.threads.getThreadShell(child))?.activeRunId).toBeNull();
    expect((yield* b.threads.getThreadShell(child))?.providerInstanceId).toBe(destinationInstance);
    expect(yield* b.registry.scopeForThread(child)).toBeUndefined();
  });
  yield* assertIdle;
  return { a, b, parent, child, grant, caps, credential, route, call, assertIdle };
});

export const backgroundRoster383 = Effect.fn("PeerAuthority.backgroundRoster383")(function* (
  node: Effect.Success<ReturnType<typeof makeNode>>,
  threadId: ThreadId,
  pending: boolean,
  completeForeground = false,
) {
  const projection = yield* node.orchestrator.getThreadProjection(threadId);
  const providerThread = projection.providerThreads[0]!;
  const now = yield* DateTime.now;
  yield* node.sink.write({
    events: [
      {
        id: EventId.make(
          `background383-${pending}-${yield* Crypto.Crypto.pipe(Effect.flatMap((c) => c.randomUUIDv4()))}`,
        ),
        type: "provider-thread.updated",
        threadId,
        occurredAt: now,
        payload: {
          ...providerThread,
          status: "idle",
          updatedAt: now,
          pendingBackgroundTasks: pending
            ? [
                {
                  taskId: "native-background",
                  kind: "subagent",
                  description: "Native background resource task",
                },
              ]
            : [],
        },
      },
    ],
  });
  if (completeForeground) {
    const run = projection.runs.at(-1)!;
    yield* node.sink.write({
      events: [
        {
          id: EventId.make(`background383-complete-${run.id}`),
          type: "run.updated",
          threadId,
          runId: run.id,
          occurredAt: now,
          payload: { ...run, status: "completed", completedAt: now },
        },
      ],
    });
  }
});

export const localBackground383 = Effect.fn("PeerAuthority.localBackground383")(function* (
  id: string,
) {
  const node = yield* makeNode(descriptorOf(id, "local", { peerAuthority: 1 }), "disk", {
    productionKernel: true,
    attachmentRequests: true,
    preferencesOwner: true,
    browserOwner: true,
    deviceOwner: true,
    previewControlsOwner: true,
  });
  const threadId = ThreadId.make(id);
  yield* seedParent(node, threadId);
  const caps = new Set(["orchestration", "preview", "device"] as const);
  const credential = yield* node.registry.issue({
    threadId,
    providerInstanceId: instanceId,
    capabilities: caps,
  });
  const call = (name: string, input: Record<string, unknown> = {}, token = credential) =>
    nativeCall(node, threadId, name, input, caps, token);
  return { node, threadId, caps, credential, call };
});

export const stageInput383 = Effect.fn("PeerAuthority.stageInput383")(function* (
  call: (name: string, input: Record<string, unknown>) => ReturnType<typeof nativeCall>,
  origin: string,
  name: string,
  bytes: Uint8Array,
  key: string,
) {
  const upload = {
    type: "file" as const,
    name,
    mimeType: name.endsWith(".apk") ? "application/vnd.android.package-archive" : "text/plain",
    sizeBytes: bytes.length,
  };
  const prepared = yield* call("t3_attachment_prepare_upload", {
    upload,
    digest: yield* typed383Digest(bytes),
    clientRequestId: key,
  });
  expect(prepared.isError, json(prepared)).not.toBe(true);
  const issued = yield* decodeTyped383Upload(prepared.structuredContent);
  const http = yield* HttpClient.HttpClient;
  expect(
    (yield* http.execute(
      HttpClientRequest.post(`${origin}${issued.relativeUrl}`).pipe(
        HttpClientRequest.setBody(HttpBody.uint8Array(bytes)),
      ),
    )).status,
  ).toBe(204);
  const inspected = yield* call("t3_attachment_inspect", {
    source: { type: "upload", attachmentId: issued.attachmentId, upload },
  });
  expect(inspected.isError, json(inspected)).not.toBe(true);
  return (yield* Schema.decodeUnknownEffect(
    Schema.Struct({
      inspection: Schema.Struct({
        stage: Schema.Literal("staged"),
        reference: PeerAttachmentReference,
      }),
    }),
  )(inspected.structuredContent)).inspection.reference;
});
