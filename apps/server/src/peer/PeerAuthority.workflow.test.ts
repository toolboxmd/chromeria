import * as Tracer from "effect/Tracer";
import { DeviceStandardToolkit as LegacyDeviceToolkit } from "../device/testing/LegacyDeviceTools.fixture.ts";
import * as LegacyDeviceHandlers from "../device/testing/LegacyDeviceHandlers.fixture.ts";
import * as Crypto from "effect/Crypto";
import * as Hex from "effect/encoding/Hex";
import { clientHandlers as pendingRequestClientHandlers } from "../childThreads/PendingRequestIngress.ts";
import * as RpcAuthorization from "../auth/RpcAuthorization.ts";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServer from "effect/http/HttpServer";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import { RpcServer, RpcSerialization } from "effect/rpc";
import {
  PendingRequestClientRpcGroup,
  RemotePendingRequestResult,
  WS_METHODS,
} from "@t3tools/contracts";
import * as PendingRequests from "../childThreads/PendingRequestService.ts";
import * as RuntimeRequests from "../orchestration-v2/RuntimeRequestService.ts";
import * as AuthSessions from "../persistence/AuthSessions.ts";
import { AuthSessionId, RuntimeRequestId } from "@t3tools/contracts";
import * as AttachmentStaging from "../assets/AttachmentStaging.ts";
import * as AttachmentClaims from "../orchestration-v2/AttachmentClaims.ts";
import * as Invocation from "../mcp/McpInvocationContext.ts";
import { PeerAttachmentUploadResult } from "@t3tools/contracts";
import * as Queue from "effect/Queue";
import { dual } from "effect/Function";
import * as Path from "effect/Path";
import * as NativeDevices from "../device/DeviceService.ts";
import * as DeviceHost from "../device/DeviceHost.ts";
import { agentDeviceConfigPath } from "../device/AgentDeviceTarget.ts";
import { AGENT_DEVICE_VERSION, DEVICE_HUB_VERSION } from "../device/DeviceToolchain.ts";
import * as Net from "@t3tools/shared/Net";
import { LOCAL_DEVICE_HOST_ID, DeviceId } from "@t3tools/contracts";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import * as PullRequestReviewGuideService from "../pullRequest/PullRequestReviewGuideService.ts";
import {
  McpSavedSnapshotReceipt,
  McpScheduledMutationReceipt,
  SecretRef,
  SecretRequestError,
} from "@t3tools/contracts";
import * as ScheduleMutation from "../scheduledTasks/mutationReceipts.ts";
import { IssuesToolkit } from "../mcp/toolkits/issues/tools.ts";
import * as IssueHandlers from "../mcp/toolkits/issues/handlers.ts";
import * as IssueLinks from "../issueLinks/IssueLinks.ts";
import * as ClosingReferences from "../issueLinks/closingReferences.ts";
import * as WorktreeMcp from "../mcp/WorktreeMcpService.ts";
import { WorktreeToolkit } from "../mcp/toolkits/worktree/tools.ts";
import * as WorktreeHandlers from "../mcp/toolkits/worktree/handlers.ts";
import * as VcsStatus from "../vcs/VcsStatusBroadcaster.ts";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as Sink from "effect/Sink";
import * as ScheduledChecks from "../scheduledTaskChecks/ScheduledTaskChecks.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as AttachmentUpload from "../assets/AttachmentUpload.ts";
import {
  PeerAttachmentReference,
  DeviceCommandTarget,
  DeviceCommandResult,
} from "@t3tools/contracts";
import * as SpectrumLaunch from "../spectrum/LaunchService.ts";
import * as SpectrumMcp from "../spectrum/mcpToolkit.ts";
import * as SpectrumStore from "../spectrum/store.ts";
import * as CheckStore from "../scheduledTaskChecks/store.ts";
import * as Prism from "../prism/PrismService.ts";
import * as HtmlRender from "../htmlRender/HtmlRender.ts";
import * as PreviewBrowser from "../preview/PreviewBrowser.ts";
import { resolveAttachmentPathById } from "../attachmentStore.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import * as ProjectEnrichment from "../project/ProjectEnrichmentService.ts";
import * as ProjectFavicon from "../project/ProjectFaviconResolver.ts";
import * as GitManager from "../git/GitManager.ts";
import * as VcsRegistry from "../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as Fiber from "effect/Fiber";
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
import { resolveAttachmentPath } from "../attachmentStore.ts";
import * as FileSystem from "effect/FileSystem";
import * as PlatformError from "effect/PlatformError";
import { EnvironmentToolkit } from "../mcp/toolkits/environment/tools.ts";
import * as BrowserBroker from "../mcp/PreviewAutomationBroker.ts";
import * as Instances from "../provider/ProviderInstanceRegistry.ts";
import type { ProviderInstance } from "@t3tools/provider-core/server/driver";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as NativeSessions from "../mcp/McpSessionRegistry.testkit.ts";
import * as Checkpoints from "../checkpointing/CheckpointStore.ts";
import * as ProviderCatalog from "../provider/ProviderRegistry.ts";
import * as RecoveryStore from "../prism/RecoveryStore.ts";
import * as PeerTaskLifecycle from "./PeerTaskLifecycle.ts";
import { RecoveryWaitState } from "./recoveryWait.ts";
import * as RecoveryHistory from "../prism/RecoveryHistory.ts";
import * as RecoverySignals from "./recoveryWaitSignals.ts";
import { PeerDispatchAuthority } from "./peerEffectAuthority.ts";
import { vi } from "vite-plus/test";

// Preserve real queue execution while observing the external delivery boundary.
vi.mock("effect/Queue", { spy: true });
import * as ProviderSessionManager from "../orchestration-v2/ProviderSessionManager.ts";
import { ProviderTurnId } from "@t3tools/contracts";
import { assertNotLinked } from "../mcp/linkOrigin.ts";
import * as Clock from "effect/Clock";
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
import { NodeHttpServer } from "@effect/platform-node";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
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
} from "@t3tools/contracts";
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
import type { McpThreadInvocationScope } from "../mcp/McpInvocationContext.ts";

const instanceId = ProviderInstanceId.make("codex");
const driver = ProviderDriverKind.make("codex");
const selection = { instanceId, model: "gpt-5.4" };
const decodeSavedPngReceipt = Schema.decodeUnknownEffect(McpSavedSnapshotReceipt);
const decodeScheduledTaskList = Schema.decodeUnknownEffect(
  OrchestratorToolkit.tools.list_scheduled_tasks.successSchema,
);
const adapter = {
  instanceId,
  driver,
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("The isolated workflow never starts a provider process"),
} as Adapter.ProviderAdapterV2["Service"];

// A graph can dial a later-created server. Close all graph clients before any
// server drains requests, including bounded lifecycle waits and reverse control streams.
const graphConnections = new WeakMap<Scope.Scope, Set<Scope.Closeable>>();

/** Real commands, projections, receipts and intake; provider execution remains an external fixture. */
const makeNode = (
  descriptor: ExecutionEnvironmentDescriptor,
  storage: "memory" | "disk" = "memory",
  {
    productionKernel = false,
    attachmentRequests = false,
    preferencesOwner = false,
    nativeOwners = false,
    deviceOwner = false,
    legacyDeviceDomain = false,
    commandSpawner,
    ownerWorkspace,
  }: {
    readonly productionKernel?: boolean;
    readonly attachmentRequests?: boolean;
    readonly preferencesOwner?: boolean;
    readonly nativeOwners?: boolean;
    readonly deviceOwner?: boolean;
    readonly legacyDeviceDomain?: boolean;
    readonly commandSpawner?: ChildProcessSpawner.ChildProcessSpawner["Service"];
    readonly ownerWorkspace?: string;
  } = {},
) =>
  Effect.gen(function* () {
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
        }).pipe(Layer.fresh, Layer.build);
    const settings = Layer.succeedContext(settingsContext);
    const enrichment = ProjectEnrichment.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          Layer.succeed(RepositoryIdentity.RepositoryIdentityResolver, {
            resolve: () => Effect.succeed(null),
          }),
          Layer.succeed(ProjectFavicon.ProjectFaviconResolver, {
            resolvePath: () => Effect.succeed(null),
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
    const testInstance = {
      instanceId,
      driverKind: driver,
      displayName: "Deterministic native boundary",
      enabled: true,
      continuationIdentity: { driverKind: driver, continuationKey: "production-binding" },
      snapshot: { getSnapshot: Effect.succeed({}) } as unknown as ProviderInstance["snapshot"],
      textGeneration: {} as ProviderInstance["textGeneration"],
      orchestrationAdapter: adapter,
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
      Layer.provide(NativeSessions.layer),
      Layer.provide(McpProviderSessions.layer),
      Layer.provide(Layer.mock(Checkpoints.CheckpointStore)({})),
      Layer.provide(Layer.mock(ProviderCatalog.ProviderRegistry)({})),
      Layer.provide(Layer.mock(GitWorkflow.GitWorkflowService)({})),
      Layer.provide(
        Layer.mock(SourceProviders.SourceControlProviderRegistry)({
          resolveLink: () => Effect.die("No external repository host in this fixture"),
        }),
      ),
      Layer.provide(projectLayer),
      Layer.provide(nodeConfig),
      Layer.provide(settings),
      Layer.provide(db),
    );
    const runtime = yield* productionKernel
      ? production.pipe(Layer.fresh, Layer.build)
      : Runtime.layerWithRegistry(
          { name: `peer-authority-${descriptor.label}` },
          Adapters.layerFromAdapters([adapter]),
          { databaseLayer: db, runEffectWorker: false },
        ).pipe(Layer.fresh, Layer.build);
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
    const baseServices = yield* Layer.mergeAll(noticeDelivery, PeerAuthority.layer).pipe(
      Layer.provideMerge(PeerLinks.layer),
      Layer.provideMerge(Secrets.layer),
      Layer.provide(Layer.succeedContext(runtime)),
      Layer.provide(db),
      Layer.provide(environment),
      Layer.provide(FetchHttpClient.layer),
      Layer.provide(nodeConfig),
      Layer.fresh,
      Layer.build,
    );
    const requestContext = attachmentRequests
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
          taskLifecycle: (...args: Parameters<NonNullable<typeof peers.taskLifecycle>>) =>
            peers.taskLifecycle!(...args).pipe(
              Effect.flatMap((result) =>
                Effect.gen(function* () {
                  if (args[2].operation.type === "resultText" && (yield* Ref.get(holdResultPage))) {
                    yield* Deferred.succeed(resultPageStarted, undefined);
                    yield* Deferred.await(releaseResultPage);
                  }
                  if (
                    (args[2].operation.type === "acknowledge" ||
                      args[2].operation.type === "stop") &&
                    (yield* Ref.get(dropAckResponses)) > 0
                  ) {
                    yield* Ref.update(dropAckResponses, (count) => count - 1);
                    yield* Deferred.succeed(ackResponseLost, undefined);
                    return yield* new OrchestratorMcpFailure({
                      code: "orchestration_error",
                      message:
                        "Fixture dropped the acknowledgement response after destination commit.",
                    });
                  }
                  return result;
                }),
              ),
            ),
        };
      }),
    ).pipe(Layer.provide(PeerMcpClient.layer));
    const connect = () =>
      Effect.gen(function* () {
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
          Layer.provide(Layer.mock(RepositoryIdentity.RepositoryIdentityResolver)({})),
          Layer.provide(environment),
          Layer.provide(FetchHttpClient.layer),
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
      Layer.mock(GitWorkflow.GitWorkflowService)({}),
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
    const secretConsumes = yield* Ref.make(0);
    const loseSecretConfirmation = yield* Ref.make(false);
    const nativeSecrets = SecretRequests.layer.pipe(
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
              consume: (input: Parameters<typeof actual.consume>[0]) =>
                Effect.gen(function* () {
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
        Layer.succeed(
          ScheduledTasks.ScheduledTaskWebhookOrigin,
          Effect.succeed({ relayHookBaseUrl: "https://hooks.example.invalid/v1/hooks/fixture" }),
        ),
      ),
      Layer.provide(Scheduler.layer),
      Layer.provide(scheduleSecrets),
      Layer.provide(scheduledLaunch),
      Layer.provide(Prism.layer.pipe(Layer.provide(settings), Layer.provide(layerLaunchCatalog))),
      Layer.provide(ProcessRunner.layer),
      Layer.provide(
        commandSpawner === undefined
          ? Layer.empty
          : Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, commandSpawner),
      ),
      Layer.provide(ProjectStore.layer.pipe(Layer.provide(db))),
      Layer.provide(launchDependencies),
    );
    const scheduleScope = yield* Scope.make();
    yield* Effect.addFinalizer(() => Scope.close(scheduleScope, Exit.void));
    const schedules = yield* scheduleLayer.pipe(Layer.fresh, Layer.buildWithScope(scheduleScope));
    if (attachmentRequests) {
      yield* SpectrumStore.ensureSpectrumSchema.pipe(Effect.provide(db));
      yield* CheckStore.ensureCheckSchema.pipe(Effect.provide(db));
    }
    const spectrumLayer = SpectrumLaunch.layer.pipe(
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
    const handoffGitManager = GitManager.layer.pipe(
      Layer.provide(actualGit),
      Layer.provide(
        Layer.mock(SourceProviders.SourceControlProviderRegistry)({
          resolveLink: () => Effect.die("No external repository host in this fixture"),
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
    const browser = yield* BrowserBroker.make;
    const previewBrowser = Layer.succeed(PreviewBrowser.PreviewBrowser, {
      installed: Effect.succeedNone,
      executable: Effect.die("This isolated fixture cannot launch a browser"),
    });
    const htmlLayer = HtmlRender.layer.pipe(
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
    let fixtureDevice: NativeDevices.DeviceService["Service"] | undefined;
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
            run: (input) =>
              Effect.gen(function* () {
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
                  stdout: JSON.stringify({ success: true, data }),
                  stderr: "",
                  timedOut: false,
                  stdoutTruncated: false,
                  stderrTruncated: false,
                  stdoutInvalidUtf8: false,
                  stderrInvalidUtf8: false,
                };
              }),
          });
          const ready: DeviceHost.DeviceHostReady = {
            nodePath: "/remote-host/node-must-never-execute",
            hub: { origin: "http://device382.invalid" },
            helpers: { serveSimAxSettings: null, serveSimCli: null },
            run: () => Effect.succeed({ code: 0, stdout: "", stderr: "" }),
          };
          const host: DeviceHost.DeviceHost["Service"] = {
            id: LOCAL_DEVICE_HOST_ID,
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
            ensureReady: () =>
              Effect.gen(function* () {
                yield* yield* Ref.get(deviceReadinessHook);
                // A direct native UI setup call has no MCP request identity.
                // Every registered action must be started before preparation.
                if ((yield* RequestReceipts.CurrentRequest) !== undefined) yield* assertStarted;
              }).pipe(
                Effect.andThen(Ref.update(deviceHostStarts, (n) => n + 1)),
                Effect.andThen(
                  fs
                    .writeFileString(
                      `${config.stateDir}/fixture-device-host-started`,
                      "Isolated native preparation footprint",
                    )
                    .pipe(Effect.orDie),
                ),
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
            current: Effect.succeed(null),
            stopAgent: Effect.void,
            stop: Effect.void,
          };
          const hub = HttpClient.make((request) =>
            Effect.gen(function* () {
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
                yield* assertStarted;
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
          return Layer.succeed(NativeDevices.DeviceService, fixtureDevice);
        })
      : Layer.mock(NativeDevices.DeviceService)({});

    const toolsFor = (
      scheduleServices: typeof schedules,
      requests: typeof requestContext = requestContext,
      nativeServices = services,
    ) =>
      Layer.mergeAll(
        McpHttpServer.layerProjectRegistration,
        McpHttpServer.layerOrchestratorToolkit,
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
              McpHttpServer.layerPreviewToolkit,
              McpHttpServer.layerThreadToolkit,
              McpHttpServer.layerHtmlToolkit,
              handoff,
              McpHttpServer.toolkitRegistration(SpectrumMcp.SpectrumToolkit, SpectrumMcp.layer),
            ]
          : []),
      ).pipe(
        Layer.provide(Layer.mergeAll(spectrumLayer, htmlLayer, previewBrowser)),
        Layer.provide(
          Layer.mergeAll(
            Layer.mock(PullRequestReviewGuideService.PullRequestReviewGuideService)({}),
            deviceLayer,
          ),
        ),
        Layer.provide(ThreadSearch.layer.pipe(Layer.provide(db))),
        Layer.provide(Layer.succeed(BrowserBroker.PreviewAutomationBroker, browser)),
        Layer.provide(Layer.succeedContext(requests)),
        Layer.provide(nodeConfig),
        Layer.provide(Layer.succeedContext(baseServices)),
        Layer.provide(Layer.succeedContext(scheduleServices)),
        Layer.provide(Layer.mock(SecretRequests.SecretRequests)({})),
        Layer.provide(Layer.mock(BareLaunch.BareLaunch)({})),
        Layer.provide(Layer.succeedContext(launches)),
        Layer.provide(Layer.succeedContext(runtime)),
        Layer.provide(Layer.succeedContext(nativeServices)),
        Layer.provide(projectLayer),
        Layer.provide(folders),
        Layer.provide(settings),
        Layer.provide(attachmentRequests ? Adapters.layerFromAdapters([adapter]) : Layer.empty),
        Layer.provide(layerLaunchCatalog),
        Layer.provide(Layer.mock(Git.GitVcsDriver)({})),
        Layer.provide(Layer.mock(Repositories.SourceControlRepositoryService)({})),
      );
    const tools = toolsFor(schedules);
    const serverContext = yield* NodeHttpServer.layerTest.pipe(Layer.fresh, Layer.build);
    const registry = yield* McpSessionRegistry.__testing
      .make()
      .pipe(
        Effect.provideService(
          PeerAuthority.PeerAuthority,
          Context.get(baseServices, PeerAuthority.PeerAuthority),
        ),
        Effect.provide(Layer.merge(environment, Layer.succeedContext(serverContext))),
      );
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
      authorityServices: Context.mergeAll(lifecycleContext, lifecycle, pendingRequests),
    }).pipe(Effect.provide(Layer.succeedContext(serverContext)));
    yield* Effect.addFinalizer(() =>
      Effect.forEach([...connectionScopes], (lifetime) => Scope.close(lifetime, Exit.void), {
        concurrency: "unbounded",
        discard: true,
      }),
    );
    const rebuildScheduler = Scope.close(scheduleScope, Exit.void).pipe(
      Effect.andThen(scheduleLayer.pipe(Layer.fresh, Layer.build)),
    );
    const restartSchedulerTransportWith = (durableRequests: boolean) =>
      Effect.gen(function* () {
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
        const http = yield* NodeHttpServer.layerTest.pipe(Layer.fresh, Layer.build);
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
        const currentAuthority = Context.mergeAll(lifecycleContext, lifecycle, pendingRequests);
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
      });
    const restartSchedulerTransport = restartSchedulerTransportWith(attachmentRequests);
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
      resultPageStarted,
      releaseResultPage,
      reopenDatabase,
      environmentId: descriptor.environmentId,
      project,
      database,
      lifecycleRecovery,
      signals,
      runtime,
      requestContext,
      config: Context.get(nodeConfigContext, Config.ServerConfig),
      browser,
      fixtureDevice,
      deviceProcessCalls,
      deviceProcessHook,
      deviceSummaryHook,
      deviceReadinessHook,
      deviceObservedScope,
      deviceMediaPaths,
      deviceMediaOutside,
      deviceScreenshotCalls,
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
  });
type Node = Effect.Success<ReturnType<typeof makeNode>>;

it.live(
  "scheduler-replay382: replayed creation preserves later independent schedule edits",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const node = yield* makeNode(
          descriptorOf("scheduler-replay382", "Scheduler", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const parent = ThreadId.make("scheduler-replay382-parent");
        yield* seedParent(node, parent);
        const input = {
          prompt: "Original isolated schedule intent",
          title: "Original title",
          schedule: { type: "interval", everyMs: 60_000 },
          clientRequestId: "scheduler-replay382-create",
        };
        const first = yield* nativeCall(node, parent, "schedule_task", input);
        expect(first.isError, json(first)).not.toBe(true);
        const result = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ scheduledTaskId: Schema.String }),
        )(first.structuredContent);
        const later = yield* nativeCall(node, parent, "update_scheduled_task", {
          scheduledTaskId: result.scheduledTaskId,
          title: "Later independent title",
          clientRequestId: "scheduler-replay382-later-edit",
        });
        expect(later.isError, json(later)).not.toBe(true);
        const replay = yield* nativeCall(node, parent, "schedule_task", input);
        expect(replay.isError, json(replay)).not.toBe(true);
        expect(
          (yield* node.scheduled.list()).tasks.find((task) => task.id === result.scheduledTaskId)
            ?.title,
        ).toBe("Later independent title");
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "scheduler-replay382: native acceptance survives edits, reconstruction and schedule incarnation replacement",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const node = yield* makeNode(
          descriptorOf("scheduler-history382", "Scheduler history", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const parent = ThreadId.make("scheduler-history382-parent");
        yield* seedParent(node, parent);
        const firstPrivate = "ISOLATED_SCHEDULE_PROMPT_382_ALPHA_NEVER_IN_RECEIPTS";
        const changedPrivate = "ISOLATED_SCHEDULE_PROMPT_382_BETA_CHANGED_INPUT";
        const create = {
          prompt: firstPrivate,
          title: "Original",
          schedule: { type: "interval", everyMs: 60_000 },
          checkCommand: "test -f isolated-original",
          checkReason: "Original isolated proof",
          bindToCurrentThread: false,
          clientRequestId: "history382-create",
        };
        const initialRivals = yield* Effect.all(
          [
            nativeCall(node, parent, "schedule_task", create),
            nativeCall(node, parent, "schedule_task", create),
          ],
          { concurrency: "unbounded" },
        );
        const first = initialRivals[0]!;
        expect(initialRivals[1]?.structuredContent).toEqual(first.structuredContent);
        expect(first.isError, json(first)).not.toBe(true);
        const receipt = yield* Schema.decodeUnknownEffect(McpScheduledMutationReceipt)(
          first.structuredContent,
        );
        expect(Object.keys(receipt).sort()).toEqual([
          "applied",
          "operationId",
          "requestId",
          "scheduledTaskId",
          "stage",
          "type",
        ]);
        const id = receipt.scheduledTaskId;
        const edit1 = {
          scheduledTaskId: id,
          title: "First edit",
          prompt: "Independent prompt",
          checkCommand: "test -f isolated-first",
          checkReason: "First edit",
          clientRequestId: "history382-edit1",
        };
        const edit2 = {
          scheduledTaskId: id,
          title: "Second edit",
          checkCommand: "test -f isolated-second",
          checkReason: "Second edit",
          clientRequestId: "history382-edit2",
        };
        const firstEdit = yield* nativeCall(node, parent, "update_scheduled_task", edit1);
        expect(firstEdit.isError, json(firstEdit)).not.toBe(true);
        expect((yield* nativeCall(node, parent, "update_scheduled_task", edit2)).isError).not.toBe(
          true,
        );
        expect(
          (yield* nativeCall(node, parent, "schedule_task", create)).structuredContent,
        ).toEqual(receipt);
        expect((yield* nativeCall(node, parent, "update_scheduled_task", edit1)).isError).not.toBe(
          true,
        );
        let current = (yield* node.scheduled.list()).tasks.find((task) => task.id === id)!;
        expect(current.title).toBe("Second edit");
        expect(current.prompt).toBe("Independent prompt");
        expect(current.outcomeCheck?.command).toBe("test -f isolated-second");
        const changed = yield* nativeCall(node, parent, "schedule_task", {
          ...create,
          prompt: changedPrivate,
        });
        expect(changed.isError).toBe(true);
        const sql = Context.get(node.database, SqlClient.SqlClient);
        const before = yield* sql`SELECT * FROM scheduled_mutation_receipts ORDER BY request_id`;
        expect(before).toHaveLength(3);
        for (const rows of [before, yield* sql`SELECT * FROM mcp_request_receipts`]) {
          expect(json(rows)).not.toContain(firstPrivate);
          expect(json(rows)).not.toContain(changedPrivate);
          expect(json(rows)).not.toContain("isolated-second");
        }
        const reopened = yield* node.reopenDatabase;
        const original = (yield* sql<{
          request_id: string;
          generation: string;
          input_digest: string;
        }>`SELECT * FROM scheduled_mutation_receipts WHERE request_id=${receipt.operationId}`)[0]!;
        const recovered = yield* ScheduleMutation.confirmed({
          id: original.request_id,
          generation: original.generation,
          inputDigest: original.input_digest,
          clientRequestId: create.clientRequestId,
        }).pipe(Effect.provide(Layer.succeedContext(reopened)));
        expect(Option.getOrThrow(recovered)).toEqual(receipt);
        const active = { ...node, ...(yield* node.restartSchedulerTransport) };
        const transportRecovered = yield* nativeCall(active, parent, "schedule_task", create);
        expect(transportRecovered.isError, json(transportRecovered)).not.toBe(true);
        expect(transportRecovered.structuredContent).toEqual(receipt);
        expect((yield* active.scheduled.list()).tasks.find((task) => task.id === id)?.title).toBe(
          "Second edit",
        );
        const concurrent = yield* Effect.all(
          [
            nativeCall(active, parent, "schedule_task", create),
            nativeCall(active, parent, "schedule_task", create),
          ],
          { concurrency: "unbounded" },
        );
        expect(concurrent.map((result) => result.structuredContent)).toEqual([receipt, receipt]);
        expect(yield* sql`SELECT * FROM scheduled_mutation_receipts ORDER BY request_id`).toEqual(
          before,
        );
        const rivalEdits = yield* Effect.all(
          [
            nativeCall(active, parent, "update_scheduled_task", {
              scheduledTaskId: id,
              title: "Rival A",
              clientRequestId: "history382-rival-a",
            }),
            nativeCall(active, parent, "update_scheduled_task", {
              scheduledTaskId: id,
              prompt: "ISOLATED_RIVAL_PRIVATE_PROMPT_382",
              clientRequestId: "history382-rival-b",
            }),
          ],
          { concurrency: "unbounded" },
        );
        expect(rivalEdits.every((result) => result.isError !== true)).toBe(true);
        const rivalTask = (yield* active.scheduled.list()).tasks.find((task) => task.id === id);
        expect(rivalTask?.title).toBe("Rival A");
        expect(rivalTask?.prompt).toBe("ISOLATED_RIVAL_PRIVATE_PROMPT_382");
        expect(yield* sql`SELECT * FROM scheduled_mutation_receipts`).toHaveLength(5);
        const failure = yield* nativeCall(active, parent, "update_scheduled_task", {
          scheduledTaskId: id,
          schedule: { type: "once", at: "2000-01-01T00:00:00.000Z" },
          clientRequestId: "history382-before-commit",
        });
        expect(failure.isError).toBe(true);
        expect(yield* sql`SELECT * FROM scheduled_mutation_receipts`).toHaveLength(5);
        const remove = { scheduledTaskId: id, clientRequestId: "history382-delete" };
        const deleted = yield* nativeCall(active, parent, "delete_scheduled_task", remove);
        expect(deleted.isError, json(deleted)).not.toBe(true);
        expect(
          (yield* nativeCall(active, parent, "schedule_task", create)).structuredContent,
        ).toEqual(receipt);
        expect(
          (yield* nativeCall(active, parent, "update_scheduled_task", edit1)).structuredContent,
        ).toEqual(firstEdit.structuredContent);
        expect(
          (yield* nativeCall(active, parent, "delete_scheduled_task", remove)).structuredContent,
        ).toEqual(deleted.structuredContent);
        // Deliberate unkeyed local replacement keeps the visible id but gets a new native incarnation.
        yield* active.scheduled.upsert({
          ...current,
          title: "Replacement incarnation",
          prompt: "Replacement private prompt",
          requireExisting: false,
        });
        const replacement = (yield* active.scheduled.list()).tasks.find((task) => task.id === id)!;
        for (const [tool, input] of [
          ["schedule_task", create],
          ["update_scheduled_task", edit1],
          ["delete_scheduled_task", remove],
        ] as const) {
          const historical = yield* nativeCall(active, parent, tool, input);
          expect(historical.isError, json(historical)).not.toBe(true);
        }
        current = (yield* active.scheduled.list()).tasks.find((task) => task.id === id)!;
        expect(current).toEqual(replacement);
        const nativeRows = yield* sql<{
          incarnation: string;
        }>`SELECT incarnation FROM scheduled_task_request_incarnations WHERE task_id=${id}`;
        const acceptedRows = yield* sql<{
          incarnation: string;
        }>`SELECT incarnation FROM scheduled_mutation_receipts WHERE request_id=${receipt.operationId}`;
        expect(nativeRows[0]?.incarnation).not.toBe(acceptedRows[0]?.incarnation);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "scheduler-secret382: committed native replay consumes once, while consume-to-commit uncertainty never consumes again",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("scheduler-secret-a382", "A", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf("scheduler-secret-b382", "B", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("scheduler-secret382-parent");
        yield* seedParent(a, parent);
        const store = Context.get(b.services, Secrets.ServerSecretStore);
        const seed = (hex: string) =>
          Clock.currentTimeMillis.pipe(
            Effect.flatMap((savedAt) =>
              store.set(
                `secret-request-${hex}`,
                new TextEncoder().encode(
                  json({ projectId: b.project.id, value: `ISOLATED_SECRET_382_${hex}`, savedAt }),
                ),
              ),
            ),
          );
        const firstHex = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        yield* seed(firstHex);
        const input = {
          environmentId: b.environmentId,
          projectId: b.project.id,
          title: "Webhook",
          prompt: "ISOLATED_WEBHOOK_PROMPT_382",
          schedule: {
            type: "webhook",
            signature: {
              header: "x-fixture-signature",
              encoding: "hex",
              prefix: "sha256=",
              secretRef: SecretRef.make(`secret-ref:${firstHex}`),
            },
          },
          clientRequestId: "secret382-committed",
        };
        yield* Ref.set(a.dropCommittedToolReply, "schedule_task");
        const lost = yield* nativeCall(a, parent, "schedule_task", input);
        expect(lost.isError, json(lost)).toBe(true);
        expect(yield* Ref.get(b.secretConsumes)).toBe(1);
        expect(Option.isNone(yield* store.get(`secret-request-${firstHex}`))).toBe(true);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const rows = yield* sql<{
          result_json: string;
        }>`SELECT result_json FROM scheduled_mutation_receipts WHERE stage='confirmed'`;
        expect(rows).toHaveLength(1);
        const receipt = yield* Schema.decodeUnknownEffect(
          Schema.fromJsonString(McpScheduledMutationReceipt),
        )(rows[0]!.result_json);
        const beforeRoute = Option.getOrThrow(yield* a.links.get(b.environmentId));
        const resumed = { ...b, ...(yield* b.restartSchedulerTransport) };
        // Only isolated transport location changes. Existing session, grant and
        // generation stay untouched; the real resolver probes the new server.
        yield* Context.get(
          a.database,
          SqlClient.SqlClient,
        )`UPDATE peer_environment_links SET urls_json=${json([resumed.url])} WHERE environment_id=${b.environmentId}`;
        expect(Option.getOrThrow(yield* a.links.get(b.environmentId)).launchIdentity).toBe(
          beforeRoute.launchIdentity,
        );
        const replay = yield* nativeCall(a, parent, "schedule_task", input);
        expect(replay.isError, json(replay)).not.toBe(true);
        expect(replay.structuredContent).toEqual(receipt);
        expect(yield* Ref.get(b.secretConsumes)).toBe(1);
        const listed = yield* nativeCall(a, parent, "list_scheduled_tasks", {
          environmentId: b.environmentId,
          projectId: b.project.id,
        });
        const projection = yield* decodeScheduledTaskList(listed.structuredContent);
        expect(projection.tasks[0]?.schedule.type).toBe("webhook");
        expect(
          decodeURIComponent(new URL(projection.tasks[0]!.webhookUrl!).pathname.split("/").at(-2)!),
        ).toBe(receipt.scheduledTaskId);
        const secondHex = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        yield* seed(secondHex);
        yield* Ref.set(b.loseSecretConfirmation, true);
        const uncertain = {
          ...input,
          clientRequestId: "secret382-uncertain",
          schedule: {
            ...input.schedule,
            signature: {
              ...input.schedule.signature,
              secretRef: SecretRef.make(`secret-ref:${secondHex}`),
            },
          },
        };
        const failed = yield* nativeCall(a, parent, "schedule_task", uncertain);
        expect(failed.isError, json(failed)).toBe(true);
        expect(json(failed)).toContain("outcome_unknown");
        expect(yield* Ref.get(b.secretConsumes)).toBe(2);
        expect(Option.isNone(yield* store.get(`secret-request-${secondHex}`))).toBe(true);
        const reconstructed = { ...b, ...(yield* b.restartSchedulerTransport) };
        yield* Context.get(
          a.database,
          SqlClient.SqlClient,
        )`UPDATE peer_environment_links SET urls_json=${json([reconstructed.url])} WHERE environment_id=${b.environmentId}`;
        const native = reconstructed.scheduled;
        expect((yield* native.list()).tasks).toHaveLength(1);
        const again = yield* nativeCall(a, parent, "schedule_task", uncertain);
        expect(json(again)).toContain("outcome_unknown");
        expect(yield* Ref.get(b.secretConsumes)).toBe(2);
        for (const records of [
          yield* sql`SELECT * FROM scheduled_mutation_receipts`,
          yield* sql`SELECT * FROM mcp_request_receipts`,
        ]) {
          expect(json(records)).not.toContain("ISOLATED_SECRET_382_");
          expect(json(records)).not.toContain(input.prompt);
          expect(json(records)).not.toContain("https://hooks.");
        }
        yield* a.links.unlink(b.environmentId);
        yield* linkTo(a.links, b, "full-access");
        const replaced = yield* nativeCall(a, parent, "schedule_task", input);
        expect(replaced.isError).toBe(true);
        expect(yield* Ref.get(b.secretConsumes)).toBe(2);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "links382: registered SQL and native command link receipts cannot undo later independent links",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const node = yield* makeNode(
          descriptorOf("links382", "Links", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true, nativeOwners: true },
        );
        const parent = ThreadId.make("links382-parent");
        yield* seedParent(node, parent);
        const caps = new Set<
          McpThreadInvocationScope["capabilities"] extends ReadonlySet<infer A> ? A : never
        >(["orchestration", "pull-requests"]);
        const call = (tool: string, args: Record<string, unknown>) =>
          nativeCall(node, parent, tool, args, caps);
        for (const [link, unlink, list, url] of [
          [
            "link_issue",
            "unlink_issue",
            "list_thread_issues",
            "https://github.com/fixture/repo/issues/382",
          ],
          [
            "link_pull_request",
            "unlink_pull_request",
            "list_thread_pull_requests",
            "https://github.com/fixture/repo/pull/382",
          ],
        ]) {
          const initial = { url, clientRequestId: `${link}-original` };
          const first = yield* call(link!, initial);
          expect(first.isError, json(first)).not.toBe(true);
          const removed = yield* call(unlink!, { url, clientRequestId: `${unlink}-first` });
          expect(removed.isError, json(removed)).not.toBe(true);
          expect((yield* call(link!, initial)).structuredContent).toEqual(first.structuredContent);
          const absent = yield* call(list!, {});
          expect(json(absent.structuredContent)).not.toContain('"number":382');
          const later = yield* call(link!, { url, clientRequestId: `${link}-later` });
          expect(later.isError, json(later)).not.toBe(true);
          expect(
            (yield* call(unlink!, { url, clientRequestId: `${unlink}-first` })).structuredContent,
          ).toEqual(removed.structuredContent);
          const present = yield* call(list!, {});
          expect(json(present.structuredContent)).toContain('"number":382');
          const noOpLink = { url, clientRequestId: `${link}-original-no-op` };
          const noOp = yield* call(link!, noOpLink);
          expect(noOp.isError, json(noOp)).not.toBe(true);
          expect(
            (yield* call(unlink!, { url, clientRequestId: `${unlink}-independent` })).isError,
          ).not.toBe(true);
          expect((yield* call(link!, noOpLink)).structuredContent).toEqual(noOp.structuredContent);
          expect(json((yield* call(list!, {})).structuredContent)).not.toContain('"number":382');
          const noOpUnlink = { url, clientRequestId: `${unlink}-original-no-op` };
          const noOpRemoved = yield* call(unlink!, noOpUnlink);
          expect(noOpRemoved.isError, json(noOpRemoved)).not.toBe(true);
          expect(
            (yield* call(link!, { url, clientRequestId: `${link}-after-no-op` })).isError,
          ).not.toBe(true);
          expect((yield* call(unlink!, noOpUnlink)).structuredContent).toEqual(
            noOpRemoved.structuredContent,
          );
          expect(json((yield* call(list!, {})).structuredContent)).toContain('"number":382');
        }
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "worktree382: native worktree acceptance preserves files and bounds unresolved follow-up effects",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "worktree382-native-" });
        const git = (args: ReadonlyArray<string>) =>
          ProcessRunner.ProcessRunner.pipe(
            Effect.flatMap((runner) => runner.run({ command: "git", args: ["-C", root, ...args] })),
            Effect.provide(ProcessRunner.layer),
          );
        expect((yield* git(["init", "--quiet", "--initial-branch=main"])).code).toBe(0);
        yield* fs.writeFileString(`${root}/README.md`, "Isolated repository");
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
        const node = yield* makeNode(
          descriptorOf("worktree382", "Worktree", { peerAuthority: 1 }),
          "disk",
          {
            productionKernel: true,
            attachmentRequests: true,
            nativeOwners: true,
            ownerWorkspace: root,
          },
        );
        const parent = ThreadId.make("worktree382-parent");
        yield* seedParent(node, parent);
        const capabilities = new Set(["orchestration", "worktree"] as const);
        const input = {
          branch: "worktree382-accepted",
          path: `${root}/accepted`,
          baseRef: "main",
          startFromOrigin: false,
          runSetupScript: false,
          clientRequestId: "worktree382-once",
        };
        const first = yield* nativeCall(node, parent, "t3_worktree_handoff", input, capabilities);
        expect(first.isError, json(first)).not.toBe(true);
        yield* fs.writeFileString(`${root}/accepted/README.md`, "Later independent edit");
        const replay = yield* nativeCall(node, parent, "t3_worktree_handoff", input, capabilities);
        expect(replay.isError, json(replay)).not.toBe(true);
        expect(replay.structuredContent).toEqual(first.structuredContent);
        expect(yield* fs.readFileString(`${root}/accepted/README.md`)).toBe(
          "Later independent edit",
        );
        const next = ThreadId.make("worktree382-ambiguous-parent");
        yield* seedParent(node, next);
        yield* Ref.set(node.loseWorktreeReply, true);
        const ambiguousInput = {
          ...input,
          branch: "worktree382-ambiguous",
          path: `${root}/ambiguous`,
          clientRequestId: "worktree382-lost-native-reply",
        };
        const lost = yield* nativeCall(
          node,
          next,
          "t3_worktree_handoff",
          ambiguousInput,
          capabilities,
        );
        expect(lost.isError).toBe(true);
        expect(json(lost)).toContain("outcome_unknown");
        expect(yield* fs.exists(`${root}/ambiguous/README.md`)).toBe(true);
        const again = yield* nativeCall(
          node,
          next,
          "t3_worktree_handoff",
          ambiguousInput,
          capabilities,
        );
        expect(again.isError).toBe(true);
        expect(json(again)).toContain("native_binding_accepted");
        expect((yield* node.threads.getThreadShell(next))?.worktreePath).toBe(`${root}/ambiguous`);
        expect(yield* fs.exists(`${root}/ambiguous/README.md`)).toBe(true);
        const sql = Context.get(node.database, SqlClient.SqlClient);
        expect(
          yield* sql`SELECT command_id FROM orchestration_command_receipts WHERE command_type='thread.metadata.update' AND command_id LIKE 'worktree-handoff:%'`,
        ).toHaveLength(2);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "manual382: a durable command admission survives reconstruction without a second external start",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>();
        let starts = 0;
        const spawner = ChildProcessSpawner.make(() =>
          Effect.gen(function* () {
            starts++;
            yield* Deferred.succeed(started, undefined);
            return ChildProcessSpawner.makeHandle({
              pid: ChildProcessSpawner.ProcessId(382),
              exitCode: Effect.never,
              isRunning: Effect.succeed(true),
              kill: () => Effect.void,
              stdin: Sink.drain,
              stdout: Stream.empty,
              stderr: Stream.empty,
              all: Stream.empty,
              getInputFd: () => Sink.drain,
              getOutputFd: () => Stream.empty,
              unref: Effect.succeed(Effect.void),
            });
          }),
        );
        const node = yield* makeNode(
          descriptorOf("manual382-command", "Command", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true, commandSpawner: spawner },
        );
        const parent = ThreadId.make("manual382-command-parent");
        yield* seedParent(node, parent);
        const scheduled = yield* nativeCall(node, parent, "schedule_task", {
          prompt: "Isolated external command",
          command: "isolated-external-boundary-382",
          title: "One command",
          schedule: { type: "interval", everyMs: 60_000 },
          bindToCurrentThread: false,
          clientRequestId: "manual382-command-schedule",
        });
        expect(scheduled.isError, json(scheduled)).not.toBe(true);
        const task = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ scheduledTaskId: Schema.String }),
        )(scheduled.structuredContent);
        const input = { taskId: task.scheduledTaskId, clientRequestId: "manual382-command-fire" };
        const first = yield* nativeCall(node, parent, "run_scheduled_task_now", input);
        expect(first.isError, json(first)).not.toBe(true);
        const result = yield* Schema.decodeUnknownEffect(
          ThreadToolkit.tools.run_scheduled_task_now.successSchema,
        )(first.structuredContent);
        expect(result.manualReceipt?.stage).toBe("admitted");
        yield* Deferred.await(started);
        expect(starts).toBe(1);
        expect(
          (yield* nativeCall(node, parent, "run_scheduled_task_now", input)).structuredContent,
        ).toEqual(result);
        expect(starts).toBe(1);
        const sql = Context.get(node.database, SqlClient.SqlClient);
        const row = (yield* sql<{
          request_id: string;
          generation: string;
          input_digest: string;
        }>`SELECT request_id,generation,input_digest FROM scheduled_manual_requests`)[0]!;
        const rebuilt = yield* node.rebuildScheduler;
        const owner = Context.get(rebuilt, ScheduledTasks.ScheduledTaskService);
        const identity = {
          id: row.request_id,
          generation: row.generation,
          inputDigest: row.input_digest,
          clientRequestId: input.clientRequestId,
        };
        const replay = yield* owner.runNow({ id: result.taskId }, { request: identity });
        expect(replay.manualReceipt).toEqual(result.manualReceipt);
        expect(starts).toBe(1);
        // Drive the existing command owner after a process restart. Its unresolved
        // external start remains needs-you, never silently executed again.
        yield* Option.getOrThrow(Context.getOption(rebuilt, ScheduledChecks.ScheduledTaskChecks))
          .reconcile;
        expect(starts).toBe(1);
        const current = (yield* owner.list()).tasks.find((t) => t.id === result.taskId)!;
        expect(current.command?.run).toMatchObject({ stage: "needs-you", imported: false });
        expect(current.command?.run?.error).toBeTruthy();
        expect(current.runCount).toBe(1);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

// Finite #383 core proof: these cases require the parent-integrated typed device domain.
// No service replacement: every intent enters the source's native MCP credential path.
const decodeTyped383Open = Schema.decodeUnknownEffect(
  Schema.Struct({
    target: Schema.Struct({
      hostId: Schema.String,
      deviceId: Schema.String,
      toolSessionId: Schema.String,
    }),
    quickStart: Schema.String,
  }),
);
const typed383Digest = (bytes: Uint8Array) =>
  Effect.gen(function* () {
    const crypto = yield* Crypto.Crypto;
    return Hex.encode(yield* crypto.digest("SHA-256", bytes));
  }).pipe(Effect.orDie);
const decodeTyped383Command = Schema.decodeUnknownEffect(DeviceCommandResult);
const decodeTyped383Upload = Schema.decodeUnknownEffect(PeerAttachmentUploadResult);
const decodeTyped383Target = Schema.decodeUnknownEffect(
  Schema.Struct({ target: DeviceCommandTarget }),
);
const decodeTyped383Artifact = Schema.decodeUnknownEffect(
  Schema.Struct({
    type: Schema.Literal("artifact"),
    artifact: PeerAttachmentReference,
  }),
);
const assertTyped383SourceIdle = (source: Node) =>
  Effect.gen(function* () {
    expect(yield* Ref.get(source.deviceHostStarts)).toBe(0);
    expect(yield* Ref.get(source.deviceAgentStarts)).toBe(0);
    expect(yield* Ref.get(source.deviceConfigurations)).toBe(0);
    expect(yield* Ref.get(source.deviceScreenshotCalls)).toBe(0);
    expect(yield* Ref.get(source.deviceShutdownCalls)).toBe(0);
    expect(source.deviceProcessCalls).toHaveLength(0);
  });
const typed383Pair = (id: string, legacyDeviceDomain = false, sourcePreferencesOwner = false) =>
  Effect.gen(function* () {
    const a = yield* makeNode(descriptorOf(`${id}-a`, "A", { peerAuthority: 1 }), "disk", {
      productionKernel: true,
      attachmentRequests: true,
      preferencesOwner: sourcePreferencesOwner,
      deviceOwner: true,
    });
    yield* Effect.addFinalizer(() => assertTyped383SourceIdle(a));
    const b = yield* makeNode(descriptorOf(`${id}-b`, "B", { peerAuthority: 1 }), "disk", {
      productionKernel: true,
      attachmentRequests: true,
      preferencesOwner: true,
      legacyDeviceDomain,
      deviceOwner: true,
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
    const footprint = () =>
      Effect.gen(function* () {
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

it.live(
  "typed383-core: registered two-peer loop preserves private binding, PNG bytes and exact historical receipts",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, child, route, open, footprint } =
          yield* typed383Pair("typed383-core");
        const discovery = yield* nativeCall(a, parent, "device_commands", route);
        expect(discovery.isError, json(discovery)).not.toBe(true);
        expect(discovery.structuredContent).toMatchObject({
          protocol: 1,
          agentDeviceVersion: AGENT_DEVICE_VERSION,
          deviceHubVersion: DEVICE_HUB_VERSION,
          commands: expect.arrayContaining([
            "open",
            "snapshot",
            "click",
            "fill",
            "screenshot",
            "close",
          ]),
        });
        const opened = yield* open("typed383-open");
        expect(opened.isError, json(opened)).not.toBe(true);
        const { target, quickStart } = yield* decodeTyped383Open(opened.structuredContent);
        expect(target).toMatchObject({
          hostId: "local",
          deviceId: "fixture-device382",
          toolSessionId: expect.stringMatching(/^[0-9a-f-]{36}$/),
        });
        expect(quickStart).toContain("device_commands");
        expect(quickStart).toContain("device_execute");
        for (const privateValue of [
          "agentDevice",
          "--config",
          "ISOLATED_DEVICE_TOKEN_382",
          b.config.stateDir,
        ])
          expect(json(opened.structuredContent)).not.toContain(privateValue);
        const drive = (key: string, command: unknown) =>
          nativeCall(a, parent, "device_execute", {
            ...route,
            target,
            command,
            clientRequestId: key,
          });
        expect(
          (yield* drive("typed383-app", { type: "open", app: "com.example.app" }))
            .structuredContent,
        ).toEqual({ type: "action", target, status: "dispatched" });
        const tree = yield* drive("typed383-tree", {
          type: "snapshot",
          interactiveOnly: true,
          depth: 8,
        });
        expect(tree.structuredContent).toEqual({
          type: "snapshot",
          target,
          nodes: [{ index: 1, ref: "e1", role: "button", label: "Continue" }],
          truncated: false,
        });
        expect((yield* drive("typed383-click", { type: "click", ref: "@e1" })).isError).not.toBe(
          true,
        );
        expect(
          (yield* drive("typed383-fill", { type: "fill", ref: "@e1", text: "--config /evil\n" }))
            .isError,
        ).not.toBe(true);
        const fs = yield* FileSystem.FileSystem;
        const sessions = new Set<string>();
        for (const call of b.deviceProcessCalls) {
          expect(call.command).not.toBe("/remote-host/node-must-never-execute");
          expect(call.args[0]).toBe(
            `${b.config.baseDir}/tools/agent-device/${AGENT_DEVICE_VERSION}/node_modules/agent-device/bin/agent-device.mjs`,
          );
          const separator = call.args.indexOf("--");
          expect(separator).toBeGreaterThan(1);
          const flags = call.args.slice(0, separator);
          for (const flag of ["--json", "--platform", "--serial", "--config", "--session"])
            expect(flags).toContain(flag);
          expect(flags[flags.indexOf("--platform") + 1]).toBe("android");
          expect(flags[flags.indexOf("--serial") + 1]).toBe("fixture-device382");
          const config = flags[flags.indexOf("--config") + 1]!;
          expect(config).toBe(`${b.config.stateDir}/device/sessions/${target.toolSessionId}.json`);
          expect(yield* fs.readFileString(config)).toContain("ISOLATED_DEVICE_TOKEN_382");
          sessions.add(flags[flags.indexOf("--session") + 1]!);
          if (call.args[1] === "fill") {
            expect(call.stdin).toBe("--config /evil\n\r\n");
            expect(call.args.slice(separator + 1)).toEqual(["@e1"]);
          }
        }
        expect(sessions.size).toBe(1);
        const pngInput = {
          ...route,
          target,
          command: { type: "screenshot" },
          clientRequestId: "typed383-png",
        };
        yield* Ref.set(a.dropCommittedToolReply, "device_execute");
        const lost = yield* nativeCall(a, parent, "device_execute", pngInput);
        expect(lost.isError).toBe(true);
        expect(json(lost)).toContain("Isolated committed tool reply loss");
        const beforeReplay = yield* footprint();
        const rebuilt = { ...b, ...(yield* b.restartSchedulerTransport) };
        yield* Context.get(
          a.database,
          SqlClient.SqlClient,
        )`UPDATE peer_environment_links SET urls_json=${json([rebuilt.url])} WHERE environment_id=${b.environmentId}`;
        const image = yield* nativeCall(a, parent, "device_execute", pngInput);
        expect(image.isError, json(image)).not.toBe(true);
        const { artifact } = yield* decodeTyped383Artifact(image.structuredContent);
        const png = Buffer.from(
          "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScLbtAAAAABJRU5ErkJggg==",
          "base64",
        );
        const digest = yield* typed383Digest(png);
        expect(artifact).toMatchObject({
          environmentId: b.environmentId,
          mimeType: "image/png",
          sizeBytes: png.length,
          digest,
        });
        const file = resolveAttachmentPathById({
          attachmentsDir: b.config.attachmentsDir,
          attachmentId: artifact.attachmentId,
        });
        expect(file).not.toBeNull();
        expect(Array.from(yield* fs.readFile(file!))).toEqual(Array.from(png));
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const rows = yield* sql<{
          owner: string;
          digest: string;
          expected_digest: string;
          manifest: string;
        }>`SELECT owner,digest,expected_digest,manifest FROM attachment_staging WHERE id=${artifact.attachmentId}`;
        expect(rows).toMatchObject([{ owner: artifact.owner, digest, expected_digest: digest }]);
        expect(rows[0]!.manifest).toContain("image/png");
        expect(rows[0]!.manifest).toContain(String(png.length));
        expect(
          (yield* nativeCall(a, parent, "device_execute", pngInput)).structuredContent,
        ).toEqual(image.structuredContent);
        expect(yield* footprint()).toEqual(beforeReplay);
        expect(yield* Ref.get(b.deviceScreenshotCalls)).toBe(1);
        expect(json(yield* sql`SELECT * FROM mcp_request_receipts`)).not.toContain(
          "ISOLATED_DEVICE_TOKEN_382",
        );
        const close = {
          ...route,
          target,
          command: { type: "close" },
          clientRequestId: "typed383-close",
        };
        const closed = yield* nativeCall(a, parent, "device_execute", close);
        expect(closed.structuredContent).toEqual({ type: "closed", target });
        expect(yield* b.fixtureDevice!.sessionsForThread(child)).toHaveLength(0);
        const reopened = yield* decodeTyped383Open(
          (yield* open("typed383-reopen")).structuredContent,
        );
        expect(reopened.target.toolSessionId).not.toBe(target.toolSessionId);
        const stable = yield* footprint();
        expect((yield* nativeCall(a, parent, "device_execute", close)).structuredContent).toEqual(
          closed.structuredContent,
        );
        expect((yield* drive("typed383-stale", { type: "click", ref: "@e1" })).isError).toBe(true);
        expect(yield* footprint()).toEqual(stable);
        expect((yield* b.fixtureDevice!.sessionsForThread(child))[0]?.toolSessionId).toBe(
          reopened.target.toolSessionId,
        );
        const fresh = yield* nativeCall(a, parent, "device_execute", {
          ...route,
          target: reopened.target,
          command: { type: "click", ref: "@e1" },
          clientRequestId: "typed383-reopened-click",
        });
        expect(fresh.isError, json(fresh)).not.toBe(true);
        const last = b.deviceProcessCalls.at(-1)!;
        expect(last.args[last.args.indexOf("--session") + 1]).not.toBe([...sessions][0]);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

const decodeTyped383Example = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
);
it.live(
  "typed383-guidance: an A-only caller executes the returned snapshot example unchanged",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, open } = yield* typed383Pair("typed383-guidance");
        const opened = yield* open("typed383-guidance-open");
        expect(opened.isError, json(opened)).not.toBe(true);
        const { target, quickStart } = yield* decodeTyped383Open(opened.structuredContent);
        const discoveryExample = quickStart
          .split("\n")
          .find((line) => line.startsWith("device_commands "));
        expect(discoveryExample).toBeDefined();
        const discoveryArgs = yield* decodeTyped383Example(
          discoveryExample!.slice("device_commands ".length),
        );
        const discovery = yield* nativeCall(a, parent, "device_commands", discoveryArgs);
        expect(discovery.isError, json(discovery)).not.toBe(true);
        expect(discovery.structuredContent).toMatchObject({ protocol: 1, hostId: "local" });
        const example = quickStart
          .split("\n")
          .find((line) => line.startsWith("device_execute ") && line.includes('"type":"snapshot"'));
        expect(example).toBeDefined();
        const args = yield* decodeTyped383Example(example!.slice("device_execute ".length));
        // No caller-supplied routing repair: follow the exact returned public example on A.
        const result = yield* nativeCall(a, parent, "device_execute", args);
        expect(result.isError, json(result)).not.toBe(true);
        expect(result.structuredContent).toEqual({
          type: "snapshot",
          target,
          nodes: [{ index: 1, ref: "e1", role: "button", label: "Continue" }],
          truncated: false,
        });
        expect(b.deviceProcessCalls.filter((call) => call.args.includes("snapshot"))).toHaveLength(
          1,
        );
        yield* assertTyped383SourceIdle(a);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  60_000,
);

it.live.each(["device_open", "device_execute"] as const)(
  "typed383-preflight: %s rechecks the original grant after host inspection before replay or fresh effects",
  (operation) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, route, open, footprint } = yield* typed383Pair(
          `typed383-${operation}-preflight`,
        );
        const opened = yield* open("preflight-opening");
        expect(opened.isError, json(opened)).not.toBe(true);
        const { target } = yield* decodeTyped383Open(opened.structuredContent);
        const invoke = (key: string) =>
          operation === "device_open"
            ? open(key)
            : nativeCall(a, parent, "device_execute", {
                ...route,
                target,
                command: { type: "snapshot" },
                clientRequestId: key,
              });
        const key = operation === "device_open" ? "preflight-opening" : "preflight-snapshot";
        const completed = yield* invoke(key);
        expect(completed.isError, json(completed)).not.toBe(true);
        const grant = (yield* b.authority.grants)[0]!;
        const armed = yield* Ref.make(false);
        yield* Ref.set(
          b.deviceSummaryHook,
          Effect.gen(function* () {
            if ((yield* RequestReceipts.CurrentOperation)?.tool !== operation) return;
            if (!(yield* Ref.getAndSet(armed, false))) return;
            // External host inspection completes only after the real grant changes.
            yield* b.authority.setGrant({ ...grant, device: false }).pipe(Effect.orDie);
          }),
        );
        for (const nextKey of [key, `${key}-fresh`]) {
          const before = yield* footprint();
          yield* Ref.set(armed, true);
          const denied = yield* invoke(nextKey);
          expect(denied.isError, json(denied)).toBe(true);
          expect(yield* Ref.get(armed)).toBe(false);
          expect(yield* footprint()).toEqual(before);
          yield* b.authority.setGrant({ ...grant, device: true });
          expect((yield* invoke(key)).structuredContent).toEqual(completed.structuredContent);
          expect(yield* footprint()).toEqual(before);
        }
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "typed383-core: pure consent retries while actual config persistence remains unknown across transport reconstruction",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, route, open, footprint } = yield* typed383Pair("typed383-consent");
        yield* b.fixtureSettings.updateSettings({ enableAgentDeviceAccess: false });
        const untouched = yield* footprint();
        const denied = yield* open("typed383-consent-open");
        expect(denied.isError).toBe(true);
        expect(json(denied)).not.toContain("outcome_unknown");
        expect(yield* footprint()).toEqual(untouched);
        yield* b.fixtureSettings.updateSettings({ enableAgentDeviceAccess: true });
        expect((yield* open("typed383-consent-open")).isError).not.toBe(true);
        const target = (yield* decodeTyped383Open(
          (yield* open("typed383-current-open")).structuredContent,
        )).target;
        const input = {
          ...route,
          target,
          command: { type: "click", ref: "@e1" },
          clientRequestId: "typed383-config-unknown",
        };
        const before = yield* footprint();
        yield* Ref.set(b.loseDeviceConfiguration, true);
        const unknown = yield* nativeCall(a, parent, "device_execute", input);
        expect(unknown.isError).toBe(true);
        expect(json(unknown)).toContain("outcome_unknown");
        const accepted = yield* footprint();
        expect(accepted.config).toBeGreaterThan(before.config);
        expect(accepted.process).toBe(before.process);
        expect(accepted.configuration).toContain("ISOLATED_DEVICE_TOKEN_382");
        const rebuilt = { ...b, ...(yield* b.restartSchedulerTransport) };
        yield* Context.get(
          a.database,
          SqlClient.SqlClient,
        )`UPDATE peer_environment_links SET urls_json=${json([rebuilt.url])} WHERE environment_id=${b.environmentId}`;
        expect(json(yield* nativeCall(a, parent, "device_execute", input))).toContain(
          "outcome_unknown",
        );
        expect(yield* footprint()).toEqual(accepted);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "typed383-core: current actor mode contributor host device and generation deny before leaves on fresh and replay keys",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, route, open, footprint } = yield* typed383Pair("typed383-authority");
        const target = (yield* decodeTyped383Open(
          (yield* open("typed383-authority-open")).structuredContent,
        )).target;
        const input = {
          ...route,
          target,
          command: { type: "click", ref: "@e1" },
          clientRequestId: "typed383-authority-click",
        };
        expect((yield* nativeCall(a, parent, "device_execute", input)).isError).not.toBe(true);
        const stable = yield* footprint();
        const rival = ThreadId.make("typed383-rival");
        yield* seedParent(a, rival);
        for (const key of [input.clientRequestId, "typed383-rival-fresh"])
          expect(
            (yield* nativeCall(a, rival, "device_execute", { ...input, clientRequestId: key }))
              .isError,
          ).toBe(true);
        for (const changed of [
          { hostId: "foreign-host" },
          { deviceId: "foreign-device" },
          { toolSessionId: "00000000-0000-4000-8000-000000000000" },
        ])
          expect(
            (yield* nativeCall(a, parent, "device_execute", {
              ...input,
              target: { ...target, ...changed },
              clientRequestId: `typed383-target-${Object.keys(changed)[0]}`,
            })).isError,
          ).toBe(true);
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("typed383-plan"),
          threadId: parent,
          runtimeMode: "auto",
        });
        for (const key of [input.clientRequestId, "typed383-plan-fresh"])
          expect(
            (yield* nativeCall(a, parent, "device_execute", { ...input, clientRequestId: key }))
              .isError,
          ).toBe(true);
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("typed383-restore"),
          threadId: parent,
          runtimeMode: "full-access",
        });
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.auth.revokeSession(grant.sessionId);
        for (const key of [input.clientRequestId, "typed383-revoked-fresh"])
          expect(
            (yield* nativeCall(a, parent, "device_execute", { ...input, clientRequestId: key }))
              .isError,
          ).toBe(true);
        expect(yield* footprint()).toEqual(stable);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "typed383-wait: original source mode changes while the native command lock is held deny queued effects",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const queued = yield* Deferred.make<void>();
        let invocations = 0;
        const tracer = Tracer.make({
          span: (options) => {
            if (options.name === "DeviceService.executeCommand" && ++invocations === 2)
              Deferred.doneUnsafe(queued, Effect.void);
            return Tracer.nativeTracer.span(options);
          },
        });
        yield* Effect.gen(function* () {
          const { a, b, parent, route, open, footprint } = yield* typed383Pair("typed383-lock");
          const { target } = yield* decodeTyped383Open(
            (yield* open("lock-open")).structuredContent,
          );
          const held = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          yield* Ref.set(b.deviceProcessHook, (call) =>
            call.args[1] === "click"
              ? Deferred.succeed(held, undefined).pipe(Effect.andThen(Deferred.await(release)))
              : Effect.void,
          );
          const first = yield* nativeCall(a, parent, "device_execute", {
            ...route,
            target,
            command: { type: "click", ref: "@e1" },
            clientRequestId: "lock-click",
          }).pipe(Effect.forkChild);
          yield* Deferred.await(held);
          const input = {
            ...route,
            target,
            command: { type: "fill", ref: "@e1", text: "queued" },
            clientRequestId: "lock-fill",
          };
          const second = yield* nativeCall(a, parent, "device_execute", input).pipe(
            Effect.forkChild,
          );
          // Observe the real service span BEFORE its actual semaphore acquisition.
          // No semaphore, service, authority or policy method is mocked or injected.
          yield* Deferred.await(queued);
          const before = yield* footprint();
          yield* a.threads.dispatch({
            type: "thread.runtime-mode.set",
            commandId: CommandId.make("lock-source-plan"),
            threadId: parent,
            runtimeMode: "auto",
          });
          yield* Deferred.succeed(release, undefined);
          expect((yield* Fiber.join(first)).isError).not.toBe(true);
          const denied = yield* Fiber.join(second);
          expect(denied.isError, json(denied)).toBe(true);
          expect(yield* footprint()).toEqual(before);
          expect(b.deviceProcessCalls.filter((call) => call.args[1] === "fill")).toHaveLength(0);
          yield* a.threads.dispatch({
            type: "thread.runtime-mode.set",
            commandId: CommandId.make("lock-source-restore"),
            threadId: parent,
            runtimeMode: "full-access",
          });
          expect((yield* nativeCall(a, parent, "device_execute", input)).isError).not.toBe(true);
          expect(b.deviceProcessCalls.filter((call) => call.args[1] === "fill")).toHaveLength(1);
        }).pipe(Effect.withTracer(tracer), Effect.withTracerEnabled(true));
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "typed383-media: native batch input media claims and reconstruction preserve exact bytes and caller ownership",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, route, open, footprint } = yield* typed383Pair("typed383-media");
        const { target } = yield* decodeTyped383Open((yield* open("media-open")).structuredContent);
        const drive = (key: string, command: unknown) =>
          nativeCall(a, parent, "device_execute", {
            ...route,
            target,
            command,
            clientRequestId: key,
          });
        const batch = {
          type: "batch",
          steps: [
            { type: "snapshot", interactiveOnly: true },
            { type: "click", ref: "@e1" },
            { type: "fill", ref: "@e1", text: "--session rival\n" },
          ],
        };
        const first = yield* drive("media-loop", batch);
        expect(first.isError, json(first)).not.toBe(true);
        expect(first.structuredContent).toMatchObject({
          type: "batch",
          target,
          results: [
            { type: "snapshot", nodes: [{ ref: "e1" }] },
            { status: "dispatched" },
            { status: "dispatched" },
          ],
        });
        const calls = b.deviceProcessCalls.length;
        expect((yield* drive("media-loop", batch)).structuredContent).toEqual(
          first.structuredContent,
        );
        expect(b.deviceProcessCalls).toHaveLength(calls);
        const beforeInvalid = yield* footprint();
        for (const steps of [
          [
            { type: "click", ref: "@e1" },
            { type: "fill", ref: "@e1", text: "safe", daemonAuthToken: "borrowed" },
          ],
          [{ type: "batch", steps: [{ type: "click", ref: "@e1" }] }],
        ]) {
          const raw = yield* nativeInvalidParams(a, parent, "device_execute", {
            ...route,
            target,
            command: { type: "batch", steps },
            clientRequestId: "invalid-native-batch",
          });
          expect(raw).toMatchObject({ jsonrpc: "2.0", id: 1, error: { code: -32602 } });
        }
        expect(yield* footprint()).toEqual(beforeInvalid);
        const fs = yield* FileSystem.FileSystem;
        const sql = Context.get(b.database, SqlClient.SqlClient);
        for (const type of ["record", "trace"] as const) {
          expect((yield* drive(`${type}-start`, { type, action: "start" })).isError).not.toBe(true);
          if (type === "record") {
            yield* Ref.set(a.dropCommittedToolReply, "device_execute");
            const lost = yield* drive(`${type}-stop`, { type, action: "stop" });
            expect(lost.isError).toBe(true);
            expect(json(lost)).toContain("Isolated committed tool reply loss");
            const count = b.deviceProcessCalls.length;
            const rebuilt = { ...b, ...(yield* b.restartSchedulerTransport) };
            yield* Context.get(
              a.database,
              SqlClient.SqlClient,
            )`UPDATE peer_environment_links SET urls_json=${json([rebuilt.url])} WHERE environment_id=${b.environmentId}`;
            const replay = yield* drive(`${type}-stop`, { type, action: "stop" });
            expect(replay.isError, json(replay)).not.toBe(true);
            expect(b.deviceProcessCalls).toHaveLength(count);
          }
          const stopped = yield* drive(`${type}-stop`, { type, action: "stop" });
          expect(stopped.isError, json(stopped)).not.toBe(true);
          const result = yield* decodeTyped383Command(stopped.structuredContent);
          if (result.type !== "artifacts") throw new Error("Expected native media references");
          expect(result.recording).toEqual({
            durationMs: 200,
            capturedDurationMs: 150,
            recorder: "unconfirmed",
            nativePathDisposition: "pending",
          });
          const reference = result.artifacts[0]!;
          const file = resolveAttachmentPathById({
            attachmentsDir: b.config.attachmentsDir,
            attachmentId: reference.attachmentId,
          })!;
          const bytes = yield* fs.readFile(file);
          const expected =
            type === "record"
              ? new Uint8Array([
                  0, 0, 0, 0, 0x66, 0x74, 0x79, 0x70, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                  0,
                ])
              : new TextEncoder().encode("trace [private runtime] [private runtime]");
          expect(Array.from(bytes)).toEqual(Array.from(expected));
          expect(reference).toMatchObject({
            environmentId: b.environmentId,
            digest: yield* typed383Digest(expected),
            sizeBytes: expected.length,
          });
          const rows = yield* sql<{
            digest: string;
            expected_digest: string;
            manifest: string;
            consumer: string | null;
          }>`SELECT digest,expected_digest,manifest,consumer FROM attachment_staging WHERE id=${reference.attachmentId}`;
          expect(rows[0]).toMatchObject({
            digest: reference.digest,
            expected_digest: reference.digest,
            consumer: null,
          });
          expect(JSON.parse(rows[0]!.manifest)).toMatchObject({
            sizeBytes: expected.length,
            name: reference.name,
            mimeType: reference.mimeType,
          });
          expect(json(stopped)).not.toMatch(
            /ISOLATED_DEVICE_TOKEN|device-agent382|outPath|localPath/,
          );
          const stable = yield* footprint();
          expect(
            (yield* drive(`${type}-stop`, { type, action: "stop" })).structuredContent,
          ).toEqual(stopped.structuredContent);
          expect((yield* drive(`${type}-fresh-stop`, { type, action: "stop" })).isError).toBe(true);
          expect(yield* footprint()).toEqual(stable);
        }
        // Public upload + real native staging. The qualified reference is read from
        // its existing owner using an observed, genuinely verified invocation.
        const metadata = {
          type: "file" as const,
          name: "app.apk",
          mimeType: "application/vnd.android.package-archive",
          sizeBytes: 4,
        };
        const packageBytes = new Uint8Array([0x50, 0x4b, 0x03, 0x04]);
        const digest = yield* typed383Digest(packageBytes);
        const prepared = yield* nativeCall(a, parent, "t3_attachment_prepare_upload", {
          ...route,
          upload: metadata,
          digest,
          clientRequestId: "media-package-upload",
        });
        expect(prepared.isError, json(prepared)).not.toBe(true);
        const issued = yield* decodeTyped383Upload(prepared.structuredContent);
        const http = yield* HttpClient.HttpClient;
        expect(
          (yield* http.execute(
            HttpClientRequest.post(`${b.url}${issued.relativeUrl}`).pipe(
              HttpClientRequest.setBody(HttpBody.uint8Array(packageBytes)),
            ),
          )).status,
        ).toBe(204);
        const observed = Option.getOrThrow(yield* Ref.get(b.deviceObservedScope));
        const staging = Option.getOrThrow(
          yield* Effect.serviceOption(AttachmentStaging.AttachmentStaging).pipe(
            Effect.provide(Layer.succeedContext(b.requestContext)),
          ),
        );
        const nativeReference = yield* staging.verify(observed, {
          ...metadata,
          id: issued.attachmentId,
        });
        const reference = nativeReference.reference;
        const beforePackages = yield* footprint();
        for (const [index, artifact] of [
          { ...reference, environmentId: a.environmentId },
          { ...reference, owner: "0".repeat(64) },
          { ...reference, digest: "0".repeat(64) },
          { ...reference, name: "borrowed.apk" },
        ].entries())
          expect(
            (yield* drive(`package-denied-${index}`, {
              type: "install",
              artifact,
              app: "com.example.app",
            })).isError,
          ).toBe(true);
        expect(yield* footprint()).toEqual(beforePackages);
        const command = { type: "install", artifact: reference, app: "com.example.app" };
        const installed = yield* drive("package-install", command);
        expect(installed.isError, json(installed)).not.toBe(true);
        const leaf = b.deviceProcessCalls.at(-1)!;
        expect(leaf.args.slice(leaf.args.indexOf("--") + 1)).toEqual([
          "com.example.app",
          nativeReference.path,
        ]);
        expect(json(installed)).not.toContain(nativeReference.path);
        const afterInstall = yield* footprint();
        expect((yield* drive("package-install", command)).structuredContent).toEqual(
          installed.structuredContent,
        );
        yield* fs.writeFile(nativeReference.path, new Uint8Array([1, 2, 3, 4]));
        expect((yield* drive("package-mutated", command)).isError).toBe(true);
        expect(yield* footprint()).toEqual(afterInstall);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "typed383-media: producer escape and changed package bytes remain unknown without publication or repeated effects",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, route, open, footprint } =
          yield* typed383Pair("typed383-media-denial");
        const { target } = yield* decodeTyped383Open(
          (yield* open("escape-open")).structuredContent,
        );
        const drive = (key: string, command: unknown) =>
          nativeCall(a, parent, "device_execute", {
            ...route,
            target,
            command,
            clientRequestId: key,
          });
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const beforeRows = yield* sql`SELECT * FROM attachment_staging`;
        expect((yield* drive("escape-start", { type: "trace", action: "start" })).isError).not.toBe(
          true,
        );
        yield* Ref.set(b.deviceMediaOutside, true);
        const escaped = yield* drive("escape-stop", { type: "trace", action: "stop" });
        expect(escaped.isError).toBe(true);
        expect(json(escaped)).toContain("outcome_unknown");
        expect(yield* sql`SELECT * FROM attachment_staging`).toEqual(beforeRows);
        const afterEscape = yield* footprint();
        yield* Ref.set(b.deviceMediaOutside, false);
        expect(json(yield* drive("escape-stop", { type: "trace", action: "stop" }))).toContain(
          "outcome_unknown",
        );
        expect(yield* footprint()).toEqual(afterEscape);
        const fs = yield* FileSystem.FileSystem;
        for (const file of b.deviceMediaPaths.values()) expect(yield* fs.exists(file)).toBe(true);
        const metadata = {
          type: "file" as const,
          name: "app.apk",
          mimeType: "application/vnd.android.package-archive",
          sizeBytes: 4,
        };
        const bytes = new Uint8Array([0x50, 0x4b, 0x03, 0x04]);
        const digest = yield* typed383Digest(bytes);
        const prepared = yield* nativeCall(a, parent, "t3_attachment_prepare_upload", {
          ...route,
          upload: metadata,
          digest,
          clientRequestId: "race-package-upload",
        });
        expect(prepared.isError, json(prepared)).not.toBe(true);
        const issued = yield* decodeTyped383Upload(prepared.structuredContent);
        const http = yield* HttpClient.HttpClient;
        expect(
          (yield* http.execute(
            HttpClientRequest.post(`${b.url}${issued.relativeUrl}`).pipe(
              HttpClientRequest.setBody(HttpBody.uint8Array(bytes)),
            ),
          )).status,
        ).toBe(204);
        const scope = Option.getOrThrow(yield* Ref.get(b.deviceObservedScope));
        const staging = Option.getOrThrow(
          yield* Effect.serviceOption(AttachmentStaging.AttachmentStaging).pipe(
            Effect.provide(Layer.succeedContext(b.requestContext)),
          ),
        );
        const verified = yield* staging.verify(scope, { ...metadata, id: issued.attachmentId });
        const input = { type: "install", app: "com.example.app", artifact: verified.reference };
        yield* Ref.set(b.deviceProcessHook, (call) =>
          call.args[1] === "capabilities"
            ? fs.writeFile(verified.path, new Uint8Array([4, 3, 2, 1])).pipe(Effect.orDie)
            : Effect.void,
        );
        const changed = yield* drive("race-package-install", input);
        expect(changed.isError).toBe(true);
        expect(json(changed)).toContain("outcome_unknown");
        expect(b.deviceProcessCalls.filter((call) => call.args[1] === "install")).toHaveLength(0);
        const afterChange = yield* footprint();
        yield* fs.writeFile(verified.path, bytes);
        yield* Ref.set(b.deviceProcessHook, () => Effect.void);
        expect(json(yield* drive("race-package-install", input))).toContain("outcome_unknown");
        expect(yield* footprint()).toEqual(afterChange);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "typed383-negotiation: requests-v1 with actual old device domain refuses before destination preparation",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { open, footprint } = yield* typed383Pair("typed383-legacy-domain", true);
        const untouched = yield* footprint();
        const response = yield* open("legacy-domain-open");
        expect(response.isError, json(response)).toBe(true);
        expect(json(response)).not.toContain("--config");
        expect(json(response)).not.toContain("agent-device.mjs");
        expect(yield* footprint()).toEqual(untouched);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "typed383-effects: native grant retirement after click prevents fill and uncertain replay never repeats work",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, route, open } = yield* typed383Pair("typed383-retire");
        const opened = yield* open("retire-open");
        expect(opened.isError, json(opened)).not.toBe(true);
        const { target } = yield* decodeTyped383Target(opened.structuredContent);
        const grant = (yield* b.authority.grants)[0]!;
        yield* Ref.set(b.deviceProcessHook, (call) =>
          call.args[1] === "click"
            ? b.authority.setGrant({ ...grant, device: false }).pipe(Effect.asVoid, Effect.orDie)
            : Effect.void,
        );
        const input = {
          ...route,
          target,
          command: {
            type: "batch",
            steps: [
              { type: "click", ref: "@e1" },
              { type: "fill", ref: "@e1", text: "must never dispatch" },
            ],
          },
          clientRequestId: "retire-batch",
        };
        const failed = yield* nativeCall(a, parent, "device_execute", input);
        expect(failed.isError, json(failed)).toBe(true);
        expect(json(failed)).toContain("outcome_unknown");
        expect(b.deviceProcessCalls.filter((call) => call.args[1] === "click")).toHaveLength(1);
        expect(b.deviceProcessCalls.filter((call) => call.args[1] === "fill")).toHaveLength(0);
        yield* b.authority.setGrant({ ...grant, device: true });
        yield* Ref.set(b.deviceProcessHook, () => Effect.void);
        const before = b.deviceProcessCalls.length;
        const replay = yield* nativeCall(a, parent, "device_execute", input);
        expect(replay.isError, json(replay)).toBe(true);
        expect(json(replay)).toContain("outcome_unknown");
        expect(b.deviceProcessCalls).toHaveLength(before);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "typed383-source-policy: actual originating project policy retirement prevents the next batch effect",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, route, open } = yield* typed383Pair(
          "typed383-source-policy",
          false,
          true,
        );
        const opened = yield* open("source-policy-open");
        expect(opened.isError, json(opened)).not.toBe(true);
        const { target } = yield* decodeTyped383Open(opened.structuredContent);
        const original = yield* a.fixtureSettings.getSettings;
        yield* Ref.set(b.deviceProcessHook, (call) =>
          call.args[1] === "click"
            ? a.fixtureSettings
                .updateSettings({
                  projectSettingsOverrides: {
                    ...original.projectSettingsOverrides,
                    [a.project.id]: { enableAgentDeviceAccess: false },
                  },
                })
                .pipe(Effect.asVoid, Effect.orDie)
            : Effect.void,
        );
        const input = {
          ...route,
          target,
          clientRequestId: "source-policy-batch",
          command: {
            type: "batch",
            steps: [
              { type: "click", ref: "@e1" },
              { type: "fill", ref: "@e1", text: "must not dispatch" },
            ],
          },
        };
        const denied = yield* nativeCall(a, parent, "device_execute", input);
        expect(denied.isError, json(denied)).toBe(true);
        expect(json(denied)).toContain("outcome_unknown");
        expect(b.deviceProcessCalls.filter((call) => call.args[1] === "click")).toHaveLength(1);
        expect(b.deviceProcessCalls.filter((call) => call.args[1] === "fill")).toHaveLength(0);
        expect(
          (yield* a.fixtureSettings.getSettings).projectSettingsOverrides[a.project.id],
        ).toMatchObject({ enableAgentDeviceAccess: false });
        yield* a.fixtureSettings.updateSettings({
          projectSettingsOverrides: original.projectSettingsOverrides,
        });
        yield* Ref.set(b.deviceProcessHook, () => Effect.void);
        const before = b.deviceProcessCalls.length;
        const replay = yield* nativeCall(a, parent, "device_execute", input);
        expect(replay.isError, json(replay)).toBe(true);
        expect(json(replay)).toContain("outcome_unknown");
        expect(b.deviceProcessCalls).toHaveLength(before);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "typed383-core: wrong installed runtime or missing pin refuses without preparation",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, route, open, footprint } = yield* typed383Pair("typed383-version");
        const target = (yield* decodeTyped383Open(
          (yield* open("typed383-version-open")).structuredContent,
        )).target;
        const stable = yield* footprint();
        yield* Ref.set(b.deviceOldVersion, true);
        expect((yield* nativeCall(a, parent, "device_commands", route)).isError).toBe(true);
        expect(
          (yield* nativeCall(a, parent, "device_execute", {
            ...route,
            target,
            command: { type: "click", ref: "@e1" },
            clientRequestId: "typed383-old-version",
          })).isError,
        ).toBe(true);
        expect(yield* footprint()).toEqual(stable);
        yield* Ref.set(b.deviceOldVersion, false);
        const fs = yield* FileSystem.FileSystem;
        const sentinel = `${b.config.baseDir}/tools/agent-device/${AGENT_DEVICE_VERSION}/.install-complete`;
        yield* fs.remove(sentinel);
        expect((yield* nativeCall(a, parent, "device_commands", route)).isError).toBe(true);
        expect((yield* open("typed383-missing-pin")).isError).toBe(true);
        expect(yield* footprint()).toEqual(stable);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "typed383-core: closed inputs and actual capabilities cannot redirect or dispatch an unsupported action",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, route, open, footprint } = yield* typed383Pair("typed383-boundary");
        const target = (yield* decodeTyped383Open(
          (yield* open("typed383-boundary-open")).structuredContent,
        )).target;
        const input = {
          ...route,
          target,
          command: { type: "click", ref: "@e1" },
          clientRequestId: "typed383-invalid",
        };
        const stable = yield* footprint();
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const receipts = yield* sql`SELECT id,state FROM mcp_request_receipts ORDER BY id`;
        for (const command of [
          { type: "click", ref: "@e1", config: "/evil" },
          { type: "fill", ref: "@e1", text: "x", session: "evil" },
          { type: "snapshot", argv: ["--config", "/evil"] },
          { type: "click", x: NaN, y: Infinity },
          { type: "close", token: "evil" },
        ]) {
          expect(
            (yield* nativeInvalidParams(a, parent, "device_execute", { ...input, command })).error
              .code,
          ).toBe(-32602);
        }
        for (const extra of [
          { session: "evil" },
          { config: "/evil" },
          { token: "evil" },
          { argv: [] },
          { daemon: true },
        ])
          expect(
            (yield* nativeInvalidParams(a, parent, "device_execute", { ...input, ...extra })).error
              .code,
          ).toBe(-32602);
        expect(yield* footprint()).toEqual(stable);
        expect(yield* sql`SELECT id,state FROM mcp_request_receipts ORDER BY id`).toEqual(receipts);
        const supported = yield* Ref.get(b.deviceCapabilities);
        for (const [index, device] of [
          { ...supported.device, id: "rival" },
          { ...supported.device, platform: "ios" },
          { ...supported.device, kind: "physical" },
        ].entries()) {
          yield* Ref.set(b.deviceCapabilities, { ...supported, device });
          const before = b.deviceProcessCalls.filter((call) => call.args[1] === "click").length;
          expect(
            (yield* nativeCall(a, parent, "device_execute", {
              ...input,
              clientRequestId: `typed383-capability-${index}`,
            })).isError,
          ).toBe(true);
          expect(b.deviceProcessCalls.filter((call) => call.args[1] === "click")).toHaveLength(
            before,
          );
        }
        yield* Ref.set(b.deviceCapabilities, { ...supported, availableCommands: ["snapshot"] });
        const before = b.deviceProcessCalls.filter((call) => call.args[1] === "click").length;
        expect(
          (yield* nativeCall(a, parent, "device_execute", {
            ...input,
            clientRequestId: "typed383-missing-command",
          })).isError,
        ).toBe(true);
        expect(b.deviceProcessCalls.filter((call) => call.args[1] === "click")).toHaveLength(
          before,
        );
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "typed383-core: an old peer without durable requests cannot fall back to a destination launcher",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("typed383-old-a", "A", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true, deviceOwner: true },
        );
        yield* Effect.addFinalizer(() => assertTyped383SourceIdle(a));
        const b = yield* makeNode(
          descriptorOf("typed383-old-b", "B", { peerAuthority: 1 }),
          "disk",
          {
            productionKernel: true,
            attachmentRequests: true,
            preferencesOwner: true,
            deviceOwner: true,
          },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("typed383-old-parent");
        yield* seedParent(a, parent);
        const launch = yield* nativeCall(a, parent, "t3_thread_launch", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          title: "Old peer",
          message: "Wait",
          modelSelection: selection,
        });
        expect(launch.isError, json(launch)).not.toBe(true);
        const child = (yield* decodeLaunchReceipt(launch.structuredContent)).threadId;
        yield* activate(b, child);
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, device: true });
        const sessions = yield* b.fixtureDevice!.sessionsForThread(child);
        // Launch and pair were accepted by the modern owner. Reconstruct the actual
        // same-database transport without request/staging owners before the device call.
        // No auth/session/grant/pair is fabricated or weakened.
        const old = { ...b, ...(yield* b.restartWithoutRequestsTransport) };
        yield* Context.get(
          a.database,
          SqlClient.SqlClient,
        )`UPDATE peer_environment_links SET urls_json=${json([old.url])} WHERE environment_id=${b.environmentId}`;
        const unsupported = yield* Context.get(a.services, PeerMcpClient.PeerMcpClient)
          .requireRequestProtocol!(b.environmentId).pipe(Effect.result);
        expect(unsupported._tag).toBe("Failure");
        const result = yield* nativeCall(a, parent, "device_open", {
          environmentId: b.environmentId,
          contextThreadId: child,
          deviceId: "fixture-device382",
          platform: "android",
          clientRequestId: "typed383-old-peer-open",
        });
        expect(result.isError, json(result)).toBe(true);
        expect(yield* Ref.get(b.deviceHostStarts)).toBe(0);
        expect(yield* Ref.get(b.deviceAgentStarts)).toBe(0);
        expect(yield* Ref.get(b.deviceConfigurations)).toBe(0);
        expect(b.deviceProcessCalls).toHaveLength(0);
        expect(yield* b.fixtureDevice!.sessionsForThread(child)).toEqual(sessions);
        expect(json(result)).not.toContain("agent-device.mjs");
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "typed383-core: actual unlink and replacement grant fence old openings while a lawful new child can open",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, child, route, open, footprint } =
          yield* typed383Pair("typed383-generation");
        const oldTarget = (yield* decodeTyped383Open(
          (yield* open("typed383-generation-open")).structuredContent,
        )).target;
        const original = {
          ...route,
          target: oldTarget,
          command: { type: "click", ref: "@e1" },
          clientRequestId: "typed383-generation-click",
        };
        expect((yield* nativeCall(a, parent, "device_execute", original)).isError).not.toBe(true);
        const oldLink = yield* a.links.resolve(b.environmentId);
        const oldGrant = (yield* b.authority.grants)[0]!;
        const before = yield* footprint();
        yield* a.links.unlink(b.environmentId);
        yield* linkTo(a.links, b, "full-access");
        const currentLink = yield* a.links.resolve(b.environmentId);
        expect(currentLink.link.launchIdentity).not.toBe(oldLink.link.launchIdentity);
        const enrolled = yield* nativeCall(a, parent, "t3_project_list", {
          environmentId: b.environmentId,
        });
        expect(enrolled.isError, json(enrolled)).not.toBe(true);
        const replacementGrant = (yield* b.authority.grants).find(
          (grant) => grant.sessionId !== oldGrant.sessionId,
        );
        if (replacementGrant === undefined)
          return yield* Effect.die("Expected a real newly paired grant");
        yield* b.authority.setGrant({ ...replacementGrant, device: true });
        // The old exact context and old completed action must fail under current link authority.
        for (const key of [original.clientRequestId, "typed383-generation-old-fresh"])
          expect(
            (yield* nativeCall(a, parent, "device_execute", { ...original, clientRequestId: key }))
              .isError,
          ).toBe(true);
        expect(yield* footprint()).toEqual(before);
        const launch = yield* nativeCall(a, parent, "t3_thread_launch", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          title: "Replacement generation",
          message: "Wait",
          modelSelection: selection,
          clientRequestId: "typed383-generation-child-new",
        });
        expect(launch.isError, json(launch)).not.toBe(true);
        const newChild = (yield* decodeLaunchReceipt(launch.structuredContent)).threadId;
        expect(newChild).not.toBe(child);
        yield* activate(b, newChild);
        const replacementRoute = { environmentId: b.environmentId, contextThreadId: newChild };
        // A current lawful context cannot claim the old opening, even with the old key.
        for (const key of [original.clientRequestId, "typed383-generation-new-context-old-target"])
          expect(
            (yield* nativeCall(a, parent, "device_execute", {
              ...original,
              ...replacementRoute,
              clientRequestId: key,
            })).isError,
          ).toBe(true);
        expect(yield* footprint()).toEqual(before);
        expect(yield* b.fixtureDevice!.sessionsForThread(newChild)).toHaveLength(0);
        const opened = yield* nativeCall(a, parent, "device_open", {
          ...replacementRoute,
          deviceId: "fixture-device382",
          platform: "android",
          clientRequestId: "typed383-generation-legitimate-open",
        });
        expect(opened.isError, json(opened)).not.toBe(true);
        const target = (yield* decodeTyped383Open(opened.structuredContent)).target;
        expect(target.toolSessionId).not.toBe(oldTarget.toolSessionId);
        const action = yield* nativeCall(a, parent, "device_execute", {
          ...replacementRoute,
          target,
          command: { type: "click", ref: "@e1" },
          clientRequestId: "typed383-generation-legitimate-click",
        });
        expect(action.structuredContent).toEqual({ type: "action", target, status: "dispatched" });
        expect(b.deviceProcessCalls.filter((call) => call.args[1] === "click")).toHaveLength(2);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "device382: registered native device owners start after receipt and historical open/empty-close cannot replace later intent",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const node = yield* makeNode(
          descriptorOf("device382", "Isolated devices", { peerAuthority: 1 }),
          "disk",
          {
            productionKernel: true,
            attachmentRequests: true,
            preferencesOwner: true,
            deviceOwner: true,
          },
        );
        const parent = ThreadId.make("device382-parent");
        yield* seedParent(node, parent);
        const devices = node.fixtureDevice;
        if (devices === undefined) throw new Error("Expected the real isolated device owner");
        const emptyClose = { clientRequestId: "device382-empty-close" };
        expect((yield* nativeCall(node, parent, "device_close", emptyClose)).isError).not.toBe(
          true,
        );
        expect(yield* Ref.get(node.deviceAgentStarts)).toBe(0);
        yield* devices.open({
          threadId: parent,
          hostId: LOCAL_DEVICE_HOST_ID,
          deviceId: DeviceId.make("fixture-device382"),
          platform: "android",
        });
        expect(yield* devices.sessionsForThread(parent)).toHaveLength(1);
        expect((yield* nativeCall(node, parent, "device_close", emptyClose)).isError).not.toBe(
          true,
        );
        expect(yield* devices.sessionsForThread(parent)).toHaveLength(1);
        const input = {
          deviceId: "fixture-device382",
          platform: "android",
          clientRequestId: "device382-open",
        };
        const opened = yield* nativeCall(node, parent, "device_open", input);
        expect(opened.isError, json(opened)).not.toBe(true);
        expect(yield* Ref.get(node.deviceAgentStarts)).toBe(1);
        expect(yield* Ref.get(node.deviceConfigurations)).toBe(1);
        expect((yield* nativeCall(node, parent, "device_open", input)).structuredContent).toEqual(
          opened.structuredContent,
        );
        expect(yield* Ref.get(node.deviceAgentStarts)).toBe(1);
        expect(yield* Ref.get(node.deviceConfigurations)).toBe(1);
        yield* devices.close({ threadId: parent });
        expect((yield* nativeCall(node, parent, "device_open", input)).structuredContent).toEqual(
          opened.structuredContent,
        );
        expect(yield* devices.sessionsForThread(parent)).toHaveLength(0);
        const fs = yield* FileSystem.FileSystem;
        const file = yield* agentDeviceConfigPath(
          node.config.stateDir,
          LOCAL_DEVICE_HOST_ID,
          yield* Path.Path,
        );
        yield* fs.writeFileString(file, "Later native configuration before the new action");
        yield* Ref.set(node.loseDeviceConfiguration, true);
        const uncertain = { ...input, clientRequestId: "device382-uncertain" };
        const failed = yield* nativeCall(node, parent, "device_open", uncertain);
        expect(failed.isError, json(failed)).toBe(true);
        expect(json(failed)).toContain("outcome_unknown");
        expect(yield* Ref.get(node.deviceAgentStarts)).toBe(2);
        expect(yield* Ref.get(node.deviceConfigurations)).toBe(2);
        expect(yield* fs.readFileString(file)).toContain("ISOLATED_DEVICE_TOKEN_382");
        const later = "Independent configuration after the uncertain original action";
        yield* fs.writeFileString(file, later);
        const active = { ...node, ...(yield* node.restartSchedulerTransport) };
        const again = yield* nativeCall(active, parent, "device_open", uncertain);
        expect(json(again)).toContain("outcome_unknown");
        expect(yield* Ref.get(node.deviceAgentStarts)).toBe(2);
        expect(yield* Ref.get(node.deviceConfigurations)).toBe(2);
        expect(yield* fs.readFileString(file)).toBe(later);
        expect(yield* devices.sessionsForThread(parent)).toHaveLength(0);
        const sql = Context.get(node.database, SqlClient.SqlClient);
        expect(json(yield* sql`SELECT * FROM mcp_request_receipts`)).not.toContain(
          "ISOLATED_DEVICE_TOKEN_382",
        );
        yield* node.fixtureSettings.updateSettings({ enableAgentDeviceAccess: false });
        const denied = yield* nativeCall(active, parent, "device_open", input);
        expect(denied.isError, json(denied)).toBe(true);
        expect(yield* Ref.get(node.deviceAgentStarts)).toBe(2);
        expect(yield* Ref.get(node.deviceConfigurations)).toBe(2);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "png382: registered peer PNG saves stay on the originating environment and retain exact artifact bytes",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("png382-source", "PNG source", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf("png382-destination", "PNG destination", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("png382-parent");
        yield* seedParent(a, parent);
        const launch = yield* nativeCall(a, parent, "t3_thread_launch", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          title: "PNG owner",
          message: "Wait",
          modelSelection: selection,
          clientRequestId: "png382-owner",
        });
        expect(launch.isError, json(launch)).not.toBe(true);
        const child = (yield* decodeLaunchReceipt(launch.structuredContent)).threadId;
        yield* activate(b, child);
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, preview: true });
        const connected = yield* Deferred.make<void>();
        const png = Buffer.from(
          "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScLbtAAAAABJRU5ErkJggg==",
          "base64",
        );
        let reads = 0;
        const input = {
          environmentId: b.environmentId,
          contextThreadId: child,
          save: true,
          includeImage: true,
          clientRequestId: "png382-saved",
        };
        const first = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* Stream.runForEach(
              yield* b.browser.connect({
                clientId: "png382-renderer",
                environmentId: b.environmentId,
              }),
              (event) =>
                event.type === "connected"
                  ? Deferred.succeed(connected, undefined)
                  : Effect.gen(function* () {
                      reads++;
                      yield* b.browser.respond({
                        clientId: "png382-renderer",
                        connectionId: event.connectionId,
                        requestId: event.request.requestId,
                        ok: true,
                        result: {
                          url: "https://isolated-png.example.test/private?fixture=DO_NOT_CACHE_PAGE_URL_382",
                          title: "Isolated page",
                          loading: false,
                          visibleText: "DO_NOT_CACHE_PAGE_TEXT_382",
                          interactiveElements: [],
                          accessibilityTree: {},
                          consoleEntries: [],
                          networkEntries: [],
                          actionTimeline: [],
                          screenshot: {
                            mimeType: "image/png",
                            data: png.toString("base64"),
                            width: 1,
                            height: 1,
                          },
                        },
                      });
                    }),
            ).pipe(Effect.forkScoped);
            yield* Deferred.await(connected);
            return yield* nativeCall(a, parent, "preview_snapshot", input);
          }),
        );

        expect(first.isError, json(first)).not.toBe(true);
        const receipt = yield* decodeSavedPngReceipt(first.structuredContent);
        expect(receipt.artifact.environmentId).toBe(a.environmentId);
        expect(receipt.artifact.sizeBytes).toBe(png.byteLength);
        expect(receipt.artifact.digest).toBe(
          "2640059609118b695c139804676372eca75c30d30e5906775902d13e44f5356c",
        );
        const fs = yield* FileSystem.FileSystem;
        expect(Array.from(yield* fs.readFile(receipt.artifact.screenshotPath))).toEqual(
          Array.from(png),
        );
        expect(receipt.artifact.screenshotPath.startsWith(a.config.browserArtifactsDir)).toBe(true);
        expect(yield* fs.exists(b.config.browserArtifactsDir)).toBe(false);
        expect(first.content.some((item) => item.type === "image")).toBe(true);
        expect((yield* nativeCall(a, parent, "preview_snapshot", input)).structuredContent).toEqual(
          receipt,
        );
        expect(yield* fs.readDirectory(a.config.browserArtifactsDir)).toHaveLength(1);
        const sql = Context.get(a.database, SqlClient.SqlClient);
        expect(json(yield* sql`SELECT * FROM mcp_request_receipts`)).not.toContain(
          "DO_NOT_CACHE_PAGE_",
        );
        expect(reads).toBe(1);
        // Existing confirmed PNGs can reconcile their exact original forwarding ledger.
        yield* sql`DELETE FROM mcp_request_destinations WHERE request_id=${receipt.operationId} AND operation='saved-preview-authorization'`;
        expect((yield* nativeCall(a, parent, "preview_snapshot", input)).structuredContent).toEqual(
          receipt,
        );
        const revokedPreview = { ...grant, preview: false };
        yield* b.authority.setGrant(revokedPreview);
        const previewDenied = yield* nativeCall(a, parent, "preview_snapshot", input);
        expect(previewDenied.isError).toBe(true);
        expect(previewDenied.structuredContent).toMatchObject({
          code: "capability_denied",
          message:
            'MCP credential does not grant the preview capability: browser preview tools are off for this thread. Do not retry them. To check a page, use a headless browser from the shell, such as Playwright, or curl. The user can turn on "Agent browser access" in Settings; it applies when the agent session next starts.',
        });
        const newSaveDenied = yield* nativeCall(a, parent, "preview_snapshot", {
          ...input,
          clientRequestId: "png382-ungranted-new-save",
        });
        expect(newSaveDenied.isError).toBe(true);
        expect(newSaveDenied.structuredContent).toEqual(previewDenied.structuredContent);
        expect(reads).toBe(1);
        expect(yield* fs.readDirectory(a.config.browserArtifactsDir)).toHaveLength(1);
        expect(Array.from(yield* fs.readFile(receipt.artifact.screenshotPath))).toEqual(
          Array.from(png),
        );
        yield* b.authority.setGrant({ ...grant, preview: true });
        expect((yield* nativeCall(a, parent, "preview_snapshot", input)).structuredContent).toEqual(
          receipt,
        );
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("png382-narrow-current"),
          threadId: parent,
          runtimeMode: "auto",
        });
        expect((yield* nativeCall(a, parent, "preview_snapshot", input)).isError).toBe(true);
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("png382-restore-current"),
          threadId: parent,
          runtimeMode: "full-access",
        });
        const unrelated = ThreadId.make("png382-unrelated-context");
        yield* seedParent(b, unrelated);
        const outside = yield* nativeCall(a, parent, "preview_snapshot", {
          ...input,
          contextThreadId: unrelated,
          clientRequestId: "png382-outside-context",
        });
        expect(outside.isError).toBe(true);
        expect(json(outside)).toContain("resource scope");
        yield* b.setSavedPreviewTransport("unsupported");
        const older = yield* nativeCall(a, parent, "preview_snapshot", input);
        expect(older.isError).toBe(true);
        expect(json(older)).toContain("does not support saved-preview authorization");
        const unsupportedNewSave = yield* nativeCall(a, parent, "preview_snapshot", {
          ...input,
          clientRequestId: "png382-old-peer-new-save",
        });
        expect(unsupportedNewSave.isError).toBe(true);
        expect(json(unsupportedNewSave)).toContain("does not support saved-preview authorization");
        expect(yield* fs.readDirectory(a.config.browserArtifactsDir)).toHaveLength(1);
        yield* b.setSavedPreviewTransport("unavailable");
        const unavailableAuthority = yield* nativeCall(a, parent, "preview_snapshot", input);
        expect(unavailableAuthority.isError).toBe(true);
        expect(json(unavailableAuthority)).toContain("authority is unavailable");
        yield* b.setSavedPreviewTransport("supported");
        expect((yield* nativeCall(a, parent, "preview_snapshot", input)).structuredContent).toEqual(
          receipt,
        );
        expect(reads).toBe(1);
        yield* fs.writeFileString(
          receipt.artifact.screenshotPath,
          "Later independent PNG path edit",
        );
        const replay = yield* nativeCall(a, parent, "preview_snapshot", input);
        expect(replay.structuredContent).toEqual(receipt);
        expect(replay.content.some((item) => item.type === "image")).toBe(false);
        expect(yield* fs.readFileString(receipt.artifact.screenshotPath)).toBe(
          "Later independent PNG path edit",
        );
        expect(reads).toBeGreaterThan(0);
        // Ordinary inspection remains available independently of historical saves.
        yield* Stream.runForEach(
          yield* b.browser.connect({ clientId: "png382-renderer", environmentId: b.environmentId }),
          (event) =>
            event.type === "connected"
              ? Deferred.succeed(connected, undefined)
              : Effect.gen(function* () {
                  reads++;
                  yield* b.browser.respond({
                    clientId: "png382-renderer",
                    connectionId: event.connectionId,
                    requestId: event.request.requestId,
                    ok: true,
                    result: {
                      url: "https://isolated-png.example.test/private?fixture=DO_NOT_CACHE_PAGE_URL_382",
                      title: "Isolated page",
                      loading: false,
                      visibleText: "DO_NOT_CACHE_PAGE_TEXT_382",
                      interactiveElements: [],
                      accessibilityTree: {},
                      consoleEntries: [],
                      networkEntries: [],
                      actionTimeline: [],
                      screenshot: {
                        mimeType: "image/png",
                        data: png.toString("base64"),
                        width: 1,
                        height: 1,
                      },
                    },
                  });
                }),
        ).pipe(Effect.forkScoped);
        yield* Deferred.await(connected);
        yield* b.setSavedPreviewTransport("unsupported");
        const inspect = yield* nativeCall(a, parent, "preview_snapshot", {
          environmentId: b.environmentId,
          contextThreadId: child,
        });
        expect(inspect.isError, json(inspect)).not.toBe(true);
        expect(inspect.structuredContent).not.toHaveProperty("type");
        yield* b.setSavedPreviewTransport("supported");
        yield* b.auth.revokeSession(grant.sessionId);
        const revoked = yield* nativeCall(a, parent, "preview_snapshot", input);
        expect(revoked.isError).toBe(true);
        expect(revoked.content.some((item) => item.type === "image")).toBe(false);
        yield* a.links.unlink(b.environmentId);
        expect((yield* nativeCall(a, parent, "preview_snapshot", input)).isError).toBe(true);
        yield* linkTo(a.links, b, "full-access");
        yield* a.forwarding.call(
          caller(a, parent),
          ProjectToolkit.tools.t3_project_list,
          b.environmentId,
          {},
        );
        const replacementGrant = (yield* b.authority.grants).find(
          (item) => item.sessionId !== grant.sessionId,
        )!;
        yield* b.authority.setGrant({
          ...replacementGrant,
          preview: true,
          resourceScope: "environment-resources",
        });
        const replaced = yield* nativeCall(a, parent, "preview_snapshot", input);
        expect(replaced.isError).toBe(true);
        expect(json(replaced)).toContain("original destination grant generation");
        expect(yield* fs.readFileString(receipt.artifact.screenshotPath)).toBe(
          "Later independent PNG path edit",
        );
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "recording382: registered peer recording returns an owned bounded reference usable by attachment send",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("recording382-source", "Source", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf("recording382-destination", "Destination", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("recording382-parent");
        yield* seedParent(a, parent);
        const launched = yield* nativeCall(a, parent, "t3_thread_launch", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          title: "Recording owner",
          message: "Wait",
          modelSelection: selection,
          clientRequestId: "recording382-owner",
        });
        expect(launched.isError, json(launched)).not.toBe(true);
        const child = (yield* decodeLaunchReceipt(launched.structuredContent)).threadId;
        yield* activate(b, child);
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, preview: true });
        const bytes = new Uint8Array([0x1a, 0x45, 0xdf, 0xa3, 0x81, 1, 0x42, 0x86, 0x81, 1]);
        const uploaded = yield* AttachmentUpload.issueAttachmentUploadUrl({
          type: "file",
          name: "recording.webm",
          mimeType: "video/webm",
          sizeBytes: bytes.byteLength,
        }).pipe(
          Effect.provideService(Config.ServerConfig, b.config),
          Effect.provideService(
            Secrets.ServerSecretStore,
            Context.get(b.services, Secrets.ServerSecretStore),
          ),
        );
        const http = yield* HttpClient.HttpClient;
        expect(
          (yield* http.execute(
            HttpClientRequest.post(`${b.url}${uploaded.relativeUrl}`).pipe(
              HttpClientRequest.setBody(HttpBody.uint8Array(bytes)),
            ),
          )).status,
        ).toBe(204);
        const connected = yield* Deferred.make<void>();
        let stops = 0;
        let rendererCalls = 0;
        yield* Stream.runForEach(
          yield* b.browser.connect({
            clientId: "recording382-renderer",
            environmentId: b.environmentId,
          }),
          (event) =>
            event.type === "connected"
              ? Deferred.succeed(connected, undefined)
              : Effect.gen(function* () {
                  const request = event.request;
                  rendererCalls++;
                  if (request.operation === "recordingStop") {
                    stops++;
                    yield* b.browser.respond({
                      clientId: "recording382-renderer",
                      connectionId: event.connectionId,
                      requestId: request.requestId,
                      ok: true,
                      result: {
                        id: "renderer-recording",
                        tabId: "recording382-tab",
                        path: "/isolated-renderer/recording.webm",
                        uploadedAttachmentId: uploaded.attachmentId,
                        mimeType: "video/webm",
                        sizeBytes: bytes.byteLength,
                        createdAt: "2026-10-18T00:00:00Z",
                      },
                    });
                  } else
                    yield* b.browser.respond({
                      clientId: "recording382-renderer",
                      connectionId: event.connectionId,
                      requestId: request.requestId,
                      ok: true,
                      result: {
                        available: true,
                        visible: true,
                        tabId: null,
                        url: null,
                        title: null,
                        loading: false,
                      },
                    });
                }),
        ).pipe(Effect.forkScoped);
        yield* Deferred.await(connected);
        const input = {
          environmentId: b.environmentId,
          contextThreadId: child,
          clientRequestId: "recording382-stop",
        };
        const stopped = yield* nativeCall(a, parent, "preview_recording_stop", input);
        expect(stopped.isError, json(stopped)).not.toBe(true);
        const { resource } = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ resource: PeerAttachmentReference }),
        )(stopped.structuredContent);
        expect(resource).toMatchObject({
          environmentId: b.environmentId,
          name: "recording.webm",
          mimeType: "video/webm",
          sizeBytes: bytes.byteLength,
        });
        expect(resource.owner).toMatch(/^[a-f0-9]{64}$/);
        expect(resource.digest).toMatch(/^[a-f0-9]{64}$/);
        expect(stopped.structuredContent).not.toHaveProperty("path");
        expect(json(stopped)).not.toContain("/isolated-renderer/");
        const sent = yield* nativeCall(a, parent, "t3_thread_send_attachments", {
          environmentId: b.environmentId,
          threadId: child,
          clientRequestId: "recording382-send",
          message: "Owned recording",
          attachments: [
            {
              type: "file",
              id: resource.attachmentId,
              environmentId: resource.environmentId,
              name: resource.name,
              mimeType: resource.mimeType,
              sizeBytes: resource.sizeBytes,
            },
          ],
        });
        expect(sent.isError, json(sent)).not.toBe(true);
        const delivered = (yield* b.threads.getThreadRecords(child, ["messages"])).messages.flatMap(
          (message) => message.attachments,
        );
        expect(delivered).toHaveLength(1);
        const path = resolveAttachmentPathById({
          attachmentsDir: b.config.attachmentsDir,
          attachmentId: delivered[0]!.id,
        });
        expect(path).not.toBeNull();
        const fs = yield* FileSystem.FileSystem;
        expect(Array.from(yield* fs.readFile(path!))).toEqual(Array.from(bytes));
        const nativeDb = Context.get(b.database, SqlClient.SqlClient);
        const nativeBefore =
          yield* nativeDb`SELECT id,generation,input_digest,state,result_json FROM mcp_request_receipts WHERE state='completed' ORDER BY id`;
        expect(nativeBefore.length).toBeGreaterThan(0);
        const originalPath = resolveAttachmentPathById({
          attachmentsDir: b.config.attachmentsDir,
          attachmentId: resource.attachmentId,
        })!;
        const laterBytes = new Uint8Array([9, 8, 7, 6, 5, 4, 3, 2, 1, 0]);
        yield* fs.writeFile(originalPath, laterBytes);
        expect(Array.from(yield* fs.readFile(originalPath))).toEqual(Array.from(laterBytes));
        // A delayed duplicate POST uses the same still-valid app-issued upload token.
        expect(
          (yield* http.execute(
            HttpClientRequest.post(`${b.url}${uploaded.relativeUrl}`).pipe(
              HttpClientRequest.setBody(HttpBody.uint8Array(bytes)),
            ),
          )).status,
        ).toBe(204);
        const replay = yield* nativeCall(a, parent, "preview_recording_stop", input);
        expect(replay.isError, json(replay)).not.toBe(true);
        expect(replay.structuredContent).toEqual(stopped.structuredContent);
        expect(stops).toBe(1);
        expect(rendererCalls).toBe(1);
        expect(
          yield* nativeDb`SELECT id,generation,input_digest,state,result_json FROM mcp_request_receipts WHERE state='completed' ORDER BY id`,
        ).toEqual(nativeBefore);
        expect(Array.from(yield* fs.readFile(originalPath))).toEqual(Array.from(laterBytes));
        expect(stops).toBe(1);
        expect(rendererCalls).toBe(1);
        const originalNative =
          yield* nativeDb`SELECT id,owner,generation,manifest,digest,expected_digest FROM attachment_staging WHERE id=${resource.attachmentId}`;
        const rebuilt = { ...b, ...(yield* b.restartSchedulerTransport) };
        yield* Context.get(
          a.database,
          SqlClient.SqlClient,
        )`UPDATE peer_environment_links SET urls_json=${json([rebuilt.url])} WHERE environment_id=${b.environmentId}`;
        const renewed = yield* nativeCall(a, parent, "preview_recording_stop", input);
        expect(renewed.isError, json(renewed)).not.toBe(true);
        expect(renewed.structuredContent).toEqual(stopped.structuredContent);
        expect(stops).toBe(1);
        expect(rendererCalls).toBe(1);
        expect(Array.from(yield* fs.readFile(originalPath))).toEqual(Array.from(laterBytes));
        expect(
          yield* nativeDb`SELECT id,owner,generation,manifest,digest,expected_digest FROM attachment_staging WHERE id=${resource.attachmentId}`,
        ).toEqual(originalNative);
        expect(
          (yield* nativeCall(a, parent, "preview_recording_stop", {
            ...input,
            tabId: "changed-intent",
          })).isError,
        ).toBe(true);
        const other = ThreadId.make("recording382-rival");
        yield* seedParent(a, other);
        expect((yield* nativeCall(a, other, "preview_recording_stop", input)).isError).toBe(true);
        // A separate authorized caller has an independent request namespace.
        // Its new stop may run, but cannot acquire the first caller's reference.
        expect(stops).toBe(2);
        expect(rendererCalls).toBe(2);
        expect(Array.from(yield* fs.readFile(originalPath))).toEqual(Array.from(laterBytes));
        expect(
          yield* nativeDb`SELECT id,owner,generation,manifest,digest,expected_digest FROM attachment_staging WHERE id=${resource.attachmentId}`,
        ).toEqual(originalNative);
        const unrelated = ThreadId.make("recording382-outside-grant");
        yield* seedParent(b, unrelated);
        expect(
          (yield* nativeCall(a, parent, "preview_recording_stop", {
            ...input,
            contextThreadId: unrelated,
            clientRequestId: "recording382-outside",
          })).isError,
        ).toBe(true);
        expect(stops).toBe(2);
        yield* b.authority.setGrant({ ...grant, preview: false });
        expect((yield* nativeCall(a, parent, "preview_recording_stop", input)).isError).toBe(true);
        yield* b.authority.setGrant({ ...grant, preview: true });
        expect((yield* nativeCall(a, parent, "preview_recording_stop", input)).isError).not.toBe(
          true,
        );
        expect(stops, "grant denial / restore must not stop renderer").toBe(2);
        yield* a.links.unlink(b.environmentId);
        expect((yield* nativeCall(a, parent, "preview_recording_stop", input)).isError).toBe(true);
        expect(stops, "unlink denial must not stop renderer").toBe(2);
        yield* linkTo(a.links, rebuilt, "full-access");
        expect(
          (yield* nativeCall(a, parent, "t3_project_list", { environmentId: b.environmentId }))
            .isError,
        ).not.toBe(true);
        const replacement = (yield* b.authority.grants).find(
          (g) => g.sessionId !== grant.sessionId,
        )!;
        yield* b.authority.setGrant({ ...replacement, preview: true });
        expect((yield* nativeCall(a, parent, "preview_recording_stop", input)).isError).toBe(true);
        expect(stops).toBe(2);
        expect(rendererCalls).toBe(2);
        expect(Array.from(yield* fs.readFile(originalPath))).toEqual(Array.from(laterBytes));
        expect(
          yield* nativeDb`SELECT id,owner,generation,manifest,digest,expected_digest FROM attachment_staging WHERE id=${resource.attachmentId}`,
        ).toEqual(originalNative);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "spectrum382: registered Spectrum and participant commands retain identity across credential renewal",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const node = yield* makeNode(
          descriptorOf("spectrum382", "Spectrum", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const parent = ThreadId.make("spectrum382-parent");
        yield* seedParent(node, parent);
        const input = {
          question: "An isolated identity check",
          mode: "council",
          colors: [
            { label: "Blue", selection },
            { label: "Red", selection },
          ],
          clientRequestId: "spectrum382-once",
        };
        const first = yield* nativeCall(node, parent, "start_spectrum", input);
        expect(first.isError, json(first)).not.toBe(true);
        const spectrum = yield* Schema.decodeUnknownEffect(SpectrumMcp.StartSpectrumResult)(
          first.structuredContent,
        );
        expect(
          (yield* nativeCall(node, parent, "start_spectrum", input)).structuredContent,
        ).toEqual(spectrum);
        const sql = Context.get(node.database, SqlClient.SqlClient);
        expect(yield* sql`SELECT thread_id FROM fork_spectra`).toHaveLength(1);
        expect(
          yield* sql`SELECT command_id FROM orchestration_command_receipts WHERE command_type='spectrum.register'`,
        ).toHaveLength(1);
        const member = spectrum.participants[0]!.threadId;
        yield* node.threads.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("spectrum382-member-turn"),
          threadId: member,
          messageId: MessageId.make("spectrum382-member-input"),
          text: "Isolated work",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        yield* activate(node, member);
        const send = {
          threadId: parent,
          message: "Isolated participant report",
          clientRequestId: "spectrum382-member-send",
        };
        const sent = yield* nativeCall(node, member, "t3_thread_send", send);
        expect(sent.isError, json(sent)).not.toBe(true);
        const decoded = yield* decodeSend(sent.structuredContent);
        const replay = yield* nativeCall(node, member, "t3_thread_send", send);
        expect(replay.isError, json(replay)).not.toBe(true);
        expect(replay.structuredContent).toEqual(decoded);
        const messages = (yield* node.threads.getThreadRecords(parent, [
          "messages",
        ])).messages.filter((message) => message.id === decoded.messageId);
        expect(messages).toHaveLength(1);
        const changed = yield* nativeCall(node, parent, "start_spectrum", {
          ...input,
          question: "Changed question",
        });
        expect(changed.isError).toBe(true);
        expect(yield* sql`SELECT thread_id FROM fork_spectra`).toHaveLength(1);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "html382: registered peer HTML publication keeps its accepted artifact after replay and later edits",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("html382-source", "Source", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf("html382-destination", "Destination", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("html382-parent");
        yield* seedParent(a, parent);
        const launch = yield* nativeCall(a, parent, "t3_thread_launch", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          title: "HTML target",
          message: "Wait",
          modelSelection: selection,
          clientRequestId: "html382-target",
        });
        expect(launch.isError, json(launch)).not.toBe(true);
        const child = (yield* decodeLaunchReceipt(launch.structuredContent)).threadId;
        yield* activate(b, child);
        const input = {
          environmentId: b.environmentId,
          contextThreadId: child,
          html: "<!doctype html><html><body>Isolated HTML 382</body></html>",
          title: "Stable page",
          height: 420,
          clientRequestId: "html382-once",
        };
        const first = yield* nativeCall(a, parent, "html_render", input);
        expect(first.isError, json(first)).not.toBe(true);
        const result = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ htmlRender: Schema.Struct({ attachmentId: Schema.String }) }),
        )(first.structuredContent);
        const path = resolveAttachmentPathById({
          attachmentsDir: a.config.attachmentsDir,
          attachmentId: result.htmlRender.attachmentId,
        });
        expect(path).not.toBeNull();
        const fs = yield* FileSystem.FileSystem;
        expect(yield* fs.readFileString(path!)).toContain("Isolated HTML 382");
        yield* fs.writeFileString(path!, "Later independent page edit 382");
        const replay = yield* nativeCall(a, parent, "html_render", input);
        expect(replay.isError, json(replay)).not.toBe(true);
        expect(replay.structuredContent).toEqual(first.structuredContent);
        expect(yield* fs.readFileString(path!)).toBe("Later independent page edit 382");
        const changed = yield* nativeCall(a, parent, "html_render", {
          ...input,
          html: "<html>Changed semantic input</html>",
        });
        expect(changed.isError).toBe(true);
        expect(yield* fs.readFileString(path!)).toBe("Later independent page edit 382");
        const sql = Context.get(a.database, SqlClient.SqlClient);
        const rows = yield* sql`SELECT * FROM mcp_request_receipts WHERE state='completed'`;
        expect(rows).toHaveLength(1);
        expect(json(rows)).not.toContain(input.html);
        expect(json(rows)).toContain(result.htmlRender.attachmentId);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "named382: registered title-only project creation replays its exact native owner",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const node = yield* makeNode(
          descriptorOf("named382-registered", "Named", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true, nativeOwners: true },
        );
        const parent = ThreadId.make("named382-parent");
        yield* seedParent(node, parent);
        const input = { title: "Isolated named request", clientRequestId: "named382-original" };
        const first = yield* nativeCall(node, parent, "t3_project_create", input);
        expect(first.isError, json(first)).not.toBe(true);
        const created = yield* Schema.decodeUnknownEffect(
          ProjectToolkit.tools.t3_project_create.successSchema,
        )(first.structuredContent);
        const sql = Context.get(node.database, SqlClient.SqlClient);
        const rows = yield* sql<{
          project_id: string;
          workspace_root: string;
        }>`SELECT project_id,workspace_root FROM managed_project_requests`;
        expect(rows).toHaveLength(1);
        const fs = yield* FileSystem.FileSystem;
        const readme = `${rows[0]!.workspace_root}/README.md`;
        yield* fs.writeFileString(readme, "Later independent README edit 382");
        const replay = yield* nativeCall(node, parent, "t3_project_create", input);
        expect(replay.isError, json(replay)).not.toBe(true);
        expect(replay.structuredContent).toEqual(created);
        expect(yield* fs.readFileString(readme)).toBe("Later independent README edit 382");
        expect(yield* sql`SELECT project_id FROM managed_project_requests`).toHaveLength(1);
        expect(
          yield* sql`SELECT command_id FROM orchestration_command_receipts WHERE aggregate_kind='project' AND command_type='project.create'`,
        ).toHaveLength(1);
        const changed = yield* nativeCall(node, parent, "t3_project_create", {
          ...input,
          title: "Different title",
        });
        expect(changed.isError).toBe(true);
        expect(yield* sql`SELECT project_id FROM managed_project_requests`).toHaveLength(1);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "manual382: registered manual identity recovers native acceptance and preserves later scheduler work",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const node = yield* makeNode(
          descriptorOf("manual382", "Manual", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const parent = ThreadId.make("manual382-parent");
        yield* seedParent(node, parent);
        const { task } = yield* node.scheduled.upsert({
          commandId: CommandId.make("manual382-schedule"),
          title: "One native action",
          prompt: "Isolated manual work",
          enabled: true,
          schedule: { type: "interval", everyMs: 60_000 },
          projectId: node.project.id,
          threadId: null,
          workspaceStrategy: { type: "root" },
          modelSelection: selection,
          runtimeMode: "full-access",
          interactionMode: "default",
        });
        const input = { taskId: task.id, clientRequestId: "manual382-request" };
        const call = yield* nativeCall(node, parent, "run_scheduled_task_now", input);
        expect(call.isError, json(call)).not.toBe(true);
        const decoded = yield* Schema.decodeUnknownEffect(
          ThreadToolkit.tools.run_scheduled_task_now.successSchema,
        )(call.structuredContent);
        expect(decoded.manualReceipt).toMatchObject({
          requestId: input.clientRequestId,
          stage: "native-accepted",
        });
        const sql = Context.get(node.database, SqlClient.SqlClient);
        const messages =
          yield* sql`SELECT message_id FROM orchestration_v2_projection_messages WHERE role='user'`;
        expect(messages).toHaveLength(2);
        const before = (yield* node.scheduled.list()).tasks.find((t) => t.id === task.id)!;
        expect(before.runCount).toBe(1);
        yield* node.scheduled.setEnabled({ id: task.id, enabled: false });
        const later = (yield* node.scheduled.list()).tasks.find((t) => t.id === task.id)!;
        const replay = yield* nativeCall(node, parent, "run_scheduled_task_now", input);
        expect(replay.structuredContent).toEqual(decoded);
        expect((yield* node.scheduled.list()).tasks.find((t) => t.id === task.id)).toEqual(later);
        expect(
          yield* sql`SELECT message_id FROM orchestration_v2_projection_messages WHERE role='user'`,
        ).toHaveLength(2);
        const changed = yield* nativeCall(node, parent, "run_scheduled_task_now", {
          ...input,
          taskId: "different-schedule",
        });
        expect(changed.isError).toBe(true);

        // Stop the caller after actual native commit but before its reply/bookkeeping.
        yield* Ref.set(node.holdManualLaunchReply, true);
        const requestOwner = yield* RequestReceipts.layer.pipe(
          Layer.provide(Layer.succeedContext(node.database)),
          Layer.fresh,
          Layer.build,
        );
        const identity = yield* Context.get(
          requestOwner,
          RequestReceipts.McpRequestReceipts,
        ).identify(
          caller(node, parent),
          "run_scheduled_task_now",
          { taskId: task.id },
          "manual382-lost-native-reply",
        );
        const interrupted = yield* node.scheduled
          .runNow({ id: task.id }, { request: identity })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(node.manualLaunchReplyHeld);
        yield* Fiber.interrupt(interrupted);
        const nativeMessages =
          yield* sql`SELECT message_id FROM orchestration_v2_projection_messages WHERE role='user'`;
        expect(nativeMessages).toHaveLength(3);
        const rebuilt = yield* node.rebuildScheduler;
        const recovered = yield* Context.get(rebuilt, ScheduledTasks.ScheduledTaskService).runNow(
          { id: task.id },
          { request: identity },
        );
        expect(recovered.manualReceipt?.stage).toBe("native-accepted");
        expect(
          yield* sql`SELECT message_id FROM orchestration_v2_projection_messages WHERE role='user'`,
        ).toHaveLength(3);
        expect((yield* node.scheduled.list()).tasks.find((t) => t.id === task.id)?.runCount).toBe(
          2,
        );
        const denied = yield* Effect.result(
          Context.get(rebuilt, ScheduledTasks.ScheduledTaskService).runNow(
            { id: task.id },
            { request: { ...identity, generation: "replacement" } },
          ),
        );
        expect(denied._tag).toBe("Failure");
        yield* node.scheduled.delete({ id: task.id });
        yield* node.scheduled.upsert({
          id: task.id,
          title: "Replacement",
          prompt: "Other action",
          enabled: false,
          schedule: { type: "interval", everyMs: 60_000 },
          projectId: node.project.id,
          threadId: parent,
          workspaceStrategy: { type: "root" },
          modelSelection: selection,
          runtimeMode: "full-access",
          interactionMode: "default",
        });
        const replacement = yield* Effect.result(
          Context.get(rebuilt, ScheduledTasks.ScheduledTaskService).runNow(
            { id: task.id },
            { request: identity },
          ),
        );
        expect(replacement._tag).toBe("Failure");
        expect(
          yield* sql`SELECT message_id FROM orchestration_v2_projection_messages WHERE role='user'`,
        ).toHaveLength(3);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "preferences382: negotiated peer result retains its bounded variant and original grant checks",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("preferences382-source", "Source", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true, preferencesOwner: true },
        );
        const b = yield* makeNode(
          descriptorOf("preferences382-destination", "Destination", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true, preferencesOwner: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("preferences382-peer-parent");
        yield* seedParent(a, parent);
        yield* a.forwarding.call(
          caller(a, parent),
          ProjectToolkit.tools.t3_project_list,
          b.environmentId,
          {},
        );
        const input = {
          environmentId: b.environmentId,
          clientRequestId: "peer-preferences-confirmed",
          sourceControlWritingStyle: { customInstructions: "ISOLATED_PEER_PRIVATE_TEXT_382" },
        };
        // The approved direct full-access peer exception remains supported.
        const accepted = yield* nativeCall(a, parent, "t3_environment_preferences_update", input);
        expect(accepted.isError, json(accepted)).not.toBe(true);
        const decoded = yield* Schema.decodeUnknownEffect(
          EnvironmentToolkit.tools.t3_environment_preferences_update.successSchema,
        )(accepted.structuredContent);
        expect(decoded).toMatchObject({
          type: "confirmed-operation",
          applied: true,
          stage: "confirmed",
        });
        const writes = yield* Ref.get(b.settingsWrites);
        expect(writes).toBeGreaterThan(0);
        expect(
          (yield* nativeCall(a, parent, "t3_environment_preferences_update", input))
            .structuredContent,
        ).toEqual(decoded);
        const reordered = yield* nativeCall(
          a,
          parent,
          "t3_environment_preferences_update",
          input,
          new Set(["device", "preview", "orchestration"]),
        );
        expect(reordered.isError, json(reordered)).not.toBe(true);
        expect(reordered.structuredContent).toEqual(decoded);
        expect(yield* Ref.get(b.settingsWrites)).toBe(writes);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        for (const rows of [
          yield* sql`SELECT * FROM mcp_request_receipts`,
          yield* sql`SELECT * FROM peer_authority_records`,
          yield* sql`SELECT * FROM peer_control_frames`,
        ])
          expect(json(rows)).not.toContain(input.sourceControlWritingStyle.customInstructions);
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("preferences382-narrow"),
          threadId: parent,
          runtimeMode: "auto",
        });
        const narrowed = yield* nativeCall(a, parent, "t3_environment_preferences_update", input);
        expect(narrowed.isError).toBe(true);
        expect(yield* Ref.get(b.settingsWrites)).toBe(writes);
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("preferences382-restore"),
          threadId: parent,
          runtimeMode: "full-access",
        });
        // A destination restored without its request ledger cannot turn source
        // historical intent into a fresh operation under a replacement grant.
        yield* sql`DELETE FROM mcp_request_receipts`;
        yield* a.links.unlink(b.environmentId);
        yield* linkTo(a.links, b, "full-access");
        const replaced = yield* nativeCall(a, parent, "t3_environment_preferences_update", input);
        expect(replaced.isError).toBe(true);
        expect(yield* Ref.get(b.settingsWrites)).toBe(writes);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "preferences382: registered keyed receipts exclude private text and never overwrite later settings",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const node = yield* makeNode(
          descriptorOf("preferences382", "Preferences", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true, preferencesOwner: true },
        );
        const parent = ThreadId.make("preferences382-parent");
        yield* seedParent(node, parent);
        const firstPrivate = "ISOLATED_PRIVATE_INSTRUCTIONS_382_ALPHA_DO_NOT_PERSIST_IN_RECEIPTS";
        const secondPrivate = "ISOLATED_PRIVATE_INSTRUCTIONS_382_BETA_CHANGED_INPUT";
        const input = {
          clientRequestId: "preferences-confirmed",
          sourceControlWritingStyle: { customInstructions: firstPrivate },
        };
        const response = yield* nativeCall(
          node,
          parent,
          "t3_environment_preferences_update",
          input,
        );
        expect(response.isError, json(response)).not.toBe(true);
        const receipt = yield* Schema.decodeUnknownEffect(
          EnvironmentToolkit.tools.t3_environment_preferences_update.successSchema,
        )(response.structuredContent);
        expect(receipt).toMatchObject({
          type: "confirmed-operation",
          applied: true,
          requestId: "preferences-confirmed",
          stage: "confirmed",
        });
        expect(Object.keys(receipt).sort()).toEqual([
          "applied",
          "operationId",
          "requestId",
          "stage",
          "type",
        ]);
        const writes = yield* Ref.get(node.settingsWrites);
        expect(writes).toBeGreaterThan(0);
        const sql = Context.get(node.database, SqlClient.SqlClient);
        const persisted = yield* sql`SELECT * FROM mcp_request_receipts`;
        expect(json(persisted)).not.toContain(firstPrivate);
        expect(json(persisted)).toContain("confirmed-operation");
        // Lose the confirmed wire reply, renew the credential and reconstruct the SQL receipt owner.
        const reopened = yield* node.reopenDatabase;
        const rebuilt = yield* RequestReceipts.layer.pipe(
          Layer.provide(Layer.succeedContext(reopened)),
          Layer.fresh,
          Layer.build,
        );
        const rows = yield* sql<{
          id: string;
          generation: string;
          input_digest: string;
        }>`SELECT id,generation,input_digest FROM mcp_request_receipts WHERE state='completed'`;
        expect(rows).toHaveLength(1);
        const row = rows[0]!;
        const recovered = yield* Context.get(rebuilt, RequestReceipts.McpRequestReceipts).replay({
          id: row.id,
          generation: row.generation,
          inputDigest: row.input_digest,
          clientRequestId: input.clientRequestId,
        });
        expect(Option.getOrThrow(recovered)).toEqual(receipt);
        const replay = yield* nativeCall(node, parent, "t3_environment_preferences_update", input);
        expect(replay.structuredContent).toEqual(receipt);
        expect(yield* Ref.get(node.settingsWrites)).toBe(writes);
        const changed = yield* nativeCall(node, parent, "t3_environment_preferences_update", {
          ...input,
          sourceControlWritingStyle: { customInstructions: secondPrivate },
        });
        expect(changed.isError).toBe(true);
        expect(yield* Ref.get(node.settingsWrites)).toBe(writes);
        const later = yield* nativeCall(node, parent, "t3_environment_preferences_update", {
          sourceControlWritingStyle: { customInstructions: "ISOLATED_LATER_INDEPENDENT_EDIT_382" },
        });
        expect(later.isError, json(later)).not.toBe(true);
        expect(later.structuredContent).toMatchObject({
          sourceControlWritingStyle: { customInstructions: "ISOLATED_LATER_INDEPENDENT_EDIT_382" },
        });
        expect(later.structuredContent).not.toHaveProperty("type");
        const afterLater = yield* Ref.get(node.settingsWrites);
        expect(
          (yield* nativeCall(node, parent, "t3_environment_preferences_update", input))
            .structuredContent,
        ).toEqual(receipt);
        expect(yield* Ref.get(node.settingsWrites)).toBe(afterLater);
        const current = yield* nativeCall(node, parent, "t3_environment_read", {});
        expect(current.structuredContent).toMatchObject({
          preferences: {
            sourceControlWritingStyle: {
              customInstructions: "ISOLATED_LATER_INDEPENDENT_EDIT_382",
            },
          },
        });
        yield* Ref.set(node.loseSettingsConfirmation, true);
        const ambiguousInput = {
          clientRequestId: "preferences-write-confirmation-lost",
          sourceControlWritingStyle: { customInstructions: "ISOLATED_AMBIGUOUS_INSTRUCTIONS_382" },
        };
        const failed = yield* nativeCall(
          node,
          parent,
          "t3_environment_preferences_update",
          ambiguousInput,
        );
        expect(failed.isError).toBe(true);
        const ambiguousWrites = yield* Ref.get(node.settingsWrites);
        // Reopen the actual settings owner and observe that the write happened despite failed acknowledgement.
        const freshSettings = yield* node.rebuildSettings;
        expect(
          (yield* Context.get(freshSettings, Settings.ServerSettingsService).getSettings)
            .sourceControlWritingStyle.customInstructions,
        ).toBe(ambiguousInput.sourceControlWritingStyle.customInstructions);
        const unknown = yield* nativeCall(
          node,
          parent,
          "t3_environment_preferences_update",
          ambiguousInput,
        );
        expect(unknown.isError).toBe(true);
        expect(json(unknown)).toContain("outcome_unknown");
        expect(yield* Ref.get(node.settingsWrites)).toBe(ambiguousWrites);
        const allRows = json(yield* sql`SELECT * FROM mcp_request_receipts`);
        for (const privateText of [
          firstPrivate,
          secondPrivate,
          ambiguousInput.sourceControlWritingStyle.customInstructions,
          "ISOLATED_LATER_INDEPENDENT_EDIT_382",
        ])
          expect(allRows).not.toContain(privateText);
        expect(allRows).not.toContain("customInstructions");
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "attachments382: destination upload rivals consume once and an old generation cannot publish bytes",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("destination382-a", "A", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf("destination382-b", "B", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("destination382-parent");
        yield* seedParent(a, parent);
        const launched = yield* nativeCall(a, parent, "t3_thread_launch", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          title: "Destination",
          message: "Receive",
          modelSelection: selection,
          clientRequestId: "destination-owner",
        });
        expect(launched.isError, json(launched.content)).not.toBe(true);
        const child = (yield* decodeLaunchReceipt(launched.structuredContent)).threadId;
        const capabilities = new Set(["orchestration", "preview", "device"] as const);
        const credential = yield* a.registry.issue({
          threadId: parent,
          providerInstanceId: instanceId,
          capabilities,
        });
        const upload = {
          type: "file",
          name: "once.pdf",
          mimeType: "application/pdf",
          sizeBytes: 4,
        };
        const prepare = (key: string) =>
          nativeCall(
            a,
            parent,
            "t3_attachment_prepare_upload",
            {
              environmentId: b.environmentId,
              upload,
              digest: "9f64a747e1b97f131fabb6b447296c9b6f0201e79fb3c5356e6c77e89b6a806a",
              clientRequestId: key,
            },
            capabilities,
            credential,
          );
        const prepared = yield* prepare("destination-upload");
        expect(prepared.isError, json(prepared.content)).not.toBe(true);
        const decodeUpload = Schema.decodeUnknownEffect(
          Schema.Struct({
            attachmentId: Schema.String,
            relativeUrl: Schema.String,
            uploadOrigin: Schema.String,
          }),
        );
        const issued = yield* decodeUpload(prepared.structuredContent);
        expect(issued.uploadOrigin).toBe(b.url);
        const http = yield* HttpClient.HttpClient;
        const store = (url: string) =>
          http.execute(
            HttpClientRequest.post(url).pipe(
              HttpClientRequest.setBody(HttpBody.uint8Array(new Uint8Array([1, 2, 3, 4]))),
            ),
          );
        expect((yield* store(`${issued.uploadOrigin}${issued.relativeUrl}`)).status).toBe(204);
        const send = (key: string) =>
          nativeCall(
            a,
            parent,
            "t3_thread_send_attachments",
            {
              environmentId: b.environmentId,
              threadId: child,
              message: "One use",
              attachments: [{ ...upload, id: issued.attachmentId, environmentId: b.environmentId }],
              clientRequestId: key,
            },
            capabilities,
            credential,
          );
        const raced = yield* Effect.all(
          [send("destination-consumer-a"), send("destination-consumer-b")],
          { concurrency: "unbounded" },
        );
        expect(
          raced.filter((result) => result.isError !== true),
          json(raced),
        ).toHaveLength(1);
        expect(raced.filter((result) => result.isError === true)).toHaveLength(1);
        const records = yield* b.threads.getThreadRecords(child, ["messages"]);
        expect(records.messages.filter((message) => message.role === "user")).toHaveLength(2);
        expect(records.messages.flatMap((message) => message.attachments)).toHaveLength(1);
        const stale = yield* prepare("stale-upload");
        expect(stale.isError, json(stale.content)).not.toBe(true);
        const old = yield* decodeUpload(stale.structuredContent);
        yield* a.links.unlink(b.environmentId);
        expect((yield* store(`${old.uploadOrigin}${old.relativeUrl}`)).status).toBe(403);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        expect(
          (yield* sql<{
            digest: string | null;
          }>`SELECT digest FROM attachment_staging WHERE id=${old.attachmentId}`)[0]?.digest,
        ).toBe(null);
        const fs = yield* FileSystem.FileSystem;
        expect(
          (yield* fs.readDirectory(b.config.attachmentsDir)).some((path) =>
            path.startsWith(old.attachmentId),
          ),
        ).toBe(false);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "attachments382: registered media send preserves each MIME and digest and refuses rival source consumption",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(descriptorOf("media382-a", "A", { peerAuthority: 1 }), "disk", {
          productionKernel: true,
          attachmentRequests: true,
        });
        const b = yield* makeNode(descriptorOf("media382-b", "B", { peerAuthority: 1 }), "disk", {
          productionKernel: true,
          attachmentRequests: true,
        });
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("media382-parent");
        yield* seedParent(a, parent);
        const launched = yield* nativeCall(a, parent, "t3_thread_launch", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          title: "Media",
          message: "Receive files",
          modelSelection: selection,
          clientRequestId: "media-owner",
        });
        expect(launched.isError, json(launched.content)).not.toBe(true);
        const child = (yield* decodeLaunchReceipt(launched.structuredContent)).threadId;
        const fixtures = [
          {
            type: "image",
            name: "pixel.png",
            mimeType: "image/png",
            bytes: new Uint8Array(
              Buffer.from(
                "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScLbtAAAAABJRU5ErkJggg==",
                "base64",
              ),
            ),
          },
          {
            type: "file",
            name: "clip.mp4",
            mimeType: "video/mp4",
            bytes: new Uint8Array([
              0, 0, 0, 20, 102, 116, 121, 112, 105, 115, 111, 109, 0, 0, 0, 0, 105, 115, 111, 109,
            ]),
          },
          {
            type: "file",
            name: "sound.wav",
            mimeType: "audio/wav",
            bytes: new TextEncoder().encode("RIFF0000WAVEfmt "),
          },
          {
            type: "file",
            name: "notes.txt",
            mimeType: "text/plain",
            bytes: new TextEncoder().encode("destination bytes\n"),
          },
        ];
        const http = yield* HttpClient.HttpClient;
        const attachments = yield* Effect.forEach(fixtures, (fixture, index) =>
          Effect.gen(function* () {
            const upload = {
              type: fixture.type,
              name: fixture.name,
              mimeType: fixture.mimeType,
              sizeBytes: fixture.bytes.byteLength,
            };
            const prepared = yield* nativeCall(a, parent, "t3_attachment_prepare_upload", {
              upload,
              clientRequestId: `media-upload-${index}`,
            });
            expect(prepared.isError, json(prepared.content)).not.toBe(true);
            const issued = yield* Schema.decodeUnknownEffect(
              Schema.Struct({ attachmentId: Schema.String, relativeUrl: Schema.String }),
            )(prepared.structuredContent);
            expect(
              (yield* http.execute(
                HttpClientRequest.post(`${a.url}${issued.relativeUrl}`).pipe(
                  HttpClientRequest.setBody(HttpBody.uint8Array(fixture.bytes)),
                ),
              )).status,
            ).toBe(204);
            return { ...upload, id: issued.attachmentId };
          }),
        );
        const capabilities = new Set(["orchestration", "preview", "device"] as const);
        const credential = yield* a.registry.issue({
          threadId: parent,
          providerInstanceId: instanceId,
          capabilities,
        });
        const send = (clientRequestId: string) =>
          nativeCall(
            a,
            parent,
            "t3_thread_send_attachments",
            {
              environmentId: b.environmentId,
              threadId: child,
              attachments,
              message: "Exact media",
              clientRequestId,
            },
            capabilities,
            credential,
          );
        const raced = yield* Effect.all([send("source-consumer-a"), send("source-consumer-b")], {
          concurrency: "unbounded",
        });
        expect(
          raced.filter((result) => result.isError !== true),
          json(raced),
        ).toHaveLength(1);
        expect(raced.filter((result) => result.isError === true)).toHaveLength(1);
        const winningKey = raced[0]!.isError === true ? "source-consumer-b" : "source-consumer-a";
        const replayed = yield* send(winningKey);
        expect(replayed.isError, json(replayed.content)).not.toBe(true);
        const records = yield* b.threads.getThreadRecords(child, ["messages", "runs"]);
        expect(records.messages.filter((message) => message.role === "user")).toHaveLength(2);
        const delivered = records.messages.flatMap((message) => message.attachments);
        expect(delivered).toHaveLength(fixtures.length);
        const fs = yield* FileSystem.FileSystem;
        for (const fixture of fixtures) {
          const attachment = delivered.find((item) => item.name === fixture.name)!;
          expect(attachment.mimeType).toBe(fixture.mimeType);
          expect(attachment.sizeBytes).toBe(fixture.bytes.byteLength);
          const path = resolveAttachmentPath({
            attachmentsDir: b.config.attachmentsDir,
            attachment,
          });
          expect(Array.from(yield* fs.readFile(path!))).toEqual(Array.from(fixture.bytes));
        }
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "requests382: mixed versions negotiate before effects and imported unowned launches cannot duplicate",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(descriptorOf("import382-a", "A", { peerAuthority: 1 }), "disk", {
          productionKernel: true,
          attachmentRequests: true,
        });
        const b = yield* makeNode(descriptorOf("import382-b", "B", { peerAuthority: 1 }), "disk", {
          productionKernel: true,
          attachmentRequests: true,
        });
        const old = yield* makeNode(
          descriptorOf("import382-old", "Old", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true },
        );
        const parent = ThreadId.make("import382-parent");
        yield* seedParent(a, parent);
        yield* linkTo(a.links, b, "full-access");
        yield* linkTo(a.links, old, "full-access");
        const unavailable = yield* nativeCall(a, parent, "t3_thread_launch", {
          environmentId: old.environmentId,
          projectId: old.project.id,
          title: "Unsupported",
          message: "Do not launch",
          modelSelection: selection,
          clientRequestId: "needs-protocol",
        });
        expect(unavailable.isError).toBe(true);
        const oldSql = Context.get(old.database, SqlClient.SqlClient);
        expect(
          yield* oldSql`SELECT thread_id FROM orchestration_v2_projection_threads WHERE project_id=${old.project.id}`,
        ).toHaveLength(0);
        const http = yield* HttpClient.HttpClient;
        expect((yield* http.get(`${b.url}/mcp/peer-control/requests`)).status).toBe(401);
        // The old source path has no new receipt owner. Its original accepted
        // destination record remains verifiable as an effect, but has no input ledger.
        const legacy = yield* PeerForwarding.PeerForwarding.pipe(
          Effect.provide(
            PeerForwarding.layer.pipe(
              Layer.provide(
                Layer.succeedContext(Context.omit(RequestReceipts.McpRequestReceipts)(a.services)),
              ),
              Layer.provide(Layer.succeedContext(a.runtime)),
            ),
          ),
        );
        const input = {
          projectId: b.project.id,
          title: "Imported",
          message: "Exactly one",
          modelSelection: selection,
          clientRequestId: "legacy-launch",
        };
        const accepted = yield* legacy.call(
          caller(a, parent),
          ProjectToolkit.tools.t3_thread_launch,
          b.environmentId,
          input,
        );
        const receipt = yield* decodeLaunchReceipt(accepted);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        yield* sql`DELETE FROM mcp_request_receipts`;
        const upgraded = yield* Effect.result(
          a.forwarding.call(
            caller(a, parent),
            ProjectToolkit.tools.t3_thread_launch,
            b.environmentId,
            input,
          ),
        );
        expect(upgraded._tag).toBe("Failure");
        if (upgraded._tag === "Failure")
          expect(upgraded.failure.message).toContain("legacy operation");
        const records = yield* b.threads.getThreadRecords(receipt.threadId, ["messages", "runs"]);
        expect(records.messages.filter((m) => m.role === "user")).toHaveLength(1);
        expect(
          yield* sql`SELECT thread_id FROM orchestration_v2_projection_threads WHERE project_id=${b.project.id}`,
        ).toHaveLength(1);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "requests382: registered browser ambiguity persists before dispatch and cannot repeat an action",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(descriptorOf("external-a", "A", { peerAuthority: 1 }), "disk", {
          productionKernel: true,
          attachmentRequests: true,
        });
        const b = yield* makeNode(descriptorOf("external-b", "B", { peerAuthority: 1 }), "disk", {
          productionKernel: true,
          attachmentRequests: true,
        });
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("external-parent");
        yield* seedParent(a, parent);
        const launched = yield* nativeCall(a, parent, "t3_thread_launch", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          title: "Browser owner",
          message: "Wait",
          modelSelection: selection,
          clientRequestId: "browser-owner",
        });
        expect(launched.isError, json(launched.content)).not.toBe(true);
        const child = (yield* decodeLaunchReceipt(launched.structuredContent)).threadId;
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, preview: true });
        yield* activate(b, child);
        const connected = yield* Deferred.make<void>();
        let actions = 0;
        const sql = Context.get(b.database, SqlClient.SqlClient);
        yield* Stream.runForEach(
          yield* b.browser.connect({
            clientId: "deterministic-browser",
            environmentId: b.environmentId,
          }),
          (event) =>
            event.type === "connected"
              ? Deferred.succeed(connected, undefined)
              : Effect.gen(function* () {
                  const request = event.request;
                  if (request.operation === "click") {
                    const started = yield* sql<{
                      state: string;
                    }>`SELECT state FROM mcp_request_receipts WHERE state='started'`;
                    expect(started).toHaveLength(1);
                    actions++;
                    yield* b.browser.respond({
                      clientId: "deterministic-browser",
                      connectionId: event.connectionId,
                      requestId: request.requestId,
                      ok: false,
                      error: {
                        _tag: "PreviewAutomationExecutionError",
                        message: "External action happened; its confirmation was lost.",
                      },
                    });
                  } else
                    yield* b.browser.respond({
                      clientId: "deterministic-browser",
                      connectionId: event.connectionId,
                      requestId: request.requestId,
                      ok: true,
                      result: {
                        available: true,
                        visible: true,
                        tabId: null,
                        url: null,
                        title: null,
                        loading: false,
                      },
                    });
                }),
        ).pipe(Effect.forkScoped);
        yield* Deferred.await(connected);
        const input = {
          environmentId: b.environmentId,
          contextThreadId: child,
          locator: "role=button[name='Send']",
          clientRequestId: "deliberate-click",
        };
        for (let attempt = 0; attempt < 2; attempt++) {
          const response = yield* nativeCall(a, parent, "preview_click", input);
          expect(response.isError).toBe(true);
          const failure = response.content.map((item) => item.text ?? "").join("");
          expect(failure, json(response)).toContain('"code":"outcome_unknown"');
          expect(failure).toContain('"requestId":"deliberate-click"');
          expect(failure).toContain('"lastConfirmedStage":"dispatch_started"');
        }
        expect(actions).toBe(1);
        const inspected = yield* nativeCall(a, parent, "preview_status", {
          environmentId: b.environmentId,
          contextThreadId: child,
        });
        expect(inspected.isError, json(inspected.content)).not.toBe(true);
        expect(actions).toBe(1);
        const changed = yield* nativeCall(a, parent, "preview_click", {
          ...input,
          locator: "role=button[name='Different']",
        });
        expect(changed.isError).toBe(true);
        expect(actions).toBe(1);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "attachments382: registered native and peer launches preserve bytes and one accepted launch after reply loss",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(descriptorOf("attachment-a", "A", { peerAuthority: 1 }), "disk", {
          productionKernel: true,
          attachmentRequests: true,
        });
        const b = yield* makeNode(descriptorOf("attachment-b", "B", { peerAuthority: 1 }), "disk", {
          productionKernel: true,
          attachmentRequests: true,
        });
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("attachment-parent");
        yield* seedParent(a, parent);
        const metadata = {
          type: "file",
          name: "sample.pdf",
          mimeType: "application/pdf",
          sizeBytes: 4,
        };
        const prepared = yield* nativeCall(a, parent, "t3_attachment_prepare_upload", {
          upload: metadata,
          clientRequestId: "source-upload",
        });
        expect(prepared.isError, json(prepared.content)).not.toBe(true);
        const upload = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ attachmentId: Schema.String, relativeUrl: Schema.String }),
        )(prepared.structuredContent);
        const http = yield* HttpClient.HttpClient;
        const stored = yield* http.execute(
          HttpClientRequest.post(`${a.url}${upload.relativeUrl}`).pipe(
            HttpClientRequest.setBody(HttpBody.uint8Array(new Uint8Array([1, 2, 3, 4]))),
          ),
        );
        expect(stored.status, yield* stored.text).toBe(204);
        const input = {
          environmentId: b.environmentId,
          projectId: b.project.id,
          title: "Attachment fixture",
          message: "Read this file",
          attachments: [{ ...metadata, id: upload.attachmentId }],
          modelSelection: selection,
          clientRequestId: "one-peer-launch",
        };
        yield* Ref.set(a.dropLaunchResponses, 1);
        const lost = yield* nativeCall(a, parent, "t3_thread_launch", input);
        expect(lost.isError).toBe(true);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const before = yield* sql<{
          thread_id: string;
        }>`SELECT thread_id FROM orchestration_v2_projection_threads WHERE project_id=${b.project.id}`;
        expect(before, json(lost)).toHaveLength(1);
        const acceptedId = ThreadId.make(before[0]!.thread_id);
        const first = yield* b.threads.getThreadRecords(acceptedId, ["messages", "runs"]);
        expect(first.messages.filter((message) => message.role === "user")).toHaveLength(1);
        const delivered = first.messages.flatMap((message) => message.attachments);
        expect(delivered).toHaveLength(1);
        expect(delivered[0]).toMatchObject({
          mimeType: "application/pdf",
          sizeBytes: 4,
          name: "sample.pdf",
        });
        const path = resolveAttachmentPath({
          attachmentsDir: b.config.attachmentsDir,
          attachment: delivered[0]!,
        });
        const fs = yield* FileSystem.FileSystem;
        expect(Array.from(yield* fs.readFile(path!))).toEqual([1, 2, 3, 4]);
        // Simulate pending-upload expiry after the actual native commit and
        // lost reply. Native acceptance, not ephemeral pending bytes, owns replay.
        const sourcePending = resolveAttachmentPath({
          attachmentsDir: a.config.attachmentsDir,
          attachment: { ...metadata, type: "file", id: upload.attachmentId },
        });
        yield* fs.remove(sourcePending!);
        const pending = yield* sql<{ id: string }>`SELECT id FROM attachment_staging`;
        for (const row of pending) {
          const pendingPath = resolveAttachmentPathById({
            attachmentsDir: b.config.attachmentsDir,
            attachmentId: row.id,
          });
          if (pendingPath !== null) yield* fs.remove(pendingPath);
        }
        yield* a.stopConnections;
        const reconnected = yield* a.reconnect();
        yield* reconnected.peers.start;
        const retried = yield* nativeCall(a, parent, "t3_thread_launch", input);
        expect(retried.isError, json(retried.content)).not.toBe(true);
        const receipt = yield* decodeLaunchReceipt(retried.structuredContent);
        expect(receipt.threadId).toBe(acceptedId);
        expect(
          yield* sql`SELECT thread_id FROM orchestration_v2_projection_threads WHERE project_id=${b.project.id}`,
        ).toHaveLength(1);
        const after = yield* b.threads.getThreadRecords(acceptedId, ["messages", "runs"]);
        expect(after.messages.map((m) => [m.id, m.attachments])).toEqual(
          first.messages.map((m) => [m.id, m.attachments]),
        );
        expect(after.runs.map((r) => r.id)).toEqual(first.runs.map((r) => r.id));
        // Later independent local edits belong to the accepted file. A replay
        // must consult the native receipt instead of recopying the upload.
        yield* fs.writeFile(path!, new Uint8Array([9, 8, 7, 6]));
        const again = yield* nativeCall(a, parent, "t3_thread_launch", input);
        expect(again.isError, json(again.content)).not.toBe(true);
        expect(Array.from(yield* fs.readFile(path!))).toEqual([9, 8, 7, 6]);
        const reopened = yield* a.reopenDatabase;
        const rebuilt = yield* RequestReceipts.McpRequestReceipts.pipe(
          Effect.provide(RequestReceipts.layer.pipe(Layer.provide(Layer.succeedContext(reopened)))),
        );
        yield* rebuilt.identify(
          caller(a, parent),
          "t3_thread_launch",
          input,
          input.clientRequestId,
        );
        const changed = yield* nativeCall(a, parent, "t3_thread_launch", {
          ...input,
          message: "Different intent",
        });
        expect(changed.isError).toBe(true);
        expect((yield* b.threads.getThreadRecords(acceptedId, ["messages"])).messages).toEqual(
          after.messages,
        );
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

/** Production claim/receipt worker, with provider I/O replaced only at its external boundary. */
const drainStops = (node: Node) =>
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

const restoreAuthority = (node: Node) =>
  Effect.gen(function* () {
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

const activate = (node: Node, threadId: ThreadId) =>
  Effect.gen(function* () {
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
const seedParent = (node: Node, threadId: ThreadId, linkOrigin?: OrchestrationV2LinkOrigin) =>
  Effect.gen(function* () {
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
const caller = (node: Node, threadId: ThreadId): McpThreadInvocationScope => ({
  environmentId: node.environmentId,
  requestNamespace: `fixture:${threadId}`,
  issuedAt: 0,
  thread: { threadId, providerSessionId: `fixture:${threadId}`, providerInstanceId: instanceId },
  client: undefined,
  capabilities: new Set(["orchestration"]),
});

const sendResult = Schema.decodeUnknownEffect(
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
const json = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeTaskReceipt = Schema.decodeUnknownEffect(Schema.Struct({ taskId: NodeId }));
const decodeCreatedThreads = Schema.decodeUnknownEffect(
  OrchestratorToolkit.tools.create_threads.successSchema,
);
const decodeLaunchReceipt = Schema.decodeUnknownEffect(
  ProjectToolkit.tools.t3_thread_launch.successSchema,
);
const nativeRpcCall = (
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
) =>
  Effect.gen(function* () {
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
  }).pipe(Effect.provide(FetchHttpClient.layer));
// Positive callers retain their original success-result decoder and fail on RPC errors.
const nativeCall = (...input: Parameters<typeof nativeRpcCall>) =>
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
// This observer accepts only the actual InvalidParams wire variant. It neither
// invents a tool result nor maps another transport/parser error into a refusal.
const nativeInvalidParams = (...input: Parameters<typeof nativeRpcCall>) =>
  nativeRpcCall(...input).pipe(Effect.flatMap(decodeNativeInvalidParams));
const decodeSend = Schema.decodeUnknownEffect(
  OrchestratorToolkit.tools.t3_thread_send.successSchema,
);
const sendNotice = (
  node: Node,
  threadId: ThreadId,
  environmentId: Node["environmentId"],
  parent: ThreadId,
  message: string,
  key: string,
) =>
  Effect.gen(function* () {
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

const awaitNotice = (node: Node, threadId: ThreadId, text: string) =>
  node.orchestrator
    .streamStoredEventsFrom({ threadId, afterSequence: 0, eventType: "message.updated" })
    .pipe(
      Stream.filter(
        (stored) => stored.event.type === "message.updated" && stored.event.payload.text === text,
      ),
      Stream.runHead,
      Effect.flatMap(Effect.fromOption),
    );
const attachedChild = (node: Node, parentThreadId: ThreadId, taskId: NodeId) =>
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
const delegate = (source: Node, destination: Node, parentId: ThreadId, key: string) =>
  Effect.gen(function* () {
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

it.live(
  "a real delegated child returns a deduplicated queued notice with a parent-local task identity",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* makeNode(descriptorOf("workflow-b", "B", { peerAuthority: 1 }));
        const a = yield* makeNode(descriptorOf("workflow-a", "A", { peerAuthority: 1 }));
        yield* linkTo(a.links, b, "full-access");
        const parentId = ThreadId.make("workflow-parent");
        yield* seedParent(a, parentId);
        const aScope = caller(a, parentId);
        const { taskId } = yield* a.remote.delegate(aScope, {
          task: "Return evidence",
          target: {
            environmentId: descriptorOf("workflow-b", "B").environmentId,
            projectId: b.project.id,
            providerInstanceId: instanceId,
            model: "gpt-5.4",
          },
          mode: "async",
          clientRequestId: "return-workflow",
        });
        const attached = yield* a.orchestrator
          .streamStoredEventsFrom({
            threadId: parentId,
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
          );
        if (
          attached.event.type !== "subagent.updated" ||
          attached.event.payload.remoteChild?.threadId === undefined
        )
          return yield* Effect.die("Missing remote child");
        const childId = attached.event.payload.remoteChild.threadId;
        yield* activate(b, childId);
        const sent = yield* sendNotice(
          b,
          childId,
          aScope.environmentId,
          parentId,
          "Evidence from B",
          "explicit-notice",
        );
        expect(["queued", "pending"]).toContain(sent.delivery);
        yield* awaitNotice(a, parentId, "Evidence from B");
        yield* sendNotice(
          b,
          childId,
          aScope.environmentId,
          parentId,
          "Evidence from B",
          "explicit-notice",
        );
        const records = yield* a.threads.getThreadRecords(parentId, [
          "messages",
          "runs",
          "subagents",
        ]);
        const notices = records.messages.filter((message) => message.text === "Evidence from B");
        expect(notices).toHaveLength(1);
        expect(notices[0]?.notification?.source).toEqual({
          kind: "delegated_task",
          taskIds: [taskId],
        });
        expect(notices[0]?.senderThreadId).toBeUndefined();
        expect(records.subagents.find((task) => task.id === taskId)?.childThreadId).toBeNull();
        expect(records.runs.find((run) => run.userMessageId === notices[0]?.id)?.status).toBe(
          "queued",
        );
        expect(yield* b.links.list).toEqual([]);
        for (const mode of ["steer", "restart"]) {
          const refused = yield* nativeCall(b, childId, "t3_thread_send", {
            environmentId: a.environmentId,
            threadId: parentId,
            message: "Do not interrupt",
            mode,
          });
          expect(refused.isError).toBe(true);
          expect(json(refused.content)).toContain("only queue a notice");
        }
        const active = (yield* a.threads.getThreadShell(parentId))?.activeRunId;
        if (active == null) return yield* Effect.die("Missing parent run");
        yield* a.threads.dispatch({
          type: "run.interrupt",
          commandId: CommandId.make("stop-parent-run"),
          threadId: parentId,
          runId: active,
          holdQueue: true,
        });
        yield* nativeCall(b, childId, "t3_thread_send", {
          environmentId: a.environmentId,
          threadId: parentId,
          message: "Notice to stopped parent",
          mode: "auto",
          clientRequestId: "stopped-notice",
        });
        yield* Deferred.await(a.noticeFailure);
        const stopped = yield* a.threads.getThreadRecords(parentId, ["messages", "runs"]);
        expect(
          stopped.messages.some((message) => message.text === "Notice to stopped parent"),
        ).toBe(false);
        expect((yield* a.threads.getThreadShell(parentId))?.activeRunId).toBeNull();
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("delegates A to B to C and returns notices using each parent's own task identity", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const c = yield* makeNode(descriptorOf("onward-c", "C", { peerAuthority: 1 }));
      const b = yield* makeNode(descriptorOf("onward-b", "B", { peerAuthority: 1 }));
      const a = yield* makeNode(descriptorOf("onward-a", "A", { peerAuthority: 1 }));
      yield* linkTo(a.links, b, "full-access");
      yield* linkTo(b.links, c, "full-access");
      const parentId = ThreadId.make("onward-parent");
      yield* seedParent(a, parentId);
      yield* a.forwarding.call(
        caller(a, parentId),
        ProjectToolkit.tools.t3_project_list,
        b.environmentId,
        {},
      );
      const grant = (yield* b.authority.grants)[0];
      if (!grant) return yield* Effect.die("Missing B consent");
      yield* b.authority.setGrant({ ...grant, onward: true });
      const first = yield* delegate(a, b, parentId, "a-b");
      yield* activate(b, first.childId);
      const second = yield* delegate(b, c, first.childId, "b-c");
      yield* activate(c, second.childId);
      yield* sendNotice(
        c,
        second.childId,
        b.environmentId,
        first.childId,
        "C evidence",
        "c-result",
      );
      yield* awaitNotice(b, first.childId, "C evidence");
      yield* sendNotice(
        b,
        first.childId,
        a.environmentId,
        parentId,
        "B evidence including C",
        "b-result",
      );
      yield* awaitNotice(a, parentId, "B evidence including C");
      const aMessages = (yield* a.threads.getThreadRecords(parentId, ["messages"])).messages;
      const bMessages = (yield* b.threads.getThreadRecords(first.childId, ["messages"])).messages;
      expect(
        aMessages.find((message) => message.text === "B evidence including C")?.notification
          ?.source,
      ).toEqual({ kind: "delegated_task", taskIds: [first.taskId] });
      expect(
        bMessages.find((message) => message.text === "C evidence")?.notification?.source,
      ).toEqual({ kind: "delegated_task", taskIds: [second.taskId] });
      const unrelated = yield* c.forwarding
        .call(
          caller(c, second.childId),
          OrchestratorToolkit.tools.t3_thread_send,
          a.environmentId,
          { threadId: parentId, message: "Cannot skip my parent", clientRequestId: "forbidden" },
        )
        .pipe(Effect.flip);
      expect(unrelated.code).toBe("capability_denied");
      expect(aMessages.some((message) => message.text === "Cannot skip my parent")).toBe(false);
      // A later new job on A is ancestry, not a repeated hop in this operation's route.
      yield* linkTo(b.links, a, "full-access");
      const back = yield* delegate(b, a, first.childId, "later-b-a-job");
      expect((yield* a.threads.getThreadShell(back.childId))?.projectId).toBe(a.project.id);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("persists an offline return and resumes it from stored links without a new tool call", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* makeNode(descriptorOf("resume-b", "B", { peerAuthority: 1 }));
      const a = yield* makeNode(descriptorOf("resume-a", "A", { peerAuthority: 1 }));
      yield* linkTo(a.links, b, "full-access");
      const parentId = ThreadId.make("resume-parent");
      yield* seedParent(a, parentId);
      const task = yield* delegate(a, b, parentId, "resume-job");
      yield* activate(b, task.childId);
      yield* a.stopConnections;
      const pending = yield* sendNotice(
        b,
        task.childId,
        a.environmentId,
        parentId,
        "Queued while A is offline",
        "offline-notice",
      );
      expect(pending.delivery).toBe("pending");
      expect(
        (yield* a.threads.getThreadRecords(parentId, ["messages"])).messages.some(
          (message) => message.text === "Queued while A is offline",
        ),
      ).toBe(false);
      const reconnected = yield* a.reconnect();
      yield* reconnected.peers.start;
      yield* a.orchestrator
        .streamStoredEventsFrom({
          threadId: parentId,
          afterSequence: 0,
          eventType: "message.updated",
        })
        .pipe(
          Stream.filter(
            (stored) =>
              stored.event.type === "message.updated" &&
              stored.event.payload.text === "Queued while A is offline",
          ),
          Stream.runHead,
          Effect.flatMap(Effect.fromOption),
        );
      const acknowledged = yield* sendNotice(
        b,
        task.childId,
        a.environmentId,
        parentId,
        "Queued while A is offline",
        "offline-notice",
      );
      expect(acknowledged.delivery).toBe("queued");
      expect(acknowledged.messageId).toBe(pending.messageId);
      expect(
        (yield* a.threads.getThreadRecords(parentId, ["messages"])).messages.filter(
          (message) => message.text === "Queued while A is offline",
        ),
      ).toHaveLength(1);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "revalidates committed provider effects at claim and denies revoked authority before execution",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* makeNode(descriptorOf("effect-b", "B", { peerAuthority: 1 }));
        const a = yield* makeNode(descriptorOf("effect-a", "A", { peerAuthority: 1 }));
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("effect-parent");
        yield* seedParent(a, parent);
        const executed = yield* Ref.make<ReadonlyArray<EffectOutbox.OrchestrationEffectV2>>([]);
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
          Effect.provide(
            EffectWorker.layerWithOptions({ maxAttempts: 1 }).pipe(
              Layer.provide(
                Layer.succeed(EffectWorker.OrchestrationEffectExecutorV2, {
                  execute: (effect) => Ref.update(executed, (effects) => [...effects, effect]),
                }),
              ),
              Layer.provide(Layer.succeed(PeerAuthority.PeerAuthority, b.authority)),
              Layer.provide(Layer.succeedContext(b.runtime)),
            ),
          ),
        );
        const allowed = yield* delegate(a, b, parent, "allowed-effect");
        yield* worker.drain();
        expect(
          (yield* Ref.get(executed)).some(
            (effect) =>
              effect.threadId === allowed.childId && effect.request.type === "provider-turn.start",
          ),
        ).toBe(true);
        yield* Ref.set(executed, []);
        const revoked = yield* delegate(a, b, parent, "revoked-effect");
        const checked = yield* b.authority.checkThread(revoked.childId);
        expect(checked?.ceiling.runtimeMode).toBe("full-access");
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const mapping = yield* sql<{
          effect_id: string;
        }>`SELECT p.effect_id FROM peer_effect_authority p JOIN orchestration_v2_effect_outbox e ON e.effect_id = p.effect_id WHERE e.thread_id = ${revoked.childId} AND e.effect_type = 'provider-turn.start'`;
        expect(mapping).toHaveLength(1);
        yield* a.links.unlink(b.environmentId);
        yield* worker.drain();
        expect(
          (yield* Ref.get(executed)).some(
            (effect) =>
              effect.threadId === revoked.childId && effect.request.type === "provider-turn.start",
          ),
        ).toBe(false);
        const effect = yield* Context.get(b.runtime, EffectOutbox.EffectOutboxV2).get(
          mapping[0]?.effect_id ?? "missing-effect",
        );
        expect(Option.isSome(effect) && effect.value.status).toBe("failed");
        expect(Option.isSome(effect) && effect.value.lastError).toContain("no longer authorized");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "retains frozen ceilings through a reopened disk database and rotated credentials and rejects a replacement generation",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* makeNode(descriptorOf("frozen-b", "B", { peerAuthority: 1 }), "disk");
        const a = yield* makeNode(descriptorOf("frozen-a", "A", { peerAuthority: 1 }));
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("frozen-parent");
        yield* seedParent(a, parent);
        const task = yield* delegate(a, b, parent, "frozen-job");
        const original = yield* b.authority.forThread(task.childId);
        const grant = (yield* b.authority.grants)[0];
        if (!grant || !original) return yield* Effect.die("Missing persisted authority");
        yield* b.authority.setGrant({
          ...grant,
          resourceScope: "environment-resources",
          onward: true,
          preview: true,
          device: true,
        });
        // Import the first candidate's control schema, including real pair receipts.
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const receipts =
          yield* sql`SELECT rowid AS cursor, id, session_id, frame_json, answer_json FROM peer_control_frames ORDER BY rowid`;
        expect(receipts.length).toBeGreaterThan(0);
        yield* sql.withTransaction(
          Effect.gen(function* () {
            yield* sql`ALTER TABLE peer_control_frames RENAME TO peer_control_frames_fixture`;
            yield* sql`CREATE TABLE peer_control_frames (id TEXT PRIMARY KEY, session_id TEXT NOT NULL, frame_json TEXT NOT NULL, answer_json TEXT)`;
            yield* sql`INSERT INTO peer_control_frames (rowid, id, session_id, frame_json, answer_json) SELECT rowid, id, session_id, frame_json, answer_json FROM peer_control_frames_fixture`;
            yield* sql`DROP TABLE peer_control_frames_fixture`;
          }),
        );
        const restored = yield* restoreAuthority(b);
        expect(yield* restored.forThread(task.childId)).toEqual(original);
        expect(
          yield* sql`SELECT rowid AS cursor, id, session_id, frame_json, answer_json FROM peer_control_frames ORDER BY rowid`,
        ).toEqual(receipts);
        // The existing stream already observed the old cursor. It must see new work.
        yield* a.forwarding.call(
          caller(a, parent),
          ProjectToolkit.tools.t3_project_list,
          b.environmentId,
          {},
        );
        const registry = yield* McpSessionRegistry.__testing.make().pipe(
          Effect.provideService(PeerAuthority.PeerAuthority, restored),
          Effect.provide(Layer.succeedContext(b.serverContext)),
          Effect.provideService(Environment.ServerEnvironment, {
            getEnvironmentId: Effect.succeed(b.environmentId),
            getDescriptor: Ref.get(b.descriptor),
          }),
        );
        for (let i = 0; i < 2; i++) {
          const credential = yield* registry.issue({
            threadId: task.childId,
            providerInstanceId: instanceId,
            browserToolsAvailable: true,
            capabilities: new Set(["orchestration", "preview", "device"]),
          });
          expect([...(credential.config.capabilities ?? [])]).toEqual(["orchestration"]);
          yield* registry.revokeAll;
        }
        yield* linkTo(a.links, b, "full-access");
        const denied = yield* restored.checkThread(task.childId).pipe(Effect.flip);
        expect(denied.code).toBe("capability_denied");
        const replay = yield* a.remote
          .delegate(caller(a, parent), {
            task: "Return evidence",
            target: {
              environmentId: b.environmentId,
              projectId: b.project.id,
              providerInstanceId: instanceId,
              model: "gpt-5.4",
            },
            mode: "async",
            clientRequestId: "frozen-job",
          })
          .pipe(Effect.flip);
        expect(replay.code).toBe("capability_denied");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "migrates pre-protocol linked work to a persisted created-work ceiling without new capabilities",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* makeNode(descriptorOf("legacy-b", "B", { peerAuthority: 1 }));
        const a = yield* makeNode(descriptorOf("legacy-a", "A", { peerAuthority: 1 }));
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("legacy-source");
        yield* seedParent(a, parent);
        yield* a.forwarding.call(
          caller(a, parent),
          ProjectToolkit.tools.t3_project_list,
          b.environmentId,
          {},
        );
        const grant = (yield* b.authority.grants)[0];
        if (!grant) return yield* Effect.die("Missing authenticated link");
        const threadId = ThreadId.make("legacy-destination-work");
        yield* b.threads.dispatch({
          type: "thread.create",
          commandId: CommandId.make("legacy-create"),
          threadId,
          projectId: b.project.id,
          title: "Legacy child",
          modelSelection: selection,
          runtimeMode: "auto",
          interactionMode: "plan",
          branch: null,
          worktreePath: null,
          createdBy: "agent",
          creationSource: "mcp",
          linkOrigin: { sessionId: grant.sessionId, label: "A" },
        });
        const ceiling = yield* b.authority.legacyCeiling(threadId);
        expect(ceiling).toEqual({
          runtimeMode: "auto",
          interactionMode: "plan",
          resourceScope: "created-work",
          onward: false,
          capabilities: ["orchestration", "worktree", "pull-requests"],
        });
        yield* b.authority.setGrant({
          ...grant,
          resourceScope: "environment-resources",
          onward: true,
          preview: true,
          device: true,
        });
        yield* b.threads.dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("legacy-mode"),
          threadId,
          runtimeMode: "full-access",
        });
        expect(yield* b.authority.legacyCeiling(threadId)).toEqual(ceiling);
        const issued = yield* b.registry.issue({
          threadId,
          providerInstanceId: instanceId,
          browserToolsAvailable: true,
          capabilities: new Set(["preview", "device"]),
        });
        expect(issued.config.capabilities?.has("preview")).toBe(false);
        expect(issued.config.capabilities?.has("device")).toBe(false);
        yield* b.threads.dispatch({
          type: "thread.interaction-mode.set",
          commandId: CommandId.make("legacy-interaction-mode"),
          threadId,
          interactionMode: "default",
        });
        yield* b.threads.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("legacy-private-output-run"),
          threadId,
          messageId: MessageId.make("legacy-private-output-message"),
          text: "Fixture only",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        yield* activate(b, threadId);
        const fixtures = [];
        for (const modes of [
          { runtimeMode: "full-access", interactionMode: "plan" },
          { runtimeMode: "auto", interactionMode: "default" },
          { runtimeMode: "auto", interactionMode: "plan" },
        ] as const) {
          const fixture = yield* b.scheduled.upsert({
            title: `Legacy ${modes.runtimeMode}/${modes.interactionMode}`,
            prompt: "Never executed",
            enabled: false,
            schedule: { type: "webhook" },
            projectId: b.project.id,
            threadId: null,
            workspaceStrategy: { type: "root" },
            modelSelection: selection,
            ...modes,
            createdBy: "user",
            creationSource: "web",
            linkOrigin: { sessionId: grant.sessionId, label: "A" },
          });
          expect(fixture.task.webhook?.url).toBeTruthy();
          fixtures.push(fixture.task.id);
        }
        const listed = yield* nativeCall(b, threadId, "list_scheduled_tasks", {});
        expect(listed.isError, json(listed.content)).not.toBe(true);
        const summaries = yield* decodeScheduledTaskList(listed.structuredContent);
        expect(
          fixtures.map(
            (id) =>
              summaries.tasks.find((task) => task.scheduledTaskId === id)?.webhookUrl !== undefined,
          ),
        ).toEqual([false, false, true]);
        expect(summaries.tasks.map((task) => task.scheduledTaskId)).toEqual(
          expect.arrayContaining(fixtures),
        );
        const denied = yield* b.authority.checkEffect("legacy-effect", threadId).pipe(Effect.flip);
        expect(denied.code).toBe("capability_denied");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("review: resource-scope removal must deny an already mapped effect on unrelated work", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* makeNode(descriptorOf("review-scope-b", "B", { peerAuthority: 1 }));
      const a = yield* makeNode(descriptorOf("review-scope-a", "A", { peerAuthority: 1 }));
      yield* linkTo(a.links, b, "full-access");
      const parent = ThreadId.make("review-scope-parent");
      const target = ThreadId.make("review-scope-unrelated");
      yield* seedParent(a, parent);
      yield* seedParent(b, target);
      const active = (yield* b.threads.getThreadShell(target))!.activeRunId!;
      yield* b.threads.dispatch({
        type: "run.interrupt",
        commandId: CommandId.make("review-stop"),
        threadId: target,
        runId: active,
        holdQueue: true,
      });
      yield* a.forwarding.call(
        caller(a, parent),
        ProjectToolkit.tools.t3_project_list,
        b.environmentId,
        {},
      );
      const grant = (yield* b.authority.grants)[0]!;
      yield* b.authority.setGrant({ ...grant, resourceScope: "environment-resources" });
      const sent = yield* nativeCall(a, parent, "t3_thread_send", {
        environmentId: b.environmentId,
        threadId: target,
        message: "Remote command",
        mode: "auto",
        clientRequestId: "review-scope-send",
      });
      expect(sent.isError, json(sent.content)).not.toBe(true);
      const sql = Context.get(b.database, SqlClient.SqlClient);
      const rows = yield* sql<{
        effect_id: string;
      }>`SELECT p.effect_id FROM peer_effect_authority p JOIN orchestration_v2_effect_outbox e ON e.effect_id=p.effect_id WHERE e.thread_id=${target} AND e.effect_type='provider-turn.start'`;
      expect(rows).toHaveLength(1);
      yield* b.authority.setGrant({ ...grant, resourceScope: "created-work" });
      const checked = yield* Effect.result(b.authority.checkEffect(rows[0]!.effect_id, target));
      const executed = yield* Ref.make<ReadonlyArray<string>>([]);
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
        Effect.provide(
          EffectWorker.layerWithOptions({ maxAttempts: 1 }).pipe(
            Layer.provide(
              Layer.succeed(EffectWorker.OrchestrationEffectExecutorV2, {
                execute: (effect) => Ref.update(executed, (ids) => [...ids, effect.id]),
              }),
            ),
            Layer.provide(Layer.succeed(PeerAuthority.PeerAuthority, b.authority)),
            Layer.provide(Layer.succeedContext(b.runtime)),
          ),
        ),
      );
      yield* worker.drain();
      const effectId = rows[0]!.effect_id;
      expect({
        guard: checked._tag,
        executed: (yield* Ref.get(executed)).includes(effectId),
      }).toEqual({ guard: "Failure", executed: false });
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("review: queued sends must retain authority when promoted after link revocation", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* makeNode(descriptorOf("review-queue-b", "B", { peerAuthority: 1 }), "disk");
      const a = yield* makeNode(descriptorOf("review-queue-a", "A", { peerAuthority: 1 }));
      yield* linkTo(a.links, b, "full-access");
      const parent = ThreadId.make("review-queue-parent");
      const target = ThreadId.make("review-queue-unrelated");
      yield* seedParent(a, parent);
      yield* seedParent(b, target);
      yield* a.forwarding.call(
        caller(a, parent),
        ProjectToolkit.tools.t3_project_list,
        b.environmentId,
        {},
      );
      const grant = (yield* b.authority.grants)[0]!;
      yield* b.authority.setGrant({ ...grant, resourceScope: "environment-resources" });
      const sent = yield* nativeCall(a, parent, "t3_thread_send", {
        environmentId: b.environmentId,
        threadId: target,
        message: "Queued remote command",
        mode: "queue",
        clientRequestId: "review-queue-send",
      });
      expect(sent.isError, json(sent.content)).not.toBe(true);
      const runs = (yield* b.threads.getThreadRecords(target, ["runs"])).runs;
      const queued = runs.find((run) => run.status === "queued")!;
      expect(queued).toBeDefined();
      yield* a.links.unlink(b.environmentId);
      const active = runs.find((run) => run.status === "running")!;
      const now = yield* DateTime.now;
      yield* b.sink.write({
        events: [
          {
            id: EventId.make("review-queue-finish"),
            type: "run.updated",
            threadId: target,
            runId: active.id,
            providerInstanceId: instanceId,
            occurredAt: now,
            payload: { ...active, status: "completed", completedAt: now },
          },
        ],
      });
      yield* b.orchestrator
        .streamStoredEventsFrom({ threadId: target, afterSequence: 0, eventType: "run.updated" })
        .pipe(
          Stream.filter(
            (stored) =>
              stored.event.type === "run.updated" &&
              stored.event.payload.id === queued.id &&
              stored.event.payload.status !== "queued",
          ),
          Stream.runHead,
          Effect.flatMap(Effect.fromOption),
        );
      // Acquire the same lock so promotion has committed its outbox effect.
      yield* b.threads.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make("review-queue-barrier"),
        threadId: target,
        runtimeMode: "full-access",
      });
      const sql = Context.get(b.database, SqlClient.SqlClient);
      const effects = yield* sql<{
        effect_id: string;
      }>`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE thread_id=${target} AND effect_type='provider-turn.start' ORDER BY rowid DESC`;
      const latest = effects[0]!;
      const promoted = yield* Context.get(b.runtime, EffectOutbox.EffectOutboxV2).get(
        latest.effect_id,
      );
      expect(Option.isSome(promoted) && promoted.value.request).toEqual({
        type: "provider-turn.start",
        runId: queued.id,
      });
      const mapped =
        yield* sql`SELECT record_id FROM peer_effect_authority WHERE effect_id=${latest.effect_id}`;
      expect(mapped).toHaveLength(1);
      const reopened = yield* b.reopenDatabase;
      const persisted = yield* Context.get(reopened, SqlClient.SqlClient)<{
        record_id: string;
      }>`SELECT record_id FROM peer_run_authority WHERE run_id = ${queued.id}`;
      expect(persisted).toEqual(mapped);
      expect(
        yield* sql`SELECT record_id FROM peer_thread_authority WHERE thread_id = ${target}`,
      ).toHaveLength(0);
      const localEffect = effects.find((effect) => effect.effect_id !== latest.effect_id);
      if (!localEffect) return yield* Effect.die("Missing original local start");
      yield* b.authority.checkEffect(localEffect.effect_id, target);
      // Recovery creates a new run from its durable source without MCP intake.
      const recoveryId = RunId.make("review-queue-recovery");
      const recoveryEffect = "effect:review-queue-recovery";
      yield* b.sink.writeWithEffects({
        events: [
          {
            id: EventId.make("review-queue-recovery"),
            type: "run.created",
            threadId: target,
            runId: recoveryId,
            providerInstanceId: instanceId,
            occurredAt: now,
            payload: {
              ...queued,
              id: recoveryId,
              ordinal: queued.ordinal + 1,
              restartContinuationOfRunId: queued.id,
              status: "starting",
              queuePosition: null,
            },
          },
        ],
        effects: [
          {
            id: recoveryEffect,
            commandId: CommandId.make("review-queue-recovery"),
            threadId: target,
            request: { type: "provider-turn.start", runId: recoveryId },
          },
        ],
      });
      const recovered = yield* Effect.result(b.authority.checkEffect(recoveryEffect, target));
      expect(recovered._tag).toBe("Failure");
      const checked = yield* Effect.result(b.authority.checkEffect(latest.effect_id, target));
      const executed = yield* Ref.make<ReadonlyArray<string>>([]);
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
        Effect.provide(
          EffectWorker.layerWithOptions({ maxAttempts: 1 }).pipe(
            Layer.provide(
              Layer.succeed(EffectWorker.OrchestrationEffectExecutorV2, {
                execute: (effect) => Ref.update(executed, (ids) => [...ids, effect.id]),
              }),
            ),
            Layer.provide(Layer.succeed(PeerAuthority.PeerAuthority, b.authority)),
            Layer.provide(Layer.succeedContext(b.runtime)),
          ),
        ),
      );
      yield* worker.drain();
      expect((yield* Ref.get(executed)).includes(recoveryEffect)).toBe(false);
      expect((yield* Ref.get(executed)).includes(localEffect.effect_id)).toBe(true);
      const effectId = latest.effect_id;
      expect({
        guard: checked._tag,
        executed: (yield* Ref.get(executed)).includes(effectId),
      }).toEqual({ guard: "Failure", executed: false });
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "review: delegated launch must freeze the originating capabilities rather than the follower scope",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* makeNode(descriptorOf("review-caps-b", "B", { peerAuthority: 1 }));
        const a = yield* makeNode(descriptorOf("review-caps-a", "A", { peerAuthority: 1 }));
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("review-caps-parent");
        yield* seedParent(a, parent);
        yield* a.forwarding.call(
          caller(a, parent),
          ProjectToolkit.tools.t3_project_list,
          b.environmentId,
          {},
        );
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, preview: true, device: true });
        const result = yield* nativeCall(a, parent, "delegate_task", {
          task: "Use the explicitly permitted tools",
          target: {
            environmentId: b.environmentId,
            projectId: b.project.id,
            providerInstanceId: instanceId,
            model: "gpt-5.4",
          },
          mode: "async",
          clientRequestId: "review-caps",
        });
        expect(result.isError, json(result.content)).not.toBe(true);
        const decoded = yield* decodeTaskReceipt(result.structuredContent);
        const child = yield* attachedChild(a, parent, decoded.taskId);
        const record = yield* b.authority.forThread(child);
        expect(record?.ceiling.capabilities).toEqual(
          expect.arrayContaining(["preview", "device", "worktree", "pull-requests"]),
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("explicit descendant environment grants retain operation mode policy", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* makeNode(descriptorOf("policy-b", "B", { peerAuthority: 1 }));
      const a = yield* makeNode(descriptorOf("policy-a", "A", { peerAuthority: 1 }));
      yield* linkTo(a.links, b, "full-access");
      const parent = ThreadId.make("policy-parent");
      yield* seedParent(a, parent);
      yield* a.forwarding.call(
        caller(a, parent),
        ProjectToolkit.tools.t3_project_list,
        b.environmentId,
        {},
      );
      const grant = (yield* b.authority.grants)[0];
      if (!grant) return yield* Effect.die("Missing grant");
      yield* b.authority.setGrant({ ...grant, resourceScope: "environment-resources" });
      const task = yield* delegate(a, b, parent, "policy-job");
      const authority = yield* b.authority.checkThread(task.childId);
      const thread = yield* b.threads.getThreadShell(task.childId);
      if (!authority || !thread) return yield* Effect.die("Missing descendant authority");
      for (const [runtimeMode, interactionMode, resourceScope, allowed] of [
        ["full-access", "default", "environment-resources", true],
        ["auto", "default", "environment-resources", false],
        ["full-access", "plan", "environment-resources", false],
        ["full-access", "default", "created-work", false],
      ] as const) {
        yield* b.authority.setGrant({ ...grant, resourceScope });
        const scoped = { ...caller(b, task.childId), authority };
        const result = yield* Effect.result(
          assertNotLinked(
            { scope: scoped, caller: thread, limits: { runtimeMode, interactionMode } },
            "change environment settings",
          ).pipe(Effect.provideService(PeerAuthority.PeerAuthority, b.authority)),
        );
        expect(result._tag === "Success").toBe(allowed);
      }
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("launch capabilities never acquire missing initial preview or device consent", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* makeNode(descriptorOf("caps-narrow-b", "B", { peerAuthority: 1 }));
      const a = yield* makeNode(descriptorOf("caps-narrow-a", "A", { peerAuthority: 1 }));
      yield* linkTo(a.links, b, "full-access");
      const parent = ThreadId.make("caps-narrow-parent");
      yield* seedParent(a, parent);
      yield* a.forwarding.call(
        caller(a, parent),
        ProjectToolkit.tools.t3_project_list,
        b.environmentId,
        {},
      );
      const grant = (yield* b.authority.grants)[0];
      if (!grant) return yield* Effect.die("Missing grant");
      yield* b.authority.setGrant({ ...grant, preview: true, device: true });
      const result = yield* nativeCall(
        a,
        parent,
        "delegate_task",
        {
          task: "Keep the accepted narrow capabilities",
          target: {
            environmentId: b.environmentId,
            projectId: b.project.id,
            providerInstanceId: instanceId,
            model: "gpt-5.4",
          },
          mode: "async",
          clientRequestId: "caps-narrow-job",
        },
        new Set(["orchestration"]),
      );
      expect(result.isError, json(result.content)).not.toBe(true);
      const receipt = yield* decodeTaskReceipt(result.structuredContent);
      const child = yield* attachedChild(a, parent, receipt.taskId);
      const original = yield* b.authority.forThread(child);
      expect(original?.ceiling.capabilities).not.toContain("preview");
      expect(original?.ceiling.capabilities).not.toContain("device");
      yield* b.registry.revokeAll;
      const rotated = yield* b.registry.issue({
        threadId: child,
        providerInstanceId: instanceId,
        capabilities: new Set(["preview", "device"]),
      });
      expect(rotated.config.capabilities?.has("preview")).toBe(false);
      expect(rotated.config.capabilities?.has("device")).toBe(false);
      const source = yield* a.threads.getThreadRecords(parent, ["subagents"]);
      expect(
        source.subagents.find((task) => task.id === receipt.taskId)?.remoteChild?.launch
          ?.capabilities,
      ).not.toContain("preview");
      expect(
        source.subagents.find((task) => task.id === receipt.taskId)?.remoteChild?.launch
          ?.capabilities,
      ).not.toContain("device");
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "origin validation expires at the exact grant boundary and leaves no transient history",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* makeNode(descriptorOf("expiry-b", "B", { peerAuthority: 1 }));
        const a = yield* makeNode(descriptorOf("expiry-a", "A", { peerAuthority: 1 }));
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("expiry-parent");
        yield* seedParent(a, parent);
        const linked = yield* a.links.get(b.environmentId);
        if (Option.isNone(linked)) return yield* Effect.die("Missing link");
        const link = linked.value;
        yield* Effect.forEach(
          Array.from({ length: 12 }),
          () =>
            a.forwarding.call(
              caller(a, parent),
              ProjectToolkit.tools.t3_project_list,
              b.environmentId,
              {},
            ),
          { concurrency: 4 },
        );
        for (const node of [a, b]) {
          const sql = Context.get(node.database, SqlClient.SqlClient);
          const frames =
            yield* sql`SELECT id FROM peer_control_frames WHERE json_extract(frame_json, '$.body.type') = 'validate'`;
          expect(frames).toHaveLength(0);
        }
        const envelope = yield* a.authority.prepare(
          caller(a, parent),
          b.environmentId,
          link.launchIdentity,
          "t3_project_list",
          {},
          { runtimeMode: "full-access", interactionMode: "default" },
          undefined,
          true,
        );
        const clock = yield* Clock.Clock;
        const expiry = DateTime.toEpochMillis(link.expiresAt);
        for (const [now, expected] of [
          [expiry - 1, true],
          [expiry, false],
        ] as const) {
          const result = yield* a.authority
            .process(b.environmentId, link.launchIdentity, {
              id: "expiry-check",
              body: { type: "validate", envelope },
            })
            .pipe(
              Effect.provideService(Clock.Clock, {
                currentTimeMillis: Effect.succeed(now),
                currentTimeMillisUnsafe: () => now,
                currentTimeNanos: clock.currentTimeNanos,
                currentTimeNanosUnsafe: () => clock.currentTimeNanosUnsafe(),
                monotonicTimeNanos: clock.monotonicTimeNanos,
                monotonicTimeNanosUnsafe: () => clock.monotonicTimeNanosUnsafe(),
                sleep: (duration) => clock.sleep(duration),
              }),
            );
          expect(result.result.ok).toBe(expected);
        }
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "round2: a peer-owned run on an ordinary thread retains authority for subsequent tools",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* makeNode(descriptorOf("r2-run-b", "B", { peerAuthority: 1 }));
        const a = yield* makeNode(descriptorOf("r2-run-a", "A", { peerAuthority: 1 }));
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("r2-run-parent");
        const target = ThreadId.make("r2-existing-target");
        const unrelated = ThreadId.make("r2-other-local-thread");
        yield* seedParent(a, parent);
        yield* seedParent(b, target);
        yield* seedParent(b, unrelated);
        const cached = yield* b.registry.issue({
          threadId: target,
          providerInstanceId: instanceId,
          capabilities: new Set(["orchestration", "preview", "device"]),
        });
        expect(cached.config.capabilities?.has("preview")).toBe(true);
        const active = (yield* b.threads.getThreadShell(target))!.activeRunId!;
        yield* b.threads.dispatch({
          type: "run.interrupt",
          commandId: CommandId.make("r2-stop-target"),
          threadId: target,
          runId: active,
          holdQueue: true,
        });
        yield* a.forwarding.call(
          caller(a, parent),
          ProjectToolkit.tools.t3_project_list,
          b.environmentId,
          {},
        );
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({
          ...grant,
          resourceScope: "environment-resources",
          onward: true,
          preview: true,
          device: true,
        });
        const intake = yield* nativeCall(
          a,
          parent,
          "t3_thread_send",
          {
            environmentId: b.environmentId,
            threadId: target,
            message: "Start remote-owned run",
            mode: "auto",
            clientRequestId: "r2-run-owner",
          },
          new Set(["orchestration"]),
        );
        expect(intake.isError, json(intake.content)).not.toBe(true);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const mappings = yield* sql<{
          effect_id: string;
        }>`SELECT p.effect_id FROM peer_effect_authority p JOIN orchestration_v2_effect_outbox e ON p.effect_id=e.effect_id WHERE e.thread_id=${target} AND e.effect_type='provider-turn.start'`;
        expect(mappings).toHaveLength(1);
        // The real effect guard accepts the intact grant before the external adapter starts the run.
        yield* b.authority.checkEffect(mappings[0]!.effect_id, target);
        const startingOwner = yield* b.authority.forThread(target);
        expect(startingOwner?.ceiling.capabilities).not.toContain("preview");
        expect(startingOwner?.ceiling.capabilities).not.toContain("device");
        const run = yield* activate(b, target);
        const owners = yield* sql`SELECT record_id FROM peer_run_authority WHERE run_id=${run}`;
        expect(owners).toHaveLength(1);
        expect(
          yield* sql`SELECT record_id FROM peer_thread_authority WHERE thread_id = ${target}`,
        ).toHaveLength(0);
        const issued = yield* b.registry.issue({
          threadId: target,
          providerInstanceId: instanceId,
          capabilities: new Set(["orchestration", "preview", "device"]),
        });
        const preview = issued.config.capabilities?.has("preview") === true;
        const device = issued.config.capabilities?.has("device") === true;
        const resolvedCached = yield* b.registry.resolve(
          cached.config.authorizationHeader.replace(/^Bearer\s+/, ""),
        );
        expect(resolvedCached?.authority?.id).toBe((yield* b.authority.forThread(target))?.id);
        expect(resolvedCached?.capabilities.has("preview")).toBe(false);
        expect(resolvedCached?.capabilities.has("device")).toBe(false);
        // Local top-level creation and delegated child creation both retain the run owner.
        const created = yield* nativeCall(
          b,
          target,
          "create_threads",
          {
            threads: [
              {
                title: "Run-owned local child",
                prompt: "Keep the accepted scope",
                target: { providerInstanceId: instanceId, model: "gpt-5.4" },
              },
            ],
            clientRequestId: "r2-local-created",
          },
          undefined,
          cached,
        );
        expect(created.isError, json(created.content)).not.toBe(true);
        const children = (yield* decodeCreatedThreads(created.structuredContent)).threads;
        const child = children[0]?.threadId;
        if (!child) return yield* Effect.die("Missing run-owned local child");
        const delegated = yield* nativeCall(
          b,
          target,
          "delegate_task",
          {
            task: "Keep delegated scope",
            mode: "async",
            target: { providerInstanceId: instanceId, model: "gpt-5.4" },
            clientRequestId: "r2-local-delegated",
          },
          undefined,
          cached,
        );
        expect(delegated.isError, json(delegated.content)).not.toBe(true);
        const delegatedTask = yield* decodeTaskReceipt(delegated.structuredContent);
        const localChild = (yield* b.threads.getThreadRecords(target, [
          "subagents",
        ])).subagents.find((task) => task.id === delegatedTask.taskId)?.childThreadId;
        if (!localChild) return yield* Effect.die("Missing delegated local child");
        for (const id of [child, localChild]) {
          expect((yield* b.authority.forThread(id))?.id).toBe(resolvedCached?.authority?.id);
          const issuedChild = yield* b.registry.issue({
            threadId: id,
            providerInstanceId: instanceId,
            capabilities: new Set(["preview", "device"]),
          });
          expect(issuedChild.config.capabilities?.has("preview")).toBe(false);
          expect(issuedChild.config.capabilities?.has("device")).toBe(false);
        }
        // An onward launch uses this run's originating chain, never B's cached wide token.
        const c = yield* makeNode(descriptorOf("r2-run-c", "C", { peerAuthority: 1 }));
        yield* linkTo(b.links, c, "full-access");
        const onward = yield* nativeCall(
          b,
          target,
          "t3_thread_launch",
          {
            environmentId: c.environmentId,
            projectId: c.project.id,
            title: "Run-owned onward",
            message: "Keep the source ceiling",
            modelSelection: selection,
            clientRequestId: "r2-onward",
          },
          undefined,
          cached,
        );
        expect(onward.isError, json(onward.content)).not.toBe(true);
        const onwardChild = (yield* decodeLaunchReceipt(onward.structuredContent)).threadId;
        expect((yield* c.authority.forThread(onwardChild))?.ceiling.capabilities).not.toContain(
          "preview",
        );
        expect((yield* c.authority.forThread(onwardChild))?.ceiling.capabilities).not.toContain(
          "device",
        );
        yield* b.authority.setGrant({ ...grant, resourceScope: "created-work", onward: true });
        const ownedMutation = yield* nativeCall(a, parent, "t3_thread_update", {
          environmentId: b.environmentId,
          threadId: localChild,
          action: "rename",
          title: "Verified created descendant",
        });
        expect(ownedMutation.isError, json(ownedMutation.content)).not.toBe(true);
        expect((yield* b.threads.getThreadShell(localChild))?.title).toBe(
          "Verified created descendant",
        );
        expect((yield* Effect.result(b.authority.checkThread(target)))._tag).toBe("Failure");
        yield* b.authority.setGrant({
          ...grant,
          resourceScope: "environment-resources",
          onward: true,
        });
        yield* a.links.unlink(b.environmentId);
        // The currently active remote-owned run should now fail its next native MCP mutation.
        const sent = yield* nativeCall(
          b,
          target,
          "t3_thread_send",
          {
            threadId: unrelated,
            message: "Forbidden after originating link revocation",
            mode: "queue",
            clientRequestId: "r2-escape",
          },
          undefined,
          cached,
        );
        const records = yield* b.threads.getThreadRecords(unrelated, ["messages"]);
        expect({
          denied: sent.isError === true,
          forbiddenMessageCommitted: records.messages.some(
            (m) => m.text === "Forbidden after originating link revocation",
          ),
          preview,
          device,
        }).toEqual({
          denied: true,
          forbiddenMessageCommitted: false,
          preview: false,
          device: false,
        });
        const rotated = yield* b.registry.issue({
          threadId: target,
          providerInstanceId: instanceId,
          capabilities: new Set(["preview", "device"]),
        });
        expect(rotated.config.capabilities?.has("preview")).toBe(false);
        expect(rotated.config.capabilities?.has("device")).toBe(false);
        const deniedFresh = yield* nativeCall(
          b,
          target,
          "t3_thread_send",
          {
            threadId: unrelated,
            message: "Forbidden rotated credential",
            mode: "queue",
            clientRequestId: "r2-rotated",
          },
          undefined,
          rotated,
        );
        expect(deniedFresh.isError).toBe(true);
        expect((yield* Effect.result(c.authority.checkThread(onwardChild)))._tag).toBe("Failure");
        // A new local run is a new local owner, while the children retain their creation owner.
        yield* b.threads.dispatch({
          type: "run.interrupt",
          commandId: CommandId.make("r2-end-peer-run"),
          threadId: target,
          runId: run,
          holdQueue: true,
        });
        yield* b.threads.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("r2-local-run"),
          threadId: target,
          messageId: MessageId.make("r2-local-run"),
          text: "User local work",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        yield* activate(b, target);
        expect(yield* b.authority.forThread(target)).toBeUndefined();
        const localScope = yield* b.registry.resolve(
          cached.config.authorizationHeader.replace(/^Bearer\s+/, ""),
        );
        expect(localScope?.authority).toBeUndefined();
        expect(localScope?.capabilities.has("preview")).toBe(true);
        const localSend = yield* nativeCall(
          b,
          target,
          "t3_thread_send",
          {
            threadId: unrelated,
            message: "Permitted subsequent local run",
            mode: "queue",
            clientRequestId: "r2-local-send",
          },
          undefined,
          cached,
        );
        expect(localSend.isError, json(localSend.content)).not.toBe(true);
        expect(
          (yield* b.threads.getThreadRecords(unrelated, ["messages"])).messages.some(
            (message) => message.text === "Permitted subsequent local run",
          ),
        ).toBe(true);
        const oldPeerScope = yield* b.registry.resolve(
          rotated.config.authorizationHeader.replace(/^Bearer\s+/, ""),
        );
        expect(oldPeerScope?.authority).toBeUndefined();
        expect(oldPeerScope?.capabilities.has("preview")).toBe(false);
        const oldPeerLocal = yield* nativeCall(
          b,
          target,
          "t3_thread_send",
          {
            threadId: unrelated,
            message: "Local run with the bounded old credential",
            mode: "queue",
            clientRequestId: "r2-bounded-local",
          },
          undefined,
          rotated,
        );
        expect(oldPeerLocal.isError, json(oldPeerLocal.content)).not.toBe(true);
        for (const id of [child, localChild]) {
          expect((yield* b.authority.forThread(id))?.id).toBe(resolvedCached?.authority?.id);
          expect((yield* Effect.result(b.authority.checkThread(id)))._tag).toBe("Failure");
        }
        expect(
          yield* sql`SELECT record_id FROM peer_thread_authority WHERE thread_id=${target}`,
        ).toHaveLength(0);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "round2: registered schedule mutations require the acting descendant's current full default modes",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* makeNode(descriptorOf("r2-policy-b", "B", { peerAuthority: 1 }));
        const a = yield* makeNode(descriptorOf("r2-policy-a", "A", { peerAuthority: 1 }));
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("r2-policy-parent");
        yield* seedParent(a, parent);
        yield* a.forwarding.call(
          caller(a, parent),
          ProjectToolkit.tools.t3_project_list,
          b.environmentId,
          {},
        );
        const grant = (yield* b.authority.grants)[0];
        if (!grant) return yield* Effect.die("Missing policy grant");
        yield* b.authority.setGrant({ ...grant, resourceScope: "environment-resources" });
        const observed: Array<{
          allowed: boolean;
          create: boolean;
          update: boolean;
          remove: boolean;
        }> = [];
        for (const [key, runtimeMode, interactionMode, lower, allowed] of [
          ["auto", "auto", "default", false, false],
          ["plan", "full-access", "plan", false, false],
          ["lower-auto", "auto", "default", true, false],
          ["lower-plan", "full-access", "plan", true, false],
          ["full", "full-access", "default", false, true],
          ["created", "full-access", "default", false, false],
        ] as const) {
          yield* b.authority.setGrant({
            ...grant,
            resourceScope: key === "created" ? "created-work" : "environment-resources",
          });
          const launched = yield* nativeCall(a, parent, "delegate_task", {
            task: "Check schedule operation policy",
            mode: "async",
            clientRequestId: `r2-policy-${key}`,
            runtimeMode: lower ? "full-access" : runtimeMode,
            interactionMode: lower ? "default" : interactionMode,
            target: {
              environmentId: b.environmentId,
              projectId: b.project.id,
              providerInstanceId: instanceId,
              model: "gpt-5.4",
            },
          });
          expect(launched.isError, json(launched.content)).not.toBe(true);
          const receipt = yield* decodeTaskReceipt(launched.structuredContent);
          const child = yield* attachedChild(a, parent, receipt.taskId);
          yield* activate(b, child);
          const authority = yield* b.authority.forThread(child);
          expect(authority?.ceiling.runtimeMode).toBe("full-access");
          expect(authority?.ceiling.interactionMode).toBe("default");
          if (lower) {
            yield* b.threads.dispatch({
              type: "thread.runtime-mode.set",
              commandId: CommandId.make(`r2-policy-runtime-${key}`),
              threadId: child,
              runtimeMode,
            });
            yield* b.threads.dispatch({
              type: "thread.interaction-mode.set",
              commandId: CommandId.make(`r2-policy-interaction-${key}`),
              threadId: child,
              interactionMode,
            });
          }
          const fixture = yield* b.scheduled.upsert({
            title: `fixture-${key}`,
            prompt: "Not executed",
            enabled: false,
            schedule: { type: "interval", everyMs: 60000 },
            projectId: b.project.id,
            threadId: null,
            workspaceStrategy: { type: "root" },
            modelSelection: selection,
            runtimeMode: "approval-required",
            interactionMode: "plan",
            createdBy: "user",
            creationSource: "web",
          });
          const created = yield* nativeCall(b, child, "schedule_task", {
            title: `created-${key}`,
            prompt: "Not executed",
            enabled: false,
            schedule: { type: "interval", everyMs: 60000 },
            bindToCurrentThread: false,
            clientRequestId: `r2-schedule-${key}`,
          });
          const updated = yield* nativeCall(b, child, "update_scheduled_task", {
            scheduledTaskId: fixture.task.id,
            title: `updated-${key}`,
          });
          const removed = yield* nativeCall(b, child, "delete_scheduled_task", {
            scheduledTaskId: fixture.task.id,
          });
          const stored = (yield* b.scheduled.list()).tasks;
          observed.push({
            allowed,
            create: stored.some((task) => task.title === `created-${key}`),
            update: updated.isError !== true,
            remove: !stored.some((task) => task.id === fixture.task.id),
          });
          expect(created.isError !== true, json(created.content)).toBe(allowed);
          expect(removed.isError !== true, json(removed.content)).toBe(allowed);
          if (!allowed)
            expect(stored.find((task) => task.id === fixture.task.id)?.title).toBe(
              `fixture-${key}`,
            );
        }
        expect(observed).toEqual(
          [false, false, false, false, true, false].map((allowed) => ({
            allowed,
            create: allowed,
            update: allowed,
            remove: allowed,
          })),
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("round2: existing jobs and incoming runs retain both ceilings and live owners", () =>
  Effect.scoped(
    Effect.gen(function* () {
      for (const wideJob of [false, true]) {
        const b = yield* makeNode(
          descriptorOf(`joint-b-${wideJob}`, "B", { peerAuthority: 1 }),
          "disk",
        );
        const a = yield* makeNode(descriptorOf(`joint-a-${wideJob}`, "A", { peerAuthority: 1 }));
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make(`joint-parent-${wideJob}`);
        yield* seedParent(a, parent);
        yield* a.forwarding.call(
          caller(a, parent),
          ProjectToolkit.tools.t3_project_list,
          b.environmentId,
          {},
        );
        const grant = (yield* b.authority.grants)[0];
        if (!grant) return yield* Effect.die("Missing grant");
        yield* b.authority.setGrant({
          ...grant,
          resourceScope: "environment-resources",
          preview: true,
          device: true,
        });
        const narrow = new Set(["orchestration"] as const);
        const wide = new Set(["orchestration", "preview", "device"] as const);
        const launched = yield* nativeCall(
          a,
          parent,
          "delegate_task",
          {
            task: "Retain the original job",
            mode: "async",
            target: {
              environmentId: b.environmentId,
              projectId: b.project.id,
              providerInstanceId: instanceId,
              model: "gpt-5.4",
            },
            clientRequestId: "joint-original",
          },
          wideJob ? wide : narrow,
        );
        expect(launched.isError, json(launched.content)).not.toBe(true);
        const task = yield* decodeTaskReceipt(launched.structuredContent);
        const child = yield* attachedChild(a, parent, task.taskId);
        const original = yield* b.authority.forThread(child);
        if (!original) return yield* Effect.die("Missing creation authority");
        yield* activate(b, child);
        const old = (yield* b.threads.getThreadRecords(child, ["runs"])).runs.find(
          (run) => run.status === "running",
        );
        if (!old) return yield* Effect.die("Missing executed run");
        yield* b.sink.write({
          events: [
            {
              id: EventId.make("joint-finish"),
              type: "run.updated",
              threadId: child,
              runId: old.id,
              providerInstanceId: instanceId,
              occurredAt: yield* DateTime.now,
              payload: { ...old, status: "completed", completedAt: yield* DateTime.now },
            },
          ],
        });
        const sent = yield* nativeCall(
          a,
          parent,
          "t3_thread_send",
          {
            environmentId: b.environmentId,
            threadId: child,
            message: "New prompt, same job",
            mode: "auto",
            clientRequestId: "joint-send",
          },
          wideJob ? narrow : wide,
        );
        expect(sent.isError, json(sent.content)).not.toBe(true);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const effects = yield* sql<{
          effect_id: string;
        }>`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE thread_id=${child} AND effect_type='provider-turn.start' ORDER BY rowid DESC`;
        if (!effects[0]) return yield* Effect.die("Missing start effect");
        yield* b.authority.checkEffect(effects[0].effect_id, child);
        yield* activate(b, child);
        const issued = yield* b.registry.issue({
          threadId: child,
          providerInstanceId: instanceId,
          capabilities: wide,
        });
        expect(issued.config.capabilities?.has("preview")).toBe(false);
        expect(issued.config.capabilities?.has("device")).toBe(false);
        const compound = yield* b.authority.checkThread(child);
        if (!compound) return yield* Effect.die("Missing execution authority");
        const restored = yield* restoreAuthority(b);
        expect(yield* restored.forThread(child)).toEqual(compound);
        const restoredRegistry = yield* McpSessionRegistry.__testing.make().pipe(
          Effect.provideService(PeerAuthority.PeerAuthority, restored),
          Effect.provide(Layer.succeedContext(b.serverContext)),
          Effect.provideService(Environment.ServerEnvironment, {
            getEnvironmentId: Effect.succeed(b.environmentId),
            getDescriptor: Ref.get(b.descriptor),
          }),
        );
        const afterRestart = yield* restoredRegistry.issue({
          threadId: child,
          providerInstanceId: instanceId,
          capabilities: wide,
        });
        expect(afterRestart.config.capabilities?.has("preview")).toBe(false);
        expect(afterRestart.config.capabilities?.has("device")).toBe(false);
        if (compound.context !== undefined) {
          const { context: _context, ...legacyView } = compound;
          const alias = `legacy-reader:${compound.id}`;
          yield* sql`INSERT INTO peer_authority_records (id,record_json) VALUES (${alias},${json({ ...legacyView, id: alias })})`;
          yield* sql`INSERT INTO peer_effect_authority (effect_id,record_id) VALUES ('joint-legacy-effect',${alias})`;
          expect(
            (yield* Effect.result(b.authority.checkEffect("joint-legacy-effect", child)))._tag,
          ).toBe("Failure");
        }
        const descendants = yield* nativeCall(b, child, "create_threads", {
          threads: [
            {
              prompt: "Keep both ceilings",
              target: { providerInstanceId: instanceId, model: "gpt-5.4" },
            },
          ],
          clientRequestId: "joint-descendant",
        });
        expect(descendants.isError, json(descendants.content)).not.toBe(true);
        const descendant = (yield* decodeCreatedThreads(descendants.structuredContent)).threads[0]
          ?.threadId;
        if (!descendant) return yield* Effect.die("Missing descendant");
        expect((yield* b.authority.forThread(descendant))?.ceiling.capabilities).not.toContain(
          "preview",
        );
        const returned = yield* sendNotice(
          b,
          child,
          a.environmentId,
          parent,
          "Joint job returns to its original parent",
          "joint-return",
        );
        expect(returned.threadId).toBe(parent);
        const notice = yield* awaitNotice(a, parent, "Joint job returns to its original parent");
        if (notice.event.type !== "message.updated")
          return yield* Effect.die("Missing original-pair notice");
        expect(notice.event.payload.notification?.source).toMatchObject({
          kind: "delegated_task",
          taskIds: [task.taskId],
        });
        const source = (yield* a.threads.getThreadRecords(parent, ["runs"])).runs.find(
          (run) => run.status === "running",
        );
        if (!source) return yield* Effect.die("Missing source run");
        yield* a.sink.write({
          events: [
            {
              id: EventId.make("joint-source-finish"),
              type: "run.updated",
              threadId: parent,
              runId: source.id,
              providerInstanceId: instanceId,
              occurredAt: yield* DateTime.now,
              payload: { ...source, status: "completed", completedAt: yield* DateTime.now },
            },
          ],
        });
        const stillValid = yield* a.authority.process(b.environmentId, original.peer.generation, {
          id: "joint-creation-still-valid",
          body: { type: "validate", envelope: original.envelope },
        });
        expect(stillValid.result.ok).toBe(true);
        const forbidden = yield* nativeCall(b, child, "t3_thread_send", {
          threadId: child,
          message: "Forbidden after contributor ended",
          mode: "queue",
          clientRequestId: "joint-forbidden",
        });
        const messages = (yield* b.threads.getThreadRecords(child, ["messages"])).messages;
        expect({
          denied: forbidden.isError === true,
          committed: messages.some(
            (message) => message.text === "Forbidden after contributor ended",
          ),
        }).toEqual({ denied: true, committed: false });
        expect((yield* Effect.result(b.authority.checkThread(descendant)))._tag).toBe("Failure");
      }
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "round2: a modern run on imported work intersects its live owner with the frozen legacy ceiling",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* makeNode(descriptorOf("legacy-run-b", "B", { peerAuthority: 1 }), "disk");
        const a = yield* makeNode(descriptorOf("legacy-run-a", "A", { peerAuthority: 1 }));
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("legacy-run-parent");
        yield* seedParent(a, parent);
        yield* a.forwarding.call(
          caller(a, parent),
          ProjectToolkit.tools.t3_project_list,
          b.environmentId,
          {},
        );
        const grant = (yield* b.authority.grants)[0];
        if (!grant) return yield* Effect.die("Missing legacy grant");
        const legacySource = yield* makeNode(
          descriptorOf("legacy-original-source", "Original", { peerAuthority: 1 }),
        );
        yield* linkTo(legacySource.links, b, "full-access");
        const legacyParent = ThreadId.make("legacy-original-parent");
        yield* seedParent(legacySource, legacyParent);
        yield* legacySource.forwarding.call(
          caller(legacySource, legacyParent),
          ProjectToolkit.tools.t3_project_list,
          b.environmentId,
          {},
        );
        const originalGrant = (yield* b.authority.grants).find(
          (candidate) => candidate.environmentId === legacySource.environmentId,
        );
        if (!originalGrant) return yield* Effect.die("Missing independent legacy grant");
        const target = ThreadId.make("legacy-run-target");
        yield* b.threads.dispatch({
          type: "thread.create",
          commandId: CommandId.make("legacy-run-create"),
          threadId: target,
          projectId: b.project.id,
          title: "Imported linked work",
          modelSelection: selection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          createdBy: "agent",
          creationSource: "mcp",
          linkOrigin: { sessionId: originalGrant.sessionId, label: "Legacy link" },
        });
        expect((yield* b.authority.legacyCeiling(target))?.capabilities).not.toContain("preview");
        yield* b.authority.setGrant({
          ...grant,
          resourceScope: "environment-resources",
          preview: true,
          device: true,
        });
        const sent = yield* nativeCall(a, parent, "t3_thread_send", {
          environmentId: b.environmentId,
          threadId: target,
          message: "Modern contribution to imported work",
          clientRequestId: "legacy-run-send",
        });
        expect(sent.isError, json(sent.content)).not.toBe(true);
        yield* activate(b, target);
        const issued = yield* b.registry.issue({
          threadId: target,
          providerInstanceId: instanceId,
          capabilities: new Set(["preview", "device"]),
        });
        expect(issued.config.capabilities?.has("preview")).toBe(false);
        expect(issued.config.capabilities?.has("device")).toBe(false);
        const restored = yield* restoreAuthority(b);
        const restoredContext = yield* restored.forThread(target);
        if (restoredContext !== undefined) {
          expect(restoredContext.resourceSessionId).toBe(originalGrant.sessionId);
          expect(restoredContext.ceiling.capabilities).not.toContain("preview");
        }
        const created = yield* nativeCall(b, target, "create_threads", {
          threads: [
            {
              prompt: "Keep legacy and current ownership",
              target: { providerInstanceId: instanceId, model: "gpt-5.4" },
            },
          ],
          clientRequestId: "legacy-run-child",
        });
        expect(created.isError, json(created.content)).not.toBe(true);
        const child = (yield* decodeCreatedThreads(created.structuredContent)).threads[0]?.threadId;
        if (!child) return yield* Effect.die("Missing imported-work child");
        yield* a.links.unlink(b.environmentId);
        const denied = yield* nativeCall(b, target, "t3_thread_send", {
          threadId: target,
          message: "Forbidden imported run after unlink",
          mode: "queue",
          clientRequestId: "legacy-run-denied",
        });
        expect(denied.isError).toBe(true);
        expect(
          (yield* b.threads.getThreadRecords(target, ["messages"])).messages.some(
            (message) => message.text === "Forbidden imported run after unlink",
          ),
        ).toBe(false);
        expect((yield* Effect.result(b.authority.checkThread(child)))._tag).toBe("Failure");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

const activeProvider = (
  node: Node,
  target: ThreadId,
  capabilities: Adapter.ProviderAdapterV2SessionRuntime["providerSession"]["capabilities"] = CodexProviderCapabilitiesV2,
) =>
  Effect.gen(function* () {
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

it.live.each(["auto", "steer", "restart", "restart-only"] as const)(
  "accepted active %s input preserves contributor authority",
  (mode) =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* makeNode(descriptorOf(`r3-${mode}-b`, "B", { peerAuthority: 1 }));
        const a = yield* makeNode(descriptorOf(`r3-${mode}-a`, "A", { peerAuthority: 1 }));
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make(`r3-${mode}-source`);
        const target = ThreadId.make(`r3-${mode}-target`);
        const unrelated = ThreadId.make(`r3-${mode}-unrelated`);
        yield* seedParent(a, parent);
        yield* seedParent(b, target);
        yield* seedParent(b, unrelated);
        yield* a.forwarding.call(
          caller(a, parent),
          ProjectToolkit.tools.t3_project_list,
          b.environmentId,
          {},
        );
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({
          ...grant,
          resourceScope: "environment-resources",
          preview: true,
          device: true,
        });
        yield* activeProvider(
          b,
          target,
          mode === "restart-only"
            ? {
                ...CodexProviderCapabilitiesV2,
                turns: { ...CodexProviderCapabilitiesV2.turns, supportsActiveSteering: false },
              }
            : CodexProviderCapabilitiesV2,
        );
        const sent = yield* nativeCall(
          a,
          parent,
          "t3_thread_send",
          {
            environmentId: b.environmentId,
            threadId: target,
            message: "Incoming peer instruction",
            mode: mode === "restart-only" ? "auto" : mode,
            clientRequestId: `r3-${mode}-incoming`,
          },
          new Set(["orchestration"]),
        );
        expect(sent.isError, json(sent.content)).not.toBe(true);
        const after = yield* b.orchestrator.getThreadProjection(target);
        expect(after.runs).toHaveLength(1);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const effects = yield* sql<{
          effect_id: string;
          effect_type: string;
        }>`SELECT effect_id,effect_type FROM orchestration_v2_effect_outbox WHERE thread_id=${target} AND effect_type=${mode === "auto" || mode === "steer" ? "provider-turn.steer" : "provider-turn.restart"}`;
        expect(effects).toHaveLength(1);
        yield* b.authority.checkEffect(effects[0]!.effect_id, target);
        const executed = yield* Ref.make<ReadonlyArray<string>>([]);
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
          Effect.provide(
            EffectWorker.layerWithOptions({ maxAttempts: 1 }).pipe(
              Layer.provide(
                Layer.succeed(EffectWorker.OrchestrationEffectExecutorV2, {
                  execute: (effect) => Ref.update(executed, (ids) => [...ids, effect.id]),
                }),
              ),
              Layer.provide(Layer.succeed(PeerAuthority.PeerAuthority, b.authority)),
              Layer.provide(Layer.succeedContext(b.runtime)),
            ),
          ),
        );
        yield* worker.drain();
        expect((yield* Ref.get(executed)).includes(effects[0]!.effect_id)).toBe(true);
        const credential = yield* b.registry.issue({
          threadId: target,
          providerInstanceId: instanceId,
          capabilities: new Set(["orchestration", "preview", "device"]),
        });
        const owner = yield* b.authority.forThread(target);
        yield* a.links.unlink(b.environmentId);
        const forbidden = yield* nativeCall(
          b,
          target,
          "t3_thread_send",
          {
            threadId: unrelated,
            message: "Forbidden after active contributor unlink",
            mode: "queue",
            clientRequestId: `r3-${mode}-escape`,
          },
          undefined,
          credential,
        );
        const messages = (yield* b.threads.getThreadRecords(unrelated, ["messages"])).messages;
        expect({
          owner: owner !== undefined,
          preview: credential.config.capabilities?.has("preview"),
          device: credential.config.capabilities?.has("device"),
          denied: forbidden.isError === true,
          committed: messages.some((m) => m.text === "Forbidden after active contributor unlink"),
        }).toEqual({ owner: true, preview: false, device: false, denied: true, committed: false });
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live.each([
  { revoked: "first", reversed: false },
  { revoked: "last", reversed: false },
  { revoked: "first", reversed: true },
  { revoked: "last", reversed: true },
] as const)(
  "round4 resource conjunction after $revoked revocation, reverse sources $reversed",
  ({ revoked, reversed }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* makeNode(
          descriptorOf(`multi-${revoked}-b`, "B", { peerAuthority: 1 }),
          "disk",
        );
        const a = yield* makeNode(descriptorOf(`multi-${revoked}-a`, "A", { peerAuthority: 1 }));
        const d = yield* makeNode(descriptorOf(`multi-${revoked}-d`, "D", { peerAuthority: 1 }));
        const c = yield* makeNode(descriptorOf(`multi-${revoked}-c`, "C", { peerAuthority: 1 }));
        yield* linkTo(b.links, c, "full-access");
        const target = ThreadId.make(`multi-${revoked}-target`);
        const unrelated = ThreadId.make(`multi-${revoked}-local`);
        yield* seedParent(b, target);
        yield* seedParent(b, unrelated);
        const run = yield* activeProvider(b, target);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const cached = yield* b.registry.issue({
          threadId: target,
          providerInstanceId: instanceId,
          capabilities: new Set(["orchestration", "preview", "device"]),
        });
        const sources = reversed ? [d, a] : [a, d];
        for (const node of sources) {
          const caps =
            node === a
              ? new Set(["orchestration", "device"] as const)
              : new Set(["orchestration", "preview"] as const);
          yield* linkTo(node.links, b, "full-access");
          const parent = ThreadId.make(`multi-${node.environmentId}-source`);
          yield* seedParent(node, parent);
          yield* node.forwarding.call(
            caller(node, parent),
            ProjectToolkit.tools.t3_project_list,
            b.environmentId,
            {},
          );
          for (const grant of yield* b.authority.grants)
            yield* b.authority.setGrant({
              ...grant,
              resourceScope: "environment-resources",
              preview: true,
              device: true,
              onward: true,
            });
          const credential = yield* node.registry.issue({
            threadId: parent,
            providerInstanceId: instanceId,
            capabilities: caps,
          });
          const args = {
            environmentId: b.environmentId,
            threadId: target,
            message: `Input from ${node.environmentId}`,
            mode: "steer",
            clientRequestId: `input-${node.environmentId}`,
          };
          const accepted = yield* nativeCall(
            node,
            parent,
            "t3_thread_send",
            args,
            caps,
            credential,
          );
          expect(accepted.isError, json(accepted.content)).not.toBe(true);
          const beforeReplay = yield* b.authority.forThread(target);
          const replay = yield* nativeCall(node, parent, "t3_thread_send", args, caps, credential);
          expect(replay.isError, json(replay.content)).not.toBe(true);
          expect((yield* b.authority.forThread(target))?.id).toBe(beforeReplay?.id);
        }
        const both = yield* b.authority.checkThread(target);
        expect(both?.context?.type).toBe("run-inputs");
        expect(both?.ceiling.capabilities).not.toContain("preview");
        expect(both?.ceiling.capabilities).not.toContain("device");
        expect(
          (yield* b.registry.resolve(cached.config.authorizationHeader.slice(7)))?.authority?.id,
        ).toBe(both?.id);
        // Local steering of this same run must not replace its accepted contributors.
        const local = yield* nativeCall(b, unrelated, "t3_thread_send", {
          threadId: target,
          message: "Local same-run input",
          mode: "steer",
          clientRequestId: "local-steer",
        });
        expect(local.isError, json(local.content)).not.toBe(true);
        expect((yield* b.authority.forThread(target))?.id).toBe(both?.id);
        const childCall = yield* nativeCall(b, target, "create_threads", {
          threads: [{ prompt: "Inherited active inputs" }],
        });
        expect(childCall.isError, json(childCall.content)).not.toBe(true);
        const child = (yield* decodeCreatedThreads(childCall.structuredContent)).threads[0]!
          .threadId;
        expect((yield* b.authority.forThread(child))?.id).toBe(both?.id);
        yield* activate(b, child);
        // Combining inputs must preserve the existing created-work predicate:
        // narrowing consent denies the ordinary parent, not its actual new child.
        const exclusiveTargets = [];
        for (const source of [a, d]) {
          const exclusiveCall = yield* nativeCall(
            source,
            ThreadId.make(`multi-${source.environmentId}-source`),
            "t3_thread_launch",
            {
              environmentId: b.environmentId,
              projectId: b.project.id,
              modelSelection: selection,
              title: `Exclusive ${source.environmentId}`,
              message: "Owned by one source only",
              clientRequestId: `exclusive-${source.environmentId}`,
            },
          );
          expect(exclusiveCall.isError, json(exclusiveCall.content)).not.toBe(true);
          exclusiveTargets.push(
            (yield* decodeLaunchReceipt(exclusiveCall.structuredContent)).threadId,
          );
        }
        const sourceGrants = yield* b.authority.grants;
        const exclusiveSchedules = [];
        for (const source of [a, d]) {
          const origin = sourceGrants.find(
            (grant) => grant.environmentId === source.environmentId,
          )!;
          const created = yield* b.scheduled.upsert({
            title: `Webhook ${source.environmentId}`,
            prompt: "Not executed",
            enabled: false,
            schedule: { type: "webhook" },
            projectId: b.project.id,
            threadId: null,
            workspaceStrategy: { type: "root" },
            modelSelection: selection,
            runtimeMode: "approval-required",
            interactionMode: "plan",
            createdBy: "agent",
            creationSource: "mcp",
            linkOrigin: { sessionId: origin.sessionId, label: source.environmentId },
          });
          expect(created.task.webhook?.url).toBeTruthy();
          exclusiveSchedules.push(created.task);
        }
        const scheduleSummary = Schema.decodeUnknownEffect(
          OrchestratorToolkit.tools.list_scheduled_tasks.successSchema,
        );
        const widerGrants = yield* b.authority.grants;
        for (const grant of widerGrants)
          yield* b.authority.setGrant({ ...grant, resourceScope: "created-work" });
        expect((yield* Effect.result(b.authority.checkThread(target)))._tag).toBe("Failure");
        const rename = yield* nativeCall(b, child, "t3_thread_update", {
          threadId: child,
          action: "rename",
          title: "Still legitimate created work",
        });
        expect(rename.isError, json(rename.content)).not.toBe(true);
        expect((yield* b.threads.getThreadShell(child))?.title).toBe(
          "Still legitimate created work",
        );
        for (const source of [a, d]) {
          const resourceUpdate = yield* nativeCall(
            source,
            ThreadId.make(`multi-${source.environmentId}-source`),
            "t3_thread_update",
            {
              environmentId: b.environmentId,
              threadId: child,
              action: "rename",
              title: `Created under ${source.environmentId}`,
            },
          );
          expect(resourceUpdate.isError, json(resourceUpdate.content)).not.toBe(true);
          expect((yield* b.threads.getThreadShell(child))?.title).toBe(
            `Created under ${source.environmentId}`,
          );
        }
        const contextChildren = [];
        for (const source of [a, d]) {
          // A canonical joint birth binding permits either source's private
          // context selection even though the child has no single linkOrigin.
          const created = yield* nativeCall(
            source,
            ThreadId.make(`multi-${source.environmentId}-source`),
            "create_threads",
            {
              environmentId: b.environmentId,
              contextThreadId: child,
              threads: [
                { prompt: `Child through ${source.environmentId}'s authenticated context` },
              ],
              clientRequestId: `context-child-${source.environmentId}`,
            },
          );
          expect(created.isError, json(created.content)).not.toBe(true);
          contextChildren.push(
            (yield* decodeCreatedThreads(created.structuredContent)).threads[0]!.threadId,
          );
        }
        for (const [index, exclusive] of exclusiveTargets.entries()) {
          const otherSource = index === 0 ? d : a;
          const rejected = yield* nativeCall(
            otherSource,
            ThreadId.make(`multi-${otherSource.environmentId}-source`),
            "t3_thread_update",
            {
              environmentId: b.environmentId,
              threadId: exclusive,
              action: "rename",
              title: "Forbidden other-source rename",
            },
          );
          expect(rejected.isError, json(rejected.content)).toBe(true);
          expect((yield* b.threads.getThreadShell(exclusive))?.title).not.toBe(
            "Forbidden other-source rename",
          );
        }
        const listed = yield* nativeCall(b, child, "list_scheduled_tasks", {});
        expect(listed.isError, json(listed.content)).not.toBe(true);
        const summaries = (yield* scheduleSummary(listed.structuredContent)).tasks;
        for (const schedule of exclusiveSchedules) {
          const visible = summaries.find((task) => task.scheduledTaskId === schedule.id);
          expect(visible).toBeDefined();
          expect(visible?.webhookUrl).toBeUndefined();
        }
        for (const [index, source] of [a, d].entries()) {
          const listed = yield* nativeCall(
            source,
            ThreadId.make(`multi-${source.environmentId}-source`),
            "list_scheduled_tasks",
            { environmentId: b.environmentId },
          );
          expect(listed.isError, json(listed.content)).not.toBe(true);
          const summaries = (yield* scheduleSummary(listed.structuredContent)).tasks;
          for (const [taskIndex, schedule] of exclusiveSchedules.entries())
            expect(
              summaries.find((task) => task.scheduledTaskId === schedule.id)?.webhookUrl !==
                undefined,
            ).toBe(index === taskIndex);
        }
        const recoveredResources = yield* restoreAuthority(b);
        expect((yield* recoveredResources.forThread(child))?.id).toBe(both?.id);
        const recoveredCredential = yield* b.registry.issue({
          threadId: child,
          providerInstanceId: instanceId,
        });
        // Reissued native intake reads that persisted owner and must check the
        // actual other target before immediate metadata commit on every request.
        const resourceObservations = [];
        for (const exclusive of exclusiveTargets) {
          const forbiddenTitle = `Forbidden composite rename ${exclusive}`;
          const operation = yield* nativeCall(
            b,
            child,
            "t3_thread_update",
            {
              threadId: exclusive,
              action: "rename",
              title: forbiddenTitle,
            },
            undefined,
            recoveredCredential,
          );
          resourceObservations.push({
            denied: operation.isError === true,
            committed: (yield* b.threads.getThreadShell(exclusive))?.title === forbiddenTitle,
          });
        }
        expect(resourceObservations).toEqual([
          { denied: true, committed: false },
          { denied: true, committed: false },
        ]);
        // One narrow owner must still fence the target when the other is broad.
        // Neither source order nor a matching copied linkOrigin changes this intersection.
        for (const broadSource of [a, d]) {
          for (const grant of widerGrants)
            yield* b.authority.setGrant({
              ...grant,
              resourceScope:
                grant.environmentId === broadSource.environmentId
                  ? "environment-resources"
                  : "created-work",
            });
          for (const [index, exclusive] of exclusiveTargets.entries()) {
            const allowed = [a, d][index] !== broadSource;
            const title = `Mixed ${broadSource.environmentId} ${exclusive}`;
            for (let repeat = 0; repeat < 2; repeat++) {
              const operation = yield* nativeCall(b, child, "t3_thread_update", {
                threadId: exclusive,
                action: "rename",
                title,
              });
              expect(operation.isError !== true, json(operation.content)).toBe(allowed);
              expect((yield* b.threads.getThreadShell(exclusive))?.title === title).toBe(allowed);
            }
          }
        }
        for (const grant of widerGrants) yield* b.authority.setGrant(grant);
        for (const exclusive of exclusiveTargets) {
          const title = `Both explicitly broad ${exclusive}`;
          const permitted = yield* nativeCall(b, child, "t3_thread_update", {
            threadId: exclusive,
            action: "rename",
            title,
          });
          expect(permitted.isError, json(permitted.content)).not.toBe(true);
          expect((yield* b.threads.getThreadShell(exclusive))?.title).toBe(title);
        }
        // Adding an accepted input to an already jointly created child produces
        // an owned-thread context. Its birth and current run owners both constrain targets.
        yield* activeProvider(b, child);
        const extra = yield* nativeCall(
          a,
          ThreadId.make(`multi-${a.environmentId}-source`),
          "t3_thread_send",
          {
            environmentId: b.environmentId,
            threadId: child,
            message: "Additional accepted child input",
            mode: "steer",
            clientRequestId: "owned-child-input",
          },
        );
        expect(extra.isError, json(extra.content)).not.toBe(true);
        expect((yield* b.authority.forThread(child))?.context?.type).toBe("owned-thread");
        for (const grant of widerGrants)
          yield* b.authority.setGrant({ ...grant, resourceScope: "created-work" });
        for (const exclusive of exclusiveTargets) {
          const title = `Forbidden owned-context ${exclusive}`;
          const operation = yield* nativeCall(b, child, "t3_thread_update", {
            threadId: exclusive,
            action: "rename",
            title,
          });
          expect(operation.isError, json(operation.content)).toBe(true);
          expect((yield* b.threads.getThreadShell(exclusive))?.title).not.toBe(title);
          const read = yield* nativeCall(b, child, "t3_thread_read", { threadId: exclusive });
          expect(read.isError, json(read.content)).not.toBe(true);
        }
        const jointGrandchildCall = yield* nativeCall(b, child, "create_threads", {
          threads: [{ prompt: "Joint creation through owned context" }],
        });
        expect(jointGrandchildCall.isError, json(jointGrandchildCall.content)).not.toBe(true);
        const grandchild = (yield* decodeCreatedThreads(jointGrandchildCall.structuredContent))
          .threads[0]!.threadId;
        yield* activate(b, grandchild);
        for (const source of [a, d]) {
          const legitimate = yield* nativeCall(
            source,
            ThreadId.make(`multi-${source.environmentId}-source`),
            "t3_thread_update",
            {
              environmentId: b.environmentId,
              threadId: grandchild,
              action: "rename",
              title: `Grandchild under ${source.environmentId}`,
            },
          );
          expect(legitimate.isError, json(legitimate.content)).not.toBe(true);
        }
        for (const grant of widerGrants) yield* b.authority.setGrant(grant);
        const onwardCall = yield* nativeCall(b, target, "t3_thread_launch", {
          environmentId: c.environmentId,
          projectId: c.project.id,
          modelSelection: selection,
          title: "Multiple contributors onward",
          message: "Both contributor constraints onward",
          clientRequestId: "multi-onward",
        });
        expect(onwardCall.isError, json(onwardCall.content)).not.toBe(true);
        const onward = (yield* decodeLaunchReceipt(onwardCall.structuredContent)).threadId;
        const onwardOwner = yield* c.authority.checkThread(onward);
        expect(onwardOwner?.ceiling.capabilities).not.toContain("preview");
        expect(onwardOwner?.ceiling.capabilities).not.toContain("device");
        const restored = yield* restoreAuthority(b);
        expect((yield* restored.forThread(target))?.id).toBe(both?.id);
        const now = yield* DateTime.now;
        const recoveryEffects = [];
        // Both supported continuation facts preserve the complete immutable input set.
        for (const kind of [
          "restartContinuationOfRunId",
          "forkPrismContinuationSourceRunId",
        ] as const) {
          const id = RunId.make(`${target}-${kind}`);
          const effectId = `effect:${id}`;
          yield* b.sink.writeWithEffects({
            events: [
              {
                id: EventId.make(`event:${id}`),
                type: "run.created",
                threadId: target,
                runId: id,
                occurredAt: now,
                providerInstanceId: instanceId,
                payload: {
                  ...run,
                  id,
                  ordinal: run.ordinal + (kind === "restartContinuationOfRunId" ? 1 : 2),
                  [kind]: run.id,
                  status: "queued",
                  queuePosition: 1,
                },
              },
            ],
            effects: [
              {
                id: effectId,
                commandId: CommandId.make(`command:${id}`),
                threadId: target,
                request: { type: "provider-turn.start", runId: id },
              },
            ],
          });
          yield* b.authority.checkEffect(effectId, target);
          const inherited = yield* sql<{
            record_id: string;
          }>`SELECT record_id FROM peer_run_authority WHERE run_id=${id}`;
          expect(inherited[0]?.record_id).toBe(both?.id);
          recoveryEffects.push(effectId);
        }
        // Unowned local recovery never adopts a different ambient caller.
        const localRun = (yield* b.threads.getThreadRecords(unrelated, ["runs"])).runs[0]!;
        const localId = RunId.make(`${target}-local-continuation`);
        yield* b.sink
          .writeWithEffects({
            events: [
              {
                id: EventId.make(`event:${localId}`),
                type: "run.created",
                threadId: unrelated,
                runId: localId,
                occurredAt: now,
                providerInstanceId: instanceId,
                payload: {
                  ...localRun,
                  id: localId,
                  ordinal: localRun.ordinal + 1,
                  restartContinuationOfRunId: localRun.id,
                  status: "queued",
                  queuePosition: 1,
                },
              },
            ],
            effects: [
              {
                id: `effect:${localId}`,
                commandId: CommandId.make(`command:${localId}`),
                threadId: unrelated,
                request: { type: "provider-turn.start", runId: localId },
              },
            ],
          })
          .pipe(Effect.provideService(PeerDispatchAuthority, both?.id));
        expect(
          yield* sql`SELECT record_id FROM peer_run_authority WHERE run_id=${localId}`,
        ).toHaveLength(0);
        yield* sources[revoked === "first" ? 0 : 1]!.links.unlink(b.environmentId);
        expect((yield* Effect.result(b.authority.checkThread(target)))._tag).toBe("Failure");
        for (const effectId of recoveryEffects)
          expect((yield* Effect.result(b.authority.checkEffect(effectId, target)))._tag).toBe(
            "Failure",
          );
        expect((yield* Effect.result(b.authority.checkThread(child)))._tag).toBe("Failure");
        expect((yield* Effect.result(c.authority.checkThread(onward)))._tag).toBe("Failure");
        for (const contextChild of contextChildren)
          expect((yield* Effect.result(b.authority.checkThread(contextChild)))._tag).toBe(
            "Failure",
          );
        yield* b.authority.checkEffect(`effect:${localId}`, unrelated);
        const forbidden = yield* nativeCall(
          b,
          target,
          "t3_thread_send",
          {
            threadId: unrelated,
            message: "Forbidden multi-contributor mutation",
            mode: "queue",
            clientRequestId: "multi-denied",
          },
          undefined,
          cached,
        );
        expect(forbidden.isError).toBe(true);
        expect(
          (yield* b.threads.getThreadRecords(unrelated, ["messages"])).messages.some(
            (message) => message.text === "Forbidden multi-contributor mutation",
          ),
        ).toBe(false);
        expect(
          yield* sql`SELECT record_id FROM peer_thread_authority WHERE thread_id=${target}`,
        ).toHaveLength(0);
        // A genuinely independent local run does not inherit ended inputs.
        const finishedAt = yield* DateTime.now;
        const ended = (yield* b.threads.getThreadRecords(target, ["runs"])).runs;
        yield* b.sink.write({
          events: ended.map((endedRun) => ({
            id: EventId.make(`end:${endedRun.id}`),
            type: "run.updated" as const,
            threadId: target,
            runId: endedRun.id,
            occurredAt: finishedAt,
            providerInstanceId: instanceId,
            payload: {
              ...endedRun,
              status: "completed" as const,
              queuePosition: null,
              completedAt: finishedAt,
            },
          })),
        });
        yield* b.threads.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(`${target}-local-next`),
          threadId: target,
          messageId: MessageId.make(`${target}-local-next`),
          text: "Independent local run",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        yield* activate(b, target);
        expect(yield* b.authority.forThread(target)).toBeUndefined();
        const localAfter = yield* nativeCall(
          b,
          target,
          "t3_thread_send",
          {
            threadId: unrelated,
            message: "Permitted later local mutation",
            mode: "queue",
            clientRequestId: "multi-local-next",
          },
          undefined,
          cached,
        );
        expect(localAfter.isError, json(localAfter.content)).not.toBe(true);
        expect(
          (yield* b.threads.getThreadRecords(unrelated, ["messages"])).messages.some(
            (message) => message.text === "Permitted later local mutation",
          ),
        ).toBe(true);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("promoting a queued input retains its original contributor on the active run", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* makeNode(descriptorOf("reattach-b", "B", { peerAuthority: 1 }), "disk");
      const a = yield* makeNode(descriptorOf("reattach-a", "A", { peerAuthority: 1 }));
      yield* linkTo(a.links, b, "full-access");
      const parent = ThreadId.make("reattach-source"),
        target = ThreadId.make("reattach-target");
      yield* seedParent(a, parent);
      yield* seedParent(b, target);
      const active = yield* activeProvider(b, target);
      yield* a.forwarding.call(
        caller(a, parent),
        ProjectToolkit.tools.t3_project_list,
        b.environmentId,
        {},
      );
      const grant = (yield* b.authority.grants)[0]!;
      yield* b.authority.setGrant({
        ...grant,
        resourceScope: "environment-resources",
        preview: true,
        device: true,
      });
      const queued = yield* nativeCall(
        a,
        parent,
        "t3_thread_send",
        {
          environmentId: b.environmentId,
          threadId: target,
          message: "Move this accepted input",
          mode: "queue",
          clientRequestId: "reattach-queued",
        },
        new Set(["orchestration"]),
      );
      expect(queued.isError, json(queued.content)).not.toBe(true);
      const queuedRun = (yield* b.threads.getThreadRecords(target, ["runs"])).runs.find(
        (run) => run.status === "queued",
      )!;
      expect(yield* b.authority.forThread(target)).toBeUndefined();
      // Simulate the additive migration for an already accepted queued input.
      const sql = Context.get(b.database, SqlClient.SqlClient);
      yield* sql`DROP TABLE IF EXISTS peer_message_authority`;
      yield* restoreAuthority(b);
      yield* b.threads.dispatch({
        type: "queued-message.promote-to-steer",
        commandId: CommandId.make("reattach-promote"),
        threadId: target,
        queuedRunId: queuedRun.id,
        targetRunId: active.id,
        createdBy: "user",
        creationSource: "web",
      });
      const owner = yield* b.authority.checkThread(target);
      expect(owner).toBeDefined();
      expect(owner?.ceiling.capabilities).not.toContain("preview");
      const effects = yield* sql<{
        effect_id: string;
      }>`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE thread_id=${target} AND effect_type IN ('provider-turn.steer','provider-turn.restart')`;
      expect(effects).toHaveLength(1);
      yield* b.authority.checkEffect(effects[0]!.effect_id, target);
      const localTarget = ThreadId.make("reattach-local-target");
      yield* seedParent(b, localTarget);
      const localRun = yield* activeProvider(b, localTarget);
      yield* b.threads.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("reattach-local-queued"),
        threadId: localTarget,
        messageId: MessageId.make("reattach-local-queued"),
        text: "A genuinely local queued instruction",
        attachments: [],
        dispatchMode: { type: "queue_after_active" },
        createdBy: "user",
        creationSource: "web",
      });
      const localQueued = (yield* b.threads.getThreadRecords(localTarget, ["runs"])).runs.find(
        (run) => run.status === "queued",
      )!;
      // An unrelated authenticated promoter does not become this input's author.
      yield* b.threads
        .dispatch({
          type: "queued-message.promote-to-steer",
          commandId: CommandId.make("reattach-local-promote"),
          threadId: localTarget,
          queuedRunId: localQueued.id,
          targetRunId: localRun.id,
          createdBy: "user",
          creationSource: "web",
        })
        .pipe(Effect.provideService(PeerDispatchAuthority, owner?.id));
      expect(yield* b.authority.forThread(localTarget)).toBeUndefined();
      const localEffects = yield* sql<{
        effect_id: string;
      }>`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE thread_id=${localTarget} AND effect_type IN ('provider-turn.steer','provider-turn.restart')`;
      expect(localEffects).toHaveLength(1);
      yield* a.links.unlink(b.environmentId);
      yield* b.authority.checkEffect(localEffects[0]!.effect_id, localTarget);
      expect(
        (yield* Effect.result(b.authority.checkEffect(effects[0]!.effect_id, target)))._tag,
      ).toBe("Failure");
      const forbidden = yield* nativeCall(b, target, "t3_thread_send", {
        threadId: target,
        message: "Forbidden reattached mutation",
        mode: "queue",
        clientRequestId: "reattach-denied",
      });
      expect(forbidden.isError).toBe(true);
      expect(
        (yield* b.threads.getThreadRecords(target, ["messages"])).messages.some(
          (message) => message.text === "Forbidden reattached mutation",
        ),
      ).toBe(false);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("non-input effect persistence retains its caller without assigning the local run", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* makeNode(descriptorOf("stop-owner-b", "B", { peerAuthority: 1 }));
      const a = yield* makeNode(descriptorOf("stop-owner-a", "A", { peerAuthority: 1 }));
      yield* linkTo(a.links, b, "full-access");
      const parent = ThreadId.make("stop-owner-source"),
        target = ThreadId.make("stop-owner-local-target");
      yield* seedParent(a, parent);
      yield* seedParent(b, target);
      yield* activeProvider(b, target);
      const projection = yield* b.orchestrator.getThreadProjection(target);
      const scope = projection.checkpointScopes[0]!;
      expect(scope).toBeDefined();
      const root = projection.nodes.find((node) => node.id === projection.runs[0]!.rootNodeId)!;
      const at = yield* DateTime.now;
      yield* b.sink.write({
        events: [
          {
            id: EventId.make("stop-owner-checkpoint-root"),
            type: "node.updated",
            threadId: target,
            runId: root.runId!,
            occurredAt: at,
            payload: { ...root, checkpointScopeId: scope.id },
          },
        ],
      });
      yield* a.forwarding.call(
        caller(a, parent),
        ProjectToolkit.tools.t3_project_list,
        b.environmentId,
        {},
      );
      const grant = (yield* b.authority.grants)[0]!;
      yield* b.authority.setGrant({ ...grant, resourceScope: "environment-resources" });
      const stopped = yield* nativeCall(a, parent, "t3_thread_interrupt", {
        environmentId: b.environmentId,
        threadId: target,
        clientRequestId: "stop-owner-input",
      });
      expect(stopped.isError, json(stopped.content)).not.toBe(true);
      const sql = Context.get(b.database, SqlClient.SqlClient);
      const intake = yield* sql<{
        record_id: string;
      }>`SELECT owner.record_id FROM peer_effect_authority AS owner JOIN orchestration_v2_effect_outbox AS effect ON effect.effect_id=owner.effect_id WHERE effect.thread_id=${target} AND effect.effect_type='provider-turn.interrupt'`;
      expect(intake).toHaveLength(1);
      // Exercise the existing non-input effect hook with the actual accepted
      // caller. Provider execution/completion is an external fixture here.
      yield* b.sink
        .writeWithEffects({
          events: [],
          effects: [
            {
              id: "effect:non-input-capture",
              commandId: CommandId.make("non-input-capture"),
              threadId: target,
              request: {
                type: "checkpoint.capture",
                runId: projection.runs[0]!.id,
                scopeId: scope.id,
              },
            },
          ],
        })
        .pipe(Effect.provideService(PeerDispatchAuthority, intake[0]!.record_id));
      const captures = yield* sql<{
        effect_id: string;
      }>`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE thread_id=${target} AND effect_type='checkpoint.capture'`;
      expect(captures).toHaveLength(1);
      yield* b.authority.checkEffect(captures[0]!.effect_id, target);
      yield* a.links.unlink(b.environmentId);
      expect(
        (yield* Effect.result(b.authority.checkEffect(captures[0]!.effect_id, target)))._tag,
      ).toBe("Failure");
      expect(yield* sql`SELECT record_id FROM peer_run_authority`).toHaveLength(0);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("legacy creation and a modern run both constrain every actual target", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* makeNode(descriptorOf("r4-legacy-b", "B", { peerAuthority: 1 }), "disk");
      const a = yield* makeNode(descriptorOf("r4-legacy-a", "A", { peerAuthority: 1 }));
      const d = yield* makeNode(descriptorOf("r4-legacy-d", "D", { peerAuthority: 1 }));
      const c = yield* makeNode(descriptorOf("r5-legacy-c", "C", { peerAuthority: 1 }));
      yield* linkTo(b.links, c, "full-access");
      const parents = [ThreadId.make("r4-legacy-a-parent"), ThreadId.make("r4-legacy-d-parent")];
      for (const [index, source] of [a, d].entries()) {
        yield* linkTo(source.links, b, "full-access");
        yield* seedParent(source, parents[index]!);
        yield* source.forwarding.call(
          caller(source, parents[index]!),
          ProjectToolkit.tools.t3_project_list,
          b.environmentId,
          {},
        );
      }
      const grants = yield* b.authority.grants;
      for (const grant of grants)
        yield* b.authority.setGrant({ ...grant, resourceScope: "environment-resources" });
      const dGrant = grants.find((grant) => grant.environmentId === d.environmentId)!;
      const legacyThread = ThreadId.make("r4-legacy-existing");
      yield* seedParent(b, legacyThread, { sessionId: dGrant.sessionId, label: "D legacy" });
      yield* b.authority.legacyCeiling(legacyThread);
      yield* activeProvider(b, legacyThread);
      const exclusiveTargets = [];
      for (const [index, source] of [a, d].entries()) {
        const launched = yield* nativeCall(source, parents[index]!, "t3_thread_launch", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          modelSelection: selection,
          title: `Legacy test exclusive ${index}`,
          message: "Not executed",
          clientRequestId: `r4-legacy-exclusive-${index}`,
        });
        expect(launched.isError, json(launched.content)).not.toBe(true);
        exclusiveTargets.push((yield* decodeLaunchReceipt(launched.structuredContent)).threadId);
      }
      const input = yield* nativeCall(a, parents[0]!, "t3_thread_send", {
        environmentId: b.environmentId,
        threadId: legacyThread,
        message: "Modern accepted contributor",
        mode: "steer",
        clientRequestId: "r4-legacy-modern-input",
      });
      expect(input.isError, json(input.content)).not.toBe(true);
      expect((yield* b.authority.forThread(legacyThread))?.context?.type).toBe("legacy-thread");
      const cached = yield* b.registry.issue({
        threadId: legacyThread,
        providerInstanceId: instanceId,
      });
      for (const [index, target] of exclusiveTargets.entries()) {
        const title = `Legacy cross-target ${index}`;
        const operation = yield* nativeCall(
          b,
          legacyThread,
          "t3_thread_update",
          {
            threadId: target,
            action: "rename",
            title,
          },
          undefined,
          cached,
        );
        expect(operation.isError !== true, json(operation.content)).toBe(index === 1);
        expect((yield* b.threads.getThreadShell(target))?.title === title).toBe(index === 1);
      }
      // A new child records both real creators, without changing the old thread's owner.
      const created = yield* nativeCall(b, legacyThread, "create_threads", {
        threads: [{ prompt: "Legacy and modern joint child" }],
      });
      expect(created.isError, json(created.content)).not.toBe(true);
      const child = (yield* decodeCreatedThreads(created.structuredContent)).threads[0]!.threadId;
      yield* activate(b, child);
      const childCredential = yield* b.registry.issue({
        threadId: child,
        providerInstanceId: instanceId,
        capabilities: new Set(["orchestration", "preview", "device"]),
      });
      expect(childCredential.config.capabilities?.has("preview")).toBe(false);
      expect(childCredential.config.capabilities?.has("device")).toBe(false);

      for (const [index, source] of [a, d].entries()) {
        const renamed = yield* nativeCall(source, parents[index]!, "t3_thread_update", {
          environmentId: b.environmentId,
          threadId: child,
          action: "rename",
          title: `Joint legacy child ${index}`,
        });
        expect(renamed.isError, json(renamed.content)).not.toBe(true);
      }
      const modernGrant = grants.find((grant) => grant.environmentId === a.environmentId)!;
      yield* b.authority.setGrant({ ...modernGrant, resourceScope: "created-work" });
      const creator = yield* nativeCall(a, parents[0]!, "t3_thread_update", {
        environmentId: b.environmentId,
        threadId: child,
        action: "rename",
        title: "Created under both legacy and modern owners",
      });
      expect(creator.isError, json(creator.content)).not.toBe(true);
      expect((yield* b.threads.getThreadShell(child))?.title).toBe(
        "Created under both legacy and modern owners",
      );

      for (const grant of grants)
        yield* b.authority.setGrant({ ...grant, resourceScope: "created-work" });
      for (const [index, source] of [a, d].entries()) {
        const direct = yield* nativeCall(source, parents[index]!, "t3_thread_update", {
          environmentId: b.environmentId,
          threadId: child,
          action: "rename",
          title: `Legacy joint creator ${index} narrow`,
        });
        expect(direct.isError, json(direct.content)).not.toBe(true);
      }
      for (const credential of [childCredential, undefined]) {
        const selfTitle = credential ? "Legacy joint cached self" : "Legacy joint reissued self";
        const self = yield* nativeCall(
          b,
          child,
          "t3_thread_update",
          { threadId: child, action: "rename", title: selfTitle },
          undefined,
          credential,
        );
        expect(self.isError, json(self.content)).not.toBe(true);
        expect((yield* b.threads.getThreadShell(child))?.title).toBe(selfTitle);
      }
      const grandchildren = yield* nativeCall(b, child, "create_threads", {
        threads: [{ prompt: "Legacy nested narrow child" }],
      });
      expect(grandchildren.isError, json(grandchildren.content)).not.toBe(true);
      const grandchild = (yield* decodeCreatedThreads(grandchildren.structuredContent)).threads[0]!
        .threadId;
      yield* activate(b, grandchild);
      const nested = yield* nativeCall(b, grandchild, "t3_thread_update", {
        threadId: grandchild,
        action: "rename",
        title: "Legacy narrow grandchild self",
      });
      expect(nested.isError, json(nested.content)).not.toBe(true);
      expect((yield* b.threads.getThreadShell(grandchild))?.title).toBe(
        "Legacy narrow grandchild self",
      );
      for (const exclusive of exclusiveTargets) {
        const deniedTitle = `Forbidden legacy child exclusive ${exclusive}`;
        const other = yield* nativeCall(b, child, "t3_thread_update", {
          threadId: exclusive,
          action: "rename",
          title: deniedTitle,
        });
        expect(other.isError).toBe(true);
        expect((yield* b.threads.getThreadShell(exclusive))?.title).not.toBe(deniedTitle);
      }
      expect((yield* b.authority.checkThread(child))?.ceiling.onward).toBe(false);
      const beforeOnward = yield* c.threads.listProjectThreads({
        projectId: c.project.id,
        includeSubagents: true,
      });
      const deniedOnward = yield* nativeCall(b, grandchild, "t3_thread_launch", {
        environmentId: c.environmentId,
        projectId: c.project.id,
        modelSelection: selection,
        title: "Forbidden legacy onward",
        message: "Not executed",
        clientRequestId: "r5-legacy-onward",
      });
      expect(deniedOnward.isError).toBe(true);
      expect(json(deniedOnward.content)).toContain("onward");
      expect(
        yield* c.threads.listProjectThreads({ projectId: c.project.id, includeSubagents: true }),
      ).toEqual(beforeOnward);
      const sql = Context.get(b.database, SqlClient.SqlClient);
      const effects = yield* sql<{
        effect_id: string;
      }>`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE thread_id=${grandchild} AND effect_type='provider-turn.start'`;
      expect(effects).toHaveLength(1);
      yield* b.authority.checkEffect(effects[0]!.effect_id, grandchild);

      const title = "Forbidden narrowed legacy run";
      const narrowed = yield* nativeCall(
        b,
        legacyThread,
        "t3_thread_update",
        {
          threadId: exclusiveTargets[1],
          action: "rename",
          title,
        },
        undefined,
        cached,
      );
      expect(narrowed.isError).toBe(true);
      expect((yield* b.threads.getThreadShell(exclusiveTargets[1]!))?.title).not.toBe(title);
      const restored = yield* restoreAuthority(b);
      expect((yield* restored.forThread(legacyThread))?.context?.type).toBe("legacy-thread");
      yield* b.authority.setGrant({ ...modernGrant, resourceScope: "environment-resources" });
      yield* b.auth.revokeSession(dGrant.sessionId);
      const recovered = yield* Effect.result(
        restored.checkTarget(caller(b, legacyThread), {
          type: "thread",
          threadId: exclusiveTargets[1]!,
        }),
      );
      expect(recovered._tag).toBe("Failure");
      if (recovered._tag === "Failure") expect(recovered.failure.code).toBe("capability_denied");
      const revoked = yield* nativeCall(b, legacyThread, "t3_thread_update", {
        threadId: exclusiveTargets[1],
        action: "rename",
        title: "Forbidden revoked legacy owner",
      });
      expect(revoked.isError).toBe(true);
      expect((yield* b.threads.getThreadShell(exclusiveTargets[1]!))?.title).not.toBe(
        "Forbidden revoked legacy owner",
      );
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("legacy direct full access keeps unstamped schedule operations and the thread fence", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* makeNode(descriptorOf("r4-direct-b", "B", { peerAuthority: 1 }));
      const a = yield* makeNode(descriptorOf("r4-direct-a", "A", { peerAuthority: 1 }));
      yield* linkTo(a.links, b, "full-access");
      const parent = ThreadId.make("r4-direct-parent");
      const unrelated = ThreadId.make("r4-direct-unrelated");
      yield* seedParent(a, parent);
      yield* seedParent(b, unrelated);
      yield* a.forwarding.call(
        caller(a, parent),
        ProjectToolkit.tools.t3_project_list,
        b.environmentId,
        {},
      );
      // Reuse this isolated OAuth session through its real HTTP authenticator,
      // without modern envelopes, to exercise the separately preserved legacy policy.
      const authorizationHeader = (yield* Ref.get(b.bearers)).at(-1)!;
      const native = yield* b.registry.issue({
        threadId: unrelated,
        providerInstanceId: instanceId,
      });
      const credential = {
        ...native,
        config: { ...native.config, endpoint: `${b.url}/mcp`, authorizationHeader },
      };
      const fixture = yield* b.scheduled.upsert({
        title: "Unstamped legacy fixture",
        prompt: "Not executed",
        enabled: false,
        schedule: { type: "interval", everyMs: 60000 },
        projectId: b.project.id,
        threadId: null,
        workspaceStrategy: { type: "root" },
        modelSelection: selection,
        runtimeMode: "approval-required",
        interactionMode: "plan",
        createdBy: "user",
        creationSource: "web",
      });
      const updated = yield* nativeCall(
        b,
        unrelated,
        "update_scheduled_task",
        {
          scheduledTaskId: fixture.task.id,
          title: "Direct legacy updated",
        },
        undefined,
        credential,
      );
      expect(updated.isError, json(updated.content)).not.toBe(true);
      expect(
        (yield* b.scheduled.list()).tasks.find((task) => task.id === fixture.task.id)?.title,
      ).toBe("Direct legacy updated");
      const removed = yield* nativeCall(
        b,
        unrelated,
        "delete_scheduled_task",
        { scheduledTaskId: fixture.task.id },
        undefined,
        credential,
      );
      expect(removed.isError, json(removed.content)).not.toBe(true);
      expect((yield* b.scheduled.list()).tasks.some((task) => task.id === fixture.task.id)).toBe(
        false,
      );
      const forbidden = yield* nativeCall(
        b,
        unrelated,
        "t3_thread_update",
        {
          threadId: unrelated,
          action: "rename",
          title: "Forbidden legacy direct takeover",
        },
        undefined,
        credential,
      );
      expect(forbidden.isError).toBe(true);
      expect((yield* b.threads.getThreadShell(unrelated))?.title).not.toBe(
        "Forbidden legacy direct takeover",
      );
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live.each([{ retired: "source-run" }, { retired: "link" }] as const)(
  "round5: newly joint-created work remains usable after both creators narrow, $retired retirement",
  ({ retired }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* makeNode(descriptorOf("r5-b", "B", { peerAuthority: 1 }));
        const a = yield* makeNode(descriptorOf("r5-a", "A", { peerAuthority: 1 }));
        const d = yield* makeNode(descriptorOf("r5-d", "D", { peerAuthority: 1 }));
        const c = yield* makeNode(descriptorOf("r5-c", "C", { peerAuthority: 1 }));
        yield* linkTo(b.links, c, "full-access");
        const parents = [ThreadId.make("r5-a-source"), ThreadId.make("r5-d-source")];
        for (const [i, source] of [a, d].entries()) {
          yield* linkTo(source.links, b, "full-access");
          yield* seedParent(source, parents[i]!);
          yield* source.forwarding.call(
            caller(source, parents[i]!),
            ProjectToolkit.tools.t3_project_list,
            b.environmentId,
            {},
          );
        }
        const grants = yield* b.authority.grants;
        for (const grant of grants)
          yield* b.authority.setGrant({
            ...grant,
            resourceScope: "environment-resources",
            onward: true,
          });
        const launch = yield* nativeCall(a, parents[0]!, "t3_thread_launch", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          modelSelection: selection,
          title: "A-only parent",
          message: "Work",
          clientRequestId: "r5-parent",
        });
        expect(launch.isError, json(launch.content)).not.toBe(true);
        const target = (yield* decodeLaunchReceipt(launch.structuredContent)).threadId;
        yield* activate(b, target);
        yield* activeProvider(b, target);
        const steer = yield* nativeCall(d, parents[1]!, "t3_thread_send", {
          environmentId: b.environmentId,
          threadId: target,
          message: "D contributes",
          mode: "steer",
          clientRequestId: "r5-steer",
        });
        expect(steer.isError, json(steer.content)).not.toBe(true);
        expect((yield* b.authority.forThread(target))?.context?.type).toBe("owned-thread");
        const dLaunch = yield* nativeCall(d, parents[1]!, "t3_thread_launch", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          modelSelection: selection,
          title: "D-only resource",
          message: "Not executed",
          clientRequestId: "r5-d-exclusive",
        });
        expect(dLaunch.isError, json(dLaunch.content)).not.toBe(true);
        const dExclusive = (yield* decodeLaunchReceipt(dLaunch.structuredContent)).threadId;
        const oldForward = yield* nativeCall(b, target, "t3_thread_launch", {
          environmentId: c.environmentId,
          projectId: c.project.id,
          modelSelection: selection,
          title: "Old exclusive source forward",
          message: "Not executed",
          clientRequestId: "r5-old-forward",
        });
        expect(oldForward.isError, json(oldForward.content)).not.toBe(true);
        const oldOnward = (yield* decodeLaunchReceipt(oldForward.structuredContent)).threadId;
        yield* activate(c, oldOnward);
        const creation = yield* nativeCall(b, target, "create_threads", {
          threads: [{ prompt: "Joint A/D child" }],
        });
        expect(creation.isError, json(creation.content)).not.toBe(true);
        const child = (yield* decodeCreatedThreads(creation.structuredContent)).threads[0]!
          .threadId;
        yield* activate(b, child);
        const cached = yield* b.registry.issue({ threadId: child, providerInstanceId: instanceId });
        for (const grant of grants)
          yield* b.authority.setGrant({ ...grant, resourceScope: "created-work", onward: true });
        // Both independent source checks prove permanent joint creation membership.
        for (const [i, source] of [a, d].entries()) {
          const result = yield* nativeCall(source, parents[i]!, "t3_thread_update", {
            environmentId: b.environmentId,
            threadId: child,
            action: "rename",
            title: `Creator ${i} can rename joint child`,
          });
          expect(result.isError, json(result.content)).not.toBe(true);
          expect((yield* b.threads.getThreadShell(child))?.title).toBe(
            `Creator ${i} can rename joint child`,
          );
        }
        const title = "Joint child self remains allowed";
        const result = yield* nativeCall(b, child, "t3_thread_update", {
          threadId: child,
          action: "rename",
          title,
        });
        const narrowObservation = {
          denied: result.isError === true,
          committed: (yield* b.threads.getThreadShell(child))?.title === title,
        };
        const reason = json(result.content);
        for (const grant of grants)
          yield* b.authority.setGrant({
            ...grant,
            resourceScope: "environment-resources",
            onward: true,
          });
        const broadTitle = "Same child succeeds when parent access restored";
        const broad = yield* nativeCall(b, child, "t3_thread_update", {
          threadId: child,
          action: "rename",
          title: broadTitle,
        });
        expect(broad.isError, json(broad.content)).not.toBe(true);
        expect((yield* b.threads.getThreadShell(child))?.title).toBe(broadTitle);
        expect(narrowObservation, reason).toEqual({ denied: false, committed: true });
        for (const grant of grants)
          yield* b.authority.setGrant({ ...grant, resourceScope: "created-work", onward: true });
        for (const credential of [cached, undefined]) {
          const selfTitle = credential ? "Owned cached narrow self" : "Owned reissued narrow self";
          const self = yield* nativeCall(
            b,
            child,
            "t3_thread_update",
            { threadId: child, action: "rename", title: selfTitle },
            undefined,
            credential,
          );
          expect(self.isError, json(self.content)).not.toBe(true);
          expect((yield* b.threads.getThreadShell(child))?.title).toBe(selfTitle);
        }
        for (const exclusive of [target, dExclusive]) {
          const forbiddenTitle = `Forbidden owned exclusive ${exclusive}`;
          const forbidden = yield* nativeCall(b, child, "t3_thread_update", {
            threadId: exclusive,
            action: "rename",
            title: forbiddenTitle,
          });
          expect(forbidden.isError).toBe(true);
          expect((yield* b.threads.getThreadShell(exclusive))?.title).not.toBe(forbiddenTitle);
        }
        const hostTitle = "Forbidden original host self";
        const host = yield* nativeCall(b, target, "t3_thread_update", {
          threadId: target,
          action: "rename",
          title: hostTitle,
        });
        expect(host.isError).toBe(true);
        expect((yield* b.threads.getThreadShell(target))?.title).not.toBe(hostTitle);
        const grandchildren = yield* nativeCall(b, child, "create_threads", {
          threads: [{ prompt: "Joint nested narrow child" }],
        });
        expect(grandchildren.isError, json(grandchildren.content)).not.toBe(true);
        const grandchild = (yield* decodeCreatedThreads(grandchildren.structuredContent))
          .threads[0]!.threadId;
        yield* activate(b, grandchild);
        const nested = yield* nativeCall(b, grandchild, "t3_thread_update", {
          threadId: grandchild,
          action: "rename",
          title: "Owned narrow grandchild self",
        });
        expect(nested.isError, json(nested.content)).not.toBe(true);
        expect((yield* b.threads.getThreadShell(grandchild))?.title).toBe(
          "Owned narrow grandchild self",
        );
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const effects = yield* sql<{
          effect_id: string;
        }>`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE thread_id=${grandchild} AND effect_type='provider-turn.start'`;
        expect(effects).toHaveLength(1);
        yield* b.authority.checkEffect(effects[0]!.effect_id, grandchild);
        const oldTitle = "Forbidden now-ineligible source host forward";
        const old = yield* nativeCall(c, oldOnward, "t3_thread_update", {
          threadId: oldOnward,
          action: "rename",
          title: oldTitle,
        });
        expect(old.isError).toBe(true);
        expect((yield* c.threads.getThreadShell(oldOnward))?.title).not.toBe(oldTitle);
        const onwardCall = yield* nativeCall(b, grandchild, "t3_thread_launch", {
          environmentId: c.environmentId,
          projectId: c.project.id,
          modelSelection: selection,
          title: "Narrow joint child onward",
          message: "Not executed",
          clientRequestId: "r5-child-onward",
        });
        expect(onwardCall.isError, json(onwardCall.content)).not.toBe(true);
        const onward = (yield* decodeLaunchReceipt(onwardCall.structuredContent)).threadId;
        yield* activate(c, onward);
        const onwardSelf = yield* nativeCall(c, onward, "t3_thread_update", {
          threadId: onward,
          action: "rename",
          title: "Narrow onward self",
        });
        expect(onwardSelf.isError, json(onwardSelf.content)).not.toBe(true);
        expect((yield* c.threads.getThreadShell(onward))?.title).toBe("Narrow onward self");
        if (retired === "link") yield* d.links.unlink(b.environmentId);
        else {
          const source = (yield* a.threads.getThreadRecords(parents[0]!, ["runs"])).runs[0]!;
          const now = yield* DateTime.now;
          yield* a.sink.write({
            events: [
              {
                id: EventId.make("r5-source-retired"),
                type: "run.updated",
                threadId: parents[0]!,
                runId: source.id,
                providerInstanceId: instanceId,
                occurredAt: now,
                payload: { ...source, status: "completed", completedAt: now },
              },
            ],
          });
        }
        for (const [node, threadId] of [
          [b, child],
          [b, grandchild],
          [c, onward],
        ] as const) {
          const deniedTitle = `Forbidden retired ${retired} ${threadId}`;
          const denied = yield* nativeCall(node, threadId, "t3_thread_update", {
            threadId,
            action: "rename",
            title: deniedTitle,
          });
          expect(denied.isError).toBe(true);
          expect((yield* node.threads.getThreadShell(threadId))?.title).not.toBe(deniedTitle);
        }
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "round7: private webhook URLs retain the frozen runtime ceiling after destination mode increase",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* makeNode(descriptorOf("r7-webhook-b", "B", { peerAuthority: 1 }));
        const a = yield* makeNode(descriptorOf("r7-webhook-a", "A", { peerAuthority: 1 }));
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("r7-webhook-source");
        yield* seedParent(a, parent);
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("r7-source-auto"),
          threadId: parent,
          runtimeMode: "auto",
        });
        yield* a.forwarding.call(
          caller(a, parent),
          ProjectToolkit.tools.t3_project_list,
          b.environmentId,
          {},
        );
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, resourceScope: "environment-resources" });
        const launched = yield* nativeCall(a, parent, "t3_thread_launch", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          modelSelection: selection,
          title: "Frozen auto child",
          message: "Work",
          clientRequestId: "r7-webhook-child",
        });
        expect(launched.isError, json(launched.content)).not.toBe(true);
        const child = (yield* decodeLaunchReceipt(launched.structuredContent)).threadId;
        yield* activate(b, child);
        expect((yield* b.authority.checkThread(child))?.ceiling.runtimeMode).toBe("auto");
        const fixture = yield* b.scheduled.upsert({
          title: "Full access webhook",
          prompt: "Never executed",
          enabled: false,
          schedule: { type: "webhook" },
          projectId: b.project.id,
          threadId: null,
          workspaceStrategy: { type: "root" },
          modelSelection: selection,
          runtimeMode: "full-access",
          interactionMode: "default",
          createdBy: "user",
          creationSource: "web",
        });
        expect(fixture.task.webhook?.url).toBeTruthy();
        const list = () =>
          Effect.gen(function* () {
            const result = yield* nativeCall(b, child, "list_scheduled_tasks", {});
            expect(result.isError, json(result.content)).not.toBe(true);
            return (yield* decodeScheduledTaskList(result.structuredContent)).tasks.find(
              (t) => t.scheduledTaskId === fixture.task.id,
            );
          });
        const narrow = yield* list();
        expect(narrow).toBeDefined();
        expect(narrow?.webhookUrl).toBeUndefined();
        yield* b.threads.dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("r7-destination-full"),
          threadId: child,
          runtimeMode: "full-access",
        });
        expect((yield* b.authority.checkThread(child))?.ceiling.runtimeMode).toBe("auto");
        const elevated = yield* list();
        const write = yield* nativeCall(b, child, "update_scheduled_task", {
          scheduledTaskId: fixture.task.id,
          title: "Forbidden higher-mode edit",
        });
        const stored = (yield* b.scheduled.list()).tasks.find((t) => t.id === fixture.task.id);
        expect(write.isError).toBe(true);
        expect(stored?.title).toBe("Full access webhook");
        expect({
          frozenMode: (yield* b.authority.checkThread(child))?.ceiling.runtimeMode,
          metadataVisible: elevated !== undefined,
          privateUrlVisible: elevated?.webhookUrl !== undefined,
          writeDenied: write.isError === true,
        }).toEqual({
          frozenMode: "auto",
          metadataVisible: true,
          privateUrlVisible: false,
          writeDenied: true,
        });
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "round7: private webhook URLs retain current originating runtime after source narrowing",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* makeNode(descriptorOf("r7-webhook-b", "B", { peerAuthority: 1 }));
        const a = yield* makeNode(descriptorOf("r7-webhook-a", "A", { peerAuthority: 1 }));
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("r7-webhook-source");
        yield* seedParent(a, parent);
        yield* a.forwarding.call(
          caller(a, parent),
          ProjectToolkit.tools.t3_project_list,
          b.environmentId,
          {},
        );
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, resourceScope: "environment-resources" });
        const launched = yield* nativeCall(a, parent, "t3_thread_launch", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          modelSelection: selection,
          title: "Frozen auto child",
          message: "Work",
          clientRequestId: "r7-webhook-child",
        });
        expect(launched.isError, json(launched.content)).not.toBe(true);
        const child = (yield* decodeLaunchReceipt(launched.structuredContent)).threadId;
        yield* activate(b, child);
        expect((yield* b.authority.checkThread(child))?.ceiling.runtimeMode).toBe("full-access");
        const fixture = yield* b.scheduled.upsert({
          title: "Full access webhook",
          prompt: "Never executed",
          enabled: false,
          schedule: { type: "webhook" },
          projectId: b.project.id,
          threadId: null,
          workspaceStrategy: { type: "root" },
          modelSelection: selection,
          runtimeMode: "full-access",
          interactionMode: "default",
          createdBy: "user",
          creationSource: "web",
        });
        expect(fixture.task.webhook?.url).toBeTruthy();
        const list = () =>
          Effect.gen(function* () {
            const result = yield* nativeCall(b, child, "list_scheduled_tasks", {});
            expect(result.isError, json(result.content)).not.toBe(true);
            return (yield* decodeScheduledTaskList(result.structuredContent)).tasks.find(
              (t) => t.scheduledTaskId === fixture.task.id,
            );
          });
        const narrow = yield* list();
        expect(narrow).toBeDefined();
        expect(narrow?.webhookUrl).toBeTruthy();
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("r7-source-narrow"),
          threadId: parent,
          runtimeMode: "auto",
        });
        expect((yield* b.authority.checkThread(child))?.ceiling.runtimeMode).toBe("auto");
        const elevated = yield* list();
        const write = yield* nativeCall(b, child, "update_scheduled_task", {
          scheduledTaskId: fixture.task.id,
          title: "Forbidden higher-mode edit",
        });
        const stored = (yield* b.scheduled.list()).tasks.find((t) => t.id === fixture.task.id);
        expect(write.isError).toBe(true);
        expect(stored?.title).toBe("Full access webhook");
        expect({
          frozenMode: (yield* b.authority.checkThread(child))?.ceiling.runtimeMode,
          metadataVisible: elevated !== undefined,
          privateUrlVisible: elevated?.webhookUrl !== undefined,
          writeDenied: write.isError === true,
        }).toEqual({
          frozenMode: "auto",
          metadataVisible: true,
          privateUrlVisible: false,
          writeDenied: true,
        });
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("round8-cancel: native task cancellation must retain live originating mode ceilings", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* makeNode(descriptorOf("r8-cancel-b", "B", { peerAuthority: 1 }));
      const a = yield* makeNode(descriptorOf("r8-cancel-a", "A", { peerAuthority: 1 }));
      yield* linkTo(a.links, b, "full-access");
      const source = ThreadId.make("r8-cancel-source");
      yield* seedParent(a, source);
      const launched = yield* nativeCall(a, source, "t3_thread_launch", {
        environmentId: b.environmentId,
        projectId: b.project.id,
        modelSelection: selection,
        title: "Remote parent",
        message: "Work",
        clientRequestId: "r8-cancel-parent",
      });
      expect(launched.isError, json(launched.content)).not.toBe(true);
      const parent = (yield* decodeLaunchReceipt(launched.structuredContent)).threadId;
      yield* activate(b, parent);
      const delegated = yield* nativeCall(b, parent, "delegate_task", {
        task: "Local child",
        mode: "async",
        clientRequestId: "r8-cancel-child",
        target: { providerInstanceId: instanceId, model: "gpt-5.4" },
      });
      expect(delegated.isError, json(delegated.content)).not.toBe(true);
      const taskId = (yield* decodeTaskReceipt(delegated.structuredContent)).taskId;
      const task = (yield* b.threads.getThreadRecords(parent, ["subagents"])).subagents.find(
        (t) => t.id === taskId,
      )!;
      const child = task.childThreadId;
      if (child === null) return yield* Effect.die("Missing local delegated child");
      const runId = yield* activate(b, child);
      expect((yield* b.threads.getThreadShell(child))?.runtimeMode).toBe("full-access");
      yield* a.threads.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make("r8-cancel-source-auto"),
        threadId: source,
        runtimeMode: "auto",
      });
      expect((yield* b.authority.checkThread(parent))?.ceiling.runtimeMode).toBe("auto");
      const interrupt = yield* nativeCall(b, parent, "t3_thread_interrupt", {
        threadId: child,
        clientRequestId: "r8-cancel-interrupt-control",
      });
      expect(interrupt.isError).toBe(true);
      const before = (yield* b.threads.getThreadRecords(child, ["runs"])).runs.find(
        (r) => r.id === runId,
      )!;
      expect(before.status).toBe("running");
      const sql = Context.get(b.database, SqlClient.SqlClient);
      const effectsBefore =
        yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE thread_id=${child} ORDER BY effect_id`;
      const taskBefore = (yield* b.threads.getThreadRecords(parent, ["subagents"])).subagents.find(
        (t) => t.id === taskId,
      )!;
      const cancel = yield* nativeCall(b, parent, "task_cancel", {
        taskId,
        clientRequestId: "r8-cancel-effective",
      });
      const after = (yield* b.threads.getThreadRecords(child, ["runs"])).runs.find(
        (r) => r.id === runId,
      )!;
      expect(
        {
          effectiveMode: (yield* b.authority.checkThread(parent))?.ceiling.runtimeMode,
          interruptDenied: interrupt.isError === true,
          cancelDenied: cancel.isError === true,
          childStatus: after.status,
        },
        json(cancel),
      ).toEqual({
        effectiveMode: "auto",
        interruptDenied: true,
        cancelDenied: true,
        childStatus: "running",
      });
      expect(
        yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE thread_id=${child} ORDER BY effect_id`,
      ).toEqual(effectsBefore);
      const taskAfter = (yield* b.threads.getThreadRecords(parent, ["subagents"])).subagents.find(
        (t) => t.id === taskId,
      )!;
      expect(taskAfter.completionDelivery).toEqual(taskBefore.completionDelivery);
      yield* a.threads.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make("r8-cancel-source-restored"),
        threadId: source,
        runtimeMode: "full-access",
      });
      const permitted = yield* nativeCall(b, parent, "task_cancel", {
        taskId,
        clientRequestId: "r8-cancel-effective",
      });
      expect(permitted.isError, json(permitted)).not.toBe(true);
      expect(permitted.structuredContent).toMatchObject({ taskId, status: "cancel_requested" });
      expect(
        (yield* b.threads.getThreadRecords(child, ["runs"])).runs.find((r) => r.id === runId)
          ?.status,
      ).toBe("interrupted");
      expect(
        (yield* b.threads.getThreadRecords(parent, ["subagents"])).subagents.find(
          (t) => t.id === taskId,
        )?.completionDelivery?.state,
      ).toBe("disposed");
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "round8-cancel-plan: native task cancellation must retain live originating mode ceilings",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* makeNode(descriptorOf("r8-cancel-b", "B", { peerAuthority: 1 }));
        const a = yield* makeNode(descriptorOf("r8-cancel-a", "A", { peerAuthority: 1 }));
        yield* linkTo(a.links, b, "full-access");
        const source = ThreadId.make("r8-cancel-source");
        yield* seedParent(a, source);
        const launched = yield* nativeCall(a, source, "t3_thread_launch", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          modelSelection: selection,
          title: "Remote parent",
          message: "Work",
          clientRequestId: "r8-cancel-parent",
        });
        expect(launched.isError, json(launched.content)).not.toBe(true);
        const parent = (yield* decodeLaunchReceipt(launched.structuredContent)).threadId;
        yield* activate(b, parent);
        const delegated = yield* nativeCall(b, parent, "delegate_task", {
          task: "Local child",
          mode: "async",
          clientRequestId: "r8-cancel-child",
          target: { providerInstanceId: instanceId, model: "gpt-5.4" },
        });
        expect(delegated.isError, json(delegated.content)).not.toBe(true);
        const taskId = (yield* decodeTaskReceipt(delegated.structuredContent)).taskId;
        const task = (yield* b.threads.getThreadRecords(parent, ["subagents"])).subagents.find(
          (t) => t.id === taskId,
        )!;
        const child = task.childThreadId;
        if (child === null) return yield* Effect.die("Missing local delegated child");
        const runId = yield* activate(b, child);
        expect((yield* b.threads.getThreadShell(child))?.runtimeMode).toBe("full-access");
        yield* a.threads.dispatch({
          type: "thread.interaction-mode.set",
          commandId: CommandId.make("r8-cancel-source-auto"),
          threadId: source,
          interactionMode: "plan",
        });
        expect((yield* b.authority.checkThread(parent))?.ceiling.interactionMode).toBe("plan");
        const interrupt = yield* nativeCall(b, parent, "t3_thread_interrupt", {
          threadId: child,
          clientRequestId: "r8-cancel-interrupt-control",
        });
        expect(interrupt.isError).toBe(true);
        const before = (yield* b.threads.getThreadRecords(child, ["runs"])).runs.find(
          (r) => r.id === runId,
        )!;
        expect(before.status).toBe("running");
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const effectsBefore =
          yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE thread_id=${child} ORDER BY effect_id`;
        const taskBefore = (yield* b.threads.getThreadRecords(parent, [
          "subagents",
        ])).subagents.find((t) => t.id === taskId)!;
        const cancel = yield* nativeCall(b, parent, "task_cancel", {
          taskId,
          clientRequestId: "r8-cancel-effective",
        });
        const after = (yield* b.threads.getThreadRecords(child, ["runs"])).runs.find(
          (r) => r.id === runId,
        )!;
        expect(
          {
            effectiveMode: (yield* b.authority.checkThread(parent))?.ceiling.interactionMode,
            interruptDenied: interrupt.isError === true,
            cancelDenied: cancel.isError === true,
            childStatus: after.status,
          },
          json(cancel),
        ).toEqual({
          effectiveMode: "plan",
          interruptDenied: true,
          cancelDenied: true,
          childStatus: "running",
        });
        expect(
          yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE thread_id=${child} ORDER BY effect_id`,
        ).toEqual(effectsBefore);
        const taskAfter = (yield* b.threads.getThreadRecords(parent, ["subagents"])).subagents.find(
          (t) => t.id === taskId,
        )!;
        expect(taskAfter.completionDelivery).toEqual(taskBefore.completionDelivery);
        yield* a.threads.dispatch({
          type: "thread.interaction-mode.set",
          commandId: CommandId.make("r8-cancel-source-restored"),
          threadId: source,
          interactionMode: "default",
        });
        const permitted = yield* nativeCall(b, parent, "task_cancel", {
          taskId,
          clientRequestId: "r8-cancel-effective",
        });
        expect(permitted.isError, json(permitted)).not.toBe(true);
        expect(permitted.structuredContent).toMatchObject({ taskId, status: "cancel_requested" });
        expect(
          (yield* b.threads.getThreadRecords(child, ["runs"])).runs.find((r) => r.id === runId)
            ?.status,
        ).toBe("interrupted");
        expect(
          (yield* b.threads.getThreadRecords(parent, ["subagents"])).subagents.find(
            (t) => t.id === taskId,
          )?.completionDelivery?.state,
        ).toBe("disposed");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

const finishLifecycleRun = (
  node: Node,
  childId: ThreadId,
  label: string,
  status: "completed" | "failed" = "completed",
  text = label,
) =>
  Effect.gen(function* () {
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

it.live(
  "lifecycle381: registered original plus two continuations retain exact results and replay after source restart",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* makeNode(descriptorOf("lifecycle-b", "B", { peerAuthority: 1 }), "disk");
        const a = yield* makeNode(descriptorOf("lifecycle-a", "A", { peerAuthority: 1 }), "disk");
        yield* linkTo(a.links, b, "full-access");
        const parentId = ThreadId.make("lifecycle-parent");
        yield* seedParent(a, parentId);
        const receipt = yield* nativeCall(a, parentId, "delegate_task", {
          task: "First result and two follow-ups",
          target: {
            environmentId: b.environmentId,
            projectId: b.project.id,
            providerInstanceId: instanceId,
            model: selection.model,
          },
          clientRequestId: "lifecycle381",
        });
        expect(receipt.isError, json(receipt.content)).not.toBe(true);
        const { taskId } = yield* decodeTaskReceipt(receipt.structuredContent);
        const childId = yield* attachedChild(a, parentId, taskId);
        const negotiatedLaunch = (yield* a.threads.getThreadRecords(parentId, [
          "subagents",
        ])).subagents.find((task) => task.id === taskId)!.remoteChild!.launch!;
        expect(negotiatedLaunch.lifecycleVersion).toBe(1);

        const terminal = (label: string, status: "completed" | "failed" = "completed") =>
          finishLifecycleRun(b, childId, label, status);
        const observed = (label: string) =>
          a.orchestrator
            .streamStoredEventsFrom({
              threadId: parentId,
              afterSequence: 0,
              eventType: "subagent.updated",
            })
            .pipe(
              Stream.filter(
                (stored) =>
                  stored.event.type === "subagent.updated" &&
                  stored.event.payload.id === taskId &&
                  stored.event.payload.remoteChild?.latestResult?.text === label,
              ),
              Stream.runHead,
              Effect.flatMap(Effect.fromOption),
            );
        const original = yield* terminal("original381");
        yield* a.orchestrator
          .streamStoredEventsFrom({
            threadId: parentId,
            afterSequence: 0,
            eventType: "subagent.updated",
          })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "subagent.updated" &&
                stored.event.payload.id === taskId &&
                stored.event.payload.result === "original381",
            ),
            Stream.runHead,
            Effect.flatMap(Effect.fromOption),
          );
        const first = yield* nativeCall(a, parentId, "task_status", { taskId });
        expect(first.structuredContent).toMatchObject({
          childThreadId: null,
          summary: "original381",
          latestTerminalRunId: original,
          latestTerminalSummary: "original381",
        });
        const send1 = yield* nativeCall(a, parentId, "t3_thread_send", {
          environmentId: b.environmentId,
          threadId: childId,
          message: "Continue once",
          clientRequestId: "continue381-1",
        });
        expect(send1.isError, json(send1.content)).not.toBe(true);
        yield* Ref.set(a.dropAckResponses, 1);
        const second = yield* terminal("second381");
        yield* observed("second381");
        const secondStatus = yield* nativeCall(a, parentId, "task_status", { taskId });
        expect(secondStatus.structuredContent).toMatchObject({
          summary: "original381",
          latestTerminalRunId: second,
          latestTerminalSummary: "second381",
        });
        const send2 = yield* nativeCall(a, parentId, "t3_thread_send", {
          environmentId: b.environmentId,
          threadId: childId,
          message: "Continue twice",
          clientRequestId: "continue381-2",
        });
        expect(send2.isError, json(send2.content)).not.toBe(true);
        yield* Deferred.await(a.ackResponseLost);
        yield* a.stopConnections;
        const third = yield* terminal("third381", "failed");
        const reconnected = yield* a.reconnect();
        const resumed = Context.get(reconnected.services, RemoteDelegation.RemoteDelegation);
        yield* resumed.start();
        yield* observed("third381");
        const status = yield* nativeCall(a, parentId, "task_status", { taskId });
        expect(status.structuredContent).toMatchObject({
          childThreadId: null,
          summary: "original381",
          latestTerminalRunId: third,
          latestTerminalSummary: "third381",
          latestTerminalStatus: "failed",
          remoteObservation: { originalRunId: original, availability: "current" },
        });
        yield* b.threads.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("monitor-only381"),
          threadId: childId,
          messageId: MessageId.make("monitor-message381"),
          text: "Monitor bookkeeping",
          attachments: [],
          notification: {
            source: { kind: "monitor" },
            outcome: "updated",
            summary: "Monitor bookkeeping",
          },
          dispatchMode: { type: "queue_after_active" },
          createdBy: "agent",
          creationSource: "server",
        });
        const monitorRun = yield* finishLifecycleRun(b, childId, "monitor381");
        const monitor = (yield* b.threads.getThreadRecords(childId, ["runs"])).runs.find(
          (run) => run.id === monitorRun,
        )!;
        const monitorTime = yield* DateTime.now;
        yield* b.sink.write({
          events: [
            {
              id: EventId.make("monitor-checkpoint-rewrite381"),
              type: "run.updated",
              threadId: childId,
              runId: monitorRun,
              occurredAt: monitorTime,
              payload: { ...monitor, completedAt: monitorTime },
            },
          ],
        });
        const throughMonitor = yield* b.threads.getThreadEventSequence(childId);
        yield* a.orchestrator
          .streamStoredEventsFrom({
            threadId: parentId,
            afterSequence: 0,
            eventType: "subagent.updated",
          })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "subagent.updated" &&
                stored.event.payload.id === taskId &&
                (stored.event.payload.remoteChild?.cursor ?? 0) >= throughMonitor,
            ),
            Stream.runHead,
          );
        const afterMonitor = (yield* a.threads.getThreadRecords(parentId, [
          "subagents",
        ])).subagents.find((task) => task.id === taskId)!;
        expect(afterMonitor.remoteChild?.launch).toEqual(negotiatedLaunch);
        expect(afterMonitor.remoteChild?.latestResult?.runId).toBe(third);
        expect(afterMonitor.remoteChild?.latestResult?.status).toBe("failed");
        expect(afterMonitor.remoteChild?.snapshot?.queuedRuns).toBe(0);
        const view = yield* resumed.view(parentId, taskId, 0);
        expect(view.environmentId).toBe(b.environmentId);
        expect(
          view.messages
            .filter((message) => message.role === "assistant")
            .map((message) => message.text),
        ).toEqual(["original381", "second381", "third381", "monitor381"]);
        expect(yield* a.orchestrator.getThreadShell(childId)).toBe(null);
        const control = reconnected.peers.taskLifecycle!;
        const pair = {
          parentEnvironmentId: a.environmentId,
          parentThreadId: parentId,
          taskId,
          childEnvironmentId: b.environmentId,
          childThreadId: childId,
          childRunId: null,
        };
        const generation = afterMonitor.remoteChild!.launch!.linkIdentity;
        const beforeText = yield* control(b.environmentId, generation, {
          pair,
          operation: { type: "snapshot", after: 0, wait: false },
        });
        if (beforeText.type !== "snapshot") return yield* Effect.die("Expected lifecycle snapshot");
        const originalMessage = (yield* b.threads.getThreadRecords(childId, ["messages"]))
          .messages[0]!;
        const textTime = yield* DateTime.now;
        yield* b.sink.write({
          events: [
            {
              id: EventId.make("text-only381"),
              type: "message.updated",
              threadId: childId,
              occurredAt: textTime,
              payload: {
                ...originalMessage,
                id: MessageId.make("text-only381"),
                runId: null,
                role: "system",
                text: "Stream-only update",
                createdAt: textTime,
                updatedAt: textTime,
              },
            },
          ],
        });
        const afterText = yield* control(b.environmentId, generation, {
          pair,
          operation: {
            type: "observe",
            after: afterMonitor.remoteChild!.cursor!,
            afterSnapshot: beforeText.snapshot.sequence,
            wait: false,
          },
        });
        expect(afterText).toMatchObject({
          type: "observation",
          observation: {
            cursor: afterMonitor.remoteChild!.cursor!,
            snapshot: { sequence: beforeText.snapshot.sequence },
          },
        });
        if (afterText.type !== "observation") return yield* Effect.die("Expected observation");
        expect(afterText.observation.result).toBeUndefined();
        const lateMessage = (yield* b.threads.getThreadRecords(childId, [
          "messages",
        ])).messages.find((message) => message.runId === original && message.role === "assistant")!;
        const lateTime = yield* DateTime.now;
        yield* b.sink.write({
          events: [
            {
              id: EventId.make("late-original-message381"),
              type: "message.updated",
              threadId: childId,
              runId: original,
              occurredAt: lateTime,
              payload: { ...lateMessage, text: "Late changed original text", updatedAt: lateTime },
            },
          ],
        });
        const originalIdentity = afterMonitor.remoteChild!.originalResult!;
        const originalPage = yield* control(b.environmentId, generation, {
          pair,
          operation: {
            type: "resultText",
            runId: originalIdentity.runId,
            eventId: originalIdentity.eventId,
            sequence: originalIdentity.sequence,
            textOffset: 0,
          },
        });
        expect(originalPage).toEqual({
          type: "resultText",
          text: "original381",
          nextTextOffset: null,
        });
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.live(
  "lifecycle381: stop keeps the checked generation across relink and retains actual mode policy",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* makeNode(descriptorOf("stop381-b", "B", { peerAuthority: 1 }));
        const a = yield* makeNode(descriptorOf("stop381-a", "A", { peerAuthority: 1 }));
        yield* linkTo(a.links, b, "full-access");
        const source = ThreadId.make("stop381-source");
        const target = ThreadId.make("stop381-existing-target");
        yield* seedParent(a, source);
        yield* seedParent(b, target);
        yield* a.forwarding.readThread(caller(a, source), b.environmentId, {
          threadId: target,
          view: "messages",
        });
        const grant = (yield* b.authority.grants).find(
          (grant) => grant.environmentId === a.environmentId,
        )!;
        yield* b.authority.setGrant({ ...grant, resourceScope: "environment-resources" });
        const checked = yield* a.links.get(b.environmentId).pipe(Effect.flatMap(Effect.fromOption));
        const priorGrants = new Set((yield* b.authority.grants).map((grant) => grant.sessionId));
        // The local caller checked the old link. Replacement happens before forward resolves it.
        yield* a.links.unlink(b.environmentId);
        yield* linkTo(a.links, b, "full-access");
        const replacement = yield* a.links
          .get(b.environmentId)
          .pipe(Effect.flatMap(Effect.fromOption));
        expect(replacement.launchIdentity).not.toBe(checked.launchIdentity);
        yield* a.forwarding.readThread(caller(a, source), b.environmentId, {
          threadId: target,
          view: "messages",
        });
        const nextGrant = (yield* b.authority.grants).find(
          (grant) => !priorGrants.has(grant.sessionId),
        )!;
        yield* b.authority.setGrant({ ...nextGrant, resourceScope: "environment-resources" });
        const before = yield* b.threads.getThreadRecords(target, ["runs"]);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const beforeEffects = yield* sql<{
          effect_id: string;
        }>`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE thread_id = ${target} ORDER BY effect_id`;
        const denied = yield* a.forwarding
          .interruptThread(
            caller(a, source),
            b.environmentId,
            { threadId: target, stop: true, clientRequestId: "generation-bound-stop381" },
            { expectedLinkIdentity: checked.launchIdentity },
          )
          .pipe(Effect.result);
        expect(denied._tag).toBe("Failure");
        const afterDenied = yield* b.threads.getThreadRecords(target, ["runs"]);
        expect(afterDenied.runs).toEqual(before.runs);
        expect(
          yield* sql<{
            effect_id: string;
          }>`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE thread_id = ${target} ORDER BY effect_id`,
        ).toEqual(beforeEffects);
        // Positive control: the same current grant and actual target can stop lawfully.
        yield* a.forwarding.interruptThread(
          caller(a, source),
          b.environmentId,
          { threadId: target, stop: true, clientRequestId: "generation-bound-stop381" },
          { expectedLinkIdentity: replacement.launchIdentity },
        );
        expect(
          (yield* b.threads.getThreadRecords(target, ["runs"])).runs.some(
            (run) => run.status === "running",
          ),
        ).toBe(false);
        const higher = ThreadId.make("stop381-higher-mode-target");
        yield* seedParent(b, higher);
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("stop381-lower-source"),
          threadId: source,
          runtimeMode: "auto",
        });
        const modeDenied = yield* a.forwarding
          .interruptThread(
            caller(a, source),
            b.environmentId,
            { threadId: higher, stop: true, clientRequestId: "mode-stop381" },
            { expectedLinkIdentity: replacement.launchIdentity },
          )
          .pipe(Effect.result);
        expect(modeDenied._tag).toBe("Failure");
        expect(
          (yield* b.threads.getThreadRecords(higher, ["runs"])).runs.some(
            (run) => run.status === "running",
          ),
        ).toBe(true);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.live(
  "lifecycle381: nested and queued work, persisted recovery and destination reader restart retain ordered truth",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* makeNode(
          descriptorOf("lifecycle-state-b", "B", { peerAuthority: 1 }),
          "disk",
        );
        const a = yield* makeNode(
          descriptorOf("lifecycle-state-a", "A", { peerAuthority: 1 }),
          "disk",
        );
        yield* linkTo(a.links, b, "full-access");
        const parentId = ThreadId.make("lifecycle-state-parent");
        yield* seedParent(a, parentId);
        const receipt = yield* nativeCall(a, parentId, "delegate_task", {
          task: "Lifecycle state",
          target: {
            environmentId: b.environmentId,
            projectId: b.project.id,
            providerInstanceId: instanceId,
            model: selection.model,
          },
          clientRequestId: "state381",
        });
        expect(receipt.isError, json(receipt.content)).not.toBe(true);
        const { taskId } = yield* decodeTaskReceipt(receipt.structuredContent);
        const childId = yield* attachedChild(a, parentId, taskId);
        const firstRun = yield* finishLifecycleRun(b, childId, "state-first");
        yield* a.orchestrator
          .streamStoredEventsFrom({
            threadId: parentId,
            afterSequence: 0,
            eventType: "subagent.updated",
          })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "subagent.updated" &&
                stored.event.payload.id === taskId &&
                stored.event.payload.remoteChild?.latestResult?.runId === firstRun,
            ),
            Stream.runHead,
          );
        const original = (yield* a.threads.getThreadRecords(parentId, [
          "subagents",
        ])).subagents.find((task) => task.id === taskId)!;
        yield* nativeCall(a, parentId, "task_status", { taskId });
        yield* nativeCall(a, parentId, "t3_thread_send", {
          environmentId: b.environmentId,
          threadId: childId,
          message: "Continue with nested work",
          clientRequestId: "state-followup",
        });
        const continuation = yield* activate(b, childId);
        const nestedReceipt = yield* nativeCall(b, childId, "delegate_task", {
          task: "Nested work",
          target: { providerInstanceId: instanceId, model: selection.model },
          clientRequestId: "state-nested",
        });
        expect(nestedReceipt.isError, json(nestedReceipt.content)).not.toBe(true);
        const { taskId: nestedTask } = yield* decodeTaskReceipt(nestedReceipt.structuredContent);
        const nested = (yield* b.threads.getThreadRecords(childId, ["subagents"])).subagents.find(
          (task) => task.id === nestedTask,
        )!;
        expect(nested.childThreadId).not.toBe(null);
        yield* activate(b, nested.childThreadId!);
        yield* nativeCall(a, parentId, "t3_thread_send", {
          environmentId: b.environmentId,
          threadId: childId,
          message: "Queued continuation",
          mode: "queue",
          clientRequestId: "state-queued",
        });
        const liveView = yield* a.remote.view(parentId, taskId, 0);
        expect(liveView.snapshot.pendingChildren).toBeGreaterThan(0);
        expect(liveView.snapshot.queuedRuns).toBeGreaterThan(0);
        expect(liveView.snapshot.executionSettled).toBe(false);
        expect(liveView.children).toContainEqual(
          expect.objectContaining({
            taskId: nestedTask,
            environmentId: b.environmentId,
            threadId: nested.childThreadId,
          }),
        );
        const nestedView = yield* a.remote.view(parentId, taskId, 0, undefined, {
          environmentId: b.environmentId,
          threadId: nested.childThreadId!,
        });
        expect(nestedView.threadId).toBe(nested.childThreadId);
        expect(nestedView.messages.some((message) => message.text === "Nested work")).toBe(true);
        const unrelated = ThreadId.make("unrelated-local-resource");
        yield* seedParent(b, unrelated);
        const refusedView = yield* a.remote
          .view(parentId, taskId, 0, undefined, {
            environmentId: b.environmentId,
            threadId: unrelated,
          })
          .pipe(Effect.result);
        expect(refusedView._tag).toBe("Failure");
        expect(yield* a.orchestrator.getThreadShell(nested.childThreadId!)).toBe(null);
        yield* a.stopConnections;
        const reconnected = yield* a.reconnect();
        const control = reconnected.peers.taskLifecycle!;
        const pair = {
          parentEnvironmentId: a.environmentId,
          parentThreadId: parentId,
          taskId,
          childEnvironmentId: b.environmentId,
          childThreadId: childId,
          childRunId: null,
        };
        const generation = original.remoteChild!.launch!.linkIdentity;
        const after = original.remoteChild!.cursor!;
        const sql = Context.get(b.database, SqlClient.SqlClient);
        yield* RecoveryHistory.writeRecoveryOutcome({
          sourceRunId: continuation,
          threadId: childId,
          status: "pending",
          reason: "decision",
        }).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
          Effect.provideService(RecoverySignals.RecoveryWaitSignals, b.signals),
        );
        // External provider completion uses the persisted run selected above, while another run remains queued.
        const records = yield* b.threads.getThreadRecords(childId, ["runs"]);
        const run = records.runs.find((run) => run.id === continuation)!;
        const now = yield* DateTime.now;
        yield* b.sink.write({
          events: [
            {
              id: EventId.make("state-failure"),
              type: "run.updated",
              threadId: childId,
              runId: continuation,
              occurredAt: now,
              payload: { ...run, status: "failed", completedAt: now },
            },
          ],
        });
        const pending = yield* control(b.environmentId, generation, {
          pair,
          operation: { type: "observe", after, wait: false },
        });
        expect(pending).toMatchObject({
          type: "observation",
          observation: { snapshot: { recoveryWaiting: true, executionSettled: false } },
        });
        if (pending.type !== "observation") return yield* Effect.die("Expected observation");
        expect(pending.observation.result).toBeUndefined();
        const originalGrant = (yield* b.authority.grants)[0];
        expect(originalGrant).toBeDefined();
        const beforeSessions = yield* b.linkedSessions;
        expect(
          beforeSessions.some((session) => session.sessionId === originalGrant?.sessionId),
        ).toBe(true);
        // Reconstruct the destination reader on a separately opened connection to the same on-disk database.
        const reopened = yield* b.reopenDatabase;
        const restoredRuntime = yield* Runtime.layerWithRegistry(
          { name: "lifecycle-destination-restored" },
          Adapters.layerFromAdapters([adapter]),
          { databaseLayer: Layer.succeedContext(reopened), runEffectWorker: false },
        ).pipe(Layer.fresh, Layer.build);
        const reader = yield* PeerTaskLifecycle.layer.pipe(
          Layer.provide(
            Layer.mergeAll(
              Layer.succeedContext(b.services),
              Layer.succeedContext(restoredRuntime),
              Layer.succeedContext(reopened),
              Layer.succeed(RecoveryWaitState, {
                ...b.lifecycleRecovery,
                read: (_threadId, runId) =>
                  RecoveryHistory.readRecoveryState(runId).pipe(
                    Effect.provide(Layer.succeedContext(reopened)),
                  ),
              }),
            ),
          ),
          Layer.fresh,
          Layer.build,
        );
        const session = (yield* b.linkedSessions).find(
          (session) => session.sessionId === originalGrant?.sessionId,
        );
        if (session === undefined)
          return yield* Effect.die(
            "Original authenticated peer session disappeared after reader reconstruction",
          );
        expect(yield* b.authority.grants).toContainEqual(originalGrant);
        const persistedSession = yield* Context.get(reopened, SqlClient.SqlClient)<{
          expires_at: string;
          revoked_at: string | null;
        }>`SELECT expires_at,revoked_at FROM auth_sessions WHERE session_id=${session.sessionId}`;
        expect(persistedSession[0]).toEqual({
          expires_at: "9999-12-31T23:59:59.999Z",
          revoked_at: null,
        });
        const peerScope = {
          ...caller(b, childId),
          thread: undefined,
          client: {
            sessionId: session.sessionId,
            label: "A",
            access: "full-access" as const,
            linked: true,
          },
        };
        const restored = Context.get(reader, PeerTaskLifecycle.PeerTaskLifecycle);
        const request = {
          environmentId: a.environmentId,
          generation,
          pair,
          operation: { type: "observe" as const, after, wait: false },
        };
        const replayPending = yield* restored.execute(peerScope, request);
        expect(replayPending).toMatchObject({
          type: "observation",
          observation: { snapshot: { recoveryWaiting: true } },
        });
        // A decision-only commit wakes readers without requiring another run event.
        yield* RecoveryHistory.writeRecoveryOutcome({
          sourceRunId: continuation,
          threadId: childId,
          status: "decided",
          outcome: "not_retryable",
          reason: "non_retryable",
        }).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
          Effect.provideService(RecoverySignals.RecoveryWaitSignals, b.signals),
        );
        const recovered = yield* restored.execute(peerScope, request);
        expect(recovered).toMatchObject({
          type: "observation",
          observation: {
            result: { runId: continuation, status: "failed" },
            snapshot: { recoveryWaiting: false },
          },
        });
        const replay = yield* restored.execute(peerScope, request);
        if (replay.type !== "observation") return yield* Effect.die("Expected replay");
        if (recovered.type !== "observation") return yield* Effect.die("Expected result");
        expect(replay.observation.result).toEqual(recovered.observation.result);
        expect(replay.observation.cursor).toBe(recovered.observation.cursor);
        if (recovered.type !== "observation") return yield* Effect.die("Expected observation");
        yield* restored.execute(peerScope, {
          ...request,
          operation: { type: "acknowledge", through: recovered.observation.cursor },
        });
        const ack = yield* sql<{
          cursor: number;
        }>`SELECT cursor FROM peer_task_observation_ack WHERE task_id=${taskId}`;
        expect(ack[0]?.cursor).toBe(recovered.observation.cursor);
        const forged = yield* control(b.environmentId, generation, {
          pair: { ...pair, childThreadId: nested.childThreadId! },
          operation: { type: "view", after: 0 },
        }).pipe(Effect.result);
        expect(forged._tag).toBe("Failure");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.live(
  "lifecycle381: parent Stop after the original reaches later nested work and a late result cannot wake it",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* makeNode(descriptorOf("lifecycle-stop-b", "B", { peerAuthority: 1 }));
        const a = yield* makeNode(descriptorOf("lifecycle-stop-a", "A", { peerAuthority: 1 }));
        yield* linkTo(a.links, b, "full-access");
        const parentId = ThreadId.make("lifecycle-stop-parent");
        yield* seedParent(a, parentId);
        const receipt = yield* nativeCall(a, parentId, "delegate_task", {
          task: "Original and later nested work",
          target: {
            environmentId: b.environmentId,
            projectId: b.project.id,
            providerInstanceId: instanceId,
            model: selection.model,
          },
          clientRequestId: "stop-after-original381",
        });
        expect(receipt.isError, json(receipt.content)).not.toBe(true);
        const { taskId } = yield* decodeTaskReceipt(receipt.structuredContent);
        const childId = yield* attachedChild(a, parentId, taskId);
        const original = yield* finishLifecycleRun(b, childId, "stop-original-result");
        const observed = (
          predicate: (task: import("@t3tools/contracts").OrchestrationV2Subagent) => boolean,
        ) =>
          a.orchestrator
            .streamStoredEventsFrom({
              threadId: parentId,
              afterSequence: 0,
              eventType: "subagent.updated",
            })
            .pipe(
              Stream.filter(
                (stored) =>
                  stored.event.type === "subagent.updated" &&
                  stored.event.payload.id === taskId &&
                  predicate(stored.event.payload),
              ),
              Stream.runHead,
              Effect.flatMap(Effect.fromOption),
            );
        yield* observed((task) => task.remoteChild?.latestResult?.runId === original);
        yield* nativeCall(a, parentId, "task_status", { taskId });
        const follow = yield* nativeCall(a, parentId, "t3_thread_send", {
          environmentId: b.environmentId,
          threadId: childId,
          message: "Continue this job",
          clientRequestId: "stop-followup381",
        });
        expect(follow.isError, json(follow.content)).not.toBe(true);
        const laterRun = yield* activate(b, childId);
        const nestedReceipt = yield* nativeCall(b, childId, "delegate_task", {
          task: "Later nested work",
          target: { providerInstanceId: instanceId, model: selection.model },
          clientRequestId: "stop-nested381",
        });
        expect(nestedReceipt.isError, json(nestedReceipt.content)).not.toBe(true);
        const { taskId: nestedId } = yield* decodeTaskReceipt(nestedReceipt.structuredContent);
        const nested = (yield* b.threads.getThreadRecords(childId, ["subagents"])).subagents.find(
          (task) => task.id === nestedId,
        )!;
        const nestedRun = yield* activate(b, nested.childThreadId!);
        yield* a.threads.dispatch({
          type: "thread.stop",
          threadId: parentId,
          commandId: CommandId.make("actual-parent-stop381"),
        });
        yield* a.threads.stopDelegatedTasks({
          threadId: parentId,
          commandId: CommandId.make("actual-parent-stop381"),
        });
        yield* observed((task) => task.remoteChild?.stop?.state === "acknowledged").pipe(
          Effect.raceFirst(
            observed((task) => task.remoteChild?.stop?.state === "unconfirmed").pipe(
              Effect.flatMap(() =>
                Effect.die(
                  new Error(
                    "The real parent Stop was denied after the parent run ended; the later destination run remains active.",
                  ),
                ),
              ),
            ),
          ),
        );
        yield* drainStops(b);
        expect(
          (yield* b.threads.getThreadRecords(childId, ["runs"])).runs.find(
            (run) => run.id === laterRun,
          )?.status,
        ).toBe("interrupted");
        expect(
          (yield* b.threads.getThreadRecords(nested.childThreadId!, ["runs"])).runs.find(
            (run) => run.id === nestedRun,
          )?.status,
        ).toBe("interrupted");
        yield* observed((task) => task.remoteChild?.latestResult?.runId === laterRun);
        const stopped = yield* a.threads.getThreadRecords(parentId, ["subagents", "messages"]);
        expect(stopped.subagents.find((task) => task.id === taskId)).toMatchObject({
          result: "stop-original-result",
          remoteChild: { runId: original, stop: { state: "acknowledged" } },
          completionDelivery: { state: "disposed" },
        });
        const notices = stopped.messages
          .filter((message) => message.notification?.source.kind === "delegated_task")
          .map((message) => message.id);
        const run = (yield* b.threads.getThreadRecords(childId, ["runs"])).runs.find(
          (run) => run.id === laterRun,
        )!;
        const now = yield* DateTime.now;
        const eventId = EventId.make("late-real-terminal381");
        yield* b.sink.write({
          events: [
            {
              id: eventId,
              type: "run.updated",
              threadId: childId,
              runId: laterRun,
              occurredAt: now,
              payload: { ...run, status: "completed", completedAt: now },
            },
          ],
        });
        yield* observed((task) => task.remoteChild?.latestResult?.eventId === eventId);
        const late = yield* a.threads.getThreadRecords(parentId, ["subagents", "messages"]);
        expect(late.subagents.find((task) => task.id === taskId)).toMatchObject({
          result: "stop-original-result",
          remoteChild: {
            runId: original,
            latestResult: { runId: laterRun, status: "completed", eventId },
          },
          completionDelivery: { state: "disposed" },
        });
        expect(
          late.messages
            .filter((message) => message.notification?.source.kind === "delegated_task")
            .map((message) => message.id),
        ).toEqual(notices);
        expect(yield* a.orchestrator.getThreadShell(childId)).toBe(null);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.live(
  "lifecycle381: preserves a whole long original result across bounded result and client message pages",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* makeNode(descriptorOf("lifecycle-long-b", "B", { peerAuthority: 1 }));
        const a = yield* makeNode(descriptorOf("lifecycle-long-a", "A", { peerAuthority: 1 }));
        yield* linkTo(a.links, b, "full-access");
        const parentId = ThreadId.make("lifecycle-long-parent");
        yield* seedParent(a, parentId);
        const receipt = yield* nativeCall(a, parentId, "delegate_task", {
          task: "Long result",
          target: {
            environmentId: b.environmentId,
            projectId: b.project.id,
            providerInstanceId: instanceId,
            model: selection.model,
          },
          clientRequestId: "long381",
        });
        expect(receipt.isError, json(receipt.content)).not.toBe(true);
        const { taskId } = yield* decodeTaskReceipt(receipt.structuredContent);
        const childId = yield* attachedChild(a, parentId, taskId);
        const text = "begin381\n" + "x".repeat(120000) + "\nend381";
        const runId = yield* finishLifecycleRun(b, childId, "long381", "completed", text);
        yield* a.orchestrator
          .streamStoredEventsFrom({
            threadId: parentId,
            afterSequence: 0,
            eventType: "subagent.updated",
          })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "subagent.updated" &&
                stored.event.payload.id === taskId &&
                stored.event.payload.remoteChild?.latestResult?.runId === runId,
            ),
            Stream.runHead,
          );
        const task = (yield* a.threads.getThreadRecords(parentId, ["subagents"])).subagents.find(
          (task) => task.id === taskId,
        )!;
        expect(task.result === text).toBe(true);
        expect(task.remoteChild?.latestResult?.text === text).toBe(true);
        expect(task.remoteChild?.latestResult?.truncated).toBe(false);
        let after = 0;
        let offset = 0;
        let combined = "";
        for (let page = 0; page < 6; page++) {
          const view = yield* a.remote.view(parentId, taskId, after, offset);
          expect(
            view.messages.reduce((count, message) => count + message.text.length, 0),
          ).toBeLessThanOrEqual(50000);
          combined += view.messages
            .filter((message) => message.role === "assistant")
            .map((message) => message.text)
            .join("");
          if (view.nextCursor === null) break;
          after = view.nextCursor;
          offset = view.nextTextOffset ?? 0;
        }
        expect(combined === text).toBe(true);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.live(
  "lifecycle381: cancellation interrupts a held result continuation without publishing its partial first result",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* makeNode(descriptorOf("lifecycle-page-stop-b", "B", { peerAuthority: 1 }));
        const a = yield* makeNode(descriptorOf("lifecycle-page-stop-a", "A", { peerAuthority: 1 }));
        yield* linkTo(a.links, b, "full-access");
        const parentId = ThreadId.make("lifecycle-page-stop-parent");
        yield* seedParent(a, parentId);
        const receipt = yield* nativeCall(a, parentId, "delegate_task", {
          task: "Page cancellation",
          target: {
            environmentId: b.environmentId,
            projectId: b.project.id,
            providerInstanceId: instanceId,
            model: selection.model,
          },
          clientRequestId: "page-stop381",
        });
        expect(receipt.isError, json(receipt.content)).not.toBe(true);
        const { taskId } = yield* decodeTaskReceipt(receipt.structuredContent);
        const childId = yield* attachedChild(a, parentId, taskId);
        yield* Ref.set(a.holdResultPage, true);
        const runId = yield* finishLifecycleRun(
          b,
          childId,
          "page-stop381",
          "completed",
          "late381" + "x".repeat(60000),
        );
        yield* Deferred.await(a.resultPageStarted);
        const cancelled = yield* nativeCall(a, parentId, "task_cancel", {
          taskId,
          clientRequestId: "cancel-page381",
        });
        expect(cancelled.isError, json(cancelled.content)).not.toBe(true);
        expect(cancelled.structuredContent).toMatchObject({
          taskId,
          status: "cancel_requested",
          stopState: "pending",
        });
        const event = yield* a.orchestrator
          .streamStoredEventsFrom({
            threadId: parentId,
            afterSequence: 0,
            eventType: "subagent.updated",
          })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "subagent.updated" &&
                stored.event.payload.id === taskId &&
                stored.event.payload.remoteChild?.stop?.state === "acknowledged",
            ),
            Stream.runHead,
            Effect.flatMap(Effect.fromOption),
          );
        expect(event.event.type === "subagent.updated" && event.event.payload.result).toBe(
          "Cancelled.",
        );
        yield* Ref.set(a.holdResultPage, false);
        yield* Deferred.succeed(a.releaseResultPage, undefined);
        yield* a.orchestrator
          .streamStoredEventsFrom({
            threadId: parentId,
            afterSequence: 0,
            eventType: "subagent.updated",
          })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "subagent.updated" &&
                stored.event.payload.id === taskId &&
                stored.event.payload.remoteChild?.latestResult?.runId === runId,
            ),
            Stream.runHead,
          );
        const task = (yield* a.threads.getThreadRecords(parentId, ["subagents"])).subagents.find(
          (task) => task.id === taskId,
        )!;
        expect(task.result).toBe("Cancelled.");
        expect(task.completionDelivery?.state).toBe("disposed");
        expect(task.remoteChild?.latestResult?.text.length).toBe(60007);
        const drainWith = (failStop: boolean) =>
          EffectWorker.OrchestrationEffectWorkerV2.pipe(
            Effect.provide(
              EffectWorker.layerWithOptions({ maxAttempts: 1 }).pipe(
                Layer.provide(
                  Layer.succeed(EffectWorker.OrchestrationEffectExecutorV2, {
                    execute: (effect) =>
                      failStop && effect.request.type === "delegated-tasks.stop"
                        ? Effect.fail(
                            new EffectWorker.OrchestrationEffectExecutionError({
                              effectId: effect.id,
                              effectType: effect.request.type,
                              cause: "Fixture rejects native stop completion",
                            }),
                          )
                        : Effect.void,
                  }),
                ),
                Layer.provide(Layer.succeed(PeerAuthority.PeerAuthority, b.authority)),
                Layer.provide(Layer.succeedContext(b.runtime)),
              ),
            ),
          );
        const stoppedObservation = (
          predicate: (
            remote: NonNullable<
              import("@t3tools/contracts").OrchestrationV2Subagent["remoteChild"]
            >,
          ) => boolean,
        ) =>
          a.orchestrator
            .streamStoredEventsFrom({
              threadId: parentId,
              afterSequence: 0,
              eventType: "subagent.updated",
            })
            .pipe(
              Stream.filter(
                (stored) =>
                  stored.event.type === "subagent.updated" &&
                  stored.event.payload.id === taskId &&
                  stored.event.payload.remoteChild !== undefined &&
                  predicate(stored.event.payload.remoteChild),
              ),
              Stream.runHead,
              Effect.flatMap(Effect.fromOption),
            );
        yield* (yield* drainWith(true)).drain();
        const failed = yield* stoppedObservation(
          (remote) =>
            remote.stop?.state === "unconfirmed" &&
            remote.snapshot?.stopExecution?.state === "failed",
        );
        expect(
          failed.event.type === "subagent.updated" &&
            failed.event.payload.remoteChild?.stoppedThere,
        ).not.toBe(true);
        const failedCommand =
          failed.event.type === "subagent.updated"
            ? failed.event.payload.remoteChild?.stop?.destinationCommandId
            : undefined;
        expect(failedCommand).toBeDefined();
        const retried = yield* nativeCall(a, parentId, "task_cancel", {
          taskId,
          clientRequestId: "retry-native-stop381",
        });
        expect(retried.isError, json(retried.content)).not.toBe(true);
        yield* stoppedObservation(
          (remote) =>
            remote.stop?.state === "acknowledged" &&
            remote.stop.destinationCommandId !== undefined &&
            remote.stop.destinationCommandId !== failedCommand,
        );
        yield* (yield* drainWith(false)).drain();
        const confirmed = yield* stoppedObservation((remote) => remote.stop?.state === "confirmed");
        expect(confirmed.event.type === "subagent.updated" && confirmed.event.payload.result).toBe(
          "Cancelled.",
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.live(
  "lifecycle381: durable stopped-parent control denies forged intent and mode escalation and replays a lost Stop without stopping later work",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* makeNode(
          descriptorOf("stop-control-b", "B", { peerAuthority: 1 }),
          "disk",
        );
        const a = yield* makeNode(
          descriptorOf("stop-control-a", "A", { peerAuthority: 1 }),
          "disk",
        );
        yield* linkTo(a.links, b, "full-access");
        const parentId = ThreadId.make("stop-control-parent");
        yield* seedParent(a, parentId);
        const { taskId, childId } = yield* delegate(a, b, parentId, "stop-control-job");
        yield* activate(b, childId);
        yield* a.stopConnections;
        yield* a.threads.dispatch({
          type: "thread.stop",
          threadId: parentId,
          commandId: CommandId.make("stop-control-parent381"),
        });
        yield* a.threads.stopDelegatedTasks({
          threadId: parentId,
          commandId: CommandId.make("stop-control-parent381"),
        });
        yield* a.remote.requestStop(parentId, taskId, "client-stop:review-first381");
        const task = (yield* a.threads.getThreadRecords(parentId, ["subagents"])).subagents.find(
          (t) => t.id === taskId,
        )!;
        const remote = task.remoteChild!;
        const generation = remote.launch!.linkIdentity;
        const pair = {
          parentEnvironmentId: a.environmentId,
          parentThreadId: parentId,
          taskId,
          childEnvironmentId: b.environmentId,
          childThreadId: childId,
          childRunId: null,
        };
        const operation = { type: "stop" as const, requestId: remote.stop!.requestId };
        const sourceConflict = yield* a.remote
          .requestStop(parentId, taskId, operation.requestId, "changed semantics")
          .pipe(Effect.result);
        expect(sourceConflict._tag).toBe("Failure");
        expect(
          (yield* a.threads.getThreadRecords(parentId, ["subagents"])).subagents.find(
            (candidate) => candidate.id === taskId,
          )?.remoteChild?.stop,
        ).toEqual(remote.stop);
        let connected = yield* a.reconnect();
        const state = () => b.threads.getThreadEventSequence(childId);
        const before = yield* state();
        const initialOutbox = yield* Context.get(
          b.database,
          SqlClient.SqlClient,
        )`SELECT effect_id FROM orchestration_v2_effect_outbox ORDER BY effect_id`;
        const denied = (input: Parameters<NonNullable<typeof connected.peers.taskLifecycle>>[2]) =>
          connected.peers.taskLifecycle!(b.environmentId, generation, input).pipe(
            Effect.result,
            Effect.tap((result) => Effect.sync(() => expect(result._tag).toBe("Failure"))),
          );
        yield* denied({ pair, operation: { ...operation, requestId: "never-accepted381" } });
        yield* denied({ pair: { ...pair, taskId: NodeId.make("other-task381") }, operation });
        yield* denied({ pair: { ...pair, childThreadId: parentId }, operation });
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          threadId: parentId,
          commandId: CommandId.make("stop-control-auto"),
          runtimeMode: "auto",
        });
        yield* denied({ pair, operation });
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          threadId: parentId,
          commandId: CommandId.make("stop-control-full"),
          runtimeMode: "full-access",
        });
        yield* a.threads.dispatch({
          type: "thread.interaction-mode.set",
          threadId: parentId,
          commandId: CommandId.make("stop-control-plan"),
          interactionMode: "plan",
        });
        yield* denied({ pair, operation });
        yield* a.threads.dispatch({
          type: "thread.interaction-mode.set",
          threadId: parentId,
          commandId: CommandId.make("stop-control-default"),
          interactionMode: "default",
        });
        expect(yield* state()).toBe(before);
        expect(
          yield* Context.get(
            b.database,
            SqlClient.SqlClient,
          )`SELECT effect_id FROM orchestration_v2_effect_outbox ORDER BY effect_id`,
        ).toEqual(initialOutbox);
        const currentGrant = (yield* b.authority.grants)[0]!;
        const expiry = (yield* Context.get(b.database, SqlClient.SqlClient)<{
          expires_at: string;
        }>`SELECT expires_at FROM auth_sessions WHERE session_id=${currentGrant.sessionId}`)[0]!
          .expires_at;
        yield* Context.get(
          b.database,
          SqlClient.SqlClient,
        )`UPDATE auth_sessions SET expires_at='2020-01-01T00:00:00.000Z' WHERE session_id=${currentGrant.sessionId}`;
        yield* denied({ pair, operation });
        yield* Context.get(
          b.database,
          SqlClient.SqlClient,
        )`UPDATE auth_sessions SET expires_at=${expiry} WHERE session_id=${currentGrant.sessionId}`;
        expect(yield* state()).toBe(before);
        yield* b.threads.dispatch({
          type: "thread.runtime-mode.set",
          threadId: childId,
          commandId: CommandId.make("stop-positive-narrow-target381"),
          runtimeMode: "auto",
        });
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          threadId: parentId,
          commandId: CommandId.make("stop-positive-narrow-source381"),
          runtimeMode: "auto",
        });
        yield* Ref.set(a.dropAckResponses, 1);
        const lost = yield* connected.peers.taskLifecycle!(b.environmentId, generation, {
          pair,
          operation,
        }).pipe(Effect.result);
        expect(lost._tag).toBe("Failure");
        yield* Deferred.await(a.ackResponseLost);
        const committed = yield* state();
        expect(committed).toBeGreaterThan(before);
        yield* connected.stop;
        connected = yield* a.reconnect();
        const replay = yield* connected.peers.taskLifecycle!(b.environmentId, generation, {
          pair,
          operation,
        });
        expect(replay.type).toBe("stop");
        expect(yield* state()).toBe(committed);
        yield* denied({ pair, operation: { ...operation, reason: "changed semantics" } });
        const effects = yield* Context.get(b.database, SqlClient.SqlClient)<{
          effect_id: string;
          thread_id: string;
        }>`SELECT effect_id,thread_id FROM orchestration_v2_effect_outbox WHERE command_id=${replay.type === "stop" ? replay.commandId : "missing"}`;
        expect(effects.length).toBeGreaterThan(0);
        for (const effect of effects)
          yield* b.authority.checkEffect(effect.effect_id, ThreadId.make(effect.thread_id));
        const stopOwners = yield* Context.get(b.database, SqlClient.SqlClient)<{
          record_id: string;
        }>`SELECT owner.record_id FROM peer_effect_authority AS owner JOIN orchestration_v2_effect_outbox AS effect ON effect.effect_id=owner.effect_id WHERE effect.command_id=${replay.type === "stop" ? replay.commandId : "missing"} LIMIT 1`;
        expect(stopOwners).toHaveLength(1);
        const illegal = yield* b.threads
          .dispatch({
            type: "message.dispatch",
            threadId: childId,
            commandId: CommandId.make("stop-cannot-start381"),
            messageId: MessageId.make("stop-cannot-start381"),
            text: "Must never start",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "agent",
            creationSource: "mcp",
          })
          .pipe(
            Effect.provideService(PeerDispatchAuthority, stopOwners[0]!.record_id),
            Effect.exit,
          );
        expect(illegal._tag).toBe("Failure");
        expect(yield* state()).toBe(committed);
        yield* b.threads.dispatch({
          type: "thread.runtime-mode.set",
          threadId: childId,
          commandId: CommandId.make("raise-target-before-native-claim381"),
          runtimeMode: "full-access",
        });
        yield* drainStops(b);
        const deniedEffects = yield* Context.get(b.database, SqlClient.SqlClient)<{
          status: string;
        }>`SELECT status FROM orchestration_v2_effect_outbox WHERE command_id=${replay.type === "stop" ? replay.commandId : "missing"}`;
        expect(deniedEffects.length).toBeGreaterThan(0);
        expect(deniedEffects.every((effect) => effect.status === "failed")).toBe(true);
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          threadId: parentId,
          commandId: CommandId.make("restore-stop-native-claim381"),
          runtimeMode: "full-access",
        });
        // A fresh local input after the accepted Stop is independent later work.
        yield* b.threads.dispatch({
          type: "message.dispatch",
          threadId: childId,
          commandId: CommandId.make("independent-after-stop381"),
          messageId: MessageId.make("independent-after-stop381"),
          text: "Independent later work",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        const later = yield* activate(b, childId);
        const laterSequence = yield* state();
        const same = yield* connected.peers.taskLifecycle!(b.environmentId, generation, {
          pair,
          operation,
        });
        expect(same).toEqual(replay);
        expect(yield* state()).toBe(laterSequence);
        expect(
          (yield* b.threads.getThreadRecords(childId, ["runs"])).runs.find((r) => r.id === later)
            ?.status,
        ).toBe("running");
        // An old lost reply cannot replace a newer deliberate obligation.
        yield* Effect.all(
          [
            a.remote.requestStop(parentId, taskId, "client-stop:review-newer381", "later work"),
            a.remote.requestStop(parentId, taskId, operation.requestId),
            a.remote.requestStop(parentId, taskId, "client-stop:review-newer381", "later work"),
          ],
          { concurrency: "unbounded" },
        );
        const newer = (yield* a.threads.getThreadRecords(parentId, ["subagents"])).subagents.find(
          (candidate) => candidate.id === taskId,
        )!.remoteChild!.stop;
        yield* a.remote.requestStop(parentId, taskId, operation.requestId);
        expect(
          (yield* a.threads.getThreadRecords(parentId, ["subagents"])).subagents.find(
            (candidate) => candidate.id === taskId,
          )!.remoteChild!.stop,
        ).toEqual(newer);
        yield* a.threads.dispatch({
          type: "delegated_task.remote.observe",
          commandId: CommandId.make("serialized-old-replay381"),
          parentThreadId: parentId,
          taskId,
          stop: { requestId: operation.requestId, state: "pending" },
        });
        expect(
          (yield* a.threads.getThreadRecords(parentId, ["subagents"])).subagents.find(
            (candidate) => candidate.id === taskId,
          )!.remoteChild!.stop,
        ).toEqual(newer);
        const historicalConflict = yield* a.remote
          .requestStop(parentId, taskId, operation.requestId, "changed historical reason")
          .pipe(Effect.result);
        expect(historicalConflict._tag).toBe("Failure");
        yield* connected.stop;
        connected = yield* a.reconnect();
        yield* Context.get(connected.services, RemoteDelegation.RemoteDelegation).start();
        const stopEvent = yield* a.orchestrator
          .streamStoredEventsFrom({
            threadId: parentId,
            afterSequence: 0,
            eventType: "subagent.updated",
          })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "subagent.updated" &&
                stored.event.payload.id === taskId &&
                stored.event.payload.remoteChild?.stop?.requestId ===
                  "client-stop:review-newer381" &&
                stored.event.payload.remoteChild.stop.state === "acknowledged",
            ),
            Stream.runHead,
            Effect.flatMap(Effect.fromOption),
          );
        expect(
          stopEvent.event.type === "subagent.updated" &&
            stopEvent.event.payload.remoteChild?.stop?.destinationCommandId,
        ).not.toBe(replay.type === "stop" ? replay.commandId : undefined);
        yield* drainStops(b);
        expect(
          (yield* b.threads.getThreadRecords(childId, ["runs"])).runs.find(
            (run) => run.id === later,
          )?.status,
        ).toBe("interrupted");
        yield* a.orchestrator
          .streamStoredEventsFrom({
            threadId: parentId,
            afterSequence: 0,
            eventType: "subagent.updated",
          })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "subagent.updated" &&
                stored.event.payload.id === taskId &&
                stored.event.payload.remoteChild?.stop?.requestId ===
                  "client-stop:review-newer381" &&
                stored.event.payload.remoteChild.stop.state === "confirmed",
            ),
            Stream.runHead,
            Effect.flatMap(Effect.fromOption),
          );
        const pendingRows = yield* Context.get(a.database, SqlClient.SqlClient)<{
          count: number;
        }>`SELECT COUNT(*) AS count FROM orchestration_events WHERE stream_id=${parentId} AND event_type='subagent.updated' AND json_extract(payload_json,'$.id')=${taskId} AND json_extract(payload_json,'$.remoteChild.stop.requestId')='client-stop:review-newer381' AND json_extract(payload_json,'$.remoteChild.stop.state')='pending'`;
        expect(pendingRows[0]?.count).toBe(1);
        yield* connected.stop;
        yield* a.threads.dispatch({
          type: "delegated_task.remote.observe",
          commandId: CommandId.make("new-stop-obligation381"),
          parentThreadId: parentId,
          taskId,
          stop: { requestId: "new-deliberate-stop381", state: "pending" },
        });
        const afterNewStopSequence = yield* state();
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.auth.revokeSession(grant.sessionId);
        yield* denied({ pair, operation: { type: "stop", requestId: "new-deliberate-stop381" } });
        expect(yield* state()).toBe(afterNewStopSequence);
        yield* linkTo(a.links, b, "full-access");
        yield* denied({ pair, operation: { type: "stop", requestId: "new-deliberate-stop381" } });
        expect(yield* state()).toBe(afterNewStopSequence);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.live(
  "lifecycle381: native Stop recovery carries only each hop's accepted obligation from stopped A through B to C",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const c = yield* makeNode(descriptorOf("stop-chain-c", "C", { peerAuthority: 1 }), "disk");
        const b = yield* makeNode(descriptorOf("stop-chain-b", "B", { peerAuthority: 1 }), "disk");
        const a = yield* makeNode(descriptorOf("stop-chain-a", "A", { peerAuthority: 1 }), "disk");
        yield* linkTo(a.links, b, "full-access");
        yield* linkTo(b.links, c, "full-access");
        const parentId = ThreadId.make("stop-chain-parent");
        yield* seedParent(a, parentId);
        yield* a.forwarding.call(
          caller(a, parentId),
          ProjectToolkit.tools.t3_project_list,
          b.environmentId,
          {},
        );
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, onward: true });
        const first = yield* delegate(a, b, parentId, "stop-chain-a-b");
        const bRun = yield* activate(b, first.childId);
        const second = yield* delegate(b, c, first.childId, "stop-chain-b-c");
        const cRun = yield* activate(c, second.childId);
        const observed = (
          node: Node,
          parent: ThreadId,
          taskId: NodeId,
          state: string,
          requestId?: string,
        ) =>
          node.orchestrator
            .streamStoredEventsFrom({
              threadId: parent,
              afterSequence: 0,
              eventType: "subagent.updated",
            })
            .pipe(
              Stream.filter(
                (stored) =>
                  stored.event.type === "subagent.updated" &&
                  stored.event.payload.id === taskId &&
                  stored.event.payload.remoteChild?.stop?.state === state &&
                  (requestId === undefined ||
                    stored.event.payload.remoteChild.stop.requestId === requestId),
              ),
              Stream.runHead,
              Effect.flatMap(Effect.fromOption),
            );
        // A native caller on a raised B shell still retains A's live ceiling.
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          threadId: parentId,
          commandId: CommandId.make("stop-chain-narrow-origin"),
          runtimeMode: "auto",
        });
        const cBeforeNative = yield* c.threads.getThreadEventSequence(second.childId);
        const nativeCancel = yield* nativeCall(b, first.childId, "task_cancel", {
          taskId: second.taskId,
          clientRequestId: "stop-chain-native-ceiling",
        });
        expect(nativeCancel.isError, json(nativeCancel.content)).not.toBe(true);
        yield* observed(b, first.childId, second.taskId, "unconfirmed");
        expect(yield* c.threads.getThreadEventSequence(second.childId)).toBe(cBeforeNative);
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          threadId: parentId,
          commandId: CommandId.make("stop-chain-restore-origin"),
          runtimeMode: "full-access",
        });
        yield* b.threads.dispatch({
          type: "thread.runtime-mode.set",
          threadId: first.childId,
          commandId: CommandId.make("stop-chain-lower-b"),
          runtimeMode: "auto",
        });
        yield* a.threads.dispatch({
          type: "thread.stop",
          threadId: parentId,
          commandId: CommandId.make("stop-chain-a-first"),
        });
        yield* a.threads.stopDelegatedTasks({
          threadId: parentId,
          commandId: CommandId.make("stop-chain-a-first"),
        });
        yield* observed(a, parentId, first.taskId, "acknowledged");
        expect((yield* a.threads.getThreadShell(parentId))?.activeRunId).toBeNull();
        expect(
          (yield* b.threads.getThreadRecords(first.childId, ["runs"])).runs.find(
            (run) => run.id === bRun,
          )?.status,
        ).toBe("interrupted");
        // Before the native descendant effect executes, C is still running and A cannot confirm.
        expect(
          (yield* c.threads.getThreadRecords(second.childId, ["runs"])).runs.find(
            (run) => run.id === cRun,
          )?.status,
        ).toBe("running");
        yield* drainStops(b);
        const currentStop = (yield* b.threads.getThreadRecords(first.childId, [
          "subagents",
        ])).subagents.find((task) => task.id === second.taskId)!.remoteChild!.stop!;
        yield* observed(b, first.childId, second.taskId, "unconfirmed", currentStop.requestId);
        const aTask = (yield* a.threads.getThreadRecords(parentId, ["subagents"])).subagents.find(
          (task) => task.id === first.taskId,
        )!;
        expect(aTask.remoteChild?.stop?.state).not.toBe("confirmed");
        const cBefore = yield* c.threads.getThreadEventSequence(second.childId);
        const cEffects = yield* Context.get(
          c.database,
          SqlClient.SqlClient,
        )`SELECT effect_id FROM orchestration_v2_effect_outbox ORDER BY effect_id`;
        expect(
          (yield* c.threads.getThreadRecords(second.childId, ["runs"])).runs.find(
            (run) => run.id === cRun,
          )?.status,
        ).toBe("running");
        const obligation = (yield* b.threads.getThreadRecords(first.childId, [
          "subagents",
        ])).subagents.find((task) => task.id === second.taskId)!.remoteChild!.stop!;
        const inherited = yield* Context.get(b.database, SqlClient.SqlClient)<{
          record_id: string;
        }>`SELECT record_id FROM peer_task_stop_authority WHERE task_id=${second.taskId} AND request_id=${obligation.requestId}`;
        expect(inherited).toHaveLength(1);
        // Restart the observer after the persisted denied intent, then restore its current modes.
        yield* b.stopConnections;
        yield* b.threads.dispatch({
          type: "thread.runtime-mode.set",
          threadId: first.childId,
          commandId: CommandId.make("stop-chain-restore-b"),
          runtimeMode: "full-access",
        });
        const resumed = yield* b.reconnect();
        yield* Context.get(resumed.services, RemoteDelegation.RemoteDelegation).start();
        yield* observed(b, first.childId, second.taskId, "acknowledged", obligation.requestId);
        expect(
          (yield* c.threads.getThreadRecords(second.childId, ["runs"])).runs.find(
            (run) => run.id === cRun,
          )?.status,
        ).toBe("interrupted");
        expect(yield* c.threads.getThreadEventSequence(second.childId)).toBeGreaterThan(cBefore);
        expect(
          yield* Context.get(
            c.database,
            SqlClient.SqlClient,
          )`SELECT effect_id FROM orchestration_v2_effect_outbox ORDER BY effect_id`,
        ).not.toEqual(cEffects);
        yield* drainStops(c);
        yield* observed(b, first.childId, second.taskId, "confirmed", obligation.requestId);
        yield* observed(a, parentId, first.taskId, "confirmed");
        const bTask = (yield* b.threads.getThreadRecords(first.childId, [
          "subagents",
        ])).subagents.find((task) => task.id === second.taskId)!;
        const finalA = (yield* a.threads.getThreadRecords(parentId, ["subagents"])).subagents.find(
          (task) => task.id === first.taskId,
        )!;
        expect(bTask.remoteChild?.stop?.requestId).toBe(obligation.requestId);
        expect(bTask.remoteChild?.stop?.destinationCommandId).toBeDefined();
        expect(finalA.remoteChild?.stop?.destinationCommandId).toBeDefined();
        expect(bTask.remoteChild?.stop?.destinationCommandId).not.toBe(
          finalA.remoteChild?.stop?.destinationCommandId,
        );
        const nativeEvidence = (node: Node, commandId: CommandId) =>
          Context.get(node.database, SqlClient.SqlClient)<{
            effect_id: string;
            effect_type: string;
            status: string;
          }>`SELECT effect_id,effect_type,status FROM orchestration_v2_effect_outbox WHERE command_id=${commandId} ORDER BY effect_id`;
        yield* Effect.logInfo("381 generated-fixture Stop proof", {
          sourceParentThreadId: parentId,
          bTaskId: first.taskId,
          bThreadId: first.childId,
          bRunId: bRun,
          bSourceRequestId: finalA.remoteChild!.stop!.requestId,
          bCommandId: finalA.remoteChild!.stop!.destinationCommandId,
          cTaskId: second.taskId,
          cThreadId: second.childId,
          cRunId: cRun,
          cSourceRequestId: obligation.requestId,
          cCommandId: bTask.remoteChild!.stop!.destinationCommandId,
          bEffects: yield* nativeEvidence(b, finalA.remoteChild!.stop!.destinationCommandId!),
          cEffects: yield* nativeEvidence(c, bTask.remoteChild!.stop!.destinationCommandId!),
        });

        expect((yield* a.threads.getThreadShell(parentId))?.activeRunId).toBeNull();
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.live(
  "lifecycle381: same public cancellation replays after an in-flight launch attaches",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* makeNode(
          descriptorOf("review-attach-b", "B", { peerAuthority: 1 }),
          "disk",
        );
        const a = yield* makeNode(
          descriptorOf("review-attach-a", "A", { peerAuthority: 1 }),
          "disk",
        );
        yield* linkTo(a.links, b, "full-access");
        const parentId = ThreadId.make("review-attach-parent");
        yield* seedParent(a, parentId);
        yield* Ref.set(a.holdLaunchReply, true);
        yield* nativeCall(a, parentId, "delegate_task", {
          task: "Cancel an accepted launch with a delayed reply",
          target: {
            environmentId: b.environmentId,
            projectId: b.project.id,
            providerInstanceId: instanceId,
            model: selection.model,
          },
          clientRequestId: "review-attach-launch",
        }).pipe(Effect.forkScoped);
        yield* Deferred.await(a.launchReplyHeld);
        const before = (yield* a.threads.getThreadRecords(parentId, ["subagents"])).subagents[0]!;
        expect(before.remoteChild?.threadId).toBeUndefined();
        const stop = { taskId: before.id, clientRequestId: "review-attach-stop" };
        const stopCredential = yield* a.registry.issue({
          threadId: parentId,
          providerInstanceId: instanceId,
          capabilities: new Set(["orchestration"]),
        });
        const accepted = yield* nativeCall(
          a,
          parentId,
          "task_cancel",
          stop,
          new Set(["orchestration"]),
          stopCredential,
        );
        expect(accepted.isError, json(accepted.content)).not.toBe(true);
        const sameBeforeAttach = yield* nativeCall(
          a,
          parentId,
          "task_cancel",
          stop,
          new Set(["orchestration"]),
          stopCredential,
        );
        expect(sameBeforeAttach.isError, json(sameBeforeAttach.content)).not.toBe(true);
        const intent = (yield* a.threads.getThreadRecords(parentId, ["subagents"])).subagents[0]!
          .remoteChild!.stop!;
        yield* Ref.set(a.holdLaunchReply, false);
        yield* Deferred.succeed(a.releaseLaunchReply, undefined);
        const childId = yield* attachedChild(a, parentId, before.id);
        const after = (yield* a.threads.getThreadRecords(parentId, ["subagents"])).subagents[0]!;
        expect(after.remoteChild?.stop?.requestId).toBe(intent.requestId);
        expect(after.remoteChild?.threadId).toBe(childId);
        const history = yield* Context.get(
          a.database,
          SqlClient.SqlClient,
        )`SELECT sequence,json_extract(payload_json,'$.remoteChild.threadId') AS target,json_extract(payload_json,'$.remoteChild.stop.requestId') AS request_id FROM orchestration_events WHERE stream_id=${parentId} AND event_type='subagent.updated' AND json_extract(payload_json,'$.remoteChild.stop.requestId')=${intent.requestId} ORDER BY sequence`;
        expect(history.some((row) => row.target === null)).toBe(true);
        expect(history.some((row) => row.target === childId)).toBe(true);
        const replay = yield* nativeCall(
          a,
          parentId,
          "task_cancel",
          stop,
          new Set(["orchestration"]),
          stopCredential,
        );

        expect(replay.isError, json(replay.content)).not.toBe(true);
        yield* a.stopConnections;
        const restored = yield* a.reconnect();
        const resumed = Context.get(restored.services, RemoteDelegation.RemoteDelegation);
        const current = (yield* a.threads.getThreadRecords(parentId, ["subagents"])).subagents.find(
          (task) => task.id === before.id,
        )!;
        const destinationBefore = yield* b.threads.getThreadEventSequence(childId);
        // A projection target alone cannot mint the original accepted attachment.
        for (const changed of [
          { ...current.remoteChild!, threadId: ThreadId.make("forged-other-child") },
          {
            ...current.remoteChild!,
            environmentId: EnvironmentId.make("forged-other-environment"),
          },
          {
            ...current.remoteChild!,
            launch: { ...current.remoteChild!.launch!, linkIdentity: "replacement-generation" },
          },
          {
            ...current.remoteChild!,
            launch: { ...current.remoteChild!.launch!, clientRequestId: "replacement-launch" },
          },
        ]) {
          const event = yield* a.orchestrator
            .streamStoredEventsFrom({
              threadId: parentId,
              afterSequence: 0,
              eventType: "subagent.updated",
            })
            .pipe(Stream.runHead, Effect.flatMap(Effect.fromOption));
          if (event.event.type !== "subagent.updated")
            return yield* Effect.die("Missing persisted task");
          yield* a.sink.write({
            events: [
              {
                ...event.event,
                id: EventId.make(
                  `forged-attachment:${changed.threadId}:${changed.environmentId}:${changed.launch!.linkIdentity}:${changed.launch!.clientRequestId}`,
                ),
                payload: { ...current, remoteChild: changed },
              },
            ],
          });
          expect(
            (yield* resumed.requestStop(parentId, before.id, intent.requestId).pipe(Effect.result))
              ._tag,
          ).toBe("Failure");
        }
        const now = yield* DateTime.now;
        yield* a.sink.write({
          events: [
            {
              id: EventId.make("restore-original-attachment"),
              type: "subagent.updated",
              threadId: parentId,
              occurredAt: now,
              payload: current,
            },
          ],
        });
        expect(yield* b.threads.getThreadEventSequence(childId)).toBe(destinationBefore);
        const afterRestart = yield* nativeCall(
          a,
          parentId,
          "task_cancel",
          stop,
          new Set(["orchestration"]),
          stopCredential,
        );
        expect(afterRestart.isError, json(afterRestart.content)).not.toBe(true);
        yield* resumed.requestStop(
          parentId,
          before.id,
          "client-stop:attachment-newer",
          "later intent",
        );
        const oldRetry = yield* nativeCall(
          a,
          parentId,
          "task_cancel",
          stop,
          new Set(["orchestration"]),
          stopCredential,
        );
        expect(oldRetry.isError, json(oldRetry.content)).not.toBe(true);
        expect(
          (yield* a.threads.getThreadRecords(parentId, ["subagents"])).subagents.find(
            (task) => task.id === before.id,
          )?.remoteChild?.stop?.requestId,
        ).toBe("client-stop:attachment-newer");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.live(
  "lifecycle381: production kernel exports the authenticated lifecycle transport and registered delegation",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* makeNode(
          descriptorOf("native-route-b", "B", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true },
        );
        const a = yield* makeNode(
          descriptorOf("native-route-a", "A", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const session = (yield* b.linkedSessions)[0];
        expect(session).toBeDefined();
        // A lifecycle-capability probe is an authenticated peer operation, not a descriptor assertion.
        const peers = Context.get(a.services, PeerMcpClient.PeerMcpClient);
        yield* peers.requireTaskLifecycle!(b.environmentId);
        const parentId = ThreadId.make("native-route-parent");
        yield* seedParent(a, parentId);
        const launched = yield* nativeCall(a, parentId, "delegate_task", {
          task: "Return native route evidence",
          mode: "async",
          clientRequestId: "native-route-job",
          target: {
            environmentId: b.environmentId,
            projectId: b.project.id,
            providerInstanceId: instanceId,
            model: "gpt-5.4",
          },
        });
        expect(launched.isError, json(launched.content)).not.toBe(true);
        const { taskId } = yield* decodeTaskReceipt(launched.structuredContent);
        const childId = yield* attachedChild(a, parentId, taskId);
        const view = yield* a.remote.view(parentId, taskId, 0);
        expect(view.threadId).toBe(childId);
        expect(view.environmentId).toBe(b.environmentId);
        expect(
          (yield* a.threads.getThreadRecords(parentId, ["subagents"])).subagents.find(
            (task) => task.id === taskId,
          )?.remoteChild?.launch?.lifecycleVersion,
        ).toBe(1);
        const http = yield* HttpClient.HttpClient;
        const unauthenticated = yield* http.execute(
          HttpClientRequest.get(`${b.url}/mcp/peer-control/tasks`),
        );
        expect(unauthenticated.status).toBe(401);
        const runId = yield* activate(b, childId);
        const stop = yield* nativeCall(a, parentId, "task_cancel", {
          taskId,
          clientRequestId: "native-route-stop",
        });
        expect(stop.isError, json(stop.content)).not.toBe(true);
        const stoppedObservation = (state: "acknowledged" | "confirmed") =>
          a.orchestrator
            .streamStoredEventsFrom({
              threadId: parentId,
              afterSequence: 0,
              eventType: "subagent.updated",
            })
            .pipe(
              Stream.filter(
                (stored) =>
                  stored.event.type === "subagent.updated" &&
                  stored.event.payload.id === taskId &&
                  stored.event.payload.remoteChild?.stop?.state === state,
              ),
              Stream.runHead,
              Effect.flatMap(Effect.fromOption),
            );
        const accepted = yield* stoppedObservation("acknowledged");
        if (accepted.event.type !== "subagent.updated")
          return yield* Effect.die("Expected Stop projection");
        const commandId = accepted.event.payload.remoteChild!.stop!.destinationCommandId!;
        const pair = {
          parentEnvironmentId: a.environmentId,
          parentThreadId: parentId,
          taskId,
          childEnvironmentId: b.environmentId,
          childThreadId: childId,
          childRunId: null,
        };
        const generation = accepted.event.payload.remoteChild!.launch!.linkIdentity;
        const observe = () =>
          peers.taskLifecycle!(b.environmentId, generation, {
            pair,
            operation: { type: "snapshot", after: 0, wait: false, stopCommandId: commandId },
          });
        expect(yield* observe()).toMatchObject({
          type: "snapshot",
          snapshot: { stopExecution: { state: "pending" } },
        });
        const sql = Context.get(b.database, SqlClient.SqlClient);
        expect(
          yield* sql`SELECT command_id FROM peer_task_stop_receipts WHERE command_id=${commandId}`,
        ).toHaveLength(1);
        yield* drainStops(b);
        expect(yield* observe()).toMatchObject({
          type: "snapshot",
          snapshot: { stopExecution: { state: "settled" } },
        });
        yield* stoppedObservation("confirmed");
        expect(
          (yield* b.threads.getThreadRecords(childId, ["runs"])).runs.find(
            (run) => run.id === runId,
          )?.status,
        ).toBe("interrupted");
        const effects = yield* sql<{
          effect_id: string;
          status: string;
        }>`SELECT effect_id,status FROM orchestration_v2_effect_outbox WHERE command_id=${commandId} AND effect_type='delegated-tasks.stop'`;
        expect(effects).toHaveLength(1);
        expect(effects[0]?.status).toBe("succeeded");
        yield* Effect.logInfo("Production lifecycle native receipt and shared outbox proof", {
          commandId,
          effects,
        });
      }),
    ).pipe(Effect.provide(Layer.merge(FetchHttpClient.layer, NodeServices.layer))),
  30_000,
);
it.live(
  "reviewer382: separate callers may reuse a local request key",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const node = yield* makeNode(
          descriptorOf("reviewer382", "Review", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const one = ThreadId.make("reviewer382-one"),
          two = ThreadId.make("reviewer382-two");
        yield* seedParent(node, one);
        yield* seedParent(node, two);
        const args = {
          message: "isolated request",
          mode: "queue",
          clientRequestId: "reviewer-shared-key",
        };
        const first = yield* nativeCall(node, one, "t3_thread_send", { ...args, threadId: one });
        expect(first.isError, json(first)).not.toBe(true);
        const second = yield* nativeCall(node, two, "t3_thread_send", { ...args, threadId: two });
        expect(second.isError, json(second)).not.toBe(true);
        for (const thread of [one, two]) {
          const renamed = yield* nativeCall(node, thread, "t3_thread_update", {
            action: "rename",
            title: `Independent ${thread}`,
            clientRequestId: "reviewer-shared-key",
          });
          expect(renamed.isError, json(renamed)).not.toBe(true);
          const scheduled = yield* nativeCall(node, thread, "schedule_task", {
            title: "Independent task",
            prompt: "Independent prompt",
            bindToCurrentThread: true,
            enabled: false,
            schedule: { type: "interval", everyMs: 3600000 },
            clientRequestId: "reviewer-shared-key",
          });
          expect(scheduled.isError, json(scheduled)).not.toBe(true);
        }
        expect((yield* node.scheduled.list()).tasks).toHaveLength(2);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30000,
);

it.live(
  "reviewer382: legacy metadata replay preserves later title",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const node = yield* makeNode(
          descriptorOf("reviewer382-import", "Import", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const parent = ThreadId.make("reviewer382-import-parent");
        yield* seedParent(node, parent);
        const oldCommand = CommandId.make(
          [
            "command",
            "mcp",
            "pre-upgrade-credential",
            "thread-update",
            encodeURIComponent(parent),
            "rename",
            "old-rename",
          ].join(":"),
        );
        yield* node.threads.dispatch({
          type: "thread.metadata.update",
          commandId: oldCommand,
          threadId: parent,
          title: "Original title",
        });
        const later = yield* nativeCall(node, parent, "t3_thread_update", {
          action: "rename",
          title: "Later independent title",
          clientRequestId: "later-edit",
        });
        expect(later.isError, json(later)).not.toBe(true);
        const replay = yield* nativeCall(node, parent, "t3_thread_update", {
          action: "rename",
          title: "Original title",
          clientRequestId: "old-rename",
        });
        const shell = yield* node.threads.getThreadShell(parent);
        expect(shell?.title, json(replay)).toBe("Later independent title");
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30000,
);

it.live(
  "reviewer382: concurrent keyed field edits retain both intents",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const node = yield* makeNode(
          descriptorOf("reviewer382-rivals", "Rivals", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const parent = ThreadId.make("reviewer382-rivals-parent");
        yield* seedParent(node, parent);
        const created = yield* nativeCall(node, parent, "schedule_task", {
          prompt: "Original prompt",
          title: "Original title",
          schedule: { type: "interval", everyMs: 60000 },
          clientRequestId: "reviewer-rival-create",
        });
        expect(created.isError, json(created)).not.toBe(true);
        const receipt = yield* Schema.decodeUnknownEffect(McpScheduledMutationReceipt)(
          created.structuredContent,
        );
        const bothRead = yield* Deferred.make<void>();
        let reads = 0;
        const realList = node.scheduled.list;
        const spy = vi.spyOn(node.scheduled, "list").mockImplementation(() =>
          realList().pipe(
            Effect.tap(() =>
              Effect.gen(function* () {
                reads++;
                if (reads === 2) yield* Deferred.succeed(bothRead, undefined);
                if (reads <= 2) yield* Deferred.await(bothRead);
              }),
            ),
          ),
        );
        const results = yield* Effect.all(
          [
            nativeCall(node, parent, "update_scheduled_task", {
              scheduledTaskId: receipt.scheduledTaskId,
              title: "Title A",
              clientRequestId: "reviewer-rival-a",
            }),
            nativeCall(node, parent, "update_scheduled_task", {
              scheduledTaskId: receipt.scheduledTaskId,
              prompt: "Prompt B",
              clientRequestId: "reviewer-rival-b",
            }),
          ],
          { concurrency: "unbounded" },
        ).pipe(Effect.ensuring(Effect.sync(() => spy.mockRestore())));
        expect(
          results.every((r) => r.isError !== true),
          json(results),
        ).toBe(true);
        const current = (yield* realList()).tasks.find((t) => t.id === receipt.scheduledTaskId)!;
        expect({ title: current.title, prompt: current.prompt }).toEqual({
          title: "Title A",
          prompt: "Prompt B",
        });
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30000,
);

it.live(
  "runtime382-F1: remote scheduler validation leaves SQLite free for reverse control",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(descriptorOf("runtime382-a", "A", { peerAuthority: 1 }), "disk", {
          productionKernel: true,
          attachmentRequests: true,
        });
        const b = yield* makeNode(descriptorOf("runtime382-b", "B", { peerAuthority: 1 }), "disk", {
          productionKernel: true,
          attachmentRequests: true,
        });
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("runtime382-parent");
        yield* seedParent(a, parent);
        const input = {
          environmentId: b.environmentId,
          projectId: b.project.id,
          title: "Original title",
          prompt: "Disabled fixture",
          enabled: false,
          schedule: { type: "interval", everyMs: 3600000 },
          clientRequestId: "runtime382-create",
        };
        const created = yield* nativeCall(a, parent, "schedule_task", input);
        expect(created.isError, json(created)).not.toBe(true);
        const receipt = yield* Schema.decodeUnknownEffect(McpScheduledMutationReceipt)(
          created.structuredContent,
        );
        const sql = Context.get(b.database, SqlClient.SqlClient);
        let transactionalValidations = 0;
        const actual = b.authority.checkTarget;
        const observer = vi.spyOn(b.authority, "checkTarget").mockImplementation((...args) =>
          Effect.gen(function* () {
            if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
              transactionalValidations++;
            return yield* actual(...args);
          }),
        );
        const update = yield* nativeCall(a, parent, "update_scheduled_task", {
          environmentId: b.environmentId,
          scheduledTaskId: receipt.scheduledTaskId,
          title: "Later independent title",
          clientRequestId: "runtime382-update",
        }).pipe(Effect.ensuring(Effect.sync(() => observer.mockRestore())));
        expect.soft(transactionalValidations).toBe(0);
        expect(update.isError, json(update)).not.toBe(true);
        expect(
          (yield* b.scheduled.list()).tasks.find((t) => t.id === receipt.scheduledTaskId),
        ).toMatchObject({
          title: "Later independent title",
          enabled: false,
          runCount: 0,
          nextRunAt: null,
        });
        const replay = yield* nativeCall(a, parent, "schedule_task", input);
        expect(replay.structuredContent).toEqual(created.structuredContent);
        expect(
          (yield* b.scheduled.list()).tasks.find((t) => t.id === receipt.scheduledTaskId)?.title,
        ).toBe("Later independent title");
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  90000,
);

it.live(
  "runtime382-F1: remote keyed manual admission validates outside SQL and replays native acceptance",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("manual-runtime382-a", "A", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf("manual-runtime382-b", "B", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("manual-runtime382-source"),
          target = ThreadId.make("manual-runtime382-target");
        yield* seedParent(a, parent);
        yield* seedParent(b, target);
        const created = yield* nativeCall(b, target, "schedule_task", {
          title: "Manual target",
          prompt: "One queued message",
          bindToCurrentThread: true,
          enabled: false,
          schedule: { type: "interval", everyMs: 3600000 },
          clientRequestId: "manual-runtime382-create",
        });
        expect(created.isError, json(created)).not.toBe(true);
        const receipt = yield* Schema.decodeUnknownEffect(McpScheduledMutationReceipt)(
          created.structuredContent,
        );
        const sql = Context.get(b.database, SqlClient.SqlClient);
        let transactionalValidations = 0;
        const actual = b.authority.checkTarget;
        const observer = vi.spyOn(b.authority, "checkTarget").mockImplementation((...args) =>
          Effect.gen(function* () {
            if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
              transactionalValidations++;
            return yield* actual(...args);
          }),
        );
        const input = {
          environmentId: b.environmentId,
          taskId: receipt.scheduledTaskId,
          clientRequestId: "manual-runtime382-run",
        };
        const first = yield* nativeCall(a, parent, "run_scheduled_task_now", input);
        const again = yield* nativeCall(a, parent, "run_scheduled_task_now", input).pipe(
          Effect.ensuring(Effect.sync(() => observer.mockRestore())),
        );
        expect(first.isError, json(first)).not.toBe(true);
        expect(again.structuredContent).toEqual(first.structuredContent);
        expect(transactionalValidations).toBe(0);
        expect(
          (yield* b.scheduled.list()).tasks.find((t) => t.id === receipt.scheduledTaskId)?.runCount,
        ).toBe(1);
        expect(yield* sql`SELECT * FROM scheduled_manual_requests`).toHaveLength(1);
        const removed = yield* nativeCall(a, parent, "delete_scheduled_task", {
          environmentId: b.environmentId,
          scheduledTaskId: receipt.scheduledTaskId,
          clientRequestId: "manual-runtime382-delete",
        });
        expect(removed.isError, json(removed)).not.toBe(true);
        expect((yield* b.scheduled.list()).tasks).toHaveLength(0);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  90000,
);

it.live(
  "migration382-S2: each legacy metadata action is target-scoped and never becomes a fresh command",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const node = yield* makeNode(
          descriptorOf("migration382-metadata", "Metadata", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const parent = ThreadId.make("migration382-metadata-parent"),
          other = ThreadId.make("migration382-metadata-other");
        yield* seedParent(node, parent);
        yield* seedParent(node, other);
        const sql = Context.get(node.database, SqlClient.SqlClient);
        const pullRequest = {
          repository: "isolated/repository",
          number: 382,
          url: "https://github.com/isolated/repository/pull/382",
        };
        for (const action of [
          "rename",
          "regenerate_title",
          "link_pull_request",
          "unlink_pull_request",
        ] as const) {
          const key = `old:${action}:key`,
            commandId = CommandId.make(
              [
                "command",
                "mcp",
                "pre-upgrade-credential",
                "thread-update",
                encodeURIComponent(parent),
                action,
                encodeURIComponent(key),
              ].join(":"),
            );
          yield* node.threads.dispatch({
            type: "thread.metadata.update",
            commandId,
            threadId: parent,
            ...(action === "rename"
              ? { title: "Original title" }
              : action === "regenerate_title"
                ? { regenerateTitle: true }
                : {
                    linkedPullRequest:
                      action === "link_pull_request"
                        ? { projectId: node.project.id, ...pullRequest }
                        : null,
                  }),
          });
          const later = yield* nativeCall(node, parent, "t3_thread_update", {
            action: "rename",
            title: "Later independent title",
            clientRequestId: `later:${action}`,
          });
          expect(later.isError, json(later)).not.toBe(true);
          const before = yield* sql`SELECT command_id FROM orchestration_command_receipts`;
          const replay = yield* nativeCall(node, parent, "t3_thread_update", {
            action,
            ...(action === "rename"
              ? { title: "Original title" }
              : action === "link_pull_request"
                ? { pullRequest }
                : {}),
            clientRequestId: key,
          });
          expect(replay.isError).toBe(true);
          expect(json(replay)).toContain("legacy operation");
          expect(yield* sql`SELECT command_id FROM orchestration_command_receipts`).toEqual(before);
          expect((yield* node.threads.getThreadShell(parent))?.title).toBe(
            "Later independent title",
          );
          const unrelated = yield* nativeCall(node, other, "t3_thread_update", {
            action: "rename",
            title: "Independent other caller",
            clientRequestId: key,
          });
          expect(unrelated.isError, json(unrelated)).not.toBe(true);
        }
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30000,
);

it.live(
  "runtime382-F1: a local binding change during live authorization cannot cross the commit guard",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const node = yield* makeNode(
          descriptorOf("fence382", "Fence", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const parent = ThreadId.make("fence382-parent");
        yield* seedParent(node, parent);
        const created = yield* nativeCall(node, parent, "schedule_task", {
          title: "Original title",
          prompt: "Original prompt",
          enabled: false,
          schedule: { type: "interval", everyMs: 3600000 },
          clientRequestId: "fence382-create",
        });
        expect(created.isError, json(created)).not.toBe(true);
        const receipt = yield* Schema.decodeUnknownEffect(McpScheduledMutationReceipt)(
          created.structuredContent,
        );
        const entered = yield* Deferred.make<void>(),
          release = yield* Deferred.make<void>();
        const actual = node.authority.checkTarget;
        const observer = vi.spyOn(node.authority, "checkTarget").mockImplementation((...args) =>
          Effect.gen(function* () {
            yield* actual(...args);
            if ((yield* ScheduleMutation.CurrentMutation) !== undefined) {
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(release);
            }
          }),
        );
        const pending = yield* nativeCall(node, parent, "update_scheduled_task", {
          scheduledTaskId: receipt.scheduledTaskId,
          title: "Must not overwrite",
          clientRequestId: "fence382-update",
        }).pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        const current = (yield* node.scheduled.list()).tasks.find(
          (t) => t.id === receipt.scheduledTaskId,
        )!;
        yield* node.scheduled.upsert({ ...current, runtimeMode: "approval-required" });
        yield* Deferred.succeed(release, undefined);
        const refused = yield* Fiber.join(pending).pipe(
          Effect.ensuring(Effect.sync(() => observer.mockRestore())),
        );
        expect(refused.isError, json(refused)).toBe(true);
        expect(json(refused)).toContain("binding changed");
        expect(
          (yield* node.scheduled.list()).tasks.find((t) => t.id === receipt.scheduledTaskId),
        ).toMatchObject({ title: "Original title", runtimeMode: "approval-required" });
        const sql = Context.get(node.database, SqlClient.SqlClient);
        expect(yield* sql`SELECT * FROM scheduled_mutation_receipts`).toHaveLength(1);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30000,
);

it.live(
  "Grok382-G1: owner-proved preview nondelivery leaves the same key retryable",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const node = yield* makeNode(
          descriptorOf("nondelivery382", "Nondelivery", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const parent = ThreadId.make("nondelivery382-parent");
        yield* seedParent(node, parent);
        let clicks = 0;
        const connect = (clientId: string) =>
          Effect.gen(function* () {
            const connected = yield* Deferred.make<void>();
            yield* Stream.runForEach(
              yield* node.browser.connect({ clientId, environmentId: node.environmentId }),
              (event) =>
                event.type === "connected"
                  ? Deferred.succeed(connected, undefined)
                  : Effect.gen(function* () {
                      if (event.request.operation === "click") clicks++;
                      yield* node.browser.respond({
                        clientId,
                        connectionId: event.connectionId,
                        requestId: event.request.requestId,
                        ok: true,
                        result:
                          event.request.operation === "status"
                            ? {
                                available: true,
                                visible: true,
                                tabId: null,
                                url: null,
                                title: null,
                                loading: false,
                              }
                            : {},
                      });
                    }),
            ).pipe(Effect.forkScoped);
            yield* Deferred.await(connected);
          });
        yield* connect("before-closure");
        const originalOffer = (yield* Effect.promise(() =>
          vi.importActual<typeof import("effect/Queue")>("effect/Queue"),
        )).offer;
        let closed = false;
        const closeFirstAction: typeof Queue.offer = dual(
          2,
          <A, E>(queue: Queue.Enqueue<A, E>, value: A) => {
            if (
              !closed &&
              typeof value === "object" &&
              value !== null &&
              "type" in value &&
              value.type === "request" &&
              "request" in value &&
              typeof value.request === "object" &&
              value.request !== null &&
              "operation" in value.request &&
              value.request.operation === "click"
            ) {
              closed = true;
              // Fault only the actual external delivery queue; do not fake its result.
              Queue.shutdownUnsafe(queue);
            }
            return originalOffer(queue, value);
          },
        );
        const delivery = vi.spyOn(Queue, "offer").mockImplementation(closeFirstAction);
        const input = {
          locator: "role=button[name='Send']",
          clientRequestId: "nondelivery382-click",
        };
        const failed = yield* nativeCall(node, parent, "preview_click", input).pipe(
          Effect.ensuring(Effect.sync(() => delivery.mockRestore())),
        );
        expect(closed).toBe(true);
        expect(failed.isError).toBe(true);
        expect(json(failed)).not.toContain("outcome_unknown");
        expect(clicks).toBe(0);
        const sql = Context.get(node.database, SqlClient.SqlClient);
        expect(
          yield* sql<{
            state: string;
            attempt_id: string | null;
          }>`SELECT state,attempt_id FROM mcp_request_receipts`,
        ).toEqual([{ state: "bound", attempt_id: null }]);
        const active = { ...node, ...(yield* node.restartSchedulerTransport) };
        yield* connect("after-closure");
        const accepted = yield* nativeCall(active, parent, "preview_click", input);
        expect(accepted.isError, json(accepted)).not.toBe(true);
        const replay = yield* nativeCall(active, parent, "preview_click", input);
        expect(replay.structuredContent).toEqual(accepted.structuredContent);
        expect(clicks).toBe(1);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30000,
);

it.live.each(["reserve", "discard"] as const)(
  "Grok382-G2: registered discard and native claim retain the %s winner",
  (winner) =>
    Effect.scoped(
      Effect.gen(function* () {
        const node = yield* makeNode(
          descriptorOf(`discard-race382-${winner}`, "Discard race", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const parent = ThreadId.make(`discard-race382-${winner}`);
        yield* seedParent(node, parent);
        const metadata = {
          type: "file" as const,
          name: "race.pdf",
          mimeType: "application/pdf",
          sizeBytes: 4,
        };
        const prepared = yield* nativeCall(node, parent, "t3_attachment_prepare_upload", {
          upload: metadata,
          digest: "9f64a747e1b97f131fabb6b447296c9b6f0201e79fb3c5356e6c77e89b6a806a",
          clientRequestId: "race382-upload",
        });
        expect(prepared.isError, json(prepared)).not.toBe(true);
        const issued = yield* Schema.decodeUnknownEffect(PeerAttachmentUploadResult)(
          prepared.structuredContent,
        );
        const http = yield* HttpClient.HttpClient;
        expect(
          (yield* http.execute(
            HttpClientRequest.post(`${node.url}${issued.relativeUrl}`).pipe(
              HttpClientRequest.setBody(HttpBody.uint8Array(new Uint8Array([1, 2, 3, 4]))),
            ),
          )).status,
        ).toBe(204);
        const attachment = { ...metadata, id: issued.attachmentId };
        const staging = Option.getOrThrow(
          yield* Effect.serviceOption(AttachmentStaging.AttachmentStaging).pipe(
            Effect.provide(Layer.succeedContext(node.requestContext)),
          ),
        );
        const entered = yield* Deferred.make<void>(),
          release = yield* Deferred.make<void>();
        const file = resolveAttachmentPathById({
          attachmentsDir: node.config.attachmentsDir,
          attachmentId: attachment.id,
        })!;
        const fs = yield* FileSystem.FileSystem;
        if (winner === "reserve") {
          const actual = staging.discard;
          const paused = vi
            .spyOn(staging, "discard")
            .mockImplementation((...args) =>
              Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Deferred.await(release)),
                Effect.andThen(actual(...args)),
              ),
            );
          const pending = yield* nativeCall(node, parent, "t3_attachment_discard", {
            attachmentId: attachment.id,
            clientRequestId: "race382-discard",
          }).pipe(Effect.forkScoped);
          yield* Deferred.await(entered);
          yield* staging.reserve(
            caller(node, parent),
            "race382-native-command",
            parent,
            attachment,
          );
          yield* Deferred.succeed(release, undefined);
          const refused = yield* Fiber.join(pending).pipe(
            Effect.ensuring(Effect.sync(() => paused.mockRestore())),
          );
          expect(refused.isError).toBe(true);
          expect(Array.from(yield* fs.readFile(file))).toEqual([1, 2, 3, 4]);
          const claimed = yield* AttachmentClaims.claimPendingAttachments({
            commandId: "race382-native-command",
            threadId: parent,
            attachments: [attachment],
          }).pipe(
            Effect.provideService(Invocation.McpInvocationContext, caller(node, parent)),
            Effect.provide(
              Layer.mergeAll(
                Layer.succeedContext(node.requestContext),
                Layer.succeed(Config.ServerConfig, node.config),
              ),
            ),
          );
          const deliveredFile = resolveAttachmentPathById({
            attachmentsDir: node.config.attachmentsDir,
            attachmentId: claimed.attachments[0]!.id,
          })!;
          expect(Array.from(yield* fs.readFile(deliveredFile))).toEqual([1, 2, 3, 4]);
          yield* node.threads.sendToThread({
            projectId: node.project.id,
            commandId: CommandId.make("race382-native-command"),
            threadId: parent,
            messageId: MessageId.make("race382-message"),
            text: "Reserved original bytes",
            attachments: claimed.attachments,
            mode: "queue",
            modelSelection: selection,
            createdBy: "agent",
            creationSource: "mcp",
          });
          const sql = Context.get(node.database, SqlClient.SqlClient);
          expect(
            yield* sql`SELECT status FROM orchestration_command_receipts WHERE command_id='race382-native-command'`,
          ).toEqual([{ status: "accepted" }]);
        } else {
          const actual = staging.bindConsumption;
          const paused = vi
            .spyOn(staging, "bindConsumption")
            .mockImplementation((...args) =>
              Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Deferred.await(release)),
                Effect.andThen(actual(...args)),
              ),
            );
          const pending = yield* nativeCall(node, parent, "t3_thread_send_attachments", {
            threadId: parent,
            message: "Must not accept discarded bytes",
            attachments: [attachment],
            clientRequestId: "race382-send",
          }).pipe(Effect.forkScoped);
          yield* Deferred.await(entered);
          const discarded = yield* nativeCall(node, parent, "t3_attachment_discard", {
            attachmentId: attachment.id,
            clientRequestId: "race382-discard",
          });
          expect(discarded.isError, json(discarded)).not.toBe(true);
          expect(Array.from(yield* fs.readFile(file))).toEqual([1, 2, 3, 4]);
          const proofSql = Context.get(node.database, SqlClient.SqlClient);
          expect(
            yield* proofSql`SELECT consumer FROM attachment_staging WHERE id=${attachment.id}`,
          ).toEqual([{ consumer: "discarded" }]);
          const reconstructed = { ...node, ...(yield* node.restartSchedulerTransport) };
          const replayDiscard = yield* nativeCall(reconstructed, parent, "t3_attachment_discard", {
            attachmentId: attachment.id,
            clientRequestId: "race382-discard",
          });
          expect(replayDiscard.isError, json(replayDiscard)).not.toBe(true);
          expect(replayDiscard.content).toEqual(discarded.content);
          const acceptance = yield* proofSql<{
            discard_receipt: string;
            discard_cleanup: string;
          }>`SELECT discard_receipt,discard_cleanup FROM attachment_staging WHERE id=${attachment.id}`;
          expect(acceptance[0]?.discard_receipt).toContain('"inputDigest"');
          expect(acceptance[0]?.discard_receipt).toContain('"generation":"local"');
          expect(acceptance[0]?.discard_cleanup).toBe("retained");
          expect(
            (yield* nativeCall(reconstructed, parent, "t3_attachment_discard", {
              attachmentId: attachment.id,
              clientRequestId: "race382-rival-discard",
            })).isError,
          ).toBe(true);
          expect(
            (yield* nativeCall(reconstructed, parent, "t3_attachment_discard", {
              attachmentId: "pending-unrelated.pdf",
              clientRequestId: "race382-discard",
            })).isError,
          ).toBe(true);
          const other = ThreadId.make("race382-other-caller");
          yield* seedParent(node, other);
          expect(
            (yield* nativeCall(reconstructed, other, "t3_attachment_discard", {
              attachmentId: attachment.id,
              clientRequestId: "race382-discard",
            })).isError,
          ).toBe(true);
          yield* Deferred.succeed(release, undefined);
          const refused = yield* Fiber.join(pending).pipe(
            Effect.ensuring(Effect.sync(() => paused.mockRestore())),
          );
          expect(refused.isError).toBe(true);
          expect(Array.from(yield* fs.readFile(file))).toEqual([1, 2, 3, 4]);
          expect(
            (yield* node.threads.getThreadRecords(parent, ["messages"])).messages.filter(
              (message) => message.text === "Must not accept discarded bytes",
            ),
          ).toHaveLength(0);
          // Historical acceptance does not delete independently replaced bytes.
          yield* fs.writeFile(file, new Uint8Array([9, 8, 7, 6]));
          expect(
            (yield* nativeCall(reconstructed, parent, "t3_attachment_discard", {
              attachmentId: attachment.id,
              clientRequestId: "race382-discard",
            })).isError,
          ).not.toBe(true);
          expect(Array.from(yield* fs.readFile(file))).toEqual([9, 8, 7, 6]);
        }
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30000,
);

it.live(
  "Grok382-G1: native device pure refusals retry, but selection after host preparation stays unknown",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const options = {
          productionKernel: true,
          attachmentRequests: true,
          preferencesOwner: true,
          deviceOwner: true,
        };
        const node = yield* makeNode(
          descriptorOf("device-preflight382", "Preflight", { peerAuthority: 1 }),
          "disk",
          options,
        );
        const parent = ThreadId.make("device-preflight382-parent");
        yield* seedParent(node, parent);
        const input = {
          deviceId: "fixture-device382",
          platform: "android",
          clientRequestId: "device-preflight382-open",
        };
        yield* node.fixtureSettings.updateSettings({ enableDeviceSupport: false });
        const disabled = yield* nativeCall(node, parent, "device_open", input);
        expect(disabled.isError).toBe(true);
        expect(json(disabled)).not.toContain("outcome_unknown");
        yield* node.fixtureSettings.updateSettings({ enableDeviceSupport: true });
        yield* Ref.set(node.devicePlatformAvailable, false);
        const unsupported = yield* nativeCall(node, parent, "device_open", input);
        expect(unsupported.isError).toBe(true);
        expect(json(unsupported)).not.toContain("outcome_unknown");
        expect(yield* Ref.get(node.deviceHostStarts)).toBe(0);
        expect(yield* Ref.get(node.deviceAgentStarts)).toBe(0);
        const sql = Context.get(node.database, SqlClient.SqlClient);
        expect(yield* sql`SELECT state FROM mcp_request_receipts`).toEqual([{ state: "bound" }]);
        yield* Ref.set(node.devicePlatformAvailable, true);
        const accepted = yield* nativeCall(node, parent, "device_open", input);
        expect(accepted.isError, json(accepted)).not.toBe(true);
        const count = yield* Ref.get(node.deviceHostStarts);
        expect((yield* nativeCall(node, parent, "device_open", input)).structuredContent).toEqual(
          accepted.structuredContent,
        );
        expect(yield* Ref.get(node.deviceHostStarts)).toBe(count);
        expect(yield* Ref.get(node.deviceAgentStarts)).toBe(1);
        const other = yield* makeNode(
          descriptorOf("device-selection382", "Selection", { peerAuthority: 1 }),
          "disk",
          options,
        );
        const otherParent = ThreadId.make("device-selection382-parent");
        yield* seedParent(other, otherParent);
        const missing = {
          deviceId: "not-discovered",
          platform: "android",
          clientRequestId: "device-selection382-open",
        };
        const unknown = yield* nativeCall(other, otherParent, "device_open", missing);
        expect(json(unknown)).toContain("outcome_unknown");
        const fs = yield* FileSystem.FileSystem;
        expect(
          yield* fs.readFileString(`${other.config.stateDir}/fixture-device-host-started`),
        ).toBe("Isolated native preparation footprint");
        const prepared = yield* Ref.get(other.deviceHostStarts);
        expect(prepared).toBeGreaterThan(0);
        expect(yield* Ref.get(other.deviceAgentStarts)).toBe(0);
        expect(json(yield* nativeCall(other, otherParent, "device_open", missing))).toContain(
          "outcome_unknown",
        );
        expect(yield* Ref.get(other.deviceHostStarts)).toBe(prepared);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30000,
);

it.live(
  "Grok382-G3: historical receipts survive deletion with current owner, generation and mode checks",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const options = { productionKernel: true, attachmentRequests: true };
        const a = yield* makeNode(
          descriptorOf("history-access382-a", "A", { peerAuthority: 1 }),
          "disk",
          options,
        );
        const b = yield* makeNode(
          descriptorOf("history-access382-b", "B", { peerAuthority: 1 }),
          "disk",
          options,
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("history-access382-parent"),
          other = ThreadId.make("history-access382-other");
        yield* seedParent(a, parent);
        yield* seedParent(a, other);
        const create = {
          environmentId: b.environmentId,
          projectId: b.project.id,
          title: "Original",
          prompt: "ISOLATED_G3_PRIVATE_PROMPT_382",
          enabled: false,
          schedule: { type: "interval" as const, everyMs: 3600000 },
          clientRequestId: "history-access382-create",
        };
        const first = yield* nativeCall(a, parent, "schedule_task", create);
        expect(first.isError, json(first)).not.toBe(true);
        const receipt = yield* Schema.decodeUnknownEffect(McpScheduledMutationReceipt)(
          first.structuredContent,
        );
        const edit = {
          environmentId: b.environmentId,
          scheduledTaskId: receipt.scheduledTaskId,
          title: "Later",
          clientRequestId: "history-access382-edit",
        };
        const edited = yield* nativeCall(a, parent, "update_scheduled_task", edit);
        expect(edited.isError, json(edited)).not.toBe(true);
        const remove = {
          environmentId: b.environmentId,
          scheduledTaskId: receipt.scheduledTaskId,
          clientRequestId: "history-access382-delete",
        };
        const deleted = yield* nativeCall(a, parent, "delete_scheduled_task", remove);
        expect(deleted.isError, json(deleted)).not.toBe(true);
        const replay = () => nativeCall(a, parent, "schedule_task", create);
        expect((yield* replay()).structuredContent).toEqual(first.structuredContent);
        expect(
          (yield* nativeCall(a, parent, "update_scheduled_task", edit)).structuredContent,
        ).toEqual(edited.structuredContent);
        expect(
          (yield* nativeCall(a, parent, "delete_scheduled_task", remove)).structuredContent,
        ).toEqual(deleted.structuredContent);
        expect((yield* nativeCall(a, other, "update_scheduled_task", edit)).isError).toBe(true);
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("history-access382-narrow"),
          threadId: parent,
          runtimeMode: "auto",
        });
        expect((yield* replay()).isError).toBe(true);
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("history-access382-restore"),
          threadId: parent,
          runtimeMode: "full-access",
        });
        expect((yield* replay()).structuredContent).toEqual(first.structuredContent);
        // Native credentials always include orchestration. Exercise a genuinely
        // narrowed server-side forwarding scope rather than an ignored issue option.
        const deniedCapability = yield* Effect.result(
          a.forwarding.call(
            { ...caller(a, parent), capabilities: new Set(["preview"]) },
            OrchestratorToolkit.tools.schedule_task,
            b.environmentId,
            create,
          ),
        );
        expect(deniedCapability._tag).toBe("Failure");
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const evidence = yield* sql<{
          request_id: string;
          resource_json: string;
        }>`SELECT request_id,resource_json FROM scheduled_mutation_receipts`;
        expect(evidence).toHaveLength(3);
        expect(json(evidence)).not.toContain(create.prompt);
        expect((yield* b.scheduled.list()).tasks).toHaveLength(0);
        // An imported confirmation lacks historical proof; never backfill from absence or replacement.
        const original = evidence.find((row) => row.request_id === receipt.operationId)!;
        yield* sql`UPDATE scheduled_mutation_receipts SET resource_json=NULL WHERE request_id=${original.request_id}`;
        const unsupported = yield* replay();
        expect(unsupported.isError).toBe(true);
        expect(json(unsupported)).toContain("unsupported");
        yield* sql`UPDATE scheduled_mutation_receipts SET resource_json='{}' WHERE request_id=${original.request_id}`;
        expect((yield* replay()).isError).toBe(true);
        yield* sql`UPDATE scheduled_mutation_receipts SET resource_json=${original.resource_json} WHERE request_id=${original.request_id}`;
        expect((yield* replay()).structuredContent).toEqual(first.structuredContent);
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.auth.revokeSession(grant.sessionId);
        expect((yield* replay()).isError).toBe(true);
        yield* a.links.unlink(b.environmentId);
        yield* linkTo(a.links, b, "full-access");
        expect((yield* replay()).isError).toBe(true);
        expect(yield* sql`SELECT request_id FROM scheduled_mutation_receipts`).toHaveLength(3);
        expect((yield* b.scheduled.list()).tasks).toHaveLength(0);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  60000,
);

it.live(
  "Grok382-G3: deleted native acceptance retains every original downstream contributor",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const options = { productionKernel: true, attachmentRequests: true };
        const a = yield* makeNode(
          descriptorOf("history-chain382-a", "A", { peerAuthority: 1 }),
          "disk",
          options,
        );
        const b = yield* makeNode(
          descriptorOf("history-chain382-b", "B", { peerAuthority: 1 }),
          "disk",
          options,
        );
        const c = yield* makeNode(
          descriptorOf("history-chain382-c", "C", { peerAuthority: 1 }),
          "disk",
          options,
        );
        yield* linkTo(a.links, b, "full-access");
        yield* linkTo(b.links, c, "full-access");
        const parent = ThreadId.make("history-chain382-parent");
        yield* seedParent(a, parent);
        yield* a.forwarding.call(
          caller(a, parent),
          ProjectToolkit.tools.t3_project_list,
          b.environmentId,
          {},
        );
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({
          ...grant,
          onward: true,
          resourceScope: "environment-resources",
        });
        const first = yield* delegate(a, b, parent, "history-chain382-a-b");
        yield* activate(b, first.childId);
        yield* b.forwarding.call(
          caller(b, first.childId),
          ProjectToolkit.tools.t3_project_list,
          c.environmentId,
          {},
        );
        const onwardGrant = (yield* c.authority.grants)[0]!;
        yield* c.authority.setGrant({ ...onwardGrant, resourceScope: "environment-resources" });
        const second = yield* delegate(b, c, first.childId, "history-chain382-b-c");
        yield* activate(c, second.childId);
        const create = {
          title: "Original downstream acceptance",
          prompt: "Isolated chain fixture",
          enabled: false,
          schedule: { type: "interval", everyMs: 3600000 },
          clientRequestId: "history-chain382-create",
        };
        const accepted = yield* nativeCall(c, second.childId, "schedule_task", create);
        expect(accepted.isError, json(accepted)).not.toBe(true);
        const receipt = yield* Schema.decodeUnknownEffect(McpScheduledMutationReceipt)(
          accepted.structuredContent,
        );
        const removed = yield* nativeCall(c, second.childId, "delete_scheduled_task", {
          scheduledTaskId: receipt.scheduledTaskId,
          clientRequestId: "history-chain382-delete",
        });
        expect(removed.isError, json(removed)).not.toBe(true);
        expect(
          (yield* nativeCall(c, second.childId, "schedule_task", create)).structuredContent,
        ).toEqual(accepted.structuredContent);
        const sql = Context.get(c.database, SqlClient.SqlClient);
        const rows = yield* sql<{
          resource_json: string;
        }>`SELECT resource_json FROM scheduled_mutation_receipts`;
        expect(rows).toHaveLength(2);
        expect(rows.every((row) => row.resource_json.includes("authorityRecordId"))).toBe(true);
        yield* b.auth.revokeSession(grant.sessionId);
        expect((yield* nativeCall(c, second.childId, "schedule_task", create)).isError).toBe(true);
        expect((yield* c.scheduled.list()).tasks).toHaveLength(0);
        expect(yield* sql`SELECT request_id FROM scheduled_mutation_receipts`).toHaveLength(2);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  60000,
);

it.live(
  "reviewer382-r2: saved PNG replays through registered transport without its renderer",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const node = yield* makeNode(
          descriptorOf("reviewer382-r2-png", "PNG", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const parent = ThreadId.make("reviewer382-r2-png-parent");
        yield* seedParent(node, parent);
        const input = { save: true, includeImage: true, clientRequestId: "reviewer382-r2-save" };
        let screenshots = 0;
        const first = yield* Effect.scoped(
          Effect.gen(function* () {
            const ready = yield* Deferred.make<void>();
            yield* Stream.runForEach(
              yield* node.browser.connect({
                clientId: "reviewer382-r2-renderer",
                environmentId: node.environmentId,
              }),
              (event) =>
                event.type === "connected"
                  ? Deferred.succeed(ready, undefined)
                  : Effect.sync(() => {
                      screenshots++;
                    }).pipe(
                      Effect.andThen(
                        node.browser.respond({
                          clientId: "reviewer382-r2-renderer",
                          connectionId: event.connectionId,
                          requestId: event.request.requestId,
                          ok: true,
                          result: {
                            url: "https://isolated.example.test/",
                            title: "Fixture",
                            loading: false,
                            visibleText: "Fixture",
                            interactiveElements: [],
                            accessibilityTree: {},
                            consoleEntries: [],
                            networkEntries: [],
                            actionTimeline: [],
                            screenshot: {
                              mimeType: "image/png",
                              data: Buffer.from([112, 110, 103]).toString("base64"),
                              width: 1,
                              height: 1,
                            },
                          },
                        }),
                      ),
                    ),
            ).pipe(Effect.forkScoped);
            yield* Deferred.await(ready);
            return yield* nativeCall(node, parent, "preview_snapshot", input);
          }),
        );
        expect(first.isError, json(first)).not.toBe(true);
        const receipt = yield* decodeSavedPngReceipt(first.structuredContent);
        const fs = yield* FileSystem.FileSystem;
        expect(Array.from(yield* fs.readFile(receipt.artifact.screenshotPath))).toEqual([
          112, 110, 103,
        ]);
        const sql = Context.get(node.database, SqlClient.SqlClient);
        expect(
          yield* sql`SELECT state FROM mcp_request_receipts WHERE id=${receipt.operationId}`,
        ).toEqual([{ state: "completed" }]);
        const active = { ...node, ...(yield* node.restartSchedulerTransport) };
        const replay = yield* nativeCall(active, parent, "preview_snapshot", input);
        expect(replay.isError, json(replay)).not.toBe(true);
        expect(replay.structuredContent).toEqual(first.structuredContent);
        expect(replay.content.some((item) => item.type === "image")).toBe(true);
        expect(screenshots).toBe(1);
        const persisted = (yield* sql<{
          result_json: string;
        }>`SELECT result_json FROM mcp_request_receipts WHERE id=${receipt.operationId}`)[0]!;
        // A malformed imported binding is not a grant to read its stored path.
        yield* sql`UPDATE mcp_request_receipts SET result_json=json_set(result_json,'$.value.artifact.owner',${"f".repeat(64)}) WHERE id=${receipt.operationId}`;
        const unproved = yield* nativeCall(active, parent, "preview_snapshot", input);
        expect(unproved.isError).toBe(true);
        expect(unproved.content.some((item) => item.type === "image")).toBe(false);
        yield* sql`UPDATE mcp_request_receipts SET result_json=${persisted.result_json} WHERE id=${receipt.operationId}`;
        expect(
          (yield* nativeCall(active, parent, "preview_snapshot", input)).structuredContent,
        ).toEqual(first.structuredContent);
        const conflict = yield* nativeCall(active, parent, "preview_snapshot", {
          ...input,
          includeImage: false,
        });
        expect(conflict.isError).toBe(true);
        expect(json(conflict)).toContain("different input");
        yield* fs.writeFileString(receipt.artifact.screenshotPath, "Later independent bytes");
        const changed = yield* nativeCall(active, parent, "preview_snapshot", input);
        expect(changed.structuredContent).toEqual(first.structuredContent);
        expect(changed.content.some((item) => item.type === "image")).toBe(false);
        expect(yield* fs.readFileString(receipt.artifact.screenshotPath)).toBe(
          "Later independent bytes",
        );
        yield* fs.remove(receipt.artifact.screenshotPath);
        const missing = yield* nativeCall(active, parent, "preview_snapshot", input);
        expect(missing.structuredContent).toEqual(first.structuredContent);
        expect(missing.content.some((item) => item.type === "image")).toBe(false);
        expect(yield* fs.exists(receipt.artifact.screenshotPath)).toBe(false);
        const narrowed = yield* nativeCall(
          active,
          parent,
          "preview_snapshot",
          input,
          new Set(["orchestration"]),
        );
        expect(narrowed.isError).toBe(true);
        expect(json(narrowed)).not.toContain(receipt.artifact.screenshotPath);
        expect(screenshots).toBe(1);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30000,
);

it.live(
  "reviewer382-r2: manual admission rechecks a replacement schedule target",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const options = { productionKernel: true, attachmentRequests: true };
        const a = yield* makeNode(
          descriptorOf("reviewer382-r2-ma", "A", { peerAuthority: 1 }),
          "disk",
          options,
        );
        const b = yield* makeNode(
          descriptorOf("reviewer382-r2-mb", "B", { peerAuthority: 1 }),
          "disk",
          options,
        );
        const c = yield* makeNode(
          descriptorOf("reviewer382-r2-mc", "C", { peerAuthority: 1 }),
          "disk",
          options,
        );
        yield* linkTo(a.links, b, "full-access");
        yield* linkTo(c.links, b, "full-access");
        const parent = ThreadId.make("reviewer382-r2-ma-parent"),
          other = ThreadId.make("reviewer382-r2-mc-parent"),
          local = ThreadId.make("reviewer382-r2-mb-parent");
        yield* seedParent(a, parent);
        yield* seedParent(c, other);
        yield* seedParent(b, local);
        const fields = {
          title: "Original",
          prompt: "Do nothing",
          enabled: false,
          schedule: { type: "interval", everyMs: 3600000 },
        };
        const created = yield* nativeCall(b, local, "schedule_task", {
          ...fields,
          bindToCurrentThread: false,
          clientRequestId: "reviewer382-r2-local-create",
        });
        expect(created.isError, json(created)).not.toBe(true);
        const receipt = yield* Schema.decodeUnknownEffect(McpScheduledMutationReceipt)(
          created.structuredContent,
        );
        const foreign = yield* nativeCall(c, other, "schedule_task", {
          ...fields,
          environmentId: b.environmentId,
          projectId: b.project.id,
          clientRequestId: "reviewer382-r2-foreign-create",
        });
        expect(foreign.isError, json(foreign)).not.toBe(true);
        const foreignReceipt = yield* Schema.decodeUnknownEffect(McpScheduledMutationReceipt)(
          foreign.structuredContent,
        );
        const replacement = (yield* b.scheduled.list()).tasks.find(
          (t) => t.id === foreignReceipt.scheduledTaskId,
        )!;
        expect(replacement.linkOrigin).toBeDefined();
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const acceptedBefore =
          yield* sql`SELECT command_id FROM orchestration_command_receipts ORDER BY command_id`;
        const entered = yield* Deferred.make<void>(),
          release = yield* Deferred.make<void>();
        const actual = b.scheduled.runNow;
        const observer = vi
          .spyOn(b.scheduled, "runNow")
          .mockImplementation((...args: Parameters<typeof actual>) =>
            Effect.gen(function* () {
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(release);
              return yield* actual(...args);
            }),
          );
        const pending = yield* nativeCall(a, parent, "run_scheduled_task_now", {
          environmentId: b.environmentId,
          taskId: receipt.scheduledTaskId,
          clientRequestId: "reviewer382-r2-manual",
        }).pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        yield* b.scheduled.delete({ id: receipt.scheduledTaskId });
        yield* b.scheduled.upsert({ ...replacement, id: receipt.scheduledTaskId });
        yield* Deferred.succeed(release, undefined);
        const result = yield* Fiber.join(pending).pipe(
          Effect.ensuring(Effect.sync(() => observer.mockRestore())),
        );
        expect(result.isError, json(result)).toBe(true);
        expect(
          (yield* b.scheduled.list()).tasks.find((t) => t.id === receipt.scheduledTaskId)?.runCount,
        ).toBe(0);
        expect(
          yield* sql`SELECT command_id FROM orchestration_command_receipts ORDER BY command_id`,
        ).toEqual(acceptedBefore);
        expect(yield* sql`SELECT request_id FROM scheduled_manual_requests`).toHaveLength(0);
        const fresh = yield* nativeCall(a, parent, "run_scheduled_task_now", {
          environmentId: b.environmentId,
          taskId: receipt.scheduledTaskId,
          clientRequestId: "reviewer382-r2-fresh-manual",
        });
        expect(fresh.isError).toBe(true);
        expect(json(fresh)).toContain("resource scope");
        expect(yield* sql`SELECT request_id FROM scheduled_manual_requests`).toHaveLength(0);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30000,
);

it.live(
  "manual382-S4: replacement after live target validation cannot cross the native fence",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const options = { productionKernel: true, attachmentRequests: true };
        const a = yield* makeNode(
          descriptorOf("reviewer382-r2-ma", "A", { peerAuthority: 1 }),
          "disk",
          options,
        );
        const b = yield* makeNode(
          descriptorOf("reviewer382-r2-mb", "B", { peerAuthority: 1 }),
          "disk",
          options,
        );
        const c = yield* makeNode(
          descriptorOf("reviewer382-r2-mc", "C", { peerAuthority: 1 }),
          "disk",
          options,
        );
        yield* linkTo(a.links, b, "full-access");
        yield* linkTo(c.links, b, "full-access");
        const parent = ThreadId.make("reviewer382-r2-ma-parent"),
          other = ThreadId.make("reviewer382-r2-mc-parent"),
          local = ThreadId.make("reviewer382-r2-mb-parent");
        yield* seedParent(a, parent);
        yield* seedParent(c, other);
        yield* seedParent(b, local);
        const fields = {
          title: "Original",
          prompt: "Do nothing",
          enabled: false,
          schedule: { type: "interval", everyMs: 3600000 },
        };
        const created = yield* nativeCall(b, local, "schedule_task", {
          ...fields,
          bindToCurrentThread: false,
          clientRequestId: "reviewer382-r2-local-create",
        });
        expect(created.isError, json(created)).not.toBe(true);
        const receipt = yield* Schema.decodeUnknownEffect(McpScheduledMutationReceipt)(
          created.structuredContent,
        );
        const foreign = yield* nativeCall(c, other, "schedule_task", {
          ...fields,
          environmentId: b.environmentId,
          projectId: b.project.id,
          clientRequestId: "reviewer382-r2-foreign-create",
        });
        expect(foreign.isError, json(foreign)).not.toBe(true);
        const foreignReceipt = yield* Schema.decodeUnknownEffect(McpScheduledMutationReceipt)(
          foreign.structuredContent,
        );
        const replacement = (yield* b.scheduled.list()).tasks.find(
          (t) => t.id === foreignReceipt.scheduledTaskId,
        )!;
        expect(replacement.linkOrigin).toBeDefined();
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const acceptedBefore =
          yield* sql`SELECT command_id FROM orchestration_command_receipts ORDER BY command_id`;
        const entered = yield* Deferred.make<void>(),
          release = yield* Deferred.make<void>();
        const actual = b.authority.checkTarget;
        let targetChecks = 0;
        const observer = vi
          .spyOn(b.authority, "checkTarget")
          .mockImplementation((...args: Parameters<typeof actual>) =>
            Effect.gen(function* () {
              yield* actual(...args);
              if (
                args[1].type === "schedule" &&
                args[1].taskId === receipt.scheduledTaskId &&
                ++targetChecks === 2
              ) {
                yield* Deferred.succeed(entered, undefined);
                yield* Deferred.await(release);
              }
            }),
          );
        const pending = yield* nativeCall(a, parent, "run_scheduled_task_now", {
          environmentId: b.environmentId,
          taskId: receipt.scheduledTaskId,
          clientRequestId: "reviewer382-r2-manual",
        }).pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        yield* b.scheduled.delete({ id: receipt.scheduledTaskId });
        yield* b.scheduled.upsert({ ...replacement, id: receipt.scheduledTaskId });
        yield* Deferred.succeed(release, undefined);
        const result = yield* Fiber.join(pending).pipe(
          Effect.ensuring(Effect.sync(() => observer.mockRestore())),
        );
        expect(result.isError, json(result)).toBe(true);
        expect(json(result)).toContain("binding changed");
        expect(
          (yield* b.scheduled.list()).tasks.find((t) => t.id === receipt.scheduledTaskId)?.runCount,
        ).toBe(0);
        expect(
          yield* sql`SELECT command_id FROM orchestration_command_receipts ORDER BY command_id`,
        ).toEqual(acceptedBefore);
        expect(yield* sql`SELECT request_id FROM scheduled_manual_requests`).toHaveLength(0);
        const fresh = yield* nativeCall(a, parent, "run_scheduled_task_now", {
          environmentId: b.environmentId,
          taskId: receipt.scheduledTaskId,
          clientRequestId: "reviewer382-r2-fresh-manual",
        });
        expect(fresh.isError).toBe(true);
        expect(json(fresh)).toContain("resource scope");
        expect(yield* sql`SELECT request_id FROM scheduled_manual_requests`).toHaveLength(0);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30000,
);

it.live(
  "png382-authority: renderer-free remote replay retains its original source contributor",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const options = { productionKernel: true, attachmentRequests: true };
        const c = yield* makeNode(
          descriptorOf("png-chain382-c", "C", { peerAuthority: 1 }),
          "disk",
          options,
        );
        const a = yield* makeNode(
          descriptorOf("png-chain382-a", "A", { peerAuthority: 1 }),
          "disk",
          options,
        );
        const b = yield* makeNode(
          descriptorOf("png-chain382-b", "B", { peerAuthority: 1 }),
          "disk",
          options,
        );
        yield* linkTo(c.links, a, "full-access");
        yield* linkTo(a.links, b, "full-access");
        const root = ThreadId.make("png-chain382-root");
        yield* seedParent(c, root);
        yield* c.forwarding.call(
          caller(c, root),
          ProjectToolkit.tools.t3_project_list,
          a.environmentId,
          {},
        );
        const sourceGrant = (yield* a.authority.grants)[0]!;
        yield* a.authority.setGrant({
          ...sourceGrant,
          preview: true,
          onward: true,
          resourceScope: "environment-resources",
        });
        const first = yield* nativeCall(c, root, "delegate_task", {
          task: "Isolated PNG source",
          target: {
            environmentId: a.environmentId,
            projectId: a.project.id,
            providerInstanceId: instanceId,
            model: "gpt-5.4",
          },
          mode: "async",
          clientRequestId: "png-chain382-first",
        });
        expect(first.isError, json(first)).not.toBe(true);
        const parent = yield* attachedChild(
          c,
          root,
          (yield* decodeTaskReceipt(first.structuredContent)).taskId,
        );
        yield* activate(a, parent);
        yield* a.forwarding.call(
          caller(a, parent),
          ProjectToolkit.tools.t3_project_list,
          b.environmentId,
          {},
        );
        const destinationGrant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({
          ...destinationGrant,
          preview: true,
          resourceScope: "environment-resources",
        });
        const second = yield* nativeCall(a, parent, "delegate_task", {
          task: "Isolated PNG context",
          target: {
            environmentId: b.environmentId,
            projectId: b.project.id,
            providerInstanceId: instanceId,
            model: "gpt-5.4",
          },
          mode: "async",
          clientRequestId: "png-chain382-second",
        });
        expect(second.isError, json(second)).not.toBe(true);
        const context = yield* attachedChild(
          a,
          parent,
          (yield* decodeTaskReceipt(second.structuredContent)).taskId,
        );
        yield* activate(b, context);
        const input = {
          environmentId: b.environmentId,
          contextThreadId: context,
          save: true,
          includeImage: true,
          clientRequestId: "png-chain382-save",
        };
        let snapshots = 0;
        const saved = yield* Effect.scoped(
          Effect.gen(function* () {
            const connected = yield* Deferred.make<void>();
            yield* Stream.runForEach(
              yield* b.browser.connect({
                clientId: "png-chain382-renderer",
                environmentId: b.environmentId,
              }),
              (event) =>
                event.type === "connected"
                  ? Deferred.succeed(connected, undefined)
                  : Effect.gen(function* () {
                      snapshots++;
                      yield* b.browser.respond({
                        clientId: "png-chain382-renderer",
                        connectionId: event.connectionId,
                        requestId: event.request.requestId,
                        ok: true,
                        result: {
                          url: "https://isolated-chain.example.test/",
                          title: "Fixture",
                          loading: false,
                          visibleText: "Isolated fixture",
                          interactiveElements: [],
                          accessibilityTree: {},
                          consoleEntries: [],
                          networkEntries: [],
                          actionTimeline: [],
                          screenshot: {
                            mimeType: "image/png",
                            data: Buffer.from([112, 110, 103]).toString("base64"),
                            width: 1,
                            height: 1,
                          },
                        },
                      });
                    }),
            ).pipe(Effect.forkScoped);
            yield* Deferred.await(connected);
            return yield* nativeCall(a, parent, "preview_snapshot", input);
          }),
        );
        expect(saved.isError, json(saved)).not.toBe(true);
        const receipt = yield* decodeSavedPngReceipt(saved.structuredContent);
        expect((yield* nativeCall(a, parent, "preview_snapshot", input)).structuredContent).toEqual(
          receipt,
        );
        yield* a.auth.revokeSession(sourceGrant.sessionId);
        const denied = yield* nativeCall(a, parent, "preview_snapshot", input);
        expect(denied.isError).toBe(true);
        expect(denied.content.some((item) => item.type === "image")).toBe(false);
        expect(snapshots).toBe(1);
        const fs = yield* FileSystem.FileSystem;
        expect(Array.from(yield* fs.readFile(receipt.artifact.screenshotPath))).toEqual([
          112, 110, 103,
        ]);
        expect(yield* fs.readDirectory(a.config.browserArtifactsDir)).toHaveLength(1);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  60000,
);

it.live(
  "png382-authority: absent destination registration and unauthenticated contexts fail before rendering",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("png-minimal382-a", "A", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf("png-minimal382-b", "B", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("png-minimal382-parent"),
          context = ThreadId.make("png-minimal382-context");
        yield* seedParent(a, parent);
        yield* seedParent(b, context);
        const unsupported = yield* nativeCall(a, parent, "preview_snapshot", {
          environmentId: b.environmentId,
          contextThreadId: context,
          save: true,
          clientRequestId: "png-minimal382-save",
        });
        expect(unsupported.isError).toBe(true);
        expect(json(unsupported)).toContain("does not support saved-preview authorization");
        const fs = yield* FileSystem.FileSystem;
        expect(yield* fs.exists(a.config.browserArtifactsDir)).toBe(false);
        const http = yield* HttpClient.HttpClient;
        expect(
          (yield* http.execute(HttpClientRequest.get(`${a.url}/mcp/peer-control/saved-preview`)))
            .status,
        ).toBe(401);
        const credential = yield* a.registry.issue({
          threadId: parent,
          providerInstanceId: instanceId,
        });
        const body = {
          version: 1,
          purpose: "saved-preview-authorization",
          environmentId: a.environmentId,
          generation: "f".repeat(64),
          contextThreadId: parent,
          operationId: "a".repeat(64),
        };
        const refused = yield* http.execute(
          HttpClientRequest.post(`${a.url}/mcp/peer-control/saved-preview`).pipe(
            HttpClientRequest.setHeader("authorization", credential.config.authorizationHeader),
            HttpClientRequest.setBody(HttpBody.text(json(body), "application/json")),
          ),
        );
        expect(refused.status).toBe(403);
        expect(yield* refused.json).toEqual({
          code: "capability_denied",
          message:
            "Saved-preview authorization needs its authenticated original destination generation.",
        });
        const malformedEnvelope = yield* http.execute(
          HttpClientRequest.post(`${a.url}/mcp/peer-control/saved-preview`).pipe(
            HttpClientRequest.setHeader("authorization", credential.config.authorizationHeader),
            HttpClientRequest.setHeader(PeerAuthority.AUTHORITY_HEADER, "{"),
            HttpClientRequest.setBody(HttpBody.text(json(body), "application/json")),
          ),
        );
        expect(malformedEnvelope.status).toBe(403);
        expect(yield* malformedEnvelope.json).toEqual({
          code: "invalid_request",
          message: "Invalid peer authority envelope.",
        });
        const malformedInput = yield* http.execute(
          HttpClientRequest.post(`${a.url}/mcp/peer-control/saved-preview`).pipe(
            HttpClientRequest.setHeader("authorization", credential.config.authorizationHeader),
            HttpClientRequest.setBody(HttpBody.text("{}", "application/json")),
          ),
        );
        expect(malformedInput.status).toBe(403);
        expect(yield* malformedInput.json).toEqual({
          code: "invalid_request",
          message: "Invalid saved-preview authorization context.",
        });
        expect(yield* fs.exists(a.config.browserArtifactsDir)).toBe(false);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30000,
);

const decodeDiscardUpload = Schema.decodeUnknownEffect(PeerAttachmentUploadResult);
it.live(
  "S6: remote discard recovers lost native acceptance after renewal/reconstruction and retains current authority",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const options = { productionKernel: true, attachmentRequests: true };
        const a = yield* makeNode(
          descriptorOf("discard-s6-a", "A", { peerAuthority: 1 }),
          "disk",
          options,
        );
        const b = yield* makeNode(
          descriptorOf("discard-s6-b", "B", { peerAuthority: 1 }),
          "disk",
          options,
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("discard-s6-parent");
        yield* seedParent(a, parent);
        const metadata = {
          type: "file",
          name: "discard.pdf",
          mimeType: "application/pdf",
          sizeBytes: 4,
        };
        const prepared = yield* nativeCall(a, parent, "t3_attachment_prepare_upload", {
          environmentId: b.environmentId,
          upload: metadata,
          digest: "9f64a747e1b97f131fabb6b447296c9b6f0201e79fb3c5356e6c77e89b6a806a",
          clientRequestId: "discard-s6-upload",
        });
        expect(prepared.isError, json(prepared)).not.toBe(true);
        const issued = yield* decodeDiscardUpload(prepared.structuredContent);
        const http = yield* HttpClient.HttpClient;
        expect(
          (yield* http.execute(
            HttpClientRequest.post(`${b.url}${issued.relativeUrl}`).pipe(
              HttpClientRequest.setBody(HttpBody.uint8Array(new Uint8Array([1, 2, 3, 4]))),
            ),
          )).status,
        ).toBe(204);
        const input = {
          environmentId: b.environmentId,
          attachmentId: issued.attachmentId,
          clientRequestId: "discard-s6-accept",
        };
        const fs = yield* FileSystem.FileSystem;
        const path = resolveAttachmentPathById({
          attachmentsDir: b.config.attachmentsDir,
          attachmentId: issued.attachmentId,
        })!;
        expect(Array.from(yield* fs.readFile(path))).toEqual([1, 2, 3, 4]);
        yield* Ref.set(a.dropCommittedToolReply, "t3_attachment_discard");
        const lost = yield* nativeCall(a, parent, "t3_attachment_discard", input);
        expect(lost.isError).toBe(true);
        expect(json(lost)).toContain("Isolated committed tool reply loss");
        expect(Array.from(yield* fs.readFile(path))).toEqual([1, 2, 3, 4]);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const accepted =
          yield* sql`SELECT consumer,discard_receipt,discard_cleanup FROM attachment_staging WHERE id=${issued.attachmentId}`;
        expect(accepted).toMatchObject([{ consumer: "discarded", discard_cleanup: "retained" }]);
        const rebuilt = { ...b, ...(yield* b.restartSchedulerTransport) };
        yield* Context.get(
          a.database,
          SqlClient.SqlClient,
        )`UPDATE peer_environment_links SET urls_json=${json([rebuilt.url])} WHERE environment_id=${b.environmentId}`;
        const recovered = yield* nativeCall(a, parent, "t3_attachment_discard", input);
        expect(recovered.isError, json(recovered)).not.toBe(true);
        expect(recovered.structuredContent).toEqual({});
        expect(
          yield* sql`SELECT consumer,discard_receipt,discard_cleanup FROM attachment_staging WHERE id=${issued.attachmentId}`,
        ).toEqual(accepted);
        expect(Array.from(yield* fs.readFile(path))).toEqual([1, 2, 3, 4]);
        const rival = ThreadId.make("discard-s6-rival");
        yield* seedParent(a, rival);
        expect((yield* nativeCall(a, rival, "t3_attachment_discard", input)).isError).toBe(true);
        expect(
          (yield* nativeCall(a, parent, "t3_attachment_discard", {
            ...input,
            attachmentId: "pending-unrelated.pdf",
          })).isError,
        ).toBe(true);
        yield* fs.writeFile(path, new Uint8Array([9, 8, 7, 6]));
        expect((yield* nativeCall(a, parent, "t3_attachment_discard", input)).isError).not.toBe(
          true,
        );
        expect(Array.from(yield* fs.readFile(path))).toEqual([9, 8, 7, 6]);
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.auth.revokeSession(grant.sessionId);
        expect((yield* nativeCall(a, parent, "t3_attachment_discard", input)).isError).toBe(true);
        yield* a.links.unlink(b.environmentId);
        yield* linkTo(a.links, rebuilt, "full-access");
        const enrolled = yield* nativeCall(a, parent, "t3_project_list", {
          environmentId: b.environmentId,
        });
        expect(enrolled.isError, json(enrolled)).not.toBe(true);
        expect((yield* nativeCall(a, parent, "t3_attachment_discard", input)).isError).toBe(true);
        expect(Array.from(yield* fs.readFile(path))).toEqual([9, 8, 7, 6]);
        expect(
          yield* sql`SELECT consumer,discard_receipt,discard_cleanup FROM attachment_staging WHERE id=${issued.attachmentId}`,
        ).toEqual(accepted);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30000,
);

it.live(
  "S8: registered local keyed replay and deliberate unkeyed stops preserve native recording evidence",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const node = yield* makeNode(
          descriptorOf("recording382-local", "Local", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const parent = ThreadId.make("recording382-local-parent");
        yield* seedParent(node, parent);
        const bytes = new Uint8Array([26, 69, 223, 163, 129, 1, 66, 134, 129, 1]);
        const uploaded = yield* AttachmentUpload.issueAttachmentUploadUrl({
          type: "file",
          name: "recording.webm",
          mimeType: "video/webm",
          sizeBytes: bytes.byteLength,
        }).pipe(
          Effect.provideService(Config.ServerConfig, node.config),
          Effect.provideService(
            Secrets.ServerSecretStore,
            Context.get(node.services, Secrets.ServerSecretStore),
          ),
        );
        const http = yield* HttpClient.HttpClient;
        const post = http.execute(
          HttpClientRequest.post(`${node.url}${uploaded.relativeUrl}`).pipe(
            HttpClientRequest.setBody(HttpBody.uint8Array(bytes)),
          ),
        );
        expect((yield* post).status).toBe(204);
        const ready = yield* Deferred.make<void>();
        let rendererCalls = 0;
        yield* Stream.runForEach(
          yield* node.browser.connect({
            clientId: "recording382-local-renderer",
            environmentId: node.environmentId,
          }),
          (event) =>
            event.type === "connected"
              ? Deferred.succeed(ready, undefined)
              : Effect.gen(function* () {
                  rendererCalls++;
                  expect(event.request.operation).toBe("recordingStop");
                  yield* node.browser.respond({
                    clientId: "recording382-local-renderer",
                    connectionId: event.connectionId,
                    requestId: event.request.requestId,
                    ok: true,
                    result: {
                      id: "local-renderer-recording",
                      tabId: "local-recording-tab",
                      path: "/isolated-renderer/local.webm",
                      uploadedAttachmentId: uploaded.attachmentId,
                      mimeType: "video/webm",
                      sizeBytes: bytes.byteLength,
                      createdAt: "2026-10-18T00:00:00Z",
                    },
                  });
                }),
        ).pipe(Effect.forkScoped);
        yield* Deferred.await(ready);
        const input = { clientRequestId: "recording382-local-keyed" };
        const first = yield* nativeCall(node, parent, "preview_recording_stop", input);
        expect(first.isError, json(first)).not.toBe(true);
        const result = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ id: Schema.String, path: Schema.String }),
        )(first.structuredContent);
        const fs = yield* FileSystem.FileSystem;
        expect(Array.from(yield* fs.readFile(result.path))).toEqual(Array.from(bytes));
        yield* fs.writeFile(result.path, new Uint8Array([9, 8, 7, 6, 5, 4, 3, 2, 1, 0]));
        expect((yield* post).status).toBe(204);
        const rebuilt = { ...node, ...(yield* node.restartSchedulerTransport) };
        const replay = yield* nativeCall(rebuilt, parent, "preview_recording_stop", input);
        expect(replay.isError, json(replay)).not.toBe(true);
        expect(replay.structuredContent).toEqual(first.structuredContent);
        expect(rendererCalls).toBe(1);
        const unkeyed = yield* nativeCall(rebuilt, parent, "preview_recording_stop", {});
        expect(unkeyed.isError, json(unkeyed)).not.toBe(true);
        expect(unkeyed.structuredContent).toEqual(first.structuredContent);
        expect(rendererCalls).toBe(2); // A deliberate unkeyed action still reaches its renderer.
        expect(Array.from(yield* fs.readFile(result.path))).toEqual([9, 8, 7, 6, 5, 4, 3, 2, 1, 0]);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30000,
);

const decodePending369Content = Schema.decodeUnknownEffect(
  ThreadToolkit.tools.t3_pending_request_read.successSchema,
);
const decodePending369ClientResult = Schema.decodeUnknownEffect(RemotePendingRequestResult);
const decodePending369LocalTask = Schema.decodeUnknownEffect(
  OrchestratorToolkit.tools.delegate_task.successSchema,
);

const seedPending369 = (node: Node, threadId: ThreadId, suffix = "question") =>
  Effect.gen(function* () {
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

it.live(
  "pending369: authenticated native HTTP proves pair ancestry and replays one native answer",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(descriptorOf("pending369-a", "A", { peerAuthority: 1 }), "disk", {
          attachmentRequests: true,
          productionKernel: true,
        });
        const b = yield* makeNode(descriptorOf("pending369-b", "B", { peerAuthority: 1 }), "disk", {
          attachmentRequests: true,
          productionKernel: true,
        });
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("pending369-parent");
        yield* seedParent(a, parent);
        const child = yield* delegate(a, b, parent, "pending369-child");
        yield* activate(b, child.childId);
        const requestId = yield* seedPending369(b, child.childId);
        const args = { environmentId: b.environmentId, threadId: child.childId, requestId };
        const read = yield* nativeCall(a, parent, "t3_pending_request_read", args);
        expect(read.isError, json(read.content)).not.toBe(true);
        const content = yield* decodePending369Content(read.structuredContent);
        expect(content.kind).toBe("user-input");
        expect(content.revision).toBeGreaterThan(0);
        const answer = {
          ...args,
          expectedRevision: content.revision,
          answers: { color: "Blue" },
          clientRequestId: "pending369-answer",
        };
        const concurrent = yield* Effect.all(
          [
            nativeCall(a, parent, "t3_pending_request_respond", answer),
            nativeCall(a, parent, "t3_pending_request_respond", answer),
          ],
          { concurrency: "unbounded" },
        );
        const response = concurrent[0];
        expect(concurrent[1]?.structuredContent).toEqual(response.structuredContent);
        expect(response.isError, json(response.content)).not.toBe(true);
        const replay = yield* nativeCall(a, parent, "t3_pending_request_respond", answer);
        expect(replay.isError, json(replay.content)).not.toBe(true);
        expect(replay.structuredContent).toEqual(response.structuredContent);
        // Rebuild the destination's actual disk request runtime and authenticated
        // transport. Renewed source credentials still recover native acceptance.
        const rebuilt = { ...b, ...(yield* b.restartSchedulerTransport) };
        yield* Context.get(
          a.database,
          SqlClient.SqlClient,
        )`UPDATE peer_environment_links SET urls_json=${json([rebuilt.url])} WHERE environment_id=${b.environmentId}`;
        expect(
          (yield* nativeCall(a, parent, "t3_pending_request_respond", answer)).structuredContent,
        ).toEqual(response.structuredContent);
        const changed = yield* nativeCall(a, parent, "t3_pending_request_respond", {
          ...answer,
          answers: { color: "Red" },
        });
        expect(changed.isError).toBe(true);
        const records = yield* b.threads.getThreadRecords(child.childId, [
          "runtimeRequests",
          "messages",
        ]);
        expect(records.runtimeRequests.find((r) => r.id === requestId)?.status).toBe("resolved");
        expect(records.messages.filter((m) => m.text === "Which color?\nBlue")).toHaveLength(1);
        const selfRequestId = yield* seedPending369(b, child.childId, "self");
        const self = yield* nativeCall(b, child.childId, "t3_pending_request_read", {
          threadId: child.childId,
          requestId: selfRequestId,
        });
        expect(self.isError).toBe(true);
        const dismissArgs = { ...args, requestId: selfRequestId };
        const dismissRead = yield* nativeCall(a, parent, "t3_pending_request_read", dismissArgs);
        const dismissContent = yield* decodePending369Content(dismissRead.structuredContent);
        const dismissed = yield* nativeCall(a, parent, "t3_pending_request_dismiss", {
          ...dismissArgs,
          expectedRevision: dismissContent.revision,
          clientRequestId: "explicit-dismiss",
        });
        expect(dismissed.isError, json(dismissed.content)).not.toBe(true);
        expect(
          (yield* b.threads.getThreadRecords(child.childId, [
            "runtimeRequests",
          ])).runtimeRequests.find((request) => request.id === selfRequestId),
        ).toMatchObject({ status: "resolved", decision: "cancel" });
        const shapeRequest = yield* seedPending369(b, child.childId, "shape-revision");
        const shapeArgs = { ...args, requestId: shapeRequest };
        const shapeRead = yield* decodePending369Content(
          (yield* nativeCall(a, parent, "t3_pending_request_read", shapeArgs)).structuredContent,
        );
        const shapeItem = (yield* b.threads.getThreadRecords(child.childId, [
          "turnItems",
        ])).turnItems.find(
          (item) => item.type === "user_input_request" && item.requestId === shapeRequest,
        );
        if (shapeItem?.type !== "user_input_request")
          return yield* Effect.die("Expected current question shape");
        const shapeNow = yield* DateTime.now;
        yield* b.sink.write({
          events: [
            {
              id: EventId.make("pending369-question-shape-update"),
              type: "turn-item.updated",
              threadId: child.childId,
              occurredAt: shapeNow,
              payload: {
                ...shapeItem,
                questions: shapeItem.questions.map((question) => ({
                  ...question,
                  question: "Which updated color?",
                })),
              },
            },
          ],
        });
        const staleShape = yield* nativeCall(a, parent, "t3_pending_request_respond", {
          ...shapeArgs,
          expectedRevision: shapeRead.revision,
          answers: { color: "White" },
          clientRequestId: "pending369-old-shape",
        });
        expect(staleShape.isError, json(staleShape.content)).toBe(true);
        const freshShape = yield* decodePending369Content(
          (yield* nativeCall(a, parent, "t3_pending_request_read", shapeArgs)).structuredContent,
        );
        expect(freshShape.revision).toBeGreaterThan(shapeRead.revision!);
        expect(
          (yield* nativeCall(a, parent, "t3_pending_request_respond", {
            ...shapeArgs,
            expectedRevision: freshShape.revision,
            answers: { color: "White" },
            clientRequestId: "pending369-new-shape",
          })).isError,
        ).not.toBe(true);
        expect(
          (yield* b.threads.getThreadRecords(child.childId, ["messages"])).messages.filter(
            (message) => message.text === "Which updated color?\nWhite",
          ),
        ).toHaveLength(1);
        const protectedRequest = yield* seedPending369(b, child.childId, "current-policy");
        const protectedArgs = { ...args, requestId: protectedRequest };
        const protectedRead = yield* nativeCall(
          a,
          parent,
          "t3_pending_request_read",
          protectedArgs,
        );
        const protectedContent = yield* decodePending369Content(protectedRead.structuredContent);
        const protectedAnswer = {
          ...protectedArgs,
          expectedRevision: protectedContent.revision,
          answers: { color: "Black" },
          clientRequestId: "pending369-current-policy",
        };
        const beforePolicy = yield* b.threads.getThreadRecords(child.childId, [
          "messages",
          "runtimeRequests",
        ]);
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("pending369-narrow-mode"),
          threadId: parent,
          runtimeMode: "auto",
        });
        expect(
          (yield* nativeCall(a, parent, "t3_pending_request_respond", protectedAnswer)).isError,
        ).toBe(true);
        expect((yield* nativeCall(a, parent, "t3_pending_request_respond", answer)).isError).toBe(
          true,
        );
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("pending369-restore-mode"),
          threadId: parent,
          runtimeMode: "full-access",
        });
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.auth.revokeSession(grant.sessionId);
        expect(
          (yield* nativeCall(a, parent, "t3_pending_request_read", protectedArgs)).isError,
        ).toBe(true);
        expect(
          (yield* nativeCall(a, parent, "t3_pending_request_respond", protectedAnswer)).isError,
        ).toBe(true);
        expect((yield* nativeCall(a, parent, "t3_pending_request_respond", answer)).isError).toBe(
          true,
        );
        const afterPolicy = yield* b.threads.getThreadRecords(child.childId, [
          "messages",
          "runtimeRequests",
        ]);
        expect(afterPolicy.messages).toEqual(beforePolicy.messages);
        expect(afterPolicy.runtimeRequests).toEqual(beforePolicy.runtimeRequests);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "pending369: grandparent routes A-B-C without destination profiles and denies unrelated or stale actions",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("pending369-grand-a", "A", { peerAuthority: 1 }),
          "disk",
          { attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf("pending369-grand-b", "B", { peerAuthority: 1 }),
          "disk",
          { attachmentRequests: true },
        );
        const c = yield* makeNode(
          descriptorOf("pending369-grand-c", "C", { peerAuthority: 1 }),
          "disk",
          { attachmentRequests: true },
        );
        yield* linkTo(a.links, b, "full-access");
        yield* linkTo(b.links, c, "full-access");
        const parent = ThreadId.make("pending369-grandparent");
        yield* seedParent(a, parent);
        yield* a.forwarding.call(
          caller(a, parent),
          ProjectToolkit.tools.t3_project_list,
          b.environmentId,
          {},
        );
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, onward: true });
        const ab = yield* delegate(a, b, parent, "pending369-ab");
        yield* activate(b, ab.childId);
        const bc = yield* delegate(b, c, ab.childId, "pending369-bc");
        yield* activate(c, bc.childId);
        const requestId = yield* seedPending369(c, bc.childId);
        const requestTarget = { environmentId: c.environmentId, threadId: bc.childId };
        const requestRoute = [
          { environmentId: b.environmentId, threadId: ab.childId },
          requestTarget,
        ];
        const args = { threadId: parent, requestId, requestTarget, requestRoute };
        const read = yield* nativeCall(a, parent, "t3_pending_request_read", args);
        expect(read.isError, json(read.content)).not.toBe(true);
        const content = yield* decodePending369Content(read.structuredContent);
        const unrelated = ThreadId.make("pending369-unrelated");
        yield* seedParent(a, unrelated);
        expect(
          (yield* nativeCall(a, unrelated, "t3_pending_request_respond", {
            ...args,
            expectedRevision: content.revision,
            answers: { color: "Red" },
            clientRequestId: "unrelated",
          })).isError,
        ).toBe(true);
        expect(
          (yield* nativeCall(a, parent, "t3_pending_request_respond", {
            ...args,
            expectedRevision: 0,
            answers: { color: "Red" },
            clientRequestId: "stale",
          })).isError,
        ).toBe(true);
        const good = yield* nativeCall(a, parent, "t3_pending_request_respond", {
          ...args,
          expectedRevision: content.revision,
          answers: { color: "Green" },
          clientRequestId: "grandparent",
        });
        expect(good.isError, json(good.content)).not.toBe(true);
        expect(
          (yield* c.threads.getThreadRecords(bc.childId, ["messages"])).messages.filter(
            (m) => m.text === "Which color?\nGreen",
          ),
        ).toHaveLength(1);
        expect(yield* c.links.list).toEqual([]);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "pending369: source client derives its actor from a real current session and preserves native action identity",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("pending369-human-a", "A", { peerAuthority: 1 }),
          "disk",
          { attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf("pending369-human-b", "B", { peerAuthority: 1 }),
          "disk",
          { attachmentRequests: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("pending369-human-parent");
        yield* seedParent(a, parent);
        yield* a.forwarding.call(
          caller(a, parent),
          ProjectToolkit.tools.t3_project_list,
          b.environmentId,
          {},
        );
        const initialGrant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...initialGrant, onward: true });
        const child = yield* delegate(a, b, parent, "pending369-human-child");
        yield* activate(b, child.childId);
        const requestId = yield* seedPending369(b, child.childId);
        const sessionId = AuthSessionId.make("pending369-real-client-session");
        const now = yield* DateTime.now;
        const auth = yield* AuthSessions.layer.pipe(
          Layer.provide(Layer.succeedContext(a.database)),
          Layer.build,
        );
        const sessions = Context.get(auth, AuthSessions.AuthSessionRepository);
        yield* sessions.create({
          sessionId,
          subject: "isolated-client",
          method: "browser-session-cookie",
          scopes: ["orchestration:read", "orchestration:operate"],
          client: {
            label: "Isolated source client",
            ipAddress: null,
            userAgent: null,
            deviceType: "unknown",
            os: null,
            browser: null,
          },
          issuedAt: now,
          expiresAt: DateTime.add(now, { hours: 1 }),
        });
        const base = {
          parentThreadId: parent,
          taskId: child.taskId,
          requestId,
          requestTarget: { environmentId: b.environmentId, threadId: child.childId },
        };
        const unrelated = ThreadId.make("pending369-unrelated-destination");
        yield* seedParent(b, unrelated);
        const unrelatedRequest = yield* seedPending369(b, unrelated);
        const unrelatedRead = yield* a.pendingRequests
          .fromTask(sessionId, {
            ...base,
            requestId: unrelatedRequest,
            requestTarget: { environmentId: b.environmentId, threadId: unrelated },
            operation: { type: "read" },
          })
          .pipe(Effect.result);
        expect(unrelatedRead._tag).toBe("Failure");
        const read = yield* a.pendingRequests.fromTask(sessionId, {
          ...base,
          operation: { type: "read" },
        });
        if (!("kind" in read) || read.revision === undefined)
          return yield* Effect.die("Missing qualified read");
        const action = {
          ...base,
          operation: {
            type: "respond" as const,
            clientRequestId: "human369-answer",
            expectedRevision: read.revision,
            answers: { color: "Orange" },
          },
        };
        const accepted = yield* a.pendingRequests.fromTask(sessionId, action);
        expect(yield* a.pendingRequests.fromTask(sessionId, action)).toEqual(accepted);
        expect(
          (yield* b.threads.getThreadRecords(child.childId, ["messages"])).messages.filter(
            (m) => m.text === "Which color?\nOrange",
          ),
        ).toHaveLength(1);
        yield* linkTo(b.links, a, "full-access");
        const sourceGrant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...sourceGrant, onward: true });
        const back = yield* delegate(
          b,
          a,
          child.childId,
          "pending369-return-to-earlier-environment",
        );
        yield* activate(a, back.childId);
        const backRequest = yield* seedPending369(a, back.childId);
        const nativeRead = yield* nativeCall(a, parent, "t3_pending_request_read", {
          threadId: back.childId,
          requestId: backRequest,
        });
        expect(nativeRead.isError, json(nativeRead.content)).not.toBe(true);
        const backBase = {
          ...base,
          requestId: backRequest,
          requestTarget: { environmentId: a.environmentId, threadId: back.childId },
          requestRoute: [{ environmentId: a.environmentId, threadId: back.childId }],
        };
        const backRead = yield* a.pendingRequests.fromTask(sessionId, {
          ...backBase,
          operation: { type: "read" },
        });
        if (!("kind" in backRead) || backRead.revision === undefined)
          return yield* Effect.die("Missing returned descendant request");
        const backAnswer = {
          ...backBase,
          operation: {
            type: "respond" as const,
            expectedRevision: backRead.revision,
            clientRequestId: "human369-back",
            answers: { color: "Silver" },
          },
        };
        const backAccepted = yield* a.pendingRequests.fromTask(sessionId, backAnswer);
        expect(yield* a.pendingRequests.fromTask(sessionId, backAnswer)).toEqual(backAccepted);
        expect(
          (yield* a.threads.getThreadRecords(back.childId, ["messages"])).messages.filter(
            (message) => message.text === "Which color?\nSilver",
          ),
        ).toHaveLength(1);
        yield* sessions.revoke({ sessionId, revokedAt: now });
        const denied = yield* a.pendingRequests.fromTask(sessionId, action).pipe(Effect.result);
        expect(denied._tag).toBe("Failure");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "pending369: local-equivalent remote approval commits once and the real native effect calls its provider once",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("pending369-approve-a", "A", { peerAuthority: 1 }),
          "disk",
          { attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf("pending369-approve-b", "B", { peerAuthority: 1 }),
          "disk",
          { attachmentRequests: true, productionKernel: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("pending369-approve-parent");
        yield* seedParent(a, parent);
        const child = yield* delegate(a, b, parent, "pending369-approve-child");
        yield* activate(b, child.childId);
        const projection = yield* b.orchestrator.getThreadProjection(child.childId);
        const providerSessionId = projection.providerThreads[0]!.providerSessionId!;
        const now = yield* DateTime.now;
        const providerSession = {
          id: providerSessionId,
          driver,
          providerInstanceId: instanceId,
          status: "running" as const,
          cwd: process.cwd(),
          model: selection.model,
          capabilities: CodexProviderCapabilitiesV2,
          createdAt: now,
          updatedAt: now,
          lastError: null,
        };
        yield* b.sink.write({
          events: [
            {
              id: EventId.make("pending369-approve-session"),
              type: "provider-session.attached",
              threadId: child.childId,
              occurredAt: now,
              payload: providerSession,
            },
          ],
        });
        const requestId = RuntimeRequestId.make("pending369-approval");
        const nodeId = NodeId.make("pending369-approval-node");
        const common = { threadId: child.childId, nodeId, occurredAt: now };
        yield* b.sink.write({
          events: [
            {
              ...common,
              id: EventId.make("pending369-approval-node"),
              type: "node.updated",
              payload: {
                id: nodeId,
                threadId: child.childId,
                runId: null,
                parentNodeId: null,
                rootNodeId: nodeId,
                kind: "approval_request",
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
              id: EventId.make("pending369-approval-request"),
              type: "runtime-request.updated",
              payload: {
                id: requestId,
                nodeId,
                providerTurnId: null,
                nativeRequestRef: null,
                kind: "command",
                status: "pending",
                responseCapability: { type: "live", providerSessionId },
                createdAt: now,
                resolvedAt: null,
              },
            },
            {
              ...common,
              id: EventId.make("pending369-approval-item"),
              type: "turn-item.updated",
              payload: {
                id: TurnItemId.make("pending369-approval-item"),
                type: "approval_request",
                threadId: child.childId,
                runId: null,
                nodeId,
                providerThreadId: null,
                providerTurnId: null,
                nativeItemRef: null,
                parentItemId: null,
                ordinal: 1,
                status: "waiting",
                title: null,
                startedAt: now,
                completedAt: null,
                updatedAt: now,
                requestId,
                requestKind: "command",
                prompt: "Run the requested command?",
                options: [
                  { decision: "accept", label: "Approve" },
                  { decision: "decline", label: "Deny" },
                ],
              },
            },
          ],
        });
        yield* a.orchestrator
          .streamStoredEventsFrom({
            threadId: parent,
            afterSequence: 0,
            eventType: "subagent.updated",
          })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "subagent.updated" &&
                stored.event.payload.id === child.taskId &&
                (stored.event.payload.remoteChild?.snapshot?.requests.some(
                  (request) => request.requestId === requestId,
                ) ??
                  false),
            ),
            Stream.runHead,
            Effect.flatMap(Effect.fromOption),
          );
        expect((yield* a.threads.getThreadShell(parent))?.remotePendingRequest).toBe("approval");
        const status = yield* nativeCall(a, parent, "task_status", { taskId: child.taskId });
        expect(status.isError, json(status.content)).not.toBe(true);
        expect(status.structuredContent).toMatchObject({
          hasPendingApproval: true,
          childThreadId: null,
        });
        const args = { environmentId: b.environmentId, threadId: child.childId, requestId };
        const read = yield* nativeCall(a, parent, "t3_pending_request_read", args);
        expect(read.isError, json(read.content)).not.toBe(true);
        const content = yield* decodePending369Content(read.structuredContent);
        expect(content.kind).toBe("approval");
        const malformed = yield* nativeCall(a, parent, "t3_pending_request_respond", {
          ...args,
          expectedRevision: content.revision,
          decision: "cancel",
          clientRequestId: "bad-shape",
        });
        expect(malformed.isError).toBe(true);
        const answer = {
          ...args,
          expectedRevision: content.revision,
          decision: "accept",
          clientRequestId: "native-approval",
        };
        yield* Ref.set(a.dropCommittedToolReply, "t3_pending_request_respond");
        const lost = yield* nativeCall(a, parent, "t3_pending_request_respond", answer);
        expect(lost.isError).toBe(true);
        const accepted = yield* nativeCall(a, parent, "t3_pending_request_respond", answer);
        expect(accepted.isError, json(accepted.content)).not.toBe(true);
        expect(
          (yield* nativeCall(a, parent, "t3_pending_request_respond", answer)).structuredContent,
        ).toEqual(accepted.structuredContent);
        const callbacks: Array<Adapter.ProviderAdapterV2RuntimeRequestResponseInput> = [];
        const sessions = Context.get(b.runtime, ProviderSessionManager.ProviderSessionManagerV2);
        const spy = vi.spyOn(sessions, "get").mockReturnValue(
          Effect.succeed(
            Option.some({
              providerSession,
              instanceId,
              driver,
              providerSessionId,
              events: Stream.empty,
              ensureThread: () => Effect.die("No external provider process in this proof"),
              resumeThread: () => Effect.die("No external provider process in this proof"),
              startTurn: () => Effect.die("No external provider process in this proof"),
              steerTurn: () => Effect.die("No external provider process in this proof"),
              interruptTurn: () => Effect.die("No external provider process in this proof"),
              readThreadSnapshot: () => Effect.die("No external provider process in this proof"),
              rollbackThread: () => Effect.die("No external provider process in this proof"),
              forkThread: () => Effect.die("No external provider process in this proof"),
              respondToRuntimeRequest: (
                input: Adapter.ProviderAdapterV2RuntimeRequestResponseInput,
              ) =>
                Effect.sync(() => {
                  callbacks.push(input);
                }),
            } satisfies Adapter.ProviderAdapterV2SessionRuntime),
          ),
        );
        yield* Effect.addFinalizer(() => Effect.sync(() => spy.mockRestore()));
        const native = yield* RuntimeRequests.layer.pipe(
          Layer.provide(Layer.succeedContext(b.runtime)),
          Layer.build,
        );
        const runtimeRequests = Context.get(native, RuntimeRequests.RuntimeRequestServiceV2);
        const worker = yield* EffectWorker.layerWithOptions({ maxAttempts: 1 }).pipe(
          Layer.provide(
            Layer.succeed(EffectWorker.OrchestrationEffectExecutorV2, {
              execute: (effect) =>
                effect.request.type === "runtime-request.respond"
                  ? runtimeRequests
                      .respond({
                        threadId: effect.threadId,
                        providerSessionId: effect.request.providerSessionId,
                        requestId: effect.request.requestId,
                        ...(effect.request.decision === undefined
                          ? {}
                          : { decision: effect.request.decision }),
                        ...(effect.request.answers === undefined
                          ? {}
                          : { answers: effect.request.answers }),
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
          Layer.provide(Layer.succeed(PeerAuthority.PeerAuthority, b.authority)),
          Layer.provide(Layer.succeedContext(b.runtime)),
          Layer.build,
        );
        const drain = Context.get(worker, EffectWorker.OrchestrationEffectWorkerV2).drain();
        yield* drain;
        yield* drain;
        expect(callbacks).toEqual([{ requestId, decision: "accept" }]);
        const self = yield* nativeCall(b, child.childId, "t3_pending_request_respond", {
          threadId: child.childId,
          requestId,
          decision: "accept",
          actor: "human",
          human: true,
          clientRequestId: "spoof",
        });
        expect(self.isError).toBe(true);
        expect(callbacks).toHaveLength(1);
        const stoppingRequest = yield* seedPending369(b, child.childId, "live-stop");
        const stoppingRecords = yield* b.threads.getThreadRecords(child.childId, [
          "nodes",
          "runtimeRequests",
          "turnItems",
        ]);
        const stoppedRunId = (yield* b.threads.getThreadShell(child.childId))?.activeRunId;
        if (stoppedRunId == null)
          return yield* Effect.die("Expected a live native run before Stop");
        const stoppingNode = stoppingRecords.nodes.find(
          (node) => node.runtimeRequestId === stoppingRequest,
        )!;
        const stoppingRuntimeRequest = stoppingRecords.runtimeRequests.find(
          (request) => request.id === stoppingRequest,
        )!;
        const stoppingItem = stoppingRecords.turnItems.find(
          (item) => item.type === "user_input_request" && item.requestId === stoppingRequest,
        );
        if (stoppingItem?.type !== "user_input_request")
          return yield* Effect.die("Expected live question item");
        const stopNow = yield* DateTime.now;
        yield* b.sink.write({
          events: [
            {
              id: EventId.make("pending369-live-stop-node"),
              type: "node.updated",
              threadId: child.childId,
              runId: stoppedRunId,
              nodeId: stoppingNode.id,
              occurredAt: stopNow,
              payload: { ...stoppingNode, runId: stoppedRunId },
            },
            {
              id: EventId.make("pending369-live-stop-request"),
              type: "runtime-request.updated",
              threadId: child.childId,
              runId: stoppedRunId,
              nodeId: stoppingNode.id,
              occurredAt: stopNow,
              payload: {
                ...stoppingRuntimeRequest,
                responseCapability: { type: "live", providerSessionId },
              },
            },
            {
              id: EventId.make("pending369-live-stop-item"),
              type: "turn-item.updated",
              threadId: child.childId,
              runId: stoppedRunId,
              nodeId: stoppingNode.id,
              occurredAt: stopNow,
              payload: { ...stoppingItem, runId: stoppedRunId, responseMode: undefined },
            },
          ],
        });
        const stopArgs = { ...args, requestId: stoppingRequest };
        const stopRead = yield* decodePending369Content(
          (yield* nativeCall(a, parent, "t3_pending_request_read", stopArgs)).structuredContent,
        );
        const stopped = yield* nativeCall(a, parent, "t3_thread_interrupt", {
          environmentId: b.environmentId,
          threadId: child.childId,
          clientRequestId: "pending369-stop-before-answer",
        });
        expect(stopped.isError, json(stopped.content)).not.toBe(true);
        const tooLate = yield* nativeCall(a, parent, "t3_pending_request_respond", {
          ...stopArgs,
          expectedRevision: stopRead.revision,
          answers: { color: "Late" },
          clientRequestId: "pending369-after-stop",
        });
        expect(tooLate.isError, json(tooLate.content)).toBe(true);
        const stopSql = Context.get(b.database, SqlClient.SqlClient);
        const previousEffects = yield* stopSql<{
          count: number;
        }>`SELECT COUNT(*) AS count FROM orchestration_v2_effect_outbox WHERE effect_type='runtime-request.respond' AND json_extract(payload_json,'$.requestId')=${requestId}`;
        expect(previousEffects[0]?.count).toBe(1);
        const lateEffects = yield* stopSql<{
          count: number;
        }>`SELECT COUNT(*) AS count FROM orchestration_v2_effect_outbox WHERE effect_type='runtime-request.respond' AND json_extract(payload_json,'$.requestId')=${stoppingRequest}`;
        expect(lateEffects[0]?.count).toBe(0);
        expect(callbacks).toHaveLength(1);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("pending369: asynchronous answer and dismiss race in the native serialized owner", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const a = yield* makeNode(
        descriptorOf("pending369-race-a", "A", { peerAuthority: 1 }),
        "disk",
        { attachmentRequests: true },
      );
      const b = yield* makeNode(
        descriptorOf("pending369-race-b", "B", { peerAuthority: 1 }),
        "disk",
        { attachmentRequests: true },
      );
      yield* linkTo(a.links, b, "full-access");
      const parent = ThreadId.make("pending369-race-parent");
      yield* seedParent(a, parent);
      const child = yield* delegate(a, b, parent, "pending369-race-child");
      yield* activate(b, child.childId);
      const requestId = yield* seedPending369(b, child.childId);
      const args = { environmentId: b.environmentId, threadId: child.childId, requestId };
      const read = yield* nativeCall(a, parent, "t3_pending_request_read", args);
      const content = yield* decodePending369Content(read.structuredContent);
      const outside = yield* Context.get(a.services, PeerMcpClient.PeerMcpClient)
        .call({
          environmentId: b.environmentId,
          tool: "t3_pending_request_read",
          arguments: args,
          limits: { runtimeMode: "full-access", interactionMode: "default" },
          success: ThreadToolkit.tools.t3_pending_request_read.successSchema,
        })
        .pipe(Effect.result);
      expect(outside._tag).toBe("Failure");
      const raced = yield* Effect.all(
        [
          nativeCall(a, parent, "t3_pending_request_respond", {
            ...args,
            expectedRevision: content.revision,
            answers: { color: "Purple" },
            clientRequestId: "answer-race",
          }),
          nativeCall(a, parent, "t3_pending_request_dismiss", {
            ...args,
            expectedRevision: content.revision,
            clientRequestId: "dismiss-race",
          }),
        ],
        { concurrency: "unbounded" },
      );
      expect(raced.filter((result) => result.isError !== true)).toHaveLength(1);
      const records = yield* b.threads.getThreadRecords(child.childId, [
        "runtimeRequests",
        "messages",
      ]);
      expect(records.runtimeRequests.find((request) => request.id === requestId)?.status).toBe(
        "resolved",
      );
      expect(
        records.messages.filter((message) => message.text === "Which color?\nPurple"),
      ).toHaveLength(raced[0]?.isError === true ? 0 : 1);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

const decodePendingWsReply = Schema.decodeUnknownEffect(
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
it.live(
  "pending369: real authenticated source WebSocket invokes the production request handlers and rejects agent credentials",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("pending369-ws-a", "A", { peerAuthority: 1 }),
          "disk",
          { attachmentRequests: true, productionKernel: true },
        );
        const b = yield* makeNode(
          descriptorOf("pending369-ws-b", "B", { peerAuthority: 1 }),
          "disk",
          { attachmentRequests: true, productionKernel: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("pending369-ws-parent");
        yield* seedParent(a, parent);
        const child = yield* delegate(a, b, parent, "pending369-ws-child");
        yield* activate(b, child.childId);
        const requestId = yield* seedPending369(b, child.childId);
        const server = yield* NodeHttpServer.layerTest.pipe(Layer.fresh, Layer.build);
        const route = HttpRouter.add(
          "GET",
          "/ws",
          Effect.gen(function* () {
            const request = yield* HttpServerRequest.HttpServerRequest;
            const authenticated = yield* a.auth
              .authenticateWebSocketUpgrade(request)
              .pipe(Effect.result);
            if (authenticated._tag === "Failure") return HttpServerResponse.empty({ status: 403 });
            const session = authenticated.success;
            const { protocol, httpEffect } = yield* RpcServer.makeProtocolWithHttpEffectWebsocket;
            yield* RpcServer.make(PendingRequestClientRpcGroup, {
              disableTracing: true,
              disableFatalDefects: true,
            }).pipe(
              Effect.provideService(RpcServer.Protocol, protocol),
              Effect.provide(
                Layer.merge(
                  PendingRequestClientRpcGroup.toLayer(
                    pendingRequestClientHandlers(session, a.pendingRequests),
                  ),
                  RpcAuthorization.layer(session.scopes),
                ),
              ),
              Effect.forkScoped,
            );
            return yield* httpEffect;
          }),
        );
        yield* HttpRouter.serve(route, { disableListenLog: true, disableLogger: true }).pipe(
          Layer.provide(Layer.succeedContext(server)),
          Layer.provide(RpcSerialization.layerJson),
          Layer.fresh,
          Layer.build,
        );
        const address = Context.get(server, HttpServer.HttpServer).address;
        if (address._tag === "UnixPathAddress") return yield* Effect.die("Expected test socket");
        const messages = yield* Queue.unbounded<string>();
        const connect = (ticket: string) =>
          Effect.acquireRelease(
            Effect.tryPromise({
              try: () =>
                new Promise<WebSocket>((resolve, reject) => {
                  const socket = new WebSocket(
                    `ws://127.0.0.1:${address.port}/ws?wsTicket=${encodeURIComponent(ticket)}`,
                  );
                  socket.addEventListener("message", (event) =>
                    Queue.offerUnsafe(messages, String(event.data)),
                  );
                  socket.addEventListener("open", () => resolve(socket), { once: true });
                  socket.addEventListener(
                    "error",
                    () => reject(new Error("Unauthenticated source socket refused")),
                    { once: true },
                  );
                }),
              catch: () =>
                new OrchestratorMcpFailure({
                  code: "orchestration_error",
                  message: "Isolated source WebSocket failed",
                }),
            }),
            (socket) => Effect.sync(() => socket.close()),
          );
        const pairing = yield* a.auth.issuePairingCredential({
          scopes: ["orchestration:read", "orchestration:operate"],
        });
        const token = yield* a.auth.exchangeBootstrapCredentialForAccessToken(
          pairing.credential,
          undefined,
          { deviceType: "desktop", os: "fixture", browser: "fixture", ipAddress: "127.0.0.1" },
        );
        const session = yield* a.auth.authenticateHttpRequest(
          HttpServerRequest.fromWeb(
            new Request(a.url, { headers: { authorization: `Bearer ${token.access_token}` } }),
          ),
        );
        const ticket = yield* a.auth.issueWebSocketTicket(session);
        const socket = yield* connect(ticket.ticket);
        let ordinal = 0;
        const rpc = (tag: string, payload: unknown, client = socket) =>
          Effect.gen(function* () {
            const id = String(++ordinal);
            client.send(json({ _tag: "Request", id, tag, payload, headers: [] }));
            const reply = yield* decodePendingWsReply(yield* Queue.take(messages));
            expect(reply.requestId).toBe(id);
            return reply;
          });
        const base = {
          parentThreadId: parent,
          taskId: child.taskId,
          requestId,
          requestTarget: { environmentId: b.environmentId, threadId: child.childId },
        };
        const read = yield* rpc(WS_METHODS.remoteTaskRequestRead, {
          ...base,
          operation: { type: "read" },
        });
        expect(read.exit?._tag).toBe("Success");
        const content = yield* decodePending369ClientResult(read.exit?.value);
        if (!("kind" in content) || content.revision === undefined)
          return yield* Effect.die("Missing native qualified request");
        const answer = {
          ...base,
          operation: {
            type: "respond",
            clientRequestId: "ws369-answer",
            expectedRevision: content.revision,
            answers: { color: "Teal" },
          },
          actor: "human",
        };
        const accepted = yield* rpc(WS_METHODS.remoteTaskRequest, answer);
        expect(accepted.exit?._tag).toBe("Success");
        expect((yield* rpc(WS_METHODS.remoteTaskRequest, answer)).exit).toEqual(accepted.exit);
        expect(
          (yield* b.threads.getThreadRecords(child.childId, ["messages"])).messages.filter(
            (message) => message.text === "Which color?\nTeal",
          ),
        ).toHaveLength(1);
        const peerSession = yield* a.auth.issueMcpClientSession({
          label: "Outside OAuth",
          access: "full-access",
          peerLink: false,
          client: {
            deviceType: "desktop",
            os: "fixture",
            browser: "fixture",
            ipAddress: "127.0.0.1",
          },
        });
        const impersonation = yield* a.auth
          .authenticateWebSocketUpgrade(
            HttpServerRequest.fromWeb(
              new Request(a.url, { headers: { authorization: `Bearer ${peerSession.token}` } }),
            ),
          )
          .pipe(Effect.result);
        expect(impersonation._tag).toBe("Failure");
        const readPairing = yield* a.auth.issuePairingCredential({
          scopes: ["orchestration:read"],
        });
        const readToken = yield* a.auth.exchangeBootstrapCredentialForAccessToken(
          readPairing.credential,
          undefined,
          { deviceType: "desktop", os: "fixture", browser: "fixture", ipAddress: "127.0.0.1" },
        );
        const readSession = yield* a.auth.authenticateHttpRequest(
          HttpServerRequest.fromWeb(
            new Request(a.url, { headers: { authorization: `Bearer ${readToken.access_token}` } }),
          ),
        );
        const readTicket = yield* a.auth.issueWebSocketTicket(readSession);
        const readSocket = yield* connect(readTicket.ticket);
        const secondRequestId = yield* seedPending369(b, child.childId, "read-only");
        const secondRead = yield* rpc(
          WS_METHODS.remoteTaskRequestRead,
          { ...base, requestId: secondRequestId, operation: { type: "read" } },
          readSocket,
        );
        expect(secondRead.exit?._tag).toBe("Success");
        expect(
          (yield* rpc(
            WS_METHODS.remoteTaskRequest,
            { ...answer, requestId: secondRequestId },
            readSocket,
          )).exit?._tag,
        ).toBe("Failure");
        const authRepository = yield* AuthSessions.layer.pipe(
          Layer.provide(Layer.succeedContext(a.database)),
          Layer.build,
        );
        yield* Context.get(authRepository, AuthSessions.AuthSessionRepository).revoke({
          sessionId: session.sessionId,
          revokedAt: yield* DateTime.now,
        });
        expect((yield* rpc(WS_METHODS.remoteTaskRequest, answer)).exit?._tag).toBe("Failure");
        expect((yield* connect("unproved-agent-token").pipe(Effect.result))._tag).toBe("Failure");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "pending369: native intake resolving after planning wins before the response transaction commits",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("pending369-intake-a", "A", { peerAuthority: 1 }),
          "disk",
          { attachmentRequests: true, productionKernel: true },
        );
        const b = yield* makeNode(
          descriptorOf("pending369-intake-b", "B", { peerAuthority: 1 }),
          "disk",
          { attachmentRequests: true, productionKernel: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("pending369-intake-parent");
        yield* seedParent(a, parent);
        const child = yield* delegate(a, b, parent, "pending369-intake-child");
        yield* activate(b, child.childId);
        const requestId = yield* seedPending369(b, child.childId);
        const args = { environmentId: b.environmentId, threadId: child.childId, requestId };
        const read = yield* nativeCall(a, parent, "t3_pending_request_read", args);
        const content = yield* decodePending369Content(read.structuredContent);
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const commit = b.sink.commitCommand;
        const held = vi
          .spyOn(b.sink, "commitCommand")
          .mockImplementation((input) =>
            input.commandType !== "runtime-request.respond"
              ? commit(input)
              : Deferred.succeed(entered, undefined).pipe(
                  Effect.andThen(Deferred.await(release)),
                  Effect.andThen(commit(input)),
                ),
          );
        yield* Effect.addFinalizer(() => Effect.sync(() => held.mockRestore()));
        const pending = yield* nativeCall(a, parent, "t3_pending_request_respond", {
          ...args,
          expectedRevision: content.revision,
          answers: { color: "Obsolete" },
          clientRequestId: "intake-response",
        }).pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        const request = (yield* b.threads.getThreadRecords(child.childId, [
          "runtimeRequests",
        ])).runtimeRequests.find((request) => request.id === requestId)!;
        const now = yield* DateTime.now;
        yield* b.sink.write({
          events: [
            {
              id: EventId.make("pending369-intake-cancel"),
              type: "runtime-request.updated",
              threadId: child.childId,
              nodeId: request.nodeId,
              occurredAt: now,
              payload: { ...request, status: "cancelled", resolvedAt: now },
            },
          ],
        });
        yield* Deferred.succeed(release, undefined);
        const refused = yield* Fiber.join(pending);
        expect(refused.isError, json(refused.content)).toBe(true);
        const records = yield* b.threads.getThreadRecords(child.childId, [
          "runtimeRequests",
          "messages",
        ]);
        expect(records.runtimeRequests.find((request) => request.id === requestId)?.status).toBe(
          "cancelled",
        );
        expect(records.messages.some((message) => message.text === "Which color?\nObsolete")).toBe(
          false,
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "pending369: a native local grandparent crosses a real local child and an authenticated pair",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("pending369-local-a", "A", { peerAuthority: 1 }),
          "disk",
          { attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf("pending369-local-b", "B", { peerAuthority: 1 }),
          "disk",
          { attachmentRequests: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const root = ThreadId.make("pending369-local-root");
        yield* seedParent(a, root);
        const launched = yield* nativeCall(a, root, "delegate_task", {
          task: "Own local intermediate work",
          target: {
            projectId: a.project.id,
            providerInstanceId: instanceId,
            model: selection.model,
          },
          mode: "async",
          clientRequestId: "pending369-local-child",
        });
        expect(launched.isError, json(launched.content)).not.toBe(true);
        const local = yield* decodePending369LocalTask(launched.structuredContent);
        if (local.childThreadId === null)
          return yield* Effect.die("Expected a real native local child");
        expect((yield* a.threads.getThreadShell(local.childThreadId))?.lineage).toMatchObject({
          parentThreadId: root,
          relationshipToParent: "subagent",
        });
        yield* activate(a, local.childThreadId);
        const remote = yield* delegate(a, b, local.childThreadId, "pending369-local-pair");
        yield* activate(b, remote.childId);
        const requestId = yield* seedPending369(b, remote.childId);
        const args = { environmentId: b.environmentId, threadId: remote.childId, requestId };
        const read = yield* nativeCall(a, root, "t3_pending_request_read", args);
        expect(read.isError, json(read.content)).not.toBe(true);
        const content = yield* decodePending369Content(read.structuredContent);
        const answered = yield* nativeCall(a, root, "t3_pending_request_respond", {
          ...args,
          expectedRevision: content.revision,
          answers: { color: "Amber" },
          clientRequestId: "pending369-local-grandparent",
        });
        expect(answered.isError, json(answered.content)).not.toBe(true);
        expect(
          (yield* b.threads.getThreadRecords(remote.childId, ["messages"])).messages.filter(
            (message) => message.text === "Which color?\nAmber",
          ),
        ).toHaveLength(1);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "pending369 R1: read content and revision describe one snapshot",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(descriptorOf("r1-read-a", "A", { peerAuthority: 1 }), "disk", {
          attachmentRequests: true,
          productionKernel: true,
        });
        const b = yield* makeNode(descriptorOf("r1-read-b", "B", { peerAuthority: 1 }), "disk", {
          attachmentRequests: true,
          productionKernel: true,
        });
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("r1-read-parent");
        yield* seedParent(a, parent);
        const child = yield* delegate(a, b, parent, "r1-read-child");
        yield* activate(b, child.childId);
        const requestId = yield* seedPending369(b, child.childId);
        const args = { environmentId: b.environmentId, threadId: child.childId, requestId };
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const original = b.threads.getProjectThreadRecords;
        let once = true;
        // Scheduling barrier only: the real native owner executes and its exact result is retained.
        // No authority decision, projection value or SQL operation is replaced.
        const barrier = vi
          .spyOn(b.threads, "getProjectThreadRecords")
          .mockImplementation((...input) =>
            original(...input).pipe(
              Effect.flatMap((records) => {
                if (
                  !once ||
                  input[0].threadId !== child.childId ||
                  !input[1]?.includes("turnItems")
                )
                  return Effect.succeed(records);
                once = false;
                return Deferred.succeed(entered, undefined).pipe(
                  Effect.andThen(Deferred.await(release)),
                  Effect.as(records),
                );
              }),
            ),
          );
        yield* Effect.addFinalizer(() => Effect.sync(() => barrier.mockRestore()));
        const reading = yield* nativeCall(a, parent, "t3_pending_request_read", args).pipe(
          Effect.forkScoped,
        );
        yield* Deferred.await(entered);
        const records = yield* b.threads.getThreadRecords(child.childId, ["turnItems"]);
        const item = records.turnItems.find(
          (i) => i.type === "user_input_request" && i.requestId === requestId,
        );
        if (item?.type !== "user_input_request") return yield* Effect.die("Missing native item");
        const changed = "Which UPDATED color?";
        yield* b.sink.write({
          events: [
            {
              id: EventId.make("r1-read-change"),
              type: "turn-item.updated",
              threadId: child.childId,
              occurredAt: yield* DateTime.now,
              payload: {
                ...item,
                questions: item.questions.map((q) => ({ ...q, question: changed })),
              },
            },
          ],
        });
        yield* Deferred.succeed(release, undefined);
        const result = yield* Fiber.join(reading);
        expect(result.isError, json(result)).not.toBe(true);
        const content = yield* decodePending369Content(result.structuredContent);
        const accepted = yield* nativeCall(a, parent, "t3_pending_request_respond", {
          ...args,
          expectedRevision: content.revision,
          answers: { color: "Blue" },
          clientRequestId: "r1-old-content-answer",
        });
        const after = yield* b.threads.getThreadRecords(child.childId, [
          "messages",
          "runtimeRequests",
        ]);
        yield* Effect.log(
          "R1 READ RACE EVIDENCE",
          json({
            content,
            accepted,
            updatedMessage: after.messages.find((m) => m.text.includes(changed)),
            status: after.runtimeRequests.find((r) => r.id === requestId)?.status,
          }),
        );
        // Either read must return updated content, or its stale revision must reject the answer.
        expect(
          (content.kind === "user-input" && content.questions[0]?.question === changed) ||
            accepted.isError === true,
        ).toBe(true);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30000,
);

it.live(
  "pending369 R1: advertised empty live option retains local-equivalent response",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(descriptorOf("r1-empty-a", "A", { peerAuthority: 1 }), "disk", {
          attachmentRequests: true,
          productionKernel: true,
        });
        const b = yield* makeNode(descriptorOf("r1-empty-b", "B", { peerAuthority: 1 }), "disk", {
          attachmentRequests: true,
          productionKernel: true,
        });
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("r1-empty-parent");
        yield* seedParent(a, parent);
        const child = yield* delegate(a, b, parent, "r1-empty-child");
        yield* activate(b, child.childId);
        const requestId = yield* seedPending369(b, child.childId);
        const projection = yield* b.orchestrator.getThreadProjection(child.childId);
        const providerSessionId = projection.providerThreads[0]!.providerSessionId!;
        const now = yield* DateTime.now;
        const request = projection.runtimeRequests.find((r) => r.id === requestId)!;
        const item = projection.turnItems.find(
          (i) => i.type === "user_input_request" && i.requestId === requestId,
        );
        if (item?.type !== "user_input_request")
          return yield* Effect.die("Missing real pending item");
        // This is the exact input/select empty-value shape produced by piQuestion
        // in packages/provider-pi/src/server/adapter.ts. No invented optional flag.
        yield* b.sink.write({
          events: [
            {
              id: EventId.make("r1-empty-session"),
              type: "provider-session.attached",
              threadId: child.childId,
              occurredAt: now,
              payload: {
                id: providerSessionId,
                driver,
                providerInstanceId: instanceId,
                status: "running",
                cwd: process.cwd(),
                model: selection.model,
                capabilities: CodexProviderCapabilitiesV2,
                createdAt: now,
                updatedAt: now,
                lastError: null,
              },
            },
            {
              id: EventId.make("r1-empty-request"),
              type: "runtime-request.updated",
              threadId: child.childId,
              nodeId: request.nodeId,
              occurredAt: now,
              payload: { ...request, responseCapability: { type: "live", providerSessionId } },
            },
            {
              id: EventId.make("r1-empty-item"),
              type: "turn-item.updated",
              threadId: child.childId,
              nodeId: request.nodeId,
              occurredAt: now,
              payload: {
                ...item,
                responseMode: undefined,
                questions: [
                  {
                    id: "color",
                    header: "Input",
                    question: "Value?",
                    options: [
                      {
                        label: "Submit empty value",
                        description: "Send an empty string to the extension.",
                        value: "",
                      },
                    ],
                  },
                ],
              },
            },
          ],
        });
        const args = { environmentId: b.environmentId, threadId: child.childId, requestId };
        const read = yield* decodePending369Content(
          (yield* nativeCall(a, parent, "t3_pending_request_read", args)).structuredContent,
        );
        for (const [index, answers] of [{}, { color: [""] }].entries()) {
          const rejected = yield* nativeCall(a, parent, "t3_pending_request_respond", {
            ...args,
            expectedRevision: read.revision,
            answers,
            clientRequestId: `r1-empty-invalid-${index}`,
          });
          expect(rejected.isError).toBe(true);
        }
        const remote = yield* nativeCall(a, parent, "t3_pending_request_respond", {
          ...args,
          expectedRevision: read.revision,
          answers: { color: "" },
          clientRequestId: "r1-empty-answer",
        });
        const after = yield* b.threads.getThreadRecords(child.childId, ["runtimeRequests"]);
        expect(remote.isError, json(remote)).not.toBe(true);
        expect(after.runtimeRequests.find((r) => r.id === requestId)).toMatchObject({
          status: "resolved",
          answers: { color: "" },
        });
        const fixedId = yield* seedPending369(b, child.childId, "fixed-choice-control");
        const fixedItem = (yield* b.threads.getThreadRecords(child.childId, [
          "turnItems",
        ])).turnItems.find(
          (entry) => entry.type === "user_input_request" && entry.requestId === fixedId,
        );
        if (fixedItem?.type !== "user_input_request")
          return yield* Effect.die("Missing fixed choice control");
        yield* b.sink.write({
          events: [
            {
              id: EventId.make("r1-fixed-shape"),
              type: "turn-item.updated",
              threadId: child.childId,
              occurredAt: yield* DateTime.now,
              payload: {
                ...fixedItem,
                questions: [
                  {
                    id: "color",
                    header: "Choice",
                    question: "Choose?",
                    allowCustomAnswer: false,
                    options: [{ label: "Blue", description: "Allowed" }],
                  },
                ],
              },
            },
          ],
        });
        const fixedArgs = { ...args, requestId: fixedId };
        const fixedRead = yield* decodePending369Content(
          (yield* nativeCall(a, parent, "t3_pending_request_read", fixedArgs)).structuredContent,
        );
        const invalidOption = yield* nativeCall(a, parent, "t3_pending_request_respond", {
          ...fixedArgs,
          expectedRevision: fixedRead.revision,
          answers: { color: "not offered" },
          clientRequestId: "r1-fixed-invalid",
        });
        expect(invalidOption.isError).toBe(true);
        expect(
          (yield* b.threads.getThreadRecords(child.childId, [
            "runtimeRequests",
          ])).runtimeRequests.find((entry) => entry.id === fixedId)?.status,
        ).toBe("pending");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30000,
);

it.live(
  "review383-read-capture: readonly capture rechecks after external readiness",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, route, open } = yield* typed383Pair("review383-read-capture");
        const opened = yield* open("read-capture-open");
        expect(opened.isError, json(opened)).not.toBe(true);
        const { target } = yield* decodeTyped383Open(opened.structuredContent);
        const held = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        yield* Ref.set(b.deviceProcessHook, (call) =>
          call.args[1] === "click"
            ? Deferred.succeed(held, undefined).pipe(Effect.andThen(Deferred.await(release)))
            : Effect.void,
        );
        const running = yield* nativeCall(a, parent, "device_execute", {
          ...route,
          target,
          command: { type: "click", ref: "@e1" },
          clientRequestId: "held-click",
        }).pipe(Effect.forkChild);
        yield* Deferred.await(held);
        yield* Effect.gen(function* () {
          const input = { ...route, hostId: "local", deviceId: "fixture-device382" };
          const lawful = yield* nativeCall(a, parent, "device_screenshot", input);
          expect(lawful.isError, json(lawful)).not.toBe(true);
          const before = yield* Ref.get(b.deviceScreenshotCalls);
          const grant = (yield* b.authority.grants)[0]!;
          const armed = yield* Ref.make(true);
          yield* Ref.set(
            b.deviceReadinessHook,
            Effect.gen(function* () {
              if (!(yield* Ref.getAndSet(armed, false))) return;
              yield* b.authority.setGrant({ ...grant, device: false }).pipe(Effect.orDie);
            }),
          );
          const revoked = yield* nativeCall(a, parent, "device_screenshot", input);
          expect(yield* Ref.get(armed)).toBe(false);
          expect((yield* b.authority.grants)[0]!.device).toBe(false);
          expect(revoked.isError, "revoked read must deny before capture").toBe(true);
          expect(yield* Ref.get(b.deviceScreenshotCalls)).toBe(before);
        }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
        yield* Fiber.join(running);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "review383-local-auto: keyed local opening preserves existing capability policy",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const n = yield* makeNode(
          descriptorOf("review383-local", "local", { peerAuthority: 1 }),
          "disk",
          {
            productionKernel: true,
            attachmentRequests: true,
            preferencesOwner: true,
            deviceOwner: true,
          },
        );
        const parent = ThreadId.make("review383-local-parent");
        yield* seedParent(n, parent);
        const args = { deviceId: "fixture-device382", platform: "android" };
        const full = yield* nativeCall(n, parent, "device_open", {
          ...args,
          clientRequestId: "full",
        });
        expect(full.isError, json(full)).not.toBe(true);
        yield* n.threads.dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("review383-auto"),
          threadId: parent,
          runtimeMode: "auto",
        });
        const keyed = yield* nativeCall(n, parent, "device_open", {
          ...args,
          clientRequestId: "auto",
        });
        expect(keyed.isError, json(keyed)).not.toBe(true);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "typed383-literals: registered empty/CR fill and app deep link preserve exact bound intent",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, route, open, footprint } = yield* typed383Pair("typed383-literals");
        const opened = yield* open("literal-open");
        expect(opened.isError, json(opened)).not.toBe(true);
        const { target } = yield* decodeTyped383Open(opened.structuredContent);
        const texts = [
          "",
          "abc\r",
          "\n",
          "\r\n",
          "first\nsecond",
          "--config /rival",
          '"quoted" Ω🦊',
        ];
        for (const [index, text] of texts.entries()) {
          const input = {
            ...route,
            target,
            command: { type: "fill", ref: "@e1", text },
            clientRequestId: `literal-${index}`,
          };
          const reply = yield* nativeCall(a, parent, "device_execute", input);
          expect(reply.structuredContent).toEqual({ type: "action", target, status: "dispatched" });
          const call = b.deviceProcessCalls.at(-1)!;
          expect(call.args[1]).toBe("fill");
          const args = call.args.slice(call.args.indexOf("--") + 1);
          expect(args).toEqual(text === "" ? ["@e1", ""] : ["@e1"]);
          expect(call.stdin).toBe(text === "" ? undefined : text + "\r\n");
          const beforeReplay = yield* footprint();
          expect((yield* nativeCall(a, parent, "device_execute", input)).structuredContent).toEqual(
            reply.structuredContent,
          );
          expect(yield* footprint()).toEqual(beforeReplay);
        }
        const command = { type: "open", app: "com.example.app", url: "example://settings?tab=one" };
        const input = { ...route, target, command, clientRequestId: "deep-link" };
        expect((yield* nativeCall(a, parent, "device_execute", input)).structuredContent).toEqual({
          type: "action",
          target,
          status: "dispatched",
        });
        const call = b.deviceProcessCalls.at(-1)!;
        expect(call.args[1]).toBe("open");
        expect(call.args.slice(call.args.indexOf("--") + 1)).toEqual([command.app, command.url]);
        const beforeInvalid = yield* footprint();
        for (const url of ["--config", "example://bad\nlink", "x:" + "a".repeat(8192)])
          expect(
            (yield* nativeInvalidParams(a, parent, "device_execute", {
              ...input,
              command: { ...command, url },
            })).error.code,
          ).toBe(-32602);
        expect(yield* footprint()).toEqual(beforeInvalid);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live.each(["android", "ios"] as const)(
  "typed383-shutdown: %s readiness-time grant retirement denies shutdown outside stopped-device recovery",
  (platform) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, route } = yield* typed383Pair(`typed383-shutdown-${platform}`);
        yield* Ref.set(b.deviceHubPlatform, platform);
        yield* Ref.update(b.deviceCapabilities, (c) => ({
          ...c,
          device: { ...c.device, platform, kind: platform === "ios" ? "simulator" : "emulator" },
        }));
        const open = (key: string) =>
          nativeCall(a, parent, "device_open", {
            ...route,
            deviceId: "fixture-device382",
            platform,
            clientRequestId: key,
          });
        const close = (key: string) =>
          nativeCall(a, parent, "device_close", {
            ...route,
            hostId: "local",
            deviceId: "fixture-device382",
            shutdown: true,
            clientRequestId: key,
          });
        const firstOpen = yield* open("lawful-open");
        expect(firstOpen.isError, json(firstOpen)).not.toBe(true);
        const lawful = yield* close("lawful-close");
        expect(lawful.isError, json(lawful)).not.toBe(true);
        expect(yield* Ref.get(b.deviceShutdownCalls)).toBe(1);
        const nextOpen = yield* open("revoked-open");
        expect(nextOpen.isError, json(nextOpen)).not.toBe(true);
        const grant = (yield* b.authority.grants)[0]!;
        const armed = yield* Ref.make(true);
        yield* Ref.set(
          b.deviceReadinessHook,
          Effect.gen(function* () {
            if (!(yield* Ref.getAndSet(armed, false))) return;
            yield* b.authority.setGrant({ ...grant, device: false }).pipe(Effect.orDie);
            // A refusal caught by the iOS already-stopped fallback must not become success.
            yield* Ref.set(b.deviceHubBooted, false);
          }),
        );
        const revoked = yield* close("revoked-close");
        expect(yield* Ref.get(armed)).toBe(false);
        expect((yield* b.authority.grants)[0]!.device).toBe(false);
        expect(
          yield* Ref.get(b.deviceShutdownCalls),
          "no new shutdown after observed grant retirement",
        ).toBe(1);
        expect(
          revoked.isError,
          "retired grant must deny shutdown even when the hub reports stopped",
        ).toBe(true);
        expect(json(revoked)).toContain("outcome_unknown");
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const unknown = yield* sql`SELECT id FROM mcp_request_receipts WHERE state='started'`;
        expect(unknown.length).toBeGreaterThan(0);
        yield* b.authority.setGrant({ ...grant, device: true });
        expect(json(yield* close("revoked-close"))).toContain("outcome_unknown");
        expect(yield* Ref.get(b.deviceShutdownCalls)).toBe(1);
        yield* Ref.set(b.deviceHubBooted, true);
        const later = yield* open("later-open");
        expect(later.isError, json(later)).not.toBe(true);
        const { target } = yield* decodeTyped383Open(later.structuredContent);
        expect(json(yield* close("revoked-close"))).toContain("outcome_unknown");
        expect(yield* Ref.get(b.deviceShutdownCalls)).toBe(1);
        const click = yield* nativeCall(a, parent, "device_execute", {
          ...route,
          target,
          command: { type: "click", ref: "@e1" },
          clientRequestId: "later-click",
        });
        expect(click.isError, json(click)).not.toBe(true);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);
