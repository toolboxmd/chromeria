import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as SourceProviders from "../sourceControl/SourceControlProviderRegistry.ts";
import * as ProductionRuntime from "../orchestration-v2/runtimeLayer.ts";
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
  productionKernel = false,
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
      workspaceRoot: process.cwd(),
      repositoryIdentity: null,
      defaultModelSelection: selection,
      scripts: [],
      createdAt: "2026-10-16T00:00:00Z",
      updatedAt: "2026-10-16T00:00:00Z",
      deletedAt: null,
    };
    const projectLayer = Layer.mock(Projects.ProjectService)({
      getById: () => Effect.succeed(Option.some(project)),
      listPage: () => Effect.succeed({ projects: [project], nextCursor: null }),
    });
    const environment = Layer.succeed(Environment.ServerEnvironment, {
      getEnvironmentId: Effect.succeed(descriptor.environmentId),
      getDescriptor: Effect.succeed(descriptor),
    });
    const nodeConfig = Config.layerTest(process.cwd(), {
      prefix: `peer-authority-${descriptor.label}-`,
    });
    const settings = Settings.layerTest({
      providerInstances: { [instanceId]: { driver, enabled: true } },
    });
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
    const folders = Layer.mock(Folders.ManagedProjectFolders)({
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
    const holdLaunchReply = yield* Ref.make(false);
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
          Layer.provideMerge(PeerForwarding.layer),
          Layer.provideMerge(faultablePeers),
          Layer.provideMerge(Layer.succeedContext(baseServices)),
          Layer.provide(Layer.succeedContext(runtime)),
          Layer.provide(db),
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
    const schedules = yield* ScheduledTasks.layer.pipe(
      Layer.provide(
        Layer.succeed(
          ScheduledTasks.ScheduledTaskWebhookOrigin,
          Effect.succeed({ relayHookBaseUrl: "https://hooks.example.invalid/v1/hooks/fixture" }),
        ),
      ),
      Layer.provide(Scheduler.layer),
      Layer.provide(Layer.mock(SecretRequests.SecretRequests)({})),
      Layer.provide(Layer.succeedContext(launches)),
      Layer.provide(launchDependencies),
      Layer.fresh,
      Layer.build,
    );
    const tools = Layer.merge(
      McpHttpServer.layerProjectRegistration,
      McpHttpServer.layerOrchestratorToolkit,
    ).pipe(
      Layer.provide(Layer.succeedContext(schedules)),
      Layer.provide(Layer.mock(SecretRequests.SecretRequests)({})),
      Layer.provide(Layer.mock(BareLaunch.BareLaunch)({})),
      Layer.provide(Layer.succeedContext(launches)),
      Layer.provide(Layer.succeedContext(runtime)),
      Layer.provide(Layer.succeedContext(services)),
      Layer.provide(projectLayer),
      Layer.provide(folders),
      Layer.provide(settings),
      Layer.provide(layerLaunchCatalog),
      Layer.provide(Layer.mock(Git.GitVcsDriver)({})),
      Layer.provide(Layer.mock(Repositories.SourceControlRepositoryService)({})),
    );
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
    const peer = yield* servePeer(descriptor, tools, {
      database: db,
      registry,
      secretStore: Context.get(services, Secrets.ServerSecretStore),
      authorityServices: Context.merge(lifecycleContext, lifecycle),
    }).pipe(Effect.provide(Layer.succeedContext(serverContext)));
    yield* Effect.addFinalizer(() =>
      Effect.forEach([...connectionScopes], (lifetime) => Scope.close(lifetime, Exit.void), {
        concurrency: "unbounded",
        discard: true,
      }),
    );
    return {
      ...peer,
      noticeFailure,
      holdLaunchReply,
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
      services,
      registry,
      scheduled: Context.get(schedules, ScheduledTasks.ScheduledTaskService),
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
const nativeCall = (
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
      return yield* Effect.die(`Native initialization failed: ${initialized.status}`);
    yield* post({ jsonrpc: "2.0", method: "notifications/initialized" }, sessionId);
    const result = yield* post(
      { jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: tool, arguments: args } },
      sessionId,
    );
    const text = yield* result.text;
    return (yield* sendResult(text.startsWith("data:") ? text.slice(5).trim() : text)).result;
  }).pipe(Effect.provide(FetchHttpClient.layer));
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
          true,
        );
        const a = yield* makeNode(
          descriptorOf("native-route-a", "A", { peerAuthority: 1 }),
          "disk",
          true,
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
