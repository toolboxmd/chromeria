import * as Deferred from "effect/Deferred";
import * as Ref from "effect/Ref";
import * as EffectWorker from "../orchestration-v2/EffectWorker.ts";
import * as EffectOutbox from "../orchestration-v2/EffectOutbox.ts";
import * as McpSessionRegistry from "../mcp/McpSessionRegistry.ts";
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
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type Project,
  type ExecutionEnvironmentDescriptor,
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
const adapter = {
  instanceId,
  driver,
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("The isolated workflow never starts a provider process"),
} as Adapter.ProviderAdapterV2["Service"];

/** Real commands, projections, receipts and intake; provider execution remains an external fixture. */
const makeNode = (
  descriptor: ExecutionEnvironmentDescriptor,
  storage: "memory" | "disk" = "memory",
) =>
  Effect.gen(function* () {
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
    const runtime = yield* Runtime.layerWithRegistry(
      { name: `peer-authority-${descriptor.label}` },
      Adapters.layerFromAdapters([adapter]),
      { databaseLayer: db, runEffectWorker: false },
    ).pipe(Layer.fresh, Layer.build);
    const nodeConfig = Config.layerTest(process.cwd(), {
      prefix: `peer-authority-${descriptor.label}-`,
    });
    const settings = Settings.layerTest({
      providerInstances: { [instanceId]: { driver, enabled: true } },
    });
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
    const connect = () =>
      Effect.gen(function* () {
        const lifetime = yield* Scope.make();
        yield* Effect.addFinalizer(() => Scope.close(lifetime, Exit.void));
        const services = yield* RemoteDelegation.layer.pipe(
          Layer.provideMerge(PeerForwarding.layer),
          Layer.provideMerge(PeerMcpClient.layer),
          Layer.provideMerge(Layer.succeedContext(baseServices)),
          Layer.provide(Layer.succeedContext(runtime)),
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
    const tools = Layer.merge(
      McpHttpServer.layerProjectRegistration,
      McpHttpServer.layerOrchestratorToolkit,
    ).pipe(
      Layer.provide(Layer.mock(ScheduledTasks.ScheduledTaskService)({})),
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
    const peer = yield* servePeer(descriptor, tools, {
      database: db,
      registry,
      secretStore: Context.get(services, Secrets.ServerSecretStore),
      authorityServices: services,
    }).pipe(Effect.provide(Layer.succeedContext(serverContext)));
    return {
      ...peer,
      noticeFailure,
      reopenDatabase,
      environmentId: descriptor.environmentId,
      project,
      database,
      runtime,
      services,
      registry,
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
const seedParent = (node: Node, threadId: ThreadId) =>
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
const nativeCall = (node: Node, threadId: ThreadId, tool: string, args: Record<string, unknown>) =>
  Effect.gen(function* () {
    const issued = yield* node.registry.issue({
      threadId,
      providerInstanceId: instanceId,
      capabilities: new Set(["orchestration", "preview", "device"]),
    });
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
        const reopened = yield* b.reopenDatabase;
        const restored = yield* PeerAuthority.PeerAuthority.pipe(
          Effect.provide(
            PeerAuthority.layer.pipe(
              Layer.provide(Layer.succeed(PeerLinks.PeerLinks, b.links)),
              Layer.provide(Layer.succeedContext(b.runtime)),
              Layer.provide(Layer.succeedContext(reopened)),
              Layer.provide(
                Layer.succeed(Environment.ServerEnvironment, {
                  getEnvironmentId: Effect.succeed(b.environmentId),
                  getDescriptor: Ref.get(b.descriptor),
                }),
              ),
              Layer.fresh,
            ),
          ),
        );
        expect(yield* restored.forThread(task.childId)).toEqual(original);
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
        const denied = yield* b.authority.checkEffect("legacy-effect", threadId).pipe(Effect.flip);
        expect(denied.code).toBe("capability_denied");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);
