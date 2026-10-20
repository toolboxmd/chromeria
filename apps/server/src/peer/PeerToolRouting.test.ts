import { NodeHttpServer } from "@effect/platform-node";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  DEFAULT_SERVER_SETTINGS,
  Project,
  ProjectId,
  ProviderInstanceId,
  RuntimeRequestId,
  ScheduledTaskId,
  ThreadId,
  type OrchestrationProjectShell,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as FileSystem from "effect/FileSystem";
import { readHtmlRenderReference } from "@t3tools/shared/htmlRender";
import { resolveAttachmentPathById } from "../attachmentStore.ts";
import * as PreviewBrowser from "../preview/PreviewBrowser.ts";
import { McpSchema, McpServer } from "effect/ai";

import * as ServerConfig from "../config.ts";
import * as Settings from "../serverSettings.ts";
import * as Environment from "../environment/ServerEnvironment.ts";
import * as Engine from "../orchestration-v2/Orchestrator.ts";
import * as IssueLinks from "../issueLinks/IssueLinks.ts";
import * as Spectrum from "../spectrum/mcpToolkit.ts";
import * as SpectrumLaunch from "../spectrum/LaunchService.ts";
import * as SecretStore from "../auth/ServerSecretStore.ts";
import * as IssueHandlers from "../mcp/toolkits/issues/handlers.ts";
import { IssuesToolkit } from "../mcp/toolkits/issues/tools.ts";
import * as PullRequestHandlers from "../mcp/toolkits/pullRequests/handlers.ts";
import * as ReviewGuides from "../pullRequest/PullRequestReviewGuideService.ts";
import { PullRequestsToolkit } from "../mcp/toolkits/pullRequests/tools.ts";
import * as AttachmentHandlers from "../mcp/toolkits/attachment/handlers.ts";
import { AttachmentToolkit } from "../mcp/toolkits/attachment/tools.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as ThreadCommandExecutor from "../orchestration-v2/ThreadCommandExecutor.ts";
import * as ThreadSearch from "../orchestration-v2/ThreadSearch.ts";
import * as Projects from "../project/ProjectService.ts";
import * as ManagedFolders from "../project/ManagedProjectFolders.ts";
import * as Launches from "../orchestration-v2/ThreadLaunchService.ts";
import * as Adapters from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Providers from "../provider/ProviderRegistry.ts";
import * as Repositories from "../sourceControl/SourceControlRepositoryService.ts";
import * as Git from "../vcs/GitVcsDriver.ts";
import * as GitWorkflow from "../git/GitWorkflowService.ts";
import * as ScheduledTasks from "../scheduledTasks/ScheduledTaskService.ts";
import * as Worktrees from "../mcp/WorktreeMcpService.ts";
import * as Html from "../htmlRender/HtmlRender.ts";
import * as Devices from "../device/DeviceService.ts";
import * as Preview from "../preview/Manager.ts";
import * as Broker from "../mcp/PreviewAutomationBroker.ts";
import * as Orchestrator from "../mcp/OrchestratorMcpService.ts";
import * as Metadata from "../mcp/ThreadMetadataMcpService.ts";
import * as Invocation from "../mcp/McpInvocationContext.ts";
import * as McpHttpServer from "../mcp/McpHttpServer.ts";
import { idleThreadProjection, liveThreadShell } from "../mcp/McpToolAccess.testkit.ts";
import * as OrchestratorHandlers from "../mcp/toolkits/orchestrator/handlers.ts";
import { OrchestratorToolkit } from "../mcp/toolkits/orchestrator/tools.ts";
import * as ThreadHandlers from "../mcp/toolkits/thread/handlers.ts";
import { ThreadToolkit } from "../mcp/toolkits/thread/tools.ts";
import * as ProjectHandlers from "../mcp/toolkits/project/handlers.ts";
import { ProjectToolkit } from "../mcp/toolkits/project/tools.ts";
import * as EnvironmentHandlers from "../mcp/toolkits/environment/handlers.ts";
import { EnvironmentToolkit } from "../mcp/toolkits/environment/tools.ts";
import * as WorktreeHandlers from "../mcp/toolkits/worktree/handlers.ts";
import { WorktreeToolkit } from "../mcp/toolkits/worktree/tools.ts";
import * as PreviewHandlers from "../mcp/toolkits/preview/handlers.ts";
import { PreviewStandardToolkit } from "../mcp/toolkits/preview/tools.ts";
import * as PreviewControlsHandlers from "../mcp/toolkits/previewControls/handlers.ts";
import { PreviewControlsToolkit } from "../mcp/toolkits/previewControls/tools.ts";
import * as HtmlHandlers from "../mcp/toolkits/html/handlers.ts";
import { HtmlPreviewToolkit, HtmlRenderToolkit } from "../mcp/toolkits/html/tools.ts";
import * as PeerForwarding from "./PeerForwarding.ts";
import * as PeerLinks from "./PeerLinks.ts";
import { descriptorOf, layerLinkingEnvironment, linkTo, servePeer } from "./PeerLinks.testkit.ts";

const laptop = descriptorOf("routing-laptop", "Laptop");
const box = descriptorOf("routing-box", "Box");
const threadId = ThreadId.make("routing-thread");
const projectId = ProjectId.make("routing-project");
const capabilities = new Set<Invocation.McpCapability>([
  "orchestration",
  "preview",
  "device",
  "worktree",
  "pull-requests",
]);
const scope = (environmentId = laptop.environmentId): Invocation.McpThreadInvocationScope => ({
  environmentId,
  thread: {
    threadId,
    providerInstanceId: ProviderInstanceId.make("codex"),
    providerSessionId: "routing-provider",
  },
  requestNamespace: "routing-provider",
  client: undefined,
  capabilities,
  issuedAt: 1,
});
const client = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  clientCapabilities: {},
  clientInfo: { name: "routing-test", version: "1" },
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "routing-test", version: "1" },
  },
  getClient: Effect.die("unused"),
});
const project = Project.make({
  id: projectId,
  title: "Box project",
  workspaceRoot: "/box/project",
  defaultModelSelection: null,
  scripts: [],
  createdAt: "2026-10-14T00:00:00Z",
  updatedAt: "2026-10-14T00:00:00Z",
  deletedAt: null,
});
const projectShell: OrchestrationProjectShell = {
  ...project,
  repositoryIdentity: {
    canonicalKey: "github.com/toolboxmd/chromeria",
    locator: {
      source: "git-remote",
      remoteName: "origin",
      remoteUrl: "git@github.com:toolboxmd/chromeria.git",
    },
    provider: "github",
    displayName: "toolboxmd/chromeria",
    owner: "toolboxmd",
    name: "chromeria",
  },
};

const registrations = Layer.mergeAll(
  McpHttpServer.toolkitRegistration(IssuesToolkit, IssueHandlers.layer),
  McpHttpServer.toolkitRegistration(PullRequestsToolkit, PullRequestHandlers.layer),
  McpHttpServer.toolkitRegistration(AttachmentToolkit, AttachmentHandlers.layer),
  McpHttpServer.toolkitRegistration(Spectrum.SpectrumToolkit, Spectrum.layer),
  McpHttpServer.toolkitRegistration(OrchestratorToolkit, OrchestratorHandlers.layer),
  McpHttpServer.toolkitRegistration(ThreadToolkit, ThreadHandlers.layer),
  McpHttpServer.toolkitRegistration(ProjectToolkit, ProjectHandlers.layer),
  McpHttpServer.toolkitRegistration(EnvironmentToolkit, EnvironmentHandlers.layer),
  McpHttpServer.toolkitRegistration(WorktreeToolkit, WorktreeHandlers.layer),
  McpHttpServer.toolkitRegistration(PreviewStandardToolkit, PreviewHandlers.layerStandard),
  McpHttpServer.toolkitRegistration(PreviewControlsToolkit, PreviewControlsHandlers.layer),
  McpHttpServer.layerDeviceToolkit,
  McpHttpServer.toolkitRegistration(HtmlPreviewToolkit, HtmlHandlers.layerPreview),
  McpHttpServer.toolkitRegistration(HtmlRenderToolkit, HtmlHandlers.layerRender),
);

type Seen = { operation: string; environmentId: string; threadId?: string; input?: unknown };
const makeFixture = (realHtml = false) =>
  Effect.gen(function* () {
    const seen = yield* Ref.make<ReadonlyArray<Seen>>([]);
    const session = yield* Ref.make("");
    const destinationCapabilities = yield* Ref.make(capabilities);
    const shell = Ref.get(session).pipe(
      Effect.map((sessionId) => ({
        ...liveThreadShell(threadId),
        projectId,
        linkOrigin: { sessionId, label: "Laptop" },
      })),
    );
    const projection = shell.pipe(
      Effect.map((value) => ({
        ...idleThreadProjection(value),
        runtimeRequests: [
          { id: RuntimeRequestId.make("question"), status: "pending", kind: "user_input" },
        ],
        turnItems: [
          {
            type: "user_input_request",
            requestId: RuntimeRequestId.make("question"),
            questions: [],
          },
        ],
      })),
    );
    const dependenciesAt = (environmentId: typeof laptop.environmentId) => {
      const record = <A>(operation: string, output: A, input?: unknown) =>
        Effect.gen(function* () {
          yield* Ref.update(seen, (items) => [
            ...items,
            { operation, environmentId, threadId, ...(input === undefined ? {} : { input }) },
          ]);
          return output;
        });
      // Fakes stop at domain services. Tool schemas, declarations, routing, peer HTTP,
      // OAuth and mode narrowing are real on both sides.
      const dependencies = Layer.mergeAll(
        Layer.mock(ManagedFolders.ManagedProjectFolders)({ namedProjectsRoot: "/box/folders" }),
        Layer.mock(Launches.ThreadLaunchService)({}),
        Layer.mock(Adapters.ProviderAdapterRegistryV2)({}),
        Layer.mock(Providers.ProviderRegistry)({}),
        Layer.mock(Repositories.SourceControlRepositoryService)({}),
        Layer.mock(Git.GitVcsDriver)({}),
        Layer.mock(GitWorkflow.GitWorkflowService)({}),
        Layer.mock(Engine.OrchestratorV2)({
          getThreadShell: () => shell,
          dispatch: (command) =>
            record(command.type, { sequence: 7, storedEvents: [] } as never, command),
        }),
        Layer.mock(IssueLinks.IssueLinks)({
          forThread: () => record("issue-list", []),
          link: (input) =>
            record(
              "issue-link",
              {
                alreadyLinked: false,
                link: {
                  host: "github.com",
                  repository: "toolboxmd/chromeria",
                  number: 302,
                  url: "https://github.com/toolboxmd/chromeria/issues/302",
                },
              } as never,
              input,
            ),
        }),
        Layer.mock(SpectrumLaunch.SpectrumLaunchService)({
          register: (input) =>
            record(
              "spectrum-start",
              {
                threadId: input.threadId,
                callerThreadId: input.callerThreadId,
                mode: input.mode,
                participants: [],
                white: {
                  threadId: input.threadId,
                  label: "White",
                  selection: { instanceId: "codex", model: "gpt-5" },
                },
                delegatedTaskId: null,
              } as never,
              input,
            ),
        }),
        Layer.mock(SecretStore.ServerSecretStore)({
          getOrCreateRandom: () => record("attachment-prepare", new Uint8Array(32).fill(7)),
        }),
        Layer.mock(ThreadManagement.ThreadManagementService)({
          getThreadShell: (id) =>
            id !== threadId
              ? Effect.succeed(null)
              : environmentId === laptop.environmentId
                ? Effect.succeed({ ...liveThreadShell(threadId), projectId })
                : shell,
          getProjectThreadRecords: () => projection as never,
          stopWorkflow: (input) => record("workflow-stop", undefined, input),
          dispatch: (command) =>
            record(command.type, { sequence: 7, storedEvents: [] } as never, command),
        }),
        Layer.mock(ThreadSearch.ThreadSearch)({
          search: (input) => record("search", { matches: [] }, input),
        }),
        Layer.mock(Projects.ProjectService)({
          getById: () => record("project-read", Option.some(project)),
          update: (input) => record("project-update", project, input),
          getShell: () => Effect.succeed(Option.none()),
          listShells: () => Effect.succeed([projectShell]),
        }),
        Layer.mock(ReviewGuides.PullRequestReviewGuideService)({
          get: (input) =>
            record(
              "guide-read",
              {
                guide: null,
                run: null,
                currentHeadSha: environmentId === box.environmentId ? "box-head" : "laptop-head",
              },
              input,
            ),
          generate: (input) =>
            record(
              "guide-write",
              {
                guide: null,
                run: null,
                currentHeadSha: environmentId === box.environmentId ? "box-head" : "laptop-head",
                started: true,
              },
              input,
            ),
        }),
        Layer.mock(Settings.ServerSettingsService)({
          getSettings: record("preferences-read", DEFAULT_SERVER_SETTINGS),
          updateSettings: (input) => record("preferences-update", DEFAULT_SERVER_SETTINGS, input),
        }),
        ThreadCommandExecutor.layer,
        Layer.mock(Orchestrator.OrchestratorMcpService)({
          listScheduledTasks: () => record("schedules-list", { tasks: [] }),
          scheduleTask: (_scope, input) =>
            record("schedule-create", {
              scheduledTaskId: ScheduledTaskId.make("timer"),
              title: "Timer",
              prompt: input.prompt,
              enabled: true,
              projectId,
              boundThreadId: null,
              schedule: { type: "interval" as const, everyMs: 60_000 },
              nextRunAt: null,
              lastRunStatus: "never" as const,
            }),
          deleteScheduledTask: (_scope, input) =>
            record("schedule-delete", { scheduledTaskId: input.scheduledTaskId, deleted: true }),
          createThreads: () => record("threads-create", { threads: [] }),
          requestSecret: () => record("secret-request", { status: "declined" as const }),
        }),
        Layer.mock(Metadata.ThreadMetadataMcpService)({}),
        Layer.mock(ScheduledTasks.ScheduledTaskService)({}),
        Layer.mock(Worktrees.WorktreeMcpService)({
          status: () =>
            record("worktree-status", {
              attached: false,
              worktreePath: null,
              branch: null,
              projectWorkspaceRoot: "/box/project",
              defaultStartFromOrigin: false,
            }),
          handoff: (_scope, input) =>
            record(
              "worktree-handoff",
              {
                worktreePath: "/box/project-wt",
                branch: "work",
                baseRef: "main",
                startedFromOrigin: false,
                setupScript: { status: "no-script" as const },
                continuation: { status: "skipped" as const },
                note: "",
              },
              input,
            ),
        }),
        Layer.mock(Devices.DeviceService)({
          list: record("device-list", {
            hostStatus: "ready",
            hostStatuses: {},
            hosts: [],
            devices: [],
            sessions: [],
          } as never),
          close: (input) => record("device-close", undefined, input),
          sessionsForThread: (id) =>
            Effect.succeed(
              id === threadId
                ? [
                    {
                      threadId,
                      hostId: "local",
                      deviceId: "device",
                      platform: "ios" as const,
                      openedAt: "2026-10-14T00:00:00Z",
                    },
                  ]
                : [],
            ),
          screenshot: (input) =>
            record(
              "device-screenshot",
              {
                device: {
                  hostId: "local",
                  id: "device",
                  platform: "ios" as const,
                  name: "Test",
                  version: "27",
                  booted: true,
                  physical: false,
                },
                png: new Uint8Array([1, 2, 3]),
              },
              input,
            ),
        }),
        Layer.mock(Preview.PreviewManager)({
          list: (input) =>
            record("preview-list", { sessions: [], hostStatus: "ready" } as never, input),
          close: (input) => record("preview-close", undefined, input),
        }),
        Layer.mock(Broker.PreviewAutomationBroker)({
          invoke: ({ operation, input }) => record(`browser-${operation}`, {} as never, input),
        }),
        Layer.mock(Html.HtmlRender)({
          preview: (input) =>
            record(
              "html-preview",
              {
                png: "cG5n",
                width: 400,
                contentHeight: 300,
                capturedHeight: 300,
                consoleMessages: [],
              },
              input,
            ),
          prepare: (input) => record("html-render", input, input),
          publishPrepared: (input) =>
            record(
              "html-store",
              { attachmentId: "source-html", title: "Page", height: 300 },
              input,
            ),
        }),
        NodeCrypto.layer,
        NodeServices.layer,
        ServerConfig.layerTest(process.cwd(), { prefix: "t3-routing-" }).pipe(
          Layer.provide(NodeServices.layer),
        ),
      );
      return realHtml
        ? Html.layer.pipe(
            Layer.provideMerge(dependencies),
            Layer.provide(
              Layer.mock(PreviewBrowser.PreviewBrowser)({ installed: Effect.succeedNone }),
            ),
          )
        : dependencies;
    };
    const sourceDependencies = yield* dependenciesAt(laptop.environmentId).pipe(Layer.build);
    const destinationDependencies = yield* dependenciesAt(box.environmentId).pipe(Layer.build);
    const dependencies = Layer.succeedContext(sourceDependencies);
    const b = yield* servePeer(
      box,
      registrations.pipe(
        Layer.provide(Layer.succeedContext(destinationDependencies)),
        Layer.provide(Layer.mock(PeerForwarding.PeerForwarding)({})),
      ),
      {
        scopeForThread: (id) =>
          Ref.get(destinationCapabilities).pipe(
            Effect.map((granted) =>
              id === threadId ? { ...scope(box.environmentId), capabilities: granted } : undefined,
            ),
          ),
      },
    );
    const linking = yield* layerLinkingEnvironment(laptop).pipe(Layer.build);
    const here = yield* registrations.pipe(
      Layer.provideMerge(McpServer.McpServer.layer),
      Layer.provide(dependencies),
      Layer.provide(
        PeerForwarding.layer.pipe(
          Layer.provideMerge(Layer.succeedContext(linking)),
          Layer.provide(dependencies),
        ),
      ),
      Layer.provide(
        Layer.mock(Environment.ServerEnvironment)({ getDescriptor: Effect.succeed(laptop) }),
      ),
      Layer.build,
    );
    const links = Context.get(linking, PeerLinks.PeerLinks);
    const server = Context.get(here, McpServer.McpServer);
    const call = (
      name: string,
      args: Record<string, unknown>,
      context = false,
      granted = capabilities,
      target: typeof box.environmentId | null = box.environmentId,
    ) =>
      server
        .callTool({
          name,
          arguments: {
            ...args,
            ...(target === null ? {} : { environmentId: target }),
            ...(context ? { contextThreadId: threadId } : {}),
          },
        })
        .pipe(
          Effect.provideService(Invocation.McpInvocationContext, {
            ...scope(),
            capabilities: granted,
          }),
          Effect.provideService(McpSchema.McpServerClient, client),
        );
    const link = (access: "full-access" | "auto" | "read-only") =>
      linkTo(links, b, access).pipe(
        Effect.andThen(b.linkedSessions),
        Effect.flatMap((sessions) => Ref.set(session, sessions.at(-1)!.sessionId)),
      );
    return {
      call,
      link,
      seen,
      b,
      destinationCapabilities,
      sourceDependencies,
      destinationDependencies,
      session,
    };
  });

const fixture = makeFixture();

const cases = [
  {
    group: "scheduled creation",
    name: "schedule_task",
    args: {
      projectId,
      prompt: "Do peer work",
      schedule: { type: "interval", everyMs: 60_000 },
      bindToCurrentThread: false,
    },
    operation: "schedule-create",
  },
  {
    group: "batch thread creation",
    name: "create_threads",
    args: { threads: [{ prompt: "remote" }] },
    operation: "threads-create",
    context: true,
  },
  {
    group: "Issue links",
    name: "link_issue",
    args: { url: "https://github.com/toolboxmd/chromeria/issues/302" },
    operation: "issue-link",
    context: true,
  },
  {
    group: "PR links",
    name: "link_pull_request",
    args: { threadId, url: "https://github.com/toolboxmd/chromeria/pull/290" },
    operation: "thread.pull-request.link",
  },
  {
    group: "Spectrum",
    name: "start_spectrum",
    args: {
      callerThreadId: threadId,
      question: "remote",
      colors: [{ label: "one" }, { label: "two" }],
      mode: "council",
    },
    operation: "spectrum-start",
  },
  {
    group: "schedules",
    name: "delete_scheduled_task",
    args: { scheduledTaskId: "task" },
    operation: "schedule-delete",
  },
  {
    group: "thread lifecycle",
    name: "t3_thread_fork",
    args: { threadId, sourcePoint: { type: "latest_stable" } },
    operation: "thread.fork",
  },
  {
    group: "native workflows",
    name: "t3_workflow_stop",
    args: { threadId, subagentId: "workflow-coordinator" },
    operation: "workflow-stop",
  },
  {
    group: "thread configuration",
    name: "t3_thread_configure",
    args: { threadId, modelSelection: { instanceId: "codex", model: "gpt-5" } },
    operation: "thread.model-selection.set",
  },
  {
    group: "thread organization",
    name: "t3_thread_organize",
    args: { threadId, action: "pin" },
    operation: "thread.pin",
  },
  {
    group: "queues",
    name: "t3_queue_edit",
    args: { threadId, queuedRunId: "queued", text: "remote" },
    operation: "queued-run.edit",
  },
  {
    group: "projects",
    name: "t3_project_update",
    args: { projectId, title: "Remote" },
    operation: "project-update",
  },
  {
    group: "preferences",
    name: "t3_environment_preferences_update",
    args: { newWorktreesStartFromOrigin: true },
    operation: "preferences-update",
  },
  {
    group: "worktrees",
    name: "t3_worktree_handoff",
    args: { baseRef: "main", branch: "work" },
    operation: "worktree-handoff",
    context: true,
  },
  {
    group: "secrets",
    name: "request_secret",
    args: { label: "A secret", reason: "Test", clientRequestId: "secret-1" },
    operation: "secret-request",
    context: true,
  },
  {
    group: "browser automation",
    name: "preview_press",
    args: { key: "Escape" },
    operation: "browser-press",
    context: true,
  },
  {
    group: "devices",
    name: "device_close",
    args: { deviceId: "device" },
    operation: "device-close",
    context: true,
  },
  {
    group: "HTML",
    name: "html_render",
    args: { html: "<p>remote</p>", title: "Page", height: 300 },
    operation: "html-render",
    context: true,
  },
];

it.effect("native guide tools reach the peer without broadening their capability policy", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const f = yield* fixture;
      const target = { url: "https://github.com/toolboxmd/chromeria/pull/304" };
      yield* f.link("auto");
      const write = yield* f.call("write_pull_request_guides", { pullRequests: [target] });
      expect(write.isError, JSON.stringify(write)).toBe(false);
      expect(write.structuredContent).toMatchObject({
        guides: [{ repository: "toolboxmd/chromeria", number: 304, outcome: "started" }],
      });
      expect(yield* Ref.get(f.seen)).toEqual([
        expect.objectContaining({
          operation: "guide-write",
          environmentId: box.environmentId,
          input: { projectId, host: "github.com", repository: "toolboxmd/chromeria", number: 304 },
        }),
      ]);
      yield* Ref.set(f.seen, []);
      yield* f.link("read-only");
      const refused = yield* f.call("write_pull_request_guides", { pullRequests: [target] });
      expect(refused.isError, JSON.stringify(refused)).toBe(true);
      expect(yield* Ref.get(f.seen)).toEqual([]);
      const read = yield* f.call("read_pull_request_guide", target);
      expect(read.isError, JSON.stringify(read)).toBe(false);
      expect(read.structuredContent).toMatchObject({ currentHeadSha: "box-head", stale: false });
      expect(yield* Ref.get(f.seen)).toEqual([
        expect.objectContaining({ operation: "guide-read", environmentId: box.environmentId }),
      ]);
      yield* Ref.set(f.seen, []);
      yield* f.link("auto");
      const noCapability = new Set<Invocation.McpCapability>(["orchestration"]);
      const sourceRefused = yield* f.call(
        "write_pull_request_guides",
        { pullRequests: [target] },
        false,
        noCapability,
      );
      expect(sourceRefused.isError).toBe(true);
      expect(yield* Ref.get(f.seen)).toEqual([]);
      yield* f.link("full-access");
      yield* Ref.set(f.destinationCapabilities, noCapability);
      const destinationRefused = yield* f.call("read_pull_request_guide", target, true);
      expect(destinationRefused.isError).toBe(true);
      expect(yield* Ref.get(f.seen)).toEqual([]);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("new native tools retain local execution when no environment is selected", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const f = yield* fixture;
      const target = { url: "https://github.com/toolboxmd/chromeria/pull/304" };
      for (const [name, args] of [
        ["t3_workflow_stop", { threadId, subagentId: "workflow-coordinator" }],
        ["write_pull_request_guides", { pullRequests: [target] }],
        ["read_pull_request_guide", target],
      ] as const) {
        const result = yield* f.call(name, args, false, capabilities, null);
        expect(result.isError, `${name}: ${JSON.stringify(result)}`).toBe(false);
        if (name === "read_pull_request_guide") {
          expect(result.structuredContent).toMatchObject({ currentHeadSha: "laptop-head" });
        }
      }
      expect(
        (yield* Ref.get(f.seen)).map(({ operation, environmentId }) => ({
          operation,
          environmentId,
        })),
      ).toEqual([
        { operation: "workflow-stop", environmentId: laptop.environmentId },
        { operation: "guide-write", environmentId: laptop.environmentId },
        { operation: "guide-read", environmentId: laptop.environmentId },
      ]);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect(
  "pending request mutations refuse an old peer before its unverified request handler",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* f.link("full-access");
        const before = yield* Ref.get(f.seen);
        const result = yield* f.call(
          "t3_pending_request_respond",
          { threadId, requestId: "question", answers: {} },
          false,
        );
        expect(result.isError).toBe(true);
        expect(JSON.stringify(result)).toContain(
          "does not support verified pending request actions",
        );
        expect(yield* Ref.get(f.seen)).toEqual(before);
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

// Lawful linked close/reopen is exercised with the actual authority owner in
// PeerAuthority.workflow.test.ts: "modern opener survives authenticated reads...".
// This older routing fixture intentionally has no verified original PeerAuthority.
it.effect(
  "legacy preview close stays local but linked close requires current original authority",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        const local = yield* f.call("t3_preview_close", { tabId: "tab" }, true, capabilities, null);
        expect(local.isError, JSON.stringify(local)).toBe(false);
        expect(yield* Ref.get(f.seen)).toContainEqual(
          expect.objectContaining({
            operation: "preview-close",
            environmentId: laptop.environmentId,
          }),
        );
        yield* Ref.set(f.seen, []);
        yield* f.link("full-access");
        const unverified = yield* f.call("t3_preview_close", { tabId: "tab" }, true);
        expect(unverified.isError, JSON.stringify(unverified)).toBe(true);
        expect(JSON.stringify(unverified)).toContain(
          "Current original preview authority is unavailable",
        );
        expect(yield* Ref.get(f.seen)).toEqual([]);
        yield* f.link("auto");
        const narrowed = yield* f.call("t3_preview_close", { tabId: "tab" }, true);
        expect(narrowed.isError, JSON.stringify(narrowed)).toBe(true);
        expect(yield* Ref.get(f.seen)).toEqual([]);
        yield* f.link("full-access");
        const noPreview = yield* f.call(
          "t3_preview_close",
          { tabId: "tab" },
          true,
          new Set(["orchestration"]),
        );
        expect(noPreview.isError).toBe(true);
        expect(yield* Ref.get(f.seen)).toEqual([]);
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

const automationCases = cases.filter(
  (test) => test.name === "schedule_task" || test.name === "start_spectrum",
);

it.effect.each(automationCases)(
  "$group refuses this old peer before its unverified service",
  (test) =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        for (const access of ["full-access", "auto"] as const) {
          yield* f.link(access);
          const before = yield* Ref.get(f.seen);
          const result = yield* f.call(test.name, test.args, test.context);
          expect(result.isError, JSON.stringify(result)).toBe(true);
          if (access === "full-access")
            expect(JSON.stringify(result)).toContain(
              "does not support destination automation authority v1",
            );
          expect(yield* Ref.get(f.seen)).toEqual(before);
        }
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect.each(cases.filter((test) => !automationCases.includes(test)))(
  "$group reaches the peer and refuses a narrower link",
  (test) =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* f.link("full-access");
        const result = yield* f.call(test.name, test.args, test.context);
        expect(result.isError, JSON.stringify(result)).toBe(false);
        expect(yield* Ref.get(f.seen)).toContainEqual(
          expect.objectContaining({ operation: test.operation, environmentId: box.environmentId }),
        );
        const before = (yield* Ref.get(f.seen)).length;
        yield* f.link("auto");
        const refused = yield* f.call(test.name, test.args, test.context);
        expect(refused.isError, JSON.stringify(refused)).toBe(true);
        expect((yield* Ref.get(f.seen)).length).toBe(before);
        expect((yield* Ref.get(f.b.bearers)).length).toBeGreaterThan(0);
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("remote reads return the destination's state", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.link("full-access");
      for (const [name, args, context] of [
        ["device_screenshot", { deviceId: "device", includeImage: false }, true],
        ["t3_environment_read", {}, false],
        ["t3_project_read", { projectId }, false],
        ["list_scheduled_tasks", {}, false],
        ["t3_thread_configuration", { threadId }, false],
        ["t3_thread_transfers", { threadId }, false],
        ["t3_thread_search", { query: "find" }, false],
        ["t3_queue_list", { threadId }, false],
        ["t3_pending_request_list", { threadId }, false],
        ["t3_worktree_status", {}, true],
        ["device_list", {}, true],
        ["list_thread_issues", {}, true],
        ["list_thread_pull_requests", { threadId }, false],
        [
          "read_pull_request_guide",
          { url: "https://github.com/toolboxmd/chromeria/pull/304" },
          false,
        ],
        ["html_preview", { html: "<p>remote</p>" }, true],
      ] as const) {
        const result = yield* f.call(name, args, context);
        expect(result.isError, `${name}: ${JSON.stringify(result)}`).toBe(false);
      }
      expect(
        (yield* Ref.get(f.seen)).every((item) => item.environmentId === box.environmentId),
      ).toBe(true);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("destination contexts retain capability grants and link ownership", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.link("full-access");
      const result = yield* f.call(
        "device_close",
        { deviceId: "device" },
        true,
        new Set(["orchestration"]),
      );
      expect(result.isError).toBe(true);
      expect(yield* Ref.get(f.seen)).toEqual([]);
      yield* Ref.set(f.destinationCapabilities, new Set(["orchestration"]));
      const destinationRefused = yield* f.call("device_close", { deviceId: "device" }, true);
      expect(destinationRefused.isError).toBe(true);
      expect(yield* Ref.get(f.seen)).toEqual([]);
      const wrong = yield* f.call("device_close", {
        deviceId: "device",
        contextThreadId: "other-thread",
      });
      expect(wrong.isError).toBe(true);
      expect(yield* Ref.get(f.seen)).toEqual([]);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect(
  "attachment uploads identify their destination and a read-only link cannot create them",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* f.link("full-access");
        const args = { upload: { name: "photo.png", mimeType: "image/png", sizeBytes: 10 } };
        const uploaded = yield* f.call("t3_attachment_prepare_upload", args);
        expect(uploaded.isError, JSON.stringify(uploaded)).toBe(false);
        expect(uploaded.structuredContent).toMatchObject({
          uploadOrigin: f.b.url,
          relativeUrl: expect.stringMatching(/^\/api\/attachments\/upload\//),
        });
        yield* f.link("read-only");
        const refused = yield* f.call("t3_attachment_prepare_upload", args);
        expect(refused.isError).toBe(true);
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("peer screenshots preserve PNG bytes through the production image registration", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.link("full-access");
      const result = yield* f.call("device_screenshot", { deviceId: "device" }, true);
      expect(result.isError).toBe(false);
      const image = result.content.find((part) => part.type === "image");
      expect(image?.data).toEqual(new Uint8Array([1, 2, 3]));
      const textOnly = yield* f.call(
        "device_screenshot",
        { deviceId: "device", includeImage: false },
        true,
      );
      expect(textOnly.isError).toBe(false);
      expect(textOnly.content.some((part) => part.type === "image")).toBe(false);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("forwarded HTML is a consumable source-thread attachment with peer images", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const f = yield* makeFixture(true);
      yield* f.link("full-access");
      const fs = Context.get(f.destinationDependencies, FileSystem.FileSystem);
      const remote = Context.get(f.destinationDependencies, ServerConfig.ServerConfig);
      const image = `${remote.attachmentsDir}/peer.png`;
      yield* fs.writeFile(image, new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]));
      const result = yield* f.call(
        "html_render",
        { html: `<p>Remote page</p><img src="${image}">`, title: "Page", height: 300 },
        true,
      );
      expect(result.isError, JSON.stringify(result)).toBe(false);
      const reference = readHtmlRenderReference(
        (result.structuredContent as { htmlRender: unknown }).htmlRender,
      );
      expect(reference).toBeDefined();
      const source = Context.get(f.sourceDependencies, ServerConfig.ServerConfig);
      const file = resolveAttachmentPathById({
        attachmentsDir: source.attachmentsDir,
        attachmentId: reference!.attachmentId,
      });
      expect(file).not.toBeNull();
      const stored = yield* fs.readFileString(file!);
      expect(stored).toContain("<p>Remote page</p>");
      expect(stored).toContain("data:image/png;base64,iVBORw0KGgo=");
      expect(stored).not.toContain(image);
      expect(stored.match(/id="t3-theme"/g)).toHaveLength(1);
      expect(yield* fs.readDirectory(remote.attachmentsDir)).toEqual(["peer.png"]);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("Spectrum refuses to use a caller belonging to another link", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.link("full-access");
      yield* Ref.set(f.session, "another-link");
      const result = yield* f.call("start_spectrum", {
        callerThreadId: threadId,
        question: "Q",
        colors: [
          { label: "Blue", selection: { instanceId: "codex", model: "test" } },
          { label: "Red", selection: { instanceId: "codex", model: "test" } },
        ],
        mode: "council",
      });
      expect(result.isError, JSON.stringify(result)).toBe(true);
      expect(yield* Ref.get(f.seen)).not.toContainEqual(
        expect.objectContaining({ operation: "spectrum-start" }),
      );
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);
