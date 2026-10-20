import * as DocumentText from "../provider/DocumentText.ts";
import { McpSchema, McpServer } from "effect/ai";
import * as McpHttpServer from "../mcp/McpHttpServer.ts";
import * as RequestReceipts from "../mcp/McpRequestReceipts.ts";
import { ThreadMcpToolkit } from "../mcp/toolkits/threadMcp/tools.ts";
import * as ThreadMcpHandlers from "../mcp/toolkits/threadMcp/handlers.ts";
import * as ThreadMcpConfig from "./ThreadMcpConfig.ts";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as BareLaunch from "./BareLaunch.ts";
import { registerPromachosLaunchTests } from "../promachos/PromachosLaunch.tests.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import * as WorktreeSetupTracker from "../project/WorktreeSetupTracker.ts";
import * as ProjectCloneTracker from "../project/ProjectCloneTracker.ts";
import * as TerminalManager from "../terminal/Manager.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as FileSystem from "effect/FileSystem";
import * as ServerConfig from "../config.ts";
import { createPendingAttachmentId, resolveAttachmentPath } from "../attachmentStore.ts";
import * as ThreadMessageIntake from "./ThreadMessageIntake.ts";
import { assert, expect, it, vi } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import {
  ChatAttachmentId,
  ComposerContextId,
  type ChatAttachment,
  CommandId,
  DEFAULT_SERVER_SETTINGS,
  GitCommandError,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  OrchestrationV2ThreadProjectionJson,
  OrchestratorMcpFailure,
  ScheduledTaskError,
  ScheduledTaskId,
  type ServerProvider,
  ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Result from "effect/Result";
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";

import * as GitWorkflow from "../git/GitWorkflowService.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProjectSetupScriptRunner from "../project/ProjectSetupScriptRunner.ts";
import * as ManagedProjectFolders from "../project/ManagedProjectFolders.ts";
import * as ProviderRegistryMock from "../provider/testUtils/providerRegistryMock.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as OrchestratorMcp from "../mcp/OrchestratorMcpService.ts";
import * as Invocation from "../mcp/McpInvocationContext.ts";
import { assertSameLink, assertNotLinked } from "../mcp/linkOrigin.ts";
import * as PeerForwarding from "../peer/PeerForwarding.ts";
import * as PeerLinks from "../peer/PeerLinks.ts";
import * as PeerMcpClient from "../peer/PeerMcpClient.ts";
import { OrchestratorToolkit } from "../mcp/toolkits/orchestrator/tools.ts";
import * as ScheduledTasks from "../scheduledTasks/ScheduledTaskService.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as TextGeneration from "../textGeneration/TextGeneration.ts";
import { ClaudeProviderCapabilitiesV2 } from "./Adapters/ClaudeAdapterV2.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ThreadLaunch from "./ThreadLaunchService.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as OrchestratorMcpService from "../mcp/OrchestratorMcpService.ts";
import * as RemoteDelegation from "../peer/RemoteDelegation.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";
import * as ThreadTitleRegeneration from "./ThreadTitleRegenerationService.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import * as RemoteDelegation from "../peer/RemoteDelegation.ts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

const projectId = ProjectId.make("project:launch-test");
const otherProjectId = ProjectId.make("project:launch-other");
const isTaskError = Schema.is(ScheduledTaskError);
const encodeThreadProjection = Schema.encodeEffect(OrchestrationV2ThreadProjectionJson);
const modelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5.1-codex",
} as const;
const project = {
  id: projectId,
  title: "Project",
  workspaceRoot: "/repo",
  repositoryIdentity: null,
  faviconPath: null,
  defaultModelSelection: modelSelection,
  defaultThreadEnvMode: null,
  scripts: [],
  createdAt: "2026-06-20T00:00:00.000Z",
  updatedAt: "2026-06-20T00:00:00.000Z",
  deletedAt: null,
} as const;

const otherProject = {
  ...project,
  id: otherProjectId,
  title: "Other",
  workspaceRoot: "/other-repo",
} as const;

const adapter = {
  instanceId: modelSelection.instanceId,
  driver: ProviderDriverKind.make("codex"),
  bareSupport: Effect.succeed({ supported: true }),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("provider execution is disabled in launch tests"),
} as ProviderAdapter.ProviderAdapterV2["Service"];

const claudeModelSelection = {
  instanceId: ProviderInstanceId.make("claudeAgent"),
  model: "claude-opus-5-5",
} as const;
const claudeAdapter = {
  instanceId: claudeModelSelection.instanceId,
  driver: ProviderDriverKind.make("claudeAgent"),
  getCapabilities: () => Effect.succeed(ClaudeProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("provider execution is disabled in launch tests"),
} as ProviderAdapter.ProviderAdapterV2["Service"];

interface HarnessOptions {
  readonly closeTerminal?: TerminalManager.TerminalManager["Service"]["close"];
  readonly adapters?: ReadonlyArray<ProviderAdapter.ProviderAdapterV2["Service"]>;
  readonly bare?: boolean;
  readonly otherWorkspaceRoot?: string;
  readonly databaseLayer?: typeof SqlitePersistence.layerMemory;
  readonly managedFolders?: Layer.Layer<ManagedProjectFolders.ManagedProjectFolders>;
  readonly createWorktree?: GitWorkflow.GitWorkflowService["Service"]["createWorktree"];
  readonly fetchWorktreeBaseCommit?: GitWorkflow.GitWorkflowService["Service"]["fetchWorktreeBaseCommit"];
  readonly remoteExists?: GitWorkflow.GitWorkflowService["Service"]["remoteExists"];
  readonly hasCommit?: GitWorkflow.GitWorkflowService["Service"]["hasCommit"];
  readonly renameBranch?: GitWorkflow.GitWorkflowService["Service"]["renameBranch"];
  readonly runSetup?: ProjectSetupScriptRunner.ProjectSetupScriptRunner["Service"]["runForThread"];
  readonly generateTitle?: TextGeneration.TextGeneration["Service"]["generateThreadTitle"];
  readonly generateBranchName?: TextGeneration.TextGeneration["Service"]["generateBranchName"];
  readonly serverSettings?: Parameters<typeof ServerSettings.layerTest>[0];
  readonly providers?: ReadonlyArray<ServerProvider>;
  readonly git?: Partial<GitWorkflow.GitWorkflowService["Service"]>;
  readonly gitVcs?: Partial<GitVcsDriver.GitVcsDriver["Service"]>;
}

function makeHarness(options: HarnessOptions = {}) {
  const layerDatabase = options.databaseLayer ?? SqlitePersistence.layerMemory;
  const layerRegistry = ProviderAdapterRegistry.layerFromAdapters(options.adapters ?? [adapter]);
  const layerOrchestrator = ProviderReplayHarness.layerWithRegistry(
    { name: "thread-launch" },
    layerRegistry,
    { databaseLayer: layerDatabase, runEffectWorker: false },
  );
  const layerServerSettings = ServerSettings.layerTest(options.serverSettings);
  const layerReceipts = CommandReceiptStore.layer.pipe(Layer.provide(layerDatabase));
  const layerThreadManagement = Layer.fresh(ThreadManagement.layer).pipe(
    Layer.provide(layerOrchestrator),
    Layer.provide(layerServerSettings),
    Layer.provide(layerReceipts),
  );
  const layerOutbox = EffectOutbox.layer.pipe(Layer.provide(layerDatabase));
  const createWorktree = vi.fn(
    options.createWorktree ??
      ((input) =>
        Effect.succeed({
          worktree: { path: "/repo-worktrees/feature", refName: input.newRefName, headSha: "abc" },
        } as never)),
  );
  const renameBranch = vi.fn(
    options.renameBranch ?? ((input) => Effect.succeed({ branch: input.newBranch })),
  );
  const removeWorktree = vi.fn(
    (_input: Parameters<GitWorkflow.GitWorkflowService["Service"]["removeWorktree"]>[0]) =>
      Effect.void,
  );
  const runSetup = vi.fn(
    options.runSetup ?? (() => Effect.succeed({ status: "no-script" as const })),
  );
  const closeTerminal = vi.fn(options.closeTerminal ?? (() => Effect.void));
  const generateBranchName = vi.fn(
    options.generateBranchName ?? (() => Effect.succeed({ branch: "generated-branch" })),
  );
  const generateThreadTitle = vi.fn(
    options.generateTitle ?? (() => Effect.succeed({ title: "Generated title" })),
  );
  const layerExternalServices = Layer.mergeAll(
    WorktreeSetupTracker.layer,
    Layer.mock(ProjectCloneTracker.ProjectCloneTracker)({ get: () => Effect.succeed(null) }),
    Layer.mock(TerminalManager.TerminalManager)({ close: closeTerminal }),
    Layer.succeed(ProjectService.ProjectService, {
      create: () => Effect.die("unused"),
      bootstrap: () => Effect.die("unused"),
      update: () => Effect.die("unused"),
      delete: () => Effect.die("unused"),
      listPage: () => Effect.die("unused"),
      getById: (id) =>
        Effect.succeed(
          id === projectId
            ? Option.some(project)
            : id === otherProjectId
              ? Option.some({
                  ...otherProject,
                  workspaceRoot: options.otherWorkspaceRoot ?? otherProject.workspaceRoot,
                })
              : Option.none(),
        ),
      getByWorkspaceRoot: () => Effect.succeedSome(project),
      snapshot: Effect.die("unused"),
      getShell: () => Effect.die("unused"),
      listShells: () => Effect.die("unused"),
    }),
    Layer.mock(GitWorkflow.GitWorkflowService)({
      createWorktree,
      renameBranch,
      fetchWorktreeBaseCommit:
        options.fetchWorktreeBaseCommit ??
        (() => Effect.succeed({ commitSha: "remote-main-sha", remoteRefName: "origin/main" })),
      hasCommit: options.hasCommit ?? (() => Effect.succeed(false)),
      resolveWorktreeBaseRemote: () => Effect.succeed({ remoteName: "origin", branchName: "main" }),
      remoteExists: options.remoteExists ?? (() => Effect.succeed(true)),
      remoteBranchExists: () => Effect.succeed(true),
      removeWorktree,
      ...options.git,
    }),
    Layer.succeed(ProjectSetupScriptRunner.ProjectSetupScriptRunner, {
      runForThread: runSetup,
    }),
    Layer.mock(TextGeneration.TextGeneration)({
      generateThreadTitle,
      generateBranchName,
    }),
    layerServerSettings,
    ProviderRegistryMock.layer(options.providers),
    options.managedFolders ??
      Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
        namedProjectsRoot: "/projects",
        isScratchProject: () => Effect.succeed(false),
        folderForThread: () => Effect.succeedNone,
      }),
  );
  const bareLayer = options.bare
    ? Layer.fresh(BareLaunch.layer).pipe(
        Layer.provide(Layer.mergeAll(layerRegistry, layerExternalServices)),
        Layer.provide(NodeServices.layer),
      )
    : Layer.mock(BareLaunch.BareLaunch)({});
  const layerLaunch = ThreadLaunch.layer
    .pipe(Layer.provide(bareLayer))
    .pipe(Layer.provide(layerRegistry))
    .pipe(
      Layer.provide(
        ThreadMcpConfig.layer.pipe(
          Layer.provide(ServerSecretStore.layer),
          Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "t3-tool-policy-" })),
          Layer.provide(NodeServices.layer),
        ),
      ),
    )
    .pipe(
      Layer.provide(
        Layer.mergeAll(
          layerExternalServices,
          layerThreadManagement,
          layerReceipts,
          IdAllocator.layer,
        ),
      ),
    );
  const layerProjectedProjects = Layer.mock(ProjectStore.ProjectStoreV2)({
    get: (requestedProjectId) =>
      Effect.succeed(
        requestedProjectId === projectId
          ? Option.some({
              projectId,
              title: project.title,
              workspaceRoot: project.workspaceRoot,
              defaultModelSelection: project.defaultModelSelection,
              defaultThreadEnvMode: null,
              autoPull: false,
              faviconPath: null,
              projectIcon: null,
              scripts: project.scripts,
              createdAt: project.createdAt,
              updatedAt: project.updatedAt,
              deletedAt: project.deletedAt,
            })
          : Option.none(),
      ),
  });
  const layerTitleRegeneration = ThreadTitleRegeneration.layer.pipe(
    Layer.provide(
      Layer.mergeAll(layerThreadManagement, layerProjectedProjects, layerExternalServices),
    ),
  );
  return {
    bareLayer,
    layer: Layer.mergeAll(
      layerRegistry,
      layerLaunch,
      layerRegistry,
      layerThreadManagement,
      layerTitleRegeneration,
      layerOutbox,
      layerDatabase,
      layerExternalServices,
      NodeServices.layer,
      Layer.mock(GitVcsDriver.GitVcsDriver)(options.gitVcs ?? {}),
      ProjectStore.layer.pipe(Layer.provide(layerDatabase)),
    ),
    createWorktree,
    removeWorktree,
    renameBranch,
    generateBranchName,
    generateThreadTitle,
    runSetup,
    closeTerminal,
  };
}

function launchInput(input: {
  readonly command: string;
  readonly thread: string;
  readonly message?: string;
  readonly workspace?: ThreadLaunch.ThreadLaunchWorkspaceStrategy;
}) {
  return {
    commandId: CommandId.make(input.command),
    threadId: ThreadId.make(input.thread),
    projectId,
    title: "New thread",
    modelSelection,
    runtimeMode: "full-access" as const,
    interactionMode: "default" as const,
    workspaceStrategy: input.workspace ?? { type: "root" as const },
    ...(input.message === undefined
      ? {}
      : {
          initialMessage: {
            messageId: MessageId.make(`${input.message}:id`),
            text: input.message,
            attachments: [],
          },
        }),
    createdBy: "user" as const,
    creationSource: "web" as const,
  };
}

it.effect.each(
  (["new", "existing"] as const).flatMap((target) =>
    (["user", "agent"] as const).map((createdBy) => ({ target, createdBy })),
  ),
)(
  "attributes $createdBy-configured automations in $target threads without changing their prompt",
  ({ target, createdBy }) => {
    const harness = makeHarness();
    const layerScheduledTasks = ScheduledTasks.layer.pipe(
      Layer.provide(Layer.mock(BareLaunch.BareLaunch)({})),
      Layer.provide(
        Layer.mergeAll(
          harness.layer,
          NodeCrypto.layer,
          Scheduler.layer,
          Layer.mock(SecretRequests.SecretRequests)({}),
          Layer.mock(RemoteDelegation.RemoteDelegation)({}),
        ),
      ),
    );
    return Effect.gen(function* () {
      const tasks = yield* ScheduledTasks.ScheduledTaskService;
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const existing =
        target === "existing"
          ? yield* launches.launch(
              launchInput({ command: "command:existing", thread: "thread:existing" }),
            )
          : null;
      const { task } = yield* tasks.upsert({
        purposeIntent: { version: 1, intent: { type: "unscoped" } },
        id: ScheduledTaskId.make("scheduled-task:attribution"),
        title: "Daily audit",
        prompt: "Audit performance and crashes.",
        enabled: false,
        schedule: { type: "interval", everyMs: 60_000 },
        projectId,
        threadId: existing?.threadId ?? null,
        workspaceStrategy: { type: "root" },
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        createdBy,
        creationSource: createdBy === "agent" ? "mcp" : "web",
      });
      const result = yield* tasks.runNow({ id: task.id });
      assert.equal(result.task.lastRunStatus, "succeeded");
      const projectThreads = yield* threads.listProjectThreads({
        projectId,
        includeSubagents: false,
      });
      const thread =
        projectThreads.find((candidate) => candidate.id === existing?.threadId) ??
        projectThreads[0];
      assert.isDefined(thread);
      assert.equal(result.threadId, thread!.id);
      const projection = yield* threads.getThreadProjection(thread!.id);
      const wire = yield* encodeThreadProjection(projection);
      assert.equal(wire.messages[0]?.text, task.prompt);
      assert.equal(wire.messages[0]?.scheduledTaskId, task.id);
      assert.equal(wire.messages[0]?.createdBy, createdBy);
      const turnItem = wire.turnItems.find(
        (item): item is Extract<typeof item, { type: "user_message" }> =>
          item.type === "user_message",
      );
      assert.equal(turnItem?.text, task.prompt);
      assert.equal(turnItem?.scheduledTaskId, task.id);
    }).pipe(Effect.provide(Layer.mergeAll(harness.layer, layerScheduledTasks)));
  },
);

it.effect(
  "returns no dispatched thread when sending an automation to a missing thread fails",
  () => {
    const harness = makeHarness();
    const layerScheduledTasks = ScheduledTasks.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          harness.layer,
          NodeCrypto.layer,
          Scheduler.layer,
          Layer.mock(SecretRequests.SecretRequests)({}),
        ),
      ),
    );
    return Effect.gen(function* () {
      const tasks = yield* ScheduledTasks.ScheduledTaskService;
      const threadId = ThreadId.make("thread:missing");
      const { task } = yield* tasks.upsert({
        title: "Daily audit",
        prompt: "Audit performance and crashes.",
        enabled: false,
        schedule: { type: "interval", everyMs: 60_000 },
        projectId,
        threadId,
        workspaceStrategy: { type: "root" },
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        createdBy: "user",
        creationSource: "web",
      });
      const result = yield* tasks.runNow({ id: task.id });
      assert.equal(result.task.lastRunStatus, "failed");
      assert.isNull(result.threadId);
      assert.equal(result.task.threadId, threadId);
    }).pipe(Effect.provide(Layer.mergeAll(harness.layer, layerScheduledTasks)));
  },
);

it.effect("runs a thread-bound automation on the provider the thread uses now", () => {
  const harness = makeHarness({ adapters: [adapter, claudeAdapter] });
  const layerScheduledTasks = ScheduledTasks.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        harness.layer,
        NodeCrypto.layer,
        Scheduler.layer,
        Layer.mock(SecretRequests.SecretRequests)({}),
      ),
    ),
  );
  return Effect.gen(function* () {
    const tasks = yield* ScheduledTasks.ScheduledTaskService;
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const launched = yield* launches.launch(
      launchInput({ command: "command:switched", thread: "thread:switched" }),
    );
    const { task } = yield* tasks.upsert({
      id: ScheduledTaskId.make("scheduled-task:switched"),
      title: "Hourly check",
      prompt: "Check on the delegated workers.",
      enabled: false,
      schedule: { type: "interval", everyMs: 3_600_000 },
      projectId,
      threadId: launched.threadId,
      workspaceStrategy: { type: "root" },
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      createdBy: "agent",
      creationSource: "mcp",
    });
    yield* threads.dispatch({
      type: "thread.model-selection.set",
      commandId: CommandId.make("command:switch-selection"),
      threadId: launched.threadId,
      modelSelection: claudeModelSelection,
    });

    const result = yield* tasks.runNow({ id: task.id });
    assert.equal(result.task.lastRunStatus, "succeeded");
    const projection = yield* threads.getThreadProjection(launched.threadId);
    const message = projection.messages.find((item) => item.scheduledTaskId === task.id);
    const run = projection.runs.find((item) => item.id === message?.runId);
    assert.deepStrictEqual(run?.modelSelection, claudeModelSelection);
    assert.deepStrictEqual(projection.thread.modelSelection, claudeModelSelection);
  }).pipe(Effect.provide(Layer.mergeAll(harness.layer, layerScheduledTasks)));
});

it.effect("retains automation and sender attribution while a message waits in the queue", () => {
  const harness = makeHarness({ runSetup: () => Effect.never });
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const launched = yield* launches.launch(
      launchInput({
        command: "command:automation:queue",
        thread: "thread:automation:queue",
        message: "First message",
        workspace: { type: "worktree", baseRef: "main" },
      }),
    );
    const scheduledTaskId = ScheduledTaskId.make("scheduled-task:queued");
    const senderThreadId = ThreadId.make("thread:agent-sender");
    yield* launches.launch(
      launchInput({ command: "command:agent-sender", thread: senderThreadId }),
    );
    const queued = yield* threads.sendToThread({
      projectId,
      commandId: CommandId.make("command:automation:queued"),
      threadId: launched.threadId,
      messageId: MessageId.make("message:automation:queued"),
      scheduledTaskId,
      senderThreadId,
      text: "Run the audit",
      attachments: [],
      mode: "queue",
      createdBy: "agent",
      creationSource: "mcp",
    });
    assert.equal(queued.delivery, "queued");
    const projection = yield* threads.getThreadProjection(launched.threadId);
    const message = projection.messages.find((item) => item.id === queued.message.id);
    assert.equal(message?.scheduledTaskId, scheduledTaskId);
    assert.equal(message?.senderThreadId, senderThreadId);
    assert.equal(message?.text, "Run the audit");
  }).pipe(Effect.provide(harness.layer));
});

it.effect("provisions independent launches concurrently instead of behind a global semaphore", () =>
  Effect.gen(function* () {
    const setupCount = yield* Ref.make(0);
    const bothEntered = yield* Deferred.make<void>();
    const allowSetup = yield* Deferred.make<void>();
    const harness = makeHarness({
      runSetup: () =>
        Ref.updateAndGet(setupCount, (count) => count + 1).pipe(
          Effect.tap((count) =>
            count === 2 ? Deferred.succeed(bothEntered, undefined) : Effect.void,
          ),
          Effect.andThen(Deferred.await(allowSetup)),
          Effect.as({ status: "no-script" as const }),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const results = yield* Effect.all(
        [
          launches.launch(
            launchInput({
              command: "command:launch:concurrent-a",
              thread: "thread:launch:concurrent-a",
              message: "First",
              workspace: { type: "worktree", baseRef: "main" },
            }),
          ),
          launches.launch(
            launchInput({
              command: "command:launch:concurrent-b",
              thread: "thread:launch:concurrent-b",
              message: "Second",
              workspace: { type: "worktree", baseRef: "main" },
            }),
          ),
        ],
        { concurrency: "unbounded" },
      );
      assert.deepEqual(
        results.map((result) => result.projection.runs[0]?.status),
        ["preparing", "preparing"],
      );
      yield* Deferred.await(bothEntered);
      assert.equal(yield* Ref.get(setupCount), 2);
      yield* Deferred.succeed(allowSetup, undefined);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect.each([" /COMPACT "])(
  "uses the first conversation message for a title after %s",
  (nativeCommand) =>
    Effect.gen(function* () {
      const harness = makeHarness();
      yield* Effect.gen(function* () {
        const launches = yield* ThreadLaunch.ThreadLaunchService;
        const threads = yield* ThreadManagement.ThreadManagementService;
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        const launched = yield* launches.launch({
          ...launchInput({
            command: "compact-title",
            thread: "compact-title-thread",
            message: nativeCommand,
          }),
          generateTitle: true,
        });
        assert.isUndefined(
          (yield* threads.getThreadProjection(launched.threadId)).thread.titleRegeneration,
        );
        assert.isNull((yield* threads.getThreadProjection(launched.threadId)).thread.lastVisitedAt);
        assert.isFalse(
          (yield* outbox.listByCommandId(CommandId.make("compact-title:initial-message"))).some(
            (effect) => effect.request.type === "thread-title.generate",
          ),
        );
        const commandId = CommandId.make("compact-title-conversation");
        const messageId = MessageId.make("compact-title-conversation-message");
        yield* threads.dispatch({
          type: "message.dispatch",
          commandId,
          threadId: launched.threadId,
          messageId,
          createdBy: "user",
          creationSource: "web",
          text: "Fix the failing parser",
          attachments: [],
          dispatchMode: { type: "defer_start" },
        });
        assert.equal(
          (yield* threads.getThreadProjection(launched.threadId)).thread.titleRegeneration
            ?.requestId,
          commandId,
        );
        assert.deepEqual(
          (yield* outbox.listByCommandId(commandId))
            .filter((effect) => effect.request.type === "thread-title.generate")
            .map((effect) => effect.request),
          [{ type: "thread-title.generate", kind: { type: "initial", messageId } }],
        );
      }).pipe(Effect.provide(harness.layer));
    }),
);

it.effect("keeps native maintenance commands out of steering and restart messages", () =>
  Effect.gen(function* () {
    const harness = makeHarness();
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      for (const scenario of [
        {
          name: "compact-steer",
          first: "Fix the parser",
          next: " /COMPACT ",
          mode: "steer_active",
        },
        {
          name: "compact-restart",
          first: "Fix the parser",
          next: "/compact",
          mode: "restart_active",
        },
        {
          name: "logout-steer",
          first: "Fix the parser",
          next: "/logout",
          mode: "steer_active",
        },
        {
          name: "logout-restart",
          first: "Fix the parser",
          next: "/logout",
          mode: "restart_active",
        },
        {
          name: "steer-logout",
          first: "/logout",
          next: "Continue with the parser",
          mode: "steer_active",
        },
        {
          name: "steer-compaction",
          first: "/compact",
          next: "Continue with the parser",
          mode: "steer_active",
        },
      ] as const) {
        const launched = yield* launches.launch(
          launchInput({
            command: `${scenario.name}:launch`,
            thread: scenario.name,
            message: scenario.first,
          }),
        );
        const before = yield* threads.getThreadProjection(launched.threadId);
        const targetRun = before.runs[0];
        if (targetRun === undefined) return yield* Effect.die("Launch must create a run");
        const commandId = CommandId.make(`${scenario.name}:message`);
        const dispatch = threads.dispatch({
          type: "message.dispatch",
          commandId,
          threadId: launched.threadId,
          messageId: MessageId.make(`${scenario.name}:message`),
          createdBy: "user",
          creationSource: "web",
          text: scenario.next,
          attachments: [],
          dispatchMode: { type: scenario.mode, targetRunId: targetRun.id },
        });
        if (scenario.name.startsWith("steer-")) {
          yield* dispatch;
          const queued = (yield* threads.getThreadProjection(launched.threadId)).runs[1];
          assert.equal(queued?.status, "queued");
          assert.equal(queued?.userMessageId, MessageId.make(`${scenario.name}:message`));
          continue;
        }
        const failure = yield* dispatch.pipe(Effect.flip);
        assert.include(
          String(failure.cause).toLowerCase(),
          scenario.name.includes("logout") ? "sign" : "context compaction",
        );
        const after = yield* threads.getThreadProjection(launched.threadId);
        assert.deepEqual(after.messages, before.messages);
        assert.deepEqual(after.thread.titleRegeneration, before.thread.titleRegeneration);
        assert.deepEqual(yield* outbox.listByCommandId(commandId), []);
      }
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("arms durable title generation after accepting the first message", () =>
  Effect.gen(function* () {
    const harness = makeHarness({
      generateTitle: (input) =>
        Effect.succeed({
          title: input.previousTitle === undefined ? "Generated title" : "Regenerated title",
        }),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const titleRegeneration = yield* ThreadTitleRegeneration.ThreadTitleRegenerationService;
      const input = {
        ...launchInput({
          command: "command:launch:title-generation",
          thread: "thread:launch:title-generation",
          message: "Generate my title",
        }),
        title: "Generate my title",
        generateTitle: true,
      };
      const launched = yield* launches.launch(input);
      const generationCommandId = CommandId.make("command:launch:title-generation:initial-message");

      const projection = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(projection.thread.title, "Generate my title");
      assert.equal(projection.thread.titleRegeneration?.requestId, generationCommandId);
      assert.deepEqual(
        (yield* outbox.listByCommandId(generationCommandId)).map((effect) => effect.request),
        [
          {
            type: "thread-title.generate",
            kind: { type: "initial", messageId: MessageId.make("Generate my title:id") },
          },
        ],
      );
      yield* titleRegeneration.execute({
        threadId: launched.threadId,
        requestId: generationCommandId,
        kind: { type: "initial", messageId: MessageId.make("Generate my title:id") },
      });
      const generated = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(generated.thread.title, "Generated title");
      assert.deepEqual(
        harness.generateThreadTitle.mock.calls[0]?.[0]?.modelSelection,
        DEFAULT_SERVER_SETTINGS.textGenerationModelSelection,
      );

      const manualRequestId = CommandId.make("command:title-generation:manual");
      yield* threads.dispatch({
        type: "thread.metadata.update",
        commandId: manualRequestId,
        threadId: launched.threadId,
        regenerateTitle: true,
      });
      assert.deepEqual(
        (yield* outbox.listByCommandId(manualRequestId)).map((effect) => effect.request),
        [{ type: "thread-title.generate", kind: { type: "regenerate" } }],
      );
      yield* titleRegeneration.execute({
        threadId: launched.threadId,
        requestId: manualRequestId,
        kind: { type: "regenerate" },
      });
      const regenerated = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(regenerated.thread.title, "Regenerated title");
      assert.equal(
        harness.generateThreadTitle.mock.calls[1]?.[0]?.previousTitle,
        "Generated title",
      );

      yield* threads.dispatch({
        type: "thread.metadata.update",
        commandId: CommandId.make("command:title-generation:user-rename"),
        threadId: launched.threadId,
        title: "Keep my title",
      });
      const renamed = yield* threads.getThreadProjection(launched.threadId);
      yield* TestClock.adjust(Duration.seconds(1));
      yield* threads.dispatch({
        type: "thread.title.regeneration.complete",
        commandId: CommandId.make("command:title-generation:stale-completion"),
        threadId: launched.threadId,
        requestId: generationCommandId,
        title: "Stale generated title",
      });
      const afterStaleCompletion = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(afterStaleCompletion.thread.title, "Keep my title");
      assert.equal(
        DateTime.toEpochMillis(afterStaleCompletion.thread.updatedAt),
        DateTime.toEpochMillis(renamed.thread.updatedAt),
      );
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("does not update a reused thread title when the initial message is rejected", () =>
  Effect.gen(function* () {
    const harness = makeHarness();
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const threadId = ThreadId.make("thread:launch:reused-title-failure");
      yield* threads.dispatch({
        type: "thread.create",
        commandId: CommandId.make("command:launch:reused-title-failure:create"),
        threadId,
        projectId,
        title: "Original title",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      });

      const commandId = CommandId.make("command:launch:reused-title-failure");
      const failed = yield* launches
        .launch({
          ...launchInput({
            command: commandId,
            thread: threadId,
            message: "Generate a provisional title",
          }),
          reuseExistingThread: true,
          title: "Generate a provisional title",
          generateTitle: true,
          modelSelection: {
            instanceId: ProviderInstanceId.make("missing-provider"),
            model: "missing-model",
          },
        })
        .pipe(Effect.exit);

      assert.isTrue(Exit.isFailure(failed));
      const projection = yield* threads.getThreadProjection(threadId);
      assert.equal(projection.thread.title, "Original title");
      assert.isUndefined(projection.thread.titleRegeneration);
      assert.isEmpty(projection.messages);
      assert.isEmpty(yield* outbox.listByCommandId(CommandId.make(`${commandId}:initial-message`)));
    }).pipe(Effect.provide(harness.layer));
  }),
);

// Stands `projectId` in for the Scratch project: a plain folder, not a Git
// repository, so Git fails there with the error a real one raises.
function scratchHarness() {
  const claimed: Array<ThreadId> = [];
  const scratchRoot = project.workspaceRoot;
  const notARepository = (operation: string, cwd: string) =>
    new GitCommandError({
      operation,
      command: "vcs-route",
      cwd,
      detail: "Failed to resolve the VCS driver for this Git command.",
    });
  const harness = makeHarness({
    managedFolders: Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
      namedProjectsRoot: "/projects",
      isScratchProject: (id) => Effect.succeed(id === projectId),
      folderForThread: (input) =>
        Effect.sync(() => {
          if (input.projectId !== projectId) return Option.none();
          claimed.push(input.threadId);
          return Option.some(`/scratch/folder-${claimed.length}`);
        }),
    }),
    remoteExists: (input) =>
      input.cwd === scratchRoot
        ? Effect.fail(notARepository("GitWorkflowService.remoteExists", input.cwd))
        : Effect.succeed(true),
    createWorktree: (input) =>
      input.cwd === scratchRoot
        ? Effect.fail(notARepository("GitWorkflowService.createWorktree", input.cwd))
        : Effect.succeed({
            worktree: {
              path: "/other-repo-worktrees/feature",
              refName: input.newRefName,
              headSha: "abc",
            },
          } as never),
  });
  const layerScheduledTasks = ScheduledTasks.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        harness.layer,
        NodeCrypto.layer,
        Scheduler.layer,
        Layer.mock(SecretRequests.SecretRequests)({}),
      ),
    ),
  );
  return {
    ...harness,
    claimed,
    layer: Layer.mergeAll(harness.layer, layerScheduledTasks),
  };
}

const worktreeStrategy = { type: "worktree", baseRef: "main", startFromOrigin: true } as const;

/** Settles once preparation releases the run, or fails it. */
const preparationSettled = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const threads = yield* ThreadManagement.ThreadManagementService;
    yield* threads.streamStoredEventsFrom({ threadId, afterSequence: 0 }).pipe(
      Stream.filter(
        (stored) =>
          stored.event.type === "run.updated" &&
          (stored.commandId?.endsWith(":release") === true ||
            stored.event.payload.status === "failed"),
      ),
      Stream.runHead,
    );
    return yield* threads.getThreadProjection(threadId);
  });

function scheduledTaskInput(id: string, targetProjectId: ProjectId) {
  return {
    id: ScheduledTaskId.make(id),
    title: "Daily summary",
    prompt: "Summarize the day.",
    enabled: false,
    schedule: { type: "interval" as const, everyMs: 60_000 },
    projectId: targetProjectId,
    threadId: null,
    workspaceStrategy: worktreeStrategy,
    modelSelection,
    runtimeMode: "full-access" as const,
    interactionMode: "default" as const,
  };
}

it.effect("launches a worktree request for Scratch at the root, in its own folder", () => {
  const harness = scratchHarness();
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const launched = yield* launches.launch(
      launchInput({
        command: "command:launch:scratch-worktree",
        thread: "thread:launch:scratch-worktree",
        message: "Tidy my notes",
        workspace: worktreeStrategy,
      }),
    );
    const scratch = yield* preparationSettled(launched.threadId);
    assert.equal(scratch.thread.worktreePath, "/scratch/folder-1");
    // Released past preparation; provider execution is disabled here.
    assert.equal(scratch.runs[0]?.status, "starting");
    assert.deepEqual(scratch.runs[0]?.workspacePreparation, {
      type: "existing_worktree",
      worktreePath: "/scratch/folder-1",
    });
    assert.equal(harness.runSetup.mock.calls[0]?.[0]?.worktreePath, "/scratch/folder-1");
    assert.equal(harness.createWorktree.mock.calls.length, 0);

    // A Git project still gets the worktree it asked for.
    const other = yield* launches.launch({
      ...launchInput({
        command: "command:launch:git-worktree",
        thread: "thread:launch:git-worktree",
        message: "Build the feature",
        workspace: worktreeStrategy,
      }),
      projectId: otherProjectId,
    });
    const git = yield* preparationSettled(other.threadId);
    assert.equal(git.runs[0]?.status, "starting");
    assert.equal(git.runs[0]?.workspacePreparation?.type, "worktree");
    assert.equal(git.thread.worktreePath, "/other-repo-worktrees/feature");
    assert.equal(harness.createWorktree.mock.calls[0]?.[0]?.cwd, "/other-repo");
    assert.deepEqual(harness.claimed, [launched.threadId]);
  }).pipe(Effect.provide(harness.layer));
});

it.effect("stores a Scratch scheduled task at the root and keeps worktrees elsewhere", () => {
  const harness = scratchHarness();
  return Effect.gen(function* () {
    const tasks = yield* ScheduledTasks.ScheduledTaskService;
    const scratch = yield* tasks.upsert(
      scheduledTaskInput("scheduled-task:scratch-upsert", projectId),
    );
    assert.deepEqual(scratch.task.workspaceStrategy, { type: "root" });
    const git = yield* tasks.upsert(
      scheduledTaskInput("scheduled-task:git-upsert", otherProjectId),
    );
    assert.deepEqual(git.task.workspaceStrategy, worktreeStrategy);
  }).pipe(Effect.provide(harness.layer));
});

it.effect("runs a Scratch scheduled task saved with a worktree in its own folder", () => {
  const harness = scratchHarness();
  return Effect.gen(function* () {
    const tasks = yield* ScheduledTasks.ScheduledTaskService;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const sql = yield* SqlClient.SqlClient;
    const { task } = yield* tasks.upsert(
      scheduledTaskInput("scheduled-task:scratch-stored", projectId),
    );
    // Tasks saved before Scratch was coerced still hold a worktree strategy.
    yield* sql`
      UPDATE scheduled_tasks
      SET workspace_strategy_json = ${JSON.stringify(worktreeStrategy)}
      WHERE task_id = ${task.id}
    `;
    const ran = yield* tasks.runNow({ id: task.id });
    assert.equal(ran.task.lastRunStatus, "succeeded");
    const [thread] = yield* threads.listProjectThreads({ projectId, includeSubagents: false });
    assert.isDefined(thread);
    const projection = yield* preparationSettled(thread!.id);
    assert.equal(projection.runs[0]?.status, "starting");
    assert.equal(projection.thread.worktreePath, "/scratch/folder-1");
    assert.deepEqual(projection.runs[0]?.workspacePreparation, {
      type: "existing_worktree",
      worktreePath: "/scratch/folder-1",
    });
    assert.equal(harness.runSetup.mock.calls[0]?.[0]?.worktreePath, "/scratch/folder-1");
    assert.equal(harness.createWorktree.mock.calls.length, 0);
  }).pipe(Effect.provide(harness.layer));
});

it.effect.each(["stable", "upstream/release"])(
  "launches from %s with the fetched commit and remote PR base",
  (baseRef) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const git = yield* GitVcsDriver.GitVcsDriver;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "launch-remote-base-" });
      const cwd = `${directory}/repo`;
      const remote = `${directory}/remote.git`;
      const worktreePath = `${directory}/worktree`;
      yield* fs.makeDirectory(cwd);
      const command = (at: string, args: string[]) =>
        git.execute({ operation: "launch-test", cwd: at, args });
      yield* command(cwd, ["init", "-b", "stable"]);
      yield* command(cwd, ["config", "user.name", "Test"]);
      yield* command(cwd, ["config", "user.email", "test@example.test"]);
      yield* command(cwd, ["commit", "--allow-empty", "-m", "initial"]);
      const localHead = (yield* command(cwd, ["rev-parse", "HEAD"])).stdout.trim();
      yield* command(cwd, ["init", "--bare", remote]);
      yield* command(cwd, ["remote", "add", "upstream", remote]);
      yield* command(cwd, ["push", "-u", "upstream", "stable:release"]);
      yield* command(cwd, ["commit", "--allow-empty", "-m", "remote advance"]);
      yield* command(cwd, ["push", "upstream", "stable:release"]);
      const remoteHead = (yield* command(cwd, ["rev-parse", "HEAD"])).stdout.trim();
      yield* command(cwd, ["reset", "--hard", localHead]);
      yield* command(cwd, ["update-ref", "-d", "refs/remotes/upstream/release"]);

      const setupStarted = yield* Deferred.make<void>();
      const harness = makeHarness({
        hasCommit: ({ refName }) =>
          command(cwd, ["rev-parse", "--verify", `${refName}^{commit}`]).pipe(
            Effect.as(true),
            Effect.orElseSucceed(() => false),
          ),
        git: {
          resolveWorktreeBaseRemote: (input) => git.resolveWorktreeBaseRemote({ ...input, cwd }),
          hasRefNamed: () => Effect.succeed(false),
        },
        fetchWorktreeBaseCommit: (input) => git.fetchWorktreeBaseCommit({ ...input, cwd }),
        createWorktree: (input, options) =>
          git.createWorktree({ ...input, cwd, path: worktreePath }, options),
        runSetup: () =>
          Deferred.succeed(setupStarted, undefined).pipe(
            Effect.as({ status: "no-script" as const }),
          ),
      });
      yield* Effect.gen(function* () {
        const launches = yield* ThreadLaunch.ThreadLaunchService;
        const threads = yield* ThreadManagement.ThreadManagementService;
        const workspace = {
          type: "worktree" as const,
          baseRef,
          branch: "feature",
          startFromOrigin: true,
        };
        yield* launches.checkWorktreeBase({ projectId, workspaceStrategy: workspace });
        const launched = yield* launches.launch(
          launchInput({
            command: "command:launch:remote-base",
            thread: "thread:launch:remote-base",
            workspace,
          }),
        );
        yield* Deferred.await(setupStarted);
        const projection = yield* threads.getThreadProjection(launched.threadId);
        assert.equal(projection.thread.worktreePath, worktreePath);
        assert.equal(
          (yield* command(worktreePath, ["rev-parse", "HEAD"])).stdout.trim(),
          remoteHead,
        );
        const preview = yield* git.getReviewDiffPreview({ cwd: worktreePath });
        const comparisonBase = preview.sources.find(
          (source) => source.kind === "branch-range",
        )!.baseRef;
        assert.equal(
          (yield* command(worktreePath, ["rev-parse", `${comparisonBase}^{commit}`])).stdout.trim(),
          remoteHead,
        );
      }).pipe(Effect.provide(harness.layer));
    }).pipe(
      Effect.provide(
        GitVcsDriver.layer.pipe(
          Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "launch-remote-git-" })),
          Layer.provideMerge(NodeServices.layer),
        ),
      ),
    ),
);

it.effect("shows the fetch diagnosis when preparing a worktree from origin fails", () => {
  const detail =
    "Git could not authenticate with the remote. Check Git credentials or SSH access on the server, then retry.";
  const harness = makeHarness({
    fetchWorktreeBaseCommit: () =>
      Effect.fail(
        new GitCommandError({
          operation: "GitVcsDriver.fetchRemote",
          command: "git",
          cwd: project.workspaceRoot,
          detail,
          exitCode: 128,
        }),
      ),
  });
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const launched = yield* launches.launch(
      launchInput({
        command: "command:launch:fetch-failure",
        thread: "thread:launch:fetch-failure",
        message: "Start from origin",
        workspace: { type: "worktree", baseRef: "main", startFromOrigin: true },
      }),
    );
    yield* threads.streamStoredEventsFrom({ threadId: launched.threadId, afterSequence: 0 }).pipe(
      Stream.filter(
        (stored) => stored.event.type === "run.updated" && stored.event.payload.status === "failed",
      ),
      Stream.runHead,
    );
    const projection = yield* threads.getThreadProjection(launched.threadId);
    assert.equal(projection.messages[0]?.text, "Start from origin");
    assert.equal(projection.runs[0]?.status, "failed");
    assert.equal(projection.thread.worktreePath, null);
    assert.equal(
      projection.turnItems.find((item) => item.type === "command_execution")?.status,
      "failed",
    );
    assert.include(
      projection.turnItems.find((item) => item.type === "error")?.failure.message ?? "",
      detail,
    );
    assert.equal(harness.createWorktree.mock.calls.length, 0);
    assert.equal(harness.runSetup.mock.calls.length, 0);
  }).pipe(Effect.provide(harness.layer));
});

it.effect.each(["fails", "is interrupted"] as const)(
  "holds a message queued during setup when the workspace preparation %s",
  (ending) =>
    Effect.gen(function* () {
      const fetchEntered = yield* Deferred.make<void>();
      const allowFetch = yield* Deferred.make<void>();
      let fetchFailures = 1;
      const harness = makeHarness({
        fetchWorktreeBaseCommit: () =>
          fetchFailures-- <= 0
            ? Effect.succeed({ commitSha: "remote-main-sha", remoteRefName: "origin/main" })
            : Deferred.succeed(fetchEntered, undefined).pipe(
                Effect.andThen(Deferred.await(allowFetch)),
                Effect.andThen(
                  Effect.fail(
                    new GitCommandError({
                      operation: "GitVcsDriver.fetchRemote",
                      command: "git",
                      cwd: project.workspaceRoot,
                      detail: "Git could not reach the remote.",
                      exitCode: 128,
                    }),
                  ),
                ),
              ),
      });
      yield* Effect.gen(function* () {
        const launches = yield* ThreadLaunch.ThreadLaunchService;
        const threads = yield* ThreadManagement.ThreadManagementService;
        const launched = yield* launches.launch(
          launchInput({
            command: `command:launch:queued-setup-${ending}`,
            thread: `thread:launch:queued-setup-${ending}`,
            message: "First message",
            workspace: { type: "worktree", baseRef: "main", startFromOrigin: true },
          }),
        );
        yield* Deferred.await(fetchEntered);
        const queued = yield* threads.sendToThread({
          projectId,
          commandId: CommandId.make(`command:launch:queued-setup-${ending}:follow-up`),
          threadId: launched.threadId,
          messageId: MessageId.make(`message:launch:queued-setup-${ending}:follow-up`),
          text: "Sent during setup",
          attachments: [],
          mode: "queue",
          createdBy: "user",
          creationSource: "web",
        });
        assert.equal(queued.delivery, "queued");
        if (ending === "fails") {
          yield* Deferred.succeed(allowFetch, undefined);
        } else {
          yield* threads.dispatch({
            type: "run.interrupt",
            commandId: CommandId.make("command:launch:queued-setup-interrupt"),
            threadId: launched.threadId,
            runId: launched.projection.runs[0]!.id,
          });
        }

        yield* threads
          .streamStoredEventsFrom({ threadId: launched.threadId, afterSequence: 0 })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "run.updated" && stored.event.payload.queueHeld === true,
            ),
            Stream.runHead,
          );
        const projection = yield* threads.getThreadProjection(launched.threadId);
        assert.equal(projection.runs.at(-1)?.status, "queued");
        assert.equal(projection.thread.worktreePath, null);

        if (ending === "fails") {
          yield* launches.retryPreparation({
            commandId: CommandId.make("command:launch:queued-setup-retry"),
            threadId: launched.threadId,
            runId: launched.projection.runs[0]!.id,
          });
          const retried = yield* threads.getThreadProjection(launched.threadId);
          assert.equal(retried.runs.at(-1)?.queueHeld, false);
        }
      }).pipe(Effect.provide(harness.layer));
    }),
);

it.effect.each(["failed", "interrupted"] as const)(
  "holds a message queued during setup that arrives after the setup %s",
  (ending) => {
    let fetchEntered: Deferred.Deferred<void> | null = null;
    const harness = makeHarness({
      fetchWorktreeBaseCommit: () =>
        ending === "interrupted"
          ? Deferred.succeed(fetchEntered!, undefined).pipe(Effect.andThen(Effect.never))
          : Effect.fail(
              new GitCommandError({
                operation: "GitVcsDriver.fetchRemote",
                command: "git",
                cwd: project.workspaceRoot,
                detail: "Git could not reach the remote.",
                exitCode: 128,
              }),
            ),
    });
    return Effect.gen(function* () {
      fetchEntered = yield* Deferred.make<void>();
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const launched = yield* launches.launch(
        launchInput({
          command: `command:launch:late-queued-follow-up-${ending}`,
          thread: `thread:launch:late-queued-follow-up-${ending}`,
          message: "First message",
          workspace: { type: "worktree", baseRef: "main", startFromOrigin: true },
        }),
      );
      if (ending === "interrupted") {
        yield* Deferred.await(fetchEntered);
        yield* threads.dispatch({
          type: "run.interrupt",
          commandId: CommandId.make("command:launch:late-queued-follow-up:interrupt"),
          threadId: launched.threadId,
          runId: launched.projection.runs[0]!.id,
        });
      }
      yield* threads.streamStoredEventsFrom({ threadId: launched.threadId, afterSequence: 0 }).pipe(
        Stream.filter(
          (stored) => stored.event.type === "run.updated" && stored.event.payload.status === ending,
        ),
        Stream.runHead,
      );

      const queued = yield* threads.sendToThread({
        projectId,
        commandId: CommandId.make(`command:launch:late-queued-follow-up-${ending}:send`),
        threadId: launched.threadId,
        messageId: MessageId.make(`message:launch:late-queued-follow-up-${ending}:send`),
        text: "Sent during setup",
        attachments: [],
        mode: "queue",
        createdBy: "user",
        creationSource: "web",
      });
      assert.equal(queued.delivery, "queued");
      const projection = yield* threads.getThreadProjection(launched.threadId);
      const held = projection.runs.at(-1)!;
      assert.equal(held.status, "queued");
      assert.equal(held.queueHeld, true);

      // A message the user starts does not release it: the held message would
      // run after it in the project checkout.
      yield* threads.sendToThread({
        projectId,
        commandId: CommandId.make(`command:launch:late-queued-follow-up-${ending}:start`),
        threadId: launched.threadId,
        messageId: MessageId.make(`message:launch:late-queued-follow-up-${ending}:start`),
        text: "Sent after setup",
        attachments: [],
        mode: "auto",
        createdBy: "user",
        creationSource: "web",
      });
      yield* threads.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(`command:launch:late-queued-follow-up-${ending}:wake`),
        threadId: launched.threadId,
        messageId: MessageId.make(`message:launch:late-queued-follow-up-${ending}:wake`),
        text: "Background task completed.",
        attachments: [],
        dispatchMode: { type: "queue_after_active" },
        createdBy: "agent",
        creationSource: "provider",
      });
      const wake = (yield* threads.getThreadProjection(launched.threadId)).runs.at(-1)!;
      assert.equal(wake.status, "queued");
      assert.isTrue(wake.queueHeld);

      // Nor does the next one, though the run that message started is now the
      // latest and carries no workspace preparation.
      yield* threads.sendToThread({
        projectId,
        commandId: CommandId.make(`command:launch:late-queued-follow-up-${ending}:again`),
        threadId: launched.threadId,
        messageId: MessageId.make(`message:launch:late-queued-follow-up-${ending}:again`),
        text: "Sent after that",
        attachments: [],
        mode: "auto",
        createdBy: "user",
        creationSource: "web",
      });
      const after = yield* threads.getThreadProjection(launched.threadId);
      assert.isTrue(after.runs.find((run) => run.id === held.id)?.queueHeld);
    }).pipe(Effect.provide(harness.layer));
  },
);

it.effect("replays a server-allocated launch", () =>
  Effect.gen(function* () {
    const setupEntered = yield* Deferred.make<void>();
    const allowSetup = yield* Deferred.make<void>();
    const harness = makeHarness({
      runSetup: () =>
        Deferred.succeed(setupEntered, undefined).pipe(
          Effect.andThen(Deferred.await(allowSetup)),
          Effect.as({ status: "no-script" as const }),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const { threadId: _unusedThreadId, ...rest } = launchInput({
        command: "command:launch:allocated-retry",
        thread: "unused",
        message: "Only once",
        workspace: { type: "worktree", baseRef: "main" },
      });
      const first = yield* launches.launch(rest);
      yield* Deferred.await(setupEntered);
      const retry = yield* launches.launch(rest);
      assert.equal(first.threadId, retry.threadId);
      assert.isFalse(first.resumed);
      assert.isTrue(retry.resumed);
      // Clients drop their pending copy once the thread reaches the message's commit.
      assert.isNumber(first.sequence);
      assert.equal(retry.sequence, first.sequence);
      assert.equal(harness.runSetup.mock.calls.length, 1);
      assert.equal(retry.projection.messages.length, 1);
      assert.equal(retry.projection.runs.length, 1);
      assert.equal(retry.projection.messages[0]?.id, first.projection.messages[0]?.id);
      assert.equal(retry.projection.runs[0]?.id, first.projection.runs[0]?.id);
      yield* Deferred.succeed(allowSetup, undefined);
      yield* threads.streamStoredEventsFrom({ threadId: first.threadId, afterSequence: 0 }).pipe(
        Stream.filter(
          (stored) =>
            stored.commandId === CommandId.make(`${rest.commandId}:release`) &&
            stored.event.type === "run.updated",
        ),
        Stream.runHead,
      );
      const settled = yield* launches.launch(rest);
      assert.equal(settled.threadId, first.threadId);
      assert.isTrue(settled.resumed);
      assert.equal(settled.projection.messages[0]?.id, first.projection.messages[0]?.id);
      assert.equal(settled.projection.runs[0]?.id, first.projection.runs[0]?.id);
      assert.equal(harness.runSetup.mock.calls.length, 1);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("replays an accepted launch after the project disables the instance", () => {
  const harness = makeHarness();
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const settings = yield* ServerSettings.ServerSettingsService;
    const input = launchInput({
      command: "command:launch:retry-after-disable",
      thread: "thread:launch:retry-after-disable",
      message: "Hello",
    });
    const first = yield* launches.launch(input);

    yield* settings.updateSettings({
      projectSettingsOverrides: {
        [projectId]: { providerInstances: { [modelSelection.instanceId]: { enabled: false } } },
      },
    });

    const retry = yield* launches.launch(input);
    assert.equal(retry.threadId, first.threadId);
    assert.isTrue(retry.resumed);

    const fresh = yield* launches
      .launch(
        launchInput({
          command: "command:launch:fresh-after-disable",
          thread: "thread:launch:fresh-after-disable",
          message: "Hello",
        }),
      )
      .pipe(Effect.flip);
    if (fresh._tag !== "ThreadManagementProviderInstanceDisabledError") {
      assert.fail(`expected ThreadManagementProviderInstanceDisabledError, got ${fresh._tag}`);
    }
  }).pipe(Effect.provide(harness.layer));
});

it.effect("replays an accepted launch receipt after rebuilding the destination runtime", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const databaseLayer = Layer.succeedContext(database);
      const { threadId: _unusedThreadId, ...input } = launchInput({
        command: "command:launch:restart-receipt",
        thread: "unused",
        message: "Only once across restart",
      });
      const first = yield* Effect.scoped(
        Effect.gen(function* () {
          const launches = yield* ThreadLaunch.ThreadLaunchService;
          const threads = yield* ThreadManagement.ThreadManagementService;
          const launched = yield* launches.launch(input);
          yield* threads
            .streamStoredEventsFrom({ threadId: launched.threadId, afterSequence: 0 })
            .pipe(
              Stream.filter(
                (stored) =>
                  stored.commandId === CommandId.make(`${input.commandId}:release`) &&
                  stored.event.type === "run.updated",
              ),
              Stream.runHead,
            );
          return launched;
        }).pipe(Effect.provide(makeHarness({ databaseLayer }).layer)),
      );
      const after = makeHarness({ databaseLayer });
      yield* Effect.gen(function* () {
        const launches = yield* ThreadLaunch.ThreadLaunchService;
        const retry = yield* launches.launch(input);
        assert.isTrue(retry.resumed);
        assert.equal(retry.threadId, first.threadId);
        assert.equal(retry.projection.messages.length, 1);
        assert.equal(retry.projection.runs.length, 1);
        assert.equal(retry.projection.messages[0]?.id, first.projection.messages[0]?.id);
        assert.equal(retry.projection.runs[0]?.id, first.projection.runs[0]?.id);
        assert.equal(after.runSetup.mock.calls.length, 0);
      }).pipe(Effect.provide(after.layer));
    }),
  ),
);

it.effect(
  "accepted launch replay skips unavailable admission and enforces identity and mode limits",
  () => {
    const harness = makeHarness();
    return Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const input = {
        ...launchInput({
          command: "command:receipt-first",
          thread: "thread:receipt-first",
          message: "Once",
        }),
        createdBy: "agent" as const,
        linkOrigin: { sessionId: "link:original", label: "Original" },
      };
      const first = yield* launches.launch(input);
      const identity = { ...input, messageId: input.initialMessage!.messageId };
      const unavailable = new OrchestratorMcpFailure({
        code: "provider_unavailable",
        message: "destination catalog has no eligible models",
      });
      const replay = yield* launches.replayOrLaunch(identity, Effect.fail(unavailable));
      assert.equal(replay.threadId, first.threadId);
      assert.isTrue(replay.resumed);
      assert.equal(replay.projection.runs.length, 1);
      assert.deepEqual(replay.projection.runs[0]?.modelSelection, modelSelection);
      for (const changed of [
        { ...identity, threadId: ThreadId.make("thread:wrong") },
        { ...identity, projectId: otherProjectId },
        { ...identity, linkOrigin: { sessionId: "link:other", label: "Other" } },
        { ...identity, runtimeMode: "approval-required" as const },
        { ...identity, interactionMode: "plan" as const },
      ]) {
        const error = yield* launches
          .replayOrLaunch(changed, Effect.fail(unavailable))
          .pipe(Effect.flip);
        assert.equal(error._tag, "ThreadLaunchError");
      }
      const fresh = yield* launches
        .replayOrLaunch(
          { ...identity, commandId: CommandId.make("command:fresh-unavailable") },
          Effect.fail(unavailable),
        )
        .pipe(Effect.flip);
      assert.equal(fresh, unavailable);
    }).pipe(Effect.provide(harness.layer));
  },
);

it.effect("launch receipt replay refuses inheritance from a different-purpose parent run", () => {
  const harness = makeHarness();
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const parents = [];
    for (const number of [11, 12]) {
      const parent = yield* launches.launch({
        ...launchInput({
          command: `purpose-parent-${number}`,
          thread: `purpose-parent-${number}`,
          message: "Parent fixture",
        }),
        purposeIntent: {
          version: 1,
          intent: {
            type: "reference",
            reference: { host: "github.com", repository: "example/repo", kind: "issue", number },
          },
        },
      });
      parents.push({ threadId: parent.threadId, runId: parent.projection.runs[0]!.id });
    }
    const input = {
      ...launchInput({
        command: "purpose-child-replay",
        thread: "purpose-child-replay",
        message: "Child fixture",
      }),
      purposeIntent: { version: 1, intent: { type: "inherit" } } as const,
      purposeParentRun: parents[0]!,
    };
    const first = yield* launches.launch(input);
    const identity = { ...input, messageId: input.initialMessage!.messageId };
    const replay = yield* launches.replayOrLaunch(
      identity,
      Effect.die("A receipt must bypass new execution"),
    );
    assert.deepEqual(replay.projection.runs[0]?.purpose, first.projection.runs[0]?.purpose);
    const error = yield* launches
      .replayOrLaunch(
        { ...identity, purposeParentRun: parents[1]! },
        Effect.die("A changed source must be rejected before execution"),
      )
      .pipe(Effect.flip);
    assert.equal(error._tag, "ThreadLaunchError");
    assert.include(String(error.cause), "purpose intent");
  }).pipe(Effect.provide(harness.layer));
});

it.effect("a launch inheriting a deliberately unscoped parent run keeps that exact scope", () => {
  const harness = makeHarness();
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const parent = yield* launches.launch({
      ...launchInput({
        command: "purpose-unscoped-parent",
        thread: "purpose-unscoped-parent",
        message: "Parent fixture",
      }),
      purposeIntent: { version: 1, intent: { type: "unscoped" } },
    });
    const parentRun = { threadId: parent.threadId, runId: parent.projection.runs[0]!.id };
    const child = yield* launches.launch({
      ...launchInput({
        command: "purpose-unscoped-child",
        thread: "purpose-unscoped-child",
        message: "Child fixture",
      }),
      purposeIntent: { version: 1, intent: { type: "inherit" } },
      purposeParentRun: parentRun,
    });
    assert.deepEqual(child.projection.runs[0]?.purpose, {
      version: 1,
      scope: { type: "unscoped", reason: "deliberate" },
      provenance: { type: "parent_run", ...parentRun },
    });
  }).pipe(Effect.provide(harness.layer));
});

it.effect("an agent's launch never starts work above the modes it asks for", () => {
  const harness = makeHarness();
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const threads = yield* ThreadManagement.ThreadManagementService;
    // The first launch, at full access, creates the thread without a message.
    const created = yield* launches.launch(
      launchInput({ command: "command:launch:narrowed-retry", thread: "thread:narrowed" }),
    );
    // The same key comes back from a narrower agent, now carrying a message,
    // as a retry or a concurrent launch that lost the create would.
    const failed = yield* launches
      .launch({
        ...launchInput({
          command: "command:launch:narrowed-retry",
          thread: "thread:narrowed",
          message: "Run this",
        }),
        runtimeMode: "approval-required",
        interactionMode: "plan",
        createdBy: "agent",
        creationSource: "mcp",
      })
      .pipe(Effect.flip);
    if (failed._tag !== "ThreadLaunchError") {
      assert.fail(`expected ThreadLaunchError, got ${failed._tag}`);
    }
    assert.equal(failed.operation, "dispatch-message");
    const projection = yield* threads.getThreadProjection(created.threadId);
    assert.equal(projection.runs.length, 0);
    assert.equal(projection.messages.length, 0);
  }).pipe(Effect.provide(harness.layer));
});

it.effect("rejects a server-allocated launch replay with a mismatching thread id", () => {
  const harness = makeHarness();
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const { threadId: _unusedThreadId, ...rest } = launchInput({
      command: "command:launch:allocated-mismatch",
      thread: "unused",
      message: "Mismatch",
    });
    const first = yield* launches.launch(rest);
    const failed = yield* launches
      .launch({
        ...rest,
        threadId: ThreadId.make("thread:launch:allocated-mismatch"),
      })
      .pipe(Effect.flip);
    assert.notEqual(first.threadId, ThreadId.make("thread:launch:allocated-mismatch"));
    if (failed._tag !== "ThreadLaunchError") {
      assert.fail(`expected ThreadLaunchError, got ${failed._tag}`);
    }
    assert.equal(failed.operation, "create-thread");
    assert.include(String(failed.cause), "cannot be replayed");
  }).pipe(Effect.provide(harness.layer));
});

it.effect("does not treat an unrelated accepted command receipt as a launch", () => {
  const harness = makeHarness();
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const threadId = ThreadId.make("thread:launch:unrelated-receipt");
    yield* threads.dispatch({
      type: "thread.create",
      commandId: CommandId.make("command:launch:unrelated-receipt:create"),
      threadId,
      projectId,
      title: "Existing",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });
    yield* threads.dispatch({
      type: "thread.metadata.update",
      commandId: CommandId.make("command:launch:unrelated-receipt"),
      threadId,
      expectedEmpty: true,
    });
    const { threadId: _unusedThreadId, ...rest } = launchInput({
      command: "command:launch:unrelated-receipt",
      thread: "unused",
      message: "Should not become a launch",
    });
    const failed = yield* launches.launch(rest).pipe(Effect.flip);
    if (failed._tag !== "ThreadLaunchError") {
      assert.fail(`expected ThreadLaunchError, got ${failed._tag}`);
    }
    assert.equal(failed.operation, "create-thread");
    assert.include(String(failed.cause), "cannot be replayed");
    const projection = yield* threads.getThreadProjection(threadId);
    assert.equal(projection.messages.length, 0);
    assert.equal(projection.runs.length, 0);
  }).pipe(Effect.provide(harness.layer));
});

it.effect("bounds concurrent first launches to one thread per command", () =>
  Effect.gen(function* () {
    const setupEntered = yield* Deferred.make<void>();
    const allowSetup = yield* Deferred.make<void>();
    const harness = makeHarness({
      runSetup: () =>
        Deferred.succeed(setupEntered, undefined).pipe(
          Effect.andThen(Deferred.await(allowSetup)),
          Effect.as({ status: "no-script" as const }),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const { threadId: _unusedThreadId, ...rest } = launchInput({
        command: "command:launch:concurrent-allocated",
        thread: "unused",
        message: "Race me",
        workspace: { type: "worktree", baseRef: "main" },
      });
      const results = yield* Effect.all(
        [launches.launch(rest).pipe(Effect.exit), launches.launch(rest).pipe(Effect.exit)],
        { concurrency: "unbounded" },
      );
      const winner = results.find(Exit.isSuccess);
      assert.isDefined(winner);
      const threadId = winner!.value.threadId;
      for (const result of results) {
        if (Exit.isSuccess(result)) {
          assert.equal(result.value.threadId, threadId);
          continue;
        }
        const error = Cause.findErrorOption(result.cause).pipe(Option.getOrThrow);
        if (error._tag !== "ThreadLaunchError") {
          assert.fail(`expected ThreadLaunchError, got ${error._tag}`);
        }
        assert.equal(error.operation, "create-thread");
        assert.include(String(error.cause), "cannot be replayed");
        const retried = yield* launches.launch(rest);
        assert.equal(retried.threadId, threadId);
      }
      const projectThreads = yield* threads.listProjectThreads({
        projectId,
        includeSubagents: false,
      });
      assert.equal(projectThreads.length, 1);
      const projection = yield* threads.getThreadProjection(threadId);
      assert.equal(projection.messages.length, 1);
      assert.equal(projection.runs.length, 1);
      yield* Deferred.await(setupEntered);
      assert.equal(harness.runSetup.mock.calls.length, 1);
      yield* Deferred.succeed(allowSetup, undefined);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("does not let a failing same-command caller strand a concurrent durable launch", () =>
  Effect.gen(function* () {
    const setupEntered = yield* Deferred.make<void>();
    const allowSetup = yield* Deferred.make<void>();
    const harness = makeHarness({
      runSetup: () =>
        Deferred.succeed(setupEntered, undefined).pipe(
          Effect.andThen(Deferred.await(allowSetup)),
          Effect.as({ status: "no-script" as const }),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const command = "command:launch:failed-owner-race";
      const [failed, launched] = yield* Effect.all(
        [
          launches
            .launch({
              ...launchInput({
                command,
                thread: "thread:launch:failed-owner-race",
                message: "This invalid reuse fails",
                workspace: { type: "worktree", baseRef: "main" },
              }),
              reuseExistingThread: true,
            })
            .pipe(Effect.exit),
          launches.launch(
            launchInput({
              command,
              thread: "thread:launch:successful-peer",
              message: "This peer persists",
              workspace: { type: "worktree", baseRef: "main" },
            }),
          ),
        ],
        { concurrency: "unbounded" },
      );
      assert.isTrue(Exit.isFailure(failed));
      assert.equal(launched.projection.runs[0]?.status, "preparing");
      const entered = yield* Deferred.await(setupEntered).pipe(
        Effect.timeoutOption(Duration.seconds(2)),
      );
      assert.isTrue(Option.isSome(entered));
      assert.equal(harness.runSetup.mock.calls.length, 1);
      yield* Deferred.succeed(allowSetup, undefined);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("reports one concurrent same-command launch as new and the rest as resumed", () =>
  Effect.gen(function* () {
    // Both launches read no receipt before either one claims the thread.
    const arrived = yield* Ref.make(0);
    const bothRead = yield* Deferred.make<void>();
    const harness = makeHarness({
      managedFolders: Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
        namedProjectsRoot: "/projects",
        isScratchProject: () => Effect.succeed(false),
        folderForThread: () =>
          Ref.updateAndGet(arrived, (count) => count + 1).pipe(
            Effect.tap((count) =>
              count === 2 ? Deferred.succeed(bothRead, undefined) : Effect.void,
            ),
            Effect.andThen(Deferred.await(bothRead)),
            Effect.as(Option.none()),
          ),
      }),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const input = launchInput({
        command: "command:launch:concurrent-resumed",
        thread: "thread:launch:concurrent-resumed",
      });
      const results = yield* Effect.all([launches.launch(input), launches.launch(input)], {
        concurrency: "unbounded",
      });
      assert.deepEqual(results.map((result) => result.resumed).sort(), [false, true]);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect(
  "does not report a retry as resumed when the earlier attempt only claimed the thread",
  () =>
    Effect.gen(function* () {
      const harness = makeHarness();
      yield* Effect.gen(function* () {
        const launches = yield* ThreadLaunch.ThreadLaunchService;
        const threads = yield* ThreadManagement.ThreadManagementService;
        const input = launchInput({
          command: "command:launch:claimed-only",
          thread: "thread:launch:claimed-only",
          message: "Counted once",
        });
        yield* threads.dispatch({
          type: "thread.create",
          commandId: input.commandId,
          threadId: input.threadId,
          projectId: input.projectId,
          title: input.title,
          modelSelection: input.modelSelection,
          runtimeMode: input.runtimeMode,
          interactionMode: input.interactionMode,
          branch: null,
          worktreePath: null,
          createdBy: input.createdBy,
          creationSource: input.creationSource,
        });
        const retried = yield* launches.launch(input);
        assert.isFalse(retried.resumed);
        assert.isTrue((yield* launches.launch(input)).resumed);
      }).pipe(Effect.provide(harness.layer));
    }),
);

it.effect("schedules an accepted preparing message exactly once across concurrent retries", () =>
  Effect.gen(function* () {
    const setupEntered = yield* Deferred.make<void>();
    const allowSetup = yield* Deferred.make<void>();
    const harness = makeHarness({
      runSetup: () =>
        Deferred.succeed(setupEntered, undefined).pipe(
          Effect.andThen(Deferred.await(allowSetup)),
          Effect.as({ status: "no-script" as const }),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const input = launchInput({
        command: "command:launch:accepted-before-fork",
        thread: "thread:launch:accepted-before-fork",
        message: "Resume preparation",
        workspace: { type: "worktree", baseRef: "main" },
      });
      const messageId = MessageId.make("message:launch:accepted-before-fork");

      yield* threads.dispatch({
        type: "thread.create",
        commandId: input.commandId,
        threadId: input.threadId,
        projectId: input.projectId,
        title: input.title,
        modelSelection: input.modelSelection,
        runtimeMode: input.runtimeMode,
        interactionMode: input.interactionMode,
        branch: null,
        worktreePath: null,
        createdBy: input.createdBy,
        creationSource: input.creationSource,
      });
      yield* threads.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(`${input.commandId}:initial-message`),
        threadId: input.threadId,
        messageId,
        text: "Resume preparation",
        attachments: [],
        modelSelection: input.modelSelection,
        dispatchMode: { type: "defer_start" },
        createdBy: input.createdBy,
        creationSource: input.creationSource,
      });
      const preparing = yield* threads.getThreadProjection(input.threadId);
      assert.equal(preparing.runs[0]?.status, "preparing");

      const [first, second] = yield* Effect.all([launches.launch(input), launches.launch(input)], {
        concurrency: "unbounded",
      });
      yield* Deferred.await(setupEntered);
      assert.isTrue(first.resumed);
      assert.isTrue(second.resumed);
      assert.equal(harness.runSetup.mock.calls.length, 1);
      yield* Deferred.succeed(allowSetup, undefined);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("creates a strong provider-thread mapping for an imported native session", () => {
  const harness = makeHarness();
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const input = {
      ...launchInput({
        command: "command:launch:imported-native-session",
        thread: "thread:launch:imported-native-session",
      }),
      importedNativeThread: {
        ref: {
          driver: ProviderDriverKind.make("codex"),
          nativeId: "native-session-42",
          strength: "strong" as const,
        },
        metadata: {
          title: "Native session",
          updatedAt: "2026-08-23T00:00:00Z",
        },
      },
    };

    const launched = yield* launches.launch(input);

    assert.deepInclude(launched.projection.providerThreads[0], {
      id: IdAllocator.deriveProviderThread({
        driver: input.importedNativeThread.ref.driver,
        providerInstanceId: modelSelection.instanceId,
        nativeThreadId: input.importedNativeThread.ref.nativeId,
      }),
      driver: input.importedNativeThread.ref.driver,
      providerInstanceId: modelSelection.instanceId,
      appThreadId: input.threadId,
      nativeThreadRef: input.importedNativeThread.ref,
      status: "not_loaded",
      nativeMetadata: input.importedNativeThread.metadata,
    });
    assert.equal(
      launched.projection.thread.activeProviderThreadId,
      launched.projection.providerThreads[0]?.id,
    );
  }).pipe(Effect.provide(harness.layer));
});

it.effect("shared intake preserves durable attachment bytes after a lost launch result", () => {
  const harness = makeHarness();
  const layerFiles = ServerConfig.layerTest(process.cwd(), { prefix: "t3-message-intake-" }).pipe(
    Layer.provideMerge(DocumentText.layer.pipe(Layer.provideMerge(NodeServices.layer))),
    Layer.merge(Layer.mock(GitVcsDriver.GitVcsDriver)({})),
  );
  return Effect.gen(function* () {
    const config = yield* ServerConfig.ServerConfig;
    const fs = yield* FileSystem.FileSystem;
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const pendingId = createPendingAttachmentId();
    assert.isNotNull(pendingId);
    const attachment: ChatAttachment = {
      type: "image",
      id: ChatAttachmentId.make(pendingId),
      name: "image.png",
      mimeType: "image/png",
      sizeBytes: 4,
    };
    const pendingPath = resolveAttachmentPath({
      attachmentsDir: config.attachmentsDir,
      attachment,
    });
    assert.isNotNull(pendingPath);
    yield* fs.makeDirectory(config.attachmentsDir, { recursive: true });
    yield* fs.writeFile(pendingPath, new Uint8Array([1, 2, 3, 4]));
    const input = {
      ...launchInput({ command: "intake-launch", thread: "intake-thread" }),
      initialMessage: {
        messageId: MessageId.make("intake-first"),
        text: "First [file](t3-context://v1/file/intake-file)",
        context: {
          version: 1 as const,
          records: [
            {
              version: 1 as const,
              contextId: ComposerContextId.make("intake-file"),
              kind: "file" as const,
              label: attachment.name,
              attachmentId: attachment.id,
              name: attachment.name,
              mimeType: attachment.mimeType,
              sizeBytes: attachment.sizeBytes,
            },
          ],
        },
        attachments: [attachment],
      },
    };
    const failed = yield* ThreadMessageIntake.launchThread(input).pipe(
      Effect.provideService(ThreadLaunch.ThreadLaunchService, {
        launch: (request) =>
          launches.launch(request).pipe(
            Effect.andThen(
              new ThreadLaunch.ThreadLaunchError({
                operation: "create-thread",
                commandId: request.commandId,
                projectId,
                cause: "lost result after acceptance",
              }),
            ),
          ),
        configureMcp: launches.configureMcp,
        checkWorktreeBase: launches.checkWorktreeBase,
        replayOrLaunch: launches.replayOrLaunch,
        retryPreparation: launches.retryPreparation,
        prepareRun: launches.prepareRun,
      }),
      Effect.flip,
    );
    assert.equal(failed._tag, "ThreadLaunchError");
    const accepted = yield* threads.getThreadProjection(input.threadId);
    const stored = accepted.messages.find(
      (message) => message.id === input.initialMessage.messageId,
    );
    assert.isDefined(stored);
    assert.notEqual(stored.attachments[0]?.id, attachment.id);
    const record = stored.context?.records[0];
    assert.isDefined(record);
    assert.equal((record as { attachmentId: string }).attachmentId, stored.attachments[0]?.id);
    const userItem = accepted.turnItems.find(
      (item) => item.type === "user_message" && item.messageId === stored.id,
    );
    assert.ok(userItem?.type === "user_message");
    assert.deepEqual(userItem.context, stored.context);
    const storedPath = resolveAttachmentPath({
      attachmentsDir: config.attachmentsDir,
      attachment: stored.attachments[0]!,
    });
    assert.isNotNull(storedPath);
    assert.deepEqual(yield* fs.readFile(storedPath), new Uint8Array([1, 2, 3, 4]));
    assert.deepEqual(yield* fs.readFile(pendingPath), new Uint8Array([1, 2, 3, 4]));

    const replayed = yield* ThreadMessageIntake.launchThread(input);
    assert.equal(replayed.projection.messages[0]?.id, stored.id);
    assert.deepEqual(replayed.projection.messages[0]?.attachments, stored.attachments);
    const claimedFiles = Effect.map(fs.readDirectory(config.attachmentsDir), (files) =>
      files.filter((name) => !name.startsWith("pending-")),
    );
    assert.equal((yield* claimedFiles).length, 1);

    const duplicate = yield* ThreadMessageIntake.launchThread({
      ...input,
      commandId: CommandId.make("intake-duplicate-create"),
      initialMessage: { ...input.initialMessage, messageId: MessageId.make("intake-duplicate") },
    }).pipe(Effect.result);
    assert.ok(Result.isFailure(duplicate));
    assert.ok(duplicate.failure._tag === "ThreadLaunchError");
    assert.equal(duplicate.failure.operation, "create-thread");
    assert.propertyVal(duplicate.failure.cause, "_tag", "OrchestratorThreadAlreadyExistsError");
    assert.equal((yield* claimedFiles).length, 1);
    assert.deepEqual(yield* fs.readFile(storedPath), new Uint8Array([1, 2, 3, 4]));
    assert.deepEqual(yield* fs.readFile(pendingPath), new Uint8Array([1, 2, 3, 4]));

    const missingProject = yield* ThreadMessageIntake.launchThread({
      ...input,
      commandId: CommandId.make("intake-no-project"),
      projectId: ProjectId.make("missing-project"),
    }).pipe(Effect.flip);
    assert.equal(missingProject._tag, "ThreadLaunchError");
    assert.equal((yield* claimedFiles).length, 1);
    const missingThread = yield* ThreadMessageIntake.dispatchCommand({
      type: "message.dispatch",
      commandId: CommandId.make("intake-no-thread"),
      threadId: ThreadId.make("missing-thread"),
      messageId: MessageId.make("intake-missing"),
      text: "Missing",
      attachments: [attachment],
      dispatchMode: { type: "start_immediately" },
      createdBy: "user",
      creationSource: "web",
    }).pipe(Effect.flip);
    assert.equal(missingThread._tag, "OrchestratorProjectionError");
    assert.equal((yield* claimedFiles).length, 1);

    const dispatch = ThreadMessageIntake.dispatchCommand({
      type: "message.dispatch",
      commandId: CommandId.make("intake-dispatch"),
      threadId: input.threadId,
      messageId: MessageId.make("intake-second"),
      text: "Second",
      attachments: [attachment],
      dispatchMode: { type: "queue_after_active" },
      createdBy: "user",
      creationSource: "web",
    });
    yield* dispatch;
    yield* dispatch;
    const queuedProjection = yield* threads.getThreadProjection(input.threadId);
    const queuedRun = queuedProjection.runs.find(
      (run) => run.userMessageId === MessageId.make("intake-second"),
    );
    assert.isDefined(queuedRun);
    assert.equal(queuedRun.status, "queued");
    const queuedMessage = queuedProjection.messages.find(
      (message) => message.id === queuedRun.userMessageId,
    );
    assert.isDefined(queuedMessage);
    const file: ChatAttachment = {
      type: "file",
      id: ChatAttachmentId.make(createPendingAttachmentId("pdf")),
      name: "queued.pdf",
      mimeType: "application/pdf",
      sizeBytes: 4,
    };
    const filePath = resolveAttachmentPath({
      attachmentsDir: config.attachmentsDir,
      attachment: file,
    });
    assert.isNotNull(filePath);
    yield* fs.writeFile(filePath, new Uint8Array([5, 6, 7, 8]));
    const edit = ThreadMessageIntake.dispatchCommand({
      type: "queued-run.edit",
      commandId: CommandId.make("intake-edit"),
      threadId: input.threadId,
      runId: queuedRun.id,
      text: "Edited with a file",
      attachments: [...queuedMessage.attachments, file],
    });
    yield* edit;
    yield* edit;
    const editedProjection = yield* threads.getThreadProjection(input.threadId);
    const editedMessage = editedProjection.messages.find(
      (message) => message.id === queuedRun.userMessageId,
    );
    assert.isDefined(editedMessage);
    assert.equal(editedMessage.attachments.length, 2);
    assert.deepEqual(editedMessage.attachments[0], queuedMessage.attachments[0]);
    assert.notEqual(editedMessage.attachments[1]?.id, file.id);
    const durableFilePath = resolveAttachmentPath({
      attachmentsDir: config.attachmentsDir,
      attachment: editedMessage.attachments[1]!,
    });
    assert.isNotNull(durableFilePath);
    assert.deepEqual(yield* fs.readFile(durableFilePath), new Uint8Array([5, 6, 7, 8]));
    assert.deepEqual(yield* fs.readFile(filePath), new Uint8Array([5, 6, 7, 8]));
    const beforeRejectedEdit = (yield* claimedFiles).length;
    const rejectedEdit = yield* ThreadMessageIntake.dispatchCommand({
      type: "queued-run.edit",
      commandId: CommandId.make("intake-edit-rejected"),
      threadId: input.threadId,
      runId: queuedRun.id,
      text: "",
      attachments: [file],
    }).pipe(Effect.flip);
    assert.equal(rejectedEdit._tag, "OrchestratorCommandRejectedError");
    assert.equal((yield* claimedFiles).length, beforeRejectedEdit);
    assert.deepEqual(yield* fs.readFile(filePath), new Uint8Array([5, 6, 7, 8]));
    const send = ThreadMessageIntake.sendToThread({
      commandId: CommandId.make("intake-send"),
      projectId,
      threadId: input.threadId,
      messageId: MessageId.make("intake-third"),
      text: "Third",
      attachments: [attachment],
      mode: "queue",
      createdBy: "agent",
      creationSource: "mcp",
    });
    yield* send;
    yield* send;
    assert.equal((yield* claimedFiles).length, 4);
    const final = yield* threads.getThreadProjection(input.threadId);
    assert.equal(final.messages.length, 3);
    for (const message of final.messages) {
      const path = resolveAttachmentPath({
        attachmentsDir: config.attachmentsDir,
        attachment: message.attachments[0]!,
      });
      assert.isNotNull(path);
      assert.deepEqual(yield* fs.readFile(path), new Uint8Array([1, 2, 3, 4]));
    }
  }).pipe(Effect.provide(Layer.mergeAll(harness.layer, layerFiles)));
});

it.effect("cancels tracked setup before provider work is released", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>();
    const harness = makeHarness({
      runSetup: () => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const tracker = yield* WorktreeSetupTracker.WorktreeSetupTracker;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const input = launchInput({
        command: "launch:cancel-tracked",
        thread: "thread:cancel-tracked",
        message: "Start",
        workspace: { type: "worktree", baseRef: "main" },
      });
      const launched = yield* launches.launch(input);
      yield* Deferred.await(entered);
      assert.equal((yield* tracker.get(launched.threadId))?.phase, "running");
      assert.isTrue(yield* tracker.cancel(launched.threadId));
      assert.equal((yield* tracker.get(launched.threadId))?.phase, "cancelled");
      const projection = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(projection.runs[0]?.status, "failed");
      assert.isNull(projection.thread.worktreePath);
      assert.isEmpty(yield* outbox.listByCommandId(CommandId.make(`${input.commandId}:release`)));
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("stops an in-flight branch rename when tracked setup is cancelled", () =>
  Effect.gen(function* () {
    const setupEntered = yield* Deferred.make<void>();
    const branchNameStarted = yield* Deferred.make<void>();
    const allowBranchName = yield* Deferred.make<void>();
    const branchNameInterrupted = yield* Deferred.make<void>();
    const renameAttempted = yield* Deferred.make<void>();
    const harness = makeHarness({
      renameBranch: (input) =>
        Deferred.succeed(renameAttempted, undefined).pipe(Effect.as({ branch: input.newBranch })),
      runSetup: () => Deferred.succeed(setupEntered, undefined).pipe(Effect.andThen(Effect.never)),
      generateBranchName: () =>
        Deferred.succeed(branchNameStarted, undefined).pipe(
          Effect.andThen(Deferred.await(allowBranchName)),
          Effect.as({ branch: "generated-branch" }),
          Effect.onInterrupt(() => Deferred.succeed(branchNameInterrupted, undefined)),
        ),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const tracker = yield* WorktreeSetupTracker.WorktreeSetupTracker;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const launched = yield* launches.launch(
        launchInput({
          command: "launch:cancel-renaming",
          thread: "thread:cancel-renaming",
          message: "Start",
          workspace: { type: "worktree", baseRef: "main" },
        }),
      );
      yield* Deferred.await(setupEntered);
      yield* Deferred.await(branchNameStarted);
      assert.isTrue(yield* tracker.cancel(launched.threadId));
      yield* Deferred.succeed(allowBranchName, undefined);
      yield* Effect.raceFirst(
        Deferred.await(branchNameInterrupted),
        Deferred.await(renameAttempted),
      );
      assert.isTrue(
        yield* Deferred.isDone(branchNameInterrupted),
        "the cancelled setup's branch rename kept running",
      );
      assert.equal(harness.renameBranch.mock.calls.length, 0);
      assert.isNull((yield* threads.getThreadProjection(launched.threadId)).thread.worktreePath);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("drains a delayed branch rename before cancelled setup clears its workspace", () =>
  Effect.gen(function* () {
    const setupEntered = yield* Deferred.make<void>();
    const renameEntered = yield* Deferred.make<void>();
    const renameResume = yield* Deferred.make<void>();
    const renameInterrupted = yield* Ref.make(false);
    const harness = makeHarness({
      renameBranch: () =>
        Deferred.succeed(renameEntered, undefined).pipe(
          Effect.andThen(Deferred.await(renameResume)),
          Effect.as({ branch: "late-generated-branch" }),
          Effect.onInterrupt(() => Ref.set(renameInterrupted, true)),
        ),
      runSetup: () => Deferred.succeed(setupEntered, undefined).pipe(Effect.andThen(Effect.never)),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const tracker = yield* WorktreeSetupTracker.WorktreeSetupTracker;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const input = launchInput({
        command: "launch:delayed-cancel-rename",
        thread: "thread:delayed-cancel-rename",
        message: "Start",
        workspace: { type: "worktree", baseRef: "main" },
      });
      const launched = yield* launches.launch(input);
      yield* Deferred.await(setupEntered);
      yield* Deferred.await(renameEntered);
      assert.isTrue(yield* tracker.cancel(launched.threadId));
      const interrupted = yield* Ref.get(renameInterrupted);
      yield* Deferred.succeed(renameResume, undefined);
      // On the broken source, observe the delayed write after cleanup rather than relying on timing.
      if (!interrupted)
        yield* threads
          .streamStoredEventsFrom({ threadId: launched.threadId, afterSequence: 0 })
          .pipe(
            Stream.filter(
              (stored) => stored.commandId === CommandId.make(`${input.commandId}:branch-rename`),
            ),
            Stream.runHead,
          );
      const projection = yield* threads.getThreadProjection(launched.threadId);
      assert.isNull(projection.thread.worktreePath);
      assert.isNull(projection.thread.branch);
      assert.isTrue(interrupted);
      assert.equal(projection.runs[0]?.status, "failed");
      assert.equal(harness.removeWorktree.mock.calls.length, 1);
      assert.isEmpty(yield* outbox.listByCommandId(CommandId.make(`${input.commandId}:release`)));
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect.each([0, 1])("releases an async setup before its completion with exit %s", (exitCode) =>
  Effect.gen(function* () {
    const completion = yield* Deferred.make<{ exitCode: number | null; durationMs: number }>();
    const harness = makeHarness({
      runSetup: () =>
        Effect.succeed({
          status: "started" as const,
          async: true,
          scriptId: "setup",
          scriptName: "Setup",
          scriptCommand: "vp install",
          terminalId: "setup",
          cwd: "/repo-worktrees/feature",
          completion: Deferred.await(completion),
        }),
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const threads = yield* ThreadManagement.ThreadManagementService;
      const tracker = yield* WorktreeSetupTracker.WorktreeSetupTracker;
      const launched = yield* launches.launch(
        launchInput({
          command: `command:launch:async-${exitCode}`,
          thread: `thread:launch:async-${exitCode}`,
          message: "Start during setup",
          workspace: { type: "worktree", baseRef: "main" },
        }),
      );
      yield* tracker.stream(launched.threadId).pipe(
        Stream.filter(
          (snapshot) =>
            snapshot?.stages.some((stage) => stage.id === "agent" && stage.status === "done") ===
            true,
        ),
        Stream.runHead,
      );
      const running = yield* threads.getThreadProjection(launched.threadId);
      assert.equal(running.runs[0]?.status, "starting");
      assert.equal((yield* tracker.get(launched.threadId))?.phase, "running");
      yield* Deferred.succeed(completion, { exitCode, durationMs: 1 });
      yield* tracker.stream(launched.threadId).pipe(
        Stream.filter((snapshot) => snapshot?.phase === "done"),
        Stream.runHead,
      );
      const settled = yield* tracker.get(launched.threadId);
      assert.equal(
        settled?.stages.find((stage) => stage.id === "setup-script")?.status,
        exitCode === 0 ? "done" : "failed",
      );
      assert.equal(
        (yield* threads.getThreadProjection(launched.threadId)).runs[0]?.status,
        "starting",
      );
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect.each([
  { name: "is missing", baseRef: "t3/renamed", startFromOrigin: false, origin: true },
  {
    name: "has no origin to come from",
    baseRef: "t3/renamed",
    startFromOrigin: true,
    origin: false,
  },
  {
    name: "is a previous checkout there never was",
    baseRef: "-",
    startFromOrigin: false,
    origin: true,
  },
])("refuses a new worktree whose base ref $name", (testCase) =>
  Effect.gen(function* () {
    const harness = makeHarness({
      hasCommit: ({ refName }) => Effect.succeed(refName === "HEAD"),
      git: {
        resolveWorktreeBaseRemote: () =>
          Effect.succeed(testCase.origin ? { remoteName: "origin", branchName: "main" } : null),
        hasRefNamed: () => Effect.succeed(false),
      },
    });
    yield* Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const error = yield* launches
        .checkWorktreeBase({
          projectId,
          workspaceStrategy: {
            type: "worktree",
            baseRef: testCase.baseRef,
            startFromOrigin: testCase.startFromOrigin,
          },
        })
        .pipe(Effect.flip);
      assert.deepInclude(error, {
        _tag: "ThreadLaunchBaseRefError",
        projectId,
        baseRef: testCase.baseRef,
      });
      assert.equal(
        error.message,
        `Base ref "${testCase.baseRef}" does not resolve to a commit in project "Project" (/repo).`,
      );
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("replays an accepted send after the project disables the instance", () => {
  const harness = makeHarness();
  const layerFiles = ServerConfig.layerTest(process.cwd(), { prefix: "t3-send-retry-" }).pipe(
    Layer.provideMerge(DocumentText.layer.pipe(Layer.provideMerge(NodeServices.layer))),
  );
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const settings = yield* ServerSettings.ServerSettingsService;
    const created = yield* launches.launch(
      launchInput({
        command: "command:send:retry-setup",
        thread: "thread:send:retry-after-disable",
      }),
    );

    const sendInput = {
      commandId: CommandId.make("command:send:retry-after-disable"),
      projectId,
      threadId: created.threadId,
      messageId: MessageId.make("message:send:retry-after-disable"),
      text: "Hello again",
      attachments: [],
      mode: "auto" as const,
      createdBy: "user" as const,
      creationSource: "web" as const,
    };
    const first = yield* ThreadMessageIntake.sendToThread(sendInput);

    yield* settings.updateSettings({
      projectSettingsOverrides: {
        [projectId]: { providerInstances: { [modelSelection.instanceId]: { enabled: false } } },
      },
    });

    const retry = yield* ThreadMessageIntake.sendToThread(sendInput);
    assert.equal(retry.message.id, first.message.id);

    const fresh = yield* ThreadMessageIntake.sendToThread({
      ...sendInput,
      commandId: CommandId.make("command:send:fresh-after-disable"),
      messageId: MessageId.make("message:send:fresh-after-disable"),
    }).pipe(Effect.flip);
    if (fresh._tag !== "ThreadManagementProviderInstanceDisabledError") {
      assert.fail(`expected ThreadManagementProviderInstanceDisabledError, got ${fresh._tag}`);
    }
  }).pipe(Effect.provide(Layer.mergeAll(harness.layer, layerFiles)));
});


it.effect(
  "prepares an accepted delegated destination once through the existing workspace pipeline",
  () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const harness = makeHarness({
        runSetup: () =>
          Deferred.succeed(entered, undefined).pipe(
            Effect.andThen(Deferred.await(release)),
            Effect.as({ status: "no-script" as const }),
          ),
      });
      yield* Effect.gen(function* () {
        const launches = yield* ThreadLaunch.ThreadLaunchService;
        const threads = yield* ThreadManagement.ThreadManagementService;
        const threadId = ThreadId.make("delegated-destination");
        yield* threads.dispatch({
          type: "thread.create",
          commandId: CommandId.make("delegated-create"),
          threadId,
          projectId: otherProjectId,
          title: "Delegated destination",
          modelSelection,
          runtimeMode: "approval-required",
          interactionMode: "plan",
          branch: null,
          worktreePath: null,
          createdBy: "agent",
          creationSource: "mcp",
        });
        yield* threads.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("delegated-message"),
          threadId,
          messageId: MessageId.make("delegated-message"),
          text: "Destination task",
          attachments: [],
          modelSelection,
          dispatchMode: {
            type: "defer_start",
            workspaceStrategy: { type: "worktree", baseRef: "main" },
          },
          createdBy: "agent",
          creationSource: "mcp",
        });
        const before = yield* threads.getThreadProjection(threadId);
        const run = before.runs[0]!;
        const released = yield* threads.streamStoredEventsFrom({ afterSequence: 0 }).pipe(
          Stream.filter(
            (stored) =>
              stored.event.threadId === threadId &&
              stored.event.type === "run.updated" &&
              stored.event.payload.id === run.id &&
              stored.event.payload.status === "starting",
          ),
          Stream.runHead,
          Effect.forkChild,
        );
        const input = {
          commandId: CommandId.make("delegated-prepare"),
          threadId,
          runId: run.id,
          projectId: otherProjectId,
        };
        yield* Effect.all([launches.prepareRun(input), launches.prepareRun(input)], {
          concurrency: "unbounded",
        });
        yield* Deferred.await(entered);
        assert.equal(harness.createWorktree.mock.calls.length, 1);
        assert.equal(harness.runSetup.mock.calls.length, 1);
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(released);
        const after = yield* threads.getThreadProjection(threadId);
        assert.equal(after.runs[0]!.status, "starting");
        assert.equal(after.thread.projectId, otherProjectId);
        assert.equal(after.thread.worktreePath, "/repo-worktrees/feature");
        assert.equal(after.thread.runtimeMode, "approval-required");
        assert.equal(after.thread.interactionMode, "plan");
      }).pipe(Effect.provide(harness.layer));
    }),
);

it.effect.each([false, true])(
  "direct intake validates destination, prepares once on replay, and honors Stop during preparation: %s",
  (stopDuringPreparation) =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const harness = makeHarness({
        otherWorkspaceRoot: "/destination",
        gitVcs: {
          listWorktreePaths: (cwd) => {
            assert.equal(cwd, "/destination");
            return Effect.succeed(["/destination", "/destination-worktree"]);
          },
        },
        runSetup: () =>
          Deferred.succeed(entered, undefined).pipe(
            Effect.andThen(Deferred.await(release)),
            Effect.as({ status: "no-script" as const }),
          ),
      });
      yield* Effect.gen(function* () {
        const threads = yield* ThreadManagement.ThreadManagementService;
        const sql = yield* SqlClient.SqlClient;
        yield* sql`INSERT INTO projection_projects (project_id, title, workspace_root, scripts_json, default_thread_env_mode, created_at, updated_at)
        VALUES (${otherProjectId}, 'Destination', '/destination', '[]', 'local', '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z')`;
        const parentId = ThreadId.make("direct-parent");
        yield* threads.dispatch({
          type: "thread.create",
          commandId: CommandId.make("direct-create"),
          threadId: parentId,
          projectId,
          title: "Parent",
          modelSelection,
          runtimeMode: "approval-required",
          interactionMode: "plan",
          branch: null,
          worktreePath: null,
          createdBy: "user",
          creationSource: "web",
        });
        yield* threads.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("direct-parent-work"),
          threadId: parentId,
          messageId: MessageId.make("direct-parent-work"),
          text: "Work",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        const parent = yield* threads.getThreadProjection(parentId);
        const command = {
          type: "delegated_task.request" as const,
          commandId: CommandId.make("direct-delegate"),
          parentThreadId: parentId,
          parentRunId: parent.runs[0]!.id,
          parentNodeId: parent.runs[0]!.rootNodeId!,
          projectId: otherProjectId,
          workspaceStrategy: stopDuringPreparation
            ? { type: "worktree" as const, baseRef: "main" }
            : {
                type: "existing_worktree" as const,
                worktreePath: "/destination-worktree",
              },
          task: "Child work",
          modelSelection,
          runtimeMode: "approval-required" as const,
          interactionMode: "plan" as const,
          createdBy: "user" as const,
          creationSource: "web" as const,
        };
        const before = yield* sql<{
          count: number;
        }>`SELECT COUNT(*) AS count FROM projection_threads`;
        for (const invalid of [
          {
            ...command,
            commandId: CommandId.make("direct-unregistered"),
            workspaceStrategy: {
              type: "existing_worktree" as const,
              worktreePath: "/other-project-worktree",
            },
          },
          {
            ...command,
            commandId: CommandId.make("direct-missing"),
            projectId: ProjectId.make("missing"),
          },
        ]) {
          const failure = yield* ThreadMessageIntake.dispatchCommand(invalid).pipe(Effect.flip);
          assert.equal(failure._tag, "OrchestratorMcpFailure");
          if (failure._tag === "OrchestratorMcpFailure")
            assert.equal(failure.code, "invalid_request");
          assert.deepEqual(
            yield* sql<{ count: number }>`SELECT COUNT(*) AS count FROM projection_threads`,
            before,
          );
          assert.lengthOf((yield* threads.getThreadProjection(parentId)).subagents, 0);
        }
        const result = yield* ThreadMessageIntake.dispatchCommand(command);
        yield* Deferred.await(entered);
        const task = (yield* threads.getThreadProjection(parentId)).subagents[0]!;
        const childId = task.childThreadId!;
        assert.equal((yield* threads.getThreadProjection(childId)).runs[0]!.status, "preparing");
        assert.equal(harness.runSetup.mock.calls[0]![0].projectCwd, "/destination");
        assert.equal(
          harness.runSetup.mock.calls[0]![0].worktreePath,
          stopDuringPreparation ? "/repo-worktrees/feature" : "/destination-worktree",
        );
        const replay = yield* ThreadMessageIntake.dispatchCommand(command);
        assert.equal(replay.sequence, result.sequence);
        assert.equal(harness.runSetup.mock.calls.length, 1);
        if (stopDuringPreparation) {
          const tracker = yield* WorktreeSetupTracker.WorktreeSetupTracker;
          const settled = yield* tracker.stream(childId).pipe(
            Stream.filter((snapshot) => snapshot !== null && snapshot.phase !== "running"),
            Stream.runHead,
            Effect.forkChild,
          );
          yield* threads.dispatch({
            type: "thread.stop",
            commandId: CommandId.make("direct-stop-preparing"),
            threadId: childId,
          });
          yield* Deferred.succeed(release, undefined);
          assert.isTrue(Option.isSome(yield* Fiber.join(settled)));
          const releaseReceipts = yield* sql<{
            status: string;
          }>`SELECT status FROM orchestration_command_receipts WHERE command_id = 'direct-delegate:release'`;
          assert.deepEqual(releaseReceipts, [{ status: "rejected" }]);
          const stopped = yield* threads.getThreadProjection(childId);
          assert.equal(stopped.runs[0]!.status, "interrupted");
          assert.equal(harness.runSetup.mock.calls.length, 1);
          return;
        }
        const started = yield* threads.streamStoredEventsFrom({ afterSequence: 0 }).pipe(
          Stream.filter(
            (stored) =>
              stored.event.threadId === childId &&
              stored.event.type === "run.updated" &&
              stored.event.payload.status === "starting",
          ),
          Stream.runHead,
          Effect.forkChild,
        );
        yield* Deferred.succeed(release, undefined);
        assert.isTrue(Option.isSome(yield* Fiber.join(started)));
        const child = yield* threads.getThreadProjection(childId);
        assert.equal(child.thread.projectId, otherProjectId);
        assert.equal(child.thread.lineage.parentThreadId, parentId);
        assert.equal(child.thread.runtimeMode, "approval-required");
        assert.equal(child.thread.interactionMode, "plan");
        yield* threads.dispatch({
          type: "thread.stop",
          commandId: CommandId.make("direct-stop"),
          threadId: childId,
        });
        assert.equal((yield* threads.getThreadProjection(childId)).runs[0]!.status, "interrupted");
      }).pipe(
        Effect.provide(
          harness.layer.pipe(
            Layer.provideMerge(
              Layer.mergeAll(
                ProjectStore.layer,
                ServerConfig.layerTest(process.cwd(), { prefix: "t3-delegated-intake-" }).pipe(
                  Layer.provideMerge(NodeServices.layer),
                ),
              ).pipe(Layer.provide(harness.layer)),
            ),
          ),
        ),
      );
    }),
);

registerPromachosLaunchTests({ makeHarness, launchInput, modelSelection });

it.effect(
  "peer schedules retain origin through persistence, edits and a restarted real launch",
  () => {
    const h = makeHarness();
    const schedulerDeps = Layer.mergeAll(
      h.layer,
      NodeCrypto.layer,
      Scheduler.layer,
      Layer.mock(SecretRequests.SecretRequests)({}),
    );
    const tasksLayer = ScheduledTasks.layer.pipe(
      Layer.provide(Layer.mock(BareLaunch.BareLaunch)({})),
      Layer.provideMerge(schedulerDeps),
    );
    const mcp = OrchestratorMcp.layer.pipe(
      Layer.provide(Layer.mock(BareLaunch.BareLaunch)({})),
      Layer.provideMerge(
        Layer.mergeAll(
          tasksLayer,
          NodeServices.layer,
          Layer.mock(GitVcsDriver.GitVcsDriver)({}),
          Layer.mock(RemoteDelegation.RemoteDelegation)({}),
        ),
      ),
    );
    return Effect.gen(function* () {
      const origin = { sessionId: "peer-schedule", label: "Peer" };
      const scope: Invocation.McpInvocationScope = {
        environmentId: EnvironmentId.make("destination"),
        thread: undefined,
        client: { ...origin, linked: true, access: "full-access" },
        capabilities: new Set(["orchestration"]),
        requestNamespace: "peer-schedule",
        issuedAt: 1,
      };
      const tools = yield* OrchestratorMcp.OrchestratorMcpService;
      const created = yield* tools.scheduleTask(scope, {
        purposeIntent: { version: 1, intent: { type: "unscoped" } },
        projectId,
        prompt: "Do linked work",
        enabled: false,
        schedule: { type: "interval", everyMs: 60_000 },
        bindToCurrentThread: false,
        clientRequestId: "peer-timer",
      });
      const tasks = yield* ScheduledTasks.ScheduledTaskService;
      const stored = (yield* tasks.list()).tasks.find(
        (task) => task.id === created.scheduledTaskId,
      )!;
      assert.deepStrictEqual(stored.linkOrigin, origin);
      const { linkOrigin: _origin, ...form } = stored;
      const edited = yield* tasks.upsert({
        purposeIntent: { version: 1, intent: { type: "unscoped" } },
        ...form,
        requireExisting: true,
        title: "UI edit without ownership fields",
      });
      assert.deepStrictEqual(edited.task.linkOrigin, origin);
      const foreign = { ...scope, client: { ...scope.client!, sessionId: "another-peer" } };
      assert.strictEqual(
        (yield* tools
          .updateScheduledTask(foreign, { scheduledTaskId: stored.id, prompt: "Foreign" })
          .pipe(Effect.flip)).code,
        "capability_denied",
      );
      assert.strictEqual(
        (yield* tools
          .deleteScheduledTask(foreign, { scheduledTaskId: stored.id })
          .pipe(Effect.flip)).code,
        "capability_denied",
      );
      const persistedServices = yield* Effect.context<Layer.Success<typeof schedulerDeps>>();
      // A fresh service reads durable origin, rather than relying on the creating request's context.
      yield* Effect.gen(function* () {
        const restarted = yield* ScheduledTasks.ScheduledTaskService;
        assert.deepStrictEqual((yield* restarted.list()).tasks[0]!.linkOrigin, origin);
        const run = yield* restarted.runNow({ id: stored.id });
        assert.strictEqual(run.task.lastRunStatus, "succeeded");
      }).pipe(
        Effect.provide(
          ScheduledTasks.layer.pipe(
            Layer.provide(Layer.mock(BareLaunch.BareLaunch)({})),
            Layer.provide(Layer.succeedContext(persistedServices)),
          ),
        ),
      );
      const threads = yield* ThreadManagement.ThreadManagementService;
      const child = (yield* threads.listProjectThreads({ projectId, includeSubagents: false }))[0]!;
      const shell = yield* threads.getThreadShell(child.id);
      assert.isNotNull(shell);
      if (shell === null) return;
      assert.deepStrictEqual(shell.linkOrigin, origin);
      const childScope: Invocation.McpInvocationScope = {
        ...scope,
        client: undefined,
        thread: {
          threadId: shell.id,
          providerInstanceId: modelSelection.instanceId,
          providerSessionId: "scheduled-agent",
        },
      };
      const context = { scope: childScope, caller: shell, limits: shell };
      assert.strictEqual(
        (yield* assertSameLink(context, { linkOrigin: undefined }).pipe(Effect.flip)).code,
        "capability_denied",
      );
      assert.strictEqual(
        (yield* assertNotLinked(context, "change projects").pipe(Effect.flip)).code,
        "capability_denied",
      );
      const denial = yield* Effect.gen(function* () {
        const forwarding = yield* PeerForwarding.PeerForwarding;
        return yield* forwarding.call(
          childScope,
          OrchestratorToolkit.tools.t3_thread_list,
          EnvironmentId.make("third-machine"),
          {},
        );
      }).pipe(
        Effect.provide(
          PeerForwarding.layer.pipe(
            Layer.provide(
              Layer.mergeAll(
                Layer.succeedContext(persistedServices),
                Layer.mock(PeerLinks.PeerLinks)({
                  get: () => Effect.die("Must not use onward links"),
                }),
                Layer.mock(PeerMcpClient.PeerMcpClient)({
                  call: () => Effect.die("Must not reach another peer"),
                }),
              ),
            ),
          ),
        ),
        Effect.provideService(Invocation.McpInvocationContext, childScope),
        Effect.flip,
      );
      if (denial._tag !== "OrchestratorMcpFailure") return yield* Effect.die(denial);
      assert.strictEqual(denial.code, "capability_denied");
      const userThread = yield* (yield* ThreadLaunch.ThreadLaunchService).launch(
        launchInput({ command: "user-thread", thread: "user-thread" }),
      );
      const rebound = yield* tasks
        .upsert({ ...form, threadId: userThread.threadId, requireExisting: true })
        .pipe(Effect.flip);
      assert.include(rebound.message, "own link");
      // A binding changed outside the service is checked again at execution.
      const sql = yield* SqlClient.SqlClient;
      yield* sql`UPDATE scheduled_tasks SET thread_id = ${userThread.threadId} WHERE task_id = ${stored.id}`;
      const refused = yield* tasks.runNow({ id: stored.id }).pipe(Effect.flip);
      if (!isTaskError(refused.cause)) return yield* Effect.die(refused);
      assert.include(refused.cause.message, "own link");
      const untouched = yield* threads.getThreadProjection(userThread.threadId);
      assert.deepStrictEqual(untouched.messages, []);
    }).pipe(Effect.provide(mcp));
  },
);

it.effect(
  "bare launch resolves in the provisioned worktree before creating a thread and persists only hashes",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const root = yield* fs.makeTempDirectoryScoped().pipe(Effect.flatMap(fs.realPath));
      yield* fs.writeFileString(`${root}/input.txt`, "only named input");
      const h = makeHarness({
        bare: true,
        createWorktree: (input) =>
          Effect.succeed({
            worktree: { path: root, refName: input.newRefName, headSha: "abc" },
          } as never),
      });
      yield* Effect.gen(function* () {
        const launches = yield* ThreadLaunch.ThreadLaunchService;
        const input = {
          ...launchInput({
            command: "bare-final-cwd",
            thread: "thread:bare-final-cwd",
            workspace: { type: "worktree", baseRef: "main", branch: "bare-final" },
          }),
          runtimeMode: "approval-required" as const,
          bareReadGrant: { type: "roots" as const, roots: [] },
          bare: { additions: [{ type: "file" as const, path: `${root}/input.txt` }] },
        };
        const result = yield* launches.launch(input);
        expect(result.projection.thread.bare?.nativeHomeOwnerThreadId).toBe(result.threadId);
        expect(result.projection.thread.bare?.additions[0]).toMatchObject({
          type: "file",
          path: `${root}/input.txt`,
          byteCount: 16,
        });
        expect(result.projection.thread.worktreePath).toBe(root);
        expect(result.projection.thread.runtimeMode).toBe("approval-required");
        expect(result.projection.thread.bare?.additions[0]).not.toHaveProperty("body");
        expect(h.createWorktree).toHaveBeenCalledTimes(1);
        yield* fs.writeFileString(`${root}/input.txt`, "Changed after acceptance");
        const replay = yield* launches.launch(input);
        expect(replay.projection.thread.bare).toEqual(result.projection.thread.bare);
        expect(h.createWorktree).toHaveBeenCalledTimes(1);
      }).pipe(Effect.provide(h.layer));
    }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "invalid bare additions and native imports create no thread and normal launches stay normal",
  () => {
    const h = makeHarness({ bare: true });
    return Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const rejected = yield* launches
        .launch({
          ...launchInput({ command: "bare-unsupported", thread: "thread:bare-unsupported" }),
          bare: { additions: [{ type: "plugin", name: "not-attached" }] },
        })
        .pipe(Effect.flip);
      expect(rejected.cause).toMatchObject({ reason: expect.stringContaining("not supported") });
      expect(
        yield* (yield* ThreadManagement.ThreadManagementService).listProjectThreads({
          projectId,
          includeSubagents: true,
        }),
      ).toHaveLength(0);
      const imported = yield* launches
        .launch({
          ...launchInput({ command: "bare-import", thread: "thread:bare-import" }),
          bare: {},
          importedNativeThread: {
            ref: {
              driver: ProviderDriverKind.make("codex"),
              nativeId: "existing",
              strength: "strong",
            },
          },
        })
        .pipe(Effect.flip);
      expect(String(imported.cause)).toContain("import");
      const normal = yield* launches.launch(
        launchInput({ command: "normal-after-bare", thread: "thread:normal-after-bare" }),
      );
      expect(normal.projection.thread).not.toHaveProperty("bare");
    }).pipe(Effect.provide(h.layer));
  },
);

it.effect.each(["full-access", "approval-required"] as const)(
  "MCP delegate and batch launches retain bare intent and the %s caller ceiling",
  (mode) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const root = yield* fs.makeTempDirectoryScoped();
      yield* fs.writeFileString(`${root}/named.txt`, "Named body");
      const h = makeHarness({
        bare: true,
        serverSettings: {
          prismRoles: { worker: { instructions: "UNASKED_ROLE_KIT", skills: ["unasked-skill"] } },
        },
        providers: [
          {
            instanceId: modelSelection.instanceId,
            driver: ProviderDriverKind.make("codex"),
            enabled: true,
            installed: true,
            version: "test",
            checkedAt: "2026-10-14T00:00:00Z",
            slashCommands: [],
            skills: [],
            status: "ready",
            auth: { status: "authenticated" },
            models: [
              { slug: modelSelection.model, name: "Test", isCustom: false, capabilities: null },
            ],
          },
        ],
      });
      const mcp = OrchestratorMcpService.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            h.layer,
            h.bareLayer,
            NodeCrypto.layer,
            Layer.mock(RemoteDelegation.RemoteDelegation)({}),
            Layer.mock(ScheduledTasks.ScheduledTaskService)({}),
            Layer.mock(SecretRequests.SecretRequests)({}),
          ),
        ),
      );
      yield* Effect.gen(function* () {
        const threads = yield* ThreadManagement.ThreadManagementService;
        const service = yield* OrchestratorMcpService.OrchestratorMcpService;
        const parentId = ThreadId.make("bare:mcp:parent");
        const sql = yield* SqlClient.SqlClient;
        yield* sql`INSERT INTO projection_projects (project_id,title,workspace_root,scripts_json,created_at,updated_at)
        VALUES (${projectId},${project.title},${project.workspaceRoot},'[]',${project.createdAt},${project.updatedAt})`;
        yield* threads.dispatch({
          ...launchInput({ command: "mcp:create-parent", thread: parentId }),
          type: "thread.create",
          bare: { additions: [] },
          runtimeMode: mode,
          branch: null,
          worktreePath: root,
        });
        yield* threads.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("mcp:start-parent"),
          threadId: parentId,
          messageId: MessageId.make("mcp:parent-input"),
          text: "Launch children",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        const scope: Invocation.McpInvocationScope = {
          environmentId: EnvironmentId.make("bare:mcp"),
          requestNamespace: "bare:mcp",
          thread: {
            threadId: parentId,
            providerInstanceId: modelSelection.instanceId,
            providerSessionId: "parent-session",
          },
          capabilities: new Set(["orchestration"]),
          client: undefined,
          issuedAt: 1,
        };
        const bare = { additions: [{ type: "file" as const, path: `${root}/named.txt` }] };
        const intent = {
          task: "Only the caller task",
          role: "implementation" as const,
          bare,
          clientRequestId: "bare-delegate",
          target: { providerInstanceId: modelSelection.instanceId, model: modelSelection.model },
        };
        const delegated = yield* service.delegateTask(scope, intent);
        assert.isNotNull(delegated.childThreadId);
        const child = yield* threads.getThreadProjection(delegated.childThreadId!);
        expect(child.thread.bare?.nativeHomeOwnerThreadId).toBe(child.thread.id);
        expect(child.thread.bare?.nativeHomeOwnerThreadId).not.toBe(parentId);
        expect(child.thread.runtimeMode).toBe(mode);
        expect(child.thread.bare?.additions[0]).toMatchObject({ type: "file", byteCount: 10 });
        expect(child.messages[0]?.text).toBe(intent.task);
        const batchInput = {
          clientRequestId: "bare-batch",
          threads: [{ bare, prompt: "Batch task" }, { prompt: "Ordinary task" }],
        };
        const batch = yield* service.createThreads(scope, batchInput);
        const bareBatch = yield* threads.getThreadProjection(batch.threads[0]!.threadId);
        const normalBatch = yield* threads.getThreadProjection(batch.threads[1]!.threadId);
        expect(bareBatch.thread.bare?.additions).toEqual(child.thread.bare?.additions);
        expect(bareBatch.thread.bare?.nativeHomeOwnerThreadId).toBe(bareBatch.thread.id);
        expect(bareBatch.thread.bare?.nativeHomeOwnerThreadId).not.toBe(
          child.thread.bare?.nativeHomeOwnerThreadId,
        );
        expect(bareBatch.thread.runtimeMode).toBe(mode);
        expect(bareBatch.messages[0]?.text).toBe("Batch task");
        expect(normalBatch.thread).not.toHaveProperty("bare");
        yield* fs.writeFileString(`${root}/named.txt`, "Changed after acceptance");
        expect((yield* service.delegateTask(scope, intent)).taskId).toBe(delegated.taskId);
        expect((yield* service.createThreads(scope, batchInput)).threads[0]!.threadId).toBe(
          bareBatch.thread.id,
        );
        expect((yield* threads.getThreadProjection(child.thread.id)).thread.bare).toEqual(
          child.thread.bare,
        );
      }).pipe(Effect.provide(Layer.merge(h.layer, mcp)));
    }).pipe(Effect.provide(NodeServices.layer)),
);

const encodeToolPolicyProjection = Schema.encodeEffect(
  Schema.fromJsonString(OrchestrationV2ThreadProjectionJson),
);

it.effect(
  "restricted launch persists only the allowlist and private config reference, rejects other drivers, and rotates one thread independently",
  () => {
    const h = makeHarness({
      adapters: [{ ...adapter, driver: ProviderDriverKind.make("claudeAgent") }],
    });
    return Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const tools = {
        allow: ["mcp:outreach:read_venue", "mcp:outreach:submit_result"],
        mcpServers: {
          outreach: {
            type: "http" as const,
            url: "http://localhost/outreach",
            headers: { Authorization: "test-venue-secret" },
          },
        },
      };
      const firstInput = {
        ...launchInput({ command: "restricted-one", thread: "restricted-one" }),
        tools,
      };
      const first = yield* launches.launch(firstInput);
      const second = yield* launches.launch({
        ...launchInput({ command: "restricted-two", thread: "restricted-two" }),
        tools,
      });
      expect(first.projection.thread.tools?.allow).toEqual(tools.allow);
      expect(first.projection.thread.tools?.mcpConfigId).toBeDefined();
      expect(first.projection.thread.tools?.mcpConfigId).not.toEqual(
        second.projection.thread.tools?.mcpConfigId,
      );
      const encoded = yield* encodeToolPolicyProjection(first.projection);
      expect(encoded).not.toContain("test-venue-secret");
      expect(encoded).not.toContain("localhost/outreach");
      const replay = yield* launches.replayOrLaunch(
        { ...firstInput, tools: { ...tools, allow: [...tools.allow] } },
        Effect.die("must replay"),
      );
      expect(replay.threadId).toBe(first.threadId);
      yield* launches
        .replayOrLaunch({ ...firstInput, tools: { allow: ["Bash"] } }, Effect.die("must reject"))
        .pipe(Effect.flip);
      yield* launches.configureMcp({ threadId: first.threadId, servers: {} });
      const threads = yield* ThreadManagement.ThreadManagementService;
      const updated = yield* threads.getThreadRecords(first.threadId, []);
      const untouched = yield* threads.getThreadRecords(second.threadId, []);
      expect(updated.thread.tools?.allow).toEqual(tools.allow);
      expect(updated.thread.tools?.mcpConfigId).not.toEqual(
        first.projection.thread.tools?.mcpConfigId,
      );
      expect(untouched.thread.tools).toEqual(second.projection.thread.tools);
      yield* threads
        .dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("restricted-lower-mode"),
          threadId: first.threadId,
          runtimeMode: "approval-required",
        })
        .pipe(Effect.flip);
      yield* threads
        .dispatch({
          type: "thread.interaction-mode.set",
          commandId: CommandId.make("restricted-plan-mode"),
          threadId: first.threadId,
          interactionMode: "plan",
        })
        .pipe(Effect.flip);
      const admitted = yield* threads.getThreadRecords(first.threadId, []);
      expect(admitted.thread.runtimeMode).toBe("full-access");
      expect(admitted.thread.interactionMode).toBe("default");
      yield* threads.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("restricted-busy"),
        threadId: second.threadId,
        messageId: MessageId.make("restricted-busy"),
        text: "Stay active",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        createdBy: "agent",
        creationSource: "mcp",
      });
      const busy = yield* launches
        .configureMcp({ threadId: second.threadId, servers: {} })
        .pipe(Effect.flip);
      expect(busy.message).toContain("idle restricted thread");
    }).pipe(Effect.provide(h.layer));
  },
);

it.effect(
  "tool allowlists reject providers without proven native enforcement before creating a thread",
  () => {
    const h = makeHarness();
    return Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const error = yield* launches
        .launch({
          ...launchInput({ command: "reject-tools", thread: "reject-tools" }),
          tools: { allow: [] },
        })
        .pipe(Effect.flip);
      expect(error.cause).toMatchObject({
        reason: expect.stringContaining("not supported on codex"),
      });
      expect(error.message).toContain("not supported on codex");
      const threads = yield* ThreadManagement.ThreadManagementService;
      expect(yield* threads.listProjectThreads({ projectId, includeSubagents: true })).toHaveLength(
        0,
      );
    }).pipe(Effect.provide(h.layer));
  },
);

it.effect.each([
  { runtimeMode: "approval-required" as const, interactionMode: "default" as const },
  { runtimeMode: "auto-accept-edits" as const, interactionMode: "default" as const },
  { runtimeMode: "auto" as const, interactionMode: "default" as const },
  { runtimeMode: "full-access" as const, interactionMode: "plan" as const },
])("rejects restricted launches in unsupported modes: $runtimeMode/$interactionMode", (modes) => {
  const h = makeHarness({
    adapters: [{ ...adapter, driver: ProviderDriverKind.make("claudeAgent") }],
  });
  return Effect.gen(function* () {
    const launches = yield* ThreadLaunch.ThreadLaunchService;
    const error = yield* launches
      .launch({
        ...launchInput({ command: "unsupported-mode", thread: "unsupported-mode" }),
        ...modes,
        tools: {
          allow: ["mcp:outreach:read_venue"],
          mcpServers: {
            outreach: { type: "http", url: "http://localhost/outreach" },
          },
        },
      })
      .pipe(Effect.flip);
    expect(error.message).toContain("full-access");
    expect(
      yield* (yield* ThreadManagement.ThreadManagementService).listProjectThreads({
        projectId,
        includeSubagents: true,
      }),
    ).toHaveLength(0);
  }).pipe(Effect.provide(h.layer));
});

it.effect(
  "configure379: registered keyed replay preserves a later private configuration and original receipt",
  () => {
    const h = makeHarness({
      adapters: [{ ...adapter, driver: ProviderDriverKind.make("claudeAgent") }],
    });
    const services = h.layer.pipe(
      Layer.provideMerge(RequestReceipts.layer.pipe(Layer.provide(h.layer))),
    );
    const registered = McpHttpServer.toolkitRegistration(
      ThreadMcpToolkit,
      ThreadMcpHandlers.layer,
    ).pipe(Layer.provideMerge(McpServer.McpServer.layer), Layer.provideMerge(services));
    return Effect.scoped(
      Effect.gen(function* () {
        const launches = yield* ThreadLaunch.ThreadLaunchService;
        const threads = yield* ThreadManagement.ThreadManagementService;
        const server = yield* McpServer.McpServer;
        const launched = yield* launches.launch({
          ...launchInput({ command: "configure379-launch", thread: "configure379-thread" }),
          tools: { allow: ["mcp:outreach:read_venue"] },
        });
        const scope: Invocation.McpInvocationScope = {
          environmentId: EnvironmentId.make("configure379"),
          requestNamespace: "configure379-client",
          issuedAt: 0,
          capabilities: new Set(["orchestration"]),
          thread: undefined,
          client: {
            sessionId: "configure379-client",
            label: "Test",
            access: "full-access",
            linked: false,
          },
        };
        const client = McpSchema.McpServerClient.of({
          clientId: 1,
          protocolVersion: "2025-06-18",
          clientCapabilities: {},
          clientInfo: { name: "configure379", version: "1" },
          initializePayload: {
            protocolVersion: "2025-06-18",
            capabilities: {},
            clientInfo: { name: "configure379", version: "1" },
          },
          getClient: Effect.die("unused"),
        });
        const servers = (label: string) => ({
          outreach: {
            type: "http" as const,
            url: `https://${label}.example.test/mcp`,
            headers: { Authorization: `synthetic-private-${label}` },
          },
        });
        const call = (key: string, label: string) =>
          server
            .callTool({
              name: "t3_thread_mcp_configure",
              arguments: {
                threadId: launched.threadId,
                servers: servers(label),
                clientRequestId: key,
              },
            })
            .pipe(
              Effect.provideService(Invocation.McpInvocationContext, scope),
              Effect.provideService(McpSchema.McpServerClient, client),
            );
        const first = yield* call("K", "a");
        expect(first.isError, JSON.stringify(first)).not.toBe(true);
        const configA = (yield* threads.getThreadRecords(launched.threadId, [])).thread.tools
          ?.mcpConfigId;
        const second = yield* call("L", "b");
        expect(second.isError, JSON.stringify(second)).not.toBe(true);
        const configB = (yield* threads.getThreadRecords(launched.threadId, [])).thread.tools
          ?.mcpConfigId;
        expect(configB).not.toBe(configA);
        const replay = yield* call("K", "a");
        expect(replay.isError, JSON.stringify(replay)).not.toBe(true);
        expect(replay.structuredContent).toEqual(first.structuredContent);
        expect(
          (yield* threads.getThreadRecords(launched.threadId, [])).thread.tools?.mcpConfigId,
        ).toBe(configB);
        const sql = yield* SqlClient.SqlClient;
        const receipts = yield* sql`SELECT * FROM mcp_request_receipts`;
        expect(JSON.stringify(receipts)).not.toContain("synthetic-private");
        expect(JSON.stringify(receipts)).not.toContain("example.test");
        expect(JSON.stringify(yield* call("K", "changed"))).toContain("invalid_request");
        expect(
          (yield* threads.getThreadRecords(launched.threadId, [])).thread.tools?.mcpConfigId,
        ).toBe(configB);
      }),
    ).pipe(Effect.provide(registered));
  },
);
