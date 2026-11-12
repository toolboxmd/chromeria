import { BareLaunchError } from "@t3tools/provider-core/server/bareLaunch";
import { CommandId } from "@t3tools/contracts";
import * as AdapterRegistry from "../../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderRegistry from "../../../provider/ProviderRegistry.ts";
import * as ServerSettings from "../../../serverSettings.ts";
import * as Database from "../../../persistence/Sqlite.ts";
import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  GitCommandError,
  ProjectId,
  ProviderInstanceId,
  ProviderDriverKind,
  type ServerProvider,
  ThreadId,
  type OrchestrationV2ThreadShell,
  type Project as ProjectRecord,
  type ModelSelection,
  type RuntimeMode,
} from "@t3tools/contracts";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/process";
import * as TestClock from "effect/testing/TestClock";

import * as DocumentText from "../../../provider/DocumentText.ts";
import { OrchestratorRuntimeModeAboveCeilingError } from "../../../orchestration-v2/Orchestrator.ts";
import * as ThreadLaunch from "../../../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagement from "../../../orchestration-v2/ThreadManagementService.ts";
import * as ServerConfig from "../../../config.ts";
import * as Project from "../../../project/ProjectService.ts";
import * as ProjectCloneTracker from "../../../project/ProjectCloneTracker.ts";
import * as ManagedProjectFolders from "../../../project/ManagedProjectFolders.ts";
import * as PeerForwarding from "../../../peer/PeerForwarding.ts";
import * as SourceControlProviderRegistry from "../../../sourceControl/SourceControlProviderRegistry.ts";
import * as Repositories from "../../../sourceControl/SourceControlRepositoryService.ts";
import * as GitVcsDriver from "../../../vcs/GitVcsDriver.ts";
import * as VcsProcess from "../../../vcs/VcsProcess.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import * as ProjectHandlers from "./handlers.ts";
import { ProjectToolkit } from "./tools.ts";
import { liveThreadShell } from "../../McpToolAccess.testkit.ts";

const prismDependencies = Layer.mergeAll(
  DocumentText.layer.pipe(Layer.provide(NodeServices.layer)),
  Database.layerMemory,
  Layer.mock(AdapterRegistry.ProviderAdapterRegistryV2)({
    get: (instanceId) =>
      Effect.succeed({
        instanceId,
        driver: ProviderDriverKind.make("claudeAgent"),
        bareSupport: Effect.succeed({ supported: true }),
        getCapabilities: () => Effect.die("unused"),
        planSelectionTransition: () => Effect.die("unused"),
        openSession: () => Effect.die("unused"),
      }),
    list: () =>
      Effect.succeed([
        ProviderInstanceId.make("codex"),
        ProviderInstanceId.make("claudeAgent"),
        ProviderInstanceId.make("claude"),
      ]),
  }),
  ServerSettings.layerTest(),
  Layer.mock(ProviderRegistry.ProviderRegistry)({
    getProviders: Effect.succeed([
      {
        instanceId: ProviderInstanceId.make("codex"),
        driver: ProviderDriverKind.make("codex"),
        enabled: true,
        installed: true,
        version: "test",
        status: "ready",
        auth: { status: "authenticated" },
        checkedAt: "2026-10-08T00:00:00Z",
        models: [
          { slug: "gpt-5", name: "test", isCustom: false, capabilities: null },
          // The project default a Settings override names.
          { slug: "gpt-settings", name: "test", isCustom: false, capabilities: null },
        ],
        skills: [],
        slashCommands: [],
      },
      {
        instanceId: ProviderInstanceId.make("claudeAgent"),
        driver: ProviderDriverKind.make("claudeAgent"),
        enabled: true,
        installed: true,
        version: "test",
        status: "ready",
        auth: { status: "authenticated" },
        checkedAt: "2026-10-08T00:00:00Z",
        models: [{ slug: "claude-opus", name: "test", isCustom: false, capabilities: null }],
        skills: [],
        slashCommands: [],
      },
      {
        instanceId: ProviderInstanceId.make("claude"),
        driver: ProviderDriverKind.make("claudeAgent"),
        enabled: true,
        installed: true,
        version: "test",
        status: "ready",
        auth: { status: "authenticated" },
        checkedAt: "2026-10-08T00:00:00Z",
        models: [{ slug: "claude-opus", name: "test", isCustom: false, capabilities: null }],
        skills: [],
        slashCommands: [],
      },
    ]),
  }),
);

it.effect.each(["full-access", "approval-required"] as const)(
  "bare launch reads under the %s caller grant even when the child is restricted in another project",
  (runtimeMode) =>
    Effect.gen(function* () {
      const caller = {
        ...liveThreadShell(ThreadId.make("bare-caller"), { runtimeMode }),
        worktreePath: "/caller",
      };
      const destination = ProjectId.make("other-project");
      let launched: ThreadLaunch.ThreadLaunchInput | undefined;
      const dependencies = Layer.mergeAll(
        prismDependencies,
        NodeServices.layer,
        NodeCrypto.layer,
        Layer.mock(AdapterRegistry.ProviderAdapterRegistryV2)({
          list: () => Effect.succeed([caller.providerInstanceId]),
          get: () =>
            Effect.succeed({
              instanceId: caller.providerInstanceId,
              driver: ProviderDriverKind.make("codex"),
              bareSupport: Effect.succeed({ supported: true }),
              getCapabilities: () => Effect.die("unused"),
              planSelectionTransition: () => Effect.die("unused"),
              openSession: () => Effect.die("unused"),
            }),
        }),
        Layer.succeed(McpInvocationContext.McpInvocationContext, {
          environmentId: EnvironmentId.make("here"),
          requestNamespace: "bare-caller",
          thread: {
            threadId: caller.id,
            providerSessionId: "session",
            providerInstanceId: caller.providerInstanceId,
          },
          client: undefined,
          issuedAt: 0,
          capabilities: new Set(["orchestration"] as const),
        }),
        Layer.mock(ThreadManagement.ThreadManagementService)({
          getThreadShell: () => Effect.succeed(caller),
        }),
        Layer.mock(ThreadLaunch.ThreadLaunchService)({
          replayOrLaunch: (_input, launch) => launch,
          launch: (input) => {
            launched = input;
            return Effect.succeed({
              threadId: input.threadId,
              projection: {
                thread: {
                  id: input.threadId,
                  projectId: destination,
                  title: input.title ?? "New thread",
                  modelSelection: caller.modelSelection,
                },
                runs: [],
              },
              resumed: false,
            } as unknown as ThreadLaunch.ThreadLaunchResult);
          },
        }),
        Layer.mock(Project.ProjectService)({
          getById: (id) =>
            Effect.succeed(
              Option.some({ id, workspaceRoot: "/projects/target" } as unknown as ProjectRecord),
            ),
        }),
        Layer.mock(PeerForwarding.PeerForwarding)({}),
        Layer.mock(GitVcsDriver.GitVcsDriver)({}),
        Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({ namedProjectsRoot: "/projects" }),
        ServerConfig.layerTest(process.cwd(), { prefix: "t3-bare-grant-" }).pipe(
          Layer.provide(NodeServices.layer),
        ),
      );
      const toolkit = yield* ProjectToolkit.pipe(
        Effect.provide(
          McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(
            Layer.provide(dependencies),
          ),
        ),
      );
      yield* toolkit
        .handle("t3_thread_launch", {
          projectId: destination,
          title: "Bare",
          bare: {},
          runtimeMode: "approval-required",
          message: "Only task text",
          modelSelection: caller.modelSelection,
        })
        .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));
      expect(launched?.runtimeMode).toBe("approval-required");
      expect(launched?.bareReadGrant).toEqual(
        runtimeMode === "full-access" ? { type: "all" } : { type: "roots", roots: ["/caller"] },
      );
      expect(launched?.initialMessage?.text).toBe("Only task text");
    }),
);

it.effect("attributes a launched thread's first message to the calling thread", () =>
  Effect.gen(function* () {
    const sourceThreadId = ThreadId.make("source-thread");
    const projectId = ProjectId.make("project");
    const providerInstanceId = ProviderInstanceId.make("codex");
    const modelSelection = { instanceId: providerInstanceId, model: "gpt-5" };
    const caller = {
      id: sourceThreadId,
      projectId,
      providerInstanceId,
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      activeRunId: "active-run",
      archivedAt: null,
      deletedAt: null,
      owner: "Pauli",
    } as OrchestrationV2ThreadShell;
    let launchedSender: ThreadId | undefined;
    let launchedOwner: string | undefined;
    let launchedRole: string | undefined;
    const layerDependencies = Layer.mergeAll(
      ServerSettings.layerTest(),
      NodeCrypto.layer,
      Layer.succeed(McpInvocationContext.McpInvocationContext, {
        environmentId: EnvironmentId.make("environment"),
        requestNamespace: "session",
        thread: {
          threadId: sourceThreadId,
          providerSessionId: "session",
          providerInstanceId,
        },
        client: undefined,
        issuedAt: 0,
        capabilities: new Set(["orchestration" as const]),
      }),
      Layer.mock(ThreadManagement.ThreadManagementService)({
        getThreadShell: () => Effect.succeed(caller),
      }),
      Layer.mock(ThreadLaunch.ThreadLaunchService)({
        replayOrLaunch: (_input, launch) => launch,
        launch: (input) => {
          launchedSender = input.initialMessage?.senderThreadId;
          launchedOwner = input.owner;
          launchedRole = input.prismRole;
          return Effect.succeed({
            threadId: input.threadId,
            projection: {
              thread: { id: input.threadId, projectId, title: "New thread", modelSelection },
              runs: [],
            },
            resumed: false,
          } as unknown as ThreadLaunch.ThreadLaunchResult);
        },
      }),
      Layer.mock(Project.ProjectService)({
        getById: (id) =>
          Effect.succeed(
            Option.some({ id, workspaceRoot: "/projects/target" } as unknown as ProjectRecord),
          ),
      }),
      Layer.mock(PeerForwarding.PeerForwarding)({}),
      Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({ namedProjectsRoot: "/projects" }),
      Layer.mock(GitVcsDriver.GitVcsDriver)({}),
      NodeServices.layer,
      ServerConfig.layerTest(process.cwd(), { prefix: "t3-source-link-" }).pipe(
        Layer.provide(NodeServices.layer),
      ),
    );
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(
          Layer.provide(prismDependencies),
          Layer.provide(layerDependencies),
        ),
      ),
    );
    const result = yield* toolkit
      .handle("t3_thread_launch", {
        title: "Audit",
        message: "Review the change",
        prismRole: "promachos",
      })
      .pipe(
        Stream.unwrap,
        Stream.runCollect,
        Effect.provide(Layer.mergeAll(layerDependencies, prismDependencies)),
      );
    expect(result.at(-1)?.result).toMatchObject({ projectId, modelSelection });
    expect(launchedSender).toBe(sourceThreadId);
    // Fork: a launched thread belongs to the caller's owner (toolboxmd/chromeria#170).
    expect(launchedOwner).toBe("Pauli");
    // Fork: an agent cannot make itself the Promachos (toolboxmd/chromeria#532).
    expect(launchedRole).toBeUndefined();
  }),
);

it.effect("launches a scratch thread into the Scratch project", () =>
  Effect.gen(function* () {
    const sourceThreadId = ThreadId.make("source-thread");
    const scratchProjectId = ProjectId.make("project:scratch");
    const providerInstanceId = ProviderInstanceId.make("codex");
    const modelSelection = { instanceId: providerInstanceId, model: "gpt-5" };
    const caller = {
      id: sourceThreadId,
      projectId: ProjectId.make("project:caller"),
      providerInstanceId,
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      activeRunId: "active-run",
      archivedAt: null,
      deletedAt: null,
    } as OrchestrationV2ThreadShell;
    const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
    const layerDependencies = Layer.mergeAll(
      ServerSettings.layerTest(),
      NodeCrypto.layer,
      Layer.succeed(McpInvocationContext.McpInvocationContext, {
        environmentId: EnvironmentId.make("environment"),
        requestNamespace: "session",
        thread: {
          threadId: sourceThreadId,
          providerSessionId: "session",
          providerInstanceId,
        },
        client: undefined,
        issuedAt: 0,
        capabilities: new Set(["orchestration" as const]),
      }),
      Layer.mock(ThreadManagement.ThreadManagementService)({
        getThreadShell: () => Effect.succeed(caller),
      }),
      Layer.mock(ThreadLaunch.ThreadLaunchService)({
        replayOrLaunch: (_input, launch) => launch,
        launch: (input) => {
          launched.push(input);
          return Effect.succeed({
            threadId: input.threadId,
            projection: {
              thread: {
                id: input.threadId,
                projectId: input.projectId,
                title: "New thread",
                modelSelection,
              },
              runs: [],
            },
            resumed: false,
          } as unknown as ThreadLaunch.ThreadLaunchResult);
        },
      }),
      Layer.mock(Project.ProjectService)({
        getById: (id) =>
          Effect.succeed(
            Option.some({ id, workspaceRoot: "/projects/target" } as unknown as ProjectRecord),
          ),
      }),
      Layer.mock(PeerForwarding.PeerForwarding)({}),
      Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
        namedProjectsRoot: "/projects",
        ensureScratchProject: Effect.succeed({ projectId: scratchProjectId }),
      }),
      Layer.mock(GitVcsDriver.GitVcsDriver)({}),
      NodeServices.layer,
      ServerConfig.layerTest(process.cwd(), { prefix: "t3-scratch-launch-" }).pipe(
        Layer.provide(NodeServices.layer),
      ),
    );
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(
          Layer.provide(prismDependencies),
          Layer.provide(layerDependencies),
        ),
      ),
    );
    const handle = (params: Parameters<typeof toolkit.handle<"t3_thread_launch">>[1]) =>
      toolkit
        .handle("t3_thread_launch", params)
        .pipe(
          Stream.unwrap,
          Stream.runCollect,
          Effect.provide(Layer.mergeAll(layerDependencies, prismDependencies)),
        );

    const result = yield* handle({ title: "Notes", scratch: true, message: "Draft a list" });
    expect(result.at(-1)?.result).toMatchObject({ projectId: scratchProjectId });
    expect(launched.map((input) => [input.projectId, input.workspaceStrategy])).toEqual([
      [scratchProjectId, { type: "root" }],
    ]);

    const rejected = yield* handle({
      title: "Notes",
      scratch: true,
      projectId: caller.projectId,
    });
    expect(rejected.at(-1)?.result).toMatchObject({ code: "invalid_request" });
    expect(launched).toHaveLength(1);
  }),
);

it.effect("starts a project from just a title when workspaceRoot is omitted", () =>
  Effect.gen(function* () {
    const sourceThreadId = ThreadId.make("source-thread");
    const createdProjectId = ProjectId.make("project:named");
    const providerInstanceId = ProviderInstanceId.make("codex");
    const caller = {
      id: sourceThreadId,
      projectId: ProjectId.make("project:caller"),
      providerInstanceId,
      modelSelection: { instanceId: providerInstanceId, model: "gpt-5" },
      runtimeMode: "full-access",
      interactionMode: "default",
      activeRunId: "active-run",
      archivedAt: null,
      deletedAt: null,
    } as OrchestrationV2ThreadShell;
    const named: Array<string> = [];
    const registered: Array<string> = [];
    const createdProject = {
      id: createdProjectId,
      title: "Pinball Stats",
      workspaceRoot: "/projects/pinball-stats",
      defaultModelSelection: null,
      scripts: [],
      createdAt: "2026-10-01T00:00:00.000Z",
      updatedAt: "2026-10-01T00:00:00.000Z",
      deletedAt: null,
    };
    const layerDependencies = Layer.mergeAll(
      ServerSettings.layerTest(),
      NodeCrypto.layer,
      Layer.succeed(McpInvocationContext.McpInvocationContext, {
        environmentId: EnvironmentId.make("environment"),
        requestNamespace: "session",
        thread: {
          threadId: sourceThreadId,
          providerSessionId: "session",
          providerInstanceId,
        },
        client: undefined,
        issuedAt: 0,
        capabilities: new Set(["orchestration" as const]),
      }),
      Layer.mock(ThreadManagement.ThreadManagementService)({
        getThreadShell: () => Effect.succeed(caller),
      }),
      Layer.mock(ThreadLaunch.ThreadLaunchService)({}),
      Layer.mock(Project.ProjectService)({
        create: (input) =>
          Effect.sync(() => {
            registered.push(input.workspaceRoot);
            return { ...createdProject, id: input.projectId, workspaceRoot: input.workspaceRoot };
          }),
        getById: (projectId) =>
          Effect.succeed(
            projectId === createdProjectId ? Option.some(createdProject) : Option.none(),
          ),
      }),
      Layer.mock(PeerForwarding.PeerForwarding)({}),
      Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
        namedProjectsRoot: "/projects",
        createNamedProject: (input) =>
          Effect.sync(() => {
            named.push(input.name);
            return {
              projectId: createdProjectId,
              workspaceRoot: createdProject.workspaceRoot,
              commitError: "Git has no name or email on this machine.",
            };
          }),
      }),
      Layer.mock(GitVcsDriver.GitVcsDriver)({}),
      NodeServices.layer,
      ServerConfig.layerTest(process.cwd(), { prefix: "t3-named-project-" }).pipe(
        Layer.provide(NodeServices.layer),
      ),
    );
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(
          Layer.provide(prismDependencies),
          Layer.provide(layerDependencies),
        ),
      ),
    );
    const handle = (params: Parameters<typeof toolkit.handle<"t3_project_create">>[1]) =>
      toolkit
        .handle("t3_project_create", params)
        .pipe(
          Stream.unwrap,
          Stream.runCollect,
          Effect.provide(Layer.mergeAll(layerDependencies, prismDependencies)),
        );

    const result = yield* handle({ title: "Pinball Stats" });
    expect(result.at(-1)?.result).toMatchObject({
      id: createdProjectId,
      workspaceRoot: "/projects/pinball-stats",
      commitError: "Git has no name or email on this machine.",
    });
    expect(named).toEqual(["Pinball Stats"]);

    yield* handle({ title: "Existing", workspaceRoot: "/work/existing" });
    expect(registered).toEqual(["/work/existing"]);

    for (const extra of [
      { scripts: [] },
      { defaultModelSelection: { instanceId: providerInstanceId, model: "gpt-5" } },
    ]) {
      const rejected = yield* handle({ title: "Configured", ...extra });
      expect(rejected.at(-1)?.result).toMatchObject({ code: "invalid_request" });
    }
    expect(named).toEqual(["Pinball Stats"]);
  }),
);

const clientLaunchHarness = (input: {
  readonly runtimeModeCeiling: RuntimeMode;
  readonly defaultRuntimeMode?: RuntimeMode;
  readonly fallbackModel?: ModelSelection;
  readonly providers?: readonly ServerProvider[];
  /** Where Settings saves a project's model; the legacy project field stays null. */
  readonly overrideModel?: ModelSelection;
  readonly launched: Array<ThreadLaunch.ThreadLaunchInput>;
  readonly workspaceRoot?: string;
  readonly checkWorktreeBase?: ThreadLaunch.ThreadLaunchService["Service"]["checkWorktreeBase"];
  readonly hostedAppUrl?: string;
  /** The orchestrator refuses the new thread's mode, as under a managed ceiling. */
  readonly refusedByCeiling?: true;
}) => {
  const projectId = ProjectId.make("project:client-target");
  const modelSelection = {
    instanceId: ProviderInstanceId.make("claudeAgent"),
    model: "claude-opus",
  };
  // The project's own default model, which a test can clear between launches.
  const projectDefaults: { model: ModelSelection | null } = {
    model: input.overrideModel === undefined ? modelSelection : null,
  };
  const launchResult = (launch: ThreadLaunch.ThreadLaunchInput) =>
    ({
      threadId: launch.threadId,
      projection: {
        thread: {
          id: launch.threadId,
          projectId: launch.projectId,
          title: launch.title ?? "New thread",
          modelSelection: launch.modelSelection,
        },
        runs: [],
      },
      resumed: false,
    }) as unknown as ThreadLaunch.ThreadLaunchResult;
  const layerDependencies = Layer.mergeAll(
    prismDependencies,
    input.providers === undefined
      ? Layer.empty
      : Layer.mock(ProviderRegistry.ProviderRegistry)({
          getProviders: Effect.succeed(input.providers),
        }),
    ServerSettings.layerTest({
      fallbackModelSelection: input.fallbackModel ?? null,
      ...(input.defaultRuntimeMode === undefined
        ? {}
        : { defaultRuntimeMode: input.defaultRuntimeMode }),
      ...(input.overrideModel === undefined
        ? {}
        : {
            projectSettingsFolded: true,
            projectSettingsOverrides: {
              [projectId]: { defaultModelSelection: input.overrideModel },
            },
          }),
    }),
    Layer.succeed(McpInvocationContext.McpInvocationContext, {
      environmentId: EnvironmentId.make("environment"),
      requestNamespace: "client:session-1",
      thread: undefined,
      client: {
        sessionId: "session-1",
        label: "Claude Code",
        access: input.runtimeModeCeiling,
        hostedAppUrl: input.hostedAppUrl,
      },
      issuedAt: 0,
      capabilities: new Set(["orchestration" as const]),
    }),
    Layer.mock(ThreadManagement.ThreadManagementService)({
      // A thread exists once a launch started it.
      getThreadShell: (threadId) => {
        const launch = input.launched.find((candidate) => candidate.threadId === threadId);
        return Effect.succeed(
          launch === undefined
            ? null
            : ({
                id: launch.threadId,
                projectId: launch.projectId,
                modelSelection: launch.modelSelection,
                deletedAt: null,
              } as unknown as OrchestrationV2ThreadShell),
        );
      },
    }),
    Layer.mock(ThreadLaunch.ThreadLaunchService)({
      replayOrLaunch: (request, launch) =>
        Effect.suspend(() => {
          const accepted = input.launched.find((entry) => entry.commandId === request.commandId);
          if (accepted === undefined) return launch;
          input.launched.push(accepted);
          return Effect.succeed(launchResult(accepted));
        }),
      checkWorktreeBase: input.checkWorktreeBase ?? (() => Effect.void),
      launch: (launch) => {
        input.launched.push(launch);
        if (input.refusedByCeiling) {
          return Effect.fail(
            new ThreadLaunch.ThreadLaunchError({
              operation: "create-thread",
              commandId: launch.commandId,
              projectId: launch.projectId,
              cause: new OrchestratorRuntimeModeAboveCeilingError({
                commandId: launch.commandId,
                threadId: launch.threadId ?? ThreadId.make("thread:refused"),
                refused: "mode",
                runtimeMode: launch.runtimeMode,
                maxRuntimeMode: "approval-required",
                organization: "Example Corp",
              }),
            }),
          );
        }
        return Effect.succeed(launchResult(launch));
      },
    }),
    Layer.mock(Project.ProjectService)({
      getById: (id) =>
        Effect.succeed(
          id === projectId
            ? Option.some({
                id,
                workspaceRoot: input.workspaceRoot ?? "/projects/client-target",
                defaultModelSelection: projectDefaults.model,
              } as unknown as ProjectRecord)
            : Option.none(),
        ),
    }),
    Layer.mock(PeerForwarding.PeerForwarding)({}),
    Layer.mock(PeerForwarding.PeerForwarding)({}),
    Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({ namedProjectsRoot: "/projects" }),
    DocumentText.layer.pipe(Layer.provideMerge(NodeServices.layer)),
  ).pipe(
    Layer.provideMerge(GitVcsDriver.layer),
    Layer.provideMerge(VcsProcess.layer),
    Layer.provideMerge(
      ServerConfig.layerTest(process.cwd(), { prefix: "t3-client-launch-" }).pipe(
        Layer.provide(NodeServices.layer),
      ),
    ),
    Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
  );
  return { projectId, modelSelection, projectDefaults, dependencies: layerDependencies };
};

it.effect(
  "a client launches with the project's default model and mode, capped at its ceiling",
  () =>
    Effect.gen(function* () {
      const launchWith = Effect.fn("test.launchWith")(function* (
        runtimeModeCeiling: RuntimeMode,
        defaultRuntimeMode: RuntimeMode,
        params: {
          readonly title: string;
          readonly message?: string;
          readonly runtimeMode?: RuntimeMode;
          readonly targeted?: false;
          readonly overrideModel?: ModelSelection;
          readonly hostedAppUrl?: string;
          readonly refusedByCeiling?: true;
        },
      ) {
        const { targeted, overrideModel, hostedAppUrl, refusedByCeiling, ...input } = params;
        const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
        const { projectId, modelSelection, dependencies } = clientLaunchHarness({
          runtimeModeCeiling,
          defaultRuntimeMode,
          launched,
          ...(overrideModel === undefined ? {} : { overrideModel }),
          ...(hostedAppUrl === undefined ? {} : { hostedAppUrl }),
          ...(refusedByCeiling === undefined ? {} : { refusedByCeiling }),
        });
        const toolkit = yield* ProjectToolkit.pipe(
          Effect.provide(
            McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(
              Layer.provide(dependencies),
            ),
          ),
        );
        const result = yield* toolkit
          .handle("t3_thread_launch", targeted === false ? input : { ...input, projectId })
          .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));
        return { result: result.at(-1)?.result, launched, projectId, modelSelection };
      });

      const belowCeiling = yield* launchWith("full-access", "approval-required", {
        title: "Fix",
        message: "Fix the bug",
      });
      expect(belowCeiling.result).toMatchObject({
        projectId: belowCeiling.projectId,
        modelSelection: belowCeiling.modelSelection,
        link: `[Fix](t3-thread://v2/environment/${belowCeiling.launched[0]?.threadId})`,
      });
      expect(belowCeiling.launched[0]?.runtimeMode).toBe("approval-required");
      expect(belowCeiling.launched[0]?.initialMessage?.senderThreadId).toBeUndefined();

      const aboveCeiling = yield* launchWith("auto-accept-edits", "full-access", {
        title: "Fix",
        hostedAppUrl: "https://t3.example.com",
      });
      expect(aboveCeiling.launched[0]?.runtimeMode).toBe("auto-accept-edits");
      expect(aboveCeiling.result).toMatchObject({
        link: `https://t3.example.com/threads/environment/${encodeURIComponent(aboveCeiling.launched[0]?.threadId ?? "")}`,
      });

      const requested = yield* launchWith("full-access", "approval-required", {
        title: "Fix",
        runtimeMode: "auto",
      });
      expect(requested.launched[0]?.runtimeMode).toBe("auto");

      const escalated = yield* launchWith("auto-accept-edits", "auto-accept-edits", {
        title: "Fix",
        runtimeMode: "full-access",
      });
      expect(escalated.result).toMatchObject({ code: "runtime_mode_escalation_denied" });
      expect(escalated.launched).toHaveLength(0);

      const untargeted = yield* launchWith("full-access", "full-access", {
        title: "Fix",
        targeted: false,
      });
      expect(untargeted.result).toMatchObject({ code: "target_required" });

      const overrideModel = {
        instanceId: ProviderInstanceId.make("codex"),
        model: "gpt-settings",
      };
      const fromSettings = yield* launchWith("full-access", "full-access", {
        title: "Fix",
        overrideModel,
      });
      expect(fromSettings.launched[0]?.modelSelection).toEqual(overrideModel);

      // An administrator's ceiling the orchestrator enforces is named, not reported as unavailable.
      const managed = yield* launchWith("full-access", "full-access", {
        title: "Fix",
        refusedByCeiling: true,
      });
      expect(managed.result).toMatchObject({
        code: "invalid_request",
        message: "Example Corp limits threads to Supervised.",
      });
    }),
);

it.effect("a launch steps an inherited mode down to the provider's, and refuses one it lacks", () =>
  Effect.gen(function* () {
    // Pi offers no Auto; a mode a provider lacks would run as Supervised.
    const pi = ProviderInstanceId.make("pi");
    const launchPi = Effect.fn("test.launchPi")(function* (runtimeMode?: RuntimeMode) {
      const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
      const { projectId, dependencies: harnessDependencies } = clientLaunchHarness({
        runtimeModeCeiling: "auto",
        launched,
        providers: [
          {
            instanceId: pi,
            driver: ProviderDriverKind.make("pi"),
            enabled: true,
            installed: true,
            status: "ready",
            version: null,
            auth: { status: "authenticated" },
            checkedAt: "2026-01-01T00:00:00Z",
            models: [{ slug: "pi-model", name: "test", isCustom: false, capabilities: null }],
            slashCommands: [],
            skills: [],
            supportedRuntimeModes: ["approval-required", "auto-accept-edits", "full-access"],
          },
        ],
      });
      const dependencies = Layer.merge(
        harnessDependencies,
        Layer.mock(AdapterRegistry.ProviderAdapterRegistryV2)({
          list: () => Effect.succeed([pi]),
        }),
      );
      const toolkit = yield* ProjectToolkit.pipe(
        Effect.provide(
          McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(
            Layer.provide(dependencies),
          ),
        ),
      );
      const result = yield* toolkit
        .handle("t3_thread_launch", {
          projectId,
          title: "Fix",
          modelSelection: { instanceId: pi, model: "pi-model" },
          ...(runtimeMode === undefined ? {} : { runtimeMode }),
        })
        .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));
      return { result: result.at(-1)?.result, launched };
    });

    const inherited = yield* launchPi();
    expect(inherited.launched[0]?.runtimeMode).toBe("auto-accept-edits");

    const requested = yield* launchPi("auto");
    expect(requested.result).toMatchObject({ code: "invalid_request" });
    expect(requested.launched).toHaveLength(0);

    const offered = yield* launchPi("approval-required");
    expect(offered.launched[0]?.runtimeMode).toBe("approval-required");
  }),
);

it.effect("a launch with a model its provider does not advertise creates no thread", () =>
  Effect.gen(function* () {
    const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
    const providers: ServerProvider[] = ["claudeAgent", "codex"].map((driver) => ({
      instanceId: ProviderInstanceId.make(driver),
      driver: ProviderDriverKind.make(driver),
      enabled: true,
      installed: true,
      status: "ready",
      version: null,
      auth: { status: "authenticated" },
      checkedAt: "2026-01-01T00:00:00Z",
      models: [
        driver === "claudeAgent"
          ? {
              slug: "claude-opus",
              name: "Opus",
              aliases: ["opus"],
              isCustom: false,
              capabilities: null,
            }
          : { slug: "gpt-5.4", name: "GPT", isCustom: false, capabilities: null },
      ],
      slashCommands: [],
      skills: [],
    }));
    const { projectId, dependencies } = clientLaunchHarness({
      runtimeModeCeiling: "auto",
      launched,
      providers,
    });
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(
          Layer.provide(prismDependencies),
          Layer.provide(dependencies),
        ),
      ),
    );
    const launchWith = (instanceId: string, model: string) =>
      toolkit
        .handle("t3_thread_launch", {
          title: "Fix",
          projectId,
          modelSelection: { instanceId: ProviderInstanceId.make(instanceId), model },
        })
        .pipe(
          Stream.unwrap,
          Stream.runCollect,
          Effect.map((result) => result.at(-1)?.result),
          Effect.provide(dependencies),
        );

    for (const [instanceId, model] of [
      ["claudeAgent", "claude-unadvertised"],
      ["codex", "opus"],
    ] as const) {
      expect(yield* launchWith(instanceId, model)).toMatchObject({
        code: "model_unavailable",
        message: `Model ${model} is not advertised by provider ${instanceId}.`,
      });
    }
    expect(launched).toHaveLength(0);

    // Fork: Prism launches an accepted alias as its canonical slug.
    expect(yield* launchWith("claudeAgent", "Opus")).toMatchObject({
      modelSelection: { instanceId: "claudeAgent", model: "claude-opus" },
    });
    expect(launched).toHaveLength(1);
  }),
);

it.effect("an outside full-access client forwards bare and private HTTP MCP configuration", () =>
  Effect.gen(function* () {
    const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
    const { projectId, dependencies } = clientLaunchHarness({
      runtimeModeCeiling: "full-access",
      launched,
    });
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(
          Layer.provide(prismDependencies),
          Layer.provide(dependencies),
        ),
      ),
    );
    const tools = {
      allow: ["mcp:outreach:read_venue", "mcp:outreach:submit_result"],
      mcpServers: {
        outreach: {
          type: "http" as const,
          url: "http://localhost/mcp",
          headers: { Authorization: "test-only-generation-token" },
        },
      },
    };
    yield* toolkit
      .handle("t3_thread_launch", { title: "Restricted venue", projectId, bare: {}, tools })
      .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));
    expect(launched[0]?.bare).toEqual({});
    expect(launched[0]?.tools).toEqual(tools);
    expect(launched[0]?.initialMessage?.text).toBeUndefined();

    expect(launched[0]?.runtimeMode).toBe("full-access");
  }),
);

it.effect("a launch retried with the same clientRequestId replays the first one", () =>
  Effect.gen(function* () {
    const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
    const { projectId, dependencies } = clientLaunchHarness({
      runtimeModeCeiling: "auto",
      launched,
    });
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(Layer.provide(dependencies)),
      ),
    );
    const launch = (clientRequestId: string | undefined) =>
      toolkit
        .handle("t3_thread_launch", {
          title: "Fix",
          projectId,
          message: "Fix the bug",
          ...(clientRequestId === undefined ? {} : { clientRequestId }),
        })
        .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));

    yield* launch("fix-1");
    yield* launch("fix-1");
    yield* launch("fix-2");
    yield* launch(undefined);
    yield* launch(undefined);
    const ids = launched.map((input) => [input.commandId, input.threadId]);
    // The same key sends the same command and thread ids, which the launch
    // service replays instead of starting a second thread.
    expect(ids[0]).toEqual(ids[1]);
    expect(ids[0]?.[0]).toMatch(/^mcp:launch:[0-9a-f]{64}$/);
    expect(ids[2]).not.toEqual(ids[0]);
    // Without a key every launch is new.
    expect(ids[3]).not.toEqual(ids[4]);
  }),
);

it.effect("a launch key of any length or content gives a short id and never a defect", () =>
  Effect.gen(function* () {
    const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
    const { projectId, dependencies } = clientLaunchHarness({
      runtimeModeCeiling: "auto",
      launched,
    });
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(Layer.provide(dependencies)),
      ),
    );
    const launch = (clientRequestId: string) =>
      toolkit
        .handle("t3_thread_launch", { title: "Fix", projectId, clientRequestId })
        .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));

    // Percent-encoding would grow this six times; scratch folders are named from the id.
    yield* launch("\u00e9".repeat(256));
    expect(launched.map((input) => input.commandId)).toEqual([
      expect.stringMatching(/^mcp:launch:[0-9a-f]{64}$/),
    ]);

    const loneSurrogate = yield* launch("key-\ud800").pipe(Effect.exit);
    expect(Exit.isFailure(loneSurrogate) && Cause.hasDies(loneSurrogate.cause)).toBe(false);
    expect(launched).toHaveLength(1);
  }),
);

it.effect("a retried launch replays after the project loses its default model", () =>
  Effect.gen(function* () {
    const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
    const { projectId, modelSelection, projectDefaults, dependencies } = clientLaunchHarness({
      runtimeModeCeiling: "auto",
      launched,
    });
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(Layer.provide(dependencies)),
      ),
    );
    const launch = toolkit
      .handle("t3_thread_launch", { title: "Fix", projectId, clientRequestId: "fix-1" })
      .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));

    yield* launch;
    projectDefaults.model = null;
    const retried = (yield* launch).at(-1)?.result;
    expect(retried).toMatchObject({ projectId, modelSelection });
    expect(launched.map((input) => input.threadId)).toEqual([
      launched[0]?.threadId,
      launched[0]?.threadId,
    ]);
  }),
);

it.effect("a launch binds only an existing checkout that is one of the project's worktrees", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const root = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-launch-worktree-" });
    const repo = path.join(root, "repo");
    const worktree = path.join(root, "feature");
    const outside = path.join(root, "outside");
    yield* fileSystem.makeDirectory(repo);
    yield* fileSystem.makeDirectory(outside);

    const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
    const { projectId, dependencies } = clientLaunchHarness({
      runtimeModeCeiling: "auto",
      launched,
      workspaceRoot: repo,
    });
    const git = yield* GitVcsDriver.GitVcsDriver.pipe(
      Effect.provide(Layer.mergeAll(dependencies, prismDependencies)),
    );
    for (const args of [
      ["init", "-b", "main"],
      ["-c", "user.email=t@t", "-c", "user.name=t", "commit", "--allow-empty", "-m", "init"],
      ["worktree", "add", "-b", "feature", worktree],
    ]) {
      yield* git.execute({ operation: "test.setupRepo", cwd: repo, args });
    }
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(
          Layer.provide(prismDependencies),
          Layer.provide(dependencies),
        ),
      ),
    );
    const launchInto = (worktreePath: string) =>
      toolkit
        .handle("t3_thread_launch", {
          title: "Fix",
          projectId,
          workspaceStrategy: { type: "existing_worktree", worktreePath },
        })
        .pipe(
          Stream.unwrap,
          Stream.runCollect,
          Effect.provide(Layer.mergeAll(dependencies, prismDependencies)),
        );

    expect((yield* launchInto(worktree)).at(-1)?.result).toMatchObject({ projectId });
    expect((yield* launchInto(repo)).at(-1)?.result).toMatchObject({ projectId });
    for (const elsewhere of [outside, path.join(worktree, "..", "outside"), "/"]) {
      expect((yield* launchInto(elsewhere)).at(-1)?.result).toMatchObject({
        code: "invalid_request",
      });
    }
    expect(launched.map((launch) => launch.workspaceStrategy)).toEqual([
      { type: "existing_worktree", worktreePath: worktree },
      { type: "existing_worktree", worktreePath: repo },
    ]);

    yield* fileSystem.remove(worktree, { recursive: true });
    yield* fileSystem.makeDirectory(worktree);
    expect((yield* launchInto(worktree)).at(-1)?.result).toMatchObject({
      code: "invalid_request",
    });
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("a launch into a new worktree creates nothing when its base ref has no commit", () =>
  Effect.gen(function* () {
    const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
    const { projectId, dependencies } = clientLaunchHarness({
      runtimeModeCeiling: "auto",
      launched,
      checkWorktreeBase: ({ projectId, workspaceStrategy }) =>
        workspaceStrategy.type === "worktree" && workspaceStrategy.baseRef === "t3/renamed"
          ? Effect.fail(
              new ThreadLaunch.ThreadLaunchBaseRefError({
                projectId,
                projectTitle: "Client target",
                workspaceRoot: "/projects/client-target",
                baseRef: workspaceStrategy.baseRef,
              }),
            )
          : Effect.void,
    });
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(Layer.provide(dependencies)),
      ),
    );
    const launchFrom = (baseRef: string) =>
      toolkit
        .handle("t3_thread_launch", {
          title: "Fix",
          projectId,
          workspaceStrategy: { type: "worktree", baseRef, startFromOrigin: false },
        })
        .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));

    const refused = (yield* launchFrom("t3/renamed")).at(-1)?.result;
    expect(refused).toMatchObject({ code: "invalid_request" });
    expect((refused as { message: string }).message).toContain(
      'Base ref "t3/renamed" does not resolve to a commit in project "Client target" (/projects/client-target). No thread was created.',
    );
    expect(launched).toHaveLength(0);

    expect((yield* launchFrom("main")).at(-1)?.result).toMatchObject({ projectId });
    expect(launched).toHaveLength(1);
  }),
);

it.effect("a launch into a new worktree creates nothing when its branch collides", () =>
  Effect.gen(function* () {
    const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
    const { projectId, dependencies } = clientLaunchHarness({
      runtimeModeCeiling: "auto",
      launched,
      checkWorktreeBase: ({ projectId, workspaceStrategy }) =>
        Effect.fail(
          new ThreadLaunch.ThreadLaunchBranchConflictError({
            projectId,
            projectTitle: "Client target",
            branch: workspaceStrategy.branch ?? "",
            conflictingBranch: "werkstatt",
          }),
        ),
    });
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(Layer.provide(dependencies)),
      ),
    );
    const refused = (yield* toolkit
      .handle("t3_thread_launch", {
        title: "Fix",
        projectId,
        workspaceStrategy: { type: "worktree", baseRef: "main", branch: "werkstatt/baseline" },
      })
      .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies))).at(-1)?.result;

    expect(refused).toMatchObject({
      code: "invalid_request",
      message:
        'Branch "werkstatt/baseline" cannot be created in project "Client target": branch "werkstatt" exists. No thread was created. Use a name that does not nest under or contain an existing branch, such as "werkstatt-baseline".',
    });
    expect(launched).toHaveLength(0);
  }),
);

it.effect("a launch into an unknown project says the project was not found", () =>
  Effect.gen(function* () {
    const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
    const { modelSelection, dependencies } = clientLaunchHarness({
      runtimeModeCeiling: "auto",
      launched,
    });
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(Layer.provide(dependencies)),
      ),
    );
    for (const workspaceStrategy of [
      undefined,
      { type: "worktree", baseRef: "main", startFromOrigin: false } as const,
    ]) {
      const result = yield* toolkit
        .handle("t3_thread_launch", {
          title: "Fix",
          message: "Fix the bug",
          projectId: ProjectId.make("current"),
          modelSelection,
          ...(workspaceStrategy === undefined ? {} : { workspaceStrategy }),
        })
        .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));
      expect(result.at(-1)?.result).toMatchObject({
        code: "invalid_request",
        message: "The project was not found.",
      });
    }
    expect(launched).toHaveLength(0);
  }),
);

it.effect("a retried worktree launch replays after its base ref is gone", () =>
  Effect.gen(function* () {
    const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
    const base = { exists: true };
    const { projectId, dependencies } = clientLaunchHarness({
      runtimeModeCeiling: "auto",
      launched,
      checkWorktreeBase: ({ projectId, workspaceStrategy }) =>
        base.exists || workspaceStrategy.type !== "worktree"
          ? Effect.void
          : Effect.fail(
              new ThreadLaunch.ThreadLaunchBaseRefError({
                projectId,
                projectTitle: "Client target",
                workspaceRoot: "/projects/client-target",
                baseRef: workspaceStrategy.baseRef,
              }),
            ),
    });
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(Layer.provide(dependencies)),
      ),
    );
    const launch = toolkit
      .handle("t3_thread_launch", {
        title: "Fix",
        projectId,
        clientRequestId: "fix-worktree",
        workspaceStrategy: { type: "worktree", baseRef: "feature", startFromOrigin: false },
      })
      .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));

    yield* launch;
    base.exists = false;
    expect((yield* launch).at(-1)?.result).toMatchObject({ projectId });
    expect(launched.map((input) => input.threadId)).toEqual([
      launched[0]?.threadId,
      launched[0]?.threadId,
    ]);
  }),
);

it.effect("a client launches with the fallback for an exhausted default", () =>
  Effect.gen(function* () {
    const launched: ThreadLaunch.ThreadLaunchInput[] = [];
    const fallbackModel = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-6.1-sol" };
    const providers: ServerProvider[] = ["claudeAgent", "codex"].map((driver) => ({
      instanceId: ProviderInstanceId.make(driver),
      driver: ProviderDriverKind.make(driver),
      enabled: true,
      installed: true,
      status: "ready",
      version: null,
      auth: { status: "authenticated" },
      checkedAt: "2026-01-01T00:00:00Z",
      models: [
        {
          slug: driver === "codex" ? fallbackModel.model : "claude-opus",
          name: "test",
          isCustom: false,
          capabilities: null,
        },
      ],
      slashCommands: [],
      skills: [],
      ...(driver === "claudeAgent"
        ? {
            usageLimits: {
              checkedAt: "2026-01-01T00:00:00Z",
              windows: [{ id: "session", kind: "session", label: "Session", usedPercent: 100 }],
            },
          }
        : {}),
    }));
    const { projectId, dependencies } = clientLaunchHarness({
      runtimeModeCeiling: "full-access",
      launched,
      fallbackModel,
      providers,
    });
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(Layer.provide(dependencies)),
      ),
    );
    const result = yield* toolkit
      .handle("t3_thread_launch", {
        projectId,
        title: "Fallback",
      })
      .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));
    expect(result.at(-1)?.result).toMatchObject({
      modelSelection: fallbackModel,
    });
    expect(launched[0]?.modelSelection).toEqual(fallbackModel);
  }),
);

it.effect("an agent's clone is not cut off while git is still transferring", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const parent = yield* fs.makeTempDirectoryScoped({ prefix: "t3-mcp-clone-" });
    const destinationPath = path.join(parent, "repo");
    const started = yield* Deferred.make<void>();
    const transferred = yield* Deferred.make<void>();
    const { dependencies } = clientLaunchHarness({
      runtimeModeCeiling: "full-access",
      launched: [],
    });
    const repositories = Repositories.layer.pipe(
      Layer.provide(
        Layer.mock(SourceControlProviderRegistry.SourceControlProviderRegistry)({
          resolveLink: () => undefined,
        }),
      ),
      Layer.provide(
        Layer.mock(GitVcsDriver.GitVcsDriver)({
          // Like the real driver, the clone fails once it outlives the deadline it was given.
          execute: (input) =>
            Deferred.succeed(started, undefined).pipe(
              Effect.andThen(Deferred.await(transferred)),
              Effect.as({
                exitCode: ChildProcessSpawner.ExitCode(0),
                stdout: "",
                stderr: "",
                stdoutTruncated: false,
                stderrTruncated: false,
              }),
              Effect.timeoutOrElse({
                duration: input.timeoutMs ?? Infinity,
                orElse: () =>
                  Effect.fail(
                    new GitCommandError({
                      operation: input.operation,
                      command: "git",
                      cwd: input.cwd,
                      detail: "Git command timed out.",
                    }),
                  ),
              }),
            ),
        }),
      ),
      Layer.provideMerge(dependencies),
    );
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(Layer.provide(repositories)),
      ),
    );

    const clone = yield* toolkit
      .handle("t3_project_clone", { remoteUrl: "https://example.test/repo.git", destinationPath })
      .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(repositories), Effect.forkChild);
    yield* Deferred.await(started);
    // A large repository over a slow link; agents' MCP calls get a far longer budget.
    yield* TestClock.adjust("10 minutes");
    yield* Deferred.succeed(transferred, undefined);

    expect((yield* Fiber.join(clone)).at(-1)?.result).toMatchObject({ cwd: destinationPath });
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("deleting a project through MCP also discards its clone record", () =>
  Effect.gen(function* () {
    const discarded: Array<ProjectId> = [];
    const harness = clientLaunchHarness({ runtimeModeCeiling: "full-access", launched: [] });
    const dependencies = Layer.merge(
      harness.dependencies,
      Layer.mergeAll(
        Layer.mock(Project.ProjectService)({
          delete: (input) =>
            Effect.succeed({
              id: input.projectId,
              title: "client-target",
              workspaceRoot: "/projects/client-target",
              repositoryIdentity: null,
              faviconPath: null,
              defaultModelSelection: null,
              defaultThreadEnvMode: null,
              autoPull: false,
              projectIcon: null,
              scripts: [],
              createdAt: "2026-01-01T00:00:00.000Z",
              updatedAt: "2026-01-01T00:00:00.000Z",
              deletedAt: "2026-01-01T00:00:00.000Z",
            }),
        }),
        Layer.mock(ProjectCloneTracker.ProjectCloneTracker)({
          discard: (projectId) => Effect.sync(() => void discarded.push(projectId)),
        }),
      ),
    );
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(Layer.provide(dependencies)),
      ),
    );

    yield* toolkit
      .handle("t3_project_delete", { projectId: harness.projectId })
      .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));

    expect(discarded).toEqual([harness.projectId]);
  }),
);

it.effect("an update the project refuses tells the agent why", () =>
  Effect.gen(function* () {
    const harness = clientLaunchHarness({ runtimeModeCeiling: "full-access", launched: [] });
    const detail = "Project monograms must contain at most two characters.";
    const dependencies = Layer.merge(
      harness.dependencies,
      Layer.mock(Project.ProjectService)({
        update: (input) =>
          Effect.fail(new Project.ProjectInvalidError({ projectId: input.projectId, detail })),
      }),
    );
    const toolkit = yield* ProjectToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(Layer.provide(dependencies)),
      ),
    );

    const refused = yield* toolkit
      .handle("t3_project_update", {
        projectId: harness.projectId,
        projectIcon: { kind: "monogram", text: "ABC", color: "blue" },
      })
      .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));

    expect(refused.at(-1)?.result).toMatchObject({ code: "invalid_request", message: detail });
  }),
);

it.effect(
  "launch falls through an advertised provider without an adapter, but explicit selection refuses",
  () =>
    Effect.gen(function* () {
      const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
      const { projectId, dependencies } = clientLaunchHarness({
        runtimeModeCeiling: "auto-accept-edits",
        launched,
      });
      const codex = ProviderInstanceId.make("codex");
      const claude = ProviderInstanceId.make("claude");
      const validated: string[] = [];
      const routing = Layer.mergeAll(
        prismDependencies,
        ServerSettings.layerTest({
          prismRoles: {
            planner: {
              models: [
                { instanceId: codex, model: "gpt-5" },
                { instanceId: claude, model: "claude-opus", effort: "high" },
              ],
            },
          },
        }),
        Layer.mock(AdapterRegistry.ProviderAdapterRegistryV2)({
          list: () =>
            Effect.sync(() => {
              validated.push("lookup");
              return [claude];
            }),
        }),
      );
      const toolkit = yield* ProjectToolkit.pipe(
        Effect.provide(
          McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(
            Layer.provide(routing),
            Layer.provide(dependencies),
          ),
        ),
      );
      const handle = (params: Parameters<typeof toolkit.handle<"t3_thread_launch">>[1]) =>
        toolkit
          .handle("t3_thread_launch", params)
          .pipe(
            Stream.unwrap,
            Stream.runCollect,
            Effect.provide(Layer.mergeAll(dependencies, routing)),
          );
      const result = yield* handle({ title: "Fix", projectId, message: "Fix the bug" });
      expect(result.at(-1)?.result).toMatchObject({
        projectId,
        modelSelection: {
          instanceId: claude,
          model: "claude-opus",
          options: [{ id: "effort", value: "high" }],
        },
      });
      expect(launched).toHaveLength(1);
      expect(launched[0]?.runtimeMode).toBe("auto-accept-edits");
      expect(validated.length).toBeGreaterThan(0);
      const refused = yield* handle({
        title: "Explicit",
        projectId,
        modelSelection: { instanceId: codex, model: "gpt-5" },
      });
      expect(refused.at(-1)?.result).toMatchObject({ code: "provider_unavailable" });
      expect(launched).toHaveLength(1);
    }),
);

it.effect.each(["unavailable", "missing-model", "recovered"] as const)(
  "launch rechecks a registered stale provider before selection: %s",
  (outcome) =>
    Effect.gen(function* () {
      const launched: Array<ThreadLaunch.ThreadLaunchInput> = [];
      const { projectId, dependencies } = clientLaunchHarness({
        runtimeModeCeiling: "auto-accept-edits",
        launched,
      });
      const codex = ProviderInstanceId.make("codex");
      const claude = ProviderInstanceId.make("claude");
      const providers = yield* ProviderRegistry.ProviderRegistry.pipe(
        Effect.flatMap((registry) => registry.getProviders),
        Effect.provide(prismDependencies),
      );
      const stale = providers.map((provider) =>
        provider.instanceId === codex
          ? { ...provider, availability: "unavailable" as const, models: [] }
          : provider,
      );
      const refreshed = providers.map((provider) =>
        provider.instanceId === codex
          ? outcome === "unavailable"
            ? stale[0]!
            : outcome === "missing-model"
              ? { ...provider, models: [{ ...provider.models[0]!, slug: "different-model" }] }
              : provider
          : provider,
      );
      const probes: string[] = [];
      const routing = Layer.mergeAll(
        prismDependencies,
        ServerSettings.layerTest({
          prismRoles: {
            planner: {
              models: [
                { instanceId: codex, model: "gpt-5" },
                { instanceId: claude, model: "claude-opus", effort: "high" },
              ],
            },
          },
        }),
        Layer.mock(ProviderRegistry.ProviderRegistry)({
          getProviders: Effect.succeed(stale),
          refreshInstance: (instanceId) =>
            Effect.sync(() => {
              probes.push(instanceId);
              return refreshed;
            }),
        }),
      );
      const toolkit = yield* ProjectToolkit.pipe(
        Effect.provide(
          McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(
            Layer.provide(routing),
            Layer.provide(dependencies),
          ),
        ),
      );
      const handle = (params: Parameters<typeof toolkit.handle<"t3_thread_launch">>[1]) =>
        toolkit
          .handle("t3_thread_launch", params)
          .pipe(
            Stream.unwrap,
            Stream.runCollect,
            Effect.provide(Layer.mergeAll(dependencies, routing)),
          );
      const result = yield* handle({ title: "Fix", projectId, message: "Fix the bug" });
      expect(probes).toEqual([codex]);
      expect(result.at(-1)?.result).toMatchObject({
        modelSelection:
          outcome === "recovered"
            ? { instanceId: codex, model: "gpt-5" }
            : {
                instanceId: claude,
                model: "claude-opus",
                options: [{ id: "effort", value: "high" }],
              },
      });
      expect(launched).toHaveLength(1);
      expect(launched[0]?.runtimeMode).toBe("auto-accept-edits");
      if (outcome !== "recovered") {
        const refused = yield* handle({
          title: "Explicit",
          projectId,
          modelSelection: { instanceId: codex, model: "gpt-5" },
        });
        expect(refused.at(-1)?.result).toMatchObject({
          code: outcome === "unavailable" ? "provider_unavailable" : "model_unavailable",
        });
        expect(launched).toHaveLength(1);
        expect(probes).toEqual([codex, codex]);
      }
    }),
);

it.effect(
  "reports a rejected bare addition with its reason instead of a generic orchestration error",
  () =>
    Effect.gen(function* () {
      const { projectId, dependencies } = clientLaunchHarness({
        runtimeModeCeiling: "full-access",
        launched: [],
      });
      const toolkit = yield* ProjectToolkit.pipe(
        Effect.provide(
          McpToolAccess.HandlersLayer.layer(ProjectHandlers.layer).pipe(
            Layer.provide(prismDependencies),
            Layer.provide(dependencies),
          ),
        ),
      );
      const result = yield* toolkit
        .handle("t3_thread_launch", {
          title: "Invalid bare",
          projectId,
          bare: { additions: [{ type: "mcp", server: "other" }] },
        })
        .pipe(
          Stream.unwrap,
          Stream.runCollect,
          Effect.provide(
            Layer.mergeAll(
              dependencies,
              Layer.mock(ThreadLaunch.ThreadLaunchService)({
                replayOrLaunch: (_input, launch) => launch,
                launch: () =>
                  Effect.fail(
                    new ThreadLaunch.ThreadLaunchError({
                      operation: "provision-worktree",
                      commandId: CommandId.make("invalid-bare"),
                      projectId,
                      cause: new BareLaunchError({
                        reason: "Bare addition MCP other is not supported.",
                      }),
                    }),
                  ),
              }),
            ),
          ),
        );
      expect(result[0]?.encodedResult).toMatchObject({
        code: "invalid_request",
        message: "Bare addition MCP other is not supported.",
      });
    }),
);
