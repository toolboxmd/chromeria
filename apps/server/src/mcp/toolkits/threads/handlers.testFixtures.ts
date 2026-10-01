// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import {
  CommandId,
  EnvironmentId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ProviderDriverKind,
  ThreadId,
  TurnId,
  type OrchestrationEvent,
  type RuntimeMode,
  type ServerProvider,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import type { Tool } from "effect/unstable/ai";

import { ProviderCommandReactorLive } from "../../../orchestration/Layers/ProviderCommandReactor.ts";
import { ProviderAuthService } from "../../../provider/Services/ProviderAuthService.ts";
import { TerminalManager } from "../../../terminal/Manager.ts";
import { TextGeneration } from "../../../textGeneration/TextGeneration.ts";
import { VcsStatusBroadcaster } from "../../../vcs/VcsStatusBroadcaster.ts";
import type { ProviderRegistry } from "../../../provider/Services/ProviderRegistry.ts";
import { makeProviderRegistryLayer } from "../../../provider/testUtils/providerRegistryMock.ts";
import { ProjectSetupScriptRunner } from "../../../project/ProjectSetupScriptRunner.ts";
import { GitWorkflowService } from "../../../git/GitWorkflowService.ts";
import { ServerConfig } from "../../../config.ts";
import { OrchestrationEngineLive } from "../../../orchestration/Layers/OrchestrationEngine.ts";
import { OrchestrationProjectionPipelineLive } from "../../../orchestration/Layers/ProjectionPipeline.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "../../../orchestration/Layers/ProjectionSnapshotQuery.ts";
import { OrchestrationEngineService } from "../../../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ThreadBackgroundLiveness from "../../../orchestration/ThreadBackgroundLiveness.ts";
import * as ThreadPlanProgress from "../../../orchestration/ThreadPlanProgress.ts";
import { OrchestrationCommandReceiptRepositoryLive } from "../../../persistence/Layers/OrchestrationCommandReceipts.ts";
import { OrchestrationEventStoreLive } from "../../../persistence/Layers/OrchestrationEventStore.ts";
import { makeSqlitePersistenceLive } from "../../../persistence/Layers/Sqlite.ts";
import * as RepositoryIdentityResolver from "../../../project/RepositoryIdentityResolver.ts";
import * as ProviderService from "../../../provider/Services/ProviderService.ts";
import { ServerSettingsService } from "../../../serverSettings.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { ThreadsToolkitHandlersLive } from "./handlers.ts";
import { ThreadsToolkit } from "./tools.ts";

/**
 * A threads toolkit over the real orchestration engine and projections on a
 * SQLite file, with provider services faked. Starting a second server on the
 * same file simulates a restart.
 */

const PROJECT_ID = ProjectId.make("project-threads");
export const PARENT_ID = ThreadId.make("parent-threads");
export const NOW = "2026-01-01T00:00:00.000Z";

let commandCount = 0;
export const commandId = () => CommandId.make(`test-threads-${++commandCount}`);

/** One server process: real engine and projections over a SQLite file, toolkit services faked. */
export interface SpawnTestOptions {
  readonly services?: Layer.Layer<ServerSettingsService | ProviderRegistry>;
  readonly settings?: Parameters<typeof ServerSettingsService.layerTest>[0];
  readonly git?: Partial<GitWorkflowService["Service"]>;
  readonly setup?: ProjectSetupScriptRunner["Service"]["runForThread"];
  readonly provider?: Partial<ProviderService.ProviderService["Service"]>;
  readonly providers?: ReadonlyArray<ServerProvider>;
}

const engineLayer = (databasePath: string, options: SpawnTestOptions = {}) => {
  const orchestration = Layer.mergeAll(
    OrchestrationEngineLive.pipe(
      Layer.provide(OrchestrationProjectionSnapshotQueryLive),
      Layer.provide(OrchestrationProjectionPipelineLive),
    ),
    OrchestrationProjectionSnapshotQueryLive,
  ).pipe(
    Layer.provideMerge(ThreadBackgroundLiveness.layer),
    Layer.provide(ThreadPlanProgress.layer),
    Layer.provide(OrchestrationEventStoreLive),
    Layer.provideMerge(OrchestrationCommandReceiptRepositoryLive),
    Layer.provide(RepositoryIdentityResolver.layer),
    Layer.provide(makeSqlitePersistenceLive(databasePath)),
    Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "t3-threads-" })),
    Layer.provideMerge(NodeServices.layer),
  );
  return Layer.mergeAll(
    orchestration,
    ServerSettingsService.layerTest(options.settings),
    Layer.mock(GitWorkflowService)(options.git ?? {}),
    Layer.mock(ProjectSetupScriptRunner)({
      runForThread: options.setup ?? (() => Effect.succeed({ status: "no-script" })),
    }),
    Layer.mock(ProviderService.ProviderService)({
      getInstanceInfo: (instanceId) =>
        Effect.succeed({
          instanceId,
          driverKind: ProviderDriverKind.make("codex"),
          displayName: undefined,
          enabled: true,
          continuationIdentity: {
            driverKind: ProviderDriverKind.make("codex"),
            continuationKey: "test-codex",
          },
        }),
      ...options.provider,
    }),
    makeProviderRegistryLayer(options.providers),
  );
};

/**
 * Runs after the toolkit reads one thread's shell, before the read returns:
 * the place to make an event land between a read and what follows it.
 */
type AfterShellRead = (threadId: string) => Effect.Effect<void, never, OrchestrationEngineService>;

/** The same process with the threads toolkit running. */
type BeforeToolkitDispatch = (
  command: Parameters<OrchestrationEngineService["Service"]["dispatch"]>[0],
) => Effect.Effect<void>;
const serverLayer = (
  databasePath: string,
  afterShellRead?: AfterShellRead,
  options?: SpawnTestOptions,
  beforeDispatch?: BeforeToolkitDispatch,
  afterDispatch?: BeforeToolkitDispatch,
) => {
  const dependencies = options?.services
    ? Layer.mergeAll(engineLayer(databasePath, options), options.services)
    : engineLayer(databasePath, options);
  const toolkitQuery = Layer.effect(
    ProjectionSnapshotQuery,
    Effect.gen(function* () {
      const query = yield* ProjectionSnapshotQuery;
      const engine = yield* OrchestrationEngineService;
      if (!afterShellRead) return query;
      return ProjectionSnapshotQuery.of({
        ...query,
        getThreadShellById: (threadId) =>
          query
            .getThreadShellById(threadId)
            .pipe(
              Effect.tap(() =>
                afterShellRead(threadId).pipe(
                  Effect.provideService(OrchestrationEngineService, engine),
                ),
              ),
            ),
      });
    }),
  );
  const toolkit =
    beforeDispatch || afterDispatch
      ? Layer.unwrap(
          Effect.gen(function* () {
            const engine = yield* OrchestrationEngineService;
            const wrapped = OrchestrationEngineService.of({
              ...engine,
              dispatch: (command, options) =>
                (beforeDispatch?.(command) ?? Effect.void).pipe(
                  Effect.andThen(engine.dispatch(command, options)),
                  Effect.ensuring(Effect.suspend(() => afterDispatch?.(command) ?? Effect.void)),
                ),
            });
            return ThreadsToolkitHandlersLive.pipe(
              Layer.provide(toolkitQuery),
              Layer.provide(Layer.succeed(OrchestrationEngineService, wrapped)),
            );
          }),
        ).pipe(Layer.provide(dependencies))
      : ThreadsToolkitHandlersLive.pipe(Layer.provide(toolkitQuery), Layer.provide(dependencies));
  return Layer.mergeAll(
    toolkit,
    ...(options?.provider
      ? [
          ProviderCommandReactorLive.pipe(
            Layer.provide(dependencies),
            Layer.provide(
              Layer.mergeAll(
                Layer.mock(ProviderAuthService)({
                  tryHandlePromptCommand: () => Effect.succeed(false),
                }),
                Layer.mock(TerminalManager)({ closeIdle: () => Effect.void }),
                Layer.mock(TextGeneration)({
                  generateBranchName: () => Effect.succeed({ branch: "test-spawn" }),
                  generateThreadTitle: () => Effect.succeed({ title: "Target task" }),
                }),
                Layer.mock(VcsStatusBroadcaster)({}),
              ),
            ),
          ),
        ]
      : []),
    dependencies,
  );
};

/** Runs `body` against a fresh server on `databasePath`; the server stops when it ends. */
export const withServer = <A, E, R>(
  databasePath: string,
  body: Effect.Effect<A, E, R>,
  afterShellRead?: AfterShellRead,
  options?: SpawnTestOptions,
  beforeDispatch?: BeforeToolkitDispatch,
  afterDispatch?: BeforeToolkitDispatch,
) =>
  body.pipe(
    Effect.provide(
      serverLayer(databasePath, afterShellRead, options, beforeDispatch, afterDispatch),
    ),
  );

/**
 * Runs `body` against the engine alone, with no threads toolkit listening:
 * events it dispatches are the ones a stopping server never bridged.
 */
export const withEngineOnly = <A, E, R>(databasePath: string, body: Effect.Effect<A, E, R>) =>
  body.pipe(Effect.provide(engineLayer(databasePath)));

/** A temporary directory removed when the surrounding scope closes. */
export const temporaryDirectory = (prefix: string) =>
  Effect.gen(function* () {
    const directory = yield* Effect.promise(() =>
      NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), prefix)),
    );
    yield* Effect.addFinalizer(() =>
      Effect.promise(() => NodeFSP.rm(directory, { recursive: true, force: true })),
    );
    return directory;
  });

type ThreadTools = typeof ThreadsToolkit.tools;

/** Calls one thread tool as `callerId` and returns its result. */
export const callTool = <Name extends keyof ThreadTools>(
  name: Name,
  params: Tool.Parameters<ThreadTools[Name]>,
  callerId: string = PARENT_ID,
) =>
  Effect.gen(function* () {
    const toolkit = yield* ThreadsToolkit;
    return yield* toolkit.handle(name, params as never).pipe(
      Stream.unwrap,
      Stream.runCollect,
      Effect.map((chunk) => chunk.at(-1)!.result as Tool.Success<ThreadTools[Name]>),
      Effect.provideService(McpInvocationContext.McpInvocationContext, {
        environmentId: EnvironmentId.make("environment-threads"),
        threadId: ThreadId.make(callerId),
        providerSessionId: "provider-session-threads",
        providerInstanceId: ProviderInstanceId.make("codex"),
        capabilities: new Set<McpInvocationContext.McpCapability>(),
        issuedAt: 1,
      }),
    );
  });

/** Runs `act`, then waits for the first domain event matching `until`. */
export const dispatchUntil = <A, E, R>(
  act: Effect.Effect<A, E, R>,
  until: (event: OrchestrationEvent) => boolean,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const engine = yield* OrchestrationEngineService;
      const events = yield* engine.subscribeDomainEvents;
      const result = yield* act;
      const event = yield* events.pipe(
        Stream.filter(until),
        Stream.runHead,
        Effect.map(Option.getOrThrow),
      );
      return { result, event };
    }),
  );

export const dispatchAll = (
  commands: ReadonlyArray<Parameters<OrchestrationEngineService["Service"]["dispatch"]>[0]>,
) =>
  Effect.gen(function* () {
    const engine = yield* OrchestrationEngineService;
    for (const command of commands) yield* engine.dispatch(command);
  });

/** Creates the project and the parent (planner) thread every test spawns from. */
export const createParent = (workspaceRoot: string, runtimeMode: RuntimeMode = "full-access") =>
  dispatchAll([
    {
      type: "project.create",
      commandId: commandId(),
      projectId: PROJECT_ID,
      title: "Threads",
      workspaceRoot,
      createdAt: NOW,
    },
    {
      type: "thread.create",
      commandId: commandId(),
      threadId: PARENT_ID,
      projectId: PROJECT_ID,
      title: "Parent",
      modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5" },
      runtimeMode,
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdAt: NOW,
    },
  ]);

/** A parent activity row of `kind` (and `status`) for `childId`. */
export const parentActivity =
  (childId: string, kind: string, status?: string) => (event: OrchestrationEvent) =>
    event.type === "thread.activity-appended" &&
    event.aggregateId === PARENT_ID &&
    event.payload.activity.kind === kind &&
    (event.payload.activity.payload as { taskId?: string }).taskId === childId &&
    (status === undefined ||
      (event.payload.activity.payload as { status?: string }).status === status);

export const taskStarted = (event: OrchestrationEvent) =>
  event.type === "thread.activity-appended" &&
  event.aggregateId === PARENT_ID &&
  event.payload.activity.kind === "task.started";

export const parentMessages = Effect.gen(function* () {
  const snapshots = yield* ProjectionSnapshotQuery;
  return Option.getOrThrow(yield* snapshots.getThreadDetailById(PARENT_ID)).messages;
});

export const session = (
  threadId: ThreadId,
  status: "starting" | "running" | "ready" | "interrupted" | "error",
  turnId: string | null,
  lastError: string | null = null,
) =>
  ({
    type: "thread.session.set",
    commandId: commandId(),
    threadId,
    session: {
      threadId,
      status,
      providerName: "codex",
      providerInstanceId: ProviderInstanceId.make("codex"),
      runtimeMode: "full-access",
      activeTurnId: turnId === null ? null : TurnId.make(turnId),
      lastError,
      updatedAt: NOW,
    },
    createdAt: NOW,
  }) as const;

export const assistantReply = (threadId: ThreadId, messageId: string, text: string) =>
  [
    {
      type: "thread.message.assistant.delta",
      commandId: commandId(),
      threadId,
      messageId: MessageId.make(messageId),
      delta: text,
      createdAt: NOW,
    },
    {
      type: "thread.message.assistant.complete",
      commandId: commandId(),
      threadId,
      messageId: MessageId.make(messageId),
      createdAt: NOW,
    },
  ] as const;
