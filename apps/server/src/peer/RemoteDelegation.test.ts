import { subagentResultForRun } from "@t3tools/provider-core/server/subagentProjection";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import * as Schema from "effect/Schema";
import {
  PeerTaskLifecycleRequest,
  OrchestrationV2RemoteTaskLaunch,
  OrchestrationV2Command,
  type PeerTaskLifecycleResult,
} from "@t3tools/contracts";
import * as BareLaunch from "../orchestration-v2/BareLaunch.ts";
import * as CommandReceiptStore from "../orchestration-v2/CommandReceiptStore.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as WorktreeSetupTracker from "../project/WorktreeSetupTracker.ts";
import * as ProjectCloneTracker from "../project/ProjectCloneTracker.ts";
import * as ProjectSetupScriptRunner from "../project/ProjectSetupScriptRunner.ts";
import * as TerminalManager from "../terminal/Manager.ts";
import * as GitWorkflow from "../git/GitWorkflowService.ts";
import * as TextGeneration from "../textGeneration/TextGeneration.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import { layerLaunchCatalog } from "./PeerLinks.testkit.ts";
import * as Settings from "../serverSettings.ts";
import { NodeHttpServer } from "@effect/platform-node";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  type OrchestrationV2Run,
  type OrchestrationV2Subagent,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  type Project,
  type RepositoryIdentity,
  type ExecutionPurpose,
  LEGACY_EXECUTION_PURPOSE,
  OrchestratorMcpThreadSendResult,
  RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as SqlClient from "effect/sql/SqlClient";
import { McpSchema, McpServer } from "effect/ai";

import * as ServerConfig from "../config.ts";
import * as McpHttpServer from "../mcp/McpHttpServer.ts";
import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import { idleThreadProjection, liveThreadShell } from "../mcp/McpToolAccess.testkit.ts";
import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderAdapterRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as ThreadLaunch from "../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as ProviderReplayHarness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ManagedProjectFolders from "../project/ManagedProjectFolders.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import { restartCutView } from "@t3tools/shared/restartCut";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as SourceControlRepositoryService from "../sourceControl/SourceControlRepositoryService.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as PeerForwarding from "./PeerForwarding.ts";
import * as PeerLinks from "./PeerLinks.ts";
import * as PeerMcpClient from "./PeerMcpClient.ts";
import { descriptorOf, layerLinkingEnvironment, linkTo, servePeer } from "./PeerLinks.testkit.ts";
import * as RemoteDelegation from "./RemoteDelegation.ts";

// The laptop's agent delegates a task to the box through a link. The box is
// its real /mcp behind real OAuth, with one thread the test finishes; the
// laptop is its real orchestrator and toolkits. The parent here wakes as it
// would for a local child.

const decodeRemoteTaskLaunch = Schema.decodeUnknownEffect(OrchestrationV2RemoteTaskLaunch);
const decodeCommand = Schema.decodeUnknownEffect(OrchestrationV2Command);

const laptop = descriptorOf("environment-laptop", "Laptop");
const box = descriptorOf("environment-box", "Box");
const boxProject = ProjectId.make("project:box");
const instanceId = ProviderInstanceId.make("codex");
const driver = ProviderDriverKind.make("codex");
const modelSelection = { instanceId, model: "gpt-5.4" };

type LaunchedWith = Pick<
  ThreadLaunch.ThreadLaunchInput,
  "modelSelection" | "runtimeMode" | "interactionMode" | "workspaceStrategy"
>;

/** The thread the box launches for the task, which the test finishes. */
interface BoxThread {
  readonly successor: Ref.Ref<boolean>;
  readonly launched: Ref.Ref<ThreadId | null>;
  readonly launchIds: Ref.Ref<ReadonlyArray<ThreadId>>;
  readonly holdLaunch: Ref.Ref<boolean>;
  readonly launchAccepted: Deferred.Deferred<void>;
  readonly releaseLaunch: Deferred.Deferred<void>;
  /** The link session that launched it, as the box stamps it. */
  readonly linkOrigin: Ref.Ref<{ readonly sessionId: string; readonly label: string } | null>;
  /** The parent the launch names, so the thread there can link back to it. */
  readonly delegatedFrom: Ref.Ref<ThreadLaunch.ThreadLaunchInput["delegatedFrom"] | null>;
  /** The model and modes it was launched with. */
  readonly launchedWith: Ref.Ref<LaunchedWith | null>;
  /** The first message the thread there was launched with. */
  readonly message: Ref.Ref<string | null>;
  readonly status: Ref.Ref<OrchestrationV2Run["status"]>;
  readonly reply: Ref.Ref<string>;
  /** A later run's reply, after the task's own, as a follow-up message would leave. */
  readonly laterReply: Ref.Ref<string | null>;
  /** The run each wait asked for, in order. */
  readonly waitedFor: Ref.Ref<ReadonlyArray<RunId | undefined>>;
  /** Waiting there fails with this, as a thread deleted there does. */
  readonly gone: Ref.Ref<boolean>;
  readonly interrupted: Ref.Ref<number>;
  /** What each interrupt or stop there did, once per command id. */
  readonly stops: Ref.Ref<ReadonlyArray<"interrupt" | "thread.stop" | "delegated-tasks">>;
  /** The launch starts no run, as one recorded before its run was kept. */
  readonly runless: Ref.Ref<boolean>;
  /** Completed on the first interrupt there. */
  readonly interruptedOnce: Deferred.Deferred<void>;
  readonly finished: Deferred.Deferred<void>;
  /** While set, reading the thread there waits: completed once a read starts, released by `releaseRead`. */
  readonly holdRead: Ref.Ref<boolean>;
  readonly readStarted: Deferred.Deferred<void>;
  readonly releaseRead: Deferred.Deferred<void>;
}

const makeBoxThread = Effect.gen(function* () {
  return {
    successor: yield* Ref.make(false),
    launched: yield* Ref.make<ThreadId | null>(null),
    launchIds: yield* Ref.make<ReadonlyArray<ThreadId>>([]),
    holdLaunch: yield* Ref.make(false),
    launchAccepted: yield* Deferred.make<void>(),
    releaseLaunch: yield* Deferred.make<void>(),
    linkOrigin: yield* Ref.make<{ readonly sessionId: string; readonly label: string } | null>(
      null,
    ),
    delegatedFrom: yield* Ref.make<ThreadLaunch.ThreadLaunchInput["delegatedFrom"] | null>(null),
    launchedWith: yield* Ref.make<LaunchedWith | null>(null),
    message: yield* Ref.make<string | null>(null),
    status: yield* Ref.make<OrchestrationV2Run["status"]>("running"),
    reply: yield* Ref.make("Working…"),
    laterReply: yield* Ref.make<string | null>(null),
    waitedFor: yield* Ref.make<ReadonlyArray<RunId | undefined>>([]),
    gone: yield* Ref.make(false),
    interrupted: yield* Ref.make(0),
    stops: yield* Ref.make<ReadonlyArray<"interrupt" | "thread.stop" | "delegated-tasks">>([]),
    runless: yield* Ref.make(false),
    interruptedOnce: yield* Deferred.make<void>(),
    finished: yield* Deferred.make<void>(),
    holdRead: yield* Ref.make(false),
    readStarted: yield* Deferred.make<void>(),
    releaseRead: yield* Deferred.make<void>(),
  } satisfies BoxThread;
});

/** The box's projects: more than a page, with this repository's last. */
const boxProjects: ReadonlyArray<Project> = [
  ...Array.from({ length: 120 }, (_, index) => ({
    id: ProjectId.make(`project:box-other-${index}`),
    title: `other ${index}`,
    workspaceRoot: `/srv/other-${index}`,
    repositoryIdentity: null,
    defaultModelSelection: null,
    scripts: [],
    createdAt: "2026-10-01T00:00:00.000Z",
    updatedAt: "2026-10-01T00:00:00.000Z",
    deletedAt: null,
  })),
  {
    id: boxProject,
    title: "app",
    workspaceRoot: "/srv/app",
    repositoryIdentity: {
      canonicalKey: "github.com/acme/app",
      locator: {
        source: "git-remote",
        remoteName: "origin",
        remoteUrl: "git@github.com:acme/app.git",
      },
    },
    defaultModelSelection: null,
    scripts: [],
    createdAt: "2026-10-01T00:00:00.000Z",
    updatedAt: "2026-10-01T00:00:00.000Z",
    deletedAt: null,
  },
];

const serveBox = (thread: BoxThread, lifecycleSupported: boolean | Ref.Ref<boolean> = true) => {
  return servePeer(box, boxToolkitLayer(thread, undefined, lifecycleSupported));
};

const boxRunId = RunId.make("run:box");
const boxLaterRunId = RunId.make("run:box-later");

const boxToolkitLayer = (
  thread: BoxThread,
  actual?: {
    readonly launches: ThreadLaunch.ThreadLaunchService["Service"];
    readonly threads: ThreadManagement.ThreadManagementService["Service"];
    readonly catalogLayer: Layer.Layer<
      ProviderRegistry.ProviderRegistry | ProviderAdapterRegistry.ProviderAdapterRegistryV2
    >;
  },
  lifecycleSupported: boolean | Ref.Ref<boolean> = true,
) => {
  const runId = boxRunId;
  const interrupts = new Set<string>();
  const shellOf = (threadId: ThreadId) =>
    Effect.gen(function* () {
      if ((yield* Ref.get(thread.launched)) !== threadId) return null;
      const linkOrigin = yield* Ref.get(thread.linkOrigin);
      return {
        ...liveThreadShell(threadId, { runtimeMode: "full-access" }),
        projectId: boxProject,
        ...(linkOrigin === null ? {} : { linkOrigin }),
      };
    });
  const runOf = (status: OrchestrationV2Run["status"]) =>
    ({
      id: runId,
      threadId: ThreadId.make("thread:box"),
      ordinal: 1,
      status,
      modelSelection,
      requestedAt: DateTime.makeUnsafe(0),
      startedAt: DateTime.makeUnsafe(0),
      completedAt: null,
    }) as unknown as OrchestrationV2Run;
  const projectionOf = (threadId: ThreadId) =>
    Effect.gen(function* () {
      const shell = yield* shellOf(threadId);
      const status = yield* Ref.get(thread.status);
      const runless = yield* Ref.get(thread.runless);
      return shell === null
        ? (null as never)
        : { ...idleThreadProjection(shell), runs: runless ? [] : [runOf(status)] };
    });
  // One record per command id, as its receipt replays a repeat.
  const recordStop = (commandId: string, kind: "interrupt" | "thread.stop" | "delegated-tasks") =>
    Effect.gen(function* () {
      if (interrupts.has(`${kind}:${commandId}`)) return;
      interrupts.add(`${kind}:${commandId}`);
      yield* Ref.update(thread.stops, (seen) => [...seen, kind]);
      if (kind !== "delegated-tasks") {
        yield* Ref.update(thread.interrupted, (count) => count + 1);
        yield* Deferred.succeed(thread.interruptedOnce, undefined);
      }
    });
  const layerBoxThreads = Layer.mock(ThreadManagement.ThreadManagementService)({
    readRestartCutView: (run, runs) => Effect.succeed(restartCutView(run, runs)),
    readRunRecovery: () => Effect.succeed({ state: "none" as const }),
    getThreadShell: shellOf,
    getProjectThreadRecords: (input) => projectionOf(input.threadId),
    getThreadRecords: (threadId) => projectionOf(threadId),
    waitForThread: (input) =>
      Effect.gen(function* () {
        yield* Ref.update(thread.waitedFor, (seen) => [...seen, input.runId]);
        // A thread with no run has nothing to wait for.
        if (yield* Ref.get(thread.runless)) {
          return { threadId: input.threadId, run: null, timedOut: false };
        }
        if (yield* Ref.get(thread.gone)) {
          return yield* new ThreadManagement.ThreadManagementThreadNotFoundError({
            projectId: boxProject,
            threadId: input.threadId,
          });
        }
        const done = yield* Deferred.await(thread.finished).pipe(
          Effect.timeoutOption(input.timeoutMs),
        );
        // Unpinned, a wait reports the latest run: a later one once there is one.
        const later =
          (yield* Ref.get(thread.successor)) ||
          (input.runId === undefined && (yield* Ref.get(thread.laterReply)) !== null);
        return {
          threadId: input.threadId,
          run: {
            id: later ? boxLaterRunId : runId,
            status: later ? "completed" : yield* Ref.get(thread.status),
          } as OrchestrationV2Run,
          timedOut: Option.isNone(done),
        };
      }),
    getTimelinePage: (threadId, options) =>
      Effect.gen(function* () {
        if (yield* Ref.get(thread.holdRead)) {
          yield* Deferred.succeed(thread.readStarted, undefined);
          yield* Deferred.await(thread.releaseRead);
        }
        const reply = yield* Ref.get(thread.reply);
        const later = yield* Ref.get(thread.laterReply);
        // One assistant message per page, so a read has to page to find the task's.
        const replies = [
          { id: TurnItemId.make("item:reply"), runId, text: reply },
          ...(later === null
            ? []
            : [{ id: TurnItemId.make("item:later-reply"), runId: boxLaterRunId, text: later }]),
        ].map((entry, position) => ({ ...entry, position }));
        const matching =
          options.itemId === undefined
            ? replies.filter((entry) => entry.position > (options.afterPosition ?? -1)).slice(0, 1)
            : replies.filter((entry) => entry.id === options.itemId);
        return {
          items: matching.map(({ id, runId: itemRunId, text, position }) => ({
            position,
            visibility: "local" as const,
            sourceThreadId: threadId,
            sourceItemId: id,
            item: {
              id,
              threadId,
              runId: itemRunId,
              nodeId: null,
              providerThreadId: null,
              providerTurnId: null,
              nativeItemRef: null,
              parentItemId: null,
              ordinal: 1,
              status: "completed" as const,
              title: null,
              startedAt: DateTime.makeUnsafe(0),
              completedAt: DateTime.makeUnsafe(0),
              updatedAt: DateTime.makeUnsafe(0),
              type: "assistant_message" as const,
              messageId: MessageId.make(`message:${id}`),
              text,
              streaming: false,
            } as never,
          })),
          totalItems: replies.length,
          hasMore:
            options.itemId === undefined &&
            matching.length > 0 &&
            matching.at(-1)!.position < replies.length - 1,
        };
      }),
    interruptThread: (input) =>
      recordStop(input.commandId, "interrupt").pipe(
        Effect.as({ type: "interrupt_requested", run: runOf("running") } as never),
      ),
    dispatch: (command) =>
      command.type === "thread.stop"
        ? recordStop(command.commandId, "thread.stop").pipe(
            Effect.as({ sequence: 0, storedEvents: [] } as never),
          )
        : Effect.die(`The box does not expect ${command.type}`),
    stopDelegatedTasks: (input) => recordStop(input.commandId, "delegated-tasks"),
  });
  const layerBoxLaunches = Layer.mock(ThreadLaunch.ThreadLaunchService)({
    replayOrLaunch: (_input, launch) => launch,
    launch: (input) =>
      Ref.get(thread.runless).pipe(
        Effect.flatMap((runless) =>
          Ref.set(thread.launched, input.threadId!).pipe(
            Effect.andThen(Ref.set(thread.linkOrigin, input.linkOrigin ?? null)),
            Effect.andThen(Ref.set(thread.delegatedFrom, input.delegatedFrom ?? null)),
            Effect.andThen(Ref.set(thread.message, input.initialMessage?.text ?? null)),
            Effect.andThen(
              Ref.set(thread.launchedWith, {
                modelSelection: input.modelSelection,
                runtimeMode: input.runtimeMode,
                interactionMode: input.interactionMode,
                workspaceStrategy: input.workspaceStrategy,
              }),
            ),
            Effect.andThen(Ref.update(thread.launchIds, (ids) => [...ids, input.threadId!])),
            Effect.andThen(Deferred.succeed(thread.launchAccepted, undefined)),
            Effect.andThen(
              Ref.get(thread.holdLaunch).pipe(
                Effect.flatMap((held) =>
                  held ? Deferred.await(thread.releaseLaunch) : Effect.void,
                ),
              ),
            ),
            Effect.as({
              threadId: input.threadId,
              projection: {
                thread: {
                  id: input.threadId,
                  projectId: input.projectId,
                  title: input.title,
                  modelSelection: input.modelSelection,
                },
                runs:
                  input.initialMessage?.messageId && !runless
                    ? [
                        {
                          id: runId,
                          status: "running",
                          userMessageId: input.initialMessage.messageId,
                        },
                      ]
                    : [],
              },
              resumed: false,
            } as unknown as ThreadLaunch.ThreadLaunchResult),
          ),
        ),
      ),
  });
  const decodeLifecycle = Schema.decodeUnknownEffect(PeerTaskLifecycleRequest);
  const lifecycle = Layer.merge(
    HttpRouter.add(
      "GET",
      "/mcp/peer-control/tasks",
      Effect.gen(function* () {
        const supported =
          typeof lifecycleSupported === "boolean"
            ? lifecycleSupported
            : yield* Ref.get(lifecycleSupported);
        return supported
          ? HttpServerResponse.jsonUnsafe({ version: 1 })
          : HttpServerResponse.empty({ status: 404 });
      }),
    ),
    HttpRouter.add(
      "POST",
      "/mcp/peer-control/tasks",
      Effect.gen(function* () {
        const supported =
          typeof lifecycleSupported === "boolean"
            ? lifecycleSupported
            : yield* Ref.get(lifecycleSupported);
        if (!supported) return HttpServerResponse.empty({ status: 404 });
        const request = yield* HttpServerRequest.HttpServerRequest;
        const input = yield* decodeLifecycle(yield* request.json);
        const { operation } = input;
        if (operation.type === "acknowledge")
          return HttpServerResponse.jsonUnsafe({
            type: "acknowledged",
            through: operation.through,
          });
        if (operation.type === "stop") {
          if (yield* Ref.get(thread.gone)) return HttpServerResponse.empty({ status: 403 });
          const commandId = CommandId.make(`fixture-stop:${operation.requestId}`);
          if (actual !== undefined) {
            yield* actual.threads.dispatch({
              type: "thread.stop",
              threadId: input.pair.childThreadId,
              commandId,
              ...(operation.reason === undefined ? {} : { reason: operation.reason }),
            });
            yield* actual.threads.stopDelegatedTasks({
              threadId: input.pair.childThreadId,
              commandId,
              reason: operation.reason,
            });
          }
          yield* recordStop(commandId, "thread.stop");
          yield* recordStop(commandId, "delegated-tasks");
          return HttpServerResponse.jsonUnsafe({ type: "stop", commandId });
        }
        if (operation.type === "snapshot") {
          if (operation.wait && operation.after >= 1 && (yield* Ref.get(thread.interrupted)) === 0)
            yield* Deferred.await(thread.interruptedOnce);
          return HttpServerResponse.jsonUnsafe({
            type: "snapshot",
            snapshot: {
              sequence: 1,
              observedAt: "2026-10-17T00:00:00Z",
              title: "Fixture",
              workState: "result_available",
              pendingChildren: 0,
              queuedRuns: 0,
              recoveryWaiting: false,
              incomplete: false,
              executionSettled: (yield* Ref.get(thread.interrupted)) > 0,
              pendingQuestions: 0,
              requestCount: 0,
              requests: [],
            },
          });
        }
        if (operation.type !== "observe") return HttpServerResponse.empty({ status: 409 });
        yield* Ref.update(thread.waitedFor, (seen) => [...seen, boxRunId]);
        if (yield* Ref.get(thread.gone)) return HttpServerResponse.empty({ status: 403 });
        let result: PeerTaskLifecycleResult;
        if (actual !== undefined) {
          const before = yield* actual.threads.getThreadEventSequence(input.pair.childThreadId);
          let records = yield* actual.threads.getThreadRecords(input.pair.childThreadId, [
            "runs",
            "messages",
            "turnItems",
          ]);
          let run = records.runs.toSorted((a, b) => a.ordinal - b.ordinal)[0];
          if (run && !["completed", "failed", "cancelled", "interrupted"].includes(run.status)) {
            yield* actual.threads
              .streamStoredEventsFrom({
                threadId: input.pair.childThreadId,
                afterSequence: before,
                eventType: "run.updated",
              })
              .pipe(
                Stream.filter(
                  (stored) =>
                    stored.event.type === "run.updated" &&
                    ["completed", "failed", "cancelled", "interrupted"].includes(
                      stored.event.payload.status,
                    ),
                ),
                Stream.runHead,
              );
            records = yield* actual.threads.getThreadRecords(input.pair.childThreadId, [
              "runs",
              "messages",
              "turnItems",
            ]);
            run = records.runs.toSorted((a, b) => a.ordinal - b.ordinal)[0];
          }
          if (run === undefined) return HttpServerResponse.empty({ status: 403 });
          if (operation.after >= 1) return yield* Effect.never;
          result = {
            type: "observation",
            observation: {
              cursor: 1,
              snapshot: {
                sequence: 1,
                observedAt: "2026-10-17T00:00:00Z",
                title: "Fixture",
                workState: "result_available",
                pendingChildren: 0,
                queuedRuns: 0,
                recoveryWaiting: false,
                incomplete: false,
                executionSettled: false,
                pendingQuestions: 0,
                requestCount: 0,
                requests: [],
              },
              result: {
                environmentId: box.environmentId,
                threadId: input.pair.childThreadId,
                runId: run.id,
                eventId: EventId.make(`fixture-terminal:${run.id}`),
                sequence: 1,
                status:
                  run.status === "failed" ||
                  run.status === "cancelled" ||
                  run.status === "interrupted"
                    ? run.status
                    : "completed",
                text: subagentResultForRun(records, run).text,
                truncated: false,
              },
            },
          };
        } else {
          if (!(yield* Ref.get(thread.runless))) {
            if (operation.after < 1)
              yield* Deferred.await(thread.finished).pipe(
                Effect.raceFirst(Deferred.await(thread.interruptedOnce)),
              );
            else yield* Deferred.await(thread.interruptedOnce);
          }
          if (yield* Ref.get(thread.gone)) return HttpServerResponse.empty({ status: 403 });
          if (yield* Ref.get(thread.holdRead)) {
            yield* Deferred.succeed(thread.readStarted, undefined);
            yield* Deferred.await(thread.releaseRead);
          }
          const stopped = (yield* Ref.get(thread.interrupted)) > 0;
          if (stopped && operation.after >= 1 && operation.wait) return yield* Effect.never;
          const status = yield* Ref.get(thread.status);
          const cursor = operation.after >= 1 ? operation.after : 1;
          result = {
            type: "observation",
            observation: {
              cursor,
              snapshot: {
                sequence: cursor,
                observedAt: "2026-10-17T00:00:00Z",
                title: "Fixture",
                workState: "result_available",
                pendingChildren: 0,
                queuedRuns: 0,
                recoveryWaiting: false,
                incomplete: false,
                executionSettled: stopped,
                pendingQuestions: 0,
                requestCount: 0,
                requests: [],
              },
              ...(operation.after >= 1
                ? {}
                : {
                    result: {
                      environmentId: box.environmentId,
                      threadId: input.pair.childThreadId,
                      runId: (yield* Ref.get(thread.successor)) ? boxLaterRunId : boxRunId,
                      eventId: EventId.make("fixture-original-terminal"),
                      sequence: 1,
                      status: stopped
                        ? "interrupted"
                        : status === "failed" || status === "cancelled" || status === "interrupted"
                          ? status
                          : "completed",
                      text: (yield* Ref.get(thread.successor))
                        ? ((yield* Ref.get(thread.laterReply)) ?? "")
                        : yield* Ref.get(thread.reply),
                      truncated: false,
                    },
                  }),
            },
          };
        }
        return HttpServerResponse.jsonUnsafe(result);
      }),
    ),
  );
  return Layer.mergeAll(
    lifecycle,
    McpHttpServer.layerOrchestratorToolkit.pipe(
      Layer.provide(Layer.mock(BareLaunch.BareLaunch)({})),
    ),
    McpHttpServer.layerProjectRegistration,
  ).pipe(
    Layer.provide(NodeCrypto.layer),
    Layer.provide(
      Settings.layerTest({
        prismRoles: {
          worker: {
            lanes: { medium: [modelSelection], hard: [{ instanceId, model: "box-prism" }] },
          },
          reviewer: { models: [modelSelection] },
          roast: { models: [{ instanceId, model: "box-prism" }] },
        },
      }),
    ),
    Layer.provide(
      actual === undefined
        ? layerBoxThreads
        : Layer.succeed(ThreadManagement.ThreadManagementService, actual.threads),
    ),
    Layer.provide(
      actual === undefined
        ? layerBoxLaunches
        : Layer.succeed(ThreadLaunch.ThreadLaunchService, {
            ...actual.launches,
            launch: (input) =>
              actual.launches.launch(input).pipe(
                Effect.tap((result) =>
                  Effect.gen(function* () {
                    yield* Ref.set(thread.launched, result.threadId);
                    yield* Ref.update(thread.launchIds, (ids) => [...ids, result.threadId]);
                    yield* Deferred.succeed(thread.launchAccepted, undefined);
                    if (yield* Ref.get(thread.holdLaunch))
                      yield* Deferred.await(thread.releaseLaunch);
                  }),
                ),
              ),
          }),
    ),
    Layer.provide(Layer.mock(RemoteDelegation.RemoteDelegation)({})),
    Layer.provide(Layer.mock(PeerForwarding.PeerForwarding)({})),
    Layer.provide(
      Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({ namedProjectsRoot: "/p" }),
    ),
    Layer.provide(Layer.mock(GitVcsDriver.GitVcsDriver)({})),
    Layer.provide(Layer.mock(SourceControlRepositoryService.SourceControlRepositoryService)({})),
    Layer.provide(
      ServerConfig.layerTest(process.cwd(), { prefix: "t3-remote-delegation-box-" }).pipe(
        Layer.provide(NodeServices.layer),
      ),
    ),
    Layer.provide(actual?.catalogLayer ?? layerLaunchCatalog),

    Layer.provide(Layer.mock(ScheduledTaskService.ScheduledTaskService)({})),
    Layer.provide(
      Layer.mock(ProjectService.ProjectService)({
        getById: () =>
          Effect.succeed(Option.some({ defaultModelSelection: modelSelection } as never)),
        // More than a page, so finding the project with this repository pages.
        listPage: ({ cursor, limit }) =>
          Effect.succeed({
            projects: boxProjects.slice(cursor, cursor + limit),
            nextCursor: cursor + limit < boxProjects.length ? cursor + limit : null,
          }),
      }),
    ),
    Layer.provide(Layer.mock(SecretRequests.SecretRequests)({})),
    Layer.provide(Layer.mock(ThreadLaunch.ThreadLaunchService)({})),
    Layer.provide(Layer.mock(GitVcsDriver.GitVcsDriver)({})),
    Layer.provide(NodeServices.layer),
  );
};

const mcpClient = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  clientCapabilities: {},
  clientInfo: { name: "remote-delegation", version: "1" },
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "remote-delegation", version: "1" },
  },
  getClient: Effect.die("unused"),
});

const adapter = {
  instanceId,
  driver,
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("No provider process needed"),
} as ProviderAdapter.ProviderAdapterV2["Service"];

/** Real destination receipts, launch and thread state; only provider execution is disabled. */
const makeRealBox = Effect.gen(function* () {
  const catalog = yield* layerLaunchCatalog.pipe(Layer.build);
  const providers = yield* Context.get(catalog, ProviderRegistry.ProviderRegistry).getProviders;
  const unavailable = yield* Ref.make<"catalog" | "headroom" | null>(null);
  const catalogLayer = Layer.mock(ProviderRegistry.ProviderRegistry)({
    getProviders: Ref.get(unavailable).pipe(
      Effect.map((reason) =>
        providers.map((provider) => ({
          ...provider,
          models: reason === "catalog" ? [] : provider.models,
          ...(reason === "headroom"
            ? {
                usageLimits: {
                  checkedAt: "2026-10-14T00:00:00Z",
                  windows: [
                    { id: "weekly", kind: "weekly" as const, label: "Weekly", usedPercent: 100 },
                  ],
                },
              }
            : {}),
        })),
      ),
    ),
  });
  const databaseLayer = SqlitePersistence.layerMemory;
  const runtime = ProviderReplayHarness.layerWithRegistry(
    { name: "remote-delegation-real-box" },
    ProviderAdapterRegistry.layerFromAdapters([adapter]),
    { databaseLayer, runEffectWorker: false },
  );
  const dependencies = Layer.mergeAll(
    runtime,
    CommandReceiptStore.layer.pipe(Layer.provide(databaseLayer)),
    IdAllocator.layer,
    WorktreeSetupTracker.layer,
    Layer.mock(ProjectCloneTracker.ProjectCloneTracker)({ get: () => Effect.succeed(null) }),
    Layer.mock(TerminalManager.TerminalManager)({ close: () => Effect.void }),
    Layer.mock(GitWorkflow.GitWorkflowService)({}),
    Layer.mock(TextGeneration.TextGeneration)({}),
    Layer.mock(ProjectSetupScriptRunner.ProjectSetupScriptRunner)({
      runForThread: () => Effect.succeed({ status: "no-script" as const }),
    }),
    Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
      namedProjectsRoot: "/p",
      folderForThread: () => Effect.succeed(Option.none()),
    }),
    Layer.mock(ProjectService.ProjectService)({
      getById: () => Effect.succeed(Option.some(boxProjects.at(-1)!)),
    }),
    Settings.layerTest({
      providerInstances: { [instanceId]: { driver, enabled: true } },
    }),
    catalogLayer,
  );
  const services = yield* ThreadLaunch.layer.pipe(
    Layer.provide(Layer.mock(BareLaunch.BareLaunch)({})),
    Layer.provideMerge(dependencies),
    Layer.build,
  );
  return {
    launches: Context.get(services, ThreadLaunch.ThreadLaunchService),
    threads: Context.get(services, ThreadManagement.ThreadManagementService),
    sink: Context.get(services, EventSink.EventSinkV2),
    catalogLayer: Layer.merge(catalogLayer, ProviderAdapterRegistry.layerFromAdapters([adapter])),
    unavailable,
  };
});

/**
 * The laptop: a real orchestrator (in-memory SQLite) whose parent thread has
 * a live run, the real orchestrator toolkit, and RemoteDelegation. Built in
 * its own scope so a test can tear it down and build it again on the same
 * database, as a restart does.
 */
const makeLaptop = (
  database: Context.Context<SqlClient.SqlClient>,
  offers: SubscriptionRef.SubscriptionRef<
    ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
  >,
  /** The laptop's links from before a restart, which keep their sessions there. */
  kept?: Context.Context<Layer.Success<ReturnType<typeof layerLinkingEnvironment>>>,
  /** Wraps the threads only the remote follower reads, to see what it saw. */
  followerThreads: Layer.Layer<
    ThreadManagement.ThreadManagementService,
    never,
    ThreadManagement.ThreadManagementService
  > = Layer.empty as never,
) =>
  Effect.gen(function* () {
    const linking = kept ?? (yield* layerLinkingEnvironment(laptop).pipe(Layer.build));
    const layerDatabase = Layer.succeedContext(database);
    const layerOrchestrator = Layer.mergeAll(
      layerDatabase,
      CommandReceiptStore.layer.pipe(Layer.provide(layerDatabase)),
      ProjectionStore.layer.pipe(Layer.provide(layerDatabase)),
      ProviderReplayHarness.layerWithRegistry(
        { name: "remote-delegation-laptop" },
        ProviderAdapterRegistry.layerFromAdapters([adapter]),
        { databaseLayer: layerDatabase, runEffectWorker: false },
      ),
    ).pipe(
      Layer.provide(
        Layer.succeed(ProviderContinuationRequests.ProviderContinuationRequests, {
          offer: (request) => SubscriptionRef.update(offers, (seen) => [...seen, request]),
          take: Effect.never,
        }),
      ),
    );
    const layerThreads = ThreadManagement.layer.pipe(Layer.provideMerge(layerOrchestrator));
    const layerHere = McpHttpServer.layerOrchestratorToolkit.pipe(
      Layer.provide(Layer.mock(BareLaunch.BareLaunch)({})),
      Layer.provideMerge(McpServer.McpServer.layer),
      Layer.provideMerge(
        RemoteDelegation.layer.pipe(
          Layer.provide(followerThreads),
          Layer.provideMerge(PeerForwarding.layer),
          Layer.provide(
            Layer.mock(ProjectService.ProjectService)({
              // A cold enrichment cache: the project itself reports no identity yet.
              getById: () =>
                Effect.succeed(
                  Option.some({
                    workspaceRoot: "/home/me/app",
                    repositoryIdentity: null,
                  } as never),
                ),
            }),
          ),
          Layer.provide(
            Layer.mock(RepositoryIdentityResolver.RepositoryIdentityResolver)({
              resolve: (cwd) =>
                Effect.succeed(
                  cwd === "/home/me/app"
                    ? ({ canonicalKey: "github.com/acme/app" } as RepositoryIdentity)
                    : null,
                ),
            }),
          ),
        ),
      ),
      Layer.provide(Layer.succeedContext(linking)),
      Layer.provide(NodeCrypto.layer),
      Layer.provide(Settings.layerTest()),
      Layer.provideMerge(layerThreads),
      Layer.provide(layerLaunchCatalog),

      Layer.provide(Layer.mock(ScheduledTaskService.ScheduledTaskService)({})),
      Layer.provide(Layer.mock(ProjectService.ProjectService)({})),
      Layer.provide(Layer.mock(SecretRequests.SecretRequests)({})),
      Layer.provide(Layer.mock(ThreadLaunch.ThreadLaunchService)({})),
      Layer.provide(Layer.mock(GitVcsDriver.GitVcsDriver)({})),
      Layer.provide(Settings.layerTest()),
      Layer.fresh,
    );
    const here = yield* Layer.build(layerHere);
    const server = Context.get(here, McpServer.McpServer);
    return {
      links: Context.get(linking, PeerLinks.PeerLinks),
      orchestrator: Context.get(here, Orchestrator.OrchestratorV2),
      remote: Context.get(here, RemoteDelegation.RemoteDelegation),
      threads: Context.get(here, ThreadManagement.ThreadManagementService),
      projections: Context.get(here, ProjectionStore.ProjectionStoreV2),
      sink: Context.get(here, EventSink.EventSinkV2),
      call: (name: string, args: Record<string, unknown>, scope = parentScope) =>
        server
          .callTool({ name, arguments: args })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, scope),
            Effect.provideService(McpSchema.McpServerClient, mcpClient),
          ),
    };
  });

const parentThreadId = ThreadId.make("thread:laptop-parent");
const parentRunId = RunId.make("run:laptop-parent");
const parentRootNode = NodeId.make("node:laptop-parent:root");
const parentScope: McpInvocationContext.McpInvocationScope = {
  environmentId: laptop.environmentId,
  requestNamespace: "provider-session:laptop-parent",
  thread: {
    threadId: parentThreadId,
    providerSessionId: "provider-session:laptop-parent",
    providerInstanceId: instanceId,
  },
  client: undefined,
  capabilities: new Set(["orchestration"]),
  issuedAt: 1,
};

/** The laptop's parent thread, mid-turn when its agent delegates. */
const seedParent = (
  laptopEnv: Effect.Success<ReturnType<typeof makeLaptop>>,
  purpose?: ExecutionPurpose,
) =>
  Effect.gen(function* () {
    const now = yield* DateTime.now;
    const providerThreadId = ProviderThreadId.make("provider-thread:laptop-parent");
    yield* laptopEnv.orchestrator.dispatch({
      type: "thread.create",
      createdBy: "user",
      creationSource: "web",
      commandId: CommandId.make("command:create:laptop-parent"),
      threadId: parentThreadId,
      projectId: ProjectId.make("project:laptop"),
      title: "Laptop parent",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
    });
    yield* laptopEnv.sink.write({
      commandId: CommandId.make("command:seed:laptop-parent"),
      events: [
        {
          id: EventId.make("event:seed-provider-thread:laptop-parent"),
          type: "provider-thread.updated",
          threadId: parentThreadId,
          driver,
          providerInstanceId: instanceId,
          occurredAt: now,
          payload: {
            id: providerThreadId,
            driver,
            providerInstanceId: instanceId,
            providerSessionId: null,
            appThreadId: parentThreadId,
            ownerNodeId: parentRootNode,
            nativeThreadRef: { driver, nativeId: "native:laptop-parent", strength: "strong" },
            nativeConversationHeadRef: null,
            status: "active",
            firstRunOrdinal: 1,
            lastRunOrdinal: 1,
            handoffIds: [],
            forkedFrom: null,
            createdAt: now,
            updatedAt: now,
          },
        },
        {
          id: EventId.make("event:seed-node:laptop-parent"),
          type: "node.updated",
          threadId: parentThreadId,
          runId: parentRunId,
          nodeId: parentRootNode,
          driver,
          providerInstanceId: instanceId,
          occurredAt: now,
          payload: {
            id: parentRootNode,
            threadId: parentThreadId,
            runId: parentRunId,
            parentNodeId: null,
            rootNodeId: parentRootNode,
            kind: "root_turn",
            status: "running",
            countsForRun: true,
            providerThreadId,
            providerTurnId: null,
            nativeItemRef: null,
            runtimeRequestId: null,
            checkpointScopeId: null,
            startedAt: now,
            completedAt: null,
          },
        },
        {
          id: EventId.make("event:seed-run:laptop-parent"),
          type: "run.updated",
          threadId: parentThreadId,
          runId: parentRunId,
          nodeId: parentRootNode,
          providerInstanceId: instanceId,
          occurredAt: now,
          payload: {
            id: parentRunId,
            threadId: parentThreadId,
            ordinal: 1,
            providerInstanceId: instanceId,
            modelSelection,
            providerThreadId,
            userMessageId: MessageId.make("message:seed-user:laptop-parent"),
            rootNodeId: parentRootNode,
            activeAttemptId: null,
            status: "running",
            requestedAt: now,
            startedAt: now,
            completedAt: null,
            checkpointId: null,
            contextHandoffId: null,
            ...(purpose === undefined ? {} : { purpose }),
          },
        },
      ],
    });
  });

const delegateToBox = (
  laptopEnv: Effect.Success<ReturnType<typeof makeLaptop>>,
  route: "target" | "environmentId" = "target",
) =>
  laptopEnv
    .call("delegate_task", {
      task: "Run the full suite on the box.",
      ...(route === "environmentId" ? { environmentId: box.environmentId } : {}),
      target: {
        ...(route === "target" ? { environmentId: box.environmentId } : {}),
        providerInstanceId: instanceId,
        model: "gpt-5.4",
      },
      lane: "hard",
      mode: "async",
      clientRequestId: "suite-on-box",
    })
    .pipe(
      Effect.map((result) => {
        expect(result.isError, JSON.stringify(result.content)).toBe(false);
        return result.structuredContent as {
          taskId: NodeId;
          status: string;
          childThreadId: null;
          remoteChild: { environmentId: string; threadId: ThreadId; label: string };
        };
      }),
    );

it.effect.each([false, true])(
  "delivered remote purpose survives a B chip and validates its origin launch (delayed attach=$0)",
  (delayedAttach) =>
    Effect.scoped(
      Effect.gen(function* () {
        const referenceA = {
          host: "github.com",
          repository: "example/repo",
          kind: "issue",
          number: 11,
        } as const;
        const scopeB = { type: "single", reference: { ...referenceA, number: 12 } } as const;
        const thread = yield* makeBoxThread;
        yield* Ref.set(thread.holdLaunch, delayedAttach);
        const actual = yield* makeRealBox;
        const b = yield* servePeer(box, boxToolkitLayer(thread, actual));
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const linking = yield* layerLinkingEnvironment(laptop).pipe(Layer.build);
        const a = yield* makeLaptop(database, offers, linking);
        yield* linkTo(a.links, b, "full-access");
        yield* seedParent(a, {
          version: 1,
          scope: { type: "unscoped", reason: "deliberate" },
          provenance: { type: "declared" },
        });
        const launching = yield* a
          .call("delegate_task", {
            task: "Purpose fixture in an isolated peer.",
            target: {
              environmentId: box.environmentId,
              providerInstanceId: instanceId,
              model: "gpt-5.4",
            },
            purposeIntent: { version: 1, intent: { type: "reference", reference: referenceA } },
            mode: "async",
            clientRequestId: "remote-purpose",
          })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(thread.launchAccepted);
        if (delayedAttach) {
          const parent = yield* a.orchestrator.getThreadProjection(parentThreadId);
          const now = yield* DateTime.now;
          yield* a.sink.write({
            events: [
              {
                id: EventId.make("remote-purpose-parent-ended"),
                type: "run.updated",
                threadId: parentThreadId,
                occurredAt: now,
                payload: { ...parent.runs[0]!, status: "completed", completedAt: now },
              },
            ],
          });
          yield* Deferred.succeed(thread.releaseLaunch, undefined);
        }
        const launched = yield* Fiber.join(launching);
        expect(launched.isError, JSON.stringify(launched.content)).toBe(false);
        const childId = (yield* Ref.get(thread.launched))!;
        const child = yield* actual.threads.getThreadProjection(childId);
        const original = child.runs[0]!;
        expect(original.purpose?.scope).toEqual({ type: "single", reference: referenceA });
        const evidence = child.thread.delegatedFrom?.purposeLaunch;
        expect(evidence).toMatchObject({
          environmentId: laptop.environmentId,
          threadId: parentThreadId,
          runId: parentRunId,
        });
        expect(child.thread.lineage.parentThreadId).toBeNull();
        const now = yield* DateTime.now;
        yield* actual.sink.write({
          events: [
            {
              id: EventId.make("remote-purpose-child-stopped"),
              type: "run.updated",
              threadId: childId,
              occurredAt: now,
              payload: { ...original, status: "completed", completedAt: now },
            },
          ],
        });
        yield* actual.threads.dispatch({
          type: "thread.metadata.update",
          commandId: CommandId.make("remote-purpose-chip-b"),
          threadId: childId,
          purposeSelection: { expectedRevision: 1, scope: scopeB },
        });
        if (delayedAttach) {
          // A later live parent run acts through the same authorized origin. The
          // original ended run is retained solely as launch evidence.
          yield* a.orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("remote-purpose-parent-resume"),
            threadId: parentThreadId,
            messageId: MessageId.make("remote-purpose-parent-resume"),
            text: "Continue handling delegated work.",
            attachments: [],
            createdBy: "user",
            creationSource: "web",
            dispatchMode: { type: "start_immediately" },
            purposeIntent: { version: 1, intent: { type: "unscoped" } },
          });
        }
        const send = {
          environmentId: box.environmentId,
          threadId: childId,
          message: "Continue the original delegated job.",
          clientRequestId: "remote-purpose-continue",
        };
        const continued = yield* a.call("t3_thread_send", send);
        expect(continued.isError, JSON.stringify(continued.content)).toBe(false);
        const result = continued.structuredContent as { runId: RunId; purpose: unknown };
        expect(result.purpose).toMatchObject({
          scope: { type: "single", reference: referenceA },
          provenance: { type: "remote_launch" },
        });
        const retried = yield* a.call("t3_thread_send", send);
        expect(retried.isError).toBe(false);
        expect(retried.structuredContent).toMatchObject({ runId: result.runId });
        const before = yield* actual.threads.getThreadProjection(childId);
        expect(before.runs).toHaveLength(2);
        expect(before.thread.purposeSelection).toEqual({ revision: 2, scope: scopeB });
        expect(before.runs[1]?.purpose?.scope).toEqual({ type: "single", reference: referenceA });
        const explicit = yield* a.call("t3_thread_send", {
          ...send,
          clientRequestId: "remote-purpose-inherit",
          purposeIntent: { version: 1, intent: { type: "inherit" } },
        });
        expect(explicit.isError, JSON.stringify(explicit.content)).toBe(false);
        expect(explicit.structuredContent).toMatchObject({
          purpose: { scope: { type: "single", reference: referenceA } },
        });
        const beforeTamper = yield* actual.threads.getThreadProjection(childId);
        const peers = Context.get(linking, PeerMcpClient.PeerMcpClient);
        for (const [index, forged] of [
          { ...evidence!, environmentId: box.environmentId },
          { ...evidence!, launchId: "another-launch" },
          { ...evidence!, purpose: { ...evidence!.purpose, scope: scopeB } },
        ].entries()) {
          const error = yield* peers
            .call({
              environmentId: box.environmentId,
              tool: "t3_thread_send",
              success: OrchestratorMcpThreadSendResult,
              arguments: {
                threadId: childId,
                message: "Forged continuation",
                clientRequestId: `forged-${index}`,
                delegatedContinuation: forged,
              },
              limits: { runtimeMode: "full-access", interactionMode: "default" },
            })
            .pipe(Effect.flip);
          expect(error.code).toBe("invalid_request");
        }
        const forgedAtOrigin = yield* a.call("t3_thread_send", {
          ...send,
          clientRequestId: "forged-at-origin",
          delegatedContinuation: evidence,
        });
        expect(forgedAtOrigin.isError).toBe(true);
        const after = yield* actual.threads.getThreadProjection(childId);
        expect(after.runs).toEqual(beforeTamper.runs);
        expect(after.messages).toEqual(beforeTamper.messages);
        if (!delayedAttach) {
          const { purposeLaunch: _purposeLaunch, ...legacyOrigin } = after.thread.delegatedFrom!;
          yield* actual.sink.write({
            events: [
              {
                id: EventId.make("remote-purpose-legacy-launch"),
                type: "thread.metadata-updated",
                threadId: childId,
                occurredAt: now,
                payload: { ...after.thread, delegatedFrom: legacyOrigin },
              },
            ],
          });
          const missing = yield* a.call("t3_thread_send", {
            ...send,
            clientRequestId: "legacy-launch-has-no-purpose-proof",
          });
          expect(missing.isError).toBe(true);
          expect(JSON.stringify(missing.content)).toContain("remote purpose launch");
          expect((yield* actual.threads.getThreadProjection(childId)).runs).toEqual(after.runs);
        }
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect.each(["omitted", "inherit"] as const)(
  "a remote child of an unscoped parent launched with $0 intent starts and continues",
  (launchIntent) =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        const actual = yield* makeRealBox;
        const b = yield* servePeer(box, boxToolkitLayer(thread, actual));
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const linking = yield* layerLinkingEnvironment(laptop).pipe(Layer.build);
        const a = yield* makeLaptop(database, offers, linking);
        yield* linkTo(a.links, b, "full-access");
        const deliberate = { type: "unscoped", reason: "deliberate" } as const;
        yield* seedParent(a, { version: 1, scope: deliberate, provenance: { type: "declared" } });
        const launched = yield* a.call("delegate_task", {
          task: "Unscoped purpose fixture in an isolated peer.",
          target: {
            environmentId: box.environmentId,
            providerInstanceId: instanceId,
            model: "gpt-5.4",
          },
          ...(launchIntent === "inherit"
            ? { purposeIntent: { version: 1, intent: { type: "inherit" } } }
            : {}),
          mode: "async",
          clientRequestId: `remote-unscoped-${launchIntent}`,
        });
        expect(launched.isError, JSON.stringify(launched.content)).toBe(false);
        const childId = (yield* Ref.get(thread.launched))!;
        const child = yield* actual.threads.getThreadProjection(childId);
        const original = child.runs[0]!;
        const expectedScope =
          launchIntent === "inherit"
            ? deliberate
            : { type: "unscoped", reason: "legacy_no_intent" };
        expect(original.purpose?.scope ?? LEGACY_EXECUTION_PURPOSE.scope).toEqual(expectedScope);
        if (launchIntent === "inherit") {
          expect(child.thread.delegatedFrom?.purposeLaunch?.purpose).toEqual({
            version: 1,
            scope: deliberate,
            provenance: { type: "parent_run", threadId: parentThreadId, runId: parentRunId },
          });
        } else {
          expect(child.thread.delegatedFrom?.purposeLaunch).toBeUndefined();
        }
        const now = yield* DateTime.now;
        yield* actual.sink.write({
          events: [
            {
              id: EventId.make(`remote-unscoped-${launchIntent}-stopped`),
              type: "run.updated",
              threadId: childId,
              occurredAt: now,
              payload: { ...original, status: "completed", completedAt: now },
            },
          ],
        });
        const continued = yield* a.call("t3_thread_send", {
          environmentId: box.environmentId,
          threadId: childId,
          message: "Continue the delegated job.",
          clientRequestId: `remote-unscoped-${launchIntent}-continue`,
        });
        expect(continued.isError, JSON.stringify(continued.content)).toBe(false);
        expect(
          (continued.structuredContent as { purpose: { scope: unknown } }).purpose.scope,
        ).toEqual(expectedScope);
        expect((yield* actual.threads.getThreadProjection(childId)).runs).toHaveLength(2);
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

/** Waits until the task here has the result its thread there ended with. */
const taskResult = (laptopEnv: Effect.Success<ReturnType<typeof makeLaptop>>, taskId: NodeId) =>
  laptopEnv.orchestrator
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
          stored.event.payload.result !== null,
      ),
      Stream.runHead,
      Effect.map((stored) =>
        Option.isSome(stored) && stored.value.event.type === "subagent.updated"
          ? stored.value.event.payload
          : undefined,
      ),
    );

const remoteState = (
  a: Effect.Success<ReturnType<typeof makeLaptop>>,
  taskId: NodeId,
  predicate: (task: OrchestrationV2Subagent) => boolean,
) =>
  a.orchestrator
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
          predicate(stored.event.payload),
      ),
      Stream.runHead,
      Effect.flatMap(Effect.fromOption),
      Effect.map((stored) =>
        stored.event.type === "subagent.updated" ? stored.event.payload : undefined,
      ),
    );

it.effect.each(["target", "environmentId"] as const)(
  "$0 delegation keeps the source parent's record and completion wake",
  (route) =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        const b = yield* serveBox(thread);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const a = yield* makeLaptop(database, offers);
        yield* linkTo(a.links, b, "full-access");
        yield* seedParent(a);

        const conflict = yield* a.call("delegate_task", {
          task: "Conflicting destination",
          environmentId: box.environmentId,
          target: { environmentId: laptop.environmentId },
          mode: "async",
        });
        expect(conflict.isError).toBe(true);
        expect((yield* a.orchestrator.getThreadProjection(parentThreadId)).subagents).toHaveLength(
          0,
        );
        expect(yield* Ref.get(thread.launchIds)).toEqual([]);
        const delegated = yield* delegateToBox(a, route);
        expect(delegated).toMatchObject({
          status: "running",
          childThreadId: null,
          remoteChild: { environmentId: box.environmentId, label: "Box" },
        });
        expect((yield* Ref.get(thread.launchedWith))?.modelSelection).toEqual(modelSelection);
        // The box launched it in the project with the same repository.
        expect(yield* Ref.get(thread.launched)).toBe(delegated.remoteChild.threadId);
        // ...naming the parent here, so the thread there links back to it.
        expect(yield* Ref.get(thread.delegatedFrom)).toEqual({
          environmentId: laptop.environmentId,
          threadId: parentThreadId,
          title: "Laptop parent",
        });
        // A retry with the same key launches nothing new and records nothing new.
        const retried = yield* delegateToBox(a);
        expect(retried.taskId).toBe(delegated.taskId);
        expect((yield* a.orchestrator.getThreadProjection(parentThreadId)).subagents).toHaveLength(
          1,
        );

        yield* Ref.set(thread.reply, "All 412 tests pass on the box.");
        yield* Ref.set(thread.status, "completed");
        yield* Deferred.succeed(thread.finished, undefined);
        const task = yield* taskResult(a, delegated.taskId);
        expect(task).toMatchObject({
          status: "completed",
          result: "All 412 tests pass on the box.",
        });
        // The parent's live run claims the result, as for a child here.
        const parentRun = (yield* a.orchestrator.getThreadProjection(parentThreadId)).runs.find(
          (run) => run.id === parentRunId,
        );
        expect(parentRun?.delegatedCompletion?.delivery?.taskIds).toEqual([delegated.taskId]);
        // The wake is offered after the result commits, so the result can be seen first.
        const offered = yield* SubscriptionRef.changes(offers).pipe(
          Stream.filter((seen) => seen.some((offer) => offer.threadId === parentThreadId)),
          Stream.runHead,
        );
        expect(Option.isSome(offered)).toBe(true);

        const status = yield* a.call("task_status", { taskId: delegated.taskId });
        expect(status.structuredContent).toMatchObject({
          status: "completed",
          summary: "All 412 tests pass on the box.",
        });
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect.each(["target", "environmentId"] as const)(
  "$0 destination uses target Prism and refuses a broader mode than the link",
  (route) =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        const b = yield* serveBox(thread);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const a = yield* makeLaptop(database, offers);
        // The parent runs with full access; the link allows less.
        yield* linkTo(a.links, b, "auto-accept-edits");
        yield* seedParent(a);

        const result = yield* a.call("delegate_task", {
          task: "Run the suite on the box.",
          ...(route === "target"
            ? { target: { environmentId: box.environmentId } }
            : { environmentId: box.environmentId }),
          lane: "hard",
        });
        expect(result.isError, JSON.stringify(result.content)).toBe(false);
        // The destination kit picks a different model; the link caps the modes.
        expect(yield* Ref.get(thread.launchedWith)).toMatchObject({
          modelSelection: { instanceId, model: "box-prism" },
          runtimeMode: "auto-accept-edits",
          interactionMode: "default",
        });

        // Asking for more than the link allows is still refused there.
        const broader = yield* a.call("delegate_task", {
          task: "Run the suite on the box.",
          ...(route === "target"
            ? { target: { environmentId: box.environmentId } }
            : { environmentId: box.environmentId }),
          runtimeMode: "full-access",
          clientRequestId: "broader",
        });
        expect(broader.isError).toBe(true);
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("follows an open task again after the laptop restarts", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const linking = yield* layerLinkingEnvironment(laptop).pipe(Layer.build);
      // The laptop before the restart delegates, then goes away.
      const delegated = yield* Effect.scoped(
        Effect.gen(function* () {
          const before = yield* makeLaptop(database, offers, linking);
          yield* linkTo(before.links, b, "full-access");
          yield* seedParent(before);
          return yield* delegateToBox(before);
        }),
      );
      // The task finishes on the box while the laptop is down.
      yield* Ref.set(thread.reply, "Finished while you were away.");
      yield* Ref.set(thread.status, "completed");
      yield* Deferred.succeed(thread.finished, undefined);

      const after = yield* makeLaptop(database, offers, linking);
      // Restore the same authenticated link generation, never a replacement.
      yield* after.remote.start();
      const task = yield* taskResult(after, delegated.taskId);
      expect(task).toMatchObject({ status: "completed", result: "Finished while you were away." });
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("a reopened parent follows its remote task again", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const linking = yield* layerLinkingEnvironment(laptop).pipe(Layer.build);
      const delegated = yield* Effect.scoped(
        Effect.gen(function* () {
          const before = yield* makeLaptop(database, offers, linking);
          yield* linkTo(before.links, b, "full-access");
          yield* seedParent(before);
          const delegated = yield* delegateToBox(before);
          yield* before.orchestrator.dispatch({
            type: "thread.archive",
            commandId: CommandId.make("command:archive:laptop-parent"),
            threadId: parentThreadId,
          });
          return delegated;
        }),
      );

      // The follower reads the parent's records right after its event sequence.
      const sawArchived = yield* Deferred.make<void>();
      let checking = false;
      const followerThreads = Layer.effect(
        ThreadManagement.ThreadManagementService,
        Effect.gen(function* () {
          const threads = yield* ThreadManagement.ThreadManagementService;
          return ThreadManagement.ThreadManagementService.of({
            ...threads,
            getThreadEventSequence: (threadId) =>
              Effect.sync(() => {
                checking = true;
              }).pipe(Effect.andThen(threads.getThreadEventSequence(threadId))),
            getThreadRecords: ((threadId, include) =>
              threads.getThreadRecords(threadId, include).pipe(
                Effect.tap((records) => {
                  const checked = checking;
                  checking = false;
                  return checked && records.thread.archivedAt !== null
                    ? Deferred.succeed(sawArchived, undefined)
                    : Effect.void;
                }),
              )) as typeof threads.getThreadRecords,
          });
        }),
      );
      const after = yield* makeLaptop(database, offers, linking, followerThreads);
      yield* after.remote.start();
      // Archived, the follower stops observing the box.
      yield* Deferred.await(sawArchived);
      yield* after.orchestrator.dispatch({
        type: "thread.unarchive",
        commandId: CommandId.make("command:unarchive:laptop-parent"),
        threadId: parentThreadId,
      });

      yield* Ref.set(thread.reply, "Finished after the parent reopened.");
      yield* Ref.set(thread.status, "completed");
      yield* Deferred.succeed(thread.finished, undefined);
      const task = yield* taskResult(after, delegated.taskId);
      expect(task).toMatchObject({
        status: "completed",
        result: "Finished after the parent reopened.",
      });
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect(
  "recovers an accepted launch after restart without its response or an active parent run",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        yield* Ref.set(thread.holdLaunch, true);
        const b = yield* serveBox(thread);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const linking = yield* layerLinkingEnvironment(laptop).pipe(Layer.build);
        const pending = yield* Effect.scoped(
          Effect.gen(function* () {
            const before = yield* makeLaptop(database, offers, linking);
            yield* linkTo(before.links, b, "full-access");
            yield* seedParent(before);
            yield* delegateToBox(before).pipe(Effect.forkScoped);
            yield* Deferred.await(thread.launchAccepted);
            const projection = yield* before.orchestrator.getThreadProjection(parentThreadId);
            // B accepted, but its response has not reached A. A must already own the task.
            expect(projection.subagents).toHaveLength(1);
            const task = projection.subagents[0]!;
            expect(task.remoteChild?.threadId).toBeUndefined();
            const run = projection.runs.find((candidate) => candidate.id === parentRunId)!;
            const now = yield* DateTime.now;
            yield* before.sink.write({
              commandId: CommandId.make("command:restart-during-launch"),
              events: [
                {
                  id: EventId.make("event:restart-during-launch"),
                  type: "run.updated",
                  threadId: parentThreadId,
                  runId: parentRunId,
                  nodeId: parentRootNode,
                  driver,
                  providerInstanceId: instanceId,
                  occurredAt: now,
                  payload: { ...run, status: "interrupted", completedAt: now },
                },
              ],
            });
            return task;
          }),
        );
        const acceptedThreadId = yield* Ref.get(thread.launched);
        yield* Ref.set(thread.holdLaunch, false);
        yield* Deferred.succeed(thread.releaseLaunch, undefined);
        yield* Ref.set(thread.reply, "Recovered the original launch.");
        yield* Ref.set(thread.status, "completed");
        yield* Deferred.succeed(thread.finished, undefined);

        const after = yield* makeLaptop(database, offers, linking);
        yield* after.remote.start();
        const task = yield* taskResult(after, pending.id);
        expect(task).toMatchObject({
          id: pending.id,
          status: "completed",
          result: "Recovered the original launch.",
          remoteChild: { threadId: acceptedThreadId, runId: boxRunId },
        });
        expect(new Set(yield* Ref.get(thread.launchIds)).size).toBe(1);
        yield* SubscriptionRef.changes(offers).pipe(
          Stream.filter((seen) => seen.some((offer) => offer.threadId === parentThreadId)),
          Stream.runHead,
        );
        expect(
          (yield* after.orchestrator.getThreadProjection(parentThreadId)).subagents,
        ).toHaveLength(1);
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect.each(
  (["catalog", "headroom"] as const).flatMap((unavailable) =>
    ([false, true] as const).map((cancel) => ({ unavailable, cancel })),
  ),
)(
  "replays real destination receipts after lost response with $unavailable unavailable (cancel=$cancel)",
  ({ unavailable, cancel }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        yield* Ref.set(thread.holdLaunch, true);
        const actual = yield* makeRealBox;
        const b = yield* servePeer(box, boxToolkitLayer(thread, actual));
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const linking = yield* layerLinkingEnvironment(laptop).pipe(Layer.build);
        const pending = yield* Effect.scoped(
          Effect.gen(function* () {
            const before = yield* makeLaptop(database, offers, linking);
            yield* linkTo(before.links, b, "full-access");
            yield* seedParent(before);
            yield* before
              .call("delegate_task", {
                task: "Recover the accepted child.",
                target: { environmentId: box.environmentId },
                mode: "async",
                clientRequestId: "real-receipt-replay",
              })
              .pipe(Effect.forkScoped);
            yield* Deferred.await(thread.launchAccepted);
            const task = (yield* before.orchestrator.getThreadProjection(parentThreadId))
              .subagents[0]!;
            expect(task.remoteChild?.threadId).toBeUndefined();
            // Keep the accepted response unreachable while recording a pending cancellation.
            yield* Ref.set(b.descriptor, descriptorOf("environment-away", "Away"));
            if (cancel) yield* before.call("task_cancel", { taskId: task.id });
            return task;
          }),
        );
        const acceptedThreadId = (yield* Ref.get(thread.launched))!;
        yield* actual.threads
          .streamStoredEventsFrom({
            threadId: acceptedThreadId,
            afterSequence: 0,
            eventType: "run.updated",
          })
          .pipe(
            Stream.filter(
              (stored) => stored.commandId === CommandId.make(`${acceptedThreadId}:release`),
            ),
            Stream.runHead,
          );
        const accepted = yield* actual.threads.getThreadProjection(acceptedThreadId);
        const run = accepted.runs[0]!;
        if (!cancel) {
          const now = yield* DateTime.now;
          yield* actual.sink.write({
            events: [
              {
                id: EventId.make("real-box-completed"),
                type: "run.updated",
                threadId: acceptedThreadId,
                occurredAt: now,
                payload: { ...run, status: "completed", completedAt: now },
              },
              {
                id: EventId.make("real-box-reply"),
                type: "turn-item.updated",
                threadId: acceptedThreadId,
                occurredAt: now,
                payload: {
                  id: TurnItemId.make("real-box-reply"),
                  threadId: acceptedThreadId,
                  runId: run.id,
                  nodeId: null,
                  providerThreadId: null,
                  providerTurnId: null,
                  nativeItemRef: null,
                  parentItemId: null,
                  ordinal: 1,
                  status: "completed",
                  title: null,
                  startedAt: now,
                  completedAt: now,
                  updatedAt: now,
                  type: "assistant_message",
                  messageId: MessageId.make("real-box-reply"),
                  text: "The original child finished.",
                  streaming: false,
                },
              },
            ],
          });
        }
        yield* Ref.set(actual.unavailable, unavailable);
        yield* Ref.set(b.descriptor, box);
        yield* Ref.set(thread.holdLaunch, false);
        yield* Deferred.succeed(thread.releaseLaunch, undefined);
        const after = yield* makeLaptop(database, offers, linking);
        yield* after.remote.start();
        if (cancel) yield* recordedStop(after, pending.id);
        else {
          expect(yield* taskResult(after, pending.id)).toMatchObject({
            status: "completed",
            result: "The original child finished.",
          });
          yield* SubscriptionRef.changes(offers).pipe(
            Stream.filter((seen) => seen.some((offer) => offer.threadId === parentThreadId)),
            Stream.runHead,
          );
        }
        const task = (yield* after.orchestrator.getThreadProjection(parentThreadId)).subagents.find(
          (entry) => entry.id === pending.id,
        );
        expect(task).toMatchObject({
          status: cancel ? "cancelled" : "completed",
          remoteChild: {
            threadId: acceptedThreadId,
            runId: run.id,
            ...(cancel ? { stop: { state: "acknowledged" } } : {}),
          },
        });
        expect(yield* Ref.get(thread.launchIds)).toEqual([acceptedThreadId]);
        const persisted = yield* actual.threads.getThreadProjection(acceptedThreadId);
        expect(persisted.messages).toHaveLength(1);
        expect(persisted.runs).toHaveLength(1);
        expect(persisted.runs[0]?.status).toBe(cancel ? "interrupted" : "completed");
        // This outage really rejects new admission; only the receipt bypasses it.
        const fresh = yield* after.call("delegate_task", {
          task: "A new child must fail admission.",
          target: { environmentId: box.environmentId },
          mode: "async",
          clientRequestId: "new-unavailable-child",
        });
        expect(fresh.isError).toBe(true);
        expect(yield* Ref.get(thread.launchIds)).toEqual([acceptedThreadId]);
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

/** A committed source intent, before any launch marker or network request. */
const recordPending = (
  a: Effect.Success<ReturnType<typeof makeLaptop>>,
  bare?: ThreadLaunch.ThreadLaunchInput["bare"],
) =>
  Effect.gen(function* () {
    const link = Option.getOrThrow(yield* a.links.get(box.environmentId));
    const recorded = yield* a.orchestrator.dispatch({
      type: "delegated_task.remote.request",
      commandId: CommandId.make("command:pending-launch"),
      parentThreadId,
      parentRunId,
      parentNodeId: parentRootNode,
      task: "Run the full suite on the box.",
      driver: ProviderDriverKind.make("remote"),
      remoteChild: {
        environmentId: box.environmentId,
        label: "Box",
        launch: {
          ...(bare === undefined ? {} : { bare }),
          requestNamespace: parentScope.requestNamespace,
          clientRequestId: `delegate:${parentThreadId}:suite-on-box`,
          linkIdentity: link.launchIdentity,
          projectId: boxProject,
          title: "Remote suite",
          prismRole: "roast",
          lane: "hard",
          workspaceStrategy: { type: "root", branch: "remote-root" },
          runtimeMode: "full-access",
          interactionMode: "default",
          delegatedFrom: {
            environmentId: laptop.environmentId,
            threadId: parentThreadId,
            title: "Laptop parent",
          },
        },
      },
      completionWake: "always",
    });
    const event = recorded.storedEvents.find((stored) => stored.event.type === "subagent.updated");
    if (event?.event.type !== "subagent.updated") return yield* Effect.die("No pending task event");
    return event.event.payload.id;
  });

/** A specific durable milestone, including attachment and an acknowledged stop. */
const recordedStop = (a: Effect.Success<ReturnType<typeof makeLaptop>>, taskId: NodeId) =>
  a.orchestrator
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
          (stored.event.payload.remoteChild?.stoppedThere === true ||
            ["acknowledged", "confirmed", "not_started"].includes(
              stored.event.payload.remoteChild?.stop?.state ?? "",
            ) ||
            stored.event.payload.remoteChild?.launchError !== undefined),
      ),
      Stream.runHead,
    );

it.effect.each([false, true])(
  "recovers a never-sent intent after restart (cancelled=%s)",
  (cancelled) =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        const b = yield* serveBox(thread);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const linking = yield* layerLinkingEnvironment(laptop).pipe(Layer.build);
        const taskId = yield* Effect.scoped(
          Effect.gen(function* () {
            const before = yield* makeLaptop(database, offers, linking);
            yield* linkTo(before.links, b, "full-access");
            yield* seedParent(before);
            const id = yield* recordPending(
              before,
              cancelled
                ? { additions: [{ type: "skill", name: "only-requested-body" }] }
                : undefined,
            );
            if (cancelled) {
              // Commit cancellation while the source is down, before a follower can send.
              yield* before.orchestrator.dispatch({
                type: "delegated_task.remote.complete",
                commandId: CommandId.make("command:cancel-before-send"),
                parentThreadId,
                taskId: id,
                status: "cancelled",
                result: "Cancelled before send.",
              });
            }
            return id;
          }),
        );
        yield* Ref.set(thread.reply, "Recovered the unsent intent.");
        yield* Ref.set(thread.status, "completed");
        yield* Deferred.succeed(thread.finished, undefined);
        const after = yield* makeLaptop(database, offers, linking);
        yield* after.remote.start();
        if (cancelled) {
          yield* recordedStop(after, taskId);
          expect(yield* Ref.get(thread.launchIds)).toEqual([]);
          expect(yield* taskResult(after, taskId)).toMatchObject({
            status: "cancelled",
            result: "Cancelled before send.",
            remoteChild: {
              launch: { bare: { additions: [{ type: "skill", name: "only-requested-body" }] } },
            },
          });
        } else {
          expect(yield* taskResult(after, taskId)).toMatchObject({
            status: "completed",
            result: "Recovered the unsent intent.",
          });
          expect(yield* Ref.get(thread.launchIds)).toHaveLength(1);
          const task = yield* taskResult(after, taskId);
          expect(task?.remoteChild?.launch).toMatchObject({
            prismRole: "roast",
            lane: "hard",
            workspaceStrategy: { type: "root", branch: "remote-root" },
          });
          expect(task?.remoteChild?.launch?.modelSelection).toBeUndefined();
          expect(task?.model).toBe("box-prism");
          expect(yield* Ref.get(thread.launchedWith)).toMatchObject({
            modelSelection: { instanceId, model: "box-prism" },
            workspaceStrategy: { type: "root", branch: "remote-root" },
          });
          yield* SubscriptionRef.changes(offers).pipe(
            Stream.filter((seen) => seen.some((offer) => offer.threadId === parentThreadId)),
            Stream.runHead,
          );
        }
        // Imported #380 intents retain their original single-result observation.
        expect(yield* after.projections.getOpenRemoteDelegatedTasks).toEqual([]);
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect.each(["lowered parent", "revoked link"] as const)(
  "refuses a pending launch after restart with a %s",
  (restriction) =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        const b = yield* serveBox(thread);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const linking = yield* layerLinkingEnvironment(laptop).pipe(Layer.build);
        const taskId = yield* Effect.scoped(
          Effect.gen(function* () {
            const before = yield* makeLaptop(database, offers, linking);
            yield* linkTo(before.links, b, "full-access");
            yield* seedParent(before);
            const id = yield* recordPending(before);
            if (restriction === "lowered parent") {
              yield* before.orchestrator.dispatch({
                type: "thread.runtime-mode.set",
                commandId: CommandId.make("command:lower-before-recovery"),
                threadId: parentThreadId,
                runtimeMode: "approval-required",
              });
            } else {
              const [session] = yield* b.linkedSessions;
              yield* b.auth.revokeSession(session!.sessionId);
            }
            return id;
          }),
        );
        const after = yield* makeLaptop(database, offers, linking);
        yield* after.remote.start();
        expect(yield* taskResult(after, taskId)).toMatchObject({ status: "failed" });
        expect(yield* Ref.get(thread.launchIds)).toEqual([]);
        expect(yield* after.projections.getOpenRemoteDelegatedTasks).toEqual([]);
        expect(yield* b.linkedSessions).toHaveLength(restriction === "revoked link" ? 0 : 1);
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect.each(
  [false, true].flatMap((cancel) =>
    (["lowered parent", "revoked link", "replaced link"] as const).map((restriction) => ({
      cancel,
      restriction,
    })),
  ),
)(
  "keeps acceptance uncertainty visible with $restriction (cancel=$cancel)",
  ({ cancel, restriction }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        yield* Ref.set(thread.holdLaunch, true);
        const b = yield* serveBox(thread);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const linking = yield* layerLinkingEnvironment(laptop).pipe(Layer.build);
        const taskId = yield* Effect.scoped(
          Effect.gen(function* () {
            const before = yield* makeLaptop(database, offers, linking);
            yield* linkTo(before.links, b, "full-access");
            yield* seedParent(before);
            yield* delegateToBox(before).pipe(Effect.forkScoped);
            yield* Deferred.await(thread.launchAccepted);
            const task = (yield* before.orchestrator.getThreadProjection(parentThreadId))
              .subagents[0]!;
            if (cancel) {
              expect((yield* before.call("task_cancel", { taskId: task.id })).isError).toBe(false);
            }
            if (restriction === "lowered parent") {
              yield* before.orchestrator.dispatch({
                type: "thread.runtime-mode.set",
                commandId: CommandId.make("command:lower-after-acceptance"),
                threadId: parentThreadId,
                runtimeMode: "approval-required",
              });
            } else if (restriction === "revoked link") {
              const [session] = yield* b.linkedSessions;
              yield* b.auth.revokeSession(session!.sessionId);
            }
            return task.id;
          }),
        );
        yield* Ref.set(thread.holdLaunch, false);
        yield* Deferred.succeed(thread.releaseLaunch, undefined);
        const after = yield* makeLaptop(database, offers, linking);
        if (restriction === "replaced link") {
          yield* linkTo(after.links, b, "full-access");
          yield* Ref.set(thread.status, "completed");
          yield* Deferred.succeed(thread.finished, undefined);
        }
        yield* after.remote.start();
        if (cancel)
          yield* after.orchestrator
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
                  stored.event.payload.remoteChild?.launchError !== undefined,
              ),
              Stream.runHead,
            );
        yield* taskResult(after, taskId);
        const task = (yield* after.orchestrator.getThreadProjection(parentThreadId)).subagents.find(
          (entry) => entry.id === taskId,
        );
        expect(new Set(yield* Ref.get(thread.launchIds)).size).toBe(1);
        expect(task).toMatchObject({ status: cancel ? "cancelled" : "failed" });
        if (cancel) expect(task?.result).toBe("Cancelled.");
        expect(task?.remoteChild?.launchError).toContain(
          "If it was already accepted, work may still be running on Box.",
        );
        // Refusal prevents confirmation/Stop; it does not mean B's accepted work ended.
        expect(yield* Ref.get(thread.status)).toBe(
          restriction === "replaced link" ? "completed" : "running",
        );
        expect(yield* Ref.get(thread.interrupted)).toBe(0);
        if (!cancel) expect(yield* Ref.get(thread.launchIds)).toHaveLength(1);
        expect(yield* b.linkedSessions).toHaveLength(
          restriction === "revoked link" ? 0 : restriction === "replaced link" ? 2 : 1,
        );
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect.each(["cancel", "parent Stop"] as const)(
  "reconciles %s after acceptance without a launch response",
  (action) =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        yield* Ref.set(thread.holdLaunch, true);
        const b = yield* serveBox(thread);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const linking = yield* layerLinkingEnvironment(laptop).pipe(Layer.build);
        const taskId = yield* Effect.scoped(
          Effect.gen(function* () {
            const before = yield* makeLaptop(database, offers, linking);
            yield* linkTo(before.links, b, "full-access");
            yield* seedParent(before);
            yield* delegateToBox(before).pipe(Effect.forkScoped);
            yield* Deferred.await(thread.launchAccepted);
            const task = (yield* before.orchestrator.getThreadProjection(parentThreadId))
              .subagents[0]!;
            // The accepted child is unreachable until after the source restarts.
            yield* Ref.set(b.descriptor, descriptorOf("environment-elsewhere", "Elsewhere"));
            if (action === "cancel") {
              const result = yield* before.call("task_cancel", { taskId: task.id });
              expect(result.isError).toBe(false);
            } else {
              yield* before.threads.stopDelegatedTasks({
                threadId: parentThreadId,
                commandId: CommandId.make("command:parent-stop-during-launch"),
              });
            }
            expect(yield* taskResult(before, task.id)).toMatchObject({
              status: action === "cancel" ? "cancelled" : "interrupted",
            });
            return task.id;
          }),
        );
        const acceptedThreadId = yield* Ref.get(thread.launched);
        yield* Ref.set(b.descriptor, box);
        yield* Ref.set(thread.holdLaunch, false);
        yield* Deferred.succeed(thread.releaseLaunch, undefined);
        const after = yield* makeLaptop(database, offers, linking);
        yield* after.remote.start();
        yield* recordedStop(after, taskId);
        const task = (yield* after.orchestrator.getThreadProjection(parentThreadId)).subagents.find(
          (entry) => entry.id === taskId,
        );
        expect(task).toMatchObject({
          status: action === "cancel" ? "cancelled" : "interrupted",
          remoteChild: {
            threadId: acceptedThreadId,
            runId: boxRunId,
            stop: { state: "acknowledged" },
          },
        });
        expect(new Set(yield* Ref.get(thread.launchIds)).size).toBe(1);
        expect(yield* Ref.get(thread.stops)).toEqual(["thread.stop", "delegated-tasks"]);
        expect(yield* after.projections.getOpenRemoteDelegatedTasks).toContainEqual({
          parentThreadId,
          taskId,
        });
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect(
  "cancel records and acknowledges each later Stop once while keeping its original result",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        const b = yield* serveBox(thread);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const a = yield* makeLaptop(database, offers);
        yield* linkTo(a.links, b, "full-access");
        yield* seedParent(a);

        const cancelled = yield* delegateToBox(a);
        const cancel = yield* a.call("task_cancel", {
          taskId: cancelled.taskId,
          reason: "Not needed",
        });
        expect(cancel.structuredContent).toEqual({
          taskId: cancelled.taskId,
          status: "cancel_requested",
          stopState: "pending",
        });
        yield* recordedStop(a, cancelled.taskId);
        // Acceptance is asynchronous; the persisted acknowledgement follows the real transport effect.
        expect(yield* Ref.get(thread.stops)).toEqual(["thread.stop", "delegated-tasks"]);
        expect(yield* taskResult(a, cancelled.taskId)).toMatchObject({ status: "cancelled" });

        // Cancelling again, once the result is in, still stops what came after it there.
        const again = yield* a.call("task_cancel", {
          taskId: cancelled.taskId,
          clientRequestId: "cancel-again",
        });
        expect(again.structuredContent).toMatchObject({
          taskId: cancelled.taskId,
          status: "cancel_requested",
        });
        yield* remoteState(
          a,
          cancelled.taskId,
          (task) =>
            task.remoteChild?.stop?.requestId.endsWith(":cancel-again") === true &&
            ["acknowledged", "confirmed"].includes(task.remoteChild.stop.state),
        );
        expect(yield* Ref.get(thread.interrupted)).toBe(2);
        // ...and a retry of that cancel stops nothing new.
        yield* a.call("task_cancel", { taskId: cancelled.taskId, clientRequestId: "cancel-again" });
        expect(yield* Ref.get(thread.interrupted)).toBe(2);
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("a task's result is its own run's whole last reply, not a later run's", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);

      const result = yield* a.call("delegate_task", {
        task: "Review the diff.",
        role: "review",
        target: { environmentId: box.environmentId },
        clientRequestId: "review-on-box",
      });
      expect(result.isError, JSON.stringify(result.content)).toBe(false);
      const { taskId } = result.structuredContent as { taskId: NodeId };
      // The role reaches the thread there, as it does a child here.
      expect(yield* Ref.get(thread.message)).toBe("Review the diff.");

      // A reply longer than one read, then a later run's reply on the same thread.
      const long = "x".repeat(45_000);
      yield* Ref.set(thread.reply, long);
      yield* Ref.set(thread.laterReply, "Someone else asked a follow-up.");
      yield* Ref.set(thread.status, "completed");
      yield* Deferred.succeed(thread.finished, undefined);
      const task = yield* taskResult(a, taskId);
      expect(task?.status).toBe("completed");
      expect(task?.result).toBe(long);
      // Every wait was for the run the launch started.
      const waits = yield* Ref.get(thread.waitedFor);
      expect(waits.length).toBeGreaterThan(0);
      expect(waits.every((runId) => runId === boxRunId)).toBe(true);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("credential rotation replays the same remote task for the same parent and key", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);

      const first = yield* delegateToBox(a);
      const later = yield* a.call(
        "delegate_task",
        {
          task: "Run the full suite on the box.",
          target: { environmentId: box.environmentId },
          clientRequestId: "suite-on-box",
        },
        { ...parentScope, requestNamespace: "provider-session:laptop-parent-2" },
      );
      expect(later.isError, JSON.stringify(later.content)).toBe(false);
      const second = later.structuredContent as {
        taskId: NodeId;
        remoteChild: { threadId: ThreadId };
      };
      expect(second.taskId).toBe(first.taskId);
      expect(second.remoteChild.threadId).toBe(first.remoteChild.threadId);
      expect((yield* a.orchestrator.getThreadProjection(parentThreadId)).subagents).toHaveLength(1);
      const changedPolicy = yield* a.call(
        "delegate_task",
        {
          task: "Run the full suite on the box.",
          target: { environmentId: box.environmentId },
          clientRequestId: "suite-on-box",
          tools: { allow: ["Read"] },
        },
        { ...parentScope, requestNamespace: "provider-session:laptop-parent-2" },
      );
      expect(changedPolicy.isError).toBe(true);
      expect(JSON.stringify(changedPolicy.content)).toContain("Launch tool allowlist changed.");
      expect(yield* Ref.get(thread.launchIds)).toEqual([first.remoteChild.threadId]);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect(
  "marks a lost destination or forgotten link stale without manufacturing a run result",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        const b = yield* serveBox(thread);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const a = yield* makeLaptop(database, offers);
        yield* linkTo(a.links, b, "full-access");
        yield* seedParent(a);

        yield* Ref.set(thread.gone, true);
        const gone = yield* delegateToBox(a);
        const failed = yield* remoteState(
          a,
          gone.taskId,
          (task) => task.remoteChild?.availability === "stale",
        );
        expect(failed?.result).toBe(null);
        expect(failed?.remoteChild?.latestResult).toBeUndefined();

        // The link is forgotten here while the task runs there.
        yield* Ref.set(thread.gone, false);
        const forgotten = yield* a
          .call("delegate_task", {
            task: "Another one.",
            target: { environmentId: box.environmentId },
            clientRequestId: "forgotten",
          })
          .pipe(Effect.map((result) => result.structuredContent as { taskId: NodeId }));
        yield* a.links.unlink(box.environmentId);
        yield* Ref.set(thread.status, "completed");
        yield* Deferred.succeed(thread.finished, undefined);
        const unlinked = yield* remoteState(
          a,
          forgotten.taskId,
          (task) => task.remoteChild?.availability === "stale",
        );
        expect(unlinked?.remoteChild?.latestResult).toBeUndefined();
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("cancel ends the task here even when the link is gone", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);

      const delegated = yield* delegateToBox(a);
      yield* a.links.unlink(box.environmentId);
      const cancel = yield* a.call("task_cancel", { taskId: delegated.taskId });
      expect(cancel.structuredContent).toEqual({
        taskId: delegated.taskId,
        status: "cancel_requested",
        stopState: "pending",
      });
      const task = yield* taskResult(a, delegated.taskId);
      expect(task?.status).toBe("cancelled");
      expect(task?.result).toBe("Cancelled.");
      const unconfirmed = yield* remoteState(
        a,
        delegated.taskId,
        (task) => task.remoteChild?.stop?.state === "unconfirmed",
      );
      expect(unconfirmed?.remoteChild?.stoppedThere).not.toBe(true);
      expect(yield* Ref.get(thread.interrupted)).toBe(0);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("a parent stopped while the task's result is read still stops its thread there", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);
      const delegated = yield* delegateToBox(a);

      // The task's run ends there, and while the follower reads its reply,
      // the parent here is stopped: more work may follow that run there.
      yield* Ref.set(thread.holdRead, true);
      yield* Ref.set(thread.reply, "Done there.");
      yield* Ref.set(thread.status, "completed");
      yield* Deferred.succeed(thread.finished, undefined);
      yield* Deferred.await(thread.readStarted);
      yield* a.threads.stopDelegatedTasks({
        threadId: parentThreadId,
        commandId: CommandId.make("command:stop:laptop-parent-mid-read"),
      });
      yield* Deferred.succeed(thread.releaseRead, undefined);

      // The Stop's result stands, and the thread there is still stopped.
      expect(yield* taskResult(a, delegated.taskId)).toMatchObject({ status: "interrupted" });
      yield* Deferred.await(thread.interruptedOnce);
      expect(yield* Ref.get(thread.stops)).toContain("thread.stop");
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("a stop the box refuses remains unconfirmed without a destination effect", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);
      const delegated = yield* delegateToBox(a);

      // The external destination refuses the Stop. Source-mode policy is exercised
      // against real authenticated authority in PeerAuthority.workflow.test.ts.
      yield* Ref.set(thread.gone, true);
      yield* a.orchestrator.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make("command:lower:laptop-parent"),
        threadId: parentThreadId,
        runtimeMode: "approval-required",
      });
      yield* a.threads.stopDelegatedTasks({
        threadId: parentThreadId,
        commandId: CommandId.make("command:stop:laptop-parent-lowered"),
      });

      // A refusal is a durable unconfirmed obligation, never proof the process stopped.
      const settled = yield* a.orchestrator
        .streamStoredEventsFrom({
          threadId: parentThreadId,
          afterSequence: 0,
          eventType: "subagent.updated",
        })
        .pipe(
          Stream.filter(
            (stored) =>
              stored.event.type === "subagent.updated" &&
              stored.event.payload.id === delegated.taskId &&
              stored.event.payload.remoteChild?.stop?.state === "unconfirmed",
          ),
          Stream.runHead,
        );
      expect(Option.isSome(settled)).toBe(true);
      expect(yield* Ref.get(thread.stops)).toEqual([]);
      expect(yield* a.projections.getOpenRemoteDelegatedTasks).toContainEqual({
        parentThreadId,
        taskId: delegated.taskId,
      });
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("stopping the parent stops its task there", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);

      const delegated = yield* delegateToBox(a);
      // What the delegated-tasks.stop effect runs once the parent's Stop commits.
      yield* a.threads.stopDelegatedTasks({
        threadId: parentThreadId,
        commandId: CommandId.make("command:stop:laptop-parent"),
        reason: "User stopped",
      });
      expect(yield* taskResult(a, delegated.taskId)).toMatchObject({
        status: "interrupted",
        result: "Stopped with its parent: User stopped",
      });
      // The follower sees it ended here and stops it there.
      yield* Deferred.await(thread.interruptedOnce);
      expect(yield* Ref.get(thread.interrupted)).toBe(1);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.live("a parent stopped while the box is unreachable still stops its task there", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);
      // The box sleeps as soon as the task is there, so its follower backs off.
      const delegated = yield* delegateToBox(a);
      yield* Ref.set(b.descriptor, descriptorOf("environment-elsewhere", "Elsewhere"));
      yield* Ref.set(thread.gone, true);
      yield* Deferred.succeed(thread.finished, undefined);
      yield* remoteState(a, delegated.taskId, (task) => task.remoteChild?.availability === "stale");
      // The parent is stopped meanwhile.
      yield* a.threads.stopDelegatedTasks({
        threadId: parentThreadId,
        commandId: CommandId.make("command:stop:laptop-parent-offline"),
      });
      expect(yield* taskResult(a, delegated.taskId)).toMatchObject({ status: "interrupted" });

      yield* remoteState(
        a,
        delegated.taskId,
        (task) => task.remoteChild?.stop?.state === "unconfirmed",
      );
      // Production's native retry resumes after the destination becomes reachable.
      yield* Ref.set(b.descriptor, box);
      yield* Ref.set(thread.gone, false);
      yield* Deferred.await(thread.interruptedOnce);
      expect(yield* Ref.get(thread.interrupted)).toBe(1);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("a task cancelled while the box is unreachable is stopped there after a restart", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      // Its links outlive the restart, so the box knows it as the same link session.
      const linking = yield* layerLinkingEnvironment(laptop).pipe(Layer.build);
      // The laptop cancels while the box sleeps, then restarts before it wakes.
      const delegated = yield* Effect.scoped(
        Effect.gen(function* () {
          const before = yield* makeLaptop(database, offers, linking);
          yield* linkTo(before.links, b, "full-access");
          yield* seedParent(before);
          const task = yield* delegateToBox(before);
          yield* Ref.set(b.descriptor, descriptorOf("environment-elsewhere", "Elsewhere"));
          yield* before.call("task_cancel", { taskId: task.taskId });
          expect(yield* taskResult(before, task.taskId)).toMatchObject({ status: "cancelled" });
          return task;
        }),
      );
      expect(yield* Ref.get(thread.interrupted)).toBe(0);

      yield* Ref.set(b.descriptor, box);
      const after = yield* makeLaptop(database, offers, linking);
      yield* after.remote.start();
      yield* Deferred.await(thread.interruptedOnce);
      yield* recordedStop(after, delegated.taskId);
      expect(yield* after.projections.getOpenRemoteDelegatedTasks).toContainEqual({
        parentThreadId,
        taskId: delegated.taskId,
      });
      expect(yield* Ref.get(thread.stops)).toEqual(["thread.stop", "delegated-tasks"]);
      // The result here is still the cancel's.
      expect(yield* taskResult(after, delegated.taskId)).toMatchObject({ status: "cancelled" });
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("a task whose thread there has no run ends with its last reply", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);
      // As a task recorded before its run was kept, whose thread there is idle.
      yield* Ref.set(thread.runless, true);
      yield* Ref.set(thread.reply, "Done before it was followed.");

      const legacyThread = ThreadId.make("legacy-box-thread");
      yield* Ref.set(thread.launched, legacyThread);
      const recorded = yield* a.orchestrator.dispatch({
        type: "delegated_task.remote.request",
        commandId: CommandId.make("legacy-runless-import"),
        parentThreadId,
        parentRunId,
        parentNodeId: parentRootNode,
        task: "Imported remote work",
        driver: ProviderDriverKind.make("remote"),
        remoteChild: { environmentId: box.environmentId, threadId: legacyThread, label: "Box" },
      });
      const created = recorded.storedEvents.find(
        (stored) => stored.event.type === "subagent.updated",
      );
      if (created?.event.type !== "subagent.updated")
        return yield* Effect.die("Expected legacy task");
      const taskId = created.event.payload.id;
      yield* a.remote.start();
      expect(yield* taskResult(a, taskId)).toMatchObject({
        status: "completed",
        result: "Done before it was followed.",
      });
      expect((yield* taskResult(a, taskId))?.remoteChild?.latestResult).toBeUndefined();
      // Imported provenance stays unknown; no real run identity is invented.
      expect(yield* Ref.get(thread.waitedFor)).toEqual([undefined]);
      const cancelled = yield* a.call("task_cancel", {
        taskId,
        clientRequestId: "legacy-unconfirmed",
      });
      expect(cancelled.structuredContent).toMatchObject({
        taskId,
        status: "completed",
        stopState: "unconfirmed",
      });
      expect(yield* Ref.get(thread.stops)).toEqual([]);
      expect((yield* taskResult(a, taskId))?.result).toBe("Done before it was followed.");
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("a local task runs in this thread's project", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* seedParent(a);

      const result = yield* a.call("delegate_task", {
        task: "Run it here.",
        target: { projectId: "project:elsewhere" },
      });
      expect(result.isError).toBe(true);
      expect(JSON.stringify(result.content)).toContain("invalid_request");
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("the remote follower reports the recovery successor reply returned by wait", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);
      const delegated = yield* delegateToBox(a);
      yield* Ref.set(thread.reply, "First run failed.");
      yield* Ref.set(thread.laterReply, "Recovery succeeded.");
      yield* Ref.set(thread.successor, true);
      yield* Deferred.succeed(thread.finished, undefined);
      const task = yield* taskResult(a, delegated.taskId);
      expect(task?.remoteChild?.runId).toBe(boxRunId);
      expect(task?.result).toBe("Recovery succeeded.");
      expect(task?.status).toBe("completed");
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("refuses an old destination before recording or launching delegated work", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread, false);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);
      const receipt = yield* a.call("delegate_task", {
        lifecycleVersion: 1,
        remoteChild: { launch: { lifecycleVersion: 1 } },
        task: "Unsupported lifecycle",
        target: { environmentId: box.environmentId, projectId: boxProject },
        mode: "async",
        clientRequestId: "unsupported381",
      });
      expect(receipt.isError).toBe(true);
      expect(JSON.stringify(receipt.content)).toContain("does not support delegated lifecycle v1");
      expect((yield* a.orchestrator.getThreadProjection(parentThreadId)).subagents).toHaveLength(0);
      expect(yield* Ref.get(thread.launchIds)).toEqual([]);
      expect(yield* Ref.get(thread.stops)).toEqual([]);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("discards an in-flight original reply after its recorded link is replaced", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);
      const task = yield* delegateToBox(a);
      yield* Ref.set(thread.holdRead, true);
      yield* Ref.set(thread.status, "completed");
      yield* Ref.set(thread.reply, "Old-generation reply");
      yield* Deferred.succeed(thread.finished, undefined);
      yield* Deferred.await(thread.readStarted);
      yield* a.links.unlink(box.environmentId);
      yield* linkTo(a.links, b, "full-access");
      yield* Deferred.succeed(thread.releaseRead, undefined);
      const stale = yield* remoteState(
        a,
        task.taskId,
        (task) => task.remoteChild?.availability === "stale",
      );
      expect(stale?.result).toBe(null);
      expect(stale?.remoteChild?.latestResult).toBeUndefined();
      expect(yield* Ref.get(thread.stops)).toEqual([]);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect.each([false, true])(
  "imports persisted #380 launch provenance without a lifecycle marker, then negotiates only a deliberate Stop (support=%s)",
  (supported) =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        const support = yield* Ref.make(true);
        const b = yield* serveBox(thread, support);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const linking = yield* layerLinkingEnvironment(laptop).pipe(Layer.build);
        const exported = yield* Effect.scoped(
          Effect.gen(function* () {
            const before = yield* makeLaptop(database, offers, linking);
            yield* linkTo(before.links, b, "full-access");
            yield* seedParent(before);
            const receipt = yield* delegateToBox(before);
            const task = (yield* before.orchestrator.getThreadProjection(
              parentThreadId,
            )).subagents.find((task) => task.id === receipt.taskId)!;
            expect(task.remoteChild?.launch?.lifecycleVersion).toBe(1);
            return task;
          }),
        );
        const sql = Context.get(database, SqlClient.SqlClient);
        // Serialize the isolated record through #380's persisted shape. All real
        // accepted task/thread/run/namespace/grant identities remain unchanged.
        for (const table of [
          "orchestration_v2_projection_subagents",
          "orchestration_v2_projection_turn_items",
          "orchestration_events",
        ]) {
          yield* sql.unsafe(
            `UPDATE ${table} SET payload_json=json_remove(payload_json,'$.remoteChild.launch.lifecycleVersion','$.remoteChild.cursor','$.remoteChild.snapshot','$.remoteChild.latestResult','$.remoteChild.originalResult','$.remoteChild.availability','$.remoteChild.stop') WHERE json_type(payload_json,'$.remoteChild')='object'`,
          );
        }
        yield* Ref.set(support, supported);
        yield* Ref.set(thread.reply, "Imported original result.");
        yield* Ref.set(thread.status, "completed");
        yield* Deferred.succeed(thread.finished, undefined);
        const after = yield* makeLaptop(database, offers, linking);
        yield* after.remote.start();
        const imported = yield* taskResult(after, exported.id);
        expect(imported?.result).toBe("Imported original result.");
        expect(imported?.remoteChild?.launch).toEqual({
          ...exported.remoteChild!.launch!,
          lifecycleVersion: undefined,
        });
        expect(imported?.remoteChild?.threadId).toBe(exported.remoteChild?.threadId);
        expect(imported?.remoteChild?.runId).toBe(exported.remoteChild?.runId);
        expect(imported?.remoteChild?.latestResult).toBeUndefined();
        expect(imported?.remoteChild?.availability).toBeUndefined();
        // A same-key retry reuses the original admission even if the destination is old.
        const retry = yield* delegateToBox(after);
        expect(retry.taskId).toBe(exported.id);
        expect(new Set(yield* Ref.get(thread.launchIds)).size).toBe(1);
        const cancel = yield* after.call("task_cancel", {
          taskId: exported.id,
          clientRequestId: "imported-stop381",
        });
        expect(cancel.isError).not.toBe(true);
        yield* after.orchestrator
          .streamStoredEventsFrom({
            threadId: parentThreadId,
            afterSequence: 0,
            eventType: "subagent.updated",
          })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "subagent.updated" &&
                stored.event.payload.id === exported.id &&
                stored.event.payload.remoteChild?.stop?.state ===
                  (supported ? "acknowledged" : "unconfirmed"),
            ),
            Stream.runHead,
            Effect.flatMap(Effect.fromOption),
          );
        const final = (yield* after.orchestrator.getThreadProjection(
          parentThreadId,
        )).subagents.find((task) => task.id === exported.id)!;
        expect(final.result).toBe(exported.result ?? "Imported original result.");
        expect(final.remoteChild?.launch?.lifecycleVersion).toBeUndefined();
        expect(yield* Ref.get(thread.interrupted)).toBe(supported ? 1 : 0);
        // #380's terminal observation may have recorded stoppedThere. It is
        // retained history and never promotes this new Stop to process confirmation.
        expect(final.remoteChild?.stoppedThere).toBe(imported?.remoteChild?.stoppedThere);
        expect(final.remoteChild?.stop?.state).not.toBe("confirmed");
        const malformed = yield* decodeRemoteTaskLaunch({
          ...final.remoteChild!.launch,
          lifecycleVersion: 2,
        }).pipe(Effect.result);
        expect(malformed._tag).toBe("Failure");
        const forged = yield* decodeCommand({
          type: "delegated_task.remote.request",
          remoteChild: { launch: { lifecycleVersion: 1 } },
        }).pipe(Effect.result);
        expect(forged._tag).toBe("Failure");
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);
