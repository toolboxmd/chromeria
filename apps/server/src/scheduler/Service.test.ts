// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";
import * as NodeFSP from "node:fs/promises";
import { describe, expect, it } from "@effect/vitest";
import {
  AuthSessionId,
  CommandId,
  EventId,
  EnvironmentId,
  MessageId,
  OrchestrationProposedPlanId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  ClientOrchestrationCommand,
  SchedulerStateCommand,
  SchedulerError,
  DEFAULT_PRISM_ROLE_KITS,
  type ServerProvider,
  type ScheduledTask,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { McpServer } from "effect/unstable/ai";
import * as Stream from "effect/Stream";
import * as SessionStore from "../auth/SessionStore.ts";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import { ServerEnvironmentIdentity } from "../environment/ServerEnvironment.ts";
import { SchedulerToolkit } from "../mcp/toolkits/scheduler/tools.ts";
import { SchedulerToolkitHandlersLive } from "../mcp/toolkits/scheduler/handlers.ts";
import { McpInvocationContext, type McpCapability } from "../mcp/McpInvocationContext.ts";
import { Scheduler } from "./Service.ts";
import { makeSchedulerRpcHandlers } from "./rpcHandlers.ts";
import { iso } from "./Schedule.ts";
import { RECOVERY_DELAYS } from "./Scheduler.ts";
import { TestClock } from "effect/testing";
import { OrchestrationLayerLive } from "../orchestration/runtimeLayer.ts";
import { ProviderCommandReactor } from "../orchestration/Services/ProviderCommandReactor.ts";
import { OrchestrationEngineService } from "../orchestration/Services/OrchestrationEngine.ts";
import { OrchestrationCommandReceiptRepository } from "../persistence/Services/OrchestrationCommandReceipts.ts";
import { makeSqlitePersistenceLive } from "../persistence/Layers/Sqlite.ts";
import { ServerConfig } from "../config.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import * as ThreadPlanProgress from "../orchestration/ThreadPlanProgress.ts";
import { ServerSettingsService, layer as liveSettingsLayer } from "../serverSettings.ts";
import { ServerActivation } from "../serverActivation.ts";
import { ServerRuntimeStartup, makeCommandGate } from "../serverRuntimeStartup.ts";
import * as ProcessRunner from "../processRunner.ts";
import { ProviderRegistry } from "../provider/Services/ProviderRegistry.ts";
import { makeProviderRegistryLayer } from "../provider/testUtils/providerRegistryMock.ts";
import { makeLiveScheduler, CHECK_OUTPUT_BYTES, truncateCheckOutput } from "./Service.ts";
import {
  temporaryDirectory,
  createParent,
  PARENT_ID,
  NOW,
  session,
  commandId,
  assistantReply,
  dispatchAll,
  withServer,
  callTool,
  dispatchUntil,
  parentActivity,
} from "../mcp/toolkits/threads/handlers.testFixtures.ts";
import { makeThreadTurnSender } from "../mcp/toolkits/threads/sendThreadTurn.ts";
import { ProjectionSnapshotQuery } from "../orchestration/Services/ProjectionSnapshotQuery.ts";

const decodeClientCommand = Schema.decodeUnknownOption(ClientOrchestrationCommand);
const decodeSchedulerCommand = Schema.decodeUnknownOption(SchedulerStateCommand);
const provider: ServerProvider = {
  instanceId: ProviderInstanceId.make("codex"),
  driver: ProviderDriverKind.make("codex"),
  enabled: true,
  installed: true,
  version: null,
  status: "ready",
  auth: { status: "authenticated" },
  checkedAt: NOW,
  models: [{ slug: "gpt-5", name: "GPT", isCustom: false, capabilities: null }],
  slashCommands: [],
  skills: [],
};
const preferences = {
  prismRoles: {
    ...DEFAULT_PRISM_ROLE_KITS,
    worker: {
      ...DEFAULT_PRISM_ROLE_KITS.worker,
      lanes: { easy: [], hard: [], medium: [{ instanceId: provider.instanceId, model: "gpt-5" }] },
    },
  },
};
const layer = (directory: string) =>
  Layer.mergeAll(
    OrchestrationLayerLive,
    ServerSettingsService.layerTest(preferences),
    makeProviderRegistryLayer([provider]),
    ProcessRunner.layer,
  ).pipe(
    Layer.provide(ThreadPlanProgress.layer),
    Layer.provide(RepositoryIdentityResolver.layer),
    Layer.provideMerge(makeSqlitePersistenceLive(NodePath.join(directory, "state.sqlite"))),
    Layer.provideMerge(ServerConfig.layerTest(directory, { prefix: "scheduler-test-" })),
    Layer.provideMerge(NodeServices.layer),
  );
const within = <A, E, R>(directory: string, effect: Effect.Effect<A, E, R>) =>
  Effect.scoped(effect.pipe(Effect.provide(layer(directory))));
const definition = {
  title: "Verified task",
  prompt: "EFFECT: create the deliverable once",
  target: { kind: "thread" as const, threadId: PARENT_ID },
  role: "worker" as const,
  schedule: { kind: "interval" as const, minutes: 60 },
  checkCommand: "test -f result.txt",
  checkReason: "Deliverable exists",
};
const currentTask = (scheduler: Effect.Success<typeof makeLiveScheduler>) =>
  scheduler.list.pipe(Effect.map((tasks) => tasks[0]!));
const userTexts = (engine: OrchestrationEngineService["Service"], id: ThreadId) =>
  engine
    .readThreadEvents({
      threadId: id,
      fromSequenceExclusive: 0,
      toSequenceInclusive: Number.MAX_SAFE_INTEGER,
      limit: Number.MAX_SAFE_INTEGER,
    })
    .pipe(
      Stream.runCollect,
      Effect.map((events) =>
        events
          .filter((event) => event.type === "thread.message-sent" && event.payload.role === "user")
          .map((event) => (event.type === "thread.message-sent" ? event.payload.text : "")),
      ),
    );

/** The fake provider executes each initial side-effect instruction, rather than deduping it itself. */
const fakeProvider = (
  engine: OrchestrationEngineService["Service"],
  state = { sideEffects: 0, executed: new Set<string>(), context: new Set<string>() },
) => {
  const { executed, context } = state;
  return {
    get sideEffects() {
      return state.sideEffects;
    },
    execute: (
      mode: "before-output" | "after-work" | "completed" | "ambiguous-crash" = "completed",
    ) =>
      Effect.gen(function* () {
        const events = yield* engine.readEvents(0, Number.MAX_SAFE_INTEGER).pipe(Stream.runCollect);
        for (const event of events) {
          if (
            event.type !== "thread.turn-start-requested" ||
            !event.commandId ||
            executed.has(event.commandId)
          )
            continue;
          executed.add(event.commandId);
          const texts = yield* userTexts(engine, ThreadId.make(event.aggregateId));
          const text = texts.at(-1)!;
          if (mode === "before-output") {
            yield* engine.dispatch(
              session(
                ThreadId.make(event.aggregateId),
                "error",
                "failed-before",
                "provider unavailable",
              ),
            );
            continue;
          }
          yield* engine.dispatch(
            session(ThreadId.make(event.aggregateId), "running", `turn-${executed.size}`),
          );
          if (text.includes("EFFECT:") || !context.has(event.aggregateId)) {
            state.sideEffects++;
            context.add(event.aggregateId);
          }
          if (mode !== "ambiguous-crash")
            yield* engine.dispatch({
              type: "thread.activity.append",
              commandId: commandId(),
              threadId: ThreadId.make(event.aggregateId),
              activity: {
                id: EventId.make(`tool-${executed.size}`),
                kind: "tool.started",
                tone: "info",
                summary: "Executed filesystem side effect",
                payload: {},
                turnId: null,
                createdAt: NOW,
              },
              createdAt: NOW,
            });
          yield* engine.dispatch(
            session(
              ThreadId.make(event.aggregateId),
              mode === "completed" ? "ready" : "error",
              `turn-${executed.size}`,
              mode === "completed" ? null : "provider disconnected",
            ),
          );
        }
      }),
  };
};

describe("scheduler real SQLite and sender boundary", () => {
  it.effect("keeps retained done-run evidence immutable while allowing old-run eviction", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory("scheduler-done-evidence-");
      yield* within(
        directory,
        Effect.gen(function* () {
          yield* createParent(directory);
          const scheduler = yield* makeLiveScheduler;
          const engine = yield* OrchestrationEngineService;
          const created = yield* scheduler.create(definition, "creator");
          yield* scheduler.runNow(created.id);
          yield* scheduler.reconcile();
          yield* fakeProvider(engine).execute();
          const unfinished = yield* currentTask(scheduler);
          for (const verdict of [
            null,
            { version: 1, passed: false, output: "failed", checkedAt: NOW },
            { version: 2, passed: true, output: "wrong judge", checkedAt: NOW },
          ]) {
            const invalid = yield* Effect.exit(
              engine.dispatch({
                type: "scheduler.state.set",
                commandId: commandId(),
                threadId: ThreadId.make(unfinished.id),
                expectedRevision: unfinished.revision,
                createdAt: NOW,
                task: {
                  ...unfinished,
                  revision: unfinished.revision + 1,
                  runs: unfinished.runs.map((run) => ({
                    ...run,
                    status: "done" as const,
                    check: verdict,
                  })),
                },
              }),
            );
            expect(Exit.isFailure(invalid)).toBe(true);
            expect((yield* currentTask(scheduler)).runs[0]!.status).toBe("running");
          }
          yield* Effect.promise(() =>
            NodeFSP.writeFile(NodePath.join(directory, "result.txt"), "done"),
          );
          yield* scheduler.reconcile();
          const task = yield* currentTask(scheduler);
          const run = task.runs[0]!;
          expect(run.status).toBe("done");
          for (const patch of [
            { checkVersion: 2 },
            { checkCwd: "/different" },
            { definition: { ...run.definition, prompt: "replacement work" } },
            { threadId: ThreadId.make("replacement-thread") },
            { check: { ...run.check!, passed: false } },
            { check: { ...run.check!, output: "replacement verdict evidence" } },
            { status: "needs-you" as const },
          ]) {
            const outcome = yield* Effect.exit(
              engine.dispatch({
                type: "scheduler.state.set",
                commandId: commandId(),
                threadId: ThreadId.make(task.id),
                expectedRevision: task.revision,
                createdAt: NOW,
                task: { ...task, revision: task.revision + 1, runs: [{ ...run, ...patch }] },
              }),
            );
            expect(Exit.isFailure(outcome)).toBe(true);
            expect((yield* currentTask(scheduler)).runs[0]).toEqual(run);
          }
          yield* engine.dispatch({
            type: "scheduler.state.set",
            commandId: commandId(),
            threadId: ThreadId.make(task.id),
            expectedRevision: task.revision,
            createdAt: NOW,
            task: { ...task, revision: task.revision + 1, runs: [] },
          });
          expect((yield* currentTask(scheduler)).runs).toEqual([]);
        }),
      );
    }),
  );
  it.effect(
    "continues the pinned run after a real provider stop acknowledgement without lifting retirement",
    () =>
      Effect.gen(function* () {
        yield* TestClock.setTime(Date.parse(NOW));
        const directory = yield* temporaryDirectory("scheduler-provider-stop-");
        let starts = 0;
        let stops = 0;
        let sideEffects = 0;
        const texts: string[] = [];
        yield* Effect.scoped(
          withServer(
            NodePath.join(directory, "state.sqlite"),
            Effect.gen(function* () {
              yield* createParent(directory);
              const scheduler = yield* makeLiveScheduler.pipe(Effect.provide(ProcessRunner.layer));
              const engine = yield* OrchestrationEngineService;
              const reactor = yield* ProviderCommandReactor;
              yield* reactor.start();
              const created = yield* scheduler.create(definition, "creator");
              yield* scheduler.runNow(created.id);
              yield* scheduler.reconcile();
              yield* reactor.drain;
              expect(texts).toHaveLength(1);
              const original = (yield* currentTask(scheduler)).runs[0]!;
              yield* engine.dispatch(session(PARENT_ID, "running", "provider-turn-1"));
              yield* engine.dispatch({
                type: "thread.session.stop",
                commandId: commandId(),
                threadId: PARENT_ID,
                createdAt: NOW,
              });
              yield* reactor.drain;
              const shell = Option.getOrThrow(
                yield* (yield* ProjectionSnapshotQuery).getThreadShellById(PARENT_ID),
              );
              expect(shell.session?.status).toBe("stopped");
              expect(stops).toBe(1);
              expect((yield* engine.getThreadRetirement(PARENT_ID))?.retired ?? false).toBe(false);
              const observation = yield* scheduler.inspectRun(
                yield* currentTask(scheduler),
                original,
              );
              expect(observation.retired).toBe(false);
              expect(observation.idle).toBe(true);
              yield* scheduler.reconcile();
              yield* TestClock.adjust(30_000);
              yield* scheduler.reconcile();
              yield* reactor.drain;
              expect(texts).toHaveLength(2);
              expect(starts).toBe(2);
              expect(sideEffects).toBe(1);
              expect(texts[1]).toContain("Immutable outcome check version 1");
              expect((yield* currentTask(scheduler)).runs[0]).toMatchObject({
                id: original.id,
                threadId: PARENT_ID,
                checkVersion: 1,
              });
              yield* engine.dispatch(session(PARENT_ID, "running", "provider-turn-2"));
              yield* engine.dispatch(session(PARENT_ID, "ready", "provider-turn-2"));
              yield* callTool("interrupt_thread", {
                threadId: PARENT_ID,
                scope: "project",
                retireSubtree: true,
              });
              yield* reactor.drain;
              expect((yield* engine.getThreadRetirement(PARENT_ID))?.retired).toBe(true);
              expect((yield* engine.getThreadRetirement(PARENT_ID))?.pendingStop).toBe(false);
              yield* scheduler.reconcile();
              expect((yield* currentTask(scheduler)).runs[0]!.status).toBe("needs-you");
              expect(yield* scheduler.runNow(created.id).pipe(Effect.flip)).toBeInstanceOf(
                SchedulerError,
              );
              expect(texts).toHaveLength(2);
            }).pipe(
              Effect.provide(makeSqlitePersistenceLive(NodePath.join(directory, "state.sqlite"))),
            ),
            undefined,
            {
              settings: preferences,
              providers: [provider],
              provider: {
                listSessions: () => Effect.succeed([]),
                getCapabilities: () =>
                  Effect.succeed({
                    sessionModelSwitch: "in-session",
                    promptlessTurnContinuation: true,
                  }),
                startSession: (threadId, input) =>
                  Effect.sync(() => {
                    starts++;
                    return {
                      threadId,
                      provider: ProviderDriverKind.make("codex"),
                      providerInstanceId: provider.instanceId,
                      status: "ready",
                      runtimeMode: "full-access",
                      cwd: input.cwd,
                      model: "gpt-5",
                      resumeCursor: null,
                      createdAt: NOW,
                      updatedAt: NOW,
                    };
                  }),
                stopSession: () =>
                  Effect.sync(() => {
                    stops++;
                  }),
                sendTurn: (input) =>
                  Effect.sync(() => {
                    texts.push(input.input ?? "");
                    if (input.input?.includes("EFFECT: create the deliverable once")) sideEffects++;
                    return {
                      threadId: input.threadId,
                      turnId: TurnId.make(`provider-turn-${texts.length}`),
                    };
                  }),
              },
            },
          ),
        );
      }),
  );
  it.effect("registers every scheduler tool with the actual MCP server", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory("scheduler-registration-");
      yield* within(
        directory,
        Effect.gen(function* () {
          const scheduler = yield* makeLiveScheduler;
          yield* McpServer.registerToolkit(SchedulerToolkit).pipe(
            Effect.provide(SchedulerToolkitHandlersLive),
            Effect.provideService(Scheduler, scheduler),
            Effect.provide(McpServer.McpServer.layer),
          );
        }),
      );
    }),
  );
  it.effect("builds the actual Scheduler layer before activation and command readiness", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory("scheduler-startup-");
      yield* within(
        directory,
        Effect.gen(function* () {
          const activation = yield* Deferred.make<void>();
          const parked = yield* Deferred.make<void>();
          const reconciled = yield* Deferred.make<void>();
          const gate = yield* makeCommandGate;
          const engine = yield* OrchestrationEngineService;
          const settingsContext = yield* Layer.build(
            liveSettingsLayer.pipe(Layer.provide(ServerSecretStore.layer)),
          );
          const settings = Context.get(settingsContext, ServerSettingsService);
          // Real SQLite engine and live settings PubSub subscriptions, not empty-stream mocks.
          const schedulerContext = yield* Layer.build(
            Scheduler.layer.pipe(
              Layer.provide(Layer.succeed(ServerActivation, Deferred.await(activation))),
              Layer.provide(
                Layer.succeed(ServerRuntimeStartup, {
                  ...gate,
                  awaitCommandReady: Deferred.succeed(parked, undefined).pipe(
                    Effect.andThen(gate.awaitCommandReady),
                  ),
                  markHttpListening: Effect.void,
                  markRunningProviderSessionsForContinuation: Effect.succeed([]),
                  clearProviderSessionContinuationMarkers: () => Effect.void,
                }),
              ),
              Layer.provide(Layer.succeed(ServerSettingsService, settings)),
              Layer.provide(
                Layer.succeed(OrchestrationEngineService, {
                  ...engine,
                  getScheduledTasks: engine.getScheduledTasks!.pipe(
                    Effect.tap(() => Deferred.succeed(reconciled, undefined)),
                  ),
                }),
              ),
            ),
          );
          expect(Context.get(schedulerContext, Scheduler)).toBeDefined();
          yield* Deferred.await(parked);
          expect(yield* Deferred.isDone(activation)).toBe(false);
          expect(yield* Deferred.isDone(reconciled)).toBe(false);
          yield* Deferred.succeed(activation, undefined);
          expect(yield* Deferred.isDone(reconciled)).toBe(false);
          yield* gate.signalCommandReady;
          yield* Deferred.await(reconciled);
        }),
      );
    }),
  );

  it.effect("RPC creator lookup failure is typed and creates no task", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory("scheduler-auth-failure-");
      yield* within(
        directory,
        Effect.gen(function* () {
          yield* createParent(directory);
          const scheduler = yield* makeLiveScheduler;
          const api = makeSchedulerRpcHandlers(
            scheduler,
            { subject: "authorized", sessionId: AuthSessionId.make("failure") },
            {
              getPerson: () =>
                Effect.fail(
                  new SessionStore.ActiveSessionsListError({
                    cause: new Error("Store unavailable"),
                  }),
                ),
            },
            (_, effect) => effect,
          );
          expect(yield* api["scheduler.create"](definition).pipe(Effect.flip)).toBeInstanceOf(
            SchedulerError,
          );
          expect(yield* scheduler.list).toHaveLength(0);
        }),
      );
    }),
  );

  for (const retired of [false, true]) {
    it.effect(
      `explicit management safely handles quiescent exhausted one-shot, retired=${retired}`,
      () =>
        Effect.gen(function* () {
          const directory = yield* temporaryDirectory("scheduler-management-");
          yield* within(
            directory,
            Effect.gen(function* () {
              yield* createParent(directory);
              const scheduler = yield* makeLiveScheduler;
              const engine = yield* OrchestrationEngineService;
              const created = yield* scheduler.create(
                {
                  ...definition,
                  schedule: {
                    kind: "once",
                    at: iso((yield* Clock.currentTimeMillis) + 60_000),
                    windowMinutes: 0,
                  },
                },
                "creator",
              );
              yield* scheduler.runNow(created.id);
              yield* scheduler.reconcile();
              const fake = fakeProvider(engine);
              yield* fake.execute("after-work");
              yield* scheduler.reconcile();
              for (const delay of RECOVERY_DELAYS) {
                yield* TestClock.adjust(delay);
                yield* scheduler.reconcile();
                yield* fake.execute("after-work");
                yield* scheduler.reconcile();
              }
              const exhausted = yield* currentTask(scheduler);
              expect(exhausted.runs[0]!.status).toBe("needs-you");
              yield* TestClock.adjust(86_400_000);
              yield* scheduler.reconcile();
              expect((yield* currentTask(scheduler)).runs).toHaveLength(1);
              expect((yield* currentTask(scheduler)).runs[0]!.status).toBe("needs-you");
              expect(
                yield* scheduler.delete(created.id, PARENT_ID).pipe(Effect.flip),
              ).toBeInstanceOf(SchedulerError);
              if (retired) {
                yield* engine.dispatch({
                  type: "thread.activity.append",
                  commandId: commandId(),
                  threadId: PARENT_ID,
                  activity: {
                    id: EventId.make("retire-exhausted"),
                    kind: "thread.subtree-retire-requested",
                    summary: "Explicit retirement",
                    payload: {},
                    tone: "info",
                    turnId: null,
                    createdAt: NOW,
                  },
                  createdAt: NOW,
                });
                const retirement = yield* engine.getThreadRetirement(PARENT_ID);
                expect(retirement?.retired).toBe(true);
                // External provider stop acknowledgment uses the exact durable receipt identity.
                yield* engine.dispatch({
                  ...session(PARENT_ID, "interrupted", null),
                  commandId: CommandId.make(retirement!.stopAckCommandId),
                });
                expect((yield* engine.getThreadRetirement(PARENT_ID))?.pendingStop).toBe(false);
                expect(
                  (yield* scheduler.inspectRun(yield* currentTask(scheduler), exhausted.runs[0]!))
                    .retired,
                ).toBe(true);
                expect(yield* scheduler.runNow(created.id).pipe(Effect.flip)).toBeInstanceOf(
                  SchedulerError,
                );
                const removed = yield* scheduler.delete(created.id, "user:explicit-delete");
                expect(removed.deleted).toBe(true);
                expect(removed.runs[0]).toMatchObject({
                  id: exhausted.runs[0]!.id,
                  status: "needs-you",
                  checkVersion: 1,
                  hasWork: true,
                });
                expect(yield* scheduler.list).toHaveLength(0);
                expect((yield* engine.getScheduledTasks ?? Effect.succeed([]))[0]!.checks).toEqual(
                  exhausted.checks,
                );
              } else {
                // A quiescent exhausted task is removable, but live approval/report/queued work is not.
                yield* engine.dispatch({
                  type: "thread.activity.append",
                  commandId: commandId(),
                  threadId: PARENT_ID,
                  activity: {
                    id: EventId.make("delete-approval"),
                    kind: "approval.requested",
                    summary: "Pending approval",
                    payload: { requestId: "delete-approval" },
                    tone: "info",
                    turnId: null,
                    createdAt: NOW,
                  },
                  createdAt: NOW,
                });
                expect(
                  yield* scheduler.delete(created.id, "user:delete").pipe(Effect.flip),
                ).toBeInstanceOf(SchedulerError);
                yield* engine.dispatch({
                  type: "thread.activity.append",
                  commandId: commandId(),
                  threadId: PARENT_ID,
                  activity: {
                    id: EventId.make("delete-approval-resolved"),
                    kind: "approval.resolved",
                    summary: "Resolved",
                    payload: { requestId: "delete-approval" },
                    tone: "info",
                    turnId: null,
                    createdAt: NOW,
                  },
                  createdAt: NOW,
                });
                const shell = Option.getOrThrow(
                  yield* (yield* ProjectionSnapshotQuery).getThreadShellById(PARENT_ID),
                );
                const child = ThreadId.make("management-child");
                yield* engine.dispatch({
                  type: "thread.create",
                  commandId: commandId(),
                  threadId: child,
                  projectId: shell.projectId,
                  title: "Pending Drafter",
                  modelSelection: shell.modelSelection,
                  runtimeMode: shell.runtimeMode,
                  interactionMode: "default",
                  branch: null,
                  worktreePath: null,
                  createdAt: NOW,
                });
                yield* engine.dispatch(session(child, "running", "child-management"));
                yield* engine.dispatch({
                  type: "thread.activity.append",
                  commandId: commandId(),
                  threadId: PARENT_ID,
                  activity: {
                    id: EventId.make("pending-child"),
                    kind: "task.started",
                    summary: "Drafter running",
                    payload: { taskId: child, status: "running" },
                    tone: "info",
                    turnId: null,
                    createdAt: NOW,
                  },
                  createdAt: NOW,
                });
                expect(
                  yield* scheduler.delete(created.id, "user:delete").pipe(Effect.flip),
                ).toBeInstanceOf(SchedulerError);
                yield* engine.dispatch(
                  session(child, "error", "child-management", "Drafter failed"),
                );
                yield* engine.dispatch({
                  type: "thread.activity.append",
                  commandId: commandId(),
                  threadId: PARENT_ID,
                  activity: {
                    id: EventId.make("pending-report"),
                    kind: "task.updated",
                    summary: "Failed child report",
                    payload: { taskId: child, status: "failed" },
                    tone: "info",
                    turnId: null,
                    createdAt: NOW,
                  },
                  createdAt: NOW,
                });
                expect(
                  yield* scheduler.delete(created.id, "user:delete").pipe(Effect.flip),
                ).toBeInstanceOf(SchedulerError);
                yield* engine.dispatch({
                  type: "thread.turn.start",
                  commandId: commandId(),
                  threadId: PARENT_ID,
                  message: {
                    messageId: MessageId.make("delete-user-turn"),
                    role: "user",
                    text: "Pending user turn",
                    attachments: [],
                  },
                  runtimeMode: shell.runtimeMode,
                  interactionMode: "default",
                  createdAt: iso(yield* Clock.currentTimeMillis),
                });
                expect(
                  yield* scheduler.delete(created.id, "user:delete").pipe(Effect.flip),
                ).toBeInstanceOf(SchedulerError);
                yield* engine.dispatch(session(PARENT_ID, "interrupted", null));
                const resumed = yield* scheduler.runNow(created.id);
                expect(resumed.runs).toHaveLength(1);
                expect(resumed.runs[0]).toMatchObject({
                  id: exhausted.runs[0]!.id,
                  threadId: PARENT_ID,
                  checkVersion: 1,
                  hasWork: true,
                  status: "retry",
                });
                yield* scheduler.reconcile();
                yield* fake.execute();
                expect(fake.sideEffects).toBe(1);
                expect(
                  yield* scheduler.delete(created.id, "user:delete").pipe(Effect.flip),
                ).toBeInstanceOf(SchedulerError);
                yield* Effect.promise(() =>
                  NodeFSP.writeFile(NodePath.join(directory, "result.txt"), "pass"),
                );
                yield* scheduler.reconcile();
                expect((yield* currentTask(scheduler)).runs[0]!.status).toBe("done");
              }
            }),
          );
        }),
    );
  }

  for (const person of ["Pauli", null]) {
    it.effect(
      `pins RPC creator ${person ?? "default Luke"} separately from audit through full SQLite restart`,
      () =>
        Effect.gen(function* () {
          const directory = yield* temporaryDirectory("scheduler-people-");
          let saved: ScheduledTask | undefined;
          let closed = 0;
          const auth = SessionStore.layer.pipe(
            Layer.provide(ServerSecretStore.layer),
            Layer.provide(
              Layer.succeed(ServerEnvironmentIdentity, {
                getEnvironmentId: Effect.succeed(EnvironmentId.make("isolated-people")),
              }),
            ),
          );
          yield* within(
            directory,
            Effect.gen(function* () {
              yield* Effect.addFinalizer(() =>
                Effect.sync(() => {
                  closed++;
                }),
              );
              yield* createParent(directory);
              const sessions = yield* SessionStore.SessionStore;
              const issued = yield* sessions.issue({ subject: "authenticated-creator" });
              if (person) yield* sessions.setPerson(issued.sessionId, person);
              const scheduler = yield* makeLiveScheduler;
              const shell = Option.getOrThrow(
                yield* (yield* ProjectionSnapshotQuery).getThreadShellById(PARENT_ID),
              );
              const api = makeSchedulerRpcHandlers(
                scheduler,
                { subject: "authenticated-creator", sessionId: issued.sessionId },
                sessions,
                (_, effect) => effect,
              );
              const raw = {
                ...definition,
                target: { kind: "new-thread" as const, projectId: shell.projectId },
                owner: "forged",
                actor: "forged",
              };
              const created = yield* api["scheduler.create"](raw);
              expect(created).toMatchObject({ owner: person ?? "Luke" });
              expect(created.checks[0]!.actor).toBe("user:authenticated-creator");
              yield* scheduler.runNow(created.id);
              yield* sessions.setPerson(issued.sessionId, person === "Pauli" ? "Luke" : "Pauli");
              const editor = yield* sessions.issue({ subject: "elevated-editor" });
              yield* sessions.setPerson(editor.sessionId, person === "Pauli" ? "Luke" : "Pauli");
              const editorApi = makeSchedulerRpcHandlers(
                scheduler,
                { subject: "elevated-editor", sessionId: editor.sessionId },
                sessions,
                (_, effect) => effect,
              );
              const edit = {
                taskId: created.id,
                checkCommand: "exit 2",
                checkReason: "Future-only change",
                owner: "Elevated",
              };
              yield* editorApi["scheduler.edit"](edit);
              expect((yield* currentTask(scheduler)).owner).toBe(person ?? "Luke");
              expect((yield* currentTask(scheduler)).checks.at(-1)!.actor).toBe(
                "user:elevated-editor",
              );
              saved = yield* currentTask(scheduler);
              expect(saved.runs[0]).toMatchObject({ owner: person ?? "Luke", checkVersion: 1 });
            }).pipe(Effect.provide(auth)),
          );
          expect(closed).toBe(1);
          yield* within(
            directory,
            Effect.gen(function* () {
              yield* Effect.addFinalizer(() =>
                Effect.sync(() => {
                  closed++;
                }),
              );
              const scheduler = yield* makeLiveScheduler;
              yield* scheduler.reconcile();
              const replayed = yield* currentTask(scheduler);
              expect(replayed.owner).toBe(person ?? "Luke");
              expect(replayed.runs[0]!.owner).toBe(person ?? "Luke");
              expect(replayed.runs[0]!.checkVersion).toBe(1);
              const shell = Option.getOrThrow(
                yield* (yield* ProjectionSnapshotQuery).getThreadShellById(
                  replayed.runs[0]!.threadId!,
                ),
              );
              expect(shell.owner).toBe(person ?? "Luke");
              const engine = yield* OrchestrationEngineService;
              for (const forged of [
                { ...replayed, owner: "Elevated" },
                { ...replayed, runs: replayed.runs.map((run) => ({ ...run, owner: "Elevated" })) },
              ]) {
                expect(
                  Exit.isFailure(
                    yield* Effect.exit(
                      engine.dispatch({
                        type: "scheduler.state.set",
                        commandId: commandId(),
                        threadId: ThreadId.make(replayed.id),
                        expectedRevision: replayed.revision,
                        task: { ...forged, revision: replayed.revision + 1 },
                        createdAt: NOW,
                      }),
                    ),
                  ),
                ).toBe(true);
              }
              expect(saved!.checks[0]!.actor).toBe("user:authenticated-creator");
            }),
          );
          expect(closed).toBe(2);
        }),
    );
  }

  it.effect("MCP creation resolves the caller thread owner rather than payload person", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory("scheduler-mcp-owner-");
      yield* within(
        directory,
        Effect.gen(function* () {
          yield* createParent(directory);
          const scheduler = yield* makeLiveScheduler;
          const engine = yield* OrchestrationEngineService;
          const parent = Option.getOrThrow(
            yield* (yield* ProjectionSnapshotQuery).getThreadShellById(PARENT_ID),
          );
          const caller = ThreadId.make("pauli-caller");
          yield* engine.dispatch({
            type: "thread.create",
            commandId: commandId(),
            threadId: caller,
            owner: "Pauli",
            projectId: parent.projectId,
            title: "Pauli caller",
            modelSelection: parent.modelSelection,
            runtimeMode: parent.runtimeMode,
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            createdAt: NOW,
          });
          const result = yield* Effect.gen(function* () {
            const toolkit = yield* SchedulerToolkit;
            return yield* toolkit
              .handle("create_scheduled_task", {
                ...definition,
                owner: "Luke",
              } as typeof definition)
              .pipe(Stream.unwrap, Stream.runCollect);
          }).pipe(
            Effect.provide(SchedulerToolkitHandlersLive),
            Effect.provideService(Scheduler, scheduler),
            Effect.provideService(McpInvocationContext, {
              environmentId: EnvironmentId.make("isolated-mcp"),
              threadId: caller,
              providerSessionId: "test",
              providerInstanceId: provider.instanceId,
              capabilities: new Set<McpCapability>(),
              issuedAt: 1,
            }),
          );
          expect(result.at(-1)!.result).toMatchObject({
            owner: "Pauli",
            checks: [{ actor: caller }],
          });
        }),
      );
    }),
  );

  it.effect(
    "retains the same pinned worked run through exhaustion, latest slot and durable restart",
    () =>
      Effect.gen(function* () {
        const directory = yield* temporaryDirectory("scheduler-exhaustion-");
        const state = {
          sideEffects: 0,
          executed: new Set<string>(),
          context: new Set<string>(),
          closed: 0,
        };
        let original: ScheduledTask | undefined;
        yield* within(
          directory,
          Effect.gen(function* () {
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                state.closed++;
              }),
            );
            yield* createParent(directory);
            const scheduler = yield* makeLiveScheduler;
            const engine = yield* OrchestrationEngineService;
            const fake = fakeProvider(engine, state);
            const created = yield* scheduler.create(
              { ...definition, schedule: { kind: "interval", minutes: 1440 } },
              "creator",
            );
            yield* scheduler.runNow(created.id);
            yield* scheduler.reconcile();
            yield* fake.execute("after-work");
            yield* scheduler.reconcile();
            for (const delay of RECOVERY_DELAYS) {
              yield* TestClock.adjust(delay);
              yield* scheduler.reconcile();
              yield* fake.execute("after-work");
              yield* scheduler.reconcile();
            }
            original = yield* currentTask(scheduler);
            expect(original.runs[0]!.status).toBe("needs-you");
            expect(fake.sideEffects).toBe(1);
            const before = yield* userTexts(engine, PARENT_ID);
            for (const kind of ["wight", "usage-limit-resume", "spectrum"]) {
              yield* engine.dispatch({
                type: "thread.turn.start",
                commandId: CommandId.make(
                  kind === "spectrum"
                    ? `server:spectrum:host:0:${PARENT_ID}:0:0:exhausted`
                    : `server:${kind}:exhausted`,
                ),
                threadId: PARENT_ID,
                message: {
                  messageId: MessageId.make(`competing-${kind}`),
                  role: "user",
                  text: "EFFECT: competing automatic wake",
                  attachments: [],
                },
                runtimeMode: "full-access",
                interactionMode: "default",
                createdAt: NOW,
              });
            }
            expect(yield* userTexts(engine, PARENT_ID)).toEqual(before);
            expect(
              yield* scheduler
                .edit(
                  { taskId: created.id, checkCommand: "exit 0", checkReason: "Cheat" },
                  PARENT_ID,
                )
                .pipe(Effect.flip),
            ).toBeInstanceOf(SchedulerError);
            for (const forged of [
              { ...original, checks: original.checks.filter((check) => check.version !== 1) },
              {
                ...original,
                runs: original.runs.map((run) => ({
                  ...run,
                  checkCwd: "/wrong",
                  definition: { ...run.definition, prompt: "EFFECT: restart" },
                })),
              },
            ])
              expect(
                Exit.isFailure(
                  yield* Effect.exit(
                    engine.dispatch({
                      type: "scheduler.state.set",
                      commandId: commandId(),
                      threadId: ThreadId.make(original.id),
                      expectedRevision: original.revision,
                      task: { ...forged, revision: original.revision + 1 },
                      createdAt: NOW,
                    }),
                  ),
                ),
              ).toBe(true);
            for (let i = 0; i < 24; i++)
              yield* scheduler.edit(
                { taskId: created.id, checkCommand: `exit ${i + 1}`, checkReason: "Future judge" },
                "other-author",
              );
            expect(
              (yield* currentTask(scheduler)).checks.some((check) => check.version === 1),
            ).toBe(true);
            original = yield* currentTask(scheduler);
          }),
        );
        expect(state.closed).toBe(1);
        yield* within(
          directory,
          Effect.gen(function* () {
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                state.closed++;
              }),
            );
            const scheduler = yield* makeLiveScheduler;
            const engine = yield* OrchestrationEngineService;
            yield* TestClock.adjust(3 * 86_400_000);
            yield* scheduler.reconcile();
            const resumed = yield* currentTask(scheduler);
            expect(resumed.runs).toHaveLength(1);
            expect(resumed.runs[0]).toMatchObject({
              id: original!.runs[0]!.id,
              threadId: PARENT_ID,
              checkVersion: 1,
              checkCwd: directory,
              definition: original!.runs[0]!.definition,
              status: "running",
            });
            expect(resumed.consumedSlot).not.toBe(original!.consumedSlot);
            const fake = fakeProvider(engine, state);
            yield* fake.execute();
            expect(fake.sideEffects).toBe(1);
            yield* Effect.promise(() =>
              NodeFSP.writeFile(NodePath.join(directory, "result.txt"), "pass"),
            );
            yield* scheduler.reconcile();
            expect((yield* currentTask(scheduler)).runs[0]!.status).toBe("done");
          }),
        );
        expect(state.closed).toBe(2);
      }),
  );

  for (const block of ["approval", "user-input", "plan", "queued-turn"] as const) {
    it.effect(`does not finish a passing run with actual ${block} after its lease expires`, () =>
      Effect.gen(function* () {
        const directory = yield* temporaryDirectory(`scheduler-blocked-${block}-`);
        yield* within(
          directory,
          Effect.gen(function* () {
            yield* createParent(directory);
            const engine = yield* OrchestrationEngineService;
            const scheduler = yield* makeLiveScheduler;
            const created = yield* scheduler.create(definition, "creator");
            yield* scheduler.runNow(created.id);
            yield* scheduler.reconcile();
            yield* fakeProvider(engine).execute();
            if (block === "plan") {
              yield* engine.dispatch({
                type: "thread.proposed-plan.upsert",
                commandId: commandId(),
                threadId: PARENT_ID,
                proposedPlan: {
                  id: OrchestrationProposedPlanId.make("pending-plan"),
                  turnId: null,
                  planMarkdown: "Approve real unfinished work",
                  implementedAt: null,
                  implementationThreadId: null,
                  createdAt: NOW,
                  updatedAt: NOW,
                },
                createdAt: NOW,
              });
            } else if (block === "queued-turn") {
              yield* engine.dispatch({
                type: "thread.turn.start",
                commandId: commandId(),
                threadId: PARENT_ID,
                message: {
                  messageId: MessageId.make("queued-user"),
                  role: "user",
                  text: "User work still waiting",
                  attachments: [],
                },
                runtimeMode: "full-access",
                interactionMode: "default",
                createdAt: iso((yield* Clock.currentTimeMillis) + 1),
              });
            } else {
              yield* engine.dispatch({
                type: "thread.activity.append",
                commandId: commandId(),
                threadId: PARENT_ID,
                activity: {
                  id: EventId.make(`pending-${block}`),
                  kind: `${block}.requested`,
                  summary: "Awaiting user",
                  payload: {
                    requestId: `pending-${block}`,
                    responseMode: "message",
                    questions: [],
                  },
                  tone: "info",
                  turnId: null,
                  createdAt: NOW,
                },
                createdAt: NOW,
              });
            }
            const shell = Option.getOrThrow(
              yield* (yield* ProjectionSnapshotQuery).getThreadShellById(PARENT_ID),
            );
            if (block === "approval") expect(shell.hasPendingApprovals).toBe(true);
            if (block === "user-input") expect(shell.hasPendingUserInput).toBe(true);
            if (block === "plan") expect(shell.hasActionableProposedPlan).toBe(true);
            yield* Effect.promise(() =>
              NodeFSP.writeFile(NodePath.join(directory, "result.txt"), "pass"),
            );
            yield* TestClock.adjust(130_000);
            yield* scheduler.reconcile();
            expect((yield* currentTask(scheduler)).runs[0]!.status).toBe("running");
            expect(yield* userTexts(engine, PARENT_ID)).toHaveLength(
              block === "queued-turn" ? 2 : 1,
            );
            yield* TestClock.adjust(130_000);
            yield* scheduler.reconcile();
            expect((yield* currentTask(scheduler)).runs[0]!.status).toBe("running");
            if (block === "plan")
              yield* engine.dispatch({
                type: "thread.proposed-plan.upsert",
                commandId: commandId(),
                threadId: PARENT_ID,
                proposedPlan: {
                  id: OrchestrationProposedPlanId.make("pending-plan"),
                  turnId: null,
                  planMarkdown: "Approve real unfinished work",
                  implementedAt: NOW,
                  implementationThreadId: PARENT_ID,
                  createdAt: NOW,
                  updatedAt: NOW,
                },
                createdAt: NOW,
              });
            else if (block === "queued-turn") {
              yield* engine.dispatch(session(PARENT_ID, "running", "processed-user"));
              yield* engine.dispatch(session(PARENT_ID, "ready", "processed-user"));
            } else
              yield* engine.dispatch({
                type: "thread.activity.append",
                commandId: commandId(),
                threadId: PARENT_ID,
                activity: {
                  id: EventId.make(`resolved-${block}`),
                  kind: `${block}.resolved`,
                  summary: "Resolved",
                  payload: { requestId: `pending-${block}` },
                  tone: "info",
                  turnId: null,
                  createdAt: NOW,
                },
                createdAt: NOW,
              });
            yield* scheduler.reconcile();
            expect((yield* currentTask(scheduler)).runs[0]!.status).toBe("done");
          }),
        );
      }),
    );
  }

  it.effect(
    "create RPC returns typed calendar failures and derives check authors from its authenticated caller",
    () =>
      Effect.gen(function* () {
        const directory = yield* temporaryDirectory("scheduler-api-");
        yield* within(
          directory,
          Effect.gen(function* () {
            yield* createParent(directory);
            const scheduler = yield* makeLiveScheduler;
            const api = makeSchedulerRpcHandlers(
              scheduler,
              { subject: "session", sessionId: AuthSessionId.make("session") },
              { getPerson: () => Effect.succeed(null) },
              (_, effect) => effect,
            );
            for (const schedule of [
              { kind: "weekly" as const, weekdays: [4], times: ["08:00"], timeZone: "Not/AZone" },
              { kind: "once" as const, at: "1969-12-31T08:00:00Z", windowMinutes: 0 },
            ]) {
              const result = yield* api["scheduler.create"]({ ...definition, schedule }).pipe(
                Effect.flip,
              );
              expect(result).toBeInstanceOf(SchedulerError);
              expect(yield* scheduler.list).toHaveLength(0);
            }
            const task = yield* api["scheduler.create"]({
              ...definition,
              actor: "forged",
            } as typeof definition);
            expect(task.checks[0]!.actor).toBe("user:session");
            const failedEdit = yield* api["scheduler.edit"]({
              taskId: task.id,
              definition: {
                ...task.definition,
                schedule: {
                  kind: "weekly",
                  weekdays: [4],
                  times: ["08:00"],
                  timeZone: "Not/AZone",
                },
              },
            }).pipe(Effect.flip);
            expect(failedEdit).toBeInstanceOf(SchedulerError);
            expect((yield* scheduler.list)[0]!.revision).toBe(task.revision);
          }),
        );
      }),
  );

  it.effect(
    "atomically rejects racing claims, rejects public forged scheduler commands, replays checks and pinned runs",
    () =>
      Effect.gen(function* () {
        const directory = yield* temporaryDirectory("scheduler-durable-");
        let saved: ScheduledTask | undefined;
        yield* within(
          directory,
          Effect.gen(function* () {
            yield* createParent(directory);
            const scheduler = yield* makeLiveScheduler;
            const engine = yield* OrchestrationEngineService;
            const task = yield* scheduler.create(definition, "user:authenticated");
            expect(task.checkCwd).toBe(directory);
            const raced = yield* Effect.all(
              [
                Effect.exit(scheduler.runNow(task.id)),
                Effect.exit((yield* makeLiveScheduler).runNow(task.id)),
              ],
              { concurrency: "unbounded" },
            );
            expect(raced.filter(Exit.isSuccess)).toHaveLength(1);
            saved = yield* currentTask(scheduler);
            const command = {
              type: "scheduler.state.set" as const,
              commandId: commandId(),
              threadId: ThreadId.make(saved.id),
              expectedRevision: saved.revision,
              task: { ...saved, revision: saved.revision + 1 },
              createdAt: NOW,
            };
            expect(decodeClientCommand(command)).toEqual(Option.none());
            expect(Option.isSome(decodeSchedulerCommand(command))).toBe(true);
            expect(
              Exit.isFailure(
                yield* Effect.exit(
                  engine.dispatch({
                    ...command,
                    commandId: commandId(),
                    task: {
                      ...command.task,
                      runs: command.task.runs.map((run) => ({ ...run, checkVersion: 999 })),
                    },
                  }),
                ),
              ),
            ).toBe(true);
            yield* scheduler.edit(
              { taskId: task.id, checkCommand: "test -f fixed.txt", checkReason: "Correct judge" },
              "other-author",
            );
            const history = yield* scheduler.checkHistory({ taskId: task.id });
            expect(history.map((check) => check.actor)).toEqual([
              "other-author",
              "user:authenticated",
            ]);
            saved = yield* currentTask(scheduler);
          }),
        );
        yield* within(
          directory,
          Effect.gen(function* () {
            const scheduler = yield* makeLiveScheduler;
            const replayed = yield* currentTask(scheduler);
            expect(replayed).toEqual(saved);
            expect(replayed.runs[0]!.checkVersion).toBe(1);
          }),
        );
      }),
  );

  for (const failure of ["before-output", "after-work"] as const) {
    it.effect(
      `does not repeat a fake provider side effect across ${failure}, receipt replay and continuation`,
      () =>
        Effect.gen(function* () {
          const directory = yield* temporaryDirectory(`scheduler-${failure}-`);
          yield* within(
            directory,
            Effect.gen(function* () {
              yield* createParent(directory);
              let scheduler = yield* makeLiveScheduler;
              const engine = yield* OrchestrationEngineService;
              const fake = fakeProvider(engine);
              const task = yield* scheduler.create(definition, "creator");
              yield* scheduler.runNow(task.id);
              yield* Effect.all([scheduler.reconcile(), scheduler.reconcile()], {
                concurrency: "unbounded",
              });
              yield* fake.execute(failure);
              expect(fake.sideEffects).toBe(failure === "before-output" ? 0 : 1);
              const inspected = yield* scheduler.inspectRun(
                yield* currentTask(scheduler),
                (yield* currentTask(scheduler)).runs[0]!,
              );
              expect(inspected.hasWork).toBe(failure === "after-work"); // No assistant text in either path.
              yield* scheduler.reconcile();
              for (const [index, delay] of RECOVERY_DELAYS.entries()) {
                yield* TestClock.adjust(delay);
                yield* scheduler.reconcile();
                yield* fake.execute(index === RECOVERY_DELAYS.length - 1 ? "completed" : failure);
                expect(fake.sideEffects).toBe(
                  failure === "before-output" && index < RECOVERY_DELAYS.length - 1 ? 0 : 1,
                );
                if (index < RECOVERY_DELAYS.length - 1) yield* scheduler.reconcile();
              }
              const run = (yield* currentTask(scheduler)).runs[0]!;
              expect(run.threadId).toBe(PARENT_ID);
              const receipts = yield* OrchestrationCommandReceiptRepository;
              const id = CommandId.make(`server:scheduler-turn:${run.id}:${run.sendIndex}`);
              expect(Option.isSome(yield* receipts.getByCommandId({ commandId: id }))).toBe(true);
              const shell = Option.getOrThrow(
                yield* (yield* ProjectionSnapshotQuery).getThreadShellById(PARENT_ID),
              );
              const sender = makeThreadTurnSender({
                dispatch: engine.dispatch,
                commandId: Effect.succeed(id),
                messageId: Effect.succeed(MessageId.make("duplicate")),
                now: Effect.succeed(NOW),
              });
              yield* sender(shell, "EFFECT: duplicate must never reach provider", id);
              yield* fake.execute();
              expect(fake.sideEffects).toBe(1);
              yield* Effect.promise(() =>
                NodeFSP.writeFile(NodePath.join(directory, "result.txt"), "verified"),
              );
              yield* scheduler.reconcile();
              expect((yield* currentTask(scheduler)).runs[0]!.status).toBe("done");
            }),
          );
        }),
    );
  }
  it.effect(
    "disposes and rebuilds the engine and scheduler after ambiguous execution, preserving provider context",
    () =>
      Effect.gen(function* () {
        const directory = yield* temporaryDirectory("scheduler-real-restart-");
        const state = {
          sideEffects: 0,
          executed: new Set<string>(),
          context: new Set<string>(),
          closed: 0,
        };
        let claimed: ScheduledTask | undefined;
        yield* within(
          directory,
          Effect.gen(function* () {
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                state.closed++;
              }),
            );
            yield* createParent(directory);
            const scheduler = yield* makeLiveScheduler;
            yield* scheduler.start;
            const engine = yield* OrchestrationEngineService;
            const task = yield* scheduler.create(definition, "creator");
            yield* scheduler.runNow(task.id);
            yield* scheduler.reconcile();
            yield* fakeProvider(engine, state).execute("ambiguous-crash");
            claimed = yield* currentTask(scheduler);
            expect(claimed.runs[0]!.hasWork).toBe(false);
            expect(state.sideEffects).toBe(1);
          }),
        );
        expect(state.closed).toBe(1);
        yield* within(
          directory,
          Effect.gen(function* () {
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                state.closed++;
              }),
            );
            const scheduler = yield* makeLiveScheduler;
            yield* scheduler.start;
            const engine = yield* OrchestrationEngineService;
            expect((yield* currentTask(scheduler)).runs[0]!.id).toBe(claimed!.runs[0]!.id);
            yield* scheduler.reconcile();
            yield* TestClock.adjust(30_000);
            yield* scheduler.reconcile();
            yield* Effect.promise(() =>
              NodeFSP.writeFile(NodePath.join(directory, "result.txt"), "verified"),
            );
            yield* fakeProvider(engine, state).execute();
            expect(state.sideEffects).toBe(1);
            expect((yield* currentTask(scheduler)).runs[0]!.threadId).toBe(PARENT_ID);
            expect((yield* userTexts(engine, PARENT_ID)).at(-1)).toContain("provider disconnected");
            yield* scheduler.reconcile();
            expect((yield* currentTask(scheduler)).runs[0]!.status).toBe("done");
          }),
        );
        expect(state.closed).toBe(2);
      }),
  );
  it.effect(
    "usage-limit reset has one owner and a fresh continuation receipt with pinned context",
    () =>
      Effect.gen(function* () {
        const directory = yield* temporaryDirectory("scheduler-reset-");
        yield* within(
          directory,
          Effect.gen(function* () {
            yield* createParent(directory);
            const registry = yield* ProviderRegistry;
            let used = 100;
            const scheduler = yield* makeLiveScheduler.pipe(
              Effect.provideService(ProviderRegistry, {
                ...registry,
                getProviders: Effect.sync(() => [
                  {
                    ...provider,
                    usageLimits: {
                      checkedAt: NOW,
                      windows: [
                        {
                          id: "session",
                          kind: "session" as const,
                          label: "Session",
                          usedPercent: used,
                          resetsAt: "1970-01-01T00:10:00Z",
                        },
                      ],
                    },
                  },
                ]),
              }),
            );
            used = 0;
            const engine = yield* OrchestrationEngineService;
            const fake = fakeProvider(engine);
            const task = yield* scheduler.create(definition, "creator");
            yield* scheduler.runNow(task.id);
            yield* scheduler.reconcile();
            yield* fake.execute("after-work");
            used = 100;
            yield* engine.dispatch(
              session(PARENT_ID, "error", "turn-1", "Codex usage limit reached"),
            );
            yield* scheduler.reconcile();
            expect((yield* currentTask(scheduler)).runs[0]!.status).toBe("usage-limit");
            yield* TestClock.adjust(700_000);
            yield* scheduler.reconcile();
            expect(yield* userTexts(engine, PARENT_ID)).toHaveLength(1);
            used = 0;
            yield* scheduler.reconcile();
            yield* fake.execute();
            expect(fake.sideEffects).toBe(1);
            const run = (yield* currentTask(scheduler)).runs[0]!;
            expect(run.sendIndex).toBe(2);
            expect({ attempt: run.attempt, error: run.error }).toEqual({
              attempt: 0,
              error: "Codex usage limit reached",
            });
            expect((yield* userTexts(engine, PARENT_ID)).at(-1)).toContain(
              "Immutable outcome check version 1",
            );
          }),
        );
      }),
  );
  it.effect(
    "normal turn interruption permits continuation; retired threads and competing automation cannot start",
    () =>
      Effect.gen(function* () {
        const directory = yield* temporaryDirectory("scheduler-admission-");
        yield* within(
          directory,
          Effect.gen(function* () {
            yield* createParent(directory);
            const scheduler = yield* makeLiveScheduler;
            const engine = yield* OrchestrationEngineService;
            const fake = fakeProvider(engine);
            const task = yield* scheduler.create(definition, "user");
            yield* scheduler.runNow(task.id);
            yield* scheduler.reconcile();
            yield* fake.execute("after-work");
            yield* engine.dispatch(session(PARENT_ID, "running", "turn-1"));
            yield* TestClock.adjust(300_000);
            yield* scheduler.reconcile();
            expect((yield* engine.getThreadRetirement(PARENT_ID))?.retired ?? false).toBe(false);
            const events = yield* engine
              .readEvents(0, Number.MAX_SAFE_INTEGER)
              .pipe(Stream.runCollect);
            expect(events.some((event) => event.type === "thread.turn-interrupt-requested")).toBe(
              true,
            );
            yield* engine.dispatch(session(PARENT_ID, "interrupted", "turn-1"));
            const observation = yield* scheduler.inspectRun(
              yield* currentTask(scheduler),
              (yield* currentTask(scheduler)).runs[0]!,
            );
            expect(observation.idle).toBe(true);
            expect(observation.retired).toBe(false);
            yield* TestClock.adjust(30_000);
            yield* scheduler.reconcile();
            expect((yield* userTexts(engine, PARENT_ID)).at(-1)).toContain("Provider stalled");
            yield* fake.execute();
            expect(fake.sideEffects).toBe(1);
            const shell = Option.getOrThrow(
              yield* (yield* ProjectionSnapshotQuery).getThreadShellById(PARENT_ID),
            );
            const sender = makeThreadTurnSender({
              dispatch: engine.dispatch,
              commandId: Effect.succeed(CommandId.make("server:wight:competing")),
              messageId: Effect.succeed(MessageId.make("competing")),
              now: Effect.succeed(NOW),
            });
            yield* engine.dispatch(session(PARENT_ID, "ready", "continued"));
            const before = (yield* userTexts(engine, PARENT_ID)).length;
            yield* sender(shell, "competing legacy continuation");
            expect((yield* userTexts(engine, PARENT_ID)).length).toBe(before);
            yield* engine.dispatch({
              type: "thread.activity.append",
              commandId: commandId(),
              threadId: PARENT_ID,
              activity: {
                id: EventId.make("retire"),
                kind: "thread.subtree-retire-requested",
                tone: "info",
                summary: "Retire subtree",
                payload: {},
                turnId: null,
                createdAt: NOW,
              },
              createdAt: NOW,
            });
            yield* scheduler.reconcile();
            expect((yield* currentTask(scheduler)).runs[0]!.status).toBe("needs-you");
          }),
        );
      }),
  );

  it.effect(
    "real child lifecycle records failed Drafters without blocking parent recovery or competing report turns",
    () =>
      Effect.gen(function* () {
        const directory = yield* temporaryDirectory("scheduler-children-");
        yield* Effect.scoped(
          withServer(
            NodePath.join(directory, "state.sqlite"),
            Effect.gen(function* () {
              yield* createParent(directory);
              const scheduler = yield* makeLiveScheduler.pipe(
                Effect.provide(
                  Layer.mergeAll(
                    ServerSettingsService.layerTest(preferences),
                    makeProviderRegistryLayer([provider]),
                    ProcessRunner.layer,
                  ),
                ),
              );
              const engine = yield* OrchestrationEngineService;
              const task = yield* scheduler.create(definition, "creator");
              yield* scheduler.runNow(task.id);
              yield* scheduler.reconcile();
              yield* engine.dispatch(session(PARENT_ID, "running", "parent-turn"));
              const { result: spawned } = yield* dispatchUntil(
                callTool("spawn_thread", { task: "Drafter contribution", reportBack: true }),
                (event) =>
                  event.type === "thread.activity-appended" &&
                  event.payload.activity.kind === "task.started",
              );
              const child = ThreadId.make(spawned.threadId);
              yield* engine.dispatch(session(child, "running", "child-turn"));
              yield* dispatchUntil(
                engine.dispatch(session(child, "error", "child-turn", "child failed")),
                parentActivity(child, "task.updated", "failed"),
              );
              const inspection = yield* scheduler.inspectRun(
                yield* currentTask(scheduler),
                (yield* currentTask(scheduler)).runs[0]!,
              );
              expect(inspection.drafterIds).toContain(child);
              expect(inspection.pendingDrafters).toBe(false);
              expect(inspection.hasWork).toBe(true);
              yield* engine.dispatch(session(PARENT_ID, "ready", "parent-turn"));
              yield* scheduler.reconcile();
              yield* dispatchAll(
                assistantReply(child, "child-reply", "Drafter result survives restart"),
              );
              const before = (yield* userTexts(engine, PARENT_ID)).length;
              yield* dispatchUntil(
                engine.dispatch(session(child, "ready", "child-turn")),
                parentActivity(child, "task.progress", "idle"),
              );
              expect((yield* userTexts(engine, PARENT_ID)).length).toBe(before);
            }).pipe(
              Effect.provide(makeSqlitePersistenceLive(NodePath.join(directory, "state.sqlite"))),
            ),
            undefined,
            { settings: preferences },
          ),
        );
        yield* within(
          directory,
          Effect.gen(function* () {
            const scheduler = yield* makeLiveScheduler;
            const engine = yield* OrchestrationEngineService;
            yield* TestClock.adjust(30_000);
            yield* scheduler.reconcile();
            expect((yield* userTexts(engine, PARENT_ID)).at(-1)).toContain(
              "Drafter result survives restart",
            );
            expect((yield* currentTask(scheduler)).runs[0]!.drafterIds).toHaveLength(1);
            yield* engine.dispatch(session(PARENT_ID, "running", "report-context"));
            yield* engine.dispatch(session(PARENT_ID, "ready", "report-context"));
            yield* scheduler.reconcile();
            yield* TestClock.adjust(60_000);
            yield* scheduler.reconcile();
            expect((yield* userTexts(engine, PARENT_ID)).at(-1)).not.toContain(
              "Drafter result survives restart",
            );
            expect(
              (yield* userTexts(engine, PARENT_ID)).filter((text) =>
                text.includes("Drafter result survives restart"),
              ),
            ).toHaveLength(1);
          }),
        );
      }),
  );
  it.effect("a quota-wait Drafter prevents done until it resumes and settles", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory("scheduler-child-quota-");
      yield* withServer(
        NodePath.join(directory, "state.sqlite"),
        Effect.gen(function* () {
          yield* createParent(directory);
          const scheduler = yield* makeLiveScheduler.pipe(
            Effect.provide(
              Layer.mergeAll(
                ServerSettingsService.layerTest(preferences),
                makeProviderRegistryLayer([provider]),
                ProcessRunner.layer,
              ),
            ),
          );
          const engine = yield* OrchestrationEngineService;
          const task = yield* scheduler.create(definition, "creator");
          yield* scheduler.runNow(task.id);
          yield* scheduler.reconcile();
          yield* engine.dispatch(session(PARENT_ID, "running", "parent-turn"));
          const { result: spawned } = yield* dispatchUntil(
            callTool("spawn_thread", { task: "Drafter contribution", reportBack: true }),
            (event) =>
              event.type === "thread.activity-appended" &&
              event.payload.activity.kind === "task.started",
          );
          const child = ThreadId.make(spawned.threadId);
          yield* engine.dispatch(session(child, "running", "child-turn"));
          yield* dispatchUntil(
            engine.dispatch(session(child, "error", "child-turn", "Codex usage limit reached")),
            parentActivity(child, "task.updated", "failed"),
          );
          yield* Effect.promise(() =>
            NodeFSP.writeFile(NodePath.join(directory, "result.txt"), "verified"),
          );
          yield* engine.dispatch(session(PARENT_ID, "ready", "parent-turn"));
          yield* scheduler.reconcile();
          const waiting = (yield* currentTask(scheduler)).runs[0]!;
          expect(waiting.check?.passed).toBe(true);
          expect(waiting.status).toBe("running");
          expect(
            (yield* scheduler.inspectRun(yield* currentTask(scheduler), waiting)).pendingDrafters,
          ).toBe(true);
          yield* engine.dispatch(session(child, "running", "resumed-child"));
          yield* dispatchAll(assistantReply(child, "child-result", "Quota recovery completed"));
          yield* dispatchUntil(
            engine.dispatch(session(child, "ready", "resumed-child")),
            parentActivity(child, "task.progress", "idle"),
          );
          yield* scheduler.reconcile();
          expect((yield* currentTask(scheduler)).runs[0]!.status).toBe("running");
          expect((yield* userTexts(engine, PARENT_ID)).at(-1)).toContain(
            "Quota recovery completed",
          );
          yield* engine.dispatch(session(PARENT_ID, "running", "quota-report-context"));
          yield* engine.dispatch(session(PARENT_ID, "ready", "quota-report-context"));
          yield* scheduler.reconcile();
          expect((yield* currentTask(scheduler)).runs[0]!.status).toBe("done");
        }).pipe(
          Effect.provide(makeSqlitePersistenceLive(NodePath.join(directory, "state.sqlite"))),
        ),
        undefined,
        { settings: preferences },
      );
    }),
  );
});
describe("bounded output", () => {
  it("caps UTF-8 check output bytes", () => {
    expect(Buffer.byteLength(truncateCheckOutput("x".repeat(100_000)))).toBe(CHECK_OUTPUT_BYTES);
    expect(Buffer.byteLength(truncateCheckOutput("x" + "😀".repeat(100_000)))).toBeLessThanOrEqual(
      CHECK_OUTPUT_BYTES,
    );
  });
});
