// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";
import * as NodeFSP from "node:fs/promises";
import { describe, expect, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  ClientOrchestrationCommand,
  SchedulerStateCommand,
  SchedulerError,
  DEFAULT_PRISM_ROLE_KITS,
  type ServerProvider,
  type ScheduledTask,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { makeSchedulerRpcHandlers } from "./rpcHandlers.ts";
import { RECOVERY_DELAYS } from "./Scheduler.ts";
import { TestClock } from "effect/testing";
import { OrchestrationLayerLive } from "../orchestration/runtimeLayer.ts";
import { OrchestrationEngineService } from "../orchestration/Services/OrchestrationEngine.ts";
import { OrchestrationCommandReceiptRepository } from "../persistence/Services/OrchestrationCommandReceipts.ts";
import { makeSqlitePersistenceLive } from "../persistence/Layers/Sqlite.ts";
import { ServerConfig } from "../config.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import * as ThreadPlanProgress from "../orchestration/ThreadPlanProgress.ts";
import { ServerSettingsService } from "../serverSettings.ts";
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
            const api = makeSchedulerRpcHandlers(scheduler, "user:session", (_, effect) => effect);
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
