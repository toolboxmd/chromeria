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
  TurnId,
  SchedulerError,
  DEFAULT_PRISM_ROLE_KITS,
  type OrchestrationEvent,
  type ServerProvider,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { TestClock } from "effect/testing";
import { RECOVERY_DELAYS } from "./Scheduler.ts";
import { makeLiveScheduler } from "./Service.ts";
import { SpectrumState, makeSpectrum } from "../mcp/toolkits/threads/spectrum.ts";
import {
  temporaryDirectory,
  createParent,
  PARENT_ID,
  NOW,
  commandId,
  session,
  dispatchAll,
  dispatchUntil,
  callTool,
  withServer,
} from "../mcp/toolkits/threads/handlers.testFixtures.ts";
import { OrchestrationEngineService } from "../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import { ServerSettingsService } from "../serverSettings.ts";
import { makeProviderRegistryLayer } from "../provider/testUtils/providerRegistryMock.ts";
import * as ProcessRunner from "../processRunner.ts";
import { makeSqlitePersistenceLive } from "../persistence/Layers/Sqlite.ts";
import { makeThreadTurnSender } from "../mcp/toolkits/threads/sendThreadTurn.ts";

const provider: ServerProvider = {
  instanceId: ProviderInstanceId.make("codex"),
  driver: ProviderDriverKind.make("codex"),
  enabled: true,
  installed: true,
  version: null,
  status: "ready",
  auth: { status: "authenticated" },
  checkedAt: NOW,
  models: [{ slug: "role-model", name: "Role", isCustom: false, capabilities: null }],
  slashCommands: [],
  skills: [],
};
const preferences = {
  prismRoles: {
    ...DEFAULT_PRISM_ROLE_KITS,
    worker: {
      ...DEFAULT_PRISM_ROLE_KITS.worker,
      lanes: {
        easy: [],
        hard: [],
        medium: [{ instanceId: provider.instanceId, model: "role-model" }],
      },
    },
  },
};
const withRuntime = <A, E, R>(
  directory: string,
  body: Effect.Effect<A, E, R>,
  beforeDispatch?: Parameters<typeof withServer>[4],
) =>
  Effect.scoped(
    withServer(
      NodePath.join(directory, "state.sqlite"),
      body.pipe(
        Effect.provide(
          Layer.mergeAll(
            ServerSettingsService.layerTest(preferences),
            makeProviderRegistryLayer([provider]),
            ProcessRunner.layer,
          ),
        ),
      ),
      undefined,
      { settings: preferences, providers: [provider] },
      beforeDispatch,
    ).pipe(
      Effect.provide(
        makeSqlitePersistenceLive(NodePath.join(directory, "state.sqlite")).pipe(
          Layer.provide(NodeServices.layer),
        ),
      ),
    ),
  );
const definition = {
  title: "Scheduled council",
  prompt: "INITIAL SIDE EFFECT",
  target: { kind: "thread" as const, threadId: PARENT_ID },
  role: "worker" as const,
  schedule: { kind: "interval" as const, minutes: 60 },
  checkCommand: "test -f result.txt",
  checkReason: "Deliverable exists",
};
const spectrumInput = {
  question: "Collect the full result",
  mode: "free" as const,
  limit: 2,
  colors: [
    { label: "Blue", model: "role-model" },
    { label: "Red", model: "role-model" },
  ],
};
const decodeState = Schema.decodeUnknownSync(SpectrumState);
const task = (scheduler: Effect.Success<typeof makeLiveScheduler>) =>
  scheduler.list.pipe(Effect.map((tasks) => tasks[0]!));
const events = (engine: OrchestrationEngineService["Service"]) =>
  engine.readEvents(0, Number.MAX_SAFE_INTEGER).pipe(Stream.runCollect);
const parentTexts = (engine: OrchestrationEngineService["Service"]) =>
  events(engine).pipe(
    Effect.map((rows) =>
      rows
        .filter(
          (event) =>
            event.type === "thread.message-sent" &&
            event.aggregateId === PARENT_ID &&
            event.payload.role === "user",
        )
        .map((event) => (event.type === "thread.message-sent" ? event.payload.text : "")),
    ),
  );
const spectrumState = (id: string) =>
  Effect.gen(function* () {
    const rows = yield* (yield* ProjectionSnapshotQuery).listActivitiesByKind("spectrum.state");
    return rows
      .map((row) => decodeState(row.payload))
      .filter((state) => state.id === id)
      .toSorted((a, b) => b.revision - a.revision)[0]!;
  });
const stateIs =
  (id: string, test: (state: SpectrumState) => boolean) => (event: OrchestrationEvent) =>
    event.aggregateId === id &&
    event.type === "thread.activity-appended" &&
    event.payload.activity.kind === "spectrum.state" &&
    test(decodeState(event.payload.activity.payload));
const answer = (pending: SpectrumState["pending"][number], turn: string, text: string) =>
  dispatchAll([
    {
      type: "thread.activity.append",
      commandId: commandId(),
      threadId: ThreadId.make(pending.threadId),
      activity: {
        id: EventId.make(`bind-${turn}`),
        kind: "spectrum.turn-bound",
        summary: "Bound",
        tone: "info",
        payload: { messageId: pending.messageId, turnId: turn },
        turnId: TurnId.make(turn),
        createdAt: NOW,
      },
      createdAt: NOW,
    },
    session(ThreadId.make(pending.threadId), "running", turn),
    {
      type: "thread.message.assistant.delta",
      commandId: commandId(),
      threadId: ThreadId.make(pending.threadId),
      messageId: MessageId.make(`reply-${turn}`),
      turnId: TurnId.make(turn),
      delta: text,
      createdAt: NOW,
    },
    {
      type: "thread.message.assistant.complete",
      commandId: commandId(),
      threadId: ThreadId.make(pending.threadId),
      messageId: MessageId.make(`reply-${turn}`),
      turnId: TurnId.make(turn),
      createdAt: NOW,
    },
    session(ThreadId.make(pending.threadId), "ready", null),
  ]);

const executeParent = (
  engine: OrchestrationEngineService["Service"],
  history: { executed: Set<string>; work: number; reports: number },
  failed = false,
) =>
  Effect.gen(function* () {
    const rows = yield* events(engine);
    for (const event of rows) {
      if (
        event.type !== "thread.turn-start-requested" ||
        event.aggregateId !== PARENT_ID ||
        !event.commandId?.startsWith("server:scheduler-turn:") ||
        history.executed.has(event.commandId)
      )
        continue;
      history.executed.add(event.commandId);
      const message = rows.find(
        (row) =>
          row.type === "thread.message-sent" && row.payload.messageId === event.payload.messageId,
      );
      const text = message?.type === "thread.message-sent" ? message.payload.text : "";
      if (text.includes("INITIAL SIDE EFFECT")) history.work++;
      if (text.includes("FULL SPECTRUM RESULT")) history.reports++;
      const turn = `parent-${history.executed.size}`;
      yield* engine.dispatch(session(PARENT_ID, "running", turn));
      yield* engine.dispatch({
        type: "thread.activity.append",
        commandId: commandId(),
        threadId: PARENT_ID,
        activity: {
          id: EventId.make(`parent-tool-${turn}`),
          kind: "tool.started",
          summary: "Provider processed work/context",
          payload: {},
          tone: "info",
          turnId: TurnId.make(turn),
          createdAt: NOW,
        },
        createdAt: NOW,
      });
      yield* engine.dispatch(
        session(
          PARENT_ID,
          failed ? "error" : "ready",
          turn,
          failed ? "provider disconnected" : null,
        ),
      );
    }
  });

describe("scheduler Spectrum integration", () => {
  it.effect(
    "retains Spectrum ownership and pending report context while its parent needs-you",
    () =>
      Effect.gen(function* () {
        const directory = yield* temporaryDirectory("scheduler-spectrum-exhausted-");
        yield* withRuntime(
          directory,
          Effect.gen(function* () {
            yield* createParent(directory);
            const scheduler = yield* makeLiveScheduler;
            const engine = yield* OrchestrationEngineService;
            const created = yield* scheduler.create(
              { ...definition, schedule: { kind: "interval", minutes: 1440 } },
              "creator",
            );
            yield* scheduler.runNow(created.id);
            yield* scheduler.reconcile();
            const history = { executed: new Set<string>(), work: 0, reports: 0 };
            yield* executeParent(engine, history, true);
            yield* scheduler.reconcile();
            for (const delay of RECOVERY_DELAYS) {
              yield* TestClock.adjust(delay);
              yield* scheduler.reconcile();
              yield* executeParent(engine, history, true);
              yield* scheduler.reconcile();
            }
            expect((yield* task(scheduler)).runs[0]!.status).toBe("needs-you");
            const before = yield* parentTexts(engine);
            const started = yield* callTool("start_spectrum", { ...spectrumInput, limit: 1 });
            expect(
              yield* scheduler.delete(created.id, "user:delete").pipe(Effect.flip),
            ).toBeInstanceOf(SchedulerError);
            const state = yield* spectrumState(started.threadId);
            yield* dispatchUntil(
              answer(state.pending[0]!, "exhausted-color", "FULL SPECTRUM RESULT after exhaustion"),
              (event) => event.type === "thread.settled" && event.aggregateId === started.threadId,
            );
            expect(yield* parentTexts(engine)).toEqual(before);
            expect(
              yield* scheduler.delete(created.id, "user:delete").pipe(Effect.flip),
            ).toBeInstanceOf(SchedulerError);
            yield* scheduler.runNow(created.id);
            yield* scheduler.reconcile();
            expect((yield* parentTexts(engine)).at(-1)).toContain(
              "FULL SPECTRUM RESULT after exhaustion",
            );
            yield* executeParent(engine, history);
            expect(history.work).toBe(1);
            expect(history.reports).toBe(1);
            yield* Effect.promise(() =>
              NodeFSP.writeFile(NodePath.join(directory, "result.txt"), "pass"),
            );
            yield* scheduler.reconcile();
            expect((yield* task(scheduler)).runs[0]!.status).toBe("done");
          }),
        );
      }),
  );

  it.effect(
    "uses the task role's first Color for an idle existing target before its first provider request",
    () =>
      Effect.gen(function* () {
        const directory = yield* temporaryDirectory("scheduler-role-");
        yield* withRuntime(
          directory,
          Effect.gen(function* () {
            yield* createParent(directory);
            const engine = yield* OrchestrationEngineService;
            yield* engine.dispatch({
              type: "thread.meta.update",
              commandId: commandId(),
              threadId: PARENT_ID,
              modelSelection: {
                instanceId: ProviderInstanceId.make("old-provider"),
                model: "old-model",
              },
            });
            const scheduler = yield* makeLiveScheduler;
            const created = yield* scheduler.create(definition, "user");
            yield* scheduler.runNow(created.id);
            yield* scheduler.reconcile();
            const shell = Option.getOrThrow(
              yield* (yield* ProjectionSnapshotQuery).getThreadShellById(PARENT_ID),
            );
            expect(shell.modelSelection).toEqual({
              instanceId: provider.instanceId,
              model: "role-model",
            });
            expect(
              (yield* events(engine)).filter(
                (event) =>
                  event.type === "thread.turn-start-requested" && event.aggregateId === PARENT_ID,
              ),
            ).toHaveLength(1);
            expect((yield* task(scheduler)).runs[0]!.threadId).toBe(PARENT_ID);
          }),
        );
      }),
  );
  it.effect("rejects providerless Spectrum transcript targets through create and edit", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory("scheduler-spectrum-target-");
      yield* withRuntime(
        directory,
        Effect.gen(function* () {
          yield* createParent(directory);
          const spectrum = yield* callTool("start_spectrum", spectrumInput);
          const scheduler = yield* makeLiveScheduler;
          const invalid = {
            ...definition,
            target: { kind: "thread" as const, threadId: ThreadId.make(spectrum.threadId) },
          };
          expect(yield* scheduler.create(invalid, "user").pipe(Effect.flip)).toBeInstanceOf(
            SchedulerError,
          );
          const created = yield* scheduler.create(definition, "user");
          expect(
            yield* scheduler
              .edit({ taskId: created.id, definition: invalid }, "user")
              .pipe(Effect.flip),
          ).toBeInstanceOf(SchedulerError);
          expect((yield* task(scheduler)).definition).toMatchObject({ target: definition.target });
        }),
      );
    }),
  );
  it.effect(
    "holds a passing check while a settled Spectrum outbox awaits its durable handoff",
    () =>
      Effect.gen(function* () {
        const directory = yield* temporaryDirectory("scheduler-spectrum-handoff-");
        const reached = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        yield* withRuntime(
          directory,
          Effect.gen(function* () {
            yield* createParent(directory);
            const scheduler = yield* makeLiveScheduler;
            const engine = yield* OrchestrationEngineService;
            const created = yield* scheduler.create(definition, "user");
            yield* scheduler.runNow(created.id);
            yield* scheduler.reconcile();
            yield* executeParent(engine, { executed: new Set(), work: 0, reports: 0 });
            const started = yield* callTool("start_spectrum", { ...spectrumInput, limit: 1 });
            const current = yield* spectrumState(started.threadId);
            yield* answer(
              current.pending[0]!,
              "handoff-color",
              "Settled result waits for its durable handoff",
            );
            yield* Deferred.await(reached);
            yield* Effect.promise(() =>
              NodeFSP.writeFile(NodePath.join(directory, "result.txt"), "pass"),
            );
            yield* scheduler.reconcile();
            expect((yield* task(scheduler)).runs[0]!.check?.passed).toBe(true);
            expect((yield* task(scheduler)).runs[0]!.status).toBe("running");
            expect(yield* parentTexts(engine)).toHaveLength(1);
            yield* dispatchUntil(
              Deferred.succeed(release, undefined),
              (event) => event.type === "thread.settled" && event.aggregateId === started.threadId,
            );
            yield* scheduler.reconcile();
            expect((yield* parentTexts(engine)).at(-1)).toContain(
              "Settled result waits for its durable handoff",
            );
            expect((yield* task(scheduler)).runs[0]!.status).toBe("running");
          }),
          (command) =>
            command.type === "thread.activity.append" &&
            command.activity.kind === "scheduler.spectrum-report"
              ? Deferred.succeed(reached, undefined).pipe(
                  Effect.andThen(Deferred.await(release)),
                  Effect.asVoid,
                )
              : Effect.void,
        );
      }),
  );
  it.effect(
    "gates done on Spectrum participants and delivers full context once through restart, receipt replay and later retries",
    () =>
      Effect.gen(function* () {
        const directory = yield* temporaryDirectory("scheduler-spectrum-restart-");
        const history = { executed: new Set<string>(), work: 0, reports: 0, closed: 0 };
        const full = `FULL SPECTRUM RESULT\n${"verbatim 🟦\n".repeat(900)}END FULL RESULT`;
        let spectrumId = "";
        yield* withRuntime(
          directory,
          Effect.gen(function* () {
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                history.closed++;
              }),
            );
            yield* createParent(directory);
            const scheduler = yield* makeLiveScheduler;
            const engine = yield* OrchestrationEngineService;
            const created = yield* scheduler.create(definition, "user");
            yield* scheduler.runNow(created.id);
            yield* scheduler.reconcile();
            yield* executeParent(engine, history);
            const started = yield* callTool("start_spectrum", spectrumInput);
            spectrumId = started.threadId;
            yield* Effect.promise(() =>
              NodeFSP.writeFile(NodePath.join(directory, "result.txt"), "pass"),
            );
            yield* scheduler.reconcile();
            const waiting = (yield* task(scheduler)).runs[0]!;
            expect(waiting.check?.passed).toBe(true);
            expect(waiting.status).toBe("running");
            expect(
              (yield* scheduler.inspectRun(yield* task(scheduler), waiting)).pendingDrafters,
            ).toBe(true);
            expect(waiting.drafterIds).toEqual(
              expect.arrayContaining(started.participants.map((child) => child.threadId)),
            );
            const first = yield* spectrumState(spectrumId);
            yield* dispatchUntil(
              answer(first.pending[0]!, "blue", "Blue full contribution"),
              stateIs(
                spectrumId,
                (state) => state.step === 1 && state.pending[0]?.requested === true,
              ),
            );
            const second = yield* spectrumState(spectrumId);
            yield* dispatchUntil(
              answer(second.pending[0]!, "red", full),
              (event) => event.type === "thread.settled" && event.aggregateId === spectrumId,
            );
            expect(yield* parentTexts(engine)).toHaveLength(1); // Spectrum never competes for the scheduled caller.
          }),
        );
        expect(history.closed).toBe(1);
        yield* withRuntime(
          directory,
          Effect.gen(function* () {
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                history.closed++;
              }),
            );
            const scheduler = yield* makeLiveScheduler;
            const engine = yield* OrchestrationEngineService;
            yield* scheduler.start;
            yield* scheduler.reconcile();
            const run = (yield* task(scheduler)).runs[0]!;
            expect(run.status).toBe("running");
            expect(run.threadId).toBe(PARENT_ID);
            const texts = yield* parentTexts(engine);
            expect(texts).toHaveLength(2);
            expect(texts[1]).toContain(full);
            expect(texts[1]).toContain("Blue full contribution");
            const replay = makeThreadTurnSender({
              dispatch: engine.dispatch,
              commandId: Effect.succeed(
                CommandId.make(`server:scheduler-turn:${run.id}:${run.sendIndex}`),
              ),
              messageId: Effect.succeed(MessageId.make("receipt-replay")),
              now: Effect.succeed(NOW),
            });
            yield* replay(
              Option.getOrThrow(
                yield* (yield* ProjectionSnapshotQuery).getThreadShellById(PARENT_ID),
              ),
              "INITIAL SIDE EFFECT: replay must not execute",
            );
            expect(yield* parentTexts(engine)).toHaveLength(2);
          }),
        );
        expect(history.closed).toBe(2);
        yield* withRuntime(
          directory,
          Effect.gen(function* () {
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                history.closed++;
              }),
            );
            const scheduler = yield* makeLiveScheduler;
            const engine = yield* OrchestrationEngineService;
            yield* Effect.promise(() => NodeFSP.unlink(NodePath.join(directory, "result.txt")));
            yield* executeParent(engine, history, true);
            yield* scheduler.reconcile();
            yield* TestClock.adjust(30_000);
            yield* scheduler.reconcile();
            expect((yield* parentTexts(engine)).at(-1)).not.toContain("FULL SPECTRUM RESULT");
            yield* executeParent(engine, history);
            yield* Effect.promise(() =>
              NodeFSP.writeFile(NodePath.join(directory, "result.txt"), "pass"),
            );
            yield* scheduler.reconcile();
            expect((yield* task(scheduler)).runs[0]!.status).toBe("done");
            expect(history.work).toBe(1);
            expect(history.reports).toBe(1);
            expect(
              (yield* parentTexts(engine)).filter((text) => text.includes("FULL SPECTRUM RESULT")),
            ).toHaveLength(1);
            expect(
              (yield* events(engine)).filter(
                (event) =>
                  event.type === "thread.turn-start-requested" &&
                  event.aggregateId === PARENT_ID &&
                  event.commandId?.startsWith("server:spectrum:"),
              ),
            ).toHaveLength(0);
          }),
        );
        expect(history.closed).toBe(3);
        yield* withRuntime(
          directory,
          Effect.gen(function* () {
            const engine = yield* OrchestrationEngineService;
            yield* (yield* makeSpectrum(() => "reasoning_effort")).recover;
            expect(
              (yield* parentTexts(engine)).filter((text) => text.includes("FULL SPECTRUM RESULT")),
            ).toHaveLength(1);
            expect(
              (yield* events(engine)).filter(
                (event) =>
                  event.type === "thread.turn-start-requested" &&
                  event.aggregateId === PARENT_ID &&
                  event.commandId?.startsWith("server:spectrum:"),
              ),
            ).toHaveLength(0);
          }),
        );
      }),
  );
});
