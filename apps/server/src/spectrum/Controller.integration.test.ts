import * as BareLaunch from "../orchestration-v2/BareLaunch.ts";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  ProviderDriverKind,
  ProviderInstanceId,
  RunId,
  RuntimeRequestId,
  ScheduledTaskId,
  ThreadId,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as Option from "effect/Option";
import * as Events from "../orchestration-v2/EventSink.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as Settings from "../serverSettings.ts";
import * as Projection from "../orchestration-v2/ProjectionStore.ts";
import { layer as serialization } from "../orchestration-v2/ThreadCommandExecutor.ts";
import { initializeRecoveryHistory } from "../prism/RecoveryHistory.ts";
import { reportsFence } from "../scheduledTaskChecks/handoff.ts";
import { ensureCheckSchema, writeCheckState } from "../scheduledTaskChecks/store.ts";
import * as Controller from "./Controller.ts";
import * as McpInterrupt from "./mcpInterrupt.ts";
import * as McpSend from "./mcpSend.ts";
import * as CommandPlans from "./commandPlan.ts";
import * as Adapter from "./SchedulerAdapter.ts";
import * as Launch from "./LaunchService.ts";
import * as Round from "./RoundService.ts";
import * as Transcript from "./TranscriptService.ts";
import * as Reports from "./ReportService.ts";
import { base, input, setup } from "./controllerTestkit.ts";
import { makeRun, NOW } from "./testFixtures.ts";
import { readSpectrum } from "./store.ts";
import { attemptOutcome, REPORT_STOPPED, reportAdmission, reportNeedsYou } from "./reportPolicy.ts";
import { ForkDispatchPlans } from "../fork/ForkDispatchPlans.ts";
import { ForkCommandInterceptor } from "../fork/ForkCommandInterceptor.ts";
import { spectrumLaunchAdmission } from "./launchAdmission.ts";

const dependencies = Layer.mergeAll(base, serialization);
const services = Layer.mergeAll(
  Launch.layer.pipe(Layer.provide(Layer.mock(BareLaunch.BareLaunch)({}))),
  Round.layer,
  Transcript.layer,
  Reports.layer,
).pipe(Layer.provideMerge(dependencies));
const runtime = Layer.fresh(
  Layer.mergeAll(
    CommandPlans.layer.pipe(Layer.provide(dependencies)),
    McpInterrupt.layer.pipe(Layer.provide(dependencies)),
    Adapter.layer,
    Controller.layer.pipe(Layer.provideMerge(services)),
  ),
);
const read = (id: typeof input.threadId) => readSpectrum(id).pipe(Effect.map(Option.getOrThrow));
const finish = Effect.fn("test.Spectrum.finish")(function* (run: OrchestrationV2Run, text: string) {
  const sink = yield* Events.EventSinkV2;
  yield* sink.write({
    events: [
      {
        id: EventId.make(`${run.id}:complete`),
        type: "run.updated",
        threadId: run.threadId,
        occurredAt: NOW,
        payload: { ...run, status: "completed", completedAt: NOW },
      },
      {
        id: EventId.make(`${run.id}:reply`),
        type: "message.updated",
        threadId: run.threadId,
        occurredAt: NOW,
        payload: {
          id: MessageId.make(`${run.id}:reply`),
          threadId: run.threadId,
          runId: run.id,
          nodeId: null,
          createdBy: "agent",
          creationSource: "provider",
          role: "assistant",
          text,
          attachments: [],
          streaming: false,
          createdAt: NOW,
          updatedAt: NOW,
        },
      },
    ],
  });
});

const loseAwaitedWork = Effect.fn("test.Spectrum.loseAwaitedWork")(function* (
  run: OrchestrationV2Run,
  status: "completed" | "cancelled" = "completed",
) {
  const commandId = CommandId.make(`command:runtime-reconcile:fixture:${run.id}`);
  yield* (yield* Events.EventSinkV2).commitCommand({
    commandId,
    threadId: run.threadId,
    commandType: "fixture",
    acceptedAt: NOW,
    events: [
      {
        id: EventId.make(`${run.id}:lost-work`),
        type: "run.updated",
        threadId: run.threadId,
        occurredAt: NOW,
        payload: {
          ...run,
          status,
          completedAt: NOW,
          restartCancelledBackgroundWork: [
            { kind: "monitor", id: "lost", label: "Awaited work", awaited: true },
          ],
        },
      },
    ],
    effects: [
      {
        id: `effect:restart-continuation:${run.id}`,
        commandId,
        threadId: run.threadId,
        request: { type: "provider-runtime.continue", sourceRunId: run.id },
      },
    ],
  });
});

it.effect(
  "council waits for a lost-work restart and relays only its exact successor's answer",
  () =>
    Effect.gen(function* () {
      yield* setup;
      yield* initializeRecoveryHistory;
      const state = yield* (yield* Launch.SpectrumLaunchService).register(input);
      const controller = yield* Controller.SpectrumController;
      const running = yield* controller.resume(state.threadId);
      const slot = running.round!.slots[0]!;
      const projections = yield* Projection.ProjectionStoreV2;
      const source = (yield* projections.getThreadRecords(slot.threadId, ["runs"])).runs.find(
        (run) => run.id === slot.runId,
      )!;
      yield* finish(source, "Partial answer awaiting a monitor.");
      yield* loseAwaitedWork(source);
      const held = yield* controller.resume(state.threadId);
      assert.strictEqual(held.status, "active");
      assert.strictEqual(held.round!.step, 0);
      assert.isNull(held.round!.slots[0]!.replyIds);
      assert.isNull(held.report);
      const successor = {
        ...source,
        id: RunId.make("run:council:restart"),
        ordinal: source.ordinal + 1,
        userMessageId: MessageId.make("message:council:restart"),
        restartContinuationOfRunId: source.id,
        status: "running" as const,
        completedAt: null,
      };
      yield* (yield* Events.EventSinkV2).write({
        events: [
          {
            id: EventId.make("event:council:restart"),
            type: "run.created",
            threadId: successor.threadId,
            occurredAt: NOW,
            payload: successor,
          },
        ],
      });
      assert.isNull((yield* controller.resume(state.threadId)).round!.slots[0]!.replyIds);
      yield* finish(successor, "Complete answer after restart.");
      const accepted = yield* controller.resume(state.threadId);
      assert.strictEqual(accepted.round!.step, 0);
      assert.strictEqual(accepted.round!.slots[0]!.runId, successor.id);
      assert.deepStrictEqual(accepted.round!.slots[0]!.replyIds, [
        MessageId.make(`${successor.id}:reply`),
      ]);
      const transcript = yield* projections.getThreadRecords(state.threadId, ["messages"]);
      assert.isTrue(
        transcript.messages.some(
          (message) => message.text === "[Blue]\nComplete answer after restart.",
        ),
      );
      assert.isFalse(
        transcript.messages.some((message) => message.text.includes("Partial answer")),
      );
    }).pipe(Effect.provide(runtime)),
);

it.effect.each(["completed", "cancelled"] as const)(
  "council retires a %s lost-work reply when native restart declines without relaying partial text",
  (status) =>
    Effect.gen(function* () {
      yield* setup;
      yield* initializeRecoveryHistory;
      const state = yield* (yield* Launch.SpectrumLaunchService).register(input);
      const controller = yield* Controller.SpectrumController;
      const running = yield* controller.resume(state.threadId);
      const slot = running.round!.slots[0]!;
      const projections = yield* Projection.ProjectionStoreV2;
      const source = (yield* projections.getThreadRecords(slot.threadId, ["runs"])).runs.find(
        (run) => run.id === slot.runId,
      )!;
      yield* finish(source, "Partial reply that did not finish its work.");
      yield* loseAwaitedWork(source, status);
      assert.isNull((yield* controller.resume(state.threadId)).round!.slots[0]!.replyIds);
      const sql = yield* SqlClient.SqlClient;
      yield* sql`UPDATE orchestration_v2_effect_outbox SET status='succeeded'
        WHERE effect_id=${`effect:restart-continuation:${source.id}`}`;
      const ended = yield* controller.resume(state.threadId);
      assert.strictEqual(ended.status, "retired");
      assert.isNull(ended.round);
      assert.strictEqual(ended.report!.text, "Spectrum could not finish: run-ended.");
      const transcript = yield* projections.getThreadRecords(state.threadId, ["messages"]);
      assert.isFalse(transcript.messages.some((message) => message.text.includes("Partial reply")));
    }).pipe(Effect.provide(runtime)),
);

it.effect(
  "reply acceptance rolls back when the completed run loses awaited work before commit",
  () =>
    Effect.gen(function* () {
      yield* setup;
      yield* initializeRecoveryHistory;
      const state = yield* (yield* Launch.SpectrumLaunchService).register(input);
      const running = yield* (yield* Controller.SpectrumController).resume(state.threadId);
      const slot = running.round!.slots[0]!;
      const projections = yield* Projection.ProjectionStoreV2;
      const source = (yield* projections.getThreadRecords(slot.threadId, ["runs"])).runs.find(
        (run) => run.id === slot.runId,
      )!;
      yield* finish(source, "Answer whose awaited work is lost before acceptance.");
      const sink = yield* Events.EventSinkV2;
      const replyKey = CommandId.make(`${slot.commandId}:reply:enqueue`);
      const refused = yield* Effect.gen(function* () {
        return yield* (yield* Controller.SpectrumController).resume(state.threadId);
      }).pipe(
        Effect.provide(Controller.layer),
        Effect.provideService(Events.EventSinkV2, {
          ...sink,
          commitCommand: (command) =>
            command.commandId === replyKey
              ? loseAwaitedWork(source).pipe(Effect.andThen(sink.commitCommand(command)))
              : sink.commitCommand(command),
        }),
        Effect.exit,
      );
      assert.strictEqual(refused._tag, "Failure");
      const current = yield* read(state.threadId);
      assert.strictEqual(current.round!.step, 0);
      assert.isNull(current.round!.slots[0]!.replyIds);
      const sql = yield* SqlClient.SqlClient;
      assert.deepStrictEqual(
        yield* sql`SELECT command_id FROM orchestration_command_receipts WHERE command_id=${replyKey}`,
        [],
      );
      const transcript = yield* projections.getThreadRecords(state.threadId, ["messages"]);
      assert.isFalse(transcript.messages.some((message) => message.text.includes("Answer whose")));
    }).pipe(Effect.provide(runtime)),
);

it.effect(
  "delivered-report reopen rejects completion lost after planning and follows the exact restart",
  () =>
    Effect.gen(function* () {
      yield* setup;
      yield* initializeRecoveryHistory;
      const state = yield* (yield* Launch.SpectrumLaunchService).register(input);
      const reports = yield* Reports.SpectrumReportService;
      yield* reports.settle(state.threadId, "Completed report before restart.");
      const before = yield* reports.deliver(state.threadId);
      const projections = yield* Projection.ProjectionStoreV2;
      const source = (yield* projections.getThreadRecords(input.callerThreadId, [
        "runs",
      ])).runs.find((run) => run.userMessageId === before.report!.messageId)!;
      yield* finish(source, "The report was processed.");
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const interceptor = yield* ForkCommandInterceptor;
      const reopen = {
        type: "thread.unsettle" as const,
        reason: "user" as const,
        threadId: state.threadId,
        commandId: CommandId.make("restart:reopen-race"),
      };
      const refused = yield* orchestrator.dispatch(reopen).pipe(
        Effect.provideService(ForkCommandInterceptor, {
          plan: (command) =>
            Effect.gen(function* () {
              const planned = yield* interceptor.plan(command);
              if (command.commandId === reopen.commandId) yield* loseAwaitedWork(source);
              return planned;
            }),
        }),
        Effect.exit,
      );
      assert.strictEqual(refused._tag, "Failure");
      assert.deepStrictEqual(yield* read(state.threadId), before);
      const successor = {
        ...source,
        id: RunId.make("run:reopen:successor"),
        ordinal: source.ordinal + 1,
        userMessageId: MessageId.make("message:reopen:successor"),
        restartContinuationOfRunId: source.id,
        status: "completed" as const,
        completedAt: NOW,
      };
      yield* (yield* Events.EventSinkV2).write({
        events: [
          {
            id: EventId.make("event:reopen:successor"),
            type: "run.created",
            threadId: source.threadId,
            occurredAt: NOW,
            payload: successor,
          },
        ],
      });
      yield* orchestrator.dispatch({
        ...reopen,
        commandId: CommandId.make("restart:reopen-complete"),
      });
      const reopened = yield* read(state.threadId);
      assert.strictEqual(reopened.status, "active");
      assert.isNull(reopened.report);
    }).pipe(Effect.provide(runtime)),
);

it.effect("reopening a stopped council wakes a Drafter request suppressed by Stop", () =>
  Effect.gen(function* () {
    yield* setup;
    yield* initializeRecoveryHistory;
    const state = yield* (yield* Launch.SpectrumLaunchService).register(input);
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const sink = yield* Events.EventSinkV2;
    yield* orchestrator.dispatch({
      type: "thread.stop",
      threadId: state.threadId,
      commandId: CommandId.make("drafter-request:stop"),
    });
    const retired = yield* read(state.threadId);
    yield* (yield* Reports.SpectrumReportService).abandon(
      state.threadId,
      retired.report!.commandId,
      "alice",
    );
    const drafter = retired.participants[0]!.threadId;
    const requestId = RuntimeRequestId.make("drafter-request:approval");
    // The listener wakes this request at once, while the Drafter is retired.
    yield* sink.write({
      events: [
        {
          id: EventId.make("drafter-request:pending"),
          type: "runtime-request.updated",
          threadId: drafter,
          occurredAt: NOW,
          payload: {
            id: requestId,
            nodeId: NodeId.make("drafter-request:node"),
            providerTurnId: null,
            nativeRequestRef: null,
            kind: "command",
            status: "pending",
            responseCapability: { type: "message" },
            createdAt: NOW,
            resolvedAt: null,
          },
        },
      ],
    });
    const wakeCommandId = CommandId.make(
      `command:child-request:${drafter}:${requestId}:${state.threadId}:0`,
    );
    const delivered = yield* sink.stream({ afterSequence: yield* sink.latestSequence() }).pipe(
      Stream.filter(
        (stored) => stored.commandId === wakeCommandId && stored.event.type === "message.updated",
      ),
      Stream.runHead,
      Effect.forkChild,
    );
    yield* orchestrator.dispatch({
      type: "thread.unsettle",
      reason: "user",
      threadId: state.threadId,
      commandId: CommandId.make("drafter-request:reopen"),
    });
    assert.strictEqual((yield* read(state.threadId)).status, "active");
    assert.isTrue(Option.isSome(yield* Fiber.join(delivered)));
  }).pipe(Effect.provide(runtime)),
);

it.effect(
  "a retired council cannot reopen while an owned native restart effect is still pending",
  () =>
    Effect.gen(function* () {
      yield* setup;
      yield* initializeRecoveryHistory;
      const state = yield* (yield* Launch.SpectrumLaunchService).register(input);
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      yield* orchestrator.dispatch({
        type: "thread.stop",
        threadId: state.threadId,
        commandId: CommandId.make("native-drain:stop"),
      });
      const retired = yield* read(state.threadId);
      yield* (yield* Reports.SpectrumReportService).abandon(
        state.threadId,
        retired.report!.commandId,
        "alice",
      );
      const childId = retired.participants[0]!.threadId;
      const source = makeRun(
        {
          threadId: childId,
          commandId: CommandId.make("native-drain:source"),
          messageId: MessageId.make("native-drain:source"),
          runId: null,
          replyIds: null,
        },
        { status: "completed", completedAt: NOW },
      );
      yield* loseAwaitedWork(source);
      const reopen = {
        type: "thread.unsettle" as const,
        reason: "user" as const,
        threadId: state.threadId,
        commandId: CommandId.make("native-drain:reopen"),
      };
      const refused = yield* orchestrator.dispatch(reopen).pipe(Effect.exit);
      assert.strictEqual(refused._tag, "Failure");
      assert.strictEqual((yield* read(state.threadId)).status, "retired");
      const sql = yield* SqlClient.SqlClient;
      yield* sql`UPDATE orchestration_v2_effect_outbox SET status='succeeded'
      WHERE effect_id=${`effect:restart-continuation:${source.id}`}`;
      yield* orchestrator.dispatch({
        ...reopen,
        commandId: CommandId.make("native-drain:reopen-settled"),
      });
      assert.strictEqual((yield* read(state.threadId)).status, "active");
    }).pipe(Effect.provide(runtime)),
);

it.effect(
  "disabled instances still accept registered composer and MCP transcripts while refusing provider starts",
  () =>
    Effect.gen(function* () {
      yield* setup;
      const state = yield* (yield* Launch.SpectrumLaunchService).register(input);
      const projections = yield* Projection.ProjectionStoreV2;
      const projectId = (yield* projections.getThread(state.threadId)).projectId;
      const gate = Layer.fresh(Threads.layer).pipe(
        Layer.provideMerge(
          Settings.layerTest({
            providerInstances: {
              [ProviderInstanceId.make("codex")]: {
                driver: ProviderDriverKind.make("codex"),
                enabled: true,
              },
            },
            projectSettingsOverrides: {
              [projectId]: {
                providerInstances: { [ProviderInstanceId.make("codex")]: { enabled: false } },
              },
            },
          }),
        ),
      );
      yield* Effect.gen(function* () {
        const threads = yield* Threads.ThreadManagementService;
        const composer = {
          type: "message.dispatch" as const,
          commandId: CommandId.make("disabled:composer"),
          messageId: MessageId.make("disabled:composer"),
          threadId: state.threadId,
          text: "A human joins the transcript.",
          attachments: [],
          createdBy: "user" as const,
          creationSource: "web" as const,
          dispatchMode: { type: "start_immediately" as const },
        };
        const first = yield* threads.dispatch(composer);
        assert.deepStrictEqual(yield* threads.dispatch(composer), { ...first, replayed: true });
        const send = yield* McpSend.SpectrumMcpSend;
        assert.deepStrictEqual(
          yield* send.send(
            threads,
            {
              threadId: state.threadId,
              message: "An agent joins the transcript.",
            },
            CommandId.make("disabled:mcp"),
            MessageId.make("disabled:mcp"),
            input.callerThreadId,
          ),
          {
            threadId: state.threadId,
            messageId: MessageId.make("disabled:mcp"),
            runId: null,
            status: "idle",
            delivery: "transcript",
          },
        );
        const rows = yield* projections.getThreadRecords(state.threadId, ["runs", "messages"]);
        assert.deepStrictEqual(rows.runs, []);
        assert.strictEqual(
          rows.messages.filter((message) => message.id === composer.messageId).length,
          1,
        );
        assert.strictEqual(
          rows.messages.find((message) => message.id === composer.messageId)!.text,
          composer.text,
        );
        assert.strictEqual(
          rows.messages.find((message) => message.id === "disabled:mcp")!.text,
          "An agent joins the transcript.",
        );
        const sql = yield* SqlClient.SqlClient;
        assert.deepStrictEqual(
          yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE thread_id=${state.threadId}`,
          [],
        );
        const ordinary = yield* threads
          .dispatch({
            ...composer,
            commandId: CommandId.make("disabled:ordinary"),
            messageId: MessageId.make("disabled:ordinary"),
            threadId: input.callerThreadId,
          })
          .pipe(Effect.flip);
        assert.strictEqual(ordinary._tag, "ThreadManagementProviderInstanceDisabledError");

        const placeholder = ThreadId.make("spectrum:unregistered-placeholder");
        const caller = yield* projections.getThread(input.callerThreadId);
        yield* (yield* Events.EventSinkV2).write({
          events: [
            {
              id: EventId.make("disabled:unregistered-placeholder"),
              type: "thread.created",
              threadId: placeholder,
              occurredAt: NOW,
              payload: {
                ...caller,
                id: placeholder,
                forkSpectrumRunning: true,
                lineage: {
                  parentThreadId: null,
                  relationshipToParent: null,
                  rootThreadId: placeholder,
                },
              },
            },
          ],
        });
        const unregistered = yield* threads
          .dispatch({
            ...composer,
            commandId: CommandId.make("disabled:unregistered-send"),
            messageId: MessageId.make("disabled:unregistered-send"),
            threadId: placeholder,
          })
          .pipe(Effect.flip);
        assert.instanceOf(unregistered, Threads.ThreadManagementProviderInstanceDisabledError);
        assert.deepStrictEqual(
          (yield* projections.getThreadRecords(placeholder, ["runs"])).runs,
          [],
        );
        assert.deepStrictEqual(
          yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE thread_id=${placeholder}`,
          [],
        );

        const prepared = yield* (yield* Round.SpectrumRoundService).prepare(state.threadId);
        const command = prepared.outbox[0]!;
        const refused = yield* Effect.gen(function* () {
          return yield* (yield* Round.SpectrumRoundService).dispatch(
            state.threadId,
            command.commandId,
          );
        }).pipe(Effect.provide(Round.layer), Effect.flip);
        assert.instanceOf(refused.cause, Threads.ThreadManagementProviderInstanceDisabledError);
        assert.deepStrictEqual(yield* read(state.threadId), prepared);
        for (const color of prepared.participants) {
          assert.deepStrictEqual(
            (yield* projections.getThreadRecords(color.threadId, ["runs"])).runs,
            [],
          );
          assert.deepStrictEqual(
            yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE thread_id=${color.threadId}`,
            [],
          );
        }
      }).pipe(Effect.provide(Layer.merge(gate, McpSend.layer)));
      // The same durable start remains eligible after the instance is enabled again.
      const prepared = yield* read(state.threadId);
      const command = prepared.outbox[0]!;
      if (command.type !== "message.dispatch")
        return yield* Effect.die(new Error("Expected a pending native message dispatch"));
      yield* (yield* Round.SpectrumRoundService).dispatch(state.threadId, command.commandId);
      assert.strictEqual(
        (yield* projections.getThreadRecords(command.threadId, ["runs"])).runs.length,
        1,
      );
    }).pipe(Effect.provide(runtime)),
);

it.effect(
  "free mode preserves verbatim replies, exact barrier order and reports through the real scheduler adapter",
  () =>
    Effect.gen(function* () {
      yield* setup;
      yield* initializeRecoveryHistory;
      yield* ensureCheckSchema;
      const sink = yield* Events.EventSinkV2;
      const originMessage = MessageId.make("scheduler:origin");
      const origin = makeRun(
        {
          threadId: input.callerThreadId,
          commandId: CommandId.make("scheduler:send"),
          messageId: originMessage,
          runId: null,
          replyIds: null,
        },
        { id: RunId.make("scheduler:caller-run"), status: "completed", completedAt: NOW },
      );
      yield* sink.write({
        events: [
          {
            id: EventId.make("scheduler:origin"),
            type: "run.created",
            threadId: origin.threadId,
            occurredAt: NOW,
            payload: origin,
          },
        ],
      });
      const taskId = ScheduledTaskId.make("scheduler:task");
      yield* writeCheckState(null, {
        version: 1,
        taskId,
        revision: 0,
        kind: "agent",
        command: null,
        role: null,
        lane: null,
        checks: [],
        runs: [
          {
            id: "scheduler:run",
            slot: DateTime.formatIso(NOW),
            checkVersion: null,
            threadId: origin.threadId,
            checkCwd: null,
            stage: "running",
            attempt: 0,
            retryAt: null,
            hasWork: true,
            sends: [
              {
                index: 0,
                commandId: CommandId.make("scheduler:send"),
                messageId: originMessage,
                kind: "start",
                createdAt: DateTime.formatIso(NOW),
              },
            ],
            error: null,
            check: null,
          },
        ],
        failureStreak: 0,
        lastError: null,
        lastSuccessfulRunId: null,
      });
      const launch = yield* Launch.SpectrumLaunchService;
      const state = yield* launch.register({
        ...input,
        mode: "free",
        limit: 2,
        callerRunId: origin.id,
      });
      assert.strictEqual(state.schedulerRunId, "scheduler:run");
      const controller = yield* Controller.SpectrumController;
      const projections = yield* Projection.ProjectionStoreV2;
      let current = yield* controller.resume(state.threadId);
      assert.strictEqual(current.round!.slots.length, 1);
      assert.strictEqual(current.round!.slots[0]!.threadId, current.participants[0]!.threadId);
      const firstReply = "First line\n" + "🟦".repeat(20000) + " verbatim";
      for (const text of [firstReply, "Second answer exactly.", "DONE", "Summary."]) {
        const slot = current.round!.slots[0]!;
        const records = yield* projections.getThreadRecords(slot.threadId, ["runs"]);
        const run = records.runs.find((run) => run.id === slot.runId)!;
        yield* finish(run, text);
        current = yield* Effect.gen(function* () {
          return yield* (yield* Controller.SpectrumController).resume(state.threadId);
        }).pipe(Effect.provide(Layer.fresh(Controller.layer)));
      }
      assert.strictEqual(current.status, "settled");
      assert.strictEqual(
        (yield* projections.getThreadShell(state.threadId))!.forkSpectrumRunning,
        false,
      );
      const transcript = yield* projections.getThreadRecords(state.threadId, ["messages", "runs"]);
      assert.deepStrictEqual(transcript.runs, []);
      assert.isTrue(
        transcript.messages.some((message) => message.text === `[Blue]\n${firstReply}`),
      );
      assert.isTrue(
        transcript.messages.some((message) => message.text === "[Red]\nSecond answer exactly."),
      );
      assert.deepStrictEqual(yield* reportsFence("scheduler:run"), { kind: "waiting" });
      const caller = yield* projections.getThreadRecords(origin.threadId, ["runs"]);
      const reportRun = caller.runs.find((run) => run.userMessageId === current.report!.messageId)!;
      yield* finish(reportRun, "Report acknowledged.");
      assert.deepStrictEqual(yield* reportsFence("scheduler:run"), { kind: "released" });
    }).pipe(Effect.provide(runtime)),
);

/** Answers every slot of each council round in turn, recording who was asked in which phase. */
const runCouncil = Effect.fn("test.Spectrum.runCouncil")(function* (threadId: ThreadId) {
  const controller = yield* Controller.SpectrumController;
  const projections = yield* Projection.ProjectionStoreV2;
  let current = yield* controller.resume(threadId);
  const rounds: Array<{ readonly phase: string; readonly threads: ReadonlyArray<string> }> = [];
  for (let step = 0; step < 4; step++) {
    const slots = current.round!.slots;
    rounds.push({ phase: current.round!.phase, threads: slots.map((slot) => slot.threadId) });
    for (let index = 0; index < slots.length; index++) {
      const slot = slots[index]!;
      const child = yield* projections.getThreadRecords(slot.threadId, ["runs"]);
      yield* finish(
        child.runs.find((run) => run.id === slot.runId)!,
        `${step}/${index} reply`,
      );
      current = yield* controller.resume(threadId);
      if (index < slots.length - 1) assert.strictEqual(current.round!.step, step);
    }
  }
  return { current, rounds };
});

it.effect(
  "council gives every Color's answer and relays, then only the separate White synthesizes",
  () =>
    Effect.gen(function* () {
      yield* setup;
      yield* initializeRecoveryHistory;
      const state = yield* (yield* Launch.SpectrumLaunchService).register(input);
      const colors = state.participants.map((color) => color.threadId);
      assert.strictEqual(colors.length, 2);
      assert.notInclude(colors, state.white!.threadId);
      const { current, rounds } = yield* runCouncil(state.threadId);
      assert.deepStrictEqual(rounds, [
        { phase: "independent", threads: colors },
        { phase: "relay", threads: colors },
        { phase: "relay", threads: colors },
        { phase: "synthesis", threads: [state.white!.threadId] },
      ]);
      assert.strictEqual(current.status, "settled");
      const transcript = (yield* (yield* Projection.ProjectionStoreV2).getThreadRecords(
        state.threadId,
        ["messages"],
      )).messages.map((message) => message.text);
      // Two Color answers, then White's synthesis under White's own name.
      assert.deepStrictEqual(
        transcript.filter((text) => text.endsWith("0/0 reply") || text.endsWith("0/1 reply")),
        ["[Blue]\n0/0 reply", "[Red]\n0/1 reply"],
      );
      assert.include(transcript, "[White]\n3/0 reply");
    }).pipe(Effect.provide(runtime)),
);

it.effect(
  "a Color labelled White keeps its name and the separate White becomes White (moderator)",
  () =>
    Effect.gen(function* () {
      yield* setup;
      yield* initializeRecoveryHistory;
      const state = yield* (yield* Launch.SpectrumLaunchService).register({
        ...input,
        colors: input.colors.map((color, index) =>
          index === 0 ? { ...color, label: "White" } : color,
        ),
      });
      const projections = yield* Projection.ProjectionStoreV2;
      const color = state.participants[0]!;
      assert.strictEqual(color.label, "White");
      assert.strictEqual(state.white!.label, "White (moderator)");
      assert.strictEqual((yield* projections.getThread(color.threadId)).title, "White");
      assert.strictEqual(
        (yield* projections.getThread(state.white!.threadId)).title,
        "White (moderator)",
      );
      const { current } = yield* runCouncil(state.threadId);
      assert.strictEqual(current.status, "settled");
      const transcript = (yield* projections.getThreadRecords(state.threadId, [
        "messages",
      ])).messages
        .map((message) => message.text)
        .filter((text) => text.startsWith("[White"));
      // The Color answers and relays as White; only the synthesis is tagged as the moderator.
      assert.deepStrictEqual(transcript, [
        "[White]\n0/0 reply",
        "[White]\n1/0 reply",
        "[White]\n2/0 reply",
        "[White (moderator)]\n3/0 reply",
      ]);
    }).pipe(Effect.provide(runtime)),
);

it.effect("Colors called White and White (moderator) leave the separate White a third name", () =>
  Effect.gen(function* () {
    yield* setup;
    yield* initializeRecoveryHistory;
    const labels = ["White", "White (moderator)"];
    const state = yield* (yield* Launch.SpectrumLaunchService).register({
      ...input,
      colors: input.colors.map((color, index) => ({ ...color, label: labels[index]! })),
    });
    const projections = yield* Projection.ProjectionStoreV2;
    assert.deepStrictEqual(
      state.participants.map((color) => color.label),
      labels,
    );
    assert.strictEqual(state.white!.label, "White (moderator 2)");
    const titles = yield* Effect.forEach([...state.participants, state.white!], (member) =>
      projections.getThread(member.threadId).pipe(Effect.map((thread) => thread.title)),
    );
    assert.deepStrictEqual(titles, ["White", "White (moderator)", "White (moderator 2)"]);
    const { current } = yield* runCouncil(state.threadId);
    assert.strictEqual(current.status, "settled");
    const tags = (yield* projections.getThreadRecords(state.threadId, ["messages"])).messages
      .filter((message) => message.role === "assistant")
      .map((message) => message.text.slice(0, message.text.indexOf("\n")));
    assert.deepStrictEqual(tags, [
      ...Array.from({ length: 3 }, () => ["[White]", "[White (moderator)]"]).flat(),
      "[White (moderator 2)]",
    ]);
  }).pipe(Effect.provide(runtime)),
);

it.effect(
  "a Spectrum persisted before White was its own member resumes with the Color as White",
  () =>
    Effect.gen(function* () {
      yield* setup;
      yield* initializeRecoveryHistory;
      const state = yield* (yield* Launch.SpectrumLaunchService).register(input);
      // The row shape of earlier builds: no White member, White is the Color at index 1.
      const sql = yield* SqlClient.SqlClient;
      yield* sql`UPDATE fork_spectra
      SET payload_json = json_set(json_remove(payload_json, '$.white'), '$.moderator', 1)
      WHERE thread_id = ${state.threadId}`;
      const legacy = Option.getOrThrow(yield* readSpectrum(state.threadId));
      assert.isUndefined(legacy.white);
      assert.strictEqual(legacy.moderator, 1);
      const colors = state.participants.map((color) => color.threadId);
      const { current, rounds } = yield* runCouncil(state.threadId);
      assert.deepStrictEqual(rounds.at(-1), { phase: "synthesis", threads: [colors[1]!] });
      assert.strictEqual(current.status, "settled");
    }).pipe(Effect.provide(runtime)),
);

const retireCaller = Effect.fn("test.Spectrum.retireCaller")(function* (
  caller: Effect.Success<typeof setup>,
  resumed: boolean,
) {
  const token = CommandId.make("caller:restart:stop");
  yield* (yield* Events.EventSinkV2).write({
    events: [
      {
        id: EventId.make(`caller:retire:${resumed}`),
        type: "thread.metadata-updated",
        threadId: caller.id,
        occurredAt: NOW,
        payload: {
          ...caller,
          forkRetirement: { token },
          ...(resumed ? { forkResumedRetirements: [token] } : {}),
        },
      },
    ],
  });
});

it.effect("a council launched from a resumed caller dispatches round 1 and settles", () =>
  Effect.gen(function* () {
    yield* retireCaller(yield* setup, true);
    yield* initializeRecoveryHistory;
    const state = yield* (yield* Launch.SpectrumLaunchService).register(input);
    const controller = yield* Controller.SpectrumController;
    const projections = yield* Projection.ProjectionStoreV2;
    let current = yield* controller.resume(state.threadId);
    assert.strictEqual(current.status, "active");
    assert.isTrue(current.round!.slots.every((slot) => slot.runId !== null));
    while (current.status === "active") {
      const slot = current.round!.slots.find((candidate) => candidate.replyIds === null)!;
      const child = yield* projections.getThreadRecords(slot.threadId, ["runs"]);
      yield* finish(
        child.runs.find((run) => run.id === slot.runId)!,
        "reply",
      );
      current = yield* controller.resume(state.threadId);
    }
    assert.strictEqual(current.status, "settled");
    const caller = yield* projections.getThreadRecords(input.callerThreadId, ["runs"]);
    const reportRun = caller.runs.find((run) => run.userMessageId === current.report!.messageId)!;
    yield* finish(reportRun, "Report acknowledged.");
    const outcome = yield* attemptOutcome(input.callerThreadId, current.report!);
    assert.strictEqual(outcome.kind, "delivered");
  }).pipe(Effect.provide(runtime)),
);

it.effect("a council retired before round 1 holds its refused stop report for a person", () =>
  Effect.gen(function* () {
    const caller = yield* setup;
    yield* initializeRecoveryHistory;
    const state = yield* (yield* Launch.SpectrumLaunchService).register(input);
    yield* retireCaller(caller, false);
    const controller = yield* Controller.SpectrumController;
    const retired = yield* controller.resume(state.threadId);
    assert.strictEqual(retired.status, "retired");
    assert.isNull(retired.round);
    assert.isFalse(
      retired.outbox.some((command) => command.commandId === retired.report?.commandId),
    );
    // The stopped caller refused the notice before commit, so it holds for a person.
    assert.deepStrictEqual(yield* reportNeedsYou(retired), {
      commandId: retired.report!.commandId,
      reason: REPORT_STOPPED,
    });
    assert.deepStrictEqual(yield* controller.resume(state.threadId), retired);
  }).pipe(Effect.provide(runtime)),
);

it.effect(
  "composer commands commit transcript rows, deduplicate replay, and leave unrelated dispatch unchanged",
  () =>
    Effect.gen(function* () {
      yield* setup;
      const state = yield* (yield* Launch.SpectrumLaunchService).register(input);
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const command = {
        type: "message.dispatch" as const,
        commandId: CommandId.make("composer:input"),
        messageId: MessageId.make("composer:input"),
        threadId: state.threadId,
        text: "Join this discussion",
        attachments: [],
        createdBy: "user" as const,
        creationSource: "web" as const,
        dispatchMode: { type: "start_immediately" as const },
      };
      const first = yield* orchestrator.dispatch(command);
      const replay = yield* orchestrator.dispatch(command);
      assert.deepStrictEqual(replay, { ...first, replayed: true });
      const projections = yield* Projection.ProjectionStoreV2;
      const records = yield* projections.getThreadRecords(state.threadId, ["messages", "runs"]);
      assert.deepStrictEqual(records.runs, []);
      assert.strictEqual(
        records.messages.find((message) => message.id === command.messageId)!.text,
        command.text,
      );
      yield* orchestrator.dispatch({
        type: "thread.visit",
        threadId: input.callerThreadId,
        commandId: CommandId.make("unrelated:visit"),
        visitedAt: DateTime.formatIso(NOW),
      });
      const current = yield* read(state.threadId);
      assert.deepStrictEqual(current.inbox, [command.messageId]);
    }).pipe(Effect.provide(runtime)),
);

it.effect(
  "Stop retires the transcript, cancels owned lineage-only Colors, and explicit reopen starts a new generation",
  () =>
    Effect.gen(function* () {
      yield* setup;
      yield* initializeRecoveryHistory;
      const state = yield* (yield* Launch.SpectrumLaunchService).register(input);
      const controller = yield* Controller.SpectrumController;
      const running = yield* controller.resume(state.threadId);
      const projections = yield* Projection.ProjectionStoreV2;
      assert.strictEqual(
        (yield* projections.getThreadShell(state.threadId))!.forkSpectrumRunning,
        true,
      );
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const stop = {
        type: "thread.stop" as const,
        threadId: state.threadId,
        commandId: CommandId.make("human:stop"),
      };
      const hook = yield* McpInterrupt.SpectrumMcpInterrupt;
      const threads = { dispatch: orchestrator.dispatch };
      const interrupt = yield* hook.interrupt(
        threads,
        { threadId: state.threadId },
        stop.commandId,
      );
      assert.strictEqual(interrupt?.status, "interrupt_requested");
      assert.deepStrictEqual(
        yield* hook.interrupt(threads, { threadId: state.threadId }, stop.commandId),
        interrupt,
      );
      assert.strictEqual(
        (yield* projections.getThreadShell(state.threadId))!.forkSpectrumRunning,
        false,
      );
      const retired = yield* controller.resume(state.threadId);
      assert.strictEqual(retired.status, "retired");
      assert.isAbove(retired.generation, running.generation);
      for (const participant of retired.participants) {
        const child = yield* projections.getThreadRecords(participant.threadId, ["runs"]);
        assert.isTrue(
          child.runs.every((run) =>
            ["completed", "failed", "interrupted", "cancelled", "rolled_back"].includes(run.status),
          ),
        );
      }
      assert.deepStrictEqual(
        (yield* projections.getThreadRecords(state.threadId, ["runs"])).runs,
        [],
      );
      const sql = yield* SqlClient.SqlClient;
      const pendingOwned = yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox
        WHERE thread_id IN ${sql.in(retired.participants.map((participant) => participant.threadId))}
          AND status IN ('pending','running') AND json_extract(payload_json,'$.type') IN ('provider-turn.start','provider-turn.restart','provider-turn.interrupt')`;
      assert.deepStrictEqual(pendingOwned, []);
      assert.isFalse(
        retired.outbox.some((command) => command.commandId === retired.report?.commandId),
      );
      // The stop notice is an undelivered report, so reopening needs explicit abandonment first.
      yield* (yield* Reports.SpectrumReportService).abandon(
        state.threadId,
        retired.report!.commandId,
        "alice",
      );
      yield* orchestrator.dispatch({
        type: "thread.unsettle",
        reason: "user",
        threadId: state.threadId,
        commandId: CommandId.make("human:reopen"),
      });
      const reopened = yield* read(state.threadId);
      assert.strictEqual(reopened.status, "active");
      assert.strictEqual(
        (yield* projections.getThreadShell(state.threadId))!.forkSpectrumRunning,
        true,
      );
      assert.isAbove(reopened.generation, retired.generation);
      assert.isNull(reopened.report);
      yield* controller.resume(state.threadId);
      const resumed = yield* read(state.threadId);
      assert.strictEqual(resumed.round!.generation, reopened.generation);
    }).pipe(Effect.provide(runtime)),
);

it.effect("abandon then reopen invalidates a delayed old report dispatch", () =>
  Effect.gen(function* () {
    yield* setup;
    yield* initializeRecoveryHistory;
    const state = yield* (yield* Launch.SpectrumLaunchService).register(input);
    const reports = yield* Reports.SpectrumReportService;
    const pending = yield* reports.settle(state.threadId, "Immutable report text.");
    const old = pending.report!;
    yield* reports.abandon(state.threadId, old.commandId, "alice");
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    yield* orchestrator.dispatch({
      type: "thread.unsettle",
      reason: "user",
      threadId: state.threadId,
      commandId: CommandId.make("reopen:after-abandon"),
    });
    const reopened = yield* read(state.threadId);
    assert.isNull(reopened.report);
    // A restarted outbox reader rebuilds admission from the old durable attempt identity.
    const refused = yield* orchestrator
      .dispatch(old)
      .pipe(
        Effect.provideService(ForkDispatchPlans, [reportAdmission(state.threadId, old)]),
        Effect.flip,
      );
    assert.isDefined(refused);
    const caller = yield* (yield* Projection.ProjectionStoreV2).getThreadRecords(
      state.callerThreadId,
      ["runs", "messages"],
    );
    assert.isFalse(caller.runs.some((run) => run.userMessageId === old.messageId));
    assert.isFalse(caller.messages.some((message) => message.id === old.messageId));
  }).pipe(Effect.provide(runtime)),
);

it.effect(
  "Stop wins after a Drafter launch reads its durable command, and restart repairs the shell without reactivating it",
  () =>
    Effect.gen(function* () {
      yield* setup;
      yield* initializeRecoveryHistory;
      const state = yield* (yield* Launch.SpectrumLaunchService).register(input);
      const prepared = yield* (yield* Round.SpectrumRoundService).prepare(state.threadId);
      const command = prepared.outbox[0]!;
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threads = yield* Threads.ThreadManagementService;
      const stop = {
        type: "thread.stop" as const,
        threadId: state.threadId,
        commandId: CommandId.make("race:stop"),
      };
      // The round service has already read and rebuilt its generation admission before this dispatch boundary.
      yield* Effect.gen(function* () {
        yield* (yield* Round.SpectrumRoundService)
          .dispatch(state.threadId, command.commandId)
          .pipe(Effect.flip);
      }).pipe(
        Effect.provide(Round.layer),
        Effect.provideService(Threads.ThreadManagementService, {
          ...threads,
          dispatch: (launch) =>
            orchestrator.dispatch(stop).pipe(Effect.andThen(threads.dispatch(launch))),
        }),
      );
      const retired = yield* read(state.threadId);
      assert.strictEqual(retired.status, "retired");
      yield* orchestrator.dispatch(stop);
      assert.deepStrictEqual(yield* read(state.threadId), retired);
      const projections = yield* Projection.ProjectionStoreV2;
      for (const color of retired.participants)
        assert.deepStrictEqual(
          (yield* projections.getThreadRecords(color.threadId, ["runs"])).runs,
          [],
        );
      const sql = yield* SqlClient.SqlClient;
      assert.deepStrictEqual(
        yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE thread_id IN ${sql.in(retired.participants.map((color) => color.threadId))} AND status IN('pending','running')`,
        [],
      );
      // An old shell snapshot must be repaired from the fork row, never treated as permission to run.
      const thread = yield* projections.getThread(state.threadId);
      yield* (yield* Events.EventSinkV2).write({
        events: [
          {
            id: EventId.make("legacy:spectrum:flag"),
            type: "thread.metadata-updated",
            threadId: state.threadId,
            occurredAt: NOW,
            payload: { ...thread, forkSpectrumRunning: true },
          },
        ],
      });
      yield* (yield* Controller.SpectrumController).resume(state.threadId);
      assert.strictEqual(
        (yield* projections.getThreadShell(state.threadId))!.forkSpectrumRunning,
        false,
      );
      assert.strictEqual((yield* read(state.threadId)).status, "retired");
    }).pipe(Effect.provide(runtime)),
);

it.effect("Stop reconciles a Color launch committed after its cancellation snapshot", () =>
  Effect.gen(function* () {
    yield* setup;
    yield* initializeRecoveryHistory;
    const state = yield* (yield* Launch.SpectrumLaunchService).register(input);
    const prepared = yield* (yield* Round.SpectrumRoundService).prepare(state.threadId);
    const launch = prepared.outbox[0]!;
    assert.strictEqual(launch.type, "message.dispatch");
    if (launch.type !== "message.dispatch") return;
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const interceptor = yield* ForkCommandInterceptor;
    yield* orchestrator
      .dispatch({
        type: "thread.stop",
        threadId: state.threadId,
        commandId: CommandId.make("snapshot-race:stop"),
      })
      .pipe(
        Effect.provideService(ForkCommandInterceptor, {
          plan: (command) =>
            Effect.gen(function* () {
              const planned = yield* interceptor.plan(command);
              if (command.type === "thread.stop" && command.threadId === state.threadId) {
                // Commit only the Color launch, before RoundService can bind it or advance the root revision.
                yield* orchestrator
                  .dispatch(launch)
                  .pipe(
                    Effect.provideService(ForkDispatchPlans, [
                      spectrumLaunchAdmission(state.threadId, prepared.generation, launch),
                    ]),
                  );
              }
              return planned;
            }),
        }),
      );
    const retired = yield* read(state.threadId);
    assert.strictEqual(retired.status, "retired");
    const projections = yield* Projection.ProjectionStoreV2;
    const launched = yield* projections.getThreadRecords(launch.threadId, ["runs"]);
    assert.strictEqual(launched.runs.length, 1);
    yield* (yield* Reports.SpectrumReportService).abandon(
      state.threadId,
      retired.report!.commandId,
      "alice",
    );
    const reopen = {
      type: "thread.unsettle" as const,
      reason: "user" as const,
      threadId: state.threadId,
      commandId: CommandId.make("snapshot-race:reopen"),
    };
    const early = yield* orchestrator.dispatch(reopen).pipe(Effect.exit);
    assert.strictEqual(early._tag, "Failure");
    yield* (yield* Controller.SpectrumController).resume(state.threadId);
    const drained = yield* projections.getThreadRecords(launch.threadId, ["runs"]);
    assert.isTrue(
      drained.runs.every((run) =>
        ["completed", "failed", "interrupted", "cancelled", "rolled_back"].includes(run.status),
      ),
    );
    const sql = yield* SqlClient.SqlClient;
    assert.deepStrictEqual(
      yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE thread_id=${launch.threadId} AND status IN('pending','running')`,
      [],
    );
    yield* orchestrator.dispatch({
      ...reopen,
      commandId: CommandId.make("snapshot-race:reopen-drained"),
    });
    assert.strictEqual((yield* read(state.threadId)).status, "active");
  }).pipe(Effect.provide(runtime)),
);
