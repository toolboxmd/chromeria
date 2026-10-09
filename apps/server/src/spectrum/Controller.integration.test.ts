import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  RunId,
  ScheduledTaskId,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Option from "effect/Option";
import * as Events from "../orchestration-v2/EventSink.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as Projection from "../orchestration-v2/ProjectionStore.ts";
import { layer as serialization } from "../orchestration-v2/ThreadCommandExecutor.ts";
import { initializeRecoveryHistory } from "../prism/RecoveryHistory.ts";
import { reportsFence } from "../scheduledTaskChecks/handoff.ts";
import { ensureCheckSchema, writeCheckState } from "../scheduledTaskChecks/store.ts";
import * as Controller from "./Controller.ts";
import * as CommandPlans from "./commandPlan.ts";
import * as Adapter from "./SchedulerAdapter.ts";
import * as Launch from "./LaunchService.ts";
import * as Round from "./RoundService.ts";
import * as Transcript from "./TranscriptService.ts";
import * as Reports from "./ReportService.ts";
import { base, input, setup } from "./controllerTestkit.ts";
import { makeRun, NOW } from "./testFixtures.ts";
import { readSpectrum } from "./store.ts";
import { reportAdmission } from "./reportPolicy.ts";
import { ForkDispatchPlans } from "../fork/ForkDispatchPlans.ts";

const dependencies = Layer.mergeAll(base, serialization);
const services = Layer.mergeAll(Launch.layer, Round.layer, Transcript.layer, Reports.layer).pipe(
  Layer.provideMerge(dependencies),
);
const runtime = Layer.fresh(
  Layer.mergeAll(
    CommandPlans.layer,
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
      for (const text of [firstReply, "Second answer exactly."]) {
        const slot = current.round!.slots[0]!;
        const records = yield* projections.getThreadRecords(slot.threadId, ["runs"]);
        const run = records.runs.find((run) => run.id === slot.runId)!;
        yield* finish(run, text);
        current = yield* Effect.gen(function* () {
          return yield* (yield* Controller.SpectrumController).resume(state.threadId);
        }).pipe(Effect.provide(Layer.fresh(Controller.layer)));
      }
      assert.strictEqual(current.status, "settled");
      assert.isAbove(current.cursor, 0);
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

it.effect(
  "council waits for every Color before relays and then only the moderator synthesizes",
  () =>
    Effect.gen(function* () {
      yield* setup;
      yield* initializeRecoveryHistory;
      const state = yield* (yield* Launch.SpectrumLaunchService).register(input);
      const controller = yield* Controller.SpectrumController;
      const projections = yield* Projection.ProjectionStoreV2;
      let current = yield* controller.resume(state.threadId);
      const phases = [];
      for (let step = 0; step < 4; step++) {
        phases.push(current.round!.phase);
        const slots = current.round!.slots;
        if (step === 3)
          assert.deepStrictEqual(
            slots.map((slot) => slot.threadId),
            [state.participants[1]!.threadId],
          );
        for (let index = 0; index < slots.length; index++) {
          const slot = slots[index]!;
          const child = yield* projections.getThreadRecords(slot.threadId, ["runs"]);
          yield* finish(
            child.runs.find((run) => run.id === slot.runId)!,
            `${step}/${index} reply`,
          );
          current = yield* controller.resume(state.threadId);
          if (index < slots.length - 1) assert.strictEqual(current.round!.step, step);
        }
      }
      assert.deepStrictEqual(phases, ["independent", "relay", "relay", "synthesis"]);
      assert.strictEqual(current.status, "settled");
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
      assert.deepStrictEqual(replay, first);
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
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const stop = {
        type: "thread.stop" as const,
        threadId: state.threadId,
        commandId: CommandId.make("human:stop"),
      };
      yield* orchestrator.dispatch(stop);
      const retired = yield* controller.resume(state.threadId);
      assert.strictEqual(retired.status, "retired");
      assert.isAbove(retired.generation, running.generation);
      const projections = yield* Projection.ProjectionStoreV2;
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
