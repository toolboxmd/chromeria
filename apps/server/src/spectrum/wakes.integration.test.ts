import * as BareLaunch from "../orchestration-v2/BareLaunch.ts";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  RunId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
  type ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as SqlClient from "effect/sql/SqlClient";
import * as EffectOutbox from "../orchestration-v2/EffectOutbox.ts";
import * as Events from "../orchestration-v2/EventSink.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as Projection from "../orchestration-v2/ProjectionStore.ts";
import { layer as serialization } from "../orchestration-v2/ThreadCommandExecutor.ts";
import * as RecoveryWaitSignals from "../peer/recoveryWaitSignals.ts";
import { errorFor } from "../prism/recovery.testkit.ts";
import * as History from "../prism/RecoveryHistory.ts";
import { Scheduler } from "../scheduling/Scheduler.ts";
import * as CommandPlans from "./commandPlan.ts";
import * as Controller from "./Controller.ts";
import { base, input, setup } from "./controllerTestkit.ts";
import * as Launch from "./LaunchService.ts";
import * as McpInterrupt from "./mcpInterrupt.ts";
import * as Reports from "./ReportService.ts";
import * as Round from "./RoundService.ts";
import * as Runtime from "./runtimeLayer.ts";
import * as Adapter from "./SchedulerAdapter.ts";
import type { SpectrumState } from "./state.ts";
import { NOW } from "./testFixtures.ts";
import * as Transcript from "./TranscriptService.ts";
import * as Wakes from "./wakes.ts";

const dependencies = Layer.mergeAll(base, serialization);
const services = Layer.mergeAll(
  Launch.layer.pipe(Layer.provide(Layer.mock(BareLaunch.BareLaunch)({}))),
  Round.layer,
  Transcript.layer,
  Reports.layer,
).pipe(Layer.provideMerge(dependencies));
// Recovery writers and the wake layer share one signal hub, as in the server runtime.
const core = Layer.fresh(
  Layer.mergeAll(
    CommandPlans.layer.pipe(Layer.provide(dependencies)),
    McpInterrupt.layer.pipe(Layer.provide(dependencies)),
    Adapter.layer,
    Controller.layer.pipe(Layer.provideMerge(services)),
  ).pipe(Layer.provideMerge(RecoveryWaitSignals.layerSignals)),
);

/** One resume the wake worker ran, with the newest event sequence before and after it. */
interface Resumed {
  readonly from: number;
  readonly to: number;
  readonly state: SpectrumState;
}

/** The production wake layer around a controller that records each resume; nothing calls resume directly. */
const wakes = (resumed: Queue.Queue<Resumed>) =>
  Wakes.layer.pipe(
    Layer.provide(
      Layer.effect(
        Controller.SpectrumController,
        Effect.gen(function* () {
          const inner = yield* Controller.SpectrumController;
          const sink = yield* Events.EventSinkV2;
          return Controller.SpectrumController.of({
            resume: (id) =>
              Effect.gen(function* () {
                const from = yield* sink.latestSequence().pipe(Effect.orDie);
                const state = yield* inner.resume(id);
                const to = yield* sink.latestSequence().pipe(Effect.orDie);
                yield* Queue.offer(resumed, { from, to, state });
                return state;
              }),
          });
        }),
      ),
    ),
  );

const until = (resumed: Queue.Queue<Resumed>, done: (entry: Resumed) => boolean) =>
  Effect.gen(function* () {
    for (;;) {
      const entry = yield* Queue.take(resumed);
      if (done(entry)) return entry.state;
    }
  });

const latest = Effect.gen(function* () {
  return yield* (yield* Events.EventSinkV2).latestSequence().pipe(Effect.orDie);
});

/**
 * Waits until every wake so far has run, so the next trigger alone must wake the
 * Spectrum. Events route in order, so once a marker on the Spectrum thread is
 * routed, the earlier events' wakes are queued or coalesced; a resume that started
 * after it and committed nothing leaves nothing behind.
 */
const quiesce = (resumed: Queue.Queue<Resumed>, threadId: ThreadId) =>
  Effect.gen(function* () {
    const projections = yield* Projection.ProjectionStoreV2;
    for (let marker = 0; ; marker++) {
      yield* commit([
        {
          id: EventId.make(`marker:${threadId}:${yield* latest}:${marker}`),
          type: "thread.metadata-updated",
          threadId,
          occurredAt: NOW,
          payload: yield* projections.getThread(threadId),
        },
      ]);
      const after = yield* latest;
      for (;;) {
        const entry = yield* Queue.take(resumed);
        if (entry.from < after) continue;
        if (entry.to === entry.from) return entry.state;
        break;
      }
    }
  });

const commit = (events: ReadonlyArray<OrchestrationV2DomainEvent>) =>
  Effect.gen(function* () {
    yield* (yield* Events.EventSinkV2).write({ events });
  });

const finish = (run: OrchestrationV2Run, text: string, streaming = false) =>
  commit([
    {
      id: EventId.make(`${run.id}:complete:${streaming}`),
      type: "run.updated",
      threadId: run.threadId,
      occurredAt: NOW,
      payload: { ...run, status: "completed", completedAt: NOW },
    },
    reply(run, text, streaming),
  ]);

const reply = (
  run: OrchestrationV2Run,
  text: string,
  streaming: boolean,
): OrchestrationV2DomainEvent => ({
  id: EventId.make(`${run.id}:reply:${streaming}`),
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
    streaming,
    createdAt: NOW,
    updatedAt: NOW,
  },
});

const finishEvent = (
  run: OrchestrationV2Run,
  status: OrchestrationV2Run["status"],
): OrchestrationV2DomainEvent => ({
  id: EventId.make(`${run.id}:${status}`),
  type: "run.updated",
  threadId: run.threadId,
  occurredAt: NOW,
  payload: { ...run, status, completedAt: NOW },
});

const runOf = (threadId: ThreadId, select: (run: OrchestrationV2Run) => boolean) =>
  Effect.gen(function* () {
    const projections = yield* Projection.ProjectionStoreV2;
    const { runs } = yield* projections.getThreadRecords(threadId, ["runs"]);
    return runs.find(select)!;
  });

const slotRun = (state: SpectrumState, index: number) => {
  const slot = state.round!.slots[index]!;
  return runOf(slot.threadId, (run) => run.id === slot.runId);
};

const dispatched = (entry: Resumed) =>
  entry.state.round?.slots.every((slot) => slot.runId !== null) === true;

const retryableFailure = (run: OrchestrationV2Run): OrchestrationV2DomainEvent => {
  const item = errorFor(run);
  if (item.type !== "error") throw new Error("errorFor returns an error item");
  return {
    id: EventId.make(`event:${item.id}`),
    type: "turn-item.updated",
    threadId: run.threadId,
    occurredAt: NOW,
    payload: {
      ...item,
      failure: { class: "provider_error", message: "Provider failed", code: null, retryable: true },
    },
  };
};

const decide = (run: OrchestrationV2Run) =>
  History.writeRecoveryOutcome({
    sourceRunId: run.id,
    threadId: run.threadId,
    status: "decided",
    outcome: "not_retryable",
    reason: "non_mcp",
  });

const attempt = (state: SpectrumState) => Number(state.report?.commandId.split(":").at(-1));
const reportSent = (state: SpectrumState) =>
  state.report !== null &&
  !state.outbox.some((command) => command.commandId === state.report?.commandId);

/** A settled Spectrum, registered and settled before any wake runs, so the catch-up sends its report. */
const settled = Effect.gen(function* () {
  const state = yield* (yield* Launch.SpectrumLaunchService).register(input);
  yield* (yield* Reports.SpectrumReportService).settle(state.threadId, "Report text");
  return state.threadId;
});

const sentRun = (resumed: Queue.Queue<Resumed>) =>
  Effect.gen(function* () {
    const sent = yield* until(resumed, (entry) => reportSent(entry.state));
    assert.strictEqual(attempt(sent), 1);
    return yield* runOf(
      input.callerThreadId,
      (run) => run.userMessageId === sent.report!.messageId,
    );
  });

const scenario = <A, E, R>(body: (resumed: Queue.Queue<Resumed>) => Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    yield* setup;
    yield* History.initializeRecoveryHistory;
    const resumed = yield* Queue.unbounded<Resumed>();
    return yield* body(resumed);
  }).pipe(Effect.provide(core));

it.effect("registration and owned Color events advance a council with no sweep registered", () =>
  scenario((resumed) =>
    Effect.gen(function* () {
      const state = yield* (yield* Launch.SpectrumLaunchService).register(input);
      yield* until(resumed, dispatched);
      const running = yield* quiesce(resumed, state.threadId);
      yield* finish(yield* slotRun(running, 0), "Blue answer.");
      yield* finish(yield* slotRun(running, 1), "Red answer.");
      const relay = yield* until(resumed, (entry) => entry.state.round?.step === 1);
      assert.strictEqual(relay.threadId, state.threadId);
      assert.strictEqual(relay.status, "active");
    }).pipe(Effect.provide(wakes(resumed))),
  ),
);

it.effect("a still-streaming reply is relayed on the event that ends its stream", () =>
  scenario((resumed) =>
    Effect.gen(function* () {
      const state = yield* (yield* Launch.SpectrumLaunchService).register(input);
      yield* until(resumed, dispatched);
      const running = yield* quiesce(resumed, state.threadId);
      const blue = yield* slotRun(running, 0);
      yield* finish(blue, "Partial", true);
      const streaming = yield* quiesce(resumed, state.threadId);
      assert.isNull(streaming.round!.slots[0]!.replyIds);
      yield* commit([reply(blue, "Blue final answer.", false)]);
      const accepted = yield* until(
        resumed,
        (entry) => entry.state.round?.slots[0]?.replyIds !== null,
      );
      const projections = yield* Projection.ProjectionStoreV2;
      const transcript = yield* projections.getThreadRecords(accepted.threadId, ["messages"]);
      assert.isTrue(
        transcript.messages.some((message) => message.text === "[Blue]\nBlue final answer."),
      );
      assert.isFalse(transcript.messages.some((message) => message.text.includes("Partial")));
    }).pipe(Effect.provide(wakes(resumed))),
  ),
);

it.effect("an undelivered report retries on the caller's terminal event", () =>
  scenario((resumed) =>
    Effect.gen(function* () {
      const threadId = yield* settled;
      yield* Effect.gen(function* () {
        const first = yield* sentRun(resumed);
        yield* decide(first);
        yield* quiesce(resumed, threadId);
        yield* commit([finishEvent(first, "failed"), retryableFailure(first)]);
        const retried = yield* until(resumed, (entry) => attempt(entry.state) === 2);
        assert.strictEqual(retried.report!.text, "Report text");
      }).pipe(Effect.provide(wakes(resumed)));
    }),
  ),
);

it.effect(
  "a recovery decision after the terminal event delivers the next attempt with no further run event",
  () =>
    scenario((resumed) =>
      Effect.gen(function* () {
        const threadId = yield* settled;
        yield* Effect.gen(function* () {
          const first = yield* sentRun(resumed);
          yield* commit([finishEvent(first, "failed"), retryableFailure(first)]);
          // Every wake from the terminal event has run and held: no decision yet.
          const held = yield* quiesce(resumed, threadId);
          assert.strictEqual(attempt(held), 1);
          yield* decide(first);
          const retried = yield* until(resumed, (entry) => attempt(entry.state) === 2);
          assert.strictEqual(retried.report!.text, held.report!.text);
        }).pipe(Effect.provide(wakes(resumed)));
      }),
    ),
);

it.effect("Stop drain delivers the stop report once an owned effect settles without an event", () =>
  scenario((resumed) =>
    Effect.gen(function* () {
      const state = yield* (yield* Launch.SpectrumLaunchService).register(input);
      const effectId = "effect:fixture:owned-continue";
      const color = state.participants[0]!.threadId;
      yield* (yield* Events.EventSinkV2).commitCommand({
        commandId: CommandId.make("fixture:owned-effect"),
        threadId: color,
        commandType: "fixture",
        acceptedAt: NOW,
        events: [
          {
            id: EventId.make("fixture:owned-effect"),
            type: "thread.metadata-updated",
            threadId: color,
            occurredAt: NOW,
            payload: yield* (yield* Projection.ProjectionStoreV2).getThread(color),
          },
        ],
        effects: [
          {
            id: effectId,
            commandId: CommandId.make("fixture:owned-effect"),
            threadId: color,
            request: { type: "provider-runtime.continue", sourceRunId: RunId.make("run:fixture") },
          },
        ],
      });
      yield* (yield* Orchestrator.OrchestratorV2).dispatch({
        type: "thread.stop",
        threadId: state.threadId,
        commandId: CommandId.make("human:stop"),
      });
      yield* Effect.gen(function* () {
        yield* until(resumed, (entry) => entry.state.status === "retired");
        const held = yield* quiesce(resumed, state.threadId);
        assert.isFalse(reportSent(held));
        const sql = yield* SqlClient.SqlClient;
        yield* sql`UPDATE orchestration_v2_effect_outbox SET status='running', lease_owner='test'
          WHERE effect_id=${effectId}`;
        assert.isTrue(
          yield* (yield* EffectOutbox.EffectOutboxV2).succeed({ effectId, workerId: "test" }),
        );
        const delivered = yield* until(resumed, (entry) => reportSent(entry.state));
        assert.strictEqual(delivered.status, "retired");
      }).pipe(Effect.provide(wakes(resumed)));
    }),
  ),
);

it.effect("an ancestor's retirement stops a running council", () =>
  scenario((resumed) =>
    Effect.gen(function* () {
      const state = yield* (yield* Launch.SpectrumLaunchService).register(input);
      yield* until(resumed, dispatched);
      yield* quiesce(resumed, state.threadId);
      const projections = yield* Projection.ProjectionStoreV2;
      const caller = yield* projections.getThread(input.callerThreadId);
      yield* commit([
        {
          id: EventId.make("caller:retire"),
          type: "thread.metadata-updated",
          threadId: caller.id,
          occurredAt: NOW,
          payload: { ...caller, forkRetirement: { token: CommandId.make("caller:stop") } },
        },
      ]);
      const retired = yield* until(resumed, (entry) => entry.state.status === "retired");
      assert.isNotNull(retired.report);
    }).pipe(Effect.provide(wakes(resumed))),
  ),
);

it.effect("reopen after an abandoned stop report starts the next generation's round", () =>
  scenario((resumed) =>
    Effect.gen(function* () {
      const state = yield* (yield* Launch.SpectrumLaunchService).register(input);
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      yield* orchestrator.dispatch({
        type: "thread.stop",
        threadId: state.threadId,
        commandId: CommandId.make("human:stop"),
      });
      yield* Effect.gen(function* () {
        const retired = yield* until(resumed, (entry) => entry.state.status === "retired");
        yield* (yield* Reports.SpectrumReportService).abandon(
          state.threadId,
          retired.report!.commandId,
          "alice",
        );
        yield* quiesce(resumed, state.threadId);
        yield* orchestrator.dispatch({
          type: "thread.unsettle",
          reason: "user",
          threadId: state.threadId,
          commandId: CommandId.make("human:reopen"),
        });
        const reopened = yield* until(
          resumed,
          (entry) => dispatched(entry) && entry.state.generation > retired.generation,
        );
        assert.strictEqual(reopened.status, "active");
        assert.strictEqual(reopened.round!.generation, reopened.generation);
      }).pipe(Effect.provide(wakes(resumed)));
    }),
  ),
);

it.effect("an in-flight council resumes after a restart through the start-up catch-up", () =>
  scenario((resumed) =>
    Effect.gen(function* () {
      yield* (yield* Launch.SpectrumLaunchService).register(input);
      // The first server life dispatches round 1, then stops.
      const running = yield* until(resumed, dispatched).pipe(Effect.provide(wakes(resumed)));
      // Replies land while no wake worker runs.
      yield* finish(yield* slotRun(running, 0), "Blue answer.");
      yield* finish(yield* slotRun(running, 1), "Red answer.");
      const relay = yield* until(resumed, (entry) => entry.state.round?.step === 1).pipe(
        Effect.provide(wakes(resumed)),
      );
      assert.strictEqual(relay.status, "active");
    }),
  ),
);

it.effect("no Spectrum work runs on a timer: the runtime registers no scheduler source", () =>
  Effect.gen(function* () {
    yield* setup;
    yield* Effect.void.pipe(
      Effect.provide(Runtime.layer),
      Effect.provideService(Scheduler, {
        register: (name) => Effect.die(new Error(`Spectrum registered scheduler source ${name}`)),
      }),
    );
  }).pipe(Effect.provide(Layer.fresh(dependencies))),
);
