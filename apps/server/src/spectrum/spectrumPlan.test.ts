import { assert, it } from "@effect/vitest";
import { EventId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";

import { ForkCommitGuardRejected } from "../childThreads/ForkCommitPlan.ts";
import * as CommandReceiptStore from "../orchestration-v2/CommandReceiptStore.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { APPEND, makeState, makeThread, NOW } from "./testFixtures.ts";
import { spectrumPlan } from "./spectrumPlan.ts";
import {
  applySpectrumMutation,
  ensureSpectrumSchema,
  insertSpectrum,
  readSpectrum,
} from "./store.ts";
import { planTranscriptAppend } from "./transcript.ts";

const database = SqlitePersistence.layerMemory;
const stores = Layer.mergeAll(
  EventStore.layer,
  ProjectionStore.layer,
  CommandReceiptStore.layer,
).pipe(Layer.provideMerge(database));
const runtime = Layer.fresh(EventSink.layer.pipe(Layer.provideMerge(stores)));
const setup = Effect.gen(function* () {
  yield* ensureSpectrumSchema;
  const state = makeState({ outbox: [APPEND] });
  yield* insertSpectrum(state);
  const sink = yield* EventSink.EventSinkV2;
  const thread = makeThread();
  yield* sink.write({
    events: [
      {
        id: EventId.make("spectrum:created"),
        type: "thread.created",
        threadId: thread.id,
        occurredAt: NOW,
        payload: thread,
      },
    ],
  });
  const planned = planTranscriptAppend(state, thread, APPEND, NOW);
  return {
    state,
    sink,
    planned,
    input: {
      commandId: APPEND.commandId,
      threadId: APPEND.threadId,
      commandType: APPEND.type,
      acceptedAt: NOW,
      events: planned.events,
      effects: [],
      forkPlans: [spectrumPlan(planned.mutation)],
    },
  };
});

const assertNoAppend = Effect.gen(function* () {
  const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
  const eventStore = yield* EventStore.EventStoreV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  assert.isTrue(Option.isNone(yield* receipts.getByCommandId(APPEND.commandId)));
  assert.deepStrictEqual(
    Array.from(
      yield* eventStore.readByCommandId({ commandId: APPEND.commandId }).pipe(Stream.runCollect),
    ),
    [],
  );
  const { messages, turnItems, runs } = yield* projections.getThreadRecords(APPEND.threadId, [
    "messages",
    "turnItems",
    "runs",
  ]);
  assert.deepStrictEqual(messages, []);
  assert.deepStrictEqual(turnItems, []);
  assert.deepStrictEqual(runs, []);
});

it.effect(
  "commits state, outbox consumption, transcript and receipt together, then dedupes before guards",
  () =>
    Effect.gen(function* () {
      const { sink, input, planned } = yield* setup;
      assert.isTrue((yield* sink.commitCommand(input)).committed);
      assert.deepStrictEqual(
        Option.getOrThrow(yield* readSpectrum(APPEND.threadId)),
        planned.mutation.state,
      );
      // The original guard is now stale. A receipt replay must not evaluate it.
      assert.isFalse((yield* sink.commitCommand(input)).committed);
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const { messages } = yield* store.getThreadRecords(APPEND.threadId, ["messages"]);
      assert.strictEqual(messages.length, 1);
      assert.strictEqual(messages[0]!.text, APPEND.text);
    }).pipe(Effect.provide(runtime)),
);

it.effect(
  "rechecks generation in the transaction and rolls back a reserved receipt on rejection",
  () =>
    Effect.gen(function* () {
      const { state, sink, input } = yield* setup;
      const stopped = { ...state, revision: 1, generation: 1, status: "retired" as const };
      yield* applySpectrumMutation({ expectedRevision: 0, expectedGeneration: 0, state: stopped });
      const failure = yield* sink.commitCommand(input).pipe(Effect.flip);
      assert.instanceOf(failure, ForkCommitGuardRejected);
      if (failure._tag === "ForkCommitGuardRejected")
        assert.strictEqual(failure.kind, "state_conflict");
      yield* assertNoAppend;
      assert.deepStrictEqual(Option.getOrThrow(yield* readSpectrum(APPEND.threadId)), stopped);
    }).pipe(Effect.provide(runtime)),
);

it.effect("rolls back an earlier Spectrum mutation when a later fork mutation fails", () =>
  Effect.gen(function* () {
    const { state, sink, input } = yield* setup;
    yield* sink
      .commitCommand({
        ...input,
        forkPlans: [
          ...input.forkPlans,
          {
            guards: [],
            mutations: [
              Effect.fail(
                new ForkCommitGuardRejected({ threadId: APPEND.threadId, kind: "storage_failure" }),
              ),
            ],
          },
        ],
      })
      .pipe(Effect.flip);
    yield* assertNoAppend;
    assert.deepStrictEqual(Option.getOrThrow(yield* readSpectrum(APPEND.threadId)), state);
  }).pipe(Effect.provide(runtime)),
);

it.effect("rolls back Spectrum state and the reserved receipt when event append fails", () =>
  Effect.gen(function* () {
    const { state, sink, input } = yield* setup;
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TRIGGER spectrum_reject_event BEFORE INSERT ON orchestration_events
      BEGIN SELECT RAISE(ABORT, 'Spectrum event write failed'); END`;
    yield* sink.commitCommand(input).pipe(Effect.flip);
    yield* assertNoAppend;
    assert.deepStrictEqual(Option.getOrThrow(yield* readSpectrum(APPEND.threadId)), state);
  }).pipe(Effect.provide(runtime)),
);
