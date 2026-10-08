import { assert, it } from "@effect/vitest";
import { MessageId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { APPEND, makeState } from "./testFixtures.ts";
import {
  applySpectrumMutation,
  ensureSpectrumSchema,
  insertSpectrum,
  readSpectrum,
  SpectrumMutationRejected,
  SpectrumStoreError,
} from "./store.ts";

const database = Layer.fresh(SqlitePersistence.layerMemory);
const prepared = Effect.gen(function* () {
  yield* ensureSpectrumSchema;
  yield* insertSpectrum(makeState({ outbox: [APPEND] }));
});
const read = Effect.gen(function* () {
  return Option.getOrThrow(yield* readSpectrum(APPEND.threadId));
});

it.effect(
  "refuses duplicate participant threads before persisting colliding barrier identities",
  () =>
    Effect.gen(function* () {
      yield* ensureSpectrumSchema;
      const state = makeState();
      const duplicate = {
        ...state,
        participants: [state.participants[0]!, state.participants[0]!],
      };
      const result = yield* insertSpectrum(duplicate).pipe(Effect.result);
      assert.isTrue(result._tag === "Failure");
      assert.isTrue(Option.isNone(yield* readSpectrum(state.threadId)));
      yield* insertSpectrum(state);
      assert.deepStrictEqual(Option.getOrThrow(yield* readSpectrum(state.threadId)), state);
    }).pipe(Effect.provide(database)),
);

it.effect("recovers exact persisted inbox, outbox, cursor and request identities", () =>
  Effect.gen(function* () {
    yield* ensureSpectrumSchema;
    const state = makeState({
      cursor: 193,
      inbox: [MessageId.make("user-input")],
      outbox: [APPEND],
      schedulerRunId: "scheduler-original-run",
    });
    yield* insertSpectrum(state);
    yield* ensureSpectrumSchema;
    assert.deepStrictEqual(Option.getOrThrow(yield* readSpectrum(state.threadId)), state);
  }).pipe(Effect.provide(database)),
);

it.effect("atomically advances a revision and refuses a stale outbox replay", () =>
  Effect.gen(function* () {
    yield* prepared;
    const old = yield* read;
    const mutation = {
      expectedRevision: 0,
      expectedGeneration: 0,
      state: { ...old, revision: 1, cursor: 22, outbox: [] },
    };
    yield* applySpectrumMutation(mutation);
    assert.deepStrictEqual(yield* read, mutation.state);
    const failure = yield* applySpectrumMutation(mutation).pipe(Effect.flip);
    assert.instanceOf(failure, SpectrumMutationRejected);
    if (failure._tag === "SpectrumMutationRejected") assert.strictEqual(failure.kind, "stale");
    assert.deepStrictEqual(yield* read, mutation.state);
  }).pipe(Effect.provide(database)),
);

it.effect("rolls back mutations and inbox/outbox consumption when a later operation fails", () =>
  Effect.gen(function* () {
    yield* prepared;
    const sql = yield* SqlClient.SqlClient;
    const old = yield* read;
    const failure = yield* sql
      .withTransaction(
        Effect.gen(function* () {
          yield* applySpectrumMutation({
            expectedRevision: 0,
            expectedGeneration: 0,
            state: { ...old, revision: 1, transcript: [APPEND.messageId], outbox: [] },
          });
          return yield* Effect.fail("event-write-failed");
        }),
      )
      .pipe(Effect.flip);
    assert.strictEqual(failure, "event-write-failed");
    assert.deepStrictEqual(yield* read, old);
  }).pipe(Effect.provide(database)),
);

it.effect("refuses cursor rewind and modification of original report ownership", () =>
  Effect.gen(function* () {
    yield* ensureSpectrumSchema;
    const old = makeState({ cursor: 42, schedulerRunId: "run-one" });
    yield* insertSpectrum(old);
    for (const next of [
      { ...old, revision: 1, cursor: 41 },
      { ...old, revision: 1, schedulerRunId: "run-two" },
      { ...old, revision: 3 },
    ]) {
      const failure = yield* applySpectrumMutation({
        expectedRevision: 0,
        expectedGeneration: 0,
        state: next,
      }).pipe(Effect.flip);
      assert.instanceOf(failure, SpectrumMutationRejected);
      if (failure._tag === "SpectrumMutationRejected")
        assert.strictEqual(failure.kind, "invalid-transition");
    }
    assert.deepStrictEqual(yield* read, old);
  }).pipe(Effect.provide(database)),
);

it.effect("rejects corrupt persisted state rather than silently starting a fresh controller", () =>
  Effect.gen(function* () {
    yield* prepared;
    const sql = yield* SqlClient.SqlClient;
    yield* sql`UPDATE fork_spectra SET payload_json = '{"version":99}' WHERE thread_id = ${APPEND.threadId}`;
    const failure = yield* readSpectrum(APPEND.threadId).pipe(Effect.flip);
    assert.instanceOf(failure, SpectrumStoreError);
  }).pipe(Effect.provide(database)),
);

it.effect(
  "drains retired outbox effects without allowing recovery to reopen a stopped generation",
  () =>
    Effect.gen(function* () {
      yield* ensureSpectrumSchema;
      const stopped = makeState({ status: "retired", outbox: [APPEND] });
      yield* insertSpectrum(stopped);
      const failure = yield* applySpectrumMutation({
        expectedRevision: 0,
        expectedGeneration: 0,
        state: { ...stopped, revision: 1, status: "active" },
      }).pipe(Effect.flip);
      assert.instanceOf(failure, SpectrumMutationRejected);
      const drained = { ...stopped, revision: 1, outbox: [] };
      yield* applySpectrumMutation({ expectedRevision: 0, expectedGeneration: 0, state: drained });
      assert.deepStrictEqual(yield* read, drained);
    }).pipe(Effect.provide(database)),
);
