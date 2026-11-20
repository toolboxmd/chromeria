import { AuthSessionId } from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Ref from "effect/Ref";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as TestClock from "effect/testing/TestClock";

import { PersistenceSqlError } from "../persistence/Errors.ts";
import {
  CROSS_PROCESS_REVOCATION_POLL_INTERVAL,
  watchCrossProcessRevocations,
} from "./forkCrossProcessRevocation.ts";

const revoked = AuthSessionId.make("revoked-session");
const kept = AuthSessionId.make("kept-session");
const lockError = new PersistenceSqlError({
  operation: "SessionStore.listActive:query",
  detail: "database is locked",
});

const watching = (sessionIds: ReadonlyArray<AuthSessionId>) =>
  SubscriptionRef.make<ReadonlyMap<AuthSessionId, number>>(
    new Map(sessionIds.map((sessionId) => [sessionId, 1])),
  );

const startWatcher = Effect.fn("startWatcher")(function* (state: {
  readonly version: Ref.Ref<number>;
  readonly active: Ref.Ref<ReadonlyArray<AuthSessionId>>;
  readonly failReads: Ref.Ref<boolean>;
}) {
  const invalidated = yield* Ref.make<ReadonlyArray<AuthSessionId>>([]);
  const queries = yield* Ref.make(0);
  const failIfLocked = Effect.gen(function* () {
    if (yield* Ref.get(state.failReads)) return yield* lockError;
  });
  yield* watchCrossProcessRevocations({
    source: {
      dataVersion: Ref.get(state.version),
      listActive: (sessionIds) =>
        failIfLocked.pipe(
          Effect.andThen(Ref.update(queries, (count) => count + 1)),
          Effect.andThen(Ref.get(state.active)),
          Effect.map((active) => sessionIds.filter((sessionId) => active.includes(sessionId))),
        ),
    },
    watchedSessions: yield* watching([revoked, kept]),
    invalidate: (sessionId) => Ref.update(invalidated, (current) => [...current, sessionId]),
  }).pipe(Effect.forkScoped({ startImmediately: true }));
  return { invalidated, queries };
});

const tick = TestClock.adjust(CROSS_PROCESS_REVOCATION_POLL_INTERVAL);

it.effect("invalidates only sessions another process revoked, once per commit", () =>
  Effect.gen(function* () {
    const version = yield* Ref.make(1);
    const active = yield* Ref.make<ReadonlyArray<AuthSessionId>>([revoked, kept]);
    const failReads = yield* Ref.make(false);
    const { invalidated, queries } = yield* startWatcher({ version, active, failReads });

    yield* tick;
    expect(yield* Ref.get(queries)).toBe(1);
    yield* tick;
    expect(yield* Ref.get(queries)).toBe(1);

    // An unrelated commit re-checks but closes nothing.
    yield* Ref.set(version, 2);
    yield* tick;
    expect(yield* Ref.get(queries)).toBe(2);
    expect(yield* Ref.get(invalidated)).toEqual([]);

    yield* Ref.set(active, [kept]);
    yield* Ref.set(version, 3);
    yield* tick;
    expect(yield* Ref.get(invalidated)).toEqual([revoked]);
  }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
);

it.effect("keeps sessions open through a locked read and retries on the next tick", () =>
  Effect.gen(function* () {
    const version = yield* Ref.make(1);
    const active = yield* Ref.make<ReadonlyArray<AuthSessionId>>([kept]);
    const failReads = yield* Ref.make(true);
    const { invalidated } = yield* startWatcher({ version, active, failReads });

    yield* tick;
    yield* tick;
    expect(yield* Ref.get(invalidated)).toEqual([]);

    // The commit stays unchecked until a read succeeds, even with no newer commit.
    yield* Ref.set(failReads, false);
    yield* tick;
    expect(yield* Ref.get(invalidated)).toEqual([revoked]);
  }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
);

it.effect("checks a socket that registers while the commit version is being read", () =>
  Effect.gen(function* () {
    const late = AuthSessionId.make("late-session");
    const watched = yield* watching([kept]);
    const invalidated = yield* Ref.make<ReadonlyArray<AuthSessionId>>([]);
    yield* watchCrossProcessRevocations({
      source: {
        // The late socket registers during the version read, after a commit revoked it.
        dataVersion: SubscriptionRef.update(watched, (current) =>
          new Map(current).set(late, 1),
        ).pipe(Effect.as(1)),
        listActive: (sessionIds) => Effect.succeed(sessionIds.filter((id) => id === kept)),
      },
      watchedSessions: watched,
      invalidate: (sessionId) => Ref.update(invalidated, (current) => [...current, sessionId]),
    }).pipe(Effect.forkScoped({ startImmediately: true }));

    yield* tick;
    expect(yield* Ref.get(invalidated)).toEqual([late]);
  }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
);

it.effect("stays idle with nothing watched and starts checking once a session is", () =>
  Effect.gen(function* () {
    const watched = yield* watching([]);
    const reads = yield* Ref.make(0);
    const invalidated = yield* Ref.make<ReadonlyArray<AuthSessionId>>([]);
    yield* watchCrossProcessRevocations({
      source: {
        dataVersion: Ref.updateAndGet(reads, (count) => count + 1),
        listActive: () => Effect.succeed([]),
      },
      watchedSessions: watched,
      invalidate: (sessionId) => Ref.update(invalidated, (current) => [...current, sessionId]),
    }).pipe(Effect.forkScoped({ startImmediately: true }));

    yield* TestClock.adjust("3653 days");
    expect(yield* Ref.get(reads)).toBe(0);
    yield* SubscriptionRef.set(watched, new Map([[revoked, 1]]));
    yield* tick;
    expect(yield* Ref.get(invalidated)).toEqual([revoked]);
  }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
);
