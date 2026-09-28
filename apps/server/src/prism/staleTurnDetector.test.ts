import { type ProviderRuntimeEvent, type ProviderSession, ThreadId } from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import { TestClock } from "effect/testing";

import { make, tapLoggers } from "./streamClock.ts";
import { StaleTurnDetector, staleTurnDetectorLayer } from "./staleTurnDetector.ts";

const THREAD = ThreadId.make("sub.parent.child");
const session = { threadId: THREAD, status: "running" } as ProviderSession;
const stats = {
  provider: "opencode",
  model: "muse",
  outcome: "completed",
  timeToFirstTokenMs: 10_000,
  maxGapMs: 18_000,
};

const fixture = Effect.gen(function* () {
  const detector = yield* StaleTurnDetector;
  const clock = yield* make;
  const loggers = tapLoggers({ native: undefined, canonical: undefined }, clock);
  let now = 0;
  let monotonic = 0;
  const send = (type: string, payload: unknown = {}, turnId = "turn-1") =>
    TestClock.setTime(now).pipe(
      Effect.andThen(
        clock.canonical({
          eventId: `${type}-${now}`,
          type,
          threadId: THREAD,
          provider: "opencode",
          turnId,
          itemId: "tool",
          createdAt: DateTime.formatIso(DateTime.makeUnsafe(now)),
          payload,
        } as ProviderRuntimeEvent),
      ),
    );
  const tick = (wallStep = 1_000, monoStep = wallStep, sessions = [session]) => {
    now += wallStep;
    monotonic += monoStep;
    detector.state.tick(now, monotonic, sessions);
  };
  const seconds = (count: number) => {
    for (let i = 0; i < count; i++) tick();
  };
  const native = () =>
    TestClock.setTime(now).pipe(
      Effect.andThen(loggers.native!.write({ type: "reasoning.delta" }, THREAD)),
    );
  yield* send("turn.started", { model: "muse" });
  detector.state.tick(0, 0, [session]);
  return { detector, send, tick, seconds, native, status: () => detector.state.status(THREAD) };
});

const test = (body: Effect.Effect<void, never, StaleTurnDetector>) =>
  body.pipe(Effect.provide(staleTurnDetectorLayer));

it.effect("18-second native reasoning gaps remain healthy beyond the default window", () =>
  test(
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.send("content.delta", { delta: "thinking", kind: "reasoning" });
      for (let i = 0; i < 20; i++) {
        f.seconds(18);
        expect(f.status().stale).toBe(false);
        yield* f.native();
      }
    }),
  ),
);

it.effect("confirms first-token silence at threshold plus 25% within one tick", () =>
  test(
    Effect.gen(function* () {
      const f = yield* fixture;
      f.seconds(150);
      expect(f.status()).toMatchObject({ stale: false, thresholdMs: 120_000 });
      f.tick();
      expect(f.status()).toMatchObject({ stale: true, staleSince: 151_000, reason: "silence" });
    }),
  ),
);

it.effect("uses the mid-stream window and any new event clears suspicion and stale", () =>
  test(
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.send("content.delta", { delta: "thinking" });
      f.seconds(100);
      expect(f.status().stale).toBe(false);
      yield* f.native();
      f.seconds(112);
      expect(f.status().stale).toBe(false);
      f.tick();
      expect(f.status()).toMatchObject({ stale: true, thresholdMs: 90_000 });
      const first = yield* f.detector.takeStale;
      expect(first).toMatchObject({ lastEventKind: "native:reasoning.delta", silenceMs: 113_000 });
      yield* f.native();
      expect(f.status()).toMatchObject({ stale: false, staleSince: null, reason: null });
    }),
  ),
);

it.effect("open tools suppress silence until a fresh window after the tool closes", () =>
  test(
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.send("content.delta", { delta: "thinking" });
      f.seconds(100);
      yield* f.send("item.started", { itemType: "command_execution" });
      f.seconds(500);
      expect(f.status().stale).toBe(false);
      yield* f.send("item.completed", { itemType: "command_execution" });
      f.seconds(112);
      expect(f.status().stale).toBe(false);
      f.tick();
      expect(f.status().stale).toBe(true);
    }),
  ),
);

it.effect("sleep restarts silence for monotonic clocks that exclude or include suspend", () =>
  test(
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.send("content.delta", { delta: "thinking" });
      f.seconds(100);
      f.tick(99_000, 0);
      expect(f.status().stale).toBe(false);
      f.seconds(100);
      f.tick(99_000, 99_000);
      expect(f.status().stale).toBe(false);
      f.seconds(112);
      expect(f.status().stale).toBe(false);
      f.tick();
      expect(f.status().stale).toBe(true);
    }),
  ),
);

it.effect("missing or errored provider sessions flag immediately, including during a tool", () =>
  test(
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.send("item.started", { itemType: "command_execution" });
      f.tick(1_000, 1_000, []);
      expect(f.status()).toMatchObject({ stale: true, reason: "provider-dead" });
      yield* f.send("turn.started", { model: "muse" }, "turn-2");
      f.tick(1_000, 1_000, [{ ...session, status: "error" }]);
      expect(f.status()).toMatchObject({ stale: true, reason: "provider-error" });
    }),
  ),
);

it.effect(
  "provider error and exit events flag without waiting for a tick or losing a failed turn",
  () =>
    test(
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* f.send("runtime.error", { message: "connection lost" });
        expect(f.status()).toMatchObject({ stale: true, reason: "provider-error" });
        yield* f.send("turn.completed", { state: "failed" });
        expect(f.status()).toMatchObject({ stale: true, reason: "provider-error" });
        yield* f.native();
        yield* f.send("session.state.changed", { state: "ready" });
        expect(f.status()).toMatchObject({ stale: true, reason: "provider-error" });
        const stale = yield* f.detector.takeStale;
        expect(stale.turn.turnId).toBe("turn-1");
        yield* f.send("turn.started", { model: "muse" }, "turn-2");
        expect(f.status().stale).toBe(false);
        yield* f.send("session.exited", { exitKind: "error" }, "turn-2");
        expect(f.status()).toMatchObject({ stale: true, reason: "provider-dead" });
      }),
    ),
);

it.effect("learns per provider/model/phase from 20 distinct healthy recorded samples", () =>
  test(
    Effect.gen(function* () {
      const f = yield* fixture;
      for (let i = 0; i < 19; i++) f.detector.state.record(`sample-${i}`, stats);
      f.detector.state.record("sample-0", stats);
      f.detector.state.record("failed", { ...stats, outcome: "error", maxGapMs: 900_000 });
      f.detector.state.record("no-token", { ...stats, timeToFirstTokenMs: null });
      f.detector.state.record("other-model", { ...stats, model: "other", maxGapMs: 900_000 });
      f.detector.state.record("other-provider", { ...stats, provider: "other", maxGapMs: 900_000 });
      expect(f.status()).toMatchObject({ thresholdMs: 120_000, thresholdSource: "default" });
      f.detector.state.record("sample-19", stats);
      expect(f.status()).toMatchObject({ thresholdMs: 15_000, thresholdSource: "measured" });
      yield* f.send("content.delta", { delta: "thinking" });
      expect(f.status()).toMatchObject({ thresholdMs: 27_000, thresholdSource: "measured" });
      f.seconds(33);
      expect(f.status().stale).toBe(false);
      f.tick();
      expect(f.status().stale).toBe(true);
    }),
  ),
);

it.effect(
  "defaults never undercut an observed healthy gap and invalid samples cannot poison them",
  () =>
    test(
      Effect.gen(function* () {
        const f = yield* fixture;
        f.detector.state.record("long", {
          ...stats,
          timeToFirstTokenMs: 200_000,
          maxGapMs: 150_000,
        });
        f.detector.state.record("invalid", { ...stats, maxGapMs: Infinity });
        expect(f.status()).toMatchObject({ thresholdMs: 200_000, thresholdSource: "default" });
        yield* f.send("content.delta", { delta: "thinking" });
        expect(f.status()).toMatchObject({ thresholdMs: 150_000, thresholdSource: "default" });
      }),
    ),
);
