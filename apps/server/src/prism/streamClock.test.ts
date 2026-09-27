import { type ProviderRuntimeEvent, ThreadId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { TestClock } from "effect/testing";

import type { EventNdjsonLogger } from "../provider/Layers/EventNdjsonLogger.ts";
import { livenessView, streamStatsActivity } from "./livenessRoute.ts";
import { make, makeStreamClockState, tapLoggers, type TurnStreamStats } from "./streamClock.ts";

const THREAD = ThreadId.make("thread-1");

/** A canonical runtime event as `ProviderService` writes it to the canonical logger. */
function canonical(
  type: string,
  fields: { turnId?: string; itemId?: string; payload?: unknown } = {},
): ProviderRuntimeEvent {
  return {
    eventId: `event-${type}-${fields.itemId ?? ""}`,
    provider: "claudeAgent",
    threadId: THREAD,
    createdAt: "2026-09-27T00:00:00.000Z",
    type,
    ...(fields.turnId ? { turnId: fields.turnId } : {}),
    ...(fields.itemId ? { itemId: fields.itemId } : {}),
    payload: fields.payload ?? {},
  } as unknown as ProviderRuntimeEvent;
}

function fakeClock() {
  const ended: TurnStreamStats[] = [];
  return { ended, clock: makeStreamClockState((stats) => ended.push(stats)) };
}

it("advances on every native delta, including ones with no canonical event", () => {
  const { clock } = fakeClock();
  clock.canonical(
    canonical("turn.started", { turnId: "t1", payload: { model: "claude-opus-5-5" } }),
    1_000,
  );
  // Claude `system/thinking_tokens` and `stream_event` deltas reach only the native logger.
  clock.native(THREAD, 2_000);
  clock.native(THREAD, 9_000);
  const live = clock.liveness(THREAD);
  assert.strictEqual(live.lastStreamAt, 9_000);
  assert.strictEqual(live.turn?.eventCount, 2);
  assert.strictEqual(live.turn?.model, "claude-opus-5-5");
  assert.deepStrictEqual(livenessView(THREAD, 12_500, live).silenceMs, 3_500);
});

it("reports an open tool call and leaves its silence out of the max gap", () => {
  const { clock, ended } = fakeClock();
  clock.canonical(canonical("turn.started", { turnId: "t1" }), 0);
  clock.native(THREAD, 1_500);
  clock.canonical(canonical("content.delta", { turnId: "t1", payload: { delta: "Hi" } }), 1_500);
  clock.native(THREAD, 4_000);
  clock.canonical(
    canonical("item.started", {
      turnId: "t1",
      itemId: "tool-1",
      payload: { itemType: "command_execution", title: "vp test run" },
    }),
    4_000,
  );
  assert.deepStrictEqual(clock.liveness(THREAD).openTool, {
    itemId: "tool-1",
    itemType: "command_execution",
    title: "vp test run",
    startedAt: 4_000,
  });
  // A 60 s test run: silence the model did not cause.
  clock.canonical(
    canonical("item.completed", {
      turnId: "t1",
      itemId: "tool-1",
      payload: { itemType: "command_execution" },
    }),
    64_000,
  );
  assert.strictEqual(clock.liveness(THREAD).openTool, null);
  clock.native(THREAD, 71_000);
  clock.canonical(
    canonical("turn.completed", { turnId: "t1", payload: { state: "completed" } }),
    71_000,
  );

  assert.deepStrictEqual(ended, [
    {
      threadId: THREAD,
      turnId: "t1",
      provider: "claudeAgent",
      model: null,
      outcome: "completed",
      state: "completed",
      startedAt: 0,
      endedAt: 71_000,
      timeToFirstTokenMs: 1_500,
      maxGapMs: 7_000,
      eventCount: 3,
    },
  ]);
  assert.strictEqual(clock.liveness(THREAD).turn, null);
  assert.strictEqual(
    streamStatsActivity(ended[0]!).summary,
    "Stream completed: first token 1.5 s, max gap 7.0 s, 3 events",
  );
});

it("stops advancing when a turn is aborted and records it as aborted", () => {
  const { clock, ended } = fakeClock();
  clock.canonical(canonical("turn.started", { turnId: "t1" }), 0);
  clock.native(THREAD, 5_000);
  clock.canonical(
    canonical("turn.aborted", { turnId: "t1", payload: { reason: "interrupted" } }),
    6_000,
  );
  assert.strictEqual(clock.liveness(THREAD).lastStreamAt, 6_000);
  assert.strictEqual(ended[0]?.outcome, "aborted");
  assert.strictEqual(ended[0]?.timeToFirstTokenMs, null);

  clock.canonical(canonical("turn.started", { turnId: "t2" }), 10_000);
  clock.canonical(
    canonical("turn.completed", { turnId: "t2", payload: { state: "failed" } }),
    11_000,
  );
  clock.canonical(canonical("turn.started", { turnId: "t3" }), 12_000);
  clock.canonical(
    canonical("turn.completed", { turnId: "t3", payload: { state: "interrupted" } }),
    13_000,
  );
  assert.deepStrictEqual(
    ended.map((stats) => [stats.outcome, stats.state]),
    [
      ["aborted", "aborted"],
      ["error", "failed"],
      ["aborted", "interrupted"],
    ],
  );
});

it("knows nothing about a thread that has not streamed since startup", () => {
  const { clock } = fakeClock();
  assert.deepStrictEqual(livenessView("other", 1_000, clock.liveness(ThreadId.make("other"))), {
    threadId: "other",
    now: "1970-01-01T00:00:01.000Z",
    lastStreamAt: null,
    silenceMs: null,
    openTool: null,
    turn: null,
  });
});

it.effect("stamps the clock through the loggers and still writes to the log", () =>
  Effect.gen(function* () {
    const clock = yield* make;
    const written: unknown[] = [];
    const recorder: EventNdjsonLogger = {
      filePath: "/tmp/native.ndjson",
      write: (event) => Effect.sync(() => void written.push(event)),
      close: () => Effect.void,
    };
    // Logging is off for the canonical stream; the clock must still run.
    const loggers = tapLoggers({ native: recorder, canonical: undefined }, clock);

    yield* TestClock.setTime(1_000);
    yield* loggers.canonical!.write(canonical("turn.started", { turnId: "t1" }), THREAD);
    yield* TestClock.setTime(4_000);
    yield* loggers.native!.write({ method: "item/reasoning/textDelta" }, THREAD);
    yield* loggers.native!.write({ method: "unattributed" }, null);
    assert.strictEqual((yield* clock.liveness(THREAD)).lastStreamAt, 4_000);
    assert.strictEqual(written.length, 2);

    yield* TestClock.setTime(9_000);
    yield* loggers.canonical!.write(
      canonical("turn.completed", { turnId: "t1", payload: { state: "completed" } }),
      THREAD,
    );
    const stats = yield* clock.takeTurnEnd;
    assert.strictEqual(stats.maxGapMs, 5_000);
    assert.strictEqual(stats.eventCount, 1);
  }),
);
