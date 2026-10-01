import { CommandId, EventId, PRISM_STREAM_STATS_ACTIVITY_KIND, TurnId } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import * as OrchestrationEngine from "../orchestration/Services/OrchestrationEngine.ts";
import { StreamClock, type TurnStreamStats } from "./streamClock.ts";
import { StaleTurnDetector } from "./staleTurnDetector.ts";

const iso = (ms: number) => DateTime.formatIso(DateTime.makeUnsafe(ms));

const seconds = (ms: number) => `${(ms / 1000).toFixed(1)} s`;

export function streamStatsActivity(stats: TurnStreamStats) {
  const firstToken =
    stats.timeToFirstTokenMs === null
      ? "no tokens"
      : `first token ${seconds(stats.timeToFirstTokenMs)}`;
  return {
    kind: PRISM_STREAM_STATS_ACTIVITY_KIND,
    summary: `Stream ${stats.outcome}: ${firstToken}, max gap ${seconds(stats.maxGapMs)}, ${stats.eventCount} events`,
    payload: {
      provider: stats.provider,
      model: stats.model,
      outcome: stats.outcome,
      state: stats.state,
      startedAt: iso(stats.startedAt),
      endedAt: iso(stats.endedAt),
      durationMs: stats.endedAt - stats.startedAt,
      timeToFirstTokenMs: stats.timeToFirstTokenMs,
      maxGapMs: stats.maxGapMs,
      eventCount: stats.eventCount,
    },
  };
}

/** Records each finished turn's stream statistics as one thread activity. */
export const streamStatsRecorderLayer = Layer.effectDiscard(
  Effect.gen(function* () {
    const clock = yield* Effect.serviceOption(StreamClock);
    if (Option.isNone(clock)) return;
    const engine = yield* OrchestrationEngine.OrchestrationEngineService;
    const detector = yield* Effect.serviceOption(StaleTurnDetector);
    const crypto = yield* Crypto.Crypto;
    const record = (stats: TurnStreamStats) =>
      Effect.gen(function* () {
        const id = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
        const createdAt = iso(stats.endedAt);
        yield* engine.dispatch({
          type: "thread.activity.append",
          commandId: CommandId.make(`server:prism-stream-stats:${id}`),
          threadId: stats.threadId,
          activity: {
            id: EventId.make(id),
            tone: "info",
            ...streamStatsActivity(stats),
            turnId: stats.turnId === null ? null : TurnId.make(stats.turnId),
            createdAt,
          },
          createdAt,
        });
        if (Option.isSome(detector))
          detector.value.state.record(id, streamStatsActivity(stats).payload);
      }).pipe(
        Effect.catchCause((cause) =>
          Cause.hasInterruptsOnly(cause)
            ? Effect.failCause(cause)
            : Effect.logWarning("Could not record Prism stream statistics", { cause }),
        ),
      );
    yield* Effect.forkScoped(Effect.forever(Effect.flatMap(clock.value.takeTurnEnd, record)));
  }),
);
