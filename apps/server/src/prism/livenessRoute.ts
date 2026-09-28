import {
  AuthOrchestrationReadScope,
  CommandId,
  EventId,
  PRISM_STREAM_STATS_ACTIVITY_KIND,
  type PrismLiveness,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http";

import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as OrchestrationEngine from "../orchestration/Services/OrchestrationEngine.ts";
import { StreamClock, type ThreadLiveness, type TurnStreamStats } from "./streamClock.ts";
import { StaleTurnDetector, type StaleStatus } from "./staleTurnDetector.ts";

const PRISM_LIVENESS_PATH = "/api/prism/liveness";

const iso = (ms: number) => DateTime.formatIso(DateTime.makeUnsafe(ms));

/**
 * What `GET /api/prism/liveness?threadId=` returns: the server's `now` beside
 * the thread's last stream event, so the caller measures silence without
 * clock skew. `lastStreamAt` is null until the thread streams after startup.
 */
export function livenessView(
  threadId: string,
  now: number,
  liveness: ThreadLiveness,
  stale: StaleStatus,
): PrismLiveness {
  const turn = liveness.turn;
  return {
    threadId,
    now: iso(now),
    lastStreamAt: liveness.lastStreamAt === null ? null : iso(liveness.lastStreamAt),
    silenceMs: liveness.lastStreamAt === null ? null : now - liveness.lastStreamAt,
    openTool: liveness.openTool
      ? { ...liveness.openTool, startedAt: iso(liveness.openTool.startedAt) }
      : null,
    turn: turn
      ? {
          turnId: turn.turnId,
          provider: turn.provider,
          model: turn.model,
          startedAt: iso(turn.startedAt),
          eventCount: turn.eventCount,
        }
      : null,
    ...stale,
    staleSince: stale.staleSince === null ? null : iso(stale.staleSince),
  };
}

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

const jsonError = (status: number, error: string) =>
  HttpServerResponse.jsonUnsafe({ error }, { status });

/** `GET /api/prism/liveness?threadId=…`, bearer token with `orchestration:read`. */
export const prismLivenessRouteLayer = HttpRouter.add(
  "GET",
  PRISM_LIVENESS_PATH,
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const auth = yield* EnvironmentAuth.EnvironmentAuth;
    const session = yield* auth.authenticateHttpRequest(request).pipe(Effect.option);
    if (Option.isNone(session)) return jsonError(401, "unauthorized");
    if (!session.value.scopes.includes(AuthOrchestrationReadScope)) {
      return jsonError(403, `scope ${AuthOrchestrationReadScope} required`);
    }
    const url = HttpServerRequest.toURL(request);
    const threadId = (Option.isSome(url) ? url.value.searchParams.get("threadId") : null)?.trim();
    if (!threadId) return jsonError(400, "threadId required");
    // Optional so route harnesses without provider wiring still build.
    const clock = yield* Effect.serviceOption(StreamClock);
    if (Option.isNone(clock)) return jsonError(503, "stream clock unavailable");
    const detector = yield* Effect.serviceOption(StaleTurnDetector);
    if (Option.isNone(detector)) return jsonError(503, "stale detector unavailable");
    const liveness = yield* clock.value.liveness(ThreadId.make(threadId));
    const now = yield* Clock.currentTimeMillis;
    return HttpServerResponse.jsonUnsafe(
      livenessView(threadId, now, liveness, detector.value.state.status(ThreadId.make(threadId))),
    );
  }),
);

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
