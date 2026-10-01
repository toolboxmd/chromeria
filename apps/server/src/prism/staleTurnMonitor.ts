import {
  CommandId,
  MessageId,
  PRISM_STREAM_STATS_ACTIVITY_KIND,
  ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { parentThreadIdOf } from "../mcp/toolkits/threads/subagentThreadId.ts";
import { OrchestrationEngineService } from "../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import { ProviderService } from "../provider/Services/ProviderService.ts";
import { StaleTurnDetector, type StaleTurn } from "./staleTurnDetector.ts";

const iso = (ms: number) => DateTime.formatIso(DateTime.makeUnsafe(ms));
const decodeStatsRow = Schema.decodeUnknownOption(
  Schema.Struct({
    id: Schema.String,
    payload: Schema.fromJsonString(Schema.Unknown),
  }),
);

const notifyStaleParent = Effect.fn("Prism.notifyStaleParent")(function* (stale: StaleTurn) {
  const parentId = parentThreadIdOf(stale.threadId);
  if (parentId === null) return;
  const snapshots = yield* ProjectionSnapshotQuery;
  const parent = yield* snapshots.getThreadShellById(ThreadId.make(parentId));
  if (Option.isNone(parent)) return;
  const engine = yield* OrchestrationEngineService;
  const id = yield* (yield* Crypto.Crypto).randomUUIDv4.pipe(Effect.orDie);
  const createdAt = yield* DateTime.now.pipe(Effect.map(DateTime.formatIso));
  yield* engine.dispatch({
    type: "thread.turn.start",
    commandId: CommandId.make(`server:prism-stale:${id}`),
    threadId: parent.value.id,
    message: {
      messageId: MessageId.make(id),
      role: "user",
      text:
        `[Stale child thread ${stale.threadId}]\n` +
        `Model: ${stale.turn.model ?? "unknown"} (${stale.turn.provider}).\n` +
        `Last event: ${iso(stale.lastEventAt)} (${stale.lastEventKind}).\n` +
        `Silence: ${stale.silenceMs} ms; threshold: ${stale.thresholdMs} ms (${stale.thresholdSource}).\n` +
        `Reason: ${stale.reason}.`,
      attachments: [],
    },
    runtimeMode: parent.value.runtimeMode,
    interactionMode: parent.value.interactionMode,
    createdAt,
  });
});

const logFailure = (cause: Cause.Cause<unknown>) =>
  Cause.hasInterruptsOnly(cause)
    ? Effect.interrupt
    : Effect.logWarning("Prism stale-turn monitor failed", { cause });

/** Scoped to the existing server, with no cancel/retry authority. */
export const startStaleTurnMonitor = Effect.fnUntraced(function* (
  monotonicTime: Effect.Effect<number> = Effect.sync(() => performance.now()),
) {
  const detector = yield* StaleTurnDetector;
  const sql = yield* SqlClient.SqlClient;
  const providers = yield* ProviderService;
  // The general activity query excludes archived threads. Healthy samples
  // remain useful after archival, so read only the saved stats across history.
  yield* sql`
    SELECT activity_id AS id, payload_json AS payload
    FROM projection_thread_activities
    WHERE kind = ${PRISM_STREAM_STATS_ACTIVITY_KIND}
  `.pipe(
    Effect.tap((activities) =>
      Effect.sync(() => {
        for (const row of activities) {
          const activity = decodeStatsRow(row);
          if (Option.isSome(activity))
            detector.state.record(activity.value.id, activity.value.payload);
        }
      }),
    ),
    Effect.catchCause(logFailure),
  );
  const tick = Effect.gen(function* () {
    const sessionsObservedAt = yield* Clock.currentTimeMillis;
    const sessions = yield* providers.listSessions();
    const now = yield* Clock.currentTimeMillis;
    detector.state.tick(now, yield* monotonicTime, sessions, sessionsObservedAt);
  }).pipe(Effect.catchCause(logFailure));
  yield* Effect.forkScoped(Effect.forever(tick.pipe(Effect.andThen(Effect.sleep("1 second")))));
  yield* Effect.forkScoped(
    Effect.forever(
      Effect.flatMap(detector.takeStale, notifyStaleParent).pipe(Effect.catchCause(logFailure)),
    ),
  );
});

export const staleTurnMonitorLayer = Layer.effectDiscard(
  Effect.gen(function* () {
    // Route-only test harnesses do not construct provider event loggers.
    const detector = yield* Effect.serviceOption(StaleTurnDetector);
    if (Option.isSome(detector))
      yield* startStaleTurnMonitor().pipe(Effect.provideService(StaleTurnDetector, detector.value));
  }),
);
