import { CommandId, MessageId } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Semaphore from "effect/Semaphore";

import { readRetirementState } from "../childThreads/retirement.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import { forkParked } from "../serverActivation.ts";
import { makeStaleTurnDetectorState, type StaleTurn } from "./staleTurnDetector.ts";
import * as StreamClock from "./streamClock.ts";

export class StaleTurnMonotonicClock extends Context.Reference<Effect.Effect<number>>(
  "t3/prism/StaleTurnMonotonicClock",
  {
    defaultValue: () => Effect.sync(() => performance.now()),
  },
) {}

const notice = (stale: StaleTurn) =>
  `[Stale child thread ${stale.live.threadId}]\n` +
  `Model: ${stale.live.model ?? "unknown"} (${stale.live.provider}).\n` +
  `Run: ${stale.live.runId}; attempt: ${stale.live.attemptId}.\n` +
  `Last event: ${DateTime.formatIso(DateTime.makeUnsafe(stale.live.lastStreamAt))} (${stale.live.lastEventKind}).\n` +
  `Silence: ${stale.silenceMs} ms; threshold: ${stale.thresholdMs} ms (${stale.thresholdSource}).\nReason: silence.`;

export class StaleTurnMonitor extends Context.Service<
  StaleTurnMonitor,
  {
    readonly sweep: Effect.Effect<void>;
  }
>()("t3/prism/staleTurnMonitor") {}

const make = Effect.gen(function* () {
  const clock = yield* StreamClock.StreamClock;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const threads = yield* ThreadManagement.ThreadManagementService;
  const monotonic = yield* StaleTurnMonotonicClock;
  const permit = yield* Semaphore.make(1);
  const detector = makeStaleTurnDetectorState();
  const sweep = Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;
    const lives = yield* clock.liveness;
    const samples = yield* clock.healthySamples;
    detector.tick(now, yield* monotonic, lives);
    for (const live of lives) {
      const stale = detector.check(live, now, samples);
      if (stale === null) continue;
      // Projection reads happen only at a suspected silence, never per token or per tick.
      yield* Effect.gen(function* () {
        const child = yield* projections.getThreadRecords(live.threadId, [
          "runs",
          "runtimeRequests",
        ]);
        const run = child.runs.find((run) => run.id === live.runId);
        const parentId =
          child.thread.lineage.relationshipToParent === "subagent"
            ? child.thread.lineage.parentThreadId
            : null;
        const retirement = yield* readRetirementState(live.threadId).pipe(
          Effect.provideService(ProjectionStore.ProjectionStoreV2, projections),
        );
        if (
          run?.status !== "running" ||
          run.activeAttemptId !== live.attemptId ||
          child.thread.archivedAt !== null ||
          child.thread.deletedAt !== null ||
          child.runtimeRequests.some((request) => request.status === "pending") ||
          retirement.retired ||
          !retirement.complete ||
          parentId === null
        ) {
          detector.pause(live, now);
          return;
        }
        const parent = yield* projections.getThread(parentId);
        if (parent.archivedAt !== null || parent.deletedAt !== null) {
          detector.pause(live, now);
          return;
        }
        const key = `server:prism-stale:${live.runId}:${live.attemptId}:${live.providerTurnId ?? "pending"}`;
        yield* threads.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(key),
          messageId: MessageId.make(key),
          threadId: parent.id,
          senderThreadId: child.thread.id,
          text: notice(stale),
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "system",
          creationSource: "server",
        });
        detector.notified(live);
      }).pipe(
        Effect.catchCause((cause) =>
          Cause.hasInterruptsOnly(cause)
            ? Effect.interrupt
            : Effect.logWarning("prism.stale-turn.notice-failed", {
                threadId: live.threadId,
                cause,
              }),
        ),
      );
    }
  }).pipe(
    permit.withPermits(1),
    Effect.catchCause((cause) =>
      Cause.hasInterruptsOnly(cause)
        ? Effect.interrupt
        : Effect.logWarning("prism.stale-turn.sweep-failed", { cause }),
    ),
  );
  // This scoped timer needs a one-second cadence for the existing suspend detector.
  yield* forkParked(sweep.pipe(Effect.andThen(Effect.sleep("1 second")), Effect.forever));
  return StaleTurnMonitor.of({ sweep });
});

export const layer = Layer.effect(StaleTurnMonitor, make);
