import { CommandId, MessageId, type OrchestrationV2AppThread } from "@t3tools/contracts";
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

const notice = (stale: StaleTurn, observedAt: number) =>
  `[Stale child observation]\n` +
  `Silence observed at ${DateTime.formatIso(DateTime.makeUnsafe(observedAt))} on child thread ${stale.live.threadId}.\n` +
  `Model: ${stale.live.model ?? "unknown"} (${stale.live.provider}).\n` +
  `Run: ${stale.live.runId}; attempt: ${stale.live.attemptId}.\n` +
  `Last event: ${DateTime.formatIso(DateTime.makeUnsafe(stale.live.lastStreamAt))} (${stale.live.lastEventKind}).\n` +
  `Silence: ${stale.silenceMs} ms; threshold: ${stale.thresholdMs} ms (${stale.thresholdSource}).\n` +
  `This is a snapshot from detection time. The child may have resumed or finished since, and its result can arrive before this notice.\n` +
  `Before acting, check current state: t3_thread_read on thread ${stale.live.threadId} (run ${stale.live.runId}), or task_status if you hold this child's delegated taskId.\n` +
  `This notice does not stop, cancel or complete the child.`;

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
  const hasActiveDescendant = Effect.fnUntraced(function* (
    child: OrchestrationV2AppThread,
    lives: ReadonlyArray<StreamClock.ThreadLiveness>,
  ) {
    for (const live of lives) {
      if (live.threadId === child.id) continue;
      const candidate = yield* projections.getThreadRecords(live.threadId, ["runs"], {
        runIds: [live.runId],
      });
      const run = candidate.runs.find((run) => run.id === live.runId);
      if (
        run?.status !== "running" ||
        run.activeAttemptId !== live.attemptId ||
        candidate.thread.archivedAt !== null ||
        candidate.thread.deletedAt !== null ||
        candidate.thread.lineage.rootThreadId !== child.lineage.rootThreadId
      )
        continue;
      let current = candidate.thread;
      const seen = new Set([current.id]);
      while (current.lineage.parentThreadId !== null) {
        const parentId = current.lineage.parentThreadId;
        if (parentId === child.id) return true;
        if (seen.has(parentId)) break;
        seen.add(parentId);
        current = yield* projections.getThread(parentId);
      }
    }
    return false;
  });
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
        // A parent can be quiet while its descendants work. Keep their
        // activity out of provider timing samples, but renew its silence allowance.
        if (yield* hasActiveDescendant(child.thread, lives)) {
          detector.pause(live, now);
          return;
        }
        const key = `server:prism-stale:${live.runId}:${live.attemptId}:${live.providerTurnId ?? "pending"}`;
        const text = notice(stale, now);
        const title = child.thread.title.split("\n")[0]?.trim().slice(0, 80) || "Child";
        yield* threads.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(key),
          messageId: MessageId.make(key),
          threadId: parent.id,
          senderThreadId: child.thread.id,
          text,
          notification: {
            source: { kind: "subagent", childThreadId: child.thread.id },
            outcome: "updated",
            summary: `Silence observed: "${title}" had no output for ${Math.round(stale.silenceMs / 1_000)} s`,
            detail: text,
          },
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "agent",
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
