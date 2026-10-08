import { ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import { readRetirementState } from "../childThreads/retirement.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as Projection from "../orchestration-v2/ProjectionStore.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import { forkParked } from "../serverActivation.ts";
import * as Coordinator from "./RecoveryCoordinator.ts";
import { retryCommand } from "./recoveryPolicy.ts";

export class RecoveryReactor extends Context.Service<
  RecoveryReactor,
  {
    readonly sweep: Effect.Effect<void>;
  }
>()("t3/prism/RecoveryReactor") {}

export const layer = Layer.effect(
  RecoveryReactor,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const coordinator = yield* Coordinator.RecoveryCoordinator;
    const projections = yield* Projection.ProjectionStoreV2;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const sink = yield* EventSink.EventSinkV2;
    const scheduler = yield* Scheduler.Scheduler;
    const permit = yield* Semaphore.make(1);
    const reconcile = Effect.gen(function* () {
      // Closed failures are not polled again; a new original run gets a new opportunity.
      const candidates = yield* sql<{ thread_id: string }>`
      SELECT thread_id FROM fork_prism_recovery WHERE state <> 'closed'
      UNION
      SELECT t.thread_id FROM orchestration_v2_projection_threads t
      JOIN orchestration_v2_projection_runs r ON r.run_id = (
        SELECT candidate.run_id FROM orchestration_v2_projection_runs candidate
        WHERE candidate.thread_id=t.thread_id AND candidate.status <> 'queued'
          AND NOT (candidate.status='cancelled' AND json_extract(candidate.payload_json,'$.startedAt') IS NULL)
        ORDER BY (candidate.completed_at IS NULL) DESC, candidate.completed_at DESC, candidate.ordinal DESC LIMIT 1
      )
      LEFT JOIN fork_prism_recovery recovery ON recovery.thread_id=t.thread_id
      WHERE json_extract(t.payload_json,'$.creationSource')='mcp' AND r.status='failed'
        AND (recovery.source_run_id IS NULL OR recovery.source_run_id <> r.run_id)`;
      for (const candidate of candidates) {
        const threadId = ThreadId.make(candidate.thread_id);
        yield* Effect.gen(function* () {
          const projection = yield* projections.getThreadRecords(
            threadId,
            ["runs", "turnItems", "runtimeRequests"],
            {
              turnItemTypes: ["error", "run_interrupt_request"],
            },
          );
          const retirement = yield* readRetirementState(threadId).pipe(
            Effect.provideService(Projection.ProjectionStoreV2, projections),
          );
          const record = yield* coordinator.observe(
            projection,
            retirement.retired || !retirement.complete,
          );
          if (record === null) return;
          const command = retryCommand(record);
          // Source eligibility and ancestry are rechecked by the composed commit plans.
          if (command !== null) yield* threads.dispatch(command);
        }).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("prism.recovery.reconcile-failed", { threadId, cause }),
          ),
        );
      }
      // Closing Stop/reset/retry state releases upstream's eventual result transfer.
      if (candidates.length > 0) yield* orchestrator.recoverDelegatedTasks;
    });
    const sweep = reconcile.pipe(
      permit.withPermits(1),
      Effect.catchCause((cause) => Effect.logWarning("prism.recovery.sweep-failed", { cause })),
    );
    yield* scheduler.register("prism-recovery", sweep);
    const sequence = yield* sink.latestSequence().pipe(Effect.orDie);
    // Event replay covers failures between subscription and server activation; the scheduler covers restarts.
    yield* forkParked(
      sink.stream({ afterSequence: sequence }).pipe(
        Stream.filter(
          (stored) =>
            stored.event.type === "run.updated" ||
            (stored.event.type === "thread.metadata-updated" &&
              (stored.event.payload.forkRetirement !== undefined ||
                stored.event.payload.limitRecovery !== undefined)),
        ),
        Stream.runForEach(() => sweep.pipe(Effect.andThen(orchestrator.recoverDelegatedTasks))),
        Effect.catchCause((cause) =>
          Effect.logWarning("prism.recovery.observer-failed", { cause }),
        ),
      ),
    );
    return RecoveryReactor.of({ sweep });
  }),
);
