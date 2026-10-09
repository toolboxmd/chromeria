import type {
  OrchestrationV2ServerCommand,
  OrchestrationV2DomainEvent,
  ServerSettingsError,
  ThreadId,
} from "@t3tools/contracts";
import { RunId } from "@t3tools/contracts";
import * as SqlClient from "effect/sql/SqlClient";
import { latestExecutedRun } from "@t3tools/shared/orchestrationV2ThreadError";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type { SqlError } from "effect/sql/SqlError";
import type { ForkCommitPlan } from "../childThreads/ForkCommitPlan.ts";
import * as Projection from "../orchestration-v2/ProjectionStore.ts";
import * as Coordinator from "./RecoveryCoordinator.ts";
import { readRecoveryProjection } from "./recoveryProjection.ts";
import { readRetirementState } from "../childThreads/retirement.ts";
import { continuationAdmission } from "./continuationAdmission.ts";
import { retryAdmission } from "./recoveryAdmission.ts";

/** Optional fork behavior; upstream orchestration retains its ordinary finalization. */
export class RecoveryHooks extends Context.Reference<{
  readonly holdsFinalization: (
    threadId: ThreadId,
    expectedRunId: RunId,
  ) => Effect.Effect<boolean, SqlError | ServerSettingsError | Projection.ProjectionStoreV2Error>;
  readonly commitPlans: (
    command: OrchestrationV2ServerCommand,
    events?: ReadonlyArray<OrchestrationV2DomainEvent>,
  ) => ReadonlyArray<ForkCommitPlan>;
}>("t3/prism/RecoveryHooks", {
  defaultValue: () => ({ holdsFinalization: () => Effect.succeed(false), commitPlans: () => [] }),
}) {}

/** The retirement implementation stays owned by child-threads; this layer holds no ancestry state. */
export const layer = Layer.effect(
  RecoveryHooks,
  Effect.gen(function* () {
    const coordinator = yield* Coordinator.RecoveryCoordinator;
    const projections = yield* Projection.ProjectionStoreV2;
    const sql = yield* SqlClient.SqlClient;
    return {
      holdsFinalization: (threadId: ThreadId, expectedRunId: RunId) =>
        Effect.gen(function* () {
          // Arm current recovery before result transfer; historical catch-up belongs to the reactor.
          // Monitor wake runs are not task work (delegatedTaskProgress skips them), so a later
          // one must not supersede the result run and hold it forever.
          const latest = yield* sql<{ run_id: string }>`SELECT run_id
            FROM orchestration_v2_projection_runs r WHERE thread_id=${threadId} AND status<>'queued'
              AND NOT(status='cancelled' AND json_extract(payload_json,'$.startedAt') IS NULL)
              AND NOT EXISTS (SELECT 1 FROM orchestration_v2_projection_messages m
                WHERE m.run_id=r.run_id
                  AND json_extract(m.payload_json,'$.notification.source.kind')='monitor')
            ORDER BY (completed_at IS NULL) DESC,completed_at DESC,ordinal DESC,run_id DESC LIMIT 1`;
          const runIds = [
            expectedRunId,
            ...(latest[0] === undefined ? [] : [RunId.make(latest[0].run_id)]),
          ];
          const projection = yield* readRecoveryProjection(threadId, runIds).pipe(
            Effect.provideService(Projection.ProjectionStoreV2, projections),
            Effect.provideService(SqlClient.SqlClient, sql),
          );
          const retirement = yield* readRetirementState(threadId).pipe(
            Effect.provideService(Projection.ProjectionStoreV2, projections),
          );
          yield* coordinator.observe(projection, retirement.retired || !retirement.complete);
          return (
            latestExecutedRun(projection.runs)?.id !== expectedRunId ||
            (yield* coordinator.holdsResult(threadId))
          );
        }),
      commitPlans: (
        command: OrchestrationV2ServerCommand,
        events: ReadonlyArray<OrchestrationV2DomainEvent> = [],
      ) => {
        const plan = command.type === "message.dispatch" ? retryAdmission(command) : null;
        const continuation = continuationAdmission(command, events);
        return [plan, continuation].filter((entry): entry is ForkCommitPlan => entry !== null);
      },
    };
  }),
);

/** Preserve explicit server source identity on every continuation run, never infer it from messages. */
export function continuationRunFields(command: OrchestrationV2ServerCommand) {
  if (command.type !== "message.dispatch") return {};
  const source = command.forkPrismRetryOfRunId ?? command.usageLimitContinuationOfRunId;
  return source === undefined ? {} : { forkPrismContinuationSourceRunId: source };
}
