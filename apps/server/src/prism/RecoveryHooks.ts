import type {
  OrchestrationV2ServerCommand,
  RunId,
  ServerSettingsError,
  ThreadId,
} from "@t3tools/contracts";
import { latestExecutedRun } from "@t3tools/shared/orchestrationV2ThreadError";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type { SqlError } from "effect/sql/SqlError";
import type { ForkCommitPlan } from "../childThreads/ForkCommitPlan.ts";
import * as Projection from "../orchestration-v2/ProjectionStore.ts";
import * as Coordinator from "./RecoveryCoordinator.ts";
import { retryAdmission } from "./recoveryAdmission.ts";

/** Optional fork behavior; upstream orchestration retains its ordinary finalization. */
export class RecoveryHooks extends Context.Reference<{
  readonly holdsFinalization: (
    threadId: ThreadId,
    expectedRunId: RunId,
  ) => Effect.Effect<boolean, SqlError | ServerSettingsError | Projection.ProjectionStoreV2Error>;
  readonly commitPlans: (command: OrchestrationV2ServerCommand) => ReadonlyArray<ForkCommitPlan>;
}>("t3/prism/RecoveryHooks", {
  defaultValue: () => ({ holdsFinalization: () => Effect.succeed(false), commitPlans: () => [] }),
}) {}

/** The retirement implementation stays owned by child-threads; this layer holds no ancestry state. */
export const layer = (
  readRetirementState: (threadId: ThreadId) => Effect.Effect<
    {
      readonly retired: boolean;
      readonly complete: boolean;
    },
    Projection.ProjectionStoreV2Error
  >,
) =>
  Layer.effect(
    RecoveryHooks,
    Effect.gen(function* () {
      const coordinator = yield* Coordinator.RecoveryCoordinator;
      const projections = yield* Projection.ProjectionStoreV2;
      return {
        holdsFinalization: (threadId: ThreadId, expectedRunId: RunId) =>
          Effect.gen(function* () {
            // The first terminal observer must arm recovery before transferring a failed task result.
            const projection = yield* projections.getThreadRecords(
              threadId,
              ["runs", "turnItems", "runtimeRequests"],
              {
                turnItemTypes: ["error", "run_interrupt_request"],
              },
            );
            const retirement = yield* readRetirementState(threadId);
            yield* coordinator.observe(projection, retirement.retired || !retirement.complete);
            return (
              latestExecutedRun(projection.runs)?.id !== expectedRunId ||
              (yield* coordinator.holdsResult(threadId))
            );
          }),
        commitPlans: (command: OrchestrationV2ServerCommand) => {
          const plan = command.type === "message.dispatch" ? retryAdmission(command) : null;
          return plan === null ? [] : [plan];
        },
      };
    }),
  );
