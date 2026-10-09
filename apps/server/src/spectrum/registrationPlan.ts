import { threadOwner, type OrchestrationV2AppThread } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { ForkCommitGuardRejected, type ForkCommitPlan } from "../childThreads/ForkCommitPlan.ts";
import { retirementAdmission } from "../childThreads/retirement.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import type { SpectrumState } from "./state.ts";
import { insertSpectrum } from "./store.ts";

/** Registration and lineage shells commit together; only fork SQL is written by this plan. */
export function spectrumRegistrationPlan(
  state: SpectrumState,
  caller: OrchestrationV2AppThread,
): ForkCommitPlan {
  const threadId = state.threadId;
  return {
    guards: [
      ...retirementAdmission({ threadId: caller.id }).guards,
      Effect.gen(function* () {
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const current = yield* projections.getThread(caller.id);
        if (
          current.deletedAt !== null ||
          current.projectId !== caller.projectId ||
          threadOwner(current) !== threadOwner(caller)
        )
          return yield* new ForkCommitGuardRejected({ threadId, kind: "state_conflict" });
        if (state.callerRunId !== null) {
          const records = yield* projections.getThreadRecords(caller.id, ["runs"]);
          if (!records.runs.some((run) => run.id === state.callerRunId))
            return yield* new ForkCommitGuardRejected({ threadId, kind: "state_conflict" });
        }
        for (const id of [
          threadId,
          ...state.participants.map((participant) => participant.threadId),
        ]) {
          const existing = yield* projections
            .getThread(id)
            .pipe(
              Effect.map(Option.some),
              Effect.catchTags({ ProjectionStoreThreadNotFoundError: () => Effect.succeedNone }),
            );
          if (Option.isSome(existing))
            return yield* new ForkCommitGuardRejected({ threadId, kind: "state_conflict" });
        }
      }).pipe(
        Effect.mapError((error) =>
          error._tag === "ForkCommitGuardRejected"
            ? error
            : new ForkCommitGuardRejected({ threadId, kind: "storage_failure" }),
        ),
      ),
    ],
    mutations: [
      insertSpectrum(state).pipe(
        Effect.mapError(() => new ForkCommitGuardRejected({ threadId, kind: "storage_failure" })),
      ),
    ],
  };
}
