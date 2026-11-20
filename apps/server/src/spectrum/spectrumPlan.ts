import * as Effect from "effect/Effect";

import { ForkCommitGuardRejected, type ForkCommitPlan } from "../childThreads/ForkCommitPlan.ts";
import { retirementAdmission } from "../childThreads/retirement.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import type { SpectrumMutation } from "./state.ts";
import { applySpectrumMutation, checkSpectrumMutation } from "./store.ts";

/** Constructed from data only; all reads and fork writes resolve inside the commit transaction. */
export function spectrumPlan(
  mutation: SpectrumMutation,
  admission?: { readonly resume: boolean; readonly humanOverride?: boolean },
): ForkCommitPlan {
  const threadId = mutation.state.threadId;
  const stateGuard = checkSpectrumMutation(mutation).pipe(
    Effect.catchTags({
      SpectrumMutationRejected: (error) =>
        new ForkCommitGuardRejected({
          threadId,
          kind: error.kind === "invalid-transition" ? "invalid_transition" : "state_conflict",
        }),
      SpectrumStoreError: () => new ForkCommitGuardRejected({ threadId, kind: "storage_failure" }),
    }),
  );
  const threadGuard = Effect.gen(function* () {
    const store = yield* ProjectionStore.ProjectionStoreV2;
    const thread = yield* store
      .getThread(threadId)
      .pipe(
        Effect.mapError(() => new ForkCommitGuardRejected({ threadId, kind: "storage_failure" })),
      );
    if (thread.deletedAt !== null) {
      return yield* new ForkCommitGuardRejected({ threadId, kind: "retired" });
    }
    if (thread.activeProviderThreadId !== null) {
      return yield* new ForkCommitGuardRejected({ threadId, kind: "invalid_transition" });
    }
  });
  const mutationEffect = applySpectrumMutation(mutation).pipe(
    Effect.catchTags({
      SpectrumMutationRejected: (error) =>
        new ForkCommitGuardRejected({
          threadId,
          kind: error.kind === "invalid-transition" ? "invalid_transition" : "state_conflict",
        }),
      SpectrumStoreError: () => new ForkCommitGuardRejected({ threadId, kind: "storage_failure" }),
    }),
  );
  return {
    guards: [
      ...(mutation.state.status === "retired"
        ? []
        : retirementAdmission({ threadId, ...admission }).guards),
      threadGuard,
      stateGuard,
    ],
    mutations: [mutationEffect],
  };
}
