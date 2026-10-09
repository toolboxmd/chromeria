import {
  EventId,
  OrchestrationV2AppThread as AppThreadSchema,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { ForkCommitGuardRejected, type ForkCommitPlan } from "../childThreads/ForkCommitPlan.ts";
import { ProjectionStoreV2 } from "../orchestration-v2/ProjectionStore.ts";
import type * as DateTime from "effect/DateTime";
import type { SpectrumState } from "./state.ts";

/** Publish the fork table's lifecycle in the same transaction as the state transition. */
export const lifecycleEvents = (
  thread: OrchestrationV2AppThread,
  state: SpectrumState,
  key: string,
  now: DateTime.Utc,
): ReadonlyArray<OrchestrationV2DomainEvent> =>
  thread.forkSpectrumRunning === (state.status === "active")
    ? []
    : [
        {
          id: EventId.make(`${key}:spectrum:lifecycle`),
          type: "thread.metadata-updated",
          threadId: thread.id,
          occurredAt: now,
          payload: { ...thread, forkSpectrumRunning: state.status === "active", updatedAt: now },
        },
      ];

const sameThread = Schema.toEquivalence(AppThreadSchema);

/** Full-thread metadata events must not overwrite a rename or another concurrent shell update. */
export const lifecyclePlan = (snapshot: OrchestrationV2AppThread): ForkCommitPlan => ({
  guards: [
    Effect.gen(function* () {
      const projection = yield* ProjectionStoreV2;
      const current = yield* projection
        .getThread(snapshot.id)
        .pipe(
          Effect.mapError(
            () => new ForkCommitGuardRejected({ threadId: snapshot.id, kind: "storage_failure" }),
          ),
        );
      if (!sameThread(snapshot, current))
        return yield* new ForkCommitGuardRejected({
          threadId: snapshot.id,
          kind: "state_conflict",
        });
    }),
  ],
  mutations: [],
});
