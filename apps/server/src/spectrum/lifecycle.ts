import {
  EventId,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
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
