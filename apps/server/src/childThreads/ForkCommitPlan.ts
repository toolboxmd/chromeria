import { ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { ProjectionStoreV2 } from "../orchestration-v2/ProjectionStore.ts";

export class ForkCommitGuardRejected extends Schema.TaggedError<ForkCommitGuardRejected>()(
  "ForkCommitGuardRejected",
  {
    threadId: ThreadId,
    kind: Schema.Literals([
      "retired",
      "lineage_incomplete",
      "state_conflict",
      "invalid_transition",
      "storage_failure",
    ]),
  },
) {
  override get message() {
    return `Fork transaction rejected: ${this.kind}.`;
  }
}

/** Domain-owned plans run inside the event/receipt transaction, never on receipt replay. */
export interface ForkCommitPlan {
  readonly guards: ReadonlyArray<
    Effect.Effect<void, ForkCommitGuardRejected, SqlClient.SqlClient | ProjectionStoreV2>
  >;
  readonly mutations: ReadonlyArray<
    Effect.Effect<void, ForkCommitGuardRejected, SqlClient.SqlClient | ProjectionStoreV2>
  >;
}
