import { OrchestrationV2RuntimeRequestJson, type RunId, type ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Projection from "../orchestration-v2/ProjectionStore.ts";

const decodeRequest = Schema.decodeUnknownSync(
  Schema.fromJsonString(OrchestrationV2RuntimeRequestJson),
);

/** Recovery needs pending existence, never resolved request history. */
export const readRecoveryProjection = Effect.fn("Prism.readRecoveryProjection")(function* (
  threadId: ThreadId,
  runIds: ReadonlyArray<RunId>,
) {
  const projections = yield* Projection.ProjectionStoreV2;
  const sql = yield* SqlClient.SqlClient;
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      const projection = yield* projections.getThreadRecords(threadId, ["runs", "turnItems"], {
        runIds,
        turnItemRunIds: runIds,
        turnItemTypes: ["error", "run_interrupt_request"],
      });
      // Migration055 already indexes (thread_id,status); LIMIT1 bounds hydration even with many pending requests.
      const pending = yield* sql<{ payload_json: string }>`SELECT payload_json
      FROM orchestration_v2_projection_runtime_requests WHERE thread_id=${threadId} AND status='pending' LIMIT 1`;
      return {
        ...projection,
        runtimeRequests: pending.map((row) => decodeRequest(row.payload_json)),
      };
    }),
  );
});
