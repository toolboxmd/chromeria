import type * as SqlClient from "effect/sql/SqlClient";
import type * as Statement from "effect/sql/Statement";

/** A lifecycle snapshot cannot erase or change the admitted source of an existing run. */
export function keepContinuationSource(sql: SqlClient.SqlClient, payload: Statement.Fragment) {
  const path = "$.forkPrismContinuationSourceRunId";
  return sql`CASE WHEN json_type(orchestration_v2_projection_runs.payload_json,${path}) IS NOT NULL
    THEN json_set(${payload},${path},json_extract(orchestration_v2_projection_runs.payload_json,${path}))
    ELSE ${payload} END`;
}
