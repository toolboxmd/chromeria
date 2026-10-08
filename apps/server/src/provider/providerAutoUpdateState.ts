import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

/** Only V2 projections determine admission; frozen V1 sessions are never active runtime state. */
export const readBusyProviderDrivers = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ provider: string }>`
    SELECT provider FROM orchestration_v2_projection_provider_sessions
    WHERE status IN ('starting', 'running', 'waiting')
    UNION
    SELECT provider FROM orchestration_v2_projection_runs
    WHERE status IN ('preparing', 'queued', 'starting', 'running', 'waiting')
    UNION
    SELECT provider FROM orchestration_v2_projection_provider_threads
    WHERE status = 'active'
  `;
  return new Set(rows.map((row) => row.provider));
});

export const PROVIDER_UPDATE_BUSY_REASON = "Provider work is active; the update is deferred.";
