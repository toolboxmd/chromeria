import { RunId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";

const identity = { sourceRunId: RunId, threadId: ThreadId };
/** Automatic recovery facts; abandoned never records a human report-abandon instruction. */
export const RecoveryOutcome = Schema.Union([
  Schema.Struct({ ...identity, status: Schema.Literal("pending"), reason: Schema.Literals(["decision", "retry", "reset"]) }),
  Schema.Struct({ ...identity, status: Schema.Literal("decided"), outcome: Schema.Literal("retried"), successorRunId: RunId }),
  Schema.Struct({ ...identity, status: Schema.Literal("decided"), outcome: Schema.Literal("not_retryable"), reason: Schema.Literals(["non_mcp", "superseded", "non_retryable", "missing_failure", "exhausted", "opted_out", "invalid_reset"]) }),
  Schema.Struct({ ...identity, status: Schema.Literal("decided"), outcome: Schema.Literal("abandoned"), reason: Schema.Literals(["stopped_or_retired", "ineligible", "interrupted", "cancelled"]) }),
]);
export type RecoveryOutcome = typeof RecoveryOutcome.Type;
const Json = Schema.fromJsonString(RecoveryOutcome);
const encode = Schema.encodeSync(Json);
const decode = Schema.decodeUnknownSync(Json);

export const initializeRecoveryHistory = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS fork_prism_recovery_outcomes (
    source_run_id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, status TEXT NOT NULL, payload_json TEXT NOT NULL
  )`;
  yield* sql`CREATE INDEX IF NOT EXISTS fork_prism_recovery_outcomes_pending
    ON fork_prism_recovery_outcomes(status, thread_id)`;
  yield* sql`CREATE UNIQUE INDEX IF NOT EXISTS fork_prism_recovery_successor
    ON fork_prism_recovery_outcomes(json_extract(payload_json,'$.successorRunId'))
    WHERE json_extract(payload_json,'$.outcome')='retried'`;
});

/** Read through the caller's SqlClient, including its current commit transaction. Missing is undecided. */
export const readRecoveryOutcome = Effect.fn("Prism.readRecoveryOutcome")(function* (
  sourceRunId: RunId,
): Effect.fn.Return<RecoveryOutcome | null, SqlError, SqlClient.SqlClient> {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ payload_json: string }>`SELECT payload_json FROM fork_prism_recovery_outcomes
    WHERE source_run_id=${sourceRunId}`;
  return rows[0] === undefined ? null : decode(rows[0].payload_json);
});

/** Observers may conclude pending facts, never replace a conclusive decision or a historical link. */
export const writeRecoveryOutcome = Effect.fn("Prism.writeRecoveryOutcome")(function* (outcome: RecoveryOutcome) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO fork_prism_recovery_outcomes(source_run_id,thread_id,status,payload_json)
    VALUES (${outcome.sourceRunId},${outcome.threadId},${outcome.status},${encode(outcome)})
    ON CONFLICT(source_run_id) DO UPDATE SET status=excluded.status,payload_json=excluded.payload_json
    WHERE fork_prism_recovery_outcomes.status='pending'`;
});
