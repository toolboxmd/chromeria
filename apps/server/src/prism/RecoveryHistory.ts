import {
  OrchestrationV2AppThread,
  OrchestrationV2Run,
  OrchestrationV2TurnItem,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";

import * as DateTime from "effect/DateTime";
import { latestRootProviderFailure } from "@t3tools/shared/orchestrationV2ThreadError";
import { retirementState } from "../childThreads/retirement.ts";
import { sameModelSelection } from "./recoveryPolicy.ts";

const identity = { sourceRunId: RunId, threadId: ThreadId };
/** Automatic recovery facts; abandoned never records a human report-abandon instruction. */
export const RecoveryOutcome = Schema.Union([
  Schema.Struct({
    ...identity,
    status: Schema.Literal("pending"),
    reason: Schema.Literals(["decision", "retry", "reset"]),
  }),
  Schema.Struct({
    ...identity,
    status: Schema.Literal("decided"),
    outcome: Schema.Literal("retried"),
    successorRunId: RunId,
  }),
  Schema.Struct({
    ...identity,
    status: Schema.Literal("decided"),
    outcome: Schema.Literal("not_retryable"),
    reason: Schema.Literals([
      "non_mcp",
      "superseded",
      "non_retryable",
      "missing_failure",
      "exhausted",
      "opted_out",
      "invalid_reset",
    ]),
  }),
  Schema.Struct({
    ...identity,
    status: Schema.Literal("decided"),
    outcome: Schema.Literal("abandoned"),
    reason: Schema.Literals(["stopped_or_retired", "ineligible", "interrupted", "cancelled"]),
  }),
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
  yield* sql`CREATE UNIQUE INDEX IF NOT EXISTS fork_prism_continuation_source
    ON orchestration_v2_projection_runs(json_extract(payload_json,'$.forkPrismContinuationSourceRunId'))
    WHERE json_extract(payload_json,'$.forkPrismContinuationSourceRunId') IS NOT NULL`;
  yield* sql`CREATE UNIQUE INDEX IF NOT EXISTS fork_prism_recovery_successor
    ON fork_prism_recovery_outcomes(json_extract(payload_json,'$.successorRunId'))
    WHERE json_extract(payload_json,'$.outcome')='retried'`;
});

/** Read through the caller's SqlClient, including its current commit transaction. Missing is undecided. */
export const readRecoveryOutcome = Effect.fn("Prism.readRecoveryOutcome")(function* (
  sourceRunId: RunId,
): Effect.fn.Return<RecoveryOutcome | null, SqlError, SqlClient.SqlClient> {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{
    payload_json: string;
  }>`SELECT payload_json FROM fork_prism_recovery_outcomes
    WHERE source_run_id=${sourceRunId}`;
  return rows[0] === undefined ? null : decode(rows[0].payload_json);
});

/** Observers may conclude pending facts, never replace a conclusive decision or a historical link. */
export const writeRecoveryOutcome = Effect.fn("Prism.writeRecoveryOutcome")(function* (
  outcome: RecoveryOutcome,
) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO fork_prism_recovery_outcomes(source_run_id,thread_id,status,payload_json)
    VALUES (${outcome.sourceRunId},${outcome.threadId},${outcome.status},${encode(outcome)})
    ON CONFLICT(source_run_id) DO UPDATE SET status=excluded.status,payload_json=excluded.payload_json
    WHERE fork_prism_recovery_outcomes.status='pending'`;
});

export type RecoveryState = {
  readonly decision: RecoveryOutcome | null;
  readonly admittedContinuations: ReadonlyArray<{
    readonly sourceRunId: RunId;
    readonly successorRunId: RunId;
    readonly threadId: ThreadId;
  }>;
  readonly pendingRecovery: {
    readonly sourceRunId: RunId;
    readonly reason: "retry" | "reset";
  } | null;
};
const decodeRun = Schema.decodeUnknownSync(Schema.fromJsonString(OrchestrationV2Run));
const decodeThread = Schema.decodeUnknownSync(Schema.fromJsonString(OrchestrationV2AppThread));
const decodeItem = Schema.decodeUnknownSync(Schema.fromJsonString(OrchestrationV2TurnItem));

/** Admissions outrank historical decisions. Pending recovery is read from current persisted intent. */
export const readRecoveryState = Effect.fn("Prism.readRecoveryState")(function* (
  sourceRunId: RunId,
): Effect.fn.Return<RecoveryState, SqlError, SqlClient.SqlClient> {
  const sql = yield* SqlClient.SqlClient;
  const decision = yield* readRecoveryOutcome(sourceRunId);
  const admitted = yield* sql<{ run_id: string; thread_id: string }>`SELECT run_id,thread_id
    FROM orchestration_v2_projection_runs
    WHERE json_extract(payload_json,'$.forkPrismContinuationSourceRunId') IS NOT NULL AND json_extract(payload_json,'$.forkPrismContinuationSourceRunId')=${sourceRunId}
    ORDER BY ordinal,run_id`;
  const admittedContinuations = admitted.map((row) => ({
    sourceRunId,
    successorRunId: RunId.make(row.run_id),
    threadId: ThreadId.make(row.thread_id),
  }));
  const result: RecoveryState = { decision, admittedContinuations, pendingRecovery: null };
  if (admittedContinuations.length > 0) return result;
  const rows = yield* sql<{
    run_json: string;
    thread_json: string;
  }>`SELECT r.payload_json AS run_json,t.payload_json AS thread_json
    FROM orchestration_v2_projection_runs r JOIN orchestration_v2_projection_threads t ON t.thread_id=r.thread_id
    WHERE r.run_id=${sourceRunId} AND r.status='failed' AND r.run_id=(
      SELECT run_id FROM orchestration_v2_projection_runs WHERE thread_id=r.thread_id AND status<>'queued'
        AND NOT(status='cancelled' AND json_extract(payload_json,'$.startedAt') IS NULL)
      ORDER BY (completed_at IS NULL) DESC,completed_at DESC,ordinal DESC,run_id DESC LIMIT 1)`;
  if (rows[0] === undefined) return result;
  const run = decodeRun(rows[0].run_json);
  const thread = decodeThread(rows[0].thread_json);
  if (
    thread.archivedAt !== null ||
    thread.deletedAt !== null ||
    thread.settledOverride === "settled" ||
    !sameModelSelection(thread.modelSelection, run.modelSelection)
  )
    return result;
  const ancestors = yield* sql<{
    payload_json: string;
  }>`WITH RECURSIVE ancestry(thread_id,payload_json) AS (
    SELECT thread_id,payload_json FROM orchestration_v2_projection_threads WHERE thread_id=${thread.id}
    UNION SELECT parent.thread_id,parent.payload_json FROM ancestry child JOIN orchestration_v2_projection_threads parent
      ON parent.thread_id=json_extract(child.payload_json,'$.lineage.parentThreadId')
      WHERE json_extract(child.payload_json,'$.lineage.relationshipToParent')='subagent'
    ) SELECT payload_json FROM ancestry`;
  const retirement = retirementState(
    thread,
    new Map(
      ancestors.map((row) => {
        const ancestor = decodeThread(row.payload_json);
        return [ancestor.id, ancestor] as const;
      }),
    ),
  );
  if (retirement.retired || !retirement.complete) return result;
  const requests =
    yield* sql`SELECT 1 FROM orchestration_v2_projection_runtime_requests WHERE thread_id=${thread.id} AND status='pending' LIMIT 1`;
  if (requests.length > 0) return result;
  const items = yield* sql<{
    payload_json: string;
  }>`SELECT payload_json FROM orchestration_v2_projection_turn_items
    WHERE thread_id=${thread.id} AND run_id=${sourceRunId} AND type IN('error','run_interrupt_request')`;
  const turnItems = items.map((row) => decodeItem(row.payload_json));
  if (turnItems.some((item) => item.type === "run_interrupt_request")) return result;
  const failure = latestRootProviderFailure(run, turnItems);
  const recovery = thread.limitRecovery;
  if (
    failure?.class === "usage_limit" &&
    recovery?.runId === sourceRunId &&
    recovery.resetAt === failure.resetAt &&
    recovery.autoResume &&
    Number.isFinite(Date.parse(recovery.resetAt)) &&
    Date.parse(recovery.resetAt) > DateTime.toEpochMillis(run.completedAt ?? run.requestedAt)
  ) {
    return { ...result, pendingRecovery: { sourceRunId, reason: "reset" } };
  }
  if (failure?.class === "usage_limit" && recovery?.runId === sourceRunId && !recovery.autoResume)
    return result;
  const controllers = yield* sql<{ state: string }>`SELECT state FROM fork_prism_recovery
    WHERE source_run_id=${sourceRunId} AND state IN('retry_pending','retry_started','reset_wait')`;
  if (controllers[0] !== undefined)
    return {
      ...result,
      pendingRecovery: {
        sourceRunId,
        reason: controllers[0].state === "reset_wait" ? "reset" : "retry",
      },
    };
  return result;
});
