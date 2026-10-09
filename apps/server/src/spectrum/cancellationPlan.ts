import { OrchestrationV2Command, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { ProjectionStoreV2 } from "../orchestration-v2/ProjectionStore.ts";
import { ForkCommitGuardRejected, type ForkCommitPlan } from "../childThreads/ForkCommitPlan.ts";
import { readSpectrum } from "./store.ts";

const sameCommand = Schema.toEquivalence(OrchestrationV2Command);
/** Exact run cancellation never holds a newer run's queue. Admission is rebuilt on replay. */
export function spectrumCancellationPlan(
  threadId: ThreadId,
  generation: number,
  command: OrchestrationV2Command,
): ForkCommitPlan {
  return {
    guards: [
      Effect.gen(function* () {
        const found = yield* readSpectrum(threadId);
        if (
          Option.isNone(found) ||
          found.value.generation !== generation ||
          (command.type !== "run.interrupt" && command.type !== "queued-run.cancel") ||
          (command.type === "run.interrupt" && command.holdQueue === true) ||
          !found.value.outbox.some(
            (pending) =>
              pending.type !== "spectrum.transcript.append" && sameCommand(pending, command),
          )
        )
          return yield* new ForkCommitGuardRejected({ threadId, kind: "state_conflict" });
      }).pipe(
        Effect.mapError((error) =>
          error._tag === "ForkCommitGuardRejected"
            ? error
            : new ForkCommitGuardRejected({ threadId, kind: "storage_failure" }),
        ),
      ),
    ],
    mutations: [],
  };
}

/** Planning-time enumeration also catches launches committed after Stop's original snapshot. */
export const ownedActiveRuns = Effect.fn("Spectrum.ownedActiveRuns")(function* (
  threadId: ThreadId,
) {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql<{
    thread_id: string;
    run_id: string;
    status: string;
  }>`WITH RECURSIVE owned(thread_id) AS (
    SELECT thread_id FROM orchestration_v2_projection_threads WHERE json_extract(payload_json,'$.lineage.parentThreadId')=${threadId}
      AND json_extract(payload_json,'$.lineage.relationshipToParent')='subagent'
    UNION SELECT t.thread_id FROM orchestration_v2_projection_threads t JOIN owned p ON json_extract(t.payload_json,'$.lineage.parentThreadId')=p.thread_id
      WHERE json_extract(t.payload_json,'$.lineage.relationshipToParent')='subagent'
  ) SELECT r.thread_id,r.run_id,r.status FROM orchestration_v2_projection_runs r JOIN owned o ON r.thread_id=o.thread_id
    WHERE r.status NOT IN('completed','failed','interrupted','cancelled','rolled_back')`;
});

export const ownedThreads = Effect.fn("Spectrum.ownedThreads")(function* (threadId: ThreadId) {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ thread_id: string }>`WITH RECURSIVE owned(thread_id) AS (
    SELECT thread_id FROM orchestration_v2_projection_threads WHERE json_extract(payload_json,'$.lineage.parentThreadId')=${threadId}
      AND json_extract(payload_json,'$.lineage.relationshipToParent')='subagent'
    UNION SELECT t.thread_id FROM orchestration_v2_projection_threads t JOIN owned p ON json_extract(t.payload_json,'$.lineage.parentThreadId')=p.thread_id
      WHERE json_extract(t.payload_json,'$.lineage.relationshipToParent')='subagent'
  ) SELECT thread_id FROM owned`;
  return rows.map((row) => ThreadId.make(row.thread_id));
});

/** A reopen cannot discard the cancellation generation while owned work is still live. */
export function ownedRunsDrainedPlan(
  threadId: ThreadId,
  descendants: readonly ThreadId[],
): ForkCommitPlan {
  return {
    guards: [
      Effect.gen(function* () {
        const projections = yield* ProjectionStoreV2;
        for (const id of descendants) {
          const records = yield* projections.getThreadRecords(id, ["runs"]);
          if (
            records.runs.some(
              (run) =>
                !["completed", "failed", "interrupted", "cancelled", "rolled_back"].includes(
                  run.status,
                ),
            )
          )
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
    mutations: [],
  };
}
