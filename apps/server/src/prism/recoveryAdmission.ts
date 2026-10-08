import { type OrchestrationV2Command } from "@t3tools/contracts";
import {
  latestExecutedRun,
  latestRootProviderFailure,
} from "@t3tools/shared/orchestrationV2ThreadError";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import { ForkCommitGuardRejected, type ForkCommitPlan } from "../childThreads/ForkCommitPlan.ts";
import { sameModelSelection } from "./recoveryPolicy.ts";
import * as Projection from "../orchestration-v2/ProjectionStore.ts";

/** Caller composes ancestry retirement admission in the same commit plan list. */
export function retryAdmission(
  command: Extract<OrchestrationV2Command, { type: "message.dispatch" }>,
): ForkCommitPlan | null {
  if (command.forkPrismRetryOfRunId === undefined) return null;
  const reject = () =>
    new ForkCommitGuardRejected({ threadId: command.threadId, kind: "state_conflict" });
  const guards = [
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const projections = yield* Projection.ProjectionStoreV2;
      const projection = yield* projections.getThreadRecords(
        command.threadId,
        ["runs", "turnItems", "runtimeRequests"],
        {
          turnItemTypes: ["error", "run_interrupt_request"],
        },
      );
      const latest = latestExecutedRun(projection.runs);
      const failure = latestRootProviderFailure(latest, projection.turnItems);
      const row = yield* sql<{ source_run_id: string; original_run_id: string; state: string }>`
      SELECT source_run_id, original_run_id, state FROM fork_prism_recovery WHERE thread_id=${command.threadId}`;
      const record = row[0];
      if (!record || !latest) return yield* reject();
      if (
        command.creationSource !== "server" ||
        command.createdBy !== "system" ||
        command.messageId !== `prism-retry:${command.threadId}:${command.forkPrismOriginalRunId}` ||
        record.source_run_id !== command.forkPrismRetryOfRunId ||
        record.original_run_id !== command.forkPrismOriginalRunId ||
        record.state !== "retry_pending" ||
        latest.id !== command.forkPrismRetryOfRunId ||
        latest.status !== "failed" ||
        failure?.retryable !== true ||
        failure.class === "usage_limit" ||
        projection.thread.archivedAt !== null ||
        projection.thread.deletedAt !== null ||
        projection.thread.settledOverride === "settled" ||
        projection.runtimeRequests.some((request) => request.status === "pending") ||
        projection.turnItems.some(
          (item) => item.type === "run_interrupt_request" && item.runId === latest.id,
        ) ||
        command.modelSelection === undefined ||
        !sameModelSelection(latest.modelSelection, command.modelSelection) ||
        !sameModelSelection(projection.thread.modelSelection, latest.modelSelection)
      )
        return yield* reject();
    }).pipe(
      Effect.mapError((error) =>
        error._tag === "ForkCommitGuardRejected"
          ? error
          : new ForkCommitGuardRejected({ threadId: command.threadId, kind: "storage_failure" }),
      ),
    ),
  ];
  const mutations = [
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      // Payload and indexed state move together; replay never executes this mutation again.
      yield* sql`UPDATE fork_prism_recovery SET state='retry_started',
      payload_json=json_set(payload_json,'$.state','retry_started')
      WHERE thread_id=${command.threadId} AND original_run_id=${command.forkPrismOriginalRunId}
        AND source_run_id=${command.forkPrismRetryOfRunId} AND state='retry_pending'`;
    }).pipe(
      Effect.mapError(
        () => new ForkCommitGuardRejected({ threadId: command.threadId, kind: "storage_failure" }),
      ),
    ),
  ];
  return { guards, mutations };
}
