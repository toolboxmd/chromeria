import type { OrchestrationV2ServerCommand, ThreadId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { ForkCommitGuardRejected, type ForkCommitPlan } from "../childThreads/ForkCommitPlan.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import { wightIdle } from "./wightMode.ts";

const isGuardRejected = Schema.is(ForkCommitGuardRejected);

export const readWightThread = Effect.fn("Wight.readThread")(function* (threadId: ThreadId) {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const shell = yield* projections.getThreadShell(threadId);
  if (shell === null) return undefined;
  const sql = yield* SqlClient.SqlClient;
  const queued = yield* sql`
    SELECT 1 FROM orchestration_v2_projection_runs
    WHERE thread_id = ${threadId} AND status = 'queued' LIMIT 1
  `;
  return { ...shell, hasQueuedRuns: queued.length > 0 };
});

/** Stale Wight admission never creates a run, a message or a queued continuation. */
export function wightAdmissionPlan(
  command: Extract<OrchestrationV2ServerCommand, { type: "message.dispatch" }>,
): ForkCommitPlan | null {
  const expected = command.wightAdmission;
  if (expected === undefined) return null;
  const threadId = command.threadId;
  return {
    guards: [
      Effect.gen(function* () {
        const current = yield* readWightThread(threadId);
        if (
          command.creationSource !== "server" ||
          command.dispatchMode?.type !== "start_immediately" ||
          current === undefined ||
          !wightIdle(current) ||
          current.latestRunId !== expected.latestRunId ||
          DateTime.toEpochMillis(current.updatedAt) !== expected.updatedAt ||
          current.providerInstanceId !== expected.providerInstanceId
        )
          return yield* new ForkCommitGuardRejected({ threadId, kind: "state_conflict" });
      }).pipe(
        Effect.mapError((error) =>
          isGuardRejected(error)
            ? error
            : new ForkCommitGuardRejected({ threadId, kind: "storage_failure" }),
        ),
      ),
    ],
    mutations: [],
  };
}
