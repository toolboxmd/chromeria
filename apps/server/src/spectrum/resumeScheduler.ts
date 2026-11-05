import { SPECTRUM_REPORT_NEEDS_YOU_PREFIX } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import { reportsFence } from "../scheduledTaskChecks/handoff.ts";
import type { CheckState } from "../scheduledTaskChecks/state.ts";
import { readCheckState, writeCheckState } from "../scheduledTaskChecks/store.ts";

/** D44: resume only the exact human report hold observed before abandonment, never fire a new task. */
export const resumeAbandonedReport = Effect.fn("Spectrum.resumeAbandonedReport")(function* (
  expected: CheckState,
  schedulerRunId: string,
  taskCreatedAt: string,
) {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql
    .withTransaction(
      Effect.gen(function* () {
        const task = yield* sql<{
          created_at: string;
        }>`SELECT created_at FROM scheduled_tasks WHERE task_id=${expected.taskId}`;
        const current = yield* readCheckState(expected.taskId);
        const observed = expected.runs.find((run) => run.id === schedulerRunId);
        const run = current?.runs.find((run) => run.id === schedulerRunId);
        if (
          task[0]?.created_at !== taskCreatedAt ||
          current === null ||
          current.revision !== expected.revision ||
          run?.stage !== "needs-you" ||
          run.awaitingReports !== true ||
          run.error !== observed?.error ||
          !run.error?.startsWith(SPECTRUM_REPORT_NEEDS_YOU_PREFIX)
        )
          return false;
        const fence = yield* reportsFence(schedulerRunId);
        if (fence.kind === "needs-you") return false;
        yield* writeCheckState(current, {
          ...current,
          runs: current.runs.map((entry) =>
            entry.id === schedulerRunId ? { ...entry, stage: "running", error: null } : entry,
          ),
        });
        return true;
      }),
    )
    .pipe(Effect.catchTags({ CheckStateConflict: () => Effect.succeed(false) }));
});
