import {
  ThreadId,
  SPECTRUM_REPORT_NEEDS_YOU_PREFIX,
  AuthOrchestrationOperateScope,
  EnvironmentAuthorizationError,
  SpectrumReportAbandonError,
  type SpectrumReportAbandonInput,
  type SpectrumReportAbandonResult,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import type { AuthenticatedSession } from "../auth/EnvironmentAuth.ts";
import { SessionStore } from "../auth/SessionStore.ts";
import { sessionPerson } from "../orchestration-v2/ThreadPeople.ts";
import { readCheckState } from "../scheduledTaskChecks/store.ts";
import { SpectrumReportService } from "./ReportService.ts";
import { readSpectrum } from "./store.ts";
import { attemptOutcome } from "./reportPolicy.ts";
import { resumeAbandonedReport } from "./resumeScheduler.ts";

/** EnvironmentAuth authenticates transport sessions; MCP OAuth is a separate audience, never human approval. */
export const requireHumanReportSession = (session: AuthenticatedSession | undefined) =>
  session !== undefined &&
  session.subject !== "mcp-client" &&
  session.scopes.includes(AuthOrchestrationOperateScope)
    ? Effect.succeed(session)
    : Effect.fail(
        new EnvironmentAuthorizationError({
          requiredScope: AuthOrchestrationOperateScope,
          message:
            "Report abandonment requires an authenticated human session with permission to operate threads.",
        }),
      );

export const abandonScheduledReport = Effect.fn("Spectrum.abandonScheduledReport")(function* (
  session: AuthenticatedSession | undefined,
  input: SpectrumReportAbandonInput,
): Effect.fn.Return<
  SpectrumReportAbandonResult,
  EnvironmentAuthorizationError | SpectrumReportAbandonError,
  SqlClient.SqlClient | SessionStore | SpectrumReportService
> {
  const authenticated = yield* requireHumanReportSession(session);
  return yield* Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const sessions = yield* SessionStore;
    const reports = yield* SpectrumReportService;
    const { scheduler, taskCreatedAt } = yield* sql.withTransaction(
      Effect.gen(function* () {
        const scheduler = yield* readCheckState(input.scheduledTaskId);
        const task = yield* sql<{
          created_at: string;
        }>`SELECT created_at FROM scheduled_tasks WHERE task_id=${input.scheduledTaskId}`;
        return { scheduler, taskCreatedAt: task[0]?.created_at ?? null };
      }),
    );
    const run = scheduler?.runs.find((run) => run.id === input.schedulerRunId);
    if (run?.stage !== "needs-you" || !run.error?.startsWith(SPECTRUM_REPORT_NEEDS_YOU_PREFIX))
      return { abandonedCommandIds: [] };
    const rows = yield* sql<{
      thread_id: string;
    }>`SELECT thread_id FROM fork_spectra WHERE scheduler_run_id=${input.schedulerRunId}`;
    const person = yield* sessionPerson(sessions, authenticated.sessionId);
    const abandonedCommandIds = [];
    for (const row of rows) {
      const found = yield* readSpectrum(ThreadId.make(row.thread_id));
      if (
        Option.isNone(found) ||
        found.value.scheduledTaskId !== input.scheduledTaskId ||
        found.value.report === null
      )
        continue;
      const outcome = yield* attemptOutcome(found.value.callerThreadId, found.value.report);
      if (outcome.kind === "delivered") continue;
      const state = yield* reports.abandon(
        found.value.threadId,
        found.value.report.commandId,
        person,
      );
      if (state.reportAbandonment?.commandId === found.value.report.commandId)
        abandonedCommandIds.push(found.value.report.commandId);
    }
    if (abandonedCommandIds.length > 0 && scheduler !== null && taskCreatedAt !== null)
      yield* resumeAbandonedReport(scheduler, input.schedulerRunId, taskCreatedAt);
    return { abandonedCommandIds };
  }).pipe(
    Effect.mapError(
      () =>
        new SpectrumReportAbandonError({
          message: "The report could not be abandoned. Refresh and try again.",
        }),
    ),
  );
});
