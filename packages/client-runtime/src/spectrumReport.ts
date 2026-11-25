import {
  EnvironmentAuthorizationError,
  type ScheduledTask,
  SPECTRUM_REPORT_NEEDS_YOU_PREFIX,
  SpectrumReportAbandonError,
  type SpectrumReportAbandonInput,
  type SpectrumReportAbandonResult,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

/**
 * Fork (toolboxmd/chromeria#176): what web and mobile need to offer
 * **Abandon report** on a scheduled run that a Spectrum report holds at
 * needs-you. The server checks the run again before abandoning anything.
 */

/** The request that abandons the report holding this task's run, or null when none holds it. */
export function abandonableReportRun(
  task: Pick<ScheduledTask, "id" | "outcomeCheck">,
): SpectrumReportAbandonInput | null {
  const run = task.outcomeCheck?.run;
  if (run === null || run === undefined || run.imported || run.stage !== "needs-you") return null;
  if (run.error?.startsWith(SPECTRUM_REPORT_NEEDS_YOU_PREFIX) !== true) return null;
  return { scheduledTaskId: task.id, schedulerRunId: run.id };
}

/** What to tell the person once the server answered. */
export const abandonReportNotice = (result: SpectrumReportAbandonResult): string =>
  result.abandonedCommandIds.length === 0
    ? "No report was waiting."
    : "Report abandoned. The run goes on to its check.";

const isAbandonError = Schema.is(SpectrumReportAbandonError);
const isAuthorizationError = Schema.is(EnvironmentAuthorizationError);

/** The server's refusal in its own words; any other failure, such as a lost connection, stays generic. */
export const abandonReportFailureMessage = (failure: unknown): string =>
  isAbandonError(failure)
    ? failure.message
    : isAuthorizationError(failure)
      ? "This session is not allowed to abandon reports."
      : "Could not abandon the report. Try again.";
