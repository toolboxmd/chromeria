import {
  EnvironmentAuthorizationError,
  type ScheduledTask,
  ScheduledTaskId,
  type ScheduledTaskOutcomeCheck,
  SpectrumReportAbandonError,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { abandonableReportRun, abandonReportFailureMessage } from "./spectrumReport.ts";

type Run = NonNullable<ScheduledTaskOutcomeCheck["run"]>;

const taskId = ScheduledTaskId.make("task-1");
const heldRun: Run = {
  id: "run-7",
  stage: "needs-you",
  checkVersion: 1,
  attempt: 0,
  error: "Spectrum report: Spectrum could not deliver its report after 3 attempts",
  imported: false,
  resumable: true,
};
const task = (run: Run | null): Pick<ScheduledTask, "id" | "outcomeCheck"> => ({
  id: taskId,
  outcomeCheck: { version: 1, command: "true", role: null, lane: null, run, lastVerdict: null },
});

describe("abandonableReportRun", () => {
  it("names the task and the exact run a Spectrum report holds at needs-you", () => {
    expect(abandonableReportRun(task(heldRun))).toEqual({
      scheduledTaskId: "task-1",
      schedulerRunId: "run-7",
    });
  });

  it("offers nothing when the run needs the person for another reason", () => {
    for (const error of ["Check failed after 3 attempts", null, "Needs you: Spectrum report: x"]) {
      expect(abandonableReportRun(task({ ...heldRun, error }))).toBeNull();
    }
  });

  it("offers nothing once the run left needs-you, or for v1 history", () => {
    for (const run of [
      { ...heldRun, stage: "running" as const },
      { ...heldRun, stage: "done" as const },
      { ...heldRun, imported: true },
      null,
    ]) {
      expect(abandonableReportRun(task(run))).toBeNull();
    }
  });

  it("offers nothing for tasks without an outcome check", () => {
    expect(abandonableReportRun({ id: taskId })).toBeNull();
  });
});

describe("abandonReportFailureMessage", () => {
  it("passes the server's refusal through", () => {
    const refusal = new SpectrumReportAbandonError({ message: "This run no longer needs you." });
    expect(abandonReportFailureMessage(refusal)).toBe("This run no longer needs you.");
  });

  it("says a refused session cannot retry its way in, without naming scopes", () => {
    const denied = new EnvironmentAuthorizationError({
      message: "The authenticated token is missing required scope: orchestration:operate.",
      requiredScope: "orchestration:operate",
    });
    expect(abandonReportFailureMessage(denied)).toBe(
      "This session is not allowed to abandon reports.",
    );
  });

  it("keeps transport and unknown failures generic", () => {
    for (const failure of [
      new Error("SocketCloseError: ws://127.0.0.1:3773/ws closed with 1006"),
      "boom",
    ]) {
      expect(abandonReportFailureMessage(failure)).toBe("Could not abandon the report. Try again.");
    }
  });
});
