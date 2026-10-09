import type { ScheduledTask, ScheduledTaskOutcomeCheck } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  commandLabel,
  commandTasksTurnedFailing,
  forkScheduleLabel,
  forkTaskSummary,
  outcomeCheckLabel,
} from "./scheduledTaskFork.ts";

describe("fork schedule labels", () => {
  it("show a one-shot's instant in the viewer's zone", () => {
    const label = forkScheduleLabel(
      { type: "once", at: "2026-10-08T12:00:00Z" },
      { locale: "en-US", timeZone: "Europe/Warsaw" },
    );
    // ICU separates the day period with a narrow no-break space.
    expect(label.replace(/\u202f/g, " ")).toBe("Once at Oct 8, 2026, 2:00 PM");
  });

  it("show weekly days from Monday, each requested time with the server's move either way", () => {
    expect(
      forkScheduleLabel({
        type: "weekly",
        weekdays: [0, 3, 1],
        times: ["09:00", "23:58", "12:00"],
        timeZone: "Europe/Warsaw",
        chosen: [
          { requested: "09:00", offsetMinutes: 7 },
          { requested: "23:58", offsetMinutes: -5 },
          { requested: "12:00", offsetMinutes: 0 },
        ],
      }),
    ).toBe("Mon, Wed, Sun at 09:00 (+7 min), 23:58 (-5 min), 12:00 (Europe/Warsaw)");
  });
});

const check = (run: ScheduledTaskOutcomeCheck["run"], passed: boolean | null = null) =>
  ({
    version: 2,
    command: "test -f done",
    role: null,
    lane: null,
    run,
    lastVerdict:
      passed === null ? null : { version: 2, passed, checkedAt: "2026-10-08T12:00:00.000Z" },
  }) satisfies ScheduledTaskOutcomeCheck;

const checkedRun = (stage: NonNullable<ScheduledTaskOutcomeCheck["run"]>["stage"]) => ({
  id: "run",
  stage,
  checkVersion: 2,
  attempt: 0,
  error: null,
  imported: false,
  resumable: stage === "needs-you",
});

describe("outcome check labels", () => {
  it("say where a checked run stands, including a run whose report needs the user", () => {
    expect(outcomeCheckLabel(check(null))).toBe("Outcome check v2 · not run yet");
    expect(outcomeCheckLabel(check({ ...checkedRun("running"), imported: true }))).toBe(
      "Outcome check v2 · imported from Chromeria v1, not resumed",
    );
    expect(outcomeCheckLabel(check(checkedRun("retry"), false))).toBe(
      "Outcome check v2 · Retrying until its check passes · last check failed",
    );
    // A passed check whose Spectrum report needs the user is not shown as done.
    expect(outcomeCheckLabel(check(checkedRun("needs-you"), true))).toBe(
      "Outcome check v2 · Needs you · last check passed",
    );
    expect(outcomeCheckLabel(check(checkedRun("done"), true))).toBe(
      "Outcome check v2 · Check passed",
    );
  });

  it("describe a command's last run, and nothing for an upstream-only task", () => {
    const command = (run: NonNullable<ScheduledTask["command"]>["run"]) => ({
      command: "make backup",
      run,
      failureStreak: 0,
      lastSuccessfulRunId: null,
    });
    const run = { id: "run", endedAt: null, error: null, imported: false, timedOut: false };
    expect(commandLabel(command(null))).toBe("Shell command: make backup · not run yet");
    expect(commandLabel(command({ ...run, stage: "needs-you", exitCode: 3 }))).toBe(
      "Shell command: make backup · exited with 3",
    );
    expect(
      commandLabel(command({ ...run, stage: "needs-you", exitCode: null, timedOut: true })),
    ).toBe("Shell command: make backup · timed out");
    expect(forkTaskSummary({})).toBeNull();
  });
});

describe("command failure alerts", () => {
  const task = (id: string, streak: number, lastSuccessfulRunId: string | null) =>
    ({
      id,
      command: { command: "make", run: null, failureStreak: streak, lastSuccessfulRunId },
    }) as unknown as ScheduledTask;

  it("record a baseline first, then alert once when a command turns from passing to failing", () => {
    const first = commandTasksTurnedFailing(null, [task("a", 2, null)]);
    expect(first.failing).toEqual([]);
    const passing = commandTasksTurnedFailing(first.health, [task("a", 0, "run:1")]);
    expect(passing.failing).toEqual([]);
    const failed = commandTasksTurnedFailing(passing.health, [task("a", 1, "run:1")]);
    expect(failed.failing.map((entry) => entry.id)).toEqual(["a"]);
    // Consecutive failures stay quiet.
    expect(commandTasksTurnedFailing(failed.health, [task("a", 2, "run:1")]).failing).toEqual([]);
  });

  it("keep one failure episode across a reconnect's compact snapshot, then alert on the next", () => {
    const failing = commandTasksTurnedFailing(null, [task("a", 0, "run:1")]).health;
    const turned = commandTasksTurnedFailing(failing, [task("a", 1, "run:1")]);
    expect(turned.failing.map((entry) => entry.id)).toEqual(["a"]);
    // The live list never carries output; the same failing state after a reconnect stays quiet.
    const reconnected = commandTasksTurnedFailing(turned.health, [task("a", 1, "run:1")]);
    expect(reconnected.failing).toEqual([]);
    const passed = commandTasksTurnedFailing(reconnected.health, [task("a", 0, "run:2")]);
    expect(passed.failing).toEqual([]);
    expect(
      commandTasksTurnedFailing(passed.health, [task("a", 1, "run:2")]).failing.map(
        (entry) => entry.id,
      ),
    ).toEqual(["a"]);
  });

  it("alert when a run passed between two reads, and on a new task's first failure", () => {
    const before = commandTasksTurnedFailing(null, [task("a", 1, "run:1")]).health;
    // The streak looks unchanged, but a newer run passed in between.
    expect(
      commandTasksTurnedFailing(before, [task("a", 1, "run:3")]).failing.map((entry) => entry.id),
    ).toEqual(["a"]);
    expect(
      commandTasksTurnedFailing(before, [task("a", 1, "run:1"), task("b", 1, null)]).failing.map(
        (entry) => entry.id,
      ),
    ).toEqual(["b"]);
  });
});
