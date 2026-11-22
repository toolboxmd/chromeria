import type { ScheduledTask, ScheduledTaskOutcomeCheck } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  commandLabel,
  commandTasksTurnedFailing,
  forkScheduleLabel,
  forkTaskSummary,
  outcomeCheckLabel,
  scheduledTaskSections,
  uniqueMachineLabels,
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

  it("show an extended fixed_time's days from Monday, each time with the server's move, and its zone", () => {
    expect(
      forkScheduleLabel({
        type: "fixed_time",
        timeOfDay: "09:00",
        extraTimes: ["23:58", "12:00", "9:00"],
        weekdays: [0, 3, 1],
        timeZone: "Europe/Warsaw",
        chosen: [
          { requested: "09:00", offsetMinutes: 7 },
          { requested: "23:58", offsetMinutes: -5 },
          { requested: "12:00", offsetMinutes: 0 },
        ],
      }),
    ).toBe("Mon, Wed, Sun at 09:00 (+7 min), 23:58 (-5 min), 12:00 (Europe/Warsaw)");
    expect(
      forkScheduleLabel({
        type: "fixed_time",
        timeOfDay: "08:30",
        weekdays: [0, 1, 2, 3, 4, 5, 6],
        timeZone: "UTC",
      }),
    ).toBe("Daily at 08:30 (UTC)");
    expect(
      forkScheduleLabel({
        type: "fixed_time",
        timeOfDay: "09:00",
        monthDays: [31, 5],
        timeZone: "Europe/Warsaw",
      }),
    ).toBe("Monthly on day 5, the last day at 09:00 (Europe/Warsaw)");
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

describe("scheduled task sections", () => {
  const task = (id: string, fields: Partial<ScheduledTask>) =>
    ({
      id,
      title: id,
      enabled: true,
      schedule: { type: "interval", everyMs: 60_000 },
      nextRunAt: null,
      lastRunAt: null,
      lastRunStatus: "never",
      ...fields,
    }) as unknown as ScheduledTask;
  const once = (at: string) => ({ type: "once", at }) as const;
  const entry = (environmentId: string, value: ScheduledTask) => ({ environmentId, task: value });
  const keys = (entries: ReadonlyArray<{ environmentId: string; task: ScheduledTask }>) =>
    entries.map(({ environmentId, task: value }) => `${environmentId}/${value.id}`);

  it.each([
    ["a due one-shot", task("t", { schedule: once("2026-11-23T10:00:00Z") }), "upcoming"],
    [
      "a paused one-shot that has not reached its slot",
      task("t", { enabled: false, schedule: once("2026-11-23T10:00:00Z") }),
      "upcoming",
    ],
    [
      "a one-shot run by hand before its slot",
      task("t", {
        schedule: { type: "once", runAt: "2026-11-23T10:00:00+01:00" } as ScheduledTask["schedule"],
        lastRunAt: "2026-11-22T12:00:00.000Z",
        lastRunStatus: "succeeded",
      }),
      "upcoming",
    ],
    [
      "a one-shot still running its slot",
      task("t", {
        schedule: once("2026-11-20T10:00:00Z"),
        lastRunAt: "2026-11-20T10:00:00.000Z",
        lastRunStatus: "running",
      }),
      "upcoming",
    ],
    [
      "a paused one-shot whose last run, perhaps of an earlier schedule, is after its slot",
      task("t", {
        enabled: false,
        schedule: once("2026-11-20T10:00:00Z"),
        lastRunAt: "2026-11-20T10:00:01.000Z",
        lastRunStatus: "failed",
      }),
      "upcoming",
    ],
    [
      "a one-shot that ran its slot",
      task("t", {
        schedule: once("2026-11-20T10:00:00Z"),
        lastRunAt: "2026-11-20T10:00:01.000Z",
        lastRunStatus: "failed",
      }),
      "history",
    ],
    [
      "a one-shot imported from Chromeria v1 with its run, paused and without lastRunAt",
      task("t", {
        enabled: false,
        schedule: once("2026-10-01T10:00:00Z"),
        command: {
          command: "make",
          run: {
            id: "run",
            stage: "done",
            exitCode: 0,
            timedOut: false,
            endedAt: null,
            error: null,
            imported: true,
          },
          failureStreak: 0,
          lastSuccessfulRunId: null,
        },
      }),
      "history",
    ],
    ["an interval", task("t", {}), "recurring"],
    [
      "a webhook",
      task("t", { schedule: { type: "webhook" } as ScheduledTask["schedule"] }),
      "recurring",
    ],
  ] as const)("put %s in %s", (_name, value, section) => {
    const sections = scheduledTaskSections([entry("e", value)]);
    expect(keys(sections[section])).toEqual(["e/t"]);
  });

  it("order each section by what runs next and keep equal task ids from two environments apart", () => {
    const webhook = { type: "webhook" } as ScheduledTask["schedule"];
    const sections = scheduledTaskSections([
      entry("rocky", task("late", { schedule: once("2026-11-25T10:00:00Z") })),
      entry("bestia", task("late", { schedule: once("2026-11-24T10:00:00Z") })),
      entry("bestia", task("paused", { enabled: false, nextRunAt: null })),
      entry("bestia", task("hook", { schedule: webhook })),
      entry("rocky", task("blocked", { blockedByPolicy: "No webhooks", schedule: webhook })),
      entry("rocky", task("soon", { nextRunAt: "2026-11-22T13:00:00.000Z" })),
      entry("bestia", task("later", { nextRunAt: "2026-11-22T14:00:00.000Z" })),
      entry(
        "bestia",
        task("old", {
          schedule: once("2026-11-01T10:00:00Z"),
          lastRunAt: "2026-11-01T10:00:00.000Z",
          lastRunStatus: "succeeded",
        }),
      ),
      entry(
        "rocky",
        task("recent", {
          schedule: once("2026-11-21T10:00:00Z"),
          lastRunAt: "2026-11-21T10:00:00.000Z",
          lastRunStatus: "succeeded",
        }),
      ),
    ]);
    expect(keys(sections.upcoming)).toEqual(["bestia/late", "rocky/late"]);
    expect(keys(sections.recurring)).toEqual([
      "rocky/soon",
      "bestia/later",
      "bestia/hook",
      "rocky/blocked",
      "bestia/paused",
    ]);
    expect(keys(sections.history)).toEqual(["rocky/recent", "bestia/old"]);
  });

  it("count failing tasks across every section and narrow all sections to them", () => {
    const failed = { lastRunAt: "2026-11-21T10:00:00.000Z", lastRunStatus: "failed" } as const;
    const entries = [
      entry("e", task("spent", { ...failed, schedule: once("2026-11-21T10:00:00Z") })),
      entry("e", task("due", { ...failed, schedule: once("2026-11-30T10:00:00Z") })),
      entry("e", task("loop", failed)),
      entry("e", task("fine", { lastRunStatus: "succeeded" })),
    ];
    const all = scheduledTaskSections(entries);
    expect([all.failing, all.historyFailing]).toEqual([3, 1]);
    const narrowed = scheduledTaskSections(entries, { failingOnly: true });
    expect([keys(narrowed.upcoming), keys(narrowed.recurring), keys(narrowed.history)]).toEqual([
      ["e/due"],
      ["e/loop"],
      ["e/spent"],
    ]);
    expect(narrowed.failing).toBe(3);
  });
});

describe("machine labels", () => {
  it("add a shared label's detail, then the environment id where that still collides", () => {
    const labels = uniqueMachineLabels([
      { environmentId: "a", label: "Dev", detail: () => "SSH me@host:2222" },
      { environmentId: "b", label: "Dev", detail: () => "SSH me@host:2223" },
      { environmentId: "c", label: "Lab", detail: () => "https://lab" },
      { environmentId: "d", label: "Lab", detail: () => "https://lab" },
      { environmentId: "e", label: "rocky", detail: () => "https://rocky" },
    ]);
    expect([...labels.values()]).toEqual([
      "Dev · SSH me@host:2222",
      "Dev · SSH me@host:2223",
      "Lab · https://lab · c",
      "Lab · https://lab · d",
      "rocky",
    ]);
  });
});
