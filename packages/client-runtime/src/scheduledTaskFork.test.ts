import {
  ThreadId,
  type ScheduledTask,
  type ScheduledTaskOutcomeCheck,
  type ScheduledTaskRunRecord,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  commandLabel,
  commandTasksTurnedFailing,
  forkScheduleLabel,
  forkTaskSummary,
  outcomeCheckLabel,
  scheduledTaskFrequency,
  scheduledTaskFrequencyGroups,
  scheduledTaskRunThreadsChanged,
  scheduledTaskRunViews,
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

const NOW = Date.parse("2026-11-22T12:00:00.000Z");

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
      "a paused one-shot that failed its slot",
      task("t", {
        enabled: false,
        schedule: once("2026-11-20T10:00:00Z"),
        lastRunAt: "2026-11-20T10:00:01.000Z",
        lastRunStatus: "failed",
      }),
      "history",
    ],
    [
      "a paused one-shot that ran its slot",
      task("t", {
        enabled: false,
        schedule: once("2026-11-20T10:00:00Z"),
        lastRunAt: "2026-11-20T10:00:00.000Z",
        lastRunStatus: "succeeded",
      }),
      "history",
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
    [
      "the same imported one-shot scheduled again",
      task("t", {
        schedule: once("2026-12-01T10:00:00Z"),
        nextRunAt: "2026-12-01T10:00:00.000Z",
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
      "upcoming",
    ],
    [
      "the same imported one-shot rescheduled for later and paused",
      task("t", {
        schedule: once("2026-12-01T10:00:00Z"),
        enabled: false,
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
      "upcoming",
    ],
    ["an interval", task("t", {}), "recurring"],
    [
      "a webhook",
      task("t", { schedule: { type: "webhook" } as ScheduledTask["schedule"] }),
      "recurring",
    ],
  ] as const)("put %s in %s", (_name, value, section) => {
    const sections = scheduledTaskSections([entry("e", value)], { now: NOW });
    expect(keys(sections[section])).toEqual(["e/t"]);
  });

  it("order each section by what runs next and keep equal task ids from two environments apart", () => {
    const webhook = { type: "webhook" } as ScheduledTask["schedule"];
    const sections = scheduledTaskSections(
      [
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
      ],
      { now: NOW },
    );
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
    const all = scheduledTaskSections(entries, { now: NOW });
    expect([all.failing, all.historyFailing]).toEqual([3, 1]);
    const narrowed = scheduledTaskSections(entries, { now: NOW, failingOnly: true });
    expect([keys(narrowed.upcoming), keys(narrowed.recurring), keys(narrowed.history)]).toEqual([
      ["e/due"],
      ["e/loop"],
      ["e/spent"],
    ]);
    expect(narrowed.failing).toBe(3);
  });
});

describe("scheduled task frequency", () => {
  const MINUTE = 60_000;
  const HOUR = 60 * MINUTE;
  const DAY = 24 * HOUR;
  const fixed = (fields: object) =>
    ({ type: "fixed_time", timeOfDay: "09:00", ...fields }) as ScheduledTask["schedule"];

  it.each([
    ["every 5 minutes", { type: "interval", everyMs: 5 * MINUTE }, "minutes"],
    ["every 59 minutes", { type: "interval", everyMs: 59 * MINUTE }, "minutes"],
    ["every hour", { type: "interval", everyMs: HOUR }, "hourly"],
    ["every 23 hours", { type: "interval", everyMs: 23 * HOUR }, "hourly"],
    ["every day", { type: "interval", everyMs: DAY }, "daily"],
    ["every 36 hours", { type: "interval", everyMs: 36 * HOUR }, "weekly"],
    ["every 3 days", { type: "interval", everyMs: 3 * DAY }, "weekly"],
    ["every week", { type: "interval", everyMs: 7 * DAY }, "weekly"],
    ["every 8 days", { type: "interval", everyMs: 8 * DAY }, "monthly"],
    ["daily at one time", fixed({}), "daily"],
    ["all seven weekdays", fixed({ weekdays: [0, 1, 2, 3, 4, 5, 6] }), "daily"],
    ["chosen weekdays", fixed({ weekdays: [1, 3] }), "weekly"],
    ["several times a day", fixed({ extraTimes: ["13:00", "17:00"] }), "hourly"],
    ["days of the month", fixed({ monthDays: [1, 15] }), "monthly"],
    [
      "several times on days of the month",
      fixed({ monthDays: [1], extraTimes: ["17:00"] }),
      "monthly",
    ],
    ["several times on chosen weekdays", fixed({ weekdays: [1], extraTimes: ["17:00"] }), "weekly"],
    ["once", { type: "once", runAt: "2026-12-01T10:00:00Z" }, "once"],
    ["a webhook", { type: "webhook" }, "webhook"],
  ] as const)("files %s as %s", (_name, schedule, frequency) => {
    expect(scheduledTaskFrequency(schedule as ScheduledTask["schedule"])).toBe(frequency);
  });

  it("groups active tasks most frequent first by next run and keeps paused ones apart", () => {
    const task = (id: string, fields: Partial<ScheduledTask>) =>
      ({
        id,
        title: id,
        enabled: true,
        nextRunAt: null,
        lastRunAt: null,
        lastRunStatus: "never",
        ...fields,
      }) as ScheduledTask;
    const entry = (value: ScheduledTask) => ({ environmentId: "e", task: value });
    const every = (everyMs: number) => ({ type: "interval", everyMs }) as const;
    const sections = scheduledTaskSections(
      [
        entry(task("nightly", { schedule: fixed({}), nextRunAt: "2026-11-23T09:00:00.000Z" })),
        entry(
          task("poll-late", {
            schedule: every(10 * MINUTE),
            nextRunAt: "2026-11-22T12:10:00.000Z",
          }),
        ),
        entry(
          task("poll-soon", { schedule: every(5 * MINUTE), nextRunAt: "2026-11-22T12:05:00.000Z" }),
        ),
        entry(task("hook", { schedule: { type: "webhook" } as ScheduledTask["schedule"] })),
        entry(
          task("trip", {
            schedule: { type: "once", runAt: "2026-12-01T10:00:00Z" } as ScheduledTask["schedule"],
          }),
        ),
        entry(task("off", { schedule: every(5 * MINUTE), enabled: false })),
        entry(
          task("off-once", {
            schedule: { type: "once", runAt: "2026-12-02T10:00:00Z" } as ScheduledTask["schedule"],
            enabled: false,
          }),
        ),
      ],
      { now: NOW },
    );
    const { groups, paused } = scheduledTaskFrequencyGroups(sections);
    expect(groups.map((group) => [group.frequency, group.entries.map((e) => e.task.id)])).toEqual([
      ["minutes", ["poll-soon", "poll-late"]],
      ["daily", ["nightly"]],
      ["once", ["trip"]],
      ["webhook", ["hook"]],
    ]);
    expect(paused.map((e) => e.task.id)).toEqual(["off-once", "off"]);
    // The groups sort on their own, whatever order the sections hand over.
    const hourly = (id: string, nextRunAt: string) =>
      entry(task(id, { schedule: every(2 * HOUR), nextRunAt }));
    const unsorted = scheduledTaskFrequencyGroups({
      upcoming: [],
      recurring: [
        hourly("late", "2026-11-22T15:00:00.000Z"),
        hourly("soon", "2026-11-22T13:00:00.000Z"),
      ],
    });
    expect(unsorted.groups[0]?.entries.map((e) => e.task.id)).toEqual(["soon", "late"]);
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

describe("scheduled task run views", () => {
  const run = (
    id: string,
    status: ScheduledTaskRunRecord["status"],
    thread: readonly [string, "open" | "settled" | "missing"] | null,
  ): ScheduledTaskRunRecord => ({
    id,
    startedAt: "2026-11-22T08:00:00.000Z",
    endedAt: status === "running" ? null : "2026-11-22T08:02:30.000Z",
    status,
    thread: thread === null ? null : { id: ThreadId.make(thread[0]), state: thread[1] },
    summary: null,
  });

  it("mark each run and count the threads that still need the user", () => {
    const views = scheduledTaskRunViews(
      [
        run("running", "running", ["shared", "open"]),
        run("newest-in-shared", "succeeded", ["shared", "open"]),
        run("older-in-shared", "succeeded", ["shared", "open"]),
        run("failed-open", "failed", ["failed", "open"]),
        run("reopened", "succeeded", ["reopened", "settled"]),
        run("settled-here", "succeeded", ["settled-here", "open"]),
        run("deleted", "succeeded", ["deleted", "missing"]),
        run("command", "succeeded", null),
      ],
      // One letter per distinct thread, in run order: the client's own thread wins over what
      // the server read, and "?" is a thread the client does not hold.
      "?bos?",
    );
    expect(
      views.runs.map((view) => [view.run.id, view.mark, view.thread?.needsUser ?? null]),
    ).toEqual([
      // A thread counts once, at its newest run, and never while that run is running.
      ["running", "running", false],
      ["newest-in-shared", "succeeded", false],
      ["older-in-shared", "succeeded", false],
      ["failed-open", "failed", true],
      ["reopened", "open", true],
      ["settled-here", "succeeded", false],
      ["deleted", "succeeded", null],
      ["command", "succeeded", null],
    ]);
    expect(views.open).toBe(2);
    // Only a thread changing state says the runs are stale, never a new set of threads.
    expect(scheduledTaskRunThreadsChanged("bo", "oo")).toBe(true);
    expect(scheduledTaskRunThreadsChanged("oo", "oo")).toBe(false);
    expect(scheduledTaskRunThreadsChanged("", "bo")).toBe(false);
    expect(views.runs[0]!.durationMs).toBeNull();
    expect(views.runs[1]!.durationMs).toBe(150_000);
  });
});
