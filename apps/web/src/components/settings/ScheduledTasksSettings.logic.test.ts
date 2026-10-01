import { ProjectId, type ScheduledTask, type TaskRun } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  createPayloadFromDraft,
  deleteBlockedReason,
  describeChoice,
  describeSchedule,
  draftFromTask,
  editPayloadFromDraft,
  emptyTaskDraft,
  runNowBlockedReason,
  runStatusView,
  taskStatusView,
  type TaskDraft,
} from "./ScheduledTasksSettings.logic";

const draft = (patch: Partial<TaskDraft> = {}): TaskDraft => ({
  ...emptyTaskDraft("Europe/Warsaw"),
  title: "Daily report",
  prompt: "Write today's report.",
  projectId: "project-1",
  checkCommand: "test -f reports/{date}.md",
  checkReason: "The report file exists.",
  ...patch,
});

const run = (patch: Partial<TaskRun> = {}): TaskRun => ({
  definition: task().definition,
  checkCwd: "/repo",
  id: "task-1:2026-10-01T08:00:00.000Z",
  slot: "2026-10-01T08:00:00.000Z",
  checkVersion: 1,
  threadId: null,
  status: "running",
  processId: "p",
  originSequence: 0,
  sendIndex: 0,
  attempt: 0,
  hasWork: false,
  leaseUntil: 0,
  retryAt: null,
  dispatchedAt: null,
  observedTurnId: null,
  error: null,
  check: null,
  drafterIds: [],
  ...patch,
});

function task(patch: Partial<ScheduledTask> = {}): ScheduledTask {
  return {
    checkCwd: "/repo",
    id: "task-1",
    revision: 1,
    definition: {
      title: "Daily report",
      prompt: "Write today's report.",
      target: { kind: "new-thread", projectId: ProjectId.make("project-1") },
      role: "worker",
      lane: "medium",
      schedule: {
        kind: "weekly",
        weekdays: [1, 3],
        times: ["08:00"],
        timeZone: "Europe/Warsaw",
        windowMinutes: 30,
      },
    },
    createdAt: "2026-10-01T00:00:00.000Z",
    updatedAt: "2026-10-01T00:00:00.000Z",
    paused: false,
    deleted: false,
    checks: [
      {
        version: 1,
        command: "test -f reports/{date}.md",
        actor: "user:owner",
        reason: "The report file exists.",
        createdAt: "2026-10-01T00:00:00.000Z",
        revertedFrom: null,
      },
    ],
    choices: [],
    consumedSlot: null,
    runs: [],
    failureStreak: 0,
    lastError: null,
    ...patch,
  };
}

describe("createPayloadFromDraft", () => {
  it("refuses a task without an outcome check or its reason", () => {
    expect(createPayloadFromDraft(draft({ checkCommand: "  " }))).toEqual({
      ok: false,
      error: "An outcome check is required.",
    });
    expect(createPayloadFromDraft(draft({ checkReason: "" })).ok).toBe(false);
  });

  it("keeps the 30-minute default window and sends 0 for an exact time", () => {
    const fuzzy = createPayloadFromDraft(draft({ times: "8:00, 17:30, 08:00" }));
    expect(fuzzy.ok && fuzzy.value.schedule).toEqual({
      kind: "weekly",
      weekdays: [1, 2, 3, 4, 5],
      times: ["08:00", "17:30"],
      timeZone: "Europe/Warsaw",
      windowMinutes: 30,
    });
    const exact = createPayloadFromDraft(draft({ windowMinutes: "0" }));
    expect(exact.ok && exact.value.schedule).toMatchObject({ windowMinutes: 0 });
  });

  it("rejects malformed times and windows outside 0 to 720", () => {
    expect(createPayloadFromDraft(draft({ times: "25:00" })).ok).toBe(false);
    expect(createPayloadFromDraft(draft({ windowMinutes: "721" })).ok).toBe(false);
    expect(createPayloadFromDraft(draft({ windowMinutes: "-5" })).ok).toBe(false);
  });

  it("sends a lane only for the worker role", () => {
    const worker = createPayloadFromDraft(draft({ lane: "hard" }));
    expect(worker.ok && worker.value.lane).toBe("hard");
    const reviewer = createPayloadFromDraft(draft({ role: "reviewer", lane: "hard" }));
    expect(reviewer.ok && "lane" in reviewer.value).toBe(false);
  });

  it("targets an existing thread when chosen", () => {
    const result = createPayloadFromDraft(
      draft({ targetKind: "thread", threadId: "thread-9", scheduleKind: "interval" }),
    );
    expect(result.ok && result.value.target).toEqual({ kind: "thread", threadId: "thread-9" });
    expect(result.ok && result.value.schedule).toEqual({ kind: "interval", minutes: 60 });
  });
});

describe("editPayloadFromDraft", () => {
  it("sends nothing when the form is unchanged", () => {
    expect(editPayloadFromDraft(task(), draftFromTask(task()))).toEqual({ ok: true, value: null });
  });

  it("treats a missing window on a stored task the same as the default 30", () => {
    const stored = task();
    const { windowMinutes: _, ...schedule } = stored.definition.schedule as {
      windowMinutes?: number;
    } & typeof stored.definition.schedule;
    const legacy = task({ definition: { ...stored.definition, schedule } });
    expect(editPayloadFromDraft(legacy, draftFromTask(legacy))).toEqual({ ok: true, value: null });
  });

  it("sends a check change alone, without the definition, so minutes stay put", () => {
    const result = editPayloadFromDraft(task(), {
      ...draftFromTask(task()),
      checkCommand: "test -s reports/{date}.md",
      checkReason: "Empty files do not count.",
    });
    expect(result).toEqual({
      ok: true,
      value: {
        taskId: "task-1",
        checkCommand: "test -s reports/{date}.md",
        checkReason: "Empty files do not count.",
      },
    });
  });

  it("requires a reason for a check change", () => {
    const result = editPayloadFromDraft(task(), {
      ...draftFromTask(task()),
      checkCommand: "test -s reports/{date}.md",
    });
    expect(result.ok).toBe(false);
  });

  it("sends the whole definition when the schedule changes", () => {
    const result = editPayloadFromDraft(task(), { ...draftFromTask(task()), windowMinutes: "0" });
    expect(result.ok && result.value?.definition?.schedule).toMatchObject({ windowMinutes: 0 });
    expect(result.ok && result.value && "checkCommand" in result.value).toBe(false);
  });
});

describe("run and task status", () => {
  it("never calls a finished turn with a failing check done", () => {
    const failing = run({
      check: { version: 1, passed: false, output: "missing", checkedAt: "2026-10-01T08:10:00Z" },
    });
    expect(runStatusView(failing).label).toBe("Turn finished, check failing: continuing");
    expect(runStatusView(run({ status: "done" })).label).toBe("Verified done");
  });

  it("shows needs-you over pause while a run awaits the user", () => {
    expect(taskStatusView(task({ paused: true, runs: [run({ status: "needs-you" })] }))).toEqual({
      label: "Needs you",
      tone: "error",
    });
    expect(taskStatusView(task({ paused: true, runs: [run({ status: "retry" })] })).label).toBe(
      "Retrying (attempt 1)",
    );
    expect(taskStatusView(task({ paused: true })).label).toBe("Paused");
  });
});

describe("management limits", () => {
  it("blocks run now while paused or unfinished, and delete while unfinished", () => {
    expect(runNowBlockedReason(task())).toBeNull();
    expect(runNowBlockedReason(task({ paused: true }))).not.toBeNull();
    expect(runNowBlockedReason(task({ runs: [run({ status: "usage-limit" })] }))).not.toBeNull();
    expect(deleteBlockedReason(task({ runs: [run({ status: "claimed" })] }))).not.toBeNull();
    expect(deleteBlockedReason(task({ runs: [run({ status: "needs-you" })] }))).toBeNull();
  });
});

describe("schedule text", () => {
  it("states the window and the chosen minute", () => {
    expect(describeSchedule(task().definition.schedule)).toBe(
      "Mon, Wed at 08:00 (Europe/Warsaw), within ±30 min",
    );
    expect(
      describeSchedule({
        kind: "weekly",
        weekdays: [0, 1, 2, 3, 4, 5, 6],
        times: ["09:00"],
        timeZone: "UTC",
        windowMinutes: 0,
      }),
    ).toBe("Every day at 09:00 (UTC), exact minute");
    expect(describeSchedule({ kind: "interval", minutes: 1440 })).toBe("Every 1 day");
    expect(
      describeChoice({ requested: "08:00", chosen: "07:58", offsetMinutes: -2, neighbours: [] }),
    ).toBe("07:58 (asked 08:00, -2 min)");
    expect(
      describeChoice({ requested: "08:00", chosen: "08:00", offsetMinutes: 0, neighbours: [] }),
    ).toBe("08:00 (as asked)");
  });
});
