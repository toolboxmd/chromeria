import {
  isCommandTask,
  ProjectId,
  type CreateScheduledTask,
  type ScheduledTask,
  type TaskRun,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  commandRunView,
  createPayloadFromDraft,
  deleteBlockedReason,
  describeChoice,
  describeSchedule,
  draftFromTask,
  editPayloadFromDraft,
  emptyTaskDraft,
  needsYou,
  runNowBlockedReason,
  runStatusView,
  taskCreator,
  taskStatusView,
  type TaskDraft,
} from "./ScheduledTasksSettings.logic";
import { getSettingsSearchTargetScope, isSettingsSearchScopeAvailable } from "./settingsSearch";

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

/** The agent create payload, failing the test when the draft produced a command task. */
function agentPayload(result: ReturnType<typeof createPayloadFromDraft>) {
  if (!result.ok || isCommandTask(result.value)) throw new Error("Expected an agent payload.");
  return result.value as Exclude<CreateScheduledTask, { kind: "command" }>;
}

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
    expect(agentPayload(createPayloadFromDraft(draft({ lane: "hard" }))).lane).toBe("hard");
    const reviewer = createPayloadFromDraft(draft({ role: "reviewer", lane: "hard" }));
    expect(reviewer.ok && "lane" in reviewer.value).toBe(false);
  });

  it("targets an existing thread when chosen", () => {
    const result = createPayloadFromDraft(
      draft({ targetKind: "thread", threadId: "thread-9", scheduleKind: "interval" }),
    );
    expect(agentPayload(result).target).toEqual({ kind: "thread", threadId: "thread-9" });
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
  it("allows both actions once every run is verified done", () => {
    const settled = task({ runs: [run({ status: "done" })] });
    expect(runNowBlockedReason(settled)).toBeNull();
    expect(deleteBlockedReason(settled)).toBeNull();
    expect(needsYou(settled)).toBe(false);
  });

  it.each(["claimed", "running", "retry", "usage-limit"] as const)(
    "blocks run now and delete while a run is %s",
    (status) => {
      const busy = task({ runs: [run({ status: "done" }), run({ id: "task-1:b", status })] });
      expect(runNowBlockedReason(busy)).not.toBeNull();
      expect(deleteBlockedReason(busy)).not.toBeNull();
    },
  );

  it("leaves run now (resume) and delete to the server when the task needs you", () => {
    const waiting = task({ runs: [run({ status: "needs-you" })] });
    expect(needsYou(waiting)).toBe(true);
    expect(runNowBlockedReason(waiting)).toBeNull();
    expect(deleteBlockedReason(waiting)).toBeNull();
  });

  it("requires resuming a paused task before run now, but not before delete", () => {
    const paused = task({ paused: true, runs: [run({ status: "needs-you" })] });
    expect(runNowBlockedReason(paused)).toBe("Resume this task before running it now.");
    expect(deleteBlockedReason(paused)).toBeNull();
  });
});

describe("settings scope", () => {
  it("opens from search only for one selected environment", () => {
    const target = getSettingsSearchTargetScope("scheduled-tasks");
    expect(target?.scope).toBe("environment");
    expect(isSettingsSearchScopeAvailable(target!.scope, "environment")).toBe(true);
    expect(isSettingsSearchScopeAvailable(target!.scope, "all")).toBe(false);
    expect(isSettingsSearchScopeAvailable(target!.scope, "project")).toBe(false);
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

const commandTask = (patch: Partial<ScheduledTask> = {}): ScheduledTask =>
  task({
    definition: {
      kind: "command",
      title: "Backup",
      projectId: ProjectId.make("project-1"),
      command: "./backup.sh",
      schedule: { kind: "interval", minutes: 5 },
    },
    checks: [],
    createdBy: "user:owner",
    ...patch,
  });

const commandRun = (patch: Partial<TaskRun> = {}): TaskRun => {
  const { checkVersion: _, ...agentRun } = run();
  return {
    ...agentRun,
    definition: commandTask().definition,
    dispatchedAt: "2026-10-01T08:00:00.000Z",
    ...patch,
  };
};

describe("command tasks", () => {
  const commandDraft = (patch: Partial<TaskDraft> = {}) =>
    draft({
      kind: "command",
      command: " ./backup.sh ",
      checkCommand: "",
      checkReason: "",
      ...patch,
    });

  it("creates a command task without an outcome check, prompt or role", () => {
    expect(createPayloadFromDraft(commandDraft({ scheduleKind: "interval" }))).toEqual({
      ok: true,
      value: {
        kind: "command",
        title: "Daily report",
        projectId: "project-1",
        command: "./backup.sh",
        schedule: { kind: "interval", minutes: 60 },
      },
    });
    expect(createPayloadFromDraft(commandDraft({ command: " " })).ok).toBe(false);
    expect(createPayloadFromDraft(commandDraft({ projectId: "" })).ok).toBe(false);
  });

  it("edits a command by replacing its definition and never sends check fields", () => {
    const stored = commandTask();
    expect(editPayloadFromDraft(stored, draftFromTask(stored))).toEqual({ ok: true, value: null });
    const result = editPayloadFromDraft(stored, {
      ...draftFromTask(stored),
      command: "./backup.sh --full",
      checkCommand: "true",
      checkReason: "ignored",
    });
    expect(result).toEqual({
      ok: true,
      value: {
        taskId: "task-1",
        definition: { ...stored.definition, command: "./backup.sh --full" },
      },
    });
  });

  it("keeps showing a failure until a run passes, even while the next run is in progress", () => {
    const failed = commandRun({
      status: "needs-you",
      commandResult: { exitCode: 3, timedOut: false, endedAt: "2026-10-01T08:01:00.000Z" },
    });
    expect(taskStatusView(commandTask({ failureStreak: 1, runs: [failed] }))).toEqual({
      label: "Needs you",
      tone: "error",
    });
    expect(
      taskStatusView(
        commandTask({ failureStreak: 1, runs: [failed, commandRun({ id: "task-1:b" })] }),
      ).label,
    ).toBe("Running, last run failed");
    expect(
      taskStatusView(commandTask({ paused: true, failureStreak: 2, runs: [failed] })).label,
    ).toBe("Needs you");
    const passed = commandRun({ status: "done" });
    expect(taskStatusView(commandTask({ runs: [failed, passed] })).label).toBe("Passed");
    expect(taskStatusView(commandTask({ paused: true, runs: [passed] })).label).toBe("Paused");
  });

  it("starts a new run after a failure and refuses run now and delete only while running", () => {
    const failed = commandTask({ failureStreak: 1, runs: [commandRun({ status: "needs-you" })] });
    expect(needsYou(failed)).toBe(false);
    expect(runNowBlockedReason(failed)).toBeNull();
    expect(deleteBlockedReason(failed)).toBeNull();
    const running = commandTask({ runs: [commandRun({ status: "running" })] });
    expect(runNowBlockedReason(running)).toBe("A run is still in progress.");
    expect(deleteBlockedReason(running)).toBe("The command is still running. Wait until it ends.");
  });

  it("reports an interrupted run as unknown rather than finished", () => {
    const view = commandRunView(commandRun({ status: "needs-you", error: "restarted" }));
    expect(view.exit).toBe("Unknown: the run was interrupted and was not run again");
    expect(view.ended).toBe("Unknown");
    expect(view.output).toBeNull();
  });

  it("shows the output tail only where the server kept it", () => {
    const endedAt = "2026-10-01T08:01:00.000Z";
    const kept = commandRunView(
      commandRun({
        status: "done",
        commandResult: { exitCode: 0, output: "ok\n", timedOut: false, endedAt },
      }),
    );
    expect(kept).toMatchObject({ exit: "Exit 0", output: "ok\n", outputNote: null });
    const dropped = commandRunView(
      commandRun({ status: "needs-you", commandResult: { exitCode: 2, timedOut: false, endedAt } }),
    );
    expect(dropped).toMatchObject({ exit: "Exit 2", output: null });
    expect(dropped.outputNote).toContain("newest 3 runs");
    const timedOut = commandRunView(
      commandRun({
        status: "needs-you",
        commandResult: { exitCode: null, output: "", timedOut: true, endedAt },
      }),
    );
    expect(timedOut.exit).toBe("Timed out after 30 minutes");
  });

  it("names the recorded creator and admits when nobody was recorded", () => {
    expect(taskCreator(commandTask({ createdBy: "thread-7" }))).toBe("thread-7");
    expect(taskCreator(task())).toBe("user:owner");
    const { createdBy: _, ...legacy } = commandTask();
    expect(taskCreator(legacy)).toBeNull();
  });
});
