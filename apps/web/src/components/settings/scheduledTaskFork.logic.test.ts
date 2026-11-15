import {
  ProjectId,
  ProviderInstanceId,
  ScheduledTaskId,
  type ScheduledTask,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  scheduleDraftErrors,
  scheduleFromDraft,
  taskToDraft,
} from "./scheduledTasksSettings.logic";

/** Fork: the settings dialog edits fixed_time and keeps a fork one-shot. */
const task = (schedule: ScheduledTask["schedule"]): ScheduledTask => ({
  id: ScheduledTaskId.make("fork-task"),
  title: "Review issues",
  prompt: "Review open issues",
  enabled: true,
  schedule,
  projectId: ProjectId.make("project"),
  threadId: null,
  workspaceStrategy: { type: "root" },
  modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" },
  runtimeMode: "full-access",
  interactionMode: "default",
  createdBy: "agent",
  creationSource: "mcp",
  createdAt: "2026-09-17T00:00:00.000Z",
  updatedAt: "2026-09-17T00:00:00.000Z",
  nextRunAt: null,
  lastRunAt: null,
  lastRunStatus: "never",
  lastRunError: null,
  runCount: 0,
});

describe("fork scheduled triggers", () => {
  it("saves a fork one-shot back exactly as loaded, never converted", () => {
    const schedule = { type: "once", at: "2026-10-09T08:00:00.000Z" } as const;
    const draft = taskToDraft(task(schedule));
    expect(draft.scheduleMode).toBe("preserved");
    expect(scheduleFromDraft(draft)).toEqual(schedule);
    // Editing another field keeps the trigger as it was.
    expect(scheduleFromDraft({ ...draft, title: "Renamed", intervalMinutes: "5" })).toEqual(
      schedule,
    );
  });

  it("keeps an untouched plain fixed_time plain, so it never gains a zone", () => {
    const schedule = { type: "fixed_time", timeOfDay: "09:00", weekdays: [1, 2] } as const;
    const draft = taskToDraft(task(schedule));
    expect(draft.scheduleMode).toBe("fixed");
    expect(scheduleFromDraft(draft)).toEqual(schedule);
    expect(
      scheduleFromDraft({
        ...draft,
        fixedTimeFork: { ...draft.fixedTimeFork, extraTimes: " ", windowMinutes: "0" },
      }),
    ).toEqual(schedule);
  });

  it("round-trips an extended fixed_time and sends a cleared field as [] or 0", () => {
    const schedule = {
      type: "fixed_time",
      timeOfDay: "09:00",
      extraTimes: ["18:30"],
      monthDays: [5, 31],
      windowMinutes: 20,
      timeZone: "Europe/Warsaw",
      chosen: [{ requested: "09:00", offsetMinutes: -3 }],
    } as const;
    const draft = taskToDraft(task(schedule));
    expect(draft.scheduleMode).toBe("fixed");
    expect(draft.preservedSchedule).toBeNull();
    const { chosen: _chosen, ...requested } = schedule;
    // Month days run every weekday; the server owns the picks.
    expect(scheduleFromDraft(draft)).toEqual({ ...requested, weekdays: [0, 1, 2, 3, 4, 5, 6] });
    expect(
      scheduleFromDraft({
        ...draft,
        weekdays: new Set([1, 3]),
        fixedTimeFork: {
          ...draft.fixedTimeFork,
          dayMode: "week",
          extraTimes: "9:05,  ",
          windowMinutes: "",
        },
      }),
    ).toEqual({
      type: "fixed_time",
      timeOfDay: "09:00",
      weekdays: [1, 3],
      timeZone: "Europe/Warsaw",
      extraTimes: ["09:05"],
      monthDays: [],
      windowMinutes: 0,
    });
    for (const fixedTimeFork of [
      { ...draft.fixedTimeFork, extraTimes: "25:00" },
      { ...draft.fixedTimeFork, timeZone: "Mars/Olympus" },
      { ...draft.fixedTimeFork, monthDays: new Set<number>() },
      { ...draft.fixedTimeFork, windowMinutes: "721" },
    ]) {
      const invalid = { ...draft, fixedTimeFork };
      expect(scheduleFromDraft(invalid)).toBeNull();
      expect(scheduleDraftErrors(invalid)).toHaveLength(1);
    }
  });
});
