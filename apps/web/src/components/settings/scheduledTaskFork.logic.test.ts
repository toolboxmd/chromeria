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

/** Fork (toolboxmd/chromeria#174): the settings dialog never edits or converts a fork trigger. */
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
  it.each([
    { type: "once", at: "2026-10-09T08:00:00.000Z" },
    {
      type: "weekly",
      weekdays: [1, 4],
      times: ["09:00"],
      timeZone: "Europe/Warsaw",
      windowMinutes: 20,
      chosen: [{ requested: "09:00", offsetMinutes: -3 }],
    },
  ] as const)("saves a $type trigger back exactly as loaded, never converted", (schedule) => {
    const draft = taskToDraft(task(schedule));
    expect(draft.scheduleMode).toBe("preserved");
    expect(scheduleFromDraft(draft)).toEqual(schedule);
    // Editing another field keeps the trigger as it was.
    expect(scheduleFromDraft({ ...draft, title: "Renamed", intervalMinutes: "5" })).toEqual(
      schedule,
    );
  });

  it("edits a monthly trigger, keeping the last day and normalizing typed times", () => {
    const schedule = {
      type: "monthly",
      day: "last",
      times: ["09:00", "18:30"],
      timeZone: "Europe/Warsaw",
    } as const;
    const draft = taskToDraft(task(schedule));
    expect(draft.scheduleMode).toBe("monthly");
    expect(draft.preservedSchedule).toBeNull();
    expect(scheduleFromDraft(draft)).toEqual(schedule);
    expect(
      scheduleFromDraft({
        ...draft,
        monthly: { day: "31", times: "9:05,  18:30 9:05", timeZone: "UTC" },
      }),
    ).toEqual({ type: "monthly", day: 31, times: ["09:05", "18:30"], timeZone: "UTC" });
    for (const monthly of [
      { day: "3", times: "25:00", timeZone: "UTC" },
      { day: "3", times: "", timeZone: "UTC" },
      { day: "3", times: "09:00", timeZone: "Mars/Olympus" },
    ]) {
      const invalid = { ...draft, monthly };
      expect(scheduleFromDraft(invalid)).toBeNull();
      expect(scheduleDraftErrors(invalid)).toHaveLength(1);
    }
  });
});
