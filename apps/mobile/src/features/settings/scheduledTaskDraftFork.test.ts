import { describe, expect, it } from "vite-plus/test";

import { scheduleDraftForTask, scheduleFromDraft } from "./scheduledTaskDraft";

/** Fork: the mobile editor never edits or converts a fork schedule. */
describe("fork scheduled triggers", () => {
  it.each([
    { type: "once", at: "2026-10-09T08:00:00.000Z" },
    {
      type: "fixed_time",
      timeOfDay: "09:00",
      weekdays: [1, 4],
      timeZone: "Europe/Warsaw",
      windowMinutes: 20,
      chosen: [{ requested: "09:00", offsetMinutes: -3 }],
    },
  ] as const)("saves a fork $type schedule back exactly as loaded, never converted", (schedule) => {
    const draft = scheduleDraftForTask({ schedule });
    expect(draft.mode).toBe("preserved");
    expect(scheduleFromDraft(draft)).toEqual(schedule);
    // Fields the editor shows for upstream triggers do not touch it.
    expect(scheduleFromDraft({ ...draft, timeOfDay: "07:30", intervalMinutes: "5" })).toEqual(
      schedule,
    );
  });
});
