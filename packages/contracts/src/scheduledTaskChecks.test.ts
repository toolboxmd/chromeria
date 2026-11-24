import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { OrchestratorMcpScheduleTaskInput } from "./orchestratorMcp.ts";
import { ScheduledTaskSchedule, ScheduledTaskUpsertSchedule } from "./scheduledTask.ts";
import { isForkScheduledTaskSchedule } from "./scheduledTaskChecks.ts";

const decode = Schema.decodeUnknownSync(ScheduledTaskSchedule);
const decodeUpsert = Schema.decodeUnknownSync(ScheduledTaskUpsertSchedule);

describe("fork scheduled task triggers", () => {
  it("decode a one-shot only with an offset, so the instant never depends on the server's zone", () => {
    expect(decode({ type: "once", at: "2026-10-08T12:00:00Z" })).toEqual({
      type: "once",
      at: "2026-10-08T12:00:00Z",
    });
    expect(decodeUpsert({ type: "once", at: "2026-10-08T14:00:00+02:00" }).type).toBe("once");
    for (const at of ["2026-10-08T12:00:00", "2026-10-08", "2026-13-40T12:00:00Z", "soon"])
      expect(() => decode({ type: "once", at })).toThrow();
  });

  it("decode fixed_time's fork fields, refusing month days with a weekday subset", () => {
    const extended = {
      type: "fixed_time",
      timeOfDay: "9:00",
      weekdays: [0, 1, 2, 3, 4, 5, 6],
      extraTimes: ["17:30"],
      monthDays: [1, 31],
      timeZone: "Europe/Warsaw",
      windowMinutes: 30,
      chosen: [{ requested: "09:00", offsetMinutes: -4 }],
    };
    expect(decode(extended)).toEqual(extended);
    expect(isForkScheduledTaskSchedule(decode(extended))).toBe(true);
    expect(decodeUpsert({ ...extended, weekdays: [] }).type).toBe("fixed_time");
    // Upstream's own fixed_time stays upstream's, routed to none of the fork code.
    expect(isForkScheduledTaskSchedule(decode({ type: "fixed_time", timeOfDay: "09:00" }))).toBe(
      false,
    );
    for (const broken of [
      { ...extended, weekdays: [1, 2] },
      { ...extended, monthDays: [0] },
      { ...extended, monthDays: [32] },
      { ...extended, extraTimes: ["24:00"] },
      { ...extended, timeZone: "Mars/Olympus" },
      { ...extended, windowMinutes: 721 },
      { ...extended, chosen: [{ requested: "09:00", offsetMinutes: 721 }] },
    ])
      expect(() => decode(broken)).toThrow();
  });

  it("refuse the retired weekly and monthly triggers in every input", () => {
    const decodeTool = Schema.decodeUnknownSync(OrchestratorMcpScheduleTaskInput);
    for (const schedule of [
      { type: "weekly", weekdays: [1], times: ["09:00"], timeZone: "Europe/Warsaw" },
      { type: "monthly", day: 31, times: ["09:00"], timeZone: "Europe/Warsaw" },
    ]) {
      expect(() => decodeUpsert(schedule)).toThrow();
      expect(() => decodeTool({ prompt: "Run", schedule })).toThrow();
      expect(() => decodeTool({ prompt: "Run", schedule: JSON.stringify(schedule) })).toThrow();
    }
    expect(
      decodeTool({ prompt: "Run", schedule: { type: "fixed_time", timeOfDay: "09:00" } }).schedule,
    ).toEqual({ type: "fixed_time", timeOfDay: "09:00" });
  });

  it("leave upstream's own triggers unchanged", () => {
    expect(decode({ type: "interval", everyMs: 3_600_000 })).toEqual({
      type: "interval",
      everyMs: 3_600_000,
    });
  });
});
