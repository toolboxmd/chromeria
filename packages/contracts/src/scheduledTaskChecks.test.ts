import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { ScheduledTaskSchedule, ScheduledTaskUpsertSchedule } from "./scheduledTask.ts";

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

  it("decode weekly schedules with weekdays, wall-clock times and an IANA time zone", () => {
    const weekly = {
      type: "weekly",
      weekdays: [1, 3],
      times: ["09:00", "17:30"],
      timeZone: "Europe/Warsaw",
      windowMinutes: 30,
      chosen: [{ requested: "09:00", offsetMinutes: -4 }],
    };
    expect(decode(weekly)).toEqual(weekly);
    expect(decodeUpsert({ ...weekly, chosen: undefined }).type).toBe("weekly");
    for (const broken of [
      { ...weekly, weekdays: [] },
      { ...weekly, weekdays: [7] },
      { ...weekly, times: [] },
      { ...weekly, times: ["9:00"] },
      { ...weekly, times: ["24:00"] },
      { ...weekly, timeZone: "Mars/Olympus" },
      { ...weekly, windowMinutes: 721 },
      { ...weekly, chosen: [{ requested: "09:00", offsetMinutes: 721 }] },
    ])
      expect(() => decode(broken)).toThrow();
  });

  it("leave upstream's own triggers unchanged", () => {
    expect(decode({ type: "interval", everyMs: 3_600_000 })).toEqual({
      type: "interval",
      everyMs: 3_600_000,
    });
  });
});
