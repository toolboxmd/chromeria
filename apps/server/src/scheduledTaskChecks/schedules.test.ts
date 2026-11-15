import { assert, describe, it } from "@effect/vitest";
import {
  ScheduledTaskId,
  type ScheduledTaskForkSchedule,
  type ScheduledTaskSchedule,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

import {
  canonicalInstant,
  chooseWeeklyMinutes,
  forkFireKey,
  forkNextScheduledRunAt,
  forkSameSchedule,
  occupiedMinutes,
  weeklySlots,
} from "./schedules.ts";

type Weekly = Extract<ScheduledTaskForkSchedule, { readonly type: "weekly" }>;

const weekly = (overrides: Partial<Weekly> = {}): Weekly => ({
  type: "weekly",
  weekdays: [1],
  times: ["09:00"],
  timeZone: "UTC",
  windowMinutes: 0,
  ...overrides,
});
const ms = (iso: string) => Date.parse(iso);
const isoSlots = (schedule: Weekly, from: string, to: string) =>
  weeklySlots(schedule, ms(from), ms(to)).map((slot) =>
    DateTime.formatIso(DateTime.makeUnsafe(slot)),
  );
const minuteOf = (iso: string) => Math.floor(ms(iso) / 60_000);

describe("weekly slots", () => {
  it("keep the wall-clock time across both daylight saving changes", () => {
    const warsaw = weekly({ timeZone: "Europe/Warsaw" });
    // Spring forward on Sunday 2026-03-29: 09:00 moves from UTC+1 to UTC+2.
    assert.deepEqual(isoSlots(warsaw, "2026-03-22T00:00:00Z", "2026-04-01T00:00:00Z"), [
      "2026-03-23T08:00:00.000Z",
      "2026-03-30T07:00:00.000Z",
    ]);
    // Fall back on Sunday 2026-10-25.
    assert.deepEqual(isoSlots(warsaw, "2026-10-18T00:00:00Z", "2026-10-28T00:00:00Z"), [
      "2026-10-19T07:00:00.000Z",
      "2026-10-26T08:00:00.000Z",
    ]);
  });

  it("cover every time on every selected weekday, in order", () => {
    const schedule = weekly({
      weekdays: [4, 2],
      times: ["17:30", "08:15"],
      timeZone: "America/New_York",
    });
    assert.deepEqual(isoSlots(schedule, "2026-10-11T00:00:00Z", "2026-10-17T23:59:00Z"), [
      "2026-10-13T12:15:00.000Z",
      "2026-10-13T21:30:00.000Z",
      "2026-10-15T12:15:00.000Z",
      "2026-10-15T21:30:00.000Z",
    ]);
  });

  it("apply the server's minute picks either way, even across midnight", () => {
    const schedule = weekly({
      times: ["00:02", "12:00"],
      windowMinutes: 30,
      chosen: [
        { requested: "00:02", offsetMinutes: -5 },
        { requested: "12:00", offsetMinutes: 7 },
      ],
    });
    // Monday's 00:02 moved five minutes earlier lands on Sunday evening.
    assert.deepEqual(isoSlots(schedule, "2026-10-11T00:00:00Z", "2026-10-13T00:00:00Z"), [
      "2026-10-11T23:57:00.000Z",
      "2026-10-12T12:07:00.000Z",
    ]);
    // The next fire is strictly after the instant it is asked from.
    const next = forkNextScheduledRunAt(schedule, DateTime.makeUnsafe(ms("2026-10-12T12:07:00Z")));
    assert.equal(next === null ? null : DateTime.formatIso(next), "2026-10-18T23:57:00.000Z");
  });
});

describe("one-shot schedules", () => {
  it("fire once, at an instant that does not depend on the written offset", () => {
    const once = { type: "once", at: "2026-10-08T14:00:00+02:00" } as const;
    assert.equal(canonicalInstant(once.at), "2026-10-08T12:00:00.000Z");
    const before = forkNextScheduledRunAt(once, DateTime.makeUnsafe(ms("2026-10-08T11:59:00Z")));
    assert.equal(before === null ? null : DateTime.formatIso(before), "2026-10-08T12:00:00.000Z");
    assert.isNull(forkNextScheduledRunAt(once, DateTime.makeUnsafe(ms("2026-10-08T12:00:00Z"))));
  });
});

describe("schedule identity", () => {
  it("compares the instants a schedule fires at, never the server's picks", () => {
    const same = (a: ScheduledTaskSchedule, b: ScheduledTaskSchedule) => forkSameSchedule(a, b);
    assert.isTrue(
      same(
        { type: "once", at: "2026-10-08T12:00:00Z" },
        { type: "once", at: "2026-10-08T14:00:00+02:00" },
      ),
    );
    assert.isFalse(
      same(
        { type: "once", at: "2026-10-08T12:00:00Z" },
        { type: "once", at: "2026-10-08T12:01:00Z" },
      ),
    );
    // No window written means the default window.
    const { windowMinutes: _window, ...base } = weekly({
      weekdays: [1, 3],
      times: ["09:00", "17:00"],
    });
    assert.isTrue(
      same(base, {
        ...base,
        weekdays: [3, 1],
        times: ["17:00", "09:00"],
        windowMinutes: 30,
        chosen: [{ requested: "09:00", offsetMinutes: 4 }],
      }),
    );
    assert.isFalse(same(base, { ...base, timeZone: "Europe/Warsaw" }));
    assert.isFalse(same(base, { ...base, windowMinutes: 10 }));
    assert.isFalse(same(base, { type: "once", at: "2026-10-08T12:00:00Z" }));
  });

  it("keys a scheduled fire by its canonical slot, and leaves other fires to upstream", () => {
    const id = ScheduledTaskId.make("task:a");
    const once = (at: string) => ({ id, schedule: { type: "once", at } as const, nextRunAt: null });
    assert.equal(
      forkFireKey(once("2026-10-08T14:00:00+02:00"), "scheduled"),
      "task:a:once:2026-10-08T12:00:00.000Z",
    );
    assert.equal(
      forkFireKey(once("2026-10-08T12:00:00Z"), "scheduled"),
      forkFireKey(once("2026-10-08T14:00:00+02:00"), "scheduled"),
    );
    assert.isNull(forkFireKey(once("2026-10-08T12:00:00Z"), "manual"));
    assert.isNull(forkFireKey(once("2026-10-08T12:00:00Z"), "webhook"));
    const weeklyTask = (nextRunAt: string | null) => ({ id, schedule: weekly(), nextRunAt });
    assert.equal(
      forkFireKey(weeklyTask("2026-10-12T11:00:00+02:00"), "scheduled"),
      "task:a:weekly:2026-10-12T09:00:00.000Z",
    );
    assert.isNull(forkFireKey(weeklyTask(null), "scheduled"));
    assert.isNull(
      forkFireKey(
        { id, schedule: { type: "interval", everyMs: 3_600_000 }, nextRunAt: null },
        "scheduled",
      ),
    );
  });
});

describe("minute spreading", () => {
  // Thursday: the next nine days hold one Monday 09:00.
  const now = ms("2026-10-08T00:00:00Z");
  const monday = "2026-10-12T09:00:00Z";

  it("moves a time to the least busy minute, the smallest move first and earlier on a tie", () => {
    const schedule = weekly({ windowMinutes: 30 });
    assert.deepEqual(chooseWeeklyMinutes(schedule, new Set(), now), [
      { requested: "09:00", offsetMinutes: 0 },
    ]);
    assert.deepEqual(chooseWeeklyMinutes(schedule, new Set([minuteOf(monday)]), now), [
      { requested: "09:00", offsetMinutes: -1 },
    ]);
    const crowded = new Set(["2026-10-12T08:59:00Z", monday, "2026-10-12T09:01:00Z"].map(minuteOf));
    assert.deepEqual(chooseWeeklyMinutes(schedule, crowded, now), [
      { requested: "09:00", offsetMinutes: -2 },
    ]);
  });

  it("keeps the exact minute with no window, and spreads a task's own times apart", () => {
    const busy = new Set([minuteOf(monday)]);
    assert.deepEqual(chooseWeeklyMinutes(weekly(), busy, now), [
      { requested: "09:00", offsetMinutes: 0 },
    ]);
    assert.deepEqual(
      chooseWeeklyMinutes(weekly({ times: ["09:00", "08:59"], windowMinutes: 30 }), busy, now),
      // 09:00 takes 08:59, so 08:59 moves on to 08:58.
      [
        { requested: "09:00", offsetMinutes: -1 },
        { requested: "08:59", offsetMinutes: -1 },
      ],
    );
  });

  it("counts other tasks' minutes over the next nine days only", () => {
    assert.deepEqual(occupiedMinutes({ type: "once", at: monday }, now), [minuteOf(monday)]);
    assert.deepEqual(occupiedMinutes({ type: "once", at: "2026-10-20T09:00:00Z" }, now), []);
    assert.deepEqual(occupiedMinutes(weekly(), now), [minuteOf(monday)]);
  });
});
