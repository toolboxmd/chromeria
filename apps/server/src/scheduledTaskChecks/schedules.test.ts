import { assert, describe, it } from "@effect/vitest";
import {
  ScheduledTaskId,
  type ScheduledTaskFixedTimeForkSchedule,
  type ScheduledTaskForkSchedule,
  type ScheduledTaskSchedule,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

import { isMissedFixedTimeRun } from "../scheduledTasks/Schedule.ts";
import { fixedTimeFromLegacy } from "./fixedTimeMigration.ts";
import {
  canonicalInstant,
  chooseMinutes,
  effectiveFixedTime,
  fixedTimeSlots,
  forkFireKey,
  forkNextScheduledRunAt,
  forkSameSchedule,
  forkScheduledSlot,
  occupiedMinutes,
} from "./schedules.ts";

type FixedTime = ScheduledTaskFixedTimeForkSchedule;
type LegacyWeekly = {
  readonly type: "weekly";
  readonly weekdays: ReadonlyArray<number>;
  readonly times: ReadonlyArray<string>;
  readonly timeZone: string;
  readonly windowMinutes?: number;
  readonly chosen?: ReadonlyArray<{ readonly requested: string; readonly offsetMinutes: number }>;
};
type LegacyMonthly = {
  readonly type: "monthly";
  readonly day: number | "last";
  readonly times: ReadonlyArray<string>;
  readonly timeZone: string;
};

/** A retired trigger as the migration stores it; every literal below checks the migrated shape. */
const migrated = (legacy: LegacyWeekly | LegacyMonthly): FixedTime => {
  const schedule = fixedTimeFromLegacy(legacy);
  assert.isNotNull(schedule);
  return schedule as FixedTime;
};
const legacyWeekly = (overrides: Partial<LegacyWeekly> = {}): LegacyWeekly => ({
  type: "weekly",
  weekdays: [1],
  times: ["09:00"],
  timeZone: "UTC",
  windowMinutes: 0,
  ...overrides,
});
const weekly = (overrides: Partial<LegacyWeekly> = {}) => migrated(legacyWeekly(overrides));
const ms = (iso: string) => Date.parse(iso);
const isoSlots = (schedule: FixedTime, from: string, to: string) =>
  fixedTimeSlots(schedule, ms(from), ms(to)).map((slot) =>
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

const legacyMonthly = (overrides: Partial<LegacyMonthly> = {}): LegacyMonthly => ({
  type: "monthly",
  day: 31,
  times: ["09:00"],
  timeZone: "UTC",
  ...overrides,
});
const monthly = (overrides: Partial<LegacyMonthly> = {}) => migrated(legacyMonthly(overrides));
/** The next `count` fires after `from`, each asked from the one before. */
const nextRuns = (schedule: ScheduledTaskForkSchedule, from: string, count: number) => {
  const runs: string[] = [];
  let at: DateTime.DateTime = DateTime.makeUnsafe(ms(from));
  for (let step = 0; step < count; step += 1) {
    const next = forkNextScheduledRunAt(schedule, at);
    if (next === null) break;
    runs.push(DateTime.formatIso(next));
    at = next;
  }
  return runs;
};

describe("monthly schedules", () => {
  it("run a missing day on the month's last day, never skipping a month or entering the next", () => {
    // December to January, a leap February, then April.
    assert.deepEqual(nextRuns(monthly(), "2027-12-01T00:00:00Z", 5), [
      "2027-12-31T09:00:00.000Z",
      "2028-01-31T09:00:00.000Z",
      "2028-02-29T09:00:00.000Z",
      "2028-03-31T09:00:00.000Z",
      "2028-04-30T09:00:00.000Z",
    ]);
    // A non-leap February.
    assert.deepEqual(nextRuns(monthly({ day: 30 }), "2027-01-30T09:00:00Z", 2), [
      "2027-02-28T09:00:00.000Z",
      "2027-03-30T09:00:00.000Z",
    ]);
  });

  it("run 'last' on the last day of every month", () => {
    assert.deepEqual(nextRuns(monthly({ day: "last" }), "2028-01-15T00:00:00Z", 4), [
      "2028-01-31T09:00:00.000Z",
      "2028-02-29T09:00:00.000Z",
      "2028-03-31T09:00:00.000Z",
      "2028-04-30T09:00:00.000Z",
    ]);
    assert.deepEqual(nextRuns(monthly({ day: "last" }), "2027-02-01T00:00:00Z", 1), [
      "2027-02-28T09:00:00.000Z",
    ]);
  });

  it("run every time of the day, in order, strictly after the instant asked from", () => {
    const schedule = monthly({ day: 3, times: ["18:00", "09:00"] });
    assert.deepEqual(nextRuns(schedule, "2026-11-03T09:00:00Z", 3), [
      "2026-11-03T18:00:00.000Z",
      "2026-12-03T09:00:00.000Z",
      "2026-12-03T18:00:00.000Z",
    ]);
  });

  it("read times in the time zone and treat daylight saving like the weekly schedule", () => {
    const warsaw = monthly({ day: 29, timeZone: "Europe/Warsaw" });
    // 2026 has no February 29; March 29 is the spring-forward Sunday.
    assert.deepEqual(nextRuns(warsaw, "2026-02-01T00:00:00Z", 2), [
      "2026-02-28T08:00:00.000Z",
      "2026-03-29T07:00:00.000Z",
    ]);
    // 02:30 does not exist on 2026-03-29 and happens twice on 2026-10-25, both Sundays.
    const times = ["02:30"];
    for (const [day, from, to] of [
      [29, "2026-03-28T00:00:00Z", "2026-03-30T00:00:00Z"],
      [25, "2026-10-24T00:00:00Z", "2026-10-26T00:00:00Z"],
    ] as const) {
      const slots = fixedTimeSlots(
        monthly({ day, times, timeZone: "Europe/Warsaw" }),
        ms(from),
        ms(to),
      );
      assert.lengthOf(slots, 1);
      assert.deepEqual(
        slots,
        fixedTimeSlots(
          weekly({ weekdays: [0], times, timeZone: "Europe/Warsaw" }),
          ms(from),
          ms(to),
        ),
      );
    }
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
    const base: FixedTime = {
      type: "fixed_time",
      timeOfDay: "09:00",
      extraTimes: ["17:00"],
      weekdays: [1, 3],
      timeZone: "UTC",
    };
    // Times compare by minute and in any order; no window is a window of 0.
    assert.isTrue(
      same(base, {
        ...base,
        timeOfDay: "17:00",
        extraTimes: ["9:00"],
        weekdays: [3, 1],
        windowMinutes: 0,
        chosen: [{ requested: "09:00", offsetMinutes: 4 }],
      }),
    );
    const daily: FixedTime = { type: "fixed_time", timeOfDay: "09:00", timeZone: "UTC" };
    assert.isTrue(same(daily, { ...daily, weekdays: [0, 1, 2, 3, 4, 5, 6] }));
    assert.isTrue(same(daily, { ...daily, weekdays: [] }));
    assert.isFalse(same(base, { ...base, timeZone: "Europe/Warsaw" }));
    assert.isFalse(same(base, { ...base, windowMinutes: 10 }));
    assert.isFalse(same(base, { ...base, extraTimes: [] }));
    assert.isFalse(same(base, { type: "once", at: "2026-10-08T12:00:00Z" }));
    // A plain upstream task never equals an extended one, even at the same minute.
    assert.isFalse(same(daily, { type: "fixed_time", timeOfDay: "09:00" }));
    const month = monthly({ day: 3, times: ["09:00", "17:00"] });
    assert.isTrue(same(month, { ...month, timeOfDay: "17:00", extraTimes: ["09:00"] }));
    assert.isTrue(same(monthly({ day: "last" }), monthly({ day: 31 })));
    assert.isFalse(same(month, { ...month, monthDays: [3, 4] }));
    assert.isFalse(same(month, { ...month, timeZone: "Europe/Warsaw" }));
  });

  it("keys a scheduled fire by its canonical slot, with the keys weekly and monthly used", () => {
    const id = ScheduledTaskId.make("task:a");
    const at = (iso: string) => DateTime.makeUnsafe(ms(iso));
    const once = (value: string) => ({
      id,
      schedule: { type: "once", at: value } as const,
      nextRunAt: null,
    });
    const started = at("2026-10-08T12:00:00Z");
    assert.equal(
      forkFireKey(once("2026-10-08T14:00:00+02:00"), "scheduled", started),
      "task:a:once:2026-10-08T12:00:00.000Z",
    );
    assert.equal(
      forkFireKey(once("2026-10-08T12:00:00Z"), "scheduled", started),
      forkFireKey(once("2026-10-08T14:00:00+02:00"), "scheduled", started),
    );
    assert.isNull(forkFireKey(once("2026-10-08T12:00:00Z"), "manual", started));
    assert.isNull(forkFireKey(once("2026-10-08T12:00:00Z"), "webhook", started));
    const weeklyTask = (nextRunAt: string | null) => ({ id, schedule: weekly(), nextRunAt });
    // On time, the key is the one the weekly trigger gave this slot before migration.
    assert.equal(
      forkFireKey(
        weeklyTask("2026-10-12T11:00:00+02:00"),
        "scheduled",
        at("2026-10-12T09:00:00.5Z"),
      ),
      "task:a:weekly:2026-10-12T09:00:00.000Z",
    );
    assert.isNull(forkFireKey(weeklyTask(null), "scheduled", started));
    assert.equal(
      forkFireKey(
        { id, schedule: monthly({ day: 3 }), nextRunAt: "2026-12-03T10:00:00+01:00" },
        "scheduled",
        at("2026-12-03T09:00:00Z"),
      ),
      "task:a:monthly:2026-12-03T09:00:00.000Z",
    );
    for (const schedule of [
      { type: "interval", everyMs: 3_600_000 },
      { type: "fixed_time", timeOfDay: "09:00" },
    ] as const)
      assert.isNull(
        forkFireKey({ id, schedule, nextRunAt: "2026-10-12T09:00:00.000Z" }, "scheduled", started),
      );
  });
});

describe("catch-up after downtime (D3)", () => {
  const id = ScheduledTaskId.make("task:late");
  const at = (iso: string) => DateTime.makeUnsafe(ms(iso));
  const hourly: FixedTime = {
    type: "fixed_time",
    timeOfDay: "09:00",
    extraTimes: ["10:00", "11:00", "12:00"],
    timeZone: "UTC",
  };

  it("runs once for the latest missed slot, the same slot for every path and every restart", () => {
    const task = { id, schedule: hourly, nextRunAt: "2026-10-12T09:00:00.000Z" };
    // Down from before 09:00 until 11:40: 09, 10 and 11 were missed; only 11 runs.
    assert.equal(
      forkScheduledSlot(task, "scheduled", at("2026-10-12T11:40:00Z")),
      "2026-10-12T11:00:00.000Z",
    );
    assert.equal(
      forkFireKey(task, "scheduled", at("2026-10-12T11:40:00Z")),
      "task:late:weekly:2026-10-12T11:00:00.000Z",
    );
    // A restart before 12:00 admits the same slot again, so its fire and checked run dedupe.
    assert.equal(
      forkFireKey(task, "scheduled", at("2026-10-12T11:59:00Z")),
      forkFireKey(task, "scheduled", at("2026-10-12T11:40:00Z")),
    );
    // After the run, the next run is after the admission time, so nothing replays.
    const next = forkNextScheduledRunAt(hourly, at("2026-10-12T11:40:00Z"));
    assert.equal(next === null ? null : DateTime.formatIso(next), "2026-10-12T12:00:00.000Z");
    // A manual run keeps its own identity.
    assert.isNull(forkScheduledSlot(task, "manual", at("2026-10-12T11:40:00Z")));
  });

  it("never picks a slot before the pending one, and catches a monthly run up days late", () => {
    const task = { id, schedule: hourly, nextRunAt: "2026-10-12T11:00:00.000Z" };
    assert.equal(
      forkScheduledSlot(task, "scheduled", at("2026-10-12T11:00:00Z")),
      "2026-10-12T11:00:00.000Z",
    );
    const invoices = {
      id,
      schedule: monthly({ day: 31, timeZone: "Europe/Warsaw" }),
      nextRunAt: "2026-11-30T08:00:00.000Z",
    };
    // Three days late, far past upstream's ten-minute skip: the November run still happens.
    const due = at("2026-11-30T08:00:00Z");
    assert.isFalse(isMissedFixedTimeRun(invoices.schedule, due, at("2026-12-03T10:00:00Z")));
    assert.isTrue(
      isMissedFixedTimeRun(
        { type: "fixed_time", timeOfDay: "09:00" },
        due,
        at("2026-11-30T08:10:01Z"),
      ),
    );
    assert.equal(
      forkFireKey(invoices, "scheduled", at("2026-12-03T10:00:00Z")),
      "task:late:monthly:2026-11-30T08:00:00.000Z",
    );
    // Down for months: one run, for the latest month's slot.
    assert.equal(
      forkScheduledSlot(invoices, "scheduled", at("2027-03-15T00:00:00Z")),
      "2027-02-28T08:00:00.000Z",
    );
  });
});

describe("fork-extended fixed_time", () => {
  const at = (iso: string) => DateTime.makeUnsafe(ms(iso));
  const nextRunsOf = (schedule: FixedTime, from: string, count: number) => {
    const runs: string[] = [];
    let cursor: DateTime.DateTime = at(from);
    for (let step = 0; step < count; step += 1) {
      const next = forkNextScheduledRunAt(schedule, cursor);
      if (next === null) break;
      runs.push(DateTime.formatIso(next));
      cursor = next;
    }
    return runs;
  };

  it("reads unpadded times, runs short months once on their last day, and takes all seven weekdays as every day", () => {
    const schedule: FixedTime = {
      type: "fixed_time",
      timeOfDay: "9:05",
      extraTimes: ["09:05", "7:00"],
      monthDays: [30, 31],
      weekdays: [0, 1, 2, 3, 4, 5, 6],
      timeZone: "UTC",
    };
    assert.deepEqual(nextRunsOf(schedule, "2027-02-01T00:00:00Z", 6), [
      "2027-02-28T07:00:00.000Z",
      "2027-02-28T09:05:00.000Z",
      "2027-03-30T07:00:00.000Z",
      "2027-03-30T09:05:00.000Z",
      "2027-03-31T07:00:00.000Z",
      "2027-03-31T09:05:00.000Z",
    ]);
  });

  it("merges a save with the stored task: omitted fields kept, [] cleared, neutral plain saves stay plain", () => {
    const stored: FixedTime = {
      type: "fixed_time",
      timeOfDay: "09:00",
      extraTimes: ["17:00"],
      monthDays: [5],
      windowMinutes: 10,
      chosen: [{ requested: "09:00", offsetMinutes: 3 }],
      timeZone: "Europe/Warsaw",
    };
    const plain = { type: "fixed_time", timeOfDay: "08:00" } as const;
    assert.deepEqual(effectiveFixedTime(plain, stored, "UTC"), {
      schedule: {
        type: "fixed_time",
        timeOfDay: "08:00",
        timeZone: "Europe/Warsaw",
        extraTimes: ["17:00"],
        monthDays: [5],
        windowMinutes: 10,
      },
    });
    assert.deepEqual(
      effectiveFixedTime(
        { ...plain, extraTimes: [], monthDays: [], windowMinutes: 0 },
        stored,
        "UTC",
      ),
      { schedule: { type: "fixed_time", timeOfDay: "08:00", timeZone: "Europe/Warsaw" } },
    );
    // A weekday subset with stored month days is refused, not silently switched.
    assert.property(effectiveFixedTime({ ...plain, weekdays: [1] }, stored, "UTC"), "error");
    assert.deepEqual(
      effectiveFixedTime({ ...plain, weekdays: [1], monthDays: [] }, stored, "UTC"),
      {
        schedule: {
          type: "fixed_time",
          timeOfDay: "08:00",
          weekdays: [1],
          timeZone: "Europe/Warsaw",
          extraTimes: ["17:00"],
          windowMinutes: 10,
        },
      },
    );
    // Neutral values never turn a plain task into an extended one.
    const upstream = { type: "fixed_time", timeOfDay: "09:00", weekdays: [1, 2] } as const;
    for (const input of [
      upstream,
      { ...upstream, extraTimes: [], monthDays: [], windowMinutes: 0 },
      { ...upstream, extraTimes: ["9:00"] },
    ])
      assert.deepEqual(effectiveFixedTime(input, upstream, "Europe/Warsaw"), {
        schedule: upstream,
      });
    // Any other fork field pins the server's zone.
    assert.deepEqual(
      effectiveFixedTime({ ...upstream, extraTimes: ["18:00"] }, undefined, "Europe/Warsaw"),
      { schedule: { ...upstream, timeZone: "Europe/Warsaw", extraTimes: ["18:00"] } },
    );
  });
});

/**
 * The retired weekly and monthly slot code, frozen as the differential oracle for the migration:
 * every migrated row must fire at exactly the instants it fired at before.
 */
const legacyWallClockMs = (
  timeZone: string,
  date: { readonly year: number; readonly month: number; readonly day: number },
  minute: number,
) =>
  DateTime.toEpochMillis(
    DateTime.makeZonedUnsafe(
      { ...date, hour: Math.floor(minute / 60), minute: minute % 60, second: 0, millisecond: 0 },
      { timeZone, adjustForTimeZone: true },
    ),
  );
const legacyMinuteOf = (time: string) => Number(time.slice(0, 2)) * 60 + Number(time.slice(3));
const DAY = 86_400_000;
function legacyWeeklySlots(schedule: LegacyWeekly, startMs: number, endMs: number) {
  const weekdays = new Set(schedule.weekdays);
  const placed = [...new Set(schedule.times)].map((time) => ({
    time,
    offset: schedule.chosen?.find((choice) => choice.requested === time)?.offsetMinutes ?? 0,
  }));
  const slots: number[] = [];
  const first = DateTime.setZoneNamedUnsafe(DateTime.makeUnsafe(startMs - DAY), schedule.timeZone);
  const days = Math.ceil((endMs - startMs) / DAY) + 2;
  for (let day = 0; day <= days; day += 1) {
    const date = DateTime.toParts(DateTime.add(first, { days: day }));
    if (!weekdays.has(date.weekDay)) continue;
    for (const { time, offset } of placed) {
      const slot =
        legacyWallClockMs(schedule.timeZone, date, legacyMinuteOf(time)) + offset * 60_000;
      if (slot >= startMs && slot <= endMs) slots.push(slot);
    }
  }
  return [...new Set(slots)].toSorted((a, b) => a - b);
}
function legacyMonthlySlots(schedule: LegacyMonthly, startMs: number, endMs: number) {
  const monthIndex = (value: number) => {
    const parts = DateTime.toParts(
      DateTime.setZoneNamedUnsafe(DateTime.makeUnsafe(value), schedule.timeZone),
    );
    return parts.year * 12 + parts.month - 1;
  };
  const slots: number[] = [];
  for (let index = monthIndex(startMs - DAY); index <= monthIndex(endMs + DAY); index += 1) {
    const year = Math.floor(index / 12);
    const month = (index % 12) + 1;
    const lastDay =
      month === 2
        ? (year % 4 === 0 && year % 100 !== 0) || year % 400 === 0
          ? 29
          : 28
        : [4, 6, 9, 11].includes(month)
          ? 30
          : 31;
    const day = schedule.day === "last" ? lastDay : Math.min(schedule.day, lastDay);
    for (const time of new Set(schedule.times)) {
      const slot = legacyWallClockMs(schedule.timeZone, { year, month, day }, legacyMinuteOf(time));
      if (slot >= startMs && slot <= endMs) slots.push(slot);
    }
  }
  return [...new Set(slots)].toSorted((a, b) => a - b);
}
const legacyNext = (schedule: LegacyWeekly | LegacyMonthly, fromMs: number) =>
  (schedule.type === "weekly"
    ? legacyWeeklySlots(schedule, fromMs + 1, fromMs + 8 * DAY)
    : legacyMonthlySlots(schedule, fromMs + 1, fromMs + 33 * DAY)
  ).at(0);

describe("migrated weekly and monthly triggers", () => {
  const legacy: ReadonlyArray<LegacyWeekly | LegacyMonthly> = [
    legacyWeekly({ weekdays: [0, 1, 2, 3, 4, 5, 6], timeZone: "Europe/Warsaw" }),
    legacyWeekly({ weekdays: [0, 3], times: ["02:30", "23:59"], timeZone: "Europe/Warsaw" }),
    legacyWeekly({
      weekdays: [1, 5],
      times: ["00:02", "12:00", "17:30"],
      timeZone: "America/New_York",
      windowMinutes: 30,
      chosen: [
        { requested: "00:02", offsetMinutes: -5 },
        { requested: "12:00", offsetMinutes: 7 },
      ],
    }),
    // No window written: weekly's default of 30 minutes, kept explicitly.
    { type: "weekly", weekdays: [2], times: ["01:30"], timeZone: "America/New_York" },
    legacyMonthly({ day: 31, timeZone: "Europe/Warsaw" }),
    legacyMonthly({ day: "last", times: ["02:30", "18:00"], timeZone: "America/New_York" }),
    legacyMonthly({ day: 29, times: ["00:00"], timeZone: "UTC" }),
    legacyMonthly({ day: 1, times: ["23:30"], timeZone: "Pacific/Auckland" }),
  ];

  it("fire at exactly the instants they fired at before, across DST, leap years and year ends", () => {
    for (const schedule of legacy) {
      const fixed = migrated(schedule);
      for (const start of [
        "2026-01-20T00:00:00Z",
        "2027-10-20T00:00:00Z",
        "2028-02-01T00:00:00Z",
      ]) {
        let cursor = ms(start);
        for (let step = 0; step < 60; step += 1) {
          const expected = legacyNext(schedule, cursor);
          const actual = forkNextScheduledRunAt(fixed, DateTime.makeUnsafe(cursor));
          assert.equal(
            actual === null ? undefined : DateTime.toEpochMillis(actual),
            expected,
            `${JSON.stringify(schedule)} after ${DateTime.formatIso(DateTime.makeUnsafe(cursor))}`,
          );
          if (expected === undefined) break;
          cursor = expected;
        }
      }
    }
  });

  it("keep their minute picks and the weekly default window", () => {
    assert.deepEqual(migrated(legacy[2]!).chosen, [
      { requested: "00:02", offsetMinutes: -5 },
      { requested: "12:00", offsetMinutes: 7 },
    ]);
    assert.equal(migrated(legacy[3]!).windowMinutes, 30);
    assert.deepEqual(migrated(legacy[5]!), {
      type: "fixed_time",
      timeOfDay: "02:30",
      extraTimes: ["18:00"],
      timeZone: "America/New_York",
      monthDays: [31],
    });
  });
});

describe("minute spreading", () => {
  // Thursday: the next nine days hold one Monday 09:00.
  const now = ms("2026-10-08T00:00:00Z");
  const monday = "2026-10-12T09:00:00Z";

  it("moves a time to the least busy minute, the smallest move first and earlier on a tie", () => {
    const schedule = weekly({ windowMinutes: 30 });
    assert.deepEqual(chooseMinutes(schedule, new Set(), now), [
      { requested: "09:00", offsetMinutes: 0 },
    ]);
    assert.deepEqual(chooseMinutes(schedule, new Set([minuteOf(monday)]), now), [
      { requested: "09:00", offsetMinutes: -1 },
    ]);
    const crowded = new Set(["2026-10-12T08:59:00Z", monday, "2026-10-12T09:01:00Z"].map(minuteOf));
    assert.deepEqual(chooseMinutes(schedule, crowded, now), [
      { requested: "09:00", offsetMinutes: -2 },
    ]);
  });

  it("keeps the exact minute with no window, and spreads a task's own times apart", () => {
    const busy = new Set([minuteOf(monday)]);
    assert.deepEqual(chooseMinutes(weekly(), busy, now), [
      { requested: "09:00", offsetMinutes: 0 },
    ]);
    assert.deepEqual(
      chooseMinutes(weekly({ times: ["09:00", "08:59"], windowMinutes: 30 }), busy, now),
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
