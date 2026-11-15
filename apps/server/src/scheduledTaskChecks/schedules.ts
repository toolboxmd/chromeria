import {
  FIXED_TIME_MONTH_DAYS_WITH_WEEKDAYS,
  isEveryWeekday,
  isForkScheduledTaskSchedule,
  type ScheduledTask,
  type ScheduledTaskFixedTimeForkSchedule,
  type ScheduledTaskForkSchedule,
  type ScheduledTaskSchedule,
  type ScheduledTaskUpsertSchedule,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

/**
 * Fork schedule triggers and fork-extended `fixed_time`, reached from upstream's
 * `Schedule.ts` through one guard per helper. Pure: no import back into
 * upstream, so the hook cannot form a cycle.
 */
type FixedTime = ScheduledTaskFixedTimeForkSchedule;
type FixedTimeInput = Extract<ScheduledTaskUpsertSchedule, { readonly type: "fixed_time" }>;

const MINUTE_MS = 60_000;
const DAY_MS = 86_400_000;
const iso = (ms: number) => DateTime.formatIso(DateTime.makeUnsafe(ms));
/** Minute of the day of an `H:MM` or `HH:MM` time. */
const minuteOf = (time: string) => {
  const [hour, minute] = time.trim().split(":");
  return Number(hour) * 60 + Number(minute);
};

/** The canonical instant of an ISO timestamp, so equivalent offsets compare equal. */
export const canonicalInstant = (value: string) => iso(Date.parse(value));

/** The instant a wall-clock minute of a calendar day has in the time zone. */
const wallClockMs = (
  timeZone: string,
  date: { readonly year: number; readonly month: number; readonly day: number },
  minute: number,
) =>
  DateTime.toEpochMillis(
    DateTime.makeZonedUnsafe(
      {
        year: date.year,
        month: date.month,
        day: date.day,
        hour: Math.floor(minute / 60),
        minute: minute % 60,
        second: 0,
        millisecond: 0,
      },
      { timeZone, adjustForTimeZone: true },
    ),
  );

/** The requested times, one per minute of the day, in the order given. */
const requestedTimes = (schedule: Pick<FixedTime, "timeOfDay" | "extraTimes">) => {
  const byMinute = new Map<number, string>();
  for (const time of [schedule.timeOfDay, ...(schedule.extraTimes ?? [])])
    if (!byMinute.has(minuteOf(time))) byMinute.set(minuteOf(time), time);
  return [...byMinute.entries()].map(([minute, time]) => ({ minute, time }));
};

/** Offsets per requested time; times the server has not placed yet run on the exact minute. */
const placed = (schedule: FixedTime) =>
  requestedTimes(schedule).map(({ minute, time }) => ({
    minute,
    time,
    offset:
      schedule.chosen?.find((choice) => minuteOf(choice.requested) === minute)?.offsetMinutes ?? 0,
  }));

const daysInMonth = (year: number, month: number) =>
  month === 2
    ? (year % 4 === 0 && year % 100 !== 0) || year % 400 === 0
      ? 29
      : 28
    : [4, 6, 9, 11].includes(month)
      ? 30
      : 31;

/**
 * Instants in [startMs, endMs], read in the schedule's time zone. A day runs when it is one of
 * `monthDays` (a day a month lacks runs on its last day) or, without month days, a selected weekday.
 */
export function fixedTimeSlots(
  schedule: FixedTime,
  startMs: number,
  endMs: number,
): ReadonlyArray<number> {
  const monthDays = schedule.monthDays ?? [];
  const weekdays = isEveryWeekday(schedule.weekdays) ? null : new Set(schedule.weekdays);
  const times = placed(schedule);
  const slots: number[] = [];
  // Offsets reach at most 12 hours either way, so one extra day on each side covers them.
  const first = DateTime.setZoneNamedUnsafe(
    DateTime.makeUnsafe(startMs - DAY_MS),
    schedule.timeZone,
  );
  const days = Math.ceil((endMs - startMs) / DAY_MS) + 2;
  for (let day = 0; day <= days; day += 1) {
    const date = DateTime.toParts(DateTime.add(first, { days: day }));
    const runs =
      monthDays.length > 0
        ? monthDays.some(
            (monthDay) => Math.min(monthDay, daysInMonth(date.year, date.month)) === date.day,
          )
        : weekdays === null || weekdays.has(date.weekDay);
    if (!runs) continue;
    for (const { minute, offset } of times) {
      const slot = wallClockMs(schedule.timeZone, date, minute) + offset * MINUTE_MS;
      if (slot >= startMs && slot <= endMs) slots.push(slot);
    }
  }
  return [...new Set(slots)].toSorted((a, b) => a - b);
}

/** The longest gap between runs: a month day is 31 days plus a daylight saving hour apart. */
const horizonMs = (schedule: FixedTime) =>
  ((schedule.monthDays ?? []).length > 0 ? 33 : 8) * DAY_MS;

/** The next fire strictly after `from`, or null when there is none. */
export function forkNextScheduledRunAt(
  schedule: ScheduledTaskForkSchedule,
  from: DateTime.DateTime,
): DateTime.DateTime | null {
  const fromMs = DateTime.toEpochMillis(from);
  if (schedule.type === "once") {
    const at = Date.parse(schedule.at);
    return Number.isFinite(at) && at > fromMs ? DateTime.makeUnsafe(at) : null;
  }
  const next = fixedTimeSlots(schedule, fromMs + 1, fromMs + horizonMs(schedule)).at(0);
  return next === undefined ? null : DateTime.makeUnsafe(next);
}

const sameSet = <A>(a: ReadonlyArray<A>, b: ReadonlyArray<A>) => {
  const left = new Set(a);
  const right = new Set(b);
  return left.size === right.size && [...left].every((value) => right.has(value));
};

/** True iff both fire at the same instants; the server's minute picks do not count. */
export function forkSameSchedule(a: ScheduledTaskSchedule, b: ScheduledTaskSchedule): boolean {
  if (a.type === "once" && b.type === "once")
    return Date.parse("at" in a ? a.at : a.runAt) === Date.parse("at" in b ? b.at : b.runAt);
  if (!isForkScheduledTaskSchedule(a) || !isForkScheduledTaskSchedule(b)) return false;
  if (a.type !== "fixed_time" || b.type !== "fixed_time") return false;
  const days = (schedule: FixedTime) =>
    isEveryWeekday(schedule.weekdays) ? [] : (schedule.weekdays ?? []);
  return (
    a.timeZone === b.timeZone &&
    sameSet(days(a), days(b)) &&
    sameSet(
      requestedTimes(a).map((time) => time.minute),
      requestedTimes(b).map((time) => time.minute),
    ) &&
    sameSet(a.monthDays ?? [], b.monthDays ?? []) &&
    (a.windowMinutes ?? 0) === (b.windowMinutes ?? 0)
  );
}

/**
 * The slot a scheduled fire runs for, canonical so a replay of the same slot is the same fire;
 * null for every other fire. After downtime an extended `fixed_time` runs once, for the latest slot
 * due at `startedAt`, never earlier than its pending one. Dispatch, fire keys and checked runs all
 * take it from here with the same `startedAt`.
 */
export function forkScheduledSlot(
  task: Pick<ScheduledTask, "schedule" | "nextRunAt">,
  trigger: "scheduled" | "manual" | "webhook",
  startedAt: DateTime.DateTime,
): string | null {
  if (trigger !== "scheduled") return null;
  const schedule = task.schedule;
  if (schedule.type === "once")
    return canonicalInstant("at" in schedule ? schedule.at : schedule.runAt);
  if (!isForkScheduledTaskSchedule(schedule) || task.nextRunAt === null) return null;
  const pendingMs = Date.parse(task.nextRunAt);
  const startedMs = DateTime.toEpochMillis(startedAt);
  // Any slot run in the last horizon is the latest one, so older days need not be read.
  const latest = fixedTimeSlots(
    schedule,
    Math.max(pendingMs, startedMs - horizonMs(schedule)),
    startedMs,
  ).at(-1);
  return iso(latest ?? pendingMs);
}

/** Upstream's fire key for a fork trigger's scheduled fire; null keeps upstream's own key. */
export function forkFireKey(
  task: Pick<ScheduledTask, "id" | "schedule" | "nextRunAt">,
  trigger: "scheduled" | "manual" | "webhook",
  startedAt: DateTime.DateTime,
): string | null {
  const slot = forkScheduledSlot(task, trigger, startedAt);
  if (slot === null) return null;
  // Keys once named the retired weekly or monthly trigger; keeping them means no slot fires twice.
  const kind =
    task.schedule.type === "once"
      ? "once"
      : "monthDays" in task.schedule && (task.schedule.monthDays ?? []).length > 0
        ? "monthly"
        : "weekly";
  return `${task.id}:${kind}:${slot}`;
}

/**
 * Picks, for each requested time, the minute within the window that collides
 * least with `occupied` minutes over the next nine days (ties: the smallest
 * move, earlier first). Ported from Chromeria v1 minute spreading.
 */
export function chooseMinutes(
  schedule: FixedTime,
  occupied: ReadonlySet<number>,
  nowMs: number,
): NonNullable<FixedTime["chosen"]> {
  const window = schedule.windowMinutes ?? 0;
  const end = nowMs + 9 * DAY_MS;
  const taken = new Set<number>();
  return requestedTimes(schedule).map(({ time }) => {
    const candidates = Array.from({ length: window * 2 + 1 }, (_, index) => index - window).map(
      (offsetMinutes) => {
        const slots = fixedTimeSlots(
          {
            ...schedule,
            timeOfDay: time,
            extraTimes: [],
            chosen: [{ requested: time, offsetMinutes }],
          },
          nowMs,
          end,
        ).map((slot) => Math.floor(slot / MINUTE_MS));
        const count = slots.filter((slot) => occupied.has(slot) || taken.has(slot)).length;
        return { offsetMinutes, slots, count };
      },
    );
    candidates.sort(
      (a, b) =>
        a.count - b.count ||
        Math.abs(a.offsetMinutes) - Math.abs(b.offsetMinutes) ||
        a.offsetMinutes - b.offsetMinutes,
    );
    const picked = candidates[0]!;
    for (const slot of picked.slots) taken.add(slot);
    return { requested: time, offsetMinutes: picked.offsetMinutes };
  });
}

/** Minutes another task occupies in the next nine days, for spreading. */
export function occupiedMinutes(
  schedule: ScheduledTaskForkSchedule,
  nowMs: number,
): ReadonlyArray<number> {
  const end = nowMs + 9 * DAY_MS;
  if (schedule.type === "once") {
    const at = Date.parse(schedule.at);
    return at >= nowMs && at <= end ? [Math.floor(at / MINUTE_MS)] : [];
  }
  return fixedTimeSlots(schedule, nowMs, end).map((slot) => Math.floor(slot / MINUTE_MS));
}

/**
 * The `fixed_time` a save stores: fork fields left out keep the stored task's values and `[]`
 * clears them (D4). Neutral values (`[]`, 0) on a plain task add nothing, so it stays upstream's;
 * any other fork field pins the server's zone. Picks are the caller's to recompute.
 */
export function effectiveFixedTime(
  input: FixedTimeInput,
  stored: ScheduledTaskSchedule | undefined,
  serverTimeZone: string,
): { readonly schedule: FixedTimeInput | FixedTime } | { readonly error: string } {
  const kept = stored !== undefined && isForkScheduledTaskSchedule(stored) ? stored : undefined;
  const base = kept?.type === "fixed_time" ? kept : undefined;
  const timeZone = input.timeZone ?? base?.timeZone;
  const extraTimes = requestedTimes({
    timeOfDay: input.timeOfDay,
    extraTimes: input.extraTimes ?? base?.extraTimes ?? [],
  })
    .slice(1)
    .map(({ time }) => time);
  const monthDays = [...new Set(input.monthDays ?? base?.monthDays ?? [])].toSorted(
    (a, b) => a - b,
  );
  const windowMinutes = input.windowMinutes ?? base?.windowMinutes ?? 0;
  const upstream: FixedTimeInput = {
    type: "fixed_time",
    timeOfDay: input.timeOfDay,
    ...(input.weekdays === undefined ? {} : { weekdays: input.weekdays }),
  };
  if (monthDays.length > 0 && !isEveryWeekday(upstream.weekdays))
    return { error: FIXED_TIME_MONTH_DAYS_WITH_WEEKDAYS };
  if (
    timeZone === undefined &&
    extraTimes.length === 0 &&
    monthDays.length === 0 &&
    windowMinutes === 0
  )
    return { schedule: upstream };
  return {
    schedule: {
      ...upstream,
      timeZone: timeZone ?? serverTimeZone,
      ...(extraTimes.length === 0 ? {} : { extraTimes }),
      ...(monthDays.length === 0 ? {} : { monthDays }),
      ...(windowMinutes === 0 ? {} : { windowMinutes }),
    },
  };
}
