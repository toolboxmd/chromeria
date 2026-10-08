import {
  DEFAULT_WEEKLY_WINDOW_MINUTES,
  type ScheduledTask,
  type ScheduledTaskForkSchedule,
  type ScheduledTaskSchedule,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

/**
 * Fork schedule triggers (toolboxmd/chromeria#174), reached from upstream's
 * `Schedule.ts` through one guard per helper. Pure: no import back into
 * upstream, so the hook cannot form a cycle.
 */
type Weekly = Extract<ScheduledTaskForkSchedule, { readonly type: "weekly" }>;

const MINUTE_MS = 60_000;
const DAY_MS = 86_400_000;
const iso = (ms: number) => DateTime.formatIso(DateTime.makeUnsafe(ms));
const minuteOf = (time: string) => Number(time.slice(0, 2)) * 60 + Number(time.slice(3));

/** The canonical instant of an ISO timestamp, so equivalent offsets compare equal. */
export const canonicalInstant = (value: string) => iso(Date.parse(value));

/** Offsets per requested time; times the server has not placed yet run on the exact minute. */
const offsets = (schedule: Weekly) =>
  [...new Set(schedule.times)].map((time) => ({
    time,
    offset: schedule.chosen?.find((choice) => choice.requested === time)?.offsetMinutes ?? 0,
  }));

/** Weekly instants in [startMs, endMs], read in the schedule's time zone. */
export function weeklySlots(
  schedule: Weekly,
  startMs: number,
  endMs: number,
): ReadonlyArray<number> {
  const weekdays = new Set(schedule.weekdays);
  const placed = offsets(schedule);
  const slots: number[] = [];
  // Offsets reach at most 12 hours either way, so one extra day on each side covers them.
  const first = DateTime.setZoneNamedUnsafe(
    DateTime.makeUnsafe(startMs - DAY_MS),
    schedule.timeZone,
  );
  const days = Math.ceil((endMs - startMs) / DAY_MS) + 2;
  for (let day = 0; day <= days; day += 1) {
    const date = DateTime.toParts(DateTime.add(first, { days: day }));
    if (!weekdays.has(date.weekDay)) continue;
    for (const { time, offset } of placed) {
      const minute = minuteOf(time);
      const base = DateTime.makeZonedUnsafe(
        {
          year: date.year,
          month: date.month,
          day: date.day,
          hour: Math.floor(minute / 60),
          minute: minute % 60,
          second: 0,
          millisecond: 0,
        },
        { timeZone: schedule.timeZone, adjustForTimeZone: true },
      );
      const slot = DateTime.toEpochMillis(base) + offset * MINUTE_MS;
      if (slot >= startMs && slot <= endMs) slots.push(slot);
    }
  }
  return [...new Set(slots)].toSorted((a, b) => a - b);
}

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
  const next = weeklySlots(schedule, fromMs + 1, fromMs + 8 * DAY_MS).at(0);
  return next === undefined ? null : DateTime.makeUnsafe(next);
}

const sameSet = <A>(a: ReadonlyArray<A>, b: ReadonlyArray<A>) => {
  const left = new Set(a);
  const right = new Set(b);
  return left.size === right.size && [...left].every((value) => right.has(value));
};

/** True iff both fire at the same instants; the server's minute picks do not count. */
export function forkSameSchedule(a: ScheduledTaskSchedule, b: ScheduledTaskSchedule): boolean {
  if (a.type === "once" && b.type === "once") return Date.parse(a.at) === Date.parse(b.at);
  if (a.type === "weekly" && b.type === "weekly")
    return (
      sameSet(a.weekdays, b.weekdays) &&
      sameSet(a.times, b.times) &&
      a.timeZone === b.timeZone &&
      (a.windowMinutes ?? DEFAULT_WEEKLY_WINDOW_MINUTES) ===
        (b.windowMinutes ?? DEFAULT_WEEKLY_WINDOW_MINUTES)
    );
  return false;
}

export function forkScheduleLabel(schedule: ScheduledTaskForkSchedule): string {
  if (schedule.type === "once") return `Once at ${schedule.at}`;
  return `Weekly on ${schedule.weekdays.join(",")} at ${schedule.times.join(", ")} (${schedule.timeZone})`;
}

/**
 * The slot a scheduled fire of a fork trigger runs for, canonical so a replay
 * of the same slot is the same fire; null for every other fire.
 */
export function forkScheduledSlot(
  task: Pick<ScheduledTask, "schedule" | "nextRunAt">,
  trigger: "scheduled" | "manual" | "webhook",
): string | null {
  if (trigger !== "scheduled") return null;
  if (task.schedule.type === "once") return canonicalInstant(task.schedule.at);
  if (task.schedule.type === "weekly" && task.nextRunAt !== null)
    return canonicalInstant(task.nextRunAt);
  return null;
}

/** Upstream's fire key for a fork trigger's scheduled fire; null keeps upstream's own key. */
export function forkFireKey(
  task: Pick<ScheduledTask, "id" | "schedule" | "nextRunAt">,
  trigger: "scheduled" | "manual" | "webhook",
): string | null {
  const slot = forkScheduledSlot(task, trigger);
  return slot === null ? null : `${task.id}:${task.schedule.type}:${slot}`;
}

/**
 * Picks, for each requested time, the minute within the window that collides
 * least with `occupied` minutes over the next nine days (ties: the smallest
 * move, earlier first). Ported from Chromeria v1 minute spreading.
 */
export function chooseWeeklyMinutes(
  schedule: Weekly,
  occupied: ReadonlySet<number>,
  nowMs: number,
): NonNullable<Weekly["chosen"]> {
  const window = schedule.windowMinutes ?? DEFAULT_WEEKLY_WINDOW_MINUTES;
  const end = nowMs + 9 * DAY_MS;
  const taken = new Set<number>();
  return [...new Set(schedule.times)].map((time) => {
    const candidates = Array.from({ length: window * 2 + 1 }, (_, index) => index - window).map(
      (offsetMinutes) => {
        const slots = weeklySlots(
          { ...schedule, times: [time], chosen: [{ requested: time, offsetMinutes }] },
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
  return weeklySlots(schedule, nowMs, end).map((slot) => Math.floor(slot / MINUTE_MS));
}
