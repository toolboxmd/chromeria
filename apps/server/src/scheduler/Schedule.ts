import type { ScheduledTask, TaskDefinition, TaskMinuteChoice } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

export const iso = (ms: number) => DateTime.formatIso(DateTime.makeUnsafe(ms));
const minute = 60_000;
const timeMinute = (time: string) => Number(time.slice(0, 2)) * 60 + Number(time.slice(3));
const clockLabel = (value: number) => {
  const wrapped = ((value % 1440) + 1440) % 1440;
  return `${String(Math.floor(wrapped / 60)).padStart(2, "0")}:${String(wrapped % 60).padStart(2, "0")}`;
};

/** Enumerate only the creation-time horizon, not every missed historical slot. */
function slotsBetween(task: ScheduledTask, start: number, end: number) {
  const slots: number[] = [];
  let cursor = start - 1;
  for (;;) {
    const next = taskSlots(task, cursor).next;
    if (next === null || next > end) return slots;
    slots.push(next);
    cursor = next;
  }
}

/** Count actual competing instants across schedule kinds, weekdays and time zones. */
export function chooseMinutes(
  definition: TaskDefinition,
  tasks: ReadonlyArray<ScheduledTask>,
  now: number,
): ReadonlyArray<TaskMinuteChoice> {
  const schedule = definition.schedule;
  if (schedule.kind === "interval") return [];
  const requested = schedule.kind === "once" ? [schedule.at] : [...new Set(schedule.times)];
  const window = schedule.windowMinutes ?? 30;
  const start =
    schedule.kind === "once"
      ? Math.floor(Date.parse(schedule.at) / minute) * minute - window * minute
      : now;
  const end = schedule.kind === "once" ? start + window * 2 * minute : now + 9 * 86400_000;
  const occupancy = tasks
    .filter((task) => !task.deleted && !task.paused)
    .map((task) => ({
      task,
      slots: new Set(slotsBetween(task, start, end).map((slot) => Math.floor(slot / minute))),
    }));
  const chosenSlots = new Set<number>();
  return requested.map((time) => {
    const center =
      schedule.kind === "once" ? Math.floor(Date.parse(time) / minute) : timeMinute(time);
    const candidates = Array.from({ length: window * 2 + 1 }, (_, index) => index - window)
      .map((offsetMinutes) => {
        const chosen =
          schedule.kind === "once"
            ? iso((center + offsetMinutes) * minute)
            : clockLabel(center + offsetMinutes);
        const choice: TaskMinuteChoice = { requested: time, chosen, offsetMinutes, neighbours: [] };
        const candidate: ScheduledTask = {
          checkCwd: "/",
          id: "candidate",
          revision: 1,
          definition,
          createdAt: iso(now),
          updatedAt: iso(now),
          paused: false,
          deleted: false,
          checks: [],
          choices: [choice],
          consumedSlot: null,
          runs: [],
          failureStreak: 0,
          lastError: null,
        };
        const slots = slotsBetween(candidate, start, end).map((slot) => Math.floor(slot / minute));
        const count =
          slots.filter((slot) => chosenSlots.has(slot)).length +
          occupancy.reduce(
            (sum, entry) => sum + slots.filter((slot) => entry.slots.has(slot)).length,
            0,
          );
        return { choice, slots, count };
      })
      .filter(
        (candidate) => schedule.kind !== "once" || Date.parse(candidate.choice.chosen) >= now,
      );
    candidates.sort(
      (a, b) =>
        a.count - b.count ||
        Math.abs(a.choice.offsetMinutes) - Math.abs(b.choice.offsetMinutes) ||
        a.choice.offsetMinutes - b.choice.offsetMinutes,
    );
    const picked = candidates[0];
    if (!picked) throw new RangeError("One-shot time window is in the past.");
    for (const slot of picked.slots) chosenSlots.add(slot);
    const neighbours = occupancy.flatMap((entry) =>
      [...entry.slots]
        .filter((slot) => picked.slots.some((candidate) => Math.abs(candidate - slot) <= window))
        .map((slot) => ({
          taskId: entry.task.id,
          title: entry.task.definition.title,
          chosen: iso(slot * minute),
        })),
    );
    return { ...picked.choice, neighbours };
  });
}

/** Compute at most the latest missed slot and the next deadline. Weekdays use Sunday=0. */
export function taskSlots(
  task: ScheduledTask,
  now: number,
): { latest: number | null; next: number | null } {
  const schedule = task.definition.schedule;
  const created = Date.parse(task.createdAt);
  if (schedule.kind === "interval") {
    const width = schedule.minutes * minute;
    const index = Math.floor((now - created) / width);
    return {
      latest: index >= 1 ? created + index * width : null,
      next: created + Math.max(1, index + 1) * width,
    };
  }
  if (schedule.kind === "once") {
    const slot = Date.parse(task.choices[0]!.chosen);
    return { latest: slot <= now && slot >= created ? slot : null, next: slot > now ? slot : null };
  }
  const today = DateTime.setZoneNamedUnsafe(DateTime.makeUnsafe(now), schedule.timeZone);
  const slots: number[] = [];
  for (let day = -8; day <= 8; day++) {
    const date = DateTime.toParts(DateTime.add(today, { days: day }));
    if (!schedule.weekdays.includes(date.weekDay)) continue;
    for (const choice of task.choices) {
      const base = DateTime.makeZonedUnsafe(
        {
          year: date.year,
          month: date.month,
          day: date.day,
          hour: Math.floor(timeMinute(choice.requested) / 60),
          minute: timeMinute(choice.requested) % 60,
          second: 0,
          millisecond: 0,
        },
        { timeZone: schedule.timeZone, adjustForTimeZone: true },
      );
      const slot = DateTime.toEpochMillis(base) + choice.offsetMinutes * minute;
      if (slot >= created) slots.push(slot);
    }
  }
  const past = slots.filter((slot) => slot <= now);
  const future = slots.filter((slot) => slot > now);
  return {
    latest: past.length ? Math.max(...past) : null,
    next: future.length ? Math.min(...future) : null,
  };
}
