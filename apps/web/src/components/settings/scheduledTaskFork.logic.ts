import { ScheduledTaskMonthlySchedule } from "@t3tools/contracts";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

/** Fork (toolboxmd/chromeria#707): the settings form's monthly schedule fields. */
type Monthly = typeof ScheduledTaskMonthlySchedule.Type;

export interface MonthlyDraft {
  /** "1" to "31", or "last". */
  readonly day: string;
  /** HH:MM times separated by commas or spaces. */
  readonly times: string;
  readonly timeZone: string;
}

export const MONTHLY_DAYS = [
  ...Array.from({ length: 31 }, (_, index) => String(index + 1)),
  "last",
];

export const monthlyDayLabel = (day: string) => (day === "last" ? "Last day" : day);

export function monthlyDraft(schedule: Monthly | null): MonthlyDraft {
  return schedule === null
    ? {
        day: "1",
        times: "09:00",
        timeZone: Intl.DateTimeFormat().resolvedOptions().timeZone,
      }
    : {
        day: String(schedule.day),
        times: schedule.times.join(", "),
        timeZone: schedule.timeZone,
      };
}

const decodeMonthly = Schema.decodeUnknownOption(ScheduledTaskMonthlySchedule);

/** The monthly schedule the fields describe, or null when they do not form one. */
export function monthlyScheduleFromDraft(draft: MonthlyDraft): Monthly | null {
  const times = draft.times
    .split(/[\s,]+/)
    .filter((time) => time !== "")
    // A time input or an agent may write 9:05; the schedule needs 09:05.
    .map((time) => time.padStart(5, "0"));
  return Option.getOrNull(
    decodeMonthly({
      type: "monthly",
      day: draft.day === "last" ? "last" : Number(draft.day),
      times: [...new Set(times)],
      timeZone: draft.timeZone.trim(),
    }),
  );
}
