import {
  isForkScheduledTaskSchedule,
  type ScheduledTaskSchedule,
  type ScheduledTaskUpsertSchedule,
} from "@t3tools/contracts";
import { msg } from "gt-react";

import { m, type MessageResolver } from "../../i18n/messages";

/**
 * Fork (toolboxmd/chromeria#719): the settings form's `fixed_time` fields beyond
 * upstream's time and weekdays: a time zone, more times, days of the month and
 * minute spreading.
 */
type FixedTimeInput = Extract<ScheduledTaskUpsertSchedule, { readonly type: "fixed_time" }>;
type ForkFields = Pick<
  FixedTimeInput,
  "timeZone" | "extraTimes" | "monthDays" | "windowMinutes" | "weekdays"
>;

export interface FixedTimeForkDraft {
  /** The task opened with fork fields, so every field is sent, a cleared one as [] or 0. */
  readonly extended: boolean;
  /** Empty means the server's zone. */
  readonly timeZone: string;
  /** More HH:MM times, separated by commas or spaces. */
  readonly extraTimes: string;
  readonly dayMode: "week" | "month";
  readonly monthDays: ReadonlySet<number>;
  readonly windowMinutes: string;
}

export const MONTH_DAYS = Array.from({ length: 31 }, (_, index) => index + 1);
const ALL_WEEKDAYS = [0, 1, 2, 3, 4, 5, 6];
const TIME = /^([01]?\d|2[0-3]):([0-5]\d)$/;

export function fixedTimeForkDraft(schedule: ScheduledTaskSchedule | null): FixedTimeForkDraft {
  const fork =
    schedule !== null && schedule.type === "fixed_time" && isForkScheduledTaskSchedule(schedule)
      ? schedule
      : null;
  return {
    extended: fork !== null,
    timeZone: fork?.timeZone ?? "",
    extraTimes: (fork?.extraTimes ?? []).join(", "),
    dayMode: (fork?.monthDays ?? []).length > 0 ? "month" : "week",
    monthDays: new Set(fork?.monthDays ?? []),
    windowMinutes: String(fork?.windowMinutes ?? 0),
  };
}

const isTimeZone = (value: string) => {
  try {
    return new Intl.DateTimeFormat("en-US", { timeZone: value }).resolvedOptions().timeZone !== "";
  } catch {
    return false;
  }
};

const parsed = (draft: FixedTimeForkDraft) => {
  const extraTimes = draft.extraTimes
    .split(/[\s,]+/)
    .filter((time) => time !== "")
    .map((time) => (TIME.test(time) ? time.padStart(5, "0") : null));
  const windowMinutes = draft.windowMinutes.trim() === "" ? 0 : Number(draft.windowMinutes);
  return {
    timeZone: draft.timeZone.trim(),
    extraTimes,
    monthDays: draft.dayMode === "month" ? [...draft.monthDays].toSorted((a, b) => a - b) : [],
    windowMinutes,
  };
};

export function fixedTimeForkErrors(
  draft: FixedTimeForkDraft,
  resolve: MessageResolver = m,
): ReadonlyArray<{ readonly title: string; readonly description: string }> {
  const fields = parsed(draft);
  const errors: Array<{ readonly title: string; readonly description: string }> = [];
  if (fields.timeZone !== "" && !isTimeZone(fields.timeZone))
    errors.push({
      title: resolve(msg("Unknown time zone")),
      description: resolve(msg("Use a zone such as {example}.", { example: "Europe/Warsaw" })),
    });
  // A saved zone cannot be cleared: a blank would keep it while the form shows the server's.
  if (fields.timeZone === "" && draft.extended)
    errors.push({
      title: resolve(msg("Enter a time zone")),
      description: resolve(
        msg("This schedule keeps a named zone. Enter one, such as the server's."),
      ),
    });
  if (fields.extraTimes.includes(null))
    errors.push({
      title: resolve(msg("Invalid extra times")),
      description: resolve(msg("Enter times as HH:MM separated by commas.")),
    });
  if (draft.dayMode === "month" && fields.monthDays.length === 0)
    errors.push({
      title: resolve(msg("Choose a day of the month")),
      description: resolve(msg("Pick at least one day.")),
    });
  if (
    !Number.isInteger(fields.windowMinutes) ||
    fields.windowMinutes < 0 ||
    fields.windowMinutes > 720
  )
    errors.push({
      title: resolve(msg("Invalid spread")),
      description: resolve(msg("Spread by a whole number of minutes from 0 to 720.")),
    });
  return errors;
}

/**
 * The fork fields a save sends. An untouched plain task sends none, so it stays upstream's own;
 * an extended one sends every field, so a cleared field clears. Month days run every week day.
 */
export function fixedTimeForkFields(draft: FixedTimeForkDraft): ForkFields {
  const fields = parsed(draft);
  const extraTimes = fields.extraTimes.filter((time): time is string => time !== null);
  const neutral =
    fields.timeZone === "" &&
    extraTimes.length === 0 &&
    fields.monthDays.length === 0 &&
    fields.windowMinutes === 0;
  if (neutral && !draft.extended) return {};
  return {
    ...(fields.timeZone === "" ? {} : { timeZone: fields.timeZone }),
    extraTimes,
    monthDays: fields.monthDays,
    windowMinutes: fields.windowMinutes,
    ...(fields.monthDays.length === 0 ? {} : { weekdays: ALL_WEEKDAYS }),
  };
}

/** The zone the times are read in, when the task names one. */
export const fixedTimeZone = (draft: FixedTimeForkDraft) => draft.timeZone.trim() || undefined;
