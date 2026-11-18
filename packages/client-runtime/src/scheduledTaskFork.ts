import {
  isEveryWeekday,
  type ScheduledTask,
  type ScheduledTaskCommand,
  type ScheduledTaskForkSchedule,
  type ScheduledTaskOutcomeCheck,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import { mFallback, msg } from "gt-i18n";
import type { MFunctionType } from "gt-i18n/types";

/**
 * Fork: what web and mobile show for the fork's
 * scheduled task extensions. Upstream screens reach this through one call per label.
 */

const WEEKDAYS = [
  msg("Sun", { $context: "weekday abbreviation" }),
  msg("Mon", { $context: "weekday abbreviation" }),
  msg("Tue", { $context: "weekday abbreviation" }),
  msg("Wed", { $context: "weekday abbreviation" }),
  msg("Thu", { $context: "weekday abbreviation" }),
  msg("Fri", { $context: "weekday abbreviation" }),
  msg("Sat", { $context: "weekday abbreviation" }),
] as const;

/**
 * A requested time with the server's move, as a signed offset: shifting the
 * clock instead could cross midnight and pair the time with the wrong day.
 */
const placedTime = (time: string, offsetMinutes: number, resolve: MFunctionType) =>
  offsetMinutes === 0
    ? time
    : resolve(msg("{time} ({offset} min)"), {
        time,
        offset: `${offsetMinutes > 0 ? "+" : "-"}${Math.abs(offsetMinutes)}`,
      });

/** A one-shot's instant, or an extended `fixed_time`'s days, requested times with any move, and time zone. */
export function forkScheduleLabel(
  schedule: ScheduledTaskForkSchedule,
  options: { readonly locale?: string; readonly timeZone?: string } = {},
  resolve: MFunctionType = mFallback,
): string {
  if (schedule.type === "once") {
    const instant = DateTime.make(schedule.at);
    if (Option.isNone(instant)) return resolve(msg("Once at {time}"), { time: schedule.at });
    const formatted = new Intl.DateTimeFormat(options.locale, {
      dateStyle: "medium",
      timeStyle: "short",
      ...(options.timeZone === undefined ? {} : { timeZone: options.timeZone }),
    }).format(DateTime.toEpochMillis(instant.value));
    return resolve(msg("Once at {time}"), { time: formatted });
  }
  // Hermes lacks Array#toSorted; the spreads are already copies.
  const monthDays = [...new Set(schedule.monthDays ?? [])].sort((a, b) => a - b);
  const days =
    monthDays.length > 0
      ? resolve(msg("Monthly on {days}"), {
          days: monthDays
            .map((day) =>
              day === 31
                ? resolve(msg("the last day"))
                : resolve(msg("day {day}"), { day: String(day) }),
            )
            .join(", "),
        })
      : isEveryWeekday(schedule.weekdays)
        ? resolve(msg("Daily", { $context: "schedule frequency" }))
        : [...new Set(schedule.weekdays)]
            .sort((a, b) => ((a + 6) % 7) - ((b + 6) % 7))
            .map((day) => {
              const name = WEEKDAYS[day];
              return name === undefined ? String(day) : resolve(name);
            })
            .join(", ");
  const minuteOf = (time: string) => {
    const [hour, minute] = time.split(":");
    return Number(hour) * 60 + Number(minute);
  };
  const requested = [schedule.timeOfDay, ...(schedule.extraTimes ?? [])].filter(
    (time, index, all) => all.findIndex((other) => minuteOf(other) === minuteOf(time)) === index,
  );
  // The server may move each time within its window; show by how much.
  const times = requested
    .map((time) =>
      placedTime(
        time,
        schedule.chosen?.find((choice) => minuteOf(choice.requested) === minuteOf(time))
          ?.offsetMinutes ?? 0,
        resolve,
      ),
    )
    .join(", ");
  return resolve(msg("{days} at {times} ({timeZone})"), {
    days,
    times,
    timeZone: schedule.timeZone,
  });
}

const STAGE_LABELS: Record<NonNullable<ScheduledTaskOutcomeCheck["run"]>["stage"], string> = {
  running: msg("Working until its check passes"),
  retry: msg("Retrying until its check passes"),
  "usage-limit": msg("Waiting for a usage limit to reset"),
  "needs-you": msg("Needs you"),
  done: msg("Check passed"),
};

export function outcomeCheckLabel(
  check: ScheduledTaskOutcomeCheck,
  resolve: MFunctionType = mFallback,
): string {
  const head = resolve(msg("Outcome check v{version}"), { version: String(check.version) });
  if (check.run === null) return `${head} · ${resolve(msg("not run yet"))}`;
  if (check.run.imported)
    return `${head} · ${resolve(msg("imported from Chromeria v1, not resumed"))}`;
  const stage = resolve(STAGE_LABELS[check.run.stage]);
  const verdict =
    check.lastVerdict === null || check.run.stage === "done"
      ? ""
      : ` · ${check.lastVerdict.passed ? resolve(msg("last check passed")) : resolve(msg("last check failed"))}`;
  return `${head} · ${stage}${verdict}`;
}

export function commandLabel(
  command: ScheduledTaskCommand,
  resolve: MFunctionType = mFallback,
): string {
  const head = resolve(msg("Shell command: {command}"), { command: command.command });
  const run = command.run;
  if (run === null) return `${head} · ${resolve(msg("not run yet"))}`;
  if (run.imported) return `${head} · ${resolve(msg("last run imported from Chromeria v1"))}`;
  if (run.stage === "running")
    return `${head} · ${resolve(msg("running", { $context: "shell command status" }))}`;
  if (run.stage === "done")
    return `${head} · ${resolve(msg("passed", { $context: "shell command status" }))}`;
  return `${head} · ${
    run.timedOut
      ? resolve(msg("timed out", { $context: "shell command status" }))
      : run.exitCode === null
        ? resolve(msg("failed", { $context: "shell command status" }))
        : resolve(msg("exited with {code}"), { code: String(run.exitCode) })
  }`;
}

/** One line for a task's fork extension, or null for an upstream-only task. */
export function forkTaskSummary(
  task: Pick<ScheduledTask, "outcomeCheck" | "command">,
  resolve: MFunctionType = mFallback,
): string | null {
  if (task.outcomeCheck !== undefined) return outcomeCheckLabel(task.outcomeCheck, resolve);
  if (task.command !== undefined) return commandLabel(task.command, resolve);
  return null;
}

/** What the last read saw of each watched command task, by task id. */
export type CommandHealth = ReadonlyMap<
  string,
  { readonly streak: number; readonly lastSuccessfulRunId: string | null }
>;

/**
 * Command tasks that turned from passing to failing since `previous`, plus the health for the next
 * read. The first read only records a baseline. A task alerts when it was passing at the last read or
 * passed since, shown by a new `lastSuccessfulRunId`; repeat failures, running runs and edits stay quiet.
 */
export function commandTasksTurnedFailing(
  previous: CommandHealth | null,
  tasks: ReadonlyArray<ScheduledTask>,
): { readonly health: CommandHealth; readonly failing: ReadonlyArray<ScheduledTask> } {
  const health = new Map<
    string,
    { readonly streak: number; readonly lastSuccessfulRunId: string | null }
  >();
  const failing: ScheduledTask[] = [];
  for (const task of tasks) {
    if (task.command === undefined) continue;
    const { failureStreak: streak, lastSuccessfulRunId } = task.command;
    health.set(task.id, { streak, lastSuccessfulRunId });
    if (previous === null || streak === 0) continue;
    const before = previous.get(task.id);
    if (
      before === undefined ||
      before.streak === 0 ||
      before.lastSuccessfulRunId !== lastSuccessfulRunId
    )
      failing.push(task);
  }
  return { health, failing };
}
