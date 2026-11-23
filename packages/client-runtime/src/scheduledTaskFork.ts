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

/**
 * A one-shot's instant, or an extended `fixed_time`'s days, requested times with any move, and time zone.
 * `formatTime` renders each `H:MM` wall time, so a client can apply its clock setting.
 */
export function forkScheduleLabel(
  schedule: ScheduledTaskForkSchedule,
  options: {
    readonly locale?: string;
    readonly timeZone?: string;
    readonly formatTime?: (timeOfDay: string) => string;
  } = {},
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
        options.formatTime?.(time) ?? time,
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

/** One environment's task in the merged list; clients extend it with what they need to act on it. */
export interface ScheduledTaskListEntry {
  readonly environmentId: string;
  readonly task: ScheduledTask;
}

export const scheduledTaskEntryKey = (entry: ScheduledTaskListEntry) =>
  `${entry.environmentId}\n${entry.task.id}`;

/** A one-time task's decoded slot; upstream and fork one-shots name it differently. */
function oneTimeSlot(schedule: ScheduledTask["schedule"]): number | null {
  if (schedule.type !== "once") return null;
  const at = Date.parse("runAt" in schedule ? schedule.runAt : schedule.at);
  return Number.isFinite(at) ? at : null;
}

/**
 * In History: a one-shot, paused or not, that ran at or after its slot and is not running, or a
 * v1 import with its run whose slot passed and that is not scheduled again. A manual early run
 * leaves the slot due.
 */
export function isScheduledTaskHistory(task: ScheduledTask, now: number): boolean {
  const slot = oneTimeSlot(task.schedule);
  if (slot === null) return false;
  const imported = (task.outcomeCheck?.run ?? task.command?.run)?.imported === true;
  if (imported && task.nextRunAt === null && slot <= now) return true;
  if (task.lastRunAt === null) return false;
  return task.lastRunStatus !== "running" && Date.parse(task.lastRunAt) >= slot;
}

const recurringRank = (task: ScheduledTask) => {
  if (!task.enabled || task.blockedByPolicy !== undefined) return 2;
  if (task.nextRunAt !== null) return 0;
  return task.schedule.type === "webhook" ? 1 : 2;
};

const byTitle = (a: ScheduledTaskListEntry, b: ScheduledTaskListEntry) =>
  a.task.title.localeCompare(b.task.title) ||
  scheduledTaskEntryKey(a).localeCompare(scheduledTaskEntryKey(b));

const timeOf = (value: string | null, missing: number) =>
  value === null ? missing : Date.parse(value);

export interface ScheduledTaskSections<E extends ScheduledTaskListEntry> {
  /** One-time tasks still due, soonest first. */
  readonly upcoming: ReadonlyArray<E>;
  /** Active by next run, then listening webhooks, then paused, blocked or unscheduled. */
  readonly recurring: ReadonlyArray<E>;
  /** One-time tasks that already ran, latest first. */
  readonly history: ReadonlyArray<E>;
  /** Tasks whose last run failed, before `failingOnly` narrows the sections. */
  readonly failing: number;
  readonly historyFailing: number;
}

/** Every environment's tasks as one list, ordered by what runs next. */
export function scheduledTaskSections<E extends ScheduledTaskListEntry>(
  entries: ReadonlyArray<E>,
  options: { readonly now: number; readonly failingOnly?: boolean },
): ScheduledTaskSections<E> {
  const upcoming: E[] = [];
  const recurring: E[] = [];
  const history: E[] = [];
  let failing = 0;
  let historyFailing = 0;
  for (const entry of entries) {
    const failed = entry.task.lastRunStatus === "failed";
    if (failed) failing += 1;
    const spent = isScheduledTaskHistory(entry.task, options.now);
    if (spent && failed) historyFailing += 1;
    if (options.failingOnly && !failed) continue;
    if (spent) history.push(entry);
    else if (entry.task.schedule.type === "once") upcoming.push(entry);
    else recurring.push(entry);
  }
  upcoming.sort(
    (a, b) =>
      (oneTimeSlot(a.task.schedule) ?? Infinity) - (oneTimeSlot(b.task.schedule) ?? Infinity) ||
      byTitle(a, b),
  );
  recurring.sort(
    (a, b) =>
      recurringRank(a.task) - recurringRank(b.task) ||
      timeOf(a.task.nextRunAt, Infinity) - timeOf(b.task.nextRunAt, Infinity) ||
      byTitle(a, b),
  );
  // Imported v1 runs leave lastRunAt empty; their command end or check verdict still dates them.
  const ranAt = (task: ScheduledTask) =>
    timeOf(
      task.lastRunAt ??
        task.command?.run?.endedAt ??
        task.outcomeCheck?.lastVerdict?.checkedAt ??
        null,
      -Infinity,
    );
  history.sort((a, b) => ranAt(b.task) - ranAt(a.task) || byTitle(a, b));
  return { upcoming, recurring, history, failing, historyFailing };
}

const HOUR_MS = 3_600_000;
const DAY_MS = 24 * HOUR_MS;

/** Most frequent first; the list shows active tasks in this order (toolboxmd/chromeria#920). */
const SCHEDULED_TASK_FREQUENCIES = [
  "minutes",
  "hourly",
  "daily",
  "weekly",
  "monthly",
  "once",
  "webhook",
] as const;
export type ScheduledTaskFrequency = (typeof SCHEDULED_TASK_FREQUENCIES)[number];

/**
 * How often a schedule runs. Several set times a day count as hourly; an interval from two days up
 * to a week counts as weekly, as chosen weekdays do.
 */
export function scheduledTaskFrequency(
  schedule: ScheduledTask["schedule"],
): ScheduledTaskFrequency {
  switch (schedule.type) {
    case "interval":
      if (schedule.everyMs < HOUR_MS) return "minutes";
      if (schedule.everyMs < DAY_MS) return "hourly";
      if (schedule.everyMs < 2 * DAY_MS) return "daily";
      return schedule.everyMs <= 7 * DAY_MS ? "weekly" : "monthly";
    case "fixed_time":
      if ((schedule.extraTimes ?? []).length > 0) return "hourly";
      if ((schedule.monthDays ?? []).length > 0) return "monthly";
      return isEveryWeekday(schedule.weekdays) ? "daily" : "weekly";
    case "once":
      return "once";
    case "webhook":
      return "webhook";
  }
}

export interface ScheduledTaskFrequencyGroups<E extends ScheduledTaskListEntry> {
  /** Active tasks by how often they run, each group by next run; empty groups are left out. */
  readonly groups: ReadonlyArray<{
    readonly frequency: ScheduledTaskFrequency;
    readonly entries: ReadonlyArray<E>;
  }>;
  /** Paused, blocked or unscheduled tasks, in their section order. */
  readonly paused: ReadonlyArray<E>;
}

/** Groups the not-yet-spent tasks of `scheduledTaskSections` by frequency, keeping paused ones apart. */
export function scheduledTaskFrequencyGroups<E extends ScheduledTaskListEntry>(
  sections: Pick<ScheduledTaskSections<E>, "upcoming" | "recurring">,
): ScheduledTaskFrequencyGroups<E> {
  const byFrequency = new Map<ScheduledTaskFrequency, E[]>();
  const paused: E[] = [];
  for (const entry of [...sections.upcoming, ...sections.recurring]) {
    const { task } = entry;
    // A due one-time task without a next run still waits for its slot.
    if (
      !task.enabled ||
      task.blockedByPolicy !== undefined ||
      (task.schedule.type !== "once" && recurringRank(task) === 2)
    ) {
      paused.push(entry);
      continue;
    }
    const frequency = scheduledTaskFrequency(task.schedule);
    const group = byFrequency.get(frequency) ?? [];
    group.push(entry);
    byFrequency.set(frequency, group);
  }
  const nextRun = (entry: E) =>
    timeOf(entry.task.nextRunAt, oneTimeSlot(entry.task.schedule) ?? Infinity);
  return {
    groups: SCHEDULED_TASK_FREQUENCIES.flatMap((frequency) => {
      const entries = byFrequency.get(frequency);
      if (!entries) return [];
      entries.sort((a, b) => nextRun(a) - nextRun(b) || byTitle(a, b));
      return [{ frequency, entries }];
    }),
    paused,
  };
}

/**
 * One label per machine for the merged list, which has no per-machine headings. A label two
 * machines share gets each machine's `detail`, such as its address; if that still collides,
 * the environment id.
 */
export function uniqueMachineLabels(
  machines: ReadonlyArray<{
    readonly environmentId: string;
    readonly label: string;
    readonly detail: () => string;
  }>,
): ReadonlyMap<string, string> {
  const shared = (labels: ReadonlyArray<string>) => {
    const counts = new Map<string, number>();
    for (const label of labels) counts.set(label, (counts.get(label) ?? 0) + 1);
    return (label: string) => (counts.get(label) ?? 0) > 1;
  };
  const isShared = shared(machines.map((machine) => machine.label));
  const detailed = machines.map((machine) =>
    isShared(machine.label) ? `${machine.label} · ${machine.detail()}` : machine.label,
  );
  const stillShared = shared(detailed);
  return new Map(
    machines.map((machine, index) => {
      const label = detailed[index]!;
      return [
        machine.environmentId,
        stillShared(label) ? `${label} · ${machine.environmentId}` : label,
      ];
    }),
  );
}
