import type {
  ScheduledTask,
  ScheduledTaskCommand,
  ScheduledTaskForkSchedule,
  ScheduledTaskOutcomeCheck,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";

/**
 * Fork (toolboxmd/chromeria#174): what web and mobile show for the fork's
 * scheduled task extensions. Upstream screens reach this through one call per
 * label; the fork triggers are read-only there and edited through agent tools.
 */

const WEEKDAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"] as const;

/**
 * A requested time with the server's move, as a signed offset: shifting the
 * clock instead could cross midnight and pair the time with the wrong day.
 */
const placedTime = (time: string, offsetMinutes: number) =>
  offsetMinutes === 0
    ? time
    : `${time} (${offsetMinutes > 0 ? "+" : "-"}${Math.abs(offsetMinutes)} min)`;

/** A one-shot's instant, or a weekly schedule's days, requested times with any move, and time zone. */
export function forkScheduleLabel(
  schedule: ScheduledTaskForkSchedule,
  options: { readonly locale?: string; readonly timeZone?: string } = {},
): string {
  if (schedule.type === "once") {
    const instant = DateTime.make(schedule.at);
    if (Option.isNone(instant)) return `Once at ${schedule.at}`;
    const formatted = new Intl.DateTimeFormat(options.locale, {
      dateStyle: "medium",
      timeStyle: "short",
      ...(options.timeZone === undefined ? {} : { timeZone: options.timeZone }),
    }).format(DateTime.toEpochMillis(instant.value));
    return `Once at ${formatted}`;
  }
  // Hermes lacks Array#toSorted; the spread is already a copy.
  const days = [...new Set(schedule.weekdays)]
    .sort((a, b) => ((a + 6) % 7) - ((b + 6) % 7))
    .map((day) => WEEKDAYS[day] ?? String(day))
    .join(", ");
  // The server may move each time within its window; show by how much.
  const times = [...new Set(schedule.times)]
    .map((time) =>
      placedTime(
        time,
        schedule.chosen?.find((choice) => choice.requested === time)?.offsetMinutes ?? 0,
      ),
    )
    .join(", ");
  return `${days} at ${times} (${schedule.timeZone})`;
}

const STAGE_LABELS: Record<NonNullable<ScheduledTaskOutcomeCheck["run"]>["stage"], string> = {
  running: "Working until its check passes",
  retry: "Retrying until its check passes",
  "usage-limit": "Waiting for a usage limit to reset",
  "needs-you": "Needs you",
  done: "Check passed",
};

export function outcomeCheckLabel(check: ScheduledTaskOutcomeCheck): string {
  const head = `Outcome check v${check.version}`;
  if (check.run === null) return `${head} · not run yet`;
  if (check.run.imported) return `${head} · imported from Chromeria v1, not resumed`;
  const stage = STAGE_LABELS[check.run.stage];
  const verdict =
    check.lastVerdict === null || check.run.stage === "done"
      ? ""
      : ` · last check ${check.lastVerdict.passed ? "passed" : "failed"}`;
  return `${head} · ${stage}${verdict}`;
}

export function commandLabel(command: ScheduledTaskCommand): string {
  const head = `Shell command: ${command.command}`;
  const run = command.run;
  if (run === null) return `${head} · not run yet`;
  if (run.imported) return `${head} · last run imported from Chromeria v1`;
  if (run.stage === "running") return `${head} · running`;
  if (run.stage === "done") return `${head} · passed`;
  return `${head} · ${run.timedOut ? "timed out" : run.exitCode === null ? "failed" : `exited with ${run.exitCode}`}`;
}

/** One line for a task's fork extension, or null for an upstream-only task. */
export function forkTaskSummary(
  task: Pick<ScheduledTask, "outcomeCheck" | "command">,
): string | null {
  if (task.outcomeCheck !== undefined) return outcomeCheckLabel(task.outcomeCheck);
  if (task.command !== undefined) return commandLabel(task.command);
  return null;
}

/** What the last read saw of each watched command task, by task id. */
export type CommandHealth = ReadonlyMap<
  string,
  { readonly streak: number; readonly lastSuccessfulRunId: string | null }
>;

/**
 * The command tasks that turned from passing to failing since `previous`, and
 * the health to compare the next read with (ported from Chromeria v1 #149).
 *
 * Without `previous` (the first read after mounting) it only records a
 * baseline, so opening the app never replays old failures. A task first seen
 * after that counts as passing, so its first-ever failure alerts. Reads are
 * samples, so the newest run does not decide: a task failing now alerts when
 * it was passing at the last read, or when a run passed since then, shown by
 * a new `lastSuccessfulRunId` even if the streak looks unchanged. Consecutive
 * failures, runs in progress and edits stay quiet.
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
