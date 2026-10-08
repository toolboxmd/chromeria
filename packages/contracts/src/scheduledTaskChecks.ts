import * as Schema from "effect/Schema";

import { IsoDateTime, NonNegativeInt, PositiveInt, TrimmedNonEmptyString } from "./baseSchemas.ts";

/**
 * Fork: scheduled task extensions (toolboxmd/chromeria#174). Upstream's
 * scheduled task contracts reach these through one spread per union or
 * struct; everything else about them lives here.
 *
 * - `once` and `weekly` are schedule triggers: upstream's poller fires them.
 * - An outcome check keeps an agent working in the same thread until its
 *   pinned check command passes.
 * - A command task runs one shell command per run instead of an agent.
 *
 * These fields are edited through the agent tools only; clients show them.
 */

// An offset is required so the instant never depends on the server's time zone.
const OnceAt = Schema.String.check(
  Schema.isPattern(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d{1,9})?)?(?:Z|[+-]\d{2}:\d{2})$/),
  Schema.makeFilter((value: string) => Number.isFinite(Date.parse(value)), {
    expected: "a valid date and time",
  }),
).annotate({
  description: "When to run, as an ISO timestamp with an offset, such as 2026-10-08T10:00:00Z.",
});

export const ScheduledTaskOnceSchedule = Schema.Struct({
  type: Schema.Literal("once").annotate({ description: "Run once at an absolute time." }),
  at: OnceAt,
}).annotate({
  description: "Run once at an absolute time, or as soon as the environment is back if it was off.",
});

const ClockTime = Schema.String.check(Schema.isPattern(/^([01]\d|2[0-3]):[0-5]\d$/)).annotate({
  description: "Wall-clock time in 24-hour HH:MM form, such as 09:30.",
});

const isTimeZone = (value: string) => {
  try {
    new Intl.DateTimeFormat("en-US", { timeZone: value });
    return true;
  } catch {
    return false;
  }
};

/** Minutes a weekly time may move to its least busy minute; 0 keeps the exact minute. */
export const DEFAULT_WEEKLY_WINDOW_MINUTES = 30;

export const ScheduledTaskWeeklySchedule = Schema.Struct({
  type: Schema.Literal("weekly").annotate({
    description: "Run at wall-clock times on selected weekdays in a time zone.",
  }),
  weekdays: Schema.Array(Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 6 })))
    .check(Schema.isMinLength(1))
    .annotate({ description: "Weekday numbers where 0 is Sunday and 6 is Saturday." }),
  times: Schema.Array(ClockTime).check(Schema.isMinLength(1), Schema.isMaxLength(24)),
  timeZone: TrimmedNonEmptyString.check(
    Schema.makeFilter(isTimeZone, { expected: "an IANA time zone such as Europe/Warsaw" }),
  ).annotate({ description: "IANA time zone the times are read in, such as Europe/Warsaw." }),
  windowMinutes: Schema.optional(
    Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 720 })),
  ).annotate({
    description:
      "Each time may move up to this many minutes either way to the least busy minute. Defaults to 30; 0 keeps the exact minute.",
  }),
  /** The server's pick for each time, recomputed when the schedule changes. */
  chosen: Schema.optional(
    Schema.Array(
      Schema.Struct({
        requested: ClockTime,
        offsetMinutes: Schema.Int.check(Schema.isBetween({ minimum: -720, maximum: 720 })),
      }),
    ),
  ),
}).annotate({
  description:
    "Run at each time on each selected weekday in the time zone. The server spreads times within the window and reports its pick in chosen.",
});

/** Fork triggers, spread into upstream's read and write schedule unions. */
export const ScheduledTaskForkSchedules = [
  ScheduledTaskOnceSchedule,
  ScheduledTaskWeeklySchedule,
] as const;
export type ScheduledTaskForkSchedule =
  | typeof ScheduledTaskOnceSchedule.Type
  | typeof ScheduledTaskWeeklySchedule.Type;

export const isForkScheduledTaskSchedule = <S extends { readonly type: string }>(
  schedule: S,
): schedule is Extract<S, { readonly type: ScheduledTaskForkSchedule["type"] }> =>
  schedule.type === "once" || schedule.type === "weekly";

export const ScheduledTaskRunStage = Schema.Literals([
  "running",
  "retry",
  "usage-limit",
  "needs-you",
  "done",
]);
export type ScheduledTaskRunStage = typeof ScheduledTaskRunStage.Type;

export const ScheduledTaskCheckVerdict = Schema.Struct({
  version: PositiveInt,
  passed: Schema.Boolean,
  checkedAt: IsoDateTime,
});
export type ScheduledTaskCheckVerdict = typeof ScheduledTaskCheckVerdict.Type;

export const ScheduledTaskOutcomeCheck = Schema.Struct({
  /** The active check version that new runs are pinned to. */
  version: PositiveInt,
  command: TrimmedNonEmptyString,
  role: Schema.NullOr(TrimmedNonEmptyString),
  lane: Schema.NullOr(TrimmedNonEmptyString),
  run: Schema.NullOr(
    Schema.Struct({
      id: TrimmedNonEmptyString,
      stage: ScheduledTaskRunStage,
      checkVersion: PositiveInt,
      attempt: NonNegativeInt,
      error: Schema.NullOr(Schema.String),
      /** Imported from Chromeria v1: kept as history and never resumed. */
      imported: Schema.Boolean,
      /** Run now continues this run in its thread instead of starting a new one. */
      resumable: Schema.Boolean,
    }),
  ),
  lastVerdict: Schema.NullOr(ScheduledTaskCheckVerdict),
});
export type ScheduledTaskOutcomeCheck = typeof ScheduledTaskOutcomeCheck.Type;

/** A command task: one shell command per run, no agent and no outcome check. */
export const ScheduledTaskCommand = Schema.Struct({
  command: TrimmedNonEmptyString,
  run: Schema.NullOr(
    Schema.Struct({
      id: TrimmedNonEmptyString,
      stage: Schema.Literals(["running", "done", "needs-you"]),
      /** Null when the process timed out, was stopped by a signal or never started. */
      exitCode: Schema.NullOr(Schema.Int),
      timedOut: Schema.Boolean,
      endedAt: Schema.NullOr(IsoDateTime),
      error: Schema.NullOr(Schema.String),
      /** The last bytes of output; only the newest runs keep it. */
      output: Schema.optional(Schema.String),
      imported: Schema.Boolean,
    }),
  ),
  /** Consecutive failed runs; a passing run resets it. */
  failureStreak: NonNegativeInt,
  /** The run that most recently passed. A pass between two reads changes it. */
  lastSuccessfulRunId: Schema.NullOr(TrimmedNonEmptyString),
});
export type ScheduledTaskCommand = typeof ScheduledTaskCommand.Type;

/** Read-model fields spread into upstream's `ScheduledTask`. */
export const ScheduledTaskForkFields = {
  /** Present on tasks with an outcome check. */
  outcomeCheck: Schema.optional(ScheduledTaskOutcomeCheck),
  /** Present on command tasks. */
  command: Schema.optional(ScheduledTaskCommand),
};

const CheckCommand = TrimmedNonEmptyString.check(Schema.isMaxLength(8_000)).annotate({
  description:
    "Shell command run in the task's workspace after each run; exit code 0 means the task is done. Until it passes, runs continue in the same thread. Creation is refused if it already passes.",
});
const CheckReason = TrimmedNonEmptyString.check(Schema.isMaxLength(2_000)).annotate({
  description:
    "Why passing this check proves the task is done. Required with checkCommand or revertCheckVersion.",
});
const Role = TrimmedNonEmptyString.check(Schema.isMaxLength(64)).annotate({
  description: "Prism role that picks the model for each run, such as planner or worker.",
});
const Lane = Schema.Literals(["easy", "medium", "hard"]).annotate({
  description: "Difficulty lane for the Prism role; defaults to medium.",
});
const ShellCommand = TrimmedNonEmptyString.check(Schema.isMaxLength(8_000)).annotate({
  description:
    "Run this shell command once per run with /bin/sh in the project root instead of starting an agent. Exit code 0 passes; anything else notifies. {date}, {run_id} and {task_id} are filled in. Cannot be combined with checkCommand or role.",
});

/** Optional fields on `schedule_task`. Webhook tasks cannot have them. */
export const ScheduledTaskOutcomeCheckCreateFields = {
  checkCommand: Schema.optional(CheckCommand),
  checkReason: Schema.optional(CheckReason),
  role: Schema.optional(Role),
  lane: Schema.optional(Lane),
  command: Schema.optional(ShellCommand),
};

/** Optional fields on `update_scheduled_task`. A task's kind never changes. */
export const ScheduledTaskOutcomeCheckUpdateFields = {
  ...ScheduledTaskOutcomeCheckCreateFields,
  revertCheckVersion: Schema.optional(
    PositiveInt.annotate({
      description: "Make an earlier check version active again, as a new version.",
    }),
  ),
};

const OutcomeCheckUpdate = Schema.Struct(ScheduledTaskOutcomeCheckUpdateFields);
export type ScheduledTaskOutcomeCheckUpdate = typeof OutcomeCheckUpdate.Type;
