import * as Schema from "effect/Schema";

import { IsoDateTime, NonNegativeInt, PositiveInt, TrimmedNonEmptyString } from "./baseSchemas.ts";
import { PrismLane, PrismRole } from "./prism.ts";

/**
 * Fork scheduled task extensions, reached from upstream's contracts through one spread per union or
 * struct: the `once` trigger, `fixed_time` zones, extra times, month days and minute spreading,
 * outcome checks that keep an agent working until its check passes, and command tasks.
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

const isTimeZone = (value: string) => {
  try {
    // Intl throws a RangeError for a zone it does not know.
    return new Intl.DateTimeFormat("en-US", { timeZone: value }).resolvedOptions().timeZone !== "";
  } catch {
    return false;
  }
};

const TimeZone = TrimmedNonEmptyString.check(
  Schema.makeFilter(isTimeZone, { expected: "an IANA time zone such as Europe/Warsaw" }),
);

// Upstream's `fixed_time` pattern, so 9:05 and 09:05 mean the same minute.
const TimeOfDay = TrimmedNonEmptyString.check(Schema.isPattern(/^([01]?\d|2[0-3]):([0-5]\d)$/));

/**
 * Fork fields spread into upstream's `fixed_time`. A task with `timeZone` is fork-extended; the
 * server stores its own zone when another field needs one. Omitted fields keep their stored values
 * and `[]` clears; upstream's open month-day PR differs there (see the scheduled-tasks registry).
 */
export const ScheduledTaskFixedTimeForkFields = {
  timeZone: Schema.optional(TimeZone).annotateKey({
    description: "IANA zone for the times, such as Europe/Warsaw. Default: the server's.",
  }),
  extraTimes: Schema.optional(Schema.Array(TimeOfDay).check(Schema.isMaxLength(23))).annotateKey({
    description: "More HH:MM times each day.",
  }),
  monthDays: Schema.optional(
    Schema.Array(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 31 }))).check(
      Schema.isMaxLength(31),
    ),
  ).annotateKey({
    description:
      "Days of the month instead of weekdays; 31 is the last day. Short months run on their last day.",
  }),
  windowMinutes: Schema.optional(
    Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 720 })),
  ).annotateKey({
    description: "Move each time up to this many minutes to the least busy minute. Default 0.",
  }),
  /** The server's pick for each requested time; input is ignored. */
  chosen: Schema.optional(
    Schema.Array(
      Schema.Struct({
        requested: TimeOfDay,
        offsetMinutes: Schema.Int.check(Schema.isBetween({ minimum: -720, maximum: 720 })),
      }),
    ),
  ).annotateKey({ description: "Set by the server." }),
};

const ForkFixedTimeFields = Schema.Struct(ScheduledTaskFixedTimeForkFields);

/** All seven weekdays, none, or no list at all mean every day. */
export const isEveryWeekday = (weekdays: ReadonlyArray<number> | undefined) => {
  const unique = new Set(weekdays ?? []);
  return unique.size === 0 || unique.size === 7;
};

export const FIXED_TIME_MONTH_DAYS_WITH_WEEKDAYS =
  "monthDays cannot be combined with a subset of weekdays; omit weekdays, or send monthDays: [] to run on weekdays.";

/** Checked on upstream's `fixed_time`: month days and a weekday subset cannot both pick the days. */
export const fixedTimeForkCheck = Schema.makeFilter(
  (schedule: {
    readonly weekdays?: ReadonlyArray<number> | undefined;
    readonly monthDays?: ReadonlyArray<number> | undefined;
  }) =>
    (schedule.monthDays ?? []).length === 0 ||
    isEveryWeekday(schedule.weekdays) ||
    FIXED_TIME_MONTH_DAYS_WITH_WEEKDAYS,
);

/** A stored fork-extended `fixed_time`: it always carries its zone. */
export type ScheduledTaskFixedTimeForkSchedule = {
  readonly type: "fixed_time";
  readonly timeOfDay: string;
  readonly weekdays?: ReadonlyArray<number> | undefined;
} & Omit<typeof ForkFixedTimeFields.Type, "timeZone"> & { readonly timeZone: string };

/** Fork triggers, spread into upstream's read and write schedule unions. */
export const ScheduledTaskForkSchedules = [ScheduledTaskOnceSchedule] as const;
export type ScheduledTaskForkSchedule =
  | typeof ScheduledTaskOnceSchedule.Type
  | ScheduledTaskFixedTimeForkSchedule;

/** Fork triggers and fork-extended `fixed_time`; the rest is upstream's own. */
export const isForkScheduledTaskSchedule = <S extends { readonly type: string }>(
  schedule: S,
): schedule is S & ScheduledTaskForkSchedule =>
  (schedule.type === "once" && "at" in schedule) ||
  (schedule.type === "fixed_time" &&
    "timeZone" in schedule &&
    typeof schedule.timeZone === "string");

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
  role: Schema.NullOr(PrismRole),
  lane: Schema.NullOr(PrismLane),
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

// TrimmedNonEmptyString decodes, so a description on it never reaches the tool's JSON schema.
const CheckCommand = TrimmedNonEmptyString.check(Schema.isMaxLength(8_000));
const CHECK_COMMAND =
  "Shell command that judges a run after it ends, never a gate before it: exit code 0 in the task's workspace means done. On failure the same thread continues, up to 5 times 30s to 1h apart, then the run needs the user. Scheduled fires skip until it finishes. The judged thread cannot change or delete its task, so test external state, like a host being up, in the prompt. Creation is refused if it already passes.";
const CheckReason = TrimmedNonEmptyString.check(Schema.isMaxLength(2_000));
const CHECK_REASON =
  "Why passing this check proves the task is done. Required with checkCommand or revertCheckVersion.";
const Role = PrismRole.annotate({
  description:
    "Prism role whose kit and model list start each run, such as planner or worker. A role that Prism cannot place makes the run retry; it never falls back to another model.",
});
const Lane = PrismLane.annotate({
  description:
    "Lane for the worker role: easy, medium (default) or hard for difficulty, or ui for UI and frontend work, which runs only on the ui lane's Colors and so needs bindToCurrentThread false.",
});
const ShellCommand = TrimmedNonEmptyString.check(Schema.isMaxLength(8_000));
const SHELL_COMMAND =
  "Run this shell command once per run with /bin/sh in the project root instead of starting an agent. Exit code 0 passes; anything else notifies. {date}, {run_id} and {task_id} are filled in. Cannot be combined with checkCommand or role.";

/** Optional fields on `schedule_task`. Webhook tasks cannot have them. */
export const ScheduledTaskOutcomeCheckCreateFields = {
  checkCommand: Schema.optional(CheckCommand).annotateKey({ description: CHECK_COMMAND }),
  checkReason: Schema.optional(CheckReason).annotateKey({ description: CHECK_REASON }),
  role: Schema.optional(Role),
  lane: Schema.optional(Lane),
  command: Schema.optional(ShellCommand).annotateKey({ description: SHELL_COMMAND }),
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
