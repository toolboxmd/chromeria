import { ScheduledTaskSchedule } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

/**
 * Rewrites stored `weekly` and `monthly` triggers as `fixed_time` (toolboxmd/chromeria#719).
 * Upstream's scheduler runs it before it recovers interrupted runs, so recovery never meets a
 * trigger it cannot decode. Only the schedule changes: the next run, run state and an accepted
 * automation wrapper with its certificate stay as stored, and a second run finds nothing.
 */

// Frozen copies of the retired triggers; contracts no longer have them.
const ClockTime = Schema.String.check(Schema.isPattern(/^([01]\d|2[0-3]):[0-5]\d$/));
const ClockTimes = Schema.Array(ClockTime).check(Schema.isMinLength(1), Schema.isMaxLength(24));
const LegacyWeekly = Schema.Struct({
  type: Schema.Literal("weekly"),
  weekdays: Schema.Array(Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 6 }))).check(
    Schema.isMinLength(1),
  ),
  times: ClockTimes,
  timeZone: Schema.String,
  windowMinutes: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 720 }))),
  chosen: Schema.optional(
    Schema.Array(
      Schema.Struct({
        requested: ClockTime,
        offsetMinutes: Schema.Int.check(Schema.isBetween({ minimum: -720, maximum: 720 })),
      }),
    ),
  ),
});
const LegacyMonthly = Schema.Struct({
  type: Schema.Literal("monthly"),
  day: Schema.Union([
    Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 31 })),
    Schema.Literal("last"),
  ]),
  times: ClockTimes,
  timeZone: Schema.String,
});
const decodeLegacy = Schema.decodeUnknownOption(Schema.Union([LegacyWeekly, LegacyMonthly]));
const decodeSchedule = Schema.decodeUnknownOption(ScheduledTaskSchedule);
const encodeSchedule = Schema.encodeSync(ScheduledTaskSchedule);

/** Weekly's window defaulted to 30 minutes; `fixed_time`'s defaults to none, so it is written out. */
const LEGACY_WEEKLY_WINDOW_MINUTES = 30;

/** The `fixed_time` that fires at the same instants as a legacy trigger, or null for anything else. */
export function fixedTimeFromLegacy(value: unknown): ScheduledTaskSchedule | null {
  const legacy = decodeLegacy(value);
  if (Option.isNone(legacy)) return null;
  const [timeOfDay, ...extraTimes] = [...new Set(legacy.value.times)];
  const schedule =
    legacy.value.type === "weekly"
      ? {
          type: "fixed_time",
          timeOfDay,
          ...(extraTimes.length === 0 ? {} : { extraTimes }),
          weekdays: legacy.value.weekdays,
          timeZone: legacy.value.timeZone,
          windowMinutes: legacy.value.windowMinutes ?? LEGACY_WEEKLY_WINDOW_MINUTES,
          ...(legacy.value.chosen === undefined ? {} : { chosen: legacy.value.chosen }),
        }
      : {
          type: "fixed_time",
          timeOfDay,
          ...(extraTimes.length === 0 ? {} : { extraTimes }),
          timeZone: legacy.value.timeZone,
          monthDays: [legacy.value.day === "last" ? 31 : legacy.value.day],
        };
  return Option.getOrNull(decodeSchedule(schedule));
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const parse = (json: string): unknown => {
  try {
    return JSON.parse(json);
  } catch {
    return undefined;
  }
};

/** The stored JSON with its legacy trigger rewritten, keeping any automation wrapper around it. */
const rewrite = (json: string): string | null | undefined => {
  const stored = parse(json);
  if (!isRecord(stored)) return undefined;
  const wrapped = stored.type === "destination-automation-v1";
  const inner = wrapped ? stored.schedule : stored;
  if (!isRecord(inner) || (inner.type !== "weekly" && inner.type !== "monthly")) return undefined;
  const schedule = fixedTimeFromLegacy(inner);
  if (schedule === null) return null;
  const encoded = encodeSchedule(schedule);
  return JSON.stringify(wrapped ? { ...stored, schedule: encoded } : encoded);
};

export const migrateLegacyCalendarSchedules = Effect.fn(
  "ScheduledTaskChecks.migrateLegacyCalendarSchedules",
)(function* () {
  const sql = yield* SqlClient.SqlClient;
  // A cheap prefilter; each candidate is parsed and checked before it changes.
  const rows = yield* sql<{ task_id: string; schedule_json: string }>`
    SELECT task_id, schedule_json FROM scheduled_tasks
    WHERE schedule_json LIKE '%"weekly"%' OR schedule_json LIKE '%"monthly"%'
  `;
  const rewrites = rows.flatMap((row) => {
    const next = rewrite(row.schedule_json);
    return next === undefined ? [] : [{ ...row, next }];
  });
  if (rewrites.length === 0) return;
  const migrated = yield* sql.withTransaction(
    Effect.forEach(rewrites, (row) =>
      row.next === null
        ? Effect.succeed(0)
        : sql`
            UPDATE scheduled_tasks SET schedule_json = ${row.next}
            WHERE task_id = ${row.task_id} AND schedule_json = ${row.schedule_json}
            RETURNING task_id
          `.pipe(Effect.map((updated) => updated.length)),
    ),
  );
  const skipped = rewrites.filter((row) => row.next === null).map((row) => row.task_id);
  if (skipped.length > 0)
    yield* Effect.logWarning("Left undecodable weekly or monthly schedule tasks unchanged", {
      taskIds: skipped,
    });
  yield* Effect.logInfo("Migrated weekly and monthly schedule tasks to fixed_time", {
    migrated: migrated.reduce((sum, count) => sum + count, 0),
  });
});
