import * as Schema from "effect/Schema";
import * as Rpc from "effect/unstable/rpc/Rpc";
import * as RpcGroup from "effect/unstable/rpc/RpcGroup";
import {
  CommandId,
  IsoDateTime,
  NonNegativeInt,
  PositiveInt,
  ProjectId,
  ThreadId,
  TrimmedNonEmptyString,
} from "./baseSchemas.ts";
import { PrismLane, PrismRole } from "./prism.ts";
import { EnvironmentAuthorizationError } from "./auth.ts";

const minuteWindow = Schema.Number.check(
  Schema.isInt(),
  Schema.isBetween({ minimum: 0, maximum: 720 }),
);
const clockTime = Schema.String.check(Schema.isPattern(/^([01]\d|2[0-3]):[0-5]\d$/));
export const TaskSchedule = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("interval"), minutes: PositiveInt }),
  Schema.Struct({
    kind: Schema.Literal("once"),
    at: IsoDateTime,
    windowMinutes: Schema.optionalKey(minuteWindow),
  }),
  Schema.Struct({
    kind: Schema.Literal("weekly"),
    weekdays: Schema.Array(
      Schema.Number.check(Schema.isInt(), Schema.isBetween({ minimum: 0, maximum: 6 })),
    ).check(Schema.isMinLength(1)),
    times: Schema.Array(clockTime).check(Schema.isMinLength(1)),
    timeZone: TrimmedNonEmptyString,
    windowMinutes: Schema.optionalKey(minuteWindow),
  }),
]);
export type TaskSchedule = typeof TaskSchedule.Type;
export const TaskTarget = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("new-thread"), projectId: ProjectId }),
  Schema.Struct({ kind: Schema.Literal("thread"), threadId: ThreadId }),
]);
export const TaskDefinition = Schema.Struct({
  title: TrimmedNonEmptyString,
  prompt: TrimmedNonEmptyString,
  target: TaskTarget,
  role: PrismRole,
  lane: Schema.optionalKey(PrismLane),
  schedule: TaskSchedule,
});
export type TaskDefinition = typeof TaskDefinition.Type;
export const TaskCheckVersion = Schema.Struct({
  version: PositiveInt,
  command: TrimmedNonEmptyString,
  actor: TrimmedNonEmptyString,
  reason: TrimmedNonEmptyString,
  createdAt: IsoDateTime,
  revertedFrom: Schema.NullOr(PositiveInt),
});
export type TaskCheckVersion = typeof TaskCheckVersion.Type;
export const TaskCheckResult = Schema.Struct({
  version: PositiveInt,
  passed: Schema.Boolean,
  output: Schema.String,
  checkedAt: IsoDateTime,
});
export type TaskCheckResult = typeof TaskCheckResult.Type;
export const TaskRun = Schema.Struct({
  owner: Schema.optionalKey(TrimmedNonEmptyString),
  definition: TaskDefinition,
  checkCwd: TrimmedNonEmptyString,
  id: TrimmedNonEmptyString,
  slot: IsoDateTime,
  checkVersion: PositiveInt,
  threadId: Schema.NullOr(ThreadId),
  status: Schema.Literals(["claimed", "running", "retry", "usage-limit", "done", "needs-you"]),
  processId: Schema.String,
  originSequence: NonNegativeInt,
  sendIndex: NonNegativeInt,
  attempt: NonNegativeInt,
  hasWork: Schema.Boolean,
  leaseUntil: Schema.Number,
  retryAt: Schema.NullOr(Schema.Number),
  dispatchedAt: Schema.NullOr(IsoDateTime),
  observedTurnId: Schema.NullOr(Schema.String),
  error: Schema.NullOr(Schema.String),
  check: Schema.NullOr(TaskCheckResult),
  drafterIds: Schema.Array(ThreadId),
});
export type TaskRun = typeof TaskRun.Type;
export const TaskMinuteChoice = Schema.Struct({
  requested: Schema.String,
  offsetMinutes: Schema.Number,
  chosen: Schema.String,
  neighbours: Schema.Array(
    Schema.Struct({ taskId: Schema.String, title: Schema.String, chosen: Schema.String }),
  ),
});
export type TaskMinuteChoice = typeof TaskMinuteChoice.Type;
export const ScheduledTask = Schema.Struct({
  owner: Schema.optionalKey(TrimmedNonEmptyString),
  checkCwd: TrimmedNonEmptyString,
  id: TrimmedNonEmptyString,
  revision: PositiveInt,
  definition: TaskDefinition,
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
  paused: Schema.Boolean,
  deleted: Schema.Boolean,
  checks: Schema.Array(TaskCheckVersion).check(Schema.isMinLength(1)),
  choices: Schema.Array(TaskMinuteChoice),
  consumedSlot: Schema.NullOr(IsoDateTime),
  runs: Schema.Array(TaskRun),
  failureStreak: NonNegativeInt,
  lastError: Schema.NullOr(Schema.String),
});
export type ScheduledTask = typeof ScheduledTask.Type;
export const SchedulerStateCommand = Schema.Struct({
  type: Schema.Literal("scheduler.state.set"),
  commandId: CommandId,
  threadId: ThreadId,
  expectedRevision: NonNegativeInt,
  task: ScheduledTask,
  createdAt: IsoDateTime,
});
export const CreateScheduledTask = Schema.Struct({
  ...TaskDefinition.fields,
  checkCommand: TrimmedNonEmptyString,
  checkReason: TrimmedNonEmptyString,
});
export type CreateScheduledTask = typeof CreateScheduledTask.Type;
export const EditScheduledTask = Schema.Struct({
  taskId: TrimmedNonEmptyString,
  definition: Schema.optionalKey(TaskDefinition),
  checkCommand: Schema.optionalKey(TrimmedNonEmptyString),
  checkReason: Schema.optionalKey(TrimmedNonEmptyString),
  revertVersion: Schema.optionalKey(PositiveInt),
});
export type EditScheduledTask = typeof EditScheduledTask.Type;
export const ScheduledTaskId = Schema.Struct({ taskId: TrimmedNonEmptyString });
export const PauseScheduledTask = Schema.Struct({
  ...ScheduledTaskId.fields,
  paused: Schema.Boolean,
});
export class SchedulerError extends Schema.TaggedError<SchedulerError>()("SchedulerError", {
  detail: Schema.String,
}) {
  override get message() {
    return this.detail;
  }
}
const error = Schema.Union([SchedulerError, EnvironmentAuthorizationError]);
export const CheckHistoryInput = Schema.Struct({
  taskId: TrimmedNonEmptyString,
  beforeVersion: Schema.optionalKey(PositiveInt),
  limit: Schema.optionalKey(
    Schema.Number.check(Schema.isInt(), Schema.isBetween({ minimum: 1, maximum: 50 })),
  ),
});
export type CheckHistoryInput = typeof CheckHistoryInput.Type;
export const SchedulerRpcGroup = RpcGroup.make(
  Rpc.make("scheduler.checkHistory", {
    payload: CheckHistoryInput,
    success: Schema.Array(TaskCheckVersion),
    error,
  }),
  Rpc.make("scheduler.list", {
    payload: Schema.Struct({}),
    success: Schema.Array(ScheduledTask),
    error,
  }),
  Rpc.make("scheduler.create", { payload: CreateScheduledTask, success: ScheduledTask, error }),
  Rpc.make("scheduler.edit", { payload: EditScheduledTask, success: ScheduledTask, error }),
  Rpc.make("scheduler.pause", { payload: PauseScheduledTask, success: ScheduledTask, error }),
  Rpc.make("scheduler.delete", { payload: ScheduledTaskId, success: ScheduledTask, error }),
  Rpc.make("scheduler.runNow", { payload: ScheduledTaskId, success: ScheduledTask, error }),
);
export const SCHEDULE_WINDOW_DESCRIPTION =
  "Fixed times use a symmetric +/-30-minute window by default. Set windowMinutes: 0 for the exact minute. The server chooses the least busy minute and returns that minute and its neighbours.";
export const schedulerOwnsThread = (tasks: ReadonlyArray<ScheduledTask>, id: string) =>
  tasks.some(
    (task) =>
      !task.deleted && task.runs.some((run) => run.threadId === id && run.status !== "done"),
  );
