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
export const AgentTaskDefinition = Schema.Struct({
  kind: Schema.optionalKey(Schema.Literal("agent")),
  title: TrimmedNonEmptyString,
  prompt: TrimmedNonEmptyString,
  target: TaskTarget,
  role: PrismRole,
  lane: Schema.optionalKey(PrismLane),
  schedule: TaskSchedule,
});
export type AgentTaskDefinition = typeof AgentTaskDefinition.Type;
/** The MCP command tool's input; its kind is implied. */
export const CreateScheduledCommand = Schema.Struct({
  title: TrimmedNonEmptyString,
  projectId: ProjectId,
  command: TrimmedNonEmptyString,
  schedule: TaskSchedule,
});
/**
 * A command task starts no agent and has no outcome check or retries. Each run pins this
 * definition, runs `command` once with /bin/sh in the project root, and records its
 * `commandResult`; exit code 0 is done, anything else needs you.
 */
export const CommandTaskDefinition = Schema.Struct({
  kind: Schema.Literal("command"),
  ...CreateScheduledCommand.fields,
});
export type CommandTaskDefinition = typeof CommandTaskDefinition.Type;
export const TaskDefinition = Schema.Union([AgentTaskDefinition, CommandTaskDefinition]);
export type TaskDefinition = typeof TaskDefinition.Type;
export const isCommandTask = (definition: TaskDefinition): definition is CommandTaskDefinition =>
  definition.kind === "command";
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
/** Every state event repeats the task, so only the newest command runs keep their output. */
export const COMMAND_OUTPUTS_KEPT = 3;
/** A finished command run. It started at its run's `dispatchedAt`, persisted before the spawn. */
export const CommandResult = Schema.Struct({
  /** Null when the process timed out, was killed by a signal or never started. */
  exitCode: Schema.NullOr(Schema.Int),
  /**
   * The last bytes of stdout and stderr together, in arrival order. Only the task's newest
   * runs keep it; older runs keep their exit code and times.
   */
  output: Schema.optionalKey(Schema.String),
  timedOut: Schema.Boolean,
  endedAt: IsoDateTime,
});
export type CommandResult = typeof CommandResult.Type;
export const TaskRun = Schema.Struct({
  owner: Schema.optionalKey(TrimmedNonEmptyString),
  definition: TaskDefinition,
  checkCwd: TrimmedNonEmptyString,
  id: TrimmedNonEmptyString,
  slot: IsoDateTime,
  /** The pinned outcome check of an agent run. Command runs have none. */
  checkVersion: Schema.optionalKey(PositiveInt),
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
  /**
   * Present once a command run's process ends. A command run that needs you without one was
   * interrupted, for example by a restart: its end, exit code and output are unknown.
   */
  commandResult: Schema.optionalKey(CommandResult),
  drafterIds: Schema.Array(ThreadId),
});
export type TaskRun = typeof TaskRun.Type;
/**
 * A settled run no longer holds its task. An agent run settles only when done; a command run that
 * needs you is settled too, because it never resumes: the next slot or Run now starts a new run.
 */
export const isSettledRun = (run: TaskRun) =>
  run.status === "done" || (run.status === "needs-you" && isCommandTask(run.definition));
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
  /**
   * Who created the task, derived by the server: `user:<subject>` or the creating agent's thread
   * id. Tasks created before it existed have their first check version's actor instead.
   */
  createdBy: Schema.optionalKey(TrimmedNonEmptyString),
  checkCwd: TrimmedNonEmptyString,
  id: TrimmedNonEmptyString,
  revision: PositiveInt,
  definition: TaskDefinition,
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
  paused: Schema.Boolean,
  deleted: Schema.Boolean,
  /** Agent tasks keep at least one version; command tasks have none. */
  checks: Schema.Array(TaskCheckVersion),
  choices: Schema.Array(TaskMinuteChoice),
  consumedSlot: Schema.NullOr(IsoDateTime),
  runs: Schema.Array(TaskRun),
  failureStreak: NonNegativeInt,
  lastError: Schema.NullOr(Schema.String),
});
export type ScheduledTask = typeof ScheduledTask.Type;
/**
 * The failed run that turned a healthy command task into a failing one, or null. Notify once
 * per run id: later failures extend the streak silently, and a passing run clears it.
 */
export const commandFailureToNotify = (task: ScheduledTask): TaskRun | null => {
  const run = task.runs.at(-1);
  return run !== undefined &&
    isCommandTask(task.definition) &&
    run.status === "needs-you" &&
    task.failureStreak === 1
    ? run
    : null;
};
/** A task as the management API returns it. `nextRunAt` is computed when read and never stored. */
export const ScheduledTaskView = Schema.Struct({
  ...ScheduledTask.fields,
  nextRunAt: Schema.NullOr(IsoDateTime),
});
export type ScheduledTaskView = typeof ScheduledTaskView.Type;
export const SchedulerStateCommand = Schema.Struct({
  type: Schema.Literal("scheduler.state.set"),
  commandId: CommandId,
  threadId: ThreadId,
  expectedRevision: NonNegativeInt,
  task: ScheduledTask,
  createdAt: IsoDateTime,
});
export const CreateAgentScheduledTask = Schema.Struct({
  ...AgentTaskDefinition.fields,
  checkCommand: TrimmedNonEmptyString,
  checkReason: TrimmedNonEmptyString,
});
export const CreateScheduledTask = Schema.Union([CreateAgentScheduledTask, CommandTaskDefinition]);
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
    /**
     * `compact` returns each task with only its active check and newest run, without command
     * output: enough to watch for failures cheaply. The default is the full view.
     */
    payload: Schema.Struct({ compact: Schema.optionalKey(Schema.Boolean) }),
    success: Schema.Array(ScheduledTaskView),
    error,
  }),
  Rpc.make("scheduler.create", { payload: CreateScheduledTask, success: ScheduledTaskView, error }),
  Rpc.make("scheduler.edit", { payload: EditScheduledTask, success: ScheduledTaskView, error }),
  Rpc.make("scheduler.pause", { payload: PauseScheduledTask, success: ScheduledTaskView, error }),
  Rpc.make("scheduler.delete", { payload: ScheduledTaskId, success: ScheduledTaskView, error }),
  Rpc.make("scheduler.runNow", { payload: ScheduledTaskId, success: ScheduledTaskView, error }),
);
export const SCHEDULE_WINDOW_DESCRIPTION =
  "Fixed times use a symmetric +/-30-minute window by default. Set windowMinutes: 0 for the exact minute. The server chooses the least busy minute and returns that minute and its neighbours.";
export const schedulerOwnsThread = (tasks: ReadonlyArray<ScheduledTask>, id: string) =>
  tasks.some(
    (task) =>
      !task.deleted && task.runs.some((run) => run.threadId === id && run.status !== "done"),
  );
