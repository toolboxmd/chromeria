import * as Schema from "effect/Schema";
import * as Rpc from "effect/rpc/Rpc";
import * as RpcGroup from "effect/rpc/RpcGroup";

import { EnvironmentAuthorizationError } from "./auth.ts";
import { IsoDateTime, ScheduledTaskId, ThreadId, TrimmedNonEmptyString } from "./baseSchemas.ts";
import { ScheduledTaskError } from "./scheduledTask.ts";

/**
 * A scheduled task's kept runs (toolboxmd/chromeria#951), read on demand: the live task list
 * every client holds open carries none of them.
 */
export const SCHEDULED_TASK_RUNS_WS_METHODS = {
  list: "scheduledTasks.listRuns",
} as const;

/** The newest runs one read returns. */
export const SCHEDULED_TASK_RUNS_LIMIT = 20;

export const ScheduledTaskRunRecord = Schema.Struct({
  id: TrimmedNonEmptyString,
  startedAt: IsoDateTime,
  /** Null while it runs, and when no end was recorded. */
  endedAt: Schema.NullOr(IsoDateTime),
  status: Schema.Literals(["running", "succeeded", "failed"]),
  /**
   * The run's thread as the server read it; clients prefer their live copy of the thread.
   * `missing` is a deleted thread. Null for a shell command run, which has no thread.
   */
  thread: Schema.NullOr(
    Schema.Struct({
      id: ThreadId,
      state: Schema.Literals(["open", "settled", "missing"]),
    }),
  ),
  /** One line: the run's last agent message, or what a command run reported. */
  summary: Schema.NullOr(Schema.String),
});
export type ScheduledTaskRunRecord = typeof ScheduledTaskRunRecord.Type;

export const ScheduledTaskListRunsInput = Schema.Struct({ id: ScheduledTaskId });
export type ScheduledTaskListRunsInput = typeof ScheduledTaskListRunsInput.Type;

export const ScheduledTaskListRunsResult = Schema.Struct({
  /** Newest first, at most `SCHEDULED_TASK_RUNS_LIMIT`. */
  runs: Schema.Array(ScheduledTaskRunRecord),
});
export type ScheduledTaskListRunsResult = typeof ScheduledTaskListRunsResult.Type;

export const ScheduledTaskRunsRpcGroup = RpcGroup.make(
  Rpc.make(SCHEDULED_TASK_RUNS_WS_METHODS.list, {
    payload: ScheduledTaskListRunsInput,
    success: ScheduledTaskListRunsResult,
    error: Schema.Union([ScheduledTaskError, EnvironmentAuthorizationError]),
  }),
);
