import {
  CreateScheduledTask,
  EditScheduledTask,
  PauseScheduledTask,
  ScheduledTask,
  ScheduledTaskId,
  SchedulerError,
  SCHEDULE_WINDOW_DESCRIPTION,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as Tool from "effect/unstable/ai/Tool";
import * as Toolkit from "effect/unstable/ai/Toolkit";
import { McpInvocationContext } from "../../McpInvocationContext.ts";
const dependencies = [McpInvocationContext];
export const SchedulerToolkit = Toolkit.make(
  Tool.make("create_scheduled_task", {
    description: `Create check-gated scheduled work; creation is refused if the required server command already passes. Checks run in the target project directory or target thread worktree, pinned per run. ${SCHEDULE_WINDOW_DESCRIPTION}`,
    parameters: CreateScheduledTask,
    success: ScheduledTask,
    failure: SchedulerError,
    dependencies,
  }),
  Tool.make("list_scheduled_tasks", {
    description:
      "List scheduled tasks with bounded recent runs, immutable check versions, authors, failure streak and error.",
    parameters: Schema.Struct({}),
    success: Schema.Array(ScheduledTask),
    failure: SchedulerError,
    dependencies,
  }),
  Tool.make("edit_scheduled_task", {
    description: `Edit future runs or revert an outcome check with a reason. Active runs keep their pinned check; a judged thread cannot edit its own task. ${SCHEDULE_WINDOW_DESCRIPTION}`,
    parameters: EditScheduledTask,
    success: ScheduledTask,
    failure: SchedulerError,
    dependencies,
  }),
  Tool.make("pause_scheduled_task", {
    description:
      "Pause or resume a scheduled task. Pausing prevents automatic work while retaining unfinished runs.",
    parameters: PauseScheduledTask,
    success: ScheduledTask,
    failure: SchedulerError,
    dependencies,
  }),
  Tool.make("delete_scheduled_task", {
    description:
      "Delete a settled task, retaining its immutable audit history. Unfinished work must settle first.",
    parameters: ScheduledTaskId,
    success: ScheduledTask,
    failure: SchedulerError,
    dependencies,
  }),
  Tool.make("run_scheduled_task_now", {
    description:
      "Claim an immediate run. Refused while paused or while this task has unfinished work; other tasks run in parallel.",
    parameters: ScheduledTaskId,
    success: ScheduledTask,
    failure: SchedulerError,
    dependencies,
  }),
);
