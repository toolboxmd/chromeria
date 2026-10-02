import {
  CreateAgentScheduledTask,
  CreateScheduledCommand,
  EditScheduledTask,
  PauseScheduledTask,
  ScheduledTaskView,
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
    description: `Create check-gated scheduled agent work; creation is refused if the required server command already passes. Checks run in the target project directory or target thread worktree, pinned per run. ${SCHEDULE_WINDOW_DESCRIPTION}`,
    parameters: CreateAgentScheduledTask,
    success: ScheduledTaskView,
    failure: SchedulerError,
    dependencies,
  }),
  Tool.make("create_scheduled_command", {
    description: `Schedule a shell command that starts no agent: no thread, outcome check or retries. Each run executes the command once with /bin/sh in the project's root folder; exit code 0 is done, any other exit or a 30-minute timeout marks the run Needs you with the tail of its output. {date}, {run_id} and {task_id} are filled in. One run at a time, and only while this Chromeria server runs; after downtime the latest missed time runs once. A run interrupted by a server stop is never repeated: it needs you instead. ${SCHEDULE_WINDOW_DESCRIPTION}`,
    parameters: CreateScheduledCommand,
    success: ScheduledTaskView,
    failure: SchedulerError,
    dependencies,
  }),
  Tool.make("list_scheduled_tasks", {
    description:
      "List scheduled agent and command tasks with their next run time, bounded recent runs and command results, immutable check versions, authors, failure streak and error.",
    // An empty Struct emits a non-object JSON Schema in this Effect version. MCP requires an object root.
    parameters: Schema.Record(Schema.String, Schema.Never),
    success: Schema.Array(ScheduledTaskView),
    failure: SchedulerError,
    dependencies,
  }),
  Tool.make("edit_scheduled_task", {
    description: `Edit future runs or revert an outcome check with a reason. Active runs keep their pinned definition and check; a judged thread cannot edit its own task. A command task changes its command through the definition and has no check; a task's kind never changes. ${SCHEDULE_WINDOW_DESCRIPTION}`,
    parameters: EditScheduledTask,
    success: ScheduledTaskView,
    failure: SchedulerError,
    dependencies,
  }),
  Tool.make("pause_scheduled_task", {
    description:
      "Pause or resume a scheduled task. Pausing prevents automatic work while retaining unfinished runs.",
    parameters: PauseScheduledTask,
    success: ScheduledTaskView,
    failure: SchedulerError,
    dependencies,
  }),
  Tool.make("delete_scheduled_task", {
    description:
      "Delete a settled task or tombstone quiescent needs-you work, retaining its audit and unfinished outcome. Active work, a running command, pending children, reports, questions or queued turns refuse deletion.",
    parameters: ScheduledTaskId,
    success: ScheduledTaskView,
    failure: SchedulerError,
    dependencies,
  }),
  Tool.make("run_scheduled_task_now", {
    description:
      "Claim an immediate run, or resume the same pinned needs-you agent run and thread. A command task starts a new run of its command. Refused while paused or during other unfinished work. A retired thread must be explicitly recovered first; other tasks run in parallel.",
    parameters: ScheduledTaskId,
    success: ScheduledTaskView,
    failure: SchedulerError,
    dependencies,
  }),
);
