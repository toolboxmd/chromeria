import * as Effect from "effect/Effect";
import { Scheduler } from "../../../scheduler/Service.ts";
import { McpInvocationContext } from "../../McpInvocationContext.ts";
import { SchedulerToolkit } from "./tools.ts";
export const SchedulerToolkitHandlersLive = SchedulerToolkit.toLayer(
  Effect.gen(function* () {
    const scheduler = yield* Scheduler;
    return SchedulerToolkit.of({
      create_scheduled_task: (input) =>
        Effect.flatMap(McpInvocationContext, (caller) => scheduler.create(input, caller.threadId)),
      list_scheduled_tasks: () => scheduler.list,
      edit_scheduled_task: (input) =>
        Effect.flatMap(McpInvocationContext, (caller) => scheduler.edit(input, caller.threadId)),
      pause_scheduled_task: (input) =>
        Effect.flatMap(McpInvocationContext, (caller) =>
          scheduler.pause(input.taskId, input.paused, caller.threadId),
        ),
      delete_scheduled_task: (input) =>
        Effect.flatMap(McpInvocationContext, (caller) =>
          scheduler.delete(input.taskId, caller.threadId),
        ),
      run_scheduled_task_now: (input) => scheduler.runNow(input.taskId),
    });
  }),
);
