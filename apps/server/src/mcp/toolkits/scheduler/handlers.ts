import { threadOwner, SchedulerError } from "@t3tools/contracts";
import * as Option from "effect/Option";
import { ProjectionSnapshotQuery } from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as Effect from "effect/Effect";
import { Scheduler } from "../../../scheduler/Service.ts";
import { McpInvocationContext } from "../../McpInvocationContext.ts";
import { SchedulerToolkit } from "./tools.ts";
export const SchedulerToolkitHandlersLive = SchedulerToolkit.toLayer(
  Effect.gen(function* () {
    const scheduler = yield* Scheduler;
    const snapshots = yield* ProjectionSnapshotQuery;
    return SchedulerToolkit.of({
      create_scheduled_task: (input) =>
        Effect.gen(function* () {
          const caller = yield* McpInvocationContext;
          const thread = yield* snapshots
            .getThreadShellById(caller.threadId)
            .pipe(
              Effect.mapError(
                () =>
                  new SchedulerError({ detail: "Cannot resolve authenticated MCP caller owner." }),
              ),
            );
          if (Option.isNone(thread))
            return yield* new SchedulerError({
              detail: "Authenticated MCP caller thread is unavailable.",
            });
          return yield* scheduler.create(input, caller.threadId, threadOwner(thread.value));
        }),
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
