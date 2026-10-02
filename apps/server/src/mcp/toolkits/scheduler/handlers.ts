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
    // The creator comes from the authenticated caller thread, never from the payload.
    const creator = Effect.gen(function* () {
      const context = yield* McpInvocationContext;
      const thread = yield* snapshots
        .getThreadShellById(context.threadId)
        .pipe(
          Effect.mapError(
            () => new SchedulerError({ detail: "Cannot resolve authenticated MCP caller owner." }),
          ),
        );
      if (Option.isNone(thread))
        return yield* new SchedulerError({
          detail: "Authenticated MCP caller thread is unavailable.",
        });
      return { actor: context.threadId, owner: threadOwner(thread.value) };
    });
    return SchedulerToolkit.of({
      create_scheduled_task: (input) =>
        Effect.flatMap(creator, ({ actor, owner }) => scheduler.create(input, actor, owner)),
      create_scheduled_command: (input) =>
        Effect.flatMap(creator, ({ actor, owner }) =>
          scheduler.create({ kind: "command", ...input }, actor, owner),
        ),
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
