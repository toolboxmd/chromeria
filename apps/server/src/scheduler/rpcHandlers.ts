import type { CheckHistoryInput, CreateScheduledTask, EditScheduledTask } from "@t3tools/contracts";
import type * as Effect from "effect/Effect";
import type { Scheduler } from "./Service.ts";
type Observer = <A, E, R>(
  method: string,
  effect: Effect.Effect<A, E, R>,
) => Effect.Effect<A, E | import("@t3tools/contracts").EnvironmentAuthorizationError, R>;
export const makeSchedulerRpcHandlers = (
  scheduler: Scheduler["Service"],
  actor: string,
  observe: Observer,
) => ({
  "scheduler.list": () => observe("scheduler.list", scheduler.list),
  "scheduler.checkHistory": (input: CheckHistoryInput) =>
    observe("scheduler.checkHistory", scheduler.checkHistory(input)),
  "scheduler.create": (input: CreateScheduledTask) =>
    observe("scheduler.create", scheduler.create(input, actor)),
  "scheduler.edit": (input: EditScheduledTask) =>
    observe("scheduler.edit", scheduler.edit(input, actor)),
  "scheduler.pause": (input: { taskId: string; paused: boolean }) =>
    observe("scheduler.pause", scheduler.pause(input.taskId, input.paused, actor)),
  "scheduler.delete": (input: { taskId: string }) =>
    observe("scheduler.delete", scheduler.delete(input.taskId, actor)),
  "scheduler.runNow": (input: { taskId: string }) =>
    observe("scheduler.runNow", scheduler.runNow(input.taskId)),
});
