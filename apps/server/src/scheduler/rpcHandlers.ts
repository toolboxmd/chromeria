import {
  DEFAULT_PERSON,
  SchedulerError,
  type CheckHistoryInput,
  type CreateScheduledTask,
  type EditScheduledTask,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import type { AuthenticatedSession } from "../auth/EnvironmentAuth.ts";
import type { SessionStore } from "../auth/SessionStore.ts";
import type { Scheduler } from "./Service.ts";
type Observer = <A, E, R>(
  method: string,
  effect: Effect.Effect<A, E, R>,
) => Effect.Effect<A, E | import("@t3tools/contracts").EnvironmentAuthorizationError, R>;
export const makeSchedulerRpcHandlers = (
  scheduler: Scheduler["Service"],
  session: Pick<AuthenticatedSession, "subject" | "sessionId">,
  sessions: Pick<SessionStore["Service"], "getPerson">,
  observe: Observer,
) => {
  const actor = `user:${session.subject}`;
  return {
    "scheduler.list": (input: { compact?: boolean }) =>
      observe("scheduler.list", input.compact ? scheduler.listCompact : scheduler.list),
    "scheduler.checkHistory": (input: CheckHistoryInput) =>
      observe("scheduler.checkHistory", scheduler.checkHistory(input)),
    "scheduler.create": (input: CreateScheduledTask) =>
      observe(
        "scheduler.create",
        Effect.gen(function* () {
          const owner =
            (yield* sessions.getPerson(session.sessionId).pipe(
              Effect.mapError(
                () =>
                  new SchedulerError({
                    detail: "Cannot resolve scheduled task creator from the authenticated session.",
                  }),
              ),
            )) ?? DEFAULT_PERSON;
          return yield* scheduler.create(input, actor, owner);
        }),
      ),
    "scheduler.edit": (input: EditScheduledTask) =>
      observe("scheduler.edit", scheduler.edit(input, actor)),
    "scheduler.pause": (input: { taskId: string; paused: boolean }) =>
      observe("scheduler.pause", scheduler.pause(input.taskId, input.paused, actor)),
    "scheduler.delete": (input: { taskId: string }) =>
      observe("scheduler.delete", scheduler.delete(input.taskId, actor)),
    "scheduler.runNow": (input: { taskId: string }) =>
      observe("scheduler.runNow", scheduler.runNow(input.taskId)),
  };
};
