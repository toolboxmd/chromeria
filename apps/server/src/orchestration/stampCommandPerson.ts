import { DEFAULT_PERSON, type AuthSessionId, type OrchestrationCommand } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import type { SessionStore } from "../auth/SessionStore.ts";
import { PersistenceSqlError } from "../persistence/Errors.ts";

/** Stamp client-caused commands from the current device label, including bootstrap creation. */
export const stampCommandPerson = Effect.fnUntraced(function* (
  command: OrchestrationCommand,
  sessionId: AuthSessionId,
  sessions: Pick<SessionStore["Service"], "getPerson">,
) {
  if (
    command.type !== "thread.create" &&
    command.type !== "thread.share" &&
    command.type !== "thread.unshare" &&
    command.type !== "thread.leave" &&
    !(command.type === "thread.turn.start" && command.bootstrap?.createThread)
  )
    return command;

  const person =
    (yield* sessions
      .getPerson(sessionId)
      .pipe(
        Effect.mapError(
          (cause) => new PersistenceSqlError({ operation: "stampCommandPerson:getPerson", cause }),
        ),
      )) ?? DEFAULT_PERSON;
  if (command.type === "thread.create") return { ...command, owner: person };
  if (command.type === "thread.turn.start" && command.bootstrap?.createThread) {
    return {
      ...command,
      bootstrap: {
        ...command.bootstrap,
        createThread: { ...command.bootstrap.createThread, owner: person },
      },
    };
  }
  return { ...command, actor: person };
});
