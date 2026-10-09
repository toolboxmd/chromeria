/**
 * Thread people (toolboxmd/chromeria#170): who owns a thread and who it is
 * shared with. Ownership is a view, not access control.
 */
import {
  DEFAULT_PERSON,
  threadOwner,
  type AuthSessionId,
  type OrchestrationV2AppThread,
  type OrchestrationV2Command,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import type * as SessionStore from "../auth/SessionStore.ts";

type ThreadSharingCommand = Extract<
  OrchestrationV2Command,
  { readonly type: "thread.share" | "thread.unshare" | "thread.leave" }
>;

/** Whether a client command carries the dispatching device's person. */
function commandTakesPerson(command: OrchestrationV2Command): boolean {
  switch (command.type) {
    case "thread.create":
    case "thread.fork":
    case "thread.share":
    case "thread.unshare":
    case "thread.leave":
      return true;
    default:
      return false;
  }
}

/**
 * Stamps a client command with the person its device acts as, replacing any
 * owner or actor the client sent.
 */
function stampCommandPerson(
  command: OrchestrationV2Command,
  person: string,
): OrchestrationV2Command {
  switch (command.type) {
    case "thread.create":
    case "thread.fork":
      return { ...command, owner: person };
    case "thread.share":
    case "thread.unshare":
    case "thread.leave":
      return { ...command, actor: person };
    default:
      return command;
  }
}

type PersonLookup = Pick<SessionStore.SessionStore["Service"], "getPerson">;

/** The person a client session acts as. Unlabelled devices are the default person. */
export const sessionPerson = (sessions: PersonLookup, sessionId: AuthSessionId) =>
  sessions.getPerson(sessionId).pipe(Effect.map((person) => person ?? DEFAULT_PERSON));

/** Stamps a client command with its session's person when the command takes one. */
export const stampSessionPerson = (
  sessions: PersonLookup,
  sessionId: AuthSessionId,
  command: OrchestrationV2Command,
) =>
  commandTakesPerson(command)
    ? sessionPerson(sessions, sessionId).pipe(
        Effect.map((person) => stampCommandPerson(command, person)),
      )
    : Effect.succeed(command);

/** Why a sharing command cannot apply to this thread, or null when it can. */
export function threadSharingRefusal(
  thread: OrchestrationV2AppThread,
  command: ThreadSharingCommand,
): string | null {
  const owner = threadOwner(thread);
  switch (command.type) {
    case "thread.share":
      return command.coOwner === owner ? "The owner cannot be a co-owner." : null;
    case "thread.unshare":
      return command.actor === owner ? null : "Only the owner can stop sharing a thread.";
    case "thread.leave":
      return (thread.coOwners ?? []).includes(command.actor)
        ? null
        : "Only a current co-owner can leave a shared thread.";
  }
}

/** The thread's co-owners after a sharing command. */
export function coOwnersAfterSharing(
  thread: OrchestrationV2AppThread,
  command: ThreadSharingCommand,
): ReadonlyArray<string> {
  const coOwners = thread.coOwners ?? [];
  switch (command.type) {
    case "thread.share":
      return [...new Set([...coOwners, command.coOwner])];
    case "thread.unshare":
      return [];
    case "thread.leave":
      return coOwners.filter((person) => person !== command.actor);
  }
}
