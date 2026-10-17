import * as PeerAuthority from "../peer/PeerAuthority.ts";
import * as Option from "effect/Option";
import {
  ThreadId,
  ScheduledTaskId,
  OrchestratorMcpFailure,
  type OrchestrationV2LinkOrigin,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import { clientModeCeiling, intersectModes } from "./McpInvocationContext.ts";
import type { Caller } from "./threadAccess.ts";

/**
 * Legacy work carries one linkOrigin. Modern work additionally persists every
 * contributing authority, whose resource predicates PeerAuthority checks together.
 * Creation labels cannot stand in for that proof. Ordinary reads stay unfenced.
 */

/** Descriptive creation provenance. Never use this single origin as modern authorization. */
export const callerLinkOrigin = (
  caller: Pick<Caller, "scope"> & {
    readonly caller: Pick<OrchestrationV2ThreadShell, "linkOrigin"> | undefined;
  },
): OrchestrationV2LinkOrigin | undefined =>
  (caller.scope.authority?.resourceSessionId !== undefined
    ? {
        sessionId: caller.scope.authority.resourceSessionId,
        label: caller.caller?.linkOrigin?.label ?? "linked environment",
      }
    : undefined) ??
  (caller.scope.authority?.sessionId != null
    ? {
        sessionId: caller.scope.authority.sessionId,
        label: caller.scope.authority.peer.environmentId,
      }
    : undefined) ??
  caller.caller?.linkOrigin ??
  (caller.scope.client?.linked === true
    ? { sessionId: caller.scope.client.sessionId, label: caller.scope.client.label }
    : undefined);

/** Apply the link's resource scope to destination thread mutations. */
export const assertSameLink = <T extends Pick<OrchestrationV2ThreadShell, "linkOrigin">>(
  caller: Pick<Caller, "scope"> & {
    readonly caller: Pick<OrchestrationV2ThreadShell, "linkOrigin"> | undefined;
  },
  target: T,
) => {
  return Effect.gen(function* () {
    const owner = yield* Effect.serviceOption(PeerAuthority.PeerAuthority);
    if (Option.isSome(owner)) {
      if (!("id" in target) || typeof target.id !== "string")
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message: "A canonical destination identity is required.",
        });
      yield* owner.value.checkTarget(
        caller.scope,
        "schedule" in target
          ? { type: "schedule", taskId: ScheduledTaskId.make(target.id) }
          : { type: "thread", threadId: ThreadId.make(target.id) },
      );
      return;
    }
    const origin = callerLinkOrigin(caller);
    // Standalone legacy consumers have no durable modern authority service.
    // A modern record must never fall back to one copied linkOrigin.
    if (
      caller.scope.authority === undefined &&
      (origin === undefined || target.linkOrigin?.sessionId === origin.sessionId)
    )
      return;
    return yield* new OrchestratorMcpFailure({
      code: "capability_denied",
      message: "The destination is outside the caller's verified resource scope.",
    });
  });
};

/** Direct peer calls may use the full access granted by the user.
 * Descendants retain their origin fence; narrowed/plan calls never qualify.
 */
const directFullAccessPeer = (caller: Pick<Caller, "scope" | "caller">) => {
  const ceiling = intersectModes(
    clientModeCeiling(caller.scope.client),
    caller.scope.authority?.ceiling,
  );
  return (
    caller.caller === undefined &&
    caller.scope.client?.linked === true &&
    caller.scope.authority?.envelope.descendant !== true &&
    ceiling.runtimeMode === "full-access" &&
    ceiling.interactionMode === "default"
  );
};

/** Preserve legacy direct access and explicitly granted descendant environment operations. */
export const assertNotLinked = (
  caller: Pick<Caller, "scope" | "caller" | "limits">,
  what: string,
) => {
  const modes = intersectModes(
    caller.limits,
    caller.scope.authority?.ceiling ?? caller.scope.legacyPeerCeiling,
  );
  const origin = callerLinkOrigin(caller);
  return Effect.gen(function* () {
    const owner = yield* Effect.serviceOption(PeerAuthority.PeerAuthority);
    if (Option.isSome(owner)) {
      yield* owner.value.checkTarget(caller.scope, { type: "environment" });
      if (
        origin === undefined ||
        directFullAccessPeer(caller) ||
        (modes.runtimeMode === "full-access" && modes.interactionMode === "default")
      )
        return;
    } else if (
      caller.scope.authority === undefined &&
      (origin === undefined || directFullAccessPeer(caller))
    )
      return;
    return yield* new OrchestratorMcpFailure({
      code: "capability_denied",
      message: `The caller's current authority cannot ${what}.`,
    });
  });
};
