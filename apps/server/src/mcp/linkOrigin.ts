import * as PeerAuthority from "../peer/PeerAuthority.ts";
import * as Option from "effect/Option";
import {
  ThreadId,
  OrchestratorMcpFailure,
  type OrchestrationV2LinkOrigin,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import { clientModeCeiling, intersectModes } from "./McpInvocationContext.ts";
import type { Caller } from "./threadAccess.ts";

/**
 * Work a linked environment starts here stays within that link. An agent
 * reaches this environment through a link as one MCP client session, and
 * everything that session starts, and everything that work starts in turn,
 * carries the session in `linkOrigin`. Created-work callers retain this fence;
 * explicitly consented environment-resource callers may cross it within their
 * modes and operation policy. Ordinary reads remain unfenced.
 *
 * This is a routing rule for T3's own tools. An agent the link starts still
 * runs as this environment's user.
 */

/** The link a caller acts for: its own session, or the link that started its thread. */
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

const outsideLink = (origin: OrchestrationV2LinkOrigin, message: string) =>
  new OrchestratorMcpFailure({
    code: "capability_denied",
    message: `This work was started from ${origin.label} through a linked environment, so ${message}`,
  });

/** Apply the link's resource scope to destination thread mutations. */
export const assertSameLink = <T extends Pick<OrchestrationV2ThreadShell, "linkOrigin">>(
  caller: Pick<Caller, "scope"> & {
    readonly caller: Pick<OrchestrationV2ThreadShell, "linkOrigin"> | undefined;
  },
  target: T,
) => {
  const origin = callerLinkOrigin(caller);
  return Effect.gen(function* () {
    if (
      origin === undefined ||
      caller.scope.authority?.ceiling.resourceScope === "environment-resources" ||
      target.linkOrigin?.sessionId === origin.sessionId
    )
      return;
    // Native descendants of a peer-owned run on an ordinary local parent
    // carry durable creation authority, even without a legacy linkOrigin.
    if (caller.scope.authority !== undefined && "id" in target && typeof target.id === "string") {
      const owner = yield* Effect.serviceOption(PeerAuthority.PeerAuthority);
      if (
        Option.isSome(owner) &&
        (yield* owner.value.ownsCreatedThread(origin.sessionId, ThreadId.make(target.id)))
      )
        return;
    }
    return yield* Effect.fail(outsideLink(origin, `it may change only threads that link started.`));
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
  return origin === undefined ||
    directFullAccessPeer(caller) ||
    (caller.scope.authority?.ceiling.resourceScope === "environment-resources" &&
      modes.runtimeMode === "full-access" &&
      modes.interactionMode === "default")
    ? Effect.void
    : Effect.fail(outsideLink(origin, `it cannot ${what}.`));
};
