import { resolveLinkedThreadOpen } from "@t3tools/client-runtime/linked-thread";
import type { PendingBackgroundWorkItem } from "@t3tools/client-runtime/state/thread-execution";
import type { EnvironmentId } from "@t3tools/contracts";

import type { SubagentThreadTarget } from "./threadAgentsPresentation";

/**
 * Where a background work item's thread opens: here for a local child, in the remote child's own
 * environment when this app is connected there, else read-only through this environment's link.
 */
export function backgroundWorkThreadTarget(
  item: Pick<PendingBackgroundWorkItem, "childThreadId" | "childEnvironmentId">,
  environmentId: EnvironmentId,
  ownConnection: (environmentId: EnvironmentId) => { readonly connected: boolean } | undefined,
  /** The label of `environmentId`'s link to the given environment, when it has one. */
  linkLabel: (environmentId: EnvironmentId) => string | undefined,
): SubagentThreadTarget | null {
  if (item.childThreadId === undefined) return null;
  const childEnvironmentId = item.childEnvironmentId ?? environmentId;
  const target = { environmentId: childEnvironmentId, threadId: item.childThreadId };
  const label = linkLabel(childEnvironmentId);
  const open = resolveLinkedThreadOpen({
    environmentId: childEnvironmentId,
    viaEnvironmentId: environmentId,
    own: ownConnection(childEnvironmentId),
    linked: label !== undefined,
  });
  return open === "linked" && label !== undefined
    ? { ...target, linked: { viaEnvironmentId: environmentId, label } }
    : target;
}
