import type { OrchestrationV2ThreadShell, ServerProvider } from "@t3tools/contracts";

export interface WightUsageStatus {
  readonly paused: boolean;
  readonly text: string;
}

/**
 * Explains the server's Wight quota gate for one provider instance. Mirrors
 * `wightPaused` in the server's threads toolkit: a missing or disabled
 * instance, a 0% limit, or any reported window at the limit pauses new
 * nudges; no reported windows means the limit cannot be enforced.
 */
export function wightUsageStatus(
  provider: Pick<ServerProvider, "enabled" | "usageLimits"> | undefined,
  limit: number,
): WightUsageStatus {
  if (provider === undefined || !provider.enabled) {
    return { paused: true, text: "Paused: this provider instance is unavailable." };
  }
  if (limit === 0) return { paused: true, text: "Paused: the Wight limit is 0%." };
  const windows = provider.usageLimits?.windows ?? [];
  const highest = windows.reduce<(typeof windows)[number] | null>(
    (top, window) => (top === null || window.usedPercent > top.usedPercent ? window : top),
    null,
  );
  if (highest === null) {
    return {
      paused: false,
      text: "This provider reports no usage windows, so the Wight limit cannot be enforced.",
    };
  }
  const used = Math.round(highest.usedPercent);
  return highest.usedPercent >= limit
    ? { paused: true, text: `Paused: ${highest.label} at ${used}%, limit ${limit}%.` }
    : { paused: false, text: `${highest.label} at ${used}%, limit ${limit}%.` };
}

/**
 * Why the server is not sending "continue" to this thread right now, or null
 * when it is working or idle and ready for one. Mirrors `wightIdle` in the
 * server's threads toolkit, which leaves these states for the user to clear.
 */
export function wightThreadWait(
  thread: Pick<
    OrchestrationV2ThreadShell,
    | "archivedAt"
    | "pendingRuntimeRequest"
    | "hasActionableProposedPlan"
    | "status"
    | "lastErrorClass"
    | "limitRecovery"
  >,
): string | null {
  if (thread.archivedAt !== null) return "Waiting: the thread is archived.";
  if (thread.pendingRuntimeRequest !== null)
    return thread.pendingRuntimeRequest.kind === "user_input"
      ? "Waiting for an answer to its question."
      : "Waiting for an approval.";
  if (thread.hasActionableProposedPlan) return "Waiting for a decision on the proposed plan.";
  if (
    thread.limitRecovery != null ||
    (thread.status === "failed" && thread.lastErrorClass === "usage_limit")
  )
    return "Waiting for usage-limit reset recovery.";
  if (thread.status === "interrupted" || thread.status === "cancelled")
    return "Waiting: the last turn was interrupted. Send a message to resume.";
  if (thread.status === "failed") return "Waiting: the last turn failed. Send a message to resume.";
  return null;
}
