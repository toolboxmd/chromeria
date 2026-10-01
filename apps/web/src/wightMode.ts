import type {
  OrchestrationSession,
  OrchestrationThreadShell,
  ServerProvider,
} from "@t3tools/contracts";

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
    OrchestrationThreadShell,
    "archivedAt" | "hasPendingApprovals" | "hasPendingUserInput" | "hasActionableProposedPlan"
  > & { readonly session: Pick<OrchestrationSession, "status" | "lastError"> | null },
): string | null {
  if (thread.archivedAt !== null) return "Waiting: the thread is archived.";
  if (thread.hasPendingApprovals) return "Waiting for an approval.";
  if (thread.hasPendingUserInput) return "Waiting for an answer to its question.";
  if (thread.hasActionableProposedPlan) return "Waiting for a decision on the proposed plan.";
  switch (thread.session?.status) {
    case "interrupted":
      return "Waiting: the last turn was interrupted. Send a message to resume.";
    case "stopped":
      return "Waiting: the session is stopped. Send a message to resume.";
    case "error":
      // Usage-limit errors are left to usage-limit resume, as on the server.
      return /\busage limit\b/i.test(thread.session.lastError ?? "")
        ? null
        : "Waiting: the last turn failed. Send a message to resume.";
    default:
      return null;
  }
}
