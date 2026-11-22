import type { OrchestrationV2ThreadShell, ServerProvider } from "@t3tools/contracts";
import { msg } from "gt-react";

import { m, type MessageResolver } from "./i18n/messages";

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
  resolve: MessageResolver = m,
): WightUsageStatus {
  if (provider === undefined || !provider.enabled) {
    return { paused: true, text: resolve(msg("Paused: this provider instance is unavailable.")) };
  }
  if (limit === 0) return { paused: true, text: resolve(msg("Paused: the Wight limit is 0%.")) };
  const windows = provider.usageLimits?.windows ?? [];
  const highest = windows.reduce<(typeof windows)[number] | null>(
    (top, window) => (top === null || window.usedPercent > top.usedPercent ? window : top),
    null,
  );
  if (highest === null) {
    return {
      paused: false,
      text: resolve(
        msg("This provider reports no usage windows, so the Wight limit cannot be enforced."),
      ),
    };
  }
  const used = Math.round(highest.usedPercent);
  return highest.usedPercent >= limit
    ? {
        paused: true,
        text: resolve(
          msg("Paused: {label} at {used}%, limit {limit}%.", { label: highest.label, used, limit }),
        ),
      }
    : {
        paused: false,
        text: resolve(
          msg("{label} at {used}%, limit {limit}%.", { label: highest.label, used, limit }),
        ),
      };
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
  resolve: MessageResolver = m,
): string | null {
  if (thread.archivedAt !== null) return resolve(msg("Waiting: the thread is archived."));
  if (thread.pendingRuntimeRequest !== null)
    return thread.pendingRuntimeRequest.kind === "user_input"
      ? resolve(msg("Waiting for an answer to its question."))
      : resolve(msg("Waiting for an approval."));
  if (thread.hasActionableProposedPlan)
    return resolve(msg("Waiting for a decision on the proposed plan."));
  if (
    thread.limitRecovery != null ||
    (thread.status === "failed" && thread.lastErrorClass === "usage_limit")
  )
    return resolve(msg("Waiting for usage-limit reset recovery."));
  if (thread.status === "interrupted" || thread.status === "cancelled")
    return resolve(msg("Waiting: the last turn was interrupted. Send a message to resume."));
  if (thread.status === "failed")
    return resolve(msg("Waiting: the last turn failed. Send a message to resume."));
  return null;
}
