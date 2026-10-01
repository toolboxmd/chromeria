import type { OrchestrationThreadActivity } from "@t3tools/contracts";

/**
 * What the threads toolkit bridge needs to remember per child thread
 * (toolboxmd/t3code#48, toolboxmd/chromeria#94): whether finished turns go
 * back to the parent, and the last assistant message already reported there.
 *
 * The bridge keeps this in memory and writes both values into the parent's
 * task.* rows (task.started and every idle task.progress row). A restarted
 * server folds every such row, read by kind rather than through the parent's
 * activity window, so the newest row for each child wins. A child with no
 * row at all (spawned before #48) restores with `reportBack: true` and its
 * current reply counted as reported. Delivery itself is exactly once: each
 * report's command id is derived from the reply, and the engine's command
 * receipts drop a resend.
 */
export interface ChildReportState {
  readonly reportBack: boolean;
  readonly lastReported: string | null;
}

function payloadOf(activity: OrchestrationThreadActivity): Record<string, unknown> {
  const payload = activity.payload;
  return payload !== null && typeof payload === "object" && !Array.isArray(payload)
    ? (payload as Record<string, unknown>)
    : {};
}

/**
 * Report state per child id, from one read of the parent's activities.
 * Children without a row carrying `reportBack` are absent.
 */
export function childReportStatesFrom(
  parentActivities: ReadonlyArray<OrchestrationThreadActivity>,
): Map<string, ChildReportState> {
  const states = new Map<string, ChildReportState>();
  for (const activity of parentActivities) {
    const payload = payloadOf(activity);
    if (typeof payload.taskId !== "string" || typeof payload.reportBack !== "boolean") continue;
    const previous = states.get(payload.taskId);
    states.set(payload.taskId, {
      reportBack: payload.reportBack,
      lastReported:
        typeof payload.reportedMessageId === "string"
          ? payload.reportedMessageId
          : (previous?.lastReported ?? null),
    });
  }
  return states;
}

/** The state for a child with no surviving row: report, but not what already exists. */
export function unrecordedChildReportState(
  lastAssistantMessageId: string | null,
): ChildReportState {
  return { reportBack: true, lastReported: lastAssistantMessageId };
}
