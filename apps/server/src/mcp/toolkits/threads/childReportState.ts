import type { OrchestrationThreadActivity } from "@t3tools/contracts";

/**
 * What the threads toolkit bridge needs to remember per child thread
 * (toolboxmd/t3code#48): whether finished turns go back to the parent, and
 * the last assistant message already reported there.
 *
 * The bridge keeps this in memory and writes it into the parent's task.*
 * activities (`reportBack` on task.started, `reportedMessageId` on the
 * task.progress row that triggers a report), so a restarted server restores
 * it from the parent instead of silently dropping report-back.
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
 * Restores a child's report state from its parent's activities. A missing
 * `reportBack` (children spawned before #48, or a task.started row outside
 * the parent's activity window) falls back to spawn_thread's default, true.
 */
export function childReportStateFrom(
  parentActivities: ReadonlyArray<OrchestrationThreadActivity>,
  childId: string,
): ChildReportState {
  let reportBack = true;
  let lastReported: string | null = null;
  for (const activity of parentActivities) {
    const payload = payloadOf(activity);
    if (payload.taskId !== childId) continue;
    if (activity.kind === "task.started" && typeof payload.reportBack === "boolean") {
      reportBack = payload.reportBack;
    }
    if (typeof payload.reportedMessageId === "string") {
      lastReported = payload.reportedMessageId;
    }
  }
  return { reportBack, lastReported };
}
