import type {
  OrchestrationV2RestartCut,
  OrchestrationV2RestartCutView,
  OrchestrationV2Run,
  RunId,
} from "@t3tools/contracts";

export const RESTART_CUT_REASONS = {
  CONTINUATION_OFF: "Continuing threads after restarts is turned off.",
  SETTINGS_UNAVAILABLE: "The restart continuation settings could not be read.",
  THREAD_INACTIVE: "The thread is archived or deleted.",
  NOT_STARTED: "The restart cut it before its provider turn started.",
  BACKGROUND_ONLY: "The run had no unfinished root turn or awaited work to continue.",
  PROVIDER_CHANGED: "The thread switched provider.",
  NOT_RESUMABLE: "Its provider thread cannot be resumed.",
  SESSION_FAILED: "Its provider session failed before the restart.",
  NO_LIVE_TURN: "No provider turn was running for it at the restart.",
  SUPERSEDED: "A later run took precedence.",
  TASK_SETTLED:
    "The delegated task already returned a result before this old restart cut was recovered.",
  STOP_REQUESTED: "Stop was requested before the restart.",
  MAINTENANCE: "It was a provider maintenance command.",
  EFFECT_FAILED: "The continuation could not be started.",
  NO_CONTINUATION: "The restart continuation ended without starting a run.",
  INVALID_CHAIN: "The restart continuation chain is invalid.",
} as const;
export type RestartCutReason = (typeof RESTART_CUT_REASONS)[keyof typeof RESTART_CUT_REASONS];

/** Keep the original cut time and first decline, including across stale full snapshots. */
export function mergeRestartCut(
  current: OrchestrationV2RestartCut | undefined,
  next: OrchestrationV2RestartCut | undefined,
): OrchestrationV2RestartCut | undefined {
  if (current === undefined) return next;
  if (current.continuation === "scheduled" && next?.continuation === "declined") {
    return { ...next, cutAt: current.cutAt };
  }
  return current;
}

/**
 * Intent statuses are keyed by source run after checking exact effect ownership.
 * An unread status stays pending; explicit null means the intent is missing.
 */
export function restartCutView(
  run: OrchestrationV2Run,
  runs: ReadonlyArray<OrchestrationV2Run>,
  intentStatuses?: ReadonlyMap<
    RunId,
    "pending" | "running" | "succeeded" | "failed" | "cancelled" | null
  >,
): OrchestrationV2RestartCutView | undefined {
  if (run.status !== "cancelled") return undefined;
  const visited = new Set<RunId>([run.id]);
  let current = run;
  const declined = (reason: RestartCutReason): OrchestrationV2RestartCutView => ({
    state: "declined",
    reason,
    ...(current.id === run.id
      ? {}
      : { continuationRunId: current.id, continuationStatus: current.status }),
  });

  for (;;) {
    const replacements = runs.filter(
      (next) => next.threadId === run.threadId && next.restartContinuationOfRunId === current.id,
    );
    // Once any linked replacement started, later cuts cannot erase that fact.
    const started = replacements.find((next) => next.startedAt !== null && !visited.has(next.id));
    if (started !== undefined) {
      return {
        state: "continued",
        continuationRunId: started.id,
        continuationStatus: started.status,
      };
    }
    if (replacements.length > 1 || replacements.some((next) => visited.has(next.id))) {
      return declined(RESTART_CUT_REASONS.INVALID_CHAIN);
    }
    if (current.restartCut?.continuation === "declined") {
      const recordedReason = current.restartCut.reason;
      return declined(
        Object.values(RESTART_CUT_REASONS).find((reason) => reason === recordedReason) ??
          RESTART_CUT_REASONS.NO_CONTINUATION,
      );
    }
    const next = replacements[0];
    if (next !== undefined) {
      current = next;
      visited.add(next.id);
      if (["preparing", "queued", "starting", "running", "waiting"].includes(next.status)) {
        return { state: "continuation_pending" };
      }
      if (
        next.status === "cancelled" &&
        (next.restartCut !== undefined ||
          intentStatuses?.get(next.id) != null ||
          runs.some(
            (replacement) =>
              replacement.threadId === run.threadId &&
              replacement.restartContinuationOfRunId === next.id,
          ))
      ) {
        continue;
      }
      return declined(
        next.status === "failed"
          ? RESTART_CUT_REASONS.EFFECT_FAILED
          : RESTART_CUT_REASONS.NO_CONTINUATION,
      );
    }

    const status = intentStatuses?.get(current.id);
    if (current.restartCut === undefined && status == null) return undefined;
    if (status === undefined || status === "pending" || status === "running") {
      return { state: "continuation_pending" };
    }
    return declined(
      status === "failed" ? RESTART_CUT_REASONS.EFFECT_FAILED : RESTART_CUT_REASONS.NO_CONTINUATION,
    );
  }
}
