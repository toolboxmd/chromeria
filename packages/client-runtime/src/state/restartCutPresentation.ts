import { TurnItemId, type OrchestrationV2ThreadProjection } from "@t3tools/contracts";
import { restartCutView } from "@t3tools/shared/restartCut";
import { mFallback, msg } from "gt-i18n";
import type { MFunctionType } from "gt-i18n/types";

type Rows = OrchestrationV2ThreadProjection["visibleTurnItems"];

/** Display-only notices beside the original cut, never persisted or delivered to a provider. */
export function createRestartCutProjector(resolve: MFunctionType = mFallback) {
  let previousRows: Rows | undefined;
  let previousRuns: OrchestrationV2ThreadProjection["runs"] | undefined;
  let previousResult: Rows | undefined;
  const cache = new Map<string, Rows[number]>();
  return (projection: Pick<OrchestrationV2ThreadProjection, "visibleTurnItems" | "runs">): Rows => {
    const { visibleTurnItems: rows, runs } = projection;
    if (rows === previousRows && runs === previousRuns && previousResult) return previousResult;
    if (
      !runs.some(
        (run) => run.restartCut !== undefined || run.restartContinuationOfRunId !== undefined,
      )
    )
      return rows;
    const lastRowByRun = new Map(
      rows.flatMap((row, index) =>
        row.visibility === "local" && row.item.runId !== null
          ? [[row.item.runId, index] as const]
          : [],
      ),
    );
    const notices = new Map<number, Rows[number]>();
    const activeIds = new Set<string>();
    const linkedSources = new Set(runs.map((run) => run.restartContinuationOfRunId));
    for (const run of runs) {
      if (run.restartCut === undefined && !linkedSources.has(run.id)) continue;
      const index = lastRowByRun.get(run.id);
      if (index === undefined) continue; // Do not pull unloaded or inherited history into this window.
      const cut = restartCutView(run, runs);
      if (cut === undefined) continue;
      const detail =
        cut.state === "continued"
          ? resolve(
              msg(
                "Stopped because Chromeria restarted. Continuation started (run {run}, {status}).",
              ),
              {
                run: String(
                  runs.find((next) => next.id === cut.continuationRunId)?.ordinal ??
                    cut.continuationRunId,
                ),
                status: cut.continuationStatus,
              },
            )
          : cut.state === "continuation_pending"
            ? resolve(msg("Stopped because Chromeria restarted. Continuation pending."))
            : resolve(msg("Stopped because Chromeria restarted. Continuation declined. {reason}"), {
                reason: cut.reason,
              });
      const id = TurnItemId.make(`restart-cut:${run.id}`);
      activeIds.add(id);
      const cached = cache.get(id);
      const at = run.restartCut?.cutAt ?? run.completedAt;
      const notice: Rows[number] =
        cached?.item.type === "notification" && cached.item.detail === detail
          ? cached
          : {
              position: 0,
              visibility: "synthetic",
              sourceThreadId: run.threadId,
              sourceItemId: id,
              item: {
                id,
                threadId: run.threadId,
                runId: run.id,
                nodeId: null,
                providerThreadId: null,
                providerTurnId: null,
                nativeItemRef: null,
                parentItemId: null,
                ordinal: rows[index]!.item.ordinal,
                status: "completed",
                title: null,
                startedAt: at,
                completedAt: at,
                updatedAt: at ?? run.requestedAt,
                type: "notification",
                source: { kind: "system" },
                outcome: "cancelled",
                summary: resolve(msg("Stopped by restart")),
                detail,
              },
            };
      cache.set(id, notice);
      notices.set(index, notice);
    }
    for (const id of cache.keys()) if (!activeIds.has(id)) cache.delete(id);
    previousRows = rows;
    previousRuns = runs;
    previousResult =
      notices.size === 0
        ? rows
        : rows.flatMap((row, index) => {
            const notice = notices.get(index);
            return notice === undefined ? [row] : [row, notice];
          });
    return previousResult;
  };
}
