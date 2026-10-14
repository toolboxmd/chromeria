import type {
  NodeId,
  OrchestrationV2ContextTransfer,
  OrchestrationV2PendingBackgroundTask,
  OrchestrationV2Run,
  OrchestrationV2Subagent,
  OrchestrationV2ThreadProjection,
  ThreadId,
} from "@t3tools/contracts";
import { runRanAfter } from "@t3tools/shared/orchestrationV2ThreadError";

type ChildWork = Pick<OrchestrationV2ThreadProjection, "thread" | "runs" | "messages">;

/** Requested work shares one task; legacy unbound children retain their original job. */
export function delegatedTaskRuns(child: ChildWork, taskId: NodeId) {
  const monitorIds = new Set(
    child.messages
      .filter((message) => message.notification?.source.kind === "monitor")
      .map((message) => message.runId),
  );
  const originalRun = child.runs.toSorted((a, b) => a.ordinal - b.ordinal)[0];
  const originalTaskId =
    child.thread.forkedFrom?.type === "node" ? child.thread.forkedFrom.nodeId : undefined;
  return child.runs.filter(
    (run) =>
      !monitorIds.has(run.id) &&
      run.status !== "rolled_back" &&
      (run.delegatedTaskId === taskId ||
        (taskId === originalTaskId &&
          originalRun?.delegatedTaskId === undefined &&
          run.delegatedTaskId === undefined)),
  );
}

export function delegatedResultTransfers(
  child: Pick<OrchestrationV2ThreadProjection, "thread" | "runs">,
  taskId: NodeId,
  transfers: ReadonlyArray<OrchestrationV2ContextTransfer>,
  parentThreadId: ThreadId | null,
) {
  const originalTaskId =
    child.thread.forkedFrom?.type === "node" ? child.thread.forkedFrom.nodeId : undefined;
  const runIds = new Set(
    child.runs.filter((run) => run.delegatedTaskId === taskId).map((run) => run.id),
  );
  return transfers.filter(
    (transfer) =>
      transfer.type === "subagent_result" &&
      transfer.sourceThreadId === child.thread.id &&
      transfer.targetThreadId === parentThreadId &&
      (taskId === originalTaskId ||
        (transfer.sourcePoint.runId !== undefined && runIds.has(transfer.sourcePoint.runId))),
  );
}

/** Reading a page acknowledges the latest delivered result, not an old first reply. */
export function latestDelegatedResultTransfer(
  child: Pick<OrchestrationV2ThreadProjection, "thread" | "runs">,
  taskId: NodeId,
  transfers: ReadonlyArray<OrchestrationV2ContextTransfer>,
  parentThreadId: ThreadId,
) {
  const runs = new Map(child.runs.map((run) => [run.id, run]));
  return delegatedResultTransfers(child, taskId, transfers, parentThreadId).reduce<
    OrchestrationV2ContextTransfer | undefined
  >((latest, candidate) => {
    const run =
      candidate.sourcePoint.runId === undefined ? undefined : runs.get(candidate.sourcePoint.runId);
    const priorRun =
      latest?.sourcePoint.runId === undefined ? undefined : runs.get(latest.sourcePoint.runId);
    return latest === undefined ||
      (run !== undefined && (priorRun === undefined || runRanAfter(run, priorRun)))
      ? candidate
      : latest;
  }, undefined);
}

/** Results are per terminal requested turn; a restart successor owns its cut predecessor. */
export function untransferredDelegatedRuns(
  child: ChildWork,
  taskId: NodeId,
  transfers: ReadonlyArray<OrchestrationV2ContextTransfer>,
) {
  const runs = delegatedTaskRuns(child, taskId);
  const results = delegatedResultTransfers(
    child,
    taskId,
    transfers,
    child.thread.lineage.parentThreadId,
  );
  if (results.some((transfer) => transfer.sourcePoint.runId === undefined)) return [];
  const transferredIds = new Set(results.map((transfer) => transfer.sourcePoint.runId));
  const firstResultRun = runs
    .filter((run) => transferredIds.has(run.id))
    .toSorted((a, b) => a.ordinal - b.ordinal)[0];
  const continuedSources = new Set(runs.map((run) => run.restartContinuationOfRunId));
  return runs
    .filter(
      (run) =>
        ["completed", "failed", "cancelled", "interrupted"].includes(run.status) &&
        (run.startedAt !== null || run.ordinal === 1) &&
        !transferredIds.has(run.id) &&
        (firstResultRun === undefined || runRanAfter(run, firstResultRun)) &&
        !continuedSources.has(run.id),
    )
    .toSorted((a, b) => a.ordinal - b.ordinal);
}

export function restartContinuationRuns(
  runs: ReadonlyArray<OrchestrationV2Run>,
  source: OrchestrationV2Run,
) {
  const ids = new Set([source.id]);
  for (const run of runs.toSorted((a, b) => a.ordinal - b.ordinal)) {
    if (run.restartContinuationOfRunId !== undefined && ids.has(run.restartContinuationOfRunId)) {
      ids.add(run.id);
    }
  }
  return runs.filter((run) => ids.has(run.id));
}

export function pendingDelegatedChildTasks(
  parent: Pick<OrchestrationV2ThreadProjection, "subagents">,
  children: ReadonlyMap<
    OrchestrationV2ThreadProjection["thread"]["id"],
    OrchestrationV2ThreadProjection
  >,
) {
  return parent.subagents.filter((task) => {
    if (
      task.origin !== "app_owned" ||
      task.result === null ||
      task.childThreadId === null ||
      task.completionDelivery?.state === "disposed"
    )
      return false;
    const child = children.get(task.childThreadId);
    return (
      child !== undefined &&
      child.thread.deletedAt === null &&
      child.thread.archivedAt === null &&
      delegatedTaskRuns(child, task.id).some(
        (run) =>
          ["preparing", "starting", "running", "waiting"].includes(run.status) ||
          (run.status === "queued" && run.queueHeld !== true),
      )
    );
  });
}

/** The roster describes live follow-ups, without reopening the first task/card. */
export function withPendingDelegatedChildWork(input: {
  readonly pending: ReadonlyArray<OrchestrationV2PendingBackgroundTask>;
  readonly latestRunStatus: string | undefined;
  readonly hasActiveRun: boolean;
  readonly tasks: ReadonlyArray<OrchestrationV2Subagent>;
}) {
  if (
    input.hasActiveRun ||
    !["completed", "failed", "cancelled", "interrupted", "waiting"].includes(
      input.latestRunStatus ?? "",
    )
  ) {
    return [...input.pending];
  }
  const pending = [...input.pending];
  for (const task of input.tasks) {
    if (
      task.childThreadId === null ||
      pending.some(
        (item) =>
          item.taskId === task.id ||
          (item.kind === "subagent" && item.childThreadId === task.childThreadId),
      )
    )
      continue;
    pending.push({
      taskId: task.id,
      kind: "subagent",
      childThreadId: task.childThreadId,
      description: task.title ?? task.prompt,
    });
  }
  return pending;
}
