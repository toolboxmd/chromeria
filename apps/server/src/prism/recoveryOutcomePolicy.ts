import type { OrchestrationV2Run } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { latestRootProviderFailure } from "@t3tools/shared/orchestrationV2ThreadError";
import type { RecoveryProjection } from "./RecoveryCoordinator.ts";
import type { RecoveryOutcome } from "./RecoveryHistory.ts";
import { sameModelSelection, type RecoveryRecord } from "./recoveryPolicy.ts";

/** Historical decisions do not advance the thread's mutable retry budget. */
export function recoveryOutcomeFor(input: {
  readonly projection: RecoveryProjection;
  readonly run: OrchestrationV2Run;
  readonly latest: OrchestrationV2Run | null;
  readonly record: RecoveryRecord | null;
  readonly retiredOrIncomplete: boolean;
  readonly autoResume: boolean;
}): RecoveryOutcome | null {
  const { projection, run, latest, record } = input;
  if (
    run.status !== "failed" &&
    !(
      run.forkPrismContinuationSourceRunId !== undefined &&
      ["interrupted", "cancelled", "rolled_back"].includes(run.status)
    )
  )
    return null;
  const identity = { sourceRunId: run.id, threadId: run.threadId };
  const noRetry = (
    reason: Extract<RecoveryOutcome, { outcome: "not_retryable" }>["reason"],
  ): RecoveryOutcome => ({ ...identity, status: "decided", outcome: "not_retryable", reason });
  const abandoned = (
    reason: Extract<RecoveryOutcome, { outcome: "abandoned" }>["reason"],
  ): RecoveryOutcome => ({ ...identity, status: "decided", outcome: "abandoned", reason });
  if (run.status !== "failed")
    return abandoned(run.status === "interrupted" ? "interrupted" : "cancelled");
  // Every old failed run gets its own conclusion even when a newer run owns the controller.
  if (latest?.id !== run.id || (record !== null && record.sourceRunId !== run.id))
    return noRetry("superseded");
  if (
    input.retiredOrIncomplete ||
    projection.turnItems.some(
      (item) => item.type === "run_interrupt_request" && item.runId === run.id,
    )
  )
    return abandoned("stopped_or_retired");
  if (
    projection.thread.archivedAt !== null ||
    projection.thread.deletedAt !== null ||
    projection.thread.settledOverride === "settled" ||
    projection.runtimeRequests.some((request) => request.status === "pending") ||
    !sameModelSelection(projection.thread.modelSelection, run.modelSelection)
  )
    return abandoned("ineligible");
  const failure = latestRootProviderFailure(run, projection.turnItems);
  if (failure === null) return noRetry("missing_failure");
  if (failure.class === "usage_limit") {
    const choice = projection.thread.limitRecovery;
    const autoResume =
      choice?.runId === run.id && choice.resetAt === failure.resetAt
        ? choice.autoResume
        : input.autoResume;
    if (!autoResume) return noRetry("opted_out");
    const reset = failure.resetAt == null ? NaN : Date.parse(failure.resetAt);
    if (
      !Number.isFinite(reset) ||
      reset <= DateTime.toEpochMillis(run.completedAt ?? run.requestedAt)
    )
      return noRetry("invalid_reset");
    return { ...identity, status: "pending", reason: "reset" };
  }
  if (projection.thread.creationSource !== "mcp") return noRetry("non_mcp");
  if (failure.retryable !== true) return noRetry("non_retryable");
  if (record?.state === "retry_pending" || record?.state === "retry_started")
    return { ...identity, status: "pending", reason: "retry" };
  return noRetry("exhausted");
}
