import {
  CommandId,
  MessageId,
  ModelSelection,
  RunId,
  ThreadId,
  type OrchestrationV2Command,
  type OrchestrationV2ProviderFailure,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";

/** A retry keeps its original identity across reset waits and server restarts. */
export const RecoveryRecord = Schema.Struct({
  threadId: ThreadId,
  originalRunId: RunId,
  sourceRunId: RunId,
  sourceOrdinal: Schema.Number,
  sourceEndedAt: Schema.NullOr(Schema.Number),
  modelSelection: ModelSelection,
  retryMessageId: MessageId,
  retryUsed: Schema.Boolean,
  state: Schema.Literals(["retry_pending", "retry_started", "reset_wait", "closed"]),
  resetAt: Schema.NullOr(Schema.String),
});
export type RecoveryRecord = typeof RecoveryRecord.Type;

export const sameModelSelection = Schema.toEquivalence(ModelSelection);

/** Only server continuations belong to the same recovery budget. */
function continues(record: RecoveryRecord, run: OrchestrationV2Run) {
  return (
    run.id === record.sourceRunId ||
    run.userMessageId === record.retryMessageId ||
    String(run.userMessageId).startsWith(`limit-resume:${record.threadId}:${record.sourceRunId}:`)
  );
}

export function decideRecovery(input: {
  readonly previous: RecoveryRecord | null;
  readonly run: OrchestrationV2Run;
  readonly failure: OrchestrationV2ProviderFailure | null;
  readonly stoppedOrRetired: boolean;
  readonly autoResume: boolean;
}): RecoveryRecord {
  const { run, failure, previous } = input;
  // Execution order can differ from submission order when a held queue resumes.
  const sourceEndedAt = run.completedAt === null ? null : DateTime.toEpochMillis(run.completedAt);
  const end = sourceEndedAt ?? Infinity;
  const previousEnd = previous?.sourceEndedAt ?? Infinity;
  // Terminal observers can arrive after a newer continuation already committed.
  if (
    previous !== null &&
    run.id !== previous.sourceRunId &&
    (end < previousEnd || (end === previousEnd && run.ordinal < previous.sourceOrdinal))
  )
    return input.stoppedOrRetired ? { ...previous, state: "closed", resetAt: null } : previous;
  const related = previous !== null && continues(previous, run);
  const originalRunId = related ? previous.originalRunId : run.id;
  const record: RecoveryRecord = {
    threadId: run.threadId,
    originalRunId,
    sourceRunId: run.id,
    sourceOrdinal: run.ordinal,
    sourceEndedAt,
    modelSelection: related ? previous.modelSelection : run.modelSelection,
    retryMessageId: MessageId.make(`prism-retry:${run.threadId}:${originalRunId}`),
    retryUsed: related && previous.retryUsed,
    state: "closed",
    resetAt: null,
  };
  if (input.stoppedOrRetired) return record;
  if (
    related &&
    !["completed", "failed", "interrupted", "cancelled", "rolled_back"].includes(run.status)
  )
    return {
      ...record,
      state: run.userMessageId === previous.retryMessageId ? "retry_started" : previous.state,
      resetAt: previous.resetAt,
    };
  if (run.status !== "failed" || !failure) return record;
  if (
    related &&
    previous.state === "retry_started" &&
    run.userMessageId !== previous.retryMessageId &&
    run.id === previous.sourceRunId
  )
    return previous;
  // A changed selection is a new human decision, never an automatic handoff.
  if (!sameModelSelection(record.modelSelection, run.modelSelection)) return record;
  if (failure.class === "usage_limit") {
    const resetMs = failure.resetAt == null ? NaN : Date.parse(failure.resetAt);
    const failedMs = DateTime.toEpochMillis(run.completedAt ?? run.requestedAt);
    return input.autoResume && Number.isFinite(resetMs) && resetMs > failedMs
      ? { ...record, state: "reset_wait", resetAt: failure.resetAt! }
      : record;
  }
  if (related && previous.state === "closed") return record;
  if (related && run.id === previous.sourceRunId && previous.state === "retry_pending") {
    return { ...previous };
  }
  if (record.retryUsed || failure.retryable !== true) return record;
  return { ...record, state: "retry_pending", retryUsed: true };
}

/** Eligibility is rechecked under serialized dispatch, not trusted from this record. */
export function retryCommand(record: RecoveryRecord): OrchestrationV2Command | null {
  if (record.state !== "retry_pending") return null;
  return {
    type: "message.dispatch",
    commandId: CommandId.make(String(record.retryMessageId)),
    messageId: record.retryMessageId,
    threadId: record.threadId,
    forkPrismRetryOfRunId: record.sourceRunId,
    forkPrismOriginalRunId: record.originalRunId,
    modelSelection: record.modelSelection,
    text: "Continue where you left off. The provider failure permits one retry on the same provider and model.",
    attachments: [],
    dispatchMode: { type: "start_immediately" },
    createdBy: "system",
    creationSource: "server",
  };
}
