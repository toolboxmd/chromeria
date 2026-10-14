import type {
  MessageId,
  ModelSelection,
  NodeId,
  OrchestrationV2AppThread,
  OrchestrationV2Actor,
  OrchestrationV2ConversationMessage,
  OrchestrationV2CreationSource,
  OrchestrationV2ProviderRef,
  OrchestrationV2Run,
  OrchestrationV2ThreadProjection,
  OrchestrationV2TurnItem,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import { runRanAfter } from "@t3tools/shared/orchestrationV2ThreadError";
import * as DateTime from "effect/DateTime";
import { isOrchestrationV2WorkActive, threadOwner } from "@t3tools/contracts";
import { restartCutView } from "../restartContinuation/RestartCut.ts";

/** Include the earlier outputs a restart replacement can refer to without a provider start. */
export function restartResultRunIds(
  runs: ReadonlyArray<OrchestrationV2Run>,
  resultRunIds: ReadonlyArray<OrchestrationV2Run["id"]>,
) {
  const ids = new Set(resultRunIds);
  const byId = new Map(runs.map((run) => [run.id, run]));
  for (const id of ids) {
    const source = byId.get(id)?.restartContinuationOfRunId;
    if (source !== undefined) ids.add(source);
  }
  return [...ids];
}

function trimmed(value: string | null | undefined): string | undefined {
  const result = value?.trim();
  return result && result.length > 0 ? result : undefined;
}

export function subagentThreadTitle(input: {
  readonly parentTitle: string;
  readonly title?: string | null;
  readonly prompt: string;
  readonly ordinal: number;
}): string {
  const detail = trimmed(input.title) ?? trimmed(input.prompt);
  if (detail === undefined) {
    return `${input.parentTitle} subagent ${input.ordinal}`;
  }
  const clipped = detail.length > 72 ? `${detail.slice(0, 69)}...` : detail;
  return clipped;
}

export function makeSubagentChildThread(input: {
  readonly parentThread: OrchestrationV2AppThread;
  readonly childThreadId: ThreadId;
  readonly parentNodeId: NodeId;
  readonly activeProviderThreadId: ProviderThreadId | null;
  readonly providerInstanceId: ProviderInstanceId;
  readonly modelSelection: ModelSelection;
  readonly title: string;
  readonly now: DateTime.Utc;
  readonly createdBy: OrchestrationV2Actor;
  readonly creationSource: OrchestrationV2CreationSource;
}): OrchestrationV2AppThread {
  return {
    ...input.parentThread,
    // Fork: children belong to the parent's owner and start unshared (toolboxmd/chromeria#170).
    owner: threadOwner(input.parentThread),
    coOwners: [],
    createdBy: input.createdBy,
    creationSource: input.creationSource,
    id: input.childThreadId,
    title: input.title,
    linkedPullRequest: null,
    pullRequests: [],
    historyOrigin: undefined,
    providerInstanceId: input.providerInstanceId,
    modelSelection: input.modelSelection,
    activeProviderThreadId: input.activeProviderThreadId,
    lineage: {
      parentThreadId: input.parentThread.id,
      relationshipToParent: "subagent",
      rootThreadId: input.parentThread.lineage.rootThreadId,
    },
    forkedFrom: {
      type: "node",
      nodeId: input.parentNodeId,
    },
    createdAt: input.now,
    updatedAt: input.now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    snoozedUntil: null,
    snoozedAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
}

export function makeSubagentConversationArtifacts(input: {
  readonly messageId: MessageId;
  readonly senderThreadId?: ThreadId;
  readonly turnItemId: TurnItemId;
  readonly threadId: ThreadId;
  readonly rootNodeId: NodeId;
  readonly providerThreadId: ProviderThreadId | null;
  readonly providerTurnId: ProviderTurnId | null;
  readonly nativeItemRef: OrchestrationV2ProviderRef | null;
  readonly role: "user" | "assistant";
  readonly text: string;
  readonly ordinal: number;
  readonly now: DateTime.Utc;
}): {
  readonly message: OrchestrationV2ConversationMessage;
  readonly turnItem: OrchestrationV2TurnItem;
} {
  const message: OrchestrationV2ConversationMessage = {
    createdBy: "agent",
    creationSource: "provider",
    id: input.messageId,
    threadId: input.threadId,
    runId: null,
    nodeId: input.rootNodeId,
    role: input.role,
    ...(input.role === "user" && input.senderThreadId !== undefined
      ? { senderThreadId: input.senderThreadId }
      : {}),
    text: input.text,
    attachments: [],
    streaming: false,
    createdAt: input.now,
    updatedAt: input.now,
  };
  const base = {
    id: input.turnItemId,
    threadId: input.threadId,
    runId: null,
    nodeId: input.rootNodeId,
    providerThreadId: input.providerThreadId,
    providerTurnId: input.providerTurnId,
    nativeItemRef: input.nativeItemRef,
    parentItemId: null,
    ordinal: input.ordinal,
    status: "completed" as const,
    title: null,
    startedAt: input.now,
    completedAt: input.now,
    updatedAt: input.now,
    messageId: input.messageId,
    text: input.text,
  };
  const turnItem: OrchestrationV2TurnItem =
    input.role === "user"
      ? {
          ...base,
          createdBy: "agent",
          creationSource: "provider",
          type: "user_message",
          ...(input.senderThreadId === undefined ? {} : { senderThreadId: input.senderThreadId }),
          inputIntent: "turn_start",
          attachments: [],
        }
      : {
          ...base,
          type: "assistant_message",
          streaming: false,
        };
  return { message, turnItem };
}

export function subagentResultForRun(
  projection: Pick<OrchestrationV2ThreadProjection, "messages" | "turnItems" | "runs">,
  run: OrchestrationV2Run,
  resolvedCut?: ReturnType<typeof restartCutView>,
): {
  readonly text: string;
  readonly messageId: OrchestrationV2ConversationMessage["id"] | null;
  readonly turnItemId: OrchestrationV2TurnItem["id"] | null;
  readonly restartNote: string | null;
} {
  const message =
    projection.messages
      .filter(
        (candidate) =>
          candidate.runId === run.id &&
          candidate.role === "assistant" &&
          candidate.text.trim().length > 0,
      )
      .toSorted(
        (left, right) =>
          DateTime.toEpochMillis(right.updatedAt) - DateTime.toEpochMillis(left.updatedAt),
      )[0] ?? null;
  const turnItem =
    projection.turnItems
      .filter(
        (
          candidate,
        ): candidate is Extract<OrchestrationV2TurnItem, { readonly type: "assistant_message" }> =>
          candidate.runId === run.id &&
          candidate.type === "assistant_message" &&
          candidate.text.trim().length > 0,
      )
      .toSorted((left, right) => right.ordinal - left.ordinal)[0] ?? null;
  const failure =
    run.status === "failed"
      ? projection.turnItems
          .filter((item) => item.runId === run.id && item.type === "error")
          .toSorted((left, right) => right.ordinal - left.ordinal)[0]
      : undefined;
  const resultText =
    (failure?.type === "error" ? failure.failure.message : undefined) ??
    message?.text ??
    turnItem?.text ??
    (run.status === "completed"
      ? "Child task completed without an assistant result."
      : `Child task ended with status ${run.status}.`);
  const cut = resolvedCut ?? restartCutView(run, projection.runs);
  const restartNote =
    run.status === "cancelled" && cut !== undefined
      ? cut.state === "declined"
        ? `A server restart cut this task and it was not continued: ${cut.reason}`
        : cut.state === "continuation_pending"
          ? "A server restart cut this task; its continuation is pending."
          : "A server restart cut this task; its continuation started."
      : run.restartContinuationOfRunId !== undefined &&
          ["completed", "failed", "cancelled", "interrupted"].includes(run.status)
        ? `A server restart cut this task; its continuation ${run.status} ${run.startedAt === null ? "before starting" : "after starting"}.`
        : null;
  let partial = message?.text ?? turnItem?.text;
  if (restartNote !== null && run.startedAt === null && partial === undefined) {
    const sources = restartResultRunIds(projection.runs, [run.id]).filter((id) => id !== run.id);
    for (const sourceId of sources) {
      const sourceMessage = projection.messages
        .filter(
          (candidate) =>
            candidate.runId === sourceId &&
            candidate.role === "assistant" &&
            candidate.text.trim().length > 0,
        )
        .toSorted(
          (a, b) => DateTime.toEpochMillis(b.updatedAt) - DateTime.toEpochMillis(a.updatedAt),
        )[0];
      const sourceItem = projection.turnItems
        .filter(
          (item): item is Extract<OrchestrationV2TurnItem, { type: "assistant_message" }> =>
            item.runId === sourceId &&
            item.type === "assistant_message" &&
            item.text.trim().length > 0,
        )
        .toSorted((a, b) => b.ordinal - a.ordinal)[0];
      partial = sourceMessage?.text ?? sourceItem?.text;
      if (partial !== undefined) break;
    }
  }
  const text =
    restartNote === null
      ? resultText
      : run.restartContinuationOfRunId !== undefined &&
          run.startedAt !== null &&
          run.status !== "cancelled"
        ? `${restartNote}\n\n${resultText}`
        : partial === undefined
          ? restartNote
          : `${restartNote}\n\nLast output before the restart:\n${partial}`;
  return {
    text,
    restartNote,
    messageId: failure === undefined ? (message?.id ?? turnItem?.messageId ?? null) : null,
    turnItemId: failure?.id ?? turnItem?.id ?? null,
  };
}

/** A finished turn can still own live children or queued completion follow-ups. */
export function delegatedTaskProgress(projection: {
  readonly runs: OrchestrationV2ThreadProjection["runs"];
  readonly messages: ReadonlyArray<
    Pick<OrchestrationV2ConversationMessage, "runId" | "notification">
  >;
  readonly subagents: ReadonlyArray<
    Pick<OrchestrationV2ThreadProjection["subagents"][number], "status" | "completionDelivery">
  >;
  readonly providerThreads: ReadonlyArray<
    Pick<OrchestrationV2ThreadProjection["providerThreads"][number], "pendingBackgroundTasks">
  >;
}) {
  const terminal = (status: string) =>
    ["completed", "failed", "cancelled", "interrupted", "rolled_back"].includes(status);
  const monitorRuns = new Set(
    projection.messages
      .filter((message) => message.notification?.source.kind === "monitor")
      .map((message) => message.runId),
  );
  const workRuns = projection.runs.filter(
    (run) => !monitorRuns.has(run.id) && run.status !== "rolled_back",
  );
  // A held queue waits for the user to resume it (after Stop, a restart, or a
  // provider failure), so its runs are not work the task still owes.
  const active = workRuns.some(
    (run) => !terminal(run.status) && !(run.status === "queued" && run.queueHeld === true),
  );
  const children =
    projection.subagents.some(
      (task) =>
        isOrchestrationV2WorkActive(task.status) ||
        // Publishing a child's result precedes scheduling its parent's wake.
        // The parent still owes that follow-up even between those transactions.
        task.completionDelivery?.state === "pending" ||
        task.completionDelivery?.state === "claimed",
    ) ||
    projection.providerThreads.some((thread) => (thread.pendingBackgroundTasks?.length ?? 0) > 0);
  const resultRun = workRuns
    .filter(
      (run) =>
        terminal(run.status) &&
        (run.startedAt !== null ||
          run.ordinal === 1 ||
          run.restartContinuationOfRunId !== undefined ||
          (run.status === "cancelled" && run.restartCut !== undefined)),
    )
    .toSorted((a, b) => (runRanAfter(a, b) ? -1 : runRanAfter(b, a) ? 1 : 0))[0];
  return {
    state:
      active || resultRun === undefined
        ? ("working" as const)
        : children
          ? ("waiting_for_children" as const)
          : ("result_available" as const),
    resultRun,
  };
}
