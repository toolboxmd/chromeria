import {
  EventId,
  NodeId,
  type ModelSelection,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Subagent,
  type ProviderDriverKind,
} from "@t3tools/contracts";
import type * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import type { SpectrumState } from "./state.ts";

export type SpectrumTaskOutcome = "completed" | "failed" | "cancelled";

export const spectrumTaskId = (state: Pick<SpectrumState, "threadId">) =>
  NodeId.make(`${state.threadId}:task`);

/**
 * The caller's row for an agent-started Spectrum. It has no completion delivery:
 * the report's own dispatch is the caller's only wake.
 */
export function spectrumTaskRow(input: {
  readonly state: SpectrumState;
  readonly caller: OrchestrationV2AppThread;
  readonly parentNodeId: NodeId | null;
  readonly driver: ProviderDriverKind;
  readonly selection: ModelSelection;
  readonly title: string;
  readonly now: DateTime.Utc;
}): OrchestrationV2Subagent {
  const id = spectrumTaskId(input.state);
  return {
    id,
    threadId: input.caller.id,
    runId: input.state.callerRunId,
    // A caller between runs has no node; the row then roots at itself.
    parentNodeId: input.parentNodeId ?? id,
    origin: "app_owned",
    createdBy: "agent",
    driver: input.driver,
    providerInstanceId: input.selection.instanceId,
    providerThreadId: null,
    childThreadId: input.state.threadId,
    nativeTaskRef: null,
    prompt: input.state.question,
    title: input.title,
    model: null,
    status: "running",
    result: null,
    startedAt: input.now,
    completedAt: null,
    updatedAt: input.now,
  };
}

export const spectrumTaskEvent = (
  task: OrchestrationV2Subagent,
  key: string,
  now: DateTime.Utc,
): OrchestrationV2DomainEvent => ({
  id: EventId.make(`${key}:spectrum:task`),
  type: "subagent.updated",
  threadId: task.threadId,
  ...(task.runId === null ? {} : { runId: task.runId }),
  nodeId: task.id,
  driver: task.driver,
  providerInstanceId: task.providerInstanceId,
  occurredAt: now,
  payload: task,
});

/**
 * Every Spectrum transition that makes or clears a report mirrors it here, in
 * the same commit. Terminal rows always carry a result: task_status reads a
 * terminal status only beside one.
 */
export const spectrumTaskUpdate = Effect.fn("Spectrum.taskUpdate")(function* (
  state: SpectrumState,
  update:
    | { readonly status: SpectrumTaskOutcome; readonly result: string }
    | { readonly status: "running" },
  key: string,
  now: DateTime.Utc,
) {
  if (state.delegatedTaskId === null) return [];
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const { subagents } = yield* projections.getThreadRecords(state.callerThreadId, ["subagents"]);
  const task = subagents.find(
    (candidate) =>
      candidate.id === state.delegatedTaskId &&
      candidate.origin === "app_owned" &&
      candidate.childThreadId === state.threadId,
  );
  if (task === undefined) return [];
  const terminal = update.status !== "running";
  return [
    spectrumTaskEvent(
      {
        ...task,
        status: update.status,
        result: terminal ? update.result : null,
        completedAt: terminal ? now : null,
        updatedAt: now,
      },
      key,
      now,
    ),
  ];
});
