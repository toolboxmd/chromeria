import {
  EventId,
  ProjectId,
  TurnItemId,
  MessageId,
  ProviderInstanceId,
  RunId,
  ThreadId,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Layer from "effect/Layer";
import * as Persistence from "../persistence/Sqlite.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as Projection from "../orchestration-v2/ProjectionStore.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as Store from "./RecoveryStore.ts";

const threadId = ThreadId.make("thread:recovery");
export const recoveryRun: OrchestrationV2Run = {
  id: RunId.make("run:original"),
  threadId,
  ordinal: 1,
  providerInstanceId: ProviderInstanceId.make("codex"),
  modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "fixed" },
  providerThreadId: null,
  userMessageId: MessageId.make("message:original"),
  rootNodeId: null,
  activeAttemptId: null,
  status: "failed",
  requestedAt: DateTime.makeUnsafe("2026-10-08T10:00:00Z"),
  startedAt: DateTime.makeUnsafe("2026-10-08T10:00:00Z"),
  completedAt: DateTime.makeUnsafe("2026-10-08T10:01:00Z"),
  checkpointId: null,
  contextHandoffId: null,
};

export const database = Persistence.layerMemory;
const stores = Layer.mergeAll(
  database,
  EventStore.layer.pipe(Layer.provide(database)),
  Projection.layer.pipe(Layer.provide(database)),
);
export const recoveryTestLayer = Layer.mergeAll(
  stores,
  EventSink.layer.pipe(Layer.provide(stores)),
  Store.layer.pipe(Layer.provide(database)),
);
export function threadFor(run: OrchestrationV2Run): OrchestrationV2AppThread {
  return {
    id: run.threadId,
    projectId: ProjectId.make("project:recovery"),
    title: "Recovery",
    createdBy: "agent",
    creationSource: "mcp",
    providerInstanceId: run.providerInstanceId,
    modelSelection: run.modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: run.threadId },
    forkedFrom: null,
    createdAt: run.requestedAt,
    updatedAt: run.requestedAt,
    archivedAt: null,
    settledAt: null,
    settledOverride: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
}
export function errorFor(run: OrchestrationV2Run): OrchestrationV2TurnItem {
  return {
    id: TurnItemId.make(`error:${run.id}`),
    type: "error",
    threadId: run.threadId,
    runId: run.id,
    nodeId: run.rootNodeId,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: 1,
    status: "failed",
    title: null,
    startedAt: run.requestedAt,
    completedAt: run.completedAt,
    updatedAt: run.completedAt ?? run.requestedAt,
    failure: { class: "unknown", message: "Provider failed", code: null, retryable: true },
  };
}
export function recoveryEvents(run: OrchestrationV2Run): ReadonlyArray<OrchestrationV2DomainEvent> {
  const base = { threadId: run.threadId, occurredAt: run.requestedAt };
  return [
    {
      ...base,
      id: EventId.make("event:recovery:thread"),
      type: "thread.created",
      payload: threadFor(run),
    },
    { ...base, id: EventId.make("event:recovery:run"), type: "run.created", payload: run },
    {
      ...base,
      id: EventId.make("event:recovery:failure"),
      type: "turn-item.updated",
      payload: errorFor(run),
    },
  ];
}
