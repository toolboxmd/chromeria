import * as NodeEvents from "node:events";
import {
  EventId,
  ProjectId,
  ProviderInstanceId,
  type OrchestrationV2DomainEvent,
  type ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";

const events = new NodeEvents.EventEmitter();
export const settlementLayer = Layer.mock(Orchestrator.OrchestratorV2)({
  streamDomainEvents: Stream.callback<OrchestrationV2DomainEvent>((queue) =>
    Effect.acquireRelease(
      Effect.sync(() => {
        const listener = (event: OrchestrationV2DomainEvent) => Queue.offerUnsafe(queue, event);
        events.on("event", listener);
        return listener;
      }),
      (listener) => Effect.sync(() => events.off("event", listener)),
    ),
  ),
});

export const settleThread = (threadId: ThreadId) =>
  Effect.sync(() => {
    const now = DateTime.makeUnsafe("2026-10-20T12:00:00Z");
    const event: OrchestrationV2DomainEvent = {
      id: EventId.make(`settled:${threadId}`),
      type: "thread.settled",
      threadId,
      occurredAt: now,
      payload: {
        id: threadId,
        projectId: ProjectId.make("browser-project"),
        title: "Browser thread",
        providerInstanceId: ProviderInstanceId.make("codex"),
        modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-6.1" },
        createdBy: "user",
        creationSource: "web",
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        activeProviderThreadId: null,
        lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
        archivedAt: null,
        settledOverride: "settled",
        settledAt: now,
        lastVisitedAt: null,
        deletedAt: null,
      },
    };
    events.emit("event", event);
  });
