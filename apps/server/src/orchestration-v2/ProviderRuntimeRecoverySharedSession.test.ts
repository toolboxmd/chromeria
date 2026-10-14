import { assert, it } from "@effect/vitest";
import {
  CheckpointScopeId,
  CommandId,
  EventId,
  MessageId,
  NodeId,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ServerSettings from "../serverSettings.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderRuntimeRecovery from "./ProviderRuntimeRecoveryService.ts";

// `it.live` keeps a real clock: each reconcile gets a fresh command id, so the
// second recover cannot hide behind command receipt dedup.

const layerStores = Layer.mergeAll(
  EventStore.layer,
  ProjectionStore.layer,
  EffectOutbox.layer,
  IdAllocator.layer,
).pipe(Layer.provideMerge(SqlitePersistence.layerMemory));
const layerTest = ProviderRuntimeRecovery.layer.pipe(
  Layer.provideMerge(EventSink.layer.pipe(Layer.provideMerge(layerStores))),
  Layer.provideMerge(ServerSettings.layerTest({ continueThreadsAfterServerUpdate: true })),
);

const providerInstanceId = ProviderInstanceId.make("codex");
const driver = ProviderDriverKind.make("codex");
const modelSelection = { instanceId: providerInstanceId, model: "gpt-5.4" };
const projectId = ProjectId.make("project:recovery-bench");
const ITEMS_PER_SETTLED_THREAD = 10;
const OPEN_ITEMS_PER_ACTIVE_THREAD = 3;
const WAITING_THREADS = 3;
const SEED_BATCH = 500;

type UnstampedEvent = OrchestrationV2DomainEvent extends infer Event
  ? Event extends OrchestrationV2DomainEvent
    ? Omit<Event, "id" | "occurredAt">
    : never
  : never;

interface Scenario {
  readonly settled: number;
  readonly active: number;
  /**
   * Bind every active thread, plus an older idle thread, to one session the
   * way Codex shares a session per provider instance.
   */
  readonly sharedSessionStatus?: "ready" | "stopped";
  readonly borrowedProviderThread?: boolean;
}

interface Seeded {
  readonly unheldQueuedThreads: number;
  readonly waitingThreads: number;
}

const seedScenario = Effect.fn(function* (scenario: Scenario) {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const outbox = yield* EffectOutbox.EffectOutboxV2;
  const sql = yield* SqlClient.SqlClient;
  const now = yield* DateTime.now;
  let eventIndex = 0;
  const apply = (event: UnstampedEvent) =>
    projections.apply({
      ...event,
      id: EventId.make(`event:bench:${eventIndex++}`),
      occurredAt: now,
    } as OrchestrationV2DomainEvent);

  const thread = (id: ThreadId, overrides: Partial<OrchestrationV2AppThread> = {}) =>
    apply({
      type: "thread.created",
      threadId: id,
      payload: {
        createdBy: "user",
        creationSource: "web",
        id,
        projectId,
        title: id,
        providerInstanceId,
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        activeProviderThreadId: null,
        lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: id },
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
        archivedAt: null,
        settledOverride: null,
        settledAt: null,
        lastVisitedAt: null,
        deletedAt: null,
        ...overrides,
      },
    });

  const run = (
    threadId: ThreadId,
    ordinal: number,
    status: OrchestrationV2Run["status"],
    overrides: Partial<OrchestrationV2Run> = {},
  ) => {
    const runId = RunId.make(`run:${threadId}:${ordinal}`);
    return apply({
      type: "run.created",
      threadId,
      runId,
      payload: {
        id: runId,
        threadId,
        ordinal,
        providerInstanceId,
        modelSelection,
        providerThreadId: null,
        userMessageId: MessageId.make(`message:${runId}`),
        rootNodeId: null,
        activeAttemptId: null,
        status,
        requestedAt: now,
        startedAt: status === "queued" ? null : now,
        completedAt: status === "completed" ? now : null,
        checkpointId: null,
        contextHandoffId: null,
        ...overrides,
      },
    }).pipe(Effect.as(runId));
  };

  const commandItem = (input: {
    readonly threadId: ThreadId;
    readonly runId: RunId;
    readonly ordinal: number;
    readonly status: "running" | "completed";
    readonly nodeId?: NodeId;
    readonly providerThreadId?: ProviderThreadId;
    readonly providerTurnId?: ProviderTurnId;
  }) =>
    apply({
      type: "turn-item.updated",
      threadId: input.threadId,
      runId: input.runId,
      payload: {
        id: TurnItemId.make(`item:${input.runId}:${input.ordinal}`),
        threadId: input.threadId,
        runId: input.runId,
        nodeId: input.nodeId ?? null,
        providerThreadId: input.providerThreadId ?? null,
        providerTurnId: input.providerTurnId ?? null,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: input.ordinal,
        type: "command_execution",
        status: input.status,
        title: `Command ${input.ordinal}`,
        input: `echo ${input.ordinal}`,
        output: input.status === "completed" ? `${input.ordinal}\n` : "",
        ...(input.status === "completed" ? { exitCode: 0 } : {}),
        startedAt: now,
        completedAt: input.status === "completed" ? now : null,
        updatedAt: now,
      },
    });

  const settledThread = Effect.fn(function* (index: number) {
    const threadId = ThreadId.make(`thread:bench:settled:${index}`);
    yield* thread(threadId);
    const runId = yield* run(threadId, 1, "completed");
    for (let ordinal = 1; ordinal <= ITEMS_PER_SETTLED_THREAD; ordinal += 1) {
      yield* commandItem({ threadId, runId, ordinal, status: "completed" });
    }
    if (index % 20 === 0) {
      const queueHeld = index % 40 === 0;
      yield* run(threadId, 2, "queued", { queueHeld });
      return !queueHeld;
    }
    return false;
  });

  const attachSession = (threadId: ThreadId, providerSessionId: ProviderSessionId) =>
    apply({
      type: "provider-session.attached",
      threadId,
      driver,
      providerInstanceId,
      payload: {
        id: providerSessionId,
        driver,
        providerInstanceId,
        status: scenario.sharedSessionStatus ?? "ready",
        cwd: "/workspace",
        model: modelSelection.model,
        capabilities: CodexProviderCapabilitiesV2,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      },
    });
  const sharedSessionId = ProviderSessionId.make("session:bench:shared");

  const activeThread = Effect.fn(function* (index: number) {
    const threadId = ThreadId.make(`thread:bench:active:${index}`);
    const providerSessionId =
      scenario.sharedSessionStatus === undefined
        ? ProviderSessionId.make(`session:bench:${index}`)
        : sharedSessionId;
    const providerThreadId = ProviderThreadId.make(`provider-thread:bench:${index}`);
    const ordinal = scenario.borrowedProviderThread && index === 0 ? 2 : 1;
    const runId = RunId.make(`run:${threadId}:${ordinal}`);
    const attemptId = RunAttemptId.make(`attempt:bench:${index}`);
    const nodeId = NodeId.make(`node:bench:${index}`);
    const providerTurnId = ProviderTurnId.make(`provider-turn:bench:${index}`);
    yield* thread(threadId, { activeProviderThreadId: providerThreadId });
    if (ordinal === 2) yield* run(threadId, 1, "completed", { providerThreadId });
    yield* attachSession(threadId, providerSessionId);
    yield* apply({
      type: "provider-thread.updated",
      threadId,
      driver,
      providerInstanceId,
      payload: {
        id: providerThreadId,
        appThreadId: threadId,
        ownerNodeId: null,
        driver,
        providerInstanceId,
        providerSessionId,
        nativeThreadRef: { driver, nativeId: `native:bench:${index}`, strength: "strong" },
        nativeConversationHeadRef: null,
        status: "active",
        firstRunOrdinal: 1,
        lastRunOrdinal: ordinal,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
        pendingBackgroundTasks: [],
      },
    });
    yield* run(threadId, ordinal, "running", {
      providerThreadId,
      rootNodeId: nodeId,
      activeAttemptId: attemptId,
    });
    yield* apply({
      type: "run-attempt.created",
      threadId,
      runId,
      payload: {
        id: attemptId,
        runId,
        attemptOrdinal: 1,
        rootNodeId: nodeId,
        providerInstanceId,
        providerThreadId,
        providerTurnId,
        reason: "initial",
        status: "running",
        startedAt: now,
        completedAt: null,
      },
    });
    yield* apply({
      type: "node.updated",
      threadId,
      runId,
      nodeId,
      providerInstanceId,
      payload: {
        id: nodeId,
        threadId,
        runId,
        parentNodeId: null,
        rootNodeId: nodeId,
        kind: "root_turn",
        status: "running",
        countsForRun: true,
        providerThreadId,
        providerTurnId,
        nativeItemRef: null,
        runtimeRequestId: null,
        checkpointScopeId: null,
        startedAt: now,
        completedAt: null,
      },
    });
    yield* apply({
      type: "provider-turn.updated",
      threadId,
      runId,
      nodeId,
      providerInstanceId,
      payload: {
        id: providerTurnId,
        providerThreadId,
        nodeId,
        runAttemptId: attemptId,
        nativeTurnRef: { driver, nativeId: `native-turn:bench:${index}`, strength: "strong" },
        ordinal: 1,
        status: "running",
        startedAt: now,
        completedAt: null,
      },
    });
    for (let ordinal = 1; ordinal <= OPEN_ITEMS_PER_ACTIVE_THREAD; ordinal += 1) {
      yield* commandItem({
        threadId,
        runId,
        ordinal,
        status: "running",
        nodeId,
        providerThreadId,
        providerTurnId,
      });
    }
  });

  const waitingThread = Effect.fn(function* (index: number) {
    const threadId = ThreadId.make(`thread:bench:waiting:${index}`);
    yield* thread(threadId);
    yield* run(threadId, 1, "completed");
    const runId = yield* run(threadId, 2, "waiting");
    yield* outbox.enqueue([
      {
        id: `effect:bench:checkpoint:${runId}`,
        commandId: CommandId.make(`command:effect:checkpoint.capture:${runId}`),
        threadId,
        request: {
          type: "checkpoint.capture",
          runId,
          scopeId: CheckpointScopeId.make(`scope:bench:${index}`),
        },
      },
    ]);
  });

  let unheldQueuedThreads = 0;
  for (let start = 0; start < scenario.settled; start += SEED_BATCH) {
    const end = Math.min(scenario.settled, start + SEED_BATCH);
    yield* sql.withTransaction(
      Effect.gen(function* () {
        for (let index = start; index < end; index += 1) {
          if (yield* settledThread(index)) unheldQueuedThreads += 1;
        }
      }),
    );
  }
  yield* sql.withTransaction(
    Effect.gen(function* () {
      if (scenario.sharedSessionStatus !== undefined) {
        // Recovery visits threads oldest first, so this one reconciles first.
        const idleThreadId = ThreadId.make("thread:bench:0-idle");
        yield* thread(idleThreadId, { updatedAt: DateTime.subtract(now, { hours: 1 }) });
        yield* run(idleThreadId, 1, "completed");
        yield* attachSession(idleThreadId, sharedSessionId);
      }
      for (let index = 0; index < scenario.active; index += 1) yield* activeThread(index);
      if (scenario.borrowedProviderThread) {
        const parentThreadId = ThreadId.make("thread:bench:0-parent");
        const childThreadId = ThreadId.make("thread:bench:active:0");
        yield* thread(parentThreadId, { updatedAt: DateTime.subtract(now, { hours: 1 }) });
        const parentRunId = yield* run(parentThreadId, 1, "completed");
        yield* apply({
          type: "subagent.updated",
          threadId: parentThreadId,
          payload: {
            id: NodeId.make("node:bench:parent-task"),
            threadId: parentThreadId,
            runId: parentRunId,
            parentNodeId: NodeId.make("node:bench:parent-root"),
            origin: "app_owned",
            createdBy: "agent",
            driver,
            providerInstanceId,
            providerThreadId: ProviderThreadId.make("provider-thread:bench:0"),
            childThreadId,
            nativeTaskRef: null,
            prompt: "Complete the first delegated turn.",
            title: "Completed delegation with a live child follow-up",
            model: modelSelection.model,
            status: "completed",
            result: "First turn complete.",
            startedAt: now,
            completedAt: now,
            updatedAt: now,
          },
        });
      }
      for (let index = 0; index < WAITING_THREADS; index += 1) yield* waitingThread(index);
    }),
  );
  return { unheldQueuedThreads, waitingThreads: WAITING_THREADS } satisfies Seeded;
});

const continuationEffectCount = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ readonly count: number }>`
    SELECT COUNT(*) AS count FROM orchestration_v2_effect_outbox
    WHERE effect_id LIKE 'effect:restart-continuation:%' AND status = 'pending'
  `;
  return Number(rows[0]?.count ?? 0);
});

it.live("startup recovery continues every running thread on a shared provider session", () =>
  Effect.gen(function* () {
    const active = 3;
    yield* seedScenario({ settled: 0, active, sharedSessionStatus: "ready" });
    const recovery = yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService;
    const summary = yield* recovery.recover;
    assert.equal(summary.terminalizedRuns, active, "terminalized active runs");
    assert.equal(summary.stoppedSessions, 1, "the shared session stops once");
    assert.equal(yield* continuationEffectCount, active, "recorded continuations");
  }).pipe(Effect.provide(Layer.fresh(layerTest))),
);

it.live(
  "fresh recovery continues live turns after an earlier recovery stopped their shared session",
  () =>
    Effect.gen(function* () {
      yield* seedScenario({ settled: 0, active: 3, sharedSessionStatus: "stopped" });
      const recovery = yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService;
      const summary = yield* recovery.recover;
      assert.equal(summary.terminalizedRuns, 3, "terminalized active runs");
      assert.equal(summary.stoppedSessions, 0, "no live session to stop");
      assert.equal(
        yield* continuationEffectCount,
        3,
        "the remaining own turns still need continuation",
      );
    }).pipe(Effect.provide(Layer.fresh(layerTest))),
);

it.live("parent recovery preserves its child's live provider thread for continuation", () =>
  Effect.gen(function* () {
    yield* seedScenario({ settled: 0, active: 1, borrowedProviderThread: true });
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const ids = yield* projections.getRecoveryThreadIds("runtime");
    assert.equal(ids[0], "thread:bench:0-parent", "the parent reconciles before the child");
    yield* (yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService).recover;
    assert.equal(yield* continuationEffectCount, 1, "the child's follow-up is continued");
  }).pipe(Effect.provide(Layer.fresh(layerTest))),
);
