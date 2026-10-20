import { assert, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as Deferred from "effect/Deferred";
import * as Queue from "effect/Queue";
import {
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  RunId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  NodeId,
  RuntimeRequestId,
  type OrchestrationV2ProviderFailure,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as SqlClient from "effect/sql/SqlClient";
import * as Effect from "effect/Effect";
import * as TestClock from "effect/testing/TestClock";
import * as Layer from "effect/Layer";
import * as Projection from "../orchestration-v2/ProjectionStore.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as CommandReceipts from "../orchestration-v2/CommandReceiptStore.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as Registry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import type { ProviderAdapterV2 } from "@t3tools/provider-core/server/ProviderAdapter";
import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as Harness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as Persistence from "../persistence/Sqlite.ts";
import * as Settings from "../serverSettings.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import { ServerActivation } from "../serverActivation.ts";
import * as Hooks from "./RecoveryHooks.ts";
import * as Coordinator from "./RecoveryCoordinator.ts";
import * as History from "./RecoveryHistory.ts";
import * as Reactor from "./RecoveryReactor.ts";
import { limitRecoveryCommand } from "../orchestration-v2/UsageLimitRecoveryWorker.ts";
import { errorFor, threadFor } from "./recovery.testkit.ts";

const instanceId = ProviderInstanceId.make("codex");
const selection = {
  instanceId,
  model: "fixed",
  options: [{ id: "reasoningEffort", value: "high" }],
};
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("This proof uses durable dispatch, not a live provider"),
} satisfies ProviderAdapterV2["Service"];
const database = Persistence.layerMemory;
const foundation = Layer.mergeAll(
  database,
  Settings.layerTest(),
  CommandReceipts.layer.pipe(Layer.provide(database)),
  Projection.layer.pipe(Layer.provide(database)),
);
const recovery = Coordinator.layer.pipe(Layer.provide(foundation));
const hooks = Hooks.layer.pipe(Layer.provide(Layer.mergeAll(foundation, recovery)));
const replay = Harness.layerWithRegistry(
  { name: "prism-recovery" },
  Registry.layerFromAdapters([adapter]),
  {
    databaseLayer: database,
    runEffectWorker: false,
  },
).pipe(Layer.provide(hooks));
const dependencies = Layer.mergeAll(
  foundation,
  recovery,
  hooks,
  replay,
  Threads.layer.pipe(Layer.provide(Layer.merge(replay, foundation))),
  Layer.mock(Scheduler.Scheduler)({ register: () => Effect.void }),
  Layer.succeed(ServerActivation, Effect.never),
);
const layer = Layer.mergeAll(dependencies, Reactor.layer.pipe(Layer.provide(dependencies)));
const parentId = ThreadId.make("parent:recovery");
const setup = Effect.gen(function* () {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  yield* orchestrator.dispatch({
    type: "thread.create",
    commandId: CommandId.make("parent:create"),
    threadId: parentId,
    projectId: ProjectId.make("project:recovery"),
    title: "Parent",
    modelSelection: selection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdBy: "user",
    creationSource: "web",
  });
  yield* orchestrator.dispatch({
    type: "message.dispatch",
    commandId: CommandId.make("parent:start"),
    threadId: parentId,
    messageId: MessageId.make("parent:message"),
    text: "Delegate",
    attachments: [],
    dispatchMode: { type: "start_immediately" },
    createdBy: "user",
    creationSource: "web",
  });
  const parent = (yield* orchestrator.getThreadProjection(parentId)).runs[0]!;
  yield* orchestrator.dispatch({
    type: "delegated_task.request",
    commandId: CommandId.make("child:delegate"),
    parentThreadId: parentId,
    parentRunId: parent.id,
    parentNodeId: parent.rootNodeId!,
    task: "Work",
    modelSelection: selection,
    runtimeMode: "full-access",
    interactionMode: "default",
    completionWake: "always",
    createdBy: "agent",
    creationSource: "mcp",
  });
  const childId = (yield* orchestrator.getThreadProjection(parentId)).subagents[0]!.childThreadId!;
  return {
    orchestrator,
    childId,
    run: (yield* orchestrator.getThreadProjection(childId)).runs[0]!,
  };
});
const finish = (run: OrchestrationV2Run, failure: OrchestrationV2ProviderFailure | null) =>
  Effect.gen(function* () {
    const projections = yield* Projection.ProjectionStoreV2;
    const now = yield* DateTime.now;
    const ended = {
      ...run,
      startedAt: run.startedAt ?? run.requestedAt,
      status: failure === null ? ("completed" as const) : ("failed" as const),
      completedAt: now,
    };
    if (failure !== null) {
      const item = errorFor(ended);
      if (item.type !== "error") throw new Error("fixture");
      yield* projections.apply({
        id: EventId.make(`error:${run.id}`),
        type: "turn-item.updated",
        threadId: run.threadId,
        occurredAt: now,
        payload: { ...item, failure },
      });
    }
    yield* projections.apply({
      id: EventId.make(`ended:${run.id}`),
      type: "run.updated",
      threadId: run.threadId,
      occurredAt: now,
      payload: ended,
    });
  });
const failure = {
  class: "unknown" as const,
  message: "Retryable machine failure",
  code: null,
  retryable: true,
};
it.effect(
  "user transport recovery backs off indefinitely, publishes waits and resets after success",
  () =>
    Effect.gen(function* () {
      const { orchestrator } = yield* setup;
      let source = (yield* orchestrator.getThreadProjection(parentId)).runs[0]!;
      const transport = { ...failure, class: "transport_error" as const };
      const reactor = yield* Reactor.RecoveryReactor;
      const projections = yield* Projection.ProjectionStoreV2;
      let count = 1;
      for (const minutes of [1, 2, 5, 15, 30, 30, 30]) {
        yield* finish(source, transport);
        yield* reactor.sweep;
        const waiting = yield* orchestrator.getThreadProjection(parentId);
        const now = DateTime.toEpochMillis(yield* DateTime.now);
        assert.deepStrictEqual(waiting.thread.transportRecovery, {
          runId: source.id,
          retryAt: now + minutes * 60_000,
        });
        assert.deepStrictEqual(
          (yield* projections.getShellSnapshot()).threads.find((t) => t.id === parentId)
            ?.transportRecovery,
          waiting.thread.transportRecovery,
        );
        yield* TestClock.adjust(minutes * 60_000 - 1);
        yield* reactor.sweep;
        assert.lengthOf((yield* orchestrator.getThreadProjection(parentId)).runs, count);
        yield* TestClock.adjust(1);
        yield* reactor.sweep;
        const retried = yield* orchestrator.getThreadProjection(parentId);
        assert.lengthOf(retried.runs, ++count);
        const next = retried.runs.at(-1)!;
        assert.deepStrictEqual(next.modelSelection, selection);
        assert.strictEqual(next.forkPrismContinuationSourceRunId, source.id);
        source = next;
        // Observe an active continuation before its failure, as the live event stream does.
        yield* reactor.sweep;
        assert.strictEqual(
          (yield* orchestrator.getThreadProjection(parentId)).thread.transportRecovery,
          null,
        );
      }
      yield* finish(source, null);
      yield* reactor.sweep;
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("after-success"),
        threadId: parentId,
        messageId: MessageId.make("after-success"),
        text: "Next task",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        createdBy: "user",
        creationSource: "web",
      });
      const fresh = (yield* orchestrator.getThreadProjection(parentId)).runs.at(-1)!;
      yield* finish(fresh, transport);
      yield* reactor.sweep;
      assert.strictEqual(
        (yield* orchestrator.getThreadProjection(parentId)).thread.transportRecovery?.retryAt,
        DateTime.toEpochMillis(yield* DateTime.now) + 60_000,
      );
    }).pipe(Effect.provide(layer)),
);

it.effect("Stop during the transport delay prevents the scheduled retry", () =>
  Effect.gen(function* () {
    const { orchestrator } = yield* setup;
    const source = (yield* orchestrator.getThreadProjection(parentId)).runs[0]!;
    yield* finish(source, { ...failure, class: "transport_error" });
    const reactor = yield* Reactor.RecoveryReactor;
    yield* reactor.sweep;
    yield* orchestrator.dispatch({
      type: "run.interrupt",
      commandId: CommandId.make("stop:waiting"),
      threadId: parentId,
      runId: source.id,
      holdQueue: true,
    });
    yield* TestClock.adjust(60_000);
    yield* reactor.sweep;
    assert.lengthOf((yield* orchestrator.getThreadProjection(parentId)).runs, 1);
    assert.strictEqual(
      yield* (yield* Coordinator.RecoveryCoordinator).holdsResult(parentId),
      false,
    );
  }).pipe(Effect.provide(layer)),
);

it.effect("a user takeover stays cancelled when a stale active retry is reconciled", () =>
  Effect.gen(function* () {
    const { orchestrator } = yield* setup;
    const source = (yield* orchestrator.getThreadProjection(parentId)).runs[0]!;
    const transport = { ...failure, class: "transport_error" as const };
    const reactor = yield* Reactor.RecoveryReactor;
    const coordinator = yield* Coordinator.RecoveryCoordinator;
    yield* finish(source, transport);
    yield* reactor.sweep;
    yield* TestClock.adjust(60_000);
    yield* reactor.sweep;
    const stale = yield* orchestrator.getThreadProjection(parentId);
    const retry = stale.runs.at(-1)!;
    yield* coordinator.observe(stale, false);
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make("takeover:active"),
      threadId: parentId,
      messageId: MessageId.make("takeover:active"),
      text: "Take over",
      attachments: [],
      dispatchMode: { type: "queue_after_active" },
      createdBy: "user",
      creationSource: "web",
    });
    yield* coordinator.observe(stale, false);
    assert.strictEqual(yield* coordinator.holdsResult(parentId), false);
    yield* finish(retry, transport);
    yield* reactor.sweep;
    yield* TestClock.adjust(3_600_000);
    yield* reactor.sweep;
    assert.lengthOf(
      (yield* orchestrator.getThreadProjection(parentId)).runs.filter(
        (r) => r.forkPrismContinuationSourceRunId === retry.id,
      ),
      0,
    );
  }).pipe(Effect.provide(layer)),
);

it.effect.each(["admitted", "observed", "wait-cleared"] as const)(
  "Stop for a waiting source reaches its admitted retry (%s)",
  (stage) =>
    Effect.gen(function* () {
      const { orchestrator } = yield* setup;
      const source = (yield* orchestrator.getThreadProjection(parentId)).runs[0]!;
      const transport = { ...failure, class: "transport_error" as const };
      const reactor = yield* Reactor.RecoveryReactor;
      yield* finish(source, transport);
      yield* reactor.sweep;
      yield* TestClock.adjust(60_000);
      yield* reactor.sweep;
      const admitted = yield* orchestrator.getThreadProjection(parentId);
      const retry = admitted.runs.at(-1)!;
      if (stage !== "admitted")
        yield* (yield* Coordinator.RecoveryCoordinator).observe(admitted, false);
      if (stage === "wait-cleared") {
        yield* reactor.sweep;
        assert.isNull((yield* orchestrator.getThreadProjection(parentId)).thread.transportRecovery);
      }
      yield* orchestrator.dispatch({
        type: "run.interrupt",
        commandId: CommandId.make("stop:deadline"),
        threadId: parentId,
        runId: source.id,
        holdQueue: true,
      });
      assert.strictEqual(
        (yield* orchestrator.getThreadProjection(parentId)).runs.find((r) => r.id === retry.id)
          ?.status,
        "interrupted",
      );
      yield* reactor.sweep;
      assert.strictEqual(
        yield* (yield* Coordinator.RecoveryCoordinator).holdsResult(parentId),
        false,
      );
      yield* TestClock.adjust(3_600_000);
      yield* reactor.sweep;
      assert.lengthOf(
        (yield* orchestrator.getThreadProjection(parentId)).runs.filter(
          (r) => r.forkPrismContinuationSourceRunId === retry.id,
        ),
        0,
      );
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["message", "archive", "delete", "settle", "request", "model"] as const)(
  "%s cancels a waiting transport retry",
  (action) =>
    Effect.gen(function* () {
      const { orchestrator } = yield* setup;
      const source = (yield* orchestrator.getThreadProjection(parentId)).runs[0]!;
      yield* finish(source, { ...failure, class: "transport_error" });
      const reactor = yield* Reactor.RecoveryReactor;
      yield* reactor.sweep;
      const projections = yield* Projection.ProjectionStoreV2;
      const thread = (yield* orchestrator.getThreadProjection(parentId)).thread;
      const now = yield* DateTime.now;
      if (action === "message") {
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("takeover"),
          messageId: MessageId.make("takeover"),
          threadId: parentId,
          text: "Do something else",
          attachments: [],
          createdBy: "user",
          creationSource: "web",
          dispatchMode: { type: "queue_after_active" },
        });
      } else if (action === "request") {
        yield* projections.apply({
          id: EventId.make("block:request"),
          type: "runtime-request.updated",
          threadId: parentId,
          occurredAt: now,
          payload: {
            id: RuntimeRequestId.make("block:request"),
            nodeId: NodeId.make("block:node"),
            providerTurnId: null,
            nativeRequestRef: null,
            kind: "user_input",
            status: "pending",
            responseCapability: { type: "message" },
            createdAt: now,
            resolvedAt: null,
          },
        });
      } else {
        yield* projections.apply({
          id: EventId.make(`block:${action}`),
          type: "thread.metadata-updated",
          threadId: parentId,
          occurredAt: now,
          payload: {
            ...thread,
            ...(action === "archive" ? { archivedAt: now } : {}),
            ...(action === "delete" ? { deletedAt: now } : {}),
            ...(action === "settle" ? { settledOverride: "settled" } : {}),
            ...(action === "model" ? { modelSelection: { ...selection, model: "changed" } } : {}),
          },
        });
      }
      yield* TestClock.adjust(3_600_000);
      yield* reactor.sweep;
      const result = yield* orchestrator.getThreadProjection(parentId);
      assert.lengthOf(
        result.runs.filter((r) => r.forkPrismContinuationSourceRunId !== undefined),
        0,
      );
      assert.strictEqual(
        yield* (yield* Coordinator.RecoveryCoordinator).holdsResult(parentId),
        false,
      );
      if (action !== "delete") assert.isNull(result.thread.transportRecovery);
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "actual delegated finalization holds the first failure and transfers only the exhausted retry",
  () =>
    Effect.gen(function* () {
      const { orchestrator, childId, run } = yield* setup;
      yield* finish(run, failure);
      assert.strictEqual(
        (yield* orchestrator.getThreadProjection(childId)).thread.creationSource,
        "mcp",
      );
      assert.include(
        yield* (yield* Projection.ProjectionStoreV2).getRecoveryThreadIds("subagent-results"),
        childId,
      );
      yield* orchestrator.recoverDelegatedTasks;
      assert.strictEqual(
        (yield* orchestrator.getThreadProjection(parentId)).contextTransfers.filter(
          (x) => x.type === "subagent_result",
        ).length,
        0,
      );
      yield* (yield* Reactor.RecoveryReactor).sweep;
      const sql = yield* SqlClient.SqlClient;
      assert.deepStrictEqual(yield* sql`SELECT state,source_run_id FROM fork_prism_recovery`, [
        { state: "retry_started", source_run_id: run.id },
      ]);
      const child = yield* orchestrator.getThreadProjection(childId);
      assert.strictEqual(child.runs.length, 2);
      assert.deepStrictEqual(child.runs[1]!.modelSelection, selection);
      assert.strictEqual(child.runs[1]!.forkPrismContinuationSourceRunId, run.id);
      assert.deepStrictEqual((yield* History.readRecoveryState(run.id)).admittedContinuations, [
        { sourceRunId: run.id, successorRunId: child.runs[1]!.id, threadId: childId },
      ]);
      assert.deepStrictEqual(yield* History.readRecoveryOutcome(run.id), {
        sourceRunId: run.id,
        threadId: childId,
        status: "decided",
        outcome: "retried",
        successorRunId: child.runs[1]!.id,
      });
      yield* finish(child.runs[1]!, failure);
      yield* orchestrator.recoverDelegatedTasks;
      const parent = yield* orchestrator.getThreadProjection(parentId);
      assert.strictEqual(
        parent.contextTransfers.filter((x) => x.type === "subagent_result").length,
        1,
      );
      assert.strictEqual(parent.subagents[0]?.status, "failed");
      assert.deepStrictEqual(yield* History.readRecoveryOutcome(child.runs[1]!.id), {
        sourceRunId: child.runs[1]!.id,
        threadId: childId,
        status: "decided",
        outcome: "not_retryable",
        reason: "exhausted",
      });
      yield* (yield* Reactor.RecoveryReactor).sweep;
      assert.strictEqual((yield* orchestrator.getThreadProjection(childId)).runs.length, 2);
    }).pipe(Effect.provide(layer)),
);
it.effect("a monitor run after the result does not hold the delegated result", () =>
  Effect.gen(function* () {
    const { orchestrator, childId, run } = yield* setup;
    yield* finish(run, null);
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make("child:monitor"),
      threadId: childId,
      messageId: MessageId.make("child:monitor-message"),
      text: "Monitor stopped",
      attachments: [],
      notification: { source: { kind: "monitor" }, outcome: "cancelled", summary: "Monitor" },
      dispatchMode: { type: "queue_after_active" },
      createdBy: "agent",
      creationSource: "provider",
    });
    const monitor = (yield* orchestrator.getThreadProjection(childId)).runs[1]!;
    yield* finish(monitor, null);
    yield* orchestrator.recoverDelegatedTasks;
    const parent = yield* orchestrator.getThreadProjection(parentId);
    assert.strictEqual(
      parent.contextTransfers.filter((x) => x.type === "subagent_result").length,
      1,
    );
    assert.strictEqual(parent.subagents[0]?.status, "completed");
    assert.strictEqual(parent.subagents[0]?.completionDelivery?.state, "claimed");
  }).pipe(Effect.provide(layer)),
);
it.effect(
  "ancestor Stop after failure cancels held recovery and permits eventual parent finalization",
  () =>
    Effect.gen(function* () {
      const { orchestrator, childId, run } = yield* setup;
      yield* finish(run, failure);
      assert.strictEqual(
        (yield* orchestrator.getThreadProjection(childId)).thread.creationSource,
        "mcp",
      );
      assert.include(
        yield* (yield* Projection.ProjectionStoreV2).getRecoveryThreadIds("subagent-results"),
        childId,
      );
      yield* orchestrator.recoverDelegatedTasks;
      assert.strictEqual(
        (yield* orchestrator.getThreadProjection(parentId)).contextTransfers.filter(
          (x) => x.type === "subagent_result",
        ).length,
        0,
      );
      yield* orchestrator.dispatch({
        type: "thread.stop",
        commandId: CommandId.make("stop:parent"),
        threadId: parentId,
      });
      yield* (yield* Reactor.RecoveryReactor).sweep;
      assert.strictEqual((yield* orchestrator.getThreadProjection(childId)).runs.length, 1);
      assert.deepStrictEqual(yield* History.readRecoveryOutcome(run.id), {
        sourceRunId: run.id,
        threadId: childId,
        status: "decided",
        outcome: "abandoned",
        reason: "stopped_or_retired",
      });
      assert.strictEqual((yield* History.readRecoveryState(run.id)).pendingRecovery, null);
      assert.strictEqual(
        (yield* orchestrator.getThreadProjection(parentId)).contextTransfers.filter(
          (x) => x.type === "subagent_result",
        ).length,
        1,
      );
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "upstream reset dispatch preserves the held task until its eventual successful result",
  () =>
    Effect.gen(function* () {
      const { orchestrator, childId, run } = yield* setup;
      const now = yield* DateTime.now;
      const resetAt = DateTime.makeUnsafe(DateTime.toEpochMillis(now) + 3_600_000);
      yield* finish(run, {
        ...failure,
        class: "usage_limit",
        resetAt: DateTime.formatIso(resetAt),
      });
      yield* orchestrator.recoverDelegatedTasks;
      assert.strictEqual(
        (yield* orchestrator.getThreadProjection(parentId)).contextTransfers.filter(
          (x) => x.type === "subagent_result",
        ).length,
        0,
      );
      const projections = yield* Projection.ProjectionStoreV2;
      const candidates = yield* projections.getLimitRecoveryCandidates({
        now,
        autoResume: true,
        snooze: false,
      });
      const candidate = candidates.find((t) => t.id === childId)!;
      const arm = limitRecoveryCommand(candidate, true, DateTime.toEpochMillis(now));
      assert.strictEqual(arm?.type, "thread.metadata.update");
      yield* orchestrator.dispatch(arm!);
      assert.deepStrictEqual((yield* History.readRecoveryState(run.id)).pendingRecovery, {
        sourceRunId: run.id,
        reason: "reset",
      });
      const armed = (yield* projections.getLimitRecoveryCandidates({
        now: resetAt,
        autoResume: true,
        snooze: false,
      })).find((t) => t.id === childId)!;
      const resume = limitRecoveryCommand(armed, true, DateTime.toEpochMillis(resetAt));
      assert.strictEqual(resume?.type, "message.dispatch");
      yield* TestClock.adjust("1 hour");
      yield* orchestrator.dispatch(resume!);
      const child = yield* orchestrator.getThreadProjection(childId);
      assert.strictEqual(child.runs.length, 2);
      assert.deepStrictEqual(child.runs[1]!.modelSelection, selection);
      assert.strictEqual(child.runs[1]!.forkPrismContinuationSourceRunId, run.id);
      assert.deepStrictEqual((yield* History.readRecoveryState(run.id)).admittedContinuations, [
        { sourceRunId: run.id, successorRunId: child.runs[1]!.id, threadId: childId },
      ]);
      assert.deepStrictEqual(yield* History.readRecoveryOutcome(run.id), {
        sourceRunId: run.id,
        threadId: childId,
        status: "decided",
        outcome: "retried",
        successorRunId: child.runs[1]!.id,
      });
      yield* (yield* Reactor.RecoveryReactor).sweep;
      assert.strictEqual(
        (yield* orchestrator.getThreadProjection(parentId)).contextTransfers.filter(
          (x) => x.type === "subagent_result",
        ).length,
        0,
      );
      yield* finish(child.runs[1]!, null);
      yield* orchestrator.recoverDelegatedTasks;
      const parent = yield* orchestrator.getThreadProjection(parentId);
      assert.strictEqual(
        parent.contextTransfers.filter((x) => x.type === "subagent_result").length,
        1,
      );
      assert.strictEqual(parent.subagents[0]?.status, "completed");
    }).pipe(Effect.provide(layer)),
);
it.effect("Stop while waiting for reset releases finalization without resuming a provider", () =>
  Effect.gen(function* () {
    const { orchestrator, childId, run } = yield* setup;
    const now = yield* DateTime.now;
    yield* finish(run, {
      ...failure,
      class: "usage_limit",
      resetAt: DateTime.formatIso(DateTime.makeUnsafe(DateTime.toEpochMillis(now) + 3_600_000)),
    });
    yield* orchestrator.recoverDelegatedTasks;
    assert.strictEqual(
      (yield* orchestrator.getThreadProjection(parentId)).contextTransfers.filter(
        (x) => x.type === "subagent_result",
      ).length,
      0,
    );
    yield* orchestrator.dispatch({
      type: "thread.stop",
      commandId: CommandId.make("stop:reset"),
      threadId: childId,
    });
    yield* (yield* Reactor.RecoveryReactor).sweep;
    assert.strictEqual((yield* orchestrator.getThreadProjection(childId)).runs.length, 1);
    assert.deepStrictEqual(yield* History.readRecoveryOutcome(run.id), {
      sourceRunId: run.id,
      threadId: childId,
      status: "decided",
      outcome: "abandoned",
      reason: "stopped_or_retired",
    });
    assert.strictEqual((yield* History.readRecoveryState(run.id)).pendingRecovery, null);
    assert.strictEqual(
      (yield* orchestrator.getThreadProjection(parentId)).contextTransfers.filter(
        (x) => x.type === "subagent_result",
      ).length,
      1,
    );
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "stale propagated Stop preserves a resumed child's recovery; a newer Stop releases it",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threads = yield* Threads.ThreadManagementService;
      const projections = yield* Projection.ProjectionStoreV2;
      const sql = yield* SqlClient.SqlClient;
      const hooks = yield* Hooks.RecoveryHooks;
      const rootId = ThreadId.make("root:delayed-recovery-stop");
      const childId = ThreadId.make("child:delayed-recovery-stop");
      for (const threadId of [rootId, childId]) {
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make(`create:${threadId}`),
          threadId,
          projectId: ProjectId.make("project:recovery"),
          title: threadId,
          modelSelection: selection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          createdBy: "agent",
          creationSource: "mcp",
        });
      }
      const child = (yield* orchestrator.getThreadProjection(childId)).thread;
      yield* projections.apply({
        id: EventId.make("delayed-recovery-lineage"),
        type: "thread.metadata-updated",
        threadId: childId,
        occurredAt: yield* DateTime.now,
        payload: {
          ...child,
          lineage: {
            rootThreadId: rootId,
            parentThreadId: rootId,
            relationshipToParent: "subagent",
          },
        },
      });
      const oldToken = CommandId.make("delayed-recovery-stop:old");
      yield* orchestrator.dispatch({ type: "thread.stop", commandId: oldToken, threadId: rootId });
      const resumeId = CommandId.make("delayed-recovery:human-resume");
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: resumeId,
        threadId: childId,
        messageId: MessageId.make("delayed-recovery:human-message"),
        text: "Resume explicitly",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        createdBy: "user",
        creationSource: "web",
      });
      const resumed = yield* orchestrator.getThreadProjection(childId);
      assert.include(resumed.thread.forkResumedRetirements ?? [], oldToken);
      const run = resumed.runs[0]!;
      yield* finish(run, failure);
      assert.strictEqual(yield* hooks.holdsFinalization(childId, run.id), true);
      const recoveryBefore =
        yield* sql`SELECT * FROM fork_prism_recovery WHERE thread_id=${childId}`;
      const effectsBefore =
        yield* sql`SELECT effect_id,status FROM orchestration_v2_effect_outbox WHERE command_id=${resumeId}`;
      assert.isNotEmpty(effectsBefore);
      yield* threads.stopDelegatedTasks({ threadId: rootId, commandId: oldToken });
      const propagatedId = CommandId.make(`${oldToken}:stop:${childId}`);
      assert.deepStrictEqual(
        yield* sql`SELECT status FROM orchestration_command_receipts WHERE command_id=${propagatedId}`,
        [{ status: "accepted" }],
      );
      assert.lengthOf(
        yield* sql`SELECT event_id FROM orchestration_events WHERE command_id=${propagatedId}`,
        0,
      );
      assert.lengthOf(
        yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE command_id=${propagatedId}`,
        0,
      );
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM fork_prism_recovery WHERE thread_id=${childId}`,
        recoveryBefore,
      );
      assert.deepStrictEqual(
        yield* sql`SELECT effect_id,status FROM orchestration_v2_effect_outbox WHERE command_id=${resumeId}`,
        effectsBefore,
      );
      assert.strictEqual(yield* hooks.holdsFinalization(childId, run.id), true);
      const afterStale = yield* orchestrator.getThreadProjection(childId);
      assert.deepStrictEqual(
        afterStale.thread.forkResumedRetirements,
        resumed.thread.forkResumedRetirements,
      );
      assert.strictEqual(
        afterStale.thread.forkRetirement?.token,
        resumed.thread.forkRetirement?.token,
      );
      const newToken = CommandId.make("delayed-recovery-stop:new");
      yield* orchestrator.dispatch({ type: "thread.stop", commandId: newToken, threadId: rootId });
      assert.strictEqual(yield* hooks.holdsFinalization(childId, run.id), false);
      assert.deepStrictEqual(
        yield* sql`SELECT state FROM fork_prism_recovery WHERE thread_id=${childId}`,
        [{ state: "cancelled" }],
      );
      yield* (yield* Reactor.RecoveryReactor).sweep;
      assert.lengthOf((yield* orchestrator.getThreadProjection(childId)).runs, 1);
    }).pipe(Effect.provide(layer)),
);

it.effect("live run recovery finalizes its child without reading settled children", () =>
  Effect.gen(function* () {
    const { orchestrator, childId, run } = yield* setup;
    const projections = yield* Projection.ProjectionStoreV2;
    const sink = yield* EventSink.EventSinkV2;
    const settled = new Set<ThreadId>();
    for (let index = 0; index < 64; index++) {
      const id = ThreadId.make(`settled:${index}`);
      settled.add(id);
      const source = { ...run, id: RunId.make(`settled-run:${index}`), threadId: id };
      yield* projections.apply({
        id: EventId.make(`settled-thread:${index}`),
        type: "thread.created",
        threadId: id,
        occurredAt: run.requestedAt,
        payload: {
          ...threadFor(source),
          archivedAt: run.requestedAt,
          lineage: {
            parentThreadId: parentId,
            relationshipToParent: "subagent",
            rootThreadId: parentId,
          },
        },
      });
      yield* projections.apply({
        id: EventId.make(`settled-run:${index}`),
        type: "run.created",
        threadId: id,
        occurredAt: run.requestedAt,
        payload: { ...source, status: "completed", completedAt: run.requestedAt },
      });
    }
    const records = vi.spyOn(projections, "getThreadRecords");
    const snapshot = vi.spyOn(projections, "getShellSnapshot");
    const recovered = yield* Deferred.make<void>();
    const wake = Effect.andThen(Deferred.succeed(recovered, undefined).pipe(Effect.asVoid));
    const overrides = Layer.mergeAll(
      Layer.succeed(ServerActivation, undefined),
      Layer.succeed(Orchestrator.OrchestratorV2, {
        ...orchestrator,
        recoverDelegatedTasks: orchestrator.recoverDelegatedTasks.pipe(wake),
        recoverDelegatedTask: (threadId, sourceRunId) =>
          orchestrator.recoverDelegatedTask(threadId, sourceRunId).pipe(wake),
      }),
    );
    yield* Effect.gen(function* () {
      yield* finish(run, null);
      const ended = (yield* orchestrator.getThreadProjection(childId)).runs[0]!;
      yield* sink.write({
        events: [
          {
            id: EventId.make("active-child:completed"),
            type: "run.updated",
            threadId: childId,
            occurredAt: ended.completedAt!,
            payload: ended,
          },
        ],
      });
      yield* Deferred.await(recovered);
      assert.strictEqual(snapshot.mock.calls.length, 0);
      assert.deepStrictEqual(
        records.mock.calls.filter(([id]) => settled.has(id)),
        [],
      );
      const parent = yield* orchestrator.getThreadProjection(parentId);
      assert.strictEqual(
        parent.contextTransfers.filter((item) => item.type === "subagent_result").length,
        1,
      );
    }).pipe(
      Effect.provide(Reactor.layer.pipe(Layer.provide(Layer.merge(dependencies, overrides)))),
      Effect.ensuring(
        Effect.sync(() => {
          records.mockRestore();
          snapshot.mockRestore();
        }),
      ),
    );
  }).pipe(Effect.provide(dependencies)),
);

it.effect.each(["fresh", "decided-history", "unresolved-history"] as const)(
  "%s updates preserve the first retryable failure's retry",
  (scenario) =>
    Effect.gen(function* () {
      const { orchestrator, childId, run } = yield* setup;
      const sink = yield* EventSink.EventSinkV2;
      const observed = yield* Queue.unbounded<void>();
      const overrides = Layer.mergeAll(
        Layer.succeed(ServerActivation, undefined),
        Layer.succeed(Orchestrator.OrchestratorV2, {
          ...orchestrator,
          recoverDelegatedTask: (threadId, sourceRunId) =>
            orchestrator
              .recoverDelegatedTask(threadId, sourceRunId)
              .pipe(
                Effect.andThen(
                  threadId === childId ? Queue.offer(observed, undefined) : Effect.void,
                ),
                Effect.asVoid,
              ),
        }),
      );
      yield* Effect.gen(function* () {
        const running = {
          ...run,
          status: "running" as const,
          startedAt: run.requestedAt,
          ordinal: scenario === "fresh" ? run.ordinal : 2,
        };
        yield* sink.write({
          events: [
            {
              id: EventId.make("first-retry:running"),
              type: "run.updated",
              threadId: childId,
              occurredAt: run.requestedAt,
              payload: running,
            },
          ],
        });
        // Finish observing the nonterminal event before the failure can mask it.
        yield* Queue.take(observed);
        if (scenario !== "fresh") {
          const old = {
            ...running,
            id: RunId.make("decided:old-run"),
            ordinal: 1,
            userMessageId: MessageId.make("decided:old-message"),
            status: "failed" as const,
            completedAt: run.requestedAt,
          };
          yield* sink.write({
            events: [
              {
                id: EventId.make("decided:old-created"),
                type: "run.created",
                threadId: childId,
                occurredAt: old.requestedAt,
                payload: old,
              },
            ],
          });
          if (scenario === "decided-history")
            yield* History.writeRecoveryOutcome({
              sourceRunId: old.id,
              threadId: childId,
              status: "decided",
              outcome: "not_retryable",
              reason: "exhausted",
            });
          // Neither a late update to a decided source nor reconciling an unresolved one
          // may initialize the newer running run's controller as closed.
          yield* sink.write({
            events: [
              {
                id: EventId.make("decided:old-updated"),
                type: "run.updated",
                threadId: childId,
                occurredAt: old.completedAt,
                payload: old,
              },
            ],
          });
          yield* Queue.take(observed);
        }
        yield* finish(running, failure);
        const failed = (yield* orchestrator.getThreadProjection(childId)).runs.find(
          (candidate) => candidate.id === run.id,
        )!;
        yield* sink.write({
          events: [
            {
              id: EventId.make("first-retry:failed"),
              type: "run.updated",
              threadId: childId,
              occurredAt: failed.completedAt!,
              payload: failed,
            },
          ],
        });
        yield* Queue.take(observed);
        const child = yield* orchestrator.getThreadProjection(childId);
        const retries = child.runs.filter(
          (candidate) => candidate.forkPrismContinuationSourceRunId === run.id,
        );
        assert.lengthOf(retries, 1);
        assert.deepStrictEqual(retries[0]!.modelSelection, run.modelSelection);
        assert.lengthOf(
          (yield* orchestrator.getThreadProjection(parentId)).contextTransfers.filter(
            (item) => item.type === "subagent_result",
          ),
          0,
        );
      }).pipe(
        Effect.provide(Reactor.layer.pipe(Layer.provide(Layer.merge(dependencies, overrides)))),
      );
    }).pipe(Effect.provide(dependencies)),
);
