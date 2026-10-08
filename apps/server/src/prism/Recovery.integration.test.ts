import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
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
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as Registry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import type { ProviderAdapterV2Shape } from "../orchestration-v2/ProviderAdapter.ts";
import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as Harness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as Persistence from "../persistence/Sqlite.ts";
import * as Settings from "../serverSettings.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import { ServerActivation } from "../serverActivation.ts";
import * as Hooks from "./RecoveryHooks.ts";
import * as Coordinator from "./RecoveryCoordinator.ts";
import * as Reactor from "./RecoveryReactor.ts";
import { limitRecoveryCommand } from "../orchestration-v2/UsageLimitRecoveryWorker.ts";
import { errorFor } from "./recovery.testkit.ts";

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
} as ProviderAdapterV2Shape;
const database = Persistence.layerMemory;
const foundation = Layer.mergeAll(
  database,
  Settings.layerTest(),
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
  Threads.layer.pipe(Layer.provide(replay)),
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
      yield* finish(child.runs[1]!, failure);
      yield* orchestrator.recoverDelegatedTasks;
      const parent = yield* orchestrator.getThreadProjection(parentId);
      assert.strictEqual(
        parent.contextTransfers.filter((x) => x.type === "subagent_result").length,
        1,
      );
      assert.strictEqual(parent.subagents[0]?.status, "failed");
      yield* (yield* Reactor.RecoveryReactor).sweep;
      assert.strictEqual((yield* orchestrator.getThreadProjection(childId)).runs.length, 2);
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
        [{ state: "closed" }],
      );
      yield* (yield* Reactor.RecoveryReactor).sweep;
      assert.lengthOf((yield* orchestrator.getThreadProjection(childId)).runs, 1);
    }).pipe(Effect.provide(layer)),
);
