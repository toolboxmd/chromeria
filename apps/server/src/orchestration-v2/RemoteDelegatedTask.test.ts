import * as ThreadManagement from "./ThreadManagementService.ts";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";

import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EventSink from "./EventSink.ts";
import * as Orchestrator from "./Orchestrator.ts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";

// A delegated task whose child runs in a linked environment is recorded on
// its parent without a child thread here, and completing it wakes the parent
// the way a local child's result does.

const instanceId = ProviderInstanceId.make("codex");
const driver = ProviderDriverKind.make("codex");
const modelSelection = { instanceId, model: "gpt-5.4" };
const adapter = {
  instanceId,
  driver,
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("No provider process needed for delegated task records"),
} as ProviderAdapter.ProviderAdapterV2["Service"];

/** Every parent wake the orchestrator offers, for the tests to read. */
class Offers extends Context.Service<
  Offers,
  Ref.Ref<ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>>
>()("t3/orchestration-v2/RemoteDelegatedTask.test/Offers") {}
const layerOffers = Layer.effect(
  Offers,
  Ref.make<ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>>([]),
);
const layerDatabase = SqlitePersistence.layerMemory;
const layerTest = Layer.mergeAll(
  layerDatabase,
  ProjectionStore.layer.pipe(Layer.provide(layerDatabase)),
  ProviderReplayHarness.layerWithRegistry(
    { name: "remote-delegated-task" },
    ProviderAdapterRegistry.layerFromAdapters([adapter]),
    { databaseLayer: layerDatabase, runEffectWorker: false },
  ),
).pipe(
  Layer.provide(
    Layer.effect(
      ProviderContinuationRequests.ProviderContinuationRequests,
      Offers.pipe(
        Effect.map((offers) => ({
          offer: (request: ProviderContinuationRequests.ProviderContinuationRequest) =>
            Ref.update(offers, (seen) => [...seen, request]),
          take: Effect.never,
        })),
      ),
    ),
  ),
  Layer.provideMerge(layerOffers),
);

/** A parent thread whose run is live, as when its agent calls delegate_task. */
const seedLiveParent = (name: string) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const sink = yield* EventSink.EventSinkV2;
    const now = yield* DateTime.now;
    const threadId = ThreadId.make(`thread:${name}`);
    const runId = RunId.make(`run:${name}`);
    const rootNodeId = NodeId.make(`node:${name}:root`);
    const providerThreadId = ProviderThreadId.make(`provider-thread:${name}`);
    yield* orchestrator.dispatch({
      type: "thread.create",
      createdBy: "user",
      creationSource: "web",
      commandId: CommandId.make(`command:create:${name}`),
      threadId,
      projectId: ProjectId.make("project:remote-delegated-task"),
      title: "Parent",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
    });
    yield* sink.write({
      commandId: CommandId.make(`command:seed:${name}`),
      events: [
        {
          id: EventId.make(`event:seed-provider-thread:${name}`),
          type: "provider-thread.updated",
          threadId,
          driver,
          providerInstanceId: instanceId,
          occurredAt: now,
          payload: {
            id: providerThreadId,
            driver,
            providerInstanceId: instanceId,
            providerSessionId: null,
            appThreadId: threadId,
            ownerNodeId: rootNodeId,
            nativeThreadRef: { driver, nativeId: `native:${name}`, strength: "strong" },
            nativeConversationHeadRef: null,
            status: "active",
            firstRunOrdinal: 1,
            lastRunOrdinal: 1,
            handoffIds: [],
            forkedFrom: null,
            createdAt: now,
            updatedAt: now,
          },
        },
        {
          id: EventId.make(`event:seed-node:${name}`),
          type: "node.updated",
          threadId,
          runId,
          nodeId: rootNodeId,
          driver,
          providerInstanceId: instanceId,
          occurredAt: now,
          payload: {
            id: rootNodeId,
            threadId,
            runId,
            parentNodeId: null,
            rootNodeId,
            kind: "root_turn",
            status: "running",
            countsForRun: true,
            providerThreadId,
            providerTurnId: null,
            nativeItemRef: null,
            runtimeRequestId: null,
            checkpointScopeId: null,
            startedAt: now,
            completedAt: null,
          },
        },
        {
          id: EventId.make(`event:seed-run:${name}`),
          type: "run.updated",
          threadId,
          runId,
          nodeId: rootNodeId,
          providerInstanceId: instanceId,
          occurredAt: now,
          payload: {
            id: runId,
            threadId,
            ordinal: 1,
            providerInstanceId: instanceId,
            modelSelection,
            providerThreadId,
            userMessageId: MessageId.make(`message:seed-user:${name}`),
            rootNodeId,
            activeAttemptId: null,
            status: "running",
            requestedAt: now,
            startedAt: now,
            completedAt: null,
            checkpointId: null,
            contextHandoffId: null,
          },
        },
      ],
    });
    return { threadId, runId, rootNodeId };
  });

const remoteChild = {
  environmentId: EnvironmentId.make("environment:box"),
  threadId: ThreadId.make("thread:on-the-box"),
  label: "Box",
};

it.layer(layerTest)("a delegated task in a linked environment", (it) => {
  it.effect(
    "retains completed remote jobs for observation and records parent Stop after first result",
    () =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const parent = yield* seedLiveParent("continued-remote");
        yield* orchestrator.dispatch({
          type: "delegated_task.remote.request",
          commandId: CommandId.make("command:remote-delegate:continued"),
          parentThreadId: parent.threadId,
          parentRunId: parent.runId,
          parentNodeId: parent.rootNodeId,
          task: "Continue this same job after its first result.",
          driver: ProviderDriverKind.make("codex"),
          modelSelection,
          remoteChild: {
            ...remoteChild,
            launch: {
              lifecycleVersion: 1,
              requestNamespace: "recorded-source",
              clientRequestId: "continued-job",
              linkIdentity: "recorded-generation",
              projectId: ProjectId.make("remote-project"),
              title: "Remote job",
              delegatedFrom: {
                environmentId: EnvironmentId.make("source-environment"),
                threadId: parent.threadId,
                title: "Source parent",
              },
            },
          },
          completionWake: "always",
        });
        const task = (yield* orchestrator.getThreadProjection(parent.threadId)).subagents[0]!;
        yield* orchestrator.dispatch({
          type: "delegated_task.remote.complete",
          commandId: CommandId.make("command:remote-first-result"),
          parentThreadId: parent.threadId,
          taskId: task.id,
          status: "completed",
          result: "Original immutable result",
          stoppedThere: "ran-out",
        });
        const recoverable = yield* projections.getOpenRemoteDelegatedTasks;
        assert.includeDeepMembers(
          [...recoverable],
          [{ parentThreadId: parent.threadId, taskId: task.id }],
        );
        const threads = yield* ThreadManagement.ThreadManagementService;
        yield* threads.stopDelegatedTasks({
          threadId: parent.threadId,
          commandId: CommandId.make("parent-stop-after-original"),
        });
        const stopped = (yield* orchestrator.getThreadProjection(parent.threadId)).subagents.find(
          (candidate) => candidate.id === task.id,
        )!;
        assert.equal(stopped.result, "Original immutable result");
        assert.equal(stopped.remoteChild?.stop?.state, "pending");
        assert.equal(stopped.completionDelivery?.state, "disposed");
      }),
  );
  it.effect(
    "records cancellation and wake disposal with the Stop obligation before a first late result",
    () =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const parent = yield* seedLiveParent("atomic-stop");
        yield* orchestrator.dispatch({
          type: "delegated_task.remote.request",
          commandId: CommandId.make("atomic-stop-create"),
          parentThreadId: parent.threadId,
          parentRunId: parent.runId,
          parentNodeId: parent.rootNodeId,
          task: "Cancel atomically",
          driver,
          modelSelection,
          remoteChild,
          completionWake: "always",
        });
        const taskId = (yield* orchestrator.getThreadProjection(parent.threadId)).subagents[0]!.id;
        const offers = yield* Offers;
        yield* Ref.set(offers, []);
        yield* orchestrator.dispatch({
          type: "delegated_task.remote.observe",
          commandId: CommandId.make("atomic-stop-intent"),
          parentThreadId: parent.threadId,
          taskId,
          stop: { requestId: "atomic-stop-request", state: "pending" },
          stopResult: { status: "interrupted", result: "Stopped with parent" },
        });
        const stopped = (yield* orchestrator.getThreadProjection(parent.threadId)).subagents.find(
          (task) => task.id === taskId,
        )!;
        assert.equal(stopped.result, "Stopped with parent");
        assert.equal(stopped.status, "interrupted");
        assert.equal(stopped.completionDelivery?.state, "disposed");
        assert.equal(stopped.remoteChild?.stop?.state, "pending");
        yield* orchestrator.dispatch({
          type: "delegated_task.remote.complete",
          commandId: CommandId.make("atomic-stop-late-result"),
          parentThreadId: parent.threadId,
          taskId,
          status: "completed",
          result: "Late actual reply",
          observation: {
            cursor: 10,
            snapshot: {
              sequence: 10,
              observedAt: "2026-10-17T20:00:00Z",
              title: "Remote",
              workState: "result_available",
              pendingChildren: 0,
              queuedRuns: 0,
              recoveryWaiting: false,
              incomplete: false,
              executionSettled: true,
              pendingQuestions: 0,
              requestCount: 0,
              requests: [],
            },
            result: {
              environmentId: remoteChild.environmentId,
              threadId: remoteChild.threadId,
              runId: RunId.make("atomic-late-run"),
              eventId: EventId.make("atomic-late-event"),
              sequence: 10,
              status: "completed",
              text: "Late actual reply",
              truncated: false,
            },
          },
        });
        const late = (yield* orchestrator.getThreadProjection(parent.threadId)).subagents.find(
          (task) => task.id === taskId,
        )!;
        assert.equal(late.result, "Stopped with parent");
        assert.equal(late.status, "interrupted");
        assert.equal(late.remoteChild?.latestResult?.text, "Late actual reply");
        assert.equal((yield* Ref.get(offers)).length, 0);
      }),
  );
  it.effect(
    "delivers each real remote result once, preserves original provenance and rejects stale replay",
    () =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const parent = yield* seedLiveParent("ordered-remote");
        yield* orchestrator.dispatch({
          type: "delegated_task.remote.request",
          commandId: CommandId.make("ordered-create"),
          parentThreadId: parent.threadId,
          parentRunId: parent.runId,
          parentNodeId: parent.rootNodeId,
          task: "Three results",
          driver,
          modelSelection,
          remoteChild: { ...remoteChild, runId: RunId.make("remote-launched-before-recovery") },
          completionWake: "always",
        });
        const taskId = (yield* orchestrator.getThreadProjection(parent.threadId)).subagents[0]!.id;
        const offers = yield* Offers;
        yield* Ref.set(offers, []);
        const snapshot = (sequence: number) => ({
          sequence,
          observedAt: "2026-10-17T20:00:00Z",
          title: "Remote job",
          workState: "result_available" as const,
          pendingChildren: 0,
          queuedRuns: 0,
          recoveryWaiting: false,
          incomplete: false,
          executionSettled: true,
          pendingQuestions: 0,
          requestCount: 0,
          requests: [],
        });
        const report = (n: number, attempt = "main") =>
          orchestrator.dispatch({
            type: "delegated_task.remote.complete",
            commandId: CommandId.make(`ordered-result-${n}:${attempt}`),
            parentThreadId: parent.threadId,
            taskId,
            status: n === 3 ? "failed" : "completed",
            result: `Result ${n}`,
            observation: {
              cursor: n * 10,
              snapshot: snapshot(n * 10),
              result: {
                environmentId: remoteChild.environmentId,
                threadId: remoteChild.threadId,
                runId: RunId.make(n === 1 ? "remote-original" : `remote-followup-${n}`),
                eventId: EventId.make(`remote-terminal-${n}`),
                sequence: n * 10,
                status: n === 3 ? "failed" : "completed",
                text: `Result ${n}`,
                truncated: false,
              },
            },
          });
        for (const n of [1, 2, 3]) {
          yield* report(n);
          if (n === 1) {
            yield* report(2, "before-acknowledgement");
            const blocked = (yield* orchestrator.getThreadProjection(
              parent.threadId,
            )).subagents.find((task) => task.id === taskId)!;
            assert.equal(blocked.remoteChild?.cursor, 10);
            assert.equal(blocked.remoteChild?.latestResult?.runId, "remote-original");
            assert.equal((yield* Ref.get(offers)).length, 1);
          }
          const task = (yield* orchestrator.getThreadProjection(parent.threadId)).subagents.find(
            (task) => task.id === taskId,
          )!;
          assert.equal(task.result, "Result 1");
          assert.equal(task.remoteChild?.runId, "remote-launched-before-recovery");
          assert.equal(task.remoteChild?.originalResult?.runId, "remote-original");
          assert.equal(task.remoteChild?.originalResult?.eventId, "remote-terminal-1");
          assert.equal(task.remoteChild?.latestResult?.text, `Result ${n}`);
          assert.equal(task.childThreadId, null);
          yield* orchestrator.dispatch({
            type: "delegated_task.completion-delivery.acknowledge",
            commandId: CommandId.make(`ordered-ack-${n}`),
            parentThreadId: parent.threadId,
            taskId,
            observedByRunId: parent.runId,
          });
        }
        assert.equal((yield* Ref.get(offers)).length, 3);
        const beforeReplay = yield* orchestrator.getThreadEventSequence(parent.threadId);
        yield* report(2, "stale-distinct-command");
        yield* report(2);
        assert.equal(yield* orchestrator.getThreadEventSequence(parent.threadId), beforeReplay);
        assert.equal((yield* Ref.get(offers)).length, 3);
        yield* orchestrator.dispatch({
          type: "delegated_task.remote.observe",
          commandId: CommandId.make("monitor-only-update"),
          parentThreadId: parent.threadId,
          taskId,
          observation: {
            cursor: 40,
            snapshot: { ...snapshot(40), workState: "working", queuedRuns: 1 },
          },
          availability: "current",
        });
        const monitor = (yield* orchestrator.getThreadProjection(parent.threadId)).subagents.find(
          (task) => task.id === taskId,
        )!;
        assert.equal(monitor.remoteChild?.latestResult?.runId, "remote-followup-3");
        assert.equal(monitor.result, "Result 1");
        assert.equal((yield* Ref.get(offers)).length, 3);
        yield* orchestrator.dispatch({
          type: "delegated_task.remote.observe",
          commandId: CommandId.make("stop-after-result"),
          parentThreadId: parent.threadId,
          taskId,
          stop: { requestId: "stop-generation", state: "pending" },
        });
        // A later real result remains observable but cannot resurrect a stopped parent.
        yield* orchestrator.dispatch({
          type: "delegated_task.remote.complete",
          commandId: CommandId.make("late-after-stop"),
          parentThreadId: parent.threadId,
          taskId,
          status: "completed",
          result: "Late result",
          observation: {
            cursor: 50,
            snapshot: snapshot(50),
            result: {
              environmentId: remoteChild.environmentId,
              threadId: remoteChild.threadId,
              runId: RunId.make("late-run"),
              eventId: EventId.make("late-event"),
              sequence: 50,
              status: "completed",
              text: "Late result",
              truncated: false,
            },
          },
        });
        const late = (yield* orchestrator.getThreadProjection(parent.threadId)).subagents.find(
          (task) => task.id === taskId,
        )!;
        assert.equal(late.result, "Result 1");
        assert.equal(late.remoteChild?.latestResult?.text, "Late result");
        assert.equal(late.remoteChild?.stop?.state, "pending");
        assert.equal(late.completionDelivery?.state, "disposed");
        assert.equal((yield* Ref.get(offers)).length, 3);
        assert.equal(yield* orchestrator.getThreadShell(remoteChild.threadId), null);
      }),
  );

  it.effect("is recorded on its parent without a child thread here, once", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const parent = yield* seedLiveParent("records");
      const request = {
        type: "delegated_task.remote.request" as const,
        commandId: CommandId.make("command:remote-delegate:records"),
        parentThreadId: parent.threadId,
        parentRunId: parent.runId,
        parentNodeId: parent.rootNodeId,
        task: "Run the suite there.",
        driver: ProviderDriverKind.make("remote"),
        modelSelection,
        remoteChild,
        completionWake: "always" as const,
      };
      yield* orchestrator.dispatch(request);
      yield* orchestrator.dispatch(request);
      const projection = yield* orchestrator.getThreadProjection(parent.threadId);
      assert.equal(projection.subagents.length, 1);
      const [task] = projection.subagents;
      assert.equal(task?.childThreadId, null);
      assert.deepEqual(task?.remoteChild, remoteChild);
      assert.equal(task?.status, "running");
      const item = projection.turnItems.find((candidate) => candidate.type === "subagent");
      assert.deepEqual(item?.type === "subagent" ? item.remoteChild : undefined, remoteChild);
      // No thread was created here for the child.
      assert.equal(yield* orchestrator.getThreadShell(remoteChild.threadId), null);
    }),
  );

  it.effect("wakes its parent with the result when it completes there", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const parent = yield* seedLiveParent("completes");
      const recorded = yield* orchestrator.dispatch({
        type: "delegated_task.remote.request",
        commandId: CommandId.make("command:remote-delegate:completes"),
        parentThreadId: parent.threadId,
        parentRunId: parent.runId,
        parentNodeId: parent.rootNodeId,
        task: "Run the suite there.",
        driver: ProviderDriverKind.make("remote"),
        modelSelection,
        remoteChild,
        completionWake: "always",
      });
      const taskEvent = recorded.storedEvents.find(
        (stored) => stored.event.type === "subagent.updated",
      );
      const taskId =
        taskEvent?.event.type === "subagent.updated" ? taskEvent.event.payload.id : undefined;
      assert.isDefined(taskId);
      // Until it completes, it is open, and a restarted follower picks it up.
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const openFor = projections.getOpenRemoteDelegatedTasks.pipe(
        Effect.map((open) => open.filter((task) => task.parentThreadId === parent.threadId)),
      );
      assert.deepEqual(yield* openFor, [{ parentThreadId: parent.threadId, taskId: taskId! }]);
      const offers = yield* Offers;
      yield* Ref.set(offers, []);
      const complete = (result: string) =>
        orchestrator.dispatch({
          type: "delegated_task.remote.complete",
          commandId: CommandId.make(`command:remote-complete:${result}`),
          parentThreadId: parent.threadId,
          taskId: taskId!,
          status: "completed",
          result,
        });
      yield* complete("All 412 tests pass on the box.");
      // A duplicate terminal report is a no-op and cannot replace the first result.
      const late = yield* complete("A late, different answer.");
      assert.deepEqual(late.storedEvents, []);

      const projection = yield* orchestrator.getThreadProjection(parent.threadId);
      const task = projection.subagents.find((candidate) => candidate.id === taskId);
      assert.equal(task?.status, "completed");
      assert.equal(task?.result, "All 412 tests pass on the box.");
      // The live parent run claimed it, so task_status acknowledges it as for a local child.
      assert.equal(task?.completionDelivery?.state, "claimed");
      // This imported record has no generation-bound lifecycle contract.
      assert.deepEqual(yield* openFor, []);
      const item = projection.turnItems.find(
        (candidate) => candidate.type === "subagent" && candidate.subagentId === taskId,
      );
      assert.equal(item?.status, "completed");
      // The parent's live run is offered the delivery, as for a local child.
      const delivery = projection.runs.find((run) => run.id === parent.runId)?.delegatedCompletion
        ?.delivery;
      assert.deepEqual(delivery?.taskIds, [taskId]);
      const offered = yield* Ref.get(offers);
      assert.equal(offered.length, 1);
      assert.equal(offered[0]?.threadId, parent.threadId);
    }),
  );
  it.effect.each([null, RunId.make("run:accepted")])(
    "attaches an uncertain launch after cancellation once, and rejects another destination (run=%s)",
    (acceptedRunId) =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const parent = yield* seedLiveParent(`pending-attach:${acceptedRunId ?? "none"}`);
        const commandId = (name: string) =>
          CommandId.make(`command:pending-attach:${acceptedRunId ?? "none"}:${name}`);
        const pending = {
          environmentId: remoteChild.environmentId,
          label: remoteChild.label,
          launch: {
            requestNamespace: "original-session",
            clientRequestId: "original-key",
            linkIdentity: "original-grant",
            projectId: ProjectId.make("project:remote"),
            title: "Remote task",
            modelSelection,
            delegatedFrom: {
              environmentId: EnvironmentId.make("environment:source"),
              threadId: parent.threadId,
              title: "Parent",
            },
          },
        };
        const recorded = yield* orchestrator.dispatch({
          type: "delegated_task.remote.request",
          commandId: commandId("request"),
          parentThreadId: parent.threadId,
          parentRunId: parent.runId,
          parentNodeId: parent.rootNodeId,
          task: "Run there.",
          driver: ProviderDriverKind.make("remote"),
          modelSelection,
          remoteChild: pending,
        });
        const taskEvent = recorded.storedEvents.find(
          (stored) => stored.event.type === "subagent.updated",
        );
        assert.isTrue(taskEvent?.event.type === "subagent.updated");
        if (taskEvent?.event.type !== "subagent.updated") return;
        const taskId = taskEvent.event.payload.id;
        const attach = {
          type: "delegated_task.remote.attach" as const,
          commandId: commandId("attach"),
          parentThreadId: parent.threadId,
          taskId,
          environmentId: pending.environmentId,
          threadId: remoteChild.threadId,
          runId: acceptedRunId,
          modelSelection,
        };
        assert.equal(
          (yield* orchestrator
            .dispatch({ ...attach, commandId: commandId("too-early") })
            .pipe(Effect.flip))._tag,
          "OrchestratorDispatchError",
        );
        yield* orchestrator.dispatch({
          type: "delegated_task.remote.launch",
          commandId: commandId("launch"),
          parentThreadId: parent.threadId,
          taskId,
        });
        yield* orchestrator.dispatch({
          type: "delegated_task.remote.complete",
          commandId: commandId("cancel"),
          parentThreadId: parent.threadId,
          taskId,
          status: "cancelled",
          result: "Cancelled.",
        });
        const offers = yield* Offers;
        const offersBefore = yield* Ref.get(offers);
        yield* orchestrator.dispatch(attach);
        const repeated = yield* orchestrator.dispatch({
          ...attach,
          commandId: commandId("repeat"),
        });
        assert.equal(repeated.storedEvents.length, 0);
        for (const changed of [
          { environmentId: EnvironmentId.make("environment:wrong") },
          { threadId: ThreadId.make("thread:wrong") },
          { runId: RunId.make("run:wrong") },
        ]) {
          assert.equal(
            (yield* orchestrator
              .dispatch({
                ...attach,
                ...changed,
                commandId: commandId(`wrong:${JSON.stringify(changed)}`),
              })
              .pipe(Effect.flip))._tag,
            "OrchestratorDispatchError",
          );
        }
        const projection = yield* orchestrator.getThreadProjection(parent.threadId);
        const task = projection.subagents.find((candidate) => candidate.id === taskId);
        assert.equal(task?.result, "Cancelled.");
        assert.equal(task?.status, "cancelled");
        assert.equal(task?.remoteChild?.threadId, remoteChild.threadId);
        assert.equal(task?.remoteChild?.runId, attach.runId ?? undefined);
        assert.deepEqual(task?.remoteChild?.launch, pending.launch);
        assert.equal(
          projection.turnItems.find(
            (item) => item.type === "subagent" && item.subagentId === taskId,
          )?.status,
          "cancelled",
        );
        assert.deepEqual(yield* Ref.get(offers), offersBefore);
      }),
  );
});
