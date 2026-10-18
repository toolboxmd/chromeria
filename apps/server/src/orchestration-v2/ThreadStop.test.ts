import { assert, it, vi } from "@effect/vitest";
import {
  CommandId,
  ContextTransferId,
  EnvironmentId,
  EventId,
  MessageId,
  OrchestrationV2Command,
  NodeId,
  type OrchestrationV2DomainEvent,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RuntimeRequestId,
  ScheduledTaskId,
  type RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Stream from "effect/Stream";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import { runOrderedV2StartupPhases } from "../serverRuntimeStartup.ts";
import * as ProviderRuntimeRecovery from "./ProviderRuntimeRecoveryService.ts";
import * as ProviderContinuationService from "./ProviderContinuationService.ts";
import * as EventSink from "./EventSink.ts";
import { ForkCommandInterceptor } from "../fork/ForkCommandInterceptor.ts";
import * as WightAdmission from "../wight/AdmissionHooks.ts";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as OrchestratorMcpService from "../mcp/OrchestratorMcpService.ts";
import * as BareLaunch from "./BareLaunch.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ThreadLaunchService from "./ThreadLaunchService.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as RemoteDelegation from "../peer/RemoteDelegation.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as EventStore from "./EventStore.ts";
import * as TestClock from "effect/testing/TestClock";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ServerSettings from "../serverSettings.ts";
import { CLAUDE_PROVIDER } from "./Adapters/ClaudeAdapterV2.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import { OrchestrationEffectRequestV2 } from "./EffectOutbox.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { isThreadRetired } from "../childThreads/retirement.ts";

const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "gpt-5.1-codex" };
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("Runs here never reach a provider"),
} as ProviderAdapter.ProviderAdapterV2["Service"];
const layerDatabase = SqlitePersistence.layerMemory;
const layerTest = ThreadManagementService.layer.pipe(
  Layer.provideMerge(
    Layer.mergeAll(
      layerDatabase,
      ProjectionStore.layer.pipe(Layer.provide(layerDatabase)),
      EventStore.layer.pipe(Layer.provide(layerDatabase)),
      EffectOutbox.layer.pipe(Layer.provide(layerDatabase)),
      ProviderReplayHarness.layerWithRegistry(
        { name: "thread-stop" },
        ProviderAdapterRegistry.layerFromAdapters([adapter]),
        { databaseLayer: layerDatabase, runEffectWorker: false },
      ),
    ),
  ),
  Layer.provideMerge(ServerSettings.layerTest()),
  Layer.provideMerge(CommandReceiptStore.layer.pipe(Layer.provide(layerDatabase))),
  Layer.provideMerge(ThreadCommandExecutor.layer),
);

const encodeEffectRequest = Schema.encodeSync(Schema.fromJsonString(OrchestrationEffectRequestV2));
const decodeClientCommand = Schema.decodeUnknownEffect(OrchestrationV2Command);

// task_status uses real thread records and acknowledgement; unrelated MCP capabilities stay unused.
const layerTaskStatus = OrchestratorMcpService.layer.pipe(
  Layer.provide(
    Layer.mergeAll(
      NodeServices.layer,
      Layer.mock(BareLaunch.BareLaunch)({}),
      Layer.mock(ProviderRegistry.ProviderRegistry)({}),
      ProviderAdapterRegistry.layerFromAdapters([adapter]),
      Layer.mock(ScheduledTaskService.ScheduledTaskService)({}),
      Layer.mock(ProjectService.ProjectService)({}),
      Layer.mock(ThreadLaunchService.ThreadLaunchService)({}),
      Layer.mock(GitVcsDriver.GitVcsDriver)({}),
      Layer.mock(RemoteDelegation.RemoteDelegation)({}),
      Layer.mock(SecretRequests.SecretRequests)({}),
    ),
  ),
);

it.effect(
  "a delayed descendant Stop cannot retire a human turn that acknowledged its ancestor token",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threads = yield* ThreadManagementService.ThreadManagementService;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const sql = yield* SqlClient.SqlClient;
      const parentId = ThreadId.make("delayed-stop-parent");
      const childId = ThreadId.make("delayed-stop-child");
      yield* createWatchingThread(parentId, 20);
      yield* createWatchingThread(childId, 21);
      const child = (yield* orchestrator.getThreadProjection(childId)).thread;
      yield* projections.apply({
        id: EventId.make("delayed-stop-lineage"),
        type: "thread.metadata-updated",
        threadId: childId,
        providerInstanceId: instanceId,
        occurredAt: yield* DateTime.now,
        payload: {
          ...child,
          lineage: {
            parentThreadId: parentId,
            rootThreadId: parentId,
            relationshipToParent: "subagent",
          },
        },
      });
      const commandId = CommandId.make("delayed-ancestor-stop");
      yield* orchestrator.dispatch({ type: "thread.stop", commandId, threadId: parentId });
      yield* send(childId, "explicit human revival", "start_immediately");
      const resumed = yield* orchestrator.getThreadProjection(childId);
      assert.include(resumed.thread.forkResumedRetirements ?? [], commandId);
      assert.equal(resumed.thread.activeProviderThreadId, resumed.runs.at(-1)?.providerThreadId);
      assert.isNotNull(resumed.thread.activeProviderThreadId);
      assert.deepEqual(
        yield* sql`SELECT command_id, status FROM orchestration_command_receipts
        WHERE command_id IN (${commandId}, ${`send:${childId}:explicit human revival`}) ORDER BY command_id`,
        [
          { command_id: commandId, status: "accepted" },
          { command_id: `send:${childId}:explicit human revival`, status: "accepted" },
        ],
      );
      assert.deepEqual(
        yield* sql`SELECT effect_type FROM orchestration_v2_effect_outbox
        WHERE command_id = ${commandId} AND effect_type = 'delegated-tasks.stop'`,
        [{ effect_type: "delegated-tasks.stop" }],
      );
      // Execute the actual durable effect's service after the human admission committed.
      yield* threads.stopDelegatedTasks({ threadId: parentId, commandId });
      const after = yield* orchestrator.getThreadProjection(childId);
      assert.deepEqual(
        {
          acknowledgements: after.thread.forkResumedRetirements,
          runs: after.runs.map((run) => run.status),
          token: after.thread.forkRetirement?.token,
        },
        {
          acknowledgements: resumed.thread.forkResumedRetirements,
          runs: resumed.runs.map((run) => run.status),
          token: resumed.thread.forkRetirement?.token,
        },
      );
    }).pipe(Effect.provide(layerTest)),
);

const pullRequest = (number: number) => ({
  host: "github.com",
  repository: "pingdotgg/t3code",
  number,
});

it.effect(
  "flat native propagation crosses a resumed intermediate and stops its unresumed grandchild",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threads = yield* ThreadManagementService.ThreadManagementService;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const rootId = ThreadId.make("flat-native-root");
      const childId = ThreadId.make("flat-native-child");
      const grandchildId = ThreadId.make("flat-native-grandchild");
      for (const [id, parent] of [
        [rootId, null],
        [childId, rootId],
        [grandchildId, childId],
      ] as const) {
        yield* createWatchingThread(id, 40);
        if (parent !== null) {
          const thread = (yield* orchestrator.getThreadProjection(id)).thread;
          yield* projections.apply({
            id: EventId.make(`flat-lineage:${id}`),
            type: "thread.metadata-updated",
            threadId: id,
            providerInstanceId: instanceId,
            occurredAt: yield* DateTime.now,
            payload: {
              ...thread,
              lineage: {
                parentThreadId: parent,
                rootThreadId: rootId,
                relationshipToParent: "subagent",
              },
            },
          });
        }
      }
      yield* send(grandchildId, "old grandchild work", "start_immediately");
      const commandId = CommandId.make("flat-root-stop");
      yield* orchestrator.dispatch({ type: "thread.stop", commandId, threadId: rootId });
      yield* send(childId, "human intermediate revival", "start_immediately");
      yield* threads.stopDelegatedTasks({ threadId: rootId, commandId });
      const child = yield* orchestrator.getThreadProjection(childId);
      const grandchild = yield* orchestrator.getThreadProjection(grandchildId);
      assert.deepEqual(
        child.runs.map((run) => run.status),
        ["starting"],
      );
      assert.isFalse(yield* isThreadRetired(childId));
      assert.deepEqual(
        grandchild.runs.map((run) => run.status),
        ["interrupted"],
      );
      assert.equal(grandchild.thread.forkRetirement?.token, commandId);
      assert.isTrue(yield* isThreadRetired(grandchildId));
      assert.lengthOf((yield* orchestrator.getThreadProjection(rootId)).subagents, 0);
      assert.lengthOf(child.subagents, 0);
    }).pipe(Effect.provide(layerTest)),
);

it.effect("client commands cannot forge internal propagated Stop provenance", () =>
  Effect.gen(function* () {
    assert.isTrue(
      Exit.isFailure(
        yield* decodeClientCommand({
          type: "thread.stop",
          commandId: "forged",
          threadId: "target",
          forkRetirementStop: { ancestorThreadId: "root", originalToken: "old" },
        }).pipe(Effect.exit),
      ),
    );
    const parsed = yield* decodeClientCommand({
      type: "message.dispatch",
      commandId: "client",
      threadId: "target",
      messageId: "message",
      text: "Human",
      attachments: [],
      dispatchMode: { type: "start_immediately" },
      createdBy: "user",
      creationSource: "web",
      forkRetirementStop: { ancestorThreadId: "root", originalToken: "old" },
    });
    assert.notProperty(parsed, "forkRetirementStop");
  }),
);

it.effect.each(["no-resume", "newer-token"] as const)(
  "delayed native descendant Stop respects %s",
  (ordering) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threads = yield* ThreadManagementService.ThreadManagementService;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const sql = yield* SqlClient.SqlClient;
      const parentId = ThreadId.make(`delayed:${ordering}:parent`);
      const childId = ThreadId.make(`delayed:${ordering}:child`);
      yield* createWatchingThread(parentId, 30);
      yield* createWatchingThread(childId, 31);
      const child = (yield* orchestrator.getThreadProjection(childId)).thread;
      yield* projections.apply({
        id: EventId.make(`delayed:${ordering}:lineage`),
        type: "thread.metadata-updated",
        threadId: childId,
        providerInstanceId: instanceId,
        occurredAt: yield* DateTime.now,
        payload: {
          ...child,
          lineage: {
            parentThreadId: parentId,
            rootThreadId: parentId,
            relationshipToParent: "subagent",
          },
        },
      });
      yield* send(childId, "original work", "start_immediately");
      const originalToken = CommandId.make(`delayed:${ordering}:old`);
      yield* orchestrator.dispatch({
        type: "thread.stop",
        threadId: parentId,
        commandId: originalToken,
      });
      const newerToken = CommandId.make(`delayed:${ordering}:new`);
      if (ordering === "newer-token")
        yield* orchestrator.dispatch({
          type: "thread.stop",
          threadId: parentId,
          commandId: newerToken,
        });
      const before = yield* orchestrator.getThreadProjection(childId);
      yield* threads.stopDelegatedTasks({ threadId: parentId, commandId: originalToken });
      const after = yield* orchestrator.getThreadProjection(childId);
      if (ordering === "no-resume") {
        assert.deepEqual(
          after.runs.map((run) => run.status),
          ["interrupted"],
        );
        assert.isDefined(after.thread.forkRetirement);
      } else {
        assert.deepEqual(
          after.runs.map((run) => run.status),
          before.runs.map((run) => run.status),
        );
        assert.equal(after.thread.forkRetirement?.token, before.thread.forkRetirement?.token);
        const propagatedId = CommandId.make(`${originalToken}:stop:${childId}`);
        assert.deepEqual(
          yield* sql`SELECT status FROM orchestration_command_receipts WHERE command_id = ${propagatedId}`,
          [{ status: "accepted" }],
        );
        assert.lengthOf(
          yield* sql`SELECT event_id FROM orchestration_events WHERE command_id = ${propagatedId}`,
          0,
        );
        assert.lengthOf(
          yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE command_id = ${propagatedId}`,
          0,
        );
        yield* threads.stopDelegatedTasks({ threadId: parentId, commandId: newerToken });
        assert.deepEqual(
          (yield* orchestrator.getThreadProjection(childId)).runs.map((run) => run.status),
          ["interrupted"],
        );
        assert.equal(
          (yield* orchestrator.getThreadProjection(parentId)).thread.forkRetirement?.token,
          newerToken,
        );
      }
    }).pipe(Effect.provide(layerTest)),
);

const createWatchingThread = (threadId: ThreadId, number: number) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make(`create:${threadId}`),
      threadId,
      projectId: ProjectId.make("project:thread-stop"),
      title: threadId,
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });
    yield* watch(threadId, number);
  });

const watch = (threadId: ThreadId, number: number) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    yield* orchestrator.dispatch({
      type: "thread.pull-request.watch",
      commandId: CommandId.make(`watch:${threadId}:${number}`),
      threadId,
      ...pullRequest(number),
      watching: true,
      link: { url: `https://github.com/pingdotgg/t3code/pull/${number}`, source: "agent" },
    });
  });

const send = (threadId: ThreadId, text: string, type: "start_immediately" | "queue_after_active") =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make(`send:${threadId}:${text}`),
      threadId,
      messageId: MessageId.make(`message:${threadId}:${text}`),
      text,
      attachments: [],
      dispatchMode: { type },
      createdBy: "user",
      creationSource: "web",
    });
  });

const delegate = (parentThreadId: ThreadId, task: string) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const parentRun = (yield* orchestrator.getThreadProjection(parentThreadId)).runs.at(-1)!;
    yield* orchestrator.dispatch({
      type: "delegated_task.request",
      commandId: CommandId.make(`delegate:${task}`),
      parentThreadId,
      parentRunId: parentRun.id,
      parentNodeId: parentRun.rootNodeId!,
      task,
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      completionWake: "always",
      createdBy: "agent",
      creationSource: "mcp",
    });
    const projection = yield* orchestrator.getThreadProjection(parentThreadId);
    return projection.subagents.find((candidate) => candidate.prompt === task)!.childThreadId!;
  });

const threadState = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projection = yield* orchestrator.getThreadProjection(threadId);
    return {
      runs: projection.runs.map((run) => `${run.status}${run.queueHeld === true ? ":held" : ""}`),
      watched: (projection.thread.pullRequests ?? [])
        .filter((link) => link.watch !== undefined)
        .map((link) => link.number),
    };
  });

const completeRunWithoutProvider = (threadId: ThreadId, runId: RunId, name: string) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const sink = yield* EventSink.EventSinkV2;
    const run = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
      (run) => run.id === runId,
    )!;
    const now = yield* DateTime.now;
    yield* sink.write({
      commandId: CommandId.make(`command:runtime-reconcile:${name}`),
      events: [
        {
          id: EventId.make(name),
          type: "run.updated",
          threadId,
          runId,
          occurredAt: now,
          payload: { ...run, status: "completed", completedAt: now },
        },
      ],
    });
  });

const consumeCompletion = (request: ProviderContinuationRequests.ProviderContinuationRequest) =>
  Effect.gen(function* () {
    const threads = yield* ThreadManagementService.ThreadManagementService;
    const completion = request.delegatedCompletion!;
    const projection = yield* threads.getThreadRecords(request.threadId, ["runs"]);
    const delivery = projection.runs.find((run) => run.id === completion.parentRunId)!
      .delegatedCompletion!.delivery!;
    yield* threads.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make(`consume:${completion.messageId}:${projection.runs.length}`),
      threadId: request.threadId,
      messageId: completion.messageId,
      text: "Read the delegated result with task_status.",
      attachments: [],
      dispatchMode: { type: "queue_after_active" },
      createdBy: "agent",
      creationSource: "server",
      delegatedCompletion: {
        parentRunId: completion.parentRunId,
        generation: delivery.generation,
        taskIds: delivery.taskIds,
      },
    });
  });

// A held completion is pending visible work, not a lost result. Only the
// ordinary queue.resume command releases it; replay cannot start it twice.
const resumeHeldCompletion = (threadId: ThreadId, messageId: MessageId) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const before = yield* orchestrator.getThreadProjection(threadId);
    const wake = before.runs.find(
      (run) => run.userMessageId === messageId && run.status === "queued",
    )!;
    assert.isDefined(wake);
    assert.isTrue(wake.queueHeld);
    assert.isTrue(
      before.runs.some(
        (run) =>
          run.delegatedCompletion?.delivery?.messageId === messageId &&
          run.delegatedCompletion.delivery.taskIds.length > 0,
      ),
    );
    yield* orchestrator.resumeQueuedRuns;
    assert.equal(
      (yield* orchestrator.getThreadProjection(threadId)).runs.find((run) => run.id === wake.id)!
        .status,
      "queued",
    );
    const sql = yield* SqlClient.SqlClient;
    const starts = () =>
      sql`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE effect_type = 'provider-turn.start' AND json_extract(payload_json, '$.runId') = ${wake.id} AND status IN ('pending', 'running')`;
    assert.lengthOf(yield* starts(), 0);
    const command = {
      type: "queue.resume" as const,
      threadId,
      commandId: CommandId.make(`resume-held:${threadId}:${messageId}`),
    };
    yield* orchestrator.dispatch(command);
    yield* orchestrator.dispatch(command);
    const after = yield* orchestrator.getThreadProjection(threadId);
    assert.lengthOf(
      after.runs.filter((run) => run.userMessageId === messageId && run.status === "starting"),
      1,
    );
    assert.lengthOf(yield* starts(), 1);
  });

it.effect.each(["queued", "starting"] as const)(
  "retains an unread %s delivery until its ordinary queue is resumed",
  (phase) =>
    Effect.gen(function* () {
      const offers =
        yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const parentId = ThreadId.make(`unread-${phase}`);
        yield* createWatchingThread(parentId, 103);
        yield* send(parentId, "delegate", "start_immediately");
        const parentRun = (yield* orchestrator.getThreadProjection(parentId)).runs[0]!;
        const childId = yield* delegate(parentId, `unread child ${phase}`);
        const childRun = (yield* orchestrator.getThreadProjection(childId)).runs[0]!;
        if (phase === "starting")
          yield* completeRunWithoutProvider(parentId, parentRun.id, `parent-finished-${phase}`);
        else yield* send(parentId, "held instructions", "queue_after_active");
        yield* completeRunWithoutProvider(childId, childRun.id, `child-finished-${phase}`);
        yield* orchestrator.recoverDelegatedTask(childId, childRun.id);
        const first = yield* Queue.take(offers);
        yield* first.dispatchIfCurrent!(consumeCompletion(first));
        const dispatched = yield* orchestrator.getThreadProjection(parentId);
        assert.equal(
          dispatched.runs.find((run) => run.userMessageId === first.delegatedCompletion!.messageId)
            ?.status,
          phase,
        );
        yield* orchestrator.dispatch({
          type: "thread.stop",
          commandId: CommandId.make(`stop-unread-${phase}`),
          threadId: parentId,
        });
        yield* orchestrator.recoverDelegatedTasks;
        const stopped = yield* orchestrator.getThreadProjection(parentId);
        assert.equal(stopped.subagents[0]?.completionDelivery?.state, "claimed");
        const pending = stopped.runs.find((run) => run.id === parentRun.id)!.delegatedCompletion!
          .delivery;
        assert.isNotNull(pending);
        yield* send(parentId, "resume unread result", "start_immediately");
        const next = yield* Queue.take(offers);
        yield* next.dispatchIfCurrent!(consumeCompletion(next));
        const resumed = yield* orchestrator.getThreadProjection(parentId);
        const resumeRun = resumed.runs.find(
          (run) => run.userMessageId === MessageId.make(`message:${parentId}:resume unread result`),
        )!;
        yield* completeRunWithoutProvider(parentId, resumeRun.id, `resume-finished-${phase}`);
        if (phase === "queued") yield* resumeHeldCompletion(parentId, pending!.messageId);
        else yield* orchestrator.resumeQueuedRuns;
        const scheduled = yield* orchestrator.getThreadProjection(parentId);
        assert.equal(
          scheduled.runs.find(
            (run) => run.userMessageId === pending!.messageId && run.status === "starting",
          )?.status,
          "starting",
        );
        assert.equal(
          scheduled.runs.filter(
            (run) => run.userMessageId === pending!.messageId && run.status === "starting",
          ).length,
          1,
        );
        if (phase === "queued")
          assert.isFalse(
            scheduled.runs.find(
              (run) =>
                run.userMessageId === MessageId.make(`message:${parentId}:held instructions`),
            )?.queueHeld,
          );
      }).pipe(
        Effect.provide(layerTest),
        Effect.provideService(ProviderContinuationRequests.ProviderContinuationRequests, {
          offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
          take: Effect.never,
        }),
      );
    }),
);

it.effect("retries the same unread mailbox identity when finalization follows resume", () =>
  Effect.gen(function* () {
    const offers =
      yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const parentId = ThreadId.make("late-unread-finalization");
      yield* createWatchingThread(parentId, 106);
      yield* send(parentId, "delegate", "start_immediately");
      const parentRun = (yield* orchestrator.getThreadProjection(parentId)).runs[0]!;
      const childId = yield* delegate(parentId, "late finalization child");
      const childRun = (yield* orchestrator.getThreadProjection(childId)).runs[0]!;
      yield* completeRunWithoutProvider(parentId, parentRun.id, "late-parent-finished");
      yield* completeRunWithoutProvider(childId, childRun.id, "late-child-finished");
      yield* orchestrator.recoverDelegatedTask(childId, childRun.id);
      const first = yield* Queue.take(offers);
      yield* first.dispatchIfCurrent!(consumeCompletion(first));
      yield* send(parentId, "held instructions", "queue_after_active");
      // Runtime reconciliation's existing event marker defers terminal-listener
      // settlement so explicit resume wins before the public recovery pass.
      yield* orchestrator.dispatch({
        type: "thread.stop",
        threadId: parentId,
        commandId: CommandId.make("command:runtime-reconcile:stop-before-late-finalization"),
      });
      yield* send(parentId, "resume unread result", "start_immediately");
      const beforeFinalization = yield* orchestrator.getThreadProjection(parentId);
      const delivery = beforeFinalization.runs.find((run) => run.id === parentRun.id)!
        .delegatedCompletion!.delivery!;
      assert.equal(delivery.messageId, first.delegatedCompletion!.messageId);
      yield* orchestrator.recoverDelegatedTasks;
      const successor = yield* Queue.take(offers);
      assert.equal(successor.delegatedCompletion!.messageId, first.delegatedCompletion!.messageId);
      yield* successor.dispatchIfCurrent!(consumeCompletion(successor));
      const resumed = yield* orchestrator.getThreadProjection(parentId);
      const resumeRun = resumed.runs.find(
        (run) => run.userMessageId === MessageId.make(`message:${parentId}:resume unread result`),
      )!;
      yield* completeRunWithoutProvider(parentId, resumeRun.id, "late-resume-finished");
      yield* resumeHeldCompletion(parentId, successor.delegatedCompletion!.messageId);
      const scheduled = yield* orchestrator.getThreadProjection(parentId);
      assert.equal(
        scheduled.runs.find(
          (run) =>
            run.userMessageId === successor.delegatedCompletion!.messageId &&
            run.status === "starting",
        )?.status,
        "starting",
      );
      assert.isFalse(
        scheduled.runs.find(
          (run) => run.userMessageId === MessageId.make(`message:${parentId}:held instructions`),
        )?.queueHeld,
      );
      assert.lengthOf(
        scheduled.runs.filter(
          (run) =>
            run.userMessageId === successor.delegatedCompletion!.messageId &&
            run.status === "starting",
        ),
        1,
      );
    }).pipe(
      Effect.provide(layerTest),
      Effect.provideService(ProviderContinuationRequests.ProviderContinuationRequests, {
        offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
        take: Effect.never,
      }),
    );
  }),
);

it.effect.each(["thread", "ancestor"] as const)(
  "serializes an eligible consumer with a concurrent %s Stop without inheriting its lock into child fibers",
  (stopTarget) =>
    Effect.gen(function* () {
      const offers =
        yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const locks = yield* ThreadCommandExecutor.ThreadCommandExecutor;
        const ancestorId = ThreadId.make("consumer-stop-ancestor");
        let parentId = ThreadId.make("consumer-stop-race");
        if (stopTarget === "ancestor") {
          yield* createWatchingThread(ancestorId, 104);
          yield* send(ancestorId, "delegate", "start_immediately");
          parentId = yield* delegate(ancestorId, "middle parent");
        } else {
          yield* createWatchingThread(parentId, 104);
          yield* send(parentId, "delegate", "start_immediately");
        }
        const childId = yield* delegate(parentId, "race child");
        const childRun = (yield* orchestrator.getThreadProjection(childId)).runs[0]!;
        yield* completeRunWithoutProvider(childId, childRun.id, "race-child-finished");
        yield* orchestrator.recoverDelegatedTask(childId, childRun.id);
        const request = yield* Queue.take(offers);
        const stopped = yield* request.dispatchIfCurrent!(
          Effect.gen(function* () {
            // The offered consumer owns the same public command serialization interval.
            assert.include(yield* locks.activeKeys, parentId);
            if (stopTarget === "ancestor") assert.include(yield* locks.activeKeys, ancestorId);
            const stop = yield* orchestrator
              .dispatch({
                type: "thread.stop",
                threadId: stopTarget === "ancestor" ? ancestorId : parentId,
                commandId: CommandId.make("concurrent-stop"),
              })
              .pipe(Effect.forkChild);
            yield* consumeCompletion(request);
            assert.isFalse(yield* isThreadRetired(parentId));
            return stop;
          }),
        );
        yield* Fiber.join(Option.getOrThrow(stopped));
        assert.isTrue(yield* isThreadRetired(parentId));
        let dispatchedAfterStop = false;
        yield* request.dispatchIfCurrent!(
          Effect.sync(() => {
            dispatchedAfterStop = true;
          }),
        );
        assert.isFalse(dispatchedAfterStop);
      }).pipe(
        Effect.provide(layerTest),
        Effect.provideService(ProviderContinuationRequests.ProviderContinuationRequests, {
          offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
          take: Effect.never,
        }),
      );
    }).pipe(Effect.scoped),
);

it.effect.each(["archived", "disabled"] as const)(
  "cleans up an actual app-owned offer for an %s parent without retaining the command lock",
  (reason) =>
    Effect.gen(function* () {
      const incoming =
        yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
      const idle = yield* Queue.unbounded<void>();
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const threads = yield* ThreadManagementService.ThreadManagementService;
        const parentId = ThreadId.make(`completion-cleanup-${reason}`);
        yield* createWatchingThread(parentId, 105);
        yield* send(parentId, "delegate", "start_immediately");
        const childId = yield* delegate(parentId, `cleanup child ${reason}`);
        const childRun = (yield* orchestrator.getThreadProjection(childId)).runs[0]!;
        yield* completeRunWithoutProvider(childId, childRun.id, `cleanup-child-finished-${reason}`);
        yield* orchestrator.recoverDelegatedTask(childId, childRun.id);
        if (reason === "archived") {
          yield* orchestrator.dispatch({
            type: "thread.archive",
            threadId: parentId,
            commandId: CommandId.make("archive-before-consume"),
          });
        } else {
          const settings = yield* ServerSettings.ServerSettingsService;
          yield* settings.updateSettings({
            projectSettingsOverrides: {
              [ProjectId.make("project:thread-stop")]: {
                providerInstances: { [instanceId]: { enabled: false } },
              },
            },
          });
          assert.equal(
            (yield* settings.getSettings).projectSettingsOverrides[
              ProjectId.make("project:thread-stop")
            ]?.providerInstances?.[instanceId]?.enabled,
            false,
          );
        }
        yield* Layer.build(
          ProviderContinuationService.layer.pipe(
            Layer.provide(IdAllocator.layer),
            Layer.provide(
              Layer.fresh(ThreadManagementService.layer).pipe(
                Layer.provide(
                  Layer.succeed(
                    ServerSettings.ServerSettingsService,
                    yield* ServerSettings.ServerSettingsService,
                  ),
                ),
              ),
            ),
          ),
        );
        yield* Queue.take(idle);
        yield* Queue.take(idle);
        const settled = yield* orchestrator.getThreadProjection(parentId);
        assert.equal(
          settled.subagents[0]?.completionDelivery?.state,
          reason === "archived" ? "disposed" : "acknowledged",
        );
        // Both the shared worker and a subsequent command on this parent must remain live.
        yield* threads.dispatch({
          type: "thread.metadata.update",
          commandId: CommandId.make(`after-cleanup-${reason}`),
          threadId: parentId,
          title: "Cleanup completed",
        });
        assert.equal(
          (yield* orchestrator.getThreadRecords(parentId, [])).thread.title,
          "Cleanup completed",
        );
      }).pipe(
        Effect.provide(layerTest),
        Effect.provideService(ProviderContinuationRequests.ProviderContinuationRequests, {
          offer: (request) => Queue.offer(incoming, request).pipe(Effect.asVoid),
          take: Queue.offer(idle, undefined).pipe(Effect.andThen(Queue.take(incoming))),
        }),
      );
    }).pipe(Effect.scoped),
);

it.effect.each(["before-stop", "after-stop", "after-resume"] as const)(
  "parks child results offered %s until explicit resume and delivers each identity once",
  (timing) =>
    Effect.gen(function* () {
      const incoming =
        yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
      const idle = yield* Queue.unbounded<void>();
      const offers = yield* Ref.make(0);
      const requests = {
        offer: (request: ProviderContinuationRequests.ProviderContinuationRequest) =>
          Ref.update(offers, (n) => n + 1).pipe(
            Effect.andThen(Queue.offer(incoming, request)),
            Effect.asVoid,
          ),
        take: Queue.offer(idle, undefined).pipe(Effect.andThen(Queue.take(incoming))),
      };
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const sink = yield* EventSink.EventSinkV2;
        const parentId = ThreadId.make("parked-parent");
        yield* createWatchingThread(parentId, 101);
        yield* send(parentId, "delegate work", "start_immediately");
        const childId = yield* delegate(parentId, "late child result");
        const readChildId = yield* delegate(parentId, "result read through task_status");
        const childRun = (yield* orchestrator.getThreadProjection(childId)).runs[0]!;
        const readChildRun = (yield* orchestrator.getThreadProjection(readChildId)).runs[0]!;
        yield* send(parentId, "unrelated queued work", "queue_after_active");
        const stop = orchestrator.dispatch({
          type: "thread.stop",
          threadId: parentId,
          commandId: CommandId.make("park-parent"),
        });
        if (timing === "after-stop") yield* stop;
        const now = yield* DateTime.now;
        // A provider finishes while the durable descendant Stop effect is still pending.
        yield* sink.write({
          commandId: CommandId.make("command:runtime-reconcile:late-child"),
          events: [
            {
              id: EventId.make("late-child-completed"),
              type: "run.updated",
              threadId: childId,
              runId: childRun.id,
              occurredAt: now,
              payload: { ...childRun, status: "completed", completedAt: now },
            },
          ],
        });
        yield* orchestrator.recoverDelegatedTask(childId, childRun.id);
        if (timing !== "after-stop") yield* stop;
        assert.equal(yield* Ref.get(offers), timing === "after-stop" ? 0 : 1);
        yield* sink.write({
          commandId: CommandId.make("command:runtime-reconcile:readable-child"),
          events: [
            {
              id: EventId.make("readable-child-completed"),
              type: "run.updated",
              threadId: readChildId,
              runId: readChildRun.id,
              occurredAt: now,
              payload: { ...readChildRun, status: "completed", completedAt: now },
            },
          ],
        });
        yield* orchestrator.recoverDelegatedTask(readChildId, readChildRun.id);
        const readableTask = (yield* orchestrator.getThreadProjection(parentId)).subagents.find(
          (task) => task.childThreadId === readChildId,
        )!;
        const status = yield* Effect.gen(function* () {
          const mcp = yield* OrchestratorMcpService.OrchestratorMcpService;
          return yield* mcp.taskStatus(
            {
              environmentId: EnvironmentId.make("parked-result-test"),
              capabilities: new Set(["orchestration"]),
              issuedAt: 0,
              requestNamespace: "parked-result-test",
              client: undefined,
              thread: {
                threadId: parentId,
                providerSessionId: "parked-parent-session",
                providerInstanceId: instanceId,
              },
            },
            readableTask.id,
          );
        }).pipe(Effect.provide(layerTaskStatus));
        assert.equal(status.status, "completed");
        assert.equal(status.summary, readableTask.result);
        assert.isNotNull(status.summary);
        // Reading a terminal task acknowledges it, so only the unread result wakes on resume.
        const parked = yield* orchestrator.getThreadProjection(parentId);
        const task = parked.subagents[0]!;
        assert.equal(task.status, "completed");
        assert.isNotNull(task.result);
        assert.equal(task.completionDelivery?.state, "claimed");
        const delivery = parked.runs[0]!.delegatedCompletion!.delivery!;
        assert.deepEqual(delivery.taskIds, [task.id]);
        assert.isFalse(parked.messages.some((message) => message.id === delivery.messageId));
        if (timing === "after-resume")
          yield* send(parentId, "explicit resume", "start_immediately");

        // The real continuation worker acknowledges each consumed queue item by taking again.
        const threads = yield* ThreadManagementService.ThreadManagementService;
        let dispatches = 0;
        let reads = 0;
        yield* Layer.build(
          ProviderContinuationService.layer.pipe(
            Layer.provide(IdAllocator.layer),
            Layer.provide(
              Layer.succeed(ThreadManagementService.ThreadManagementService, {
                ...threads,
                getThreadRecords: (...args) => {
                  reads++;
                  return threads.getThreadRecords(...args);
                },
                dispatch: (command) => {
                  dispatches++;
                  return threads.dispatch(command);
                },
              }),
            ),
          ),
        );
        yield* Queue.take(idle);
        if (timing === "before-stop") yield* Queue.take(idle);
        if (timing === "after-resume") {
          // The ordinary pre-Stop offer wins FIFO consumption, followed by the resume offer.
          yield* Queue.take(idle);
          yield* Queue.take(idle);
        } else {
          // A stale offer is discarded before dispatch, including the recovery reads it triggers.
          assert.equal(reads, 0);
          assert.equal(dispatches, 0);
          yield* send(parentId, "explicit resume", "start_immediately");
          yield* Queue.take(idle);
        }
        const resumed = yield* orchestrator.getThreadProjection(parentId);
        assert.equal(
          resumed.messages.filter((message) => message.id === delivery.messageId).length,
          1,
        );
        assert.equal(
          resumed.runs.filter((run) => run.userMessageId === delivery.messageId).length,
          1,
        );
        const resumeRun = resumed.runs.find(
          (run) => run.userMessageId === MessageId.make(`message:${parentId}:explicit resume`),
        )!;
        yield* sink.write({
          commandId: CommandId.make("command:runtime-reconcile:resume-finished"),
          events: [
            {
              id: EventId.make("resume-finished"),
              type: "run.updated",
              threadId: parentId,
              runId: resumeRun.id,
              occurredAt: now,
              payload: { ...resumeRun, status: "completed", completedAt: now },
            },
          ],
        });
        yield* resumeHeldCompletion(parentId, delivery.messageId);
        const scheduled = yield* orchestrator.getThreadProjection(parentId);
        assert.equal(
          scheduled.runs.find((run) => run.userMessageId === delivery.messageId)?.status,
          "starting",
        );
        assert.isFalse(
          scheduled.runs.find(
            (run) =>
              run.userMessageId === MessageId.make(`message:${parentId}:unrelated queued work`),
          )?.queueHeld,
        );
        // Reconciliation and a second explicit resume cannot create a second delivery.
        yield* orchestrator.recoverDelegatedTask(childId, childRun.id);
        yield* send(parentId, "resume again", "queue_after_active");
        assert.equal(yield* Ref.get(offers), timing === "after-stop" ? 1 : 2);
        assert.equal(dispatches, 1);
      }).pipe(
        Effect.provide(layerTest),
        Effect.provideService(ProviderContinuationRequests.ProviderContinuationRequests, requests),
      );
    }).pipe(Effect.scoped),
);

it.effect("rejects stopped commands before Wight admission and feature interception", () =>
  Effect.gen(function* () {
    let intercepts = 0;
    let wightChecks = 0;
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadId = ThreadId.make("stop-before-interceptors");
      yield* createWatchingThread(threadId, 102);
      yield* orchestrator.dispatch({
        type: "thread.stop",
        threadId,
        commandId: CommandId.make("stop-before-hooks"),
      });
      intercepts = 0;
      wightChecks = 0;
      const exit = yield* Effect.exit(
        orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("refused-before-hooks"),
          threadId,
          messageId: MessageId.make("refused-before-hooks"),
          text: "automatic work",
          attachments: [],
          createdBy: "agent",
          creationSource: "server",
          dispatchMode: { type: "queue_after_active" },
        }),
      );
      assert.isTrue(Exit.isFailure(exit));
      assert.equal(wightChecks, 0);
      assert.equal(intercepts, 0);
    }).pipe(
      Effect.provide(layerTest),
      Effect.provideService(ForkCommandInterceptor, {
        plan: () =>
          Effect.sync(() => {
            intercepts++;
            return null;
          }),
      }),
      Effect.provideService(WightAdmission.AdmissionHooks, {
        permits: () =>
          Effect.sync(() => {
            wightChecks++;
            return true;
          }),
      }),
    );
  }),
);

it.effect.each([
  ["idle", "thread.stop"],
  ["idle", "run.interrupt"],
  ["idle", "subagent.stop"],
  ["running", "subagent.stop"],
  ["bound", "subagent.stop"],
  ["correlated", "subagent.stop"],
  ["archived", "thread.stop"],
  ["missing", "thread.stop"],
  ["running", "thread.stop"],
  ["queued", "thread.stop"],
  ["waiting", "thread.stop"],
  ["correlated", "thread.stop"],
  ["provider-active", "thread.stop"],
  ["roster", "thread.stop"],
  ["background", "thread.stop"],
  ["question", "thread.stop"],
  ["restart", "thread.stop"],
] as const)(
  "Stop clears an orphaned follow-up when its child is finished; its own Stop does unless a run is bound (%s via %s)",
  ([childState, type]) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const parentThreadId = ThreadId.make(`thread:orphan-parent:${childState}`);
      yield* createWatchingThread(parentThreadId, 1);
      yield* send(parentThreadId, "parent", "start_immediately");
      const childThreadId = yield* delegate(parentThreadId, "original task");
      const parent = yield* orchestrator.getThreadProjection(parentThreadId);
      const child = yield* orchestrator.getThreadProjection(childThreadId);
      const original = parent.subagents[0]!;
      const originalItem = parent.turnItems.find((item) => item.type === "subagent")!;
      const originalNode = parent.nodes.find((node) => node.id === original.id)!;
      const taskId = NodeId.make(`node:orphan-followup:${childState}`);
      const itemId = TurnItemId.make(`item:orphan-followup:${childState}`);
      const followupChildThreadId =
        childState === "missing" ? ThreadId.make("thread:missing-child") : childThreadId;
      const now = yield* DateTime.now;
      let sequence = 0;
      const apply = (event: Omit<OrchestrationV2DomainEvent, "id">) =>
        projections.apply({
          ...event,
          id: EventId.make(`event:orphan:${childState}:${sequence++}`),
        } as OrchestrationV2DomainEvent);
      yield* apply({
        type: "subagent.updated",
        threadId: parentThreadId,
        occurredAt: now,
        payload: {
          ...original,
          status: "completed",
          result: "Original result",
          completedAt: now,
          completionDelivery: { state: "acknowledged", observedByRunId: null },
        },
      });
      yield* apply({
        type: "context-transfer.created",
        threadId: parentThreadId,
        occurredAt: now,
        payload: {
          ...child.contextTransfers[0]!,
          id: ContextTransferId.make(`transfer:original-result:${childState}`),
          type: "subagent_result",
          sourceThreadId: childThreadId,
          targetThreadId: parentThreadId,
          sourcePoint: { threadId: childThreadId, runId: child.runs[0]!.id },
          targetRunId: parent.runs[0]!.id,
        },
      });
      yield* apply({
        type: "node.updated",
        threadId: parentThreadId,
        occurredAt: now,
        payload: { ...originalNode, status: "completed", completedAt: now },
      });
      yield* apply({
        type: "turn-item.updated",
        threadId: parentThreadId,
        occurredAt: now,
        payload: { ...originalItem, status: "completed", completedAt: now },
      });
      yield* apply({
        type: "subagent.updated",
        threadId: parentThreadId,
        occurredAt: now,
        payload: {
          ...original,
          id: taskId,
          childThreadId: followupChildThreadId,
          status: "running",
          result: null,
          completedAt: null,
        },
      });
      yield* apply({
        type: "node.updated",
        threadId: parentThreadId,
        occurredAt: now,
        payload: { ...originalNode, id: taskId, status: "running" },
      });
      assert.equal(originalItem.type, "subagent");
      if (originalItem.type !== "subagent") return;
      yield* apply({
        type: "turn-item.updated",
        threadId: parentThreadId,
        occurredAt: now,
        payload: {
          ...originalItem,
          id: itemId,
          nodeId: taskId,
          subagentId: taskId,
          childThreadId: followupChildThreadId,
          status: "running",
        },
      });
      const runAtStart = child.runs[0]!;
      const childRunStatus =
        childState === "running" || childState === "queued" || childState === "waiting"
          ? childState
          : childState === "bound"
            ? "running"
            : "completed";
      yield* apply({
        type: "run.updated",
        threadId: childThreadId,
        occurredAt: now,
        payload: { ...runAtStart, status: "running", delegatedTaskId: taskId },
      });
      assert.equal(
        (yield* orchestrator.getThreadProjection(childThreadId)).runs[0]?.delegatedTaskId,
        taskId,
      );
      // A terminal write based on the attempt's start snapshot loses a later steer binding.
      yield* apply({
        type: "run.updated",
        threadId: childThreadId,
        occurredAt: now,
        payload: {
          ...runAtStart,
          status: childRunStatus,
          completedAt: childRunStatus === "completed" ? now : null,
          ...(childState === "correlated" || childState === "bound"
            ? { delegatedTaskId: taskId }
            : {}),
        },
      });
      assert.equal(
        (yield* orchestrator.getThreadProjection(childThreadId)).runs[0]?.delegatedTaskId,
        childState === "correlated" || childState === "bound" ? taskId : undefined,
      );
      if (childState === "provider-active" || childState === "roster") {
        yield* apply({
          type: "provider-thread.updated",
          threadId: childThreadId,
          occurredAt: now,
          payload: {
            ...child.providerThreads[0]!,
            status: childState === "provider-active" ? "active" : "idle",
            pendingBackgroundTasks:
              childState === "roster" ? [{ taskId: "background-task", kind: "command" }] : [],
          },
        });
      }
      if (childState === "background") {
        yield* apply({
          type: "turn-item.updated",
          threadId: childThreadId,
          occurredAt: now,
          payload: {
            ...originalItem,
            id: TurnItemId.make("item:child-background"),
            threadId: childThreadId,
            runId: runAtStart.id,
            nodeId: runAtStart.rootNodeId,
            type: "command_execution",
            status: "running",
            input: "test background work",
          },
        });
      }
      if (childState === "question") {
        yield* apply({
          type: "runtime-request.updated",
          threadId: childThreadId,
          occurredAt: now,
          payload: {
            id: RuntimeRequestId.make("request:child-question"),
            nodeId: runAtStart.rootNodeId!,
            providerTurnId: null,
            nativeRequestRef: null,
            kind: "user_input",
            status: "pending",
            responseCapability: { type: "message" },
            createdAt: now,
            resolvedAt: null,
          },
        });
      }
      if (childState === "restart") {
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        yield* outbox.enqueue([
          {
            id: `effect:restart-continuation:${runAtStart.id}`,
            commandId: CommandId.make("command:child-restart"),
            threadId: childThreadId,
            request: { type: "provider-runtime.continue", sourceRunId: runAtStart.id },
          },
        ]);
      }
      yield* apply({
        type: "run.updated",
        threadId: parentThreadId,
        occurredAt: now,
        payload: { ...parent.runs[0]!, status: "completed", completedAt: now },
      });
      if (type === "run.interrupt") {
        const run = parent.runs[0]!;
        yield* apply({
          type: "provider-turn.updated",
          threadId: parentThreadId,
          occurredAt: now,
          payload: {
            id: ProviderTurnId.make("provider-turn:orphan-parent"),
            providerThreadId: run.providerThreadId!,
            nodeId: run.rootNodeId!,
            runAttemptId: run.activeAttemptId!,
            nativeTurnRef: null,
            ordinal: 1,
            status: "completed",
            startedAt: now,
            completedAt: now,
          },
        });
        yield* apply({
          type: "run-attempt.updated",
          threadId: parentThreadId,
          occurredAt: now,
          payload: { ...parent.attempts[0]!, status: "completed", completedAt: now },
        });
        yield* apply({
          type: "node.updated",
          threadId: parentThreadId,
          occurredAt: now,
          payload: {
            ...parent.nodes.find((node) => node.id === run.rootNodeId)!,
            status: "completed",
            completedAt: now,
          },
        });
      }
      if (childState === "archived") {
        yield* apply({
          type: "thread.archived",
          threadId: childThreadId,
          occurredAt: now,
          payload: { ...child.thread, archivedAt: now },
        });
      }
      const childBeforeStop = yield* orchestrator.getThreadProjection(childThreadId);
      assert.lengthOf(
        ((yield* orchestrator.getThreadShell(parentThreadId))?.pendingBackgroundTasks ?? []).filter(
          (task) => task.kind === "subagent",
        ),
        1,
      );
      const stop = {
        commandId: CommandId.make(`stop-orphan:${childState}`),
        threadId: parentThreadId,
      };
      yield* orchestrator.dispatch(
        type === "run.interrupt"
          ? { ...stop, type, runId: parent.runs.at(-1)!.id, holdQueue: true }
          : type === "subagent.stop"
            ? { ...stop, type, runId: original.runId!, subagentId: taskId }
            : { ...stop, type },
      );
      const after = yield* orchestrator.getThreadProjection(parentThreadId);
      // The Lineage stop of one subagent leaves the parent's watches alone.
      assert.lengthOf(
        (after.thread.pullRequests ?? []).filter((link) => link.watch !== undefined),
        type === "subagent.stop" ? 1 : 0,
      );
      // A task with a bound run, live or ended, keeps its settlement path.
      const expected =
        childState === "bound" || childState === "correlated"
          ? "running"
          : type === "subagent.stop" || childState === "idle" || childState === "archived"
            ? "cancelled"
            : "running";
      assert.equal(after.subagents.find((task) => task.id === taskId)?.status, expected);
      assert.equal(after.nodes.find((node) => node.id === taskId)?.status, expected);
      assert.equal(after.turnItems.find((item) => item.id === itemId)?.status, expected);
      assert.equal(
        after.subagents.find((task) => task.id === taskId)?.completionDelivery?.state,
        type === "subagent.stop" && expected === "running" ? undefined : "disposed",
      );
      assert.equal(
        after.subagents.find((task) => task.id === original.id)?.result,
        "Original result",
      );
      assert.deepEqual(yield* orchestrator.getThreadProjection(childThreadId), childBeforeStop);
      if (expected === "cancelled") {
        assert.lengthOf(
          (
            (yield* orchestrator.getThreadShell(parentThreadId))?.pendingBackgroundTasks ?? []
          ).filter((task) => task.kind === "subagent"),
          0,
        );
        yield* orchestrator.recoverDelegatedTasks;
        yield* orchestrator.dispatch({
          type: "thread.stop",
          commandId: CommandId.make(`stop-orphan-again:${childState}`),
          threadId: parentThreadId,
        });
        const repeated = yield* orchestrator.getThreadProjection(parentThreadId);
        assert.equal(repeated.subagents.find((task) => task.id === taskId)?.status, "cancelled");
        assert.deepEqual(repeated.messages, after.messages);
        assert.deepEqual(repeated.contextTransfers, after.contextTransfers);
        if (type === "subagent.stop") {
          // A client that missed the cancel, such as one resumed from an older cache, still
          // shows the task as running. Its Stop gets the ended task back.
          const eventStore = yield* EventStore.EventStoreV2;
          const afterSequence = yield* eventStore.latestSequence({ threadId: parentThreadId });
          yield* orchestrator.dispatch({
            ...stop,
            commandId: CommandId.make(`stop-orphan-stale-client:${childState}`),
            type,
            runId: original.runId!,
            subagentId: taskId,
          });
          const resent = yield* eventStore
            .read({ threadId: parentThreadId, afterSequence })
            .pipe(Stream.runCollect);
          assert.deepEqual(
            resent.flatMap(({ event }) =>
              event.type === "subagent.updated" ? [event.payload.status] : [],
            ),
            ["cancelled"],
          );
        }
      }
    }).pipe(Effect.provide(layerTest)),
);

it.effect("Stop still stops the thread when a delegated child cannot be read", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const sql = yield* SqlClient.SqlClient;
    const parentThreadId = ThreadId.make("thread:unreadable-child-parent");
    yield* createWatchingThread(parentThreadId, 1);
    yield* send(parentThreadId, "parent", "start_immediately");
    const childThreadId = yield* delegate(parentThreadId, "unreadable child");
    yield* sql`
      UPDATE orchestration_v2_projection_runs
      SET payload_json = '{"obsolete":true}'
      WHERE thread_id = ${childThreadId}
    `;
    yield* orchestrator.dispatch({
      type: "run.interrupt",
      commandId: CommandId.make("stop:unreadable-child"),
      threadId: parentThreadId,
      runId: (yield* orchestrator.getThreadProjection(parentThreadId)).runs.at(-1)!.id,
      holdQueue: true,
    });
    assert.deepEqual(yield* threadState(parentThreadId), { runs: ["interrupted"], watched: [] });
  }).pipe(Effect.provide(layerTest)),
);

it.effect.each(["run.interrupt", "thread.stop"] as const)(
  "%s Stop ends watches, holds queues, and stops the delegated tasks under the thread",
  (type) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const threads = yield* ThreadManagementService.ThreadManagementService;
      const sql = yield* SqlClient.SqlClient;
      const parentThreadId = ThreadId.make("thread:stop-parent");
      yield* createWatchingThread(parentThreadId, 1);
      yield* send(parentThreadId, "first", "start_immediately");
      const childThreadId = yield* delegate(parentThreadId, "child task");
      assert.isTrue(Exit.isFailure(yield* Effect.exit(watch(childThreadId, 2))));
      const grandchildThreadId = yield* delegate(childThreadId, "grandchild task");
      yield* send(childThreadId, "child follow-up", "queue_after_active");

      const now = yield* DateTime.now;
      const firstRun = (yield* orchestrator.getThreadProjection(parentThreadId)).runs[0]!;
      yield* projections.apply({
        id: EventId.make("event:stop-parent:first-run-completed"),
        type: "run.updated",
        threadId: parentThreadId,
        runId: firstRun.id,
        occurredAt: now,
        payload: { ...firstRun, status: "completed", completedAt: now },
      });
      yield* send(parentThreadId, "second", "start_immediately");
      const secondRun = (yield* orchestrator.getThreadProjection(parentThreadId)).runs.at(-1)!;

      const stopCommandId = CommandId.make("stop-parent");
      yield* orchestrator.dispatch(
        type === "thread.stop"
          ? { type, commandId: stopCommandId, threadId: parentThreadId }
          : {
              type,
              commandId: stopCommandId,
              threadId: parentThreadId,
              runId: secondRun.id,
              holdQueue: true,
            },
      );

      assert.deepEqual(yield* threadState(parentThreadId), {
        runs: ["completed", "interrupted"],
        watched: [],
      });
      const parent = yield* orchestrator.getThreadProjection(parentThreadId);
      // Durable Stop parks results; it no longer disposes the child's mailbox ownership.
      assert.isUndefined(parent.subagents[0]?.completionDelivery);
      const effects = yield* sql<{ readonly effect_type: string }>`
      SELECT effect_type FROM orchestration_v2_effect_outbox WHERE command_id = ${stopCommandId}
    `;
      assert.include(
        effects.map((row) => row.effect_type),
        "delegated-tasks.stop",
      );

      yield* threads.stopDelegatedTasks({ threadId: parentThreadId, commandId: stopCommandId });
      assert.deepEqual(yield* threadState(childThreadId), {
        runs: ["interrupted", "queued:held"],
        watched: [],
      });
      assert.deepEqual((yield* threadState(grandchildThreadId)).runs, ["interrupted"]);

      yield* threads.stopDelegatedTasks({ threadId: parentThreadId, commandId: stopCommandId });
      assert.deepEqual((yield* threadState(childThreadId)).runs, ["interrupted", "queued:held"]);
      // The walk stops each level itself, so the child stops it sends queue no walk of their own.
      const walks = yield* sql<{ readonly command_id: string }>`
      SELECT command_id FROM orchestration_v2_effect_outbox
      WHERE effect_type = 'delegated-tasks.stop'
    `;
      assert.deepEqual(
        walks.map((row) => row.command_id),
        [stopCommandId],
      );
    }).pipe(Effect.provide(layerTest)),
);

it.effect("a repeated Stop that changes nothing still stops the delegated tasks", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const sql = yield* SqlClient.SqlClient;
    const parentThreadId = ThreadId.make("thread:stop-again");
    yield* createWatchingThread(parentThreadId, 1);
    yield* send(parentThreadId, "first", "start_immediately");
    yield* delegate(parentThreadId, "stop-again child");
    const now = yield* DateTime.now;
    const run = (yield* orchestrator.getThreadProjection(parentThreadId)).runs[0]!;
    yield* projections.apply({
      id: EventId.make("event:stop-again:run-completed"),
      type: "run.updated",
      threadId: parentThreadId,
      runId: run.id,
      occurredAt: now,
      payload: { ...run, status: "completed", completedAt: now },
    });
    yield* orchestrator.dispatch({
      type: "thread.stop",
      commandId: CommandId.make("stop-again:first"),
      threadId: parentThreadId,
    });

    // The first Stop already disposed the delivery and ended the watch, so this one writes no
    // events, like Stop on a thread an older server stopped without stopping its children.
    const again = yield* orchestrator.dispatch({
      type: "thread.stop",
      commandId: CommandId.make("stop-again:second"),
      threadId: parentThreadId,
    });
    assert.lengthOf(again.storedEvents, 0);
    const walks = yield* sql<{ readonly command_id: string }>`
      SELECT command_id FROM orchestration_v2_effect_outbox
      WHERE effect_type = 'delegated-tasks.stop'
    `;
    assert.deepEqual(
      walks.map((row) => row.command_id),
      ["stop-again:first", "stop-again:second"],
    );
  }).pipe(Effect.provide(layerTest)),
);

it.effect.each(["thread.stop", "run.interrupt"] as const)(
  "%s cancels buffered wakes and preserves durable queued messages",
  (type) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadId = ThreadId.make(`thread:stop-buffered:${type}`);
      yield* createWatchingThread(threadId, 5);
      yield* send(threadId, "active", "start_immediately");
      yield* send(threadId, "user follow-up", "queue_after_active");
      for (const creationSource of ["provider", "server"] as const) {
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(`notification:${type}:${creationSource}`),
          threadId,
          messageId: MessageId.make(`notification:${type}:${creationSource}`),
          text: "Background activity updated",
          attachments: [],
          createdBy: "agent",
          creationSource,
          notification: {
            source: { kind: "background_task" },
            outcome: "updated",
            summary: "Background activity updated",
          },
          dispatchMode: { type: "queue_after_active" },
        });
      }
      const before = yield* orchestrator.getThreadProjection(threadId);
      const buffered = before.runs.find((run) =>
        before.messages.some(
          (message) => message.id === run.userMessageId && message.creationSource === "provider",
        ),
      )!;
      const stop = { commandId: CommandId.make(`stop:${type}`), threadId };
      yield* orchestrator.dispatch(
        type === "run.interrupt"
          ? { ...stop, type, runId: before.runs[0]!.id, holdQueue: true }
          : { ...stop, type },
      );
      const stopped = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(stopped.runs.find((run) => run.id === buffered.id)?.status, "cancelled");
      assert.equal(
        stopped.attempts.find((attempt) => attempt.runId === buffered.id)?.status,
        "cancelled",
      );
      assert.equal(
        stopped.nodes.find((node) => node.id === buffered.rootNodeId)?.status,
        "cancelled",
      );
      assert.deepEqual(
        stopped.runs.filter((run) => run.status === "queued").map((run) => run.queueHeld),
        [true, true],
      );
    }).pipe(Effect.provide(layerTest)),
);

it.effect("a message the user sends after Stop releases the held queue; an agent's does not", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const threadId = ThreadId.make("thread:stop-release");
    const agentSend = (text: string) =>
      orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(`agent-send:${text}`),
        threadId,
        messageId: MessageId.make(`message:${threadId}:${text}`),
        text,
        attachments: [],
        dispatchMode: { type: "queue_after_active" },
        createdBy: "agent",
        creationSource: "mcp",
      });
    yield* createWatchingThread(threadId, 6);
    yield* send(threadId, "active", "start_immediately");
    yield* send(threadId, "queued before the stop", "queue_after_active");
    yield* orchestrator.dispatch({
      type: "thread.stop",
      commandId: CommandId.make("stop-release"),
      threadId,
    });
    assert.deepEqual((yield* threadState(threadId)).runs, ["interrupted", "queued:held"]);

    // An agent's message starts on the idle thread, but the hold is the user's.
    yield* agentSend("agent nudge");
    assert.deepEqual((yield* threadState(threadId)).runs, [
      "interrupted",
      "queued:held",
      "starting",
    ]);
    const nudge = (yield* orchestrator.getThreadProjection(threadId)).runs.at(-1)!;
    const now = yield* DateTime.now;
    yield* projections.apply({
      id: EventId.make("event:stop-release:nudge-completed"),
      type: "run.updated",
      threadId,
      runId: nudge.id,
      occurredAt: now,
      payload: { ...nudge, status: "completed", completedAt: now },
    });

    // A scheduled task the user created is not the user coming back.
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make("scheduled-send"),
      threadId,
      messageId: MessageId.make(`message:${threadId}:scheduled`),
      text: "Nightly check",
      attachments: [],
      dispatchMode: { type: "queue_after_active" },
      createdBy: "user",
      creationSource: "web",
      scheduledTaskId: ScheduledTaskId.make("scheduled:nightly"),
    });
    const scheduled = (yield* orchestrator.getThreadProjection(threadId)).runs.at(-1)!;
    assert.deepEqual((yield* threadState(threadId)).runs, [
      "interrupted",
      "queued:held",
      "completed",
      "starting",
    ]);
    yield* projections.apply({
      id: EventId.make("event:stop-release:scheduled-completed"),
      type: "run.updated",
      threadId,
      runId: scheduled.id,
      occurredAt: now,
      payload: { ...scheduled, status: "completed", completedAt: now },
    });

    // The user is back: the held message follows their run, and a wake that
    // queues behind it is not held by the old Stop.
    yield* send(threadId, "back", "start_immediately");
    yield* agentSend("task finished");
    assert.deepEqual((yield* threadState(threadId)).runs, [
      "interrupted",
      "queued",
      "completed",
      "completed",
      "starting",
      "queued",
    ]);
  }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "thread.stop ends an idle thread's watches and accepts a thread with nothing to stop",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadId = ThreadId.make("thread:stop-idle");
      yield* createWatchingThread(threadId, 3);

      yield* orchestrator.dispatch({
        type: "thread.stop",
        commandId: CommandId.make("stop-idle"),
        threadId,
      });
      assert.deepEqual(yield* threadState(threadId), { runs: [], watched: [] });

      const again = yield* orchestrator.dispatch({
        type: "thread.stop",
        commandId: CommandId.make("stop-idle-again"),
        threadId,
      });
      assert.isTrue(
        again.storedEvents.some(
          (stored) =>
            stored.event.type === "thread.metadata-updated" &&
            stored.event.payload.forkRetirement?.token === CommandId.make("stop-idle-again"),
        ),
      );
      assert.deepEqual(yield* threadState(threadId), { runs: [], watched: [] });
    }).pipe(Effect.provide(layerTest)),
);

it.effect.each([false, true])(
  "a completed watch records whether it ended through Stop (%s)",
  (stopped) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const threadId = ThreadId.make(`thread:watch-completion:${stopped}`);
      yield* createWatchingThread(threadId, 7);
      yield* send(threadId, "watch", "start_immediately");
      const run = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
      const now = yield* DateTime.now;
      const completedRun = {
        ...run,
        providerThreadId: run.providerThreadId ?? ProviderThreadId.make("provider-thread:watch"),
        status: "completed" as const,
        startedAt: now,
        completedAt: now,
      };
      yield* projections.apply({
        id: EventId.make("event:watch-completed"),
        type: "run.updated",
        threadId,
        occurredAt: now,
        payload: completedRun,
      });
      assert.isFalse((yield* projections.getThreadShell(threadId))?.latestRunStopRequested);

      yield* orchestrator.dispatch(
        stopped
          ? { type: "thread.stop", commandId: CommandId.make("stop-watch"), threadId }
          : {
              type: "thread.pull-request.watch",
              commandId: CommandId.make("finish-watch"),
              threadId,
              ...pullRequest(7),
              watching: false,
            },
      );
      const shell = yield* projections.getThreadShell(threadId);
      const snapshot = (yield* projections.getShellSnapshot()).threads.find(
        (thread) => thread.id === threadId,
      );
      const memoryShell = ProjectionStore.threadShellFromProjection(
        yield* orchestrator.getThreadProjection(threadId),
      );
      for (const result of [shell, snapshot, memoryShell]) {
        assert.equal(result?.status, "completed");
        assert.isEmpty(result?.pendingBackgroundTasks ?? []);
        assert.equal(result?.latestRunStopRequested, stopped);
      }

      yield* send(threadId, "next", "start_immediately");
      assert.isFalse((yield* projections.getThreadShell(threadId))?.latestRunStopRequested);
    }).pipe(Effect.provide(layerTest)),
);

it.effect("a run Stop reached cannot delegate or start a watch, even after it ends", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const threadId = ThreadId.make("thread:stop-barrier");
    yield* createWatchingThread(threadId, 4);
    yield* send(threadId, "work", "start_immediately");
    const now = yield* DateTime.now;
    const run = { ...(yield* orchestrator.getThreadProjection(threadId)).runs[0]!, startedAt: now };
    yield* projections.apply({
      id: EventId.make("event:stop-barrier:running"),
      type: "run.updated",
      threadId,
      runId: run.id,
      occurredAt: now,
      payload: { ...run, status: "running" },
    });
    yield* send(threadId, "queued", "queue_after_active");
    const queued = (yield* orchestrator.getThreadProjection(threadId)).runs.at(-1)!;
    yield* orchestrator.dispatch({
      type: "queued-run.cancel",
      commandId: CommandId.make("cancel-queued"),
      threadId,
      runId: queued.id,
    });
    yield* projections.apply({
      id: EventId.make("event:stop-barrier:interrupt-request"),
      type: "turn-item.updated",
      threadId,
      runId: run.id,
      occurredAt: now,
      payload: {
        id: TurnItemId.make("turn-item:stop-barrier:interrupt-request"),
        threadId,
        runId: run.id,
        nodeId: run.rootNodeId!,
        providerThreadId: run.providerThreadId!,
        providerTurnId: null,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: 1_000,
        status: "completed",
        title: "Interrupt requested",
        startedAt: now,
        completedAt: now,
        updatedAt: now,
        type: "run_interrupt_request",
        message: "Stop",
      },
    });

    assert.isTrue(Exit.isFailure(yield* Effect.exit(delegate(threadId, "late task"))));
    assert.isTrue(Exit.isFailure(yield* Effect.exit(watch(threadId, 5))));

    yield* projections.apply({
      id: EventId.make("event:stop-barrier:interrupted"),
      type: "run.updated",
      threadId,
      runId: run.id,
      occurredAt: now,
      payload: { ...run, status: "interrupted", completedAt: now },
    });
    assert.isTrue(Exit.isFailure(yield* Effect.exit(watch(threadId, 5))));
    assert.deepEqual(yield* threadState(threadId), {
      runs: ["interrupted", "cancelled"],
      watched: [4],
    });

    for (const watching of [false, true]) {
      yield* orchestrator.dispatch({
        type: "thread.pull-request.watch",
        commandId: CommandId.make(`manual-watch:${watching}`),
        threadId,
        ...pullRequest(4),
        watching,
      });
    }
    assert.deepEqual((yield* threadState(threadId)).watched, [4]);
  }).pipe(Effect.provide(layerTest)),
);

it.effect("thread.stop keeps a restart continuation of the stopped run from starting", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const sql = yield* SqlClient.SqlClient;
    const threadId = ThreadId.make("thread:stop-restart");
    yield* createWatchingThread(threadId, 8);
    yield* send(threadId, "work", "start_immediately");
    const now = yield* DateTime.now;
    const run = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
    yield* projections.apply({
      id: EventId.make("event:stop-restart:cancelled"),
      type: "run.updated",
      threadId,
      runId: run.id,
      occurredAt: now,
      payload: { ...run, status: "cancelled", startedAt: null, completedAt: now },
    });
    const at = DateTime.formatIso(now);
    yield* sql`
      INSERT INTO orchestration_v2_effect_outbox (
        effect_id, command_id, thread_id, effect_type, payload_json, status,
        attempt_count, available_at, created_at, updated_at
      ) VALUES (
        ${`effect:restart-continuation:${run.id}`}, 'command:restart', ${threadId},
        'provider-runtime.continue',
        ${encodeEffectRequest({ type: "provider-runtime.continue", sourceRunId: run.id })},
        'pending', 0, ${at}, ${at}, ${at}
      )
    `;

    yield* orchestrator.dispatch({
      type: "thread.stop",
      commandId: CommandId.make("stop-restart"),
      threadId,
    });
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make("restart-continuation"),
      threadId,
      messageId: MessageId.make("message:restart-continuation"),
      text: "Continue.",
      attachments: [],
      dispatchMode: { type: "start_immediately" },
      createdBy: "agent",
      creationSource: "server",
      restartContinuationOfRunId: run.id,
    });
    assert.deepEqual(yield* threadState(threadId), { runs: ["cancelled"], watched: [] });
  }).pipe(Effect.provide(layerTest)),
);

it.effect.each(["thread.stop", "run.interrupt"] as const)(
  "a successful %s blocks a newer restart continuation when completion timestamps tie",
  (stopType) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const sql = yield* SqlClient.SqlClient;
      const threadId = ThreadId.make("thread:stop-resumed-queue");
      yield* createWatchingThread(threadId, 12);
      yield* send(threadId, "first", "start_immediately");
      yield* send(threadId, "queued", "queue_after_active");
      const now = yield* DateTime.now;
      const [first, queued] = (yield* orchestrator.getThreadProjection(threadId)).runs;
      assert.isDefined(first);
      assert.isDefined(queued);
      for (const run of [first!, queued!]) {
        yield* projections.apply({
          id: EventId.make(`event:stop-resumed-queue:hold:${run.id}`),
          type: "run.updated",
          threadId,
          runId: run.id,
          occurredAt: now,
          payload:
            run.id === first!.id
              ? { ...run, status: "completed", completedAt: now }
              : { ...run, queueHeld: true },
        });
      }
      yield* send(threadId, "restart source", "start_immediately");
      const source = (yield* orchestrator.getThreadProjection(threadId)).runs.at(-1)!;
      yield* projections.apply({
        id: EventId.make("event:stop-resumed-queue:restart-cancelled"),
        type: "run.updated",
        threadId,
        runId: source.id,
        occurredAt: now,
        payload: { ...source, status: "cancelled", startedAt: now, completedAt: now },
      });
      const at = DateTime.formatIso(now);
      yield* sql`
        INSERT INTO orchestration_v2_effect_outbox (
          effect_id, command_id, thread_id, effect_type, payload_json, status,
          attempt_count, available_at, created_at, updated_at
        ) VALUES (
          ${`effect:restart-continuation:${source.id}`}, 'command:restart', ${threadId},
          'provider-runtime.continue',
          ${encodeEffectRequest({ type: "provider-runtime.continue", sourceRunId: source.id })},
          'pending', 0, ${at}, ${at}, ${at}
        )
      `;
      yield* orchestrator.dispatch({
        type: "queue.resume",
        commandId: CommandId.make("resume-older-queue"),
        threadId,
      });
      assert.equal(
        (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (run) => run.id === queued!.id,
        )?.status,
        "starting",
      );
      yield* orchestrator.dispatch(
        stopType === "thread.stop"
          ? { type: "thread.stop", commandId: CommandId.make("stop-resumed-queue"), threadId }
          : {
              type: "run.interrupt",
              commandId: CommandId.make("stop-resumed-queue"),
              threadId,
              runId: queued!.id,
              holdQueue: true,
            },
      );
      const stopped = yield* orchestrator.getThreadProjection(threadId);
      const interrupted = stopped.runs.find((run) => run.id === queued!.id)!;
      assert.equal(interrupted.status, "interrupted");
      assert.equal(DateTime.toEpochMillis(interrupted.completedAt!), DateTime.toEpochMillis(now));
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("restart-after-stop"),
        threadId,
        messageId: MessageId.make("message:restart-after-stop"),
        text: "Continue.",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        createdBy: "agent",
        creationSource: "server",
        restartContinuationOfRunId: source.id,
      });
      assert.deepEqual(yield* threadState(threadId), {
        runs: ["completed", "cancelled", "interrupted"],
        watched: [],
      });
    }).pipe(Effect.provide(layerTest.pipe(Layer.provideMerge(TestClock.layer())))),
);

it.effect.each([
  { ending: "cancelled", question: "resolved" },
  { ending: "interrupted", question: "resolved" },
  { ending: "completed", question: "pending" },
  { ending: "failed", question: "pending" },
] as const)(
  "a $ending delegated task leaves its async question $question",
  ({ ending, question }) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const parentThreadId = ThreadId.make(`thread:stop-question-parent:${ending}`);
      yield* createWatchingThread(parentThreadId, 20);
      yield* send(parentThreadId, "first", "start_immediately");
      const childThreadId = yield* delegate(parentThreadId, `question task ${ending}`);
      const childRun = (yield* orchestrator.getThreadProjection(childThreadId)).runs[0]!;
      assert.equal(
        (yield* orchestrator.getThreadProjection(childThreadId)).nodes.find(
          (node) => node.id === childRun.rootNodeId,
        )?.runId,
        childRun.id,
      );
      const now = yield* DateTime.now;
      yield* projections.apply({
        id: EventId.make("event:stop-question:request"),
        type: "runtime-request.updated",
        threadId: childThreadId,
        runId: childRun.id,
        occurredAt: now,
        payload: {
          id: RuntimeRequestId.make("request:stop-question"),
          nodeId: childRun.rootNodeId!,
          providerTurnId: null,
          nativeRequestRef: null,
          kind: "user_input",
          status: "pending",
          responseCapability: { type: "message" },
          createdAt: now,
          resolvedAt: null,
        },
      });
      const request = Effect.map(
        orchestrator.getThreadProjection(childThreadId),
        (projection) => projection.runtimeRequests[0],
      );

      yield* orchestrator.recoverDelegatedTask(childThreadId, childRun.id);
      assert.equal((yield* request)?.status, "pending");
      if (ending === "interrupted") {
        yield* orchestrator.dispatch({
          type: "thread.stop",
          commandId: CommandId.make("stop-question-child"),
          threadId: childThreadId,
        });
      } else {
        yield* projections.apply({
          id: EventId.make(`event:stop-question:${ending}`),
          type: "run.updated",
          threadId: childThreadId,
          runId: childRun.id,
          occurredAt: now,
          payload: { ...childRun, status: ending, startedAt: now, completedAt: now },
        });
      }
      yield* orchestrator.recoverDelegatedTask(childThreadId, childRun.id);
      assert.equal((yield* request)?.status, question);
      assert.equal((yield* request)?.decision, question === "resolved" ? "cancel" : undefined);
    }).pipe(Effect.provide(layerTest)),
);

it.effect("a delegated task that cannot be stopped fails the walk after its siblings stop", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const threads = yield* ThreadManagementService.ThreadManagementService;
    const parentThreadId = ThreadId.make("thread:stop-partial");
    yield* createWatchingThread(parentThreadId, 6);
    yield* send(parentThreadId, "work", "start_immediately");
    const childThreadId = yield* delegate(parentThreadId, "stoppable task");
    const task = (yield* orchestrator.getThreadProjection(parentThreadId)).subagents[0]!;
    yield* projections.apply({
      id: EventId.make("event:stop-partial:missing-task"),
      type: "subagent.updated",
      threadId: parentThreadId,
      runId: task.runId!,
      nodeId: NodeId.make("node:stop-partial:missing"),
      providerInstanceId: instanceId,
      occurredAt: yield* DateTime.now,
      payload: {
        ...task,
        id: NodeId.make("node:stop-partial:missing"),
        childThreadId: ThreadId.make("thread:stop-partial:missing"),
        startedAt: DateTime.subtract(task.startedAt!, { hours: 1 }),
      },
    });

    // The production worker invokes this walk only after its ancestor Stop committed.
    yield* orchestrator.dispatch({
      type: "thread.stop",
      commandId: CommandId.make("stop-partial"),
      threadId: parentThreadId,
    });
    const walked = yield* Effect.exit(
      threads.stopDelegatedTasks({
        threadId: parentThreadId,
        commandId: CommandId.make("stop-partial"),
      }),
    );
    assert.isTrue(Exit.isFailure(walked));
    assert.deepEqual((yield* threadState(childThreadId)).runs, ["interrupted"]);
  }).pipe(Effect.provide(layerTest)),
);

it.effect("thread.stop marks a turn it cannot interrupt so a late agent watch is refused", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const threadId = ThreadId.make("thread:stop-lost-session");
    yield* createWatchingThread(threadId, 11);
    yield* send(threadId, "work", "start_immediately");
    const now = yield* DateTime.now;
    const run = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
    yield* projections.apply({
      id: EventId.make("event:stop-lost-session:running"),
      type: "run.updated",
      threadId,
      runId: run.id,
      occurredAt: now,
      payload: { ...run, status: "running", startedAt: now },
    });
    yield* projections.apply({
      id: EventId.make("event:stop-lost-session:turn"),
      type: "provider-turn.updated",
      threadId,
      occurredAt: now,
      payload: {
        id: ProviderTurnId.make("provider-turn:stop-lost-session"),
        providerThreadId: run.providerThreadId!,
        nodeId: run.rootNodeId!,
        runAttemptId: run.activeAttemptId!,
        nativeTurnRef: null,
        ordinal: 1,
        status: "running",
        startedAt: now,
        completedAt: null,
      },
    });

    yield* orchestrator.dispatch({
      type: "thread.stop",
      commandId: CommandId.make("stop-lost-session"),
      threadId,
    });
    assert.isTrue(Exit.isFailure(yield* Effect.exit(watch(threadId, 12))));
    assert.deepEqual(yield* threadState(threadId), { runs: ["running"], watched: [] });
  }).pipe(Effect.provide(layerTest)),
);

it.effect.each(["missing session", "completed", "failed", "cancelled"] as const)(
  "a native child Stop only settles a confirmed %s outcome and preserves its owner and sibling",
  (providerState) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const sql = yield* SqlClient.SqlClient;
      const threadId = ThreadId.make("thread:native-child-stop");
      yield* createWatchingThread(threadId, 20);
      yield* send(threadId, "owner", "start_immediately");
      yield* delegate(threadId, "native-child-fixture");
      const before = yield* orchestrator.getThreadProjection(threadId);
      const run = before.runs[0]!;
      const now = yield* DateTime.now;
      const subagent = {
        ...before.subagents[0]!,
        origin: "provider_native" as const,
        driver: CLAUDE_PROVIDER,
        providerInstanceId: ProviderInstanceId.make("claudeAgent"),
        nativeTaskRef: {
          driver: CLAUDE_PROVIDER,
          nativeId: "claude-task",
          strength: "strong" as const,
        },
        status: "running" as const,
        startedAt: now,
      };
      yield* projections.apply({
        id: EventId.make("event:native-child-stop:agent"),
        type: "subagent.updated",
        threadId,
        runId: run.id,
        nodeId: subagent.id,
        occurredAt: now,
        payload: subagent,
      });
      const sibling = {
        ...subagent,
        id: NodeId.make("node:native-stop-sibling"),
        childThreadId: null,
      };
      yield* projections.apply({
        id: EventId.make("event:native-child-stop:sibling"),
        type: "subagent.updated",
        threadId,
        runId: run.id,
        nodeId: sibling.id,
        occurredAt: now,
        payload: sibling,
      });
      const item = before.turnItems.find(
        (item) => item.type === "subagent" && item.subagentId === subagent.id,
      )!;
      yield* projections.apply({
        id: EventId.make("event:native-child-stop:item"),
        type: "turn-item.updated",
        threadId,
        runId: run.id,
        occurredAt: now,
        payload: { ...item, status: "running", completedAt: null },
      });
      const childRoot = {
        ...before.nodes.find((node) => node.id === run.rootNodeId)!,
        id: NodeId.make("node:native-stop-child-root"),
        threadId: subagent.childThreadId!,
        runId: null,
        parentNodeId: null,
        status: "running" as const,
        countsForRun: false,
        completedAt: null,
      };
      yield* projections.apply({
        id: EventId.make("event:native-child-stop:child-root"),
        type: "node.updated",
        threadId: childRoot.threadId,
        nodeId: childRoot.id,
        occurredAt: now,
        payload: childRoot,
      });
      const childTurnId = ProviderTurnId.make("turn:native-stop-child");
      yield* projections.apply({
        id: EventId.make("event:native-child-stop:child-turn"),
        type: "provider-turn.updated",
        threadId: childRoot.threadId,
        nodeId: childRoot.id,
        occurredAt: now,
        payload: {
          id: childTurnId,
          providerThreadId: ProviderThreadId.make("provider-thread:native-stop-child"),
          nodeId: childRoot.id,
          runAttemptId: null,
          nativeTurnRef: null,
          ordinal: 1,
          status: "running",
          startedAt: now,
          completedAt: null,
        },
      });
      const providerThread = before.providerThreads.find(
        (thread) => thread.id === run.providerThreadId,
      )!;
      yield* projections.apply({
        id: EventId.make("event:native-child-stop:provider-thread"),
        type: "provider-thread.updated",
        threadId,
        occurredAt: now,
        payload: {
          ...providerThread,
          driver: CLAUDE_PROVIDER,
          providerInstanceId: ProviderInstanceId.make("claudeAgent"),
          providerSessionId: ProviderSessionId.make("session:native-child-stop"),
        },
      });
      const providerTurnId = ProviderTurnId.make("turn:native-child-stop");
      yield* projections.apply({
        id: EventId.make("event:native-child-stop:provider-turn"),
        type: "provider-turn.updated",
        threadId,
        occurredAt: now,
        payload: {
          id: providerTurnId,
          providerThreadId: providerThread.id,
          nodeId: run.rootNodeId!,
          runAttemptId: run.activeAttemptId!,
          nativeTurnRef: null,
          ordinal: 1,
          status: "completed",
          startedAt: now,
          completedAt: now,
        },
      });
      yield* projections.apply({
        id: EventId.make("event:native-child-stop:owner-completed"),
        type: "run.updated",
        threadId,
        runId: run.id,
        occurredAt: now,
        payload: { ...run, status: "completed", startedAt: now, completedAt: now },
      });
      yield* send(threadId, "queued", "queue_after_active");
      const stateBefore = yield* threadState(threadId);
      const eventStore = yield* EventStore.EventStoreV2;
      const afterSequence = yield* eventStore.latestSequence({ threadId });
      const confirmedStatus =
        providerState === "completed" || providerState === "failed" || providerState === "cancelled"
          ? providerState
          : null;
      const confirmedOutcome =
        confirmedStatus === null
          ? undefined
          : { status: confirmedStatus, completedAt: now, result: "Actual provider outcome" };
      const commandId = CommandId.make("stop-native-child");
      yield* orchestrator.dispatch({
        type: "subagent.stop",
        commandId,
        threadId,
        runId: run.id,
        subagentId: subagent.id,
      });
      assert.deepEqual(yield* threadState(threadId), stateBefore);
      const effects = yield* sql<{ payload_json: string }>`
      SELECT payload_json FROM orchestration_v2_effect_outbox WHERE command_id = ${commandId}
    `;
      assert.equal(effects.length, 1);
      assert.deepEqual(JSON.parse(effects[0]!.payload_json), {
        type: "provider-turn.interrupt",
        providerSessionId: "session:native-child-stop",
        providerThreadId: providerThread.id,
        providerTurnId,
        subagent: { id: subagent.id, nativeTaskId: "claude-task" },
      });
      assert.equal(
        (yield* orchestrator.getThreadProjection(threadId)).subagents[0]?.status,
        "running",
      );
      const unrelated = yield* Effect.exit(
        orchestrator.dispatch({
          type: "subagent.stop",
          commandId: CommandId.make("stop-unknown-child"),
          threadId,
          runId: run.id,
          subagentId: NodeId.make("unknown-child"),
        }),
      );
      assert.isTrue(Exit.isFailure(unrelated));
      yield* TestClock.adjust("1 second");
      yield* sql`DELETE FROM orchestration_v2_effect_outbox WHERE command_id != ${commandId}`;
      if (providerState === "missing session") {
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        yield* worker.drain();
      } else {
        yield* orchestrator.dispatch({
          type: "thread.background-work.settle",
          commandId: CommandId.make("repair-native-child"),
          threadId,
          providerThreadId: providerThread.id,
          providerTurnId,
          subagent: { id: subagent.id, stoppedAt: now, outcome: confirmedOutcome! },
        });
      }
      const settled = yield* orchestrator.getThreadProjection(threadId);
      const cancelled = settled.subagents.find((task) => task.id === subagent.id)!;
      if (confirmedOutcome === undefined) {
        const events = yield* eventStore.read({ threadId, afterSequence }).pipe(Stream.runCollect);
        assert.isFalse(
          events.some(
            ({ event }) =>
              (event.type === "subagent.updated" ||
                event.type === "node.updated" ||
                event.type === "turn-item.updated") &&
              ["completed", "failed", "cancelled", "interrupted"].includes(event.payload.status),
          ),
        );
        assert.equal(cancelled.status, "running");
        assert.equal(
          (yield* orchestrator.getThreadProjection(childRoot.threadId)).nodes.find(
            (node) => node.id === childRoot.id,
          )?.status,
          "running",
        );
        assert.deepEqual(yield* threadState(threadId), stateBefore);
        return;
      }

      const repaired = yield* eventStore.read({ threadId, afterSequence }).pipe(Stream.runCollect);
      const terminal = repaired.flatMap(({ event }) =>
        (event.type === "subagent.updated" ||
          event.type === "node.updated" ||
          event.type === "turn-item.updated") &&
        event.payload.status === confirmedStatus
          ? [event]
          : [],
      );
      assert.deepEqual(terminal.map((event) => event.type).sort(), [
        "node.updated",
        "subagent.updated",
        "turn-item.updated",
      ]);
      for (const event of terminal) {
        assert.equal(event.nodeId, subagent.id);
        assert.equal(
          DateTime.toEpochMillis(event.payload.completedAt!),
          DateTime.toEpochMillis(confirmedOutcome!.completedAt),
        );
        if (event.type === "subagent.updated" || event.type === "turn-item.updated") {
          assert.isDefined(event.payload.updatedAt);
        }
      }

      assert.equal(cancelled.status, confirmedStatus);
      assert.isNotNull(cancelled.completedAt);
      assert.equal(
        DateTime.toEpochMillis(cancelled.completedAt!),
        DateTime.toEpochMillis(confirmedOutcome!.completedAt),
      );
      assert.equal(cancelled.result, confirmedOutcome!.result);
      assert.equal(
        settled.turnItems.find((candidate) => candidate.id === item.id)?.status,
        confirmedStatus,
      );
      const child = yield* orchestrator.getThreadProjection(childRoot.threadId);
      assert.equal(child.nodes.find((node) => node.id === childRoot.id)?.status, confirmedStatus);
      assert.equal(
        child.providerTurns.find((turn) => turn.id === childTurnId)?.status,
        confirmedStatus,
      );
      assert.isNotNull(child.nodes.find((node) => node.id === childRoot.id)?.completedAt);
      assert.equal(settled.subagents.find((task) => task.id === sibling.id)?.status, "running");
      assert.deepEqual(yield* threadState(threadId), stateBefore);
      yield* TestClock.adjust("1 second");
      const resumedAt = yield* DateTime.now;
      yield* projections.apply({
        id: EventId.make("event:native-child-stop:resume"),
        type: "subagent.updated",
        threadId,
        runId: run.id,
        nodeId: subagent.id,
        occurredAt: resumedAt,
        payload: { ...cancelled, status: "running", startedAt: resumedAt, completedAt: null },
      });
      yield* orchestrator.dispatch({
        type: "thread.background-work.settle",
        commandId: CommandId.make("old-stop-after-resume"),
        threadId,
        providerThreadId: providerThread.id,
        providerTurnId,
        subagent: { id: subagent.id, stoppedAt: now, outcome: confirmedOutcome },
      });
      assert.equal(
        (yield* orchestrator.getThreadProjection(threadId)).subagents.find(
          (task) => task.id === subagent.id,
        )?.status,
        "running",
      );
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "explicit messages resume only their named descendant while automatic reset messages stay retired",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threads = yield* ThreadManagementService.ThreadManagementService;
      const parentId = ThreadId.make("thread:retirement-resume:root");
      yield* createWatchingThread(parentId, 80);
      yield* send(parentId, "parent work", "start_immediately");
      const childId = yield* delegate(parentId, "resume child");
      const grandchildId = yield* delegate(childId, "resume grandchild");
      yield* orchestrator.dispatch({
        type: "thread.stop",
        commandId: CommandId.make("stop:retirement-resume"),
        threadId: parentId,
      });
      yield* threads.stopDelegatedTasks({
        threadId: parentId,
        commandId: CommandId.make("stop:retirement-resume"),
      });
      const child = yield* orchestrator.getThreadProjection(childId);
      const reset = {
        type: "message.dispatch" as const,
        commandId: CommandId.make("reset:retirement-resume"),
        threadId: childId,
        messageId: MessageId.make("reset:retirement-resume"),
        text: "Continue after limit reset",
        attachments: [],
        dispatchMode: { type: "start_immediately" as const },
        createdBy: "user" as const,
        creationSource: "server" as const,
        usageLimitContinuationOfRunId: child.runs[0]!.id,
      };
      assert.equal((yield* orchestrator.dispatch(reset).pipe(Effect.result))._tag, "Failure");
      assert.deepEqual(
        (yield* orchestrator.getThreadProjection(childId)).thread.forkResumedRetirements,
        [],
      );
      yield* send(childId, "explicit resume", "start_immediately");
      const resumed = yield* orchestrator.getThreadProjection(childId);
      assert.equal(resumed.runs.at(-1)?.status, "starting");
      assert.isAbove(resumed.thread.forkResumedRetirements?.length ?? 0, 0);
      const grandchild = yield* orchestrator.getThreadProjection(grandchildId);
      assert.deepEqual(grandchild.thread.forkResumedRetirements, []);
      assert.equal(
        (yield* orchestrator
          .dispatch({
            ...reset,
            commandId: CommandId.make("reset:grandchild"),
            messageId: MessageId.make("reset:grandchild"),
            threadId: grandchildId,
            usageLimitContinuationOfRunId: grandchild.runs[0]!.id,
          })
          .pipe(Effect.result))._tag,
        "Failure",
      );
      yield* orchestrator.dispatch({
        type: "thread.stop",
        commandId: CommandId.make("stop:retirement-resume:again"),
        threadId: parentId,
      });
      assert.equal(
        (yield* orchestrator
          .dispatch({
            ...reset,
            commandId: CommandId.make("reset:after-new-stop"),
            messageId: MessageId.make("reset:after-new-stop"),
          })
          .pipe(Effect.result))._tag,
        "Failure",
      );
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "incomplete native ancestry refuses MCP and recovery but records a human turn override without unknown exemptions",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const childId = ThreadId.make("thread:incomplete:child");
      const missingId = ThreadId.make("thread:incomplete:missing");
      yield* createWatchingThread(childId, 81);
      const child = (yield* orchestrator.getThreadProjection(childId)).thread;
      const now = yield* DateTime.now;
      yield* projections.apply({
        id: EventId.make("event:incomplete:link"),
        type: "thread.metadata-updated",
        threadId: childId,
        providerInstanceId: instanceId,
        occurredAt: now,
        payload: {
          ...child,
          lineage: {
            rootThreadId: missingId,
            parentThreadId: missingId,
            relationshipToParent: "subagent",
          },
        },
      });
      const message = {
        type: "message.dispatch" as const,
        commandId: CommandId.make("message:incomplete:mcp"),
        threadId: childId,
        messageId: MessageId.make("message:incomplete:mcp"),
        text: "Work",
        attachments: [],
        dispatchMode: { type: "start_immediately" as const },
        createdBy: "agent" as const,
        creationSource: "mcp" as const,
      };
      const refused = yield* orchestrator.dispatch(message).pipe(Effect.flip);
      assert.equal(refused._tag, "OrchestratorCommandRejectedError");
      if (refused._tag === "OrchestratorCommandRejectedError")
        assert.equal(refused.cause, "Fork transaction rejected: lineage_incomplete.");
      assert.lengthOf((yield* orchestrator.getThreadProjection(childId)).messages, 0);
      assert.equal(
        (yield* orchestrator
          .dispatch({
            ...message,
            commandId: CommandId.make("message:incomplete:server"),
            messageId: MessageId.make("message:incomplete:server"),
            createdBy: "user",
            creationSource: "server",
          })
          .pipe(Effect.result))._tag,
        "Failure",
      );
      yield* send(childId, "authenticated human", "start_immediately");
      const admitted = yield* orchestrator.getThreadProjection(childId);
      assert.equal(admitted.thread.forkLineageOverride?.runId, admitted.runs.at(-1)?.id);
      assert.deepEqual(admitted.thread.forkResumedRetirements, []);
      // Repair discovers an ancestor that the override could never acknowledge.
      yield* createWatchingThread(missingId, 82);
      yield* orchestrator.dispatch({
        type: "thread.stop",
        commandId: CommandId.make("stop:incomplete:discovered"),
        threadId: missingId,
      });
      assert.equal(
        (yield* orchestrator
          .dispatch({
            ...message,
            commandId: CommandId.make("message:incomplete:after-repair"),
            messageId: MessageId.make("message:incomplete:after-repair"),
            creationSource: "server",
          })
          .pipe(Effect.result))._tag,
        "Failure",
      );
      assert.deepEqual(
        (yield* orchestrator.getThreadProjection(childId)).thread.forkResumedRetirements,
        [],
      );
    }).pipe(Effect.provide(layerTest)),
);

it.effect.each(["before settlement", "after reservation", "after queue"] as const)(
  "a parked sibling survives consumed wake settlement while a later result finishes %s",
  (laterPhase) =>
    Effect.gen(function* () {
      const offers =
        yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const parentId = ThreadId.make("review-consumed-finalization");
        yield* createWatchingThread(parentId, 106);
        yield* send(parentId, "delegate", "start_immediately");
        const parentRun = (yield* orchestrator.getThreadProjection(parentId)).runs[0]!;
        const childId = yield* delegate(parentId, "late finalization child");
        const childRun = (yield* orchestrator.getThreadProjection(childId)).runs[0]!;
        const siblingId = yield* delegate(parentId, "parked sibling");
        const siblingRun = (yield* orchestrator.getThreadProjection(siblingId)).runs[0]!;
        const laterId = yield* delegate(parentId, "later result");
        const laterRun = (yield* orchestrator.getThreadProjection(laterId)).runs[0]!;
        yield* completeRunWithoutProvider(parentId, parentRun.id, "late-parent-finished");
        yield* completeRunWithoutProvider(childId, childRun.id, "late-child-finished");
        yield* orchestrator.recoverDelegatedTask(childId, childRun.id);
        const first = yield* Queue.take(offers);
        yield* first.dispatchIfCurrent!(consumeCompletion(first));
        const sink = yield* EventSink.EventSinkV2;
        const active = yield* orchestrator.getThreadProjection(parentId);
        const wakeRun = active.runs.find(
          (run) => run.userMessageId === first.delegatedCompletion!.messageId,
        )!;
        const attempt = active.attempts.find((attempt) => attempt.id === wakeRun.activeAttemptId)!;
        const now = yield* DateTime.now;
        const turnId = ProviderTurnId.make("review-accepted-wake");
        // External provider acceptance is represented by its normal persisted records.
        yield* sink.write({
          commandId: CommandId.make("command:runtime-reconcile:review-provider-start"),
          events: [
            {
              id: EventId.make("review-provider-start-run"),
              type: "run.updated",
              threadId: parentId,
              runId: wakeRun.id,
              occurredAt: now,
              payload: { ...wakeRun, status: "running", startedAt: now },
            },
            {
              id: EventId.make("review-provider-start-attempt"),
              type: "run-attempt.updated",
              threadId: parentId,
              runId: wakeRun.id,
              occurredAt: now,
              payload: { ...attempt, status: "running", providerTurnId: turnId, startedAt: now },
            },
            {
              id: EventId.make("review-provider-start-turn"),
              type: "provider-turn.updated",
              threadId: parentId,
              runId: wakeRun.id,
              occurredAt: now,
              payload: {
                id: turnId,
                providerThreadId: wakeRun.providerThreadId!,
                nodeId: wakeRun.rootNodeId!,
                runAttemptId: attempt.id,
                nativeTurnRef: null,
                ordinal: 1,
                status: "running",
                startedAt: now,
                completedAt: null,
              },
            },
          ],
        });
        yield* completeRunWithoutProvider(siblingId, siblingRun.id, "review-sibling-finished");
        yield* orchestrator.recoverDelegatedTask(siblingId, siblingRun.id);
        assert.equal(
          (yield* orchestrator.getThreadProjection(parentId)).subagents.find(
            (task) => task.childThreadId === siblingId,
          )?.completionDelivery?.state,
          "pending",
        );

        yield* send(parentId, "held instructions", "queue_after_active");
        // Runtime reconciliation's existing event marker defers terminal-listener
        // settlement so explicit resume wins before the public recovery pass.
        yield* orchestrator.dispatch({
          type: "thread.stop",
          threadId: parentId,
          commandId: CommandId.make("command:runtime-reconcile:stop-before-late-finalization"),
        });

        // Provider interruption arrives after Stop; defer only terminal-listener settlement.
        const stoppedWake = (yield* orchestrator.getThreadProjection(parentId)).runs.find(
          (run) => run.id === wakeRun.id,
        )!;
        yield* sink.write({
          commandId: CommandId.make("command:runtime-reconcile:review-provider-ended"),
          events: [
            {
              id: EventId.make("review-provider-ended"),
              type: "run.updated",
              threadId: parentId,
              runId: wakeRun.id,
              occurredAt: now,
              payload: { ...stoppedWake, status: "interrupted", completedAt: now },
            },
          ],
        });
        yield* send(parentId, "resume unread result", "start_immediately");
        const beforeFinalization = yield* orchestrator.getThreadProjection(parentId);
        const delivery = beforeFinalization.runs.find((run) => run.id === parentRun.id)!
          .delegatedCompletion!.delivery!;
        assert.equal(delivery.messageId, first.delegatedCompletion!.messageId);
        // Later results use the same ordinary mailbox path, without resume permissions.
        const finishLater = completeRunWithoutProvider(
          laterId,
          laterRun.id,
          "later-child-finished",
        ).pipe(Effect.andThen(orchestrator.recoverDelegatedTask(laterId, laterRun.id)));
        if (laterPhase === "before settlement") yield* finishLater;
        yield* orchestrator.recoverDelegatedTasks;
        const successor = yield* Queue.take(offers);
        assert.notEqual(
          successor.delegatedCompletion!.messageId,
          first.delegatedCompletion!.messageId,
        );
        if (laterPhase === "after reservation") yield* finishLater;
        yield* successor.dispatchIfCurrent!(consumeCompletion(successor));
        if (laterPhase === "after queue") yield* finishLater;
        const resumed = yield* orchestrator.getThreadProjection(parentId);
        assert.includeMembers(
          [
            ...resumed.runs.find((run) => run.id === parentRun.id)!.delegatedCompletion!.delivery!
              .taskIds,
          ],
          resumed.subagents
            .filter((task) => task.childThreadId === siblingId || task.childThreadId === laterId)
            .map((task) => task.id),
        );
        const resumeRun = resumed.runs.find(
          (run) => run.userMessageId === MessageId.make(`message:${parentId}:resume unread result`),
        )!;
        yield* completeRunWithoutProvider(parentId, resumeRun.id, "late-resume-finished");
        yield* resumeHeldCompletion(parentId, successor.delegatedCompletion!.messageId);
        const scheduled = yield* orchestrator.getThreadProjection(parentId);
        assert.equal(
          scheduled.runs.find(
            (run) => run.userMessageId === successor.delegatedCompletion!.messageId,
          )?.status,
          "starting",
        );
        assert.isFalse(
          scheduled.runs.find(
            (run) => run.userMessageId === MessageId.make(`message:${parentId}:held instructions`),
          )?.queueHeld,
        );
        assert.lengthOf(
          scheduled.runs.filter(
            (run) => run.userMessageId === successor.delegatedCompletion!.messageId,
          ),
          1,
        );
        const resumedWake = scheduled.runs.find(
          (run) => run.userMessageId === successor.delegatedCompletion!.messageId,
        )!;
        yield* completeRunWithoutProvider(parentId, resumedWake.id, "parked-siblings-consumed");
        yield* orchestrator.recoverDelegatedTasks;
        const final = yield* orchestrator.getThreadProjection(parentId);
        for (const child of [siblingId, laterId])
          assert.equal(
            final.subagents.find((task) => task.childThreadId === child)!.completionDelivery?.state,
            "delivered",
          );
      }).pipe(
        Effect.provide(layerTest),
        Effect.provideService(ProviderContinuationRequests.ProviderContinuationRequests, {
          offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
          take: Effect.never,
        }),
      );
    }),
);

it.effect("ancestor Stop wins before producer offer", () =>
  Effect.gen(function* () {
    const offers = yield* Ref.make(0);
    const reached = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const rootId = ThreadId.make("review-offer-root");
      yield* createWatchingThread(rootId, 109);
      yield* send(rootId, "delegate", "start_immediately");
      const parentId = yield* delegate(rootId, "nested parent");
      const childId = yield* delegate(parentId, "nested result");
      const childRun = (yield* orchestrator.getThreadProjection(childId)).runs[0]!;
      yield* completeRunWithoutProvider(childId, childRun.id, "review-offer-child-end");
      const original = projections.getThreadRecords;
      let pause = true;
      // Hold the existing records boundary after offer eligibility has been read.
      const delayedRead = vi
        .spyOn(projections, "getThreadRecords")
        .mockImplementation((threadId, fields, filter) => {
          const read = original(threadId, fields, filter);
          return threadId === parentId &&
            pause &&
            fields.length === 4 &&
            fields.includes("providerTurns")
            ? Effect.gen(function* () {
                pause = false;
                yield* Deferred.succeed(reached, undefined);
                yield* Deferred.await(release);
                return yield* read;
              })
            : read;
        });
      const finalization = yield* orchestrator
        .recoverDelegatedTask(childId, childRun.id)
        .pipe(Effect.forkChild);
      yield* Deferred.await(reached);
      yield* orchestrator.dispatch({
        type: "thread.stop",
        threadId: rootId,
        commandId: CommandId.make("review-offer-root-stop"),
      });
      assert.isTrue(yield* isThreadRetired(parentId));
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(finalization);
      delayedRead.mockRestore();
      assert.equal(yield* Ref.get(offers), 0);
    }).pipe(
      Effect.provide(layerTest),
      Effect.provideService(ProviderContinuationRequests.ProviderContinuationRequests, {
        offer: () => Ref.update(offers, (n) => n + 1),
        take: Effect.never,
      }),
    );
  }).pipe(Effect.scoped),
);

it.effect("a queued completion survives startup held until queue.resume", () =>
  Effect.gen(function* () {
    const offers =
      yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const parentId = ThreadId.make("review-consumed-finalization");
      yield* createWatchingThread(parentId, 106);
      yield* send(parentId, "delegate", "start_immediately");
      const parentRun = (yield* orchestrator.getThreadProjection(parentId)).runs[0]!;
      const childId = yield* delegate(parentId, "late finalization child");
      const childRun = (yield* orchestrator.getThreadProjection(childId)).runs[0]!;
      const siblingId = yield* delegate(parentId, "parked sibling");
      const siblingRun = (yield* orchestrator.getThreadProjection(siblingId)).runs[0]!;
      yield* completeRunWithoutProvider(parentId, parentRun.id, "late-parent-finished");
      yield* completeRunWithoutProvider(childId, childRun.id, "late-child-finished");
      yield* orchestrator.recoverDelegatedTask(childId, childRun.id);
      const first = yield* Queue.take(offers);
      yield* first.dispatchIfCurrent!(consumeCompletion(first));
      const sink = yield* EventSink.EventSinkV2;
      const active = yield* orchestrator.getThreadProjection(parentId);
      const wakeRun = active.runs.find(
        (run) => run.userMessageId === first.delegatedCompletion!.messageId,
      )!;
      const attempt = active.attempts.find((attempt) => attempt.id === wakeRun.activeAttemptId)!;
      const now = yield* DateTime.now;
      const turnId = ProviderTurnId.make("review-accepted-wake");
      // External provider acceptance is represented by its normal persisted records.
      yield* sink.write({
        commandId: CommandId.make("command:runtime-reconcile:review-provider-start"),
        events: [
          {
            id: EventId.make("review-provider-start-run"),
            type: "run.updated",
            threadId: parentId,
            runId: wakeRun.id,
            occurredAt: now,
            payload: { ...wakeRun, status: "running", startedAt: now },
          },
          {
            id: EventId.make("review-provider-start-attempt"),
            type: "run-attempt.updated",
            threadId: parentId,
            runId: wakeRun.id,
            occurredAt: now,
            payload: { ...attempt, status: "running", providerTurnId: turnId, startedAt: now },
          },
          {
            id: EventId.make("review-provider-start-turn"),
            type: "provider-turn.updated",
            threadId: parentId,
            runId: wakeRun.id,
            occurredAt: now,
            payload: {
              id: turnId,
              providerThreadId: wakeRun.providerThreadId!,
              nodeId: wakeRun.rootNodeId!,
              runAttemptId: attempt.id,
              nativeTurnRef: null,
              ordinal: 1,
              status: "running",
              startedAt: now,
              completedAt: null,
            },
          },
        ],
      });
      yield* completeRunWithoutProvider(siblingId, siblingRun.id, "review-sibling-finished");
      yield* orchestrator.recoverDelegatedTask(siblingId, siblingRun.id);
      assert.equal(
        (yield* orchestrator.getThreadProjection(parentId)).subagents.find(
          (task) => task.childThreadId === siblingId,
        )?.completionDelivery?.state,
        "pending",
      );

      yield* send(parentId, "held instructions", "queue_after_active");
      // Runtime reconciliation's existing event marker defers terminal-listener
      // settlement so explicit resume wins before the public recovery pass.
      yield* orchestrator.dispatch({
        type: "thread.stop",
        threadId: parentId,
        commandId: CommandId.make("command:runtime-reconcile:stop-before-late-finalization"),
      });

      // Provider interruption arrives after Stop; defer only terminal-listener settlement.
      const stoppedWake = (yield* orchestrator.getThreadProjection(parentId)).runs.find(
        (run) => run.id === wakeRun.id,
      )!;
      yield* sink.write({
        commandId: CommandId.make("command:runtime-reconcile:review-provider-ended"),
        events: [
          {
            id: EventId.make("review-provider-ended"),
            type: "run.updated",
            threadId: parentId,
            runId: wakeRun.id,
            occurredAt: now,
            payload: { ...stoppedWake, status: "interrupted", completedAt: now },
          },
        ],
      });
      yield* send(parentId, "resume unread result", "start_immediately");
      const beforeFinalization = yield* orchestrator.getThreadProjection(parentId);
      const delivery = beforeFinalization.runs.find((run) => run.id === parentRun.id)!
        .delegatedCompletion!.delivery!;
      assert.equal(delivery.messageId, first.delegatedCompletion!.messageId);
      yield* orchestrator.recoverDelegatedTasks;
      const successor = yield* Queue.take(offers);
      assert.notEqual(
        successor.delegatedCompletion!.messageId,
        first.delegatedCompletion!.messageId,
      );
      yield* successor.dispatchIfCurrent!(consumeCompletion(successor));
      const resumed = yield* orchestrator.getThreadProjection(parentId);
      const resumeRun = resumed.runs.find(
        (run) => run.userMessageId === MessageId.make(`message:${parentId}:resume unread result`),
      )!;
      yield* completeRunWithoutProvider(parentId, resumeRun.id, "late-resume-finished");
      const recovery = yield* ProviderRuntimeRecovery.make.pipe(Effect.provide(IdAllocator.layer));
      yield* runOrderedV2StartupPhases({
        importLegacyShells: Effect.void,
        recover: recovery.recover,
        recoverDelegatedTasks: orchestrator.recoverDelegatedTasks,
        startEffectWorker: Effect.void,
        autoBootstrap: Effect.void,
      });
      const scheduled = yield* orchestrator.getThreadProjection(parentId);
      const wake = scheduled.runs.find(
        (run) => run.userMessageId === successor.delegatedCompletion!.messageId,
      )!;
      const sql = yield* SqlClient.SqlClient;
      const startEffects =
        yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE effect_type = 'provider-turn.start' AND json_extract(payload_json, '$.runId') = ${wake.id} AND status IN ('pending', 'running')`;
      assert.lengthOf(startEffects, 0);
      assert.isTrue(wake.queueHeld);
      yield* resumeHeldCompletion(parentId, successor.delegatedCompletion!.messageId);
      const released = yield* orchestrator.getThreadProjection(parentId);
      assert.equal(
        released.runs.find((run) => run.userMessageId === successor.delegatedCompletion!.messageId)
          ?.status,
        "starting",
      );
      assert.isFalse(
        released.runs.find(
          (run) => run.userMessageId === MessageId.make(`message:${parentId}:held instructions`),
        )?.queueHeld,
      );
      assert.lengthOf(
        scheduled.runs.filter(
          (run) => run.userMessageId === successor.delegatedCompletion!.messageId,
        ),
        1,
      );
    }).pipe(
      Effect.provide(layerTest),
      Effect.provideService(ProviderContinuationRequests.ProviderContinuationRequests, {
        offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
        take: Effect.never,
      }),
    );
  }),
);

it.effect("acknowledging one queued completion preserves another result", () =>
  Effect.gen(function* () {
    const offers =
      yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const parentId = ThreadId.make("review-consumed-finalization");
      yield* createWatchingThread(parentId, 106);
      yield* send(parentId, "delegate", "start_immediately");
      const parentRun = (yield* orchestrator.getThreadProjection(parentId)).runs[0]!;
      const childId = yield* delegate(parentId, "late finalization child");
      const childRun = (yield* orchestrator.getThreadProjection(childId)).runs[0]!;
      const siblingId = yield* delegate(parentId, "parked sibling");
      const siblingRun = (yield* orchestrator.getThreadProjection(siblingId)).runs[0]!;
      const laterId = yield* delegate(parentId, "review later result");
      const laterRun = (yield* orchestrator.getThreadProjection(laterId)).runs[0]!;
      yield* completeRunWithoutProvider(parentId, parentRun.id, "late-parent-finished");
      yield* completeRunWithoutProvider(childId, childRun.id, "late-child-finished");
      yield* orchestrator.recoverDelegatedTask(childId, childRun.id);
      const first = yield* Queue.take(offers);
      yield* first.dispatchIfCurrent!(consumeCompletion(first));
      const sink = yield* EventSink.EventSinkV2;
      const active = yield* orchestrator.getThreadProjection(parentId);
      const wakeRun = active.runs.find(
        (run) => run.userMessageId === first.delegatedCompletion!.messageId,
      )!;
      const attempt = active.attempts.find((attempt) => attempt.id === wakeRun.activeAttemptId)!;
      const now = yield* DateTime.now;
      const turnId = ProviderTurnId.make("review-accepted-wake");
      // External provider acceptance is represented by its normal persisted records.
      yield* sink.write({
        commandId: CommandId.make("command:runtime-reconcile:review-provider-start"),
        events: [
          {
            id: EventId.make("review-provider-start-run"),
            type: "run.updated",
            threadId: parentId,
            runId: wakeRun.id,
            occurredAt: now,
            payload: { ...wakeRun, status: "running", startedAt: now },
          },
          {
            id: EventId.make("review-provider-start-attempt"),
            type: "run-attempt.updated",
            threadId: parentId,
            runId: wakeRun.id,
            occurredAt: now,
            payload: { ...attempt, status: "running", providerTurnId: turnId, startedAt: now },
          },
          {
            id: EventId.make("review-provider-start-turn"),
            type: "provider-turn.updated",
            threadId: parentId,
            runId: wakeRun.id,
            occurredAt: now,
            payload: {
              id: turnId,
              providerThreadId: wakeRun.providerThreadId!,
              nodeId: wakeRun.rootNodeId!,
              runAttemptId: attempt.id,
              nativeTurnRef: null,
              ordinal: 1,
              status: "running",
              startedAt: now,
              completedAt: null,
            },
          },
        ],
      });
      yield* completeRunWithoutProvider(siblingId, siblingRun.id, "review-sibling-finished");
      yield* orchestrator.recoverDelegatedTask(siblingId, siblingRun.id);
      assert.equal(
        (yield* orchestrator.getThreadProjection(parentId)).subagents.find(
          (task) => task.childThreadId === siblingId,
        )?.completionDelivery?.state,
        "pending",
      );

      yield* send(parentId, "held instructions", "queue_after_active");
      // Runtime reconciliation's existing event marker defers terminal-listener
      // settlement so explicit resume wins before the public recovery pass.
      yield* orchestrator.dispatch({
        type: "thread.stop",
        threadId: parentId,
        commandId: CommandId.make("command:runtime-reconcile:stop-before-late-finalization"),
      });

      // Provider interruption arrives after Stop; defer only terminal-listener settlement.
      const stoppedWake = (yield* orchestrator.getThreadProjection(parentId)).runs.find(
        (run) => run.id === wakeRun.id,
      )!;
      yield* sink.write({
        commandId: CommandId.make("command:runtime-reconcile:review-provider-ended"),
        events: [
          {
            id: EventId.make("review-provider-ended"),
            type: "run.updated",
            threadId: parentId,
            runId: wakeRun.id,
            occurredAt: now,
            payload: { ...stoppedWake, status: "interrupted", completedAt: now },
          },
        ],
      });
      yield* send(parentId, "resume unread result", "start_immediately");
      const beforeFinalization = yield* orchestrator.getThreadProjection(parentId);
      const delivery = beforeFinalization.runs.find((run) => run.id === parentRun.id)!
        .delegatedCompletion!.delivery!;
      assert.equal(delivery.messageId, first.delegatedCompletion!.messageId);
      yield* orchestrator.recoverDelegatedTasks;
      const successor = yield* Queue.take(offers);
      yield* completeRunWithoutProvider(laterId, laterRun.id, "review-later-finished");
      yield* orchestrator.recoverDelegatedTask(laterId, laterRun.id);
      assert.notEqual(
        successor.delegatedCompletion!.messageId,
        first.delegatedCompletion!.messageId,
      );
      yield* successor.dispatchIfCurrent!(consumeCompletion(successor));
      const beforeAck = yield* orchestrator.getThreadProjection(parentId);
      const siblingTask = beforeAck.subagents.find((task) => task.childThreadId === siblingId)!;
      const laterTask = beforeAck.subagents.find((task) => task.childThreadId === laterId)!;
      assert.equal(laterTask.completionDelivery?.state, "claimed");
      yield* orchestrator.dispatch({
        type: "delegated_task.completion-delivery.acknowledge",
        commandId: CommandId.make("review-ack-resumed-batch"),
        parentThreadId: parentId,
        taskId: siblingTask.id,
        observedByRunId: null,
      });
      yield* orchestrator.recoverDelegatedTasks;
      const afterAck = yield* orchestrator.getThreadProjection(parentId);
      assert.include(
        afterAck.runs.find((run) => run.id === parentRun.id)!.delegatedCompletion!.delivery
          ?.taskIds ?? [],
        laterTask.id,
      );
    }).pipe(
      Effect.provide(layerTest),
      Effect.provideService(ProviderContinuationRequests.ProviderContinuationRequests, {
        offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
        take: Effect.never,
      }),
    );
  }),
);

it.effect(
  "acknowledging a cancelled queued wake preserves another pending result while Stopped",
  () =>
    Effect.gen(function* () {
      const offers =
        yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const parentId = ThreadId.make("ack-cancelled-mailbox");
        yield* createWatchingThread(parentId, 111);
        yield* send(parentId, "delegate", "start_immediately");
        const parentRun = (yield* orchestrator.getThreadProjection(parentId)).runs[0]!;
        const firstChild = yield* delegate(parentId, "first queued result");
        const otherChild = yield* delegate(parentId, "other pending result");
        const firstRun = (yield* orchestrator.getThreadProjection(firstChild)).runs[0]!;
        const otherRun = (yield* orchestrator.getThreadProjection(otherChild)).runs[0]!;
        yield* completeRunWithoutProvider(firstChild, firstRun.id, "ack-first-child-finished");
        yield* orchestrator.recoverDelegatedTask(firstChild, firstRun.id);
        const first = yield* Queue.take(offers);
        yield* first.dispatchIfCurrent!(consumeCompletion(first));
        yield* orchestrator.dispatch({
          type: "thread.stop",
          threadId: parentId,
          commandId: CommandId.make("command:runtime-reconcile:stop-queued-ack"),
        });
        yield* completeRunWithoutProvider(otherChild, otherRun.id, "ack-other-child-finished");
        yield* orchestrator.recoverDelegatedTask(otherChild, otherRun.id);
        const beforeAck = yield* orchestrator.getThreadProjection(parentId);
        const firstTask = beforeAck.subagents.find((task) => task.childThreadId === firstChild)!;
        const otherTask = beforeAck.subagents.find((task) => task.childThreadId === otherChild)!;
        assert.equal(otherTask.completionDelivery?.state, "pending");
        yield* orchestrator.dispatch({
          type: "delegated_task.completion-delivery.acknowledge",
          parentThreadId: parentId,
          commandId: CommandId.make("ack-cancelled-first"),
          taskId: firstTask.id,
          observedByRunId: null,
        });
        const stopped = yield* orchestrator.getThreadProjection(parentId);
        assert.deepEqual(
          stopped.runs.find((run) => run.id === parentRun.id)!.delegatedCompletion!.delivery!
            .taskIds,
          [otherTask.id],
        );
        yield* send(parentId, "resume other result", "start_immediately");
        const resumedOffer = yield* Queue.take(offers);
        assert.equal(
          resumedOffer.delegatedCompletion!.messageId,
          first.delegatedCompletion!.messageId,
        );
        yield* resumedOffer.dispatchIfCurrent!(consumeCompletion(resumedOffer));
        const resumed = yield* orchestrator.getThreadProjection(parentId);
        const resumeRun = resumed.runs.find(
          (run) => run.userMessageId === MessageId.make(`message:${parentId}:resume other result`),
        )!;
        yield* completeRunWithoutProvider(parentId, resumeRun.id, "ack-resume-finished");
        yield* orchestrator.resumeQueuedRuns;
        const scheduled = yield* orchestrator.getThreadProjection(parentId);
        assert.lengthOf(
          scheduled.runs.filter(
            (run) =>
              run.userMessageId === first.delegatedCompletion!.messageId &&
              run.status === "starting",
          ),
          1,
        );
        assert.equal(
          scheduled.subagents.find((task) => task.id === firstTask.id)!.completionDelivery?.state,
          "acknowledged",
        );
      }).pipe(
        Effect.provide(layerTest),
        Effect.provideService(ProviderContinuationRequests.ProviderContinuationRequests, {
          offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
          take: Effect.never,
        }),
      );
    }),
);

it.effect.each(["live", "startup", "preparing-restart"] as const)(
  "completion obeys workspace and restart queue holds (%s)",
  (phase) =>
    Effect.gen(function* () {
      const offers =
        yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const parentId = ThreadId.make("review-worktree-guard");
        yield* createWatchingThread(parentId, 112);
        yield* send(parentId, "delegate before new workspace", "start_immediately");
        const parentRun = (yield* orchestrator.getThreadProjection(parentId)).runs[0]!;
        const childId = yield* delegate(parentId, "child finishes after workspace setup fails");
        const childRun = (yield* orchestrator.getThreadProjection(childId)).runs[0]!;
        const siblingId = yield* delegate(parentId, "sibling reports later");
        const siblingRun = (yield* orchestrator.getThreadProjection(siblingId)).runs[0]!;
        yield* completeRunWithoutProvider(parentId, parentRun.id, "review-prior-turn-finished");
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("review-prepare-new-worktree"),
          threadId: parentId,
          messageId: MessageId.make("review-prepare-new-worktree"),
          text: "work in a new worktree",
          attachments: [],
          createdBy: "user",
          creationSource: "web",
          dispatchMode: {
            type: "defer_start",
            workspaceStrategy: { type: "worktree", baseRef: "HEAD", startFromOrigin: false },
          },
        });
        const prepared = (yield* orchestrator.getThreadProjection(parentId)).runs.at(-1)!;
        assert.equal(prepared.status, "preparing");
        // Workspace preparation reports its ordinary terminal failure before producing a checkout.
        const sink = yield* EventSink.EventSinkV2;
        const now = yield* DateTime.now;
        if (phase !== "preparing-restart")
          yield* sink.write({
            commandId: CommandId.make("command:runtime-reconcile:review-worktree-setup-failure"),
            events: [
              {
                id: EventId.make("review-worktree-setup-failed"),
                type: "run.updated",
                threadId: parentId,
                runId: prepared.id,
                occurredAt: now,
                payload: { ...prepared, status: "failed", completedAt: now },
              },
            ],
          });
        yield* send(parentId, "ordinary queued message", "queue_after_active");
        yield* completeRunWithoutProvider(childId, childRun.id, "review-worktree-child-complete");
        yield* orchestrator.recoverDelegatedTask(childId, childRun.id);
        const request = yield* Queue.take(offers);
        yield* request.dispatchIfCurrent!(consumeCompletion(request));
        const queued = yield* orchestrator.getThreadProjection(parentId);
        assert.isNull(queued.thread.worktreePath);
        assert.equal(
          queued.runs.find((run) => run.userMessageId === request.delegatedCompletion!.messageId)
            ?.status,
          "queued",
        );
        yield* completeRunWithoutProvider(
          siblingId,
          siblingRun.id,
          "review-worktree-sibling-complete",
        );
        yield* orchestrator.recoverDelegatedTask(siblingId, siblingRun.id);
        if (phase !== "live") {
          const recovery = yield* ProviderRuntimeRecovery.make.pipe(
            Effect.provide(IdAllocator.layer),
          );
          yield* runOrderedV2StartupPhases({
            importLegacyShells: Effect.void,
            recover: recovery.recover,
            recoverDelegatedTasks: orchestrator.recoverDelegatedTasks,
            startEffectWorker: Effect.void,
            autoBootstrap: Effect.void,
          });
        }
        const scanned = yield* orchestrator.getThreadProjection(parentId);
        const heldWake = scanned.runs.find(
          (run) => run.userMessageId === request.delegatedCompletion!.messageId,
        )!;
        assert.isTrue(heldWake.queueHeld);
        const sql = yield* SqlClient.SqlClient;
        const starts =
          yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE effect_type = 'provider-turn.start' AND json_extract(payload_json, '$.runId') = ${heldWake.id} AND status IN ('pending', 'running')`;
        assert.lengthOf(starts, 0);
        assert.equal(
          scanned.runs.find((run) => run.userMessageId === request.delegatedCompletion!.messageId)
            ?.status,
          "queued",
        );
      }).pipe(
        Effect.provide(layerTest),
        Effect.provideService(ProviderContinuationRequests.ProviderContinuationRequests, {
          offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
          take: Effect.never,
        }),
      );
    }),
);

it.effect("review r4: provider-failure queue hold is inherited by a later completion", () =>
  Effect.gen(function* () {
    const offers =
      yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sink = yield* EventSink.EventSinkV2;
      const parentId = ThreadId.make("r4-operational-inheritance");
      yield* createWatchingThread(parentId, 115);
      yield* send(parentId, "delegate work", "start_immediately");
      const activeRun = (yield* orchestrator.getThreadProjection(parentId)).runs[0]!;
      const childId = yield* delegate(parentId, "result after provider-failure hold");
      const childRun = (yield* orchestrator.getThreadProjection(childId)).runs[0]!;
      yield* send(parentId, "queued user work", "queue_after_active");
      const queuedUser = (yield* orchestrator.getThreadProjection(parentId)).runs.at(-1)!;
      const held = yield* Deferred.make<void>();
      const seq = yield* orchestrator.getThreadEventSequence(parentId);
      yield* sink.stream({ threadId: parentId, afterSequence: seq }).pipe(
        Stream.runForEach((stored) =>
          stored.event.type === "run.updated" &&
          stored.event.payload.id === queuedUser.id &&
          stored.event.payload.queueHeld === true
            ? Deferred.succeed(held, undefined)
            : Effect.void,
        ),
        Effect.forkScoped,
      );
      yield* Effect.yieldNow;
      const now = yield* DateTime.now;
      yield* sink.write({
        commandId: CommandId.make("r4-real-provider-failure"),
        events: [
          {
            id: EventId.make("r4-provider-error"),
            type: "turn-item.updated",
            threadId: parentId,
            runId: activeRun.id,
            nodeId: activeRun.rootNodeId!,
            providerInstanceId: activeRun.providerInstanceId,
            occurredAt: now,
            payload: {
              id: TurnItemId.make("r4-provider-error"),
              type: "error",
              threadId: parentId,
              runId: activeRun.id,
              nodeId: activeRun.rootNodeId!,
              providerThreadId: activeRun.providerThreadId,
              providerTurnId: null,
              nativeItemRef: null,
              parentItemId: null,
              ordinal: 2,
              status: "failed",
              title: "Provider failure",
              startedAt: now,
              completedAt: now,
              updatedAt: now,
              failure: {
                class: "provider_error",
                message: "Provider failed.",
                code: "provider_failed",
                retryable: null,
              },
            },
          },
          {
            id: EventId.make("r4-run-failed"),
            type: "run.updated",
            threadId: parentId,
            runId: activeRun.id,
            nodeId: activeRun.rootNodeId!,
            providerInstanceId: activeRun.providerInstanceId,
            occurredAt: now,
            payload: { ...activeRun, status: "failed", completedAt: now },
          },
        ],
      });
      yield* Deferred.await(held);
      assert.isTrue(
        (yield* orchestrator.getThreadProjection(parentId)).runs.find((r) => r.id === queuedUser.id)
          ?.queueHeld,
      );
      // A new explicit turn does not resume the held queue.
      yield* send(parentId, "separate user turn", "start_immediately");
      const busy = (yield* orchestrator.getThreadProjection(parentId)).runs.at(-1)!;
      assert.equal(busy.status, "starting");
      yield* completeRunWithoutProvider(childId, childRun.id, "r4-child-completed");
      yield* orchestrator.recoverDelegatedTask(childId, childRun.id);
      const offer = yield* Queue.take(offers);
      yield* offer.dispatchIfCurrent!(consumeCompletion(offer));
      const before = yield* orchestrator.getThreadProjection(parentId);
      const wake = before.runs.find(
        (r) => r.userMessageId === offer.delegatedCompletion!.messageId,
      )!;
      assert.equal(wake.status, "queued");
      yield* completeRunWithoutProvider(parentId, busy.id, "r4-separate-turn-completed");
      const recovery = yield* ProviderRuntimeRecovery.make.pipe(Effect.provide(IdAllocator.layer));
      yield* runOrderedV2StartupPhases({
        importLegacyShells: Effect.void,
        recover: recovery.recover,
        recoverDelegatedTasks: orchestrator.recoverDelegatedTasks,
        startEffectWorker: Effect.void,
        autoBootstrap: Effect.void,
      });
      const after = yield* orchestrator.getThreadProjection(parentId);
      const afterWake = after.runs.find((r) => r.id === wake.id)!;
      const sql = yield* SqlClient.SqlClient;
      const starts =
        yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE effect_type = 'provider-turn.start' AND json_extract(payload_json, '$.runId') = ${wake.id} AND status IN ('pending', 'running')`;
      assert.equal(afterWake.status, "queued");
      assert.lengthOf(starts, 0);
    }).pipe(
      Effect.provide(layerTest),
      Effect.provideService(ProviderContinuationRequests.ProviderContinuationRequests, {
        offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
        take: Effect.never,
      }),
    );
  }),
);

it.effect("queue resume refused by Stop remains retryable with the same command id", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const sql = yield* SqlClient.SqlClient;
    const threadId = ThreadId.make("stop-retry-queue");
    yield* createWatchingThread(threadId, 501);
    yield* send(threadId, "first", "start_immediately");
    yield* send(threadId, "held", "queue_after_active");
    yield* orchestrator.dispatch({
      type: "thread.stop",
      commandId: CommandId.make("stop-retry-queue:stop"),
      threadId,
    });
    const command = {
      type: "queue.resume" as const,
      commandId: CommandId.make("stop-retry-queue:resume"),
      threadId,
    };
    yield* orchestrator.dispatch(command).pipe(Effect.flip);
    assert.deepEqual(
      yield* sql`SELECT status FROM orchestration_command_receipts WHERE command_id = ${command.commandId}`,
      [],
    );
    yield* send(threadId, "explicit resume", "start_immediately");
    yield* orchestrator.dispatch(command);
    const queued = (yield* orchestrator.getThreadProjection(threadId)).runs.filter(
      (run) => run.status === "queued",
    );
    assert.deepEqual(
      queued.map((run) => run.queueHeld),
      [false],
    );
    assert.deepEqual(
      yield* sql`SELECT status FROM orchestration_command_receipts WHERE command_id = ${command.commandId}`,
      [{ status: "accepted" }],
    );
  }).pipe(Effect.provide(layerTest)),
);
