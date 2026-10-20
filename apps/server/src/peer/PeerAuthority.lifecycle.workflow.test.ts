import type * as Adapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as EffectOutbox from "../orchestration-v2/EffectOutbox.ts";
vi.mock("effect/Queue", { spy: true });

import * as AuthSessions from "../persistence/AuthSessions.ts";
import { AuthSessionId } from "@t3tools/contracts";
import * as AttachmentStaging from "../assets/AttachmentStaging.ts";
import * as AttachmentClaims from "../orchestration-v2/AttachmentClaims.ts";
import * as Invocation from "../mcp/McpInvocationContext.ts";
import { PeerAttachmentUploadResult } from "@t3tools/contracts";
import * as Queue from "effect/Queue";
import { dual } from "effect/Function";
import * as ScheduleMutation from "../scheduledTasks/mutationReceipts.ts";
import * as AttachmentUpload from "../assets/AttachmentUpload.ts";
import { resolveAttachmentPathById } from "../attachmentStore.ts";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as PeerTaskLifecycle from "./PeerTaskLifecycle.ts";
import { RecoveryWaitState } from "./recoveryWait.ts";
import * as RecoveryHistory from "../prism/RecoveryHistory.ts";
import * as RecoverySignals from "./recoveryWaitSignals.ts";
import { PeerDispatchAuthority } from "./peerEffectAuthority.ts";
import { vi } from "vite-plus/test";
import * as Deferred from "effect/Deferred";
import * as Ref from "effect/Ref";
import * as EffectWorker from "../orchestration-v2/EffectWorker.ts";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as HttpBody from "effect/http/HttpBody";
import * as Schema from "effect/Schema";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  EventId,
  MessageId,
  NodeId,
  ThreadId,
  PeerTaskObservation,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as SqlClient from "effect/sql/SqlClient";
import * as PeerAuthority from "./PeerAuthority.ts";
import * as PeerMcpClient from "./PeerMcpClient.ts";
import * as RemoteDelegation from "./RemoteDelegation.ts";
import { descriptorOf, linkTo } from "./PeerLinks.testkit.ts";
import * as Config from "../config.ts";
import * as Secrets from "../auth/ServerSecretStore.ts";
import * as Runtime from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as Adapters from "../orchestration-v2/ProviderAdapterRegistry.ts";
import { ProjectToolkit } from "../mcp/toolkits/project/tools.ts";
import { OrchestratorToolkit } from "../mcp/toolkits/orchestrator/tools.ts";
import {
  activate,
  automationAdapter,
  automationReply,
  automationWorkspace,
  awaitAutomationRunning,
  finishAutomationRun,
  adapter,
  attachedChild,
  caller,
  decodeAutomationScheduleReceipt,
  decodeDiscardUpload,
  decodeSavedPngReceipt,
  decodeTaskReceipt,
  delegate,
  drainStops,
  finishLifecycleRun,
  instanceId,
  json,
  makeNode,
  nativeCall,
  seedParent,
  selection,
  type Node,
} from "./PeerAuthority.workflow.fixture.ts";

const decodePeerTaskObservation = Schema.decodeUnknownEffect(PeerTaskObservation);

it.live(
  "lifecycle381: registered original plus two continuations retain exact results and replay after source restart",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* makeNode(descriptorOf("lifecycle-b", "B", { peerAuthority: 1 }), "disk");
        const a = yield* makeNode(descriptorOf("lifecycle-a", "A", { peerAuthority: 1 }), "disk");
        yield* linkTo(a.links, b, "full-access");
        const parentId = ThreadId.make("lifecycle-parent");
        yield* seedParent(a, parentId);
        const reference = (number: number) => ({
          host: "github.com",
          repository: "toolboxmd/chromeria",
          kind: "issue" as const,
          number,
        });
        const receipt = yield* nativeCall(a, parentId, "delegate_task", {
          purposeIntent: { version: 1, intent: { type: "reference", reference: reference(381) } },
          task: "First result and two follow-ups",
          target: {
            environmentId: b.environmentId,
            projectId: b.project.id,
            providerInstanceId: instanceId,
            model: selection.model,
          },
          clientRequestId: "lifecycle381",
        });
        expect(receipt.isError, json(receipt.content)).not.toBe(true);
        const { taskId } = yield* decodeTaskReceipt(receipt.structuredContent);
        const childId = yield* attachedChild(a, parentId, taskId);
        const negotiatedLaunch = (yield* a.threads.getThreadRecords(parentId, [
          "subagents",
        ])).subagents.find((task) => task.id === taskId)!.remoteChild!.launch!;
        expect(negotiatedLaunch.lifecycleVersion).toBe(1);

        const terminal = (label: string, status: "completed" | "failed" = "completed") =>
          finishLifecycleRun(b, childId, label, status);
        const observed = (label: string) =>
          a.orchestrator
            .streamStoredEventsFrom({
              threadId: parentId,
              afterSequence: 0,
              eventType: "subagent.updated",
            })
            .pipe(
              Stream.filter(
                (stored) =>
                  stored.event.type === "subagent.updated" &&
                  stored.event.payload.id === taskId &&
                  stored.event.payload.remoteChild?.latestResult?.text === label,
              ),
              Stream.runHead,
              Effect.flatMap(Effect.fromOption),
            );
        const original = yield* terminal("original381");
        yield* a.orchestrator
          .streamStoredEventsFrom({
            threadId: parentId,
            afterSequence: 0,
            eventType: "subagent.updated",
          })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "subagent.updated" &&
                stored.event.payload.id === taskId &&
                stored.event.payload.result === "original381",
            ),
            Stream.runHead,
            Effect.flatMap(Effect.fromOption),
          );
        const completion = (runId: string, number: number) =>
          Effect.gen(function* () {
            const records = yield* a.threads.getThreadRecords(parentId, ["subagents", "runs"]);
            const task = records.subagents.find((task) => task.id === taskId)!;
            expect(task.completionSource?.runId).toBe(runId);
            expect(task.completionSource?.purpose.scope).toEqual({
              type: "single",
              reference: reference(number),
            });
            const delivery = records.runs
              .flatMap((run) => run.delegatedCompletion?.delivery ?? [])
              .find((delivery) => delivery.taskIds.includes(taskId));
            expect(delivery?.purposeCauses).toEqual([{ taskId, ...task.completionSource }]);
            return { task, delivery: delivery! };
          });
        const originalCompletion = yield* completion(original, 381);
        const first = yield* nativeCall(a, parentId, "task_status", { taskId });
        expect(first.structuredContent).toMatchObject({
          childThreadId: null,
          summary: "original381",
          latestTerminalRunId: original,
          latestTerminalSummary: "original381",
        });
        const send1 = yield* nativeCall(a, parentId, "t3_thread_send", {
          environmentId: b.environmentId,
          threadId: childId,
          message: "Continue once",
          purposeIntent: { version: 1, intent: { type: "reference", reference: reference(379) } },
          clientRequestId: "continue381-1",
        });
        expect(send1.isError, json(send1.content)).not.toBe(true);
        yield* Ref.set(a.dropAckResponses, 1);
        const second = yield* terminal("second381");
        yield* observed("second381");
        const secondCompletion = yield* completion(second, 379);
        expect(secondCompletion.task.purpose).toEqual(originalCompletion.task.purpose);
        expect(secondCompletion.task.result).toBe("original381");
        expect(secondCompletion.task.remoteChild?.runId).toBe(original);
        expect(secondCompletion.task.remoteChild?.originalResult).toEqual(
          originalCompletion.task.remoteChild?.originalResult,
        );
        expect(secondCompletion.task.completionSource?.purpose.provenance).toEqual({
          type: "delegated_completion",
          operationIds: [`remote:${b.environmentId}:${childId}:${second}`],
        });
        expect(secondCompletion.task.remoteChild?.latestResult?.purposeScope).toEqual({
          type: "single",
          reference: reference(379),
        });
        const secondStatus = yield* nativeCall(a, parentId, "task_status", { taskId });
        expect(secondStatus.structuredContent).toMatchObject({
          summary: "original381",
          latestTerminalRunId: second,
          latestTerminalSummary: "second381",
        });
        const send2 = yield* nativeCall(a, parentId, "t3_thread_send", {
          environmentId: b.environmentId,
          threadId: childId,
          message: "Continue twice",
          purposeIntent: { version: 1, intent: { type: "reference", reference: reference(379) } },
          clientRequestId: "continue381-2",
        });
        expect(send2.isError, json(send2.content)).not.toBe(true);
        yield* Deferred.await(a.ackResponseLost);
        yield* a.stopConnections;
        const third = yield* terminal("third381", "failed");
        const reconnected = yield* a.reconnect();
        const resumed = Context.get(reconnected.services, RemoteDelegation.RemoteDelegation);
        yield* resumed.start();
        yield* observed("third381");
        yield* completion(third, 379);
        const sql = Context.get(a.database, SqlClient.SqlClient);
        const deliveries = yield* sql<{
          message_id: string;
          run_id: string;
        }>`SELECT DISTINCT json_extract(payload_json, '$.delegatedCompletion.delivery.messageId') AS message_id, json_extract(payload_json, '$.delegatedCompletion.delivery.purposeCauses[0].runId') AS run_id FROM orchestration_events WHERE stream_id=${parentId} AND event_type='run.updated' AND json_extract(payload_json, '$.delegatedCompletion.delivery.purposeCauses[0].taskId')=${taskId}`;
        expect(deliveries.filter((row) => row.run_id === second)).toEqual([
          { message_id: secondCompletion.delivery.messageId, run_id: second },
        ]);
        expect(deliveries.filter((row) => row.run_id === original)).toEqual([
          { message_id: originalCompletion.delivery.messageId, run_id: original },
        ]);
        const status = yield* nativeCall(a, parentId, "task_status", { taskId });
        expect(status.structuredContent).toMatchObject({
          childThreadId: null,
          summary: "original381",
          latestTerminalRunId: third,
          latestTerminalSummary: "third381",
          latestTerminalStatus: "failed",
          remoteObservation: { originalRunId: original, availability: "current" },
        });
        yield* b.threads.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("monitor-only381"),
          threadId: childId,
          messageId: MessageId.make("monitor-message381"),
          text: "Monitor bookkeeping",
          attachments: [],
          notification: {
            source: { kind: "monitor" },
            outcome: "updated",
            summary: "Monitor bookkeeping",
          },
          dispatchMode: { type: "queue_after_active" },
          createdBy: "agent",
          creationSource: "server",
        });
        const monitorRun = yield* finishLifecycleRun(b, childId, "monitor381");
        const monitor = (yield* b.threads.getThreadRecords(childId, ["runs"])).runs.find(
          (run) => run.id === monitorRun,
        )!;
        const monitorTime = yield* DateTime.now;
        yield* b.sink.write({
          events: [
            {
              id: EventId.make("monitor-checkpoint-rewrite381"),
              type: "run.updated",
              threadId: childId,
              runId: monitorRun,
              occurredAt: monitorTime,
              payload: { ...monitor, completedAt: monitorTime },
            },
          ],
        });
        const throughMonitor = yield* b.threads.getThreadEventSequence(childId);
        yield* a.orchestrator
          .streamStoredEventsFrom({
            threadId: parentId,
            afterSequence: 0,
            eventType: "subagent.updated",
          })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "subagent.updated" &&
                stored.event.payload.id === taskId &&
                (stored.event.payload.remoteChild?.cursor ?? 0) >= throughMonitor,
            ),
            Stream.runHead,
          );
        const afterMonitor = (yield* a.threads.getThreadRecords(parentId, [
          "subagents",
        ])).subagents.find((task) => task.id === taskId)!;
        expect(afterMonitor.remoteChild?.launch).toEqual(negotiatedLaunch);
        expect(afterMonitor.remoteChild?.latestResult?.runId).toBe(third);
        expect(afterMonitor.remoteChild?.latestResult?.status).toBe("failed");
        expect(afterMonitor.remoteChild?.snapshot?.queuedRuns).toBe(0);
        const view = yield* resumed.view(parentId, taskId, 0);
        expect(view.environmentId).toBe(b.environmentId);
        expect(
          view.messages
            .filter((message) => message.role === "assistant")
            .map((message) => message.text),
        ).toEqual(["original381", "second381", "third381", "monitor381"]);
        expect(yield* a.orchestrator.getThreadShell(childId)).toBe(null);
        const control = reconnected.peers.taskLifecycle!;
        const pair = {
          parentEnvironmentId: a.environmentId,
          parentThreadId: parentId,
          taskId,
          childEnvironmentId: b.environmentId,
          childThreadId: childId,
          childRunId: null,
        };
        const generation = afterMonitor.remoteChild!.launch!.linkIdentity;
        const beforeText = yield* control(b.environmentId, generation, {
          pair,
          operation: { type: "snapshot", after: 0, wait: false },
        });
        if (beforeText.type !== "snapshot") return yield* Effect.die("Expected lifecycle snapshot");
        const originalMessage = (yield* b.threads.getThreadRecords(childId, ["messages"]))
          .messages[0]!;
        const textTime = yield* DateTime.now;
        yield* b.sink.write({
          events: [
            {
              id: EventId.make("text-only381"),
              type: "message.updated",
              threadId: childId,
              occurredAt: textTime,
              payload: {
                ...originalMessage,
                id: MessageId.make("text-only381"),
                runId: null,
                role: "system",
                text: "Stream-only update",
                createdAt: textTime,
                updatedAt: textTime,
              },
            },
          ],
        });
        const afterText = yield* control(b.environmentId, generation, {
          pair,
          operation: {
            type: "observe",
            after: afterMonitor.remoteChild!.cursor!,
            afterSnapshot: beforeText.snapshot.sequence,
            wait: false,
          },
        });
        expect(afterText).toMatchObject({
          type: "observation",
          observation: {
            cursor: afterMonitor.remoteChild!.cursor!,
            snapshot: { sequence: beforeText.snapshot.sequence },
          },
        });
        if (afterText.type !== "observation") return yield* Effect.die("Expected observation");
        expect(afterText.observation.result).toBeUndefined();
        const lateMessage = (yield* b.threads.getThreadRecords(childId, [
          "messages",
        ])).messages.find((message) => message.runId === original && message.role === "assistant")!;
        const lateTime = yield* DateTime.now;
        yield* b.sink.write({
          events: [
            {
              id: EventId.make("late-original-message381"),
              type: "message.updated",
              threadId: childId,
              runId: original,
              occurredAt: lateTime,
              payload: { ...lateMessage, text: "Late changed original text", updatedAt: lateTime },
            },
          ],
        });
        const originalIdentity = afterMonitor.remoteChild!.originalResult!;
        const originalPage = yield* control(b.environmentId, generation, {
          pair,
          operation: {
            type: "resultText",
            runId: originalIdentity.runId,
            eventId: originalIdentity.eventId,
            sequence: originalIdentity.sequence,
            textOffset: 0,
          },
        });
        expect(originalPage).toEqual({
          type: "resultText",
          text: "original381",
          nextTextOffset: null,
        });
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.live(
  "lifecycle381: finished jobs watching there never hold back a live job's Stop",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* makeNode(descriptorOf("quiet-b", "B", { peerAuthority: 1 }));
        const a = yield* makeNode(descriptorOf("quiet-a", "A", { peerAuthority: 1 }));
        yield* linkTo(a.links, b, "full-access");
        const parentId = ThreadId.make("quiet-parent");
        yield* seedParent(a, parentId);
        const delegate = (name: string) =>
          nativeCall(a, parentId, "delegate_task", {
            task: `Job ${name}`,
            target: {
              environmentId: b.environmentId,
              projectId: b.project.id,
              providerInstanceId: instanceId,
              model: selection.model,
            },
            clientRequestId: `quiet-${name}`,
          }).pipe(
            Effect.flatMap((receipt) => decodeTaskReceipt(receipt.structuredContent)),
            Effect.map(({ taskId }) => taskId),
          );
        const recorded = (
          taskId: NodeId,
          predicate: (task: import("@t3tools/contracts").OrchestrationV2Subagent) => boolean,
        ) =>
          a.orchestrator
            .streamStoredEventsFrom({
              threadId: parentId,
              afterSequence: 0,
              eventType: "subagent.updated",
            })
            .pipe(
              Stream.filter(
                (stored) =>
                  stored.event.type === "subagent.updated" &&
                  stored.event.payload.id === taskId &&
                  predicate(stored.event.payload),
              ),
              Stream.runHead,
              Effect.flatMap(Effect.fromOption),
            );
        // More finished jobs than there are peer request permits.
        const finished: Array<NodeId> = [];
        for (let n = 0; n < 9; n++) {
          const taskId = yield* delegate(`finished-${n}`);
          yield* finishLifecycleRun(b, yield* attachedChild(a, parentId, taskId), `finished-${n}`);
          yield* recorded(
            taskId,
            (task) =>
              task.result === `finished-${n}` &&
              task.remoteChild?.snapshot?.workState === "result_available",
          );
          finished.push(taskId);
        }
        const liveId = yield* delegate("live");
        yield* attachedChild(a, parentId, liveId);
        // Each finished job's next wait finds nothing new there and holds on.
        yield* Ref.set(a.quietWaits, new Set(finished));
        yield* a.stopConnections;
        const reconnected = yield* a.reconnect();
        const resumed = Context.get(reconnected.services, RemoteDelegation.RemoteDelegation);
        yield* resumed.start();
        yield* resumed.requestStop(parentId, liveId, "quiet-live-stop", undefined);
        yield* recorded(liveId, (task) => task.remoteChild?.stop?.state === "acknowledged");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.live(
  "lifecycle381: delayed first observation after conversation revert uses the completing purpose",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const started: Array<Adapter.ProviderAdapterV2TurnInput> = [];
        const opened: Array<Adapter.ProviderAdapterV2OpenSessionInput> = [];
        const external = automationAdapter(started, opened, (turn, events) =>
          automationReply(turn, events, `Actual result ${turn.runOrdinal}`),
        );
        const rolledBack: Array<Adapter.ProviderAdapterV2RollbackThreadInput> = [];
        const providerAdapter: Adapter.ProviderAdapterV2["Service"] = {
          ...external,
          openSession: (input) =>
            external.openSession(input).pipe(
              Effect.map((session) => ({
                ...session,
                rollbackThread: (input) =>
                  Effect.sync(() => {
                    rolledBack.push(input);
                    return {
                      providerThread: input.providerThread,
                      providerTurns: [],
                      messages: [],
                      runtimeRequests: [],
                    };
                  }),
              })),
            ),
        };
        const b = yield* makeNode(
          descriptorOf("first-observed-b", "B", { peerAuthority: 1 }),
          "disk",
          {
            productionKernel: true,
            ownerWorkspace: yield* automationWorkspace,
            providerAdapter,
          },
        );
        const a = yield* makeNode(
          descriptorOf("first-observed-a", "A", { peerAuthority: 1 }),
          "disk",
        );
        yield* linkTo(a.links, b, "full-access");
        const parentId = ThreadId.make("first-observed-parent");
        yield* seedParent(a, parentId);
        const scope = (number: number) => ({
          type: "single" as const,
          reference: {
            host: "github.com",
            repository: "toolboxmd/chromeria",
            kind: "issue" as const,
            number,
          },
        });
        const launched = yield* nativeCall(a, parentId, "delegate_task", {
          task: "Observe the actual completion purpose",
          clientRequestId: "first-observed-delegate",
          purposeIntent: {
            version: 1,
            intent: { type: "reference", reference: scope(381).reference },
          },
          target: {
            environmentId: b.environmentId,
            projectId: b.project.id,
            providerInstanceId: instanceId,
            model: selection.model,
          },
        });
        expect(launched.isError, json(launched.content)).not.toBe(true);
        const { taskId } = yield* decodeTaskReceipt(launched.structuredContent);
        const childId = yield* attachedChild(a, parentId, taskId);
        const before = (yield* a.threads.getThreadRecords(parentId, ["subagents"])).subagents.find(
          (task) => task.id === taskId,
        )!;
        expect(before.result).toBeNull();
        expect(before.purpose?.scope).toEqual(scope(381));
        yield* a.stopConnections;
        // Restore native authorization transport without restarting result observation.
        const connection = yield* a.reconnect();
        yield* connection.peers.start;
        yield* connection.peers.requireTaskLifecycle!(b.environmentId);
        const firstRun = (yield* b.threads.getThreadRecords(childId, ["runs"])).runs[0]!;
        yield* awaitAutomationRunning(b, childId);
        yield* finishAutomationRun(b, childId, firstRun.id);
        const first = yield* b.orchestrator.getThreadProjection(childId);
        expect(started.map((turn) => turn.runId)).toContain(firstRun.id);
        expect(first.runs.find((run) => run.id === firstRun.id)?.purpose?.scope).toEqual(
          scope(381),
        );
        const checkpoint = first.checkpoints.find(
          (checkpoint) => checkpoint.appRunOrdinal === null && checkpoint.ordinalWithinScope === 0,
        )!;
        expect(checkpoint.status).toBe("ready");
        const rollbackId = CommandId.make("first-observed-conversation-revert");
        yield* b.threads.dispatch({
          type: "checkpoint.rollback",
          commandId: rollbackId,
          threadId: childId,
          checkpointId: checkpoint.id,
          scopeId: checkpoint.scopeId,
          restoreFiles: false,
        });
        yield* Context.get(b.runtime, EffectWorker.OrchestrationEffectWorkerV2).drain();
        const rollbackEffects = yield* Context.get(
          b.runtime,
          EffectOutbox.EffectOutboxV2,
        ).listByCommandId(rollbackId);
        expect(
          rollbackEffects
            .filter((effect) => effect.request.type === "provider-thread.rollback")
            .map((effect) => effect.status),
        ).toEqual(["succeeded"]);
        expect(rolledBack).toHaveLength(1);
        expect(rolledBack[0]?.target.type).toBe("thread_start");
        expect(
          (yield* b.threads.getThreadRecords(childId, ["runs"])).runs.find(
            (run) => run.id === firstRun.id,
          )?.status,
        ).toBe("rolled_back");
        yield* b.threads.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("first-observed-human-r2"),
          threadId: childId,
          messageId: MessageId.make("first-observed-human-r2-message"),
          text: "New human work for Y after conversation revert",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
          purposeIntent: {
            version: 1,
            intent: { type: "reference", reference: scope(379).reference },
          },
        });
        const secondRun = (yield* b.threads.getThreadRecords(childId, ["runs"])).runs.at(-1)!;
        expect(secondRun.id).not.toBe(firstRun.id);
        yield* awaitAutomationRunning(b, childId);
        yield* finishAutomationRun(b, childId, secondRun.id);
        expect(started.map((turn) => turn.runId)).toEqual([firstRun.id, secondRun.id]);
        expect(
          (yield* b.threads.getThreadRecords(childId, ["runs"])).runs.find(
            (run) => run.id === secondRun.id,
          ),
        ).toMatchObject({ status: "completed", purpose: { scope: scope(379) } });
        const result = yield* connection.peers.taskLifecycle!(
          b.environmentId,
          before.remoteChild!.launch!.linkIdentity,
          {
            pair: {
              parentEnvironmentId: a.environmentId,
              parentThreadId: parentId,
              taskId,
              childEnvironmentId: b.environmentId,
              childThreadId: childId,
              childRunId: null,
            },
            operation: { type: "observe", after: before.remoteChild!.cursor ?? 0, wait: false },
          },
        );
        if (result.type !== "observation" || result.observation.result === undefined)
          return yield* Effect.die("Expected actual B terminal observation after revert");
        expect(result.observation.result).toMatchObject({
          runId: secondRun.id,
          purposeScope: scope(379),
        });
        expect(
          (yield* a.threads.getThreadRecords(parentId, ["subagents"])).subagents.find(
            (task) => task.id === taskId,
          )?.result,
        ).toBeNull();
        yield* a.orchestrator.dispatch({
          type: "delegated_task.remote.complete",
          commandId: CommandId.make("first-observed-complete-r2"),
          parentThreadId: parentId,
          taskId,
          status: result.observation.result.status,
          result: result.observation.result.text,
          observation: result.observation,
        });
        const completed = yield* a.orchestrator.getThreadProjection(parentId);
        const task = completed.subagents.find((task) => task.id === taskId)!;
        expect(task.remoteChild?.latestResult?.runId).toBe(secondRun.id);
        expect(task.purpose).toEqual(before.purpose);
        expect(task.remoteChild?.launch).toEqual(before.remoteChild?.launch);
        expect(task.remoteChild?.originalResult).toEqual({
          runId: secondRun.id,
          eventId: result.observation.result.eventId,
          sequence: result.observation.result.sequence,
          status: "completed",
        });
        expect(task.completionSource?.runId).toBe(secondRun.id);
        expect(task.completionSource?.purpose.scope).toEqual(scope(379));
        const ownerRun = completed.runs.find((run) => run.id === task.runId)!;
        const delivery = ownerRun.delegatedCompletion!.delivery!;
        expect(delivery.purposeCauses).toEqual([
          { taskId, runId: secondRun.id, purpose: task.completionSource!.purpose },
        ]);
        yield* a.threads.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("first-observed-parent-wake"),
          threadId: parentId,
          messageId: delivery.messageId,
          text: "R2 completion wake",
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "agent",
          creationSource: "server",
          delegatedCompletion: {
            parentRunId: ownerRun.id,
            generation: delivery.generation,
            taskIds: [taskId],
          },
        });
        const wake = (yield* a.threads.getThreadRecords(parentId, ["runs"])).runs.find(
          (run) => run.userMessageId === delivery.messageId,
        )!;
        expect(wake.purpose?.scope).toEqual(scope(379));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("lifecycle381: a valid old-peer result has unknown follow-up purpose", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* makeNode(descriptorOf("old-purpose-b", "B", { peerAuthority: 1 }));
      const a = yield* makeNode(descriptorOf("old-purpose-a", "A", { peerAuthority: 1 }));
      yield* linkTo(a.links, b, "full-access");
      const parentId = ThreadId.make("old-purpose-parent");
      yield* seedParent(a, parentId);
      const launched = yield* nativeCall(a, parentId, "delegate_task", {
        task: "Legacy purpose compatibility",
        clientRequestId: "old-purpose-delegate",
        purposeIntent: { version: 1, intent: { type: "unscoped" } },
        target: {
          environmentId: b.environmentId,
          projectId: b.project.id,
          providerInstanceId: instanceId,
          model: selection.model,
        },
      });
      expect(launched.isError, json(launched.content)).not.toBe(true);
      const { taskId } = yield* decodeTaskReceipt(launched.structuredContent);
      const childId = yield* attachedChild(a, parentId, taskId);
      const original = yield* finishLifecycleRun(b, childId, "old-purpose-original");
      yield* a.orchestrator
        .streamStoredEventsFrom({
          threadId: parentId,
          afterSequence: 0,
          eventType: "subagent.updated",
        })
        .pipe(
          Stream.filter(
            (stored) =>
              stored.event.type === "subagent.updated" &&
              stored.event.payload.id === taskId &&
              stored.event.payload.result === "old-purpose-original",
          ),
          Stream.runHead,
          Effect.flatMap(Effect.fromOption),
        );
      yield* nativeCall(a, parentId, "task_status", { taskId });
      const first = (yield* a.threads.getThreadRecords(parentId, ["subagents"])).subagents.find(
        (task) => task.id === taskId,
      )!;
      // Reconstruct only the peer reader, leaving observation admission to the real source
      // command owner. This prevents a current follower racing the old wire shape.
      yield* a.stopConnections;
      yield* b.threads.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("old-purpose-human-followup"),
        threadId: childId,
        messageId: MessageId.make("old-purpose-human-message"),
        text: "Human follow-up for another outcome",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        createdBy: "user",
        creationSource: "web",
        purposeIntent: {
          version: 1,
          intent: {
            type: "reference",
            reference: {
              host: "github.com",
              repository: "toolboxmd/chromeria",
              kind: "issue",
              number: 379,
            },
          },
        },
      });
      const later = yield* finishLifecycleRun(b, childId, "old-purpose-later");
      const connection = yield* a.reconnect();
      const result = yield* connection.peers.taskLifecycle!(
        b.environmentId,
        first.remoteChild!.launch!.linkIdentity,
        {
          pair: {
            parentEnvironmentId: a.environmentId,
            parentThreadId: parentId,
            taskId,
            childEnvironmentId: b.environmentId,
            childThreadId: childId,
            childRunId: null,
          },
          operation: { type: "observe", after: first.remoteChild!.cursor!, wait: false },
        },
      );
      if (result.type !== "observation" || result.observation.result === undefined)
        return yield* Effect.die("Expected a valid destination result");
      expect(result.observation.result.runId).toBe(later);
      expect(result.observation.result.purposeScope).toMatchObject({
        type: "single",
        reference: { number: 379 },
      });
      const { purposeScope: _scope, ...oldResult } = result.observation.result;
      const observation = yield* decodePeerTaskObservation({
        ...result.observation,
        result: oldResult,
      });
      yield* a.orchestrator.dispatch({
        type: "delegated_task.remote.complete",
        commandId: CommandId.make("old-purpose-completion"),
        parentThreadId: parentId,
        taskId,
        status: oldResult.status,
        result: oldResult.text,
        observation,
      });
      const records = yield* a.threads.getThreadRecords(parentId, ["subagents", "runs"]);
      const task = records.subagents.find((task) => task.id === taskId)!;
      expect(task.completionSource?.runId).toBe(later);
      expect(task.completionSource?.purpose.scope).toEqual({
        type: "unscoped",
        reason: "unknown_cause",
      });
      expect(task.purpose).toEqual(first.purpose);
      expect(task.remoteChild?.runId).toBe(original);
      expect(task.remoteChild?.originalResult).toEqual(first.remoteChild?.originalResult);
      const delivery = records.runs
        .flatMap((run) => run.delegatedCompletion?.delivery ?? [])
        .find((delivery) => delivery.taskIds.includes(taskId));
      expect(delivery?.purposeCauses).toEqual([{ taskId, ...task.completionSource }]);
      const status = yield* nativeCall(a, parentId, "task_status", { taskId });
      expect(status.structuredContent).toMatchObject({
        latestTerminalRunId: later,
        summary: "old-purpose-original",
      });
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "lifecycle381: stop keeps the checked generation across relink and retains actual mode policy",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* makeNode(descriptorOf("stop381-b", "B", { peerAuthority: 1 }));
        const a = yield* makeNode(descriptorOf("stop381-a", "A", { peerAuthority: 1 }));
        yield* linkTo(a.links, b, "full-access");
        const source = ThreadId.make("stop381-source");
        const target = ThreadId.make("stop381-existing-target");
        yield* seedParent(a, source);
        yield* seedParent(b, target);
        yield* a.forwarding.readThread(caller(a, source), b.environmentId, {
          threadId: target,
          view: "messages",
        });
        const grant = (yield* b.authority.grants).find(
          (grant) => grant.environmentId === a.environmentId,
        )!;
        yield* b.authority.setGrant({ ...grant, resourceScope: "environment-resources" });
        const checked = yield* a.links.get(b.environmentId).pipe(Effect.flatMap(Effect.fromOption));
        const priorGrants = new Set((yield* b.authority.grants).map((grant) => grant.sessionId));
        // The local caller checked the old link. Replacement happens before forward resolves it.
        yield* a.links.unlink(b.environmentId);
        yield* linkTo(a.links, b, "full-access");
        const replacement = yield* a.links
          .get(b.environmentId)
          .pipe(Effect.flatMap(Effect.fromOption));
        expect(replacement.launchIdentity).not.toBe(checked.launchIdentity);
        yield* a.forwarding.readThread(caller(a, source), b.environmentId, {
          threadId: target,
          view: "messages",
        });
        const nextGrant = (yield* b.authority.grants).find(
          (grant) => !priorGrants.has(grant.sessionId),
        )!;
        yield* b.authority.setGrant({ ...nextGrant, resourceScope: "environment-resources" });
        const before = yield* b.threads.getThreadRecords(target, ["runs"]);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const beforeEffects = yield* sql<{
          effect_id: string;
        }>`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE thread_id = ${target} ORDER BY effect_id`;
        const denied = yield* a.forwarding
          .interruptThread(
            caller(a, source),
            b.environmentId,
            { threadId: target, stop: true, clientRequestId: "generation-bound-stop381" },
            { expectedLinkIdentity: checked.launchIdentity },
          )
          .pipe(Effect.result);
        expect(denied._tag).toBe("Failure");
        const afterDenied = yield* b.threads.getThreadRecords(target, ["runs"]);
        expect(afterDenied.runs).toEqual(before.runs);
        expect(
          yield* sql<{
            effect_id: string;
          }>`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE thread_id = ${target} ORDER BY effect_id`,
        ).toEqual(beforeEffects);
        // Positive control: the same current grant and actual target can stop lawfully.
        yield* a.forwarding.interruptThread(
          caller(a, source),
          b.environmentId,
          { threadId: target, stop: true, clientRequestId: "generation-bound-stop381" },
          { expectedLinkIdentity: replacement.launchIdentity },
        );
        expect(
          (yield* b.threads.getThreadRecords(target, ["runs"])).runs.some(
            (run) => run.status === "running",
          ),
        ).toBe(false);
        const higher = ThreadId.make("stop381-higher-mode-target");
        yield* seedParent(b, higher);
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("stop381-lower-source"),
          threadId: source,
          runtimeMode: "auto",
        });
        const modeDenied = yield* a.forwarding
          .interruptThread(
            caller(a, source),
            b.environmentId,
            { threadId: higher, stop: true, clientRequestId: "mode-stop381" },
            { expectedLinkIdentity: replacement.launchIdentity },
          )
          .pipe(Effect.result);
        expect(modeDenied._tag).toBe("Failure");
        expect(
          (yield* b.threads.getThreadRecords(higher, ["runs"])).runs.some(
            (run) => run.status === "running",
          ),
        ).toBe(true);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.live(
  "lifecycle381: nested and queued work, persisted recovery and destination reader restart retain ordered truth",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* makeNode(
          descriptorOf("lifecycle-state-b", "B", { peerAuthority: 1 }),
          "disk",
        );
        const a = yield* makeNode(
          descriptorOf("lifecycle-state-a", "A", { peerAuthority: 1 }),
          "disk",
        );
        yield* linkTo(a.links, b, "full-access");
        const parentId = ThreadId.make("lifecycle-state-parent");
        yield* seedParent(a, parentId);
        const receipt = yield* nativeCall(a, parentId, "delegate_task", {
          task: "Lifecycle state",
          target: {
            environmentId: b.environmentId,
            projectId: b.project.id,
            providerInstanceId: instanceId,
            model: selection.model,
          },
          clientRequestId: "state381",
        });
        expect(receipt.isError, json(receipt.content)).not.toBe(true);
        const { taskId } = yield* decodeTaskReceipt(receipt.structuredContent);
        const childId = yield* attachedChild(a, parentId, taskId);
        const firstRun = yield* finishLifecycleRun(b, childId, "state-first");
        yield* a.orchestrator
          .streamStoredEventsFrom({
            threadId: parentId,
            afterSequence: 0,
            eventType: "subagent.updated",
          })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "subagent.updated" &&
                stored.event.payload.id === taskId &&
                stored.event.payload.remoteChild?.latestResult?.runId === firstRun,
            ),
            Stream.runHead,
          );
        const original = (yield* a.threads.getThreadRecords(parentId, [
          "subagents",
        ])).subagents.find((task) => task.id === taskId)!;
        yield* nativeCall(a, parentId, "task_status", { taskId });
        yield* nativeCall(a, parentId, "t3_thread_send", {
          environmentId: b.environmentId,
          threadId: childId,
          message: "Continue with nested work",
          clientRequestId: "state-followup",
        });
        const continuation = yield* activate(b, childId);
        const nestedReceipt = yield* nativeCall(b, childId, "delegate_task", {
          task: "Nested work",
          target: { providerInstanceId: instanceId, model: selection.model },
          clientRequestId: "state-nested",
        });
        expect(nestedReceipt.isError, json(nestedReceipt.content)).not.toBe(true);
        const { taskId: nestedTask } = yield* decodeTaskReceipt(nestedReceipt.structuredContent);
        const nested = (yield* b.threads.getThreadRecords(childId, ["subagents"])).subagents.find(
          (task) => task.id === nestedTask,
        )!;
        expect(nested.childThreadId).not.toBe(null);
        yield* activate(b, nested.childThreadId!);
        yield* nativeCall(a, parentId, "t3_thread_send", {
          environmentId: b.environmentId,
          threadId: childId,
          message: "Queued continuation",
          mode: "queue",
          clientRequestId: "state-queued",
        });
        const liveView = yield* a.remote.view(parentId, taskId, 0);
        expect(liveView.snapshot.pendingChildren).toBeGreaterThan(0);
        expect(liveView.snapshot.queuedRuns).toBeGreaterThan(0);
        expect(liveView.snapshot.executionSettled).toBe(false);
        expect(liveView.children).toContainEqual(
          expect.objectContaining({
            taskId: nestedTask,
            environmentId: b.environmentId,
            threadId: nested.childThreadId,
          }),
        );
        const nestedView = yield* a.remote.view(parentId, taskId, 0, undefined, {
          environmentId: b.environmentId,
          threadId: nested.childThreadId!,
        });
        expect(nestedView.threadId).toBe(nested.childThreadId);
        expect(nestedView.messages.some((message) => message.text === "Nested work")).toBe(true);
        const unrelated = ThreadId.make("unrelated-local-resource");
        yield* seedParent(b, unrelated);
        const refusedView = yield* a.remote
          .view(parentId, taskId, 0, undefined, {
            environmentId: b.environmentId,
            threadId: unrelated,
          })
          .pipe(Effect.result);
        expect(refusedView._tag).toBe("Failure");
        expect(yield* a.orchestrator.getThreadShell(nested.childThreadId!)).toBe(null);
        yield* a.stopConnections;
        const reconnected = yield* a.reconnect();
        const control = reconnected.peers.taskLifecycle!;
        const pair = {
          parentEnvironmentId: a.environmentId,
          parentThreadId: parentId,
          taskId,
          childEnvironmentId: b.environmentId,
          childThreadId: childId,
          childRunId: null,
        };
        const generation = original.remoteChild!.launch!.linkIdentity;
        const after = original.remoteChild!.cursor!;
        const sql = Context.get(b.database, SqlClient.SqlClient);
        yield* RecoveryHistory.writeRecoveryOutcome({
          sourceRunId: continuation,
          threadId: childId,
          status: "pending",
          reason: "decision",
        }).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
          Effect.provideService(RecoverySignals.RecoveryWaitSignals, b.signals),
        );
        // External provider completion uses the persisted run selected above, while another run remains queued.
        const records = yield* b.threads.getThreadRecords(childId, ["runs"]);
        const run = records.runs.find((run) => run.id === continuation)!;
        const now = yield* DateTime.now;
        yield* b.sink.write({
          events: [
            {
              id: EventId.make("state-failure"),
              type: "run.updated",
              threadId: childId,
              runId: continuation,
              occurredAt: now,
              payload: { ...run, status: "failed", completedAt: now },
            },
          ],
        });
        const pending = yield* control(b.environmentId, generation, {
          pair,
          operation: { type: "observe", after, wait: false },
        });
        expect(pending).toMatchObject({
          type: "observation",
          observation: { snapshot: { recoveryWaiting: true, executionSettled: false } },
        });
        if (pending.type !== "observation") return yield* Effect.die("Expected observation");
        expect(pending.observation.result).toBeUndefined();
        const originalGrant = (yield* b.authority.grants)[0];
        expect(originalGrant).toBeDefined();
        const beforeSessions = yield* b.linkedSessions;
        expect(
          beforeSessions.some((session) => session.sessionId === originalGrant?.sessionId),
        ).toBe(true);
        // Reconstruct the destination reader on a separately opened connection to the same on-disk database.
        const reopened = yield* b.reopenDatabase;
        const restoredRuntime = yield* Runtime.layerWithRegistry(
          { name: "lifecycle-destination-restored" },
          Adapters.layerFromAdapters([adapter]),
          { databaseLayer: Layer.succeedContext(reopened), runEffectWorker: false },
        ).pipe(Layer.fresh, Layer.build);
        const reader = yield* PeerTaskLifecycle.layer.pipe(
          Layer.provide(
            Layer.mergeAll(
              Layer.succeedContext(b.services),
              Layer.succeedContext(restoredRuntime),
              Layer.succeedContext(reopened),
              Layer.succeed(RecoveryWaitState, {
                ...b.lifecycleRecovery,
                read: (_threadId, runId) =>
                  RecoveryHistory.readRecoveryState(runId).pipe(
                    Effect.provide(Layer.succeedContext(reopened)),
                  ),
              }),
            ),
          ),
          Layer.fresh,
          Layer.build,
        );
        const session = (yield* b.linkedSessions).find(
          (session) => session.sessionId === originalGrant?.sessionId,
        );
        if (session === undefined)
          return yield* Effect.die(
            "Original authenticated peer session disappeared after reader reconstruction",
          );
        expect(yield* b.authority.grants).toContainEqual(originalGrant);
        const persistedSession = yield* Context.get(reopened, SqlClient.SqlClient)<{
          expires_at: string;
          revoked_at: string | null;
        }>`SELECT expires_at,revoked_at FROM auth_sessions WHERE session_id=${session.sessionId}`;
        expect(persistedSession[0]).toEqual({
          expires_at: "9999-12-31T23:59:59.999Z",
          revoked_at: null,
        });
        const peerScope = {
          ...caller(b, childId),
          thread: undefined,
          client: {
            sessionId: session.sessionId,
            label: "A",
            access: "full-access" as const,
            linked: true,
          },
        };
        const restored = Context.get(reader, PeerTaskLifecycle.PeerTaskLifecycle);
        const request = {
          environmentId: a.environmentId,
          generation,
          pair,
          operation: { type: "observe" as const, after, wait: false },
        };
        const replayPending = yield* restored.execute(peerScope, request);
        expect(replayPending).toMatchObject({
          type: "observation",
          observation: { snapshot: { recoveryWaiting: true } },
        });
        // A decision-only commit wakes readers without requiring another run event.
        yield* RecoveryHistory.writeRecoveryOutcome({
          sourceRunId: continuation,
          threadId: childId,
          status: "decided",
          outcome: "not_retryable",
          reason: "non_retryable",
        }).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
          Effect.provideService(RecoverySignals.RecoveryWaitSignals, b.signals),
        );
        const recovered = yield* restored.execute(peerScope, request);
        expect(recovered).toMatchObject({
          type: "observation",
          observation: {
            result: { runId: continuation, status: "failed" },
            snapshot: { recoveryWaiting: false },
          },
        });
        const replay = yield* restored.execute(peerScope, request);
        if (replay.type !== "observation") return yield* Effect.die("Expected replay");
        if (recovered.type !== "observation") return yield* Effect.die("Expected result");
        expect(replay.observation.result).toEqual(recovered.observation.result);
        expect(replay.observation.cursor).toBe(recovered.observation.cursor);
        if (recovered.type !== "observation") return yield* Effect.die("Expected observation");
        yield* restored.execute(peerScope, {
          ...request,
          operation: { type: "acknowledge", through: recovered.observation.cursor },
        });
        const ack = yield* sql<{
          cursor: number;
        }>`SELECT cursor FROM peer_task_observation_ack WHERE task_id=${taskId}`;
        expect(ack[0]?.cursor).toBe(recovered.observation.cursor);
        const forged = yield* control(b.environmentId, generation, {
          pair: { ...pair, childThreadId: nested.childThreadId! },
          operation: { type: "view", after: 0 },
        }).pipe(Effect.result);
        expect(forged._tag).toBe("Failure");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.live(
  "lifecycle381: parent Stop after the original reaches later nested work and a late result cannot wake it",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* makeNode(descriptorOf("lifecycle-stop-b", "B", { peerAuthority: 1 }));
        const a = yield* makeNode(descriptorOf("lifecycle-stop-a", "A", { peerAuthority: 1 }));
        yield* linkTo(a.links, b, "full-access");
        const parentId = ThreadId.make("lifecycle-stop-parent");
        yield* seedParent(a, parentId);
        const receipt = yield* nativeCall(a, parentId, "delegate_task", {
          task: "Original and later nested work",
          target: {
            environmentId: b.environmentId,
            projectId: b.project.id,
            providerInstanceId: instanceId,
            model: selection.model,
          },
          clientRequestId: "stop-after-original381",
        });
        expect(receipt.isError, json(receipt.content)).not.toBe(true);
        const { taskId } = yield* decodeTaskReceipt(receipt.structuredContent);
        const childId = yield* attachedChild(a, parentId, taskId);
        const original = yield* finishLifecycleRun(b, childId, "stop-original-result");
        const observed = (
          predicate: (task: import("@t3tools/contracts").OrchestrationV2Subagent) => boolean,
        ) =>
          a.orchestrator
            .streamStoredEventsFrom({
              threadId: parentId,
              afterSequence: 0,
              eventType: "subagent.updated",
            })
            .pipe(
              Stream.filter(
                (stored) =>
                  stored.event.type === "subagent.updated" &&
                  stored.event.payload.id === taskId &&
                  predicate(stored.event.payload),
              ),
              Stream.runHead,
              Effect.flatMap(Effect.fromOption),
            );
        yield* observed((task) => task.remoteChild?.latestResult?.runId === original);
        yield* nativeCall(a, parentId, "task_status", { taskId });
        const follow = yield* nativeCall(a, parentId, "t3_thread_send", {
          environmentId: b.environmentId,
          threadId: childId,
          message: "Continue this job",
          clientRequestId: "stop-followup381",
        });
        expect(follow.isError, json(follow.content)).not.toBe(true);
        const laterRun = yield* activate(b, childId);
        const nestedReceipt = yield* nativeCall(b, childId, "delegate_task", {
          task: "Later nested work",
          target: { providerInstanceId: instanceId, model: selection.model },
          clientRequestId: "stop-nested381",
        });
        expect(nestedReceipt.isError, json(nestedReceipt.content)).not.toBe(true);
        const { taskId: nestedId } = yield* decodeTaskReceipt(nestedReceipt.structuredContent);
        const nested = (yield* b.threads.getThreadRecords(childId, ["subagents"])).subagents.find(
          (task) => task.id === nestedId,
        )!;
        const nestedRun = yield* activate(b, nested.childThreadId!);
        yield* a.threads.dispatch({
          type: "thread.stop",
          threadId: parentId,
          commandId: CommandId.make("actual-parent-stop381"),
        });
        yield* a.threads.stopDelegatedTasks({
          threadId: parentId,
          commandId: CommandId.make("actual-parent-stop381"),
        });
        yield* observed((task) => task.remoteChild?.stop?.state === "acknowledged").pipe(
          Effect.raceFirst(
            observed((task) => task.remoteChild?.stop?.state === "unconfirmed").pipe(
              Effect.flatMap(() =>
                Effect.die(
                  new Error(
                    "The real parent Stop was denied after the parent run ended; the later destination run remains active.",
                  ),
                ),
              ),
            ),
          ),
        );
        yield* drainStops(b);
        expect(
          (yield* b.threads.getThreadRecords(childId, ["runs"])).runs.find(
            (run) => run.id === laterRun,
          )?.status,
        ).toBe("interrupted");
        expect(
          (yield* b.threads.getThreadRecords(nested.childThreadId!, ["runs"])).runs.find(
            (run) => run.id === nestedRun,
          )?.status,
        ).toBe("interrupted");
        yield* observed((task) => task.remoteChild?.latestResult?.runId === laterRun);
        const stopped = yield* a.threads.getThreadRecords(parentId, ["subagents", "messages"]);
        expect(stopped.subagents.find((task) => task.id === taskId)).toMatchObject({
          result: "stop-original-result",
          remoteChild: { runId: original, stop: { state: "acknowledged" } },
          completionDelivery: { state: "disposed" },
        });
        const notices = stopped.messages
          .filter((message) => message.notification?.source.kind === "delegated_task")
          .map((message) => message.id);
        const run = (yield* b.threads.getThreadRecords(childId, ["runs"])).runs.find(
          (run) => run.id === laterRun,
        )!;
        const now = yield* DateTime.now;
        const eventId = EventId.make("late-real-terminal381");
        yield* b.sink.write({
          events: [
            {
              id: eventId,
              type: "run.updated",
              threadId: childId,
              runId: laterRun,
              occurredAt: now,
              payload: { ...run, status: "completed", completedAt: now },
            },
          ],
        });
        yield* observed((task) => task.remoteChild?.latestResult?.eventId === eventId);
        const late = yield* a.threads.getThreadRecords(parentId, ["subagents", "messages"]);
        expect(late.subagents.find((task) => task.id === taskId)).toMatchObject({
          result: "stop-original-result",
          remoteChild: {
            runId: original,
            latestResult: { runId: laterRun, status: "completed", eventId },
          },
          completionDelivery: { state: "disposed" },
        });
        expect(
          late.messages
            .filter((message) => message.notification?.source.kind === "delegated_task")
            .map((message) => message.id),
        ).toEqual(notices);
        expect(yield* a.orchestrator.getThreadShell(childId)).toBe(null);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.live(
  "lifecycle381: preserves a whole long original result across bounded result and client message pages",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* makeNode(descriptorOf("lifecycle-long-b", "B", { peerAuthority: 1 }));
        const a = yield* makeNode(descriptorOf("lifecycle-long-a", "A", { peerAuthority: 1 }));
        yield* linkTo(a.links, b, "full-access");
        const parentId = ThreadId.make("lifecycle-long-parent");
        yield* seedParent(a, parentId);
        const receipt = yield* nativeCall(a, parentId, "delegate_task", {
          task: "Long result",
          target: {
            environmentId: b.environmentId,
            projectId: b.project.id,
            providerInstanceId: instanceId,
            model: selection.model,
          },
          clientRequestId: "long381",
        });
        expect(receipt.isError, json(receipt.content)).not.toBe(true);
        const { taskId } = yield* decodeTaskReceipt(receipt.structuredContent);
        const childId = yield* attachedChild(a, parentId, taskId);
        const text = "begin381\n" + "x".repeat(120000) + "\nend381";
        const runId = yield* finishLifecycleRun(b, childId, "long381", "completed", text);
        yield* a.orchestrator
          .streamStoredEventsFrom({
            threadId: parentId,
            afterSequence: 0,
            eventType: "subagent.updated",
          })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "subagent.updated" &&
                stored.event.payload.id === taskId &&
                stored.event.payload.remoteChild?.latestResult?.runId === runId,
            ),
            Stream.runHead,
          );
        const task = (yield* a.threads.getThreadRecords(parentId, ["subagents"])).subagents.find(
          (task) => task.id === taskId,
        )!;
        expect(task.result === text).toBe(true);
        expect(task.remoteChild?.latestResult?.text === text).toBe(true);
        expect(task.remoteChild?.latestResult?.truncated).toBe(false);
        let after = 0;
        let offset = 0;
        let combined = "";
        for (let page = 0; page < 6; page++) {
          const view = yield* a.remote.view(parentId, taskId, after, offset);
          expect(
            view.messages.reduce((count, message) => count + message.text.length, 0),
          ).toBeLessThanOrEqual(50000);
          combined += view.messages
            .filter((message) => message.role === "assistant")
            .map((message) => message.text)
            .join("");
          if (view.nextCursor === null) break;
          after = view.nextCursor;
          offset = view.nextTextOffset ?? 0;
        }
        expect(combined === text).toBe(true);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.live(
  "lifecycle381: cancellation interrupts a held result continuation without publishing its partial first result",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* makeNode(descriptorOf("lifecycle-page-stop-b", "B", { peerAuthority: 1 }));
        const a = yield* makeNode(descriptorOf("lifecycle-page-stop-a", "A", { peerAuthority: 1 }));
        yield* linkTo(a.links, b, "full-access");
        const parentId = ThreadId.make("lifecycle-page-stop-parent");
        yield* seedParent(a, parentId);
        const receipt = yield* nativeCall(a, parentId, "delegate_task", {
          task: "Page cancellation",
          target: {
            environmentId: b.environmentId,
            projectId: b.project.id,
            providerInstanceId: instanceId,
            model: selection.model,
          },
          clientRequestId: "page-stop381",
        });
        expect(receipt.isError, json(receipt.content)).not.toBe(true);
        const { taskId } = yield* decodeTaskReceipt(receipt.structuredContent);
        const childId = yield* attachedChild(a, parentId, taskId);
        yield* Ref.set(a.holdResultPage, true);
        const runId = yield* finishLifecycleRun(
          b,
          childId,
          "page-stop381",
          "completed",
          "late381" + "x".repeat(60000),
        );
        yield* Deferred.await(a.resultPageStarted);
        const cancelled = yield* nativeCall(a, parentId, "task_cancel", {
          taskId,
          clientRequestId: "cancel-page381",
        });
        expect(cancelled.isError, json(cancelled.content)).not.toBe(true);
        expect(cancelled.structuredContent).toMatchObject({
          taskId,
          status: "cancel_requested",
          stopState: "pending",
        });
        const event = yield* a.orchestrator
          .streamStoredEventsFrom({
            threadId: parentId,
            afterSequence: 0,
            eventType: "subagent.updated",
          })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "subagent.updated" &&
                stored.event.payload.id === taskId &&
                stored.event.payload.remoteChild?.stop?.state === "acknowledged",
            ),
            Stream.runHead,
            Effect.flatMap(Effect.fromOption),
          );
        expect(event.event.type === "subagent.updated" && event.event.payload.result).toBe(
          "Cancelled.",
        );
        yield* Ref.set(a.holdResultPage, false);
        yield* Deferred.succeed(a.releaseResultPage, undefined);
        yield* a.orchestrator
          .streamStoredEventsFrom({
            threadId: parentId,
            afterSequence: 0,
            eventType: "subagent.updated",
          })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "subagent.updated" &&
                stored.event.payload.id === taskId &&
                stored.event.payload.remoteChild?.latestResult?.runId === runId,
            ),
            Stream.runHead,
          );
        const task = (yield* a.threads.getThreadRecords(parentId, ["subagents"])).subagents.find(
          (task) => task.id === taskId,
        )!;
        expect(task.result).toBe("Cancelled.");
        expect(task.completionDelivery?.state).toBe("disposed");
        expect(task.remoteChild?.latestResult?.text.length).toBe(60007);
        const drainWith = (failStop: boolean) =>
          EffectWorker.OrchestrationEffectWorkerV2.pipe(
            Effect.provide(
              EffectWorker.layerWithOptions({ maxAttempts: 1 }).pipe(
                Layer.provide(
                  Layer.succeed(EffectWorker.OrchestrationEffectExecutorV2, {
                    execute: (effect) =>
                      failStop && effect.request.type === "delegated-tasks.stop"
                        ? Effect.fail(
                            new EffectWorker.OrchestrationEffectExecutionError({
                              effectId: effect.id,
                              effectType: effect.request.type,
                              cause: "Fixture rejects native stop completion",
                            }),
                          )
                        : Effect.void,
                  }),
                ),
                Layer.provide(Layer.succeed(PeerAuthority.PeerAuthority, b.authority)),
                Layer.provide(Layer.succeedContext(b.runtime)),
              ),
            ),
          );
        const stoppedObservation = (
          predicate: (
            remote: NonNullable<
              import("@t3tools/contracts").OrchestrationV2Subagent["remoteChild"]
            >,
          ) => boolean,
        ) =>
          a.orchestrator
            .streamStoredEventsFrom({
              threadId: parentId,
              afterSequence: 0,
              eventType: "subagent.updated",
            })
            .pipe(
              Stream.filter(
                (stored) =>
                  stored.event.type === "subagent.updated" &&
                  stored.event.payload.id === taskId &&
                  stored.event.payload.remoteChild !== undefined &&
                  predicate(stored.event.payload.remoteChild),
              ),
              Stream.runHead,
              Effect.flatMap(Effect.fromOption),
            );
        yield* (yield* drainWith(true)).drain();
        const failed = yield* stoppedObservation(
          (remote) =>
            remote.stop?.state === "unconfirmed" &&
            remote.snapshot?.stopExecution?.state === "failed",
        );
        expect(
          failed.event.type === "subagent.updated" &&
            failed.event.payload.remoteChild?.stoppedThere,
        ).not.toBe(true);
        const failedCommand =
          failed.event.type === "subagent.updated"
            ? failed.event.payload.remoteChild?.stop?.destinationCommandId
            : undefined;
        expect(failedCommand).toBeDefined();
        const retried = yield* nativeCall(a, parentId, "task_cancel", {
          taskId,
          clientRequestId: "retry-native-stop381",
        });
        expect(retried.isError, json(retried.content)).not.toBe(true);
        yield* stoppedObservation(
          (remote) =>
            remote.stop?.state === "acknowledged" &&
            remote.stop.destinationCommandId !== undefined &&
            remote.stop.destinationCommandId !== failedCommand,
        );
        yield* (yield* drainWith(false)).drain();
        const confirmed = yield* stoppedObservation((remote) => remote.stop?.state === "confirmed");
        expect(confirmed.event.type === "subagent.updated" && confirmed.event.payload.result).toBe(
          "Cancelled.",
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.live(
  "lifecycle381: durable stopped-parent control denies forged intent and mode escalation and replays a lost Stop without stopping later work",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* makeNode(
          descriptorOf("stop-control-b", "B", { peerAuthority: 1 }),
          "disk",
        );
        const a = yield* makeNode(
          descriptorOf("stop-control-a", "A", { peerAuthority: 1 }),
          "disk",
        );
        yield* linkTo(a.links, b, "full-access");
        const parentId = ThreadId.make("stop-control-parent");
        yield* seedParent(a, parentId);
        const { taskId, childId } = yield* delegate(a, b, parentId, "stop-control-job");
        yield* activate(b, childId);
        yield* a.stopConnections;
        yield* a.threads.dispatch({
          type: "thread.stop",
          threadId: parentId,
          commandId: CommandId.make("stop-control-parent381"),
        });
        yield* a.threads.stopDelegatedTasks({
          threadId: parentId,
          commandId: CommandId.make("stop-control-parent381"),
        });
        yield* a.remote.requestStop(parentId, taskId, "client-stop:review-first381");
        const task = (yield* a.threads.getThreadRecords(parentId, ["subagents"])).subagents.find(
          (t) => t.id === taskId,
        )!;
        const remote = task.remoteChild!;
        const generation = remote.launch!.linkIdentity;
        const pair = {
          parentEnvironmentId: a.environmentId,
          parentThreadId: parentId,
          taskId,
          childEnvironmentId: b.environmentId,
          childThreadId: childId,
          childRunId: null,
        };
        const operation = { type: "stop" as const, requestId: remote.stop!.requestId };
        const sourceConflict = yield* a.remote
          .requestStop(parentId, taskId, operation.requestId, "changed semantics")
          .pipe(Effect.result);
        expect(sourceConflict._tag).toBe("Failure");
        expect(
          (yield* a.threads.getThreadRecords(parentId, ["subagents"])).subagents.find(
            (candidate) => candidate.id === taskId,
          )?.remoteChild?.stop,
        ).toEqual(remote.stop);
        let connected = yield* a.reconnect();
        const state = () => b.threads.getThreadEventSequence(childId);
        const before = yield* state();
        const initialOutbox = yield* Context.get(
          b.database,
          SqlClient.SqlClient,
        )`SELECT effect_id FROM orchestration_v2_effect_outbox ORDER BY effect_id`;
        const denied = (input: Parameters<NonNullable<typeof connected.peers.taskLifecycle>>[2]) =>
          connected.peers.taskLifecycle!(b.environmentId, generation, input).pipe(
            Effect.result,
            Effect.tap((result) => Effect.sync(() => expect(result._tag).toBe("Failure"))),
          );
        yield* denied({ pair, operation: { ...operation, requestId: "never-accepted381" } });
        yield* denied({ pair: { ...pair, taskId: NodeId.make("other-task381") }, operation });
        yield* denied({ pair: { ...pair, childThreadId: parentId }, operation });
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          threadId: parentId,
          commandId: CommandId.make("stop-control-auto"),
          runtimeMode: "auto",
        });
        yield* denied({ pair, operation });
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          threadId: parentId,
          commandId: CommandId.make("stop-control-full"),
          runtimeMode: "full-access",
        });
        yield* a.threads.dispatch({
          type: "thread.interaction-mode.set",
          threadId: parentId,
          commandId: CommandId.make("stop-control-plan"),
          interactionMode: "plan",
        });
        yield* denied({ pair, operation });
        yield* a.threads.dispatch({
          type: "thread.interaction-mode.set",
          threadId: parentId,
          commandId: CommandId.make("stop-control-default"),
          interactionMode: "default",
        });
        expect(yield* state()).toBe(before);
        expect(
          yield* Context.get(
            b.database,
            SqlClient.SqlClient,
          )`SELECT effect_id FROM orchestration_v2_effect_outbox ORDER BY effect_id`,
        ).toEqual(initialOutbox);
        const currentGrant = (yield* b.authority.grants)[0]!;
        const expiry = (yield* Context.get(b.database, SqlClient.SqlClient)<{
          expires_at: string;
        }>`SELECT expires_at FROM auth_sessions WHERE session_id=${currentGrant.sessionId}`)[0]!
          .expires_at;
        yield* Context.get(
          b.database,
          SqlClient.SqlClient,
        )`UPDATE auth_sessions SET expires_at='2020-01-01T00:00:00.000Z' WHERE session_id=${currentGrant.sessionId}`;
        yield* denied({ pair, operation });
        yield* Context.get(
          b.database,
          SqlClient.SqlClient,
        )`UPDATE auth_sessions SET expires_at=${expiry} WHERE session_id=${currentGrant.sessionId}`;
        expect(yield* state()).toBe(before);
        yield* b.threads.dispatch({
          type: "thread.runtime-mode.set",
          threadId: childId,
          commandId: CommandId.make("stop-positive-narrow-target381"),
          runtimeMode: "auto",
        });
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          threadId: parentId,
          commandId: CommandId.make("stop-positive-narrow-source381"),
          runtimeMode: "auto",
        });
        yield* Ref.set(a.dropAckResponses, 1);
        const lost = yield* connected.peers.taskLifecycle!(b.environmentId, generation, {
          pair,
          operation,
        }).pipe(Effect.result);
        expect(lost._tag).toBe("Failure");
        yield* Deferred.await(a.ackResponseLost);
        const committed = yield* state();
        expect(committed).toBeGreaterThan(before);
        yield* connected.stop;
        connected = yield* a.reconnect();
        const replay = yield* connected.peers.taskLifecycle!(b.environmentId, generation, {
          pair,
          operation,
        });
        expect(replay.type).toBe("stop");
        expect(yield* state()).toBe(committed);
        yield* denied({ pair, operation: { ...operation, reason: "changed semantics" } });
        const effects = yield* Context.get(b.database, SqlClient.SqlClient)<{
          effect_id: string;
          thread_id: string;
        }>`SELECT effect_id,thread_id FROM orchestration_v2_effect_outbox WHERE command_id=${replay.type === "stop" ? replay.commandId : "missing"}`;
        expect(effects.length).toBeGreaterThan(0);
        for (const effect of effects)
          yield* b.authority.checkEffect(effect.effect_id, ThreadId.make(effect.thread_id));
        const stopOwners = yield* Context.get(b.database, SqlClient.SqlClient)<{
          record_id: string;
        }>`SELECT owner.record_id FROM peer_effect_authority AS owner JOIN orchestration_v2_effect_outbox AS effect ON effect.effect_id=owner.effect_id WHERE effect.command_id=${replay.type === "stop" ? replay.commandId : "missing"} LIMIT 1`;
        expect(stopOwners).toHaveLength(1);
        const illegal = yield* b.threads
          .dispatch({
            type: "message.dispatch",
            threadId: childId,
            commandId: CommandId.make("stop-cannot-start381"),
            messageId: MessageId.make("stop-cannot-start381"),
            text: "Must never start",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "agent",
            creationSource: "mcp",
          })
          .pipe(
            Effect.provideService(PeerDispatchAuthority, stopOwners[0]!.record_id),
            Effect.exit,
          );
        expect(illegal._tag).toBe("Failure");
        expect(yield* state()).toBe(committed);
        yield* b.threads.dispatch({
          type: "thread.runtime-mode.set",
          threadId: childId,
          commandId: CommandId.make("raise-target-before-native-claim381"),
          runtimeMode: "full-access",
        });
        yield* drainStops(b);
        const deniedEffects = yield* Context.get(b.database, SqlClient.SqlClient)<{
          status: string;
        }>`SELECT status FROM orchestration_v2_effect_outbox WHERE command_id=${replay.type === "stop" ? replay.commandId : "missing"}`;
        expect(deniedEffects.length).toBeGreaterThan(0);
        expect(deniedEffects.every((effect) => effect.status === "failed")).toBe(true);
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          threadId: parentId,
          commandId: CommandId.make("restore-stop-native-claim381"),
          runtimeMode: "full-access",
        });
        // A fresh local input after the accepted Stop is independent later work.
        yield* b.threads.dispatch({
          type: "message.dispatch",
          threadId: childId,
          commandId: CommandId.make("independent-after-stop381"),
          messageId: MessageId.make("independent-after-stop381"),
          text: "Independent later work",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        const later = yield* activate(b, childId);
        const laterSequence = yield* state();
        const same = yield* connected.peers.taskLifecycle!(b.environmentId, generation, {
          pair,
          operation,
        });
        expect(same).toEqual(replay);
        expect(yield* state()).toBe(laterSequence);
        expect(
          (yield* b.threads.getThreadRecords(childId, ["runs"])).runs.find((r) => r.id === later)
            ?.status,
        ).toBe("running");
        // An old lost reply cannot replace a newer deliberate obligation.
        yield* Effect.all(
          [
            a.remote.requestStop(parentId, taskId, "client-stop:review-newer381", "later work"),
            a.remote.requestStop(parentId, taskId, operation.requestId),
            a.remote.requestStop(parentId, taskId, "client-stop:review-newer381", "later work"),
          ],
          { concurrency: "unbounded" },
        );
        const newer = (yield* a.threads.getThreadRecords(parentId, ["subagents"])).subagents.find(
          (candidate) => candidate.id === taskId,
        )!.remoteChild!.stop;
        yield* a.remote.requestStop(parentId, taskId, operation.requestId);
        expect(
          (yield* a.threads.getThreadRecords(parentId, ["subagents"])).subagents.find(
            (candidate) => candidate.id === taskId,
          )!.remoteChild!.stop,
        ).toEqual(newer);
        yield* a.threads.dispatch({
          type: "delegated_task.remote.observe",
          commandId: CommandId.make("serialized-old-replay381"),
          parentThreadId: parentId,
          taskId,
          stop: { requestId: operation.requestId, state: "pending" },
        });
        expect(
          (yield* a.threads.getThreadRecords(parentId, ["subagents"])).subagents.find(
            (candidate) => candidate.id === taskId,
          )!.remoteChild!.stop,
        ).toEqual(newer);
        const historicalConflict = yield* a.remote
          .requestStop(parentId, taskId, operation.requestId, "changed historical reason")
          .pipe(Effect.result);
        expect(historicalConflict._tag).toBe("Failure");
        yield* connected.stop;
        connected = yield* a.reconnect();
        yield* Context.get(connected.services, RemoteDelegation.RemoteDelegation).start();
        const stopEvent = yield* a.orchestrator
          .streamStoredEventsFrom({
            threadId: parentId,
            afterSequence: 0,
            eventType: "subagent.updated",
          })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "subagent.updated" &&
                stored.event.payload.id === taskId &&
                stored.event.payload.remoteChild?.stop?.requestId ===
                  "client-stop:review-newer381" &&
                stored.event.payload.remoteChild.stop.state === "acknowledged",
            ),
            Stream.runHead,
            Effect.flatMap(Effect.fromOption),
          );
        expect(
          stopEvent.event.type === "subagent.updated" &&
            stopEvent.event.payload.remoteChild?.stop?.destinationCommandId,
        ).not.toBe(replay.type === "stop" ? replay.commandId : undefined);
        yield* drainStops(b);
        expect(
          (yield* b.threads.getThreadRecords(childId, ["runs"])).runs.find(
            (run) => run.id === later,
          )?.status,
        ).toBe("interrupted");
        yield* a.orchestrator
          .streamStoredEventsFrom({
            threadId: parentId,
            afterSequence: 0,
            eventType: "subagent.updated",
          })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "subagent.updated" &&
                stored.event.payload.id === taskId &&
                stored.event.payload.remoteChild?.stop?.requestId ===
                  "client-stop:review-newer381" &&
                stored.event.payload.remoteChild.stop.state === "confirmed",
            ),
            Stream.runHead,
            Effect.flatMap(Effect.fromOption),
          );
        const pendingRows = yield* Context.get(a.database, SqlClient.SqlClient)<{
          count: number;
        }>`SELECT COUNT(*) AS count FROM orchestration_events WHERE stream_id=${parentId} AND event_type='subagent.updated' AND json_extract(payload_json,'$.id')=${taskId} AND json_extract(payload_json,'$.remoteChild.stop.requestId')='client-stop:review-newer381' AND json_extract(payload_json,'$.remoteChild.stop.state')='pending'`;
        expect(pendingRows[0]?.count).toBe(1);
        yield* connected.stop;
        yield* a.threads.dispatch({
          type: "delegated_task.remote.observe",
          commandId: CommandId.make("new-stop-obligation381"),
          parentThreadId: parentId,
          taskId,
          stop: { requestId: "new-deliberate-stop381", state: "pending" },
        });
        const afterNewStopSequence = yield* state();
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.auth.revokeSession(grant.sessionId);
        yield* denied({ pair, operation: { type: "stop", requestId: "new-deliberate-stop381" } });
        expect(yield* state()).toBe(afterNewStopSequence);
        yield* linkTo(a.links, b, "full-access");
        yield* denied({ pair, operation: { type: "stop", requestId: "new-deliberate-stop381" } });
        expect(yield* state()).toBe(afterNewStopSequence);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.live(
  "lifecycle381: native Stop recovery carries only each hop's accepted obligation from stopped A through B to C",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const c = yield* makeNode(descriptorOf("stop-chain-c", "C", { peerAuthority: 1 }), "disk");
        const b = yield* makeNode(descriptorOf("stop-chain-b", "B", { peerAuthority: 1 }), "disk");
        const a = yield* makeNode(descriptorOf("stop-chain-a", "A", { peerAuthority: 1 }), "disk");
        yield* linkTo(a.links, b, "full-access");
        yield* linkTo(b.links, c, "full-access");
        const parentId = ThreadId.make("stop-chain-parent");
        yield* seedParent(a, parentId);
        yield* a.forwarding.call(
          caller(a, parentId),
          ProjectToolkit.tools.t3_project_list,
          b.environmentId,
          {},
        );
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, onward: true });
        const first = yield* delegate(a, b, parentId, "stop-chain-a-b");
        const bRun = yield* activate(b, first.childId);
        const second = yield* delegate(b, c, first.childId, "stop-chain-b-c");
        const cRun = yield* activate(c, second.childId);
        const observed = (
          node: Node,
          parent: ThreadId,
          taskId: NodeId,
          state: string,
          requestId?: string,
        ) =>
          node.orchestrator
            .streamStoredEventsFrom({
              threadId: parent,
              afterSequence: 0,
              eventType: "subagent.updated",
            })
            .pipe(
              Stream.filter(
                (stored) =>
                  stored.event.type === "subagent.updated" &&
                  stored.event.payload.id === taskId &&
                  stored.event.payload.remoteChild?.stop?.state === state &&
                  (requestId === undefined ||
                    stored.event.payload.remoteChild.stop.requestId === requestId),
              ),
              Stream.runHead,
              Effect.flatMap(Effect.fromOption),
            );
        // A native caller on a raised B shell still retains A's live ceiling.
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          threadId: parentId,
          commandId: CommandId.make("stop-chain-narrow-origin"),
          runtimeMode: "auto",
        });
        const cBeforeNative = yield* c.threads.getThreadEventSequence(second.childId);
        const nativeCancel = yield* nativeCall(b, first.childId, "task_cancel", {
          taskId: second.taskId,
          clientRequestId: "stop-chain-native-ceiling",
        });
        expect(nativeCancel.isError, json(nativeCancel.content)).not.toBe(true);
        yield* observed(b, first.childId, second.taskId, "unconfirmed");
        expect(yield* c.threads.getThreadEventSequence(second.childId)).toBe(cBeforeNative);
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          threadId: parentId,
          commandId: CommandId.make("stop-chain-restore-origin"),
          runtimeMode: "full-access",
        });
        yield* b.threads.dispatch({
          type: "thread.runtime-mode.set",
          threadId: first.childId,
          commandId: CommandId.make("stop-chain-lower-b"),
          runtimeMode: "auto",
        });
        yield* a.threads.dispatch({
          type: "thread.stop",
          threadId: parentId,
          commandId: CommandId.make("stop-chain-a-first"),
        });
        yield* a.threads.stopDelegatedTasks({
          threadId: parentId,
          commandId: CommandId.make("stop-chain-a-first"),
        });
        yield* observed(a, parentId, first.taskId, "acknowledged");
        expect((yield* a.threads.getThreadShell(parentId))?.activeRunId).toBeNull();
        expect(
          (yield* b.threads.getThreadRecords(first.childId, ["runs"])).runs.find(
            (run) => run.id === bRun,
          )?.status,
        ).toBe("interrupted");
        // Before the native descendant effect executes, C is still running and A cannot confirm.
        expect(
          (yield* c.threads.getThreadRecords(second.childId, ["runs"])).runs.find(
            (run) => run.id === cRun,
          )?.status,
        ).toBe("running");
        yield* drainStops(b);
        const currentStop = (yield* b.threads.getThreadRecords(first.childId, [
          "subagents",
        ])).subagents.find((task) => task.id === second.taskId)!.remoteChild!.stop!;
        yield* observed(b, first.childId, second.taskId, "unconfirmed", currentStop.requestId);
        const aTask = (yield* a.threads.getThreadRecords(parentId, ["subagents"])).subagents.find(
          (task) => task.id === first.taskId,
        )!;
        expect(aTask.remoteChild?.stop?.state).not.toBe("confirmed");
        const cBefore = yield* c.threads.getThreadEventSequence(second.childId);
        const cEffects = yield* Context.get(
          c.database,
          SqlClient.SqlClient,
        )`SELECT effect_id FROM orchestration_v2_effect_outbox ORDER BY effect_id`;
        expect(
          (yield* c.threads.getThreadRecords(second.childId, ["runs"])).runs.find(
            (run) => run.id === cRun,
          )?.status,
        ).toBe("running");
        const obligation = (yield* b.threads.getThreadRecords(first.childId, [
          "subagents",
        ])).subagents.find((task) => task.id === second.taskId)!.remoteChild!.stop!;
        const inherited = yield* Context.get(b.database, SqlClient.SqlClient)<{
          record_id: string;
        }>`SELECT record_id FROM peer_task_stop_authority WHERE task_id=${second.taskId} AND request_id=${obligation.requestId}`;
        expect(inherited).toHaveLength(1);
        // Restart the observer after the persisted denied intent, then restore its current modes.
        yield* b.stopConnections;
        yield* b.threads.dispatch({
          type: "thread.runtime-mode.set",
          threadId: first.childId,
          commandId: CommandId.make("stop-chain-restore-b"),
          runtimeMode: "full-access",
        });
        const resumed = yield* b.reconnect();
        yield* Context.get(resumed.services, RemoteDelegation.RemoteDelegation).start();
        yield* observed(b, first.childId, second.taskId, "acknowledged", obligation.requestId);
        expect(
          (yield* c.threads.getThreadRecords(second.childId, ["runs"])).runs.find(
            (run) => run.id === cRun,
          )?.status,
        ).toBe("interrupted");
        expect(yield* c.threads.getThreadEventSequence(second.childId)).toBeGreaterThan(cBefore);
        expect(
          yield* Context.get(
            c.database,
            SqlClient.SqlClient,
          )`SELECT effect_id FROM orchestration_v2_effect_outbox ORDER BY effect_id`,
        ).not.toEqual(cEffects);
        yield* drainStops(c);
        yield* observed(b, first.childId, second.taskId, "confirmed", obligation.requestId);
        yield* observed(a, parentId, first.taskId, "confirmed");
        const bTask = (yield* b.threads.getThreadRecords(first.childId, [
          "subagents",
        ])).subagents.find((task) => task.id === second.taskId)!;
        const finalA = (yield* a.threads.getThreadRecords(parentId, ["subagents"])).subagents.find(
          (task) => task.id === first.taskId,
        )!;
        expect(bTask.remoteChild?.stop?.requestId).toBe(obligation.requestId);
        expect(bTask.remoteChild?.stop?.destinationCommandId).toBeDefined();
        expect(finalA.remoteChild?.stop?.destinationCommandId).toBeDefined();
        expect(bTask.remoteChild?.stop?.destinationCommandId).not.toBe(
          finalA.remoteChild?.stop?.destinationCommandId,
        );
        const nativeEvidence = (node: Node, commandId: CommandId) =>
          Context.get(node.database, SqlClient.SqlClient)<{
            effect_id: string;
            effect_type: string;
            status: string;
          }>`SELECT effect_id,effect_type,status FROM orchestration_v2_effect_outbox WHERE command_id=${commandId} ORDER BY effect_id`;
        yield* Effect.logInfo("381 generated-fixture Stop proof", {
          sourceParentThreadId: parentId,
          bTaskId: first.taskId,
          bThreadId: first.childId,
          bRunId: bRun,
          bSourceRequestId: finalA.remoteChild!.stop!.requestId,
          bCommandId: finalA.remoteChild!.stop!.destinationCommandId,
          cTaskId: second.taskId,
          cThreadId: second.childId,
          cRunId: cRun,
          cSourceRequestId: obligation.requestId,
          cCommandId: bTask.remoteChild!.stop!.destinationCommandId,
          bEffects: yield* nativeEvidence(b, finalA.remoteChild!.stop!.destinationCommandId!),
          cEffects: yield* nativeEvidence(c, bTask.remoteChild!.stop!.destinationCommandId!),
        });

        expect((yield* a.threads.getThreadShell(parentId))?.activeRunId).toBeNull();
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.live(
  "lifecycle381: same public cancellation replays after an in-flight launch attaches",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* makeNode(
          descriptorOf("review-attach-b", "B", { peerAuthority: 1 }),
          "disk",
        );
        const a = yield* makeNode(
          descriptorOf("review-attach-a", "A", { peerAuthority: 1 }),
          "disk",
        );
        yield* linkTo(a.links, b, "full-access");
        const parentId = ThreadId.make("review-attach-parent");
        yield* seedParent(a, parentId);
        yield* Ref.set(a.holdLaunchReply, true);
        yield* nativeCall(a, parentId, "delegate_task", {
          task: "Cancel an accepted launch with a delayed reply",
          target: {
            environmentId: b.environmentId,
            projectId: b.project.id,
            providerInstanceId: instanceId,
            model: selection.model,
          },
          clientRequestId: "review-attach-launch",
        }).pipe(Effect.forkScoped);
        yield* Deferred.await(a.launchReplyHeld);
        const before = (yield* a.threads.getThreadRecords(parentId, ["subagents"])).subagents[0]!;
        expect(before.remoteChild?.threadId).toBeUndefined();
        const stop = { taskId: before.id, clientRequestId: "review-attach-stop" };
        const stopCredential = yield* a.registry.issue({
          threadId: parentId,
          providerInstanceId: instanceId,
          capabilities: new Set(["orchestration"]),
        });
        const accepted = yield* nativeCall(
          a,
          parentId,
          "task_cancel",
          stop,
          new Set(["orchestration"]),
          stopCredential,
        );
        expect(accepted.isError, json(accepted.content)).not.toBe(true);
        const sameBeforeAttach = yield* nativeCall(
          a,
          parentId,
          "task_cancel",
          stop,
          new Set(["orchestration"]),
          stopCredential,
        );
        expect(sameBeforeAttach.isError, json(sameBeforeAttach.content)).not.toBe(true);
        const intent = (yield* a.threads.getThreadRecords(parentId, ["subagents"])).subagents[0]!
          .remoteChild!.stop!;
        yield* Ref.set(a.holdLaunchReply, false);
        yield* Deferred.succeed(a.releaseLaunchReply, undefined);
        const childId = yield* attachedChild(a, parentId, before.id);
        const after = (yield* a.threads.getThreadRecords(parentId, ["subagents"])).subagents[0]!;
        expect(after.remoteChild?.stop?.requestId).toBe(intent.requestId);
        expect(after.remoteChild?.threadId).toBe(childId);
        const history = yield* Context.get(
          a.database,
          SqlClient.SqlClient,
        )`SELECT sequence,json_extract(payload_json,'$.remoteChild.threadId') AS target,json_extract(payload_json,'$.remoteChild.stop.requestId') AS request_id FROM orchestration_events WHERE stream_id=${parentId} AND event_type='subagent.updated' AND json_extract(payload_json,'$.remoteChild.stop.requestId')=${intent.requestId} ORDER BY sequence`;
        expect(history.some((row) => row.target === null)).toBe(true);
        expect(history.some((row) => row.target === childId)).toBe(true);
        const replay = yield* nativeCall(
          a,
          parentId,
          "task_cancel",
          stop,
          new Set(["orchestration"]),
          stopCredential,
        );

        expect(replay.isError, json(replay.content)).not.toBe(true);
        yield* a.stopConnections;
        const restored = yield* a.reconnect();
        const resumed = Context.get(restored.services, RemoteDelegation.RemoteDelegation);
        const current = (yield* a.threads.getThreadRecords(parentId, ["subagents"])).subagents.find(
          (task) => task.id === before.id,
        )!;
        const destinationBefore = yield* b.threads.getThreadEventSequence(childId);
        // A projection target alone cannot mint the original accepted attachment.
        for (const changed of [
          { ...current.remoteChild!, threadId: ThreadId.make("forged-other-child") },
          {
            ...current.remoteChild!,
            environmentId: EnvironmentId.make("forged-other-environment"),
          },
          {
            ...current.remoteChild!,
            launch: { ...current.remoteChild!.launch!, linkIdentity: "replacement-generation" },
          },
          {
            ...current.remoteChild!,
            launch: { ...current.remoteChild!.launch!, clientRequestId: "replacement-launch" },
          },
        ]) {
          const event = yield* a.orchestrator
            .streamStoredEventsFrom({
              threadId: parentId,
              afterSequence: 0,
              eventType: "subagent.updated",
            })
            .pipe(Stream.runHead, Effect.flatMap(Effect.fromOption));
          if (event.event.type !== "subagent.updated")
            return yield* Effect.die("Missing persisted task");
          yield* a.sink.write({
            events: [
              {
                ...event.event,
                id: EventId.make(
                  `forged-attachment:${changed.threadId}:${changed.environmentId}:${changed.launch!.linkIdentity}:${changed.launch!.clientRequestId}`,
                ),
                payload: { ...current, remoteChild: changed },
              },
            ],
          });
          expect(
            (yield* resumed.requestStop(parentId, before.id, intent.requestId).pipe(Effect.result))
              ._tag,
          ).toBe("Failure");
        }
        const now = yield* DateTime.now;
        yield* a.sink.write({
          events: [
            {
              id: EventId.make("restore-original-attachment"),
              type: "subagent.updated",
              threadId: parentId,
              occurredAt: now,
              payload: current,
            },
          ],
        });
        expect(yield* b.threads.getThreadEventSequence(childId)).toBe(destinationBefore);
        const afterRestart = yield* nativeCall(
          a,
          parentId,
          "task_cancel",
          stop,
          new Set(["orchestration"]),
          stopCredential,
        );
        expect(afterRestart.isError, json(afterRestart.content)).not.toBe(true);
        yield* resumed.requestStop(
          parentId,
          before.id,
          "client-stop:attachment-newer",
          "later intent",
        );
        const oldRetry = yield* nativeCall(
          a,
          parentId,
          "task_cancel",
          stop,
          new Set(["orchestration"]),
          stopCredential,
        );
        expect(oldRetry.isError, json(oldRetry.content)).not.toBe(true);
        expect(
          (yield* a.threads.getThreadRecords(parentId, ["subagents"])).subagents.find(
            (task) => task.id === before.id,
          )?.remoteChild?.stop?.requestId,
        ).toBe("client-stop:attachment-newer");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.live(
  "lifecycle381: production kernel exports the authenticated lifecycle transport and registered delegation",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* makeNode(
          descriptorOf("native-route-b", "B", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true },
        );
        const a = yield* makeNode(
          descriptorOf("native-route-a", "A", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const session = (yield* b.linkedSessions)[0];
        expect(session).toBeDefined();
        // A lifecycle-capability probe is an authenticated peer operation, not a descriptor assertion.
        const peers = Context.get(a.services, PeerMcpClient.PeerMcpClient);
        yield* peers.requireTaskLifecycle!(b.environmentId);
        const parentId = ThreadId.make("native-route-parent");
        yield* seedParent(a, parentId);
        const launched = yield* nativeCall(a, parentId, "delegate_task", {
          task: "Return native route evidence",
          mode: "async",
          clientRequestId: "native-route-job",
          target: {
            environmentId: b.environmentId,
            projectId: b.project.id,
            providerInstanceId: instanceId,
            model: "gpt-5.4",
          },
        });
        expect(launched.isError, json(launched.content)).not.toBe(true);
        const { taskId } = yield* decodeTaskReceipt(launched.structuredContent);
        const childId = yield* attachedChild(a, parentId, taskId);
        const view = yield* a.remote.view(parentId, taskId, 0);
        expect(view.threadId).toBe(childId);
        expect(view.environmentId).toBe(b.environmentId);
        expect(
          (yield* a.threads.getThreadRecords(parentId, ["subagents"])).subagents.find(
            (task) => task.id === taskId,
          )?.remoteChild?.launch?.lifecycleVersion,
        ).toBe(1);
        const http = yield* HttpClient.HttpClient;
        const unauthenticated = yield* http.execute(
          HttpClientRequest.get(`${b.url}/mcp/peer-control/tasks`),
        );
        expect(unauthenticated.status).toBe(401);
        const runId = yield* activate(b, childId);
        const stop = yield* nativeCall(a, parentId, "task_cancel", {
          taskId,
          clientRequestId: "native-route-stop",
        });
        expect(stop.isError, json(stop.content)).not.toBe(true);
        const stoppedObservation = (state: "acknowledged" | "confirmed") =>
          a.orchestrator
            .streamStoredEventsFrom({
              threadId: parentId,
              afterSequence: 0,
              eventType: "subagent.updated",
            })
            .pipe(
              Stream.filter(
                (stored) =>
                  stored.event.type === "subagent.updated" &&
                  stored.event.payload.id === taskId &&
                  stored.event.payload.remoteChild?.stop?.state === state,
              ),
              Stream.runHead,
              Effect.flatMap(Effect.fromOption),
            );
        const accepted = yield* stoppedObservation("acknowledged");
        if (accepted.event.type !== "subagent.updated")
          return yield* Effect.die("Expected Stop projection");
        const commandId = accepted.event.payload.remoteChild!.stop!.destinationCommandId!;
        const pair = {
          parentEnvironmentId: a.environmentId,
          parentThreadId: parentId,
          taskId,
          childEnvironmentId: b.environmentId,
          childThreadId: childId,
          childRunId: null,
        };
        const generation = accepted.event.payload.remoteChild!.launch!.linkIdentity;
        const observe = () =>
          peers.taskLifecycle!(b.environmentId, generation, {
            pair,
            operation: { type: "snapshot", after: 0, wait: false, stopCommandId: commandId },
          });
        expect(yield* observe()).toMatchObject({
          type: "snapshot",
          snapshot: { stopExecution: { state: "pending" } },
        });
        const sql = Context.get(b.database, SqlClient.SqlClient);
        expect(
          yield* sql`SELECT command_id FROM peer_task_stop_receipts WHERE command_id=${commandId}`,
        ).toHaveLength(1);
        yield* drainStops(b);
        expect(yield* observe()).toMatchObject({
          type: "snapshot",
          snapshot: { stopExecution: { state: "settled" } },
        });
        yield* stoppedObservation("confirmed");
        expect(
          (yield* b.threads.getThreadRecords(childId, ["runs"])).runs.find(
            (run) => run.id === runId,
          )?.status,
        ).toBe("interrupted");
        const effects = yield* sql<{
          effect_id: string;
          status: string;
        }>`SELECT effect_id,status FROM orchestration_v2_effect_outbox WHERE command_id=${commandId} AND effect_type='delegated-tasks.stop'`;
        expect(effects).toHaveLength(1);
        expect(effects[0]?.status).toBe("succeeded");
        yield* Effect.logInfo("Production lifecycle native receipt and shared outbox proof", {
          commandId,
          effects,
        });
      }),
    ).pipe(Effect.provide(Layer.merge(FetchHttpClient.layer, NodeServices.layer))),
  30_000,
);

it.live(
  "reviewer382: separate callers may reuse a local request key",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const node = yield* makeNode(
          descriptorOf("reviewer382", "Review", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const one = ThreadId.make("reviewer382-one"),
          two = ThreadId.make("reviewer382-two");
        yield* seedParent(node, one);
        yield* seedParent(node, two);
        const args = {
          message: "isolated request",
          mode: "queue",
          clientRequestId: "reviewer-shared-key",
        };
        const first = yield* nativeCall(node, one, "t3_thread_send", { ...args, threadId: one });
        expect(first.isError, json(first)).not.toBe(true);
        const second = yield* nativeCall(node, two, "t3_thread_send", { ...args, threadId: two });
        expect(second.isError, json(second)).not.toBe(true);
        for (const thread of [one, two]) {
          const renamed = yield* nativeCall(node, thread, "t3_thread_update", {
            action: "rename",
            title: `Independent ${thread}`,
            clientRequestId: "reviewer-shared-key",
          });
          expect(renamed.isError, json(renamed)).not.toBe(true);
          const scheduled = yield* nativeCall(node, thread, "schedule_task", {
            purposeIntent: { version: 1 as const, intent: { type: "unscoped" as const } },
            title: "Independent task",
            prompt: "Independent prompt",
            bindToCurrentThread: true,
            enabled: false,
            schedule: { type: "interval", everyMs: 3600000 },
            clientRequestId: "reviewer-shared-key",
          });
          expect(scheduled.isError, json(scheduled)).not.toBe(true);
        }
        expect((yield* node.scheduled.list()).tasks).toHaveLength(2);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30000,
);

it.live(
  "reviewer382: legacy metadata replay preserves later title",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const node = yield* makeNode(
          descriptorOf("reviewer382-import", "Import", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const parent = ThreadId.make("reviewer382-import-parent");
        yield* seedParent(node, parent);
        const oldCommand = CommandId.make(
          [
            "command",
            "mcp",
            "pre-upgrade-credential",
            "thread-update",
            encodeURIComponent(parent),
            "rename",
            "old-rename",
          ].join(":"),
        );
        yield* node.threads.dispatch({
          type: "thread.metadata.update",
          commandId: oldCommand,
          threadId: parent,
          title: "Original title",
        });
        const later = yield* nativeCall(node, parent, "t3_thread_update", {
          action: "rename",
          title: "Later independent title",
          clientRequestId: "later-edit",
        });
        expect(later.isError, json(later)).not.toBe(true);
        const replay = yield* nativeCall(node, parent, "t3_thread_update", {
          action: "rename",
          title: "Original title",
          clientRequestId: "old-rename",
        });
        const shell = yield* node.threads.getThreadShell(parent);
        expect(shell?.title, json(replay)).toBe("Later independent title");
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30000,
);

it.live(
  "reviewer382: concurrent keyed field edits retain both intents",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const node = yield* makeNode(
          descriptorOf("reviewer382-rivals", "Rivals", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const parent = ThreadId.make("reviewer382-rivals-parent");
        yield* seedParent(node, parent);
        const created = yield* nativeCall(node, parent, "schedule_task", {
          purposeIntent: { version: 1 as const, intent: { type: "unscoped" as const } },
          prompt: "Original prompt",
          title: "Original title",
          schedule: { type: "interval", everyMs: 60000 },
          clientRequestId: "reviewer-rival-create",
        });
        expect(created.isError, json(created)).not.toBe(true);
        const receipt = yield* decodeAutomationScheduleReceipt(created.structuredContent);
        const bothRead = yield* Deferred.make<void>();
        let reads = 0;
        const realList = node.scheduled.list;
        const spy = vi.spyOn(node.scheduled, "list").mockImplementation(() =>
          realList().pipe(
            Effect.tap(() =>
              Effect.gen(function* () {
                reads++;
                if (reads === 2) yield* Deferred.succeed(bothRead, undefined);
                if (reads <= 2) yield* Deferred.await(bothRead);
              }),
            ),
          ),
        );
        const results = yield* Effect.all(
          [
            nativeCall(node, parent, "update_scheduled_task", {
              purposeIntent: { version: 1 as const, intent: { type: "unscoped" as const } },
              scheduledTaskId: receipt.scheduledTaskId,
              title: "Title A",
              clientRequestId: "reviewer-rival-a",
            }),
            nativeCall(node, parent, "update_scheduled_task", {
              purposeIntent: { version: 1 as const, intent: { type: "unscoped" as const } },
              scheduledTaskId: receipt.scheduledTaskId,
              prompt: "Prompt B",
              clientRequestId: "reviewer-rival-b",
            }),
          ],
          { concurrency: "unbounded" },
        ).pipe(Effect.ensuring(Effect.sync(() => spy.mockRestore())));
        expect(
          results.every((r) => r.isError !== true),
          json(results),
        ).toBe(true);
        const current = (yield* realList()).tasks.find((t) => t.id === receipt.scheduledTaskId)!;
        expect({ title: current.title, prompt: current.prompt }).toEqual({
          title: "Title A",
          prompt: "Prompt B",
        });
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30000,
);

it.live(
  "runtime382-F1: remote scheduler validation leaves SQLite free for reverse control",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(descriptorOf("runtime382-a", "A", { peerAuthority: 1 }), "disk", {
          productionKernel: true,
          attachmentRequests: true,
        });
        const b = yield* makeNode(descriptorOf("runtime382-b", "B", { peerAuthority: 1 }), "disk", {
          productionKernel: true,
          attachmentRequests: true,
        });
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("runtime382-parent");
        yield* seedParent(a, parent);
        const input = {
          purposeIntent: { version: 1 as const, intent: { type: "unscoped" as const } },
          environmentId: b.environmentId,
          projectId: b.project.id,
          title: "Original title",
          prompt: "Disabled fixture",
          enabled: false,
          schedule: { type: "interval", everyMs: 3600000 },
          clientRequestId: "runtime382-create",
        };
        const created = yield* nativeCall(a, parent, "schedule_task", input);
        expect(created.isError, json(created)).not.toBe(true);
        const receipt = yield* decodeAutomationScheduleReceipt(created.structuredContent);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        let transactionalValidations = 0;
        const actual = b.authority.checkTarget;
        const observer = vi.spyOn(b.authority, "checkTarget").mockImplementation((...args) =>
          Effect.gen(function* () {
            if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
              transactionalValidations++;
            return yield* actual(...args);
          }),
        );
        const update = yield* nativeCall(a, parent, "update_scheduled_task", {
          purposeIntent: { version: 1 as const, intent: { type: "unscoped" as const } },
          environmentId: b.environmentId,
          scheduledTaskId: receipt.scheduledTaskId,
          title: "Later independent title",
          clientRequestId: "runtime382-update",
        }).pipe(Effect.ensuring(Effect.sync(() => observer.mockRestore())));
        expect.soft(transactionalValidations).toBe(0);
        expect(update.isError, json(update)).not.toBe(true);
        expect(
          (yield* b.scheduled.list()).tasks.find((t) => t.id === receipt.scheduledTaskId),
        ).toMatchObject({
          title: "Later independent title",
          enabled: false,
          runCount: 0,
          nextRunAt: null,
        });
        const replay = yield* nativeCall(a, parent, "schedule_task", input);
        expect(replay.structuredContent).toEqual(created.structuredContent);
        expect(
          (yield* b.scheduled.list()).tasks.find((t) => t.id === receipt.scheduledTaskId)?.title,
        ).toBe("Later independent title");
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  90000,
);

it.live(
  "toggle584: a retried keyed disable replays its receipt after a later enable",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(descriptorOf("toggle584-a", "A", { peerAuthority: 1 }), "disk", {
          productionKernel: true,
          attachmentRequests: true,
        });
        const b = yield* makeNode(descriptorOf("toggle584-b", "B", { peerAuthority: 1 }), "disk", {
          productionKernel: true,
          attachmentRequests: true,
        });
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("toggle584-parent");
        yield* seedParent(a, parent);
        const created = yield* nativeCall(a, parent, "schedule_task", {
          purposeIntent: { version: 1 as const, intent: { type: "unscoped" as const } },
          environmentId: b.environmentId,
          projectId: b.project.id,
          title: "Toggled",
          prompt: "Toggled fixture",
          schedule: { type: "interval", everyMs: 3600000 },
          clientRequestId: "toggle584-create",
        });
        expect(created.isError, json(created)).not.toBe(true);
        const { scheduledTaskId } = yield* decodeAutomationScheduleReceipt(
          created.structuredContent,
        );
        const toggle = (enabled: boolean, clientRequestId: string) =>
          nativeCall(a, parent, "update_scheduled_task", {
            environmentId: b.environmentId,
            scheduledTaskId,
            enabled,
            clientRequestId,
          });
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const certificate = sql<{
          id: string;
          disabled: number;
        }>`SELECT c.id,c.disabled FROM fork_automation_bindings b JOIN fork_automation_certificates c ON c.id=b.certificate_id WHERE b.kind='schedule' AND b.resource_id=${scheduledTaskId}`;
        const disable = yield* toggle(false, "toggle584-disable");
        expect(disable.isError, json(disable)).not.toBe(true);
        const enable = yield* toggle(true, "toggle584-enable");
        expect(enable.isError, json(enable)).not.toBe(true);
        const enabled = yield* certificate;
        expect(enabled).toEqual([{ id: expect.any(String), disabled: 0 }]);
        // A lost acknowledgement retries the same disable; it must not apply again.
        const retried = yield* toggle(false, "toggle584-disable");
        expect(retried.structuredContent).toEqual(disable.structuredContent);
        expect(yield* certificate).toEqual(enabled);
        expect(
          (yield* b.scheduled.list()).tasks.find((t) => t.id === scheduledTaskId)?.enabled,
        ).toBe(true);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  90000,
);

it.live(
  "runtime382-F1: remote keyed manual admission validates outside SQL and replays native acceptance",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("manual-runtime382-a", "A", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf("manual-runtime382-b", "B", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("manual-runtime382-source"),
          target = ThreadId.make("manual-runtime382-target");
        yield* seedParent(a, parent);
        yield* seedParent(b, target);
        const created = yield* nativeCall(b, target, "schedule_task", {
          purposeIntent: { version: 1 as const, intent: { type: "unscoped" as const } },
          title: "Manual target",
          prompt: "One queued message",
          bindToCurrentThread: true,
          enabled: false,
          schedule: { type: "interval", everyMs: 3600000 },
          clientRequestId: "manual-runtime382-create",
        });
        expect(created.isError, json(created)).not.toBe(true);
        const receipt = yield* decodeAutomationScheduleReceipt(created.structuredContent);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        yield* b.scheduled.setEnabled({ id: receipt.scheduledTaskId, enabled: true });
        let transactionalValidations = 0;
        const actual = b.authority.checkTarget;
        const observer = vi.spyOn(b.authority, "checkTarget").mockImplementation((...args) =>
          Effect.gen(function* () {
            if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
              transactionalValidations++;
            return yield* actual(...args);
          }),
        );
        const input = {
          environmentId: b.environmentId,
          taskId: receipt.scheduledTaskId,
          clientRequestId: "manual-runtime382-run",
        };
        const first = yield* nativeCall(a, parent, "run_scheduled_task_now", input);
        const again = yield* nativeCall(a, parent, "run_scheduled_task_now", input).pipe(
          Effect.ensuring(Effect.sync(() => observer.mockRestore())),
        );
        expect(first.isError, json(first)).not.toBe(true);
        expect(again.structuredContent).toEqual(first.structuredContent);
        expect(transactionalValidations).toBe(0);
        expect(
          (yield* b.scheduled.list()).tasks.find((t) => t.id === receipt.scheduledTaskId)?.runCount,
        ).toBe(1);
        expect(yield* sql`SELECT * FROM scheduled_manual_requests`).toHaveLength(1);
        const removed = yield* nativeCall(a, parent, "delete_scheduled_task", {
          environmentId: b.environmentId,
          scheduledTaskId: receipt.scheduledTaskId,
          clientRequestId: "manual-runtime382-delete",
        });
        expect(removed.isError, json(removed)).not.toBe(true);
        expect((yield* b.scheduled.list()).tasks).toHaveLength(0);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  90000,
);

it.live(
  "migration382-S2: each legacy metadata action is target-scoped and never becomes a fresh command",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const node = yield* makeNode(
          descriptorOf("migration382-metadata", "Metadata", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const parent = ThreadId.make("migration382-metadata-parent"),
          other = ThreadId.make("migration382-metadata-other");
        yield* seedParent(node, parent);
        yield* seedParent(node, other);
        const sql = Context.get(node.database, SqlClient.SqlClient);
        const pullRequest = {
          repository: "isolated/repository",
          number: 382,
          url: "https://github.com/isolated/repository/pull/382",
        };
        for (const action of [
          "rename",
          "regenerate_title",
          "link_pull_request",
          "unlink_pull_request",
        ] as const) {
          const key = `old:${action}:key`,
            commandId = CommandId.make(
              [
                "command",
                "mcp",
                "pre-upgrade-credential",
                "thread-update",
                encodeURIComponent(parent),
                action,
                encodeURIComponent(key),
              ].join(":"),
            );
          yield* node.threads.dispatch({
            type: "thread.metadata.update",
            commandId,
            threadId: parent,
            ...(action === "rename"
              ? { title: "Original title" }
              : action === "regenerate_title"
                ? { regenerateTitle: true }
                : {
                    linkedPullRequest:
                      action === "link_pull_request"
                        ? { projectId: node.project.id, ...pullRequest }
                        : null,
                  }),
          });
          const later = yield* nativeCall(node, parent, "t3_thread_update", {
            action: "rename",
            title: "Later independent title",
            clientRequestId: `later:${action}`,
          });
          expect(later.isError, json(later)).not.toBe(true);
          const before = yield* sql`SELECT command_id FROM orchestration_command_receipts`;
          const replay = yield* nativeCall(node, parent, "t3_thread_update", {
            action,
            ...(action === "rename"
              ? { title: "Original title" }
              : action === "link_pull_request"
                ? { pullRequest }
                : {}),
            clientRequestId: key,
          });
          expect(replay.isError).toBe(true);
          expect(json(replay)).toContain("legacy operation");
          expect(yield* sql`SELECT command_id FROM orchestration_command_receipts`).toEqual(before);
          expect((yield* node.threads.getThreadShell(parent))?.title).toBe(
            "Later independent title",
          );
          const unrelated = yield* nativeCall(node, other, "t3_thread_update", {
            action: "rename",
            title: "Independent other caller",
            clientRequestId: key,
          });
          expect(unrelated.isError, json(unrelated)).not.toBe(true);
        }
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30000,
);

it.live(
  "runtime382-F1: a local binding change during live authorization cannot cross the commit guard",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const node = yield* makeNode(
          descriptorOf("fence382", "Fence", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const parent = ThreadId.make("fence382-parent");
        yield* seedParent(node, parent);
        const created = yield* nativeCall(node, parent, "schedule_task", {
          purposeIntent: { version: 1 as const, intent: { type: "unscoped" as const } },
          title: "Original title",
          prompt: "Original prompt",
          enabled: false,
          schedule: { type: "interval", everyMs: 3600000 },
          clientRequestId: "fence382-create",
        });
        expect(created.isError, json(created)).not.toBe(true);
        const receipt = yield* decodeAutomationScheduleReceipt(created.structuredContent);
        const entered = yield* Deferred.make<void>(),
          release = yield* Deferred.make<void>();
        const actual = node.authority.checkTarget;
        const observer = vi.spyOn(node.authority, "checkTarget").mockImplementation((...args) =>
          Effect.gen(function* () {
            yield* actual(...args);
            if ((yield* ScheduleMutation.CurrentMutation) !== undefined) {
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(release);
            }
          }),
        );
        const pending = yield* nativeCall(node, parent, "update_scheduled_task", {
          purposeIntent: { version: 1 as const, intent: { type: "unscoped" as const } },
          scheduledTaskId: receipt.scheduledTaskId,
          title: "Must not overwrite",
          clientRequestId: "fence382-update",
        }).pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        const current = (yield* node.scheduled.list()).tasks.find(
          (t) => t.id === receipt.scheduledTaskId,
        )!;
        yield* node.scheduled.upsert({ ...current, runtimeMode: "approval-required" });
        yield* Deferred.succeed(release, undefined);
        const refused = yield* Fiber.join(pending).pipe(
          Effect.ensuring(Effect.sync(() => observer.mockRestore())),
        );
        expect(refused.isError, json(refused)).toBe(true);
        expect(json(refused)).toContain("binding changed");
        expect(
          (yield* node.scheduled.list()).tasks.find((t) => t.id === receipt.scheduledTaskId),
        ).toMatchObject({ title: "Original title", runtimeMode: "approval-required" });
        const sql = Context.get(node.database, SqlClient.SqlClient);
        expect(yield* sql`SELECT * FROM scheduled_mutation_receipts`).toHaveLength(1);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30000,
);

it.live(
  "Grok382-G1: owner-proved preview nondelivery leaves the same key retryable",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const node = yield* makeNode(
          descriptorOf("nondelivery382", "Nondelivery", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const parent = ThreadId.make("nondelivery382-parent");
        yield* seedParent(node, parent);
        let clicks = 0;
        const connect = (clientId: string) =>
          Effect.gen(function* () {
            const connected = yield* Deferred.make<void>();
            yield* Stream.runForEach(
              yield* node.browser.connect({ clientId, environmentId: node.environmentId }),
              (event) =>
                event.type === "connected"
                  ? Deferred.succeed(connected, undefined)
                  : Effect.gen(function* () {
                      if (event.request.operation === "click") clicks++;
                      yield* node.browser.respond({
                        clientId,
                        connectionId: event.connectionId,
                        requestId: event.request.requestId,
                        ok: true,
                        result:
                          event.request.operation === "status"
                            ? {
                                available: true,
                                visible: true,
                                tabId: null,
                                url: null,
                                title: null,
                                loading: false,
                              }
                            : {},
                      });
                    }),
            ).pipe(Effect.forkScoped);
            yield* Deferred.await(connected);
          });
        yield* connect("before-closure");
        const originalOffer = (yield* Effect.promise(() =>
          vi.importActual<typeof import("effect/Queue")>("effect/Queue"),
        )).offer;
        let closed = false;
        const closeFirstAction: typeof Queue.offer = dual(
          2,
          <A, E>(queue: Queue.Enqueue<A, E>, value: A) => {
            if (
              !closed &&
              typeof value === "object" &&
              value !== null &&
              "type" in value &&
              value.type === "request" &&
              "request" in value &&
              typeof value.request === "object" &&
              value.request !== null &&
              "operation" in value.request &&
              value.request.operation === "click"
            ) {
              closed = true;
              // Fault only the actual external delivery queue; do not fake its result.
              Queue.shutdownUnsafe(queue);
            }
            return originalOffer(queue, value);
          },
        );
        const delivery = vi.spyOn(Queue, "offer").mockImplementation(closeFirstAction);
        const input = {
          locator: "role=button[name='Send']",
          clientRequestId: "nondelivery382-click",
        };
        const failed = yield* nativeCall(node, parent, "preview_click", input).pipe(
          Effect.ensuring(Effect.sync(() => delivery.mockRestore())),
        );
        expect(closed).toBe(true);
        expect(failed.isError).toBe(true);
        expect(json(failed)).not.toContain("outcome_unknown");
        expect(clicks).toBe(0);
        const sql = Context.get(node.database, SqlClient.SqlClient);
        expect(
          yield* sql<{
            state: string;
            attempt_id: string | null;
          }>`SELECT state,attempt_id FROM mcp_request_receipts`,
        ).toEqual([{ state: "bound", attempt_id: null }]);
        const active = { ...node, ...(yield* node.restartSchedulerTransport) };
        yield* connect("after-closure");
        const accepted = yield* nativeCall(active, parent, "preview_click", input);
        expect(accepted.isError, json(accepted)).not.toBe(true);
        const replay = yield* nativeCall(active, parent, "preview_click", input);
        expect(replay.structuredContent).toEqual(accepted.structuredContent);
        expect(clicks).toBe(1);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30000,
);

it.live.each(["reserve", "discard"] as const)(
  "Grok382-G2: registered discard and native claim retain the %s winner",
  (winner) =>
    Effect.scoped(
      Effect.gen(function* () {
        const node = yield* makeNode(
          descriptorOf(`discard-race382-${winner}`, "Discard race", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const parent = ThreadId.make(`discard-race382-${winner}`);
        yield* seedParent(node, parent);
        const metadata = {
          type: "file" as const,
          name: "race.pdf",
          mimeType: "application/pdf",
          sizeBytes: 4,
        };
        const prepared = yield* nativeCall(node, parent, "t3_attachment_prepare_upload", {
          upload: metadata,
          digest: "9f64a747e1b97f131fabb6b447296c9b6f0201e79fb3c5356e6c77e89b6a806a",
          clientRequestId: "race382-upload",
        });
        expect(prepared.isError, json(prepared)).not.toBe(true);
        const issued = yield* Schema.decodeUnknownEffect(PeerAttachmentUploadResult)(
          prepared.structuredContent,
        );
        const http = yield* HttpClient.HttpClient;
        expect(
          (yield* http.execute(
            HttpClientRequest.post(`${node.url}${issued.relativeUrl}`).pipe(
              HttpClientRequest.setBody(HttpBody.uint8Array(new Uint8Array([1, 2, 3, 4]))),
            ),
          )).status,
        ).toBe(204);
        const attachment = { ...metadata, id: issued.attachmentId };
        const staging = Option.getOrThrow(
          yield* Effect.serviceOption(AttachmentStaging.AttachmentStaging).pipe(
            Effect.provide(Layer.succeedContext(node.requestContext)),
          ),
        );
        const entered = yield* Deferred.make<void>(),
          release = yield* Deferred.make<void>();
        const file = resolveAttachmentPathById({
          attachmentsDir: node.config.attachmentsDir,
          attachmentId: attachment.id,
        })!;
        const fs = yield* FileSystem.FileSystem;
        if (winner === "reserve") {
          const actual = staging.discard;
          const paused = vi
            .spyOn(staging, "discard")
            .mockImplementation((...args) =>
              Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Deferred.await(release)),
                Effect.andThen(actual(...args)),
              ),
            );
          const pending = yield* nativeCall(node, parent, "t3_attachment_discard", {
            attachmentId: attachment.id,
            clientRequestId: "race382-discard",
          }).pipe(Effect.forkScoped);
          yield* Deferred.await(entered);
          yield* staging.reserve(
            caller(node, parent),
            "race382-native-command",
            parent,
            attachment,
          );
          yield* Deferred.succeed(release, undefined);
          const refused = yield* Fiber.join(pending).pipe(
            Effect.ensuring(Effect.sync(() => paused.mockRestore())),
          );
          expect(refused.isError).toBe(true);
          expect(Array.from(yield* fs.readFile(file))).toEqual([1, 2, 3, 4]);
          const claimed = yield* AttachmentClaims.claimPendingAttachments({
            commandId: "race382-native-command",
            threadId: parent,
            attachments: [attachment],
          }).pipe(
            Effect.provideService(Invocation.McpInvocationContext, caller(node, parent)),
            Effect.provide(
              Layer.mergeAll(
                Layer.succeedContext(node.requestContext),
                Layer.succeed(Config.ServerConfig, node.config),
              ),
            ),
          );
          const deliveredFile = resolveAttachmentPathById({
            attachmentsDir: node.config.attachmentsDir,
            attachmentId: claimed.attachments[0]!.id,
          })!;
          expect(Array.from(yield* fs.readFile(deliveredFile))).toEqual([1, 2, 3, 4]);
          yield* node.threads.sendToThread({
            projectId: node.project.id,
            commandId: CommandId.make("race382-native-command"),
            threadId: parent,
            messageId: MessageId.make("race382-message"),
            text: "Reserved original bytes",
            attachments: claimed.attachments,
            mode: "queue",
            modelSelection: selection,
            createdBy: "agent",
            creationSource: "mcp",
          });
          const sql = Context.get(node.database, SqlClient.SqlClient);
          expect(
            yield* sql`SELECT status FROM orchestration_command_receipts WHERE command_id='race382-native-command'`,
          ).toEqual([{ status: "accepted" }]);
        } else {
          const actual = staging.bindConsumption;
          const paused = vi
            .spyOn(staging, "bindConsumption")
            .mockImplementation((...args) =>
              Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Deferred.await(release)),
                Effect.andThen(actual(...args)),
              ),
            );
          const pending = yield* nativeCall(node, parent, "t3_thread_send_attachments", {
            threadId: parent,
            message: "Must not accept discarded bytes",
            attachments: [attachment],
            clientRequestId: "race382-send",
          }).pipe(Effect.forkScoped);
          yield* Deferred.await(entered);
          const discarded = yield* nativeCall(node, parent, "t3_attachment_discard", {
            attachmentId: attachment.id,
            clientRequestId: "race382-discard",
          });
          expect(discarded.isError, json(discarded)).not.toBe(true);
          expect(Array.from(yield* fs.readFile(file))).toEqual([1, 2, 3, 4]);
          const proofSql = Context.get(node.database, SqlClient.SqlClient);
          expect(
            yield* proofSql`SELECT consumer FROM attachment_staging WHERE id=${attachment.id}`,
          ).toEqual([{ consumer: "discarded" }]);
          const reconstructed = { ...node, ...(yield* node.restartSchedulerTransport) };
          const replayDiscard = yield* nativeCall(reconstructed, parent, "t3_attachment_discard", {
            attachmentId: attachment.id,
            clientRequestId: "race382-discard",
          });
          expect(replayDiscard.isError, json(replayDiscard)).not.toBe(true);
          expect(replayDiscard.content).toEqual(discarded.content);
          const acceptance = yield* proofSql<{
            discard_receipt: string;
            discard_cleanup: string;
          }>`SELECT discard_receipt,discard_cleanup FROM attachment_staging WHERE id=${attachment.id}`;
          expect(acceptance[0]?.discard_receipt).toContain('"inputDigest"');
          expect(acceptance[0]?.discard_receipt).toContain('"generation":"local"');
          expect(acceptance[0]?.discard_cleanup).toBe("retained");
          expect(
            (yield* nativeCall(reconstructed, parent, "t3_attachment_discard", {
              attachmentId: attachment.id,
              clientRequestId: "race382-rival-discard",
            })).isError,
          ).toBe(true);
          expect(
            (yield* nativeCall(reconstructed, parent, "t3_attachment_discard", {
              attachmentId: "pending-unrelated.pdf",
              clientRequestId: "race382-discard",
            })).isError,
          ).toBe(true);
          const other = ThreadId.make("race382-other-caller");
          yield* seedParent(node, other);
          expect(
            (yield* nativeCall(reconstructed, other, "t3_attachment_discard", {
              attachmentId: attachment.id,
              clientRequestId: "race382-discard",
            })).isError,
          ).toBe(true);
          yield* Deferred.succeed(release, undefined);
          const refused = yield* Fiber.join(pending).pipe(
            Effect.ensuring(Effect.sync(() => paused.mockRestore())),
          );
          expect(refused.isError).toBe(true);
          expect(Array.from(yield* fs.readFile(file))).toEqual([1, 2, 3, 4]);
          expect(
            (yield* node.threads.getThreadRecords(parent, ["messages"])).messages.filter(
              (message) => message.text === "Must not accept discarded bytes",
            ),
          ).toHaveLength(0);
          // Historical acceptance does not delete independently replaced bytes.
          yield* fs.writeFile(file, new Uint8Array([9, 8, 7, 6]));
          expect(
            (yield* nativeCall(reconstructed, parent, "t3_attachment_discard", {
              attachmentId: attachment.id,
              clientRequestId: "race382-discard",
            })).isError,
          ).not.toBe(true);
          expect(Array.from(yield* fs.readFile(file))).toEqual([9, 8, 7, 6]);
        }
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30000,
);

it.live(
  "Grok382-G1: native device pure refusals retry, but selection after host preparation stays unknown",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const options = {
          productionKernel: true,
          attachmentRequests: true,
          preferencesOwner: true,
          deviceOwner: true,
        };
        const node = yield* makeNode(
          descriptorOf("device-preflight382", "Preflight", { peerAuthority: 1 }),
          "disk",
          options,
        );
        const parent = ThreadId.make("device-preflight382-parent");
        yield* seedParent(node, parent);
        const input = {
          deviceId: "fixture-device382",
          platform: "android",
          clientRequestId: "device-preflight382-open",
        };
        yield* node.fixtureSettings.updateSettings({ enableDeviceSupport: false });
        const disabled = yield* nativeCall(node, parent, "device_open", input);
        expect(disabled.isError).toBe(true);
        expect(json(disabled)).not.toContain("outcome_unknown");
        yield* node.fixtureSettings.updateSettings({ enableDeviceSupport: true });
        yield* Ref.set(node.devicePlatformAvailable, false);
        const unsupported = yield* nativeCall(node, parent, "device_open", input);
        expect(unsupported.isError).toBe(true);
        expect(json(unsupported)).not.toContain("outcome_unknown");
        expect(yield* Ref.get(node.deviceHostStarts)).toBe(0);
        expect(yield* Ref.get(node.deviceAgentStarts)).toBe(0);
        const sql = Context.get(node.database, SqlClient.SqlClient);
        expect(yield* sql`SELECT state FROM mcp_request_receipts`).toEqual([{ state: "bound" }]);
        yield* Ref.set(node.devicePlatformAvailable, true);
        const accepted = yield* nativeCall(node, parent, "device_open", input);
        expect(accepted.isError, json(accepted)).not.toBe(true);
        const count = yield* Ref.get(node.deviceHostStarts);
        expect((yield* nativeCall(node, parent, "device_open", input)).structuredContent).toEqual(
          accepted.structuredContent,
        );
        expect(yield* Ref.get(node.deviceHostStarts)).toBe(count);
        expect(yield* Ref.get(node.deviceAgentStarts)).toBe(1);
        const other = yield* makeNode(
          descriptorOf("device-selection382", "Selection", { peerAuthority: 1 }),
          "disk",
          options,
        );
        const otherParent = ThreadId.make("device-selection382-parent");
        yield* seedParent(other, otherParent);
        const missing = {
          deviceId: "not-discovered",
          platform: "android",
          clientRequestId: "device-selection382-open",
        };
        const unknown = yield* nativeCall(other, otherParent, "device_open", missing);
        expect(json(unknown)).toContain("outcome_unknown");
        const fs = yield* FileSystem.FileSystem;
        expect(
          yield* fs.readFileString(`${other.config.stateDir}/fixture-device-host-started`),
        ).toBe("Isolated native preparation footprint");
        const prepared = yield* Ref.get(other.deviceHostStarts);
        expect(prepared).toBeGreaterThan(0);
        expect(yield* Ref.get(other.deviceAgentStarts)).toBe(0);
        expect(json(yield* nativeCall(other, otherParent, "device_open", missing))).toContain(
          "outcome_unknown",
        );
        expect(yield* Ref.get(other.deviceHostStarts)).toBe(prepared);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30000,
);

it.live(
  "Grok382-G3: historical receipts survive deletion with current owner, generation and mode checks",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const options = { productionKernel: true, attachmentRequests: true };
        const a = yield* makeNode(
          descriptorOf("history-access382-a", "A", { peerAuthority: 1 }),
          "disk",
          options,
        );
        const b = yield* makeNode(
          descriptorOf("history-access382-b", "B", { peerAuthority: 1 }),
          "disk",
          options,
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("history-access382-parent"),
          other = ThreadId.make("history-access382-other");
        yield* seedParent(a, parent);
        yield* seedParent(a, other);
        const create = {
          purposeIntent: { version: 1 as const, intent: { type: "unscoped" as const } },
          environmentId: b.environmentId,
          projectId: b.project.id,
          title: "Original",
          prompt: "ISOLATED_G3_PRIVATE_PROMPT_382",
          enabled: false,
          schedule: { type: "interval" as const, everyMs: 3600000 },
          clientRequestId: "history-access382-create",
        };
        const first = yield* nativeCall(a, parent, "schedule_task", create);
        expect(first.isError, json(first)).not.toBe(true);
        const receipt = yield* decodeAutomationScheduleReceipt(first.structuredContent);
        const edit = {
          purposeIntent: { version: 1 as const, intent: { type: "unscoped" as const } },
          environmentId: b.environmentId,
          scheduledTaskId: receipt.scheduledTaskId,
          title: "Later",
          clientRequestId: "history-access382-edit",
        };
        const edited = yield* nativeCall(a, parent, "update_scheduled_task", edit);
        expect(edited.isError, json(edited)).not.toBe(true);
        const remove = {
          environmentId: b.environmentId,
          scheduledTaskId: receipt.scheduledTaskId,
          clientRequestId: "history-access382-delete",
        };
        const deleted = yield* nativeCall(a, parent, "delete_scheduled_task", remove);
        expect(deleted.isError, json(deleted)).not.toBe(true);
        const replay = () => nativeCall(a, parent, "schedule_task", create);
        expect((yield* replay()).structuredContent).toEqual(first.structuredContent);
        expect(
          (yield* nativeCall(a, parent, "update_scheduled_task", edit)).structuredContent,
        ).toEqual(edited.structuredContent);
        expect(
          (yield* nativeCall(a, parent, "delete_scheduled_task", remove)).structuredContent,
        ).toEqual(deleted.structuredContent);
        expect((yield* nativeCall(a, other, "update_scheduled_task", edit)).isError).toBe(true);
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("history-access382-narrow"),
          threadId: parent,
          runtimeMode: "auto",
        });
        expect((yield* replay()).isError).toBe(true);
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("history-access382-restore"),
          threadId: parent,
          runtimeMode: "full-access",
        });
        expect((yield* replay()).structuredContent).toEqual(first.structuredContent);
        // Native credentials always include orchestration. Exercise a genuinely
        // narrowed server-side forwarding scope rather than an ignored issue option.
        const deniedCapability = yield* Effect.result(
          a.forwarding.call(
            { ...caller(a, parent), capabilities: new Set(["preview"]) },
            OrchestratorToolkit.tools.schedule_task,
            b.environmentId,
            create,
          ),
        );
        expect(deniedCapability._tag).toBe("Failure");
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const evidence = yield* sql<{
          request_id: string;
          resource_json: string;
        }>`SELECT request_id,resource_json FROM scheduled_mutation_receipts`;
        expect(evidence).toHaveLength(3);
        expect(json(evidence)).not.toContain(create.prompt);
        expect((yield* b.scheduled.list()).tasks).toHaveLength(0);
        // An imported confirmation lacks historical proof; never backfill from absence or replacement.
        const original = evidence.find((row) => row.request_id === receipt.operationId)!;
        yield* sql`UPDATE scheduled_mutation_receipts SET resource_json=NULL WHERE request_id=${original.request_id}`;
        const unsupported = yield* replay();
        expect(unsupported.isError).toBe(true);
        expect(json(unsupported)).toContain("unsupported");
        yield* sql`UPDATE scheduled_mutation_receipts SET resource_json='{}' WHERE request_id=${original.request_id}`;
        expect((yield* replay()).isError).toBe(true);
        yield* sql`UPDATE scheduled_mutation_receipts SET resource_json=${original.resource_json} WHERE request_id=${original.request_id}`;
        expect((yield* replay()).structuredContent).toEqual(first.structuredContent);
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.auth.revokeSession(grant.sessionId);
        expect((yield* replay()).isError).toBe(true);
        yield* a.links.unlink(b.environmentId);
        yield* linkTo(a.links, b, "full-access");
        expect((yield* replay()).isError).toBe(true);
        expect(yield* sql`SELECT request_id FROM scheduled_mutation_receipts`).toHaveLength(3);
        expect((yield* b.scheduled.list()).tasks).toHaveLength(0);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  60000,
);

it.live.each(["single", "bulk", "replacement"] as const)(
  "Grok382-G3: deleted native acceptance retains every original downstream contributor, %s revocation",
  (revocation) =>
    Effect.scoped(
      Effect.gen(function* () {
        const options = { productionKernel: true, attachmentRequests: true };
        const a = yield* makeNode(
          descriptorOf("history-chain382-a", "A", { peerAuthority: 1 }),
          "disk",
          options,
        );
        const b = yield* makeNode(
          descriptorOf("history-chain382-b", "B", { peerAuthority: 1 }),
          "disk",
          options,
        );
        const c = yield* makeNode(
          descriptorOf("history-chain382-c", "C", { peerAuthority: 1 }),
          "disk",
          options,
        );
        yield* linkTo(a.links, b, "full-access");
        yield* linkTo(b.links, c, "full-access");
        const parent = ThreadId.make("history-chain382-parent");
        yield* seedParent(a, parent);
        yield* a.forwarding.call(
          caller(a, parent),
          ProjectToolkit.tools.t3_project_list,
          b.environmentId,
          {},
        );
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({
          ...grant,
          onward: true,
          resourceScope: "environment-resources",
        });
        const first = yield* delegate(a, b, parent, "history-chain382-a-b");
        yield* activate(b, first.childId);
        yield* b.forwarding.call(
          caller(b, first.childId),
          ProjectToolkit.tools.t3_project_list,
          c.environmentId,
          {},
        );
        const onwardGrant = (yield* c.authority.grants)[0]!;
        yield* c.authority.setGrant({ ...onwardGrant, resourceScope: "environment-resources" });
        const second = yield* delegate(b, c, first.childId, "history-chain382-b-c");
        yield* activate(c, second.childId);
        const create = {
          purposeIntent: { version: 1 as const, intent: { type: "unscoped" as const } },
          title: "Original downstream acceptance",
          prompt: "Isolated chain fixture",
          enabled: false,
          schedule: { type: "interval", everyMs: 3600000 },
          clientRequestId: "history-chain382-create",
        };
        const accepted = yield* nativeCall(c, second.childId, "schedule_task", create);
        expect(accepted.isError, json(accepted)).not.toBe(true);
        const receipt = yield* decodeAutomationScheduleReceipt(accepted.structuredContent);
        const removed = yield* nativeCall(c, second.childId, "delete_scheduled_task", {
          scheduledTaskId: receipt.scheduledTaskId,
          clientRequestId: "history-chain382-delete",
        });
        expect(removed.isError, json(removed)).not.toBe(true);
        expect(
          (yield* nativeCall(c, second.childId, "schedule_task", create)).structuredContent,
        ).toEqual(accepted.structuredContent);
        const sql = Context.get(c.database, SqlClient.SqlClient);
        const rows = yield* sql<{
          resource_json: string;
        }>`SELECT resource_json FROM scheduled_mutation_receipts`;
        expect(rows).toHaveLength(2);
        expect(rows.every((row) => row.resource_json.includes("authorityRecordId"))).toBe(true);
        const sourceSql = Context.get(b.database, SqlClient.SqlClient);
        yield* sourceSql`CREATE TABLE fixture_revoke_order_observed (session_id TEXT NOT NULL)`;
        yield* sourceSql`CREATE TRIGGER fixture_disable_before_revoke BEFORE UPDATE OF revoked_at ON auth_sessions
          WHEN OLD.revoked_at IS NULL AND NEW.revoked_at IS NOT NULL AND EXISTS (SELECT 1 FROM fork_automation_dependency_grants WHERE session_id=OLD.session_id)
          BEGIN
            SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM fork_automation_dependency_grants g JOIN fork_automation_obligations o ON o.dependency_id=g.dependency_id WHERE g.session_id=OLD.session_id AND o.pending=1 AND o.ack_json IS NULL)
              THEN RAISE(ABORT,'Disable obligation must precede credential retirement') END;
            INSERT INTO fixture_revoke_order_observed VALUES (OLD.session_id);
          END`;
        const repository = yield* AuthSessions.AuthSessionRepository.pipe(
          Effect.provide(AuthSessions.layer.pipe(Layer.provide(Layer.succeedContext(b.database)))),
        );
        const now = yield* DateTime.now;
        if (revocation === "single") yield* b.auth.revokeSession(grant.sessionId);
        else if (revocation === "bulk") {
          const keep = AuthSessionId.make("automation384-bulk-keeper");
          yield* repository.create({
            sessionId: keep,
            subject: "isolated-bulk-owner",
            method: "browser-session-cookie",
            scopes: ["orchestration:read", "orchestration:operate"],
            client: {
              label: "Isolated bulk owner",
              ipAddress: null,
              userAgent: null,
              deviceType: "unknown",
              os: null,
              browser: null,
            },
            issuedAt: now,
            expiresAt: DateTime.add(now, { hours: 1 }),
          });
          expect(
            yield* repository.revokeAllExcept({ currentSessionId: keep, revokedAt: now }),
          ).toContain(grant.sessionId);
        } else {
          const prior = Option.getOrThrow(
            yield* repository.getById({ sessionId: grant.sessionId }),
          );
          expect(
            yield* repository.createReplacingActive({
              session: {
                ...prior,
                sessionId: AuthSessionId.make("automation384-replacement"),
                issuedAt: now,
                expiresAt: DateTime.add(now, { hours: 1 }),
              },
              replaceSessionId: grant.sessionId,
              revokedAt: now,
            }),
          ).toContain(grant.sessionId);
        }
        expect(
          yield* sourceSql`SELECT * FROM fixture_revoke_order_observed WHERE session_id=${grant.sessionId}`,
        ).toHaveLength(1);
        expect((yield* nativeCall(c, second.childId, "schedule_task", create)).isError).toBe(true);
        expect((yield* c.scheduled.list()).tasks).toHaveLength(0);
        expect(yield* sql`SELECT request_id FROM scheduled_mutation_receipts`).toHaveLength(2);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  60000,
);

it.live(
  "reviewer382-r2: saved PNG replays through registered transport without its renderer",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const node = yield* makeNode(
          descriptorOf("reviewer382-r2-png", "PNG", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const parent = ThreadId.make("reviewer382-r2-png-parent");
        yield* seedParent(node, parent);
        const input = { save: true, includeImage: true, clientRequestId: "reviewer382-r2-save" };
        let screenshots = 0;
        const first = yield* Effect.scoped(
          Effect.gen(function* () {
            const ready = yield* Deferred.make<void>();
            yield* Stream.runForEach(
              yield* node.browser.connect({
                clientId: "reviewer382-r2-renderer",
                environmentId: node.environmentId,
              }),
              (event) =>
                event.type === "connected"
                  ? Deferred.succeed(ready, undefined)
                  : Effect.sync(() => {
                      screenshots++;
                    }).pipe(
                      Effect.andThen(
                        node.browser.respond({
                          clientId: "reviewer382-r2-renderer",
                          connectionId: event.connectionId,
                          requestId: event.request.requestId,
                          ok: true,
                          result: {
                            url: "https://isolated.example.test/",
                            title: "Fixture",
                            loading: false,
                            visibleText: "Fixture",
                            interactiveElements: [],
                            accessibilityTree: {},
                            consoleEntries: [],
                            networkEntries: [],
                            actionTimeline: [],
                            screenshot: {
                              mimeType: "image/png",
                              data: Buffer.from([112, 110, 103]).toString("base64"),
                              width: 1,
                              height: 1,
                            },
                          },
                        }),
                      ),
                    ),
            ).pipe(Effect.forkScoped);
            yield* Deferred.await(ready);
            return yield* nativeCall(node, parent, "preview_snapshot", input);
          }),
        );
        expect(first.isError, json(first)).not.toBe(true);
        const receipt = yield* decodeSavedPngReceipt(first.structuredContent);
        const fs = yield* FileSystem.FileSystem;
        expect(Array.from(yield* fs.readFile(receipt.artifact.screenshotPath))).toEqual([
          112, 110, 103,
        ]);
        const sql = Context.get(node.database, SqlClient.SqlClient);
        expect(
          yield* sql`SELECT state FROM mcp_request_receipts WHERE id=${receipt.operationId}`,
        ).toEqual([{ state: "completed" }]);
        const active = { ...node, ...(yield* node.restartSchedulerTransport) };
        const replay = yield* nativeCall(active, parent, "preview_snapshot", input);
        expect(replay.isError, json(replay)).not.toBe(true);
        expect(replay.structuredContent).toEqual(first.structuredContent);
        expect(replay.content.some((item) => item.type === "image")).toBe(true);
        expect(screenshots).toBe(1);
        const persisted = (yield* sql<{
          result_json: string;
        }>`SELECT result_json FROM mcp_request_receipts WHERE id=${receipt.operationId}`)[0]!;
        // A malformed imported binding is not a grant to read its stored path.
        yield* sql`UPDATE mcp_request_receipts SET result_json=json_set(result_json,'$.value.artifact.owner',${"f".repeat(64)}) WHERE id=${receipt.operationId}`;
        const unproved = yield* nativeCall(active, parent, "preview_snapshot", input);
        expect(unproved.isError).toBe(true);
        expect(unproved.content.some((item) => item.type === "image")).toBe(false);
        yield* sql`UPDATE mcp_request_receipts SET result_json=${persisted.result_json} WHERE id=${receipt.operationId}`;
        expect(
          (yield* nativeCall(active, parent, "preview_snapshot", input)).structuredContent,
        ).toEqual(first.structuredContent);
        const conflict = yield* nativeCall(active, parent, "preview_snapshot", {
          ...input,
          includeImage: false,
        });
        expect(conflict.isError).toBe(true);
        expect(json(conflict)).toContain("different input");
        yield* fs.writeFileString(receipt.artifact.screenshotPath, "Later independent bytes");
        const changed = yield* nativeCall(active, parent, "preview_snapshot", input);
        expect(changed.structuredContent).toEqual(first.structuredContent);
        expect(changed.content.some((item) => item.type === "image")).toBe(false);
        expect(yield* fs.readFileString(receipt.artifact.screenshotPath)).toBe(
          "Later independent bytes",
        );
        yield* fs.remove(receipt.artifact.screenshotPath);
        const missing = yield* nativeCall(active, parent, "preview_snapshot", input);
        expect(missing.structuredContent).toEqual(first.structuredContent);
        expect(missing.content.some((item) => item.type === "image")).toBe(false);
        expect(yield* fs.exists(receipt.artifact.screenshotPath)).toBe(false);
        const narrowed = yield* nativeCall(
          active,
          parent,
          "preview_snapshot",
          input,
          new Set(["orchestration"]),
        );
        expect(narrowed.isError).toBe(true);
        expect(json(narrowed)).not.toContain(receipt.artifact.screenshotPath);
        expect(screenshots).toBe(1);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30000,
);

it.live(
  "reviewer382-r2: manual admission rechecks a replacement schedule target",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const options = { productionKernel: true, attachmentRequests: true };
        const a = yield* makeNode(
          descriptorOf("reviewer382-r2-ma", "A", { peerAuthority: 1 }),
          "disk",
          options,
        );
        const b = yield* makeNode(
          descriptorOf("reviewer382-r2-mb", "B", { peerAuthority: 1 }),
          "disk",
          options,
        );
        const c = yield* makeNode(
          descriptorOf("reviewer382-r2-mc", "C", { peerAuthority: 1 }),
          "disk",
          options,
        );
        yield* linkTo(a.links, b, "full-access");
        yield* linkTo(c.links, b, "full-access");
        const parent = ThreadId.make("reviewer382-r2-ma-parent"),
          other = ThreadId.make("reviewer382-r2-mc-parent"),
          local = ThreadId.make("reviewer382-r2-mb-parent");
        yield* seedParent(a, parent);
        yield* seedParent(c, other);
        yield* seedParent(b, local);
        const fields = {
          purposeIntent: { version: 1 as const, intent: { type: "unscoped" as const } },
          title: "Original",
          prompt: "Do nothing",
          enabled: false,
          schedule: { type: "interval", everyMs: 3600000 },
        };
        const created = yield* nativeCall(b, local, "schedule_task", {
          ...fields,
          bindToCurrentThread: false,
          clientRequestId: "reviewer382-r2-local-create",
        });
        expect(created.isError, json(created)).not.toBe(true);
        const receipt = yield* decodeAutomationScheduleReceipt(created.structuredContent);
        const foreign = yield* nativeCall(c, other, "schedule_task", {
          ...fields,
          environmentId: b.environmentId,
          projectId: b.project.id,
          clientRequestId: "reviewer382-r2-foreign-create",
        });
        expect(foreign.isError, json(foreign)).not.toBe(true);
        const foreignReceipt = yield* decodeAutomationScheduleReceipt(foreign.structuredContent);
        const replacement = (yield* b.scheduled.list()).tasks.find(
          (t) => t.id === foreignReceipt.scheduledTaskId,
        )!;
        expect(replacement.linkOrigin).toBeDefined();
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const acceptedBefore =
          yield* sql`SELECT command_id FROM orchestration_command_receipts ORDER BY command_id`;
        const entered = yield* Deferred.make<void>(),
          release = yield* Deferred.make<void>();
        const actual = b.scheduled.runNow;
        const observer = vi
          .spyOn(b.scheduled, "runNow")
          .mockImplementation((...args: Parameters<typeof actual>) =>
            Effect.gen(function* () {
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(release);
              return yield* actual(...args);
            }),
          );
        const pending = yield* nativeCall(a, parent, "run_scheduled_task_now", {
          environmentId: b.environmentId,
          taskId: receipt.scheduledTaskId,
          clientRequestId: "reviewer382-r2-manual",
        }).pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        yield* b.scheduled.delete({ id: receipt.scheduledTaskId });
        yield* b.scheduled.upsert({ ...replacement, id: receipt.scheduledTaskId });
        yield* Deferred.succeed(release, undefined);
        const result = yield* Fiber.join(pending).pipe(
          Effect.ensuring(Effect.sync(() => observer.mockRestore())),
        );
        expect(result.isError, json(result)).toBe(true);
        expect(
          (yield* b.scheduled.list()).tasks.find((t) => t.id === receipt.scheduledTaskId)?.runCount,
        ).toBe(0);
        expect(
          yield* sql`SELECT command_id FROM orchestration_command_receipts ORDER BY command_id`,
        ).toEqual(acceptedBefore);
        expect(yield* sql`SELECT request_id FROM scheduled_manual_requests`).toHaveLength(0);
        const fresh = yield* nativeCall(a, parent, "run_scheduled_task_now", {
          environmentId: b.environmentId,
          taskId: receipt.scheduledTaskId,
          clientRequestId: "reviewer382-r2-fresh-manual",
        });
        expect(fresh.isError).toBe(true);
        expect(json(fresh)).toContain("resource scope");
        expect(yield* sql`SELECT request_id FROM scheduled_manual_requests`).toHaveLength(0);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30000,
);

it.live(
  "manual382-S4: replacement after live target validation cannot cross the native fence",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const options = { productionKernel: true, attachmentRequests: true };
        const a = yield* makeNode(
          descriptorOf("reviewer382-r2-ma", "A", { peerAuthority: 1 }),
          "disk",
          options,
        );
        const b = yield* makeNode(
          descriptorOf("reviewer382-r2-mb", "B", { peerAuthority: 1 }),
          "disk",
          options,
        );
        const c = yield* makeNode(
          descriptorOf("reviewer382-r2-mc", "C", { peerAuthority: 1 }),
          "disk",
          options,
        );
        yield* linkTo(a.links, b, "full-access");
        yield* linkTo(c.links, b, "full-access");
        const parent = ThreadId.make("reviewer382-r2-ma-parent"),
          other = ThreadId.make("reviewer382-r2-mc-parent"),
          local = ThreadId.make("reviewer382-r2-mb-parent");
        yield* seedParent(a, parent);
        yield* seedParent(c, other);
        yield* seedParent(b, local);
        const fields = {
          purposeIntent: { version: 1 as const, intent: { type: "unscoped" as const } },
          title: "Original",
          prompt: "Do nothing",
          enabled: false,
          schedule: { type: "interval", everyMs: 3600000 },
        };
        const created = yield* nativeCall(b, local, "schedule_task", {
          ...fields,
          bindToCurrentThread: false,
          clientRequestId: "reviewer382-r2-local-create",
        });
        expect(created.isError, json(created)).not.toBe(true);
        const receipt = yield* decodeAutomationScheduleReceipt(created.structuredContent);
        const foreign = yield* nativeCall(c, other, "schedule_task", {
          ...fields,
          environmentId: b.environmentId,
          projectId: b.project.id,
          clientRequestId: "reviewer382-r2-foreign-create",
        });
        expect(foreign.isError, json(foreign)).not.toBe(true);
        const foreignReceipt = yield* decodeAutomationScheduleReceipt(foreign.structuredContent);
        const replacement = (yield* b.scheduled.list()).tasks.find(
          (t) => t.id === foreignReceipt.scheduledTaskId,
        )!;
        expect(replacement.linkOrigin).toBeDefined();
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const acceptedBefore =
          yield* sql`SELECT command_id FROM orchestration_command_receipts ORDER BY command_id`;
        const entered = yield* Deferred.make<void>(),
          release = yield* Deferred.make<void>();
        const actual = b.authority.checkTarget;
        let targetChecks = 0;
        const observer = vi
          .spyOn(b.authority, "checkTarget")
          .mockImplementation((...args: Parameters<typeof actual>) =>
            Effect.gen(function* () {
              yield* actual(...args);
              if (
                args[1].type === "schedule" &&
                args[1].taskId === receipt.scheduledTaskId &&
                ++targetChecks === 2
              ) {
                yield* Deferred.succeed(entered, undefined);
                yield* Deferred.await(release);
              }
            }),
          );
        const pending = yield* nativeCall(a, parent, "run_scheduled_task_now", {
          environmentId: b.environmentId,
          taskId: receipt.scheduledTaskId,
          clientRequestId: "reviewer382-r2-manual",
        }).pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        yield* b.scheduled.delete({ id: receipt.scheduledTaskId });
        yield* b.scheduled.upsert({ ...replacement, id: receipt.scheduledTaskId });
        yield* Deferred.succeed(release, undefined);
        const result = yield* Fiber.join(pending).pipe(
          Effect.ensuring(Effect.sync(() => observer.mockRestore())),
        );
        expect(result.isError, json(result)).toBe(true);
        expect(json(result)).toContain("binding changed");
        expect(
          (yield* b.scheduled.list()).tasks.find((t) => t.id === receipt.scheduledTaskId)?.runCount,
        ).toBe(0);
        expect(
          yield* sql`SELECT command_id FROM orchestration_command_receipts ORDER BY command_id`,
        ).toEqual(acceptedBefore);
        expect(yield* sql`SELECT request_id FROM scheduled_manual_requests`).toHaveLength(0);
        const fresh = yield* nativeCall(a, parent, "run_scheduled_task_now", {
          environmentId: b.environmentId,
          taskId: receipt.scheduledTaskId,
          clientRequestId: "reviewer382-r2-fresh-manual",
        });
        expect(fresh.isError).toBe(true);
        expect(json(fresh)).toContain("resource scope");
        expect(yield* sql`SELECT request_id FROM scheduled_manual_requests`).toHaveLength(0);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30000,
);

it.live(
  "png382-authority: renderer-free remote replay retains its original source contributor",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const options = { productionKernel: true, attachmentRequests: true };
        const c = yield* makeNode(
          descriptorOf("png-chain382-c", "C", { peerAuthority: 1 }),
          "disk",
          options,
        );
        const a = yield* makeNode(
          descriptorOf("png-chain382-a", "A", { peerAuthority: 1 }),
          "disk",
          options,
        );
        const b = yield* makeNode(
          descriptorOf("png-chain382-b", "B", { peerAuthority: 1 }),
          "disk",
          options,
        );
        yield* linkTo(c.links, a, "full-access");
        yield* linkTo(a.links, b, "full-access");
        const root = ThreadId.make("png-chain382-root");
        yield* seedParent(c, root);
        yield* c.forwarding.call(
          caller(c, root),
          ProjectToolkit.tools.t3_project_list,
          a.environmentId,
          {},
        );
        const sourceGrant = (yield* a.authority.grants)[0]!;
        yield* a.authority.setGrant({
          ...sourceGrant,
          preview: true,
          onward: true,
          resourceScope: "environment-resources",
        });
        const first = yield* nativeCall(c, root, "delegate_task", {
          task: "Isolated PNG source",
          target: {
            environmentId: a.environmentId,
            projectId: a.project.id,
            providerInstanceId: instanceId,
            model: "gpt-5.4",
          },
          mode: "async",
          clientRequestId: "png-chain382-first",
        });
        expect(first.isError, json(first)).not.toBe(true);
        const parent = yield* attachedChild(
          c,
          root,
          (yield* decodeTaskReceipt(first.structuredContent)).taskId,
        );
        yield* activate(a, parent);
        yield* a.forwarding.call(
          caller(a, parent),
          ProjectToolkit.tools.t3_project_list,
          b.environmentId,
          {},
        );
        const destinationGrant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({
          ...destinationGrant,
          preview: true,
          resourceScope: "environment-resources",
        });
        const second = yield* nativeCall(a, parent, "delegate_task", {
          task: "Isolated PNG context",
          target: {
            environmentId: b.environmentId,
            projectId: b.project.id,
            providerInstanceId: instanceId,
            model: "gpt-5.4",
          },
          mode: "async",
          clientRequestId: "png-chain382-second",
        });
        expect(second.isError, json(second)).not.toBe(true);
        const context = yield* attachedChild(
          a,
          parent,
          (yield* decodeTaskReceipt(second.structuredContent)).taskId,
        );
        yield* activate(b, context);
        const input = {
          environmentId: b.environmentId,
          contextThreadId: context,
          save: true,
          includeImage: true,
          clientRequestId: "png-chain382-save",
        };
        let snapshots = 0;
        const saved = yield* Effect.scoped(
          Effect.gen(function* () {
            const connected = yield* Deferred.make<void>();
            yield* Stream.runForEach(
              yield* b.browser.connect({
                clientId: "png-chain382-renderer",
                environmentId: b.environmentId,
              }),
              (event) =>
                event.type === "connected"
                  ? Deferred.succeed(connected, undefined)
                  : Effect.gen(function* () {
                      snapshots++;
                      yield* b.browser.respond({
                        clientId: "png-chain382-renderer",
                        connectionId: event.connectionId,
                        requestId: event.request.requestId,
                        ok: true,
                        result: {
                          url: "https://isolated-chain.example.test/",
                          title: "Fixture",
                          loading: false,
                          visibleText: "Isolated fixture",
                          interactiveElements: [],
                          accessibilityTree: {},
                          consoleEntries: [],
                          networkEntries: [],
                          actionTimeline: [],
                          screenshot: {
                            mimeType: "image/png",
                            data: Buffer.from([112, 110, 103]).toString("base64"),
                            width: 1,
                            height: 1,
                          },
                        },
                      });
                    }),
            ).pipe(Effect.forkScoped);
            yield* Deferred.await(connected);
            return yield* nativeCall(a, parent, "preview_snapshot", input);
          }),
        );
        expect(saved.isError, json(saved)).not.toBe(true);
        const receipt = yield* decodeSavedPngReceipt(saved.structuredContent);
        expect((yield* nativeCall(a, parent, "preview_snapshot", input)).structuredContent).toEqual(
          receipt,
        );
        yield* a.auth.revokeSession(sourceGrant.sessionId);
        const denied = yield* nativeCall(a, parent, "preview_snapshot", input);
        expect(denied.isError).toBe(true);
        expect(denied.content.some((item) => item.type === "image")).toBe(false);
        expect(snapshots).toBe(1);
        const fs = yield* FileSystem.FileSystem;
        expect(Array.from(yield* fs.readFile(receipt.artifact.screenshotPath))).toEqual([
          112, 110, 103,
        ]);
        expect(yield* fs.readDirectory(a.config.browserArtifactsDir)).toHaveLength(1);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  60000,
);

it.live(
  "png382-authority: absent destination registration and unauthenticated contexts fail before rendering",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("png-minimal382-a", "A", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf("png-minimal382-b", "B", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("png-minimal382-parent"),
          context = ThreadId.make("png-minimal382-context");
        yield* seedParent(a, parent);
        yield* seedParent(b, context);
        const unsupported = yield* nativeCall(a, parent, "preview_snapshot", {
          environmentId: b.environmentId,
          contextThreadId: context,
          save: true,
          clientRequestId: "png-minimal382-save",
        });
        expect(unsupported.isError).toBe(true);
        expect(json(unsupported)).toContain("does not support saved-preview authorization");
        const fs = yield* FileSystem.FileSystem;
        expect(yield* fs.exists(a.config.browserArtifactsDir)).toBe(false);
        const http = yield* HttpClient.HttpClient;
        expect(
          (yield* http.execute(HttpClientRequest.get(`${a.url}/mcp/peer-control/saved-preview`)))
            .status,
        ).toBe(401);
        const credential = yield* a.registry.issue({
          threadId: parent,
          providerInstanceId: instanceId,
        });
        const body = {
          version: 1,
          purpose: "saved-preview-authorization",
          environmentId: a.environmentId,
          generation: "f".repeat(64),
          contextThreadId: parent,
          operationId: "a".repeat(64),
        };
        const refused = yield* http.execute(
          HttpClientRequest.post(`${a.url}/mcp/peer-control/saved-preview`).pipe(
            HttpClientRequest.setHeader("authorization", credential.config.authorizationHeader),
            HttpClientRequest.setBody(HttpBody.text(json(body), "application/json")),
          ),
        );
        expect(refused.status).toBe(403);
        expect(yield* refused.json).toEqual({
          code: "capability_denied",
          message:
            "Saved-preview authorization needs its authenticated original destination generation.",
        });
        const malformedEnvelope = yield* http.execute(
          HttpClientRequest.post(`${a.url}/mcp/peer-control/saved-preview`).pipe(
            HttpClientRequest.setHeader("authorization", credential.config.authorizationHeader),
            HttpClientRequest.setHeader(PeerAuthority.AUTHORITY_HEADER, "{"),
            HttpClientRequest.setBody(HttpBody.text(json(body), "application/json")),
          ),
        );
        expect(malformedEnvelope.status).toBe(403);
        expect(yield* malformedEnvelope.json).toEqual({
          code: "invalid_request",
          message: "Invalid peer authority envelope.",
        });
        const malformedInput = yield* http.execute(
          HttpClientRequest.post(`${a.url}/mcp/peer-control/saved-preview`).pipe(
            HttpClientRequest.setHeader("authorization", credential.config.authorizationHeader),
            HttpClientRequest.setBody(HttpBody.text("{}", "application/json")),
          ),
        );
        expect(malformedInput.status).toBe(403);
        expect(yield* malformedInput.json).toEqual({
          code: "invalid_request",
          message: "Invalid saved-preview authorization context.",
        });
        expect(yield* fs.exists(a.config.browserArtifactsDir)).toBe(false);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30000,
);

it.live(
  "S6: remote discard recovers lost native acceptance after renewal/reconstruction and retains current authority",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const options = { productionKernel: true, attachmentRequests: true };
        const a = yield* makeNode(
          descriptorOf("discard-s6-a", "A", { peerAuthority: 1 }),
          "disk",
          options,
        );
        const b = yield* makeNode(
          descriptorOf("discard-s6-b", "B", { peerAuthority: 1 }),
          "disk",
          options,
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("discard-s6-parent");
        yield* seedParent(a, parent);
        const metadata = {
          type: "file",
          name: "discard.pdf",
          mimeType: "application/pdf",
          sizeBytes: 4,
        };
        const prepared = yield* nativeCall(a, parent, "t3_attachment_prepare_upload", {
          environmentId: b.environmentId,
          upload: metadata,
          digest: "9f64a747e1b97f131fabb6b447296c9b6f0201e79fb3c5356e6c77e89b6a806a",
          clientRequestId: "discard-s6-upload",
        });
        expect(prepared.isError, json(prepared)).not.toBe(true);
        const issued = yield* decodeDiscardUpload(prepared.structuredContent);
        const http = yield* HttpClient.HttpClient;
        expect(
          (yield* http.execute(
            HttpClientRequest.post(`${b.url}${issued.relativeUrl}`).pipe(
              HttpClientRequest.setBody(HttpBody.uint8Array(new Uint8Array([1, 2, 3, 4]))),
            ),
          )).status,
        ).toBe(204);
        const input = {
          environmentId: b.environmentId,
          attachmentId: issued.attachmentId,
          clientRequestId: "discard-s6-accept",
        };
        const fs = yield* FileSystem.FileSystem;
        const path = resolveAttachmentPathById({
          attachmentsDir: b.config.attachmentsDir,
          attachmentId: issued.attachmentId,
        })!;
        expect(Array.from(yield* fs.readFile(path))).toEqual([1, 2, 3, 4]);
        yield* Ref.set(a.dropCommittedToolReply, "t3_attachment_discard");
        const lost = yield* nativeCall(a, parent, "t3_attachment_discard", input);
        expect(lost.isError).toBe(true);
        expect(json(lost)).toContain("Isolated committed tool reply loss");
        expect(Array.from(yield* fs.readFile(path))).toEqual([1, 2, 3, 4]);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const accepted =
          yield* sql`SELECT consumer,discard_receipt,discard_cleanup FROM attachment_staging WHERE id=${issued.attachmentId}`;
        expect(accepted).toMatchObject([{ consumer: "discarded", discard_cleanup: "retained" }]);
        const rebuilt = { ...b, ...(yield* b.restartSchedulerTransport) };
        yield* Context.get(
          a.database,
          SqlClient.SqlClient,
        )`UPDATE peer_environment_links SET urls_json=${json([rebuilt.url])} WHERE environment_id=${b.environmentId}`;
        const recovered = yield* nativeCall(a, parent, "t3_attachment_discard", input);
        expect(recovered.isError, json(recovered)).not.toBe(true);
        expect(recovered.structuredContent).toEqual({});
        expect(
          yield* sql`SELECT consumer,discard_receipt,discard_cleanup FROM attachment_staging WHERE id=${issued.attachmentId}`,
        ).toEqual(accepted);
        expect(Array.from(yield* fs.readFile(path))).toEqual([1, 2, 3, 4]);
        const rival = ThreadId.make("discard-s6-rival");
        yield* seedParent(a, rival);
        expect((yield* nativeCall(a, rival, "t3_attachment_discard", input)).isError).toBe(true);
        expect(
          (yield* nativeCall(a, parent, "t3_attachment_discard", {
            ...input,
            attachmentId: "pending-unrelated.pdf",
          })).isError,
        ).toBe(true);
        yield* fs.writeFile(path, new Uint8Array([9, 8, 7, 6]));
        expect((yield* nativeCall(a, parent, "t3_attachment_discard", input)).isError).not.toBe(
          true,
        );
        expect(Array.from(yield* fs.readFile(path))).toEqual([9, 8, 7, 6]);
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.auth.revokeSession(grant.sessionId);
        expect((yield* nativeCall(a, parent, "t3_attachment_discard", input)).isError).toBe(true);
        yield* a.links.unlink(b.environmentId);
        yield* linkTo(a.links, rebuilt, "full-access");
        const enrolled = yield* nativeCall(a, parent, "t3_project_list", {
          environmentId: b.environmentId,
        });
        expect(enrolled.isError, json(enrolled)).not.toBe(true);
        expect((yield* nativeCall(a, parent, "t3_attachment_discard", input)).isError).toBe(true);
        expect(Array.from(yield* fs.readFile(path))).toEqual([9, 8, 7, 6]);
        expect(
          yield* sql`SELECT consumer,discard_receipt,discard_cleanup FROM attachment_staging WHERE id=${issued.attachmentId}`,
        ).toEqual(accepted);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30000,
);

it.live(
  "S8: registered local keyed replay and deliberate unkeyed stops preserve native recording evidence",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const node = yield* makeNode(
          descriptorOf("recording382-local", "Local", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const parent = ThreadId.make("recording382-local-parent");
        yield* seedParent(node, parent);
        const bytes = new Uint8Array([26, 69, 223, 163, 129, 1, 66, 134, 129, 1]);
        const uploaded = yield* AttachmentUpload.issueAttachmentUploadUrl({
          type: "file",
          name: "recording.webm",
          mimeType: "video/webm",
          sizeBytes: bytes.byteLength,
        }).pipe(
          Effect.provideService(Config.ServerConfig, node.config),
          Effect.provideService(
            Secrets.ServerSecretStore,
            Context.get(node.services, Secrets.ServerSecretStore),
          ),
        );
        const http = yield* HttpClient.HttpClient;
        const post = http.execute(
          HttpClientRequest.post(`${node.url}${uploaded.relativeUrl}`).pipe(
            HttpClientRequest.setBody(HttpBody.uint8Array(bytes)),
          ),
        );
        expect((yield* post).status).toBe(204);
        const ready = yield* Deferred.make<void>();
        let rendererCalls = 0;
        yield* Stream.runForEach(
          yield* node.browser.connect({
            clientId: "recording382-local-renderer",
            environmentId: node.environmentId,
          }),
          (event) =>
            event.type === "connected"
              ? Deferred.succeed(ready, undefined)
              : Effect.gen(function* () {
                  rendererCalls++;
                  expect(event.request.operation).toBe("recordingStop");
                  yield* node.browser.respond({
                    clientId: "recording382-local-renderer",
                    connectionId: event.connectionId,
                    requestId: event.request.requestId,
                    ok: true,
                    result: {
                      id: "local-renderer-recording",
                      tabId: "local-recording-tab",
                      path: "/isolated-renderer/local.webm",
                      uploadedAttachmentId: uploaded.attachmentId,
                      mimeType: "video/webm",
                      sizeBytes: bytes.byteLength,
                      createdAt: "2026-10-18T00:00:00Z",
                    },
                  });
                }),
        ).pipe(Effect.forkScoped);
        yield* Deferred.await(ready);
        const input = { clientRequestId: "recording382-local-keyed" };
        const first = yield* nativeCall(node, parent, "preview_recording_stop", input);
        expect(first.isError, json(first)).not.toBe(true);
        const result = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ id: Schema.String, path: Schema.String }),
        )(first.structuredContent);
        const fs = yield* FileSystem.FileSystem;
        expect(Array.from(yield* fs.readFile(result.path))).toEqual(Array.from(bytes));
        yield* fs.writeFile(result.path, new Uint8Array([9, 8, 7, 6, 5, 4, 3, 2, 1, 0]));
        expect((yield* post).status).toBe(204);
        const rebuilt = { ...node, ...(yield* node.restartSchedulerTransport) };
        const replay = yield* nativeCall(rebuilt, parent, "preview_recording_stop", input);
        expect(replay.isError, json(replay)).not.toBe(true);
        expect(replay.structuredContent).toEqual(first.structuredContent);
        expect(rendererCalls).toBe(1);
        const unkeyed = yield* nativeCall(rebuilt, parent, "preview_recording_stop", {});
        expect(unkeyed.isError, json(unkeyed)).not.toBe(true);
        expect(unkeyed.structuredContent).toEqual(first.structuredContent);
        expect(rendererCalls).toBe(2); // A deliberate unkeyed action still reaches its renderer.
        expect(Array.from(yield* fs.readFile(result.path))).toEqual([9, 8, 7, 6, 5, 4, 3, 2, 1, 0]);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30000,
);
