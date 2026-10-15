import * as BareLaunch from "./BareLaunch.ts";
import type { SDKMessage, SDKUserMessage } from "@anthropic-ai/claude-agent-sdk";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  ClaudeSettings,
  CommandId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  ScheduledTaskUpsertInput,
  ThreadId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Crypto from "effect/Crypto";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { expect } from "vite-plus/test";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as ManagedProjectFolders from "../project/ManagedProjectFolders.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as StreamClock from "../prism/streamClock.ts";
import * as ClaudeAdapterV2 from "./Adapters/ClaudeAdapterV2.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as EventSink from "./EventSink.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as LegacyV1ThreadImporter from "./legacy/LegacyV1ThreadImporter.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ThreadLaunchService from "./ThreadLaunchService.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";

const sessionId = "automatic-delivery-session";
const settings = Schema.decodeSync(ClaudeSettings)({});
const decodeScheduledTask = Schema.decodeEffect(ScheduledTaskUpsertInput);
const refusal =
  "The user doesn't want to take this action right now. STOP what you are doing and wait for the user to tell you how to proceed.";
const statusIds = ["status-1", "status-2", "status-3", "status-4"];
const modelSelection = {
  instanceId: ProviderInstanceId.make("claudeAgent"),
  model: "claude-sonnet-4-6",
};

// Native frames match the captured Bash/status batch and tool_result_meta shape.
// The external SDK boundary is replayed; the adapter and durable dispatch are real.
function frame(value: unknown): SDKMessage {
  return value as SDKMessage;
}

const batch = frame({
  type: "assistant",
  uuid: "batch",
  session_id: sessionId,
  parent_tool_use_id: null,
  message: {
    id: "tool-batch",
    role: "assistant",
    type: "message",
    model: modelSelection.model,
    content: [
      { type: "tool_use", id: "build", name: "Bash", input: { command: "make" } },
      ...statusIds.map((id) => ({
        type: "tool_use",
        id,
        name: "mcp__t3-code__task_status",
        input: { taskId: id },
      })),
    ],
    stop_reason: "tool_use",
    stop_sequence: null,
    usage: { input_tokens: 1, output_tokens: 1 },
  },
});

const toolResult = (id: string, cancelled: boolean) =>
  frame({
    type: "user",
    uuid: `result-${id}`,
    session_id: sessionId,
    parent_tool_use_id: null,
    message: {
      role: "user",
      content: [
        {
          type: "tool_result",
          tool_use_id: id,
          is_error: cancelled,
          content: cancelled ? refusal : id === "build" ? "build OK" : "Task completed normally",
        },
      ],
    },
    ...(cancelled ? { tool_result_meta: [{ id, non_execution_kind: "cancelled" }] } : {}),
  });

const result = (uuid: string, aborted: boolean) =>
  frame({
    type: "result",
    subtype: "success",
    uuid,
    session_id: sessionId,
    is_error: false,
    num_turns: 1,
    result: "",
    stop_reason: "end_turn",
    terminal_reason: aborted ? "aborted_tools" : "completed",
    permission_denials: [],
    duration_ms: 1,
    duration_api_ms: 1,
    total_cost_usd: 0,
    usage: { input_tokens: 1, output_tokens: 1 },
    modelUsage: {},
  });

it.effect.each(["active", "completed-before-effect", "completed-before-effect-selection"] as const)(
  "detaching a reused Claude query releases its completed run's native child subscription (%s)",
  (cutIn) =>
    Effect.scoped(
      Effect.gen(function* () {
        const cwd = yield* checkpointWorkspace("claude-earlier-run-detach");
        const sdkMessages = yield* Queue.unbounded<SDKMessage>();
        const offers: SDKUserMessage[] = [];
        const nativeQueue: SDKUserMessage[] = [];
        // @effect-diagnostics-next-line abortControllerInEffect:off - The fake SDK interrupts its batch independently of the test fiber.
        const batchAbort = new AbortController();
        const processedMessages = new WeakMap<SDKMessage, Deferred.Deferred<void>>();
        const offerAndWait = Effect.fnUntraced(function* (message: SDKMessage) {
          const processed = yield* Deferred.make<void>();
          processedMessages.set(message, processed);
          yield* Queue.offer(sdkMessages, message);
          yield* Deferred.await(processed);
        });
        const firstIngestionEnded = yield* Deferred.make<void>();
        const endedRuns = new Set<string>();
        let firstRunId: string | undefined;
        let queryCount = 0;
        const openedEfforts: Array<string | undefined> = [];
        const adapter = yield* ClaudeAdapterV2.makeClaudeAdapterV2({
          instanceId: modelSelection.instanceId,
          settings,
          environment: {},
          attachmentsDir: cwd,
          fileSystem: yield* FileSystem.FileSystem,
          path: yield* Path.Path,
          crypto: yield* Crypto.Crypto,
          idAllocator: yield* IdAllocator.IdAllocatorV2,
          queryRunner: {
            allocateSessionId: Effect.succeed(sessionId),
            open: (input) =>
              Effect.gen(function* () {
                openedEfforts.push(input.options.effort);
                const messages =
                  queryCount++ === 0 ? sdkMessages : yield* Queue.unbounded<SDKMessage>();
                return {
                  messages: Stream.fromQueue(messages).pipe(
                    Stream.flatMap((message) =>
                      Stream.make(message).pipe(
                        Stream.concat(
                          Stream.fromEffect(
                            Effect.suspend(() => {
                              const processed = processedMessages.get(message);
                              return processed === undefined
                                ? Effect.void
                                : Deferred.succeed(processed, undefined);
                            }),
                          ).pipe(Stream.drain),
                        ),
                      ),
                    ),
                  ),
                  offer: () => Effect.void,
                  setModel: () => Effect.void,
                  setPermissionMode: () => Effect.void,
                  stopTask: () => Effect.void,
                  interrupt: Effect.void,
                  close: Queue.shutdown(messages),
                };
              }),
            forkSession: () => Effect.die("unused"),
            subagentLaunchToolUseId: () => Effect.succeed(null),
            assertComplete: Effect.void,
          },
        });
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const management = yield* ThreadManagementService.ThreadManagementService;
          const sessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
          const threadId = ThreadId.make("thread:earlier-run-detach");
          const senderThreadId = ThreadId.make("sender:earlier-run-detach");
          const projectId = ProjectId.make("project:earlier-run-detach");
          const taskId = "native-earlier-run-detach";
          const toolUseId = "toolu-native-earlier-run-detach";
          const bashId = "toolu-native-earlier-run-detach-bash";
          const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
            orchestrator.streamDomainEvents.pipe(
              Stream.filter(predicate),
              Stream.take(1),
              Stream.runDrain,
              Effect.forkScoped({ startImmediately: true }),
            );
          const storedEvents = Effect.fnUntraced(function* (ownedThreadId: ThreadId) {
            const sequence = yield* orchestrator.getThreadEventSequence(ownedThreadId);
            return yield* orchestrator
              .streamStoredEventsFrom({ afterSequence: 0, threadId: ownedThreadId })
              .pipe(
                Stream.takeUntil((stored) => stored.sequence === sequence),
                Stream.map((stored) => stored.event),
                Stream.runCollect,
              );
          });
          for (const createdThreadId of [threadId, senderThreadId]) {
            yield* orchestrator.dispatch({
              type: "thread.create",
              commandId: CommandId.make(`create:${createdThreadId}`),
              threadId: createdThreadId,
              projectId,
              title: "Claude query detach",
              modelSelection,
              runtimeMode: "full-access",
              interactionMode: "default",
              branch: null,
              worktreePath: cwd,
              createdBy: "user",
              creationSource: "web",
            });
          }
          const send = (id: string) =>
            management.sendToThread({
              commandId: CommandId.make(id),
              projectId,
              threadId,
              messageId: MessageId.make(id),
              text: "Audit in the background.",
              attachments: [],
              mode: "auto",
              createdBy: "user",
              creationSource: "web",
            });
          const firstRunning = yield* watch(
            (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
          );
          yield* send("earlier-run");
          yield* worker.drain();
          yield* Fiber.join(firstRunning);
          const childToolRunning = yield* watch(
            (event) =>
              event.type === "turn-item.updated" &&
              event.payload.nativeItemRef?.nativeId === bashId &&
              event.payload.status === "running",
          );
          for (const message of [
            frame({
              type: "system",
              subtype: "task_started",
              task_id: taskId,
              tool_use_id: toolUseId,
              description: "Audit recent commits",
              subagent_type: "general-purpose",
              is_backgrounded: true,
              task_type: "local_agent",
              prompt: "Audit the last five commits.",
              uuid: "earlier-native-started",
              session_id: sessionId,
            }),
            frame({
              type: "assistant",
              uuid: "earlier-native-bash",
              session_id: sessionId,
              parent_tool_use_id: toolUseId,
              message: {
                id: "earlier-native-message",
                role: "assistant",
                type: "message",
                model: modelSelection.model,
                content: [
                  { type: "tool_use", id: bashId, name: "Bash", input: { command: "git log -5" } },
                ],
              },
            }),
          ])
            yield* offerAndWait(message);
          yield* Fiber.join(childToolRunning);
          const before = yield* orchestrator.getThreadProjection(threadId);
          const firstRun = before.runs[0]!;
          const firstTurn = before.providerTurns[0]!;
          const child = before.subagents.find((entry) => entry.nativeTaskRef?.nativeId === taskId)!;
          assert.equal(child.origin, "provider_native");
          assert.isNotNull(child.childThreadId);
          firstRunId = firstRun.id;
          const firstCompleted = yield* watch(
            (event) =>
              event.type === "run.updated" &&
              event.payload.id === firstRun.id &&
              event.payload.status === "completed",
          );
          const firstCheckpointPending = yield* watch(
            (event) =>
              event.type === "run.updated" &&
              event.payload.id === firstRun.id &&
              event.payload.status === "waiting",
          );
          yield* offerAndWait(result("earlier-run-completed", false));
          yield* Fiber.join(firstCheckpointPending);
          yield* worker.drain();
          yield* Fiber.join(firstCompleted);
          assert.isFalse(
            endedRuns.has(firstRun.id),
            "native child keeps the completed run subscribed",
          );
          const secondRunning = yield* watch(
            (event) =>
              event.type === "provider-turn.updated" &&
              event.payload.id !== firstTurn.id &&
              event.payload.status === "running",
          );
          yield* send("later-run");
          yield* worker.drain();
          yield* Fiber.join(secondRunning);
          assert.equal(queryCount, 1, "the later run reuses the first CLI process");
          const current = yield* orchestrator.getThreadProjection(threadId);
          const secondTurn = current.providerTurns.find((turn) => turn.id !== firstTurn.id)!;
          const replacementRunning = yield* watch(
            (event) =>
              event.type === "provider-turn.updated" &&
              event.payload.id !== firstTurn.id &&
              event.payload.id !== secondTurn.id &&
              event.payload.status === "running",
          );
          const delivered = yield* management.sendToThread({
            commandId: CommandId.make("detach-earlier-query"),
            projectId,
            threadId,
            messageId: MessageId.make("detach-earlier-query"),
            text: "Stop the audit and change direction.",
            attachments: [],
            mode: "restart",
            createdBy: "agent",
            creationSource: "mcp",
            senderThreadId,
            ...(cutIn === "completed-before-effect-selection"
              ? {
                  modelSelection: {
                    ...modelSelection,
                    options: [{ id: "effort", value: "low" }],
                  },
                }
              : {}),
          });
          assert.equal(delivered.delivery, "restarted");
          if (cutIn !== "active") {
            const secondCompleted = yield* watch(
              (event) =>
                event.type === "provider-turn.updated" &&
                event.payload.id === secondTurn.id &&
                event.payload.status === "completed",
            );
            yield* offerAndWait(result("later-run-completed-before-stop-effect", false));
            yield* Fiber.join(secondCompleted);
            assert.equal(queryCount, 1, "the Stop effect has not executed yet");
          }
          yield* worker.drain();
          assert.equal(
            queryCount,
            2,
            "the urgent prompt requires the stopped query to be replaced",
          );
          yield* Fiber.join(replacementRunning);
          const after = yield* orchestrator.getThreadProjection(threadId);
          const endedChild = after.subagents.find((entry) => entry.id === child.id)!;
          assert.equal(
            endedChild.status,
            "interrupted",
            "detach must reach the earlier run's subscriber",
          );
          assert.equal(endedChild.runId, firstRun.id);
          assert.equal(endedChild.parentNodeId, child.parentNodeId);
          assert.equal(endedChild.childThreadId, child.childThreadId);
          yield* Deferred.await(firstIngestionEnded);
          assert.isTrue(
            endedRuns.has(firstRun.id),
            "the old subscription closes after its child ends",
          );
          assert.equal(
            after.providerTurns.find((turn) => turn.id === firstTurn.id)?.status,
            "completed",
          );
          assert.equal(
            after.providerTurns.find((turn) => turn.id === secondTurn.id)?.status,
            cutIn === "active" ? "interrupted" : "completed",
          );
          assert.equal(queryCount, 2);
          assert.equal(openedEfforts[0], "high");
          assert.equal(
            openedEfforts[1],
            cutIn === "completed-before-effect-selection" ? "low" : "high",
          );
          const session = Option.getOrThrow(
            yield* sessions.get(after.providerThreads[0]!.providerSessionId!),
          );
          assert.isFalse(yield* session.hasPendingBackgroundWork!);
          const parentEvents = yield* storedEvents(threadId);
          assert.lengthOf(
            parentEvents.filter(
              (event) =>
                event.type === "subagent.updated" &&
                event.payload.id === child.id &&
                event.payload.status === "interrupted",
            ),
            1,
          );
          const card = after.turnItems.find(
            (item) => item.type === "subagent" && item.subagentId === child.id,
          )!;
          assert.equal(card.status, "interrupted");
          assert.equal(card.runId, firstRun.id);
          assert.equal(card.providerTurnId, firstTurn.id);
          assert.lengthOf(
            parentEvents.filter(
              (event) =>
                event.type === "turn-item.updated" &&
                event.payload.id === card.id &&
                event.payload.status === "interrupted",
            ),
            1,
          );
          const oldNode = after.nodes.find((node) => node.id === child.id)!;
          assert.equal(oldNode.status, "interrupted");
          assert.equal(oldNode.runId, firstRun.id);
          assert.equal(oldNode.providerTurnId, firstTurn.id);
          const childAfter = yield* orchestrator.getThreadProjection(child.childThreadId!);
          const childEvents = yield* storedEvents(child.childThreadId!);
          for (const node of childAfter.nodes) {
            assert.equal(node.status, "interrupted");
            assert.lengthOf(
              childEvents.filter(
                (event) =>
                  event.type === "node.updated" &&
                  event.payload.id === node.id &&
                  event.payload.status === "interrupted",
              ),
              1,
            );
          }
          const tool = childAfter.turnItems.find(
            (item) => item.nativeItemRef?.nativeId === bashId,
          )!;
          assert.equal(tool.status, "interrupted");
          assert.equal(tool.runId, null);
          assert.equal(tool.providerTurnId, null);
          assert.lengthOf(
            childEvents.filter(
              (event) =>
                event.type === "turn-item.updated" &&
                event.payload.id === tool.id &&
                event.payload.status === "interrupted",
            ),
            1,
          );
        }).pipe(
          Effect.provide(
            ProviderReplayHarness.layerWithRegistry(
              { name: "claude-earlier-run-detach", runtimePolicyOverride: { cwd } },
              ProviderAdapterRegistry.layerSingle(adapter),
              { runEffectWorker: false },
            ).pipe(
              Layer.provide(
                Layer.succeed(StreamClock.StreamClockHooks, {
                  beginAttempt: () => Effect.void,
                  observe: () => Effect.void,
                  // This observation runs after the real RunExecutionService closes its subscription.
                  endAttempt: (_threadId, runId) =>
                    Effect.gen(function* () {
                      endedRuns.add(runId);
                      if (runId === firstRunId)
                        yield* Deferred.succeed(firstIngestionEnded, undefined);
                    }),
                }),
              ),
            ),
          ),
        );
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
);

it.effect("completes a reused Claude child after its resuming attempt ends in the same run", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace("claude-same-run-resume");
      const queryMessages: Array<Queue.Queue<SDKMessage>> = [];
      const processedMessages = new WeakMap<SDKMessage, Deferred.Deferred<void>>();
      const ingestionEnded = new Map<string, Deferred.Deferred<void>>();
      const endedAttempts = new Set<string>();
      const offerAndWait = Effect.fnUntraced(function* (
        messages: Queue.Queue<SDKMessage>,
        message: SDKMessage,
      ) {
        const processed = yield* Deferred.make<void>();
        processedMessages.set(message, processed);
        yield* Queue.offer(messages, message);
        yield* Deferred.await(processed);
      });
      const adapter = yield* ClaudeAdapterV2.makeClaudeAdapterV2({
        instanceId: modelSelection.instanceId,
        settings,
        environment: {},
        attachmentsDir: cwd,
        fileSystem: yield* FileSystem.FileSystem,
        path: yield* Path.Path,
        crypto: yield* Crypto.Crypto,
        idAllocator: yield* IdAllocator.IdAllocatorV2,
        queryRunner: {
          allocateSessionId: Effect.succeed(sessionId),
          open: () =>
            Effect.gen(function* () {
              const messages = yield* Queue.unbounded<SDKMessage>();
              queryMessages.push(messages);
              return {
                messages: Stream.fromQueue(messages).pipe(
                  Stream.flatMap((message) =>
                    Stream.make(message).pipe(
                      Stream.concat(
                        Stream.fromEffect(
                          Effect.suspend(() => {
                            const processed = processedMessages.get(message);
                            return processed === undefined
                              ? Effect.void
                              : Deferred.succeed(processed, undefined);
                          }),
                        ).pipe(Stream.drain),
                      ),
                    ),
                  ),
                ),
                offer: () => Effect.void,
                setModel: () => Effect.void,
                setPermissionMode: () => Effect.void,
                stopTask: () => Effect.void,
                interrupt: Effect.void,
                close: Queue.shutdown(messages),
              };
            }),
          forkSession: () => Effect.die("unused"),
          subagentLaunchToolUseId: () => Effect.succeed(null),
          assertComplete: Effect.void,
        },
      });
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const management = yield* ThreadManagementService.ThreadManagementService;
        const threadId = ThreadId.make("thread:same-run-resume");
        const senderThreadId = ThreadId.make("sender:same-run-resume");
        const projectId = ProjectId.make("project:same-run-resume");
        const taskId = "native-same-run-resume";
        const launchToolUseId = "toolu-same-run-resume-agent";
        const resumeToolUseId = "toolu-same-run-resume-sendmessage";
        const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
          orchestrator.streamDomainEvents.pipe(
            Stream.filter(predicate),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped({ startImmediately: true }),
          );
        const storedEvents = Effect.fnUntraced(function* (ownedThreadId: ThreadId) {
          const sequence = yield* orchestrator.getThreadEventSequence(ownedThreadId);
          return yield* orchestrator
            .streamStoredEventsFrom({ afterSequence: 0, threadId: ownedThreadId })
            .pipe(
              Stream.takeUntil((stored) => stored.sequence === sequence),
              Stream.map((stored) => stored.event),
              Stream.runCollect,
            );
        });
        const send = (id: string, text: string) =>
          management.sendToThread({
            commandId: CommandId.make(id),
            projectId,
            threadId,
            messageId: MessageId.make(id),
            text,
            attachments: [],
            mode: "auto",
            createdBy: "user",
            creationSource: "web",
          });
        for (const createdThreadId of [threadId, senderThreadId]) {
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make(`create:${createdThreadId}`),
            threadId: createdThreadId,
            projectId,
            title: "Claude same-run resume",
            modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: cwd,
            createdBy: "user",
            creationSource: "web",
          });
        }
        const firstRunning = yield* watch(
          (event) =>
            event.type === "provider-turn.updated" &&
            event.threadId === threadId &&
            event.payload.status === "running",
        );
        yield* send("same-run-audit", "Start a background audit and keep working.");
        yield* worker.drain();
        yield* Fiber.join(firstRunning);
        assert.lengthOf(queryMessages, 1);
        const firstQuery = queryMessages[0]!;
        const firstChildCompleted = yield* watch(
          (event) =>
            event.type === "turn-item.updated" &&
            event.threadId === threadId &&
            event.payload.type === "subagent" &&
            event.payload.nativeItemRef?.nativeId === taskId &&
            event.payload.status === "completed",
        );
        for (const message of [
          frame({
            type: "system",
            subtype: "task_started",
            task_id: taskId,
            tool_use_id: launchToolUseId,
            description: "Audit recent commits",
            subagent_type: "general-purpose",
            is_backgrounded: true,
            task_type: "local_agent",
            prompt: "Audit the last five commits.",
            uuid: "same-run-initial-child-started",
            session_id: sessionId,
          }),
          frame({
            type: "system",
            subtype: "task_notification",
            task_id: taskId,
            tool_use_id: launchToolUseId,
            status: "completed",
            output_file: `${cwd}/same-run-initial.output`,
            summary: "N1_INITIAL_AUDIT_DONE",
            uuid: "same-run-initial-child-completed",
            session_id: sessionId,
          }),
        ])
          yield* offerAndWait(firstQuery, message);
        yield* Fiber.join(firstChildCompleted);
        const before = yield* orchestrator.getThreadProjection(threadId);
        const firstRun = before.runs[0]!;
        const firstTurn = before.providerTurns[0]!;
        const firstAttempt = before.attempts[0]!;
        const child = before.subagents.find((entry) => entry.nativeTaskRef?.nativeId === taskId)!;
        assert.equal(firstRun.status, "running");
        assert.equal(firstTurn.status, "running");
        assert.equal(child.status, "completed");
        assert.equal(child.result, "N1_INITIAL_AUDIT_DONE");
        assert.equal(child.origin, "provider_native");
        assert.isNotNull(child.childThreadId);

        // A completed child survives the query cut and is adopted by A2.
        // An interrupted child would exercise a different ownership problem.
        const replacementRunning = yield* watch(
          (event) =>
            event.type === "provider-turn.updated" &&
            event.threadId === threadId &&
            event.payload.runAttemptId !== firstAttempt.id &&
            event.payload.status === "running",
        );
        const delivered = yield* management.sendToThread({
          commandId: CommandId.make("same-run-urgent-restart"),
          projectId,
          threadId,
          messageId: MessageId.make("same-run-urgent-restart"),
          text: "Change direction before continuing the audit.",
          attachments: [],
          mode: "restart",
          createdBy: "agent",
          creationSource: "mcp",
          senderThreadId,
        });
        assert.equal(delivered.delivery, "restarted");
        yield* worker.drain();
        yield* Fiber.join(replacementRunning);
        assert.lengthOf(queryMessages, 2);
        const secondQuery = queryMessages[1]!;
        const restarted = yield* orchestrator.getThreadProjection(threadId);
        assert.lengthOf(restarted.runs, 1, "the cut-in replaces an attempt of R1");
        const replacementAttempt = restarted.attempts.find(
          (attempt) => attempt.id !== firstAttempt.id,
        )!;
        const replacementTurn = restarted.providerTurns.find(
          (turn) => turn.runAttemptId === replacementAttempt.id,
        )!;
        assert.equal(replacementAttempt.runId, firstRun.id);
        assert.equal(replacementTurn.status, "running");
        assert.equal(
          restarted.subagents.find((entry) => entry.id === child.id)?.status,
          "completed",
        );
        assert.equal(
          restarted.providerTurns.find((turn) => turn.id === firstTurn.id)?.status,
          "interrupted",
        );
        const replacementIngestionEnded = ingestionEnded.get(replacementAttempt.id)!;
        assert.isDefined(replacementIngestionEnded);
        const reopened = yield* watch(
          (event) =>
            event.type === "turn-item.updated" &&
            event.payload.type === "subagent" &&
            event.payload.subagentId === child.id &&
            event.payload.providerTurnId === replacementTurn.id &&
            event.payload.status === "running",
        );
        // This is the recorded SendMessage delivery ACK, not a child outcome.
        const resumeAck = {
          success: true,
          message: `Resuming agent ${taskId.slice(0, 7)}`,
          resumedAgentId: taskId,
          pin: { id: taskId, name: taskId, ref: "42ab31" },
        };
        for (const message of [
          frame({
            type: "assistant",
            uuid: "same-run-resume-sendmessage",
            session_id: sessionId,
            parent_tool_use_id: null,
            message: {
              id: "same-run-resume-sendmessage-message",
              role: "assistant",
              type: "message",
              model: modelSelection.model,
              content: [
                {
                  type: "tool_use",
                  id: resumeToolUseId,
                  name: "SendMessage",
                  input: {
                    to: taskId,
                    summary: "Resume the audit",
                    message: "Resume the same native audit.",
                  },
                },
              ],
            },
          }),
          frame({
            type: "system",
            subtype: "task_started",
            task_id: taskId,
            tool_use_id: resumeToolUseId,
            description: "Resume the audit",
            subagent_type: "general-purpose",
            is_backgrounded: true,
            task_type: "local_agent",
            prompt: "Resume the same native audit.",
            uuid: "same-run-resumed-child-started",
            session_id: sessionId,
          }),
          frame({
            type: "user",
            uuid: "same-run-resume-ack",
            session_id: sessionId,
            parent_tool_use_id: null,
            message: {
              role: "user",
              content: [
                {
                  type: "tool_result",
                  tool_use_id: resumeToolUseId,
                  content: [{ type: "text", text: JSON.stringify(resumeAck) }],
                },
              ],
            },
            tool_use_result: resumeAck,
          }),
        ])
          yield* offerAndWait(secondQuery, message);
        yield* Fiber.join(reopened);
        const resumed = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(resumed.subagents.find((entry) => entry.id === child.id)?.status, "running");
        const rootWaiting = yield* watch(
          (event) =>
            event.type === "run.updated" &&
            event.payload.id === firstRun.id &&
            event.payload.status === "waiting",
        );
        const rootCompleted = yield* watch(
          (event) =>
            event.type === "run.updated" &&
            event.payload.id === firstRun.id &&
            event.payload.status === "completed",
        );
        yield* offerAndWait(secondQuery, result("same-run-replacement-root-completed", false));
        yield* Fiber.join(rootWaiting);
        yield* worker.drain();
        yield* Fiber.join(rootCompleted);
        assert.isFalse(endedAttempts.has(replacementAttempt.id));

        const nextRunning = yield* watch(
          (event) =>
            event.type === "provider-turn.updated" &&
            event.threadId === threadId &&
            event.runId !== firstRun.id &&
            event.payload.status === "running",
        );
        yield* send("same-run-next-user-run", "Work on another task while the audit finishes.");
        yield* worker.drain();
        yield* Fiber.join(nextRunning);
        assert.lengthOf(queryMessages, 2, "R2 reuses the replacement query");
        const cardCompleted = yield* watch(
          (event) =>
            event.type === "turn-item.updated" &&
            event.payload.type === "subagent" &&
            event.payload.subagentId === child.id &&
            event.payload.providerTurnId === replacementTurn.id &&
            event.payload.status === "completed",
        );
        for (const message of [
          frame({
            type: "assistant",
            uuid: "same-run-resumed-child-answer",
            session_id: sessionId,
            parent_tool_use_id: launchToolUseId,
            message: {
              id: "same-run-resumed-child-answer-message",
              role: "assistant",
              type: "message",
              model: modelSelection.model,
              content: [{ type: "text", text: "N1_RESUMED_AUDIT_DONE" }],
            },
          }),
          frame({
            type: "system",
            subtype: "task_notification",
            task_id: taskId,
            tool_use_id: resumeToolUseId,
            status: "completed",
            output_file: `${cwd}/same-run-resumed.output`,
            summary: "N1_RESUMED_AUDIT_DONE",
            uuid: "same-run-resumed-child-completed",
            session_id: sessionId,
          }),
        ])
          yield* offerAndWait(secondQuery, message);
        // The card persists even with stale tree ownership. Waiting for it
        // makes the broken subagent row a literal assertion failure.
        yield* Fiber.join(cardCompleted);
        const after = yield* orchestrator.getThreadProjection(threadId);
        const completedChild = after.subagents.find((entry) => entry.id === child.id)!;
        assert.equal(completedChild.status, "completed");
        assert.equal(completedChild.runId, firstRun.id);
        assert.equal(completedChild.parentNodeId, replacementAttempt.rootNodeId);
        assert.equal(completedChild.childThreadId, child.childThreadId);
        assert.equal(completedChild.result, "N1_RESUMED_AUDIT_DONE");
        assert.lengthOf(
          after.subagents.filter((entry) => entry.nativeTaskRef?.nativeId === taskId),
          1,
        );
        const node = after.nodes.find((entry) => entry.id === child.id)!;
        assert.equal(node.status, "completed");
        assert.equal(node.runId, firstRun.id);
        assert.equal(node.parentNodeId, replacementAttempt.rootNodeId);
        assert.equal(node.rootNodeId, replacementAttempt.rootNodeId);
        assert.equal(node.providerTurnId, replacementTurn.id);
        const card = after.turnItems.find(
          (item) => item.type === "subagent" && item.subagentId === child.id,
        )!;
        assert.equal(card.status, "completed");
        assert.equal(card.runId, firstRun.id);
        assert.equal(card.providerTurnId, replacementTurn.id);
        const parentEvents = yield* storedEvents(threadId);
        assert.lengthOf(
          parentEvents.filter(
            (event) =>
              event.type === "subagent.updated" &&
              event.payload.id === child.id &&
              event.payload.parentNodeId === replacementAttempt.rootNodeId &&
              event.payload.status === "completed",
          ),
          1,
        );
        assert.lengthOf(
          parentEvents.filter(
            (event) =>
              event.type === "node.updated" &&
              event.payload.id === child.id &&
              event.payload.rootNodeId === replacementAttempt.rootNodeId &&
              event.payload.status === "completed",
          ),
          1,
        );
        assert.lengthOf(
          parentEvents.filter(
            (event) =>
              event.type === "turn-item.updated" &&
              event.payload.id === card.id &&
              event.payload.providerTurnId === replacementTurn.id &&
              event.payload.status === "completed",
          ),
          1,
        );
        const childAfter = yield* orchestrator.getThreadProjection(child.childThreadId!);
        const childRoot = childAfter.nodes.find((entry) => entry.kind === "root_turn")!;
        assert.equal(childRoot.status, "completed");
        const childEvents = yield* storedEvents(child.childThreadId!);
        assert.lengthOf(
          childEvents.filter(
            (event) =>
              event.type === "node.updated" &&
              event.payload.id === childRoot.id &&
              event.payload.status === "completed",
          ),
          2,
          "the initial and resumed child each complete once",
        );
        assert.lengthOf(
          childAfter.messages.filter((message) => message.text === "Resume the same native audit."),
          1,
        );
        assert.lengthOf(
          childAfter.messages.filter((message) => message.text === "N1_RESUMED_AUDIT_DONE"),
          1,
        );
        // The persisted outcomes must be checked before awaiting cleanup:
        // the broken row also prevents A2's ingestion from ending.
        yield* Deferred.await(replacementIngestionEnded);
        assert.isTrue(endedAttempts.has(replacementAttempt.id));
      }).pipe(
        Effect.provide(
          ProviderReplayHarness.layerWithRegistry(
            { name: "claude-same-run-resume", runtimePolicyOverride: { cwd } },
            ProviderAdapterRegistry.layerSingle(adapter),
            { runEffectWorker: false },
          ).pipe(
            Layer.provide(
              Layer.succeed(StreamClock.StreamClockHooks, {
                beginAttempt: ({ attemptId }) =>
                  Effect.gen(function* () {
                    ingestionEnded.set(attemptId, yield* Deferred.make<void>());
                  }),
                observe: () => Effect.void,
                endAttempt: (_threadId, _runId, attemptId) =>
                  Effect.gen(function* () {
                    endedAttempts.add(attemptId);
                    yield* Deferred.succeed(ingestionEnded.get(attemptId)!, undefined);
                  }),
              }),
            ),
          ),
        ),
      );
    }).pipe(
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  ),
);

it.effect("keeps a native Claude child running until failed-close retry detaches its query", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace("claude-close-retry");
      const sdkMessages = yield* Queue.unbounded<SDKMessage>();
      const resultHandled = yield* Deferred.make<void>();
      const acknowledge = yield* Deferred.make<void>();
      const processedMessages = new WeakMap<SDKMessage, Deferred.Deferred<void>>();
      const offerAndWait = Effect.fnUntraced(function* (
        messages: Queue.Queue<SDKMessage>,
        message: SDKMessage,
      ) {
        const processed = yield* Deferred.make<void>();
        processedMessages.set(message, processed);
        yield* Queue.offer(messages, message);
        yield* Deferred.await(processed);
      });
      const offers: SDKUserMessage[] = [];
      let queryCount = 0;
      let closes = 0;
      let interrupts = 0;
      const adapter = yield* ClaudeAdapterV2.makeClaudeAdapterV2({
        instanceId: modelSelection.instanceId,
        settings,
        environment: {},
        attachmentsDir: cwd,
        fileSystem: yield* FileSystem.FileSystem,
        path: yield* Path.Path,
        crypto: yield* Crypto.Crypto,
        idAllocator: yield* IdAllocator.IdAllocatorV2,
        queryRunner: {
          allocateSessionId: Effect.succeed(sessionId),
          open: () =>
            Effect.gen(function* () {
              const messages =
                queryCount++ === 0 ? sdkMessages : yield* Queue.unbounded<SDKMessage>();
              return {
                messages: Stream.fromQueue(messages).pipe(
                  Stream.flatMap((message) =>
                    Stream.make(message).pipe(
                      // A receipt acknowledges frame handling, never terminal settlement.
                      Stream.concat(
                        Stream.fromEffect(
                          Effect.suspend(() => {
                            const processed = processedMessages.get(message);
                            return processed === undefined
                              ? Effect.void
                              : Deferred.succeed(processed, undefined);
                          }),
                        ).pipe(Stream.drain),
                      ),
                    ),
                  ),
                ),
                offer: (message: SDKUserMessage) =>
                  Effect.sync(() => {
                    offers.push(message);
                  }),
                setModel: () => Effect.void,
                setPermissionMode: () => Effect.void,
                stopTask: () => Effect.void,
                interrupt: Effect.gen(function* () {
                  interrupts++;
                  yield* offerAndWait(messages, result("close-retry-native-abort", true));
                  yield* Deferred.succeed(resultHandled, undefined);
                  // SDK control ACK and native aborted result are separate milestones.
                  yield* Deferred.await(acknowledge);
                }),
                close: Effect.gen(function* () {
                  closes++;
                  // The runner contract can fail; SDK 0.3.276 normally cannot.
                  if (closes === 1)
                    return yield* new ClaudeAdapterV2.ClaudeAgentSdkQueryRunnerError({
                      method: "close",
                      cause: "fault-injected close failure",
                    });
                  yield* Queue.shutdown(messages);
                }),
              };
            }),
          forkSession: () => Effect.die("unused"),
          subagentLaunchToolUseId: () => Effect.succeed(null),
          assertComplete: Effect.void,
        },
      });
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const management = yield* ThreadManagementService.ThreadManagementService;
        const threadId = ThreadId.make("thread:close-retry");
        const senderThreadId = ThreadId.make("sender-thread");
        const projectId = ProjectId.make("project:close-retry");
        const nativeTaskId = "native-close-retry-task";
        const urgentText = "Stop sending the email. Change the recipient first.";
        const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
          orchestrator.streamDomainEvents.pipe(
            Stream.filter(predicate),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped({ startImmediately: true }),
          );
        const childInterruptions = Effect.fnUntraced(function* () {
          const throughSequence = yield* orchestrator.getThreadEventSequence(threadId);
          // Stored-event streams tail live changes, so finish at this snapshot's last event.
          return yield* orchestrator.streamStoredEventsFrom({ afterSequence: 0, threadId }).pipe(
            Stream.takeUntil((stored) => stored.sequence === throughSequence),
            Stream.map((stored) => stored.event),
            Stream.filter(
              (event) =>
                event.type === "subagent.updated" &&
                event.payload.nativeTaskRef?.nativeId === nativeTaskId &&
                event.payload.status === "interrupted",
            ),
            Stream.runCollect,
          );
        });
        for (const createdThreadId of [senderThreadId, threadId]) {
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make(`create:${createdThreadId}`),
            threadId: createdThreadId,
            projectId,
            title: "Claude close retry",
            modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: cwd,
            createdBy: "user",
            creationSource: "web",
          });
        }
        const running = yield* watch(
          (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
        );

        yield* management.sendToThread({
          commandId: CommandId.make("first"),
          projectId,
          threadId,
          messageId: MessageId.make("first"),
          text: "Audit in the background.",
          attachments: [],
          mode: "auto",
          createdBy: "user",
          creationSource: "web",
        });
        yield* worker.drain();
        yield* Fiber.join(running);
        const childRunning = yield* watch(
          (event) =>
            event.type === "subagent.updated" &&
            event.payload.nativeTaskRef?.nativeId === nativeTaskId &&
            event.payload.status === "running",
        );
        yield* offerAndWait(
          sdkMessages,
          frame({
            type: "system",
            subtype: "task_started",
            task_id: nativeTaskId,
            tool_use_id: "toolu-native-close-retry",
            description: "Audit recent commits",
            subagent_type: "general-purpose",
            is_backgrounded: true,
            task_type: "local_agent",
            prompt: "Audit the last five commits.",
            uuid: "native-close-retry-started",
            session_id: sessionId,
          }),
        );
        yield* Fiber.join(childRunning);
        const before = yield* orchestrator.getThreadProjection(threadId);
        const child = before.subagents.find(
          (entry) => entry.nativeTaskRef?.nativeId === nativeTaskId,
        )!;
        const oldTurn = before.providerTurns[0]!;
        const oldAttempt = before.attempts[0]!;
        assert.equal(child.origin, "provider_native");
        assert.equal(child.status, "running");
        assert.isNotNull(child.childThreadId);
        const childInterrupted = yield* watch(
          (event) =>
            event.type === "subagent.updated" &&
            event.payload.id === child.id &&
            event.payload.status === "interrupted",
        );
        const replacementRunning = yield* watch(
          (event) =>
            event.type === "provider-turn.updated" &&
            event.payload.id !== oldTurn.id &&
            event.payload.status === "running",
        );
        const sent = yield* management.sendToThread({
          commandId: CommandId.make("urgent-close-retry"),
          projectId,
          threadId,
          messageId: MessageId.make("urgent-close-retry"),
          text: urgentText,
          attachments: [],
          mode: "restart",
          createdBy: "agent",
          creationSource: "mcp",
          senderThreadId,
        });
        assert.equal(sent.delivery, "restarted");
        const firstDrain = yield* worker.drain().pipe(Effect.forkChild);
        yield* Deferred.await(resultHandled);
        assert.equal(closes, 0);
        yield* Deferred.succeed(acknowledge, undefined);
        yield* Fiber.join(firstDrain);
        const failedClose = yield* orchestrator.getThreadProjection(threadId);
        expect
          .soft(failedClose.subagents.find((entry) => entry.id === child.id)?.status)
          .toBe("running");
        expect
          .soft(failedClose.providerTurns.find((turn) => turn.id === oldTurn.id)?.status)
          .toBe("running");
        assert.equal(
          failedClose.attempts.find((attempt) => attempt.id === oldAttempt.id)?.status,
          "superseded",
        );
        expect
          .soft(
            yield* childInterruptions(),
            "failed close must not cascade native child settlement",
          )
          .toEqual([]);
        assert.equal(queryCount, 1);
        assert.lengthOf(offers, 1);
        assert.equal(closes, 1);

        yield* TestClock.adjust("100 millis");
        const retryTime = yield* DateTime.now;
        yield* worker.drain();
        yield* Fiber.join(childInterrupted);
        yield* Fiber.join(replacementRunning);
        const after = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(after.subagents.find((entry) => entry.id === child.id)?.status, "interrupted");
        const interruptedTurn = after.providerTurns.find((turn) => turn.id === oldTurn.id)!;
        assert.equal(interruptedTurn.status, "interrupted");
        expect
          .soft(
            interruptedTurn.completedAt,
            "terminal time must follow close retry, not native abort",
          )
          .toEqual(retryTime);
        assert.lengthOf(yield* childInterruptions(), 1);
        expect.soft(closes, "retry must close the captured query").toBe(2);
        expect.soft(queryCount, "replacement must use a new process query").toBe(2);
        assert.equal(interrupts, 1);
        assert.lengthOf(offers, 2);
        const promptText = JSON.stringify(offers[1]?.message.content);
        assert.include(promptText, "agent in T3 thread sender-thread interrupted");
        assert.include(promptText, "interrupted, not refused");
        assert.include(promptText, urgentText);
      }).pipe(
        Effect.provide(
          ProviderReplayHarness.layerWithRegistry(
            { name: "claude-close-retry", runtimePolicyOverride: { cwd } },
            ProviderAdapterRegistry.layerSingle(adapter),
            { runEffectWorker: false },
          ),
        ),
      );
    }).pipe(
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  ),
);

it.effect.each([
  "child completion",
  "scheduled message",
  "user steering",
  "agent auto",
  "agent steer",
  "agent restart",
  "human web steer",
  "human mobile steer",
  "human promotion",
  "agent promotion",
  "human waiting steer",
  "human selection steer",
  "agent slash restart",
  "bare agent restart",
  "bare human web steer",
  "bare agent slash restart",
] as const)("delivers %s with the requested tool interruption behavior", (delivery) =>
  Effect.scoped(
    Effect.gen(function* () {
      const bare = delivery.startsWith("bare ");
      const kind = bare ? delivery.slice("bare ".length) : delivery;
      const cwd = yield* checkpointWorkspace("claude-automatic-delivery");
      const bareConfig = bare
        ? yield* Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem;
            const path = yield* Path.Path;
            const crypto = yield* Crypto.Crypto;
            const directory = yield* fs.makeTempDirectoryScoped({
              prefix: "claude-bare-addition-",
            });
            const file = path.join(directory, "instructions.txt");
            const body = "BARE-ADDITION-SENTINEL\nUse only this named addition.\n";
            yield* fs.writeFileString(file, body);
            return {
              additions: [
                {
                  type: "file" as const,
                  path: yield* fs.realPath(file),
                  sha256: Buffer.from(
                    yield* crypto.digest("SHA-256", new TextEncoder().encode(body)),
                  ).toString("hex"),
                  byteCount: Buffer.byteLength(body, "utf8"),
                },
              ],
            };
          })
        : undefined;
      const sdkMessages = yield* Queue.unbounded<SDKMessage>();
      const offers: SDKUserMessage[] = [];
      const nativeQueue: SDKUserMessage[] = [];
      const batchAbort = new AbortController();
      const urgent =
        kind === "agent restart" ||
        (bare && kind === "agent slash restart") ||
        (kind.startsWith("human") && kind !== "human waiting steer");
      const senderThreadId = ThreadId.make("sender-thread");
      let queryCount = 0;
      const openedEfforts: Array<string | undefined> = [];
      let currentMessages = sdkMessages;
      const adapter = yield* ClaudeAdapterV2.makeClaudeAdapterV2({
        instanceId: modelSelection.instanceId,
        settings,
        environment: {},
        attachmentsDir: cwd,
        fileSystem: yield* FileSystem.FileSystem,
        path: yield* Path.Path,
        crypto: yield* Crypto.Crypto,
        idAllocator: yield* IdAllocator.IdAllocatorV2,
        queryRunner: {
          allocateSessionId: Effect.succeed(sessionId),
          open: (input) =>
            Effect.gen(function* () {
              openedEfforts.push(input.options.effort);
              const messages =
                queryCount++ === 0 ? sdkMessages : yield* Queue.unbounded<SDKMessage>();
              currentMessages = messages;
              return {
                messages: Stream.fromQueue(messages),
                offer: (message) =>
                  Effect.sync(() => {
                    offers.push(message);
                    nativeQueue.push(message);
                    // Claude Code 2.1.289 aborts on now-priority commands even while Bash blocks reads.
                    if (nativeQueue.some((command) => command.priority === "now")) {
                      batchAbort.abort({ kind: "interrupt" });
                    }
                  }),
                setModel: () => Effect.void,
                applyFlagSettings: () => Effect.void,
                setPermissionMode: () => Effect.void,
                backgroundTasks: () => Effect.succeed(false),
                stopTask: () => Effect.void,
                interrupt: urgent
                  ? Effect.sync(() => batchAbort.abort({ kind: "interrupt" }))
                  : Effect.die("automatic delivery must never interrupt"),
                close: Queue.shutdown(messages),
              };
            }),
          forkSession: () => Effect.die("unused"),
          subagentLaunchToolUseId: () => Effect.succeed(null),
          assertComplete: Effect.void,
        },
      });
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = ThreadId.make("thread:automatic-delivery");
        const projectId = ProjectId.make("project:automatic-delivery");
        const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
          orchestrator.streamDomainEvents.pipe(
            Stream.filter(predicate),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
        for (const createdThreadId of [senderThreadId, threadId]) {
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make(`create:${createdThreadId}`),
            threadId: createdThreadId,
            projectId,
            title: "Automatic delivery",
            modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: cwd,
            ...(createdThreadId === threadId && bareConfig !== undefined
              ? { bare: bareConfig }
              : {}),
            createdBy: "user",
            creationSource: "web",
          });
        }
        const running = yield* watch(
          (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
        );
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("first"),
          threadId,
          messageId: MessageId.make("first"),
          text: "Build and check all four tasks.",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        yield* worker.drain();
        yield* Fiber.join(running);
        if (bare) {
          const initialText = String(offers[0]?.message.content);
          assert.equal(initialText.split("BARE-ADDITION-SENTINEL").length - 1, 1);
          assert.isTrue(initialText.endsWith("Build and check all four tasks."));
        }
        const pending = yield* watch(
          (event) =>
            event.type === "turn-item.updated" &&
            event.payload.nativeItemRef?.nativeId === "status-4",
        );
        yield* Queue.offer(sdkMessages, batch);
        yield* Fiber.join(pending);
        const before = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(before.turnItems.filter((item) => item.status === "running").length, 5);
        const parent = before.runs[0]!;
        if (parent.rootNodeId === null) return yield* Effect.die("parent has no root node");
        const messageId = MessageId.make("automatic-notice");
        if (kind === "human waiting steer" || kind === "human selection steer") {
          yield* Queue.offer(
            sdkMessages,
            frame({
              type: "system",
              subtype: "task_started",
              task_id: "background-build",
              tool_use_id: "build",
              description: "Background build",
              is_backgrounded: true,
              task_type: "local_bash",
              uuid: "background-started",
              session_id: sessionId,
            }),
          );
        }
        if (kind === "human waiting steer") {
          const waiting = yield* watch(
            (event) =>
              event.type === "run.updated" &&
              event.payload.id === parent.id &&
              event.payload.status === "waiting",
          );
          yield* Queue.offer(sdkMessages, result("root-completed-background-running", false));
          yield* Fiber.join(waiting);
          const waitingProjection = yield* orchestrator.getThreadProjection(threadId);
          assert.equal(
            waitingProjection.runs[0]?.status,
            "waiting",
            JSON.stringify(
              waitingProjection.providerThreads.map((thread) => thread.pendingBackgroundTasks),
            ),
          );
        }
        const restartedRunning = urgent
          ? yield* watch(
              (event) =>
                event.type === "provider-turn.updated" &&
                event.payload.status === "running" &&
                event.payload.runAttemptId !== parent.activeAttemptId,
            )
          : null;
        if (kind === "child completion") {
          const taskId = NodeId.make("completed-child");
          const now = yield* DateTime.now;
          const sink = yield* EventSink.EventSinkV2;
          yield* sink.write({
            events: [
              {
                id: EventId.make("cohort"),
                type: "run.updated",
                threadId,
                runId: parent.id,
                occurredAt: now,
                payload: {
                  ...parent,
                  delegatedCompletion: {
                    disposition: "open",
                    nextGeneration: 2,
                    delivery: { generation: 1, messageId, taskIds: [taskId] },
                  },
                },
              },
              {
                id: EventId.make("child"),
                type: "subagent.updated",
                threadId,
                runId: parent.id,
                nodeId: taskId,
                occurredAt: now,
                payload: {
                  id: taskId,
                  threadId,
                  runId: parent.id,
                  parentNodeId: parent.rootNodeId,
                  origin: "app_owned",
                  createdBy: "agent",
                  driver: ClaudeAdapterV2.CLAUDE_PROVIDER,
                  providerInstanceId: modelSelection.instanceId,
                  providerThreadId: null,
                  childThreadId: null,
                  nativeTaskRef: null,
                  prompt: "Background work",
                  title: "Child",
                  model: null,
                  completionWake: "always",
                  completionDelivery: { state: "claimed", observedByRunId: null },
                  status: "completed",
                  result: "done",
                  startedAt: now,
                  completedAt: now,
                  updatedAt: now,
                },
              },
            ],
          });
          const command = {
            type: "message.dispatch" as const,
            commandId: CommandId.make("completion"),
            threadId,
            messageId,
            text: "Child completed",
            attachments: [],
            dispatchMode: { type: "queue_after_active" as const },
            createdBy: "agent" as const,
            creationSource: "server" as const,
            delegatedCompletion: { parentRunId: parent.id, generation: 1, taskIds: [taskId] },
          };
          yield* orchestrator.dispatch(command);
          yield* orchestrator.dispatch(command);
          yield* orchestrator.recoverDelegatedTasks;
        } else if (kind === "scheduled message") {
          yield* Effect.gen(function* () {
            const service = yield* ScheduledTaskService.ScheduledTaskService;
            const { task } = yield* service.upsert(
              yield* decodeScheduledTask({
                title: "Status check",
                prompt: "Check the four tasks.",
                enabled: true,
                schedule: { type: "interval", everyMs: 60_000 },
                projectId,
                threadId,
                workspaceStrategy: { type: "root" },
                modelSelection,
                runtimeMode: "full-access",
                interactionMode: "default",
              }),
            );
            const ran = yield* service.runNow({ id: task.id });
            assert.equal(ran.task.lastRunStatus, "succeeded");
          }).pipe(
            Effect.provide(
              ScheduledTaskService.layer.pipe(
                Layer.provide(Layer.mock(BareLaunch.BareLaunch)({})),
                Layer.provide(ThreadManagementService.layer),
                Layer.provide(ServerSettings.layerTest()),
                Layer.provide(
                  CommandReceiptStore.layer.pipe(Layer.provide(SqlitePersistence.layerMemory)),
                ),
                Layer.provide(
                  Layer.mock(LegacyV1ThreadImporter.LegacyV1ThreadImporter)({
                    ensureTranscript: () =>
                      Effect.succeed({ importedThreadCount: 0, importedMessageCount: 0 }),
                  }),
                ),
                Layer.provide(Layer.mock(ThreadLaunchService.ThreadLaunchService)({})),
                Layer.provide(
                  Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
                    namedProjectsRoot: "/projects",
                    isScratchProject: () => Effect.succeed(false),
                  }),
                ),
                Layer.provide(Layer.mock(SecretRequests.SecretRequests)({})),
                Layer.provide(
                  Layer.mergeAll(NodeCrypto.layer, Scheduler.layer, SqlitePersistence.layerMemory),
                ),
              ),
            ),
          );
        } else if (kind === "human promotion" || kind === "agent promotion") {
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("queue-agent"),
            threadId,
            messageId,
            text: "Stop sending the email. Change the recipient first.",
            attachments: [],
            dispatchMode: { type: "queue_after_active" },
            createdBy: "agent",
            creationSource: "mcp",
            senderThreadId,
          });
          const queued = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
            (run) => run.status === "queued",
          )!;
          const promote = orchestrator.dispatch(
            ThreadManagementService.withCreationProvenance(
              {
                type: "queued-message.promote-to-steer",
                commandId: CommandId.make("human-promote"),
                threadId,
                queuedRunId: queued.id,
                targetRunId: parent.id,
              },
              kind === "human promotion"
                ? { createdBy: "user", creationSource: "web" }
                : { createdBy: "agent", creationSource: "mcp" },
            ),
          );
          if (kind === "agent promotion") {
            const error = yield* promote.pipe(Effect.flip);
            assert.equal(error._tag, "OrchestratorDispatchError");
            assert.equal(
              (yield* orchestrator.getThreadProjection(threadId)).runs.find(
                (run) => run.id === queued.id,
              )?.status,
              "queued",
            );
          } else {
            yield* promote;
            const promoted = yield* orchestrator.getThreadProjection(threadId);
            assert.equal(promoted.runs.find((run) => run.id === queued.id)?.status, "cancelled");
          }
        } else if (kind.startsWith("human")) {
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("human-cut-in"),
            threadId,
            messageId,
            text: "Stop sending the email. Change the recipient first.",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            ...(kind === "human selection steer"
              ? {
                  modelSelection: { ...modelSelection, options: [{ id: "effort", value: "low" }] },
                }
              : {}),
            deliveryIntent: kind === "human mobile steer" ? "auto" : "steer",
            createdBy: "user",
            creationSource: kind === "human mobile steer" ? "mobile" : "web",
          });
        } else if (kind.startsWith("agent")) {
          const management = yield* ThreadManagementService.ThreadManagementService;
          const send = management.sendToThread({
            commandId: CommandId.make("urgent-restart"),
            projectId,
            threadId,
            messageId,
            text:
              kind === "agent slash restart"
                ? "/review"
                : "Stop sending the email. Change the recipient first.",
            attachments: [],
            mode: kind === "agent auto" ? "auto" : kind === "agent steer" ? "steer" : "restart",
            createdBy: "agent",
            creationSource: "mcp",
            senderThreadId,
          });
          if (kind === "agent slash restart" && !bare) {
            const error = yield* send.pipe(Effect.flip);
            assert.equal(error._tag, "OrchestratorDispatchError");
            assert.isFalse(batchAbort.signal.aborted);
            assert.equal(offers.length, 1);
            assert.isFalse(
              (yield* orchestrator.getThreadProjection(threadId)).messages.some(
                (message) => message.id === messageId,
              ),
            );
            return;
          }
          const sent = yield* send;
          assert.equal(sent.delivery, urgent ? "restarted" : "queued");
        } else {
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("steer"),
            threadId,
            messageId,
            text: "Change direction now.",
            attachments: [],
            dispatchMode: { type: "steer_active", targetRunId: parent.id },
            createdBy: "user",
            creationSource: "web",
          });
        }
        yield* worker.drain();
        if (kind === "human waiting steer") {
          const after = yield* orchestrator.getThreadProjection(threadId);
          assert.equal(
            after.messages.find((message) => message.id === messageId)?.text,
            "Stop sending the email. Change the recipient first.",
          );
          assert.isFalse(batchAbort.signal.aborted);
          assert.equal(queryCount, 1);
          assert.equal(offers.length, 2);
          return;
        }
        if (urgent) {
          if (restartedRunning !== null) yield* Fiber.join(restartedRunning);
          assert.isTrue(
            batchAbort.signal.aborted,
            "restart must abort pending work before delivery",
          );
          assert.equal(queryCount, 2);
          if (kind === "human selection steer") {
            assert.equal(openedEfforts[0], "high");
            assert.equal(openedEfforts[1], "low");
          }
          assert.equal(offers.length, 2);
          assert.equal(offers.filter((offer) => offer.priority === "now").length, 0);
          const restarted = yield* orchestrator.getThreadProjection(threadId);
          assert.equal(restarted.providerTurns[0]?.status, "interrupted");
          assert.equal(restarted.attempts[0]?.status, "superseded");
          const requester = restarted.attempts.find(
            (attempt) => attempt.reason === "steering_restart",
          )?.requestedBy;
          assert.equal(requester?.createdBy, kind.startsWith("agent") ? "agent" : "user");
          const promptText = JSON.stringify(offers[1]?.message.content);
          if (bare) {
            assert.deepEqual(
              requester,
              kind.startsWith("agent")
                ? { createdBy: "agent", creationSource: "mcp", senderThreadId }
                : { createdBy: "user", creationSource: "web" },
            );
            assert.equal(
              offers[1]?.message.content,
              kind === "agent slash restart"
                ? "/review"
                : "Stop sending the email. Change the recipient first.",
            );
          } else {
            assert.include(
              promptText,
              kind === "agent restart"
                ? "agent in T3 thread sender-thread interrupted"
                : "the user interrupted",
            );
            assert.include(promptText, "interrupted, not refused");
            assert.include(promptText, "Stop sending the email. Change the recipient first.");
          }
          const stored = restarted.messages.find((message) => message.id === messageId)!;
          assert.equal(
            stored.text,
            kind === "agent slash restart"
              ? "/review"
              : "Stop sending the email. Change the recipient first.",
          );
          assert.equal(
            stored.createdBy,
            kind.startsWith("agent") || kind === "human promotion" ? "agent" : "user",
          );
          if (bare) {
            assert.equal(stored.creationSource, kind.startsWith("agent") ? "mcp" : "web");
            assert.equal(
              stored.senderThreadId,
              kind.startsWith("agent") ? senderThreadId : undefined,
            );
          }
          if (kind === "human promotion")
            assert.include(promptText, "which the agent in T3 thread sender-thread wrote");
          if (kind === "agent restart") {
            const management = yield* ThreadManagementService.ThreadManagementService;
            const nextRunning = yield* watch(
              (event) =>
                event.type === "provider-turn.updated" && event.payload.status === "running",
            );
            yield* management.sendToThread({
              commandId: CommandId.make("urgent-restart-2"),
              projectId,
              threadId,
              messageId: MessageId.make("urgent-2"),
              text: "Use the corrected recipient.",
              attachments: [],
              mode: "restart",
              createdBy: "agent",
              creationSource: "mcp",
              senderThreadId,
            });
            yield* worker.drain();
            yield* Fiber.join(nextRunning);
            assert.equal(queryCount, 3);
            if (bare) {
              assert.equal(offers.length, 3);
              assert.equal(offers[2]?.message.content, "Use the corrected recipient.");
              const repeated = yield* orchestrator.getThreadProjection(threadId);
              assert.deepEqual(
                repeated.providerTurns.map((turn) => turn.status),
                ["interrupted", "interrupted", "running"],
              );
              assert.deepEqual(
                repeated.attempts.map((attempt) => attempt.status),
                ["superseded", "superseded", "running"],
              );
              assert.deepEqual(repeated.attempts[2]?.requestedBy, {
                createdBy: "agent",
                creationSource: "mcp",
                senderThreadId,
              });
              const repeatedMessage = repeated.messages.find(
                (message) => message.id === MessageId.make("urgent-2"),
              );
              assert.equal(repeatedMessage?.text, "Use the corrected recipient.");
              assert.equal(repeatedMessage?.createdBy, "agent");
              assert.equal(repeatedMessage?.creationSource, "mcp");
              assert.equal(repeatedMessage?.senderThreadId, senderThreadId);
            } else {
              assert.equal(
                JSON.stringify(offers[2]?.message.content).split("Note from T3 Code:").length - 1,
                1,
              );
            }
          }
          const completed = yield* watch(
            (event) =>
              event.type === "provider-turn.updated" && event.payload.status === "completed",
          );
          yield* Queue.offer(currentMessages, result("urgent-completed", false));
          yield* Fiber.join(completed);
          yield* worker.drain();
          return;
        }
        assert.equal(batchAbort.signal.aborted, false);
        assert.equal(offers.length, 1);
        const finished = yield* watch(
          (event) =>
            event.type === "run.updated" &&
            event.payload.id === parent.id &&
            event.payload.status === "waiting",
        );
        // Bash finishes before the pending status reads, as in the capture.
        yield* Queue.offer(sdkMessages, toolResult("build", false));
        for (const id of statusIds) yield* Queue.offer(sdkMessages, toolResult(id, false));
        yield* Queue.offer(sdkMessages, result("completed", false));
        yield* Fiber.join(finished);
        yield* worker.drain();
        yield* orchestrator.resumeQueuedRuns;
        yield* worker.drain();
        const queued = (yield* orchestrator.getThreadProjection(threadId)).runs[1]!;
        const noticeFinished = yield* watch(
          (event) =>
            event.type === "run.updated" &&
            event.payload.id === queued.id &&
            event.payload.status === "waiting",
        );
        const delivered =
          kind === "child completion"
            ? yield* watch(
                (event) =>
                  event.type === "subagent.updated" &&
                  event.payload.completionDelivery?.state === "delivered",
              )
            : null;
        yield* Queue.offer(sdkMessages, result("notice-completed", false));
        yield* Fiber.join(noticeFinished);
        yield* worker.drain();
        if (delivered !== null) yield* Fiber.join(delivered);
        const after = yield* orchestrator.getThreadProjection(threadId);
        const reads = after.turnItems.filter((item) =>
          statusIds.includes(item.nativeItemRef?.nativeId ?? ""),
        );
        assert.equal(reads.length, 4);
        for (const read of reads) {
          assert.equal(read.status, "completed");
          assert.equal(read.toolNonExecutionKind, undefined);
          assert.equal(read.type === "dynamic_tool" && read.output === refusal, false);
        }
        assert.equal(offers.length, 2);
        assert.equal(offers.filter((offer) => offer.priority === "now").length, 0);
        assert.equal(after.runs.length, 2);
        if (kind === "child completion") {
          assert.equal(after.messages.filter((message) => message.id === messageId).length, 1);
          assert.equal(after.subagents[0]?.completionDelivery?.state, "delivered");
        } else if (kind === "scheduled message") {
          assert.equal(
            after.messages.filter((message) => message.scheduledTaskId !== undefined).length,
            1,
          );
        } else if (kind === "user steering") {
          assert.equal(
            after.messages.filter((message) => message.text === "Change direction now.").length,
            1,
          );
        }
        yield* orchestrator.recoverDelegatedTasks;
        yield* orchestrator.resumeQueuedRuns;
        yield* worker.drain();
        assert.equal(offers.length, 2);
      }).pipe(
        Effect.provide(
          ProviderReplayHarness.layerWithRegistry(
            { name: "claude-automatic-delivery" },
            ProviderAdapterRegistry.layerSingle(adapter),
            { runEffectWorker: false },
          ),
        ),
      );
    }).pipe(
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  ),
);
