import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  MessageId,
  type NodeId,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ThreadProjection,
  OrchestratorMcpDelegateTaskResult,
  OrchestratorMcpRunGroupCancelResult,
  OrchestratorMcpRunGroupResult,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  type ServerProvider,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { McpSchema, McpServer } from "effect/ai";

import { ClaudeProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as BareLaunch from "../orchestration-v2/BareLaunch.ts";
import * as CommandReceiptStore from "../orchestration-v2/CommandReceiptStore.ts";
import * as EffectWorker from "../orchestration-v2/EffectWorker.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProviderAdapterRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ThreadLaunchService from "../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import * as ThreadSearch from "../orchestration-v2/ThreadSearch.ts";
import * as ProviderReplayHarness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as PeerForwarding from "../peer/PeerForwarding.ts";
import * as RemoteDelegation from "../peer/RemoteDelegation.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProviderRegistryMock from "../provider/testUtils/providerRegistryMock.ts";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as McpHttpServer from "./McpHttpServer.ts";
import * as McpInvocationContext from "./McpInvocationContext.ts";

const parentThreadId = ThreadId.make("thread:run-group-parent");
const projectId = ProjectId.make("project:run-group");
const codex = ProviderInstanceId.make("codex");
const claude = ProviderInstanceId.make("claudeAgent");
const codexModel = "gpt-5.4";
const claudeModel = "claude-sonnet-4-6";
const codexColor = { instanceId: codex, model: codexModel };
const claudeColor = { instanceId: claude, model: claudeModel };
/** A Color whose provider this environment does not have, so it cannot start. */
const ghostColor = { instanceId: ProviderInstanceId.make("ghost"), model: "ghost-1" };

const decodeTask = Schema.decodeUnknownEffect(OrchestratorMcpDelegateTaskResult);
const decodeGroup = Schema.decodeUnknownEffect(OrchestratorMcpRunGroupResult);
const decodeGroupCancel = Schema.decodeUnknownEffect(OrchestratorMcpRunGroupCancelResult);

// A declared tool failure arrives as `isError` with its payload as JSON text.
const declaredFailure = (result: McpSchema.CallToolResult) => {
  const text = result.content[0];
  return result.isError === true && text?.type === "text" ? JSON.parse(text.text) : undefined;
};

/** How a turn ends: an answer, a provider failure, or nothing until it is interrupted. */
type TurnOutcome = "complete" | "fail" | "hang";

function provider(instanceId: ProviderInstanceId, driver: string, model: string): ServerProvider {
  return {
    instanceId,
    driver: ProviderDriverKind.make(driver),
    enabled: true,
    installed: true,
    version: "test",
    status: "ready",
    auth: { status: "authenticated" },
    checkedAt: "2026-10-19T00:00:00.000Z",
    models: [{ slug: model, name: model, isCustom: false, capabilities: null }],
    slashCommands: [],
    skills: [],
  };
}

function adapter(
  instanceId: ProviderInstanceId,
  driverName: string,
  capabilities: OrchestrationV2ProviderCapabilities,
  outcome: (
    turn: ProviderAdapter.ProviderAdapterV2TurnInput,
    instanceId: ProviderInstanceId,
  ) => Effect.Effect<TurnOutcome>,
): ProviderAdapter.ProviderAdapterV2["Service"] {
  const driver = ProviderDriverKind.make(driverName);
  const unsupported = () =>
    Effect.fail(new ProviderAdapter.ProviderAdapterProtocolError({ driver, detail: "unused" }));
  return {
    instanceId,
    driver,
    getCapabilities: () => Effect.succeed(capabilities),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
    openSession: (sessionInput) =>
      Effect.gen(function* () {
        const events = yield* PubSub.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
        const now = yield* DateTime.now;
        const providerSession: OrchestrationV2ProviderSession = {
          id: sessionInput.providerSessionId,
          driver,
          providerInstanceId: instanceId,
          status: "ready",
          cwd: sessionInput.runtimePolicy.cwd ?? process.cwd(),
          model: sessionInput.modelSelection.model,
          capabilities,
          createdAt: now,
          updatedAt: now,
          lastError: null,
        };
        const publish = (batch: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>) =>
          Effect.forEach(batch, (event) => PubSub.publish(events, event), { discard: true });
        const turns = new Map<ProviderTurnId, ProviderAdapter.ProviderAdapterV2TurnInput>();
        const providerTurn = (
          turn: ProviderAdapter.ProviderAdapterV2TurnInput,
          id: ProviderTurnId,
          status: "running" | "completed" | "failed" | "interrupted",
          at: DateTime.Utc,
        ): ProviderAdapter.ProviderAdapterV2Event => ({
          type: "provider_turn.updated",
          driver,
          providerTurn: {
            id,
            providerThreadId: turn.providerThread.id,
            nodeId: turn.rootNodeId,
            runAttemptId: turn.attemptId,
            nativeTurnRef: {
              driver,
              nativeId: `native-turn:${turn.threadId}:${turn.runOrdinal}`,
              strength: "strong",
            },
            ordinal: turn.providerTurnOrdinal,
            status,
            startedAt: at,
            completedAt: status === "running" ? null : at,
          },
        });
        return {
          instanceId,
          driver,
          providerSessionId: sessionInput.providerSessionId,
          providerSession,
          events: Stream.fromPubSub(events),
          ensureThread: (threadInput) =>
            Effect.gen(function* () {
              const createdAt = yield* DateTime.now;
              const nativeId = `${driverName}:${threadInput.threadId}`;
              return {
                id: ProviderThreadId.make(`provider-thread:${nativeId}`),
                driver,
                providerInstanceId: instanceId,
                providerSessionId: sessionInput.providerSessionId,
                appThreadId: threadInput.threadId,
                ownerNodeId: null,
                nativeThreadRef: { driver, nativeId, strength: "strong" },
                nativeConversationHeadRef: null,
                status: "idle",
                firstRunOrdinal: null,
                lastRunOrdinal: null,
                handoffIds: [],
                forkedFrom: null,
                createdAt,
                updatedAt: createdAt,
              } satisfies OrchestrationV2ProviderThread;
            }),
          resumeThread: ({ providerThread }) => Effect.succeed(providerThread),
          startTurn: (turn) =>
            Effect.gen(function* () {
              const id = ProviderTurnId.make(
                `provider-turn:${instanceId}:${turn.threadId}:${turn.runOrdinal}`,
              );
              turns.set(id, turn);
              yield* publish([providerTurn(turn, id, "running", yield* DateTime.now)]);
              const ending = yield* outcome(turn, instanceId);
              if (ending === "hang") return;
              const at = yield* DateTime.now;
              const status = ending === "complete" ? "completed" : "failed";
              yield* publish([
                providerTurn(turn, id, status, at),
                {
                  type: "turn_item.updated",
                  driver,
                  turnItem: {
                    id: TurnItemId.make(`turn-item:${turn.threadId}:${turn.runOrdinal}:assistant`),
                    threadId: turn.threadId,
                    runId: turn.runId,
                    nodeId: turn.rootNodeId,
                    providerThreadId: turn.providerThread.id,
                    providerTurnId: id,
                    nativeItemRef: null,
                    parentItemId: null,
                    ordinal: turn.runOrdinal * 100 + 1,
                    status: "completed",
                    title: null,
                    startedAt: at,
                    completedAt: at,
                    updatedAt: at,
                    type: "assistant_message",
                    messageId: MessageId.make(
                      `message:${turn.threadId}:${turn.runOrdinal}:assistant`,
                    ),
                    text: `${instanceId} ${ending === "complete" ? "answered" : "gave up"}.`,
                    streaming: false,
                  },
                },
                {
                  type: "turn.terminal",
                  driver,
                  providerThreadId: turn.providerThread.id,
                  providerTurnId: id,
                  runOrdinal: turn.runOrdinal,
                  ...(ending === "complete"
                    ? { status: "completed" as const, failure: null }
                    : {
                        status: "failed" as const,
                        failureItemOrdinal: turn.runOrdinal * 100 + 2,
                        failure: makeProviderFailure({
                          message: "Simulated provider failure.",
                          code: "simulated_failure",
                          class: "provider_error",
                        }),
                      }),
                  threadDisposition: "reusable",
                },
              ]);
            }),
          steerTurn: () => Effect.void,
          interruptTurn: ({ providerThread, providerTurnId }) =>
            Effect.gen(function* () {
              const turn = turns.get(providerTurnId);
              const at = yield* DateTime.now;
              yield* publish([
                ...(turn === undefined
                  ? []
                  : [providerTurn(turn, providerTurnId, "interrupted", at)]),
                {
                  type: "turn.terminal",
                  driver,
                  providerThreadId: providerThread.id,
                  providerTurnId,
                  runOrdinal: turn?.runOrdinal ?? 1,
                  status: "interrupted",
                  failure: null,
                  threadDisposition: "reusable",
                },
              ]);
            }),
          respondToRuntimeRequest: () => Effect.void,
          readThreadSnapshot: unsupported,
          rollbackThread: unsupported,
          forkThread: unsupported,
        };
      }),
  };
}

const client = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  clientCapabilities: {},
  clientInfo: { name: "run-group-test", version: "1.0.0" },
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "run-group-test", version: "1.0.0" },
  },
  getClient: Effect.die("unused"),
});

const unusedScheduledTasks = Layer.succeed(
  ScheduledTaskService.ScheduledTaskService,
  ScheduledTaskService.ScheduledTaskService.of({
    list: () => Effect.succeed({ tasks: [] }),
    subscribeList: () => Stream.succeed({ tasks: [] }),
    upsert: () => Effect.die("unused"),
    setEnabled: () => Effect.die("unused"),
    delete: () => Effect.die("unused"),
    runNow: () => Effect.die("unused"),
    rotateWebhookToken: () => Effect.die("unused"),
    listWebhookDeliveries: () => Effect.die("unused"),
    getWebhookDelivery: () => Effect.die("unused"),
    triggerWebhook: () => Effect.die("unused"),
  }),
);

function waitForProjection(
  orchestrator: Orchestrator.OrchestratorV2Shape,
  threadId: ThreadId,
  predicate: (projection: OrchestrationV2ThreadProjection) => boolean,
) {
  return Effect.gen(function* () {
    const afterSequence = yield* orchestrator.getThreadEventSequence(threadId);
    const current = yield* orchestrator.getThreadProjection(threadId);
    if (predicate(current)) return current;
    const matched = yield* orchestrator.streamStoredEventsFrom({ threadId, afterSequence }).pipe(
      Stream.mapEffect(() => orchestrator.getThreadProjection(threadId)),
      Stream.filter(predicate),
      Stream.runHead,
    );
    return Option.getOrThrow(matched);
  });
}

/**
 * A parent thread with a live turn on Codex, the orchestrator MCP toolkit,
 * and child turns that end as `outcome` decides (the parent's own turn hangs).
 */
const withParent = <A, E>(
  settings: Parameters<typeof ServerSettings.layerTest>[0],
  outcome: (
    turn: ProviderAdapter.ProviderAdapterV2TurnInput,
    instanceId: ProviderInstanceId,
  ) => Effect.Effect<TurnOutcome>,
  body: (harness: {
    readonly orchestrator: Orchestrator.OrchestratorV2Shape;
    readonly invoke: (
      name: string,
      args: Record<string, unknown>,
    ) => Effect.Effect<McpSchema.CallToolResult, McpSchema.InternalError | McpSchema.InvalidParams>;
    readonly offers: Ref.Ref<
      ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
    >;
    readonly drain: Effect.Effect<number, EffectWorker.OrchestrationEffectWorkerError>;
  }) => Effect.Effect<A, E, Orchestrator.OrchestratorV2>,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace("run-groups");
      const childOutcome = (
        turn: ProviderAdapter.ProviderAdapterV2TurnInput,
        instanceId: ProviderInstanceId,
      ) =>
        turn.threadId === parentThreadId
          ? Effect.succeed<TurnOutcome>("hang")
          : outcome(turn, instanceId);
      const layerRegistry = ProviderAdapterRegistry.layerFromAdapters([
        adapter(codex, "codex", CodexProviderCapabilitiesV2, childOutcome),
        adapter(claude, "claudeAgent", ClaudeProviderCapabilitiesV2, childOutcome),
      ]);
      const offers = yield* Ref.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      // Records each wake the parent would get instead of starting its turn.
      const layerOffers = Layer.succeed(ProviderContinuationRequests.ProviderContinuationRequests, {
        offer: (request) => Ref.update(offers, (existing) => [...existing, request]),
        take: Effect.never,
      });
      const layerOrchestrator = ProviderReplayHarness.layerWithRegistry(
        {
          name: "run-groups",
          runtimePolicyOverride: {
            cwd,
            approvalPolicy: "never",
            sandboxPolicy: {
              type: "readOnly",
              access: { type: "fullAccess" },
              networkAccess: false,
            },
          },
        },
        layerRegistry,
      ).pipe(Layer.provide(layerOffers));
      const layerSettings = ServerSettings.layerTest(settings);
      const layerOrchestration = Layer.merge(
        layerOrchestrator,
        ThreadManagementService.layer.pipe(
          Layer.provide(layerOrchestrator),
          Layer.provide(layerSettings),
          Layer.provide(
            CommandReceiptStore.layer.pipe(Layer.provide(SqlitePersistence.layerMemory)),
          ),
        ),
      );
      const layerTest = McpHttpServer.layerOrchestratorToolkit.pipe(
        Layer.provide(Layer.mock(BareLaunch.BareLaunch)({})),
        Layer.provide(Layer.mock(ThreadLaunchService.ThreadLaunchService)({})),
        Layer.provide(Layer.mock(GitVcsDriver.GitVcsDriver)({})),
        Layer.provideMerge(McpServer.McpServer.layer),
        Layer.provideMerge(layerOrchestration),
        Layer.provide(layerRegistry),
        Layer.provide(
          ProviderRegistryMock.layer([
            provider(codex, "codex", codexModel),
            provider(claude, "claudeAgent", claudeModel),
          ]),
        ),
        Layer.provide(layerSettings),
        Layer.provide(unusedScheduledTasks),
        Layer.provide(Layer.mock(ThreadSearch.ThreadSearch)({})),
        Layer.provide(Layer.mock(PeerForwarding.PeerForwarding)({})),
        Layer.provide(Layer.mock(RemoteDelegation.RemoteDelegation)({})),
        Layer.provide(Layer.mock(SecretRequests.SecretRequests)({})),
        Layer.provide(
          Layer.mock(ProjectService.ProjectService)({
            getById: (id) =>
              Effect.succeed(
                id === projectId
                  ? Option.some({ id, defaultModelSelection: null } as never)
                  : Option.none(),
              ),
          }),
        ),
        Layer.provide(NodeServices.layer),
        Layer.provide(McpProviderSessions.layer),
      );
      return yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const server = yield* McpServer.McpServer;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        yield* orchestrator.dispatch({
          type: "thread.create",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make("command:run-group:create"),
          threadId: parentThreadId,
          projectId,
          owner: "Pauli",
          title: "Run group parent",
          modelSelection: { instanceId: codex, model: codexModel },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: cwd,
        });
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make("command:run-group:start"),
          threadId: parentThreadId,
          messageId: MessageId.make("message:run-group:start"),
          text: "Keep the parent turn active.",
          attachments: [],
          modelSelection: { instanceId: codex, model: codexModel },
          dispatchMode: { type: "start_immediately" },
        });
        yield* waitForProjection(orchestrator, parentThreadId, (projection) =>
          projection.providerTurns.some((turn) => turn.status === "running"),
        );
        const scope: McpInvocationContext.McpInvocationScope = {
          environmentId: EnvironmentId.make("environment:run-group"),
          requestNamespace: "mcp-provider-session-run-group",
          thread: {
            threadId: parentThreadId,
            providerSessionId: "mcp-provider-session-run-group",
            providerInstanceId: codex,
          },
          client: undefined,
          capabilities: new Set(["orchestration"]),
          issuedAt: 1,
        };
        const invoke = (name: string, args: Record<string, unknown>) =>
          server
            .callTool({ name, arguments: args })
            .pipe(
              Effect.provideService(McpInvocationContext.McpInvocationContext, scope),
              Effect.provideService(McpSchema.McpServerClient, client),
            );
        return yield* body({ orchestrator, invoke, offers, drain: worker.drain() });
      }).pipe(Effect.provide(layerTest));
    }),
  );

const roastRun = (
  models: ReadonlyArray<{ instanceId: ProviderInstanceId; model: string }>,
  run: number,
) => ({ prismRoles: { roast: { models, run } } }) as Parameters<typeof ServerSettings.layerTest>[0];

const group = (result: McpSchema.CallToolResult) => {
  expect(result.isError).toBe(false);
  return decodeGroup(result.structuredContent).pipe(Effect.orDie);
};

const subagent = (projection: OrchestrationV2ThreadProjection, id: NodeId) =>
  projection.subagents.find((task) => task.id === id);

describe("Run groups", () => {
  it.live("Run 2 Roast wakes the parent once with a failed and a cancelled member", () =>
    Effect.gen(function* () {
      const codexDone = yield* Deferred.make<void>();
      yield* withParent(
        roastRun([codexColor, claudeColor], 2),
        // Codex fails when released; Claude runs until cancelled.
        (_turn, instanceId) =>
          instanceId === codex
            ? Deferred.await(codexDone).pipe(Effect.as<TurnOutcome>("fail"))
            : Effect.succeed<TurnOutcome>("hang"),
        ({ orchestrator, invoke, offers, drain }) =>
          Effect.gen(function* () {
            const started = yield* group(
              yield* invoke("delegate_task", {
                task: "Attack this plan.",
                prismRole: "roast",
                clientRequestId: "roast-1",
              }),
            );
            expect(started.status).toBe("running");
            expect(started.tasks.map((task) => task.providerInstanceId)).toEqual([codex, claude]);
            const [failing, cancelled] = started.tasks;

            yield* Deferred.succeed(codexDone, undefined);
            yield* waitForProjection(
              orchestrator,
              parentThreadId,
              (projection) => subagent(projection, failing!.taskId)?.status === "failed",
            );
            yield* drain;
            const held = yield* orchestrator.getThreadProjection(parentThreadId);
            // One member is done, but the group has not woken the parent.
            expect(subagent(held, failing!.taskId)?.completionDelivery).toBeUndefined();
            expect(held.runs[0]?.delegatedCompletion?.delivery ?? null).toBeNull();
            expect(yield* Ref.get(offers)).toHaveLength(0);

            const cancel = yield* invoke("task_cancel", { taskId: cancelled!.taskId });
            expect(cancel.isError).toBe(false);
            const woken = yield* waitForProjection(
              orchestrator,
              parentThreadId,
              (projection) =>
                (projection.runs[0]?.delegatedCompletion?.delivery?.taskIds.length ?? 0) > 0,
            );
            yield* drain;
            const delivery = woken.runs[0]?.delegatedCompletion?.delivery;
            expect(new Set(delivery?.taskIds)).toEqual(
              new Set([failing!.taskId, cancelled!.taskId]),
            );
            expect(yield* Ref.get(offers)).toHaveLength(1);

            // The wake text lists each member's status and result.
            yield* orchestrator.dispatch({
              type: "message.dispatch",
              createdBy: "agent",
              creationSource: "server",
              commandId: CommandId.make("command:run-group:wake"),
              threadId: parentThreadId,
              messageId: delivery!.messageId,
              text: "Delegated tasks reached terminal states.",
              attachments: [],
              modelSelection: { instanceId: codex, model: codexModel },
              dispatchMode: { type: "queue_after_active" },
              delegatedCompletion: {
                parentRunId: woken.runs[0]!.id,
                generation: delivery!.generation,
                taskIds: delivery!.taskIds,
              },
            });
            const wake = (yield* orchestrator.getThreadProjection(parentThreadId)).messages.find(
              (message) => message.id === delivery!.messageId,
            );
            const cancelledStatus = subagent(woken, cancelled!.taskId)?.status;
            expect(["cancelled", "interrupted"]).toContain(cancelledStatus);
            expect(wake?.text).toContain(`Run group ${started.groupId} finished.`);
            expect(wake?.text).toContain(`[${failing!.taskId} · ${codexModel}] failed`);
            expect(wake?.text).toContain(
              `[${cancelled!.taskId} · ${claudeModel}] ${cancelledStatus}`,
            );

            const status = yield* group(yield* invoke("task_status", { taskId: started.groupId }));
            expect(status.status).toBe("completed");
            expect(status.tasks.map((task) => task.taskId)).toEqual([
              failing!.taskId,
              cancelled!.taskId,
            ]);
            expect(status.tasks[0]?.status).toBe("failed");
          }),
      );
    }),
  );

  it.live("a retry with the same clientRequestId returns the committed group", () =>
    withParent(
      roastRun([codexColor, claudeColor], 2),
      () => Effect.succeed<TurnOutcome>("hang"),
      ({ orchestrator, invoke }) =>
        Effect.gen(function* () {
          const args = {
            task: "Attack this plan.",
            prismRole: "roast",
            clientRequestId: "roast-r",
          };
          const first = yield* group(yield* invoke("delegate_task", args));
          const retry = yield* group(yield* invoke("delegate_task", args));
          expect(retry.groupId).toBe(first.groupId);
          expect(retry.tasks.map((task) => task.taskId)).toEqual(
            first.tasks.map((task) => task.taskId),
          );
          const parent = yield* orchestrator.getThreadProjection(parentThreadId);
          expect(parent.subagents.filter((task) => task.origin === "app_owned")).toHaveLength(2);
        }),
    ),
  );

  it.live(
    "a Color that cannot start yields to the next, and fewer than N start a short group",
    () =>
      withParent(
        roastRun([ghostColor, codexColor, claudeColor], 3),
        () => Effect.succeed<TurnOutcome>("hang"),
        ({ invoke }) =>
          Effect.gen(function* () {
            const short = yield* group(
              yield* invoke("delegate_task", { task: "Attack this plan.", prismRole: "roast" }),
            );
            expect(short.tasks.map((task) => task.providerInstanceId)).toEqual([codex, claude]);
          }),
      ),
  );

  it.live("a Run group errors only when no Color can start", () =>
    withParent(
      roastRun([ghostColor], 2),
      () => Effect.succeed<TurnOutcome>("hang"),
      ({ orchestrator, invoke }) =>
        Effect.gen(function* () {
          const refused = yield* invoke("delegate_task", {
            task: "Attack this plan.",
            prismRole: "roast",
          });
          expect(declaredFailure(refused)).toMatchObject({ code: "provider_unavailable" });
          const parent = yield* orchestrator.getThreadProjection(parentThreadId);
          expect(parent.subagents).toHaveLength(0);
        }),
    ),
  );

  it.live("cancelling the group stops every member and gives up its wake", () =>
    withParent(
      roastRun([codexColor, claudeColor], 2),
      () => Effect.succeed<TurnOutcome>("hang"),
      ({ orchestrator, invoke, offers, drain }) =>
        Effect.gen(function* () {
          const started = yield* group(
            yield* invoke("delegate_task", { task: "Attack this plan.", prismRole: "roast" }),
          );
          // At once, while members' turns are still starting (#511).
          const cancelled = yield* decodeGroupCancel(
            (yield* invoke("task_cancel", { taskId: started.groupId })).structuredContent,
          ).pipe(Effect.orDie);
          expect(cancelled.tasks.map((task) => task.taskId)).toEqual(
            started.tasks.map((task) => task.taskId),
          );
          const stopped = yield* waitForProjection(orchestrator, parentThreadId, (projection) =>
            started.tasks.every((task) =>
              ["cancelled", "interrupted"].includes(
                subagent(projection, task.taskId)?.status ?? "",
              ),
            ),
          );
          yield* drain;
          for (const task of started.tasks)
            expect(subagent(stopped, task.taskId)?.completionDelivery?.state).toBe("disposed");
          expect(yield* Ref.get(offers)).toHaveLength(0);
        }),
    ),
  );

  it.live("a member's nested work keeps the group open until it is terminal", () =>
    Effect.gen(function* () {
      const codexTurnDone = yield* Deferred.make<void>();
      const nestedDone = yield* Deferred.make<void>();
      const nestedPrompt = "Nested check.";
      yield* withParent(
        roastRun([codexColor, claudeColor], 2),
        (turn, instanceId) =>
          turn.message.text === nestedPrompt
            ? Deferred.await(nestedDone).pipe(Effect.as<TurnOutcome>("complete"))
            : instanceId === codex && !turn.message.text.startsWith("Delegated task")
              ? Deferred.await(codexTurnDone).pipe(Effect.as<TurnOutcome>("complete"))
              : Effect.succeed<TurnOutcome>("complete"),
        ({ orchestrator, invoke, offers, drain }) =>
          Effect.gen(function* () {
            const started = yield* group(
              yield* invoke("delegate_task", { task: "Attack this plan.", prismRole: "roast" }),
            );
            const [nesting, plain] = started.tasks;
            const nestingThread = nesting!.childThreadId!;
            const live = yield* waitForProjection(orchestrator, nestingThread, (projection) =>
              projection.providerTurns.some((turn) => turn.status === "running"),
            );
            const nestingRun = live.runs[0]!;
            // The Codex member delegates work of its own while its turn runs.
            yield* orchestrator.dispatch({
              type: "delegated_task.request",
              createdBy: "agent",
              creationSource: "mcp",
              commandId: CommandId.make("command:run-group:nested"),
              parentThreadId: nestingThread,
              parentRunId: nestingRun.id,
              parentNodeId: nestingRun.rootNodeId!,
              task: nestedPrompt,
              modelSelection: { instanceId: claude, model: claudeModel },
              runtimeMode: "full-access",
              interactionMode: "default",
              completionWake: "always",
            });
            yield* Deferred.succeed(codexTurnDone, undefined);
            yield* waitForProjection(
              orchestrator,
              parentThreadId,
              (projection) => subagent(projection, plain!.taskId)?.status === "completed",
            );
            yield* waitForProjection(orchestrator, nestingThread, (projection) =>
              projection.runs.some((run) => run.id === nestingRun.id && run.status === "completed"),
            );
            yield* drain;
            const waiting = yield* orchestrator.getThreadProjection(parentThreadId);
            expect(subagent(waiting, nesting!.taskId)?.status).toBe("running");
            expect(
              (yield* Ref.get(offers)).filter((offer) => offer.threadId === parentThreadId),
            ).toHaveLength(0);

            yield* Deferred.succeed(nestedDone, undefined);
            // The nested result wakes its member, which answers and only then finishes.
            const memberWake = yield* waitForProjection(
              orchestrator,
              nestingThread,
              (projection) =>
                (projection.runs[0]?.delegatedCompletion?.delivery?.taskIds.length ?? 0) > 0,
            );
            const nestedDelivery = memberWake.runs[0]!.delegatedCompletion!.delivery!;
            const stillHeld = yield* orchestrator.getThreadProjection(parentThreadId);
            expect(subagent(stillHeld, nesting!.taskId)?.status).toBe("running");
            yield* orchestrator.dispatch({
              type: "message.dispatch",
              createdBy: "agent",
              creationSource: "server",
              commandId: CommandId.make("command:run-group:nested-wake"),
              threadId: nestingThread,
              messageId: nestedDelivery.messageId,
              text: "Delegated task reached a terminal state.",
              attachments: [],
              modelSelection: { instanceId: codex, model: codexModel },
              dispatchMode: { type: "queue_after_active" },
              delegatedCompletion: {
                parentRunId: nestingRun.id,
                generation: nestedDelivery.generation,
                taskIds: nestedDelivery.taskIds,
              },
            });
            const woken = yield* waitForProjection(
              orchestrator,
              parentThreadId,
              (projection) =>
                (projection.runs[0]?.delegatedCompletion?.delivery?.taskIds.length ?? 0) === 2,
            );
            expect(subagent(woken, nesting!.taskId)?.status).toBe("completed");
            yield* drain;
            // One offer woke the member for its nested result; one wakes the parent.
            expect(
              (yield* Ref.get(offers)).filter((offer) => offer.threadId === parentThreadId),
            ).toHaveLength(1);
          }),
      );
    }),
  );

  it.live("Worker and Planner run one Color even when saved settings ask for more", () =>
    withParent(
      {
        prismRoles: {
          worker: { lanes: { medium: [codexColor, claudeColor] }, run: 2 },
          planner: { models: [codexColor, claudeColor], run: 2 },
        },
      } as Parameters<typeof ServerSettings.layerTest>[0],
      () => Effect.succeed<TurnOutcome>("hang"),
      ({ invoke }) =>
        Effect.gen(function* () {
          for (const prismRole of ["worker", "planner"]) {
            const result = yield* invoke("delegate_task", { task: "Do it.", prismRole });
            expect(result.isError).toBe(false);
            const task = yield* decodeTask(result.structuredContent).pipe(Effect.orDie);
            expect(task.providerInstanceId).toBe(codex);
            expect(result.structuredContent).not.toHaveProperty("groupId");
          }
        }),
    ),
  );

  it.live("an explicit target runs one Color and Run 1 keeps the single-task result", () =>
    withParent(
      roastRun([codexColor, claudeColor], 2),
      () => Effect.succeed<TurnOutcome>("hang"),
      ({ invoke }) =>
        Effect.gen(function* () {
          const explicit = yield* invoke("delegate_task", {
            task: "Attack this plan.",
            prismRole: "roast",
            target: { providerInstanceId: claude, model: claudeModel },
          });
          expect(explicit.isError).toBe(false);
          expect(explicit.structuredContent).not.toHaveProperty("groupId");
          expect(
            (yield* decodeTask(explicit.structuredContent).pipe(Effect.orDie)).providerInstanceId,
          ).toBe(claude);
          // Reviewer keeps Run 1: the result is exactly today's single-task shape.
          const single = yield* invoke("delegate_task", { task: "Review it.", role: "review" });
          // `remoteChild` is present only for a task in a linked environment.
          expect(Object.keys(single.structuredContent ?? {}).toSorted()).toEqual(
            Object.keys(OrchestratorMcpDelegateTaskResult.fields)
              .filter((key) => key !== "remoteChild")
              .toSorted(),
          );
        }),
    ),
  );

  it.live("a Run group to a linked environment is refused", () =>
    withParent(
      roastRun([codexColor, claudeColor], 2),
      () => Effect.succeed<TurnOutcome>("hang"),
      ({ orchestrator, invoke }) =>
        Effect.gen(function* () {
          const refused = yield* invoke("delegate_task", {
            task: "Attack this plan.",
            prismRole: "roast",
            target: { environmentId: "environment:elsewhere" },
          });
          expect(declaredFailure(refused)).toMatchObject({ code: "invalid_request" });
          expect(declaredFailure(refused).message).toContain("linked environment");
          expect((yield* orchestrator.getThreadProjection(parentThreadId)).subagents).toHaveLength(
            0,
          );
        }),
    ),
  );
});
