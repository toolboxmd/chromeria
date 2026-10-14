import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { expect, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { McpSchema, McpServer } from "effect/ai";

import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderRuntimeRecovery from "../orchestration-v2/ProviderRuntimeRecoveryService.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ProviderAdapterRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as McpHttpServer from "./McpHttpServer.ts";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import * as ThreadSearch from "../orchestration-v2/ThreadSearch.ts";
import * as McpInvocationContext from "./McpInvocationContext.ts";
import { ThreadToolkit } from "./toolkits/thread/tools.ts";

const decodeQueueList = Schema.decodeUnknownEffect(ThreadToolkit.tools.t3_queue_list.successSchema);
const decodeQueueRead = Schema.decodeUnknownEffect(ThreadToolkit.tools.t3_queue_read.successSchema);
const legacyQueueEntry = {
  queuedRunId: "queued-main-run",
  text: "Existing queued instructions.",
  truncated: false,
};

it.effect("queue reads accept older peer responses without inventing a hold state", () =>
  Effect.gen(function* () {
    const legacyList = { items: [legacyQueueEntry], nextCursor: null };
    const listed = yield* decodeQueueList(legacyList);
    const read = yield* decodeQueueRead(legacyQueueEntry);
    expect(listed).toEqual(legacyList);
    expect(read).toEqual(legacyQueueEntry);
    expect(listed.items[0]?.held).toBeUndefined();
    expect(read.held).toBeUndefined();
  }),
);

it.effect.each([false, true])("queue reads preserve a known hold state of $0", (held) =>
  Effect.gen(function* () {
    const entry = { ...legacyQueueEntry, held };
    const listed = yield* decodeQueueList({ items: [entry], nextCursor: null });
    const read = yield* decodeQueueRead(entry);
    expect(listed.items[0]?.held).toBe(held);
    expect(read.held).toBe(held);
  }),
);

const instanceId = ProviderInstanceId.make("codex");
const callerId = ThreadId.make("queue-resume-caller");
const targetId = ThreadId.make("queue-resume-target");
const selection = { instanceId, model: "queue-proof-model" };
const layerRuntime = ProviderReplayHarness.layerWithRegistry(
  { name: "queue-resume", runtimePolicyOverride: { cwd: "/unused" } },
  ProviderAdapterRegistry.layerFromAdapters([
    {
      instanceId,
      driver: ProviderDriverKind.make("codex"),
      getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
      planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
      // This proof stops at the persisted queue command, before provider I/O.
      openSession: () => Effect.die("The queue proof must not open a provider."),
    },
  ]),
  { runEffectWorker: false },
);
const client = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  clientCapabilities: {},
  clientInfo: { name: "queue-proof", version: "1" },
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "queue-proof", version: "1" },
  },
  getClient: Effect.die("unused"),
});

it.live("agents can see and explicitly resume held messages through the mutation guard", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const server = yield* McpServer.McpServer;
    for (const threadId of [callerId, targetId]) {
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make(`create:${threadId}`),
        threadId,
        projectId: ProjectId.make("queue-proof-project"),
        title: threadId,
        branch: null,
        worktreePath: null,
        modelSelection: selection,
        runtimeMode: "full-access",
        interactionMode: "default",
        createdBy: "user",
        creationSource: "web",
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(`start:${threadId}`),
        threadId,
        messageId: MessageId.make(`start-message:${threadId}`),
        text: "Start work.",
        attachments: [],
        modelSelection: selection,
        dispatchMode: { type: "start_immediately" },
        createdBy: "user",
        creationSource: "web",
      });
    }
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make("queue:follow-up"),
      threadId: targetId,
      messageId: MessageId.make("queued-instructions"),
      text: "Instructions sent by the parent.",
      attachments: [],
      createdBy: "agent",
      creationSource: "mcp",
      dispatchMode: { type: "queue_after_active" },
    });
    yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService.use(
      (recovery) => recovery.recover,
    ).pipe(
      Effect.provide(
        ProviderRuntimeRecovery.layer.pipe(
          Layer.provide(Layer.mergeAll(IdAllocator.layer, ServerSettings.layerTest())),
        ),
      ),
    );
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make("wake:caller"),
      threadId: callerId,
      messageId: MessageId.make("wake-message:caller"),
      text: "Recover the held child queue.",
      attachments: [],
      createdBy: "user",
      creationSource: "web",
      dispatchMode: { type: "start_immediately" },
    });
    const scope: McpInvocationContext.McpInvocationScope = {
      environmentId: EnvironmentId.make("queue-proof-environment"),
      requestNamespace: "queue-proof-agent",
      issuedAt: 0,
      capabilities: new Set(["orchestration"]),
      thread: {
        threadId: callerId,
        providerSessionId: "caller-session",
        providerInstanceId: instanceId,
      },
      client: undefined,
    };
    const invoke = (
      name: string,
      invocation = scope,
      arguments_: Record<string, unknown> = { threadId: targetId },
    ) =>
      server
        .callTool({ name, arguments: arguments_ })
        .pipe(
          Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
          Effect.provideService(McpSchema.McpServerClient, client),
        );
    const before = yield* orchestrator.getThreadProjection(targetId);
    const queued = before.runs.find((run) => run.status === "queued");
    expect(queued?.queueHeld).toBe(true);
    if (queued === undefined) return yield* Effect.die("Expected held queued instructions.");
    const listed = yield* invoke("t3_queue_list");
    expect(listed.structuredContent).toMatchObject({
      items: [{ queuedRunId: queued.id, held: true }],
    });
    const read = yield* invoke("t3_queue_read", scope, {
      threadId: targetId,
      queuedRunId: queued.id,
    });
    expect(read.structuredContent).toMatchObject({
      held: true,
      text: "Instructions sent by the parent.",
    });
    const denied = yield* invoke("t3_queue_resume", {
      ...scope,
      thread: undefined,
      client: { sessionId: "read-only", label: "Read only", access: "read-only" },
    });
    expect(denied.isError).toBe(true);
    expect(
      (yield* orchestrator.getThreadProjection(targetId)).runs.find((run) => run.id === queued.id)
        ?.queueHeld,
    ).toBe(true);
    yield* orchestrator.dispatch({
      type: "thread.runtime-mode.set",
      commandId: CommandId.make("limit:caller"),
      threadId: callerId,
      runtimeMode: "approval-required",
    });
    expect((yield* invoke("t3_queue_resume")).isError).toBe(true);
    expect(
      (yield* orchestrator.getThreadProjection(targetId)).runs.find((run) => run.id === queued.id)
        ?.queueHeld,
    ).toBe(true);
    yield* orchestrator.dispatch({
      type: "thread.runtime-mode.set",
      commandId: CommandId.make("restore:caller"),
      threadId: callerId,
      runtimeMode: "full-access",
    });
    const resumed = yield* invoke("t3_queue_resume");
    expect(resumed.isError, JSON.stringify(resumed.content)).not.toBe(true);
    const after = yield* orchestrator.getThreadProjection(targetId);
    expect(after.runs.find((run) => run.id === queued.id)).toMatchObject({
      status: "starting",
      queueHeld: false,
      ordinal: queued.ordinal,
    });
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make("queue:stopped-follow-up"),
      threadId: targetId,
      messageId: MessageId.make("stopped-follow-up"),
      text: "Leave this stopped queue held.",
      attachments: [],
      createdBy: "agent",
      creationSource: "mcp",
      dispatchMode: { type: "queue_after_active" },
    });
    yield* orchestrator.dispatch({
      type: "thread.stop",
      commandId: CommandId.make("stop:target"),
      threadId: targetId,
    });
    // Stop's retirement guard still requires user intent to reopen the thread.
    expect((yield* invoke("t3_queue_resume")).isError).toBe(true);
    expect(
      (yield* orchestrator.getThreadProjection(targetId)).runs.find(
        (run) => run.userMessageId === "stopped-follow-up",
      ),
    ).toMatchObject({ status: "queued", queueHeld: true });
    // A provider credential that outlived the caller's run cannot resume queues.
    yield* orchestrator.dispatch({
      type: "thread.stop",
      commandId: CommandId.make("stop:caller"),
      threadId: callerId,
    });
    expect((yield* invoke("t3_queue_resume")).isError).toBe(true);
  }).pipe(
    Effect.provide(
      McpHttpServer.layerThreadToolkit.pipe(
        Layer.provideMerge(McpServer.McpServer.layer),
        Layer.provideMerge(layerRuntime),
        Layer.provide(NodeCrypto.layer),
        Layer.provide(Layer.mock(ScheduledTaskService.ScheduledTaskService)({})),
        Layer.provide(Layer.mock(ThreadSearch.ThreadSearch)({})),
      ),
    ),
  ),
);
