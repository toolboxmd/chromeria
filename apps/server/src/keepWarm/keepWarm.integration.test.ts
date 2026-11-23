import { assert, it } from "@effect/vitest";
import {
  CommandId,
  KEEP_WARM_INTERVAL_MS,
  KEEP_WARM_PING_TEXT,
  keepWarmPingMessageId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type KeepWarmMode,
  type ServerProvider,
} from "@t3tools/contracts";
import type { ProviderAdapterV2 } from "@t3tools/provider-core/server/ProviderAdapter";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/sql/SqlClient";
import { ClaudeProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import * as CommandReceipts from "../orchestration-v2/CommandReceiptStore.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as Projection from "../orchestration-v2/ProjectionStore.ts";
import * as Registry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as Harness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as Persistence from "../persistence/Sqlite.ts";
import * as Providers from "../provider/ProviderRegistry.ts";
import { ServerActivation } from "../serverActivation.ts";
import * as Settings from "../serverSettings.ts";
import * as Hooks from "../wight/AdmissionHooks.ts";
import * as KeepWarm from "./KeepWarmService.ts";

const instanceId = ProviderInstanceId.make("claudeAgent");
const driver = ProviderDriverKind.make("claudeAgent");
const threadId = ThreadId.make("keep-warm:integration");
const adapter = {
  instanceId,
  driver,
  getCapabilities: () => Effect.succeed(ClaudeProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("These tests prove committed dispatch without a provider process"),
} satisfies ProviderAdapterV2["Service"];
const provider = {
  instanceId,
  driver,
  enabled: true,
  usageLimits: {
    checkedAt: "1970-01-01T00:00:00.000Z",
    windows: [{ id: "session", kind: "session", label: "Session", usedPercent: 20 }],
  },
} as unknown as ServerProvider;
const database = Persistence.layerMemory;
const foundation = Layer.mergeAll(
  database,
  Settings.layerTest(),
  CommandReceipts.layer.pipe(Layer.provide(database)),
  Projection.layer.pipe(Layer.provide(database)),
  Layer.mock(Providers.ProviderRegistry)({
    getProviders: Effect.succeed([provider]),
    streamChanges: Effect.never as never,
  }),
);
const hooks = Hooks.layer.pipe(Layer.provide(foundation));
const replay = Harness.layerWithRegistry(
  { name: "keep-warm" },
  Registry.layerFromAdapters([adapter]),
  { databaseLayer: database, runEffectWorker: false },
).pipe(Layer.provide(hooks));
const dependencies = Layer.mergeAll(
  foundation,
  replay,
  hooks,
  Threads.layer.pipe(Layer.provide(Layer.merge(replay, foundation))),
  Layer.succeed(ServerActivation, Effect.never),
);
const layer = Layer.mergeAll(dependencies, KeepWarm.layer.pipe(Layer.provide(dependencies)));

const mode: KeepWarmMode = {
  type: "keepWarm",
  enabledAt: "1970-01-01T00:00:00.000Z",
  hiddenRequestAt: "1970-01-01T00:00:00.000Z",
  stopped: null,
};

it.effect("a due ping is one silent turn in the thread's own session, even when replayed", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const settings = yield* Settings.ServerSettingsService;
    const projections = yield* Projection.ProjectionStoreV2;
    const keepWarm = yield* KeepWarm.KeepWarm;
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make("keep-warm:create"),
      threadId,
      projectId: ProjectId.make("project:keep-warm"),
      title: "Keep warm",
      modelSelection: { instanceId, model: "claude-haiku-4-5" },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });
    // The user snoozed the thread past the ping; the ping must not wake it.
    const snoozedUntil = "2099-01-01T00:00:00.000Z";
    yield* orchestrator.dispatch({
      type: "thread.snooze",
      commandId: CommandId.make("keep-warm:snooze"),
      threadId,
      snoozedUntil,
    });
    // The user reopened the thread with Un-settle; the ping must keep that override.
    yield* orchestrator.dispatch({
      type: "thread.unsettle",
      commandId: CommandId.make("keep-warm:unsettle"),
      threadId,
      reason: "user",
    });
    yield* settings.updateSettings({ threadIdleModes: { [threadId]: mode } });
    yield* TestClock.adjust(KEEP_WARM_INTERVAL_MS);
    yield* keepWarm.reconcile;
    yield* keepWarm.reconcile;

    const messageId = keepWarmPingMessageId(threadId, mode, KEEP_WARM_INTERVAL_MS);
    // A restarted server that never recorded the ping sends the same command again.
    yield* orchestrator
      .dispatch({
        type: "message.dispatch",
        threadId,
        commandId: CommandId.make(`server:${messageId}`),
        messageId: MessageId.make(messageId),
        text: KEEP_WARM_PING_TEXT,
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        createdBy: "agent",
        creationSource: "server",
      })
      .pipe(Effect.ignore);

    const projection = yield* orchestrator.getThreadProjection(threadId);
    assert.deepEqual(
      projection.messages.map((message) => [message.id, message.text]),
      [[messageId, KEEP_WARM_PING_TEXT]],
    );
    assert.equal(projection.runs.length, 1);
    assert.equal(
      projection.thread.snoozedUntil == null
        ? null
        : DateTime.formatIso(projection.thread.snoozedUntil),
      snoozedUntil,
    );
    assert.equal(projection.thread.settledOverride, "active");
    assert.deepEqual(projection.runs[0]?.modelSelection, { instanceId, model: "claude-haiku-4-5" });
    const recorded = (yield* settings.getSettings).threadIdleModes[threadId];
    assert.equal(
      recorded?.type === "keepWarm" ? recorded.hiddenRequestAt : null,
      "1970-01-01T00:55:00.000Z",
    );
    // Thread lists, unread state and notifications read the shell, which leaves the ping out.
    const shell = yield* projections.getThreadShell(threadId);
    assert.equal(shell?.latestRunId, null);
    assert.equal(shell?.latestUserMessageAt, null);
    // The shell shows the thread idle, but the ping's run has not finished: no second ping.
    yield* TestClock.adjust(KEEP_WARM_INTERVAL_MS);
    yield* keepWarm.reconcile;
    assert.equal((yield* orchestrator.getThreadProjection(threadId)).messages.length, 1);
  }).pipe(Effect.provide(layer)),
);

it.effect("the latest request survives cancelled queued runs and a reordered queue", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const at = (minute: number) => `2026-11-23T12:${String(minute).padStart(2, "0")}:00.000Z`;
    const insert = (
      thread: string,
      ordinal: number,
      status: string,
      startedAt: string | null,
      completedAt: string | null,
    ) => sql`
      INSERT INTO orchestration_v2_projection_runs
        (run_id, thread_id, ordinal, provider, status, requested_at, completed_at, payload_json)
      VALUES (${`${thread}:${ordinal}`}, ${thread}, ${ordinal}, 'claudeAgent', ${status},
        ${at(0)}, ${completedAt}, ${JSON.stringify({ startedAt, userMessageId: `m:${ordinal}` })})`;
    // One finished turn, then 20 queued messages cancelled before they started.
    yield* insert("cancelled", 1, "completed", at(0), at(1));
    for (let ordinal = 2; ordinal <= 21; ordinal++)
      yield* insert("cancelled", ordinal, "cancelled", null, at(2));
    // 21 queued turns where the first one was moved to the end of the queue.
    for (let ordinal = 2; ordinal <= 21; ordinal++)
      yield* insert("reordered", ordinal, "completed", at(ordinal - 2), at(ordinal - 1));
    yield* insert("reordered", 1, "completed", at(20), at(21));
    assert.equal((yield* KeepWarm.readLatestRequest(ThreadId.make("cancelled")))?.requestAt, at(0));
    assert.equal(
      (yield* KeepWarm.readLatestRequest(ThreadId.make("reordered")))?.requestAt,
      at(20),
    );
  }).pipe(Effect.provide(layer)),
);
