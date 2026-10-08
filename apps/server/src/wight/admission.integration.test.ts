import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  RunId,
  type OrchestrationV2DomainEvent,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationV2ServerCommand,
  type ServerProvider,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/sql/SqlClient";
import * as Settings from "../serverSettings.ts";
import * as Providers from "../provider/ProviderRegistry.ts";
import * as Persistence from "../persistence/Sqlite.ts";
import * as Sink from "../orchestration-v2/EventSink.ts";
import * as Outbox from "../orchestration-v2/EffectOutbox.ts";
import * as Stream from "effect/Stream";
import * as Projection from "../orchestration-v2/ProjectionStore.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as Registry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import type { ProviderAdapterV2Shape } from "../orchestration-v2/ProviderAdapter.ts";
import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as Harness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import { ServerActivation } from "../serverActivation.ts";
import * as Hooks from "./AdmissionHooks.ts";
import * as Wight from "./WightService.ts";
import { wightAdmissionPlan, readWightThread } from "./admission.ts";

const instanceId = ProviderInstanceId.make("codex");
const threadId = ThreadId.make("wight:integration");
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
  openSession: () => Effect.die("These tests prove committed dispatch without a provider process"),
} as ProviderAdapterV2Shape;
const provider = {
  instanceId,
  enabled: true,
  usageLimits: {
    checkedAt: "2026-10-08T17:00:00.000Z",
    windows: [{ id: "session", kind: "session", label: "Session", usedPercent: 20 }],
  },
} as unknown as ServerProvider;
const database = Persistence.layerMemory;
const foundation = Layer.mergeAll(
  database,
  Settings.layerTest(),
  Projection.layer.pipe(Layer.provide(database)),
  Layer.mock(Providers.ProviderRegistry)({ getProviders: Effect.succeed([provider]) }),
);
const hooks = Hooks.layer.pipe(Layer.provide(foundation));
const replay = Harness.layerWithRegistry(
  { name: "wight-admission" },
  Registry.layerFromAdapters([adapter]),
  {
    databaseLayer: database,
    runEffectWorker: false,
  },
).pipe(Layer.provide(hooks));
const dependencies = Layer.mergeAll(
  foundation,
  replay,
  hooks,
  Threads.layer.pipe(Layer.provide(replay)),
  Layer.succeed(ServerActivation, Effect.never),
);
const layer = Layer.mergeAll(dependencies, Wight.layer.pipe(Layer.provide(dependencies)));
const enabledAt = "2026-10-08T17:00:00.000Z";
const setup = Effect.gen(function* () {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const settings = yield* Settings.ServerSettingsService;
  yield* orchestrator.dispatch({
    type: "thread.create",
    commandId: CommandId.make("wight:create"),
    threadId,
    projectId: ProjectId.make("project:wight"),
    title: "Wight",
    modelSelection: selection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdBy: "user",
    creationSource: "web",
  });
  yield* settings.updateSettings({ wightModes: { [threadId]: { enabledAt, expiresAt: null } } });
});
const prepared = Effect.gen(function* () {
  const thread = yield* readWightThread(threadId);
  if (thread === undefined) return yield* Effect.die("Missing Wight fixture");
  return {
    type: "message.dispatch",
    commandId: CommandId.make("server:wight:prepared"),
    threadId,
    messageId: MessageId.make("server:wight:prepared"),
    text: "Continue.",
    attachments: [],
    dispatchMode: { type: "start_immediately" },
    createdBy: "user",
    creationSource: "server",
    wightAdmission: {
      latestRunId: thread.latestRunId,
      updatedAt: DateTime.toEpochMillis(thread.updatedAt),
      providerInstanceId: instanceId,
      enabledAt,
    },
  } satisfies OrchestrationV2ServerCommand;
});

it.effect(
  "production Wight sends once and preserves stored model, effort and retirement provenance",
  () =>
    Effect.gen(function* () {
      yield* setup;
      const wight = yield* Wight.WightMode;
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      yield* wight.reconcile;
      yield* wight.reconcile;
      const projection = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(projection.messages.length, 1);
      assert.equal(projection.runs.length, 1);
      assert.deepEqual(projection.runs[0]?.modelSelection, selection);
      assert.deepEqual(projection.thread.forkResumedRetirements ?? [], []);
      assert.equal(projection.messages[0]?.creationSource, "server");
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["disabled", "quota", "timer", "stop"] as const)(
  "prepared continuation is refused after %s changes and leaves no message/run",
  (change) =>
    Effect.gen(function* () {
      yield* setup;
      const command = yield* prepared;
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const settings = yield* Settings.ServerSettingsService;
      if (change === "disabled")
        yield* settings.updateSettings({ wightModes: { [threadId]: null } });
      if (change === "quota")
        yield* settings.updateSettings({
          providerInstances: {
            [instanceId]: { driver: ProviderDriverKind.make("codex"), wightLimitPercent: 10 },
          },
        });
      if (change === "timer") {
        const now = yield* DateTime.now;
        yield* settings.updateSettings({
          wightModes: { [threadId]: { enabledAt, expiresAt: DateTime.toEpochMillis(now) + 1_000 } },
        });
        yield* TestClock.adjust(1_000);
      }
      if (change === "stop")
        yield* orchestrator.dispatch({
          type: "thread.stop",
          commandId: CommandId.make("stop:wight"),
          threadId,
        });
      const result = yield* orchestrator.dispatch(command).pipe(Effect.result);
      assert.equal(result._tag, "Failure");
      const projection = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(projection.messages.length, 0);
      assert.equal(projection.runs.length, 0);
      assert.deepEqual(projection.thread.forkResumedRetirements ?? [], []);
      if (change === "stop") assert.equal(projection.thread.forkRetirement?.token, "stop:wight");
    }).pipe(Effect.provide(layer)),
);

it.effect("a human message winning admission never queues a stale Wight continuation", () =>
  Effect.gen(function* () {
    yield* setup;
    const command = yield* prepared;
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      threadId,
      commandId: CommandId.make("human:wins"),
      messageId: MessageId.make("human:wins"),
      text: "Do this instead",
      attachments: [],
      dispatchMode: { type: "start_immediately" },
      createdBy: "user",
      creationSource: "web",
    });
    assert.equal((yield* orchestrator.dispatch(command).pipe(Effect.result))._tag, "Failure");
    const projection = yield* orchestrator.getThreadProjection(threadId);
    assert.equal(projection.runs.length, 1);
    assert.deepEqual(
      projection.messages.map((message) => message.text),
      ["Do this instead"],
    );
    assert.isFalse(projection.runs.some((run) => run.status === "queued"));
  }).pipe(Effect.provide(layer)),
);

it.effect("serialized dispatch rejects a projection changed after the admission snapshot", () =>
  Effect.gen(function* () {
    yield* setup;
    const command = yield* prepared;
    const projections = yield* Projection.ProjectionStoreV2;
    const sql = yield* SqlClient.SqlClient;
    const thread = yield* projections.getThread(threadId);
    // A committed metadata change leaves idle status intact but invalidates the prepared identity.
    yield* projections.apply({
      id: EventId.make("wight:changed"),
      type: "thread.metadata-updated",
      threadId,
      providerInstanceId: instanceId,
      occurredAt: yield* DateTime.now,
      payload: {
        ...thread,
        updatedAt: DateTime.makeUnsafe(DateTime.toEpochMillis(thread.updatedAt) + 1),
      },
    });
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    assert.equal((yield* orchestrator.dispatch(command).pipe(Effect.result))._tag, "Failure");
    const projection = yield* orchestrator.getThreadProjection(threadId);
    assert.equal(projection.messages.length, 0);
    assert.equal(projection.runs.length, 0);
    assert.isFalse(projection.runs.some((run) => run.status === "queued"));
    const receipts = yield* sql<{
      status: string;
    }>`SELECT status FROM orchestration_command_receipts WHERE command_id = ${command.commandId}`;
    // EventSink's projection guard rolls back the reserved receipt with every planned write.
    assert.deepEqual(receipts, []);
  }).pipe(Effect.provide(layer)),
);

it.effect.each([false, true])(
  "prepared Wight transaction writes are atomic, post-planning projection changed=%s",
  (changed) =>
    Effect.gen(function* () {
      yield* setup;
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* Projection.ProjectionStoreV2;
      const sink = yield* Sink.EventSinkV2;
      const outbox = yield* Outbox.EffectOutboxV2;
      const sql = yield* SqlClient.SqlClient;
      // Obtain genuine run/message event shapes from ordinary serialized planning.
      const template = yield* orchestrator.dispatch({
        type: "message.dispatch",
        threadId,
        commandId: CommandId.make("template:plan"),
        messageId: MessageId.make("template:message"),
        text: "Template",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        createdBy: "user",
        creationSource: "web",
      });
      const projection = yield* orchestrator.getThreadProjection(threadId);
      const run = projection.runs[0];
      if (run === undefined) return yield* Effect.die("Missing template run");
      yield* projections.apply({
        id: EventId.make("template:completed"),
        type: "run.updated",
        threadId,
        providerInstanceId: instanceId,
        occurredAt: yield* DateTime.now,
        payload: { ...run, status: "completed", completedAt: yield* DateTime.now },
      });
      const command = yield* prepared;
      const plan = wightAdmissionPlan(command);
      if (plan === null) return yield* Effect.die("Missing prepared Wight guard");
      const events = template.storedEvents.flatMap<OrchestrationV2DomainEvent>(({ event }) => {
        if (event.type === "run.created")
          return [
            {
              ...event,
              id: EventId.make("wight:planned-run"),
              payload: {
                ...event.payload,
                id: RunId.make("wight:queued"),
                ordinal: run.ordinal + 1,
                status: "queued" as const,
              },
            },
          ];
        if (event.type === "message.updated")
          return [
            {
              ...event,
              id: EventId.make("wight:planned-message"),
              payload: {
                ...event.payload,
                id: MessageId.make("wight:planned-message"),
                runId: RunId.make("wight:queued"),
                text: "Continue.",
              },
            },
          ];
        return [];
      });
      assert.isTrue(events.some((event) => event.type === "run.created"));
      assert.isTrue(events.some((event) => event.type === "message.updated"));
      // The immutable plan is complete. Only now does the projection change, before commitCommand.
      const thread = yield* projections.getThread(threadId);
      if (changed)
        yield* projections.apply({
          id: EventId.make("wight:after-plan"),
          type: "thread.metadata-updated",
          threadId,
          providerInstanceId: instanceId,
          occurredAt: yield* DateTime.now,
          payload: {
            ...thread,
            updatedAt: DateTime.makeUnsafe(DateTime.toEpochMillis(thread.updatedAt) + 1),
          },
        });
      const before = yield* orchestrator.getThreadProjection(threadId);
      const result = yield* sink
        .commitCommand({
          commandId: command.commandId,
          threadId,
          commandType: "message.dispatch",
          acceptedAt: yield* DateTime.now,
          events,
          effects: [
            {
              id: "effect:wight:planned",
              commandId: command.commandId,
              threadId,
              request: { type: "terminal.cleanup" },
            },
          ],
          forkPlans: [plan],
        })
        .pipe(Effect.result);
      const after = yield* orchestrator.getThreadProjection(threadId);
      if (!changed) {
        if (result._tag === "Failure") return yield* Effect.die(result.failure);
        assert.equal(result._tag, "Success");
        assert.equal(after.messages.length, before.messages.length + 1);
        assert.isTrue(
          after.runs.some((run) => run.id === "wight:queued" && run.status === "queued"),
        );
        assert.equal((yield* outbox.listByCommandId(command.commandId)).length, 1);
        return;
      }
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") assert.equal(result.failure._tag, "ForkCommitGuardRejected");
      assert.deepEqual(after.messages, before.messages);
      assert.deepEqual(after.runs, before.runs);
      assert.isFalse(after.runs.some((run) => run.status === "queued"));
      assert.deepEqual(
        yield* sql`SELECT status FROM orchestration_command_receipts WHERE command_id = ${command.commandId}`,
        [],
      );
      assert.deepEqual(yield* outbox.listByCommandId(command.commandId), []);
      assert.deepEqual(
        Array.from(
          yield* sink.readByCommandId({ commandId: command.commandId }).pipe(Stream.runCollect),
        ),
        [],
      );
    }).pipe(Effect.provide(layer)),
);
