import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import { ForkCommitGuardRejected } from "../childThreads/ForkCommitPlan.ts";
import { ForkDispatchPlans } from "../fork/ForkDispatchPlans.ts";
import * as CommandReceipts from "../orchestration-v2/CommandReceiptStore.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as Projection from "../orchestration-v2/ProjectionStore.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import type { ProviderAdapterV2Shape } from "../orchestration-v2/ProviderAdapter.ts";
import * as Adapters from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Harness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as Database from "../persistence/Sqlite.ts";
import * as Providers from "../provider/ProviderRegistry.ts";
import { buildUnavailableProviderSnapshot } from "../provider/unavailableProviderSnapshot.ts";
import * as Settings from "../serverSettings.ts";
import * as Prism from "../prism/PrismService.ts";
import * as Launch from "./LaunchService.ts";
import * as Round from "./RoundService.ts";
import { applySpectrumMutation, ensureSpectrumSchema, readSpectrum } from "./store.ts";
import { makeThread, NOW } from "./testFixtures.ts";

const instanceId = ProviderInstanceId.make("codex");
const selection = { instanceId, model: "test-model" };
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("No provider process runs in this test"),
} as ProviderAdapterV2Shape;
const adapters = Adapters.layerFromAdapters([adapter]);
const database = Database.layerMemory;
const registry = Layer.mock(Providers.ProviderRegistry)({
  getProviders: Effect.map(
    buildUnavailableProviderSnapshot({ driverKind: "codex", instanceId, reason: "fixture" }),
    (p) => [
      {
        ...p,
        enabled: true,
        installed: true,
        status: "ready" as const,
        availability: "available" as const,
        models: [{ slug: "test-model", name: "Test", isCustom: false, capabilities: null }],
      },
    ],
  ),
});
const prism = Prism.layer.pipe(
  Layer.provide(
    Layer.mergeAll(
      registry,
      Settings.layerTest({
        prismRoles: { reviewer: { instructions: "Use the reviewer kit.", models: [selection] } },
      }),
    ),
  ),
);
const base = Layer.mergeAll(
  database,
  adapters,
  registry,
  prism,
  CommandReceipts.layer.pipe(Layer.provide(database)),
  Harness.layerWithRegistry({ name: "spectrum-launch" }, adapters, {
    databaseLayer: database,
    runEffectWorker: false,
  }),
);
const runtime = Layer.fresh(
  Layer.mergeAll(Launch.layer, Round.layer).pipe(Layer.provideMerge(base)),
);
const input: Launch.SpectrumStart = {
  commandId: CommandId.make("register:spectrum"),
  threadId: ThreadId.make("spectrum:launch"),
  callerThreadId: ThreadId.make("caller"),
  callerRunId: null,
  question: "How should we build?",
  mode: "council",
  limit: 2,
  moderator: 1,
  colors: [
    { label: "Blue", selection },
    { label: "Red", role: "reviewer" },
  ],
};
const setup = Effect.gen(function* () {
  yield* ensureSpectrumSchema;
  const sink = yield* EventSink.EventSinkV2;
  const caller = {
    ...makeThread(),
    id: input.callerThreadId,
    owner: "alice",
    lineage: {
      parentThreadId: null,
      relationshipToParent: null,
      rootThreadId: input.callerThreadId,
    },
  } as const;
  yield* sink.write({
    events: [
      {
        id: EventId.make("caller:create"),
        type: "thread.created",
        threadId: caller.id,
        occurredAt: NOW,
        payload: caller,
      },
    ],
  });
  return caller;
});

it.effect("resolves Colors through Prism, inherits ownership and launches exact lineage runs", () =>
  Effect.gen(function* () {
    yield* setup;
    const launch = yield* Launch.SpectrumLaunchService;
    const state = yield* launch.register(input);
    assert.strictEqual(state.participants[1]!.instructions, "Use the reviewer kit.");
    assert.deepStrictEqual(yield* launch.register(input), state);
    const projections = yield* Projection.ProjectionStoreV2;
    for (const color of state.participants) {
      const child = yield* projections.getThread(color.threadId);
      assert.strictEqual(child.owner, "alice");
      assert.strictEqual(child.lineage.parentThreadId, state.threadId);
    }
    const spectrum = yield* projections.getThread(state.threadId);
    assert.strictEqual(spectrum.activeProviderThreadId, null);
    const rounds = yield* Round.SpectrumRoundService;
    const prepared = yield* rounds.prepare(state.threadId);
    for (const command of prepared.outbox)
      yield* rounds.dispatch(state.threadId, command.commandId);
    const current = Option.getOrThrow(yield* readSpectrum(state.threadId));
    assert.deepStrictEqual(current.outbox, []);
    assert.isTrue(current.round!.slots.every((slot) => slot.runId !== null));
    assert.deepStrictEqual(
      (yield* projections.getThreadRecords(state.threadId, ["runs"])).runs,
      [],
    );
  }).pipe(Effect.provide(runtime)),
);

it.effect("scoped admission cannot leak into concurrent dispatches or after failure", () =>
  Effect.gen(function* () {
    yield* setup;
    const launch = yield* Launch.SpectrumLaunchService;
    yield* launch.register(input);
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const refusal = {
      guards: [
        Effect.fail(
          new ForkCommitGuardRejected({ threadId: input.threadId, kind: "state_conflict" }),
        ),
      ],
      mutations: [],
    };
    const results = yield* Effect.all(
      [
        orchestrator
          .dispatch({
            type: "thread.visit",
            visitedAt: DateTime.formatIso(NOW),
            commandId: CommandId.make("refused"),
            threadId: input.threadId,
          })
          .pipe(Effect.provideService(ForkDispatchPlans, [refusal]), Effect.result),
        orchestrator
          .dispatch({
            type: "thread.visit",
            visitedAt: DateTime.formatIso(NOW),
            commandId: CommandId.make("unrelated"),
            threadId: input.callerThreadId,
          })
          .pipe(Effect.result),
      ],
      { concurrency: "unbounded" },
    );
    assert.strictEqual(results[0]._tag, "Failure");
    assert.strictEqual(results[1]._tag, "Success", JSON.stringify(results[1]));
    yield* orchestrator.dispatch({
      type: "thread.visit",
      visitedAt: DateTime.formatIso(NOW),
      commandId: CommandId.make("after-failure"),
      threadId: input.threadId,
    });
    assert.deepStrictEqual(yield* ForkDispatchPlans, []);
  }).pipe(Effect.provide(runtime)),
);

it.effect("rebuilds outbox admission after restart and retains native retirement admission", () =>
  Effect.gen(function* () {
    const caller = yield* setup;
    const launch = yield* Launch.SpectrumLaunchService;
    yield* launch.register(input);
    const rounds = yield* Round.SpectrumRoundService;
    const prepared = yield* rounds.prepare(input.threadId);
    const command = prepared.outbox[0]!;
    assert.isTrue(command.type === "message.dispatch");
    if (command.type !== "message.dispatch") return;
    yield* applySpectrumMutation({
      expectedRevision: prepared.revision,
      expectedGeneration: prepared.generation,
      state: { ...prepared, generation: prepared.generation + 1, revision: prepared.revision + 1 },
    });
    // New service instance has no persisted fiber context. It must derive the guard from SQL again.
    yield* Effect.gen(function* () {
      const fresh = yield* Round.SpectrumRoundService;
      yield* fresh.dispatch(input.threadId, command.commandId).pipe(Effect.flip);
    }).pipe(Effect.provide(Round.layer));
    const state = Option.getOrThrow(yield* readSpectrum(input.threadId));
    assert.deepStrictEqual(state.outbox, prepared.outbox);
    const sink = yield* EventSink.EventSinkV2;
    yield* sink.write({
      events: [
        {
          id: EventId.make("caller:stop"),
          type: "thread.metadata-updated",
          threadId: caller.id,
          occurredAt: NOW,
          payload: { ...caller, forkRetirement: { token: CommandId.make("stop") } },
        },
      ],
    });
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    yield* orchestrator
      .dispatch(command)
      .pipe(Effect.provideService(ForkDispatchPlans, []), Effect.flip);
    const projections = yield* Projection.ProjectionStoreV2;
    assert.deepStrictEqual(
      (yield* projections.getThreadRecords(command.threadId, ["runs"])).runs,
      [],
    );
  }).pipe(Effect.provide(runtime)),
);
