import * as BareProjects from "../orchestration-v2/ProjectStore.ts";
import * as BareLaunch from "../orchestration-v2/BareLaunch.ts";
import * as GitWorkflow from "../git/GitWorkflowService.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  type OrchestrationV2LinkOrigin,
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
import type { ProviderAdapterV2 } from "@t3tools/provider-core/server/ProviderAdapter";
import * as Adapters from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Harness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as Database from "../persistence/Sqlite.ts";
import * as Providers from "../provider/ProviderRegistry.ts";
import { buildUnavailableProviderSnapshot } from "../provider/unavailableProviderSnapshot.ts";
import * as Settings from "../serverSettings.ts";
import * as Prism from "../prism/PrismService.ts";
import * as Launch from "./LaunchService.ts";
import { assertSameLink, assertNotLinked } from "../mcp/linkOrigin.ts";
import * as Invocation from "../mcp/McpInvocationContext.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as Forwarding from "../peer/PeerForwarding.ts";
import * as PeerLinks from "../peer/PeerLinks.ts";
import * as PeerMcpClient from "../peer/PeerMcpClient.ts";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { EnvironmentId } from "@t3tools/contracts";
import { OrchestratorToolkit } from "../mcp/toolkits/orchestrator/tools.ts";
import * as Round from "./RoundService.ts";
import { applySpectrumMutation, ensureSpectrumSchema, readSpectrum } from "./store.ts";
import { makeThread, NOW } from "./testFixtures.ts";

const instanceId = ProviderInstanceId.make("codex");
const selection = { instanceId, model: "test-model" };
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  bareSupport: Effect.succeed({ supported: true }),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("No provider process runs in this test"),
} satisfies ProviderAdapterV2["Service"];
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
  Settings.layerTest(),
  Layer.mock(BareProjects.ProjectStoreV2)({}),
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
  Layer.mergeAll(
    Launch.layer.pipe(Layer.provide(Layer.mock(BareLaunch.BareLaunch)({}))),
    Round.layer,
  ).pipe(Layer.provideMerge(base)),
);
const input: Launch.SpectrumStartRequest = {
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
const setupWithOrigin = (linkOrigin?: OrchestrationV2LinkOrigin) =>
  Effect.gen(function* () {
    yield* ensureSpectrumSchema;
    const sink = yield* EventSink.EventSinkV2;
    const caller = {
      ...makeThread(),
      id: input.callerThreadId,
      owner: "alice",
      ...(linkOrigin === undefined ? {} : { linkOrigin }),
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

const setup = setupWithOrigin();

it.effect("bare Colors persist their additions and ceiling without a Prism kit", () =>
  Effect.gen(function* () {
    const caller = yield* setup;
    yield* (yield* EventSink.EventSinkV2).write({
      events: [
        {
          id: EventId.make("caller:restricted-workspace"),
          type: "thread.metadata-updated",
          threadId: caller.id,
          occurredAt: NOW,
          payload: { ...caller, runtimeMode: "approval-required", worktreePath: "/workspace" },
        },
      ],
    });
    const launch = yield* Launch.SpectrumLaunchService;
    const state = yield* launch.register({
      ...input,
      colors: [
        { label: "Bare", role: "reviewer", bare: {}, instructions: "Only this task input." },
        { label: "Ordinary", role: "reviewer" },
      ],
    });
    const projections = yield* Projection.ProjectionStoreV2;
    const bare = yield* projections.getThread(state.participants[0]!.threadId);
    assert.deepStrictEqual(bare.bare?.additions, []);
    assert.strictEqual(bare.bare?.nativeHomeOwnerThreadId, bare.id);
    assert.strictEqual(bare.runtimeMode, "approval-required");
    assert.strictEqual(state.participants[0]!.instructions, "Only this task input.");
    assert.strictEqual(state.participants[1]!.instructions, "Use the reviewer kit.");
    assert.isUndefined((yield* projections.getThread(state.participants[1]!.threadId)).bare);
  }).pipe(
    Effect.provide(
      Launch.layer.pipe(
        Layer.provideMerge(
          Layer.mergeAll(
            base,
            BareLaunch.layer.pipe(
              Layer.provide(
                Layer.mergeAll(
                  adapters,
                  registry,
                  Layer.mock(GitWorkflow.GitWorkflowService)({}),
                  NodeServices.layer,
                ),
              ),
            ),
          ),
        ),
      ),
    ),
  ),
);

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

it.effect("legacy Color instance and model overrides keep Prism role instructions", () =>
  Effect.gen(function* () {
    yield* setup;
    const launch = yield* Launch.SpectrumLaunchService;
    const state = yield* launch.register({
      ...input,
      title: "My council",
      colors: [
        { label: "Blue", instanceId: "codex", model: "test-model" },
        { label: "Red", role: "reviewer", instanceId: "codex", model: "test-model" },
      ],
    });
    assert.strictEqual(state.participants[1]!.instructions, "Use the reviewer kit.");
    for (const color of state.participants) {
      assert.strictEqual(color.selection.instanceId, "codex");
      assert.strictEqual(color.selection.model, "test-model");
    }
    const projections = yield* Projection.ProjectionStoreV2;
    assert.strictEqual((yield* projections.getThread(state.threadId)).title, "My council");
  }).pipe(Effect.provide(runtime)),
);

it.effect("persists peer origin on the Spectrum and Colors, fencing each descendant", () =>
  Effect.gen(function* () {
    const origin = { sessionId: "peer-session", label: "Peer" };
    yield* setupWithOrigin(origin);
    const state = yield* (yield* Launch.SpectrumLaunchService).register(input);
    const projections = yield* Projection.ProjectionStoreV2;
    for (const id of [state.threadId, ...state.participants.map((p) => p.threadId)]) {
      const child = yield* projections.getThread(id);
      assert.deepStrictEqual(child.linkOrigin, origin);
      const scope: Invocation.McpInvocationScope = {
        environmentId: EnvironmentId.make("destination"),
        thread: {
          threadId: id,
          providerInstanceId: child.providerInstanceId,
          providerSessionId: "color",
        },
        client: undefined,
        capabilities: new Set(["orchestration"]),
        requestNamespace: "color",
        issuedAt: 1,
      };
      const shell = yield* (yield* Orchestrator.OrchestratorV2).getThreadShell(id);
      if (shell === null) return yield* Effect.die("Persisted Spectrum shell missing");
      const context = { scope, caller: shell };
      const foreign = { ...child, linkOrigin: undefined };
      assert.strictEqual(
        (yield* assertSameLink(context, foreign).pipe(Effect.flip)).code,
        "capability_denied",
      );
      assert.strictEqual(
        (yield* assertNotLinked(context, "change projects").pipe(Effect.flip)).code,
        "capability_denied",
      );
      const denied = yield* Effect.gen(function* () {
        const forwarding = yield* Forwarding.PeerForwarding;
        return yield* forwarding.call(
          scope,
          OrchestratorToolkit.tools.t3_thread_list,
          EnvironmentId.make("third-machine"),
          {},
        );
      }).pipe(
        Effect.provide(
          Forwarding.layer.pipe(
            Layer.provide(
              Layer.mergeAll(
                NodeCrypto.layer,
                Layer.mock(ThreadManagement.ThreadManagementService)({
                  getThreadShell: () => Effect.succeed(shell),
                }),
                Layer.mock(PeerLinks.PeerLinks)({
                  get: () => Effect.die("Must not use onward links"),
                }),
                Layer.mock(PeerMcpClient.PeerMcpClient)({
                  call: () => Effect.die("Must not reach an onward peer"),
                }),
              ),
            ),
          ),
        ),
        Effect.provideService(Invocation.McpInvocationContext, scope),
        Effect.flip,
      );
      assert.strictEqual(denied.code, "capability_denied");
    }
  }).pipe(Effect.provide(runtime)),
);
