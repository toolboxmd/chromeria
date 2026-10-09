import {
  CommandId,
  EventId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as CommandReceipts from "../orchestration-v2/CommandReceiptStore.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import type { ProviderAdapterV2Shape } from "../orchestration-v2/ProviderAdapter.ts";
import * as Adapters from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Harness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as Database from "../persistence/Sqlite.ts";
import * as Providers from "../provider/ProviderRegistry.ts";
import { buildUnavailableProviderSnapshot } from "../provider/unavailableProviderSnapshot.ts";
import * as Settings from "../serverSettings.ts";
import * as RecoveryStore from "../prism/RecoveryStore.ts";
import * as Prism from "../prism/PrismService.ts";
import * as Launch from "./LaunchService.ts";
import { ensureSpectrumSchema } from "./store.ts";
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
export const base = Layer.mergeAll(
  database,
  EventStore.layer.pipe(Layer.provide(database)),
  RecoveryStore.layer.pipe(Layer.provide(database)),
  adapters,
  registry,
  prism,
  CommandReceipts.layer.pipe(Layer.provide(database)),
  Harness.layerWithRegistry({ name: "spectrum-launch" }, adapters, {
    databaseLayer: database,
    runEffectWorker: false,
  }),
);
export const input: Launch.SpectrumStart = {
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
export const setup = Effect.gen(function* () {
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
