import { expect, it } from "@effect/vitest";
import {
  CommandId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ModelSelection,
  type ServerProvider,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as SqlClient from "effect/sql/SqlClient";
import * as PromachosLaunch from "./PromachosLaunch.ts";
import * as Prism from "../prism/PrismService.ts";
import * as ThreadLaunch from "../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as CommandReceiptStore from "../orchestration-v2/CommandReceiptStore.ts";
import type * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import type * as AdapterRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import type * as ServerSettings from "../serverSettings.ts";

type Services =
  | ThreadLaunch.ThreadLaunchService
  | ThreadManagement.ThreadManagementService
  | ProviderRegistry.ProviderRegistry
  | AdapterRegistry.ProviderAdapterRegistryV2
  | ServerSettings.ServerSettingsService
  | SqlClient.SqlClient;

/** Register fork launch behavior against the upstream receipt/SQL harness without duplicating it. */
export function registerPromachosLaunchTests<E>({
  makeHarness,
  launchInput,
  modelSelection,
}: {
  makeHarness: (options?: {
    providers?: ReadonlyArray<ServerProvider>;
    serverSettings?: Parameters<typeof ServerSettings.layerTest>[0];
  }) => { layer: Layer.Layer<Services, E> };
  launchInput: (input: {
    command: string;
    thread: string;
    message?: string;
  }) => ThreadLaunch.ThreadLaunchInput;
  modelSelection: ModelSelection;
}) {
  const promachosProvider: ServerProvider = {
    instanceId: modelSelection.instanceId,
    driver: ProviderDriverKind.make("codex"),
    enabled: true,
    installed: true,
    version: null,
    status: "ready",
    auth: { status: "authenticated" },
    checkedAt: "2026-10-08T00:00:00.000Z",
    availability: "available",
    models: [
      { slug: modelSelection.model, name: "Promachos", isCustom: false, capabilities: null },
    ],
    slashCommands: [],
    skills: [],
  };
  const layerPromachos = PromachosLaunch.layer.pipe(
    Layer.provide(Prism.layer),
    Layer.provide(CommandReceiptStore.layer),
  );
  it.effect(
    "Promachos launch persists the Prism model and kit once, then replays its receipt",
    () => {
      const harness = makeHarness({
        providers: [promachosProvider],
        serverSettings: {
          prismRoles: {
            promachos: {
              models: [{ ...modelSelection, effort: "high" }],
              instructions: "Read the home persona.",
            },
          },
        },
      });
      return Effect.gen(function* () {
        const promachos = yield* PromachosLaunch.PromachosLaunch;
        const launches = yield* ThreadLaunch.ThreadLaunchService;
        const input = {
          ...launchInput({
            command: "promachos:first",
            thread: "promachos:first",
            message: "Hello",
          }),
          modelSelection: { ...modelSelection, model: "placeholder" },
          prismRole: "promachos" as const,
        };
        const first = yield* launches.launch(yield* promachos.prepare(input));
        expect(first.projection.thread.modelSelection).toEqual({
          ...modelSelection,
          options: [{ id: "reasoningEffort", value: "high" }],
        });
        expect(first.projection.messages.map((message) => message.text)).toEqual([
          "Read the home persona.\n\nHello",
        ]);
        const second = yield* launches.launch(yield* promachos.prepare(input));
        expect(second.resumed).toBe(true);
        expect(second.projection.messages).toHaveLength(1);
        expect(second.projection.runs).toHaveLength(1);
        expect(second.projection.thread.modelSelection).toEqual(
          first.projection.thread.modelSelection,
        );
      }).pipe(Effect.provide(layerPromachos.pipe(Layer.provideMerge(harness.layer))));
    },
  );
  it.effect(
    "Promachos launch falls through an unavailable first adapter before creating a thread",
    () => {
      const unavailable = ProviderInstanceId.make("unavailable-adapter");
      const harness = makeHarness({
        providers: [{ ...promachosProvider, instanceId: unavailable }, promachosProvider],
        serverSettings: {
          prismRoles: {
            promachos: { models: [{ ...modelSelection, instanceId: unavailable }, modelSelection] },
          },
        },
      });
      return Effect.gen(function* () {
        const promachos = yield* PromachosLaunch.PromachosLaunch;
        const threads = yield* ThreadManagement.ThreadManagementService;
        const launches = yield* ThreadLaunch.ThreadLaunchService;
        const input = {
          ...launchInput({
            command: "promachos:fallback",
            thread: "promachos:fallback",
            message: "Hello",
          }),
          prismRole: "promachos" as const,
        };
        expect(yield* threads.getThreadShell(input.threadId!)).toBeNull();
        const prepared = yield* promachos.prepare(input);
        expect(prepared.modelSelection).toEqual(modelSelection);
        expect(yield* threads.getThreadShell(input.threadId!)).toBeNull();
        const launched = yield* launches.launch(prepared);
        expect(launched.projection.thread.modelSelection).toEqual(modelSelection);
        expect(launched.projection.messages.map((message) => message.text)).toEqual(["Hello"]);
      }).pipe(Effect.provide(layerPromachos.pipe(Layer.provideMerge(harness.layer))));
    },
  );
  it.effect(
    "Promachos launch refuses an unavailable configured model without a normal launch",
    () => {
      const harness = makeHarness({
        providers: [],
        serverSettings: { prismRoles: { promachos: { models: [modelSelection] } } },
      });
      return Effect.gen(function* () {
        const promachos = yield* PromachosLaunch.PromachosLaunch;
        const threads = yield* ThreadManagement.ThreadManagementService;
        const input = {
          ...launchInput({
            command: "promachos:refused",
            thread: "promachos:refused",
            message: "Hello",
          }),
          prismRole: "promachos" as const,
        };
        expect((yield* promachos.prepare(input).pipe(Effect.flip)).reason).toBe("routing");
        expect(yield* threads.getThreadShell(input.threadId!)).toBeNull();
      }).pipe(Effect.provide(layerPromachos.pipe(Layer.provideMerge(harness.layer))));
    },
  );
  it.effect("Promachos launch cannot reroute an existing conversation or a reusable thread", () => {
    const harness = makeHarness();
    return Effect.gen(function* () {
      const launches = yield* ThreadLaunch.ThreadLaunchService;
      const promachos = yield* PromachosLaunch.PromachosLaunch;
      const input = launchInput({
        command: "ordinary:first",
        thread: "ordinary:first",
        message: "Hello",
      });
      yield* launches.launch(input);
      const reroute = {
        ...input,
        commandId: CommandId.make("promachos:reroute"),
        prismRole: "promachos" as const,
      };
      expect((yield* promachos.prepare(reroute).pipe(Effect.flip)).reason).toBe("initial-only");
      expect(
        (yield* promachos.prepare({ ...reroute, reuseExistingThread: true }).pipe(Effect.flip))
          .reason,
      ).toBe("initial-only");
    }).pipe(Effect.provide(layerPromachos.pipe(Layer.provideMerge(harness.layer))));
  });
}
