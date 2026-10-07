import { assert, describe, it } from "@effect/vitest";
import {
  DEFAULT_SERVER_SETTINGS,
  ProviderDriverKind,
  ProviderInstanceId,
  ServerProviderUpdateError,
  ThreadId,
  type ProviderRuntimeEvent,
  type ProviderSession,
  type ServerProvider,
  type ServerSettings,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";

import * as ServerSettingsModule from "../serverSettings.ts";
import * as ProviderAutoUpdate from "./providerAutoUpdate.ts";
import { makeProviderAutoUpdater } from "./providerAutoUpdate.ts";
import * as ProviderMaintenanceRunner from "./providerMaintenanceRunner.ts";
import { ProviderRegistry } from "./Services/ProviderRegistry.ts";
import { ProviderService } from "./Services/ProviderService.ts";

const CODEX = ProviderDriverKind.make("codex");
const CLAUDE = ProviderDriverKind.make("claudeAgent");

function outdated(
  driver: ProviderDriverKind,
  overrides: Partial<NonNullable<ServerProvider["versionAdvisory"]>> = {},
  instanceId: string = driver,
): ServerProvider {
  return {
    instanceId: ProviderInstanceId.make(instanceId),
    driver,
    enabled: true,
    installed: true,
    version: "1.0.0",
    status: "ready",
    auth: { status: "authenticated" },
    checkedAt: "2026-10-07T00:00:00.000Z",
    models: [],
    slashCommands: [],
    skills: [],
    versionAdvisory: {
      status: "behind_latest",
      currentVersion: "1.0.0",
      latestVersion: "1.1.0",
      updateCommand: "npm install -g pkg@latest",
      canUpdate: true,
      checkedAt: "2026-10-07T00:00:00.000Z",
      message: null,
      ...overrides,
    },
  };
}

function runningSession(driver: ProviderDriverKind): ProviderSession {
  return {
    provider: driver,
    providerInstanceId: ProviderInstanceId.make(driver),
    status: "running",
    runtimeMode: "full-access",
    threadId: ThreadId.make("thread-1"),
    createdAt: "2026-10-07T00:00:00.000Z",
    updatedAt: "2026-10-07T00:00:00.000Z",
  };
}

function harness(input: {
  readonly settings?: Partial<ServerSettings>;
  readonly providers: Array<ServerProvider>;
  readonly sessions?: Array<ProviderSession>;
  readonly fail?: boolean;
}) {
  const state = {
    settings: { ...DEFAULT_SERVER_SETTINGS, autoUpdateProviders: true, ...input.settings },
    providers: input.providers,
    sessions: input.sessions ?? [],
    calls: [] as Array<string>,
  };
  const updater = makeProviderAutoUpdater({
    getSettings: Effect.sync(() => state.settings),
    getProviders: Effect.sync(() => state.providers),
    listSessions: Effect.sync(() => state.sessions),
    updateProvider: (target) =>
      Effect.suspend(() => {
        if (typeof target === "string") return Effect.die("expected an instance target");
        state.calls.push(`${target.instanceId}`);
        return input.fail
          ? Effect.fail(
              new ServerProviderUpdateError({ provider: target.provider, reason: "boom" }),
            )
          : Effect.succeed({ providers: state.providers });
      }),
  });
  return { state, updater };
}

describe("providerAutoUpdate", () => {
  it.effect("does nothing when the setting is absent (default off)", () =>
    Effect.gen(function* () {
      const { state, updater } = harness({
        settings: { autoUpdateProviders: DEFAULT_SERVER_SETTINGS.autoUpdateProviders },
        providers: [outdated(CODEX)],
      });
      assert.strictEqual(DEFAULT_SERVER_SETTINGS.autoUpdateProviders, false);
      yield* updater.evaluate;
      assert.deepStrictEqual(state.calls, []);
    }),
  );

  it.effect("does nothing when update checks are off", () =>
    Effect.gen(function* () {
      const { state, updater } = harness({
        settings: { enableProviderUpdateChecks: false },
        providers: [outdated(CODEX)],
      });
      yield* updater.evaluate;
      assert.deepStrictEqual(state.calls, []);
    }),
  );

  it.effect("updates an outdated provider once per target version", () =>
    Effect.gen(function* () {
      const { state, updater } = harness({ providers: [outdated(CODEX)] });
      const { evaluate } = updater;
      yield* evaluate;
      yield* evaluate;
      assert.deepStrictEqual(state.calls, ["codex"]);

      state.providers = [outdated(CODEX, { latestVersion: "1.2.0" })];
      yield* evaluate;
      assert.deepStrictEqual(state.calls, ["codex", "codex"]);
    }),
  );

  it.effect("does not retry a failed update for the same version", () =>
    Effect.gen(function* () {
      const { state, updater } = harness({ providers: [outdated(CODEX)], fail: true });
      const { evaluate } = updater;
      yield* evaluate;
      yield* evaluate;
      yield* evaluate;
      assert.deepStrictEqual(state.calls, ["codex"]);
    }),
  );

  it.effect("skips providers without an automatic update command", () =>
    Effect.gen(function* () {
      const { state, updater } = harness({
        providers: [outdated(CODEX, { canUpdate: false, updateCommand: null }), outdated(CLAUDE)],
      });
      yield* updater.evaluate;
      assert.deepStrictEqual(state.calls, ["claudeAgent"]);
    }),
  );

  it.effect("defers an instance mid-turn and updates it once idle", () =>
    Effect.gen(function* () {
      const { state, updater } = harness({
        providers: [outdated(CODEX), outdated(CLAUDE)],
        sessions: [runningSession(CODEX)],
      });
      const { evaluate } = updater;
      yield* evaluate;
      assert.deepStrictEqual(state.calls, ["claudeAgent"]);

      state.sessions = [{ ...runningSession(CODEX), status: "ready" }];
      yield* evaluate;
      assert.deepStrictEqual(state.calls, ["claudeAgent", "codex"]);
    }),
  );
  it.effect("leaves a failed or unchanged update for the user", () =>
    Effect.gen(function* () {
      const failed: ServerProvider = {
        ...outdated(CODEX),
        updateState: {
          status: "failed",
          startedAt: null,
          finishedAt: null,
          message: "Update command exited with code 1.",
          output: null,
        },
      };
      const { state, updater } = harness({ providers: [failed, outdated(CLAUDE)] });
      yield* updater.evaluate;
      assert.deepStrictEqual(state.calls, ["claudeAgent"]);
    }),
  );

  it.effect("runs a shared install command once for all instances of a driver", () =>
    Effect.gen(function* () {
      const { state, updater } = harness({
        providers: [outdated(CODEX), outdated(CODEX, {}, "codex-work")],
      });
      yield* updater.evaluate;
      state.providers = [outdated(CODEX, {}, "codex-work")];
      yield* updater.evaluate;
      assert.deepStrictEqual(state.calls, ["codex"]);
    }),
  );

  it.effect("skips a driver whose instances need different update commands", () =>
    Effect.gen(function* () {
      const { state, updater } = harness({
        providers: [
          outdated(CODEX),
          outdated(CODEX, { updateCommand: "brew upgrade codex" }, "codex-brew"),
        ],
      });
      yield* updater.evaluate;
      assert.deepStrictEqual(state.calls, []);
    }),
  );

  it.effect("the layer runs a deferred update when the turn ends", () =>
    Effect.gen(function* () {
      let sessions = [runningSession(CODEX)];
      const firstPass = yield* Deferred.make<void>();
      const updated = yield* Deferred.make<string>();
      const events = yield* Queue.unbounded<ProviderRuntimeEvent>();
      const deps = Layer.mergeAll(
        ServerSettingsModule.layerTest({ autoUpdateProviders: true }),
        Layer.mock(ProviderRegistry)({
          getProviders: Effect.succeed([outdated(CODEX)]),
          streamChanges: Stream.never,
        }),
        Layer.mock(ProviderService)({
          // Read before signalling, so the first pass cannot see the flipped sessions.
          listSessions: () =>
            Effect.sync(() => sessions).pipe(
              Effect.tap(() => Deferred.succeed(firstPass, undefined)),
            ),
          streamEvents: Stream.fromQueue(events),
        }),
        Layer.mock(ProviderMaintenanceRunner.ProviderMaintenanceRunner)({
          updateProvider: (target) =>
            Deferred.succeed(
              updated,
              typeof target === "string" ? target : (target.instanceId ?? ""),
            ).pipe(Effect.as({ providers: [] })),
        }),
      );
      yield* Layer.build(ProviderAutoUpdate.layer.pipe(Layer.provide(deps)));

      yield* Deferred.await(firstPass);
      assert.isFalse(yield* Deferred.isDone(updated));
      sessions = [{ ...runningSession(CODEX), status: "ready" }];
      yield* Queue.offer(events, { type: "turn.completed" } as unknown as ProviderRuntimeEvent);
      assert.strictEqual(yield* Deferred.await(updated), "codex");
    }).pipe(Effect.scoped),
  );
});
