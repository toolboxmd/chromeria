import { assert, describe, it } from "@effect/vitest";
import {
  DEFAULT_SERVER_SETTINGS,
  ProviderDriverKind,
  ProviderInstanceId,
  ServerProviderUpdateError,
  type ServerProvider,
  type ServerSettings,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";

import * as ServerSettingsModule from "../serverSettings.ts";
import * as ProviderAutoUpdate from "./providerAutoUpdate.ts";
import { makeProviderAutoUpdater } from "./providerAutoUpdate.ts";
import * as ProviderMaintenanceRunner from "./providerMaintenanceRunner.ts";
import type { ProviderMaintenanceCommandAction } from "@t3tools/provider-core/server/maintenanceResolver";
import { ProviderRegistry } from "./ProviderRegistry.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as SqlClient from "effect/sql/SqlClient";

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

const sharedAction: ProviderMaintenanceCommandAction = {
  command: "brew upgrade codex",
  executable: "/opt/homebrew/bin/brew",
  args: ["upgrade", "codex"],
  lockKey: "homebrew",
};
const maintenanceCapabilities = (driver: ServerProvider["driver"], action = sharedAction) => ({
  provider: driver,
  packageName: "fixture-package",
  update: action,
});

function harness(input: {
  readonly settings?: Partial<ServerSettings>;
  readonly providers: Array<ServerProvider>;
  readonly fail?: boolean;
  readonly actions?: Readonly<Record<string, ProviderMaintenanceCommandAction>>;
}) {
  const state = {
    settings: { ...DEFAULT_SERVER_SETTINGS, autoUpdateProviders: true, ...input.settings },
    providers: input.providers,
    calls: [] as Array<string>,
  };
  const updater = makeProviderAutoUpdater({
    getSettings: Effect.sync(() => state.settings),
    getProviders: Effect.sync(() => state.providers),
    getMaintenanceCapabilities: (instanceId, driver) =>
      Effect.succeed(maintenanceCapabilities(driver, input.actions?.[instanceId] ?? sharedAction)),
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

  it.effect.each([
    {
      name: "Homebrew prefixes with the same display label",
      change: { executable: "/usr/local/bin/brew" },
    },
    { name: "arguments", change: { args: ["upgrade", "--cask", "codex"] } },
    { name: "environment", change: { env: { CODEX_HOME: "/other/install" } } },
    { name: "install target", change: { lockKey: "other-install" } },
  ])("skips distinct resolved $name", ({ change }) =>
    Effect.gen(function* () {
      const { state, updater } = harness({
        providers: [outdated(CODEX), outdated(CODEX, {}, "codex-work")],
        actions: { codex: sharedAction, "codex-work": { ...sharedAction, ...change } },
      });
      yield* updater.evaluate;
      yield* updater.evaluate;
      assert.deepStrictEqual(state.calls, []);
    }),
  );

  it.effect("updates identical resolved targets exactly once despite environment key order", () =>
    Effect.gen(function* () {
      const { state, updater } = harness({
        providers: [outdated(CODEX), outdated(CODEX, {}, "codex-work")],
        actions: {
          codex: { ...sharedAction, env: { CODEX_HOME: "/shared/install", PATH: "/shared/bin" } },
          "codex-work": {
            ...sharedAction,
            env: { PATH: "/shared/bin", CODEX_HOME: "/shared/install" },
          },
        },
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

  it.effect.each(["running", "queued"] as const)(
    "the opted-in trigger calls updateProvider while provider work is %s",
    (status) =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const sessionStatus = status === "running" ? "running" : "ready";
        yield* sql`INSERT INTO orchestration_v2_projection_provider_sessions
          (provider_session_id, provider, status, updated_at, payload_json)
          VALUES ('session', 'claudeAgent', ${sessionStatus}, '2026-10-07T00:00:00Z', '{}')`;
        yield* sql`INSERT INTO orchestration_v2_projection_runs
          (run_id, thread_id, ordinal, provider, status, requested_at, payload_json)
          VALUES ('run', 'thread', 1, 'claudeAgent', ${status}, '2026-10-07T00:00:00Z', '{}')`;
        const updated = yield* Deferred.make<unknown>();
        const deps = Layer.mergeAll(
          ServerSettingsModule.layerTest({ autoUpdateProviders: true }),
          Layer.mock(ProviderRegistry)({
            getProviders: Effect.succeed([outdated(CLAUDE)]),
            getProviderMaintenanceCapabilitiesForInstance: (_instanceId, driver) =>
              Effect.succeed(maintenanceCapabilities(driver)),
            streamChanges: Stream.never,
          }),
          Layer.mock(ProviderMaintenanceRunner.ProviderMaintenanceRunner)({
            updateProvider: (target) =>
              Deferred.succeed(updated, target).pipe(Effect.as({ providers: [] })),
          }),
        );
        yield* Layer.build(ProviderAutoUpdate.layer.pipe(Layer.provide(deps)));
        assert.deepStrictEqual(yield* Deferred.await(updated), {
          provider: CLAUDE,
          instanceId: ProviderInstanceId.make("claudeAgent"),
        });
        const runs = yield* sql<{
          status: string;
        }>`SELECT status FROM orchestration_v2_projection_runs`;
        assert.deepStrictEqual(runs, [{ status }]);
      }).pipe(Effect.scoped, Effect.provide(Layer.orDie(SqlitePersistence.layerMemory))),
  );
});
