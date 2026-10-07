import { assert, describe, it } from "@effect/vitest";
import {
  DEFAULT_SERVER_SETTINGS,
  ProviderDriverKind,
  ProviderInstanceId,
  ServerProviderUpdateError,
  ThreadId,
  type ProviderSession,
  type ServerProvider,
  type ServerSettings,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import { makeProviderAutoUpdater } from "./providerAutoUpdate.ts";

const CODEX = ProviderDriverKind.make("codex");
const CLAUDE = ProviderDriverKind.make("claudeAgent");

function outdated(
  driver: ProviderDriverKind,
  overrides: Partial<NonNullable<ServerProvider["versionAdvisory"]>> = {},
): ServerProvider {
  return {
    instanceId: ProviderInstanceId.make(driver),
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
});
