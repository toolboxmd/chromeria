import { assert, describe, it } from "@effect/vitest";
import {
  DEFAULT_SERVER_SETTINGS,
  ProviderDriverKind,
  ProviderInstanceId,
  ServerProviderUpdateError,
  ThreadId,
  EventId,
  ProviderSessionId,
  type OrchestrationV2StoredEvent,
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
import { ProviderRegistry } from "./ProviderRegistry.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as SqlClient from "effect/sql/SqlClient";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as DateTime from "effect/DateTime";

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

function harness(input: {
  readonly settings?: Partial<ServerSettings>;
  readonly providers: Array<ServerProvider>;
  readonly busyDrivers?: Array<string>;
  readonly fail?: boolean;
}) {
  const state = {
    settings: { ...DEFAULT_SERVER_SETTINGS, autoUpdateProviders: true, ...input.settings },
    providers: input.providers,
    busyDrivers: input.busyDrivers ?? [],
    calls: [] as Array<string>,
  };
  const updater = makeProviderAutoUpdater({
    getSettings: Effect.sync(() => state.settings),
    getProviders: Effect.sync(() => state.providers),
    getBusyDrivers: Effect.sync(() => new Set(state.busyDrivers)),
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
        busyDrivers: [CODEX],
      });
      const { evaluate } = updater;
      yield* evaluate;
      assert.deepStrictEqual(state.calls, ["claudeAgent"]);

      state.busyDrivers = [];
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

  it.effect("the layer runs a deferred update when V2 provider work ends", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`INSERT INTO orchestration_v2_projection_provider_sessions
        (provider_session_id, provider, status, updated_at, payload_json)
        VALUES ('session', 'codex', 'running', '2026-10-07T00:00:00Z', '{}')`;
      const firstPass = yield* Deferred.make<void>();
      const updated = yield* Deferred.make<string>();
      const events = yield* Queue.unbounded<OrchestrationV2StoredEvent>();
      const deps = Layer.mergeAll(
        ServerSettingsModule.layerTest({ autoUpdateProviders: true }),
        Layer.succeed(SqlClient.SqlClient, sql),
        Layer.mock(ProviderRegistry)({
          getProviders: Effect.succeed([outdated(CODEX)]).pipe(
            Effect.tap(() => Deferred.succeed(firstPass, undefined)),
          ),
          streamChanges: Stream.never,
        }),
        Layer.mock(EventSink.EventSinkV2)({ stream: () => Stream.fromQueue(events) }),
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
      yield* sql`UPDATE orchestration_v2_projection_provider_sessions SET status = 'ready' WHERE provider_session_id = 'session'`;
      const now = yield* DateTime.now;
      yield* Queue.offer(events, {
        sequence: 1,
        commandId: null,
        event: {
          id: EventId.make("terminal"),
          type: "provider-session.detached",
          threadId: ThreadId.make("thread"),
          providerInstanceId: ProviderInstanceId.make("codex"),
          occurredAt: now,
          payload: { providerSessionId: ProviderSessionId.make("session"), detachedAt: now },
        },
      });
      assert.strictEqual(yield* Deferred.await(updated), "codex");
    }).pipe(Effect.scoped, Effect.provide(Layer.orDie(SqlitePersistence.layerMemory))),
  );
});
