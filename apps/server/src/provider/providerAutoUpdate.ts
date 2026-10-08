// Fork: opt-in automatic provider updates (toolboxmd/chromeria#159).
import type { ServerProvider, ServerSettings, ServerSettingsError } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";

import * as ServerSettingsModule from "../serverSettings.ts";
import { ProviderRegistry } from "./ProviderRegistry.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as ServerActivation from "../serverActivation.ts";
import { readBusyProviderDrivers, PROVIDER_UPDATE_BUSY_REASON } from "./providerAutoUpdateState.ts";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import { ServerProviderUpdateError } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as ProviderMaintenanceRunner from "./providerMaintenanceRunner.ts";

const isProviderUpdateError = Schema.is(ServerProviderUpdateError);

export interface ProviderAutoUpdateTarget {
  readonly provider: ServerProvider;
  /** Driver, install and target version: each is attempted at most once per server run. */
  readonly attemptKey: string;
}

/**
 * Providers that should be updated now. Applies the update notification's
 * one-click rule per driver: every outdated instance must share one update
 * command, and that command runs once for all of them. A driver is skipped
 * while an update is active or its last result was a failure, and deferred
 * while any of its sessions is mid-turn.
 */
function selectProviderAutoUpdateTargets(input: {
  readonly settings: Pick<ServerSettings, "autoUpdateProviders" | "enableProviderUpdateChecks">;
  readonly providers: ReadonlyArray<ServerProvider>;
  readonly busyDrivers: ReadonlySet<string>;
  readonly attempted: ReadonlySet<string>;
}): Array<ProviderAutoUpdateTarget> {
  if (!input.settings.autoUpdateProviders || !input.settings.enableProviderUpdateChecks) {
    return [];
  }
  const outdatedByDriver = new Map<string, Array<ServerProvider>>();
  for (const provider of input.providers) {
    const advisory = provider.versionAdvisory;
    if (
      !provider.enabled ||
      !provider.installed ||
      advisory?.status !== "behind_latest" ||
      provider.compatibilityAdvisory?.latestVersionStatus === "broken" ||
      provider.compatibilityAdvisory?.latestVersionStatus === "unsupported"
    ) {
      continue;
    }
    outdatedByDriver.set(provider.driver, [
      ...(outdatedByDriver.get(provider.driver) ?? []),
      provider,
    ]);
  }

  const targets: Array<ProviderAutoUpdateTarget> = [];
  for (const [driver, outdated] of outdatedByDriver) {
    const first = outdated[0]!;
    const commands = new Set(outdated.map((provider) => provider.versionAdvisory?.updateCommand));
    const updateCommand = first.versionAdvisory?.updateCommand ?? null;
    const latestVersion = first.versionAdvisory?.latestVersion ?? null;
    if (
      commands.size !== 1 ||
      updateCommand === null ||
      latestVersion === null ||
      outdated.some(
        (provider) =>
          provider.versionAdvisory?.canUpdate !== true ||
          provider.updateState?.status === "queued" ||
          provider.updateState?.status === "running" ||
          provider.updateState?.status === "failed" ||
          provider.updateState?.status === "unchanged",
      )
    ) {
      continue;
    }
    const attemptKey = `${driver}:${updateCommand}@${latestVersion}`;
    if (input.attempted.has(attemptKey) || input.busyDrivers.has(driver)) {
      continue;
    }
    targets.push({ provider: first, attemptKey });
  }
  return targets;
}

/**
 * One evaluation pass: reads current state and runs each due update in turn
 * through the same runner the update button uses, so failures land in the
 * provider's `updateState` exactly like a manual update.
 */
export function makeProviderAutoUpdater(deps: {
  readonly getSettings: Effect.Effect<ServerSettings, ServerSettingsError>;
  readonly getProviders: Effect.Effect<ReadonlyArray<ServerProvider>>;
  readonly getBusyDrivers: Effect.Effect<ReadonlySet<string>, SqlError>;
  readonly updateProvider: ProviderMaintenanceRunner.ProviderMaintenanceRunnerShape["updateProvider"];
}) {
  const attempted = new Set<string>();
  const evaluate = Effect.gen(function* () {
    const settings = yield* deps.getSettings;
    if (!settings.autoUpdateProviders) return;
    const targets = selectProviderAutoUpdateTargets({
      settings,
      providers: yield* deps.getProviders,
      busyDrivers: yield* deps.getBusyDrivers,
      attempted,
    });
    yield* Effect.forEach(
      targets,
      ({ provider, attemptKey }) => {
        attempted.add(attemptKey);
        return Effect.logInfo("Updating provider automatically", { attemptKey }).pipe(
          Effect.andThen(
            deps.updateProvider({ provider: provider.driver, instanceId: provider.instanceId }),
          ),
          Effect.catch((error) => {
            if (isProviderUpdateError(error) && error.reason === PROVIDER_UPDATE_BUSY_REASON) {
              attempted.delete(attemptKey);
              return Effect.void;
            }
            return Effect.fail(error);
          }),
          Effect.catchCause((cause) =>
            Effect.logWarning("Automatic provider update failed", {
              attemptKey,
              cause: Cause.pretty(cause),
            }),
          ),
        );
      },
      { discard: true },
    );
  }).pipe(
    Effect.catchCause((cause) =>
      Effect.logWarning("Automatic provider update check failed", { cause: Cause.pretty(cause) }),
    ),
  );
  return { evaluate };
}

const WAKE_EVENT_TYPES = new Set([
  "run.updated",
  "provider-session.attached",
  "provider-session.updated",
  "provider-session.detached",
  "provider-thread.updated",
]);

/**
 * Re-evaluates at startup, when provider snapshots or settings change, and
 * when a turn ends so a deferred update runs once the provider is idle.
 * Wake-ups coalesce into one pending pass.
 */
export const layer = Layer.effectDiscard(
  Effect.gen(function* () {
    const providerRegistry = yield* ProviderRegistry;
    const eventSink = yield* EventSink.EventSinkV2;
    const sql = yield* SqlClient.SqlClient;
    const serverSettings = yield* ServerSettingsModule.ServerSettingsService;
    const runner = yield* ProviderMaintenanceRunner.ProviderMaintenanceRunner;
    const { evaluate } = makeProviderAutoUpdater({
      getSettings: serverSettings.getSettings,
      getProviders: providerRegistry.getProviders,
      getBusyDrivers: readBusyProviderDrivers.pipe(Effect.provideService(SqlClient.SqlClient, sql)),
      updateProvider: runner.updateProvider,
    });

    const wake = yield* Queue.sliding<void>(1);
    const signal = Queue.offer(wake, undefined).pipe(Effect.asVoid);
    const settingsChanges = yield* serverSettings.subscribeChanges;
    yield* Stream.runForEach(settingsChanges, () => signal).pipe(Effect.forkScoped);
    yield* Stream.runForEach(providerRegistry.streamChanges, () => signal).pipe(Effect.forkScoped);
    yield* eventSink.stream().pipe(
      Stream.filter((event) => WAKE_EVENT_TYPES.has(event.event.type)),
      Stream.runForEach(() => signal),
      Effect.forkScoped,
    );
    yield* signal;
    yield* ServerActivation.forkParked(
      Effect.forever(Queue.take(wake).pipe(Effect.andThen(evaluate))),
    );
  }),
);
