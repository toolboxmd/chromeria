// Fork: opt-in automatic provider updates (toolboxmd/chromeria#159).
import type { ServerProvider, ServerSettings, ServerSettingsError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";

import * as ServerSettingsModule from "../serverSettings.ts";
import { ProviderRegistry } from "./ProviderRegistry.ts";
import * as ServerActivation from "../serverActivation.ts";
import * as ProviderMaintenanceRunner from "./providerMaintenanceRunner.ts";
import type {
  ProviderMaintenanceCapabilities,
  ProviderMaintenanceCommandAction,
} from "@t3tools/provider-core/server/maintenanceResolver";

export interface ProviderAutoUpdateTarget {
  readonly provider: ServerProvider;
  /** Driver, install and target version: each is attempted at most once per server run. */
  readonly attemptKey: string;
}

/**
 * Providers to update now, by the notification's one-click rule: a driver's outdated instances must
 * share one update command, which runs once for all. Skip a driver with an active update or a failed
 * last result.
 */
function selectProviderAutoUpdateTargets(input: {
  readonly settings: Pick<ServerSettings, "autoUpdateProviders" | "enableProviderUpdateChecks">;
  readonly providers: ReadonlyArray<ServerProvider>;
  readonly attempted: ReadonlySet<string>;
  readonly updateActions: ReadonlyMap<string, ProviderMaintenanceCommandAction | null>;
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
    const identities = outdated.map((provider) => {
      const action = input.updateActions.get(provider.instanceId);
      if (!action) return null;
      // Display commands can hide distinct installer prefixes. Keep this identity
      // internal: environment values must never reach logs.
      return JSON.stringify([
        driver,
        action.executable,
        action.args,
        Object.entries(action.env ?? {}).sort(([left], [right]) => left.localeCompare(right)),
        action.lockKey,
        provider.versionAdvisory?.latestVersion,
      ]);
    });
    if (identities.some((identity) => identity === null) || new Set(identities).size !== 1) {
      continue;
    }
    const attemptKey = identities[0]!;
    if (input.attempted.has(attemptKey)) {
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
  readonly getMaintenanceCapabilities: (
    instanceId: ServerProvider["instanceId"],
    driver: ServerProvider["driver"],
  ) => Effect.Effect<ProviderMaintenanceCapabilities>;
  readonly updateProvider: ProviderMaintenanceRunner.ProviderMaintenanceRunnerShape["updateProvider"];
}) {
  const attempted = new Set<string>();
  const evaluate = Effect.gen(function* () {
    const settings = yield* deps.getSettings;
    const providers = yield* deps.getProviders;
    if (!settings.autoUpdateProviders || !settings.enableProviderUpdateChecks) return;
    const updateActions = new Map<string, ProviderMaintenanceCommandAction | null>();
    for (const provider of providers) {
      if (
        provider.enabled &&
        provider.installed &&
        provider.versionAdvisory?.status === "behind_latest" &&
        provider.versionAdvisory.canUpdate
      ) {
        const capabilities = yield* deps.getMaintenanceCapabilities(
          provider.instanceId,
          provider.driver,
        );
        updateActions.set(provider.instanceId, capabilities.update);
      }
    }
    const targets = selectProviderAutoUpdateTargets({
      settings,
      providers,
      updateActions,
      attempted,
    });
    yield* Effect.forEach(
      targets,
      ({ provider, attemptKey }) =>
        Effect.gen(function* () {
          attempted.add(attemptKey);
          yield* Effect.logInfo("Updating provider automatically", {
            driver: provider.driver,
            instanceId: provider.instanceId,
          }).pipe(
            Effect.andThen(
              deps.updateProvider({ provider: provider.driver, instanceId: provider.instanceId }),
            ),
            Effect.catchCause(() =>
              Effect.logWarning("Automatic provider update failed", {
                driver: provider.driver,
                instanceId: provider.instanceId,
              }),
            ),
          );
        }),
      { discard: true },
    );
  }).pipe(Effect.catchCause(() => Effect.logWarning("Automatic provider update check failed")));
  return { evaluate };
}

/** Re-evaluate on startup, provider snapshots and settings changes; coalesce pending passes. */
export const layer = Layer.effectDiscard(
  Effect.gen(function* () {
    const providerRegistry = yield* ProviderRegistry;
    const serverSettings = yield* ServerSettingsModule.ServerSettingsService;
    const runner = yield* ProviderMaintenanceRunner.ProviderMaintenanceRunner;
    const { evaluate } = makeProviderAutoUpdater({
      getSettings: serverSettings.getSettings,
      getProviders: providerRegistry.getProviders,
      getMaintenanceCapabilities: providerRegistry.getProviderMaintenanceCapabilitiesForInstance,
      updateProvider: runner.updateProvider,
    });

    const wake = yield* Queue.sliding<void>(1);
    const signal = Queue.offer(wake, undefined).pipe(Effect.asVoid);
    const settingsChanges = yield* serverSettings.subscribeChanges;
    yield* Stream.runForEach(settingsChanges, () => signal).pipe(Effect.forkScoped);
    yield* Stream.runForEach(providerRegistry.streamChanges, () => signal).pipe(Effect.forkScoped);
    yield* signal;
    yield* ServerActivation.forkParked(
      Effect.forever(Queue.take(wake).pipe(Effect.andThen(evaluate))),
    );
  }),
);
