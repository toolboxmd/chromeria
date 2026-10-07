// Fork: opt-in automatic provider updates (toolboxmd/chromeria#159).
import type {
  ProviderSession,
  ServerProvider,
  ServerSettings,
  ServerSettingsError,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";

import * as ServerSettingsModule from "../serverSettings.ts";
import { ProviderRegistry } from "./Services/ProviderRegistry.ts";
import { ProviderService } from "./Services/ProviderService.ts";
import * as ProviderMaintenanceRunner from "./providerMaintenanceRunner.ts";

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
  readonly sessions: ReadonlyArray<ProviderSession>;
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
    if (input.attempted.has(attemptKey) || isDriverMidTurn(driver, input.sessions)) {
      continue;
    }
    targets.push({ provider: first, attemptKey });
  }
  return targets;
}

function isDriverMidTurn(driver: string, sessions: ReadonlyArray<ProviderSession>): boolean {
  return sessions.some(
    (session) =>
      session.provider === driver &&
      (session.status === "connecting" ||
        session.status === "running" ||
        session.activeTurnId !== undefined),
  );
}

/**
 * One evaluation pass: reads current state and runs each due update in turn
 * through the same runner the update button uses, so failures land in the
 * provider's `updateState` exactly like a manual update.
 */
export function makeProviderAutoUpdater(deps: {
  readonly getSettings: Effect.Effect<ServerSettings, ServerSettingsError>;
  readonly getProviders: Effect.Effect<ReadonlyArray<ServerProvider>>;
  readonly listSessions: Effect.Effect<ReadonlyArray<ProviderSession>>;
  readonly updateProvider: ProviderMaintenanceRunner.ProviderMaintenanceRunnerShape["updateProvider"];
}) {
  const attempted = new Set<string>();
  const evaluate = Effect.gen(function* () {
    const settings = yield* deps.getSettings;
    if (!settings.autoUpdateProviders) return;
    const targets = selectProviderAutoUpdateTargets({
      settings,
      providers: yield* deps.getProviders,
      sessions: yield* deps.listSessions,
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
  "turn.completed",
  "turn.aborted",
  "session.state.changed",
  "session.exited",
]);

/**
 * Re-evaluates at startup, when provider snapshots or settings change, and
 * when a turn ends so a deferred update runs once the provider is idle.
 * Wake-ups coalesce into one pending pass.
 */
export const layer = Layer.effectDiscard(
  Effect.gen(function* () {
    const providerRegistry = yield* ProviderRegistry;
    const providerService = yield* ProviderService;
    const serverSettings = yield* ServerSettingsModule.ServerSettingsService;
    const runner = yield* ProviderMaintenanceRunner.ProviderMaintenanceRunner;
    const { evaluate } = makeProviderAutoUpdater({
      getSettings: serverSettings.getSettings,
      getProviders: providerRegistry.getProviders,
      listSessions: Effect.suspend(() => providerService.listSessions()),
      updateProvider: runner.updateProvider,
    });

    const wake = yield* Queue.sliding<void>(1);
    const signal = Queue.offer(wake, undefined).pipe(Effect.asVoid);
    const settingsChanges = yield* serverSettings.subscribeChanges;
    yield* Stream.runForEach(settingsChanges, () => signal).pipe(Effect.forkScoped);
    yield* Stream.runForEach(providerRegistry.streamChanges, () => signal).pipe(Effect.forkScoped);
    yield* providerService.streamEvents.pipe(
      Stream.filter((event) => WAKE_EVENT_TYPES.has(event.type)),
      Stream.runForEach(() => signal),
      Effect.forkScoped,
    );
    yield* signal;
    yield* Effect.forever(Queue.take(wake).pipe(Effect.andThen(evaluate))).pipe(Effect.forkScoped);
  }),
);
