import { useAtomValue } from "@effect/atom-react";
import { runAtomCommand } from "@t3tools/client-runtime/state/runtime";
import type { AutoBalanceSettings, EnvironmentId } from "@t3tools/contracts";
import { autoBalanceMigrationPatch, resolveAutoBalance } from "@t3tools/shared/loadBalancing";
import * as Option from "effect/Option";
import { AsyncResult } from "effect/reactivity";

import { environmentCatalog } from "~/connection/catalog";
import { appAtomRegistry } from "~/rpc/atomRegistry";
import { primaryServerConfigAtom, serverEnvironment } from "~/state/server";
import { primaryEnvironmentIdAtom } from "~/state/primaryEnvironment";
import {
  getClientSettings,
  isClientSettingsHydrated,
  subscribeClientSettings,
  subscribeClientSettingsHydration,
  updateClientSettings,
  useClientSettings,
  useUpdatePrimarySettings,
} from "./useSettings";

const legacyOf = (settings: {
  readonly loadBalancingEnabled: boolean;
  readonly loadBalancingWeights: AutoBalanceSettings["weights"];
}): AutoBalanceSettings => ({
  enabled: settings.loadBalancingEnabled,
  weights: settings.loadBalancingWeights,
});

/**
 * Auto balance as this client acts on it: its primary server's setting, which that server's
 * agents also use, or this client's own when it has no primary server. `ready` is false while
 * the primary's setting is still unknown; it then reads as off and cannot be changed.
 */
export function useAutoBalance() {
  const primaryEnvironmentId = useAtomValue(primaryEnvironmentIdAtom);
  const primaryConfig = useAtomValue(primaryServerConfigAtom);
  const legacy = useClientSettings(legacyOf);
  const updatePrimary = useUpdatePrimarySettings();
  const { autoBalance, ready } = resolveAutoBalance({
    hasPrimary: primaryEnvironmentId !== null,
    primary: primaryConfig === null ? undefined : primaryConfig.settings.autoBalance,
    legacy,
  });
  const update = (change: Partial<AutoBalanceSettings>) => {
    if (!ready) return;
    const next = { ...autoBalance, ...change };
    if (primaryEnvironmentId !== null) return updatePrimary({ autoBalance: next });
    void updateClientSettings({
      loadBalancingEnabled: next.enabled,
      loadBalancingWeights: next.weights,
    });
  };
  return { autoBalance, ready, update };
}

// Attempted once per connection; a failed write tries again on the next change or connection.
const attemptedGenerations = new Map<EnvironmentId, number>();

function migrateAutoBalance(): void {
  if (!isClientSettingsHydrated()) return;
  const environmentId = appAtomRegistry.get(primaryEnvironmentIdAtom);
  if (environmentId === null) return;
  const generation = Option.match(
    AsyncResult.value(appAtomRegistry.get(environmentCatalog.stateAtom(environmentId))),
    {
      onNone: () => null,
      onSome: (connection) => (connection.phase === "connected" ? connection.generation : null),
    },
  );
  if (generation === null || attemptedGenerations.get(environmentId) === generation) return;
  // Until its settings arrive, the server's policy is unknown, not absent.
  const settings = appAtomRegistry.get(primaryServerConfigAtom)?.settings;
  if (settings === undefined) return;
  const patch = autoBalanceMigrationPatch(settings.autoBalance, legacyOf(getClientSettings()));
  if (patch === null) return;
  attemptedGenerations.set(environmentId, generation);
  void runAtomCommand(
    appAtomRegistry,
    serverEnvironment.updateSettings,
    { environmentId, input: { patch } },
    { reportFailure: false },
  ).then((result) => {
    if (result._tag === "Failure") attemptedGenerations.delete(environmentId);
  });
}

/** Moves this client's Auto balance onto its primary server once, if that server has none. */
export function startAutoBalanceMigration(): () => void {
  const unsubscribes = [
    appAtomRegistry.subscribe(primaryServerConfigAtom, migrateAutoBalance),
    appAtomRegistry.subscribe(environmentCatalog.catalogValueAtom, migrateAutoBalance),
    subscribeClientSettings(migrateAutoBalance),
    subscribeClientSettingsHydration(migrateAutoBalance),
  ];
  migrateAutoBalance();
  return () => {
    for (const unsubscribe of unsubscribes) unsubscribe();
  };
}
