import { runAtomCommand } from "@t3tools/client-runtime/state/runtime";
import type { AutoBalanceSettings } from "@t3tools/contracts";
import { autoBalanceMigrationPatch } from "@t3tools/shared/loadBalancing";

import { appAtomRegistry } from "~/rpc/atomRegistry";
import { primaryServerConfigAtom, serverEnvironment } from "~/state/server";
import { primaryEnvironmentIdAtom } from "~/state/primaryEnvironment";
import { usePrimaryEnvironmentId } from "~/state/environments";
import {
  getClientSettings,
  isClientSettingsHydrated,
  subscribeClientSettings,
  subscribeClientSettingsHydration,
  updateClientSettings,
  useClientSettings,
  usePrimarySettings,
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
 * agents also use, or this client's own when it has no primary server.
 */
export function useAutoBalance() {
  const primaryEnvironmentId = usePrimaryEnvironmentId();
  const server = usePrimarySettings((settings) => settings.autoBalance);
  const legacy = useClientSettings(legacyOf);
  const updatePrimary = useUpdatePrimarySettings();
  const autoBalance = primaryEnvironmentId === null ? legacy : (server ?? legacy);
  const update = (change: Partial<AutoBalanceSettings>) => {
    const next = { ...autoBalance, ...change };
    if (primaryEnvironmentId !== null) return updatePrimary({ autoBalance: next });
    void updateClientSettings({
      loadBalancingEnabled: next.enabled,
      loadBalancingWeights: next.weights,
    });
  };
  return { autoBalance, update };
}

const migrated = new Set<string>();

function migrateAutoBalance(): void {
  if (!isClientSettingsHydrated()) return;
  const environmentId = appAtomRegistry.get(primaryEnvironmentIdAtom);
  if (environmentId === null || migrated.has(environmentId)) return;
  // Until its settings arrive, the server's policy is unknown, not absent.
  const settings = appAtomRegistry.get(primaryServerConfigAtom)?.settings;
  if (settings === undefined) return;
  const patch = autoBalanceMigrationPatch(settings.autoBalance, legacyOf(getClientSettings()));
  if (patch === null) return;
  migrated.add(environmentId);
  void runAtomCommand(
    appAtomRegistry,
    serverEnvironment.updateSettings,
    { environmentId, input: { patch } },
    { reportFailure: false },
  );
}

/** Moves this client's Auto balance onto its primary server once, if that server has none. */
export function startAutoBalanceMigration(): () => void {
  const unsubscribes = [
    appAtomRegistry.subscribe(primaryServerConfigAtom, migrateAutoBalance),
    subscribeClientSettings(migrateAutoBalance),
    subscribeClientSettingsHydration(migrateAutoBalance),
  ];
  migrateAutoBalance();
  return () => {
    for (const unsubscribe of unsubscribes) unsubscribe();
  };
}
