import type {
  AutoBalanceSettings,
  HostResourcesSnapshot,
  ServerSettingsPatch,
} from "@t3tools/contracts";

/** Callers supply only connected machines hosting the project and selected provider. */
export function chooseLoadBalancedEnvironment(
  candidates: ReadonlyArray<{
    environmentId: string;
    resources: HostResourcesSnapshot | null;
    /** Client receipt time avoids comparing clocks on different machines. */
    receivedAt?: number;
    weight: number;
  }>,
  now: number,
): string | null {
  let selected: string | null = null;
  let bestScore = 0;
  for (const { environmentId, resources, receivedAt, weight } of candidates) {
    const sampledAt = receivedAt ?? resources?.sampledAt ?? 0;
    if (
      !resources ||
      !Number.isFinite(weight) ||
      weight <= 0 ||
      now - sampledAt > 15_000 ||
      sampledAt > now + 5_000 ||
      resources.cpuUtilization === null ||
      resources.cpuUtilization >= 0.95 ||
      resources.totalMemoryBytes <= 0 ||
      resources.cpuCount <= 0
    ) {
      continue;
    }
    const memoryAvailable = resources.availableMemoryBytes / resources.totalMemoryBytes;
    if (memoryAvailable <= 0.05) continue;
    const score = weight * resources.cpuCount * (1 - resources.cpuUtilization) * memoryAvailable;
    if (score > bestScore) {
      selected = environmentId;
      bestScore = score;
    }
  }
  return selected;
}

/** Moves a client's own Auto balance onto its server once; default choices claim nothing. */
export function autoBalanceMigrationPatch(
  server: AutoBalanceSettings | null,
  legacy: AutoBalanceSettings,
): ServerSettingsPatch | null {
  return server === null && (legacy.enabled || Object.keys(legacy.weights).length > 0)
    ? { migrateAutoBalance: legacy }
    : null;
}

/**
 * The Auto balance a client acts on: its own with no primary server, the primary's once known
 * (its own until the primary has one), and off while the primary's is unknown.
 */
export function resolveAutoBalance(input: {
  readonly hasPrimary: boolean;
  /** Undefined until the primary's settings arrive; null when it has never been set. */
  readonly primary: AutoBalanceSettings | null | undefined;
  readonly legacy: AutoBalanceSettings;
}): { readonly autoBalance: AutoBalanceSettings; readonly ready: boolean } {
  if (!input.hasPrimary) return { autoBalance: input.legacy, ready: true };
  if (input.primary === undefined)
    return { autoBalance: { enabled: false, weights: {} }, ready: false };
  return { autoBalance: input.primary ?? input.legacy, ready: true };
}
