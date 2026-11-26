import type { EnvironmentId, ProviderInstanceId, ServerSettings } from "@t3tools/contracts";
import {
  instancesWithRedeemedLogin,
  type RedeemedResetEnvironment,
} from "@t3tools/client-runtime/state/redeemed-reset";

export interface CreditUseEnvironment extends RedeemedResetEnvironment {
  readonly serverConfig:
    | (NonNullable<RedeemedResetEnvironment["serverConfig"]> & {
        readonly settings: Pick<ServerSettings, "prismUseCredits">;
      })
    | null;
}

export interface CreditUseTarget {
  readonly environmentId: EnvironmentId;
  readonly label: string;
  /** Per instance of the login on this environment; an instance without the setting is on. */
  readonly instances: ReadonlyArray<{
    readonly instanceId: ProviderInstanceId;
    readonly useCredits: boolean;
  }>;
}

/** The named instance and every other instance of its login, grouped by connected environment. */
export function creditUseTargets(
  environments: ReadonlyMap<EnvironmentId, CreditUseEnvironment>,
  named: { readonly environmentId: EnvironmentId; readonly instanceId: ProviderInstanceId },
): ReadonlyArray<CreditUseTarget> {
  const targets = new Map<EnvironmentId, CreditUseTarget>();
  for (const { environmentId, instanceId } of [
    named,
    ...instancesWithRedeemedLogin(environments, named),
  ]) {
    const environment = environments.get(environmentId);
    const target = targets.get(environmentId);
    targets.set(environmentId, {
      environmentId,
      label: environment?.entry.target.label ?? environmentId,
      instances: [
        ...(target?.instances ?? []),
        {
          instanceId,
          useCredits: environment?.serverConfig?.settings.prismUseCredits[instanceId] !== false,
        },
      ],
    });
  }
  return [...targets.values()];
}

export function shownCreditUse(targets: ReadonlyArray<CreditUseTarget>): "on" | "off" | "mixed" {
  const values = new Set(
    targets.flatMap((target) => target.instances.map((instance) => instance.useCredits)),
  );
  return values.size > 1 ? "mixed" : values.has(false) ? "off" : "on";
}
