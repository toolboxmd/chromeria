import type {
  EnvironmentId,
  ProviderInstanceId,
  ServerProviderCredits,
  ServerSettings,
} from "@t3tools/contracts";
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

/**
 * The account's own instance and every other instance of its login, grouped by environment. The
 * instance comes from the account's key and environments, never from its redeem target, which a
 * usage hub holding a banked reset for the same login replaces. A hub-only account has none.
 */
export function creditUseTargets(
  environments: ReadonlyMap<EnvironmentId, CreditUseEnvironment>,
  account: {
    readonly key: string;
    readonly environments: ReadonlyArray<{ readonly environmentId: EnvironmentId }>;
  },
): ReadonlyArray<CreditUseTarget> {
  const own = account.environments.flatMap(({ environmentId }) =>
    (environments.get(environmentId)?.serverConfig?.providers ?? []).flatMap((provider) =>
      // `collectLimitAccounts` keys an account by the first native instance merged into it.
      `${environmentId}:${provider.instanceId}` === account.key
        ? [{ environmentId, instanceId: provider.instanceId }]
        : [],
    ),
  )[0];
  if (!own) return [];
  const targets = new Map<EnvironmentId, CreditUseTarget>();
  for (const { environmentId, instanceId } of [
    own,
    ...instancesWithRedeemedLogin(environments, own),
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

/** The login's credits from the freshest usage read among its instances; null when none reports any. */
export function shownCredits(
  environments: ReadonlyMap<EnvironmentId, CreditUseEnvironment>,
  targets: ReadonlyArray<CreditUseTarget>,
): ServerProviderCredits | null {
  const reads = targets.flatMap((target) =>
    (environments.get(target.environmentId)?.serverConfig?.providers ?? []).flatMap((provider) =>
      provider.credits &&
      target.instances.some((instance) => instance.instanceId === provider.instanceId)
        ? [provider.credits]
        : [],
    ),
  );
  return reads.reduce<ServerProviderCredits | null>(
    (freshest, read) =>
      freshest && Date.parse(freshest.readAt) >= Date.parse(read.readAt) ? freshest : read,
    null,
  );
}
