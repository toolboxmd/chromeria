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
  readonly connection: { readonly phase: string };
  readonly serverConfig:
    | (NonNullable<RedeemedResetEnvironment["serverConfig"]> & {
        readonly settings: Pick<ServerSettings, "prismUseCredits">;
        readonly prismUseCreditsSetting?: boolean;
      })
    | null;
}

export interface CreditUseTarget {
  readonly environmentId: EnvironmentId;
  readonly label: string;
  readonly instances: ReadonlyArray<{ readonly instanceId: ProviderInstanceId }>;
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
    const target = targets.get(environmentId);
    targets.set(environmentId, {
      environmentId,
      label: environments.get(environmentId)?.entry.target.label ?? environmentId,
      instances: [...(target?.instances ?? []), { instanceId }],
    });
  }
  return [...targets.values()];
}

export interface CreditUseSwitch {
  readonly environmentId: EnvironmentId;
  readonly instanceId: ProviderInstanceId;
  readonly label: string;
}

/**
 * One switch per stored value: a machine with two instances of the login holds two, each named
 * with its instance, so no switch has to stand for values that can differ.
 */
export function creditUseSwitches(
  environments: ReadonlyMap<EnvironmentId, CreditUseEnvironment>,
  targets: ReadonlyArray<CreditUseTarget>,
): ReadonlyArray<CreditUseSwitch> {
  return targets.flatMap((target) =>
    target.instances.map(({ instanceId }) => ({
      environmentId: target.environmentId,
      instanceId,
      label:
        target.instances.length > 1
          ? `${target.label} · ${
              environments
                .get(target.environmentId)
                ?.serverConfig?.providers.find((provider) => provider.instanceId === instanceId)
                ?.displayName ?? instanceId
            }`
          : target.label,
    })),
  );
}

export type CreditUseRow =
  | { readonly switch: null; readonly note: "notConnected" | "notAvailable" }
  | {
      readonly switch: {
        readonly checked: boolean;
        readonly busy: boolean;
        /** The value a click writes, or null when a click does nothing. */
        readonly writes: boolean | null;
      };
      readonly note: "noPermission" | null;
    };

/** The value an environment's settings hold for the instance, or null while they are not loaded. */
export function storedCreditUse(
  environment: CreditUseEnvironment | undefined,
  instanceId: ProviderInstanceId,
): boolean | null {
  const config = environment?.serverConfig ?? null;
  return config === null ? null : config.settings.prismUseCredits[instanceId] !== false;
}

/**
 * What one row shows and what a click on it does. The first line that applies decides
 * (toolboxmd/chromeria#1045); a server that can hold the setting says so itself. `writing` is
 * the value of a write under way, which lines 1 to 3 hide without ending.
 */
export function creditUseRow(input: {
  readonly environment: CreditUseEnvironment | undefined;
  readonly instanceId: ProviderInstanceId;
  readonly mayWrite: boolean;
  readonly writing: boolean | null;
}): CreditUseRow {
  const stored = storedCreditUse(input.environment, input.instanceId);
  // 1. Not connected, or its configuration is not loaded.
  if (input.environment?.connection.phase !== "connected" || stored === null) {
    return { switch: null, note: "notConnected" };
  }
  // 2. The server does not say that it holds the setting.
  if (input.environment.serverConfig?.prismUseCreditsSetting !== true) {
    return { switch: null, note: "notAvailable" };
  }
  // 3. The session may not write its settings.
  if (!input.mayWrite) {
    return { switch: { checked: stored, busy: false, writes: null }, note: "noPermission" };
  }
  // 4. A write is under way.
  if (input.writing !== null) {
    return { switch: { checked: input.writing, busy: true, writes: null }, note: null };
  }
  // 5. Everything else.
  return { switch: { checked: stored, busy: false, writes: !stored }, note: null };
}

/**
 * What the freshest usage read among the login's instances said about credits. A read that found
 * nothing to spend takes part, so it hides an older balance; an instance whose read failed or
 * never ran carries no answer and leaves the others standing. Null when no instance has one.
 */
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
