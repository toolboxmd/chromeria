import {
  WS_METHODS,
  type EnvironmentId,
  type ProviderClearRedeemedResetInput,
  type ProviderClearRedeemedResetResult,
  type ProviderConsumeResetCreditInput,
  type ProviderConsumeResetCreditResult,
  type ProviderInstanceId,
  type ServerProvider,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import type { Atom } from "effect/reactivity";
import { mFallback, msg } from "gt-i18n";
import type { MFunctionType } from "gt-i18n/types";
import type * as EnvironmentRegistry from "../connection/registry.ts";
import { type AtomCommand, createEnvironmentRpcCommand } from "./runtime.ts";

/** How long the redemption's answer waits for the other machines; a server that got the call still finishes. */
const REDEEMED_RESET_ANSWER_TIMEOUT = "10 seconds";

export interface RedeemedResetEnvironment {
  readonly entry: { readonly target: { readonly label: string } };
  readonly serverConfig: { readonly providers: ReadonlyArray<ServerProvider> } | null;
}

export interface RedeemedResetHolder {
  readonly environmentId: EnvironmentId;
  readonly instanceId: ProviderInstanceId;
  readonly label: string;
}

const loginOf = (provider: ServerProvider) => {
  const email = provider.auth.email?.toLowerCase();
  const workspaceId = provider.auth.workspaceId;
  return email && workspaceId ? { driver: provider.driver, email, workspaceId } : undefined;
};

/**
 * Every other enabled instance with the redeemed instance's login: driver, email and workspace id
 * all present and equal. A missing email or workspace matches nothing.
 */
export function instancesWithRedeemedLogin(
  environments: ReadonlyMap<EnvironmentId, RedeemedResetEnvironment>,
  redeemed: { readonly environmentId: EnvironmentId; readonly instanceId: ProviderInstanceId },
): ReadonlyArray<RedeemedResetHolder> {
  const source = environments
    .get(redeemed.environmentId)
    ?.serverConfig?.providers.find((provider) => provider.instanceId === redeemed.instanceId);
  const login = source && loginOf(source);
  if (!login) return [];
  return [...environments].flatMap(([environmentId, environment]) => {
    const matches = (environment.serverConfig?.providers ?? []).filter((provider) => {
      if (environmentId === redeemed.environmentId && provider.instanceId === redeemed.instanceId)
        return false;
      const other = provider.enabled ? loginOf(provider) : undefined;
      return (
        other?.driver === login.driver &&
        other.email === login.email &&
        other.workspaceId === login.workspaceId
      );
    });
    const named = matches.length > 1 || environmentId === redeemed.environmentId;
    return matches.map((provider) => ({
      environmentId,
      instanceId: provider.instanceId,
      label: named
        ? `${environment.entry.target.label} · ${provider.displayName ?? provider.instanceId}`
        : environment.entry.target.label,
    }));
  });
}

export type RedeemedResetReach = "cleared" | "exhausted" | "unread" | "noAnswer" | "notReached";

export interface RedeemedResetLine extends RedeemedResetHolder {
  readonly reach: RedeemedResetReach;
}

export type RedeemResetCreditResult = ProviderConsumeResetCreditResult & {
  readonly machines: ReadonlyArray<RedeemedResetLine>;
};

type ConsumeCommand<E> = AtomCommand<
  { readonly environmentId: EnvironmentId; readonly input: ProviderConsumeResetCreditInput },
  ProviderConsumeResetCreditResult,
  E
>;

type ClearCommand<C> = AtomCommand<
  { readonly environmentId: EnvironmentId; readonly input: ProviderClearRedeemedResetInput },
  ProviderClearRedeemedResetResult,
  C
>;

const REACH: Record<ProviderClearRedeemedResetResult["usage"], RedeemedResetReach> = {
  headroom: "cleared",
  exhausted: "exhausted",
  unread: "unread",
};

/** Redeems, then after a `reset` tells every other holder of the login; their failures never fail it. */
export function redeemResetCreditCommand<E, C>(options: {
  readonly consume: ConsumeCommand<E>;
  readonly clear: ClearCommand<C>;
  readonly environmentsAtom: Atom.Atom<ReadonlyMap<EnvironmentId, RedeemedResetEnvironment>>;
}): AtomCommand<
  { readonly environmentId: EnvironmentId; readonly input: ProviderConsumeResetCreditInput },
  RedeemResetCreditResult,
  E
> {
  return {
    label: "environment-data:server:redeem-reset-credit",
    run: (registry, target) =>
      options.consume.run(registry, target).pipe(
        Effect.flatMap((result) => {
          // A hub redemption names no native instance.
          if (result.outcome !== "reset" || !("instanceId" in target.input))
            return Effect.succeed({ ...result, machines: [] });
          const holders = instancesWithRedeemedLogin(registry.get(options.environmentsAtom), {
            environmentId: target.environmentId,
            instanceId: target.input.instanceId,
          });
          return Effect.forEach(
            holders,
            (holder) =>
              options.clear
                .run(registry, {
                  environmentId: holder.environmentId,
                  input: { instanceId: holder.instanceId },
                })
                .pipe(
                  Effect.timeoutOption(REDEEMED_RESET_ANSWER_TIMEOUT),
                  Effect.map((answer): RedeemedResetLine => ({
                    ...holder,
                    reach: Option.match(answer, {
                      onNone: () => "noAnswer",
                      onSome: ({ usage }) => REACH[usage],
                    }),
                  })),
                  Effect.catchCause(() =>
                    Effect.succeed<RedeemedResetLine>({ ...holder, reach: "notReached" }),
                  ),
                ),
            { concurrency: "unbounded" },
          ).pipe(Effect.map((machines) => ({ ...result, machines })));
        }),
      ),
  };
}

export function createRedeemResetCreditCommand<R, ER, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry.EnvironmentRegistry | R, ER>,
  options: {
    readonly consume: ConsumeCommand<E>;
    readonly environmentsAtom: Atom.Atom<ReadonlyMap<EnvironmentId, RedeemedResetEnvironment>>;
  },
) {
  return redeemResetCreditCommand({
    ...options,
    clear: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:server:clear-redeemed-reset",
      tag: WS_METHODS.providerClearRedeemedReset,
    }),
  });
}

export function redeemedResetLineText(
  line: RedeemedResetLine,
  resolve: MFunctionType = mFallback,
): string {
  const machine = line.label;
  switch (line.reach) {
    case "cleared":
      return resolve(msg("{machine}: cleared.", { machine }));
    case "exhausted":
      return resolve(msg("{machine}: still shows an exhausted usage window.", { machine }));
    case "unread":
      return resolve(
        msg("{machine}: usage could not be read, so its old exhausted window still blocks it.", {
          machine,
        }),
      );
    case "noAnswer":
      return resolve(
        msg(
          "{machine}: no answer within 10 seconds; if it received the call, it clears its recorded limit failures.",
          { machine },
        ),
      );
    case "notReached":
      return resolve(msg("{machine}: not reached, so its old block remains.", { machine }));
  }
}
