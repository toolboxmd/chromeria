import { RegistryContext, useAtomValue } from "@effect/atom-react";
import { AuthSettingsWriteScope } from "@t3tools/contracts";
import type { LimitAccount } from "@t3tools/shared/usageLimits";
import { formatCount } from "@t3tools/shared/usageFormat";
import { Atom } from "effect/reactivity";
import { T, useGT, useLocale, useMessages } from "gt-react";
import { useContext, useId } from "react";

import { usePrimarySettings } from "../../hooks/useSettings";
import { environmentPresentations } from "../../state/presentation";
import { serverEnvironment } from "../../state/server";
import { useEnvironmentsWithScope } from "../../state/session";
import { useAtomCommand } from "../../state/use-atom-command";
import { formatDayAwareTimestamp } from "../../timestampFormat";
import { ChromeriaFeatureMark } from "../ChromeriaFeatureMark";
import { Switch } from "../ui/switch";
import {
  type CreditUseSwitch,
  creditUseRow,
  creditUseSwitches,
  creditUseTargets,
  shownCredits,
  storedCreditUse,
} from "./PrismUseCredits.logic";

/** How long a write may take to show in the server's settings, as for a redeemed reset. */
const WRITE_TIMEOUT_MS = 10_000;

const noWrites: ReadonlyMap<string, boolean> = new Map();
/**
 * The value of each write under way, by environment and instance. It outlives a row that hides
 * its switch and the popover itself: only the server reporting that value, a failed write or the
 * timeout ends a write.
 */
const writesUnderWayAtom = Atom.make(noWrites).pipe(Atom.keepAlive);

const writeKey = (target: CreditUseSwitch) => `${target.environmentId}:${target.instanceId}`;

/**
 * Whether Prism may run this Codex login on credits: one row for each machine the login is on,
 * with a switch bound to that server's own value.
 */
export function PrismUseCredits({
  account,
  now,
}: {
  readonly account: LimitAccount;
  readonly now: number;
}) {
  const registry = useContext(RegistryContext);
  const environments = useAtomValue(environmentPresentations.presentationsWithProvidersAtom);
  const writesUnderWay = useAtomValue(writesUnderWayAtom);
  const updateSettings = useAtomCommand(serverEnvironment.updateSettings, "server settings update");
  const timestampFormat = usePrimarySettings((settings) => settings.timestampFormat);
  const gt = useGT();
  const m = useMessages();
  const locale = useLocale();
  const id = useId();
  const targets = creditUseTargets(environments, account);
  const writable = useEnvironmentsWithScope(targets, AuthSettingsWriteScope);
  if (targets.length === 0) return null;
  const read = shownCredits(environments, targets);
  const credits = read && read.balance !== "none" ? { ...read, balance: read.balance } : null;
  const readAt = credits
    ? formatDayAwareTimestamp(credits.readAt, timestampFormat, now, undefined, m)
    : "";

  const write = (target: CreditUseSwitch, value: boolean) => {
    const key = writeKey(target);
    const timeout = new AbortController();
    let over = false;
    const end = () => {
      if (over) return;
      over = true;
      clearTimeout(timer);
      unsubscribe();
      const rest = new Map(registry.get(writesUnderWayAtom));
      rest.delete(key);
      registry.set(writesUnderWayAtom, rest);
    };
    registry.set(writesUnderWayAtom, new Map(registry.get(writesUnderWayAtom)).set(key, value));
    // One bound for the answer and for the settings that must follow it.
    const timer = setTimeout(() => {
      timeout.abort();
      end();
    }, WRITE_TIMEOUT_MS);
    const unsubscribe = registry.subscribe(
      environmentPresentations.presentationsWithProvidersAtom,
      (latest) => {
        if (storedCreditUse(latest.get(target.environmentId), target.instanceId) === value) end();
      },
    );
    void updateSettings(
      {
        environmentId: target.environmentId,
        input: { patch: { prismUseCredits: { [target.instanceId]: value } } },
      },
      { signal: timeout.signal },
    ).then((result) => {
      if (result._tag !== "Success") end();
    });
  };

  return (
    <div className="flex flex-col gap-1 border-t border-border/60 pt-2.5">
      <div className="inline-flex items-center gap-1.5 text-foreground">
        <span>
          <T>Use credits</T>
        </span>
        <ChromeriaFeatureMark />
      </div>
      <p id={`${id}-description`} className="text-muted-foreground">
        <T>When the plan windows are spent, Prism may run Codex on your credits.</T>
      </p>
      {credits ? (
        <p className="text-foreground">
          {credits.balance === "unlimited"
            ? gt("Credits: unlimited (read {readAt})", { readAt })
            : gt("Credits left: {balance} (read {readAt})", {
                balance: formatCount(credits.balance, locale),
                readAt,
              })}
        </p>
      ) : null}
      {creditUseSwitches(environments, targets).map((target) => {
        const rowId = `${id}-${writeKey(target)}`;
        const shown = creditUseRow({
          environment: environments.get(target.environmentId),
          instanceId: target.instanceId,
          mayWrite: writable.has(target.environmentId),
          writing: writesUnderWay.get(writeKey(target)) ?? null,
        });
        const note =
          shown.note === "notConnected"
            ? gt("Not connected")
            : shown.note === "notAvailable"
              ? gt("Not available: update Chromeria on this machine")
              : shown.note === "noPermission"
                ? gt("No permission")
                : null;
        return (
          <div
            key={writeKey(target)}
            className="flex items-center justify-between gap-3 text-foreground"
          >
            <label htmlFor={rowId}>{target.label}</label>
            <span className="inline-flex items-center gap-2">
              {note === null ? null : (
                <span id={`${rowId}-note`} className="text-muted-foreground">
                  {note}
                </span>
              )}
              {shown.switch ? (
                <Switch
                  id={rowId}
                  size="sm"
                  checked={shown.switch.checked}
                  disabled={shown.switch.writes === null}
                  aria-busy={shown.switch.busy || undefined}
                  aria-label={gt("Use credits on {machine}", { machine: target.label })}
                  aria-describedby={
                    note === null ? `${id}-description` : `${id}-description ${rowId}-note`
                  }
                  onCheckedChange={() => {
                    if (shown.switch.writes !== null) write(target, shown.switch.writes);
                  }}
                />
              ) : null}
            </span>
          </div>
        );
      })}
    </div>
  );
}
