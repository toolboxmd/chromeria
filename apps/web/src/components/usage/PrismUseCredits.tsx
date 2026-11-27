import { useAtomValue } from "@effect/atom-react";
import { AuthSettingsWriteScope } from "@t3tools/contracts";
import type { LimitAccount } from "@t3tools/shared/usageLimits";
import { T, useGT, useMessages } from "gt-react";
import { useId, useState } from "react";

import { usePrimarySettings } from "../../hooks/useSettings";
import { environmentPresentations } from "../../state/presentation";
import { serverEnvironment } from "../../state/server";
import { readEnvironmentScope } from "../../state/session";
import { useAtomCommand } from "../../state/use-atom-command";
import { formatDayAwareTimestamp } from "../../timestampFormat";
import { Switch } from "../ui/switch";
import { creditUseTargets, shownCreditUse, shownCredits } from "./PrismUseCredits.logic";

/** How long one machine's write may take before it counts as not changed, as for a redeemed reset. */
const WRITE_TIMEOUT_MS = 10_000;

/**
 * Whether Prism may run this Codex login on credits. One switch stands for the login: it writes
 * the account's own instance and every other instance of that login on the enabled environments.
 */
export function PrismUseCredits({
  account,
  now,
}: {
  readonly account: LimitAccount;
  readonly now: number;
}) {
  const environments = useAtomValue(environmentPresentations.presentationsWithProvidersAtom);
  const updateSettings = useAtomCommand(serverEnvironment.updateSettings, {
    label: "server settings update",
    reportFailure: false,
  });
  const [busy, setBusy] = useState(false);
  const [unchanged, setUnchanged] = useState<ReadonlyArray<string>>([]);
  const timestampFormat = usePrimarySettings((settings) => settings.timestampFormat);
  const gt = useGT();
  const m = useMessages();
  const id = useId();
  const targets = creditUseTargets(environments, account);
  if (targets.length === 0) return null;
  const shown = shownCreditUse(targets);
  const credits = shownCredits(environments, targets);
  const readAt = credits
    ? formatDayAwareTimestamp(credits.readAt, timestampFormat, now, undefined, m)
    : "";

  const write = async (useCredits: boolean) => {
    setBusy(true);
    setUnchanged([]);
    const labels = await Promise.all(
      targets.map(async (target) => {
        if (!readEnvironmentScope(target.environmentId, AuthSettingsWriteScope)) {
          return target.label;
        }
        const timeout = new AbortController();
        const timer = setTimeout(() => timeout.abort(), WRITE_TIMEOUT_MS);
        const result = await updateSettings(
          {
            environmentId: target.environmentId,
            input: {
              patch: {
                prismUseCredits: Object.fromEntries(
                  target.instances.map((instance) => [instance.instanceId, useCredits]),
                ),
              },
            },
          },
          { signal: timeout.signal },
        );
        clearTimeout(timer);
        return result._tag === "Success" ? null : target.label;
      }),
    );
    setUnchanged(labels.filter((label) => label !== null));
    setBusy(false);
  };

  return (
    <div className="flex flex-col gap-1 border-t border-border/60 pt-2.5">
      <label className="flex items-center justify-between gap-3 text-foreground">
        <span id={`${id}-label`}>
          <T>Use credits</T>
        </span>
        <Switch
          size="sm"
          checked={shown === "on"}
          mixed={shown === "mixed"}
          disabled={busy}
          aria-busy={busy || undefined}
          aria-labelledby={`${id}-label`}
          aria-describedby={`${id}-description`}
          // From mixed, a click turns every instance off.
          onCheckedChange={(checked) => void write(shown === "mixed" ? false : checked)}
        />
      </label>
      <p id={`${id}-description`} className="text-muted-foreground">
        <T>When the plan windows are spent, Prism may run Codex on your credits.</T>
      </p>
      {credits ? (
        <p className="text-foreground">
          {credits.balance === "unlimited"
            ? gt("Credits: unlimited (read {readAt})", { readAt })
            : gt("Credits left: {balance} (read {readAt})", {
                balance: credits.balance.toLocaleString(),
                readAt,
              })}
        </p>
      ) : null}
      {unchanged.length > 0 ? (
        <p role="status" className="text-foreground">
          {gt("Not changed on {machines}.", { machines: unchanged.join(", ") })}
        </p>
      ) : null}
    </div>
  );
}
