import { useAtomValue } from "@effect/atom-react";
import {
  AuthSettingsWriteScope,
  type EnvironmentId,
  type ProviderInstanceId,
} from "@t3tools/contracts";
import { T, useGT } from "gt-react";
import { useId, useState } from "react";

import { environmentPresentations } from "../../state/presentation";
import { serverEnvironment } from "../../state/server";
import { readEnvironmentScope } from "../../state/session";
import { useAtomCommand } from "../../state/use-atom-command";
import { Switch } from "../ui/switch";
import { creditUseTargets, shownCreditUse } from "./PrismUseCredits.logic";

/**
 * Whether Prism may run this Codex login on credits. One switch stands for the login: it writes
 * the named instance and every other instance of that login on the connected environments.
 */
export function PrismUseCredits({
  environmentId,
  instanceId,
}: {
  readonly environmentId: EnvironmentId;
  readonly instanceId: ProviderInstanceId;
}) {
  const environments = useAtomValue(environmentPresentations.presentationsWithProvidersAtom);
  const updateSettings = useAtomCommand(serverEnvironment.updateSettings, {
    label: "server settings update",
    reportFailure: false,
  });
  const [unchanged, setUnchanged] = useState<ReadonlyArray<string>>([]);
  const gt = useGT();
  const id = useId();
  const targets = creditUseTargets(environments, { environmentId, instanceId });
  const shown = shownCreditUse(targets);

  const write = async (useCredits: boolean) => {
    setUnchanged([]);
    const labels = await Promise.all(
      targets.map(async (target) => {
        if (!readEnvironmentScope(target.environmentId, AuthSettingsWriteScope)) {
          return target.label;
        }
        const result = await updateSettings({
          environmentId: target.environmentId,
          input: {
            patch: {
              prismUseCredits: Object.fromEntries(
                target.instances.map((instance) => [instance.instanceId, useCredits]),
              ),
            },
          },
        });
        return result._tag === "Success" ? null : target.label;
      }),
    );
    setUnchanged(labels.filter((label) => label !== null));
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
          aria-labelledby={`${id}-label`}
          aria-describedby={`${id}-description`}
          // From mixed, a click turns every instance off.
          onCheckedChange={(checked) => void write(shown === "mixed" ? false : checked)}
        />
      </label>
      <p id={`${id}-description`} className="text-muted-foreground">
        <T>When the plan windows are spent, Prism may run Codex on your credits.</T>
      </p>
      {unchanged.length > 0 ? (
        <p role="status" className="text-foreground">
          {gt("Not changed on {machines}.", { machines: unchanged.join(", ") })}
        </p>
      ) : null}
    </div>
  );
}
