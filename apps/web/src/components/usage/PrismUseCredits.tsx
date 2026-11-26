import {
  AuthSettingsWriteScope,
  type EnvironmentId,
  type ProviderInstanceId,
} from "@t3tools/contracts";
import { T } from "gt-react";
import { useId } from "react";

import { useEnvironmentSettings, useUpdateEnvironmentSettings } from "../../hooks/useSettings";
import { useEnvironmentScope } from "../../state/session";
import { Switch } from "../ui/switch";

/** Whether Prism may run this Codex instance on credits; an instance without the setting is on. */
export function PrismUseCredits({
  environmentId,
  instanceId,
}: {
  readonly environmentId: EnvironmentId;
  readonly instanceId: ProviderInstanceId;
}) {
  const useCredits = useEnvironmentSettings(
    environmentId,
    (settings) => settings.prismUseCredits[instanceId] !== false,
  );
  const updateSettings = useUpdateEnvironmentSettings(environmentId);
  const canWriteSettings = useEnvironmentScope(environmentId, AuthSettingsWriteScope);
  const id = useId();
  return (
    <div className="flex flex-col gap-1 border-t border-border/60 pt-2.5">
      <label className="flex items-center justify-between gap-3 text-foreground">
        <span id={`${id}-label`}>
          <T>Use credits</T>
        </span>
        <Switch
          size="sm"
          checked={useCredits}
          disabled={!canWriteSettings}
          aria-labelledby={`${id}-label`}
          aria-describedby={`${id}-description`}
          onCheckedChange={(checked) =>
            updateSettings({ prismUseCredits: { [instanceId]: checked } })
          }
        />
      </label>
      <p id={`${id}-description`} className="text-muted-foreground">
        <T>When the plan windows are spent, Prism may run Codex on your credits.</T>
      </p>
    </div>
  );
}
