// Fork: opt-in automatic provider updates (toolboxmd/chromeria#159).
import { DEFAULT_UNIFIED_SETTINGS } from "@t3tools/contracts";
import { useGT } from "gt-react";

import { ChromeriaTitle } from "../ChromeriaFeatureMark";
import { ScopedSwitch } from "./ScopedSwitch";
import { SettingResetButton, SettingsRow } from "./settingsLayout";
import { useSearchableSetting } from "./settingsSearch";

/** Environment-wide toggle shown under Provider update checks, which it depends on. */
export function AutoUpdateProvidersSetting({
  value,
  checksEnabled,
  onChange,
}: {
  readonly value: boolean;
  readonly checksEnabled: boolean;
  readonly onChange: (value: boolean) => void;
}) {
  const gt = useGT();
  const searchableSetting = useSearchableSetting();
  const { id, title } = searchableSetting("auto-update-providers");
  return (
    <SettingsRow
      serverScoped
      settingKeys={["autoUpdateProviders"]}
      id={id}
      title={<ChromeriaTitle>{title}</ChromeriaTitle>}
      description={gt(
        "Install newer provider CLI versions without a click. Needs provider update checks.",
      )}
      resetAction={
        value !== DEFAULT_UNIFIED_SETTINGS.autoUpdateProviders ? (
          <SettingResetButton
            ariaLabel={gt("Reset automatic provider updates to default")}
            onClick={() => onChange(DEFAULT_UNIFIED_SETTINGS.autoUpdateProviders)}
          />
        ) : null
      }
      control={
        <ScopedSwitch
          settingKeys={["autoUpdateProviders"]}
          checked={value}
          disabled={!checksEnabled}
          onCheckedChange={(checked) => onChange(Boolean(checked))}
          aria-label={gt("Update providers automatically")}
        />
      }
    />
  );
}
