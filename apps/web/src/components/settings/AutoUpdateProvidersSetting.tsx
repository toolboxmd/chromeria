// Fork: opt-in automatic provider updates (toolboxmd/chromeria#159).
import { DEFAULT_UNIFIED_SETTINGS } from "@t3tools/contracts";

import { ScopedSwitch } from "./ScopedSwitch";
import { SettingResetButton, SettingsRow } from "./settingsLayout";
import { searchableSetting } from "./settingsSearch";

/** Environment-wide toggle shown under "Provider update checks", which it depends on. */
export function AutoUpdateProvidersSetting({
  value,
  checksEnabled,
  onChange,
}: {
  readonly value: boolean;
  readonly checksEnabled: boolean;
  readonly onChange: (value: boolean) => void;
}) {
  return (
    <SettingsRow
      serverScoped
      settingKeys={["autoUpdateProviders"]}
      {...searchableSetting("auto-update-providers")}
      description="Install newer provider CLI versions without a click. Needs provider update checks. Waits while that provider is running a turn."
      resetAction={
        value !== DEFAULT_UNIFIED_SETTINGS.autoUpdateProviders ? (
          <SettingResetButton
            label="automatic provider updates"
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
          aria-label="Update providers automatically"
        />
      }
    />
  );
}
