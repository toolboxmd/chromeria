// Fork: provider update controls sit with the other provider settings
// (toolboxmd/chromeria#266).
import { DEFAULT_UNIFIED_SETTINGS } from "@t3tools/contracts";
import { useGT } from "gt-react";

import { AutoUpdateProvidersSetting } from "./AutoUpdateProvidersSetting";
import { ScopedSwitch } from "./ScopedSwitch";
import { SettingResetButton, SettingsRow } from "./settingsLayout";
import { useSearchableSetting } from "./settingsSearch";
import { useScopedSettings, useUpdateScopedSettings } from "./useScopedSettings";

/**
 * Environment-wide provider update rows. Writes follow the settings scope,
 * the same writer General used, rather than the selected provider device.
 */
export function ProviderUpdateSettingsRows() {
  const gt = useGT();
  const searchableSetting = useSearchableSetting();
  const settings = useScopedSettings();
  const updateSettings = useUpdateScopedSettings();
  return (
    <>
      <SettingsRow
        serverScoped
        settingKeys={["enableProviderUpdateChecks"]}
        {...searchableSetting("provider-update-checks")}
        description={gt(
          "Check installed provider CLIs for newer available versions and fetch model catalog updates. When off, new models may not appear.",
        )}
        resetAction={
          settings.enableProviderUpdateChecks !==
          DEFAULT_UNIFIED_SETTINGS.enableProviderUpdateChecks ? (
            <SettingResetButton
              ariaLabel={gt("Reset provider update checks to default")}
              onClick={() =>
                updateSettings({
                  enableProviderUpdateChecks: DEFAULT_UNIFIED_SETTINGS.enableProviderUpdateChecks,
                })
              }
            />
          ) : null
        }
        control={
          <ScopedSwitch
            settingKeys={["enableProviderUpdateChecks"]}
            checked={settings.enableProviderUpdateChecks}
            onCheckedChange={(checked) =>
              updateSettings({ enableProviderUpdateChecks: Boolean(checked) })
            }
            aria-label={gt("Check provider versions")}
          />
        }
      />
      <AutoUpdateProvidersSetting
        value={settings.autoUpdateProviders}
        checksEnabled={settings.enableProviderUpdateChecks}
        onChange={(autoUpdateProviders) => updateSettings({ autoUpdateProviders })}
      />
    </>
  );
}
