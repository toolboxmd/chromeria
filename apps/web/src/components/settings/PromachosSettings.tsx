import { useEnvironmentProviders } from "../../state/environments";
import { EMPTY_SERVER_PROVIDERS } from "../../state/server";
import { getCustomModelOptionsByInstance } from "../../modelSelection";
import {
  applyProviderInstanceSettings,
  deriveProviderInstanceEntries,
} from "../../providerInstances";
import { Button } from "../ui/button";
import { ColorList } from "./ColorList";
import { colorProviderChoices } from "./ColorPicker.logic";
import { planPromachosReset } from "./PromachosSettings.logic";
import { SettingsGroup } from "./SettingsGroup";
import { useSettingsScope } from "./SettingsScopeContext";
import { SettingsPageContainer, SettingsSection } from "./settingsLayout";
import { useScopedSettings } from "./useScopedSettings";
import { useScopedModelDisabledReason } from "./useScopedModelAvailability";
import { usePrismSettingsSave } from "./usePrismSettingsSave";

export function PromachosSettings() {
  const { scope, target, targets, environments } = useSettingsScope();
  const settings = useScopedSettings();
  const { pendingWrite, saveError, setSaveError, savePlan, saveRole } = usePrismSettingsSave();
  const providersByEnvironment = useEnvironmentProviders();
  const providers =
    (target && providersByEnvironment.get(target.environmentId)) ?? EMPTY_SERVER_PROVIDERS;
  const entries = applyProviderInstanceSettings(
    deriveProviderInstanceEntries(providers),
    settings,
    target?.projectId ?? null,
  );
  const options = getCustomModelOptionsByInstance(settings, providers);
  const disabledReason = useScopedModelDisabledReason(settings, entries);
  const providerChoices = colorProviderChoices(entries, options, disabledReason);
  const projectScope = scope.kind === "project" || scope.kind === "checkout";
  return (
    <SettingsPageContainer width="wide">
      <SettingsSection
        id="promachos-colors"
        title="Promachos"
        variant="plain"
        headerAction={
          <Button
            size="xs"
            variant="ghost-muted"
            disabled={!target || pendingWrite !== null}
            onClick={() => {
              void savePlan(planPromachosReset(scope, environments), {
                kind: "inherit",
                roles: ["promachos"],
              }).catch((cause: unknown) =>
                setSaveError(
                  cause instanceof Error ? cause.message : "Could not reset Promachos settings.",
                ),
              );
            }}
          >
            {projectScope ? "Use environment Promachos settings" : "Reset Promachos settings"}
          </Button>
        }
      >
        <p className="px-3 text-sm text-muted-foreground sm:px-4">
          New Promachos conversations use the first Color with capacity; the rest are fallbacks. An
          empty list uses the model picked in the composer. Changes save automatically.
        </p>
        {!target && (
          <p role="status" className="px-3 text-sm text-muted-foreground sm:px-4">
            Connect an environment to edit Promachos preferences.
          </p>
        )}
        {saveError && (
          <p role="alert" className="px-3 text-sm text-destructive sm:px-4">
            {saveError}
          </p>
        )}
        <SettingsGroup>
          <div className="space-y-3 p-4">
            {pendingWrite && (
              <p role="status" className="text-xs text-muted-foreground">
                Saving…
              </p>
            )}
            <ColorList
              title="Promachos"
              saved={settings.prismRoles.promachos.models}
              providers={providerChoices}
              disabled={!target}
              saving={pendingWrite !== null}
              mixed={targets.some(
                (candidate) =>
                  JSON.stringify(candidate.settings.prismRoles.promachos.models) !==
                  JSON.stringify(settings.prismRoles.promachos.models),
              )}
              save={(models) => saveRole({ kind: "models", role: "promachos", models })}
            />
          </div>
        </SettingsGroup>
      </SettingsSection>
    </SettingsPageContainer>
  );
}
