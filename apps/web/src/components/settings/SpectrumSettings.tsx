import {
  DEFAULT_SPECTRUM_SETTINGS,
  SpectrumSettings as SpectrumPreferences,
  type SpectrumSettingsSlot,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import { useState } from "react";
import { useEnvironmentProviders } from "../../state/environments";
import { EMPTY_SERVER_PROVIDERS, serverEnvironment } from "../../state/server";
import { useAtomCommand } from "../../state/use-atom-command";
import { getCustomModelOptionsByInstance } from "../../modelSelection";
import {
  applyProviderInstanceSettings,
  deriveProviderInstanceEntries,
} from "../../providerInstances";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import { Label } from "../ui/label";
import { Select, SelectItem, SelectPopup, SelectTrigger, SelectValue } from "../ui/select";
import { ColorList } from "./ColorList";
import { colorProviderChoices, defaultColor } from "./ColorPicker.logic";
import {
  addSpectrumSlot,
  removeSpectrumSlot,
  renameSpectrumSlot,
  spectrumSlotColors,
  spectrumWhiteColors,
} from "./SpectrumSettings.logic";
import { SettingsGroup } from "./SettingsGroup";
import { useSettingsScope } from "./SettingsScopeContext";
import { SettingsPageContainer, SettingsSection } from "./settingsLayout";
import { useScopedSettings, useScopedSettingsWriteAllowed } from "./useScopedSettings";
import { useScopedModelDisabledReason } from "./useScopedModelAvailability";
import {
  persistScopedSettingsPatch,
  planScopedSettingsPatch,
  planScopedSettingsClear,
} from "./scopedSettings";

const decodeUnknownExitSpectrumPreferences = Schema.decodeUnknownExit(SpectrumPreferences);

export function SpectrumSettings() {
  const { scope, target, targets, environments } = useSettingsScope();
  const settings = useScopedSettings();
  const writable = useScopedSettingsWriteAllowed();
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
  const choices = colorProviderChoices(entries, options, disabledReason);
  // A scope change discards only this page's unsaved form, never another scope's values.
  return (
    <SpectrumEditor
      key={JSON.stringify([
        scope.kind,
        targets.map((value) => [value.environmentId, value.projectId]),
      ])}
      saved={settings.spectrum}
      providers={choices}
      disabled={!target || !writable}
      mixed={targets.some(
        (value) => JSON.stringify(value.settings.spectrum) !== JSON.stringify(settings.spectrum),
      )}
      environments={environments}
      scope={scope}
      target={target}
    />
  );
}

function SpectrumEditor({
  saved,
  providers,
  disabled,
  mixed,
  environments,
  scope,
  target,
}: {
  saved: SpectrumPreferences;
  providers: ReturnType<typeof colorProviderChoices>;
  disabled: boolean;
  mixed: boolean;
  environments: ReturnType<typeof useSettingsScope>["environments"];
  scope: ReturnType<typeof useSettingsScope>["scope"];
  target: ReturnType<typeof useSettingsScope>["target"];
}) {
  const [draft, setDraft] = useState(saved);
  const [slotKeys, setSlotKeys] = useState(saved.slots.map((_, index) => index));
  const [nextSlotKey, setNextSlotKey] = useState(saved.slots.length);
  const [source, setSource] = useState(saved);
  const [dirty, setDirty] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const persist = useAtomCommand(serverEnvironment.updateSettings, { reportFailure: false });
  if (source !== saved && !dirty && !saving) {
    setSource(saved);
    setDraft(saved);
    setSlotKeys(saved.slots.map((_, index) => index));
    setNextSlotKey(saved.slots.length);
  }
  const change = (next: SpectrumPreferences) => {
    setDraft(next);
    setDirty(true);
    setError(null);
  };
  const changeSlot = (index: number, slot: SpectrumSettingsSlot) =>
    change({ ...draft, slots: draft.slots.map((value, i) => (i === index ? slot : value)) });
  async function save(reset = false) {
    if (saving) return;
    const decoded = decodeUnknownExitSpectrumPreferences(draft);
    if (!reset && decoded._tag === "Failure") {
      setError(
        "Use two to eight unique labels, a primary Color for every slot and for White, at least two reply rounds and one to twelve turns per Color.",
      );
      return;
    }
    const projectScope = scope.kind === "project" || scope.kind === "checkout";
    const plan =
      reset && projectScope
        ? planScopedSettingsClear(scope, environments, ["spectrum"])
        : planScopedSettingsPatch(scope, environments, {
            spectrum: reset ? DEFAULT_SPECTRUM_SETTINGS : draft,
          });
    if (plan.unavailableReason) {
      setError(plan.unavailableReason);
      return;
    }
    setSaving(true);
    setError(null);
    let confirmed: SpectrumPreferences | undefined;
    const result = await persistScopedSettingsPatch(
      plan,
      async (request) => {
        const response = await persist(request);
        if (response._tag === "Success" && request.environmentId === target?.environmentId) {
          confirmed = resolveProjectSettings(response.value, target.projectId).settings.spectrum;
        }
        return response;
      },
      () => {},
    );
    setSaving(false);
    if (
      confirmed &&
      result.savedEnvironments.some((value) => value.environmentId === target?.environmentId)
    ) {
      setDirty(false);
      setDraft(confirmed);
      setSlotKeys(confirmed.slots.map((_, index) => index));
      setNextSlotKey(confirmed.slots.length);
    }
    if (result.failedEnvironments.length > 0) {
      setError(
        `Could not save Spectrum settings on ${result.failedEnvironments.map((value) => value.label).join(", ")}.`,
      );
      return;
    }
    setDirty(false);
  }

  const addColor = defaultColor(providers);
  return (
    <SettingsPageContainer width="wide">
      <SettingsSection
        id="spectrum-defaults"
        title="Spectrum"
        variant="plain"
        headerAction={
          <Button
            size="xs"
            variant="ghost-muted"
            disabled={disabled || saving}
            onClick={() => void save(true)}
          >
            {scope.kind === "project" || scope.kind === "checkout"
              ? "Use environment Spectrum settings"
              : "Reset Spectrum settings"}
          </Button>
        }
      >
        <p className="px-3 text-sm text-muted-foreground sm:px-4">
          Choose default slots and White, then ask an agent to start a Spectrum with just a
          question. Each slot and White try their primary Color, then their fallbacks, only before a
          turn starts. Save when you finish editing.
        </p>
        {mixed && (
          <p role="status" className="px-3 text-sm text-muted-foreground sm:px-4">
            Settings differ across this scope. Saving replaces Spectrum defaults on all selected
            targets.
          </p>
        )}
        {error && (
          <p role="alert" className="px-3 text-sm text-destructive sm:px-4">
            {error}
          </p>
        )}
        <SettingsGroup>
          <div className="space-y-6 p-4">
            {draft.slots.map((slot, index) => (
              <section key={slotKeys[index]} className="space-y-3">
                <div className="flex items-center gap-3">
                  <Label htmlFor={`spectrum-label-${index}`}>Slot label</Label>
                  <div className="flex-1">
                    <Input
                      id={`spectrum-label-${index}`}
                      value={slot.label}
                      disabled={disabled || saving}
                      onChange={(event) =>
                        change(renameSpectrumSlot(draft, index, event.target.value))
                      }
                    />
                  </div>
                  <Button
                    size="sm"
                    variant="ghost"
                    disabled={disabled || saving}
                    onClick={() => {
                      change(removeSpectrumSlot(draft, index));
                      setSlotKeys(slotKeys.filter((_, position) => position !== index));
                    }}
                  >
                    Remove slot
                  </Button>
                </div>
                <ColorList
                  title={slot.label || `Slot ${index + 1}`}
                  saved={[slot.primary, ...slot.fallbacks]}
                  providers={providers}
                  disabled={disabled || saving}
                  saving={false}
                  mixed={false}
                  minItems={1}
                  maxItems={5}
                  save={(colors) => {
                    changeSlot(index, spectrumSlotColors(slot, colors));
                    return Promise.resolve();
                  }}
                />
              </section>
            ))}
            <Button
              size="sm"
              variant="outline"
              disabled={disabled || saving || !addColor || draft.slots.length >= 8}
              onClick={() => {
                if (addColor) {
                  change(addSpectrumSlot(draft, addColor));
                  setSlotKeys([...slotKeys, nextSlotKey]);
                  setNextSlotKey(nextSlotKey + 1);
                }
              }}
            >
              Add slot
            </Button>
            {draft.slots.length === 0 && (
              <p className="text-sm text-muted-foreground">
                Configure two to eight slots before starting with only a question.
              </p>
            )}
            <section className="space-y-3">
              <Label>White</Label>
              <p className="text-xs text-muted-foreground">
                White is not a Color: it writes the council's final answer, and in a free discussion
                picks who speaks next and writes the closing summary.
              </p>
              <ColorList
                title="White"
                saved={draft.white === null ? [] : [draft.white.primary, ...draft.white.fallbacks]}
                providers={providers}
                disabled={disabled || saving}
                saving={false}
                mixed={false}
                minItems={1}
                maxItems={5}
                save={(colors) => {
                  change({ ...draft, white: spectrumWhiteColors(draft.white, colors) });
                  return Promise.resolve();
                }}
              />
            </section>
            <div className="space-y-2">
              <Label htmlFor="spectrum-mode">Default mode</Label>
              <Select
                value={draft.mode}
                disabled={disabled || saving}
                onValueChange={(mode) =>
                  change({ ...draft, mode: mode === "free" ? "free" : "council" })
                }
              >
                <SelectTrigger id="spectrum-mode">
                  <SelectValue>{draft.mode === "free" ? "Free" : "Council"}</SelectValue>
                </SelectTrigger>
                <SelectPopup>
                  <SelectItem value="council">Council</SelectItem>
                  <SelectItem value="free">Free</SelectItem>
                </SelectPopup>
              </Select>
            </div>
            <div className="space-y-2">
              <Label htmlFor="spectrum-rounds">Council: reply rounds</Label>
              <Input
                id="spectrum-rounds"
                type="number"
                min={2}
                step={1}
                value={draft.relayRounds}
                disabled={disabled || saving}
                onChange={(event) => change({ ...draft, relayRounds: event.target.valueAsNumber })}
              />
              <p className="text-xs text-muted-foreground">
                After every Color answers on its own, each Color reads the others' answers and
                replies this many times before White writes the final answer. At least 2.
              </p>
            </div>
            <div className="space-y-2">
              <Label htmlFor="spectrum-turns">Free discussion: turns per Color</Label>
              <Input
                id="spectrum-turns"
                type="number"
                max={12}
                min={1}
                step={1}
                value={draft.turnsPerColor}
                disabled={disabled || saving}
                onChange={(event) =>
                  change({ ...draft, turnsPerColor: event.target.valueAsNumber })
                }
              />
              <p className="text-xs text-muted-foreground">
                Sets the length of a free discussion: it ends after this many turns per Color in
                total (for example 3 with 4 Colors allows 12 turns), or earlier when White says it
                is done.
              </p>
            </div>
            <Button disabled={disabled || saving || !dirty} onClick={() => void save()}>
              {saving ? "Saving…" : "Save Spectrum settings"}
            </Button>
          </div>
        </SettingsGroup>
      </SettingsSection>
    </SettingsPageContainer>
  );
}
