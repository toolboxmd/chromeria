import { describe, expect, it } from "vite-plus/test";
import {
  DEFAULT_SPECTRUM_SETTINGS,
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
  SpectrumSettings,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { applyServerSettingsPatch } from "@t3tools/shared/serverSettings";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import { DEFAULT_SERVER_SETTINGS } from "@t3tools/contracts";
import {
  addSpectrumSlot,
  removeSpectrumSlot,
  renameSpectrumSlot,
  spectrumSlotColors,
  spectrumWhiteColors,
} from "./SpectrumSettings.logic";
import { planScopedSettingsPatch, planScopedSettingsClear } from "./scopedSettings";
import { resolveSettingsScope } from "./settingsScope";

const decodeSyncSpectrumSettings = Schema.decodeSync(SpectrumSettings);
const primary = { instanceId: ProviderInstanceId.make("codex"), model: "sol" };
const blue = { label: "Blue", primary, fallbacks: [{ ...primary, model: "backup" }] };
const red = { label: "Red", primary, fallbacks: [] };
const judge = { instanceId: ProviderInstanceId.make("claude"), model: "opus" };
const white = { primary: judge, fallbacks: [primary] };
const configured = { ...DEFAULT_SPECTRUM_SETTINGS, slots: [blue, red], white };
it("keeps White separate from slot edits and allocates unique labels", () => {
  // A slot may be called White; it stays a slot and White keeps its own Colors.
  const renamed = renameSpectrumSlot(configured, 1, "White");
  expect(renamed.slots.map((slot) => slot.label)).toEqual(["Blue", "White"]);
  expect(renamed.white).toEqual(white);
  expect(removeSpectrumSlot(configured, 1).white).toEqual(white);
  expect(addSpectrumSlot(configured, primary).white).toEqual(white);
  const first = addSpectrumSlot(DEFAULT_SPECTRUM_SETTINGS, primary);
  expect(first.white).toEqual({ primary, fallbacks: [] });
  const next = addSpectrumSlot(
    { ...first, slots: [...first.slots, { ...red, label: "Color 3" }] },
    primary,
  );
  expect(next.slots.map((slot) => slot.label)).toEqual(["Color 1", "Color 3", "Color 4"]);
});
it("saves White's list as its primary and fallbacks, and resets to no White", () => {
  const edited = { ...configured, white: spectrumWhiteColors(white, [primary, judge]) };
  expect(edited.white).toEqual({ primary, fallbacks: [judge] });
  expect(decodeSyncSpectrumSettings(edited)).toEqual(edited);
  expect(spectrumWhiteColors(white, [])).toEqual(white);
  expect(spectrumWhiteColors(null, [judge])).toEqual({ primary: judge, fallbacks: [] });
  expect(() => decodeSyncSpectrumSettings({ ...configured, white: null })).toThrow();
  expect(DEFAULT_SPECTRUM_SETTINGS.white).toBeNull();
});
it("keeps the first Color primary when reordering and never removes the last Color", () => {
  expect(spectrumSlotColors(blue, [blue.fallbacks[0]!, primary])).toEqual({
    ...blue,
    primary: blue.fallbacks[0],
    fallbacks: [primary],
  });
  expect(spectrumSlotColors(blue, [])).toEqual(blue);
  expect(() => decodeSyncSpectrumSettings(renameSpectrumSlot(configured, 1, "Blue"))).toThrow();
});
const environmentId = EnvironmentId.make("one");
const projectId = ProjectId.make("project");
const settings = {
  ...DEFAULT_SERVER_SETTINGS,
  spectrum: configured,
  projectSettingsOverrides: {
    [projectId]: { defaultAutoPull: false, spectrum: { ...configured, mode: "free" as const } },
  },
};
const environment = {
  environmentId,
  label: "one",
  connection: { phase: "connected" as const },
  serverConfig: { settings, environment: { capabilities: { projectSettingsOverrides: true } } },
};
const environmentScope = resolveSettingsScope({}, [], [environment]);
const member = {
  id: projectId,
  environmentId,
  title: "Home",
  workspaceRoot: "/home",
  physicalProjectKey: "one:/home",
  environmentLabel: "one",
  defaultModelSelection: null,
  scripts: [],
  createdAt: "2026-10-19T00:00:00Z",
  updatedAt: "2026-10-19T00:00:00Z",
};
const group = {
  ...member,
  projectKey: "group",
  displayName: "Home",
  memberProjects: [member],
  memberProjectRefs: [{ environmentId, projectId }],
  groupedProjectCount: 1,
  environmentPresence: "remote-only" as const,
  allRemoteMembersAreDesktopLocal: false,
  allRemoteMembersAreWsl: false,
  remoteEnvironmentLabels: ["one"],
};
const projectScope = resolveSettingsScope({ project: "group" }, [group], [environment]);
describe("Spectrum scoped persistence", () => {
  it("replaces the environment value and leaves unrelated settings and project overrides intact", () => {
    const plan = planScopedSettingsPatch(environmentScope, [environment], {
      spectrum: { ...configured, relayRounds: 8, slots: [red, blue] },
    });
    expect(plan.unavailableReason).toBeNull();
    const next = applyServerSettingsPatch(settings, plan.serverWrites[0]!.patch);
    expect(next.spectrum.relayRounds).toBe(8);
    expect(next.spectrum.slots.map((slot) => slot.label)).toEqual(["Red", "Blue"]);
    expect(next.projectSettingsOverrides).toEqual(settings.projectSettingsOverrides);
  });
  it("supports a whole project override, then removes only Spectrum on reset", () => {
    const plan = planScopedSettingsClear(projectScope, [environment], ["spectrum"]);
    const next = applyServerSettingsPatch(settings, plan.serverWrites[0]!.patch);
    expect(next.projectSettingsOverrides[projectId]?.defaultAutoPull).toBe(false);
    expect(resolveProjectSettings(next, projectId).settings.spectrum).toEqual(configured);
  });
});
