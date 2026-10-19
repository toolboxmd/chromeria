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
} from "./SpectrumSettings.logic";
import { planScopedSettingsPatch, planScopedSettingsClear } from "./scopedSettings";
import { resolveSettingsScope } from "./settingsScope";

const decodeSyncSpectrumSettings = Schema.decodeSync(SpectrumSettings);
const primary = { instanceId: ProviderInstanceId.make("codex"), model: "sol" };
const blue = { label: "Blue", primary, fallbacks: [{ ...primary, model: "backup" }] };
const red = { label: "Red", primary, fallbacks: [] };
const configured = { ...DEFAULT_SPECTRUM_SETTINGS, slots: [blue, red], white: "Red" };
it("renames White by identity, removes it safely, and allocates unique labels", () => {
  expect(renameSpectrumSlot(configured, 1, "White").white).toBe("White");
  expect(removeSpectrumSlot(configured, 1).white).toBe("Blue");
  const first = addSpectrumSlot(DEFAULT_SPECTRUM_SETTINGS, primary);
  expect(first.white).toBe("Color 1");
  const next = addSpectrumSlot(
    { ...first, slots: [...first.slots, { ...red, label: "Color 3" }] },
    primary,
  );
  expect(next.slots.map((slot) => slot.label)).toEqual(["Color 1", "Color 3", "Color 4"]);
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
