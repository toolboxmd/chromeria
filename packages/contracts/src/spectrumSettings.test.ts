import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { ServerSettings, ServerSettingsPatch } from "./settings.ts";
import { SpectrumSettings } from "./spectrumSettings.ts";
import { ProjectId } from "./baseSchemas.ts";

const decodeSyncServerSettings = Schema.decodeSync(ServerSettings);
const decodeSyncSpectrumSettings = Schema.decodeSync(SpectrumSettings);
const encodeSyncSpectrumSettings = Schema.encodeSync(SpectrumSettings);
const decodeSyncServerSettingsPatch = Schema.decodeSync(ServerSettingsPatch);

const color = { instanceId: "codex", model: "sol", effort: "medium" };
const judge = { instanceId: "claude", model: "opus", effort: "high" };
const backup = { instanceId: "codex", model: "backup" };
const configured = {
  slots: [
    { label: "Blue", primary: color },
    { label: "Red", primary: color },
  ],
  white: { primary: judge, fallbacks: [backup] },
};
describe("Spectrum settings", () => {
  it("loads old settings without the key and supplies numeric/mode defaults", () => {
    const old = decodeSyncServerSettings({});
    expect(old.spectrum).toEqual({
      slots: [],
      white: null,
      mode: "council",
      relayRounds: 2,
      turnsPerColor: 3,
    });
    expect(decodeSyncSpectrumSettings(configured)).toEqual({
      ...configured,
      slots: configured.slots.map((slot) => ({ ...slot, fallbacks: [] })),
      mode: "council",
      relayRounds: 2,
      turnsPerColor: 3,
    });
  });
  it("round-trips primary and fallback Colors and whole-value patches", () => {
    const settings = decodeSyncSpectrumSettings({
      ...configured,
      mode: "free",
      relayRounds: 4,
      turnsPerColor: 7,
      slots: [{ ...configured.slots[0]!, fallbacks: [color] }, configured.slots[1]!],
    });
    expect(decodeSyncSpectrumSettings(encodeSyncSpectrumSettings(settings))).toEqual(settings);
    expect(decodeSyncServerSettingsPatch({ spectrum: settings }).spectrum).toEqual(settings);
  });
  it("turns a saved White slot label into that slot's Colors and keeps every slot", () => {
    const slots = [
      { label: "White", primary: color, fallbacks: [] },
      { label: "Red", primary: judge, fallbacks: [backup] },
    ];
    const legacy = { slots, white: "Red", mode: "free" };
    const expected = { slots, white: { primary: judge, fallbacks: [backup] } };
    expect(decodeSyncSpectrumSettings(legacy)).toMatchObject(expected);
    // An older client still writes the label shape; its patch is normalized the same way.
    expect(decodeSyncServerSettingsPatch({ spectrum: legacy }).spectrum).toMatchObject(expected);
    expect(
      decodeSyncServerSettings({
        spectrum: legacy,
        projectSettingsOverrides: { project: { spectrum: { slots, white: "White" } } },
      }).projectSettingsOverrides[ProjectId.make("project")]?.spectrum?.white,
    ).toEqual({ primary: color, fallbacks: [] });
    expect(encodeSyncSpectrumSettings(decodeSyncSpectrumSettings(legacy))).toMatchObject({
      white: {
        primary: judge,
        fallbacks: [backup],
      },
    });
  });
  it("keeps settings without White unset", () => {
    expect(decodeSyncSpectrumSettings({ white: null }).white).toBeNull();
  });
  it.each([
    { relayRounds: 1 },
    { relayRounds: 2.5 },
    { turnsPerColor: 0 },
    { turnsPerColor: 13 },
    { turnsPerColor: 1.5 },
    { white: "Missing" },
    { white: null },
    { white: { primary: judge, fallbacks: [backup, backup, backup, backup, backup] } },
    { slots: [configured.slots[0]!, configured.slots[0]!] },
    { slots: [{ ...configured.slots[0]!, label: "User" }, configured.slots[1]!] },
  ])("rejects invalid settings %j", (patch) => {
    expect(() => decodeSyncSpectrumSettings({ ...configured, ...patch })).toThrow();
  });
});
