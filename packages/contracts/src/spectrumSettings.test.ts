import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { ServerSettings, ServerSettingsPatch } from "./settings.ts";
import { SpectrumSettings } from "./spectrumSettings.ts";

const decodeSyncServerSettings = Schema.decodeSync(ServerSettings);
const decodeSyncSpectrumSettings = Schema.decodeSync(SpectrumSettings);
const encodeSyncSpectrumSettings = Schema.encodeSync(SpectrumSettings);
const decodeSyncServerSettingsPatch = Schema.decodeSync(ServerSettingsPatch);

const color = { instanceId: "codex", model: "sol", effort: "medium" };
const configured = {
  slots: [
    { label: "Blue", primary: color },
    { label: "Red", primary: color },
  ],
  white: "Red",
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
  it.each([
    { relayRounds: 1 },
    { relayRounds: 2.5 },
    { turnsPerColor: 0 },
    { turnsPerColor: 13 },
    { turnsPerColor: 1.5 },
    { white: "Missing" },
    { white: null },
    { slots: [configured.slots[0]!, configured.slots[0]!] },
    { slots: [{ ...configured.slots[0]!, label: "User" }, configured.slots[1]!] },
  ])("rejects invalid settings %j", (patch) => {
    expect(() => decodeSyncSpectrumSettings({ ...configured, ...patch })).toThrow();
  });
});
