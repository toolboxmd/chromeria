import {
  type Color,
  SpectrumSettings,
  type SpectrumSettingsSlot,
  type SpectrumWhite,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

const decodeUnknownExitSpectrumSettings = Schema.decodeUnknownExit(SpectrumSettings);

/** White moderates and never speaks as a Color, so no Color may take its name. */
export const isReservedSpectrumName = (label: string) => label.trim().toLowerCase() === "white";

export function spectrumSaveError(settings: SpectrumSettings): string | null {
  if (settings.slots.some((slot) => isReservedSpectrumName(slot.label))) {
    return "White is reserved for the moderator. Rename the Color called White.";
  }
  return decodeUnknownExitSpectrumSettings(settings)._tag === "Failure"
    ? "Use two to eight Colors with unique names, a primary model for every Color and for White, at least two reply rounds and one to twelve turns per Color."
    : null;
}

export function renameSpectrumSlot(
  settings: SpectrumSettings,
  index: number,
  label: string,
): SpectrumSettings {
  return {
    ...settings,
    slots: settings.slots.map((slot, i) => (i === index ? { ...slot, label } : slot)),
  };
}

export function removeSpectrumSlot(settings: SpectrumSettings, index: number): SpectrumSettings {
  return { ...settings, slots: settings.slots.filter((_, i) => i !== index) };
}

/** The first slot also seeds White, so a page with slots never lacks one. */
export function addSpectrumSlot(settings: SpectrumSettings, primary: Color): SpectrumSettings {
  let number = settings.slots.length + 1;
  while (settings.slots.some((slot) => slot.label === `Color ${number}`)) number++;
  const label = `Color ${number}`;
  return {
    ...settings,
    slots: [...settings.slots, { label, primary, fallbacks: [] }],
    white: settings.white ?? { primary, fallbacks: [] },
  };
}

export function spectrumSlotColors(
  slot: SpectrumSettingsSlot,
  colors: readonly Color[],
): SpectrumSettingsSlot {
  const [primary, ...fallbacks] = colors;
  return primary === undefined ? slot : { ...slot, primary, fallbacks };
}

export function spectrumWhiteColors(
  white: SpectrumWhite | null,
  colors: readonly Color[],
): SpectrumWhite | null {
  const [primary, ...fallbacks] = colors;
  return primary === undefined ? white : { primary, fallbacks };
}
