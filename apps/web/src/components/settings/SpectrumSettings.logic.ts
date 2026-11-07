import {
  type Color,
  type SpectrumSettings,
  type SpectrumSettingsSlot,
  type SpectrumWhite,
} from "@t3tools/contracts";

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
