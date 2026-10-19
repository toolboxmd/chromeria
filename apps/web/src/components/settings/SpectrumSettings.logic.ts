import { type Color, type SpectrumSettings, type SpectrumSettingsSlot } from "@t3tools/contracts";

export function renameSpectrumSlot(
  settings: SpectrumSettings,
  index: number,
  label: string,
): SpectrumSettings {
  const previous = settings.slots[index]?.label;
  return {
    ...settings,
    slots: settings.slots.map((slot, i) => (i === index ? { ...slot, label } : slot)),
    white: settings.white === previous ? label : settings.white,
  };
}

export function removeSpectrumSlot(settings: SpectrumSettings, index: number): SpectrumSettings {
  const slots = settings.slots.filter((_, i) => i !== index);
  return {
    ...settings,
    slots,
    white: slots.some((slot) => slot.label === settings.white)
      ? settings.white
      : (slots[0]?.label ?? null),
  };
}

export function addSpectrumSlot(settings: SpectrumSettings, primary: Color): SpectrumSettings {
  let number = settings.slots.length + 1;
  while (settings.slots.some((slot) => slot.label === `Color ${number}`)) number++;
  const label = `Color ${number}`;
  return {
    ...settings,
    slots: [...settings.slots, { label, primary, fallbacks: [] }],
    white: settings.white ?? label,
  };
}

export function spectrumSlotColors(
  slot: SpectrumSettingsSlot,
  colors: readonly Color[],
): SpectrumSettingsSlot {
  const [primary, ...fallbacks] = colors;
  return primary === undefined ? slot : { ...slot, primary, fallbacks };
}
