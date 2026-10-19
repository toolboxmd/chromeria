import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SchemaTransformation from "effect/SchemaTransformation";
import { TrimmedNonEmptyString } from "./baseSchemas.ts";
import { Color } from "./prism.ts";

export const SpectrumMode = Schema.Literals(["council", "free"]);
export const SpectrumRelayRounds = Schema.Int.check(Schema.isGreaterThanOrEqualTo(2));
export const SpectrumTurnsPerColor = Schema.Int.check(
  Schema.isBetween({ minimum: 1, maximum: 12 }),
);
export const SpectrumSettingsSlot = Schema.Struct({
  label: TrimmedNonEmptyString.check(
    Schema.makeFilter((label) => label.toLowerCase() !== "user" || "The label User is reserved."),
  ),
  primary: Color,
  fallbacks: Schema.Array(Color)
    .check(Schema.isMaxLength(4))
    .pipe(Schema.withDecodingDefault(Effect.succeed([]))),
});
export type SpectrumSettingsSlot = typeof SpectrumSettingsSlot.Type;

/** White's model: a primary Color and fallbacks, like a slot without a label. */
export const SpectrumWhite = Schema.Struct({
  primary: Color,
  fallbacks: Schema.Array(Color)
    .check(Schema.isMaxLength(4))
    .pipe(Schema.withDecodingDefault(Effect.succeed([]))),
});
export type SpectrumWhite = typeof SpectrumWhite.Type;

const spectrumSettingsFields = {
  slots: Schema.Array(SpectrumSettingsSlot)
    .check(Schema.isMaxLength(8))
    .pipe(Schema.withDecodingDefault(Effect.succeed([]))),
  mode: SpectrumMode.pipe(Schema.withDecodingDefault(Effect.succeed("council" as const))),
  relayRounds: SpectrumRelayRounds.pipe(Schema.withDecodingDefault(Effect.succeed(2))),
  turnsPerColor: SpectrumTurnsPerColor.pipe(Schema.withDecodingDefault(Effect.succeed(3))),
};
const CurrentSpectrumSettings = Schema.Struct({
  ...spectrumSettingsFields,
  white: Schema.NullOr(SpectrumWhite).pipe(Schema.withDecodingDefault(Effect.succeed(null))),
}).check(
  Schema.makeFilter((settings) => {
    if (new Set(settings.slots.map((slot) => slot.label)).size !== settings.slots.length)
      return "Spectrum slot labels must be unique.";
    if (settings.slots.length === 0) return true;
    if (settings.slots.length < 2) return "Configure at least two Spectrum slots.";
    return settings.white !== null || "Choose White's Color.";
  }),
);

/**
 * Builds before #525 saved White as a slot label. Such a value, from the settings
 * file or an older client's write, becomes a copy of that slot's Colors; the slots
 * stay as they are, so a slot labelled "White" remains a Color. A label naming no
 * slot is left to fail decoding.
 */
function whiteFromSlotLabel(raw: unknown): unknown {
  if (typeof raw !== "object" || raw === null || !("white" in raw)) return raw;
  const { white, slots } = raw as { readonly white: unknown; readonly slots?: unknown };
  if (typeof white !== "string" || !Array.isArray(slots)) return raw;
  const slot: unknown = slots.find(
    (candidate: unknown) =>
      typeof candidate === "object" &&
      candidate !== null &&
      "label" in candidate &&
      typeof candidate.label === "string" &&
      candidate.label.trim() === white.trim(),
  );
  if (typeof slot !== "object" || slot === null || !("primary" in slot)) return raw;
  const fallbacks = "fallbacks" in slot ? slot.fallbacks : [];
  return { ...raw, white: { primary: slot.primary, fallbacks } };
}

export const SpectrumSettings = Schema.Unknown.pipe(
  Schema.decodeTo(
    CurrentSpectrumSettings,
    SchemaTransformation.transform<typeof CurrentSpectrumSettings.Encoded, unknown>({
      decode: (raw) => whiteFromSlotLabel(raw) as typeof CurrentSpectrumSettings.Encoded,
      encode: (settings) => settings,
    }),
  ),
);
export type SpectrumSettings = typeof SpectrumSettings.Type;
export const DEFAULT_SPECTRUM_SETTINGS = Schema.decodeSync(SpectrumSettings)({});
