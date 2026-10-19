import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
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

export const SpectrumSettings = Schema.Struct({
  slots: Schema.Array(SpectrumSettingsSlot)
    .check(Schema.isMaxLength(8))
    .pipe(Schema.withDecodingDefault(Effect.succeed([]))),
  white: Schema.NullOr(TrimmedNonEmptyString).pipe(
    Schema.withDecodingDefault(Effect.succeed(null)),
  ),
  mode: SpectrumMode.pipe(Schema.withDecodingDefault(Effect.succeed("council" as const))),
  relayRounds: SpectrumRelayRounds.pipe(Schema.withDecodingDefault(Effect.succeed(2))),
  turnsPerColor: SpectrumTurnsPerColor.pipe(Schema.withDecodingDefault(Effect.succeed(3))),
}).check(
  Schema.makeFilter((settings) => {
    if (new Set(settings.slots.map((slot) => slot.label)).size !== settings.slots.length)
      return "Spectrum slot labels must be unique.";
    if (settings.slots.length === 0)
      return settings.white === null || "White must name a Spectrum slot.";
    if (settings.slots.length < 2) return "Configure at least two Spectrum slots.";
    return (
      settings.slots.some((slot) => slot.label === settings.white) ||
      "White must name a Spectrum slot."
    );
  }),
);
export type SpectrumSettings = typeof SpectrumSettings.Type;
export const DEFAULT_SPECTRUM_SETTINGS = Schema.decodeSync(SpectrumSettings)({});
