import {
  SpectrumRelayRounds,
  SpectrumTurnsPerColor,
  type SpectrumSettings,
  type Color,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import type { SpectrumStart, SpectrumStartRequest } from "./LaunchService.ts";

export class SpectrumStartValidationError extends Schema.TaggedError<SpectrumStartValidationError>()(
  "SpectrumStartValidationError",
  { message: Schema.String },
) {}
export const isStartValidationError = Schema.is(SpectrumStartValidationError);

const colorCandidate = (color: Color) => ({
  selection: { instanceId: color.instanceId, model: color.model },
  ...(color.effort === undefined ? {} : { effort: color.effort }),
});

const decodeSyncSpectrumRelayRounds = Schema.decodeSync(SpectrumRelayRounds);
const decodeSyncSpectrumTurnsPerColor = Schema.decodeSync(SpectrumTurnsPerColor);

/** Resolve once, before registration persists the chosen candidates and limits. */
export function resolveSpectrumStart(input: SpectrumStartRequest, settings: SpectrumSettings) {
  if (input.colors === undefined && settings.slots.length === 0)
    throw new SpectrumStartValidationError({
      message:
        "Configure two to eight Spectrum slots in Settings > Spectrum or pass colors to start_spectrum.",
    });
  const colors: SpectrumStart["colors"] =
    input.colors ??
    settings.slots.map(({ label, primary, fallbacks }) => ({
      label,
      ...colorCandidate(primary),
      fallbacks: fallbacks.map(colorCandidate),
    }));
  const mode = input.mode ?? settings.mode;
  const relayRounds = decodeSyncSpectrumRelayRounds(input.relayRounds ?? settings.relayRounds);
  const turnsPerColor = decodeSyncSpectrumTurnsPerColor(
    input.turnsPerColor ?? settings.turnsPerColor,
  );
  const defaultWhite = input.colors === undefined ? settings.white : null;
  const white = input.white ?? (input.moderator === undefined ? defaultWhite : null);
  const moderator =
    white === null ? (input.moderator ?? 0) : colors.findIndex((slot) => slot.label === white);
  if (colors.length < 2 || colors.length > 8)
    throw new SpectrumStartValidationError({
      message: "Configure two to eight Spectrum slots in Settings > Spectrum or pass colors.",
    });
  if (new Set(colors.map((slot) => slot.label)).size !== colors.length)
    throw new SpectrumStartValidationError({ message: "Spectrum slot labels must be unique." });
  if (moderator < 0 || moderator >= colors.length)
    throw new SpectrumStartValidationError({
      message: "White must name one of the Spectrum slots.",
    });
  if (input.white !== undefined && input.moderator !== undefined && moderator !== input.moderator)
    throw new SpectrumStartValidationError({
      message: "White and moderator name different Colors.",
    });
  if (colors.some((slot) => slot.label.trim().length === 0 || slot.label.toLowerCase() === "user"))
    throw new SpectrumStartValidationError({
      message: "Spectrum slot labels must be non-empty and cannot be User.",
    });
  const limit = input.limit ?? (mode === "council" ? relayRounds : colors.length * turnsPerColor);
  if (!Number.isInteger(limit) || (input.limit !== undefined && limit > 100))
    throw new SpectrumStartValidationError({
      message:
        "Spectrum limits must meet the mode minimum; explicit legacy limit cannot exceed 100.",
    });
  if (limit < (mode === "council" ? 2 : colors.length))
    throw new SpectrumStartValidationError({
      message:
        mode === "council"
          ? "Council requires at least two relay rounds."
          : "The free-mode cap cannot be below the number of Colors.",
    });
  return { ...input, colors, mode, limit, moderator, turnsPerColor };
}
