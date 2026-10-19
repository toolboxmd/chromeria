import {
  SpectrumRelayRounds,
  SpectrumTurnsPerColor,
  type SpectrumSettings,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import type { SpectrumStart, SpectrumStartRequest } from "./LaunchService.ts";

const decodeSyncSpectrumRelayRounds = Schema.decodeSync(SpectrumRelayRounds);
const decodeSyncSpectrumTurnsPerColor = Schema.decodeSync(SpectrumTurnsPerColor);

/** Resolve once, before registration persists the chosen candidates and limits. */
export function resolveSpectrumStart(input: SpectrumStartRequest, settings: SpectrumSettings) {
  const colors: SpectrumStart["colors"] =
    input.colors ??
    settings.slots.map(({ label, primary, fallbacks }) => ({
      label,
      ...primary,
      fallbacks,
    }));
  const mode = input.mode ?? settings.mode;
  const relayRounds = decodeSyncSpectrumRelayRounds(input.relayRounds ?? settings.relayRounds);
  const turnsPerColor = decodeSyncSpectrumTurnsPerColor(
    input.turnsPerColor ?? settings.turnsPerColor,
  );
  const white = input.white ?? (input.moderator === undefined ? settings.white : null);
  const moderator =
    white === null ? (input.moderator ?? 0) : colors.findIndex((slot) => slot.label === white);
  if (colors.length < 2 || colors.length > 8)
    throw new Error("Configure two to eight Spectrum slots in Settings > Spectrum or pass colors.");
  if (new Set(colors.map((slot) => slot.label)).size !== colors.length)
    throw new Error("Spectrum slot labels must be unique.");
  if (
    moderator < 0 ||
    moderator >= colors.length ||
    (input.white !== undefined && input.moderator !== undefined && moderator !== input.moderator)
  )
    throw new Error("White must name one of the Spectrum slots.");
  if (colors.some((slot) => slot.label.trim().length === 0 || slot.label.toLowerCase() === "user"))
    throw new Error("Spectrum slot labels must be non-empty and cannot be User.");
  const limit = input.limit ?? (mode === "council" ? relayRounds : colors.length * turnsPerColor);
  if (!Number.isInteger(limit) || limit < (mode === "council" ? 2 : 1) || limit > 100)
    throw new Error("Spectrum limit must be an integer between the mode minimum and 100.");
  return { ...input, colors, mode, limit, moderator, turnsPerColor };
}
