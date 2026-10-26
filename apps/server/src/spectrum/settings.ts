import {
  SpectrumRelayRounds,
  SpectrumTurnsPerColor,
  type SpectrumSettings,
  type Color,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import type { SpectrumStart, SpectrumStartRequest, SpectrumWhiteSpec } from "./LaunchService.ts";

export class SpectrumStartValidationError extends Schema.TaggedError<SpectrumStartValidationError>()(
  "SpectrumStartValidationError",
  { message: Schema.String },
) {}
export const isStartValidationError = Schema.is(SpectrumStartValidationError);

const colorCandidate = (color: Color) => ({
  selection: { instanceId: color.instanceId, model: color.model },
  ...(color.effort === undefined ? {} : { effort: color.effort }),
});

/**
 * Settings decode already turns a saved White slot label into White's Colors;
 * this tells the trusted load to write that result back once.
 */
export function savesSpectrumWhiteLabel(raw: string): boolean {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return false;
  }
  if (typeof parsed !== "object" || parsed === null) return false;
  const settings = parsed as {
    readonly spectrum?: { readonly white?: unknown };
    readonly projectSettingsOverrides?: Record<
      string,
      { readonly spectrum?: { readonly white?: unknown } }
    >;
  };
  return [settings, ...Object.values(settings.projectSettingsOverrides ?? {})].some(
    (entry) => typeof entry?.spectrum?.white === "string",
  );
}

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
  if (colors.length < 2 || colors.length > 8)
    throw new SpectrumStartValidationError({
      message: "Configure two to eight Spectrum slots in Settings > Spectrum or pass colors.",
    });
  if (new Set(colors.map((slot) => slot.label)).size !== colors.length)
    throw new SpectrumStartValidationError({ message: "Spectrum slot labels must be unique." });
  if (typeof input.white === "object" && input.moderator !== undefined)
    throw new SpectrumStartValidationError({
      message: "Pass White as a Color or name a Color with moderator, not both.",
    });
  const named = typeof input.white === "string" ? input.white : undefined;
  const copied =
    named === undefined ? input.moderator : colors.findIndex((slot) => slot.label === named);
  if (copied !== undefined && (copied < 0 || copied >= colors.length))
    throw new SpectrumStartValidationError({
      message: "White must name one of the Spectrum slots.",
    });
  if (named !== undefined && input.moderator !== undefined && copied !== input.moderator)
    throw new SpectrumStartValidationError({
      message: "White and moderator name different Colors.",
    });
  // A named Color still speaks; White only runs on a copy of its model.
  const copyOf = ({
    label: _label,
    instructions: _instructions,
    ...model
  }: (typeof colors)[number]) => model;
  const white: SpectrumWhiteSpec =
    typeof input.white === "object"
      ? input.white
      : copied !== undefined
        ? copyOf(colors[copied]!)
        : input.colors === undefined && settings.white !== null
          ? {
              ...colorCandidate(settings.white.primary),
              fallbacks: settings.white.fallbacks.map(colorCandidate),
            }
          : copyOf(colors[0]!);
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
  const { moderator: _moderator, ...start } = input;
  return { ...start, colors, mode, limit, white, turnsPerColor } satisfies SpectrumStart;
}
