import { expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";
import { StartSpectrumInput } from "./mcpToolkit.ts";

it("accepts a question-only Spectrum start", () => {
  expect(decodeSyncStartSpectrumInput({ question: "Compare these designs" })).toEqual({
    question: "Compare these designs",
  });
});

import { CommandId, ProviderInstanceId, ThreadId, type SpectrumSettings } from "@t3tools/contracts";
import { resolveSpectrumStart } from "./settings.ts";
import { whiteMemberLabel } from "./LaunchService.ts";

const decodeSyncStartSpectrumInput = Schema.decodeSync(StartSpectrumInput);
const primary = { instanceId: ProviderInstanceId.make("codex"), model: "sol", effort: "medium" };
const fallback = { ...primary, model: "backup" };
const judge = { instanceId: ProviderInstanceId.make("claude"), model: "opus", effort: "high" };
const settings: SpectrumSettings = {
  slots: [
    { label: "Blue", primary, fallbacks: [fallback] },
    { label: "Red", primary, fallbacks: [] },
  ],
  white: { primary: judge, fallbacks: [fallback] },
  mode: "council",
  relayRounds: 4,
  turnsPerColor: 5,
};
const request = {
  commandId: CommandId.make("start"),
  threadId: ThreadId.make("spectrum"),
  callerThreadId: ThreadId.make("caller"),
  callerRunId: null,
  question: "Question",
};
const candidate = ({ instanceId, model, effort }: typeof judge | typeof primary) => ({
  selection: { instanceId, model },
  effort,
});
it("resolves configured slots, White and both mode limits", () => {
  const result = resolveSpectrumStart(request, settings);
  expect(result).toMatchObject({ mode: "council", limit: 4, turnsPerColor: 5 });
  // White is its own member on its own model, not one of the two Colors.
  expect(result.colors.map((color) => color.label)).toEqual(["Blue", "Red"]);
  expect(result.white).toEqual({ ...candidate(judge), fallbacks: [candidate(fallback)] });
  expect(result.colors[0]?.fallbacks).toEqual([
    {
      selection: { instanceId: fallback.instanceId, model: fallback.model },
      effort: fallback.effort,
    },
  ]);
  expect(resolveSpectrumStart({ ...request, mode: "free" }, settings).limit).toBe(10);
});
it("explicit Colors never inherit saved fallbacks, while supplied fallbacks survive", () => {
  const colors = [
    { label: "Blue", ...primary },
    { label: "Red", ...primary, fallbacks: [fallback] },
  ];
  const result = resolveSpectrumStart(
    { ...request, colors, mode: "free", limit: 7, moderator: 0, relayRounds: 6, turnsPerColor: 2 },
    settings,
  );
  expect(result.colors).toEqual(colors);
  expect(result.colors[0]?.fallbacks).toBeUndefined();
  expect(result).toMatchObject({ mode: "free", limit: 7, turnsPerColor: 2, white: primary });
});
it("a legacy White label or moderator index copies that Color's model, and the Color still speaks", () => {
  const colors = [
    { label: "Blue", ...primary, instructions: "Be blue." },
    { label: "Red", ...judge, fallbacks: [fallback] },
  ];
  const red = { ...judge, fallbacks: [fallback] };
  expect(resolveSpectrumStart({ ...request, colors, white: "Red" }, settings)).toMatchObject({
    colors,
    white: red,
  });
  expect(resolveSpectrumStart({ ...request, colors, moderator: 1 }, settings).white).toEqual(red);
  expect(resolveSpectrumStart({ ...request, colors, moderator: 0 }, settings).white).toEqual(
    primary,
  );
  // With saved slots, a label names a saved slot.
  const saved = resolveSpectrumStart({ ...request, white: "Blue", relayRounds: 6 }, settings);
  expect(saved).toMatchObject({ limit: 6, white: { ...candidate(primary) } });
  expect(saved.colors).toHaveLength(2);
});
it("takes White as its own Color, with fallbacks", () => {
  const white = { ...judge, fallbacks: [fallback] };
  const result = resolveSpectrumStart({ ...request, white }, settings);
  expect(result.white).toEqual(white);
  expect(result.colors).toHaveLength(2);
});
it.each([
  ["Sol", "Grok"],
  ["Sol", "Red"],
])("copies the first explicit Color's model for White when White is omitted: %j", (...labels) => {
  const colors = labels.map((label) => ({ label, ...primary }));
  expect(resolveSpectrumStart({ ...request, colors }, settings)).toMatchObject({
    colors,
    white: primary,
  });
});
it.each([
  { relayRounds: 1 },
  { turnsPerColor: 0 },
  { white: "Missing" },
  { limit: 1 },
  { mode: "free" as const, limit: 1 },
  { white: "Red", moderator: 0 },
  { moderator: 2 },
  { white: judge, moderator: 0 },
  {
    colors: [
      { label: "Red", ...primary },
      { label: "Red", ...primary },
    ],
  },
])("rejects invalid start overrides %j", (patch) => {
  expect(() => resolveSpectrumStart({ ...request, ...patch }, settings)).toThrow();
});

it("honors configured limits above the legacy tool cap, while explicit legacy limit stays capped", () => {
  expect(resolveSpectrumStart(request, { ...settings, relayRounds: 101 }).limit).toBe(101);
  expect(
    resolveSpectrumStart({ ...request, mode: "free" }, { ...settings, turnsPerColor: 12 }).limit,
  ).toBe(24);
  expect(() => resolveSpectrumStart({ ...request, limit: 101 }, settings)).toThrow();
});

it("names the separate White after the first label no Color uses, ignoring case", () => {
  expect(whiteMemberLabel(["Blue", "Red"])).toBe("White");
  expect(whiteMemberLabel(["white", "Red"])).toBe("White (moderator)");
  expect(whiteMemberLabel(["White", "white (Moderator)"])).toBe("White (moderator 2)");
  expect(whiteMemberLabel(["White", "White (moderator)", "White (moderator 2)"])).toBe(
    "White (moderator 3)",
  );
});
