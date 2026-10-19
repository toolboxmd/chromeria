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

const decodeSyncStartSpectrumInput = Schema.decodeSync(StartSpectrumInput);
const primary = { instanceId: ProviderInstanceId.make("codex"), model: "sol", effort: "medium" };
const fallback = { ...primary, model: "backup" };
const settings: SpectrumSettings = {
  slots: [
    { label: "Blue", primary, fallbacks: [fallback] },
    { label: "Red", primary, fallbacks: [] },
  ],
  white: "Red",
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
it("resolves configured slots, White and both mode limits", () => {
  const result = resolveSpectrumStart(request, settings);
  expect(result).toMatchObject({ mode: "council", limit: 4, moderator: 1, turnsPerColor: 5 });
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
  expect(result).toMatchObject({ mode: "free", limit: 7, moderator: 0, turnsPerColor: 2 });
  expect(
    resolveSpectrumStart({ ...request, white: "Blue", relayRounds: 6 }, settings),
  ).toMatchObject({ moderator: 0, limit: 6 });
});
it.each([
  { labels: ["Sol", "Grok"], moderator: 0 },
  { labels: ["Sol", "Red"], moderator: 0 },
])("chooses White for explicit Colors without White or moderator: %j", ({ labels, moderator }) => {
  const colors = labels.map((label) => ({ label, ...primary }));
  expect(resolveSpectrumStart({ ...request, colors }, settings)).toMatchObject({
    colors,
    moderator,
  });
});
it.each([
  { relayRounds: 1 },
  { turnsPerColor: 0 },
  { white: "Missing" },
  { limit: 1 },
  { mode: "free" as const, limit: 1 },
  { white: "Red", moderator: 0 },
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
