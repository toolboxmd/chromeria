import { ProviderDriverKind, ProviderInstanceId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import {
  chooseColorEffort,
  chooseColorModel,
  chooseColorProvider,
  colorEffortOptions,
  colorProviderChoices,
  defaultColor,
} from "./ColorPicker.logic";

const codex = ProviderInstanceId.make("codex");
const claude = ProviderInstanceId.make("claude");
const effortDescriptor = (...ids: string[]) => ({
  optionDescriptors: [
    {
      id: "reasoningEffort",
      label: "Reasoning",
      type: "select" as const,
      options: ids.map((id) => ({ id, label: id.toUpperCase() })),
    },
  ],
});
const entry = (
  instanceId: ProviderInstanceId,
  models: { slug: string; capabilities?: ReturnType<typeof effortDescriptor> }[],
) => ({
  instanceId,
  driverKind: ProviderDriverKind.make(instanceId === codex ? "codex" : "claudeAgent"),
  displayName: instanceId === codex ? "Codex" : "Claude",
  enabled: true,
  isAvailable: true,
  models: models.map((model) => ({
    slug: model.slug,
    name: model.slug,
    isCustom: false,
    capabilities: model.capabilities ?? null,
  })),
});

const providers = colorProviderChoices(
  [
    entry(codex, [
      { slug: "sol", capabilities: effortDescriptor("low", "high") },
      { slug: "luna", capabilities: effortDescriptor("high", "xhigh") },
    ]),
    entry(claude, [{ slug: "opus", capabilities: effortDescriptor("max") }]),
  ],
  new Map([
    [
      codex,
      [
        { slug: "sol", name: "Sol" },
        { slug: "luna", name: "Luna" },
      ],
    ],
    [claude, [{ slug: "opus", name: "Opus" }]],
  ]),
  () => null,
);

describe("colorProviderChoices", () => {
  it("lists only providers and models enabled for the scope, keeping provider icon fields", () => {
    const options = new Map([
      [
        codex,
        [
          { slug: "enabled", name: "Enabled" },
          { slug: "unavailable", name: "Unavailable", isUnavailable: true },
          { slug: "other-scope", name: "Other scope" },
        ],
      ],
      [claude, [{ slug: "opus", name: "Opus" }]],
    ]);
    const reason = (_: ProviderInstanceId, model: string) =>
      model === "other-scope" ? "Unavailable elsewhere" : null;
    const choices = colorProviderChoices(
      [entry(codex, []), { ...entry(claude, []), enabled: false }],
      options,
      reason,
    );
    expect(choices).toEqual([
      {
        instanceId: codex,
        driverKind: "codex",
        displayName: "Codex",
        models: [{ slug: "enabled", name: "Enabled", efforts: [] }],
      },
    ]);
    expect(
      colorProviderChoices([{ ...entry(codex, []), isAvailable: false }], options, reason),
    ).toEqual([]);
  });

  it("drops a provider whose every model is disabled for the scope", () => {
    expect(
      colorProviderChoices(
        [entry(codex, [])],
        new Map([[codex, [{ slug: "sol", name: "Sol" }]]]),
        () => "Disabled",
      ),
    ).toEqual([]);
  });

  it("takes effort options from each model's own capabilities", () => {
    expect(providers[0]!.models.map((model) => model.efforts.map((effort) => effort.id))).toEqual([
      ["low", "high"],
      ["high", "xhigh"],
    ]);
  });
});

describe("colorEffortOptions", () => {
  it("uses the effort select and excludes prompt-only values", () => {
    expect(
      colorEffortOptions({
        optionDescriptors: [
          {
            id: "agent",
            label: "Agent",
            type: "select",
            options: [{ id: "build", label: "Build" }],
          },
          {
            id: "variant",
            label: "Reasoning",
            type: "select",
            options: [
              { id: "deep", label: "Deep" },
              { id: "prompt", label: "Prompt" },
            ],
            promptInjectedValues: ["prompt"],
          },
        ],
      }),
    ).toEqual([{ id: "deep", label: "Deep" }]);
    expect(colorEffortOptions(null)).toEqual([]);
  });
});

describe("choosing a Color", () => {
  const current = { instanceId: codex, model: "sol", effort: "high" };

  it("starts from the first provider's first model at the default effort", () => {
    expect(defaultColor(providers)).toEqual({ instanceId: codex, model: "sol" });
    expect(defaultColor([])).toBeNull();
  });

  it("resets model and effort when the provider changes", () => {
    expect(chooseColorProvider(providers, current, claude)).toEqual({
      instanceId: claude,
      model: "opus",
    });
    expect(chooseColorProvider(providers, current, codex)).toBe(current);
    expect(chooseColorProvider(providers, current, ProviderInstanceId.make("gone"))).toBe(current);
  });

  it("keeps the effort on a new model only when that model offers it", () => {
    expect(chooseColorModel(providers, current, "luna")).toEqual({
      instanceId: codex,
      model: "luna",
      effort: "high",
    });
    expect(chooseColorModel(providers, { ...current, effort: "low" }, "luna")).toEqual({
      instanceId: codex,
      model: "luna",
    });
    expect(chooseColorModel(providers, current, "opus")).toBe(current);
  });

  it("treats an empty effort as the provider default", () => {
    expect(chooseColorEffort(current, "")).toEqual({ instanceId: codex, model: "sol" });
    expect(chooseColorEffort(current, "low")).toEqual({ ...current, effort: "low" });
  });
});
