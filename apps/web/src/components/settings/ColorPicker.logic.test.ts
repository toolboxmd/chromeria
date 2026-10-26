import { ProviderDriverKind, ProviderInstanceId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import {
  chooseColorEffort,
  chooseColorModel,
  chooseColorOption,
  colorKey,
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

const speed = {
  id: "serviceTier",
  label: "Speed",
  type: "select" as const,
  options: [
    { id: "default", label: "Standard" },
    { id: "priority", label: "Priority" },
  ],
};
it("offers advertised speed with priority labeled Fast and leaves Grok Fast as a model", () => {
  const choices = colorProviderChoices(
    [
      {
        ...entry(codex, []),
        models: [
          {
            slug: "sol",
            name: "Sol",
            isCustom: false,
            capabilities: { optionDescriptors: [speed] },
          },
        ],
      },
      { ...entry(claude, [{ slug: "grok-fast" }]), driverKind: ProviderDriverKind.make("grok") },
    ],
    new Map([
      [codex, [{ slug: "sol", name: "Sol" }]],
      [claude, [{ slug: "grok-fast", name: "Grok Fast" }]],
    ]),
    () => null,
  );
  expect(choices[0]?.models[0]?.options?.[0]).toMatchObject({
    options: [
      { id: "default", label: "Standard" },
      { id: "priority", label: "Fast" },
    ],
  });
  expect(choices[1]?.models[0]).toEqual({ slug: "grok-fast", name: "Grok Fast", efforts: [] });
});
it("preserves other options when changing effort or clearing speed, and keeps only supported options on a new model", () => {
  const fast = {
    instanceId: codex,
    model: "sol",
    effort: "high",
    options: [
      { id: "serviceTier", value: "priority" },
      { id: "enabled", value: false },
    ],
  };
  expect(chooseColorEffort(fast, "low")).toEqual({ ...fast, effort: "low" });
  expect(chooseColorOption(fast, "serviceTier", undefined).options).toEqual([
    { id: "enabled", value: false },
  ]);
  const available = [
    {
      ...providers[0]!,
      models: [
        { slug: "luna", name: "Luna", efforts: [{ id: "high", label: "High" }], options: [speed] },
      ],
    },
  ];
  expect(chooseColorModel(available, fast, "luna")).toEqual({
    ...fast,
    model: "luna",
    options: [{ id: "serviceTier", value: "priority" }],
  });
  expect(
    chooseColorModel(
      available,
      { ...fast, options: [{ id: "serviceTier", value: "turbo" }] },
      "luna",
    ).options,
  ).toBeUndefined();
  expect(chooseColorProvider(providers, fast, claude).options).toBeUndefined();
  expect(colorKey(fast)).not.toBe(
    colorKey({ ...fast, options: [{ id: "serviceTier", value: "default" }] }),
  );
  expect(colorKey(fast)).toBe(colorKey({ ...fast, options: fast.options.toReversed() }));
});
