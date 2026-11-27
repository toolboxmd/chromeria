import { assert, describe, it } from "@effect/vitest";
import {
  OrchestratorMcpFailure,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ServerProvider,
  type ProviderOptionSelection,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import * as Database from "../persistence/Sqlite.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import { buildUnavailableProviderSnapshot } from "../provider/unavailableProviderSnapshot.ts";
import * as Settings from "../serverSettings.ts";
import * as Prism from "./PrismService.ts";
import { CodexCreditReads, READ_BOUND, registerCodexCreditRead } from "./CodexCredits.ts";
import type { CodexAccountLimits } from "../provider/CodexProvider.ts";
import { ProviderDriverError } from "../provider/Errors.ts";
import * as Adapters from "../orchestration-v2/ProviderAdapterRegistry.ts";

const instanceId = ProviderInstanceId.make("codex");
const projectId = ProjectId.make("project:prism");
const selection = { instanceId, model: "configured" };
const registry = (providers: readonly ServerProvider[]) =>
  Layer.mergeAll(
    Database.layerMemory,
    Layer.mock(ProviderRegistry.ProviderRegistry)({ getProviders: Effect.succeed(providers) }),
  );
const provider = Effect.map(
  buildUnavailableProviderSnapshot({
    driverKind: "codex",
    instanceId,
    reason: "test",
    checkedAt: "2026-10-08T00:00:00Z",
  }),
  (p) => ({
    ...p,
    enabled: true,
    availability: "available" as const,
    models: [{ slug: "configured", name: "Configured", isCustom: false, capabilities: null }],
  }),
);

describe("PrismService", () => {
  it.effect("routes default computer use to Haiku and rejects it when disabled", () =>
    Effect.gen(function* () {
      const claudeId = ProviderInstanceId.make("claudeAgent");
      const claude = yield* buildUnavailableProviderSnapshot({
        driverKind: "claudeAgent",
        instanceId: claudeId,
        reason: "test",
        checkedAt: "2026-10-20T00:00:00Z",
      });
      for (const enabled of [true, false]) {
        yield* Effect.gen(function* () {
          const service = yield* Prism.PrismService;
          const resolve = service.resolve({
            projectId,
            role: "computer-use",
            inherited: selection,
          });
          if (!enabled) {
            const error = yield* Effect.flip(resolve);
            assert.strictEqual(error.code, "provider_unavailable");
            return;
          }
          const result = yield* resolve;
          assert.deepStrictEqual(result.modelSelection, {
            instanceId: claudeId,
            model: "claude-haiku-5-5",
          });
        }).pipe(
          Effect.provide(
            Prism.layer.pipe(
              Layer.provide(
                Layer.mergeAll(
                  registry([
                    {
                      ...claude,
                      enabled,
                      availability: "available",
                      models: [
                        {
                          slug: "claude-haiku-5-5",
                          name: "Haiku",
                          isCustom: false,
                          capabilities: null,
                        },
                      ],
                    },
                  ]),
                  Settings.layerTest({}),
                ),
              ),
            ),
          ),
        );
      }
    }),
  );
  it.effect("bare selection keeps routing and removes both role instructions and skill hints", () =>
    Effect.gen(function* () {
      const service = yield* Prism.PrismService;
      const intent = { projectId, role: "worker" as const, explicit: selection };
      const normal = yield* service.resolve(intent);
      const bare = yield* service.resolve({ ...intent, bare: true });
      assert.strictEqual(normal.kitText, "Follow the kit.\n\nUse these skills: unasked-skill.");
      assert.strictEqual(bare.kitText, "");
      assert.deepStrictEqual(bare.modelSelection, normal.modelSelection);
    }).pipe(
      Effect.provide(
        Prism.layer.pipe(
          Layer.provide(
            Layer.mergeAll(
              registry([]),
              Settings.layerTest({
                prismRoles: {
                  worker: { instructions: "Follow the kit.", skills: ["unasked-skill"] },
                },
              }),
            ),
          ),
        ),
      ),
    ),
  );
  it.effect("explicit model wins even when configured capacity is exhausted, and kit remains", () =>
    Effect.gen(function* () {
      const service = yield* Prism.PrismService;
      const result = yield* service.resolve({
        projectId,
        role: "worker",
        explicit: { instanceId, model: "chosen" },
      });
      assert.strictEqual(result.modelSelection.model, "chosen");
      assert.strictEqual(result.kitText, "Follow the kit.");
    }).pipe(
      Effect.provide(
        Prism.layer.pipe(
          Layer.provide(
            Layer.mergeAll(
              registry([]),
              Settings.layerTest({
                prismRoles: {
                  worker: { instructions: "Follow the kit.", lanes: { medium: [selection] } },
                },
              }),
            ),
          ),
        ),
      ),
    ),
  );
  it.effect("walks role preferences after provider/model unavailable and preserves effort", () =>
    Effect.gen(function* () {
      const p = yield* provider;
      const seen: string[] = [];
      const result = yield* Effect.gen(function* () {
        const service = yield* Prism.PrismService;
        return yield* service.resolve({
          projectId,
          role: "worker",
          lane: "hard",
          validate: (pick) => {
            seen.push(pick.model);
            return pick.model === "first"
              ? Effect.fail(
                  new OrchestratorMcpFailure({ code: "model_unavailable", message: "gone" }),
                )
              : Effect.succeed(pick);
          },
        });
      }).pipe(
        Effect.provide(
          Prism.layer.pipe(
            Layer.provide(
              Layer.mergeAll(
                registry([
                  {
                    ...p,
                    models: [
                      ...p.models,
                      { slug: "first", name: "First", isCustom: false, capabilities: null },
                    ],
                  },
                ]),
                Settings.layerTest({
                  prismRoles: {
                    worker: {
                      lanes: {
                        hard: [
                          { instanceId, model: "first" },
                          { ...selection, effort: "high" },
                        ],
                      },
                    },
                  },
                }),
              ),
            ),
          ),
        ),
      );
      assert.deepStrictEqual(seen, ["first", "configured"]);
      assert.deepStrictEqual(result.modelSelection.options, [
        { id: "reasoningEffort", value: "high" },
      ]);
    }),
  );
  it.effect("sends a Muse Color's effort as reasoningEffort", () =>
    Effect.gen(function* () {
      const museId = ProviderInstanceId.make("muse-personal");
      const muse = yield* buildUnavailableProviderSnapshot({
        driverKind: "muse",
        instanceId: museId,
        reason: "test",
        checkedAt: "2026-10-20T00:00:00Z",
      });
      const result = yield* Effect.gen(function* () {
        const service = yield* Prism.PrismService;
        return yield* service.resolve({ projectId, role: "worker", lane: "easy" });
      }).pipe(
        Effect.provide(
          Prism.layer.pipe(
            Layer.provide(
              Layer.mergeAll(
                registry([
                  {
                    ...muse,
                    enabled: true,
                    availability: "available",
                    models: [
                      {
                        slug: "muse-spark-1.3",
                        name: "Spark",
                        isCustom: false,
                        capabilities: null,
                      },
                    ],
                  },
                ]),
                Settings.layerTest({
                  prismRoles: {
                    worker: {
                      lanes: {
                        easy: [{ instanceId: museId, model: "muse-spark-1.3", effort: "high" }],
                      },
                    },
                  },
                }),
              ),
            ),
          ),
        ),
      );
      assert.deepStrictEqual(result.modelSelection, {
        instanceId: museId,
        model: "muse-spark-1.3",
        options: [{ id: "reasoningEffort", value: "high" }],
      });
    }),
  );
  it.effect("skips exhausted capacity and honors project role overrides", () =>
    Effect.gen(function* () {
      const p = yield* provider;
      const peerId = ProviderInstanceId.make("codex-work");
      const result = yield* Effect.gen(function* () {
        const service = yield* Prism.PrismService;
        return yield* service.resolve({ projectId, role: "reviewer" });
      }).pipe(
        Effect.provide(
          Prism.layer.pipe(
            Layer.provide(
              Layer.mergeAll(
                registry([
                  {
                    ...p,
                    usageLimits: {
                      checkedAt: "2026-10-08T00:00:00Z",
                      windows: [
                        {
                          id: "seven_day_configured",
                          kind: "weekly",
                          label: "Weekly · Configured",
                          usedPercent: 100,
                          modelIds: ["configured"],
                        },
                      ],
                    },
                  },
                  {
                    ...p,
                    instanceId: peerId,
                    // A spent allowance of another model leaves this one free.
                    usageLimits: {
                      checkedAt: "2026-10-08T00:00:00Z",
                      windows: [
                        {
                          id: "seven_day_other",
                          kind: "weekly",
                          label: "Weekly · Other",
                          usedPercent: 100,
                          modelIds: ["other-model"],
                        },
                      ],
                    },
                  },
                ]),
                Settings.layerTest({
                  prismRoles: {
                    reviewer: { instructions: "Environment kit", models: [selection] },
                  },
                  projectSettingsOverrides: {
                    [projectId]: {
                      prismRoles: {
                        reviewer: {
                          instructions: "Project kit",
                          models: [selection, { instanceId: peerId, model: "configured" }],
                        },
                      },
                    },
                  },
                }),
              ),
            ),
          ),
        ),
      );
      assert.strictEqual(result.modelSelection.instanceId, peerId);
      assert.strictEqual(result.kitText, "Project kit");
    }),
  );
  it.effect("names each skipped Color and why when none can start", () =>
    Effect.gen(function* () {
      const p = yield* provider;
      const limitedId = ProviderInstanceId.make("codex-work");
      const offId = ProviderInstanceId.make("codex-off");
      const error = yield* Effect.gen(function* () {
        const service = yield* Prism.PrismService;
        return yield* Effect.flip(service.resolve({ projectId, role: "reviewer" }));
      }).pipe(
        Effect.provide(
          Prism.layer.pipe(
            Layer.provide(
              Layer.mergeAll(
                registry([
                  p,
                  {
                    ...p,
                    instanceId: limitedId,
                    usageLimits: {
                      checkedAt: "2026-10-08T00:00:00Z",
                      windows: [
                        { id: "weekly", kind: "weekly", label: "Weekly", usedPercent: 100 },
                      ],
                    },
                  },
                  { ...p, instanceId: offId, availability: "unavailable" },
                ]),
                Settings.layerTest({
                  prismRoles: {
                    reviewer: {
                      models: [
                        { instanceId, model: "missing-model" },
                        { instanceId: limitedId, model: "configured" },
                        { instanceId: offId, model: "configured" },
                      ],
                    },
                  },
                }),
              ),
            ),
          ),
        ),
      );
      assert.strictEqual(error.code, "provider_unavailable");
      assert.strictEqual(
        error.message,
        "No eligible model with usage headroom for this Prism role and lane. Skipped: " +
          "codex/missing-model: model is not advertised by the provider; " +
          "codex-work/configured: Weekly usage window is exhausted with no reported reset; " +
          "codex-off/configured: provider is unavailable.",
      );
    }),
  );
  it.effect(
    "a project override of one role still routes other roles through the environment kit",
    () =>
      Effect.gen(function* () {
        const p = yield* provider;
        const caller = { instanceId: ProviderInstanceId.make("claudeAgent"), model: "caller" };
        const result = yield* Effect.gen(function* () {
          const service = yield* Prism.PrismService;
          return yield* service.resolve({ projectId, role: "reviewer", inherited: caller });
        }).pipe(
          Effect.provide(
            Prism.layer.pipe(
              Layer.provide(
                Layer.mergeAll(
                  registry([
                    p,
                    {
                      ...p,
                      instanceId: caller.instanceId,
                      models: [
                        { slug: "caller", name: "Caller", isCustom: false, capabilities: null },
                      ],
                    },
                  ]),
                  Settings.layerTest({
                    prismRoles: { reviewer: { models: [selection] } },
                    projectSettingsOverrides: {
                      [projectId]: { prismRoles: { worker: { lanes: { hard: [selection] } } } },
                    },
                  }),
                ),
              ),
            ),
          ),
        );
        assert.deepStrictEqual(result.modelSelection, selection);
      }),
  );
  it.effect("does not hide invalid options behind capacity fallback", () =>
    Effect.gen(function* () {
      const p = yield* provider;
      const error = yield* Effect.gen(function* () {
        const service = yield* Prism.PrismService;
        return yield* service.resolve({
          projectId,
          role: "worker",
          validate: () =>
            Effect.fail(
              new OrchestratorMcpFailure({ code: "invalid_request", message: "Invalid option" }),
            ),
        });
      }).pipe(
        Effect.flip,
        Effect.provide(
          Prism.layer.pipe(
            Layer.provide(
              Layer.mergeAll(
                registry([p]),
                Settings.layerTest({
                  prismRoles: { worker: { lanes: { medium: [selection, selection] } } },
                }),
              ),
            ),
          ),
        ),
      );
      assert.strictEqual(error.code, "invalid_request");
    }),
  );
  it.effect("routes each new role through its own kit and Color list", () =>
    Effect.gen(function* () {
      const p = yield* provider;
      const models = [
        ...p.models,
        ...["research", "design", "roast", "judge", "computer"].map((slug) => ({
          slug,
          name: slug,
          isCustom: false,
          capabilities: null,
        })),
      ];
      yield* Effect.gen(function* () {
        const service = yield* Prism.PrismService;
        for (const [role, model] of [
          ["researcher", "research"],
          ["designer", "design"],
          ["roast", "roast"],
          ["judge", "judge"],
          ["computer-use", "computer"],
        ] as const) {
          const result = yield* service.resolve({ projectId, role });
          assert.strictEqual(result.modelSelection.model, model);
          assert.strictEqual(result.kitText, `${role} kit.`);
        }
      }).pipe(
        Effect.provide(
          Prism.layer.pipe(
            Layer.provide(
              Layer.mergeAll(
                registry([{ ...p, models }]),
                Settings.layerTest({
                  prismRoles: {
                    "computer-use": {
                      models: [{ instanceId, model: "computer" }],
                      instructions: "computer-use kit.",
                    },
                    researcher: {
                      models: [{ instanceId, model: "research" }],
                      instructions: "researcher kit.",
                    },
                    designer: {
                      models: [{ instanceId, model: "design" }],
                      instructions: "designer kit.",
                    },
                    roast: { models: [{ instanceId, model: "roast" }], instructions: "roast kit." },
                    judge: { models: [{ instanceId, model: "judge" }], instructions: "judge kit." },
                  },
                }),
              ),
            ),
          ),
        ),
      );
    }),
  );
  describe("ui lane", () => {
    const claudeId = ProviderInstanceId.make("claudeAgent");
    const opus = { instanceId: claudeId, model: "claude-opus-5-5" };
    const withProviders = (claudeEnabled: boolean, prismRoles: object) =>
      Effect.provide(
        Prism.layer.pipe(
          Layer.provide(
            Layer.mergeAll(
              Layer.unwrap(
                Effect.map(provider, (p) =>
                  registry([
                    p,
                    {
                      ...p,
                      instanceId: claudeId,
                      driver: ProviderDriverKind.make("claudeAgent"),
                      enabled: claudeEnabled,
                      models: [
                        {
                          slug: opus.model,
                          name: "Opus",
                          aliases: ["opus"],
                          isCustom: false,
                          capabilities: null,
                        },
                      ],
                    },
                  ]),
                ),
              ),
              Settings.layerTest({ prismRoles: prismRoles as never }),
            ),
          ),
        ),
      );
    const resolveUi = Effect.gen(function* () {
      const service = yield* Prism.PrismService;
      return yield* service.resolve({
        projectId,
        role: "worker",
        lane: "ui",
        inherited: selection,
      });
    });

    it.effect("defaults to Claude Opus 5.5 at medium effort", () =>
      Effect.gen(function* () {
        const result = yield* resolveUi;
        assert.deepStrictEqual(result.modelSelection, {
          ...opus,
          options: [{ id: "effort", value: "medium" }],
        });
      }).pipe(withProviders(true, { worker: { lanes: { medium: [selection] } } })),
    );
    it.effect("never falls back to another lane or the caller's model", () =>
      Effect.gen(function* () {
        const error = yield* Effect.flip(resolveUi);
        assert.strictEqual(error.code, "provider_unavailable");
      }).pipe(withProviders(false, { worker: { lanes: { medium: [selection], ui: [opus] } } })),
    );
    it.effect("fails visibly when the ui lane has no Colors", () =>
      Effect.gen(function* () {
        const error = yield* Effect.flip(resolveUi);
        assert.strictEqual(error.code, "provider_unavailable");
        assert.include(error.message, "ui lane");
      }).pipe(withProviders(true, { worker: { lanes: { medium: [selection], ui: [] } } })),
    );
    it.effect("accepts an explicit model only when it is one of the ui lane's Colors", () =>
      Effect.gen(function* () {
        const service = yield* Prism.PrismService;
        const intent = { projectId, role: "worker" as const, lane: "ui" as const };
        // Callers pass an inherited model as explicit, such as an empty delegate_task target.
        const error = yield* Effect.flip(service.resolve({ ...intent, explicit: selection }));
        assert.strictEqual(error.code, "invalid_request");
        const chosen = yield* service.resolve({
          ...intent,
          explicit: { ...opus, options: [{ id: "effort", value: "high" }] },
        });
        assert.strictEqual(chosen.modelSelection.model, opus.model);
      }).pipe(withProviders(true, { worker: { lanes: { ui: [opus] } } })),
    );
    it.effect("matches an explicit model to a ui Color that names it by alias", () =>
      Effect.gen(function* () {
        const service = yield* Prism.PrismService;
        const chosen = yield* service.resolve({
          projectId,
          role: "worker",
          lane: "ui",
          explicit: opus,
        });
        assert.strictEqual(chosen.modelSelection.model, opus.model);
      }).pipe(withProviders(true, { worker: { lanes: { ui: [{ ...opus, model: "opus" }] } } })),
    );
    it.effect("is refused for roles other than worker", () =>
      Effect.gen(function* () {
        const service = yield* Prism.PrismService;
        const error = yield* Effect.flip(
          service.resolve({ projectId, role: "reviewer", lane: "ui", inherited: selection }),
        );
        assert.strictEqual(error.code, "invalid_request");
      }).pipe(withProviders(true, { reviewer: { models: [selection] } })),
    );
  });
  it("maps every upstream role to the fixed kit", () => {
    assert.strictEqual(Prism.delegatedPrismRole("implementation"), "worker");
    assert.strictEqual(Prism.delegatedPrismRole("test"), "worker");
    assert.strictEqual(Prism.delegatedPrismRole("general"), "worker");
    assert.strictEqual(Prism.delegatedPrismRole("review"), "reviewer");
    assert.strictEqual(Prism.delegatedPrismRole("research"), "researcher");
    assert.strictEqual(Prism.delegatedPrismRole("design"), "designer");
    assert.strictEqual(Prism.delegatedPrismRole(), "worker");
  });
});

const optionDescriptors = [
  {
    id: "reasoningEffort",
    label: "Reasoning",
    type: "select" as const,
    options: [{ id: "high", label: "High" }],
  },
  {
    id: "serviceTier",
    label: "Speed",
    type: "select" as const,
    options: [
      { id: "priority", label: "Fast" },
      { id: "default", label: "Standard" },
    ],
  },
  { id: "enabled", label: "Enabled", type: "boolean" as const },
];
const advertised = Effect.map(provider, (p) => ({
  ...p,
  installed: true,
  status: "ready" as const,
  models: p.models.map((m) => ({ ...m, aliases: ["alias"], capabilities: { optionDescriptors } })),
}));
const fastOptions = [{ id: "serviceTier", value: "priority" }];

it.effect(
  "preserves tier-only distinct Colors, effort and inherited options without a validation callback",
  () =>
    Effect.gen(function* () {
      const p = yield* advertised;
      yield* Effect.gen(function* () {
        const service = yield* Prism.PrismService;
        const result = yield* service.resolve({ projectId, role: "reviewer", count: 2 });
        assert.deepStrictEqual(result.modelSelections, [
          {
            ...selection,
            options: [
              { id: "serviceTier", value: "default" },
              { id: "reasoningEffort", value: "high" },
            ],
          },
          { ...selection, options: [...fastOptions, { id: "reasoningEffort", value: "high" }] },
        ]);
        const inherited = {
          ...selection,
          options: [...fastOptions, { id: "enabled", value: false }],
        };
        assert.deepStrictEqual(
          (yield* service.resolve({ projectId, role: "planner", inherited })).modelSelection,
          inherited,
        );
        assert.deepStrictEqual(
          (yield* service.resolve({ projectId, role: "promachos" })).modelSelection.options,
          [...fastOptions, { id: "reasoningEffort", value: "high" }],
        );
      }).pipe(
        Effect.provide(
          Prism.layer.pipe(
            Layer.provide(
              Layer.mergeAll(
                registry([p]),
                Settings.layerTest({
                  prismRoles: {
                    reviewer: {
                      models: [
                        {
                          ...selection,
                          effort: "high",
                          options: [{ id: "serviceTier", value: "default" }],
                        },
                        { ...selection, model: "alias", effort: "high", options: fastOptions },
                      ],
                    },
                    promachos: { models: [{ ...selection, effort: "high", options: fastOptions }] },
                  },
                }),
              ),
            ),
          ),
        ),
      );
    }),
);

it.effect(
  "rejects unknown ids, values, types and duplicate Color options without falling back or needing a callback",
  () =>
    Effect.gen(function* () {
      const p = yield* advertised;
      const cases: readonly (readonly ProviderOptionSelection[])[] = [
        [{ id: "unknown", value: "priority" }],
        [{ id: "serviceTier", value: "turbo" }],
        [{ id: "serviceTier", value: true }],
        [{ id: "enabled", value: "true" }],
        [{ id: "serviceTier", value: "default" }, ...fastOptions],
        [{ id: "reasoningEffort", value: "invalid" }],
      ];
      for (const options of cases) {
        const error = yield* Effect.gen(function* () {
          const service = yield* Prism.PrismService;
          return yield* service.resolve({ projectId, role: "reviewer" });
        }).pipe(
          Effect.flip,
          Effect.provide(
            Prism.layer.pipe(
              Layer.provide(
                Layer.mergeAll(
                  registry([p]),
                  Settings.layerTest({
                    prismRoles: {
                      reviewer: { models: [{ ...selection, effort: "high", options }, selection] },
                    },
                  }),
                ),
              ),
            ),
          ),
        );
        assert.strictEqual(error.code, "invalid_request");
      }
      const error = yield* Effect.gen(function* () {
        return yield* (yield* Prism.PrismService).resolve({ projectId, role: "reviewer" });
      }).pipe(
        Effect.flip,
        Effect.provide(
          Prism.layer.pipe(
            Layer.provide(
              Layer.mergeAll(
                registry([yield* provider]),
                Settings.layerTest({
                  prismRoles: { reviewer: { models: [{ ...selection, options: fastOptions }] } },
                }),
              ),
            ),
          ),
        ),
      );
      assert.strictEqual(error.code, "invalid_request");
    }),
);

it.effect(
  "native launch validation canonicalizes the model and applies explicit-target option validation",
  () =>
    Effect.gen(function* () {
      const p = yield* advertised;
      yield* Effect.gen(function* () {
        const valid = yield* Prism.validateLaunchSelection({
          ...selection,
          model: "alias",
          options: fastOptions,
        });
        assert.deepStrictEqual(valid, { ...selection, options: fastOptions });
        const error = yield* Effect.flip(
          Prism.validateLaunchSelection({
            ...selection,
            options: [{ id: "serviceTier", value: "turbo" }],
          }),
        );
        assert.strictEqual(error.code, "invalid_request");
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            registry([p]),
            Layer.mock(Adapters.ProviderAdapterRegistryV2)({
              list: () => Effect.succeed([instanceId]),
            }),
          ),
        ),
      );
    }),
);

describe("Codex on credits", () => {
  const weekly = {
    id: "secondary",
    kind: "weekly" as const,
    label: "Weekly",
    usedPercent: 100,
    resetsAt: "2026-12-01T00:00:00Z",
  };
  const plan = { secondary: { usedPercent: 100, resetsAt: 1_796_083_200 } };
  const spendable = { hasCredits: true, unlimited: false, balance: "60011" };
  const native = (rateLimits: object): CodexAccountLimits => ({
    route: "native",
    rateLimits: { ...plan, ...rateLimits },
  });
  const failedRead = Effect.fail(
    new ProviderDriverError({
      driver: ProviderDriverKind.make("codex"),
      instanceId,
      detail: "Codex could not read its account limits.",
    }),
  );
  type Resolve = Effect.Effect<string>;
  const resolveOnCredits = Effect.fn("test.resolveOnCredits")(function* (input: {
    /** The read for each call, numbered from 1. */
    readonly read: (call: number) => Effect.Effect<CodexAccountLimits, ProviderDriverError>;
    readonly patch?: (p: ServerProvider) => ServerProvider;
    readonly useCredits?: boolean;
    readonly colors?: ReadonlyArray<string>;
    readonly drive?: (
      resolve: Resolve,
      started: Deferred.Deferred<void>,
    ) => Effect.Effect<Array<string>>;
  }) {
    const p = yield* provider;
    let reads = 0;
    const started = yield* Deferred.make<void>();
    const outcomes = yield* Effect.gen(function* () {
      yield* registerCodexCreditRead(
        instanceId,
        Effect.suspend(() => {
          const call = ++reads;
          return Deferred.succeed(started, undefined).pipe(Effect.andThen(input.read(call)));
        }),
      );
      const service = yield* Prism.PrismService;
      const resolve = service.resolve({ projectId, role: "reviewer" }).pipe(
        Effect.map((result) => `admitted ${result.modelSelection.instanceId}`),
        Effect.catch((error) => Effect.succeed(error.message)),
      );
      return yield* (input.drive ?? ((once) => Effect.map(once, (outcome) => [outcome])))(
        resolve,
        started,
      );
    }).pipe(
      Effect.scoped,
      Effect.provideService(CodexCreditReads, new Map()),
      Effect.provide(
        Prism.layer.pipe(
          Layer.provide(
            Layer.mergeAll(
              registry([
                (input.patch ?? ((value) => value))({
                  ...p,
                  usageLimits: { checkedAt: "2026-11-26T00:00:00Z", windows: [weekly] },
                }),
              ]),
              Settings.layerTest({
                prismRoles: {
                  reviewer: {
                    models: (input.colors ?? ["configured"]).map((model) => ({
                      instanceId,
                      model,
                    })),
                  },
                },
                ...(input.useCredits === undefined
                  ? {}
                  : { prismUseCredits: { [instanceId]: input.useCredits } }),
              }),
            ),
          ),
        ),
      ),
    );
    return { outcomes, reads };
  });
  // The decision must return when the bound passes, with no further clock movement.
  const atTheBound = (resolve: Resolve, started: Deferred.Deferred<void>) =>
    Effect.gen(function* () {
      const fiber = yield* Effect.forkChild(resolve);
      yield* Deferred.await(started);
      yield* TestClock.adjust(READ_BOUND);
      return [yield* Fiber.join(fiber)];
    });
  const skippedFor = (reason: string, colors: ReadonlyArray<string> = ["configured"]) =>
    "No eligible model with usage headroom for this Prism role and lane. Skipped: " +
    colors
      .map(
        (color) =>
          `codex/${color}: Weekly usage window is exhausted until 2026-12-01T00:00:00Z` +
          (reason ? `; credits cannot cover it: ${reason}` : ""),
      )
      .join("; ") +
    ".";

  it.effect.each([
    {
      name: "spendable credits cover a spent weekly window",
      read: Effect.succeed(native({ credits: spendable })),
      expected: "admitted codex",
      reads: 1,
    },
    {
      name: "unlimited credits cover it",
      read: Effect.succeed(native({ credits: { hasCredits: false, unlimited: true } })),
      expected: "admitted codex",
      reads: 1,
    },
    {
      name: "the switch off skips without a read",
      read: Effect.succeed(native({ credits: spendable })),
      useCredits: false,
      expected: skippedFor("Use credits is off for this instance"),
      reads: 0,
    },
    {
      name: "the switch on explicitly admits",
      read: Effect.succeed(native({ credits: spendable })),
      useCredits: true,
      expected: "admitted codex",
      reads: 1,
    },
    {
      name: "no credits reported",
      read: Effect.succeed(native({})),
      expected: skippedFor("Codex reported no credits"),
      reads: 1,
    },
    {
      name: "hasCredits false",
      read: Effect.succeed(native({ credits: { hasCredits: false, unlimited: false } })),
      expected: skippedFor("the account has no credits"),
      reads: 1,
    },
    {
      name: "spend control reached",
      read: Effect.succeed(native({ credits: spendable, spendControlReached: true })),
      expected: skippedFor("spend control is reached"),
      reads: 1,
    },
    {
      name: "workspace credits depleted",
      read: Effect.succeed(
        native({
          credits: spendable,
          rateLimitReachedType: "workspace_member_credits_depleted",
        }),
      ),
      expected: skippedFor("the workspace reports workspace_member_credits_depleted"),
      reads: 1,
    },
    {
      name: "the read's monthly credit cap is spent",
      read: Effect.succeed(
        native({
          credits: spendable,
          individualLimit: {
            limit: "100",
            used: "100",
            remainingPercent: 0,
            resetsAt: 1_796_083_200,
          },
        }),
      ),
      expected: skippedFor("the monthly credit cap is spent until 2026-12-01T00:00:00.000Z"),
      reads: 1,
    },
    {
      name: "the snapshot's monthly credit cap is spent, without a read",
      read: Effect.succeed(native({ credits: spendable })),
      patch: (p: ServerProvider) => ({
        ...p,
        usageLimits: {
          checkedAt: "2026-11-26T00:00:00Z",
          windows: [
            weekly,
            {
              ...weekly,
              id: "individual",
              kind: "monthly" as const,
              label: "Monthly credit limit",
            },
          ],
        },
      }),
      expected: skippedFor("the monthly credit cap is spent until 2026-12-01T00:00:00Z"),
      reads: 0,
    },
    {
      name: "a proxy route in the snapshot skips without a read",
      read: Effect.succeed(native({ credits: spendable })),
      patch: (p: ServerProvider) => ({
        ...p,
        usageLimits: {
          checkedAt: "2026-11-26T00:00:00Z",
          windows: [weekly],
          unavailable: { reason: "unsupported" as const },
        },
      }),
      expected: skippedFor("its route does not spend the Codex account's own quota"),
      reads: 0,
    },
    {
      name: "a route the read finds is not the account's own",
      read: Effect.succeed({ route: "other" } satisfies CodexAccountLimits),
      expected: skippedFor("its route does not spend the Codex account's own quota"),
      reads: 1,
    },
    {
      name: "a failed read",
      read: failedRead,
      expected: skippedFor("the credit read failed"),
      reads: 1,
    },
    {
      name: "a read that does not answer within the bound",
      read: Effect.never,
      hangs: true,
      expected: skippedFor("the credit read timed out after 10s"),
      reads: 1,
    },
    {
      name: "a read that cannot be interrupted, such as a stuck spawn",
      read: Effect.uninterruptible(Effect.never),
      hangs: true,
      expected: skippedFor("the credit read timed out after 10s"),
      reads: 1,
    },
    {
      name: "a Codex instance with headroom makes no read",
      read: Effect.succeed(native({ credits: spendable })),
      patch: (p: ServerProvider) => ({ ...p, usageLimits: { ...p.usageLimits!, windows: [] } }),
      expected: "admitted codex",
      reads: 0,
    },
    {
      name: "another driver keeps main's skip and makes no read",
      read: Effect.succeed(native({ credits: spendable })),
      patch: (p: ServerProvider) => ({ ...p, driver: ProviderDriverKind.make("claudeAgent") }),
      expected: skippedFor(""),
      reads: 0,
    },
  ])("$name", ({ read, patch, useCredits, hangs, expected, reads }) =>
    Effect.gen(function* () {
      const result = yield* resolveOnCredits({
        read: () => read,
        ...(patch ? { patch } : {}),
        ...(useCredits === undefined ? {} : { useCredits }),
        ...(hangs ? { drive: atTheBound } : {}),
      });
      assert.deepStrictEqual(result, { outcomes: [expected], reads });
    }),
  );

  it.effect("decides from its own read after the instance's login changes", () =>
    Effect.gen(function* () {
      const result = yield* resolveOnCredits({
        read: (call) =>
          Effect.succeed(
            call === 1
              ? native({ credits: spendable })
              : native({ credits: { hasCredits: false, unlimited: false } }),
          ),
        drive: (resolve) => Effect.all([resolve, resolve]),
      });
      assert.deepStrictEqual(result, {
        outcomes: ["admitted codex", skippedFor("the account has no credits")],
        reads: 2,
      });
    }),
  );

  it.effect("an older positive read finishing late never decides a newer call", () =>
    Effect.gen(function* () {
      const denial = native({ credits: { hasCredits: false, unlimited: false } });
      const bothStarted = yield* Deferred.make<void>();
      const result = yield* resolveOnCredits({
        read: (call) =>
          call === 1
            ? Effect.sleep("2 seconds").pipe(Effect.as(native({ credits: spendable })))
            : Deferred.succeed(bothStarted, undefined).pipe(Effect.as(denial)),
        drive: (resolve) =>
          Effect.gen(function* () {
            const pair = yield* Effect.forkChild(
              Effect.all([resolve, resolve], { concurrency: "unbounded" }),
            );
            yield* Deferred.await(bothStarted);
            yield* TestClock.adjust("2 seconds");
            return [...(yield* Fiber.join(pair)).toSorted(), yield* resolve];
          }),
      });
      assert.deepStrictEqual(result, {
        // Sorted: the concurrent pair, then the later call.
        outcomes: [
          skippedFor("the account has no credits"),
          "admitted codex",
          skippedFor("the account has no credits"),
        ],
        reads: 3,
      });
    }),
  );

  const withSecondModel = (p: ServerProvider) => ({
    ...p,
    models: [...p.models, { ...p.models[0]!, slug: "second", name: "Second" }],
  });

  it.effect("two Colors on one instance share one read that never answers", () =>
    Effect.gen(function* () {
      const result = yield* resolveOnCredits({
        read: () => Effect.never,
        patch: withSecondModel,
        colors: ["configured", "second"],
        drive: atTheBound,
      });
      assert.deepStrictEqual(result, {
        outcomes: [skippedFor("the credit read timed out after 10s", ["configured", "second"])],
        reads: 1,
      });
    }),
  );

  it.effect("two Colors on one instance share one read that finds no credits", () =>
    Effect.gen(function* () {
      const result = yield* resolveOnCredits({
        read: () => Effect.succeed(native({ credits: { hasCredits: false, unlimited: false } })),
        patch: withSecondModel,
        colors: ["configured", "second"],
      });
      assert.deepStrictEqual(result, {
        outcomes: [skippedFor("the account has no credits", ["configured", "second"])],
        reads: 1,
      });
    }),
  );

  it.effect.each([
    {
      name: "an unavailable instance",
      patch: (p: ServerProvider) => ({ ...p, availability: "unavailable" as const }),
      colors: ["configured"],
    },
    { name: "a Color whose model is not advertised", colors: ["missing"] },
  ])("$name makes no read and keeps the window reason", ({ patch, colors }) =>
    Effect.gen(function* () {
      const result = yield* resolveOnCredits({
        read: () => Effect.succeed(native({ credits: spendable })),
        ...(patch ? { patch } : {}),
        colors,
      });
      assert.deepStrictEqual(result, { outcomes: [skippedFor("", colors)], reads: 0 });
    }),
  );
});
