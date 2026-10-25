import { assert, describe, it } from "@effect/vitest";
import {
  OrchestratorMcpFailure,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ServerProvider,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import { buildUnavailableProviderSnapshot } from "../provider/unavailableProviderSnapshot.ts";
import * as Settings from "../serverSettings.ts";
import * as Prism from "./PrismService.ts";

const instanceId = ProviderInstanceId.make("codex");
const projectId = ProjectId.make("project:prism");
const selection = { instanceId, model: "configured" };
const registry = (providers: readonly ServerProvider[]) =>
  Layer.mock(ProviderRegistry.ProviderRegistry)({ getProviders: Effect.succeed(providers) });
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
                        { id: "weekly", kind: "weekly", label: "Weekly", usedPercent: 100 },
                      ],
                    },
                  },
                  { ...p, instanceId: peerId },
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
