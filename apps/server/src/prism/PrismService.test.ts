import { assert, describe, it } from "@effect/vitest";
import {
  OrchestratorMcpFailure,
  ProjectId,
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
  it("maps every upstream role to the fixed kit", () => {
    assert.strictEqual(Prism.delegatedPrismRole("implementation"), "worker");
    assert.strictEqual(Prism.delegatedPrismRole("test"), "worker");
    assert.strictEqual(Prism.delegatedPrismRole("general"), "worker");
    assert.strictEqual(Prism.delegatedPrismRole("review"), "reviewer");
    assert.strictEqual(Prism.delegatedPrismRole("research"), "planner");
    assert.strictEqual(Prism.delegatedPrismRole("design"), "planner");
  });
});
