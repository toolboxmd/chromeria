import {
  DEFAULT_PRISM_LANE,
  OrchestratorMcpFailure,
  prismRoleModels,
  type ModelSelection,
  type OrchestratorMcpTaskRole,
  type PrismLane,
  type PrismRole,
  type ProjectId,
  type ServerProvider,
} from "@t3tools/contracts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as ServerSettings from "../serverSettings.ts";
import { ProviderAdapterRegistryV2 } from "../orchestration-v2/ProviderAdapterRegistry.ts";

/** Native launch callers validate adapter admission before creating a thread/run. */
export const validateLaunchSelection = Effect.fn("Prism.validateLaunchSelection")(function* (
  selection: ModelSelection,
) {
  const adapters = yield* ProviderAdapterRegistryV2;
  if (!(yield* adapters.list()).includes(selection.instanceId))
    return yield* new OrchestratorMcpFailure({
      code: "provider_unavailable",
      message: `No V2 provider adapter is registered for ${selection.instanceId}.`,
    });
  return selection;
});

export function delegatedPrismRole(role?: OrchestratorMcpTaskRole): PrismRole {
  switch (role) {
    case "review":
      return "reviewer";
    case "research":
    case "design":
      return "planner";
    default:
      return "worker";
  }
}

function blocked(provider: ServerProvider, now: number) {
  return (provider.usageLimits?.windows ?? []).some(
    (window) =>
      window.usedPercent >= 100 &&
      (window.resetsAt === undefined || Date.parse(window.resetsAt) > now),
  );
}

function effortOption(driver: string) {
  switch (driver) {
    case "codex":
    case "grok":
      return "reasoningEffort";
    case "opencode":
      return "variant";
    default:
      return "effort";
  }
}

export class PrismService extends Context.Service<
  PrismService,
  {
    readonly resolve: (input: {
      readonly projectId: ProjectId;
      readonly role: PrismRole;
      readonly lane?: PrismLane | undefined;
      readonly explicit?: ModelSelection | undefined;
      readonly inherited?: ModelSelection | undefined;
      readonly validate?:
        | ((selection: ModelSelection) => Effect.Effect<ModelSelection, OrchestratorMcpFailure>)
        | undefined;
    }) => Effect.Effect<
      { readonly modelSelection: ModelSelection; readonly kitText: string },
      OrchestratorMcpFailure
    >;
  }
>()("t3/prism/PrismService") {}

const make = Effect.gen(function* () {
  const registry = yield* ProviderRegistry.ProviderRegistry;
  const settings = yield* ServerSettings.ServerSettingsService;
  const resolve: PrismService["Service"]["resolve"] = Effect.fn("PrismService.resolve")(
    function* (input) {
      const kits = resolveProjectSettings(
        yield* settings.getSettings.pipe(
          Effect.mapError(
            () =>
              new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "Unable to read Prism settings.",
              }),
          ),
        ),
        input.projectId,
      ).settings.prismRoles;
      const kit = kits[input.role];
      const kitText = [
        kit.instructions,
        kit.skills.length > 0 ? `Use these skills: ${kit.skills.join(", ")}.` : "",
      ]
        .filter(Boolean)
        .join("\n\n");
      if (input.explicit !== undefined) {
        const modelSelection = input.validate
          ? yield* input.validate(input.explicit)
          : input.explicit;
        return { modelSelection, kitText };
      }
      const providers = yield* registry.getProviders;
      const now = DateTime.toEpochMillis(yield* DateTime.now);
      const preferences = prismRoleModels(kits, input.role, input.lane ?? DEFAULT_PRISM_LANE);
      const candidates =
        preferences.length > 0 ? preferences : input.inherited ? [input.inherited] : [];
      for (const candidate of candidates) {
        const provider = providers.find((p) => p.instanceId === candidate.instanceId);
        if (!provider?.enabled || blocked(provider, now)) continue;
        // A validator can re-probe stale unavailable snapshots before refusing the candidate.
        const rechecking = provider.availability === "unavailable" && input.validate !== undefined;
        if (provider.availability === "unavailable" && !rechecking) continue;
        const model = provider.models.find(
          (m) => m.slug === candidate.model || m.aliases?.includes(candidate.model),
        );
        if (!model && !rechecking && (preferences.length > 0 || provider.models.length > 0))
          continue;
        const effort = "effort" in candidate ? candidate.effort : undefined;
        const selection: ModelSelection = {
          instanceId: candidate.instanceId,
          model: model?.slug ?? candidate.model,
          ...(effort === undefined
            ? "options" in candidate
              ? { options: candidate.options }
              : {}
            : { options: [{ id: effortOption(provider.driver), value: effort }] }),
        };
        const validated = yield* (
          input.validate ? input.validate(selection) : Effect.succeed(selection)
        ).pipe(
          Effect.catch((error) =>
            error.code === "provider_unavailable" || error.code === "model_unavailable"
              ? Effect.succeed(null)
              : Effect.fail(error),
          ),
        );
        if (validated !== null) return { modelSelection: validated, kitText };
      }
      return yield* new OrchestratorMcpFailure({
        code: "provider_unavailable",
        message: "No eligible model with usage headroom for this Prism role and lane.",
      });
    },
  );
  return PrismService.of({ resolve });
});
export const layer = Layer.effect(PrismService, make);
