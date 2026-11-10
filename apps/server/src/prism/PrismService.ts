import { colorModelSelection, invalidOptionSelections } from "./modelOptions.ts";
import { unsupportedBare } from "@t3tools/provider-core/server/bareLaunch";
import {
  DEFAULT_PRISM_LANE,
  isPrismRunGroupRole,
  isProviderAvailable,
  OrchestratorMcpFailure,
  prismRoleModels,
  type ModelSelection,
  type OrchestratorMcpTaskRole,
  type PrismDelegateRole,
  type PrismLane,
  type PrismRole,
  type PrismRoleKits,
  type ProjectId,
  type ServerProvider,
} from "@t3tools/contracts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as ServerSettings from "../serverSettings.ts";
import { ProviderAdapterRegistryV2 } from "../orchestration-v2/ProviderAdapterRegistry.ts";

/** Native launch callers validate adapter admission before creating a thread/run. */
export const validateLaunchSelection = Effect.fn("Prism.validateLaunchSelection")(function* (
  selection: ModelSelection,
  bare?: boolean,
) {
  const adapters = yield* ProviderAdapterRegistryV2;
  if (!(yield* adapters.list()).includes(selection.instanceId))
    return yield* new OrchestratorMcpFailure({
      code: "provider_unavailable",
      message: `No V2 provider adapter is registered for ${selection.instanceId}.`,
    });
  if (bare) {
    const adapter = yield* adapters.get(selection.instanceId).pipe(
      Effect.mapError(
        () =>
          new OrchestratorMcpFailure({
            code: "provider_unavailable",
            message: "Bare adapter unavailable.",
          }),
      ),
    );
    const support =
      adapter.bareSupport === undefined
        ? unsupportedBare(adapter.driver)
        : yield* adapter.bareSupport;
    if (!support.supported)
      return yield* new OrchestratorMcpFailure({
        code: "provider_unavailable",
        message: support.reason,
      });
  }
  const registry = yield* ProviderRegistry.ProviderRegistry;
  const eligible = (provider: ServerProvider | undefined) =>
    provider !== undefined &&
    provider.enabled &&
    provider.installed &&
    isProviderAvailable(provider) &&
    provider.status !== "error" &&
    provider.status !== "disabled" &&
    provider.auth.status !== "unauthenticated";
  let provider = (yield* registry.getProviders).find(
    (candidate) => candidate.instanceId === selection.instanceId,
  );
  // An unattended caller can retain a stale unavailable snapshot after a provider is repaired.
  if (!eligible(provider)) {
    provider = (yield* registry.refreshInstance(selection.instanceId)).find(
      (candidate) => candidate.instanceId === selection.instanceId,
    );
  }
  if (!provider || !eligible(provider))
    return yield* new OrchestratorMcpFailure({
      code: "provider_unavailable",
      message: `Provider ${selection.instanceId} is unavailable for launch.`,
    });
  const model = provider.models.find(
    (candidate) =>
      candidate.slug === selection.model || candidate.aliases?.includes(selection.model),
  );
  if (!model)
    return yield* new OrchestratorMcpFailure({
      code: "model_unavailable",
      message: `Model ${selection.model} is not advertised by provider ${selection.instanceId}.`,
    });
  const invalid = invalidOptionSelections(
    selection.options ?? [],
    model.capabilities?.optionDescriptors,
  );
  if (invalid.length > 0)
    return yield* new OrchestratorMcpFailure({
      code: "invalid_request",
      message: `Model ${model.slug} on provider ${selection.instanceId} rejected options: ${invalid.join(" ")}`,
    });
  return { ...selection, model: model.slug };
});

export function delegatedPrismRole(role?: OrchestratorMcpTaskRole): PrismDelegateRole {
  switch (role) {
    case "review":
      return "reviewer";
    case "research":
      return "researcher";
    case "design":
      return "designer";
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

export class PrismService extends Context.Service<
  PrismService,
  {
    readonly resolve: (input: {
      readonly bare?: boolean | undefined;
      readonly projectId: ProjectId;
      readonly role: PrismRole;
      readonly lane?: PrismLane | undefined;
      readonly explicit?: ModelSelection | undefined;
      readonly inherited?: ModelSelection | undefined;
      readonly validate?:
        | ((selection: ModelSelection) => Effect.Effect<ModelSelection, OrchestratorMcpFailure>)
        | undefined;
      /**
       * How many Colors to start, for a Run group: the first that many that
       * can start, in list order. Defaults to 1; an explicit selection is one.
       */
      readonly count?: number | undefined;
    }) => Effect.Effect<
      {
        readonly modelSelection: ModelSelection;
        /** `modelSelection` first, then any further Colors `count` asked for. */
        readonly modelSelections: readonly [ModelSelection, ...Array<ModelSelection>];
        readonly kitText: string;
      },
      OrchestratorMcpFailure
    >;
    /** The role's Run: how many Colors one delegation starts. Always 1 for roles that change things. */
    readonly run: (input: {
      readonly projectId: ProjectId;
      readonly role: PrismRole;
    }) => Effect.Effect<number, OrchestratorMcpFailure>;
  }
>()("t3/prism/PrismService") {}

function roleRun(kits: PrismRoleKits, role: PrismRole): number {
  return isPrismRunGroupRole(role) ? kits[role].run : 1;
}

const make = Effect.gen(function* () {
  const registry = yield* ProviderRegistry.ProviderRegistry;
  const settings = yield* ServerSettings.ServerSettingsService;
  const sql = yield* SqlClient.SqlClient;
  const readKits = (projectId: ProjectId) =>
    settings.getSettings.pipe(
      Effect.mapError(
        () =>
          new OrchestratorMcpFailure({
            code: "orchestration_error",
            message: "Unable to read Prism settings.",
          }),
      ),
      Effect.map((saved) => resolveProjectSettings(saved, projectId).settings.prismRoles),
    );
  const run: PrismService["Service"]["run"] = (input) =>
    readKits(input.projectId).pipe(Effect.map((kits) => roleRun(kits, input.role)));
  const resolve: PrismService["Service"]["resolve"] = Effect.fn("PrismService.resolve")(
    function* (input) {
      const kits = yield* readKits(input.projectId);
      const kit = kits[input.role];
      const kitText = input.bare
        ? ""
        : [
            kit.instructions,
            kit.skills.length > 0 ? `Use these skills: ${kit.skills.join(", ")}.` : "",
          ]
            .filter(Boolean)
            .join("\n\n");
      // UI work must land on a UI Color: the ui lane never inherits the caller's
      // model, and callers often pass an inherited model as `explicit`.
      const ui = input.lane === "ui";
      if (ui && input.role !== "worker")
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: "The ui lane is only for the worker role.",
        });
      const preferences = prismRoleModels(kits, input.role, input.lane ?? DEFAULT_PRISM_LANE);
      if (input.explicit !== undefined) {
        const explicit = input.explicit;
        const modelSelection = input.validate ? yield* input.validate(explicit) : explicit;
        const providers = ui ? yield* registry.getProviders : [];
        // A Color may name a model by an alias of the slug a selection carries.
        const slug = (instanceId: string, model: string) =>
          providers
            .find((p) => p.instanceId === instanceId)
            ?.models.find((m) => m.slug === model || m.aliases?.includes(model))?.slug ?? model;
        const isUiColor = (selection: ModelSelection) =>
          preferences.some(
            (color) =>
              color.instanceId === selection.instanceId &&
              slug(color.instanceId, color.model) === slug(selection.instanceId, selection.model),
          );
        if (ui && !isUiColor(explicit) && !isUiColor(modelSelection))
          return yield* new OrchestratorMcpFailure({
            code: "invalid_request",
            message: `${modelSelection.instanceId}/${modelSelection.model} is not one of the worker's ui lane Colors. Omit the model to use them.`,
          });
        return { modelSelection, modelSelections: [modelSelection], kitText };
      }
      const providers = yield* registry.getProviders;
      if (ui && preferences.length === 0)
        return yield* new OrchestratorMcpFailure({
          code: "provider_unavailable",
          message: "The worker's ui lane has no Colors. Add one in Settings > Prism.",
        });
      const instant = yield* DateTime.now;
      const now = DateTime.toEpochMillis(instant);
      // Recovery preferences and stopping a child do not restore its subscription capacity.
      // Use the executing run's instance, even if its thread has since changed Colors.
      const limited = yield* sql<{ provider_instance_id: string }>`
        SELECT DISTINCT r.provider_instance_id
        FROM orchestration_v2_projection_turn_items item
        INNER JOIN orchestration_v2_projection_runs r ON r.run_id = item.run_id
        WHERE r.status = 'failed' AND r.provider_instance_id IS NOT NULL
          AND item.type = 'error' AND item.status = 'failed'
          AND item.turn_item_id = (
            SELECT error.turn_item_id FROM orchestration_v2_projection_turn_items error
            WHERE error.thread_id = r.thread_id AND error.run_id = r.run_id
              AND error.type = 'error' AND error.status = 'failed'
              AND error.node_id IS json_extract(r.payload_json, '$.rootNodeId')
            ORDER BY error.updated_at DESC, error.ordinal DESC, error.turn_item_id DESC
            LIMIT 1
          )
          AND json_extract(item.payload_json, '$.failure.class') = 'usage_limit'
          AND julianday(json_extract(item.payload_json, '$.failure.resetAt')) > julianday(${DateTime.formatIso(instant)})
      `.pipe(
        Effect.mapError(
          () =>
            new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: "Unable to read known provider capacity limits.",
            }),
        ),
      );
      const limitedInstances = new Set(limited.map((row) => row.provider_instance_id));
      const candidates =
        preferences.length > 0 ? preferences : input.inherited ? [input.inherited] : [];
      const count = Math.max(1, input.count ?? 1);
      const started: Array<ModelSelection> = [];
      for (const candidate of candidates) {
        if (started.length >= count) break;
        const provider = providers.find((p) => p.instanceId === candidate.instanceId);
        if (
          !provider?.enabled ||
          limitedInstances.has(candidate.instanceId) ||
          blocked(provider, now)
        )
          continue;
        // A validator can re-probe stale unavailable snapshots before refusing the candidate.
        const rechecking = provider.availability === "unavailable" && input.validate !== undefined;
        if (provider.availability === "unavailable" && !rechecking) continue;
        const model = provider.models.find(
          (m) => m.slug === candidate.model || m.aliases?.includes(candidate.model),
        );
        if (!model && !rechecking && (preferences.length > 0 || provider.models.length > 0))
          continue;
        const selection = colorModelSelection(
          { ...candidate, model: model?.slug ?? candidate.model },
          provider.driver,
        );
        const validated = yield* (
          input.validate ? input.validate(selection) : Effect.succeed(selection)
        ).pipe(
          Effect.catchIf(
            (error) => error.code === "provider_unavailable" || error.code === "model_unavailable",
            () => Effect.succeed(null),
          ),
        );
        if (validated === null) continue;
        const advertisedModel = rechecking
          ? (yield* registry.getProviders)
              .find((p) => p.instanceId === validated.instanceId)
              ?.models.find(
                (m) => m.slug === validated.model || m.aliases?.includes(validated.model),
              )
          : model;
        // New Color options must be advertised; legacy effort still works with older catalogs.
        const descriptors = advertisedModel?.capabilities?.optionDescriptors;
        const invalidColorOptions = invalidOptionSelections(
          candidate.options ?? [],
          preferences.length > 0 ? (descriptors ?? []) : descriptors,
        );
        const invalid =
          invalidColorOptions.length > 0
            ? invalidColorOptions
            : invalidOptionSelections(validated.options ?? [], descriptors);
        if (invalid.length > 0)
          return yield* new OrchestratorMcpFailure({
            code: "invalid_request",
            message: `Model ${validated.model} on provider ${validated.instanceId} rejected options: ${invalid.join(" ")}`,
          });
        started.push(validated);
      }
      const [first, ...rest] = started;
      if (first !== undefined)
        return { modelSelection: first, modelSelections: [first, ...rest], kitText };
      // Without a kit the inherited selection was the only candidate; name why it cannot run.
      if (preferences.length === 0 && input.inherited && input.validate)
        yield* input.validate(input.inherited);
      return yield* new OrchestratorMcpFailure({
        code: "provider_unavailable",
        message: "No eligible model with usage headroom for this Prism role and lane.",
      });
    },
  );
  return PrismService.of({ resolve, run });
});
export const layer = Layer.effect(PrismService, make);
