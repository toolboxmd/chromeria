import type { Color, ModelCapabilities, ProviderInstanceId } from "@t3tools/contracts";
import type { ProviderInstanceEntry } from "../../providerInstances";
import type { ModelEsque } from "../chat/providerIconUtils";

export interface ColorEffortChoice {
  readonly id: string;
  readonly label: string;
}

export interface ColorModelChoice {
  readonly slug: string;
  readonly name: string;
  readonly efforts: readonly ColorEffortChoice[];
}

/** A provider the scope allows, with the models it allows; the picker's only source of options. */
export interface ColorProviderChoice extends Pick<
  ProviderInstanceEntry,
  | "instanceId"
  | "driverKind"
  | "displayName"
  | "accentColor"
  | "acpRegistryAgentId"
  | "acpRegistryIconUrl"
> {
  readonly models: readonly ColorModelChoice[];
}

export function colorKey(color: Pick<Color, "instanceId" | "model">) {
  return JSON.stringify([color.instanceId, color.model]);
}

export function colorEffortOptions(
  capabilities: ModelCapabilities | null | undefined,
): readonly ColorEffortChoice[] {
  const descriptor = capabilities?.optionDescriptors?.find(
    (option) =>
      option.type === "select" && ["effort", "reasoningEffort", "variant"].includes(option.id),
  );
  return descriptor?.type === "select"
    ? descriptor.options.filter((option) => !descriptor.promptInjectedValues?.includes(option.id))
    : [];
}

/** Providers enabled and available for the scope, each with its models enabled for the scope. */
export function colorProviderChoices(
  entries: readonly Pick<
    ProviderInstanceEntry,
    | "instanceId"
    | "driverKind"
    | "displayName"
    | "accentColor"
    | "acpRegistryAgentId"
    | "acpRegistryIconUrl"
    | "enabled"
    | "isAvailable"
    | "models"
  >[],
  options: ReadonlyMap<ProviderInstanceId, readonly ModelEsque[]>,
  disabledReason: (instanceId: ProviderInstanceId, model: string) => string | null,
): readonly ColorProviderChoice[] {
  return entries.flatMap((entry) => {
    if (!entry.enabled || !entry.isAvailable) return [];
    const models = (options.get(entry.instanceId) ?? [])
      .filter((model) => !model.isUnavailable && !disabledReason(entry.instanceId, model.slug))
      .map((model) => ({
        slug: model.slug,
        name: model.name,
        efforts: colorEffortOptions(
          entry.models.find((candidate) => candidate.slug === model.slug)?.capabilities,
        ),
      }));
    if (models.length === 0) return [];
    const { enabled: _enabled, isAvailable: _isAvailable, models: _models, ...provider } = entry;
    return [{ ...provider, models }];
  });
}

export function findColorChoice(providers: readonly ColorProviderChoice[], color: Color) {
  const provider = providers.find((candidate) => candidate.instanceId === color.instanceId);
  return { provider, model: provider?.models.find((model) => model.slug === color.model) };
}

const withEffort = (
  instanceId: ProviderInstanceId,
  model: string,
  effort: string | undefined,
): Color => ({ instanceId, model, ...(effort ? { effort } : {}) });

/** The first model of the first provider at its default effort, or null when the scope offers none. */
export function defaultColor(providers: readonly ColorProviderChoice[]): Color | null {
  const provider = providers[0];
  const model = provider?.models[0];
  return provider && model ? withEffort(provider.instanceId, model.slug, undefined) : null;
}

/** A new provider starts on its first model at the default effort. */
export function chooseColorProvider(
  providers: readonly ColorProviderChoice[],
  current: Color,
  instanceId: ProviderInstanceId,
): Color {
  if (instanceId === current.instanceId) return current;
  const model = providers.find((provider) => provider.instanceId === instanceId)?.models[0];
  return model ? withEffort(instanceId, model.slug, undefined) : current;
}

/** Another model keeps the effort only when it offers the same one. */
export function chooseColorModel(
  providers: readonly ColorProviderChoice[],
  current: Color,
  slug: string,
): Color {
  if (slug === current.model) return current;
  const model = findColorChoice(providers, { ...current, model: slug }).model;
  if (!model) return current;
  const effort = model.efforts.some((option) => option.id === current.effort)
    ? current.effort
    : undefined;
  return withEffort(current.instanceId, slug, effort);
}

/** An empty effort means the provider default. */
export function chooseColorEffort(current: Color, effort: string): Color {
  return withEffort(current.instanceId, current.model, effort || undefined);
}
