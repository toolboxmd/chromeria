import type {
  Color,
  ModelCapabilities,
  ProviderInstanceId,
  ProviderOptionDescriptor,
  ProviderOptionSelection,
} from "@t3tools/contracts";
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
  readonly options?: readonly ProviderOptionDescriptor[];
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

export function colorKey(color: Color) {
  return JSON.stringify([
    color.instanceId,
    color.model,
    color.effort ?? null,
    [...(color.options ?? [])]
      .sort((a, b) => a.id.localeCompare(b.id))
      .map(({ id, value }) => [id, value]),
  ]);
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
      .map((model) => {
        const capabilities = entry.models.find(
          (candidate) => candidate.slug === model.slug,
        )?.capabilities;
        const options = capabilities?.optionDescriptors
          ?.filter((option) => !["effort", "reasoningEffort", "variant"].includes(option.id))
          .map((option) =>
            option.type === "select"
              ? {
                  ...option,
                  options: option.options
                    .filter((choice) => !option.promptInjectedValues?.includes(choice.id))
                    .map((choice) =>
                      option.id === "serviceTier" && choice.id === "priority"
                        ? { ...choice, label: "Fast" }
                        : choice,
                    ),
                }
              : option,
          );
        return {
          slug: model.slug,
          name: model.name,
          efforts: colorEffortOptions(capabilities),
          ...(options?.length ? { options } : {}),
        };
      });
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
  const options = current.options?.filter((selection) =>
    model.options?.some(
      (descriptor) =>
        descriptor.id === selection.id &&
        (descriptor.type === "boolean"
          ? typeof selection.value === "boolean"
          : descriptor.options.some((choice) => choice.id === selection.value)),
    ),
  );
  return {
    ...withEffort(current.instanceId, slug, effort),
    ...(options?.length ? { options } : {}),
  };
}

/** An empty effort means the provider default. */
export function chooseColorEffort(current: Color, effort: string): Color {
  const { effort: _effort, ...rest } = current;
  return { ...rest, ...(effort ? { effort } : {}) };
}

/** Undefined removes an override so the provider uses its default. */
export function chooseColorOption(
  current: Color,
  id: string,
  value: ProviderOptionSelection["value"] | undefined,
): Color {
  const { options: previous, ...rest } = current;
  const options = [
    ...(previous ?? []).filter((option) => option.id !== id),
    ...(value === undefined ? [] : [{ id, value }]),
  ];
  return { ...rest, ...(options.length ? { options } : {}) };
}
