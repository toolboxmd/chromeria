import type { Color } from "@t3tools/contracts";
import { ProviderInstanceIcon } from "../chat/ProviderInstanceIcon";
import { Select, SelectItem, SelectPopup, SelectTrigger, SelectValue } from "../ui/select";
import {
  chooseColorEffort,
  chooseColorModel,
  chooseColorOption,
  chooseColorProvider,
  findColorChoice,
  type ColorProviderChoice,
} from "./ColorPicker.logic";

function ProviderLabel({ provider }: { provider: ColorProviderChoice }) {
  return (
    <span className="flex min-w-0 items-center gap-2">
      <ProviderInstanceIcon
        driverKind={provider.driverKind}
        displayName={provider.displayName}
        accentColor={provider.accentColor}
        acpRegistryAgentId={provider.acpRegistryAgentId}
        acpRegistryIconUrl={provider.acpRegistryIconUrl}
        iconClassName="size-4"
      />
      <span className="truncate">{provider.displayName}</span>
    </span>
  );
}

/**
 * Chooses a Color: a provider enabled for the scope, then one of its models, then that model's
 * effort and advertised options. A saved Color the scope no longer offers stays visible, marked unavailable.
 */
export function ColorPicker({
  value,
  providers,
  label,
  disabled = false,
  onChange,
}: {
  value: Color;
  providers: readonly ColorProviderChoice[];
  /** Names the Color for assistive technology, such as "Reviewer preference 1". */
  label: string;
  disabled?: boolean;
  onChange: (color: Color) => void;
}) {
  const { provider, model } = findColorChoice(providers, value);
  const efforts = model?.efforts ?? [];
  const unknownEffort = value.effort && !efforts.some((effort) => effort.id === value.effort);
  return (
    <span className="flex min-w-0 flex-wrap items-center gap-2">
      <Select
        value={value.instanceId}
        disabled={disabled}
        onValueChange={(instanceId) => {
          const next = providers.find((candidate) => candidate.instanceId === instanceId);
          if (next) onChange(chooseColorProvider(providers, value, next.instanceId));
        }}
      >
        <SelectTrigger size="sm" className="w-fit" aria-label={`${label} provider`}>
          <SelectValue>
            {provider ? <ProviderLabel provider={provider} /> : `${value.instanceId} (unavailable)`}
          </SelectValue>
        </SelectTrigger>
        <SelectPopup>
          {!provider && (
            <SelectItem value={value.instanceId} disabled>
              {value.instanceId} (unavailable)
            </SelectItem>
          )}
          {providers.map((candidate) => (
            <SelectItem key={candidate.instanceId} value={candidate.instanceId}>
              <ProviderLabel provider={candidate} />
            </SelectItem>
          ))}
        </SelectPopup>
      </Select>
      <Select
        value={value.model}
        disabled={disabled || !provider}
        onValueChange={(slug) => {
          if (slug !== null) onChange(chooseColorModel(providers, value, slug));
        }}
      >
        <SelectTrigger size="sm" className="w-fit" aria-label={`${label} model`}>
          <SelectValue>{model ? model.name : `${value.model} (unavailable)`}</SelectValue>
        </SelectTrigger>
        <SelectPopup>
          {!model && (
            <SelectItem value={value.model} disabled>
              {value.model} (unavailable)
            </SelectItem>
          )}
          {provider?.models.map((candidate) => (
            <SelectItem key={candidate.slug} value={candidate.slug}>
              {candidate.name}
            </SelectItem>
          ))}
        </SelectPopup>
      </Select>
      <Select
        value={value.effort ?? ""}
        disabled={disabled || !model}
        onValueChange={(effort) => {
          if (effort !== null) onChange(chooseColorEffort(value, effort));
        }}
      >
        <SelectTrigger size="sm" className="w-fit" aria-label={`${label} effort`}>
          <SelectValue>
            {efforts.find((effort) => effort.id === value.effort)?.label ??
              value.effort ??
              "Default effort"}
          </SelectValue>
        </SelectTrigger>
        <SelectPopup>
          <SelectItem value="">Default effort</SelectItem>
          {unknownEffort && (
            <SelectItem value={value.effort!} disabled>
              {value.effort} (unavailable)
            </SelectItem>
          )}
          {efforts.map((effort) => (
            <SelectItem key={effort.id} value={effort.id}>
              {effort.label}
            </SelectItem>
          ))}
        </SelectPopup>
      </Select>
      {model?.options?.map((descriptor) => {
        const selected = value.options?.find((option) => option.id === descriptor.id)?.value;
        const choices =
          descriptor.type === "boolean"
            ? [
                { id: "true", label: "On" },
                { id: "false", label: "Off" },
              ]
            : descriptor.options;
        const selectedId = selected === undefined ? "" : String(selected);
        const unknown =
          selected !== undefined && !choices.some((choice) => choice.id === selectedId);
        return (
          <Select
            key={descriptor.id}
            value={selectedId}
            disabled={disabled}
            onValueChange={(option) => {
              if (option !== null)
                onChange(
                  chooseColorOption(
                    value,
                    descriptor.id,
                    option === ""
                      ? undefined
                      : descriptor.type === "boolean"
                        ? option === "true"
                        : option,
                  ),
                );
            }}
          >
            <SelectTrigger size="sm" className="w-fit" aria-label={`${label} ${descriptor.label}`}>
              <SelectValue>
                {choices.find((choice) => choice.id === selectedId)?.label ??
                  (unknown
                    ? `${selectedId} (unavailable)`
                    : `Default ${descriptor.label.toLowerCase()}`)}
              </SelectValue>
            </SelectTrigger>
            <SelectPopup>
              <SelectItem value="">Default {descriptor.label.toLowerCase()}</SelectItem>
              {unknown && (
                <SelectItem value={selectedId} disabled>
                  {selectedId} (unavailable)
                </SelectItem>
              )}
              {choices.map((choice) => (
                <SelectItem key={choice.id} value={choice.id}>
                  {choice.label}
                </SelectItem>
              ))}
            </SelectPopup>
          </Select>
        );
      })}
    </span>
  );
}
