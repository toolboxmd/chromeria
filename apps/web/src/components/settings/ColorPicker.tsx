import type { Color } from "@t3tools/contracts";
import { T, useGT, Var } from "gt-react";
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

export function ProviderLabel({ provider }: { provider: ColorProviderChoice }) {
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
  const gt = useGT();
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
        <SelectTrigger size="sm" className="w-fit" aria-label={gt("{label} provider", { label })}>
          <SelectValue>
            {provider ? (
              <ProviderLabel provider={provider} />
            ) : (
              gt("{name} (unavailable)", { name: value.instanceId })
            )}
          </SelectValue>
        </SelectTrigger>
        <SelectPopup>
          {!provider && (
            <SelectItem value={value.instanceId} disabled>
              <T>
                <Var>{value.instanceId}</Var> (unavailable)
              </T>
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
        <SelectTrigger size="sm" className="w-fit" aria-label={gt("{label} model", { label })}>
          <SelectValue>
            {model ? model.name : gt("{name} (unavailable)", { name: value.model })}
          </SelectValue>
        </SelectTrigger>
        <SelectPopup>
          {!model && (
            <SelectItem value={value.model} disabled>
              <T>
                <Var>{value.model}</Var> (unavailable)
              </T>
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
        <SelectTrigger size="sm" className="w-fit" aria-label={gt("{label} effort", { label })}>
          <SelectValue>
            {efforts.find((effort) => effort.id === value.effort)?.label ??
              value.effort ??
              gt("Default effort")}
          </SelectValue>
        </SelectTrigger>
        <SelectPopup>
          <SelectItem value="">
            <T>Default effort</T>
          </SelectItem>
          {unknownEffort && (
            <SelectItem value={value.effort!} disabled>
              <T>
                <Var>{value.effort}</Var> (unavailable)
              </T>
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
                { id: "true", label: gt("On", { $context: "boolean option value" }) },
                { id: "false", label: gt("Off", { $context: "boolean option value" }) },
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
            <SelectTrigger
              size="sm"
              className="w-fit"
              aria-label={gt("{label} {option}", { label, option: descriptor.label })}
            >
              <SelectValue>
                {choices.find((choice) => choice.id === selectedId)?.label ??
                  (unknown
                    ? gt("{name} (unavailable)", { name: selectedId })
                    : gt("Default {option}", { option: descriptor.label.toLowerCase() }))}
              </SelectValue>
            </SelectTrigger>
            <SelectPopup>
              <SelectItem value="">
                <T>
                  Default <Var>{descriptor.label.toLowerCase()}</Var>
                </T>
              </SelectItem>
              {unknown && (
                <SelectItem value={selectedId} disabled>
                  <T>
                    <Var>{selectedId}</Var> (unavailable)
                  </T>
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
