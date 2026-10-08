import { DEFAULT_WIGHT_LIMIT_PERCENT, type ServerProvider } from "@t3tools/contracts";
import { type CSSProperties, useState } from "react";

import { wightUsageStatus } from "~/wightMode";
import { SettingsRow } from "./settingsLayout";

/**
 * Per provider instance quota line for Wight mode. The slider keeps a local
 * draft while dragging and saves once on release, so a drag is one settings
 * write instead of one per step.
 */
export function WightLimitSetting({
  value = DEFAULT_WIGHT_LIMIT_PERCENT,
  provider,
  onChange,
  disabled,
}: {
  readonly value: number | undefined;
  readonly provider: ServerProvider | undefined;
  readonly onChange: (value: number) => void;
  readonly disabled: boolean;
}) {
  const [draft, setDraft] = useState<number | null>(null);
  const shown = draft ?? value;
  const commit = () => {
    if (draft !== null && draft !== value) onChange(draft);
    setDraft(null);
  };
  const sliderStyle = {
    "--settings-slider-progress": `${shown}%`,
    "--settings-slider-fill-offset": `${0.5 - shown / 100}rem`,
  } as CSSProperties;
  return (
    <SettingsRow
      title="Wight usage limit"
      description="Wight threads on this instance stop getting new turns while any usage window is at or above this limit, and resume once it drops below. 0% pauses them."
      status={provider ? wightUsageStatus(provider, value).text : undefined}
      control={
        <div className="flex w-full items-center gap-3 sm:w-52">
          <output className="min-w-12 rounded-md bg-muted px-2 py-1 text-center font-mono text-xs font-medium tabular-nums text-foreground">
            {shown}%
          </output>
          <input
            aria-label="Wight usage limit"
            className="settings-slider min-w-0 flex-1"
            type="range"
            min={0}
            max={100}
            step={1}
            value={shown}
            disabled={disabled}
            style={sliderStyle}
            onChange={(event) => setDraft(Number(event.currentTarget.value))}
            onPointerUp={commit}
            onKeyUp={commit}
            onBlur={commit}
          />
        </div>
      }
    />
  );
}
