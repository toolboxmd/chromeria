import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { DEFAULT_WIGHT_LIMIT_PERCENT, type EnvironmentId, type ThreadId } from "@t3tools/contracts";
import { useAtomValue } from "@effect/atom-react";
import { GhostIcon } from "lucide-react";
import { useEffect, useMemo, useState } from "react";

import { useEnvironmentSettings, useUpdateEnvironmentSettings } from "~/hooks/useSettings";
import { useThreadShell } from "~/state/entities";
import { serverEnvironment } from "~/state/server";
import { wightThreadWait, wightUsageStatus } from "~/wightMode";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import { Popover, PopoverPopup, PopoverTrigger } from "../ui/popover";
import { Switch } from "../ui/switch";

const MAX_WIGHT_HOURS = 8760;
// setTimeout overflows past ~24.8 days; longer timers re-arm when `now` advances.
const MAX_TIMEOUT_MS = 2_147_483_647;

const TRIGGER_LABEL = {
  off: "Wight mode",
  on: "Wight mode on",
  paused: "Wight mode paused by the usage limit",
  waiting: "Wight mode waiting on this thread",
} as const;
const TRIGGER_TEXT = { off: "", on: "Wight", paused: "Wight paused", waiting: "Wight waiting" };

/**
 * Wight mode toggle in the chat header. The activation lives in the thread
 * environment's server settings, so every client connected to that server
 * sees and changes the same state.
 */
export function WightModeControl({
  environmentId,
  threadId,
}: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
}) {
  const mode = useEnvironmentSettings(environmentId, (settings) => settings.wightModes[threadId]);
  const update = useUpdateEnvironmentSettings(environmentId);
  const thread = useThreadShell(
    useMemo(() => scopeThreadRef(environmentId, threadId), [environmentId, threadId]),
  );
  const instanceId = thread?.session?.providerInstanceId ?? thread?.modelSelection.instanceId;
  const limit = useEnvironmentSettings(environmentId, (settings) =>
    instanceId === undefined
      ? DEFAULT_WIGHT_LIMIT_PERCENT
      : (settings.providerInstances[instanceId]?.wightLimitPercent ?? DEFAULT_WIGHT_LIMIT_PERCENT),
  );
  const provider = useAtomValue(serverEnvironment.configValueAtom(environmentId))?.providers.find(
    (entry) => entry.instanceId === instanceId,
  );
  const [hours, setHours] = useState("");
  const [now, setNow] = useState(Date.now);
  const expiresAt = mode?.expiresAt ?? null;
  // One timeout per render at most, only while a timer is pending: it updates
  // `now` at expiry (or at the setTimeout cap, which reruns this effect).
  useEffect(() => {
    if (expiresAt === null || expiresAt <= now) return;
    const timer = window.setTimeout(
      () => setNow(Date.now()),
      Math.min(MAX_TIMEOUT_MS, Math.max(0, expiresAt - Date.now())),
    );
    return () => window.clearTimeout(timer);
  }, [expiresAt, now]);

  const enabled = mode !== undefined && (expiresAt === null || expiresAt > now);
  const duration = hours.trim() === "" ? null : Number(hours);
  const validDuration =
    duration === null || (Number.isFinite(duration) && duration > 0 && duration <= MAX_WIGHT_HOURS);
  const usage = wightUsageStatus(provider, limit);
  const wait = thread ? wightThreadWait(thread) : null;
  const state = !enabled ? "off" : usage.paused ? "paused" : wait !== null ? "waiting" : "on";

  const setEnabled = (checked: boolean) => {
    update({
      wightModes: {
        [threadId]: checked
          ? {
              enabledAt: new Date().toISOString(),
              expiresAt: duration === null ? null : Date.now() + duration * 3_600_000,
            }
          : null,
      },
    });
  };

  return (
    <Popover>
      <PopoverTrigger
        render={
          <Button
            size="xs"
            variant={enabled ? "outline" : "ghost-muted"}
            aria-label={TRIGGER_LABEL[state]}
          />
        }
      >
        <GhostIcon />
        {enabled ? <span>{TRIGGER_TEXT[state]}</span> : null}
      </PopoverTrigger>
      <PopoverPopup align="end" width="md">
        <div className="grid gap-3 text-sm">
          <label className="flex items-center justify-between gap-3 font-medium">
            Wight mode
            <Switch
              aria-label="Wight mode"
              checked={enabled}
              disabled={!enabled && !validDuration}
              onCheckedChange={setEnabled}
            />
          </label>
          <p className="text-xs text-muted-foreground">
            Sends “continue” with the time whenever this thread is idle. Turning it off or reaching
            the timer lets the current turn finish.
          </p>
          {enabled ? (
            <>
              <p className="text-xs">
                {expiresAt === null
                  ? "Runs until turned off."
                  : `Runs until ${new Date(expiresAt).toLocaleString()}.`}
              </p>
              {wait !== null ? <p className="text-xs">{wait}</p> : null}
              <p className="text-xs text-muted-foreground">{usage.text}</p>
            </>
          ) : (
            <>
              <label className="grid gap-1 text-xs">
                Timer in hours
                <Input
                  size="sm"
                  type="number"
                  inputMode="decimal"
                  min="0.25"
                  max={MAX_WIGHT_HOURS}
                  step="0.25"
                  placeholder="No timer"
                  value={hours}
                  aria-invalid={!validDuration || undefined}
                  onChange={(event) => setHours(event.target.value)}
                />
              </label>
              {mode?.expiresAt != null ? (
                <p className="text-xs text-muted-foreground">
                  Timer ended {new Date(mode.expiresAt).toLocaleString()}.
                </p>
              ) : null}
            </>
          )}
        </div>
      </PopoverPopup>
    </Popover>
  );
}
