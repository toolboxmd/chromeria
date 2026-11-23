import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import {
  keepWarmStatusText,
  keepWarmUnavailableReason,
  MAX_WIGHT_HOURS,
  parseWightHours,
  threadIdleChoice,
  threadIdleModeFor,
  wightThreadWait,
  wightUsageStatus,
  type ThreadIdleChoice,
} from "@t3tools/client-runtime/thread-idle-mode";
import {
  AuthSettingsWriteScope,
  DEFAULT_WIGHT_LIMIT_PERCENT,
  type EnvironmentId,
  type ThreadId,
} from "@t3tools/contracts";
import { useAtomValue } from "@effect/atom-react";
import { T, msg, useGT, useMessages, Var } from "gt-react";
import { FlameIcon, GhostIcon } from "lucide-react";
import { useEffect, useState } from "react";

import { useEnvironmentSettings, useUpdateEnvironmentSettings } from "~/hooks/useSettings";
import { useThreadShell } from "~/state/entities";
import { useEnvironmentScope } from "~/state/session";
import { serverEnvironment } from "~/state/server";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import { Popover, PopoverPopup, PopoverTrigger } from "../ui/popover";
import { Radio, RadioGroup } from "../ui/radio-group";

// setTimeout overflows past ~24.8 days; longer timers re-arm when `now` advances.
const MAX_TIMEOUT_MS = 2_147_483_647;

const TRIGGER_LABEL = {
  off: msg("While idle: off"),
  keepWarm: msg("While idle: keep cache warm"),
  wight: msg("Wight mode on"),
  paused: msg("Wight mode paused by the usage limit"),
  waiting: msg("Wight mode waiting on this thread"),
};
const TRIGGER_TEXT = {
  off: "",
  keepWarm: msg("Warm"),
  wight: "Wight",
  paused: msg("Wight paused"),
  waiting: msg("Wight waiting"),
};

const formatTime = (ms: number) => new Date(ms).toLocaleString();

/**
 * What the thread does while idle: nothing, keep its prompt cache warm, or Wight. The choice
 * lives in the thread environment's server settings, so every client sees the same state.
 */
export function IdleModeControl({
  environmentId,
  threadId,
}: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
}) {
  const gt = useGT();
  const m = useMessages();
  const mode = useEnvironmentSettings(
    environmentId,
    (settings) => settings.threadIdleModes[threadId],
  );
  const update = useUpdateEnvironmentSettings(environmentId);
  const thread = useThreadShell(scopeThreadRef(environmentId, threadId));
  const instanceId = thread?.providerInstanceId;
  const canWrite = useEnvironmentScope(environmentId, AuthSettingsWriteScope);
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
  const expiresAt = mode?.type === "wight" ? mode.expiresAt : null;
  // One timeout per render at most, only while a Wight timer is pending: it updates `now` at
  // expiry (or at the setTimeout cap, which reruns this effect).
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- The mounted control owns the expiry timer and cancels it when the deadline changes.
  useEffect(() => {
    if (expiresAt === null || expiresAt <= now) return;
    const timer = window.setTimeout(
      () => setNow(Date.now()),
      Math.min(MAX_TIMEOUT_MS, Math.max(0, expiresAt - Date.now())),
    );
    return () => window.clearTimeout(timer);
  }, [expiresAt, now]);

  const choice = threadIdleChoice(mode, now);
  const wightHours = parseWightHours(hours);
  const usage = wightUsageStatus(provider, limit, m);
  const wait = thread ? wightThreadWait(thread.source, m) : null;
  const keepWarmUnavailable = provider ? keepWarmUnavailableReason(provider.driver, m) : null;
  const state =
    choice !== "wight" ? choice : usage.paused ? "paused" : wait !== null ? "waiting" : "wight";

  const choose = (next: ThreadIdleChoice) => {
    if (next === choice) return;
    update({
      threadIdleModes: {
        [threadId]: threadIdleModeFor(next, wightHours ?? null),
      },
    });
  };

  return (
    <Popover>
      <PopoverTrigger
        render={
          <Button
            size="xs"
            variant={choice === "off" ? "ghost-muted" : "outline"}
            aria-label={m(TRIGGER_LABEL[state])}
          />
        }
      >
        {choice === "keepWarm" ? <FlameIcon /> : <GhostIcon />}
        {choice === "off" ? null : <span>{m(TRIGGER_TEXT[state])}</span>}
      </PopoverTrigger>
      <PopoverPopup align="end" width="md">
        <div className="grid gap-3 text-sm">
          <p className="font-medium">
            <T>While this thread is idle</T>
          </p>
          <RadioGroup
            aria-label={gt("While this thread is idle")}
            value={choice}
            disabled={!canWrite}
            onValueChange={(value) => choose(value as ThreadIdleChoice)}
          >
            <label className="flex items-start gap-2">
              <Radio value="off" />
              <span className="grid gap-0.5">
                <T>Off</T>
              </span>
            </label>
            <label className="flex items-start gap-2">
              <Radio value="keepWarm" disabled={keepWarmUnavailable !== null} />
              <span className="grid gap-0.5">
                <T>Keep cache warm</T>
                <span className="text-xs text-muted-foreground">
                  {keepWarmUnavailable ?? (
                    <T>
                      After 55 quiet minutes, sends a one-word ping so the next message reads the
                      conversation from the cache. Stops after 24 hours without your message.
                    </T>
                  )}
                </span>
              </span>
            </label>
            <label className="flex items-start gap-2">
              <Radio value="wight" disabled={choice !== "wight" && wightHours === undefined} />
              <span className="grid gap-0.5">
                <T>Wight</T>
                <span className="text-xs text-muted-foreground">
                  <T>
                    Sends “continue” with the time whenever this thread is idle. Turning it off or
                    reaching the timer lets the current turn finish.
                  </T>
                </span>
              </span>
            </label>
          </RadioGroup>
          {mode?.type === "keepWarm" ? (
            <p className="text-xs">{keepWarmStatusText(mode, thread?.source, formatTime, m)}</p>
          ) : null}
          {choice === "wight" ? (
            <>
              <p className="text-xs">
                {expiresAt === null
                  ? gt("Wight runs until turned off.")
                  : gt("Wight runs until {time}.", { time: formatTime(expiresAt) })}
              </p>
              {wait !== null ? <p className="text-xs">{wait}</p> : null}
              <p className="text-xs text-muted-foreground">{usage.text}</p>
            </>
          ) : (
            <>
              <label className="grid gap-1 text-xs">
                <T>Wight timer in hours</T>
                <Input
                  size="sm"
                  type="number"
                  inputMode="decimal"
                  min="0.25"
                  max={MAX_WIGHT_HOURS}
                  step="0.25"
                  placeholder={gt("No timer")}
                  value={hours}
                  aria-invalid={wightHours === undefined || undefined}
                  onChange={(event) => setHours(event.target.value)}
                />
              </label>
              {mode?.type === "wight" && mode.expiresAt !== null ? (
                <p className="text-xs text-muted-foreground">
                  <T>
                    Wight timer ended <Var>{formatTime(mode.expiresAt)}</Var>.
                  </T>
                </p>
              ) : null}
            </>
          )}
        </div>
      </PopoverPopup>
    </Popover>
  );
}
