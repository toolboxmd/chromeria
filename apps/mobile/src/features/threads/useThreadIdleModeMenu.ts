import { useAtomValue } from "@effect/atom-react";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import {
  keepWarmStatusText,
  keepWarmUnavailableReason,
  threadIdleChoice,
  threadIdleModeFor,
  wightThreadWait,
  wightUsageStatus,
  type ThreadIdleChoice,
} from "@t3tools/client-runtime/thread-idle-mode";
import { AuthSettingsWriteScope, DEFAULT_WIGHT_LIMIT_PERCENT } from "@t3tools/contracts";
import { useGT, useMessages } from "gt-react-native";

import type { AppBarMenu, AppBarMenuItem } from "../../components/ui/AppBar.types";
import { useNowMinuteMs } from "../../lib/clock";
import { serverEnvironment } from "../../state/server";
import { useEnvironmentScope } from "../../state/session";
import { useAtomCommand } from "../../state/use-atom-command";

// A phone menu cannot take typed hours, so Wight offers the timers people pick most.
const WIGHT_TIMER_HOURS = [null, 1, 2, 4, 8, 24] as const;

const formatTime = (ms: number) =>
  new Date(ms).toLocaleString(undefined, {
    weekday: "short",
    hour: "numeric",
    minute: "2-digit",
  });

/** What the thread does while idle, as a header menu: Off, Keep cache warm, or Wight. */
export function useThreadIdleModeMenu(thread: EnvironmentThreadShell | null): AppBarMenu | null {
  const gt = useGT();
  const m = useMessages();
  const environmentId = thread?.environmentId ?? null;
  const config = useAtomValue(serverEnvironment.configValueAtom(environmentId));
  const settings = config?.settings;
  // The settings command reports no required scope, so its permission atom would always allow it.
  const canWrite = useEnvironmentScope(environmentId, AuthSettingsWriteScope);
  const update = useAtomCommand(serverEnvironment.updateSettings, "thread idle mode update");
  const now = useNowMinuteMs();
  if (thread === null || environmentId === null || settings == null) return null;

  const mode = settings.threadIdleModes[thread.id];
  const choice = threadIdleChoice(mode, now);
  const provider = config?.providers.find(
    (entry) => entry.instanceId === thread.providerInstanceId,
  );
  const limit =
    settings.providerInstances[thread.providerInstanceId]?.wightLimitPercent ??
    DEFAULT_WIGHT_LIMIT_PERCENT;
  const usage = wightUsageStatus(provider, limit, m);
  const wait = wightThreadWait(thread.source, m);
  const keepWarmUnavailable = provider ? keepWarmUnavailableReason(provider.driver, m) : null;

  const choose = (next: ThreadIdleChoice, wightHours: number | null = null) =>
    void update({
      environmentId,
      input: {
        patch: {
          threadIdleModes: {
            [thread.id]: threadIdleModeFor(next, wightHours),
          },
        },
      },
    });

  const wightStatus =
    choice !== "wight" || mode?.type !== "wight"
      ? undefined
      : [
          mode.expiresAt === null
            ? gt("Wight runs until turned off.")
            : gt("Wight runs until {time}.", { time: formatTime(mode.expiresAt) }),
          wait,
          usage.text,
        ]
          .filter((line) => line !== null)
          .join(" ");
  const wightTimers: AppBarMenuItem[] = WIGHT_TIMER_HOURS.map((hours) => ({
    id: `idle-wight-${hours ?? "none"}`,
    title:
      hours === null
        ? gt("No timer")
        : gt("{hours, plural, one {{hours} hour} other {{hours} hours}}", { hours }),
    disabled: !canWrite,
    onPress: () => choose("wight", hours),
  }));
  const items: AppBarMenuItem[] = [
    {
      id: "idle-off",
      title: gt("Off", { $context: "thread idle mode" }),
      selected: choice === "off",
      disabled: !canWrite,
      onPress: () => choose("off"),
    },
    {
      id: "idle-keep-warm",
      title: gt("Keep cache warm"),
      selected: choice === "keepWarm",
      disabled: !canWrite || keepWarmUnavailable !== null,
      subtitle:
        keepWarmUnavailable ??
        (mode?.type === "keepWarm"
          ? keepWarmStatusText(mode, thread.source, formatTime, m)
          : gt("Pings after 55 quiet minutes; stops after 24 hours without your message.")),
      onPress: () => choose("keepWarm"),
    },
    choice === "wight"
      ? {
          id: "idle-wight",
          title: gt("Wight"),
          selected: true,
          disabled: !canWrite,
          ...(wightStatus === undefined ? {} : { subtitle: wightStatus }),
          onPress: () => undefined,
        }
      : { id: "idle-wight", title: gt("Wight"), items: wightTimers },
  ];
  const label =
    choice === "keepWarm"
      ? gt("While idle: keep cache warm")
      : choice === "wight"
        ? usage.paused
          ? gt("Wight mode paused by the usage limit")
          : wait !== null
            ? gt("Wight mode waiting on this thread")
            : gt("Wight mode on")
        : gt("While idle: off");
  return {
    title: label,
    icon:
      choice === "keepWarm"
        ? { ios: "flame", android: "bolt" }
        : choice === "wight"
          ? "arrow.clockwise"
          : "moon",
    items,
  };
}
