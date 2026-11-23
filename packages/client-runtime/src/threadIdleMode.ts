/**
 * What a thread does while idle, for every client: which choice is active, Wight's gates, and
 * keep-warm's schedule. The server's `threadIdleModes` setting is the one authoritative fact.
 */
import {
  isKeepWarmPingMessage,
  keepWarmSchedule,
  supportsKeepWarm,
  type KeepWarmMode,
  type KeepWarmStopReason,
  type OrchestrationV2ThreadShell,
  type ServerProvider,
  type ThreadIdleMode,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { mFallback, msg } from "gt-i18n";
import type { MFunctionType } from "gt-i18n/types";

export type ThreadIdleChoice = "off" | "keepWarm" | "wight";

/** A Wight timer that ran out and a stopped keep-warm both read as off. */
export function threadIdleChoice(mode: ThreadIdleMode | undefined, now: number): ThreadIdleChoice {
  if (mode === undefined) return "off";
  if (mode.type === "keepWarm") return mode.stopped === null ? "keepWarm" : "off";
  return mode.expiresAt === null || mode.expiresAt > now ? "wight" : "off";
}

/** The setting value that turns on `choice`; null clears the thread's entry. */
export function threadIdleModeFor(
  choice: ThreadIdleChoice,
  wightHours: number | null,
  now = DateTime.toEpochMillis(DateTime.nowUnsafe()),
): ThreadIdleMode | null {
  const enabledAt = DateTime.formatIso(DateTime.makeUnsafe(now));
  switch (choice) {
    case "off":
      return null;
    case "keepWarm":
      return { type: "keepWarm", enabledAt, lastPingAt: null, stopped: null };
    case "wight":
      return {
        type: "wight",
        enabledAt,
        expiresAt: wightHours === null ? null : now + wightHours * 3_600_000,
      };
  }
}

export const MAX_WIGHT_HOURS = 8760;

/** Parses the Wight timer field: null for no timer, undefined when invalid. */
export function parseWightHours(text: string): number | null | undefined {
  if (text.trim() === "") return null;
  const hours = Number(text);
  return Number.isFinite(hours) && hours > 0 && hours <= MAX_WIGHT_HOURS ? hours : undefined;
}

/** Why keep-warm cannot be chosen for this provider, or null when it can. */
export function keepWarmUnavailableReason(
  driver: string | undefined,
  resolve: MFunctionType = mFallback,
): string | null {
  return supportsKeepWarm(driver)
    ? null
    : resolve(msg("Only Claude keeps its prompt cache for an hour, so a ping would not help."));
}

const STOP_REASONS: Record<KeepWarmStopReason, string> = {
  userAway: msg("Stopped after 24 hours without your message."),
  usageLimit: msg("Stopped at the provider's usage limit."),
  archived: msg("Stopped because the thread was archived."),
  settled: msg("Stopped because the thread was settled."),
  unsupportedProvider: msg("Stopped because this thread's provider does not keep an hour cache."),
};

function keepWarmStopText(reason: KeepWarmStopReason, resolve: MFunctionType = mFallback): string {
  return resolve(STOP_REASONS[reason]);
}

type KeepWarmShell = Pick<
  OrchestrationV2ThreadShell,
  "latestRunCompletedAt" | "latestRunStartedAt" | "latestUserAuthoredMessageAt"
>;

const isoOf = (value: DateTime.Utc | null | undefined): string | null =>
  value == null ? null : DateTime.formatIso(value);

/** Next ping and the automatic stop, in epoch milliseconds, mirroring the server. */
function keepWarmTimes(mode: KeepWarmMode, thread: KeepWarmShell | undefined) {
  return keepWarmSchedule(mode, {
    latestRunAt: isoOf(thread?.latestRunCompletedAt ?? thread?.latestRunStartedAt),
    latestUserMessageAt: isoOf(thread?.latestUserAuthoredMessageAt),
  });
}

/** One status line for an active keep-warm: the next ping, then when it stops by itself. */
export function keepWarmStatusText(
  mode: KeepWarmMode,
  thread: KeepWarmShell | undefined,
  formatTime: (ms: number) => string,
  resolve: MFunctionType = mFallback,
): string {
  if (mode.stopped !== null) return keepWarmStopText(mode.stopped.reason, resolve);
  const { nextPingAt, stopsAt } = keepWarmTimes(mode, thread);
  const stops = formatTime(stopsAt);
  return nextPingAt === null || nextPingAt >= stopsAt
    ? resolve(msg("No ping due. Stops by itself at {stops}.", { stops }))
    : resolve(
        msg("Next ping at {next}. Stops by itself at {stops}.", {
          next: formatTime(nextPingAt),
          stops,
        }),
      );
}

/** The gray line standing in for consecutive pings in a conversation. */
export function keepWarmPingsLabel(count: number, resolve: MFunctionType = mFallback): string {
  return resolve(
    msg("Kept warm, {count, plural, one {{count} ping} other {{count} pings}}", { count }),
  );
}

/** Runs started by a keep-warm ping, whose prompt, reply and work fold into one line. */
export function keepWarmPingRunIds<RunId extends string>(
  messages: Iterable<{ readonly id: string; readonly runId: RunId | null }>,
): ReadonlySet<RunId> {
  const runIds = new Set<RunId>();
  for (const message of messages) {
    if (message.runId !== null && isKeepWarmPingMessage(message)) runIds.add(message.runId);
  }
  return runIds;
}

export interface WightUsageStatus {
  readonly paused: boolean;
  readonly text: string;
}

/**
 * Explains the server's Wight quota gate for one provider instance. Mirrors `wightPaused` on the
 * server: a missing or disabled instance, a 0% limit, or any reported window at the limit pauses
 * new nudges; no reported windows means the limit cannot be enforced.
 */
export function wightUsageStatus(
  provider: Pick<ServerProvider, "enabled" | "usageLimits"> | undefined,
  limit: number,
  resolve: MFunctionType = mFallback,
): WightUsageStatus {
  if (provider === undefined || !provider.enabled) {
    return { paused: true, text: resolve(msg("Paused: this provider instance is unavailable.")) };
  }
  if (limit === 0) return { paused: true, text: resolve(msg("Paused: the Wight limit is 0%.")) };
  const windows = provider.usageLimits?.windows ?? [];
  const highest = windows.reduce<(typeof windows)[number] | null>(
    (top, window) => (top === null || window.usedPercent > top.usedPercent ? window : top),
    null,
  );
  if (highest === null) {
    return {
      paused: false,
      text: resolve(
        msg("This provider reports no usage windows, so the Wight limit cannot be enforced."),
      ),
    };
  }
  const used = Math.round(highest.usedPercent);
  return highest.usedPercent >= limit
    ? {
        paused: true,
        text: resolve(
          msg("Paused: {label} at {used}%, limit {limit}%.", { label: highest.label, used, limit }),
        ),
      }
    : {
        paused: false,
        text: resolve(
          msg("{label} at {used}%, limit {limit}%.", { label: highest.label, used, limit }),
        ),
      };
}

/**
 * Why the server is not sending "continue" to this thread right now, or null when it is working
 * or idle and ready for one. Mirrors `wightIdle` on the server, which leaves these states for the
 * user to clear.
 */
export function wightThreadWait(
  thread: Pick<
    OrchestrationV2ThreadShell,
    | "archivedAt"
    | "pendingRuntimeRequest"
    | "hasActionableProposedPlan"
    | "status"
    | "lastErrorClass"
    | "limitRecovery"
  >,
  resolve: MFunctionType = mFallback,
): string | null {
  if (thread.archivedAt !== null) return resolve(msg("Waiting: the thread is archived."));
  if (thread.pendingRuntimeRequest !== null)
    return thread.pendingRuntimeRequest.kind === "user_input"
      ? resolve(msg("Waiting for an answer to its question."))
      : resolve(msg("Waiting for an approval."));
  if (thread.hasActionableProposedPlan)
    return resolve(msg("Waiting for a decision on the proposed plan."));
  if (
    thread.limitRecovery != null ||
    (thread.status === "failed" && thread.lastErrorClass === "usage_limit")
  )
    return resolve(msg("Waiting for usage-limit reset recovery."));
  if (thread.status === "interrupted" || thread.status === "cancelled")
    return resolve(msg("Waiting: the last turn was interrupted. Send a message to resume."));
  if (thread.status === "failed")
    return resolve(msg("Waiting: the last turn failed. Send a message to resume."));
  return null;
}
