import * as Schema from "effect/Schema";
import { IsoDateTime } from "./baseSchemas.ts";
import { WightMode } from "./wight.ts";

/** Pings keep a one-hour prompt cache warm; the margin covers the last request of a run. */
export const KEEP_WARM_INTERVAL_MS = 55 * 60_000;
/** The user's figure: after this long without their message, warming costs more than it saves. */
export const KEEP_WARM_MAX_AWAY_MS = 24 * 60 * 60_000;
export const KEEP_WARM_PING_TEXT =
  "Keep-warm ping from T3 Code while the user is away. Reply with one word and use no tools.";
const KEEP_WARM_MESSAGE_ID_PREFIX = "keep-warm:";

export const KeepWarmStopReason = Schema.Literals([
  "userAway",
  "usageLimit",
  "archived",
  "settled",
  "unsupportedProvider",
]);
export type KeepWarmStopReason = typeof KeepWarmStopReason.Type;

export const KeepWarmMode = Schema.Struct({
  type: Schema.Literal("keepWarm"),
  enabledAt: IsoDateTime,
  lastPingAt: Schema.NullOr(IsoDateTime),
  stopped: Schema.NullOr(Schema.Struct({ at: IsoDateTime, reason: KeepWarmStopReason })),
});
export type KeepWarmMode = typeof KeepWarmMode.Type;

/** What a thread does while idle. One entry per thread, so Wight and keep-warm never both act. */
export const ThreadIdleMode = Schema.Union([
  Schema.Struct({ type: Schema.Literal("wight"), ...WightMode.fields }),
  KeepWarmMode,
]);
export type ThreadIdleMode = typeof ThreadIdleMode.Type;

/** Only Claude's subscription cache lives an hour; see toolboxmd/chromeria#928 for each adapter. */
export const supportsKeepWarm = (driver: string | undefined): boolean => driver === "claudeAgent";

/** The server owns this id namespace; the id makes each due ping of an activation one command. */
export const keepWarmPingMessageId = (threadId: string, mode: KeepWarmMode, dueMs: number) =>
  `${KEEP_WARM_MESSAGE_ID_PREFIX}${threadId}:${Date.parse(mode.enabledAt)}:${dueMs}`;
export const isKeepWarmPingMessage = (message: { readonly id: string }): boolean =>
  message.id.startsWith(KEEP_WARM_MESSAGE_ID_PREFIX);

const epoch = (value: string | null | undefined): number | null => {
  if (value == null) return null;
  const ms = Date.parse(value);
  return Number.isFinite(ms) ? ms : null;
};

/**
 * When a keep-warm thread pings next and when it stops by itself, from the thread's latest
 * request (its latest shown run, or its latest ping) and the user's latest message.
 */
export function keepWarmSchedule(
  mode: KeepWarmMode,
  thread: {
    readonly latestRunAt: string | null;
    readonly latestUserMessageAt: string | null;
  },
): { readonly nextPingAt: number | null; readonly stopsAt: number } {
  const lastRequest = [epoch(thread.latestRunAt), epoch(mode.lastPingAt)].reduce<number | null>(
    (latest, time) => (time === null ? latest : latest === null ? time : Math.max(latest, time)),
    null,
  );
  const enabledAt = epoch(mode.enabledAt) ?? 0;
  const userAt = epoch(thread.latestUserMessageAt);
  return {
    nextPingAt: lastRequest === null ? null : lastRequest + KEEP_WARM_INTERVAL_MS,
    stopsAt: Math.max(enabledAt, userAt ?? 0) + KEEP_WARM_MAX_AWAY_MS,
  };
}
