import {
  isKeepWarmPingOf,
  keepWarmSchedule,
  supportsKeepWarm,
  ThreadId,
  type KeepWarmMode,
  type KeepWarmStopReason,
  type OrchestrationV2ThreadShell,
  type ServerProvider,
  type ServerSettings,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import { wightIdle, type WightThread } from "../wight/wightMode.ts";

export type KeepWarmThread = WightThread &
  Pick<OrchestrationV2ThreadShell, "settledOverride"> & {
    /** When the latest-started request started, pings and other runs the shell hides included. */
    readonly latestRunAt: string | null;
    /** When the shell's presented run started; clients schedule from it. */
    readonly shownRunAt: string | null;
    readonly latestUserMessageAt: string | null;
    /** The latest-started run's prompt, and its failure class when it failed. */
    readonly latestRunPromptId: string | null;
    readonly latestRunFailureClass: string | null;
  };

/** A ping the provider refused is retried after this, under the same command id. */
const PING_RETRY_MS = 60_000;

const usageLimitReached = (provider: ServerProvider | undefined): boolean =>
  (provider?.usageLimits?.windows ?? []).some((window) => window.usedPercent >= 100);

/** A ping needs a known, enabled provider with quota left. */
export const keepWarmPaused = (provider: ServerProvider | undefined): boolean =>
  provider === undefined || !provider.enabled || usageLimitReached(provider);

export type KeepWarmDecision =
  | { readonly type: "remove" }
  | { readonly type: "stop"; readonly reason: KeepWarmStopReason }
  | { readonly type: "wait"; readonly until: number | null }
  | { readonly type: "ping"; readonly dueMs: number };

export function decideKeepWarm(input: {
  readonly mode: KeepWarmMode;
  readonly thread: KeepWarmThread | undefined;
  readonly retired: boolean;
  readonly provider: ServerProvider | undefined;
  readonly now: number;
}): KeepWarmDecision {
  const { mode, thread, provider, now } = input;
  if (thread === undefined || thread.deletedAt !== null) return { type: "remove" };
  if (thread.archivedAt !== null) return { type: "stop", reason: "archived" };
  if (input.retired || thread.settledOverride === "settled")
    return { type: "stop", reason: "settled" };
  // Providers load after startup; an unknown instance is not yet a reason to stop.
  if (provider !== undefined && !supportsKeepWarm(provider.driver))
    return { type: "stop", reason: "unsupportedProvider" };
  if (
    thread.limitRecovery != null ||
    thread.lastErrorClass === "usage_limit" ||
    // Only this activation's ping: an earlier one's failure must not stop a fresh activation.
    (thread.latestRunFailureClass === "usage_limit" &&
      thread.latestRunPromptId !== null &&
      isKeepWarmPingOf(thread.latestRunPromptId, thread.id, mode)) ||
    usageLimitReached(provider)
  )
    return { type: "stop", reason: "usageLimit" };
  const { nextPingAt, stopsAt } = keepWarmSchedule(mode, thread);
  if (now >= stopsAt) return { type: "stop", reason: "userAway" };
  if (nextPingAt === null || nextPingAt >= stopsAt) return { type: "wait", until: stopsAt };
  if (now < nextPingAt) return { type: "wait", until: nextPingAt };
  // Run, queue and provider changes wake the reconciler again.
  if (!wightIdle(thread) || keepWarmPaused(provider)) return { type: "wait", until: stopsAt };
  return { type: "ping", dueMs: nextPingAt };
}

const keepWarmActivations = (settings: ServerSettings) =>
  Object.entries(settings.threadIdleModes).flatMap(([id, mode]) =>
    mode.type === "keepWarm" && mode.stopped === null ? [[ThreadId.make(id), mode] as const] : [],
  );

/** One serialized reconciler, driven by domain/settings/provider changes, with one deadline wake. */
export const makeKeepWarm = Effect.fnUntraced(function* <E, R>(deps: {
  readonly settings: Effect.Effect<ServerSettings, E, R>;
  readonly providers: Effect.Effect<ReadonlyArray<ServerProvider>, E, R>;
  readonly retired: (id: ThreadId) => Effect.Effect<boolean, E, R>;
  readonly thread: (id: ThreadId) => Effect.Effect<KeepWarmThread | undefined, E, R>;
  /** Writes `stopped`, or removes the entry when `reason` is null, if `mode` is still current. */
  readonly end: (
    id: ThreadId,
    mode: KeepWarmMode,
    reason: KeepWarmStopReason | null,
  ) => Effect.Effect<void, E, R>;
  /** Replaces `mode` with `next` if `mode` is still current. */
  readonly record: (
    id: ThreadId,
    mode: KeepWarmMode,
    next: KeepWarmMode,
  ) => Effect.Effect<void, E, R>;
  readonly ping: (
    thread: KeepWarmThread,
    mode: KeepWarmMode,
    dueMs: number,
  ) => Effect.Effect<void, E, R>;
}) {
  const scope = yield* Scope.Scope;
  const lock = yield* Semaphore.make(1);
  let deadline: number | null = null;
  let timer: Fiber.Fiber<void, never> | undefined;
  const reconcile = Effect.fn("KeepWarm.reconcile")(function* (): Effect.fn.Return<void, E, R> {
    const activations = keepWarmActivations(yield* deps.settings);
    const providers = activations.length === 0 ? [] : yield* deps.providers;
    let nextDeadline: number | null = null;
    for (const [id, activation] of activations) {
      const thread = yield* deps.thread(id);
      let mode = activation;
      // Clients schedule from the shell's presented run, which hides pings and idle compactions
      // and can be older than a queued turn that started later: record the later start.
      const hidden = thread?.latestRunAt ?? null;
      if (
        hidden !== null &&
        hidden > (thread?.shownRunAt ?? "") &&
        hidden > (mode.hiddenRequestAt ?? "")
      ) {
        mode = { ...mode, hiddenRequestAt: hidden };
        yield* deps.record(id, activation, mode);
      }
      const now = yield* Clock.currentTimeMillis;
      const decision = decideKeepWarm({
        mode,
        thread,
        retired: thread === undefined ? false : yield* deps.retired(id),
        provider: providers.find((entry) => entry.instanceId === thread?.providerInstanceId),
        now,
      });
      let wake: number | null = null;
      if (decision.type === "remove") yield* deps.end(id, mode, null);
      else if (decision.type === "stop") yield* deps.end(id, mode, decision.reason);
      else if (decision.type === "wait") wake = decision.until;
      else if (thread !== undefined) {
        // The ping's run is a new hidden request, recorded when its events reconcile again.
        yield* deps.ping(thread, mode, decision.dueMs);
        wake = now + PING_RETRY_MS;
      }
      if (wake !== null) nextDeadline = nextDeadline === null ? wake : Math.min(nextDeadline, wake);
    }
    if (deadline === nextDeadline) return;
    deadline = nextDeadline;
    if (timer) yield* Fiber.interrupt(timer);
    timer = undefined;
    if (nextDeadline === null) return;
    const now = yield* Clock.currentTimeMillis;
    timer = yield* Effect.sleep(Math.max(0, nextDeadline - now)).pipe(
      Effect.andThen(
        Effect.sync(() => {
          deadline = null;
          timer = undefined;
        }),
      ),
      Effect.andThen(Effect.suspend(() => reconcile())),
      Effect.catchCause((cause) => Effect.logWarning("Keep-warm timer failed", { cause })),
      Effect.forkIn(scope),
    );
  }, lock.withPermits(1));
  return { reconcile };
});
