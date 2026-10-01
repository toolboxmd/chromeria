import {
  DEFAULT_WIGHT_LIMIT_PERCENT,
  type OrchestrationThreadShell,
  type ServerProvider,
  type ServerSettings,
  type WightMode,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import { threadHasQueuedTurnStart } from "../../../orchestration/ThreadSettlementPolicy.ts";
import { isUsageLimitError } from "./usageLimitResume.ts";

/** Quota readings never predict a reset: only a fresh below-threshold reading permits work. */
export function wightPaused(provider: ServerProvider | undefined, limit: number): boolean {
  return (
    provider === undefined ||
    !provider.enabled ||
    limit === 0 ||
    (provider.usageLimits?.windows ?? []).some((window) => window.usedPercent >= limit)
  );
}

export function wightIdle(thread: OrchestrationThreadShell, now: string): boolean {
  const status = thread.session?.status;
  return (
    thread.archivedAt === null &&
    !thread.hasPendingApprovals &&
    !thread.hasPendingUserInput &&
    !thread.hasActionableProposedPlan &&
    status !== "running" &&
    status !== "starting" &&
    status !== "interrupted" &&
    status !== "stopped" &&
    (status !== "error" || isUsageLimitError(thread.session?.lastError)) &&
    thread.latestTurn?.state !== "running" &&
    !threadHasQueuedTurnStart(thread, now)
  );
}

/** New inputs already reach the provider through normal user/report/answer delivery. */
const wightContinueText = (now: string) =>
  `Continue. Current date and time: ${now}. (Wight mode.)\nConsider any new user messages, child reports and answers in this conversation since your last turn.`;

/** One serialized reconciler, driven by domain/settings/provider changes, with one deadline wake. */
export const makeWightMode = Effect.fnUntraced(function* <E, R>(deps: {
  readonly settings: Effect.Effect<ServerSettings, E, R>;
  readonly providers: Effect.Effect<ReadonlyArray<ServerProvider>, E, R>;
  readonly retired: (id: string) => Effect.Effect<boolean, E, R>;
  readonly thread: (id: string) => Effect.Effect<OrchestrationThreadShell | undefined, E, R>;
  readonly resume: (
    thread: OrchestrationThreadShell,
    text: string,
    activation: WightMode,
    admission: Effect.Effect<boolean, E, R>,
  ) => Effect.Effect<void, E, R>;
}) {
  const scope = yield* Scope.Scope;
  const lock = yield* Semaphore.make(1);
  let deadline: number | null = null;
  let timer: Fiber.Fiber<void, never> | undefined;
  const reconcile = Effect.fn("WightMode.reconcile")(function* (): Effect.fn.Return<void, E, R> {
    const settings = yield* deps.settings;
    const now = yield* Clock.currentTimeMillis;
    const nowIso = DateTime.formatIso(DateTime.makeUnsafe(now));
    const nextDeadline = Object.values(settings.wightModes)
      .flatMap((mode) => (mode.expiresAt !== null && mode.expiresAt > now ? [mode.expiresAt] : []))
      .reduce<number | null>(
        (earliest, time) => (earliest === null ? time : Math.min(earliest, time)),
        null,
      );
    if (deadline !== nextDeadline) {
      deadline = nextDeadline;
      if (timer) yield* Fiber.interrupt(timer);
      timer = undefined;
      if (nextDeadline !== null) {
        timer = yield* Effect.sleep(nextDeadline - now).pipe(
          Effect.andThen(
            Effect.sync(() => {
              deadline = null;
              timer = undefined;
            }),
          ),
          Effect.andThen(Effect.suspend(() => reconcile())),
          Effect.catchCause((cause) => Effect.logWarning("Wight timer failed", { cause })),
          Effect.forkIn(scope),
        );
      }
    }
    if (Object.keys(settings.wightModes).length === 0) return;
    const providers = yield* deps.providers;
    for (const [id, activation] of Object.entries(settings.wightModes)) {
      if (activation.expiresAt !== null && activation.expiresAt <= now) continue;
      if (yield* deps.retired(id)) continue;
      const thread = yield* deps.thread(id);
      if (!thread || !wightIdle(thread, nowIso)) continue;
      const instance = thread.session?.providerInstanceId ?? thread.modelSelection.instanceId;
      const provider = providers.find((entry) => entry.instanceId === instance);
      const limit =
        settings.providerInstances[instance]?.wightLimitPercent ?? DEFAULT_WIGHT_LIMIT_PERCENT;
      if (wightPaused(provider, limit)) continue;
      // Inputs may have changed while reading the shell; the dispatch also compares the live thread.
      const currentSettings = yield* deps.settings;
      const current = currentSettings.wightModes[thread.id];
      const sendAt = yield* Clock.currentTimeMillis;
      if (
        !current ||
        current.enabledAt !== activation.enabledAt ||
        (current.expiresAt !== null && current.expiresAt <= sendAt)
      )
        continue;
      const freshProvider = (yield* deps.providers).find((entry) => entry.instanceId === instance);
      if (
        wightPaused(
          freshProvider,
          currentSettings.providerInstances[instance]?.wightLimitPercent ??
            DEFAULT_WIGHT_LIMIT_PERCENT,
        )
      )
        continue;
      yield* deps.resume(
        thread,
        wightContinueText(DateTime.formatIso(DateTime.makeUnsafe(sendAt))),
        activation,
        Effect.gen(function* () {
          const current = yield* deps.settings;
          const active = current.wightModes[thread.id];
          if (
            !active ||
            active.enabledAt !== activation.enabledAt ||
            (yield* deps.retired(thread.id))
          )
            return false;
          const fresh = (yield* deps.providers).find((entry) => entry.instanceId === instance);
          const now = yield* Clock.currentTimeMillis;
          return (
            (active.expiresAt === null || active.expiresAt > now) &&
            !wightPaused(
              fresh,
              current.providerInstances[instance]?.wightLimitPercent ?? DEFAULT_WIGHT_LIMIT_PERCENT,
            )
          );
        }),
      );
    }
  }, lock.withPermits(1));
  return { reconcile };
});
