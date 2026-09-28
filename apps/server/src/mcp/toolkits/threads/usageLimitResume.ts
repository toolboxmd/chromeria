/**
 * Resume after a usage limit (toolboxmd/chromeria#61): a thread the user
 * started whose turn fails while its provider is out of usage gets one
 * "continue" shortly after the limit resets. Child threads are left alone:
 * Model Router reroutes or blocks Prism jobs itself, and a late "continue" to
 * an old worker would duplicate work. Upstream Orchestrator V2 has its own
 * "Resume at reset", so this goes away with the V2 port.
 */
import type { OrchestrationThreadShell, ServerProvider } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";

export const RESUME_TEXT = "Continue. (Sent automatically after the usage limit reset.)";

/** Lets the provider's usage windows catch up with the rate-limit event that ended the turn. */
export const SETTLE_DELAY_MS = 5_000;

/** Margin after the reset so the first resumed request is not refused again. */
export const RESUME_DELAY_MS = 60_000;

/**
 * The latest reset among the provider's exhausted usage windows, in epoch
 * milliseconds, or null when nothing is exhausted or no reset time is known.
 */
export function usageLimitResetAt(
  provider: Pick<ServerProvider, "usageLimits"> | undefined,
  nowMs: number,
): number | null {
  let latest: number | null = null;
  for (const window of provider?.usageLimits?.windows ?? []) {
    if (window.usedPercent < 100 || window.resetsAt === undefined) continue;
    const resetMs = Date.parse(window.resetsAt);
    if (!Number.isFinite(resetMs) || resetMs <= nowMs) continue;
    latest = latest === null ? resetMs : Math.max(latest, resetMs);
  }
  return latest;
}

export interface UsageLimitResumeInput {
  readonly threadId: string;
  /** The turn that failed; any later turn cancels the resume. */
  readonly turnId: string;
  readonly instanceId: string;
}

/**
 * Waits for the usage limit behind a failed turn to reset, then resumes the
 * thread once, unless the user started another turn or archived it first.
 */
export const resumeAfterUsageLimitReset = <E, R>(
  deps: {
    readonly thread: (threadId: string) => Effect.Effect<OrchestrationThreadShell | undefined>;
    readonly providers: Effect.Effect<ReadonlyArray<ServerProvider>>;
    readonly resume: (thread: OrchestrationThreadShell) => Effect.Effect<void, E, R>;
  },
  input: UsageLimitResumeInput,
) =>
  Effect.gen(function* () {
    yield* Effect.sleep(SETTLE_DELAY_MS);
    const providers = yield* deps.providers;
    const provider = providers.find((candidate) => candidate.instanceId === input.instanceId);
    const resetAt = usageLimitResetAt(provider, yield* Clock.currentTimeMillis);
    if (resetAt === null) return "not-limited" as const;
    yield* Effect.sleep(resetAt + RESUME_DELAY_MS - (yield* Clock.currentTimeMillis));
    const thread = yield* deps.thread(input.threadId);
    if (!thread || thread.archivedAt !== null || thread.latestTurn?.turnId !== input.turnId) {
      return "superseded" as const;
    }
    yield* deps.resume(thread);
    return "resumed" as const;
  });
