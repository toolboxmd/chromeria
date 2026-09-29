/**
 * Resume after a usage limit (toolboxmd/chromeria#61, #71): a thread whose
 * turn fails while its provider is out of usage gets one "continue" once the
 * limit lifts. That is a minute after the displayed reset, or earlier when
 * another thread on the same provider instance gets a reply, since the
 * displayed reset can be hours pessimistic. Prism job threads are left alone:
 * Model Router reroutes or blocks them itself, and a late "continue" to an old
 * worker would duplicate work. Upstream Orchestrator V2 has its own "Resume
 * at reset", so this goes away with the V2 port.
 */
import type { OrchestrationThreadShell, ServerProvider } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";

export const RESUME_TEXT = "Continue. (Sent automatically after the usage limit reset.)";

/**
 * Model Router opens every job thread with a first user message
 * `[model-router job <request id> <kind> on route <route>; planner thread <id>]`
 * (parsed in full by apps/web/src/components/AgentThreadTree.logic.ts).
 * Titles are no marker: they get regenerated.
 */
export function isRouterJobMessage(firstUserMessage: string): boolean {
  return firstUserMessage.trimStart().startsWith("[model-router job ");
}

/** Lets the provider's usage windows catch up with the rate-limit event that ended the turn. */
const SETTLE_DELAY_MS = 5_000;

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
  /**
   * Whether another thread's reply on the instance may resume early. False
   * after an early resume failed again, so a limit that only this thread's
   * model hits cannot turn every reply elsewhere into a retry.
   */
  readonly resumeEarly: boolean;
}

/** Why the thread was resumed: the displayed reset passed, or a reply proved the limit lifted. */
type ResumeTrigger = "reset" | "lifted";

/** The one line a child's parent gets when the child is resumed. */
export function resumeNotice(
  child: Pick<OrchestrationThreadShell, "id" | "title">,
  trigger: ResumeTrigger,
): string {
  const why =
    trigger === "reset"
      ? "its usage limit reset"
      : "another thread got a reply from the same provider, so its usage limit has lifted";
  return `[Subagent ${child.title} (thread ${child.id}) resumed automatically: ${why}.]`;
}

/**
 * Waits for the usage limit behind a failed turn to lift, then resumes the
 * thread once, unless someone started another turn or archived it first.
 */
export const resumeAfterUsageLimitReset = <E, R>(
  deps: {
    readonly thread: (threadId: string) => Effect.Effect<OrchestrationThreadShell | undefined>;
    readonly providers: Effect.Effect<ReadonlyArray<ServerProvider>>;
    /** Completes on the next reply any thread gets from the instance. */
    readonly nextReplyOn: (instanceId: string) => Effect.Effect<void>;
    readonly resume: (
      thread: OrchestrationThreadShell,
      trigger: ResumeTrigger,
    ) => Effect.Effect<void, E, R>;
  },
  input: UsageLimitResumeInput,
) =>
  Effect.gen(function* () {
    yield* Effect.sleep(SETTLE_DELAY_MS);
    const providers = yield* deps.providers;
    const provider = providers.find((candidate) => candidate.instanceId === input.instanceId);
    const resetAt = usageLimitResetAt(provider, yield* Clock.currentTimeMillis);
    if (resetAt === null) return "not-limited" as const;
    const untilReset = Effect.sleep(resetAt + RESUME_DELAY_MS - (yield* Clock.currentTimeMillis));
    const trigger: ResumeTrigger = input.resumeEarly
      ? yield* Effect.raceFirst(
          untilReset.pipe(Effect.as("reset" as const)),
          deps.nextReplyOn(input.instanceId).pipe(Effect.as("lifted" as const)),
        )
      : yield* untilReset.pipe(Effect.as("reset" as const));
    const thread = yield* deps.thread(input.threadId);
    if (!thread || thread.archivedAt !== null || thread.latestTurn?.turnId !== input.turnId) {
      return "superseded" as const;
    }
    yield* deps.resume(thread, trigger);
    return trigger;
  });
