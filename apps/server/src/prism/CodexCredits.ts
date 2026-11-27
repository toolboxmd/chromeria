import type {
  ServerProvider,
  ServerProviderCredits,
  ServerSettings,
  ServerSettingsError,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import type { CodexAccountLimits } from "../provider/CodexProvider.ts";
import type { ProviderDriverError } from "../provider/Errors.ts";

export const READ_BOUND = Duration.seconds(10);

type CodexRateLimits = Extract<CodexAccountLimits, { readonly route: "native" }>["rateLimits"];

/**
 * The whole rule for what one usage answer publishes about credits. Undefined means the answer
 * does not tell (no `credits`, or credits without a readable balance), so it must not hide what
 * another read told; `none` means it told there is nothing to spend.
 */
export function codexCreditsShown(
  credits: CodexRateLimits["credits"],
  readAt: string,
): ServerProviderCredits | undefined {
  if (credits == null) return undefined;
  if (credits.unlimited) return { readAt, balance: "unlimited" };
  if (!credits.hasCredits) return { readAt, balance: "none" };
  const text = credits.balance?.trim();
  const balance = text ? Math.round(Number(text)) : Number.NaN;
  if (!Number.isSafeInteger(balance)) return undefined;
  return { readAt, balance: balance >= 1 ? balance : "none" };
}

type AccountLimitsRead = Effect.Effect<CodexAccountLimits, ProviderDriverError>;

/** The account-limit read each local Codex instance registers for as long as it exists. */
export class CodexCreditReads extends Context.Reference<Map<string, AccountLimitsRead>>(
  "t3/prism/CodexCreditReads",
  { defaultValue: () => new Map() },
) {}

export const registerCodexCreditRead = Effect.fn("Prism.registerCodexCreditRead")(function* (
  instanceId: string,
  read: AccountLimitsRead,
) {
  const reads = yield* CodexCreditReads;
  yield* Effect.acquireRelease(
    Effect.sync(() => reads.set(instanceId, read)),
    () =>
      Effect.sync(() => {
        if (reads.get(instanceId) === read) reads.delete(instanceId);
      }),
  );
});

/**
 * Waits at most `READ_BOUND` for the read. The read runs on its own fiber, so its process
 * teardown, or a spawn that cannot be interrupted, never holds the decision.
 */
export const withinReadBound = Effect.fn("Prism.withinReadBound")(function* <A, E>(
  read: Effect.Effect<A, E>,
) {
  const fiber = yield* Effect.forkDetach(read);
  return yield* Fiber.await(fiber).pipe(
    Effect.timeoutOption(READ_BOUND),
    Effect.ensuring(Effect.forkDetach(Fiber.interrupt(fiber))),
  );
});

export type CodexCreditAdmission =
  | { readonly admitted: true }
  | { readonly admitted: false; readonly reason?: string };

const skipped = (reason: string): CodexCreditAdmission => ({ admitted: false, reason });

const epochIso = (seconds: number) =>
  Option.match(DateTime.make(seconds * 1000), {
    onNone: () => "an unreported time",
    onSome: DateTime.formatIso,
  });

/**
 * Why credits cannot pay for spent plan windows. The credit signals follow upstream's
 * `codexCreditBlocks` (pingdotgg/t3code#36341), read from one raw response without its merge.
 */
function creditBlock(limits: CodexRateLimits, now: number): string | undefined {
  const cap = limits.individualLimit;
  if (cap && cap.remainingPercent <= 0 && !(cap.resetsAt > 0 && cap.resetsAt * 1000 <= now))
    return `the monthly credit cap is spent until ${epochIso(cap.resetsAt)}`;
  if (limits.spendControlReached === true) return "spend control is reached";
  if (limits.rateLimitReachedType?.startsWith("workspace_"))
    return `the workspace reports ${limits.rateLimitReachedType}`;
  if (limits.credits == null) return "Codex reported no credits";
  if (!limits.credits.hasCredits && !limits.credits.unlimited) return "the account has no credits";
  return undefined;
}

/**
 * Whether a Codex instance skipped only for spent plan windows can run on credits. Anything
 * else stays skipped as before: other drivers, recorded failures, and instances without a read.
 */
export const codexCreditAdmission = Effect.fn("Prism.codexCreditAdmission")(function* (
  provider: ServerProvider,
  now: number,
  getSettings: Effect.Effect<ServerSettings, ServerSettingsError>,
) {
  const read =
    provider.driver === "codex" ? (yield* CodexCreditReads).get(provider.instanceId) : undefined;
  if (read === undefined) return { admitted: false } satisfies CodexCreditAdmission;
  const exhausted = (provider.usageLimits?.windows ?? []).filter(
    (window) =>
      window.usedPercent >= 100 &&
      (window.resetsAt === undefined || Date.parse(window.resetsAt) > now),
  );
  const cap = exhausted.find((window) => window.id === "individual");
  if (cap)
    return skipped(`the monthly credit cap is spent until ${cap.resetsAt ?? "an unreported time"}`);
  if (!exhausted.every((window) => window.id === "primary" || window.id === "secondary"))
    return { admitted: false } satisfies CodexCreditAdmission;
  const settings = yield* Effect.option(getSettings);
  if (Option.isNone(settings)) return skipped("the Use credits setting could not be read");
  if (settings.value.prismUseCredits[provider.instanceId] === false)
    return skipped("Use credits is off for this instance");
  if (provider.usageLimits?.unavailable?.reason === "unsupported")
    return skipped("its route does not spend the Codex account's own quota");

  // Every decision reads for itself: a stored answer could belong to another login.
  const exit = yield* withinReadBound(read);
  if (Option.isNone(exit))
    return skipped(`the credit read timed out after ${Duration.format(READ_BOUND)}`);
  if (Exit.isFailure(exit.value)) {
    yield* Effect.logWarning("Prism could not read Codex credits.", { cause: exit.value.cause });
    return skipped("the credit read failed");
  }
  const limits = exit.value.value;
  if (limits.route === "other")
    return skipped("its route does not spend the Codex account's own quota");
  const block = creditBlock(limits.rateLimits, now);
  return block === undefined ? ({ admitted: true } satisfies CodexCreditAdmission) : skipped(block);
});
