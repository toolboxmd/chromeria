import {
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type OrchestrationThreadShell,
  type ServerProvider,
  type ServerProviderUsageWindow,
} from "@t3tools/contracts";
import { assert, describe, it } from "@effect/vitest";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as TestClock from "effect/testing/TestClock";

import {
  isUsageLimitError,
  RESET_RECHECK_MS,
  RESUME_DELAY_MS,
  resumeAfterUsageLimitReset,
  usageLimitResetAt,
} from "./usageLimitResume.ts";

const MINUTE = 60_000;
const iso = (ms: number) => DateTime.formatIso(DateTime.makeUnsafe(ms));

const window = (usedPercent: number, resetsAt?: number): ServerProviderUsageWindow => ({
  id: `window-${usedPercent}-${resetsAt}`,
  kind: "session",
  label: "Session",
  usedPercent,
  ...(resetsAt === undefined ? {} : { resetsAt: iso(resetsAt) }),
});

const provider = (windows: ReadonlyArray<ServerProviderUsageWindow>) =>
  ({
    instanceId: ProviderInstanceId.make("claudeAgent"),
    usageLimits: { checkedAt: iso(0), windows },
  }) as unknown as ServerProvider;

const thread = (
  turnId: string,
  overrides: Partial<OrchestrationThreadShell> = {},
): OrchestrationThreadShell => ({
  id: ThreadId.make("thread-user"),
  projectId: ProjectId.make("project-1"),
  title: "Thread",
  modelSelection: { instanceId: ProviderInstanceId.make("claudeAgent"), model: "claude-opus" },
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  pullRequests: [],
  coOwners: [],
  latestTurn: {
    turnId: TurnId.make(turnId),
    state: "error",
    requestedAt: iso(0),
    startedAt: iso(0),
    completedAt: iso(0),
    assistantMessageId: null,
  },
  createdAt: iso(0),
  updatedAt: iso(0),
  archivedAt: null,
  settledOverride: null,
  settledAt: null,
  session: null,
  latestUserMessageAt: null,
  hasPendingApprovals: false,
  hasPendingUserInput: false,
  hasActionableProposedPlan: false,
  ...overrides,
});

describe("usageLimitResetAt", () => {
  it("returns the latest reset among exhausted windows only", () => {
    const now = 0;
    const resetAt = usageLimitResetAt(
      provider([window(100, 10 * MINUTE), window(100, 30 * MINUTE), window(80, 90 * MINUTE)]),
      now,
    );
    assert.equal(resetAt, 30 * MINUTE);
  });

  it("returns null without an exhausted window with a future reset", () => {
    assert.isNull(usageLimitResetAt(provider([window(99, 10 * MINUTE)]), 0));
    assert.isNull(usageLimitResetAt(provider([window(100)]), 0));
    assert.isNull(usageLimitResetAt(provider([window(100, 10 * MINUTE)]), 10 * MINUTE));
    assert.isNull(usageLimitResetAt(undefined, 0));
  });
});

/** Replies other threads get from the instance, at these clock times; waits for the next one. */
const nextReplyAmong = (replyTimes: ReadonlyArray<number>) => () =>
  Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;
    const next = replyTimes.find((time) => time > now);
    return yield* next === undefined ? Effect.never : Effect.sleep(next - now);
  });

interface ResumeCase {
  readonly usedPercent: number;
  /** Whether the usage window shows the limit lifted once it resets (default true). */
  readonly refreshedAtReset?: boolean;
  readonly atReset: OrchestrationThreadShell;
  /** Minutes after the failure at which other threads get replies from the instance. */
  readonly repliesAt?: ReadonlyArray<number>;
  readonly resumeEarly?: boolean;
}

/** Runs a resume for a limit resetting at 20 minutes and reports when it resumed. */
const runResume = ({
  usedPercent,
  refreshedAtReset = true,
  atReset,
  repliesAt = [],
  resumeEarly = true,
}: ResumeCase) =>
  Effect.gen(function* () {
    const failedAt = yield* Clock.currentTimeMillis;
    const resetsAt = failedAt + 20 * MINUTE;
    const windows = Effect.map(Clock.currentTimeMillis, (now) =>
      refreshedAtReset && now >= resetsAt
        ? [window(10, resetsAt + 5 * 60 * MINUTE)]
        : [window(usedPercent, resetsAt)],
    );
    const resumed: Array<{ threadId: string; trigger: string; afterMs: number }> = [];
    const fiber = yield* resumeAfterUsageLimitReset(
      {
        thread: () => Effect.succeed(atReset),
        providers: Effect.map(windows, (current) => [provider(current)]),
        nextReplyOn: nextReplyAmong(repliesAt.map((minutes) => failedAt + minutes * MINUTE)),
        resume: (resumedThread, trigger) =>
          Effect.gen(function* () {
            const afterMs = (yield* Clock.currentTimeMillis) - failedAt;
            resumed.push({ threadId: resumedThread.id, trigger, afterMs });
          }),
      },
      { threadId: "thread-user", turnId: "turn-failed", instanceId: "claudeAgent", resumeEarly },
    ).pipe(Effect.forkChild);
    // At the reset itself nothing is sent yet; the margin has not passed.
    yield* TestClock.adjust(20 * MINUTE);
    const beforeReset = resumed.filter((entry) => entry.trigger === "reset");
    yield* TestClock.adjust(RESUME_DELAY_MS);
    return { fiber, beforeReset, resumed };
  });

/** runResume, then the watcher's outcome. */
const resumeOutcome = (input: ResumeCase) =>
  Effect.gen(function* () {
    const { fiber, beforeReset, resumed } = yield* runResume(input);
    return { outcome: yield* Fiber.join(fiber), beforeReset, resumed };
  });

describe("resumeAfterUsageLimitReset", () => {
  it.effect("resumes once, a minute after the limit resets", () =>
    Effect.gen(function* () {
      const result = yield* resumeOutcome({ usedPercent: 100, atReset: thread("turn-failed") });
      assert.equal(result.outcome, "reset");
      assert.deepEqual(result.beforeReset, []);
      assert.deepEqual(result.resumed, [
        { threadId: "thread-user", trigger: "reset", afterMs: 21 * MINUTE },
      ]);
    }),
  );

  it.effect("sends nothing at the reset while the window still shows exhausted", () =>
    Effect.gen(function* () {
      const { fiber, resumed } = yield* runResume({
        usedPercent: 100,
        refreshedAtReset: false,
        atReset: thread("turn-failed"),
      });
      // A minute past the reset the reading still shows 100 %: no continue.
      yield* TestClock.adjust(3 * RESET_RECHECK_MS);
      assert.deepEqual(resumed, []);
      yield* Fiber.interrupt(fiber);
    }),
  );

  it.effect("resumes at the first reply on the instance when it comes before the reset", () =>
    Effect.gen(function* () {
      const result = yield* resumeOutcome({
        usedPercent: 100,
        atReset: thread("turn-failed"),
        repliesAt: [3, 7],
      });
      assert.equal(result.outcome, "lifted");
      assert.deepEqual(result.resumed, [
        { threadId: "thread-user", trigger: "lifted", afterMs: 3 * MINUTE },
      ]);
    }),
  );

  it.effect("waits for the reset once an early resume is spent", () =>
    Effect.gen(function* () {
      const result = yield* resumeOutcome({
        usedPercent: 100,
        atReset: thread("turn-failed"),
        repliesAt: [3],
        resumeEarly: false,
      });
      assert.equal(result.outcome, "reset");
      assert.deepEqual(result.resumed, [
        { threadId: "thread-user", trigger: "reset", afterMs: 21 * MINUTE },
      ]);
    }),
  );

  it.effect("does nothing when the provider is not out of usage", () =>
    Effect.gen(function* () {
      const result = yield* resumeOutcome({
        usedPercent: 40,
        atReset: thread("turn-failed"),
        repliesAt: [3],
      });
      assert.equal(result.outcome, "not-limited");
      assert.deepEqual(result.resumed, []);
    }),
  );

  it.effect("stands down when someone started another turn or archived the thread", () =>
    Effect.gen(function* () {
      const newerTurn = yield* resumeOutcome({ usedPercent: 100, atReset: thread("turn-newer") });
      assert.equal(newerTurn.outcome, "superseded");
      assert.deepEqual(newerTurn.resumed, []);

      const newerBeforeReply = yield* resumeOutcome({
        usedPercent: 100,
        atReset: thread("turn-newer"),
        repliesAt: [3],
      });
      assert.equal(newerBeforeReply.outcome, "superseded");
      assert.deepEqual(newerBeforeReply.resumed, []);

      const archived = yield* resumeOutcome({
        usedPercent: 100,
        atReset: thread("turn-failed", { archivedAt: iso(0) }),
      });
      assert.equal(archived.outcome, "superseded");
      assert.deepEqual(archived.resumed, []);
    }),
  );

  // toolboxmd/chromeria#71: the child failed at 13:10:29Z with a reset shown
  // for 15:40Z, while its parent on the same instance got replies at
  // 13:10:30Z (inside the settle delay, so possibly already in flight) and
  // 13:10:41Z. The user relayed "continue" at 13:14:21Z.
  it.effect("replays 2026-09-29: resumes at the 13:10:41Z reply, not at 15:41Z", () =>
    Effect.gen(function* () {
      const at = (time: string) => Date.parse(`2026-09-29T${time}Z`);
      yield* TestClock.setTime(at("13:10:29.552"));
      const resumedAt: number[] = [];
      const fiber = yield* resumeAfterUsageLimitReset(
        {
          thread: () => Effect.succeed(thread("turn-failed")),
          providers: Effect.succeed([provider([window(100, at("15:40:00"))])]),
          nextReplyOn: nextReplyAmong([at("13:10:30.490"), at("13:10:41.675")]),
          resume: () =>
            Effect.gen(function* () {
              resumedAt.push(yield* Clock.currentTimeMillis);
            }),
        },
        {
          threadId: "thread-user",
          turnId: "turn-failed",
          instanceId: "claudeAgent",
          resumeEarly: true,
        },
      ).pipe(Effect.forkChild);
      yield* TestClock.setTime(at("13:14:21"));
      assert.equal(yield* Fiber.join(fiber), "lifted");
      assert.deepEqual(resumedAt, [at("13:10:41.675")]);
    }),
  );
});

describe("resumeAfterUsageLimitReset with a reset time already past", () => {
  /** A watcher whose exhausted window claims a reset 10 minutes before the failure. */
  const watchStaleReset = (input: {
    readonly repliesAt?: ReadonlyArray<number>;
    readonly resumeEarly: boolean;
  }) =>
    Effect.gen(function* () {
      const failedAt = yield* Clock.currentTimeMillis;
      let windows = [window(100, failedAt - 10 * MINUTE)];
      const resumed: Array<{ trigger: string; afterMs: number }> = [];
      const fiber = yield* resumeAfterUsageLimitReset(
        {
          thread: () => Effect.succeed(thread("turn-failed")),
          providers: Effect.sync(() => [provider(windows)]),
          nextReplyOn: nextReplyAmong(
            (input.repliesAt ?? []).map((minutes) => failedAt + minutes * MINUTE),
          ),
          resume: (_thread, trigger) =>
            Effect.gen(function* () {
              resumed.push({ trigger, afterMs: (yield* Clock.currentTimeMillis) - failedAt });
            }),
        },
        {
          threadId: "thread-user",
          turnId: "turn-failed",
          instanceId: "claudeAgent",
          resumeEarly: input.resumeEarly,
        },
      ).pipe(Effect.forkChild);
      const liftLimit = () => {
        windows = [window(20, failedAt + 5 * 60 * MINUTE)];
      };
      return { fiber, resumed, liftLimit };
    });

  it.effect("keeps watching and resumes at the next reply on the instance", () =>
    Effect.gen(function* () {
      const { fiber, resumed } = yield* watchStaleReset({ repliesAt: [3], resumeEarly: true });
      yield* TestClock.adjust(3 * MINUTE);
      assert.equal(yield* Fiber.join(fiber), "lifted");
      assert.deepEqual(resumed, [{ trigger: "lifted", afterMs: 3 * MINUTE }]);
    }),
  );

  it.effect(
    "sends nothing while the window still shows exhausted, then resumes once it lifts",
    () =>
      Effect.gen(function* () {
        const { fiber, resumed, liftLimit } = yield* watchStaleReset({
          repliesAt: [3, 30],
          resumeEarly: false,
        });
        // An early resume is spent, so replies elsewhere do not count, and the
        // stale reading never resumes on its own.
        yield* TestClock.adjust(2 * 60 * MINUTE);
        assert.deepEqual(resumed, []);
        liftLimit();
        yield* TestClock.adjust(RESET_RECHECK_MS);
        assert.equal(yield* Fiber.join(fiber), "reset");
        assert.equal(resumed.length, 1);
      }),
  );
});

describe("isUsageLimitError", () => {
  it("accepts the adapters' usage-limit errors and nothing else", () => {
    for (const message of [
      "Claude usage limit reached. Send the message again once the limit resets.",
      "Claude stopped: a usage limit blocked the request.",
      "Codex usage limit reached. The session limit resets in 2h.",
      "Grok usage limit reached. Try again later.",
    ]) {
      assert.isTrue(isUsageLimitError(message), message);
    }
    for (const message of [
      "Provider process exited with code 1.",
      "Claude could not start the turn.",
      null,
      undefined,
    ]) {
      assert.isFalse(isUsageLimitError(message), String(message));
    }
  });
});
