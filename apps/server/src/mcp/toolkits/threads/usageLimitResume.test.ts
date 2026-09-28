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

/** Runs a resume for a limit resetting at 20 minutes, against `atReset` as the thread then. */
const runResume = (usedPercent: number, atReset: OrchestrationThreadShell) =>
  Effect.gen(function* () {
    const windows = [window(usedPercent, (yield* Clock.currentTimeMillis) + 20 * MINUTE)];
    const resumed: string[] = [];
    const fiber = yield* resumeAfterUsageLimitReset(
      {
        thread: () => Effect.succeed(atReset),
        providers: Effect.succeed([provider(windows)]),
        resume: (resumedThread) => Effect.sync(() => void resumed.push(resumedThread.id)),
      },
      { threadId: "thread-user", turnId: "turn-failed", instanceId: "claudeAgent" },
    ).pipe(Effect.forkChild);
    // At the reset itself nothing is sent yet; the margin has not passed.
    yield* TestClock.adjust(20 * MINUTE);
    const beforeReset = [...resumed];
    yield* TestClock.adjust(RESUME_DELAY_MS);
    return { outcome: yield* Fiber.join(fiber), beforeReset, resumed };
  });

describe("resumeAfterUsageLimitReset", () => {
  it.effect("resumes once, a minute after the limit resets", () =>
    Effect.gen(function* () {
      const result = yield* runResume(100, thread("turn-failed"));
      assert.equal(result.outcome, "resumed");
      assert.deepEqual(result.beforeReset, []);
      assert.deepEqual(result.resumed, ["thread-user"]);
    }),
  );

  it.effect("does nothing when the provider is not out of usage", () =>
    Effect.gen(function* () {
      const result = yield* runResume(40, thread("turn-failed"));
      assert.equal(result.outcome, "not-limited");
      assert.deepEqual(result.resumed, []);
    }),
  );

  it.effect("stands down when the user started another turn or archived the thread", () =>
    Effect.gen(function* () {
      const newerTurn = yield* runResume(100, thread("turn-newer"));
      assert.equal(newerTurn.outcome, "superseded");
      assert.deepEqual(newerTurn.resumed, []);

      const archived = yield* runResume(100, thread("turn-failed", { archivedAt: iso(0) }));
      assert.equal(archived.outcome, "superseded");
      assert.deepEqual(archived.resumed, []);
    }),
  );
});
