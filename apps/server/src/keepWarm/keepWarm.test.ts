import { describe, expect, it } from "@effect/vitest";
import {
  DEFAULT_SERVER_SETTINGS,
  KEEP_WARM_INTERVAL_MS,
  KEEP_WARM_MAX_AWAY_MS,
  keepWarmPingMessageId,
  ProviderDriverKind,
  ProviderInstanceId,
  RunId,
  ThreadId,
  type KeepWarmMode,
  type ServerProvider,
  type ServerSettings,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as TestClock from "effect/testing/TestClock";
import { decideKeepWarm, makeKeepWarm, type KeepWarmThread } from "./keepWarm.ts";

const INSTANCE = ProviderInstanceId.make("claudeAgent");
const ID = ThreadId.make("keep-warm-test");
const MINUTE = 60_000;
const HOUR = 60 * MINUTE;
const iso = (ms: number) => DateTime.formatIso(DateTime.makeUnsafe(ms));

const thread = (patch: Partial<KeepWarmThread> = {}): KeepWarmThread => ({
  id: ID,
  providerInstanceId: INSTANCE,
  modelSelection: { instanceId: INSTANCE, model: "claude-haiku-4-5" },
  runtimeMode: "full-access",
  interactionMode: "default",
  status: "completed",
  activeRunId: null,
  latestRunId: RunId.make("run-1"),
  updatedAt: DateTime.makeUnsafe(0),
  archivedAt: null,
  deletedAt: null,
  settledOverride: null,
  pendingRuntimeRequest: null,
  hasActionableProposedPlan: false,
  limitRecovery: null,
  hasPendingRuns: false,
  latestRunAt: iso(0),
  shownRunAt: iso(0),
  latestUserMessageAt: iso(0),
  latestRunPromptId: null,
  latestRunFailureClass: null,
  lastErrorClass: null,
  ...patch,
});
const provider = (usedPercent: number, driver = "claudeAgent") =>
  ({
    instanceId: INSTANCE,
    driver: ProviderDriverKind.make(driver),
    enabled: true,
    usageLimits: {
      checkedAt: iso(0),
      windows: [{ id: "session", kind: "session", label: "Session", usedPercent }],
    },
  }) as unknown as ServerProvider;
const warm = (patch: Partial<KeepWarmMode> = {}): KeepWarmMode => ({
  type: "keepWarm",
  enabledAt: iso(0),
  hiddenRequestAt: null,
  stopped: null,
  ...patch,
});
const withMode = (mode: ServerSettings["threadIdleModes"][ThreadId]): ServerSettings => ({
  ...DEFAULT_SERVER_SETTINGS,
  threadIdleModes: { [ID]: mode },
});

/** A sent ping becomes the thread's latest hidden request; the shell still shows it idle. */
const harness = (initial: ServerSettings, initialThread = thread()) =>
  Effect.gen(function* () {
    let settings = initial;
    let providers = [provider(20)];
    let current: KeepWarmThread | undefined = initialThread;
    const pings: Array<{ dueMs: number; messageId: string }> = [];
    const write = (mode: KeepWarmMode | null) => {
      const { [ID]: _removed, ...rest } = settings.threadIdleModes;
      settings = { ...settings, threadIdleModes: mode === null ? rest : { ...rest, [ID]: mode } };
    };
    const runtime = yield* makeKeepWarm({
      settings: Effect.sync(() => settings),
      providers: Effect.sync(() => providers),
      retired: () => Effect.succeed(false),
      thread: () => Effect.sync(() => current),
      end: (_id, mode, reason) =>
        Clock.currentTimeMillis.pipe(
          Effect.map((now) =>
            write(reason === null ? null : { ...mode, stopped: { at: iso(now), reason } }),
          ),
        ),
      record: (_id, _mode, next) => Effect.sync(() => write(next)),
      ping: (_thread, mode, dueMs) =>
        Clock.currentTimeMillis.pipe(
          Effect.map((now) => {
            const messageId = keepWarmPingMessageId(ID, mode, dueMs);
            pings.push({ dueMs, messageId });
            current =
              current === undefined
                ? undefined
                : { ...current, latestRunAt: iso(now), latestRunPromptId: messageId };
          }),
        ),
    });
    return {
      ...runtime,
      pings,
      mode: () => settings.threadIdleModes[ID],
      thread: (patch: Partial<KeepWarmThread>) => {
        current = current === undefined ? undefined : { ...current, ...patch };
      },
      providers: (next: ServerProvider[]) => {
        providers = next;
      },
    };
  });

describe("Keep cache warm", () => {
  it.effect("pings 55 minutes after the latest request, once per due time", () =>
    Effect.gen(function* () {
      const h = yield* harness(withMode(warm()));
      yield* h.reconcile();
      yield* TestClock.adjust(KEEP_WARM_INTERVAL_MS - 1);
      expect(h.pings).toHaveLength(0);
      yield* TestClock.adjust(1);
      yield* Effect.all([h.reconcile(), h.reconcile()], { concurrency: "unbounded" });
      expect(h.pings.map((ping) => ping.dueMs)).toEqual([KEEP_WARM_INTERVAL_MS]);
      yield* TestClock.adjust(KEEP_WARM_INTERVAL_MS);
      expect(h.pings.map((ping) => ping.dueMs)).toEqual([
        KEEP_WARM_INTERVAL_MS,
        2 * KEEP_WARM_INTERVAL_MS,
      ]);
    }).pipe(Effect.scoped),
  );

  it.effect("waits while a run is active or a message is queued", () =>
    Effect.gen(function* () {
      const h = yield* harness(withMode(warm()), thread({ status: "running", activeRunId: null }));
      yield* TestClock.adjust(KEEP_WARM_INTERVAL_MS);
      yield* h.reconcile();
      h.thread({ status: "completed", hasPendingRuns: true });
      yield* h.reconcile();
      expect(h.pings).toHaveLength(0);
      h.thread({ hasPendingRuns: false });
      yield* h.reconcile();
      expect(h.pings).toHaveLength(1);
    }).pipe(Effect.scoped),
  );

  it.effect("stops 24 hours after the user's latest message; a new message restarts them", () =>
    Effect.gen(function* () {
      const h = yield* harness(withMode(warm()));
      yield* h.reconcile();
      yield* TestClock.adjust(20 * HOUR);
      h.thread({ latestUserMessageAt: iso(20 * HOUR), latestRunAt: iso(20 * HOUR) });
      yield* h.reconcile();
      yield* TestClock.adjust(4 * HOUR);
      expect(h.mode()).toMatchObject({ stopped: null });
      yield* TestClock.adjust(20 * HOUR);
      expect(h.mode()).toMatchObject({
        stopped: { at: iso(20 * HOUR + KEEP_WARM_MAX_AWAY_MS), reason: "userAway" },
      });
      const sent = h.pings.length;
      yield* TestClock.adjust(2 * HOUR);
      expect(h.pings).toHaveLength(sent);
    }).pipe(Effect.scoped),
  );

  it.effect("stops when the provider reports a usage limit", () =>
    Effect.gen(function* () {
      const h = yield* harness(withMode(warm()));
      h.providers([provider(100)]);
      yield* TestClock.adjust(KEEP_WARM_INTERVAL_MS);
      yield* h.reconcile();
      expect(h.pings).toHaveLength(0);
      expect(h.mode()).toMatchObject({ stopped: { reason: "usageLimit" } });
    }).pipe(Effect.scoped),
  );

  it.effect("never pings a Wight thread", () =>
    Effect.gen(function* () {
      const h = yield* harness(withMode({ type: "wight", enabledAt: iso(0), expiresAt: null }));
      yield* h.reconcile();
      yield* TestClock.adjust(3 * HOUR);
      yield* h.reconcile();
      expect(h.pings).toHaveLength(0);
    }).pipe(Effect.scoped),
  );

  it.effect("after a restart, a ping due while the server was down goes out once", () =>
    Effect.gen(function* () {
      const saved = withMode(warm({ hiddenRequestAt: iso(KEEP_WARM_INTERVAL_MS) }));
      yield* TestClock.adjust(4 * HOUR);
      const first = yield* harness(saved);
      yield* Effect.all([first.reconcile(), first.reconcile()], { concurrency: "unbounded" });
      // A second start before the first recorded its ping reuses the same command identity.
      const second = yield* harness(saved);
      yield* second.reconcile();
      expect(first.pings).toEqual(second.pings);
      expect(first.pings).toEqual([
        {
          dueMs: 2 * KEEP_WARM_INTERVAL_MS,
          messageId: `keep-warm:${ID}:0:${2 * KEEP_WARM_INTERVAL_MS}`,
        },
      ]);
    }).pipe(Effect.scoped),
  );

  it.each([
    ["deleted", { deletedAt: DateTime.makeUnsafe(1) }, { type: "remove" }],
    ["archived", { archivedAt: DateTime.makeUnsafe(1) }, { type: "stop", reason: "archived" }],
    ["settled", { settledOverride: "settled" as const }, { type: "stop", reason: "settled" }],
    [
      "in usage-limit recovery",
      { limitRecovery: { resetAt: iso(HOUR) } as KeepWarmThread["limitRecovery"] },
      { type: "stop", reason: "usageLimit" },
    ],
    [
      "whose last ping hit the usage limit",
      {
        latestRunPromptId: keepWarmPingMessageId(ID, warm(), KEEP_WARM_INTERVAL_MS),
        latestRunFailureClass: "usage_limit",
      },
      { type: "stop", reason: "usageLimit" },
    ],
  ] as const)("a %s thread is not pinged", (_name, patch, decision) => {
    expect(
      decideKeepWarm({
        mode: warm(),
        thread: thread(patch),
        retired: false,
        provider: provider(0),
        now: 2 * HOUR,
      }),
    ).toEqual(decision);
  });

  it("an earlier activation's refused ping does not stop a new activation", () => {
    const earlier = keepWarmPingMessageId(ID, warm(), KEEP_WARM_INTERVAL_MS);
    expect(
      decideKeepWarm({
        mode: warm({ enabledAt: iso(2 * HOUR) }),
        thread: thread({ latestRunPromptId: earlier, latestRunFailureClass: "usage_limit" }),
        retired: false,
        provider: provider(0),
        now: 2 * HOUR,
      }),
    ).toEqual({ type: "ping", dueMs: KEEP_WARM_INTERVAL_MS });
  });

  it.effect("records a request the shell hides, so clients show the server's next ping", () =>
    Effect.gen(function* () {
      // A silent idle compaction ran at minute 58 after a shown run at minute 0.
      const h = yield* harness(
        withMode(warm()),
        thread({ latestRunAt: iso(58 * MINUTE), shownRunAt: iso(0) }),
      );
      yield* h.reconcile();
      expect(h.mode()).toMatchObject({ hiddenRequestAt: iso(58 * MINUTE) });
      yield* TestClock.adjust(KEEP_WARM_INTERVAL_MS);
      expect(h.pings).toHaveLength(0);
      yield* TestClock.adjust(58 * MINUTE);
      expect(h.pings.map((ping) => ping.dueMs)).toEqual([58 * MINUTE + KEEP_WARM_INTERVAL_MS]);
    }).pipe(Effect.scoped),
  );

  it("stops on a provider without an hour-long cache, but not before providers load", () => {
    const input = { mode: warm(), thread: thread(), retired: false, now: 2 * HOUR };
    expect(decideKeepWarm({ ...input, provider: provider(0, "codex") })).toEqual({
      type: "stop",
      reason: "unsupportedProvider",
    });
    expect(decideKeepWarm({ ...input, provider: undefined })).toEqual({
      type: "wait",
      until: KEEP_WARM_MAX_AWAY_MS,
    });
  });
});
