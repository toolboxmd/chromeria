import { describe, expect, it } from "@effect/vitest";
import {
  DEFAULT_SERVER_SETTINGS,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  RunId,
  RuntimeRequestId,
  type ServerProvider,
  type ServerSettings,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as TestClock from "effect/testing/TestClock";
import { applyServerSettingsPatch } from "@t3tools/shared/serverSettings";
import { makeWightMode, wightIdle, wightPaused, type WightThread } from "./wightMode.ts";

const INSTANCE = ProviderInstanceId.make("codex");
const ID = ThreadId.make("wight-test");
const iso = (ms: number) => DateTime.formatIso(DateTime.makeUnsafe(ms));
const thread = (id = ID): WightThread => ({
  id,
  providerInstanceId: INSTANCE,
  modelSelection: { instanceId: INSTANCE, model: "gpt-5" },
  runtimeMode: "full-access",
  interactionMode: "default",
  status: "idle",
  activeRunId: null,
  latestRunId: null,
  updatedAt: DateTime.makeUnsafe(0),
  archivedAt: null,
  deletedAt: null,
  pendingRuntimeRequest: null,
  hasActionableProposedPlan: false,
  limitRecovery: null,
  hasQueuedRuns: false,
});
const provider = (percent: number, instanceId = INSTANCE) =>
  ({
    instanceId,
    enabled: true,
    usageLimits: {
      checkedAt: iso(0),
      windows: [{ id: "session", kind: "session", label: "Session", usedPercent: percent }],
    },
  }) as unknown as ServerProvider;
const activate = (ids: readonly ThreadId[], expiresAt: number | null = null): ServerSettings => ({
  ...DEFAULT_SERVER_SETTINGS,
  wightModes: Object.fromEntries(ids.map((id) => [id, { enabledAt: iso(0), expiresAt }])),
});

/** Resumes synchronously adopt a turn, standing in for command admission; reconcile completion is the worker drain. */
const harness = (initial: ServerSettings, initialThreads = [thread()]) =>
  Effect.gen(function* () {
    let settings = initial;
    let providers = [provider(20)];
    const threads = new Map(initialThreads.map((entry) => [entry.id, entry]));
    const sends: Array<{ id: string; text: string }> = [];
    let retired = false;
    let afterRead: (() => void) | undefined;
    const runtime = yield* makeWightMode({
      settings: Effect.sync(() => settings),
      providers: Effect.sync(() => providers),
      retired: () => Effect.sync(() => retired),
      thread: (id) =>
        Effect.sync(() => {
          const value = threads.get(ThreadId.make(id));
          afterRead?.();
          return value;
        }),
      resume: (entry, text, _activation, admission) =>
        Effect.gen(function* () {
          if (!(yield* admission)) return;
          sends.push({ id: entry.id, text });
          threads.set(entry.id, {
            ...entry,
            status: "running",
          });
        }),
    });
    return {
      ...runtime,
      retire: () => {
        retired = true;
      },
      sends,
      threads,
      settings: (next: ServerSettings) => {
        settings = next;
      },
      providers: (next: ServerProvider[]) => {
        providers = next;
      },
      afterRead: (run: () => void) => {
        afterRead = run;
      },
    };
  });

describe("Wight mode", () => {
  it.effect("a retired idle shell cannot receive a nudge even at fresh below-threshold usage", () =>
    Effect.gen(function* () {
      const h = yield* harness(activate([ID]));
      h.retire();
      yield* h.reconcile();
      h.providers([provider(0)]);
      yield* h.reconcile();
      expect(h.sends).toHaveLength(0);
    }).pipe(Effect.scoped),
  );

  it.effect("enables idle, waits for active, turns off, and includes current time", () =>
    Effect.gen(function* () {
      const h = yield* harness(activate([ID]));
      yield* h.reconcile();
      yield* Effect.all([h.reconcile(), h.reconcile()], { concurrency: "unbounded" });
      expect(h.sends).toHaveLength(1);
      expect(h.sends[0]!.text).toContain(iso(yield* Clock.currentTimeMillis));
      expect(h.sends[0]!.text).toMatch(/new user messages, child reports and answers/);
      h.settings(DEFAULT_SERVER_SETTINGS);
      h.threads.set(ID, thread());
      yield* h.reconcile();
      expect(h.sends).toHaveLength(1);
    }).pipe(Effect.scoped),
  );

  it.effect(
    "pauses every thread on the instance at any window threshold and resumes below it",
    () =>
      Effect.gen(function* () {
        const second = ThreadId.make("second");
        const otherId = ThreadId.make("other");
        const otherInstance = ProviderInstanceId.make("codex_other");
        const settings = {
          ...activate([ID, second, otherId]),
          providerInstances: {
            [INSTANCE]: { driver: ProviderDriverKind.make("codex"), wightLimitPercent: 50 },
          },
        };
        const h = yield* harness(settings, [
          thread(),
          thread(second),
          {
            ...thread(otherId),
            providerInstanceId: otherInstance,
            modelSelection: { instanceId: otherInstance, model: "gpt-5" },
          },
        ]);
        h.providers([provider(50), provider(20, otherInstance)]);
        yield* h.reconcile();
        expect(h.sends.map((send) => send.id)).toEqual([otherId]);
        h.providers([provider(49), provider(20, otherInstance)]);
        yield* h.reconcile();
        yield* h.reconcile();
        expect(h.sends.map((send) => send.id)).toEqual([otherId, ID, second]);
      }).pipe(Effect.scoped),
  );

  it.effect("re-reads toggle, slider and provider usage changed during shell reads", () =>
    Effect.gen(function* () {
      for (const change of ["toggle", "slider", "usage", "timer", "retirement"] as const) {
        const h = yield* harness(activate([ID]));
        h.afterRead(() => {
          if (change === "toggle") h.settings(DEFAULT_SERVER_SETTINGS);
          if (change === "slider")
            h.settings({
              ...activate([ID]),
              providerInstances: {
                [INSTANCE]: { driver: ProviderDriverKind.make("codex"), wightLimitPercent: 10 },
              },
            });
          if (change === "usage") h.providers([provider(90)]);
          if (change === "timer") h.settings(activate([ID], 0));
          if (change === "retirement") h.retire();
        });
        yield* h.reconcile();
        expect(h.sends, change).toHaveLength(0);
      }
    }).pipe(Effect.scoped),
  );

  it.effect(
    "timer includes pauses and expires without interrupting the active turn; infinite survives an hour",
    () =>
      Effect.gen(function* () {
        const start = yield* Clock.currentTimeMillis;
        const h = yield* harness(activate([ID], start + 3_600_000));
        yield* h.reconcile();
        const active = h.threads.get(ID);
        yield* TestClock.adjust(3_600_000);
        yield* h.reconcile();
        expect(h.threads.get(ID)).toEqual(active);
        h.threads.set(ID, thread());
        yield* h.reconcile();
        expect(h.sends).toHaveLength(1);
        h.settings(activate([ID]));
        for (let index = 0; index < 60; index++) {
          h.threads.set(ID, thread());
          yield* h.reconcile();
          yield* h.reconcile();
          yield* TestClock.adjust(60_000);
        }
        expect(h.sends).toHaveLength(61);
      }).pipe(Effect.scoped),
  );

  it.each([
    "queued",
    "preparing",
    "starting",
    "running",
    "waiting",
    "failed",
    "interrupted",
    "cancelled",
  ] as const)(
    "does not continue %s work, including usage-limit failures owned by reset recovery",
    (status) => {
      expect(wightIdle({ ...thread(), status, lastErrorClass: "usage_limit" })).toBe(false);
    },
  );

  it("continues completed work but leaves an armed reset or pending question alone", () => {
    expect(wightIdle({ ...thread(), status: "completed" })).toBe(true);
    expect(
      wightIdle({
        ...thread(),
        pendingRuntimeRequest: {
          id: RuntimeRequestId.make("question"),
          kind: "user_input",
          createdAt: DateTime.makeUnsafe(0),
        },
      }),
    ).toBe(false);
    expect(
      wightIdle({
        ...thread(),
        limitRecovery: {
          runId: RunId.make("limited"),
          resetAt: iso(3_600_000),
          autoResume: true,
          snooze: false,
        },
      }),
    ).toBe(false);
  });

  it("holds archived, interrupted, pending input and queued turns; unknown providers and zero limits pause", () => {
    expect(wightIdle({ ...thread(), archivedAt: DateTime.makeUnsafe(0) })).toBe(false);
    expect(wightIdle({ ...thread(), hasQueuedRuns: true })).toBe(false);
    expect(wightIdle({ ...thread(), hasActionableProposedPlan: true })).toBe(false);
    expect(wightIdle({ ...thread(), status: "cancelled" })).toBe(false);
    expect(wightPaused(undefined, 80)).toBe(true);
    expect(wightPaused(provider(0), 0)).toBe(true);
    expect(
      wightPaused(
        {
          ...provider(20),
          usageLimits: {
            checkedAt: iso(0),
            windows: [
              ...provider(20).usageLimits!.windows,
              { id: "weekly", kind: "weekly", label: "Weekly", usedPercent: 80 },
            ],
          },
        },
        80,
      ),
    ).toBe(true);
  });

  it("per-thread patches preserve siblings and remove disabled entries", () => {
    const settings = activate([ID, ThreadId.make("sibling")]);
    const next = applyServerSettingsPatch(settings, { wightModes: { [ID]: null } });
    expect(next.wightModes[ID]).toBeUndefined();
    expect(next.wightModes[ThreadId.make("sibling")]).toEqual({
      enabledAt: iso(0),
      expiresAt: null,
    });
  });
});
