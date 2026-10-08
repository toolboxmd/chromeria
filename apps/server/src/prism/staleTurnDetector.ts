import type { ThreadId } from "@t3tools/contracts";
import type { HealthyStreamSample } from "./StreamStatsStore.ts";
import type { ThreadLiveness } from "./streamClock.ts";

export interface StaleTurn {
  readonly live: ThreadLiveness;
  readonly silenceMs: number;
  readonly thresholdMs: number;
  readonly thresholdSource: "measured" | "default";
}

/** Silence only: terminal provider failures are owned by v2 and Prism's persisted recovery. */
export function makeStaleTurnDetectorState() {
  const watched = new Map<
    ThreadId,
    {
      key: Pick<ThreadLiveness, "runId" | "attemptId" | "providerTurnId">;
      silenceFrom: number;
      lastEventAt: number;
      notified: boolean;
    }
  >();
  let previousTick: { wall: number; monotonic: number } | undefined;
  const watch = (live: ThreadLiveness) => {
    let state = watched.get(live.threadId);
    if (
      !state ||
      state.key.runId !== live.runId ||
      state.key.attemptId !== live.attemptId ||
      state.key.providerTurnId !== live.providerTurnId
    ) {
      state = {
        key: live,
        silenceFrom: live.lastStreamAt,
        lastEventAt: live.lastStreamAt,
        notified: false,
      };
      watched.set(live.threadId, state);
    }
    if (live.lastStreamAt > state.lastEventAt) {
      state.lastEventAt = live.lastStreamAt;
      state.silenceFrom = live.lastStreamAt;
    }
    return state;
  };
  return {
    tick: (now: number, monotonic: number, lives: ReadonlyArray<ThreadLiveness>) => {
      const wake =
        previousTick !== undefined &&
        (Math.abs(now - previousTick.wall - (monotonic - previousTick.monotonic)) > 1_000 ||
          monotonic - previousTick.monotonic > 2_500 ||
          now < previousTick.wall);
      previousTick = { wall: now, monotonic };
      const active = new Set(lives.map((live) => live.threadId));
      for (const threadId of watched.keys()) if (!active.has(threadId)) watched.delete(threadId);
      for (const live of lives) {
        const state = watch(live);
        // Host sleep or a suspended event loop is not evidence of provider silence.
        if (wake || live.openTool !== null) state.silenceFrom = now;
      }
    },
    pause: (live: ThreadLiveness, now: number) => {
      watch(live).silenceFrom = now;
    },
    notified: (live: ThreadLiveness) => {
      watch(live).notified = true;
    },
    check: (
      live: ThreadLiveness,
      now: number,
      samples: ReadonlyArray<HealthyStreamSample>,
    ): StaleTurn | null => {
      const state = watch(live);
      if (state.notified || live.openTool !== null) return null;
      const sample = samples.find(
        (sample) => sample.provider === live.provider && sample.model === live.model,
      );
      const firstToken = live.firstTokenAt === null;
      const max = firstToken ? sample?.maxTimeToFirstTokenMs : sample?.maxGapMs;
      const measured = sample !== undefined && sample.count >= 20;
      const thresholdMs = measured
        ? (max ?? 0) * 1.5
        : Math.max(firstToken ? 120_000 : 90_000, max ?? 0);
      const silenceMs = Math.max(0, now - state.silenceFrom);
      return silenceMs > thresholdMs * 1.25
        ? { live, silenceMs, thresholdMs, thresholdSource: measured ? "measured" : "default" }
        : null;
    },
  };
}
