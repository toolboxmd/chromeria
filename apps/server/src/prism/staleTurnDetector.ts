import type { PrismStaleReason, ProviderSession, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";

import type { ThreadLiveness } from "./streamClock.ts";

type Phase = "first-token" | "mid-stream";
type Turn = NonNullable<ThreadLiveness["turn"]>;
type Failure = Exclude<PrismStaleReason, "silence">;

export interface StaleStatus {
  readonly stale: boolean;
  readonly staleSince: number | null;
  readonly thresholdMs: number;
  readonly thresholdSource: "measured" | "default";
  readonly reason: PrismStaleReason | null;
}

export interface StaleTurn extends StaleStatus {
  readonly threadId: ThreadId;
  readonly turn: Turn;
  readonly lastEventAt: number;
  readonly lastEventKind: string;
  readonly silenceMs: number;
}

interface WatchedTurn {
  turn: Turn;
  running: boolean;
  openTool: boolean;
  lastEventAt: number;
  lastEventKind: string;
  silenceFrom: number;
  staleSince: number | null;
  reason: PrismStaleReason | null;
  notified: boolean;
}

const Gap = Schema.Number.check(Schema.isFinite(), Schema.isGreaterThanOrEqualTo(0));
// Read the existing activity payload, without changing what #56 records.
const HealthyStats = Schema.Struct({
  provider: Schema.String,
  model: Schema.NullOr(Schema.String),
  outcome: Schema.Literal("completed"),
  timeToFirstTokenMs: Schema.NullOr(Gap),
  maxGapMs: Gap,
});
const decodeStats = Schema.decodeUnknownOption(HealthyStats);
const keyOf = (provider: string, model: string | null, phase: Phase) =>
  JSON.stringify([provider, model, phase]);

/** One detector, fed by the stream clock and by recorded healthy turn activities. */
export function makeStaleTurnDetectorState(onStale: (turn: StaleTurn) => void) {
  const turns = new Map<ThreadId, WatchedTurn>();
  const samples = new Map<string, { count: number; max: number }>();
  const recorded = new Set<string>();
  let previousTick: { wall: number; monotonic: number } | undefined;

  const threshold = (turn?: Turn) => {
    const phase: Phase = turn?.firstTokenAt == null ? "first-token" : "mid-stream";
    const sample = turn ? samples.get(keyOf(turn.provider, turn.model, phase)) : undefined;
    const measured = sample !== undefined && sample.count >= 20;
    return {
      thresholdMs: measured
        ? sample.max * 1.5
        : Math.max(phase === "first-token" ? 120_000 : 90_000, sample?.max ?? 0),
      thresholdSource: measured ? ("measured" as const) : ("default" as const),
    };
  };
  const status = (threadId: ThreadId): StaleStatus => {
    const state = turns.get(threadId);
    return {
      stale: state?.staleSince != null,
      staleSince: state?.staleSince ?? null,
      reason: state?.reason ?? null,
      ...threshold(state?.turn),
    };
  };
  const flag = (threadId: ThreadId, state: WatchedTurn, now: number, reason: PrismStaleReason) => {
    state.staleSince ??= now;
    state.reason = reason;
    if (state.notified) return;
    state.notified = true;
    onStale({
      threadId,
      turn: state.turn,
      lastEventAt: state.lastEventAt,
      lastEventKind: state.lastEventKind,
      silenceMs: Math.max(0, now - state.silenceFrom),
      ...status(threadId),
    });
  };

  return {
    status,
    record: (id: string, payload: unknown) => {
      if (recorded.has(id)) return;
      const decoded = decodeStats(payload);
      if (Option.isNone(decoded)) return;
      recorded.add(id);
      const stats = decoded.value;
      const add = (phase: Phase, gap: number) => {
        const key = keyOf(stats.provider, stats.model, phase);
        const sample = samples.get(key) ?? { count: 0, max: 0 };
        sample.count += 1;
        sample.max = Math.max(sample.max, gap);
        samples.set(key, sample);
      };
      if (stats.timeToFirstTokenMs !== null) {
        add("first-token", stats.timeToFirstTokenMs);
        add("mid-stream", stats.maxGapMs);
      }
    },
    observe: (
      threadId: ThreadId,
      live: ThreadLiveness,
      now: number,
      kind: string,
      failure?: Failure,
      ended = false,
    ) => {
      let state = turns.get(threadId);
      // A terminal provider failure must survive cleanup events until the next
      // turn, so a Router poll cannot miss it between failure and session exit.
      if (state && !state.running && state.reason !== null && kind !== "turn.started") return;
      if (!live.turn) {
        turns.delete(threadId);
        return;
      }
      if (
        kind === "turn.started" ||
        !state ||
        state.turn.startedAt !== live.turn.startedAt ||
        state.turn.turnId !== live.turn.turnId
      ) {
        state = {
          turn: live.turn,
          running: true,
          openTool: false,
          lastEventAt: now,
          lastEventKind: kind,
          silenceFrom: now,
          staleSince: null,
          reason: null,
          notified: false,
        };
        turns.set(threadId, state);
      }
      state.turn = live.turn;
      state.running = !ended;
      state.openTool = live.openTool !== null;
      state.lastEventAt = now;
      state.lastEventKind = kind;
      state.silenceFrom = now;
      state.staleSince = null;
      state.reason = null;
      if (failure) flag(threadId, state, now, failure);
    },
    tick: (
      now: number,
      monotonic: number,
      sessions: ReadonlyArray<ProviderSession>,
      sessionsObservedAt = now,
    ) => {
      // Some platforms' monotonic clocks include suspend. A delayed timer also
      // resets the window: time while this detector could not run is not proof
      // of provider silence. This conservatively covers event-loop suspension.
      const wake =
        previousTick !== undefined &&
        (Math.abs(now - previousTick.wall - (monotonic - previousTick.monotonic)) > 1_000 ||
          monotonic - previousTick.monotonic > 2_500 ||
          now < previousTick.wall);
      previousTick = { wall: now, monotonic };
      const byThread = new Map(sessions.map((session) => [session.threadId, session]));
      for (const [threadId, state] of turns) {
        if (!state.running) continue;
        if (wake) {
          state.silenceFrom = now;
          state.staleSince = null;
          state.reason = null;
        }
        const session = byThread.get(threadId);
        // listSessions is asynchronous. A turn starting during that read was
        // not necessarily in its snapshot, and must be checked on the next tick.
        const sessionIsCurrent = state.turn.startedAt < sessionsObservedAt;
        if (sessionIsCurrent && (!session || session.status === "closed")) {
          flag(threadId, state, now, "provider-dead");
        } else if (sessionIsCurrent && session?.status === "error") {
          flag(threadId, state, now, "provider-error");
        } else if (
          !state.openTool &&
          now - state.silenceFrom > threshold(state.turn).thresholdMs * 1.25
        ) {
          // The suspect interval starts at threshold crossing. Confirm at 125%,
          // so sampling it on the next tick does not add a second tick of delay.
          flag(threadId, state, now, "silence");
        }
      }
    },
  };
}

export class StaleTurnDetector extends Context.Service<
  StaleTurnDetector,
  {
    readonly state: ReturnType<typeof makeStaleTurnDetectorState>;
    readonly takeStale: Effect.Effect<StaleTurn>;
  }
>()("t3/prism/staleTurnDetector") {}

export const staleTurnDetectorLayer = Layer.effect(
  StaleTurnDetector,
  Effect.gen(function* () {
    const stale = yield* Queue.unbounded<StaleTurn>();
    return StaleTurnDetector.of({
      state: makeStaleTurnDetectorState((turn) => Queue.offerUnsafe(stale, turn)),
      takeStale: Queue.take(stale),
    });
  }),
);
