/**
 * Per-thread provider stream clock for Prism (toolboxmd/t3code#55).
 *
 * Every provider event the server receives stamps its thread's clock: each
 * native event an adapter writes (every token, reasoning and tool output
 * delta, Claude `thinking_tokens`, Codex `item/*Delta`, OpenCode
 * `message.part.delta`) and each canonical runtime event. The clock taps the
 * shared `ProviderEventLoggers` pair, so no adapter changes and nothing is
 * persisted per delta. Model Router reads it through
 * `GET /api/prism/liveness` to tell a silent model from a working one; each
 * finished turn's statistics are recorded once as a thread activity.
 */
import {
  isToolLifecycleItemType,
  type ProviderRuntimeEvent,
  type ThreadId,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Predicate from "effect/Predicate";
import * as Queue from "effect/Queue";

import type { EventNdjsonLogger } from "../provider/Layers/EventNdjsonLogger.ts";
import * as ProviderEventLoggers from "../provider/Layers/ProviderEventLoggers.ts";
import { StaleTurnDetector, staleTurnDetectorLayer } from "./staleTurnDetector.ts";

export type StreamTurnOutcome = "completed" | "aborted" | "error";

export interface OpenTool {
  readonly itemId: string;
  readonly itemType: string;
  readonly title: string | null;
  readonly startedAt: number;
}

/** One finished turn, as recorded in the thread's `prism.stream-stats` activity. */
export interface TurnStreamStats {
  readonly threadId: ThreadId;
  readonly turnId: string | null;
  readonly provider: string;
  readonly model: string | null;
  readonly outcome: StreamTurnOutcome;
  /** The provider's own end state, for example `interrupted` or `failed`. */
  readonly state: string;
  readonly startedAt: number;
  readonly endedAt: number;
  /** From turn start to the first content delta; null when none streamed. */
  readonly timeToFirstTokenMs: number | null;
  /** Longest silence between two events while no tool call was open. */
  readonly maxGapMs: number;
  /** Native provider events received during the turn. */
  readonly eventCount: number;
}

interface TurnState {
  readonly turnId: string | null;
  readonly provider: string;
  readonly model: string | null;
  readonly startedAt: number;
  firstTokenAt: number | null;
  lastEventAt: number;
  maxGapMs: number;
  eventCount: number;
}

interface ThreadState {
  lastStreamAt: number;
  readonly openTools: Map<string, OpenTool>;
  turn: TurnState | null;
}

export interface ThreadLiveness {
  readonly lastStreamAt: number | null;
  /** The oldest tool call still open: silence under it is the tool's, not the model's. */
  readonly openTool: OpenTool | null;
  readonly turn: {
    readonly turnId: string | null;
    readonly provider: string;
    readonly model: string | null;
    readonly startedAt: number;
    readonly eventCount: number;
    readonly firstTokenAt: number | null;
  } | null;
}

function outcomeOf(event: ProviderRuntimeEvent): { outcome: StreamTurnOutcome; state: string } {
  if (event.type !== "turn.completed") return { outcome: "aborted", state: "aborted" };
  const state = event.payload.state;
  if (state === "completed") return { outcome: "completed", state };
  if (state === "failed") return { outcome: "error", state };
  return { outcome: "aborted", state };
}

/** The in-memory clock; `turnEnds` feeds the stats recorder. */
export function makeStreamClockState(turnEnds: (stats: TurnStreamStats) => void) {
  const threads = new Map<string, ThreadState>();

  const stateOf = (threadId: string, now: number) => {
    let state = threads.get(threadId);
    if (!state) {
      state = { lastStreamAt: now, openTools: new Map(), turn: null };
      threads.set(threadId, state);
    }
    return state;
  };

  const touch = (threadId: string, now: number, native: boolean) => {
    const state = stateOf(threadId, now);
    state.lastStreamAt = now;
    const turn = state.turn;
    if (!turn) return state;
    // Tools open and close only on events, so the set seen here held for
    // the whole interval since the previous event.
    if (state.openTools.size === 0) {
      turn.maxGapMs = Math.max(turn.maxGapMs, now - turn.lastEventAt);
    }
    turn.lastEventAt = now;
    if (native) turn.eventCount += 1;
    return state;
  };

  return {
    native: (threadId: string, now: number) => {
      touch(threadId, now, true);
    },
    canonical: (event: ProviderRuntimeEvent, now: number) => {
      const state = touch(event.threadId, now, false);
      switch (event.type) {
        case "turn.started":
          state.openTools.clear();
          state.turn = {
            turnId: event.turnId ?? null,
            provider: event.provider,
            model: event.payload.model ?? null,
            startedAt: now,
            firstTokenAt: null,
            lastEventAt: now,
            maxGapMs: 0,
            eventCount: 0,
          };
          return;
        case "content.delta":
          if (state.turn && state.turn.firstTokenAt === null) state.turn.firstTokenAt = now;
          return;
        case "item.started":
          if (isToolLifecycleItemType(event.payload.itemType)) {
            const itemId = event.itemId ?? event.eventId;
            state.openTools.set(itemId, {
              itemId,
              itemType: event.payload.itemType,
              title: event.payload.title ?? null,
              startedAt: now,
            });
          }
          return;
        case "item.completed":
          state.openTools.delete(event.itemId ?? event.eventId);
          return;
        case "turn.completed":
        case "turn.aborted": {
          const turn = state.turn;
          state.turn = null;
          state.openTools.clear();
          if (!turn) return;
          turnEnds({
            threadId: event.threadId,
            turnId: event.turnId ?? turn.turnId,
            provider: turn.provider,
            model: turn.model,
            ...outcomeOf(event),
            startedAt: turn.startedAt,
            endedAt: now,
            timeToFirstTokenMs:
              turn.firstTokenAt === null ? null : turn.firstTokenAt - turn.startedAt,
            maxGapMs: turn.maxGapMs,
            eventCount: turn.eventCount,
          });
          return;
        }
        default:
          return;
      }
    },
    liveness: (threadId: string): ThreadLiveness => {
      const state = threads.get(threadId);
      if (!state) return { lastStreamAt: null, openTool: null, turn: null };
      let openTool: OpenTool | null = null;
      for (const tool of state.openTools.values()) {
        if (!openTool || tool.startedAt < openTool.startedAt) openTool = tool;
      }
      const turn = state.turn;
      return {
        lastStreamAt: state.lastStreamAt,
        openTool,
        turn: turn
          ? {
              turnId: turn.turnId,
              provider: turn.provider,
              model: turn.model,
              startedAt: turn.startedAt,
              eventCount: turn.eventCount,
              firstTokenAt: turn.firstTokenAt,
            }
          : null,
      };
    },
  };
}

export class StreamClock extends Context.Service<
  StreamClock,
  {
    readonly native: (threadId: ThreadId | null, kind?: string) => Effect.Effect<void>;
    readonly canonical: (event: ProviderRuntimeEvent) => Effect.Effect<void>;
    readonly liveness: (threadId: ThreadId) => Effect.Effect<ThreadLiveness>;
    readonly takeTurnEnd: Effect.Effect<TurnStreamStats>;
  }
>()("t3/prism/streamClock") {}

export const make = Effect.gen(function* () {
  const detector = yield* StaleTurnDetector;
  const turnEnds = yield* Queue.unbounded<TurnStreamStats>();
  const state = makeStreamClockState((stats) => Queue.offerUnsafe(turnEnds, stats));
  return StreamClock.of({
    native: (threadId, kind = "native") =>
      threadId === null
        ? Effect.void
        : Effect.map(Clock.currentTimeMillis, (now) => {
            state.native(threadId, now);
            detector.state.observe(threadId, state.liveness(threadId), now, kind);
          }),
    canonical: (event) =>
      Effect.map(Clock.currentTimeMillis, (now) => {
        const before = state.liveness(event.threadId);
        state.canonical(event, now);
        const after = state.liveness(event.threadId);
        const failure =
          event.type === "runtime.error" ||
          (event.type === "turn.completed" && event.payload.state === "failed") ||
          (event.type === "session.state.changed" && event.payload.state === "error")
            ? "provider-error"
            : event.type === "session.exited"
              ? "provider-dead"
              : undefined;
        detector.state.observe(
          event.threadId,
          failure && !after.turn ? before : after,
          now,
          event.type,
          failure,
          event.type === "turn.completed" ||
            event.type === "turn.aborted" ||
            event.type === "session.exited",
        );
      }),
    liveness: (threadId) => Effect.sync(() => state.liveness(threadId)),
    takeTurnEnd: Queue.take(turnEnds),
  });
});

const layer = Layer.effect(StreamClock, make).pipe(Layer.provideMerge(staleTurnDetectorLayer));

/** A logger that also stamps the clock; logging off still keeps the clock. */
function tapped(
  logger: EventNdjsonLogger | undefined,
  stamp: (event: unknown, threadId: ThreadId | null) => Effect.Effect<void>,
): EventNdjsonLogger {
  return {
    filePath: logger?.filePath ?? "",
    write: (event, threadId) =>
      stamp(event, threadId).pipe(Effect.andThen(logger?.write(event, threadId) ?? Effect.void)),
    close: () => logger?.close() ?? Effect.void,
  };
}

/** Both logger streams stamping `clock`, always present so the clock runs with logging off. */
export function tapLoggers(
  loggers: ProviderEventLoggers.ProviderEventLoggers["Service"],
  clock: StreamClock["Service"],
): ProviderEventLoggers.ProviderEventLoggers["Service"] {
  return {
    native: tapped(loggers.native, (event, threadId) => {
      const kind = Predicate.isObject(event) ? (event.method ?? event.type) : undefined;
      return clock.native(threadId, typeof kind === "string" ? `native:${kind}` : "native");
    }),
    canonical: tapped(loggers.canonical, (event) => clock.canonical(event as ProviderRuntimeEvent)),
  };
}

/**
 * `ProviderEventLoggers.layer` with the clock tapped into both streams.
 * Provides `StreamClock` too, so the liveness route and recorder read the
 * same instance the adapters stamp.
 */
export const providerEventLoggersLayer = Layer.effect(
  ProviderEventLoggers.ProviderEventLoggers,
  Effect.gen(function* () {
    const clock = yield* StreamClock;
    const loggers = yield* ProviderEventLoggers.ProviderEventLoggers;
    return ProviderEventLoggers.ProviderEventLoggers.of(tapLoggers(loggers, clock));
  }),
).pipe(Layer.provide(ProviderEventLoggers.layer), Layer.provideMerge(layer));
