import {
  isToolLifecycleItemType,
  type OrchestrationV2Run,
  type OrchestrationV2RunAttempt,
  type ProviderDriverKind,
  type ProviderThreadId,
  type ProviderTurnId,
  type RunAttemptId,
  type RunId,
  type ThreadId,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import type { ProviderEventIngestInput } from "../orchestration-v2/ProviderEventIngestor.ts";
import * as StreamStatsStore from "./StreamStatsStore.ts";

interface AttemptIdentity {
  readonly threadId: ThreadId;
  readonly runId: RunId;
  readonly runOrdinal: OrchestrationV2Run["ordinal"];
  readonly attemptId: RunAttemptId;
  readonly attemptOrdinal: OrchestrationV2RunAttempt["attemptOrdinal"];
  readonly providerThreadId: ProviderThreadId;
  readonly provider: ProviderDriverKind;
  readonly model: string | null;
}

export interface ThreadLiveness extends AttemptIdentity {
  readonly providerTurnId: ProviderTurnId | null;
  readonly startedAt: number;
  readonly firstTokenAt: number | null;
  readonly lastStreamAt: number;
  readonly lastEventKind: string;
  readonly openTool: { readonly itemId: string; readonly startedAt: number } | null;
  readonly eventCount: number;
}

interface AttemptState {
  readonly identity: AttemptIdentity;
  lastProviderTurnOrdinal: number;
  turn: {
    providerTurnId: ProviderTurnId | null;
    startedAt: number;
    firstTokenAt: number | null;
    lastStreamAt: number;
    lastEventKind: string;
    maxGapMs: number;
    eventCount: number;
    readonly tools: Map<string, { readonly itemId: string; readonly startedAt: number }>;
  } | null;
}

const newTurn = (now: number): NonNullable<AttemptState["turn"]> => ({
  providerTurnId: null,
  startedAt: now,
  firstTokenAt: null,
  lastStreamAt: now,
  lastEventKind: "attempt.started",
  maxGapMs: 0,
  eventCount: 0,
  tools: new Map(),
});

/** Attempt registration fences late events and cleanup without consulting SQL per event. */
export function makeStreamClockState() {
  const attempts = new Map<ThreadId, AttemptState>();
  const finish = (
    state: AttemptState,
    now: number,
    outcome: StreamStatsStore.StreamTurnOutcome,
  ) => {
    const turn = state.turn;
    state.turn = null;
    if (turn?.providerTurnId === null || turn === null) return null;
    return {
      threadId: state.identity.threadId,
      runId: state.identity.runId,
      providerTurnId: turn.providerTurnId,
      provider: state.identity.provider,
      model: state.identity.model,
      outcome,
      startedAt: turn.startedAt,
      endedAt: now,
      timeToFirstTokenMs: turn.firstTokenAt === null ? null : turn.firstTokenAt - turn.startedAt,
      maxGapMs: turn.maxGapMs,
      eventCount: turn.eventCount,
    } satisfies StreamStatsStore.TurnStreamStats;
  };
  return {
    beginAttempt: (identity: AttemptIdentity, now: number) => {
      const previous = attempts.get(identity.threadId);
      if (
        previous &&
        (previous.identity.runOrdinal > identity.runOrdinal ||
          (previous.identity.runOrdinal === identity.runOrdinal &&
            previous.identity.attemptOrdinal >= identity.attemptOrdinal))
      )
        return null;
      const displaced = previous === undefined ? null : finish(previous, now, "aborted");
      attempts.set(identity.threadId, { identity, lastProviderTurnOrdinal: 0, turn: newTurn(now) });
      return displaced;
    },
    observe: (input: ProviderEventIngestInput, now: number, attemptId?: RunAttemptId) => {
      const state = attempts.get(input.threadId);
      if (!state || input.runId !== state.identity.runId || attemptId !== state.identity.attemptId)
        return null;
      const event = input.event;
      if (event.type === "provider_turn.updated") {
        const providerTurn = event.providerTurn;
        if (
          providerTurn.runAttemptId !== state.identity.attemptId ||
          providerTurn.providerThreadId !== state.identity.providerThreadId ||
          providerTurn.ordinal < state.lastProviderTurnOrdinal
        )
          return null;
        if (state.turn === null && providerTurn.ordinal <= state.lastProviderTurnOrdinal)
          return null;
        if (providerTurn.ordinal > state.lastProviderTurnOrdinal) {
          // One Codex run can contain several provider turns on the same attempt.
          if (state.lastProviderTurnOrdinal > 0) state.turn = newTurn(now);
          state.lastProviderTurnOrdinal = providerTurn.ordinal;
        }
        if (state.turn) state.turn.providerTurnId = providerTurn.id;
      }
      const turn = state.turn;
      if (!turn) return null;
      switch (event.type) {
        case "message.updated":
          if (event.message.threadId !== input.threadId || event.message.runId !== input.runId)
            return null;
          break;
        case "turn_item.updated":
          if (
            event.turnItem.threadId !== input.threadId ||
            event.turnItem.runId !== input.runId ||
            (event.turnItem.providerTurnId !== null &&
              event.turnItem.providerTurnId !== turn.providerTurnId)
          )
            return null;
          break;
        case "turn.terminal":
          if (
            event.providerTurnId !== turn.providerTurnId ||
            event.providerThreadId !== state.identity.providerThreadId
          )
            return null;
          break;
        case "provider_turn.updated":
          break;
        case "node.updated":
          if (event.node.threadId !== input.threadId || event.node.runId !== input.runId)
            return null;
          break;
        case "provider_thread.updated":
          if (event.providerThread.id !== state.identity.providerThreadId) return null;
          break;
        case "plan.updated":
          if (event.plan.threadId !== input.threadId || event.plan.runId !== input.runId)
            return null;
          break;
        case "runtime_request.updated":
          if (event.runtimeRequest.providerTurnId !== turn.providerTurnId) return null;
          break;
        default:
          return null;
      }
      if (turn.tools.size === 0)
        turn.maxGapMs = Math.max(turn.maxGapMs, Math.max(0, now - turn.lastStreamAt));
      turn.lastStreamAt = now;
      turn.lastEventKind = event.type;
      turn.eventCount += 1;
      if (
        (event.type === "message.updated" &&
          event.message.role === "assistant" &&
          event.message.text.length > 0) ||
        (event.type === "turn_item.updated" &&
          (event.turnItem.type === "assistant_message" || event.turnItem.type === "reasoning") &&
          event.turnItem.text.length > 0)
      )
        turn.firstTokenAt ??= now;
      if (event.type === "turn_item.updated" && isToolLifecycleItemType(event.turnItem.type)) {
        if (event.turnItem.status === "running" || event.turnItem.status === "pending") {
          if (!turn.tools.has(event.turnItem.id))
            turn.tools.set(event.turnItem.id, { itemId: event.turnItem.id, startedAt: now });
        } else turn.tools.delete(event.turnItem.id);
      }
      const terminalStatus =
        event.type === "turn.terminal"
          ? event.status
          : event.type === "provider_turn.updated"
            ? event.providerTurn.status
            : undefined;
      if (
        terminalStatus === "completed" ||
        terminalStatus === "failed" ||
        terminalStatus === "interrupted" ||
        terminalStatus === "cancelled"
      ) {
        return finish(
          state,
          now,
          terminalStatus === "completed"
            ? "completed"
            : terminalStatus === "failed"
              ? "error"
              : "aborted",
        );
      }
      return null;
    },
    endAttempt: (threadId: ThreadId, runId: RunId, attemptId: RunAttemptId, now: number) => {
      const state = attempts.get(threadId);
      if (!state || state.identity.runId !== runId || state.identity.attemptId !== attemptId)
        return null;
      const stats = finish(state, now, "aborted");
      attempts.delete(threadId);
      return stats;
    },
    liveness: (): ReadonlyArray<ThreadLiveness> =>
      [...attempts.values()].flatMap(({ identity, turn }) =>
        turn === null
          ? []
          : [
              {
                ...identity,
                providerTurnId: turn.providerTurnId,
                startedAt: turn.startedAt,
                firstTokenAt: turn.firstTokenAt,
                lastStreamAt: turn.lastStreamAt,
                lastEventKind: turn.lastEventKind,
                openTool: turn.tools.values().next().value ?? null,
                eventCount: turn.eventCount,
              },
            ],
      ),
  };
}

export class StreamClock extends Context.Service<
  StreamClock,
  {
    readonly beginAttempt: (input: AttemptIdentity) => Effect.Effect<void>;
    readonly observe: (
      input: ProviderEventIngestInput,
      attemptId?: RunAttemptId,
    ) => Effect.Effect<void>;
    readonly endAttempt: (
      threadId: ThreadId,
      runId: RunId,
      attemptId: RunAttemptId,
    ) => Effect.Effect<void>;
    readonly liveness: Effect.Effect<ReadonlyArray<ThreadLiveness>>;
    readonly healthySamples: Effect.Effect<ReadonlyArray<StreamStatsStore.HealthyStreamSample>>;
  }
>()("t3/prism/streamClock") {}

/** Default no-op keeps upstream harnesses independent of the production fork clock. */
export class StreamClockHooks extends Context.Reference<
  Pick<StreamClock["Service"], "beginAttempt" | "observe" | "endAttempt">
>("t3/prism/StreamClockHooks", {
  defaultValue: () => ({
    beginAttempt: () => Effect.void,
    observe: () => Effect.void,
    endAttempt: () => Effect.void,
  }),
}) {}

/** The captured execution attempt accompanies delivered events without changing the ingestor input. */
export class StreamClockAttempt extends Context.Reference<RunAttemptId | undefined>(
  "t3/prism/StreamClockAttempt",
  {
    defaultValue: () => undefined,
  },
) {}

const make = Effect.gen(function* () {
  const store = yield* StreamStatsStore.StreamStatsStore;
  const state = makeStreamClockState();
  const samples = new Map<
    ProviderDriverKind,
    Map<string | null, StreamStatsStore.HealthyStreamSample>
  >();
  const setSample = (sample: StreamStatsStore.HealthyStreamSample) => {
    let models = samples.get(sample.provider);
    if (models === undefined) {
      models = new Map();
      samples.set(sample.provider, models);
    }
    models.set(sample.model, sample);
  };
  for (const sample of yield* store.healthySamples) setSample(sample);
  const record = (stats: StreamStatsStore.TurnStreamStats | null) =>
    stats === null
      ? Effect.void
      : store.record(stats).pipe(
          Effect.tap((inserted) =>
            Effect.sync(() => {
              if (!inserted || stats.outcome !== "completed" || stats.timeToFirstTokenMs === null)
                return;
              const previous = samples.get(stats.provider)?.get(stats.model);
              setSample({
                provider: stats.provider,
                model: stats.model,
                count: (previous?.count ?? 0) + 1,
                maxTimeToFirstTokenMs: Math.max(
                  previous?.maxTimeToFirstTokenMs ?? 0,
                  stats.timeToFirstTokenMs,
                ),
                maxGapMs: Math.max(previous?.maxGapMs ?? 0, stats.maxGapMs),
              });
            }),
          ),
          Effect.catchTags({
            StreamStatsWriteError: (cause) =>
              Effect.logWarning("prism.stream-stats.write-failed", { cause }),
          }),
          Effect.asVoid,
        );
  return StreamClock.of({
    beginAttempt: (identity) =>
      Clock.currentTimeMillis.pipe(
        Effect.flatMap((now) => record(state.beginAttempt(identity, now))),
      ),
    observe: (input, attemptId) =>
      Clock.currentTimeMillis.pipe(
        Effect.flatMap((now) => record(state.observe(input, now, attemptId))),
      ),
    endAttempt: (threadId, runId, attemptId) =>
      Clock.currentTimeMillis.pipe(
        Effect.flatMap((now) => record(state.endAttempt(threadId, runId, attemptId, now))),
      ),
    liveness: Effect.sync(state.liveness),
    healthySamples: Effect.sync(() =>
      [...samples.values()].flatMap((models) => [...models.values()]),
    ),
  });
});

const layerClock = Layer.effect(StreamClock, make);
export const layer = Layer.effect(
  StreamClockHooks,
  Effect.map(StreamClock, (clock) => clock),
).pipe(Layer.provideMerge(layerClock));
