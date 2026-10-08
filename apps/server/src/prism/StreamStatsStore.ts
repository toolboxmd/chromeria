import { ProviderDriverKind, ProviderTurnId, RunId, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlSchema from "effect/sql/SqlSchema";

const Milliseconds = Schema.Number.check(Schema.isFinite());
const Duration = Milliseconds.check(Schema.isGreaterThanOrEqualTo(0));
const Count = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));

export const StreamTurnOutcome = Schema.Literals(["completed", "aborted", "error"]);
export type StreamTurnOutcome = typeof StreamTurnOutcome.Type;

/** The final stream measurements of one finished provider turn. */
export const TurnStreamStats = Schema.Struct({
  threadId: ThreadId,
  runId: RunId,
  providerTurnId: ProviderTurnId,
  provider: ProviderDriverKind,
  model: Schema.NullOr(Schema.String),
  outcome: StreamTurnOutcome,
  /** Epoch milliseconds. */
  startedAt: Milliseconds,
  endedAt: Milliseconds,
  /** From turn start to the first content event; null when none streamed. */
  timeToFirstTokenMs: Schema.NullOr(Duration),
  /** Longest silence between two events while no tool call was open. */
  maxGapMs: Duration,
  eventCount: Count,
});
export type TurnStreamStats = typeof TurnStreamStats.Type;

/**
 * Healthy turns of one provider and model: completed, with a first token. Each such turn is one
 * first-token sample and one mid-stream sample, so both phases share `count`.
 */
export const HealthyStreamSample = Schema.Struct({
  provider: ProviderDriverKind,
  model: Schema.NullOr(Schema.String),
  count: Count,
  maxTimeToFirstTokenMs: Duration,
  maxGapMs: Duration,
});
export type HealthyStreamSample = typeof HealthyStreamSample.Type;

export class StreamStatsWriteError extends Schema.TaggedError<StreamStatsWriteError>()(
  "StreamStatsWriteError",
  {
    runId: Schema.String,
    providerTurnId: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return "Failed to record Prism stream statistics.";
  }
}

export class StreamStatsReadError extends Schema.TaggedError<StreamStatsReadError>()(
  "StreamStatsReadError",
  { cause: Schema.Defect() },
) {
  override get message(): string {
    return "Failed to read Prism stream statistics.";
  }
}

/**
 * Finished-turn stream statistics in the fork-owned `fork_prism_stream_stats` table, outside
 * upstream migrations. One row per provider turn of a run; per-event clocks stay in memory.
 */
export class StreamStatsStore extends Context.Service<
  StreamStatsStore,
  {
    /**
     * Records a finished turn once. Returns false when that run's provider turn is already
     * recorded; a retried write never replaces the first measurements.
     */
    readonly record: (stats: TurnStreamStats) => Effect.Effect<boolean, StreamStatsWriteError>;
    /** Healthy-turn counts and maxima per provider and model, across all recorded history. */
    readonly healthySamples: Effect.Effect<
      ReadonlyArray<HealthyStreamSample>,
      StreamStatsReadError
    >;
  }
>()("t3/prism/StreamStatsStore") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE IF NOT EXISTS fork_prism_stream_stats (
      run_id TEXT NOT NULL,
      provider_turn_id TEXT NOT NULL,
      thread_id TEXT NOT NULL,
      provider TEXT NOT NULL,
      model TEXT,
      outcome TEXT NOT NULL,
      started_at REAL NOT NULL,
      ended_at REAL NOT NULL,
      time_to_first_token_ms REAL,
      max_gap_ms REAL NOT NULL,
      event_count INTEGER NOT NULL,
      PRIMARY KEY (run_id, provider_turn_id)
    )
  `;

  const insert = SqlSchema.findAll({
    Request: TurnStreamStats,
    Result: Schema.Struct({ runId: Schema.String }),
    execute: (stats) => sql`
      INSERT INTO fork_prism_stream_stats (
        run_id, provider_turn_id, thread_id, provider, model, outcome,
        started_at, ended_at, time_to_first_token_ms, max_gap_ms, event_count
      ) VALUES (
        ${stats.runId}, ${stats.providerTurnId}, ${stats.threadId}, ${stats.provider},
        ${stats.model}, ${stats.outcome}, ${stats.startedAt}, ${stats.endedAt},
        ${stats.timeToFirstTokenMs}, ${stats.maxGapMs}, ${stats.eventCount}
      )
      ON CONFLICT (run_id, provider_turn_id) DO NOTHING
      RETURNING run_id AS "runId"
    `,
  });

  const healthy = SqlSchema.findAll({
    Request: Schema.Void,
    Result: HealthyStreamSample,
    execute: () => sql`
      SELECT
        provider,
        model,
        COUNT(*) AS count,
        MAX(time_to_first_token_ms) AS "maxTimeToFirstTokenMs",
        MAX(max_gap_ms) AS "maxGapMs"
      FROM fork_prism_stream_stats
      WHERE outcome = 'completed' AND time_to_first_token_ms IS NOT NULL
      GROUP BY provider, model
    `,
  });

  return StreamStatsStore.of({
    record: (stats) =>
      insert(stats).pipe(
        Effect.map((rows) => rows.length > 0),
        Effect.mapError(
          (cause) =>
            new StreamStatsWriteError({
              runId: stats.runId,
              providerTurnId: stats.providerTurnId,
              cause,
            }),
        ),
      ),
    healthySamples: healthy(undefined).pipe(
      Effect.mapError((cause) => new StreamStatsReadError({ cause })),
    ),
  });
});

export const layer = Layer.effect(StreamStatsStore, make);
