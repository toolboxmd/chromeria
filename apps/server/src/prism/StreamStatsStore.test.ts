import * as NodeServices from "@effect/platform-node/NodeServices";
import { ProviderDriverKind, ProviderTurnId, RunId, ThreadId } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as StreamStatsStore from "./StreamStatsStore.ts";

const storeOver = <E, R>(database: Layer.Layer<SqlClient.SqlClient, E, R>) =>
  StreamStatsStore.layer.pipe(Layer.provide(database));

let turnNumber = 0;
const turn = (
  overrides: Partial<StreamStatsStore.TurnStreamStats> = {},
): StreamStatsStore.TurnStreamStats => {
  turnNumber += 1;
  return {
    threadId: ThreadId.make("thread-1"),
    runId: RunId.make(`run-${turnNumber}`),
    providerTurnId: ProviderTurnId.make("provider-turn-1"),
    provider: ProviderDriverKind.make("codex"),
    model: "gpt-5",
    outcome: "completed",
    startedAt: 1_000,
    endedAt: 9_000,
    timeToFirstTokenMs: 1_500,
    maxGapMs: 2_000,
    eventCount: 40,
    ...overrides,
  };
};

const samples = Effect.gen(function* () {
  const store = yield* StreamStatsStore.StreamStatsStore;
  const rows = yield* store.healthySamples;
  return [...rows].sort((a, b) =>
    `${a.provider}/${a.model}`.localeCompare(`${b.provider}/${b.model}`),
  );
});

describe("StreamStatsStore", () => {
  it.effect("keeps the first measurements of a provider turn when a write is retried", () =>
    Effect.gen(function* () {
      const store = yield* StreamStatsStore.StreamStatsStore;
      const first = turn({ timeToFirstTokenMs: 1_000, maxGapMs: 3_000 });

      expect(yield* store.record(first)).toBe(true);
      expect(yield* store.record({ ...first, timeToFirstTokenMs: 90_000, maxGapMs: 80_000 })).toBe(
        false,
      );
      // The same run's next provider turn is a separate measurement.
      expect(
        yield* store.record({ ...first, providerTurnId: ProviderTurnId.make("provider-turn-2") }),
      ).toBe(true);

      expect(yield* samples).toEqual([
        {
          provider: "codex",
          model: "gpt-5",
          count: 2,
          maxTimeToFirstTokenMs: 1_000,
          maxGapMs: 3_000,
        },
      ]);
    }).pipe(Effect.provide(storeOver(SqlitePersistence.layerMemory))),
  );

  it.effect("counts only completed turns that streamed a first token", () =>
    Effect.gen(function* () {
      const store = yield* StreamStatsStore.StreamStatsStore;
      yield* store.record(turn({ timeToFirstTokenMs: 2_000, maxGapMs: 4_000 }));
      yield* store.record(turn({ timeToFirstTokenMs: 5_000, maxGapMs: 1_000 }));
      yield* store.record(turn({ outcome: "error", timeToFirstTokenMs: 60_000, maxGapMs: 60_000 }));
      yield* store.record(
        turn({ outcome: "aborted", timeToFirstTokenMs: 70_000, maxGapMs: 70_000 }),
      );
      yield* store.record(turn({ timeToFirstTokenMs: null, maxGapMs: 50_000 }));

      expect(yield* samples).toEqual([
        {
          provider: "codex",
          model: "gpt-5",
          count: 2,
          maxTimeToFirstTokenMs: 5_000,
          maxGapMs: 4_000,
        },
      ]);
    }).pipe(Effect.provide(storeOver(SqlitePersistence.layerMemory))),
  );

  it.effect("keeps samples apart by provider and model", () =>
    Effect.gen(function* () {
      const store = yield* StreamStatsStore.StreamStatsStore;
      const claude = ProviderDriverKind.make("claudeAgent");
      yield* store.record(turn({ model: "gpt-5", timeToFirstTokenMs: 1_000, maxGapMs: 1_100 }));
      yield* store.record(
        turn({ model: "gpt-5-mini", timeToFirstTokenMs: 2_000, maxGapMs: 2_200 }),
      );
      yield* store.record(turn({ model: null, timeToFirstTokenMs: 3_000, maxGapMs: 3_300 }));
      yield* store.record(turn({ model: null, timeToFirstTokenMs: 3_500, maxGapMs: 3_000 }));
      yield* store.record(
        turn({ provider: claude, model: "gpt-5", timeToFirstTokenMs: 4_000, maxGapMs: 4_400 }),
      );

      expect(yield* samples).toEqual([
        {
          provider: "claudeAgent",
          model: "gpt-5",
          count: 1,
          maxTimeToFirstTokenMs: 4_000,
          maxGapMs: 4_400,
        },
        {
          provider: "codex",
          model: "gpt-5",
          count: 1,
          maxTimeToFirstTokenMs: 1_000,
          maxGapMs: 1_100,
        },
        {
          provider: "codex",
          model: "gpt-5-mini",
          count: 1,
          maxTimeToFirstTokenMs: 2_000,
          maxGapMs: 2_200,
        },
        { provider: "codex", model: null, count: 2, maxTimeToFirstTokenMs: 3_500, maxGapMs: 3_300 },
      ]);
    }).pipe(Effect.provide(storeOver(SqlitePersistence.layerMemory))),
  );

  it.effect("refuses measurements that are not finite and stores nothing", () =>
    Effect.gen(function* () {
      const store = yield* StreamStatsStore.StreamStatsStore;
      const error = yield* Effect.flip(store.record(turn({ maxGapMs: Number.NaN })));

      expect(error._tag).toBe("StreamStatsWriteError");
      expect(yield* samples).toEqual([]);
    }).pipe(Effect.provide(storeOver(SqlitePersistence.layerMemory))),
  );

  it.effect("serves recorded samples again after a restart", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const dbPath = path.join(yield* fs.makeTempDirectoryScoped(), "chromeria-v2.sqlite");
      const persistence = SqlitePersistence.layerFromPath(dbPath);
      const stats = turn({ timeToFirstTokenMs: 1_200, maxGapMs: 6_000 });

      yield* StreamStatsStore.StreamStatsStore.pipe(
        Effect.flatMap((store) => store.record(stats)),
        Effect.provide(storeOver(persistence)),
      );

      const afterRestart = yield* Effect.gen(function* () {
        const store = yield* StreamStatsStore.StreamStatsStore;
        return {
          retried: yield* store.record({ ...stats, maxGapMs: 99_000 }),
          samples: yield* samples,
        };
      }).pipe(Effect.provide(storeOver(persistence)));

      expect(afterRestart).toEqual({
        retried: false,
        samples: [
          {
            provider: "codex",
            model: "gpt-5",
            count: 1,
            maxTimeToFirstTokenMs: 1_200,
            maxGapMs: 6_000,
          },
        ],
      });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
