import { assert, describe, it } from "@effect/vitest";
import { MessageId, RunId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Settings from "../serverSettings.ts";
import * as Persistence from "../persistence/Sqlite.ts";
import * as Coordinator from "./RecoveryCoordinator.ts";
import { recoveryRun as run, threadFor, errorFor } from "./recovery.testkit.ts";
const layer = Coordinator.layer.pipe(
  Layer.provide(Layer.mergeAll(Persistence.layerMemory, Settings.layerTest())),
);
const projection = {
  thread: threadFor(run),
  runs: [run],
  turnItems: [errorFor(run)],
  runtimeRequests: [],
};

describe("Prism recovery finalization coordination", () => {
  it.effect("holds the first failed result and releases only the retry's eventual result", () =>
    Effect.gen(function* () {
      const coordinator = yield* Coordinator.RecoveryCoordinator;
      const armed = yield* coordinator.observe(projection, false);
      assert.strictEqual(armed?.state, "retry_pending");
      assert.strictEqual(yield* coordinator.holdsResult(run.threadId), true);
      const retry = {
        ...run,
        id: RunId.make("run:retry"),
        ordinal: 2,
        userMessageId: armed!.retryMessageId,
        status: "running" as const,
        completedAt: null,
      };
      yield* coordinator.observe({ ...projection, runs: [run, retry] }, false);
      assert.strictEqual(yield* coordinator.holdsResult(run.threadId), true);
      yield* coordinator.observe(
        {
          ...projection,
          runs: [run, { ...retry, status: "completed", completedAt: run.completedAt }],
        },
        false,
      );
      assert.strictEqual(yield* coordinator.holdsResult(run.threadId), false);
      // A delayed observer must not replace the eventual result with the first failure.
      yield* coordinator.observe(projection, false);
      assert.strictEqual(yield* coordinator.holdsResult(run.threadId), false);
    }).pipe(Effect.provide(layer)),
  );
  it.effect(
    "reset wait survives an active reset continuation and releases its completed result",
    () =>
      Effect.gen(function* () {
        const coordinator = yield* Coordinator.RecoveryCoordinator;
        const item = errorFor(run);
        if (item.type !== "error") throw new Error("fixture");
        const limited = {
          ...projection,
          turnItems: [
            {
              ...item,
              failure: {
                ...item.failure,
                class: "usage_limit" as const,
                resetAt: "2026-10-08T11:00:00Z",
              },
            },
          ],
        };
        const waiting = yield* coordinator.observe(limited, false);
        assert.strictEqual(waiting?.state, "reset_wait");
        assert.strictEqual(yield* coordinator.holdsResult(run.threadId), true);
        const resumed = {
          ...run,
          id: RunId.make("run:reset"),
          ordinal: 2,
          userMessageId: MessageId.make(`limit-resume:${run.threadId}:${run.id}:reset`),
          status: "running" as const,
          completedAt: null,
        };
        yield* coordinator.observe({ ...limited, runs: [run, resumed] }, false);
        assert.strictEqual(yield* coordinator.holdsResult(run.threadId), true);
        yield* coordinator.observe(
          {
            ...limited,
            runs: [run, { ...resumed, status: "completed", completedAt: run.completedAt }],
          },
          false,
        );
        assert.strictEqual(yield* coordinator.holdsResult(run.threadId), false);
      }).pipe(Effect.provide(layer)),
  );
  it.effect(
    "retirement or incomplete ancestry releases a held failure without creating another retry",
    () =>
      Effect.gen(function* () {
        const coordinator = yield* Coordinator.RecoveryCoordinator;
        yield* coordinator.observe(projection, false);
        assert.strictEqual(yield* coordinator.holdsResult(run.threadId), true);
        const closed = yield* coordinator.observe(projection, true);
        assert.strictEqual(closed?.state, "closed");
        assert.strictEqual(yield* coordinator.holdsResult(run.threadId), false);
      }).pipe(Effect.provide(layer)),
  );
});
