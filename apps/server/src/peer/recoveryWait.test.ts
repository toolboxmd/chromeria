import * as Ref from "effect/Ref";
import { errorFor } from "../prism/recovery.testkit.ts";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  RunId,
  type OrchestrationV2Run,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as Store from "../prism/RecoveryStore.ts";
import * as Projection from "../orchestration-v2/ProjectionStore.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as CommandReceiptStore from "../orchestration-v2/CommandReceiptStore.ts";
import * as ProviderSessionManager from "../orchestration-v2/ProviderSessionManager.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as Coordinator from "../prism/RecoveryCoordinator.ts";
import * as Hooks from "../prism/RecoveryHooks.ts";
import * as History from "../prism/RecoveryHistory.ts";
import * as Settings from "../serverSettings.ts";
import {
  recoveryTestLayer,
  recoveryRun as source,
  recoveryEvents,
  threadFor,
} from "../prism/recovery.testkit.ts";
import * as Wait from "./recoveryWait.ts";
import * as Signals from "./recoveryWaitSignals.ts";

const dependencies = Layer.mergeAll(
  recoveryTestLayer,
  Signals.layerSignals,
  Coordinator.layer.pipe(
    Layer.provide(Layer.mergeAll(recoveryTestLayer, Settings.layerTest(), Signals.layerSignals)),
  ),
);
const layerWait = Wait.layer.pipe(Layer.provide(Hooks.layer), Layer.provideMerge(dependencies));
const persist = Effect.fn("waitTest.persist")(function* (
  key: string,
  events: ReadonlyArray<OrchestrationV2DomainEvent>,
) {
  yield* (yield* EventSink.EventSinkV2).commitCommand({
    commandId: CommandId.make(key),
    commandType: "fixture",
    threadId: source.threadId,
    acceptedAt: source.requestedAt,
    events,
    effects: [],
  });
});
const successor: OrchestrationV2Run = {
  ...source,
  id: RunId.make("run:successor"),
  ordinal: 2,
  status: "completed",
  forkPrismContinuationSourceRunId: source.id,
};
const admit = persist("successor", [
  {
    id: EventId.make("successor"),
    type: "run.created",
    threadId: source.threadId,
    occurredAt: source.requestedAt,
    payload: successor,
  },
]);

const makeThreads = (
  ready: Deferred.Deferred<void>,
  onRead: Effect.Effect<void> = Effect.void,
  beforeSubscribe: Effect.Effect<void> = Effect.void,
) =>
  Effect.gen(function* () {
    const projections = yield* Projection.ProjectionStoreV2;
    const events = yield* EventSink.EventSinkV2;
    return yield* Threads.ThreadManagementService.pipe(
      Effect.provide(
        Threads.layer.pipe(
          Layer.provide(
            Layer.mock(Orchestrator.OrchestratorV2)({
              restartContinuationPending: () => Effect.succeed(false),
              getThreadEventSequence: (threadId) =>
                events
                  .latestSequence({ threadId })
                  .pipe(
                    Effect.mapError(
                      (cause) => new Orchestrator.OrchestratorProjectionError({ threadId, cause }),
                    ),
                  ),
              getThreadRecords: (threadId, fields, filter) =>
                projections.getThreadRecords(threadId, fields, filter).pipe(
                  Effect.tap(() => onRead),
                  Effect.mapError(
                    (cause) => new Orchestrator.OrchestratorProjectionError({ threadId, cause }),
                  ),
                ),
              streamStoredEventsFrom: (input) =>
                Stream.fromEffect(
                  Deferred.succeed(ready, undefined).pipe(Effect.andThen(beforeSubscribe)),
                ).pipe(
                  Stream.drain,
                  Stream.concat(
                    events.stream(input).pipe(
                      Stream.mapError(
                        (cause) =>
                          new Orchestrator.OrchestratorProjectionError({
                            threadId: source.threadId,
                            cause,
                          }),
                      ),
                    ),
                  ),
                ),
            }),
          ),
          Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
          Layer.provide(Settings.layerTest()),
          Layer.provide(CommandReceiptStore.layer),
        ),
      ),
    );
  });
const wait = (threads: Threads.ThreadManagementService["Service"]) =>
  threads.waitForThread({
    projectId: ProjectId.make("project:recovery"),
    threadId: source.threadId,
    runId: source.id,
    timeoutMs: 1000,
  });
import { ProjectId } from "@t3tools/contracts";

it.effect("wait follows a recorded native restart into a completed Prism successor", () =>
  Effect.gen(function* () {
    const cut: OrchestrationV2Run = {
      ...source,
      id: RunId.make("run:native-cut"),
      status: "cancelled",
    };
    const resumed: OrchestrationV2Run = {
      ...source,
      ordinal: 2,
      restartContinuationOfRunId: cut.id,
    };
    const recovered: OrchestrationV2Run = { ...successor, ordinal: 3 };
    const event = { threadId: source.threadId, occurredAt: source.requestedAt };
    yield* persist("native-recovery-seed", [
      {
        ...event,
        id: EventId.make("native-recovery-thread"),
        type: "thread.created",
        payload: threadFor(cut),
      },
      { ...event, id: EventId.make("native-cut"), type: "run.created", payload: cut },
      { ...event, id: EventId.make("native-resumed"), type: "run.created", payload: resumed },
      {
        ...event,
        id: EventId.make("native-resumed-error"),
        type: "turn-item.updated",
        payload: errorFor(resumed),
      },
      {
        ...event,
        id: EventId.make("native-prism-successor"),
        type: "run.created",
        payload: recovered,
      },
    ]);
    const threads = yield* makeThreads(yield* Deferred.make<void>());
    const result = yield* threads.waitForThread({
      projectId: ProjectId.make("project:recovery"),
      threadId: source.threadId,
      runId: cut.id,
      timeoutMs: 1000,
    });
    assert.equal(result.run?.id, RunId.make("run:successor"));
    assert.equal(result.run?.status, "completed");
    assert.isFalse(result.timedOut);
  }).pipe(Effect.provide(layerWait)),
);

it.effect("wait observes recovery first and follows admission and successor completion", () =>
  Effect.scoped(
    Effect.gen(function* () {
      yield* persist("seed", recoveryEvents(source));
      const ready = yield* Deferred.make<void>();
      const threads = yield* makeThreads(ready);
      const fiber = yield* wait(threads).pipe(Effect.forkScoped);
      yield* Deferred.await(ready);
      assert.equal((yield* History.readRecoveryState(source.id)).decision?.status, "pending");
      yield* admit;
      const result = yield* Fiber.join(fiber);
      assert.equal(result.run?.id, successor.id);
      assert.equal(result.run?.status, "completed");
      assert.isFalse(result.timedOut);
    }),
  ).pipe(Effect.provide(layerWait)),
);

it.effect("wait replays a successor committed after its cursor but before event subscription", () =>
  Effect.scoped(
    Effect.gen(function* () {
      yield* persist("seed", recoveryEvents(source));
      const events = yield* EventSink.EventSinkV2;
      assert.isAbove(yield* events.latestSequence({ threadId: source.threadId }), 0);
      const ready = yield* Deferred.make<void>();
      const subscribe = yield* Deferred.make<void>();
      const threads = yield* makeThreads(ready, Effect.void, Deferred.await(subscribe));
      const fiber = yield* wait(threads).pipe(Effect.forkScoped);
      yield* Deferred.await(ready);
      yield* admit;
      yield* Deferred.succeed(subscribe, undefined);
      const result = yield* Fiber.join(fiber);
      assert.equal(result.run?.id, successor.id);
      assert.equal(result.run?.status, "completed");
      assert.isFalse(result.timedOut);
    }),
  ).pipe(Effect.provide(layerWait)),
);

it.effect(
  "wait after successor completion follows recorded chains and ignores a newer unrelated run",
  () =>
    Effect.gen(function* () {
      yield* persist("seed", recoveryEvents(source));
      yield* admit;
      const failed = { ...successor, status: "failed" as const };
      const finalRun = {
        ...successor,
        id: RunId.make("run:final"),
        ordinal: 3,
        forkPrismContinuationSourceRunId: successor.id,
      };
      const unrelated = {
        ...successor,
        id: RunId.make("run:unrelated"),
        ordinal: 4,
        forkPrismContinuationSourceRunId: undefined,
      };
      yield* persist("chain", [
        {
          id: EventId.make("failed-successor"),
          type: "run.updated",
          threadId: source.threadId,
          occurredAt: source.requestedAt,
          payload: failed,
        },
        {
          id: EventId.make("failed-successor-error"),
          type: "turn-item.updated",
          threadId: source.threadId,
          occurredAt: source.requestedAt,
          payload: errorFor(failed),
        },
        {
          id: EventId.make("final-run"),
          type: "run.created",
          threadId: source.threadId,
          occurredAt: source.requestedAt,
          payload: finalRun,
        },
        {
          id: EventId.make("unrelated"),
          type: "run.created",
          threadId: source.threadId,
          occurredAt: source.requestedAt,
          payload: unrelated,
        },
      ]);
      const threads = yield* makeThreads(yield* Deferred.make<void>());
      const result = yield* wait(threads);
      assert.equal(result.run?.id, finalRun.id);
      assert.isFalse(result.timedOut);
    }).pipe(Effect.provide(layerWait)),
);

it.effect("one timeout budget expires while recovery is pending", () =>
  Effect.scoped(
    Effect.gen(function* () {
      yield* persist("seed", recoveryEvents(source));
      const ready = yield* Deferred.make<void>();
      const reads = yield* Ref.make(0);
      const threads = yield* makeThreads(
        ready,
        Ref.update(reads, (count) => count + 1),
      );
      const fiber = yield* wait(threads).pipe(Effect.forkScoped);
      yield* Deferred.await(ready);
      yield* TestClock.adjust(Duration.millis(1000));
      const result = yield* Fiber.join(fiber);
      assert.equal(result.run?.id, source.id);
      assert.isTrue(result.timedOut);
      // Initial read, decision notification, and timeout read. No polling or self-wake loop.
      assert.isAbove(yield* Ref.get(reads), 0);
      assert.isAtMost(yield* Ref.get(reads), 4);
    }),
  ).pipe(Effect.provide(layerWait)),
);

it.effect("a recovery decision closes without a successor and wakes the existing wait", () =>
  Effect.scoped(
    Effect.gen(function* () {
      yield* persist("seed", recoveryEvents(source));
      const ready = yield* Deferred.make<void>();
      const threads = yield* makeThreads(ready);
      const fiber = yield* wait(threads).pipe(Effect.forkScoped);
      yield* Deferred.await(ready);
      const store = yield* Store.RecoveryStore;
      const record = yield* store.get(source.threadId);
      if (record !== null) yield* store.close(record);
      yield* History.writeRecoveryOutcome({
        sourceRunId: source.id,
        threadId: source.threadId,
        status: "decided",
        outcome: "not_retryable",
        reason: "opted_out",
      });
      // A decided outcome closes the historical source even if the coordinator previously held it.
      const result = yield* Fiber.join(fiber);
      assert.equal(result.run?.id, source.id);
      assert.equal(result.run?.status, "failed");
      assert.isFalse(result.timedOut);
    }),
  ).pipe(Effect.provide(layerWait)),
);
