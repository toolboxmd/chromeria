import { assert, it } from "@effect/vitest";
import { EventId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as Sink from "../orchestration-v2/EventSink.ts";
import * as Projection from "../orchestration-v2/ProjectionStore.ts";
import { layer as serialization } from "../orchestration-v2/ThreadCommandExecutor.ts";
import { Scheduler } from "../scheduling/Scheduler.ts";
import * as Launch from "./LaunchService.ts";
import * as Runtime from "./runtimeLayer.ts";
import { base, input, setup } from "./controllerTestkit.ts";
import { NOW } from "./testFixtures.ts";

it.effect(
  "the Spectrum runtime uses the shared scheduler, without subscribing to global event bursts",
  () =>
    Effect.gen(function* () {
      const sink = yield* Sink.EventSinkV2;
      let tick = Effect.void;
      let registrations = 0;
      let subscriptions = 0;
      const register: Scheduler["Service"]["register"] = <E, R>(
        name: string,
        work: Effect.Effect<void, E, R>,
      ) =>
        Effect.gen(function* () {
          assert.strictEqual(name, "spectrum");
          registrations++;
          tick = work.pipe(Effect.provideContext(yield* Effect.context<R>()), Effect.orDie);
        });
      yield* Effect.gen(function* () {
        yield* setup;
        const state = yield* (yield* Launch.SpectrumLaunchService).register(input);
        const projections = yield* Projection.ProjectionStoreV2;
        const thread = yield* projections.getThread(input.callerThreadId);
        for (let index = 0; index < 30; index++)
          yield* sink.write({
            events: [
              {
                id: EventId.make(`global:burst:${index}`),
                type: "thread.metadata-updated",
                threadId: thread.id,
                occurredAt: NOW,
                payload: thread,
              },
            ],
          });
        assert.strictEqual(registrations, 1);
        assert.strictEqual(subscriptions, 0);
        // Publishing a burst schedules no individual sweep; the registered shared tick advances it.
        assert.deepStrictEqual(
          (yield* projections.getThreadRecords(state.participants[0]!.threadId, ["runs"])).runs,
          [],
        );
        yield* tick;
        assert.strictEqual(
          (yield* projections.getThreadRecords(state.participants[0]!.threadId, ["runs"])).runs
            .length,
          1,
        );
      }).pipe(
        Effect.provide(Layer.mergeAll(Runtime.layer, Runtime.layerAdmission)),
        Effect.provideService(Scheduler, { register }),
        Effect.provideService(Sink.EventSinkV2, {
          ...sink,
          stream: () => {
            subscriptions++;
            return Stream.empty;
          },
        }),
      );
    }).pipe(Effect.provide(Layer.mergeAll(base, serialization))),
);
