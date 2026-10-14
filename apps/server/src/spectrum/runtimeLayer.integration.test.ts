import { assert, it } from "@effect/vitest";
import { CommandId, EventId, MessageId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as Sink from "../orchestration-v2/EventSink.ts";
import * as Projection from "../orchestration-v2/ProjectionStore.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as Settings from "../serverSettings.ts";
import * as SqlClient from "effect/sql/SqlClient";
import { layer as serialization } from "../orchestration-v2/ThreadCommandExecutor.ts";
import { Scheduler } from "../scheduling/Scheduler.ts";
import * as Launch from "./LaunchService.ts";
import * as Runtime from "./runtimeLayer.ts";
import { base, input, setup } from "./controllerTestkit.ts";
import { NOW } from "./testFixtures.ts";

it.effect(
  "the runtime admission layer supplies native dispatch with the registered-transcript exemption",
  () =>
    Effect.gen(function* () {
      yield* setup;
      const state = yield* Effect.gen(function* () {
        return yield* (yield* Launch.SpectrumLaunchService).register(input);
      }).pipe(Effect.provide(Launch.layer));
      const settings = Settings.layerTest({
        providerInstances: { codex: { driver: "codex", enabled: false } },
      });
      const command = {
        type: "message.dispatch" as const,
        commandId: CommandId.make("runtime:disabled:append"),
        messageId: MessageId.make("runtime:disabled:append"),
        threadId: state.threadId,
        text: "Append through the production admission composition.",
        attachments: [],
        createdBy: "user" as const,
        creationSource: "web" as const,
        dispatchMode: { type: "start_immediately" as const },
      };
      const defaultGate = Layer.fresh(Threads.layer).pipe(Layer.provide(settings));
      const withoutAdmission = yield* Effect.gen(function* () {
        return yield* (yield* Threads.ThreadManagementService).dispatch(command);
      }).pipe(Effect.provide(defaultGate), Effect.flip);
      assert.instanceOf(withoutAdmission, Threads.ThreadManagementProviderInstanceDisabledError);

      const native = Layer.fresh(Threads.layer).pipe(
        Layer.provideMerge(Layer.merge(Runtime.layerAdmission, settings)),
      );
      const graph = Runtime.layer.pipe(Layer.provideMerge(native));
      yield* Effect.gen(function* () {
        const threads = yield* Threads.ThreadManagementService;
        const first = yield* threads.dispatch(command);
        assert.deepStrictEqual(yield* threads.dispatch(command), first);
        const projections = yield* Projection.ProjectionStoreV2;
        const records = yield* projections.getThreadRecords(state.threadId, ["runs", "messages"]);
        assert.deepStrictEqual(records.runs, []);
        assert.strictEqual(
          records.messages.filter((message) => message.id === command.messageId).length,
          1,
        );
        assert.strictEqual(
          records.messages.find((message) => message.id === command.messageId)!.text,
          command.text,
        );
        const sql = yield* SqlClient.SqlClient;
        assert.deepStrictEqual(
          yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE thread_id=${state.threadId}`,
          [],
        );
      }).pipe(
        Effect.provide(graph),
        Effect.provideService(Scheduler, { register: () => Effect.void }),
      );
    }).pipe(Effect.provide(Layer.merge(base, serialization))),
);

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
