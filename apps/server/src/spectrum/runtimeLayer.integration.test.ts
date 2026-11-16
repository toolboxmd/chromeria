import { assert, it } from "@effect/vitest";
import { CommandId, MessageId, ProviderDriverKind, ProviderInstanceId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Projection from "../orchestration-v2/ProjectionStore.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as Settings from "../serverSettings.ts";
import * as SqlClient from "effect/sql/SqlClient";
import { layer as serialization } from "../orchestration-v2/ThreadCommandExecutor.ts";
import * as Launch from "./LaunchService.ts";
import * as Runtime from "./runtimeLayer.ts";
import { base, input, setup } from "./controllerTestkit.ts";

it.effect(
  "the runtime admission layer supplies native dispatch with the registered-transcript exemption",
  () =>
    Effect.gen(function* () {
      yield* setup;
      const state = yield* Effect.gen(function* () {
        return yield* (yield* Launch.SpectrumLaunchService).register(input);
      }).pipe(Effect.provide(Launch.layer));
      const settings = Settings.layerTest({
        providerInstances: {
          [ProviderInstanceId.make("codex")]: {
            driver: ProviderDriverKind.make("codex"),
            enabled: false,
          },
        },
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
        assert.deepStrictEqual(yield* threads.dispatch(command), { ...first, replayed: true });
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
      }).pipe(Effect.provide(graph));
    }).pipe(Effect.provide(Layer.merge(base, serialization))),
);
