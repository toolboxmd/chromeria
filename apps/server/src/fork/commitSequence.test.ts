import { assert, it } from "@effect/vitest";
import { CommandId, EventId, RunId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import { ForkCommitGuardRejected } from "../childThreads/ForkCommitPlan.ts";
import * as Receipts from "../orchestration-v2/CommandReceiptStore.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as Projection from "../orchestration-v2/ProjectionStore.ts";
import * as Outbox from "../orchestration-v2/EffectOutbox.ts";
import * as Database from "../persistence/Sqlite.ts";
import { makeThread, NOW } from "../spectrum/testFixtures.ts";

const stores = Layer.mergeAll(
  EventStore.layer,
  Projection.layer,
  Receipts.layer,
  Outbox.layer,
).pipe(Layer.provideMerge(Database.layerMemory));
const runtime = Layer.fresh(EventSink.layer.pipe(Layer.provideMerge(stores)));
const setup = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE fork_counter (value INTEGER)`;
  yield* sql`INSERT INTO fork_counter VALUES (0)`;
  const sink = yield* EventSink.EventSinkV2;
  const thread = makeThread();
  const created = yield* sink.write({
    events: [
      {
        id: EventId.make("thread:create"),
        type: "thread.created",
        threadId: thread.id,
        occurredAt: NOW,
        payload: thread,
      },
    ],
  });
  const mutation = Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`UPDATE fork_counter SET value = value + 1`.pipe(
      Effect.mapError(
        () => new ForkCommitGuardRejected({ threadId: thread.id, kind: "storage_failure" }),
      ),
    );
  });
  const input = {
    commandId: CommandId.make("state:only"),
    commandType: "fork.state",
    threadId: thread.id,
    acceptedAt: NOW,
    events: [],
    effects: [],
    forkPlans: [{ guards: [], mutations: [mutation] }],
  };
  return { sql, sink, thread, input, sequence: created.at(-1)!.sequence };
});

it.effect(
  "commits state-only commands once without advancing or publishing the event sequence",
  () =>
    Effect.gen(function* () {
      const { sql, sink, thread, input, sequence } = yield* setup;
      const publication = yield* sink
        .stream({ afterSequence: sequence })
        .pipe(Stream.take(1), Stream.runCollect, Effect.forkScoped({ startImmediately: true }));
      const first = yield* sink.commitCommand(input);
      assert.strictEqual(first.receipt.resultSequence, sequence);
      assert.deepStrictEqual(first.storedEvents, []);
      assert.isFalse((yield* sink.commitCommand(input)).committed);
      assert.strictEqual(
        (yield* sql<{ value: number }>`SELECT value FROM fork_counter`)[0]!.value,
        1,
      );
      assert.strictEqual(yield* sink.latestSequence(), sequence);
      const marker = EventId.make("publication:marker");
      yield* sink.write({
        events: [
          {
            id: marker,
            type: "thread.metadata-updated",
            threadId: thread.id,
            occurredAt: NOW,
            payload: thread,
          },
        ],
      });
      assert.deepStrictEqual(
        Array.from(yield* Fiber.join(publication), (event) => event.event.id),
        [marker],
      );
    }).pipe(Effect.provide(runtime)),
);

it.effect("rolls back the reserved receipt and state on guard or later mutation rejection", () =>
  Effect.gen(function* () {
    const { sql, sink, thread, input } = yield* setup;
    const rejected = Effect.fail(
      new ForkCommitGuardRejected({ threadId: thread.id, kind: "state_conflict" }),
    );
    for (const plan of [
      { guards: [rejected], mutations: input.forkPlans[0]!.mutations },
      { guards: [], mutations: [...input.forkPlans[0]!.mutations, rejected] },
    ]) {
      yield* sink.commitCommand({ ...input, forkPlans: [plan] }).pipe(Effect.flip);
      const receipts = yield* Receipts.CommandReceiptStoreV2;
      assert.isTrue(Option.isNone(yield* receipts.getByCommandId(input.commandId)));
      assert.strictEqual(
        (yield* sql<{ value: number }>`SELECT value FROM fork_counter`)[0]!.value,
        0,
      );
    }
  }).pipe(Effect.provide(runtime)),
);

it.effect(
  "rejects an empty command without plans and rejects core effects on a state-only command",
  () =>
    Effect.gen(function* () {
      const { sql, sink, input, thread } = yield* setup;
      yield* sink.commitCommand({ ...input, forkPlans: [] }).pipe(Effect.flip);
      const effect = {
        id: "effect:forbidden",
        commandId: input.commandId,
        threadId: thread.id,
        request: { type: "thread-title.generate" as const, kind: { type: "regenerate" as const } },
      };
      yield* sink.commitCommand({ ...input, effects: [effect] }).pipe(Effect.flip);
      yield* sink
        .commitCommand({
          ...input,
          effects: [effect],
          forkPlans: [
            {
              guards: [Effect.succeed("accept_noop" as const)],
              mutations: [],
            },
          ],
        })
        .pipe(Effect.flip);
      const receipts = yield* Receipts.CommandReceiptStoreV2;
      const outbox = yield* Outbox.EffectOutboxV2;
      assert.isTrue(Option.isNone(yield* receipts.getByCommandId(input.commandId)));
      assert.deepStrictEqual(yield* outbox.listByCommandId(input.commandId), []);
      assert.strictEqual(
        (yield* sql<{ value: number }>`SELECT value FROM fork_counter`)[0]!.value,
        0,
      );
    }).pipe(Effect.provide(runtime)),
);

it.effect("rejects state-only cancellation without changing queued core work", () =>
  Effect.gen(function* () {
    const { sql, sink, input, thread, sequence } = yield* setup;
    const outbox = yield* Outbox.EffectOutboxV2;
    yield* outbox.enqueue([
      {
        id: "effect:existing",
        commandId: CommandId.make("command:existing"),
        threadId: thread.id,
        request: { type: "provider-turn.start", runId: RunId.make("run:existing") },
      },
    ]);
    const before = yield* sql`SELECT * FROM orchestration_v2_effect_outbox`;
    for (const guards of [[], [Effect.succeed("accept_noop" as const)]]) {
      const rejected = yield* sink
        .commitCommand({
          ...input,
          forkPlans: [{ ...input.forkPlans[0]!, guards }],
          cancelUnsettledEffects: {
            effectTypes: ["provider-turn.start"],
            reason: "State-only commands cannot cancel core work",
          },
        })
        .pipe(Effect.flip);
      assert.strictEqual(rejected._tag, "EventSinkWriteError");
      const receipts = yield* Receipts.CommandReceiptStoreV2;
      assert.isTrue(Option.isNone(yield* receipts.getByCommandId(input.commandId)));
      assert.deepStrictEqual(yield* sql`SELECT * FROM orchestration_v2_effect_outbox`, before);
      assert.strictEqual(
        (yield* sql<{ value: number }>`SELECT value FROM fork_counter`)[0]!.value,
        0,
      );
      assert.strictEqual(yield* sink.latestSequence(), sequence);
    }
  }).pipe(Effect.provide(runtime)),
);
