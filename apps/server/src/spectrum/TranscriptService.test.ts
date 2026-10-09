import { assert, it } from "@effect/vitest";
import { CommandId, EventId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import * as CommandReceiptStore from "../orchestration-v2/CommandReceiptStore.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as TranscriptService from "./TranscriptService.ts";
import { ensureSpectrumSchema, insertSpectrum, readSpectrum } from "./store.ts";
import { APPEND, makeState, makeThread, NOW } from "./testFixtures.ts";

const stores = Layer.mergeAll(
  EventStore.layer,
  ProjectionStore.layer,
  CommandReceiptStore.layer,
).pipe(Layer.provideMerge(SqlitePersistence.layerMemory));
const runtime = Layer.fresh(
  TranscriptService.layer.pipe(
    Layer.provideMerge(EventSink.layer.pipe(Layer.provideMerge(stores))),
  ),
);

const setup = Effect.gen(function* () {
  yield* ensureSpectrumSchema;
  yield* ensureSpectrumSchema;
  yield* insertSpectrum(makeState({ outbox: [APPEND] }));
  const sink = yield* EventSink.EventSinkV2;
  const thread = makeThread();
  yield* sink.write({
    events: [
      {
        id: EventId.make("spectrum:create"),
        type: "thread.created",
        threadId: thread.id,
        occurredAt: NOW,
        payload: thread,
      },
    ],
  });
  return { sink, thread };
});

it.effect("rejects an inherited ancestor retirement before consuming transcript work", () =>
  Effect.gen(function* () {
    const { sink, thread } = yield* setup;
    const parent = { ...makeThread(), id: makeState().callerThreadId };
    yield* sink.write({
      events: [
        {
          id: EventId.make("ancestor:create"),
          type: "thread.created",
          threadId: parent.id,
          occurredAt: NOW,
          payload: {
            ...parent,
            lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: parent.id },
            forkRetirement: { token: CommandId.make("ancestor:stop") },
          },
        },
        {
          id: EventId.make("spectrum:lineage"),
          type: "thread.metadata-updated",
          threadId: thread.id,
          occurredAt: NOW,
          payload: {
            ...thread,
            lineage: {
              parentThreadId: parent.id,
              relationshipToParent: "subagent",
              rootThreadId: parent.id,
            },
          },
        },
      ],
    });
    const service = yield* TranscriptService.SpectrumTranscriptService;
    yield* service.dispatch(APPEND.threadId, APPEND.commandId).pipe(Effect.flip);
    assert.deepStrictEqual(Option.getOrThrow(yield* readSpectrum(APPEND.threadId)).outbox, [
      APPEND,
    ]);
    const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
    assert.isTrue(Option.isNone(yield* receipts.getByCommandId(APPEND.commandId)));
  }).pipe(Effect.provide(runtime)),
);

it.effect(
  "dispatches durable transcript data and replays the receipt after outbox consumption",
  () =>
    Effect.gen(function* () {
      yield* setup;
      const service = yield* TranscriptService.SpectrumTranscriptService;
      const first = yield* service.dispatch(APPEND.threadId, APPEND.commandId);
      const state = Option.getOrThrow(yield* readSpectrum(APPEND.threadId));
      assert.deepStrictEqual(state.outbox, []);
      assert.deepStrictEqual(state.transcript, [APPEND.messageId]);
      assert.deepStrictEqual(yield* service.dispatch(APPEND.threadId, APPEND.commandId), first);
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const { messages, runs } = yield* projections.getThreadRecords(APPEND.threadId, [
        "messages",
        "runs",
      ]);
      assert.strictEqual(messages.length, 1);
      assert.strictEqual(messages[0]!.text, APPEND.text);
      assert.deepStrictEqual(runs, []);
    }).pipe(Effect.provide(runtime)),
);

it.effect("refuses commands absent from durable state without creating a receipt or message", () =>
  Effect.gen(function* () {
    yield* setup;
    const service = yield* TranscriptService.SpectrumTranscriptService;
    const id = CommandId.make("forged:append");
    yield* service.dispatch(APPEND.threadId, id).pipe(Effect.flip);
    const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
    assert.isTrue(Option.isNone(yield* receipts.getByCommandId(id)));
    assert.deepStrictEqual(Option.getOrThrow(yield* readSpectrum(APPEND.threadId)).outbox, [
      APPEND,
    ]);
  }).pipe(Effect.provide(runtime)),
);

it.effect("does not append or revive a registered Spectrum retired by subtree Stop", () =>
  Effect.gen(function* () {
    const { sink, thread } = yield* setup;
    yield* sink.write({
      events: [
        {
          id: EventId.make("spectrum:stopped"),
          type: "thread.metadata-updated",
          threadId: thread.id,
          occurredAt: NOW,
          payload: { ...thread, forkRetirement: { token: CommandId.make("stop:1") } },
        },
      ],
    });
    const service = yield* TranscriptService.SpectrumTranscriptService;
    const error = yield* service.dispatch(APPEND.threadId, APPEND.commandId).pipe(Effect.flip);
    assert.instanceOf(error, TranscriptService.SpectrumTranscriptError);
    const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
    assert.isTrue(Option.isNone(yield* receipts.getByCommandId(APPEND.commandId)));
    assert.strictEqual(Option.getOrThrow(yield* readSpectrum(APPEND.threadId)).revision, 0);
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const { messages } = yield* projections.getThreadRecords(APPEND.threadId, ["messages"]);
    assert.deepStrictEqual(messages, []);
    assert.deepStrictEqual(
      (yield* projections.getThread(APPEND.threadId)).forkResumedRetirements ?? [],
      [],
    );
  }).pipe(Effect.provide(runtime)),
);
