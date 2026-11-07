import { assert, it, vi } from "@effect/vitest";
import {
  CheckpointId,
  CommandId,
  EventId,
  MessageId,
  NodeId,
  RunId,
  ThreadId,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";

import * as CommandReceiptStore from "../orchestration-v2/CommandReceiptStore.ts";
import * as EffectOutbox from "../orchestration-v2/EffectOutbox.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import { recoveryRun, recoveryTestLayer, threadFor } from "../prism/recovery.testkit.ts";
import { RESTART_CUT_REASONS } from "@t3tools/shared/restartCut";

const cutAt = DateTime.makeUnsafe("2026-10-14T10:00:00Z");
const later = DateTime.makeUnsafe("2026-10-14T10:01:00Z");
const layerTest = Layer.mergeAll(
  recoveryTestLayer,
  CommandReceiptStore.layer.pipe(Layer.provide(recoveryTestLayer)),
  EffectOutbox.layer.pipe(Layer.provide(recoveryTestLayer)),
);

function sourceFor(id: string): OrchestrationV2Run {
  return {
    ...recoveryRun,
    id: RunId.make(`run:${id}`),
    threadId: ThreadId.make(`thread:${id}`),
    status: "cancelled",
    completedAt: cutAt,
    restartCut: { continuation: "scheduled", cutAt },
  };
}

const seed = Effect.fnUntraced(function* (run: OrchestrationV2Run) {
  const sink = yield* EventSink.EventSinkV2;
  yield* sink.write({
    events: [
      {
        id: EventId.make(`event:thread:${run.id}`),
        type: "thread.created",
        threadId: run.threadId,
        occurredAt: cutAt,
        payload: threadFor(run),
      },
      {
        id: EventId.make(`event:run:${run.id}`),
        type: "run.created",
        threadId: run.threadId,
        occurredAt: cutAt,
        payload: run,
      },
    ],
  });
});

function declineInput(
  run: OrchestrationV2Run,
): Parameters<EventSink.EventSinkV2Shape["commitCommand"]>[0] {
  return {
    commandId: CommandId.make(`command:restart-cut-decline:${run.id}`),
    threadId: run.threadId,
    commandType: "run.restart-cut.decline",
    acceptedAt: later,
    events: [],
    effects: [],
    restartCutDecline: {
      runId: run.id,
      eventId: EventId.make(`event:decline:${run.id}`),
      reason: RESTART_CUT_REASONS.EFFECT_FAILED,
    },
  };
}

const rawRun = Effect.fnUntraced(function* (run: OrchestrationV2Run) {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ readonly payload_json: string }>`
    SELECT payload_json FROM orchestration_v2_projection_runs WHERE run_id = ${run.id}
  `;
  return JSON.parse(rows[0]?.payload_json ?? "null");
});

it.effect(
  "a standalone decline patches only metadata and publishes the fresh row with its receipt",
  () =>
    Effect.gen(function* () {
      const sink = yield* EventSink.EventSinkV2;
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
      const events = yield* EventStore.EventStoreV2;
      const source = { ...sourceFor("fresh-fields"), completedAt: later };
      yield* seed(source);
      const fresh = {
        ...source,
        rootNodeId: NodeId.make("node:fresh-root"),
        checkpointId: CheckpointId.make("checkpoint:new"),
        delegatedCompletion: {
          disposition: "open" as const,
          nextGeneration: 2,
          delivery: { generation: 1, messageId: MessageId.make("message:delivered"), taskIds: [] },
        },
        restartCancelledBackgroundWork: [
          { kind: "shell" as const, label: "Build", id: "shell:new" },
        ],
      };
      yield* sink.write({
        events: [
          {
            id: EventId.make("event:fresh-fields"),
            type: "run.updated",
            threadId: source.threadId,
            occurredAt: later,
            payload: fresh,
          },
        ],
      });
      const before = yield* rawRun(source);
      const input = declineInput(source);
      const result = yield* Effect.acquireUseRelease(
        Effect.sync(() => vi.spyOn(events, "publishCommitted")),
        (publish) =>
          sink.commitCommand(input).pipe(
            Effect.tap((committed) =>
              Effect.sync(() => {
                assert.deepEqual(
                  publish.mock.calls.flatMap(([published]) => [...published]),
                  [...committed.storedEvents],
                );
              }),
            ),
          ),
        (publish) => Effect.sync(() => publish.mockRestore()),
      );
      assert.isTrue(result.committed);
      assert.lengthOf(result.storedEvents, 1);
      const stored = result.storedEvents[0];
      assert.strictEqual(stored?.commandId, input.commandId);
      assert.strictEqual(stored?.event.id, input.restartCutDecline?.eventId);
      assert.strictEqual(result.receipt.resultSequence, stored?.sequence);
      assert.deepEqual(
        Option.getOrThrow(yield* receipts.getByCommandId(input.commandId)),
        result.receipt,
      );
      const after = yield* rawRun(source);
      const { restartCut: beforeCut, ...beforeFields } = before;
      const { restartCut: afterCut, ...afterFields } = after;
      assert.deepEqual(afterFields, beforeFields);
      assert.deepEqual(afterCut, {
        continuation: "declined",
        cutAt: beforeCut.cutAt,
        reason: RESTART_CUT_REASONS.EFFECT_FAILED,
      });
      const persisted = (yield* store.getThreadProjection(source.threadId)).runs[0];
      assert.deepEqual(persisted, {
        ...fresh,
        restartCut: { continuation: "declined", cutAt, reason: RESTART_CUT_REASONS.EFFECT_FAILED },
      });
      assert.deepEqual(stored?.event.payload, persisted);
      const persistedEvents = yield* sink
        .readByCommandId({ commandId: input.commandId })
        .pipe(Stream.runCollect);
      assert.deepEqual(persistedEvents, result.storedEvents);
      assert.strictEqual(persistedEvents[0]?.event.runId, fresh.id);
      assert.strictEqual(persistedEvents[0]?.event.providerInstanceId, fresh.providerInstanceId);
      assert.strictEqual(persistedEvents[0]?.event.nodeId, fresh.rootNodeId);
      const repeated = yield* sink.commitCommand(input);
      assert.isFalse(repeated.committed);
      assert.deepEqual(repeated.storedEvents, result.storedEvents);
    }).pipe(Effect.provide(layerTest)),
);

it.effect.each(["running", "completed", "failed", "interrupted", "rolled_back"] as const)(
  "a fresh %s source skips decline without consuming the stable receipt",
  (status) =>
    Effect.gen(function* () {
      const sink = yield* EventSink.EventSinkV2;
      const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
      const source = sourceFor(`fresh-${status}`);
      yield* seed(source);
      yield* sink.write({
        events: [
          {
            id: EventId.make(`event:new-state:${status}`),
            type: "run.updated",
            threadId: source.threadId,
            occurredAt: later,
            payload: { ...source, status },
          },
        ],
      });
      const before = yield* rawRun(source);
      const input = declineInput(source);
      const skipped = yield* sink.commitCommand(input);
      assert.isFalse(skipped.committed);
      assert.lengthOf(skipped.storedEvents, 0);
      assert.isTrue(Option.isNone(yield* receipts.getByCommandId(input.commandId)));
      assert.deepEqual(yield* rawRun(source), before);
      yield* sink.write({
        events: [
          {
            id: EventId.make(`event:cut-state:${status}`),
            type: "run.updated",
            threadId: source.threadId,
            occurredAt: later,
            payload: source,
          },
        ],
      });
      assert.isTrue((yield* sink.commitCommand(input)).committed);
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "missing, foreign, unrelated cancelled and already-declined sources remain untouched",
  () =>
    Effect.gen(function* () {
      const sink = yield* EventSink.EventSinkV2;
      const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
      const missing = sourceFor("missing");
      assert.isFalse((yield* sink.commitCommand(declineInput(missing))).committed);
      const source = sourceFor("ownership");
      yield* seed(source);
      const foreign = { ...declineInput(source), threadId: ThreadId.make("thread:foreign") };
      assert.isFalse((yield* sink.commitCommand(foreign)).committed);
      assert.isTrue(Option.isNone(yield* receipts.getByCommandId(foreign.commandId)));
      const unrelated = { ...sourceFor("unrelated"), restartCut: undefined };
      yield* seed(unrelated);
      assert.isFalse((yield* sink.commitCommand(declineInput(unrelated))).committed);
      const original = yield* sink.commitCommand(declineInput(source));
      assert.isTrue(original.committed);
      const another = {
        ...declineInput(source),
        commandId: CommandId.make("command:second-decline"),
      };
      assert.isFalse((yield* sink.commitCommand(another)).committed);
      assert.isTrue(Option.isNone(yield* receipts.getByCommandId(another.commandId)));
      assert.deepEqual((yield* rawRun(source)).restartCut, {
        continuation: "declined",
        cutAt: DateTime.formatIso(cutAt),
        reason: RESTART_CUT_REASONS.EFFECT_FAILED,
      });
    }).pipe(Effect.provide(layerTest)),
);

it.effect("any linked replacement owns the outcome before a late decline", () =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
    for (const status of ["starting", "running", "failed", "cancelled", "rolled_back"] as const) {
      const source = sourceFor(`replacement-${status}`);
      yield* seed(source);
      yield* sink.write({
        events: [
          {
            id: EventId.make(`event:replacement:${status}`),
            type: "run.created",
            threadId: source.threadId,
            occurredAt: later,
            payload: {
              ...source,
              id: RunId.make(`run:replacement:${status}`),
              ordinal: 2,
              status,
              startedAt: null,
              restartCut: undefined,
              restartContinuationOfRunId: source.id,
            },
          },
        ],
      });
      const input = declineInput(source);
      assert.isFalse((yield* sink.commitCommand(input)).committed);
      assert.isTrue(Option.isNone(yield* receipts.getByCommandId(input.commandId)));
      assert.strictEqual((yield* rawRun(source)).restartCut.continuation, "scheduled");
    }
  }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "legacy ownership requires exact stable id, thread, type and source and keeps completedAt",
  () =>
    Effect.gen(function* () {
      const sink = yield* EventSink.EventSinkV2;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      for (const mismatch of ["none", "id", "thread", "type", "source"] as const) {
        const run = { ...sourceFor(`legacy-${mismatch}`), restartCut: undefined };
        yield* seed(run);
        yield* outbox.enqueue([
          {
            id:
              mismatch === "id"
                ? `effect:unrelated:${run.id}`
                : `effect:restart-continuation:${run.id}`,
            commandId: CommandId.make(`command:legacy:${mismatch}`),
            threadId: mismatch === "thread" ? ThreadId.make("thread:unrelated") : run.threadId,
            request:
              mismatch === "type"
                ? { type: "terminal.cleanup" }
                : {
                    type: "provider-runtime.continue",
                    sourceRunId: mismatch === "source" ? RunId.make("run:unrelated") : run.id,
                  },
          },
        ]);
        const result = yield* sink.commitCommand(declineInput(run));
        assert.strictEqual(result.committed, mismatch === "none");
        const current = yield* rawRun(run);
        if (mismatch === "none")
          assert.deepEqual(current.restartCut, {
            continuation: "declined",
            cutAt: DateTime.formatIso(run.completedAt ?? cutAt),
            reason: RESTART_CUT_REASONS.EFFECT_FAILED,
          });
        else assert.isUndefined(current.restartCut);
      }
    }).pipe(Effect.provide(layerTest)),
);

it.effect.each(["natural", "replacement"] as const)(
  "normal dispatch events retain their receipt when %s wins the fresh guard",
  (winner) =>
    Effect.gen(function* () {
      const sink = yield* EventSink.EventSinkV2;
      const source = sourceFor(`dispatch-${winner}`);
      yield* seed(source);
      const input = {
        ...declineInput(source),
        events: [
          {
            id: EventId.make(`event:winner:${winner}`),
            type: "run.updated" as const,
            threadId: source.threadId,
            occurredAt: later,
            payload:
              winner === "natural"
                ? { ...source, status: "completed" as const }
                : {
                    ...source,
                    id: RunId.make("run:dispatch-replacement"),
                    ordinal: 2,
                    status: "starting" as const,
                    startedAt: null,
                    restartCut: undefined,
                    restartContinuationOfRunId: source.id,
                  },
          },
        ],
      };
      const result = yield* sink.commitCommand(input);
      assert.isTrue(result.committed);
      assert.lengthOf(result.storedEvents, 1);
      assert.strictEqual(result.receipt.resultSequence, result.storedEvents[0]?.sequence);
      assert.strictEqual((yield* rawRun(source)).restartCut.continuation, "scheduled");
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "an appended decline follows normal events in durable, publication and receipt order",
  () =>
    Effect.gen(function* () {
      const sink = yield* EventSink.EventSinkV2;
      const source = sourceFor("dispatch-decline");
      const eventStore = yield* EventStore.EventStoreV2;
      yield* seed(source);
      const input = {
        ...declineInput(source),
        events: [
          {
            id: EventId.make("event:dispatch-metadata"),
            type: "thread.metadata-updated" as const,
            threadId: source.threadId,
            occurredAt: later,
            payload: { ...threadFor(source), title: "Refused continuation" },
          },
        ],
      };
      yield* Effect.acquireUseRelease(
        Effect.sync(() => vi.spyOn(eventStore, "publishCommitted")),
        (publish) =>
          Effect.gen(function* () {
            const result = yield* sink.commitCommand(input);
            assert.lengthOf(result.storedEvents, 2);
            assert.deepEqual(
              result.storedEvents.map((stored) => stored.event.type),
              ["thread.metadata-updated", "run.updated"],
            );
            assert.strictEqual(
              result.storedEvents[1]?.sequence,
              (result.storedEvents[0]?.sequence ?? 0) + 1,
            );
            assert.strictEqual(result.receipt.resultSequence, result.storedEvents[1]?.sequence);
            assert.deepEqual(
              publish.mock.calls.flatMap(([events]) => [...events]),
              [...result.storedEvents],
            );
          }),
        (publish) => Effect.sync(() => publish.mockRestore()),
      );
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "SQLite rolls back the patch, normal event and receipt when the decline append fails",
  () =>
    Effect.gen(function* () {
      const sink = yield* EventSink.EventSinkV2;
      const sql = yield* SqlClient.SqlClient;
      const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
      const events = yield* EventStore.EventStoreV2;
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const source = sourceFor("atomic-rollback");
      yield* seed(source);
      const before = yield* rawRun(source);
      yield* sql.unsafe(`CREATE TEMP TRIGGER reject_restart_decline BEFORE INSERT ON orchestration_events
      WHEN NEW.event_type = 'run.updated' BEGIN SELECT RAISE(ABORT, 'test decline append rejection'); END`);
      const input = {
        ...declineInput(source),
        events: [
          {
            id: EventId.make("event:before-rejected-decline"),
            type: "thread.metadata-updated" as const,
            threadId: source.threadId,
            occurredAt: later,
            payload: { ...threadFor(source), title: "Must roll back" },
          },
        ],
      };
      yield* Effect.acquireUseRelease(
        Effect.sync(() => vi.spyOn(events, "publishCommitted")),
        (publish) =>
          Effect.gen(function* () {
            const standaloneFailure = yield* sink
              .commitCommand(declineInput(source))
              .pipe(Effect.exit);
            assert.isTrue(Exit.isFailure(standaloneFailure));
            assert.deepEqual(yield* rawRun(source), before);
            assert.isTrue(Option.isNone(yield* receipts.getByCommandId(input.commandId)));
            const failure = yield* sink.commitCommand(input).pipe(Effect.exit);
            assert.isTrue(Exit.isFailure(failure));
            assert.lengthOf(publish.mock.calls, 0);
          }),
        (publish) => Effect.sync(() => publish.mockRestore()),
      );
      assert.deepEqual(yield* rawRun(source), before);
      assert.strictEqual((yield* store.getThread(source.threadId)).title, threadFor(source).title);
      assert.isTrue(Option.isNone(yield* receipts.getByCommandId(input.commandId)));
      assert.lengthOf(
        yield* events.readByCommandId({ commandId: input.commandId }).pipe(Stream.runCollect),
        0,
      );
      yield* sql.unsafe("DROP TRIGGER reject_restart_decline");
      const retried = yield* sink.commitCommand(input);
      assert.isTrue(retried.committed);
      assert.lengthOf(retried.storedEvents, 2);
    }).pipe(Effect.provide(layerTest)),
);
