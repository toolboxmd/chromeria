import { assert, it } from "@effect/vitest";
import {
  EventId,
  MessageId,
  OrchestrationV2Command,
  OrchestrationV2DomainEvent,
  SpectrumTranscriptAppend,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { describe, expect, it as test } from "vite-plus/test";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as TurnItemPositionStore from "../orchestration-v2/TurnItemPositionStore.ts";
import { APPEND, makeState, makeThread, NOW } from "./testFixtures.ts";
import { planTranscriptAppend } from "./transcript.ts";

const stores = Layer.mergeAll(ProjectionStore.layer, TurnItemPositionStore.layer).pipe(
  Layer.provideMerge(SqlitePersistence.layerMemory),
);
const decodeAppend = Schema.decodeUnknownSync(SpectrumTranscriptAppend);
const decodePublicCommand = Schema.decodeUnknownSync(OrchestrationV2Command);
const decodeEvent = Schema.decodeEffect(OrchestrationV2DomainEvent);

describe("Spectrum transcript append", () => {
  test("decodes its internal command while public dispatch cannot forge an assistant message", () => {
    expect(decodeAppend(APPEND)).toEqual(APPEND);
    expect(() => decodePublicCommand(APPEND)).toThrow();
  });

  test("rejects stale, retired, deleted and duplicate appends during planning", () => {
    const state = makeState();
    const thread = makeThread();
    for (const [value, shell, command] of [
      [state, thread, { ...APPEND, revision: 1 }],
      [state, thread, { ...APPEND, generation: 1 }],
      [{ ...state, status: "retired" as const }, thread, APPEND],
      [state, { ...thread, deletedAt: NOW }, APPEND],
      [{ ...state, transcript: [APPEND.messageId] }, thread, APPEND],
    ] as const)
      expect(() => planTranscriptAppend(value, shell, command, NOW)).toThrow();
  });

  test("plans input inbox and append outbox consumption together", () => {
    const command = { ...APPEND, role: "user" as const };
    const { mutation } = planTranscriptAppend(
      makeState({ outbox: [command] }),
      makeThread(),
      command,
      NOW,
    );
    expect(mutation.state.inbox).toEqual([APPEND.messageId]);
    expect(mutation.state.transcript).toEqual([APPEND.messageId]);
    expect(mutation.state.outbox).toEqual([]);
    expect(mutation.state.revision).toBe(1);
  });
});

it.effect(
  "projects exact large replies to normal searchable null-run messages and timeline rows",
  () =>
    Effect.gen(function* () {
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const positions = yield* TurnItemPositionStore.TurnItemPositionStoreV2;
      const thread = makeThread();
      yield* store.apply({
        id: EventId.make("thread:created"),
        type: "thread.created",
        threadId: thread.id,
        occurredAt: NOW,
        payload: thread,
      });
      const text = "  🟦e\u0301\r\nverbatim\n".repeat(5000) + "end\n";
      const { events } = planTranscriptAppend(makeState(), thread, { ...APPEND, text }, NOW);
      for (const event of events) {
        const parsed = yield* decodeEvent(event);
        assert.strictEqual(parsed.providerInstanceId, undefined);
        assert.strictEqual(parsed.runId, undefined);
        assert.strictEqual(parsed.nodeId, undefined);
        assert.strictEqual(parsed.driver, undefined);
        yield* store.apply(
          parsed.type === "turn-item.updated"
            ? { ...parsed, payload: yield* positions.normalize(parsed.payload) }
            : parsed,
        );
      }
      const { messages, turnItems, runs } = yield* store.getThreadRecords(thread.id, [
        "messages",
        "turnItems",
        "runs",
      ]);
      assert.strictEqual(messages.length, 1);
      assert.strictEqual(messages[0]!.id, MessageId.make("transcript:test"));
      assert.strictEqual(messages[0]!.text, text);
      assert.strictEqual(messages[0]!.runId, null);
      assert.strictEqual(turnItems.length, 1);
      const item = turnItems[0]!;
      assert.strictEqual(item.type, "assistant_message");
      if (item.type === "assistant_message") assert.strictEqual(item.text, text);
      assert.strictEqual(item.runId, null);
      assert.strictEqual(runs.length, 0);
    }).pipe(Effect.provide(stores)),
);
