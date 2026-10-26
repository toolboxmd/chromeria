import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  OrchestrationV2RunJson,
  RunId,
  ThreadId,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Tracer from "effect/Tracer";
import * as SqlClient from "effect/sql/SqlClient";

import * as EffectOutbox from "../orchestration-v2/EffectOutbox.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import { recoveryRun, recoveryTestLayer, threadFor } from "../prism/recovery.testkit.ts";
import { RESTART_CUT_REASONS } from "@t3tools/shared/restartCut";

const at = DateTime.makeUnsafe("2026-10-14T10:00:00Z");
const later = DateTime.makeUnsafe("2026-10-14T10:01:00Z");
const layerTest = Layer.mergeAll(
  recoveryTestLayer,
  EffectOutbox.layer.pipe(Layer.provide(recoveryTestLayer)),
);
const encodeRun = Schema.encodeEffect(Schema.fromJsonString(OrchestrationV2RunJson));

function sourceFor(id: string): OrchestrationV2Run {
  return {
    ...recoveryRun,
    id: RunId.make(`run:${id}`),
    threadId: ThreadId.make(`thread:${id}`),
    status: "cancelled",
    completedAt: at,
    restartCut: { continuation: "scheduled", cutAt: at },
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
        occurredAt: at,
        payload: threadFor(run),
      },
      {
        id: EventId.make(`event:run:${run.id}`),
        type: "run.created",
        threadId: run.threadId,
        occurredAt: at,
        payload: run,
      },
    ],
  });
});

const seedIntent = Effect.fnUntraced(function* (
  run: OrchestrationV2Run,
  status: EffectOutbox.OrchestrationEffectStatusV2,
) {
  const outbox = yield* EffectOutbox.EffectOutboxV2;
  const sql = yield* SqlClient.SqlClient;
  const id = `effect:restart-continuation:${run.id}`;
  yield* outbox.enqueue([
    {
      id,
      commandId: CommandId.make(`command:intent:${run.id}`),
      threadId: run.threadId,
      request: { type: "provider-runtime.continue", sourceRunId: run.id },
    },
  ]);
  // These are the persisted states at startup, independent of a running worker.
  yield* sql`UPDATE orchestration_v2_effect_outbox SET status = ${status},
    last_error = 'arbitrary provider defect text' WHERE effect_id = ${id}`;
});

it.effect(
  "discovers terminal and missing intents, including legacy ownership, with active and natural controls",
  () =>
    Effect.gen(function* () {
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const sink = yield* EventSink.EventSinkV2;
      const expected: Array<{ threadId: ThreadId; runId: RunId }> = [];
      for (const legacy of [false, true]) {
        for (const status of ["pending", "running", "succeeded", "failed", "cancelled"] as const) {
          const run = {
            ...sourceFor(`${legacy ? "legacy" : "cut"}-${status}`),
            ...(legacy ? { restartCut: undefined } : {}),
          };
          yield* seed(run);
          yield* seedIntent(run, status);
          if (status !== "pending" && status !== "running")
            expected.push({ threadId: run.threadId, runId: run.id });
        }
      }
      const missing = sourceFor("cut-missing");
      yield* seed(missing);
      expected.push({ threadId: missing.threadId, runId: missing.id });
      yield* seed({ ...sourceFor("unrelated-old-cancellation"), restartCut: undefined });
      yield* seed({
        ...sourceFor("final-decline"),
        restartCut: {
          continuation: "declined",
          cutAt: at,
          reason: RESTART_CUT_REASONS.STOP_REQUESTED,
        },
      });
      for (const status of [
        "completed",
        "failed",
        "interrupted",
        "rolled_back",
        "running",
      ] as const) {
        yield* seed({ ...sourceFor(`natural-${status}`), status });
      }
      for (const status of ["starting", "failed", "cancelled"] as const) {
        const source = sourceFor(`linked-${status}`);
        yield* seed(source);
        yield* seedIntent(source, "failed");
        yield* sink.write({
          events: [
            {
              id: EventId.make(`event:linked:${status}`),
              type: "run.created",
              threadId: source.threadId,
              occurredAt: later,
              payload: {
                ...source,
                id: RunId.make(`replacement:${source.id}`),
                ordinal: 2,
                status,
                startedAt: null,
                restartCut: undefined,
                restartContinuationOfRunId: source.id,
              },
            },
          ],
        });
      }
      expected.sort((left, right) => (left.runId < right.runId ? -1 : 1));
      assert.deepEqual(yield* store.getUnresolvedRestartCuts(), expected);
    }).pipe(Effect.provide(layerTest)),
);

it.effect("matches intent ownership by exact id, thread, type and source payload", () =>
  Effect.gen(function* () {
    const store = yield* ProjectionStore.ProjectionStoreV2;
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    const sql = yield* SqlClient.SqlClient;
    const expected: Array<RunId> = [];
    for (const hasMetadata of [false, true]) {
      for (const mismatch of ["id", "thread", "type", "source", "payload-type"] as const) {
        const run = {
          ...sourceFor(`ownership-${hasMetadata}-${mismatch}`),
          ...(hasMetadata ? {} : { restartCut: undefined }),
        };
        yield* seed(run);
        const id =
          mismatch === "id" ? `effect:other:${run.id}` : `effect:restart-continuation:${run.id}`;
        yield* outbox.enqueue([
          {
            id,
            commandId: CommandId.make(`command:wrong-intent:${run.id}`),
            threadId: mismatch === "thread" ? ThreadId.make("thread:foreign") : run.threadId,
            request:
              mismatch === "type"
                ? { type: "terminal.cleanup" }
                : {
                    type: "provider-runtime.continue",
                    sourceRunId: mismatch === "source" ? RunId.make("run:foreign") : run.id,
                  },
          },
        ]);
        yield* sql`UPDATE orchestration_v2_effect_outbox SET status = 'failed' WHERE effect_id = ${id}`;
        if (mismatch === "payload-type")
          yield* sql`UPDATE orchestration_v2_effect_outbox SET payload_json = json_set(payload_json, '$.type', 'terminal.cleanup') WHERE effect_id = ${id}`;
        if (hasMetadata) expected.push(run.id);
      }
    }
    expected.sort();
    assert.deepEqual(
      (yield* store.getUnresolvedRestartCuts()).map((row) => row.runId),
      expected,
    );
  }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "pruned succeeded intent is repaired from scheduled metadata without relabelling unrelated cancellations",
  () =>
    Effect.gen(function* () {
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const sql = yield* SqlClient.SqlClient;
      const source = sourceFor("pruned");
      yield* seed(source);
      yield* seedIntent(source, "succeeded");
      yield* sql`UPDATE orchestration_v2_effect_outbox SET completed_at = '1900-01-01T00:00:00.000Z' WHERE effect_id = ${`effect:restart-continuation:${source.id}`}`;
      assert.strictEqual(yield* outbox.pruneSettled, 1);
      assert.isTrue(Option.isNone(yield* outbox.get(`effect:restart-continuation:${source.id}`)));
      assert.deepEqual(yield* store.getUnresolvedRestartCuts(), [
        { threadId: source.threadId, runId: source.id },
      ]);
    }).pipe(Effect.provide(layerTest)),
);

it.effect("keyset pages reach the tail when a fresh guard skips the first captured row", () =>
  Effect.gen(function* () {
    const store = yield* ProjectionStore.ProjectionStoreV2;
    const sink = yield* EventSink.EventSinkV2;
    const sources = ["g", "a", "f", "b", "e", "c", "d"].map((id) => sourceFor(`page-${id}`));
    for (const source of sources) yield* seed(source);
    const visited: Array<RunId> = [];
    let after: RunId | undefined;
    for (let pageNumber = 0; pageNumber < 10; pageNumber++) {
      const page = yield* store.getUnresolvedRestartCuts(after, 2);
      if (page.length === 0) break;
      assert.isAtMost(page.length, 2);
      if (pageNumber === 0) {
        const source = sources.find((run) => run.id === page[0]?.runId);
        assert.isDefined(source);
        if (source !== undefined)
          yield* sink.write({
            events: [
              {
                id: EventId.make("event:page-natural-winner"),
                type: "run.updated",
                threadId: source.threadId,
                occurredAt: later,
                payload: { ...source, status: "completed" },
              },
            ],
          });
      }
      for (const row of page) {
        visited.push(row.runId);
        const result = yield* sink.commitCommand({
          commandId: CommandId.make(`command:page-decline:${row.runId}`),
          threadId: row.threadId,
          commandType: "run.restart-cut.decline",
          acceptedAt: later,
          events: [],
          effects: [],
          restartCutDecline: {
            runId: row.runId,
            eventId: EventId.make(`event:page-decline:${row.runId}`),
            reason: RESTART_CUT_REASONS.NO_CONTINUATION,
          },
        });
        assert.strictEqual(result.committed, row.runId !== RunId.make("run:page-a"));
      }
      // Progress uses captured ids even when the transaction guard refuses a row.
      after = page.at(-1)?.runId;
    }
    assert.deepEqual(visited, sources.map((run) => run.id).sort());
    assert.deepEqual(yield* store.getUnresolvedRestartCuts(), []);
    for (const source of sources) {
      const current = (yield* store.getThreadProjection(source.threadId)).runs[0];
      if (source.id === RunId.make("run:page-a"))
        assert.strictEqual(current?.restartCut?.continuation, "scheduled");
      else
        assert.deepEqual(current?.restartCut, {
          continuation: "declined",
          cutAt: at,
          reason: RESTART_CUT_REASONS.NO_CONTINUATION,
        });
    }
  }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "bounds default and oversized pages and exposes the actual SQLite access path on 846 synthetic runs",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const rows = yield* Effect.forEach(
        Array.from({ length: 846 }, (_, index) => index),
        (index) =>
          Effect.gen(function* () {
            const run = {
              ...sourceFor(`history-${index.toString().padStart(4, "0")}`),
              threadId: ThreadId.make(`thread:history:${Math.floor(index / 20)}`),
              ordinal: (index % 20) + 1,
            };
            return {
              run_id: run.id,
              thread_id: run.threadId,
              ordinal: run.ordinal,
              provider: run.providerInstanceId,
              provider_instance_id: run.providerInstanceId,
              provider_thread_id: null,
              status: run.status,
              requested_at: DateTime.formatIso(run.requestedAt),
              completed_at: DateTime.formatIso(at),
              payload_json: yield* encodeRun(run),
            };
          }),
      );
      yield* sql`INSERT INTO orchestration_v2_projection_runs ${sql.insert(rows)}`;
      assert.lengthOf(yield* store.getUnresolvedRestartCuts(), 100);
      assert.lengthOf(yield* store.getUnresolvedRestartCuts(undefined, 5_000), 500);
      assert.lengthOf(yield* store.getUnresolvedRestartCuts(RunId.make("run:history-0840"), 20), 5);
      const statements: Array<string> = [];
      const tracer = Tracer.make({
        span(options) {
          const span = new Tracer.NativeSpan(options);
          const end = span.end.bind(span);
          span.end = (endTime, exit) => {
            end(endTime, exit);
            const query = span.attributes.get("db.query.text");
            if (typeof query === "string") statements.push(query);
          };
          return span;
        },
      });
      const page = yield* store
        .getUnresolvedRestartCuts(undefined, 100)
        .pipe(Effect.withTracer(tracer));
      assert.lengthOf(page, 100);
      const query = statements.find((statement) =>
        statement.includes("LEFT JOIN orchestration_v2_effect_outbox"),
      );
      assert.isDefined(query);
      if (query !== undefined) {
        const plan = yield* sql.unsafe<{ readonly detail: string }>(`EXPLAIN QUERY PLAN ${query}`, [
          "",
          100,
        ]);
        process.stdout.write(
          `${JSON.stringify({
            evidence: "Restart-cut discovery on synthetic SQLite",
            runRows: 846,
            returnedRows: page.length,
            plan: plan.map((row) => row.detail),
          })}\n`,
        );
      }
    }).pipe(Effect.provide(layerTest)),
);
