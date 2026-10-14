import { assert, it } from "@effect/vitest";
import {
  ContextTransferId,
  MessageId,
  EventId,
  NodeId,
  RunId,
  ThreadId,
  type OrchestrationV2ContextTransfer,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";

import * as Persistence from "../persistence/Sqlite.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import { recoveryRun, threadFor, errorFor } from "../prism/recovery.testkit.ts";
import { RESTART_CUT_REASONS } from "@t3tools/shared/restartCut";

const cutAt = DateTime.makeUnsafe("2026-10-14T10:00:00Z");
const later = DateTime.makeUnsafe("2026-10-14T10:01:00Z");
const sqlLayer = ProjectionStore.layer.pipe(Layer.provideMerge(Persistence.layerMemory));

function runFor(id: string): OrchestrationV2Run {
  return {
    ...recoveryRun,
    id: RunId.make(`run:${id}`),
    threadId: ThreadId.make(`thread:${id}`),
    status: "cancelled",
    completedAt: cutAt,
  };
}

function runEvent(run: OrchestrationV2Run, id: string): OrchestrationV2DomainEvent {
  return {
    id: EventId.make(`event:${id}`),
    type: "run.updated",
    threadId: run.threadId,
    occurredAt: later,
    payload: run,
  };
}

const seedRun = Effect.fnUntraced(function* (run: OrchestrationV2Run) {
  const store = yield* ProjectionStore.ProjectionStoreV2;
  yield* store.apply({
    id: EventId.make(`event:thread:${run.threadId}`),
    type: "thread.created",
    threadId: run.threadId,
    occurredAt: run.requestedAt,
    payload: threadFor(run),
  });
  yield* store.apply(runEvent(run, `seed:${run.id}`));
});

function resultTransfer(run: OrchestrationV2Run, parentThreadId: ThreadId) {
  return {
    id: ContextTransferId.make(`transfer:${run.id}`),
    type: "subagent_result",
    sourceThreadId: run.threadId,
    targetThreadId: parentThreadId,
    sourcePoint: { threadId: run.threadId, runId: run.id },
    basePoint: null,
    sourceProviderInstanceId: run.providerInstanceId,
    targetProviderInstanceId: run.providerInstanceId,
    targetRunId: null,
    status: "resolved_portable",
    resolution: null,
    createdBy: "system",
    error: null,
    createdAt: cutAt,
    updatedAt: cutAt,
    consumedAt: null,
  } satisfies OrchestrationV2ContextTransfer;
}

const seedChild = Effect.fnUntraced(function* (run: OrchestrationV2Run) {
  const store = yield* ProjectionStore.ProjectionStoreV2;
  const parentThreadId = ThreadId.make(`parent:${run.threadId}`);
  yield* seedRun({ ...run, threadId: parentThreadId, id: RunId.make(`parent:${run.id}`) });
  yield* seedRun(run);
  yield* store.apply({
    id: EventId.make(`event:child-lineage:${run.id}`),
    type: "thread.metadata-updated",
    threadId: run.threadId,
    occurredAt: cutAt,
    payload: {
      ...threadFor(run),
      lineage: { parentThreadId, rootThreadId: parentThreadId, relationshipToParent: "subagent" },
      forkedFrom: { type: "node", nodeId: NodeId.make(`task:${run.id}`) },
    },
  });
  return parentThreadId;
});

const addTransfer = Effect.fnUntraced(function* (
  transfer: OrchestrationV2ContextTransfer,
  owner = transfer.sourceThreadId,
) {
  const store = yield* ProjectionStore.ProjectionStoreV2;
  yield* store.apply({
    id: EventId.make(`event:${transfer.id}`),
    type: "context-transfer.created",
    threadId: owner,
    occurredAt: transfer.createdAt,
    payload: transfer,
  });
});

it.effect("SQL run snapshots retain an existing restart cut when it is omitted", () =>
  Effect.gen(function* () {
    const store = yield* ProjectionStore.ProjectionStoreV2;
    const sql = yield* SqlClient.SqlClient;
    const run = runFor("cut-omission-sql");
    yield* seedRun(run);
    yield* sql`
      UPDATE orchestration_v2_projection_runs SET payload_json = json_set(
        payload_json, '$.restartCut',
        json('{"continuation":"scheduled","cutAt":"2026-10-14T10:00:00.000Z"}')
      ) WHERE run_id = ${run.id}
    `;
    yield* store.apply(runEvent({ ...run, checkpointId: null }, "stale-omission-sql"));
    const rows = yield* sql<{ readonly continuation: string | null }>`
      SELECT json_extract(payload_json, '$.restartCut.continuation') AS continuation
      FROM orchestration_v2_projection_runs WHERE run_id = ${run.id}
    `;
    assert.strictEqual(rows[0]?.continuation, "scheduled");
  }).pipe(Effect.provide(sqlLayer)),
);

it.effect("memory run snapshots retain an existing restart cut when it is omitted", () =>
  Effect.gen(function* () {
    const store = yield* ProjectionStore.ProjectionStoreV2;
    const run = runFor("cut-omission-memory");
    const scheduled = { ...run, restartCut: { continuation: "scheduled" as const, cutAt } };
    yield* seedRun(scheduled);
    yield* store.apply(runEvent(run, "stale-omission-memory"));
    const projection = yield* store.getThreadProjection(run.threadId);
    assert.deepEqual(projection.runs[0], scheduled);
  }).pipe(Effect.provide(ProjectionStore.layerMemory)),
);

const storeCases = [
  { name: "SQL", layer: sqlLayer },
  { name: "memory", layer: ProjectionStore.layerMemory },
] as const;
it.effect.each(storeCases)(
  "$name rediscovers a restart-cut follow-up after the first result was transferred",
  ({ name, layer }) =>
    Effect.gen(function* () {
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const first = { ...runFor(`followup-${name}`), status: "completed" as const };
      const parentThreadId = ThreadId.make(`thread:parent:${name}`);
      yield* seedRun({
        ...first,
        threadId: parentThreadId,
        id: RunId.make(`run:parent:${name}`),
      });
      yield* seedRun(first);
      yield* store.apply({
        id: EventId.make(`event:lineage:${name}`),
        type: "thread.metadata-updated",
        threadId: first.threadId,
        occurredAt: cutAt,
        payload: {
          ...threadFor(first),
          lineage: {
            parentThreadId,
            rootThreadId: parentThreadId,
            relationshipToParent: "subagent",
          },
          forkedFrom: { type: "node", nodeId: NodeId.make("task:first") },
        },
      });
      yield* store.apply({
        id: EventId.make(`event:first-transfer:${name}`),
        type: "context-transfer.created",
        threadId: first.threadId,
        occurredAt: cutAt,
        payload: resultTransfer(first, parentThreadId),
      });
      const followup = {
        ...first,
        id: RunId.make(`run:followup:${name}`),
        ordinal: 2,
        status: "cancelled" as const,
        startedAt: null,
        restartCut: {
          continuation: "declined" as const,
          cutAt,
          reason: "Continuing threads after restarts is turned off.",
        },
      };
      yield* store.apply(runEvent(followup, `followup:${name}`));
      const ids = yield* store.getRecoveryThreadIds("subagent-results");
      assert.deepEqual(ids, [first.threadId]);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(storeCases)(
  "$name retains the first decline and cut time across stale full run snapshots",
  ({ name, layer }) =>
    Effect.gen(function* () {
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const source = {
        ...runFor(`merge-${name}`),
        restartCut: { continuation: "scheduled" as const, cutAt },
      };
      yield* seedRun(source);
      yield* store.apply(
        runEvent(
          { ...source, restartCut: { continuation: "scheduled", cutAt: later } },
          `new-schedule:${name}`,
        ),
      );
      assert.deepEqual(
        (yield* store.getThreadProjection(source.threadId)).runs[0]?.restartCut,
        source.restartCut,
      );
      const terminal = {
        continuation: "declined" as const,
        cutAt: later,
        reason: RESTART_CUT_REASONS.STOP_REQUESTED,
      };
      yield* store.apply(runEvent({ ...source, restartCut: terminal }, `decline:${name}`));
      for (const restartCut of [
        undefined,
        source.restartCut,
        { ...terminal, reason: RESTART_CUT_REASONS.SUPERSEDED },
      ]) {
        yield* store.apply(
          runEvent(
            { ...source, status: "rolled_back", restartCut },
            `stale:${name}:${restartCut?.continuation ?? "none"}`,
          ),
        );
        const current = (yield* store.getThreadProjection(source.threadId)).runs[0];
        assert.strictEqual(current?.status, "rolled_back");
        assert.deepEqual(current?.restartCut, { ...terminal, cutAt });
      }
    }).pipe(Effect.provide(layer)),
);

it.effect.each(storeCases)(
  "$name uses the result that ended last when a lower-ordinal queue resumes after a restart",
  ({ name, layer }) =>
    Effect.gen(function* () {
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const first = {
        ...runFor(`resumed-queue-${name}`),
        status: "completed" as const,
        completedAt: DateTime.makeUnsafe("2026-10-14T09:30:00Z"),
      };
      const parent = yield* seedChild(first);
      yield* addTransfer(resultTransfer(first, parent));
      const queued = {
        ...first,
        id: RunId.make(`run:resumed-queue:${name}`),
        ordinal: 2,
        status: "queued" as const,
        queueHeld: true,
        startedAt: null,
        completedAt: null,
      };
      yield* store.apply(runEvent(queued, `held-queue-2:${name}`));
      yield* store.apply(
        runEvent(
          { ...queued, id: RunId.make(`run:still-held:${name}`), ordinal: 3 },
          `held-queue-3:${name}`,
        ),
      );
      const cut = {
        ...first,
        id: RunId.make(`run:earlier-restart-result:${name}`),
        ordinal: 4,
        status: "cancelled" as const,
        completedAt: cutAt,
        restartCut: {
          continuation: "declined" as const,
          cutAt,
          reason: RESTART_CUT_REASONS.CONTINUATION_OFF,
        },
      };
      yield* store.apply(runEvent(cut, `earlier-restart-result:${name}`));
      assert.deepEqual(yield* store.getRecoveryThreadIds("subagent-results"), [first.threadId]);
      yield* store.apply(
        runEvent(
          {
            ...queued,
            status: "completed",
            queueHeld: false,
            startedAt: DateTime.makeUnsafe("2026-10-14T10:00:30Z"),
            completedAt: later,
          },
          `later-ordinary-result:${name}`,
        ),
      );
      assert.deepEqual(yield* store.getRecoveryThreadIds("subagent-results"), []);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(storeCases)(
  "$name rediscovers a completed started replacement after an older first-result transfer",
  ({ name, layer }) =>
    Effect.gen(function* () {
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const first = {
        ...runFor(`started-replacement-${name}`),
        status: "completed" as const,
        completedAt: DateTime.makeUnsafe("2026-10-14T09:30:00Z"),
      };
      const parent = yield* seedChild(first);
      yield* addTransfer(resultTransfer(first, parent));
      const cut = {
        ...first,
        id: RunId.make(`run:started-replacement-source:${name}`),
        ordinal: 2,
        status: "cancelled" as const,
        completedAt: cutAt,
        restartCut: { continuation: "scheduled" as const, cutAt },
      };
      yield* store.apply(runEvent(cut, `started-replacement-source:${name}`));
      const replacement = {
        ...first,
        id: RunId.make(`run:completed-replacement:${name}`),
        ordinal: 3,
        startedAt: later,
        completedAt: DateTime.makeUnsafe("2026-10-14T10:02:00Z"),
        restartContinuationOfRunId: cut.id,
      };
      yield* store.apply(runEvent(replacement, `completed-replacement:${name}`));
      assert.deepEqual(yield* store.getRecoveryThreadIds("subagent-results"), [first.threadId]);
      yield* addTransfer(resultTransfer(replacement, parent), parent);
      assert.deepEqual(yield* store.getRecoveryThreadIds("subagent-results"), []);
      assert.deepEqual(yield* store.getRecoveryThreadIds("subagent-results"), []);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(storeCases)(
  "$name natural completion retains cut metadata without creating a restart result",
  ({ name, layer }) =>
    Effect.gen(function* () {
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const first = { ...runFor(`natural-completion-${name}`), status: "completed" as const };
      const parent = yield* seedChild(first);
      yield* addTransfer({
        ...resultTransfer(first, parent),
        sourcePoint: { threadId: first.threadId },
      });
      const cut = {
        ...first,
        id: RunId.make(`run:natural-cut:${name}`),
        ordinal: 2,
        status: "cancelled" as const,
        restartCut: { continuation: "scheduled" as const, cutAt },
      };
      yield* store.apply(runEvent(cut, `natural-cut:${name}`));
      assert.deepEqual(yield* store.getRecoveryThreadIds("subagent-results"), [first.threadId]);
      yield* store.apply(
        runEvent({ ...cut, status: "completed", completedAt: later }, `natural-winner:${name}`),
      );
      assert.deepEqual(yield* store.getRecoveryThreadIds("subagent-results"), []);
      assert.deepEqual(
        (yield* store.getThreadProjection(first.threadId)).runs.find((run) => run.id === cut.id)
          ?.restartCut,
        cut.restartCut,
      );
    }).pipe(Effect.provide(layer)),
);

const clockCases = storeCases.flatMap(({ name, layer }) =>
  (["later", "equal", "backwards"] as const).map((clock) => ({ name, layer, clock })),
);
it.effect.each(clockCases)(
  "$name rediscovers a restart explanation after a legacy transfer despite $clock wall-clock ordering",
  ({ name, layer, clock }) =>
    Effect.gen(function* () {
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const first = {
        ...runFor(`legacy-item-${clock}-${name}`),
        status: "completed" as const,
        completedAt: DateTime.makeUnsafe("2026-10-14T09:30:00Z"),
      };
      const parent = yield* seedChild(first);
      const item = errorFor(first);
      yield* store.apply({
        id: EventId.make(`event:item:${first.id}`),
        type: "turn-item.updated",
        threadId: first.threadId,
        occurredAt: cutAt,
        payload: item,
      });
      const legacy = {
        ...resultTransfer(first, parent),
        sourcePoint:
          clock === "later"
            ? { threadId: first.threadId, turnItemId: item.id }
            : { threadId: first.threadId },
        createdAt: later,
      };
      yield* addTransfer(legacy);
      const followup = {
        ...first,
        id: RunId.make(`run:new-cut:${clock}:${name}`),
        ordinal: 2,
        status: "cancelled" as const,
        completedAt:
          clock === "later"
            ? DateTime.makeUnsafe("2026-10-14T10:02:00Z")
            : clock === "equal"
              ? later
              : DateTime.makeUnsafe("2026-10-14T09:59:00Z"),
        restartCut: {
          continuation: "declined" as const,
          cutAt,
          reason: RESTART_CUT_REASONS.CONTINUATION_OFF,
        },
      };
      yield* store.apply(runEvent(followup, `new-cut:${clock}:${name}`));
      assert.deepEqual(yield* store.getRecoveryThreadIds("subagent-results"), [first.threadId]);
      const covering = {
        ...legacy,
        id: ContextTransferId.make(`transfer:current-cut:${clock}:${name}`),
        sourcePoint: { threadId: first.threadId, runId: followup.id },
        createdAt: cutAt,
      };
      yield* addTransfer(covering, parent);
      assert.deepEqual(yield* store.getRecoveryThreadIds("subagent-results"), []);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(storeCases)(
  "$name restart admission respects held work, monitors, rolled-back results and exact-run dedup",
  ({ name, layer }) =>
    Effect.gen(function* () {
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const first = { ...runFor(`exclusions-${name}`), status: "completed" as const };
      const parent = yield* seedChild(first);
      yield* addTransfer(resultTransfer(first, parent));
      const cut = {
        ...first,
        id: RunId.make(`run:eligible-cut:${name}`),
        ordinal: 2,
        status: "cancelled" as const,
        restartCut: {
          continuation: "declined" as const,
          cutAt,
          reason: RESTART_CUT_REASONS.CONTINUATION_OFF,
        },
      };
      yield* store.apply(runEvent(cut, `eligible-cut:${name}`));
      const held = {
        ...first,
        id: RunId.make(`run:held:${name}`),
        ordinal: 3,
        status: "queued" as const,
        queueHeld: true,
        startedAt: null,
        completedAt: null,
      };
      yield* store.apply(runEvent(held, `held:${name}`));
      assert.deepEqual(yield* store.getRecoveryThreadIds("subagent-results"), [first.threadId]);
      yield* store.apply(runEvent({ ...held, queueHeld: false }, `unheld:${name}`));
      assert.deepEqual(yield* store.getRecoveryThreadIds("subagent-results"), []);
      yield* store.apply(runEvent(held, `reheld:${name}`));
      const monitor = {
        ...first,
        id: RunId.make(`run:monitor:${name}`),
        ordinal: 4,
        status: "running" as const,
        completedAt: null,
      };
      yield* store.apply(runEvent(monitor, `monitor:${name}`));
      yield* store.apply({
        id: EventId.make(`event:monitor-message:${name}`),
        type: "message.updated",
        threadId: first.threadId,
        occurredAt: later,
        payload: {
          id: MessageId.make(`message:monitor:${name}`),
          createdBy: "system",
          creationSource: "server",
          threadId: first.threadId,
          runId: monitor.id,
          nodeId: null,
          role: "user",
          text: "Monitor",
          attachments: [],
          streaming: false,
          createdAt: later,
          updatedAt: later,
          notification: {
            source: { kind: "monitor" },
            outcome: "updated",
            summary: "Monitor update",
          },
        },
      });
      assert.deepEqual(yield* store.getRecoveryThreadIds("subagent-results"), [first.threadId]);
      const rolledBack = {
        ...cut,
        id: RunId.make(`run:rolled-back:${name}`),
        ordinal: 5,
        status: "rolled_back" as const,
      };
      yield* store.apply(runEvent(rolledBack, `rolled-back:${name}`));
      assert.deepEqual(yield* store.getRecoveryThreadIds("subagent-results"), [first.threadId]);
      yield* addTransfer(
        {
          ...resultTransfer(cut, parent),
          id: ContextTransferId.make(`transfer:exact-cut:${name}`),
        },
        parent,
      );
      assert.deepEqual(yield* store.getRecoveryThreadIds("subagent-results"), []);
      const aborted = {
        ...first,
        id: RunId.make(`run:aborted-replacement:${name}`),
        ordinal: 6,
        status: "failed" as const,
        startedAt: null,
        restartContinuationOfRunId: cut.id,
      };
      yield* store.apply(runEvent(aborted, `aborted:${name}`));
      assert.deepEqual(yield* store.getRecoveryThreadIds("subagent-results"), [first.threadId]);
      yield* store.apply(
        runEvent({ ...aborted, status: "rolled_back" }, `aborted-rollback:${name}`),
      );
      assert.deepEqual(yield* store.getRecoveryThreadIds("subagent-results"), []);
      yield* store.apply(
        runEvent(
          { ...cut, id: RunId.make(`run:newest-cut:${name}`), ordinal: 7 },
          `newest-cut:${name}`,
        ),
      );
      assert.deepEqual(yield* store.getRecoveryThreadIds("subagent-results"), [first.threadId]);
      yield* store.apply(
        runEvent(
          { ...first, id: RunId.make(`run:newest-ordinary:${name}`), ordinal: 8 },
          `newest-ordinary:${name}`,
        ),
      );
      assert.deepEqual(yield* store.getRecoveryThreadIds("subagent-results"), []);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(storeCases)(
  "$name ordinary outcomes keep legacy once-per-child suppression",
  ({ name, layer }) =>
    Effect.gen(function* () {
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const first = { ...runFor(`ordinary-legacy-${name}`), status: "completed" as const };
      const parent = yield* seedChild(first);
      assert.deepEqual(yield* store.getRecoveryThreadIds("subagent-results"), [first.threadId]);
      const legacy = {
        ...resultTransfer(first, parent),
        sourcePoint: { threadId: first.threadId },
      };
      yield* addTransfer(legacy);
      yield* store.apply(
        runEvent(
          { ...first, id: RunId.make(`run:ordinary-followup:${name}`), ordinal: 2 },
          `ordinary-followup:${name}`,
        ),
      );
      assert.deepEqual(yield* store.getRecoveryThreadIds("subagent-results"), []);
    }).pipe(Effect.provide(layer)),
);
