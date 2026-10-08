import {
  EventId,
  OrchestrationV2AppThreadJson,
  ThreadId,
  type OrchestrationV2AppThread,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { EventSinkV2 } from "../orchestration-v2/EventSink.ts";
import { ForkV1BackfillStepError } from "../persistence/forkV1Backfills.ts";

const decodeThread = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2AppThreadJson),
);
export const lineageBackfillId = "child-threads-lineage";

function importedParentId(thread: OrchestrationV2AppThread): ThreadId | null {
  if (thread.historyOrigin !== "v1_import" || !thread.id.startsWith("sub.")) return null;
  const lastDot = thread.id.lastIndexOf(".");
  return lastDot > 4 ? ThreadId.make(thread.id.slice(4, lastDot)) : null;
}

/** Existing native links win; orphan imports stay top-level and cycles are refused. */
export function importedLineageRepairs(threads: ReadonlyArray<OrchestrationV2AppThread>) {
  const byId = new Map(threads.map((thread) => [thread.id, thread]));
  const parents = new Map<ThreadId, ThreadId>();
  for (const thread of threads) {
    const parent = importedParentId(thread);
    if (thread.lineage.parentThreadId === null && parent !== null && byId.has(parent))
      parents.set(thread.id, parent);
  }
  const repairs: Array<OrchestrationV2AppThread> = [];
  for (const [threadId, parentId] of parents) {
    const seen = new Set<ThreadId>([threadId]);
    let ancestor = byId.get(parentId);
    let valid = true;
    while (ancestor !== undefined) {
      if (seen.has(ancestor.id)) {
        valid = false;
        break;
      }
      seen.add(ancestor.id);
      const next =
        parents.get(ancestor.id) ??
        (ancestor.lineage.relationshipToParent === "subagent"
          ? ancestor.lineage.parentThreadId
          : null);
      if (next === null) break;
      ancestor = byId.get(next);
      if (ancestor === undefined) valid = false;
    }
    if (!valid || ancestor === undefined) continue;
    const thread = byId.get(threadId)!;
    repairs.push({
      ...thread,
      lineage: {
        parentThreadId: parentId,
        relationshipToParent: "subagent",
        rootThreadId: ancestor.lineage.rootThreadId,
      },
    });
  }
  return repairs;
}

export const backfillImportedLineage = Effect.fn("childThreads.backfillImportedLineage")(
  function* () {
    const sql = yield* SqlClient.SqlClient;
    const sink = yield* EventSinkV2;
    const rows = yield* sql<{
      payload_json: string;
    }>`SELECT payload_json FROM orchestration_v2_projection_threads`;
    const threads = yield* Effect.forEach(rows, (row) =>
      decodeThread(row.payload_json).pipe(
        Effect.mapError(() => new ForkV1BackfillStepError({ backfillId: lineageBackfillId })),
      ),
    );
    for (const thread of importedLineageRepairs(threads)) {
      yield* sink.write({
        events: [
          {
            id: EventId.make(`migration:fork-v1:child-lineage:${thread.id}`),
            type: "thread.metadata-updated",
            threadId: thread.id,
            providerInstanceId: thread.providerInstanceId,
            occurredAt: thread.updatedAt,
            payload: thread,
          },
        ],
      });
    }
  },
);
