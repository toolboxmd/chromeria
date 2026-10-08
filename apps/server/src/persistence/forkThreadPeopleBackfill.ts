import {
  EventId,
  OrchestrationV2AppThreadJson,
  TrimmedNonEmptyString,
  type OrchestrationV2AppThread,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import * as EventSink from "../orchestration-v2/EventSink.ts";

const decodeStoredThread = Schema.decodeUnknownOption(
  Schema.fromJsonString(OrchestrationV2AppThreadJson),
);
const decodeFrozenOwner = Schema.decodeUnknownOption(TrimmedNonEmptyString);
const decodeFrozenCoOwners = Schema.decodeUnknownOption(
  Schema.fromJsonString(Schema.Array(TrimmedNonEmptyString)),
);

/**
 * An imported thread payload this migration reads, because it still lacks
 * people fields, no longer decodes. Startup stops until it is repaired.
 * Payloads the migration does not read are not validated here.
 */
export class ThreadPeopleBackfillPayloadError extends Schema.TaggedError<ThreadPeopleBackfillPayloadError>()(
  "ThreadPeopleBackfillPayloadError",
  { threadId: Schema.String },
) {
  override get message(): string {
    return `Imported thread ${this.threadId} has an invalid stored payload.`;
  }
}

/** One completion marker per imported thread, outside the upstream migration ledger. */
const BACKFILL_EVENT_PREFIX = "migration:fork-v1:people";

type UnresolvedReason = "invalid_owner" | "invalid_co_owners";

interface FrozenPeopleRow {
  readonly thread_id: string;
  readonly owner: unknown;
  readonly co_owners_json: unknown;
  readonly payload_json: string;
}

/**
 * Carries the frozen v1 `projection_threads.owner` and `co_owners_json`
 * columns onto imported v2 threads (toolboxmd/chromeria#170). Every imported
 * thread gets explicit `owner` and `coOwners`, null and [] included; a field
 * the v2 thread already has wins, and only the frozen values it needs are read.
 * A malformed needed value becomes null or [] and is recorded in
 * `fork_thread_people_backfill_issues` (thread id and reason kind only). Each
 * thread is one transaction whose stable event id marks it done, so reruns and
 * partial failures converge. A payload it reads that no longer decodes stops
 * startup.
 */
export const backfillThreadPeople = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const eventSink = yield* EventSink.EventSinkV2;
  const columns = yield* sql<{ readonly name: string }>`PRAGMA table_info(projection_threads)`;
  // A database that never ran the v1 people schema has no frozen people to carry.
  if (!columns.some((column) => column.name === "owner")) return;
  if (!columns.some((column) => column.name === "co_owners_json")) return;
  yield* sql`
    CREATE TABLE IF NOT EXISTS fork_thread_people_backfill_issues (
      thread_id TEXT NOT NULL,
      reason_kind TEXT NOT NULL,
      PRIMARY KEY (thread_id, reason_kind)
    )
  `;

  const rows = yield* sql<FrozenPeopleRow>`
    SELECT
      thread.thread_id,
      thread.owner,
      thread.co_owners_json,
      projection.payload_json
    FROM orchestration_v2_legacy_imports AS legacy_import
    INNER JOIN projection_threads AS thread
      ON thread.thread_id = legacy_import.thread_id
    INNER JOIN orchestration_v2_projection_threads AS projection
      ON projection.thread_id = legacy_import.thread_id
    WHERE (
        json_type(projection.payload_json, '$.owner') IS NULL
        OR json_type(projection.payload_json, '$.coOwners') IS NULL
      )
      AND NOT EXISTS (
        SELECT 1
        FROM orchestration_events AS event
        WHERE event.event_id = ${BACKFILL_EVENT_PREFIX} || ':' || thread.thread_id
      )
    ORDER BY thread.created_at ASC, thread.thread_id ASC
  `;
  for (const row of rows) {
    // Only payloads this migration consumes are validated; it rewrites the whole record.
    const decoded = decodeStoredThread(row.payload_json);
    if (Option.isNone(decoded)) {
      return yield* new ThreadPeopleBackfillPayloadError({ threadId: row.thread_id });
    }
    const current = decoded.value;
    const unresolved: Array<UnresolvedReason> = [];
    let owner = current.owner;
    if (owner === undefined) {
      const frozen = decodeFrozenOwner(row.owner);
      if (row.owner !== null && Option.isNone(frozen)) unresolved.push("invalid_owner");
      owner = Option.getOrNull(frozen);
    }
    let coOwners = current.coOwners;
    if (coOwners === undefined) {
      const frozen = decodeFrozenCoOwners(row.co_owners_json);
      if (Option.isNone(frozen)) unresolved.push("invalid_co_owners");
      coOwners = Option.getOrElse(frozen, () => []);
    }
    const thread: OrchestrationV2AppThread = { ...current, owner, coOwners };
    const now = yield* DateTime.now;
    yield* sql.withTransaction(
      Effect.gen(function* () {
        for (const reason of unresolved) {
          yield* sql`
            INSERT INTO fork_thread_people_backfill_issues (thread_id, reason_kind)
            VALUES (${row.thread_id}, ${reason})
            ON CONFLICT (thread_id, reason_kind) DO NOTHING
          `;
        }
        yield* eventSink.write({
          events: [
            {
              id: EventId.make(`${BACKFILL_EVENT_PREFIX}:${row.thread_id}`),
              type: "thread.metadata-updated",
              threadId: thread.id,
              providerInstanceId: thread.providerInstanceId,
              occurredAt: now,
              payload: thread,
            },
          ],
        });
      }),
    );
  }
  // Recorded rows stay: the frozen v1 source never changes, so a rerun cannot resolve them.
  const [unresolved] = yield* sql<{ readonly count: number }>`
    SELECT COUNT(DISTINCT thread_id) AS count FROM fork_thread_people_backfill_issues
  `;
  const unresolvedThreadCount = unresolved?.count ?? 0;
  if (rows.length > 0 || unresolvedThreadCount > 0) {
    yield* Effect.logInfo("Thread people backfill finished").pipe(
      Effect.annotateLogs({ backfilledThreadCount: rows.length, unresolvedThreadCount }),
    );
  }
});
