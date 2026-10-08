import { type ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import { decideRecovery, RecoveryRecord } from "./recoveryPolicy.ts";

const RecordJson = Schema.fromJsonString(RecoveryRecord);
const encode = Schema.encodeSync(RecordJson);
const decode = Schema.decodeUnknownSync(RecordJson);

export class RecoveryStore extends Context.Service<
  RecoveryStore,
  {
    readonly get: (threadId: ThreadId) => Effect.Effect<RecoveryRecord | null, SqlError>;
    readonly reconcile: (
      input: Parameters<typeof decideRecovery>[0],
    ) => Effect.Effect<RecoveryRecord, SqlError>;
    readonly pending: Effect.Effect<ReadonlyArray<RecoveryRecord>, SqlError>;
    readonly close: (record: RecoveryRecord) => Effect.Effect<void, SqlError>;
    readonly started: (record: RecoveryRecord) => Effect.Effect<void, SqlError>;
  }
>()("t3/prism/RecoveryStore") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS fork_prism_recovery (
    thread_id TEXT PRIMARY KEY, original_run_id TEXT NOT NULL, source_run_id TEXT NOT NULL,
    state TEXT NOT NULL, payload_json TEXT NOT NULL
  )`;
  const get = Effect.fn("RecoveryStore.get")(function* (threadId: ThreadId) {
    const rows = yield* sql<{
      payload_json: string;
    }>`SELECT payload_json FROM fork_prism_recovery WHERE thread_id = ${threadId}`;
    return rows[0] ? decode(rows[0].payload_json) : null;
  });
  const reconcile = Effect.fn("RecoveryStore.reconcile")(function* (
    input: Parameters<typeof decideRecovery>[0],
  ) {
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        // Read in the transaction: duplicate failure observers share one retry budget.
        const previous = yield* get(input.run.threadId);
        const record = decideRecovery({ ...input, previous });
        yield* sql`INSERT INTO fork_prism_recovery (thread_id, original_run_id, source_run_id, state, payload_json)
        VALUES (${record.threadId}, ${record.originalRunId}, ${record.sourceRunId}, ${record.state}, ${encode(record)})
        ON CONFLICT(thread_id) DO UPDATE SET original_run_id=excluded.original_run_id,
          source_run_id=excluded.source_run_id, state=excluded.state, payload_json=excluded.payload_json`;
        return record;
      }),
    );
  });
  const transition = (record: RecoveryRecord, state: "closed" | "retry_started") =>
    sql`UPDATE fork_prism_recovery SET state=${state}, payload_json=${encode({ ...record, state })}
      WHERE thread_id=${record.threadId} AND original_run_id=${record.originalRunId}
        AND source_run_id=${record.sourceRunId} AND state=${record.state}`.pipe(Effect.asVoid);
  return RecoveryStore.of({
    get,
    reconcile,
    pending: sql<{ payload_json: string }>`SELECT payload_json FROM fork_prism_recovery
      WHERE state IN ('retry_pending','retry_started','reset_wait')`.pipe(
      Effect.map((rows) => rows.map((row) => decode(row.payload_json))),
    ),
    close: (record) => transition(record, "closed"),
    started: (record) => transition(record, "retry_started"),
  });
});
export const layer = Layer.effect(RecoveryStore, make);
