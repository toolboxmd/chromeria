import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

/** SQL-only hooks run in the existing credential/link owner's transaction. */
export const schema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS remote_secret_requests (
    request_id TEXT PRIMARY KEY, environment_id TEXT NOT NULL, generation TEXT NOT NULL,
    namespace TEXT NOT NULL, origin_json TEXT NOT NULL, source_record_id TEXT NOT NULL,
    session_id TEXT NOT NULL, credential_identity TEXT NOT NULL, key_digest TEXT NOT NULL,
    input_digest TEXT NOT NULL, project_id TEXT NOT NULL, family TEXT NOT NULL,
    status TEXT NOT NULL, attempt_id TEXT, consumer_json TEXT, expires_at REAL NOT NULL,
    UNIQUE(environment_id,generation,namespace,key_digest))`;
  yield* sql`CREATE TABLE IF NOT EXISTS remote_secret_consumers (operation_id TEXT PRIMARY KEY, namespace TEXT NOT NULL, client_key TEXT NOT NULL, input_digest TEXT NOT NULL, generation TEXT NOT NULL, environment_id TEXT NOT NULL, query_json TEXT NOT NULL, UNIQUE(namespace,client_key))`;
  yield* sql`CREATE TABLE IF NOT EXISTS remote_secret_obligations (
    request_id TEXT PRIMARY KEY, environment_id TEXT NOT NULL, generation TEXT NOT NULL,
    thread_id TEXT NOT NULL, run_id TEXT NOT NULL, turn_item_id TEXT NOT NULL, origin_json TEXT NOT NULL,
    create_json TEXT NOT NULL, authority_json TEXT NOT NULL, attempt_id TEXT NOT NULL,
    pending INTEGER NOT NULL DEFAULT 0, ack_json TEXT, expires_at REAL NOT NULL)`;
});
export const invalidateLink = (environmentId: string, generation: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* schema;
    yield* sql`UPDATE remote_secret_obligations SET pending=1 WHERE environment_id=${environmentId} AND generation=${generation} AND ack_json IS NULL`;
  });
export const invalidateSessions = (sessionIds: ReadonlyArray<string>) =>
  Effect.gen(function* () {
    if (sessionIds.length === 0) return;
    const sql = yield* SqlClient.SqlClient;
    yield* schema;
    // A locally observed destination revocation denies immediately; protected copy cleanup is replayable.
    yield* sql`UPDATE remote_secret_requests SET status='retired' WHERE ${sql.in("session_id", sessionIds)} AND status NOT IN ('consumed','declined','retired')`;
  });

export const reconcileObligations = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* schema;
  const tables = yield* sql<{
    name: string;
  }>`SELECT name FROM sqlite_master WHERE name='orchestration_v2_projection_runs'`;
  if (tables.length === 0) return;
  const now = yield* Clock.currentTimeMillis;
  yield* sql`UPDATE remote_secret_obligations SET pending=1 WHERE ack_json IS NULL AND EXISTS(
    SELECT 1 FROM peer_environment_links p WHERE p.environment_id=remote_secret_obligations.environment_id
    AND round(unixepoch(p.expires_at,'subsec')*1000)<=${now})`;
  yield* sql`UPDATE remote_secret_obligations SET pending=1 WHERE ack_json IS NULL AND (
    expires_at <= ${now} OR
    EXISTS(SELECT 1 FROM orchestration_v2_projection_turn_items i WHERE i.turn_item_id=remote_secret_obligations.turn_item_id
      AND i.thread_id=remote_secret_obligations.thread_id AND json_extract(i.payload_json,'$.secretStatus')='cancelled') OR
    NOT EXISTS(SELECT 1 FROM orchestration_v2_projection_runs r JOIN orchestration_v2_projection_threads t ON t.thread_id=r.thread_id
      WHERE r.run_id=remote_secret_obligations.run_id AND r.thread_id=remote_secret_obligations.thread_id
      AND r.status IN ('preparing','starting','running') AND t.deleted_at IS NULL AND json_extract(t.payload_json,'$.archivedAt') IS NULL))`;
});
