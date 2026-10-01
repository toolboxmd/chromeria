import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

// Fork columns stay outside the numbered migration history reserved for upstream.
export const ensureThreadPeopleSchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const sessions = yield* sql<{ readonly name: string }>`PRAGMA table_info(auth_sessions)`;
  if (!sessions.some((column) => column.name === "person")) {
    yield* sql`ALTER TABLE auth_sessions ADD COLUMN person TEXT`;
  }
  const threads = yield* sql<{ readonly name: string }>`PRAGMA table_info(projection_threads)`;
  if (!threads.some((column) => column.name === "owner")) {
    yield* sql`ALTER TABLE projection_threads ADD COLUMN owner TEXT`;
  }
  if (!threads.some((column) => column.name === "co_owners_json")) {
    yield* sql`ALTER TABLE projection_threads ADD COLUMN co_owners_json TEXT NOT NULL DEFAULT '[]'`;
  }
});
