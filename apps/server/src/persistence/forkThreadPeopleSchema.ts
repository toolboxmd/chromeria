import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

// Fork columns stay outside the numbered migration history reserved for upstream.
// Thread owners live in the v2 thread payload; only the device's person needs a column.
export const ensureThreadPeopleSchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const sessions = yield* sql<{ readonly name: string }>`PRAGMA table_info(auth_sessions)`;
  if (!sessions.some((column) => column.name === "person")) {
    yield* sql`ALTER TABLE auth_sessions ADD COLUMN person TEXT`;
  }
});
