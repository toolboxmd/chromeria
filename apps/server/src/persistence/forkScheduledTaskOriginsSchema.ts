import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

/** Fork ownership survives scheduled fires without consuming upstream migration numbers. */
export const ensureScheduledTaskOriginsSchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  // Writable SQLite transactions use BEGIN IMMEDIATE, serializing CLI/server startup.
  yield* sql.withTransaction(
    Effect.gen(function* () {
      const columns = yield* sql<{ readonly name: string }>`PRAGMA table_info(scheduled_tasks)`;
      if (!columns.some((column) => column.name === "link_origin_json"))
        yield* sql`ALTER TABLE scheduled_tasks ADD COLUMN link_origin_json TEXT`;
    }),
  );
});
