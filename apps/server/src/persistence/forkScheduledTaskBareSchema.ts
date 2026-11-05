import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

/** Unresolved destination intent survives every scheduled fire. */
export const ensureScheduledTaskBareSchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.withTransaction(
    Effect.gen(function* () {
      const columns = yield* sql<{ readonly name: string }>`PRAGMA table_info(scheduled_tasks)`;
      if (!columns.some((column) => column.name === "bare_json"))
        yield* sql`ALTER TABLE scheduled_tasks ADD COLUMN bare_json TEXT`;
    }),
  );
});
