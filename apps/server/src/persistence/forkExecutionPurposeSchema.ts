import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

/** Existing schedules retain absent intent and continue as warned legacy work. */
export const ensureExecutionPurposeSchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.withTransaction(
    Effect.gen(function* () {
      const columns = yield* sql<{ readonly name: string }>`PRAGMA table_info(scheduled_tasks)`;
      if (!columns.some((column) => column.name === "purpose_intent_json")) {
        yield* sql`ALTER TABLE scheduled_tasks ADD COLUMN purpose_intent_json TEXT`;
      }
    }),
  );
});
