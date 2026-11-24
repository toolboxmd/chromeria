import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export const ensureScheduledTaskRunsIndex = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    CREATE INDEX IF NOT EXISTS fork_scheduled_task_messages_task_idx
    ON orchestration_v2_projection_messages(json_extract(payload_json, '$.scheduledTaskId'), message_id)
    WHERE message_id >= 'scheduled-task-message:'
      AND message_id < 'scheduled-task-message;'
      AND json_valid(payload_json)
  `;
});
