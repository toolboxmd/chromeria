import { BareLaunchRequest, type ScheduledTaskId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { BareReadGrant } from "../orchestration-v2/bareReadGrant.ts";

// A schedule retains the authority admitted at creation. Its public request
// cannot supply a grant, and every new child still adds only its final cwd.
const ScheduledBareLaunch = Schema.Struct({
  request: BareLaunchRequest,
  readGrant: BareReadGrant,
});
export const decodeScheduledBare = Schema.decodeEffect(Schema.fromJsonString(ScheduledBareLaunch));
export const encodeScheduledBare = Schema.encodeSync(Schema.fromJsonString(ScheduledBareLaunch));
export const readScheduledBare = Effect.fn("ScheduledTasks.readBare")(function* (
  id: ScheduledTaskId,
) {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{
    readonly bare_json: string | null;
  }>`SELECT bare_json FROM scheduled_tasks WHERE task_id = ${id}`;
  const raw = rows[0]?.bare_json;
  return raw == null ? undefined : yield* decodeScheduledBare(raw);
});
