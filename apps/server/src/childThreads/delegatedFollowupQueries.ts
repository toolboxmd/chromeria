import type { ThreadId } from "@t3tools/contracts";
import type * as SqlClient from "effect/sql/SqlClient";

/** Existing result recovery reads untransferred requested turns, including completed jobs. */
export const delegatedFollowupRecoveryQuery = (sql: SqlClient.SqlClient) => sql`
  WITH subagent_children AS MATERIALIZED (
    SELECT child.thread_id, child.payload_json
    FROM orchestration_v2_projection_threads AS child
    WHERE CASE WHEN json_valid(child.payload_json) THEN
      json_extract(child.payload_json, '$.lineage.relationshipToParent') = 'subagent'
      AND json_extract(child.payload_json, '$.lineage.parentThreadId') IS NOT NULL
      AND json_extract(child.payload_json, '$.forkedFrom.type') = 'node'
      ELSE 0 END
  ), requested_work AS MATERIALIZED (
    SELECT requested.* FROM orchestration_v2_projection_runs AS requested
    WHERE requested.thread_id IN (SELECT thread_id FROM subagent_children)
      AND requested.status IN ('completed', 'interrupted', 'failed', 'cancelled')
  )
  SELECT DISTINCT child.thread_id
  FROM subagent_children AS child
  JOIN requested_work AS requested ON requested.thread_id = child.thread_id
  JOIN orchestration_v2_projection_subagents AS task
    ON task.child_thread_id = child.thread_id
    AND (
      task.subagent_id = json_extract(requested.payload_json, '$.delegatedTaskId')
      OR (
        json_extract(requested.payload_json, '$.delegatedTaskId') IS NULL
        AND task.subagent_id = json_extract(child.payload_json, '$.forkedFrom.nodeId')
        AND (SELECT json_extract(first.payload_json, '$.delegatedTaskId')
          FROM orchestration_v2_projection_runs AS first
          WHERE first.thread_id = child.thread_id ORDER BY first.ordinal ASC LIMIT 1) IS NULL
      )
    )
  WHERE json_extract(child.payload_json, '$.lineage.relationshipToParent') = 'subagent'
    AND json_extract(child.payload_json, '$.lineage.parentThreadId') = task.thread_id
    AND json_extract(child.payload_json, '$.forkedFrom.type') = 'node'
    AND json_extract(task.payload_json, '$.origin') = 'app_owned'
    AND (json_extract(task.payload_json, '$.result') IS NULL
      OR task.subagent_id = json_extract(child.payload_json, '$.forkedFrom.nodeId'))
    AND requested.status IN ('completed', 'interrupted', 'failed', 'cancelled')
    AND (json_extract(requested.payload_json, '$.startedAt') IS NOT NULL OR requested.ordinal = 1)
    AND NOT EXISTS (
      SELECT 1 FROM orchestration_v2_projection_messages AS message
      WHERE message.message_id = json_extract(requested.payload_json, '$.userMessageId')
        AND json_extract(message.payload_json, '$.notification.source.kind') = 'monitor'
    )
    AND NOT EXISTS (
      SELECT 1 FROM orchestration_v2_projection_runs AS successor
      WHERE successor.thread_id = child.thread_id
        AND json_extract(successor.payload_json, '$.restartContinuationOfRunId') = requested.run_id
    )
    AND NOT EXISTS (
      SELECT 1 FROM orchestration_v2_effect_outbox AS continuation
      WHERE continuation.effect_id = 'effect:restart-continuation:' || requested.run_id
        AND continuation.status IN ('pending', 'running')
    )
    AND NOT EXISTS (
      SELECT 1 FROM orchestration_v2_projection_context_transfers AS result
      WHERE result.source_thread_id = child.thread_id
        AND result.target_thread_id = task.thread_id
        AND result.type = 'subagent_result'
        AND (json_extract(result.payload_json, '$.sourcePoint.runId') = requested.run_id
          OR json_extract(result.payload_json, '$.sourcePoint.runId') IS NULL)
    )
  UNION
  -- Native first-result recovery also covers children whose task metadata is absent.
  SELECT child.thread_id FROM subagent_children AS child
  WHERE (SELECT status FROM orchestration_v2_projection_runs AS latest
    WHERE latest.thread_id = child.thread_id
      AND NOT (latest.status = 'queued' AND json_extract(latest.payload_json, '$.queueHeld') IS 1)
    ORDER BY latest.ordinal DESC LIMIT 1
  ) IN ('completed', 'interrupted', 'failed', 'cancelled', 'rolled_back')
  AND NOT EXISTS (
    SELECT 1 FROM orchestration_v2_projection_context_transfers AS result
    WHERE result.source_thread_id = child.thread_id
      AND result.target_thread_id = json_extract(child.payload_json, '$.lineage.parentThreadId')
      AND result.type = 'subagent_result'
  )
`;

/** Sparse shell input from bound runs and the native restart outbox, without extra state. */
export const pendingDelegatedChildQuery = (
  sql: SqlClient.SqlClient,
  parentThreadIds: ReadonlyArray<ThreadId>,
) => sql<{
  readonly thread_id: string;
  readonly payload_json: string;
  readonly subagent_id: string;
}>`
  SELECT DISTINCT task.thread_id, task.payload_json, task.subagent_id
  FROM orchestration_v2_projection_subagents AS task
  JOIN orchestration_v2_projection_threads AS child ON child.thread_id = task.child_thread_id
  JOIN orchestration_v2_projection_runs AS requested
    ON requested.thread_id = child.thread_id
    AND json_extract(requested.payload_json, '$.delegatedTaskId') = task.subagent_id
  WHERE task.thread_id IN ${sql.in(parentThreadIds)}
    AND child.deleted_at IS NULL
    AND json_extract(child.payload_json, '$.archivedAt') IS NULL
    AND json_extract(task.payload_json, '$.origin') = 'app_owned'
    AND json_extract(task.payload_json, '$.result') IS NOT NULL
    AND json_extract(task.payload_json, '$.completionDelivery.state') IS NOT 'disposed'
    AND NOT EXISTS (
      SELECT 1 FROM orchestration_v2_projection_messages AS message
      WHERE message.message_id = json_extract(requested.payload_json, '$.userMessageId')
        AND json_extract(message.payload_json, '$.notification.source.kind') = 'monitor'
    )
    AND (
      requested.status IN ('preparing', 'starting', 'running', 'waiting')
      OR (requested.status = 'queued' AND json_extract(requested.payload_json, '$.queueHeld') IS NOT 1)
      OR (
        NOT EXISTS (
          SELECT 1 FROM orchestration_v2_projection_runs AS successor
          WHERE successor.thread_id = child.thread_id
            AND json_extract(successor.payload_json, '$.restartContinuationOfRunId') = requested.run_id
        )
        AND EXISTS (
          SELECT 1 FROM orchestration_v2_effect_outbox AS continuation
          WHERE continuation.effect_id = 'effect:restart-continuation:' || requested.run_id
            AND continuation.status IN ('pending', 'running')
        )
      )
    )
`;
