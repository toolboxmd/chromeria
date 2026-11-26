import {
  PROVIDER_SEND_TURN_MAX_INPUT_CHARS,
  ThreadBare,
  ThreadId,
  type OrchestrationV2ConversationMessage,
  type ScheduledTask,
} from "@t3tools/contracts";
import { expandAssistantCitationsForProvider } from "@t3tools/shared/assistantCitations";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { bareT3ToolNames } from "@t3tools/provider-core/server/bareLaunch";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { isSlashCommandMessage } from "../orchestration-v2/ContextHandoffDelivery.ts";

const settlementSuffix =
  '\n\n[T3 Code scheduled run] At the end of this run, if the user has a decision, question, approval, failure, or result to attend to, leave this thread open. Otherwise call t3_thread_organize with action "settle" and omit threadId to settle your own thread.';

export function appendScheduledRunSettlement(prompt: string): string {
  return isSlashCommandMessage({ role: "user", text: prompt })
    ? prompt
    : prompt.trimEnd() + settlementSuffix;
}

export function providerPromptExceedsInputLimit(
  message: Pick<OrchestrationV2ConversationMessage, "text" | "scheduledTaskId">,
): boolean {
  const { text, scheduledTaskId } = message;
  const authored =
    scheduledTaskId !== undefined && text.endsWith(settlementSuffix)
      ? text.slice(0, -settlementSuffix.length)
      : text;
  return expandAssistantCitationsForProvider(authored).length > PROVIDER_SEND_TURN_MAX_INPUT_CHARS;
}

export const ownedCheckedRunThreadId = (runId: string) => ThreadId.make(`scheduled-run:${runId}`);

const decodeBareContent = Schema.decodeUnknownOption(Schema.fromJsonString(ThreadBare));

export const scheduledRunCanSettle = Effect.fn("ScheduledTaskService.runCanSettle")(function* (
  bare: ScheduledTask["bare"],
  existingThreadId: ThreadId | null,
) {
  if (bare === undefined) return true;
  if (existingThreadId !== null) {
    const sql = yield* SqlClient.SqlClient;
    const [thread] = yield* sql<{ bare_json: string | null }>`
      SELECT json_extract(payload_json, '$.bare') AS bare_json
      FROM orchestration_v2_projection_threads WHERE thread_id = ${existingThreadId}
    `.pipe(NodeSqliteClient.readOnly);
    return decodeBareContent(thread?.bare_json).pipe(
      Option.match({
        onNone: () => false,
        onSome: (content) => bareT3ToolNames(content)?.includes("t3_thread_organize") ?? true,
      }),
    );
  }
  const additions = (bare.additions ?? [])
    .filter((addition) => addition.type === "mcp")
    .filter((addition) => addition.server === "t3-code");
  if (additions.some((addition) => addition.tool === undefined)) return true;
  if (additions.length === 0) return false;
  const { canonicalT3ToolName } = yield* Effect.promise(() => import("../mcp/t3ToolNames.ts"));
  return additions.some(
    (addition) =>
      addition.tool !== undefined && canonicalT3ToolName(addition.tool) === "t3_thread_organize",
  );
});

// An existing threadId can be an explicit binding or a thread this task launched.
// The creation event and projected initial message survive event log compaction.
export const ownsScheduledThread = Effect.fn("ScheduledTaskService.ownsScheduledThread")(function* (
  task: Pick<ScheduledTask, "id" | "threadId">,
) {
  if (task.threadId === null) return true;
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql`
      SELECT 1
      FROM orchestration_events AS creation
      JOIN orchestration_v2_projection_messages AS initial
        ON initial.message_id = 'scheduled-task-message:' || substr(creation.command_id, ${"scheduled-task:".length + 1})
        AND initial.thread_id = creation.stream_id
        AND json_extract(initial.payload_json, '$.scheduledTaskId') = ${task.id}
      WHERE creation.stream_id = ${task.threadId}
        AND creation.aggregate_kind = 'thread'
        AND creation.application_event_version = 2
        AND creation.event_type = 'thread.created'
        AND creation.command_id GLOB 'scheduled-task:*'
      LIMIT 1
    `.pipe(NodeSqliteClient.readOnly);
  return rows.length > 0;
});
