import {
  MessageId,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2LatestVisibleMessageSummary,
  type OrchestrationV2Run,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import { assistantCitationsToPlainText } from "@t3tools/shared/assistantCitations";
import { isAttachmentOnlyBootstrapPrompt } from "@t3tools/shared/composerContextReferences";
import * as DateTime from "effect/DateTime";
import type * as SqlClient from "effect/sql/SqlClient";

import { indexedMessageCondition } from "../orchestration-v2/messageSearchIndex.ts";

/** One list row shows one line; this covers a wide sidebar and keeps each shell small. */
export const CONVERSATION_PREVIEW_MAX_CHARS = 64;
// Markdown is stripped from at most this much of the stored text.
const PREVIEW_SOURCE_CHARS = 2_000;

/**
 * The newest message a conversation list may quote: a finished assistant message or one the user
 * wrote. The same messages search indexes, so wakes, compaction prompts and attachment
 * placeholders never show. Exclude system notices, such as keep-warm pings and their replies,
 * both here and in `latestPreviewMessageSql`.
 */
function isConversationPreviewMessage(
  message: Pick<OrchestrationV2ConversationMessage, "role" | "createdBy" | "streaming" | "text">,
): boolean {
  if (message.streaming) return false;
  if (message.role !== "assistant" && !(message.role === "user" && message.createdBy === "user"))
    return false;
  return !isAttachmentOnlyBootstrapPrompt(message.text);
}

// A rolled-back run left the conversation; a queued or unstarted cancelled one never reached it.
function leftConversation(run: Pick<OrchestrationV2Run, "status" | "startedAt">): boolean {
  return (
    run.status === "rolled_back" ||
    run.status === "queued" ||
    (run.status === "cancelled" && run.startedAt === null)
  );
}

export function conversationPreviewFromProjection(
  projection: Pick<OrchestrationV2ThreadProjection, "thread" | "messages" | "runs">,
): OrchestrationV2LatestVisibleMessageSummary | null {
  // As in search's predicate: no list shows a subagent thread.
  if (projection.thread.lineage.relationshipToParent === "subagent") return null;
  const { messages, runs } = projection;
  const leftRunIds = new Set(runs.filter(leftConversation).map((run) => run.id));
  let latest: OrchestrationV2ConversationMessage | null = null;
  for (const message of messages) {
    if (!isConversationPreviewMessage(message)) continue;
    if (message.runId !== null && leftRunIds.has(message.runId)) continue;
    if (
      latest === null ||
      DateTime.toEpochMillis(message.createdAt) > DateTime.toEpochMillis(latest.createdAt) ||
      (DateTime.toEpochMillis(message.createdAt) === DateTime.toEpochMillis(latest.createdAt) &&
        message.id > latest.id)
    )
      latest = message;
  }
  if (latest === null) return null;
  // Characters, as SQLite's substr counts them.
  const source = Array.from(latest.text.slice(0, PREVIEW_SOURCE_CHARS * 2))
    .slice(0, PREVIEW_SOURCE_CHARS)
    .join("");
  const text = conversationPreviewText(source);
  return text === null
    ? null
    : { id: latest.id, role: latest.role, text, updatedAt: latest.updatedAt };
}

/**
 * A shell query column: the newest preview message of thread `t` as JSON, read through the
 * messages' (thread_id, created_at) index. Decode it with `conversationPreviewFromRow`.
 */
export const latestPreviewMessageSql = (sql: SqlClient.SqlClient) => sql`(
  SELECT json_object(
    'id', message.message_id,
    'role', message.role,
    'text', substr(json_extract(message.payload_json, '$.text'), 1, ${PREVIEW_SOURCE_CHARS}),
    'updatedAt', message.updated_at
  )
  FROM orchestration_v2_projection_messages message
  WHERE message.thread_id = t.thread_id
    AND ${indexedMessageCondition(sql, "message")}
    AND NOT EXISTS (
      SELECT 1 FROM orchestration_v2_projection_runs run
      WHERE run.run_id = message.run_id
        AND (
          run.status IN ('rolled_back', 'queued')
          OR (run.status = 'cancelled' AND json_extract(run.payload_json, '$.startedAt') IS NULL)
        )
    )
  ORDER BY message.created_at DESC, message.message_id DESC
  LIMIT 1
)`;

export function conversationPreviewFromRow(
  json: string | null,
): OrchestrationV2LatestVisibleMessageSummary | null {
  if (json === null) return null;
  const row = JSON.parse(json) as {
    readonly id: string;
    readonly role: OrchestrationV2ConversationMessage["role"];
    readonly text: string | null;
    readonly updatedAt: string;
  };
  const text = conversationPreviewText(row.text ?? "");
  return text === null
    ? null
    : {
        id: MessageId.make(row.id),
        role: row.role,
        text,
        updatedAt: DateTime.makeUnsafe(row.updatedAt),
      };
}

// A link target may hold one level of parentheses, as in `https://x.dev/a_(b)`.
const LINK_TARGET = String.raw`\((?:[^()\s]|\([^()\s]*\))*(?:\s+"[^"]*")?\)`;
const IMAGE = new RegExp(String.raw`!\[([^\]]*)\]` + LINK_TARGET, "g");
const LINK = new RegExp(String.raw`\[([^\]]*)\]` + LINK_TARGET, "g");
const REFERENCE_LINK = /!?\[([^\]]*)\]\[[^\]]*\]/g;
const FENCE = /^\s{0,3}(`{3,}|~{3,})/;

/** Inline syntax outside code spans; a code span keeps its text as written. */
function stripInline(line: string): string {
  return line
    .split(/(`+)([^`]*?)\1/g)
    .map((part, index) => {
      if (index % 3 === 1) return "";
      if (index % 3 === 2) return part;
      return part
        .replace(IMAGE, "$1")
        .replace(LINK, "$1")
        .replace(REFERENCE_LINK, "$1")
        .replace(/\*\*|__|~~|`+/g, "")
        .replace(/(?<![\w*])[*_]((?:(?![*_]).)+?)[*_](?![\w*])/g, "$1");
    })
    .join("");
}

/** A line's text without its block syntax: quote, heading or list markers. */
function stripBlock(line: string): string {
  const quoted = line.replace(/^\s*(?:>\s?)+/, "");
  const heading = /^\s{0,3}#{1,6}(?:\s+(.*?))?(?:\s+#+)?\s*$/.exec(quoted);
  if (heading) return heading[1] ?? "";
  return quoted.replace(/^\s*(?:[-*+]\s+(?:\[[ xX]\]\s+)?|\d+[.)]\s+)/, "");
}

/** One line of plain text from a message's Markdown, capped; null when nothing readable is left. */
export function conversationPreviewText(markdown: string): string | null {
  const lines: string[] = [];
  let fence: string | null = null;
  for (const line of assistantCitationsToPlainText(markdown)
    .replace(/<!--[\s\S]*?-->/g, " ")
    .split("\n")) {
    const marker = FENCE.exec(line)?.[1];
    if (fence !== null) {
      const closes =
        marker !== undefined &&
        marker[0] === fence[0] &&
        marker.length >= fence.length &&
        line.trim() === marker;
      if (closes) fence = null;
      else lines.push(line);
      continue;
    }
    if (marker !== undefined) {
      fence = marker;
      continue;
    }
    // Reference definitions, rules and table delimiter rows have no words.
    if (/^\s{0,3}\[[^\]]+\]:\s*\S/.test(line)) continue;
    if (/^\s*(?:[-*_]\s*){3,}$/.test(line)) continue;
    if (/^\s*\|?(?:\s*:?-+:?\s*\|)+\s*:?-*:?\s*$/.test(line)) continue;
    lines.push(stripInline(stripBlock(line)));
  }
  const plain = lines.join(" ").replace(/\s+/g, " ").trim();
  if (plain.length === 0) return null;
  const chars = Array.from(plain);
  return chars.length <= CONVERSATION_PREVIEW_MAX_CHARS
    ? plain
    : `${chars
        .slice(0, CONVERSATION_PREVIEW_MAX_CHARS - 1)
        .join("")
        .trimEnd()}…`;
}
