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
const PREVIEW_SOURCE_CHARS = 1_000;

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
const ESCAPABLE = /[!-/:-@[-`{-~]/;
const FENCE = /^\s{0,3}(`{3,}|~{3,})/;
const QUOTE = /^\s*(?:>\s?)+/;
// Private-use marks around the index of a kept piece, so prose rules never touch it.
const KEPT = /(\d+)/g;

/**
 * Inline syntax off one line of prose. Code spans and backslash-escaped characters are set aside
 * first and come back as written, so emphasis and link rules only see prose.
 */
function stripInline(line: string): string {
  const kept: string[] = [];
  const keep = (value: string) => `${kept.push(value) - 1}`;
  let prose = "";
  let index = 0;
  while (index < line.length) {
    const char = line[index]!;
    if (char === "\\" && ESCAPABLE.test(line[index + 1] ?? "")) {
      prose += keep(line[index + 1]!);
      index += 2;
      continue;
    }
    if (char !== "`") {
      prose += char;
      index += 1;
      continue;
    }
    let length = 0;
    while (line[index + length] === "`") length += 1;
    // A span closes at the next backtick run of exactly the same length.
    let close = index + length;
    for (;;) {
      close = line.indexOf("`".repeat(length), close);
      if (close === -1 || (line[close - 1] !== "`" && line[close + length] !== "`")) break;
      while (line[close] === "`") close += 1;
    }
    if (close === -1) {
      prose += "`".repeat(length);
      index += length;
      continue;
    }
    const code = line.slice(index + length, close);
    prose += keep(/^ .*\S.* $/.test(code) ? code.slice(1, -1) : code);
    index = close + length;
  }
  return prose
    .replace(/<!--.*?-->/g, " ")
    .replace(IMAGE, "$1")
    .replace(LINK, "$1")
    .replace(REFERENCE_LINK, "$1")
    .replace(/\*\*|__|~~/g, "")
    .replace(/(?<![\w*])[*_]((?:(?![*_]).)+?)[*_](?![\w*])/g, "$1")
    .replace(KEPT, (_match, keptIndex: string) => kept[Number(keptIndex)]!);
}

/** A line's text without its heading or list marker; the quote marker is already off. */
function stripBlock(line: string): string {
  const heading = /^\s{0,3}#{1,6}(?:\s+(.*?))?(?:\s+#+)?\s*$/.exec(line);
  if (heading) return heading[1] ?? "";
  return line.replace(/^\s*(?:[-*+]\s+(?:\[[ xX]\]\s+)?|\d+[.)]\s+)/, "");
}

/** One line of plain text from a message's Markdown, capped; null when nothing readable is left. */
export function conversationPreviewText(markdown: string): string | null {
  const lines: string[] = [];
  let fence: { readonly marker: string; readonly quoted: boolean } | null = null;
  let inComment = false;
  for (const raw of assistantCitationsToPlainText(markdown).split("\n")) {
    const unquoted = raw.replace(QUOTE, "");
    if (fence !== null) {
      const content = fence.quoted ? unquoted : raw;
      const marker = /^\s{0,3}(`{3,}|~{3,})\s*$/.exec(content)?.[1];
      if (
        marker !== undefined &&
        marker[0] === fence.marker[0] &&
        marker.length >= fence.marker.length
      )
        fence = null;
      else lines.push(content);
      continue;
    }
    const opening = FENCE.exec(unquoted)?.[1];
    if (opening !== undefined) {
      fence = { marker: opening, quoted: unquoted !== raw };
      continue;
    }
    let line = unquoted;
    if (inComment) {
      const end = line.indexOf("-->");
      if (end === -1) continue;
      line = line.slice(end + 3);
      inComment = false;
    }
    const open = line.lastIndexOf("<!--");
    if (open !== -1 && !line.includes("-->", open)) {
      line = line.slice(0, open);
      inComment = true;
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
