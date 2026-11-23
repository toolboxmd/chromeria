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
// Any indent: a fence nested in a list item is indented by the item.
const FENCE = /^\s*(`{3,}|~{3,})/;
const LIST_ITEM = /^\s*(?:[-*+]|\d+[.)])\s+/;
const indentOf = (line: string) => /^[ \t]*/.exec(line)![0].replaceAll("\t", "    ").length;
// Private-use marks around the index of a kept piece, so prose rules never touch it.
const KEPT = /\uE000(\d+)\uE001/g;

/**
 * Inline syntax off one line of prose. Code spans, backslash-escaped characters and HTML comments
 * are found first, in one scan: kept pieces come back as written, comments go, and only the rest
 * meets the emphasis and link rules. `state.inComment` carries a comment across lines.
 */
function stripInline(line: string, state: { inComment: boolean }): string {
  const kept: string[] = [];
  const keep = (value: string) => `\uE000${kept.push(value) - 1}\uE001`;
  let prose = "";
  let index = 0;
  while (index < line.length) {
    if (state.inComment) {
      const end = line.indexOf("-->", index);
      if (end === -1) break;
      prose += " ";
      index = end + 3;
      state.inComment = false;
      continue;
    }
    const char = line[index]!;
    if (char === "\\" && ESCAPABLE.test(line[index + 1] ?? "")) {
      prose += keep(line[index + 1]!);
      index += 2;
      continue;
    }
    if (line.startsWith("<!--", index)) {
      state.inComment = true;
      index += 4;
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
  return (
    prose
      .replace(IMAGE, "$1")
      .replace(LINK, "$1")
      .replace(REFERENCE_LINK, "$1")
      // Underscores stay: in replies they name files and identifiers far more often than emphasis.
      .replace(/\*\*|~~/g, "")
      .replace(/(?<![\w*])\*((?:(?!\*).)+?)\*(?![\w*])/g, "$1")
      .replace(KEPT, (_match, keptIndex: string) => kept[Number(keptIndex)]!)
  );
}

/** Up to `limit` blockquote markers off a line, and how many there were. */
function unquote(line: string, limit = Number.POSITIVE_INFINITY) {
  let depth = 0;
  let rest = line;
  for (
    let marker = /^ {0,3}> ?/.exec(rest);
    marker && depth < limit;
    marker = /^ {0,3}> ?/.exec(rest)
  ) {
    rest = rest.slice(marker[0].length);
    depth += 1;
  }
  return { depth, rest };
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
  let fence: { readonly marker: string; readonly depth: number } | null = null;
  const state = { inComment: false };
  // An indented code block starts after a blank line, outside a list, whose lines indent alike.
  let afterBlank = true;
  // The column a list item's content starts at; its indented code sits 4 columns further in.
  let listIndent: number | null = null;
  let inIndentedCode = false;
  // Private-use characters show nothing and would collide with the kept-piece marks.
  const text = assistantCitationsToPlainText(markdown).replace(/[\uE000\uE001]/g, "");
  for (const raw of text.split("\n")) {
    if (fence !== null) {
      const quoted = unquote(raw, fence.depth);
      // A fence inside a quote ends with the quote.
      if (quoted.depth === fence.depth) {
        const marker = /^\s*(`{3,}|~{3,})\s*$/.exec(quoted.rest)?.[1];
        if (
          marker !== undefined &&
          marker[0] === fence.marker[0] &&
          marker.length >= fence.marker.length
        )
          fence = null;
        else lines.push(quoted.rest);
        continue;
      }
      fence = null;
    }
    const quoted = unquote(raw);
    const opening = state.inComment ? undefined : FENCE.exec(quoted.rest)?.[1];
    if (opening !== undefined) {
      fence = { marker: opening, depth: quoted.depth };
      continue;
    }
    const line = quoted.rest;
    const blank = line.trim().length === 0;
    if (!state.inComment && !blank) {
      inIndentedCode = indentOf(line) >= (listIndent ?? 0) + 4 && (afterBlank || inIndentedCode);
      if (!inIndentedCode) {
        const item = LIST_ITEM.exec(line);
        if (item) listIndent = item[0].length;
        else if (afterBlank && indentOf(line) === 0) listIndent = null;
      }
    }
    afterBlank = blank;
    if (inIndentedCode) {
      if (!blank) lines.push(line.trim());
      continue;
    }
    // Reference definitions, rules and table delimiter rows have no words.
    if (!state.inComment) {
      if (/^\s{0,3}\[[^\]]+\]:\s*\S/.test(line)) continue;
      if (/^\s*(?:[-*_]\s*){3,}$/.test(line)) continue;
      if (/^\s*\|?(?:\s*:?-+:?\s*\|)+\s*:?-*:?\s*$/.test(line)) continue;
    }
    lines.push(stripInline(state.inComment ? line : stripBlock(line), state));
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
