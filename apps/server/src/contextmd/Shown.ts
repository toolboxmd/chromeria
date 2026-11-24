/** Native receipt decisions only. Hook success and pre-plugin rendering never prove model input. */
import * as Option from "effect/Option";
import type * as Path from "effect/Path";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";

import type { ShownCheck } from "./ServingState.ts";

export type Source = "claude_request_replay" | "codex_rollout";
export type UnknownReason =
  | "not_checked"
  | "no_evidence_source"
  | "no_transcript"
  | "transcript_unreadable"
  | "transcript_too_large"
  | "unrecognized_format"
  | "not_found"
  | "ambiguous"
  | "no_model_response"
  | "compacted"
  | "rolled_back"
  | "final_input_unrecorded"
  | "absent_from_request"
  | "request_unverifiable";
interface NativeSlot {
  readonly id: string;
  readonly slot: number;
  readonly at: number;
  readonly turn: string | null;
}
interface Consumption {
  readonly id: string;
  readonly call: string | null;
  readonly at: number;
}
export type Evidence =
  | {
      readonly fact: "shown";
      readonly source: "claude_request_replay";
      readonly cli: string;
      readonly record: NativeSlot & { readonly turn: null };
      readonly consumed: Consumption & { readonly request: string };
    }
  | {
      readonly fact: "shown";
      readonly source: "codex_rollout";
      readonly cli: string | null;
      readonly record: NativeSlot & { readonly turn: string };
      readonly consumed: Consumption;
    }
  | {
      readonly fact: "unknown";
      readonly source: Source | null;
      readonly reason: UnknownReason;
      readonly record?: NativeSlot;
    };
export interface Served {
  readonly delivery: string;
  readonly session: string;
  readonly event: string;
  readonly text: string;
  readonly call: string | null;
  readonly turn: string | null;
  readonly createdAt: number;
  readonly settledAt: number | null;
}

export const RULE: Readonly<Record<Source, string>> = {
  claude_request_replay: "claude-1",
  codex_rollout: "codex-1",
};
export const unknown = (
  source: Source | null,
  reason: UnknownReason,
  record?: NativeSlot,
): Evidence => ({ fact: "unknown", source, reason, ...(record === undefined ? {} : { record }) });
export const backoff = (age: number) => (age < 600 ? 30 : age < 86400 ? 300 : 3600);
const WAITING: ReadonlySet<UnknownReason> = new Set([
  "not_checked",
  "transcript_unreadable",
  "not_found",
  "no_model_response",
  "final_input_unrecorded",
  "absent_from_request",
  "request_unverifiable",
]);
export const nextCheck = (evidence: Evidence, age: number, now: number) =>
  evidence.fact === "unknown" && WAITING.has(evidence.reason) ? now + backoff(age) : null;

const ObjectSchema = Schema.Record(Schema.String, Schema.Unknown);
const jsonObject = Schema.decodeUnknownOption(Schema.fromJsonString(ObjectSchema));
const Ident = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(512));
const NullableIdent = Schema.NullOr(Ident);
// Timestamp parsing is separate from structure validation so invalid dates fail closed.
const seconds = (timestamp: string) => {
  const at = Date.parse(timestamp) / 1000;
  if (!Number.isFinite(at)) throw new Error("invalid native timestamp");
  return at;
};
const decode = <S extends Schema.ConstraintDecoder<unknown>>(schema: S, raw: unknown) =>
  Schema.decodeUnknownSync(schema)(raw);

const ReplySchema = Schema.Struct({
  hookSpecificOutput: Schema.Struct({
    hookEventName: Ident,
    additionalContext: Schema.String.check(Schema.isMinLength(1)),
  }),
});
const decodeReply = Schema.decodeOption(Schema.fromJsonString(ReplySchema));
const decodeRendering = Schema.decodeUnknownOption(
  Schema.Array(Schema.Struct({ content: Schema.String })),
);
/** Denies and other non-context replies do not compete for a context slot. */
export const served = (check: ShownCheck): Served | null => {
  const body = decodeReply(check.body);
  if (Option.isNone(body)) return null;
  return {
    delivery: check.delivery,
    session: check.session,
    event: body.value.hookSpecificOutput.hookEventName,
    text: body.value.hookSpecificOutput.additionalContext,
    call: check.call,
    turn: check.turn,
    createdAt: check.createdAt,
    settledAt: check.settledAt,
  };
};
export const validPath = (
  source: Source,
  file: string | null,
  session: string,
  path: Pick<Path.Path, "isAbsolute" | "basename">,
) =>
  file !== null &&
  Buffer.byteLength(file, "utf8") <= 4096 &&
  path.isAbsolute(file) &&
  (source === "claude_request_replay"
    ? path.basename(file) === `${session}.jsonl`
    : path.basename(file).startsWith("rollout-") &&
      path.basename(file).endsWith(`-${session}.jsonl`));
const inWindow = (at: number, d: Served) =>
  at >= d.createdAt - 1 && at <= (d.settledAt ?? d.createdAt) + 5;

type BlobText =
  | { readonly kind: "input"; readonly texts: ReadonlyArray<string> }
  | { readonly kind: "none" | "unrecognized" };
type ClaudeLine =
  | {
      readonly kind: "attachment";
      readonly uuid: string;
      readonly at: number;
      readonly session: string;
      readonly sidechain: boolean;
      readonly hookEvent: string;
      readonly hookName: string;
      readonly toolUseId: string | null;
      readonly content: ReadonlyArray<string>;
      readonly rendered: unknown;
    }
  | {
      readonly kind: "request";
      readonly id: string;
      readonly at: number;
      readonly session: string;
      readonly agent: string | null;
      readonly base: string | null;
      readonly keep: number;
      readonly tail: ReadonlyArray<string>;
      readonly version: string;
      readonly cut: boolean;
      readonly withheld: ReadonlyArray<string>;
      readonly betas: ReadonlyArray<string> | null;
      readonly contextEdits: "absent" | "thinking_only" | "other";
    }
  | {
      readonly kind: "blob";
      readonly hash: string;
      readonly session: string;
      readonly agent: string | null;
      readonly text: BlobText;
    }
  | {
      readonly kind: "response";
      readonly uuid: string;
      readonly at: number;
      readonly session: string;
      readonly sidechain: boolean;
      readonly messageId: string;
      readonly synthetic: boolean;
      readonly requestRef: string | null;
      readonly firstToolUseId: string | null;
    }
  | { readonly kind: "other"; readonly session: string | null }
  | { readonly kind: "malformed" };

const NativeCommon = { sessionId: Ident, timestamp: Schema.String };
const AttachmentSchema = Schema.Struct({
  ...NativeCommon,
  uuid: Ident,
  isSidechain: Schema.optional(Schema.Boolean),
  rendered: Schema.Unknown,
  attachment: Schema.Struct({
    type: Ident,
    hookEvent: Ident,
    hookName: Ident,
    toolUseID: Schema.optional(NullableIdent),
    content: Schema.Array(Schema.String),
  }),
});
const RequestSchema = Schema.Struct({
  ...NativeCommon,
  id: Ident,
  agentId: Schema.optional(NullableIdent),
  base: Schema.optional(NullableIdent),
  keep: Schema.Number.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0)),
  tail: Schema.Array(Ident),
  version: Ident,
  params: ObjectSchema,
  withheld: Schema.optional(Schema.Array(Schema.String)),
});
const BlobSchema = Schema.Struct({
  sessionId: Ident,
  hash: Ident,
  agentId: Schema.optional(NullableIdent),
  message: ObjectSchema,
});
const ResponseSchema = Schema.Struct({
  ...NativeCommon,
  uuid: Ident,
  isSidechain: Schema.optional(Schema.Boolean),
  isApiErrorMessage: Schema.optional(Schema.Boolean),
  requestRef: Schema.optional(NullableIdent),
  message: Schema.Struct({
    id: Ident,
    model: Schema.String,
    content: Schema.Array(Schema.Unknown),
  }),
});

const NON_TEXT = new Set([
  "image",
  "document",
  "search_result",
  "thinking",
  "redacted_thinking",
  "tool_use",
  "tool_reference",
  "server_tool_use",
  "web_search_tool_result",
  "web_fetch_tool_result",
  "advisor_tool_result",
  "code_execution_tool_result",
  "bash_code_execution_tool_result",
  "text_editor_code_execution_tool_result",
  "tool_search_tool_result",
  "mcp_tool_use",
  "mcp_tool_result",
  "mcp_tool_listing",
  "container_upload",
  "compaction",
  "fallback",
  "browser_state",
]);
/** Only input text and tool-result text count, never arbitrary strings in metadata or unknown blocks. */
const inputTexts = (content: unknown): ReadonlyArray<string> | null => {
  if (typeof content === "string") return [content];
  if (!Array.isArray(content)) return null;
  const texts: string[] = [];
  for (const block of content) {
    if (!Predicate.isObject(block) || typeof block.type !== "string") return null;
    if (block.type === "text") {
      if (typeof block.text !== "string") return null;
      texts.push(block.text);
    } else if (block.type === "tool_result") {
      if (block.content === undefined) continue;
      if (typeof block.content === "string") texts.push(block.content);
      else if (Array.isArray(block.content)) {
        for (const nested of block.content) {
          if (!Predicate.isObject(nested) || typeof nested.type !== "string") return null;
          if (nested.type === "text") {
            if (typeof nested.text !== "string") return null;
            texts.push(nested.text);
          } else if (!NON_TEXT.has(nested.type)) return null;
        }
      } else return null;
    } else if (!NON_TEXT.has(block.type)) return null;
  }
  return texts;
};
const blobText = (message: Readonly<Record<string, unknown>>): BlobText => {
  if (message.role === "assistant") return { kind: "none" };
  if (message.role !== "user" && message.role !== "system") return { kind: "unrecognized" };
  if ("clear_at" in message) return { kind: "none" };
  if (
    Object.keys(message).some((k) => !["role", "content", "output_config", "clear_at"].includes(k))
  )
    return { kind: "unrecognized" };
  const texts = inputTexts(message.content);
  return texts === null ? { kind: "unrecognized" } : { kind: "input", texts };
};

export const decodeClaudeLine = (line: string): ClaudeLine => {
  try {
    const value = jsonObject(line);
    if (Option.isNone(value) || typeof value.value.type !== "string") return { kind: "malformed" };
    const raw = value.value;
    switch (raw.type) {
      case "attachment": {
        const type = decode(
          Schema.Struct({ attachment: Schema.Struct({ type: Schema.String }) }),
          raw,
        );
        if (type.attachment.type !== "hook_additional_context") break;
        const a = decode(AttachmentSchema, raw);
        return {
          kind: "attachment",
          uuid: a.uuid,
          at: seconds(a.timestamp),
          session: a.sessionId,
          sidechain: a.isSidechain ?? false,
          hookEvent: a.attachment.hookEvent,
          hookName: a.attachment.hookName,
          toolUseId: a.attachment.toolUseID ?? null,
          content: a.attachment.content,
          rendered: a.rendered,
        };
      }
      case "api-request": {
        const r = decode(RequestSchema, raw);
        if (!Number.isSafeInteger(r.keep + r.tail.length)) return { kind: "malformed" };
        const betaSchema = Schema.Struct({
          betas: Schema.optional(Schema.Array(Schema.String)),
          anthropic_beta: Schema.optional(Schema.Array(Schema.String)),
        });
        const beta = decode(betaSchema, r.params);
        const edits = r.params.context_management;
        const thinking =
          Predicate.isObject(edits) &&
          Array.isArray(edits.edits) &&
          edits.edits.every(
            (e) =>
              Predicate.isObject(e) &&
              typeof e.type === "string" &&
              /^clear_thinking_[0-9]{8}$/u.test(e.type),
          );
        return {
          kind: "request",
          id: r.id,
          at: seconds(r.timestamp),
          session: r.sessionId,
          agent: r.agentId ?? null,
          base: r.base ?? null,
          keep: r.keep,
          tail: r.tail,
          version: r.version,
          cut: "cut" in raw,
          withheld: r.withheld ?? [],
          betas:
            beta.betas === undefined && beta.anthropic_beta === undefined
              ? null
              : [...(beta.betas ?? []), ...(beta.anthropic_beta ?? [])],
          contextEdits: edits === undefined ? "absent" : thinking ? "thinking_only" : "other",
        };
      }
      case "api-request-blob": {
        const b = decode(BlobSchema, raw);
        return {
          kind: "blob",
          hash: b.hash,
          session: b.sessionId,
          agent: b.agentId ?? null,
          text: blobText(b.message),
        };
      }
      case "assistant": {
        const m = decode(ResponseSchema, raw);
        const first = m.message.content[0];
        const call =
          Predicate.isObject(first) && first.type === "tool_use"
            ? decode(Schema.Struct({ id: Ident }), first).id
            : null;
        return {
          kind: "response",
          uuid: m.uuid,
          at: seconds(m.timestamp),
          session: m.sessionId,
          sidechain: m.isSidechain ?? false,
          messageId: m.message.id,
          requestRef: m.requestRef ?? null,
          synthetic: m.message.model === "<synthetic>" || m.isApiErrorMessage === true,
          firstToolUseId: call,
        };
      }
    }
    const other = decode(Schema.Struct({ sessionId: Schema.optional(NullableIdent) }), raw);
    return { kind: "other", session: other.sessionId ?? null };
  } catch {
    return { kind: "malformed" };
  }
};

type Attachment = Extract<ClaudeLine, { kind: "attachment" }>;
type Request = Extract<ClaudeLine, { kind: "request" }>;
type Response = Extract<ClaudeLine, { kind: "response" }>;
const admitsClaude = (a: Attachment, slot: number, d: Served) =>
  a.session === d.session &&
  !a.sidechain &&
  a.hookEvent === d.event &&
  (d.call === null || a.toolUseId === d.call) &&
  a.content[slot] === d.text &&
  inWindow(a.at, d);

export const claudeEvidence = (
  lines: ReadonlyArray<ClaudeLine>,
  d: Served,
  rivals: ReadonlyArray<Served>,
): Evidence => {
  const source = "claude_request_replay";
  if (
    lines.some((l) => l.kind === "malformed") ||
    !lines.some((l) => "session" in l && l.session === d.session)
  )
    return unknown(source, "unrecognized_format");
  const slots = lines.flatMap((l) =>
    l.kind === "attachment"
      ? l.content.flatMap((_, slot) => (admitsClaude(l, slot, d) ? [{ a: l, slot }] : []))
      : [],
  );
  if (slots.length === 0) return unknown(source, "not_found");
  const chosen = slots[0]!;
  if (
    slots.length !== 1 ||
    rivals.some((r) => r.delivery !== d.delivery && admitsClaude(chosen.a, chosen.slot, r))
  )
    return unknown(source, "ambiguous");
  const { a, slot } = chosen;
  const record = { id: a.uuid, slot, at: a.at, turn: null };
  const u = (reason: UnknownReason) => unknown(source, reason, record);
  const rendering = decodeRendering(a.rendered);
  const inner = `${a.hookName} hook additional context: ${a.content.join("\n")}`;
  if (
    Option.isNone(rendering) ||
    rendering.value.length !== 1 ||
    rendering.value[0]!.content !== `<system-reminder>\n${inner}\n</system-reminder>`
  )
    return u("unrecognized_format");

  const requests = new Map<string, Request>();
  const blobs = new Map<string, BlobText>();
  const responses: Array<Response & { readonly index: number }> = [];
  for (const [index, line] of lines.entries()) {
    if (!("session" in line) || line.session !== d.session) continue;
    if (line.kind === "request" && line.agent === null) {
      const previous = requests.get(line.id);
      if (previous !== undefined && JSON.stringify(previous) !== JSON.stringify(line))
        return u("unrecognized_format");
      requests.set(line.id, line);
    } else if (line.kind === "blob" && line.agent === null) {
      const previous = blobs.get(line.hash);
      if (previous !== undefined && JSON.stringify(previous) !== JSON.stringify(line.text))
        return u("unrecognized_format");
      blobs.set(line.hash, line.text);
    } else if (line.kind === "response" && !line.sidechain && !line.synthetic)
      responses.push({ ...line, index });
  }
  responses.sort((x, y) => x.at - y.at || x.index - y.index);

  // Unknown prefix positions stay unknown. Resolve chains iteratively, never synthesize missing input.
  const positions = new Map<
    string,
    { readonly length: number; readonly known: ReadonlyMap<number, string> }
  >();
  for (const r of requests.values()) {
    const chain: Request[] = [];
    const visited = new Set<string>();
    let current: Request | undefined = r;
    while (current !== undefined && !positions.has(current.id)) {
      if (visited.has(current.id)) return u("unrecognized_format");
      visited.add(current.id);
      chain.push(current);
      current = current.keep > 0 && current.base !== null ? requests.get(current.base) : undefined;
    }
    for (const link of chain.toReversed()) {
      const base = link.base === null ? undefined : positions.get(link.base);
      const known = new Map<number, string>();
      if (link.keep > 0 && base !== undefined) {
        for (const [index, hash] of base.known) if (index < link.keep) known.set(index, hash);
      }
      for (const [index, hash] of link.tail.entries()) known.set(link.keep + index, hash);
      positions.set(link.id, { length: link.keep + link.tail.length, known });
    }
  }
  const eligible = (r: Request): "before" | "unverifiable" | "present" | "absent" => {
    if (r.at <= a.at) return "before";
    const list = positions.get(r.id)!;
    // Both beta spellings matter; neither a missing nor a withheld list attests a physical request.
    if (
      r.cut ||
      r.betas === null ||
      r.betas.includes("message-threads-2026-08-12") ||
      r.withheld.some((key) => ["betas", "anthropic_beta", "context_management"].includes(key)) ||
      r.contextEdits === "other" ||
      [...list.known.values()].some((hash) => blobs.get(hash)?.kind === "unrecognized")
    )
      return "unverifiable";
    const previous = responses.findLast((p) => p.at <= r.at && p.requestRef !== r.id);
    let bound = 0;
    if (previous !== undefined) {
      const q = previous.requestRef === null ? undefined : requests.get(previous.requestRef);
      if (q === undefined) return "unverifiable";
      const length = q.keep + q.tail.length;
      const anchor = list.known.get(length);
      if (list.length <= length || (anchor !== undefined && blobs.get(anchor)?.kind === "input"))
        return "unverifiable";
      bound = length + 1;
    }
    for (const [index, hash] of list.known) {
      const blob = blobs.get(hash);
      if (
        index >= bound &&
        blob?.kind === "input" &&
        blob.texts.some((text) => text.includes(inner))
      )
        return "present";
    }
    return "absent";
  };
  let absent = false,
    unverifiable = false,
    unrecorded = false;
  for (const m of responses) {
    if (m.at < a.at) continue;
    const r = m.requestRef === null ? undefined : requests.get(m.requestRef);
    if (r === undefined) {
      unrecorded = true;
      continue;
    }
    if (r.at > a.at && m.at < r.at) return u("unrecognized_format");
    const eligibility = eligible(r);
    if (eligibility === "present")
      return {
        fact: "shown",
        source,
        cli: r.version,
        record,
        consumed: { id: m.messageId, call: m.firstToolUseId, at: m.at, request: r.id },
      };
    absent ||= eligibility === "absent";
    unverifiable ||= eligibility === "unverifiable";
  }
  return u(
    absent
      ? "absent_from_request"
      : unverifiable
        ? "request_unverifiable"
        : unrecorded
          ? "final_input_unrecorded"
          : "no_model_response",
  );
};

type CodexLine =
  | { readonly kind: "malformed" }
  | { readonly kind: "retained"; readonly at: number; readonly turn: string | null }
  | {
      readonly kind: "meta";
      readonly at: number;
      readonly session: string;
      readonly cli: string | null;
    }
  | {
      readonly kind: "item";
      readonly at: number;
      readonly itemType: string;
      readonly role: string | null;
      readonly id: string | null;
      readonly call: string | null;
      readonly turn: string | null;
      readonly compaction: boolean;
      readonly slots: ReadonlyArray<{ readonly slot: number; readonly text: string }>;
    }
  | {
      readonly kind: "event";
      readonly at: number;
      readonly event: string;
      readonly turn: string | null;
      readonly item: string | null;
    }
  | {
      readonly kind: "control";
      readonly at: number;
      readonly type: string;
      readonly turn: string | null;
    };
const CodexItemSchema = Schema.Struct({
  type: Ident,
  role: Schema.optional(Schema.String),
  id: Schema.optional(NullableIdent),
  call_id: Schema.optional(NullableIdent),
  content: Schema.optional(Schema.Array(Schema.Unknown)),
  internal_chat_message_metadata_passthrough: Schema.optional(
    Schema.NullOr(
      Schema.Struct({
        turn_id: Schema.optional(NullableIdent),
        content_item_kinds: Schema.optional(Schema.NullOr(Schema.Array(Schema.String))),
      }),
    ),
  ),
});
export const decodeCodexLine = (line: string): CodexLine => {
  try {
    const value = jsonObject(line);
    if (Option.isNone(value)) return { kind: "malformed" };
    const common = decode(
      Schema.Struct({ type: Schema.String, timestamp: Schema.String }),
      value.value,
    );
    const raw = value.value;
    const at = seconds(common.timestamp);
    if (common.type === "session_meta") {
      const meta = decode(
        Schema.Struct({
          payload: Schema.Struct({ id: Ident, cli_version: Schema.optional(Ident) }),
        }),
        raw,
      );
      return { kind: "meta", at, session: meta.payload.id, cli: meta.payload.cli_version ?? null };
    }
    if (common.type === "response_item") {
      const item = decode(
        Schema.Struct({
          payload: CodexItemSchema,
          metadata: Schema.optional(
            Schema.Struct({
              compaction_output: Schema.optional(Schema.Boolean),
              delivered_assistant_message: Schema.optional(Schema.String),
            }),
          ),
        }),
        raw,
      );
      const p = item.payload;
      const passthrough = p.internal_chat_message_metadata_passthrough;
      const delivered = item.metadata?.delivered_assistant_message;
      if (delivered !== undefined) {
        // Codex serializes Code Mode send confirmations as assistant messages.
        // They are retained context, never output from sampling the model.
        if (
          p.type !== "message" ||
          p.role !== "assistant" ||
          item.metadata?.compaction_output === true ||
          (delivered !== "codex:code-mode-delivery:v1:complete" &&
            !delivered.startsWith("codex:code-mode-delivery:v1:incomplete:"))
        )
          return { kind: "malformed" };
        return { kind: "retained", at, turn: passthrough?.turn_id ?? null };
      }
      const kinds = passthrough?.content_item_kinds;
      const slots: Array<{ slot: number; text: string }> = [];
      if (kinds !== undefined && kinds !== null) {
        if (p.content === undefined || p.content.length !== kinds.length)
          return { kind: "malformed" };
        for (const [slot, kind] of kinds.entries()) {
          if (kind !== "hooks.additional_context") continue;
          const c = decode(
            Schema.Struct({ type: Schema.Literal("input_text"), text: Schema.String }),
            p.content[slot],
          );
          if (
            passthrough?.turn_id === undefined ||
            passthrough.turn_id === null ||
            p.id === undefined ||
            p.id === null
          )
            return { kind: "malformed" };
          slots.push({ slot, text: c.text });
        }
      }
      return {
        kind: "item",
        at,
        itemType: p.type,
        role: p.role ?? null,
        id: p.id ?? null,
        call: p.call_id ?? null,
        turn: passthrough?.turn_id ?? null,
        compaction: item.metadata?.compaction_output ?? false,
        slots,
      };
    }
    if (common.type === "event_msg") {
      const event = decode(
        Schema.Struct({
          payload: Schema.Struct({
            type: Ident,
            turn_id: Schema.optional(NullableIdent),
            item: Schema.optional(Schema.Struct({ id: Ident })),
          }),
        }),
        raw,
      ).payload;
      return {
        kind: "event",
        at,
        event: event.type,
        turn: event.turn_id ?? null,
        item: event.item?.id ?? null,
      };
    }
    // In particular, compacted.replacement_history is never walked as native output.
    const control =
      common.type === "turn_context"
        ? decode(
            Schema.Struct({ payload: Schema.Struct({ turn_id: Schema.optional(NullableIdent) }) }),
            raw,
          )
        : null;
    return { kind: "control", at, type: common.type, turn: control?.payload.turn_id ?? null };
  } catch {
    return { kind: "malformed" };
  }
};
const OUTPUT = new Set([
  "reasoning",
  "function_call",
  "custom_tool_call",
  "local_shell_call",
  "tool_search_call",
  "web_search_call",
  "image_generation_call",
]);
const INPUT = new Set([
  "function_call_output",
  "custom_tool_call_output",
  "tool_search_output",
  "additional_tools",
  "configuration_update",
]);
const COMPACTION = new Set([
  "compaction",
  "compaction_summary",
  "context_compaction",
  "compaction_trigger",
]);
const CONTROLS = new Set([
  "token_usage_record",
  "turn_context",
  "world_state",
  "retained_context",
  "security_risk_score",
  "inter_agent_communication_metadata",
]);
type ItemClass = "output" | "possible" | "input" | "compacted" | "rolled_back" | "unknown";
const itemClass = (l: CodexLine): ItemClass => {
  if (l.kind === "malformed") return "unknown";
  if (l.kind === "meta" || l.kind === "retained") return "input";
  if (l.kind === "event")
    return l.event === "thread_rolled_back"
      ? "rolled_back"
      : l.event === "context_compacted"
        ? "compacted"
        : "input";
  if (l.kind === "control")
    return l.type === "compacted" ? "compacted" : CONTROLS.has(l.type) ? "input" : "unknown";
  if (l.compaction || COMPACTION.has(l.itemType)) return "compacted";
  if (l.itemType === "message")
    return l.role === "assistant"
      ? "output"
      : ["user", "developer", "system"].includes(l.role ?? "")
        ? "input"
        : "unknown";
  return OUTPUT.has(l.itemType)
    ? "output"
    : l.itemType === "agent_message"
      ? "possible"
      : INPUT.has(l.itemType)
        ? "input"
        : "unknown";
};
const nativeTurn = (lines: ReadonlyArray<CodexLine>, index: number): string | null => {
  for (let i = index; i >= 0; i--) {
    const l = lines[i]!;
    if (
      (l.kind === "item" || l.kind === "event" || l.kind === "control" || l.kind === "retained") &&
      l.turn !== null
    )
      return l.turn;
  }
  return null;
};
const admitsCodex = (lines: ReadonlyArray<CodexLine>, index: number, slot: number, d: Served) => {
  const l = lines[index]!;
  if (
    l.kind !== "item" ||
    l.itemType !== "message" ||
    l.role !== "developer" ||
    l.compaction ||
    l.turn === null ||
    (d.turn !== null && l.turn !== d.turn) ||
    !inWindow(l.at, d) ||
    !l.slots.some((s) => s.slot === slot && s.text === d.text)
  )
    return false;
  if (d.event === "UserPromptSubmit") {
    for (let i = index - 1; i >= 0; i--) {
      const previous = lines[i]!;
      if (previous.kind === "item" && previous.itemType === "message" && previous.role === "user")
        return true;
      if (itemClass(previous) !== "input") return false;
    }
    return false;
  }
  if (d.event !== "PostToolUse" || d.call === null) return false;
  for (let i = index - 1; i >= 0; i--) {
    const previous = lines[i]!;
    if (
      previous.kind === "event" &&
      previous.event === "item_completed" &&
      previous.item === d.call &&
      previous.turn === l.turn
    )
      return true;
    if (
      previous.kind === "item" &&
      previous.call === d.call &&
      itemClass(previous) === "output" &&
      nativeTurn(lines, i) === l.turn
    ) {
      return !lines
        .slice(i + 1, index)
        .some((o) => o.kind === "item" && o.call === d.call && INPUT.has(o.itemType));
    }
  }
  return false;
};
export const codexEvidence = (
  lines: ReadonlyArray<CodexLine>,
  d: Served,
  rivals: ReadonlyArray<Served>,
): Evidence => {
  const source = "codex_rollout";
  const meta = lines.find((l) => l.kind === "meta");
  if (
    lines.some((l) => l.kind === "malformed") ||
    meta?.kind !== "meta" ||
    meta.session !== d.session
  )
    return unknown(source, "unrecognized_format");
  const slots = lines.flatMap((l, index) =>
    l.kind === "item"
      ? l.slots.flatMap((s) =>
          admitsCodex(lines, index, s.slot, d) ? [{ l, index, slot: s.slot }] : [],
        )
      : [],
  );
  if (slots.length === 0) return unknown(source, "not_found");
  const chosen = slots[0]!;
  if (
    slots.length !== 1 ||
    rivals.some(
      (r) => r.delivery !== d.delivery && admitsCodex(lines, chosen.index, chosen.slot, r),
    )
  )
    return unknown(source, "ambiguous");
  const { l, index, slot } = chosen;
  if (l.id === null || l.turn === null) return unknown(source, "unrecognized_format");
  const record = { id: l.id, slot, at: l.at, turn: l.turn };
  const u = (reason: UnknownReason) => unknown(source, reason, record);
  let start = 0;
  for (let i = index - 1; i >= 0; i--) {
    const line = lines[i]!;
    if (line.kind === "control" && line.type === "token_usage_record") {
      start = i;
      break;
    }
  }
  const before = lines.slice(start, index);
  if (before.some((line) => itemClass(line) === "unknown")) return u("unrecognized_format");
  const inflight = before.some((line) => ["output", "possible"].includes(itemClass(line)));
  let boundary = !inflight;
  for (let i = index + 1; i < lines.length; i++) {
    const line = lines[i]!;
    const kind = itemClass(line);
    if (kind === "compacted") return u("compacted");
    if (kind === "rolled_back") return u("rolled_back");
    if (kind === "unknown") return u("unrecognized_format");
    if (line.kind === "control" && line.type === "token_usage_record") {
      boundary = true;
      continue;
    }
    if (boundary && kind === "output" && line.kind === "item") {
      if (line.id === null || line.at < l.at) return u("unrecognized_format");
      return {
        fact: "shown",
        source,
        cli: meta.cli,
        record,
        consumed: { id: line.id, call: line.call, at: line.at },
      };
    }
  }
  return u("no_model_response");
};
