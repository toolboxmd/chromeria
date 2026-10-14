// @effect-diagnostics nodeBuiltinImport:off - the pure path validator takes the same path operations the service provides.
import * as NodePath from "node:path";

import { assert, describe, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";

import * as Shown from "./Shown.ts";

// Public synthetic records use native field shapes, with literal verdicts independent of the decider.
const TEXT = 'Follow this public rule\nLine two with a "quote"';
const INNER =
  'PostToolUse:Bash hook additional context: Follow this public rule\nLine two with a "quote"';
const at = (n: number) => DateTime.formatIso(DateTime.makeUnsafe(n * 1000));
const delivery: Shown.Served = {
  delivery: "delivery-one",
  session: "public-session",
  event: "PostToolUse",
  text: TEXT,
  call: "toolu-one",
  turn: null,
  createdAt: 1000,
  settledAt: 1000.01,
};
const attachment = (extra: Record<string, unknown> = {}) => ({
  type: "attachment",
  sessionId: "public-session",
  uuid: "attachment-one",
  timestamp: at(1000),
  isSidechain: false,
  attachment: {
    type: "hook_additional_context",
    hookEvent: "PostToolUse",
    hookName: "PostToolUse:Bash",
    toolUseID: "toolu-one",
    content: [TEXT],
  },
  rendered: [{ content: `<system-reminder>\n${INNER}\n</system-reminder>` }],
  ...extra,
});
const blob = (hash: string, message: unknown, extra: Record<string, unknown> = {}) => ({
  type: "api-request-blob",
  sessionId: "public-session",
  hash,
  message,
  ...extra,
});
const request = (
  id = "request-one",
  time = 1001,
  tail = ["prior-response-run", "tool-result-run", "rule"],
  extra: Record<string, unknown> = {},
) => ({
  type: "api-request",
  sessionId: "public-session",
  id,
  timestamp: at(time),
  version: "2.1.299",
  keep: 2,
  base: "prior-request",
  tail,
  params: { betas: ["normal-beta"] },
  ...extra,
});
const response = (
  requestRef: string | null = "request-one",
  time = 1002,
  extra: Record<string, unknown> = {},
) => ({
  type: "assistant",
  sessionId: "public-session",
  uuid: `response-${requestRef}`,
  timestamp: at(time),
  isSidechain: false,
  requestRef,
  message: {
    id: `message-${requestRef}`,
    model: "claude-opus-5-5",
    content: [{ type: "tool_use", id: "toolu-next" }],
  },
  ...extra,
});
const prefix = () => [
  blob("start", { role: "user", content: "A public prompt" }),
  request("prior-request", 990, ["start", "prior-system"], { keep: 0, base: undefined }),
  response("prior-request", 995),
  attachment(),
];
const claude = (
  message: unknown = { role: "system", content: [{ type: "text", text: INNER }] },
  req = request(),
  res = response(),
) => [...prefix(), blob("rule", message), req, res];
const decideClaude = (
  rows: ReadonlyArray<unknown>,
  d = delivery,
  rivals: ReadonlyArray<Shown.Served> = [],
) =>
  Shown.claudeEvidence(
    rows.map((r) => Shown.decodeClaudeLine(typeof r === "string" ? r : JSON.stringify(r))),
    d,
    rivals,
  );
const reason = (e: Shown.Evidence) => (e.fact === "unknown" ? e.reason : e.fact);

describe("Claude native request receipt", () => {
  it("E proves the suffix and R names the successful retry, while L and W remain unknown", () => {
    const e = decideClaude(claude());
    assert.equal(e.fact, "shown");
    if (e.fact !== "shown" || e.source !== "claude_request_replay") return;
    assert.deepEqual(e.record, { id: "attachment-one", slot: 0, at: 1000, turn: null });
    assert.deepEqual(e.consumed, {
      id: "message-request-one",
      call: "toolu-next",
      at: 1002,
      request: "request-one",
    });
    assert.equal(
      reason(decideClaude(claude({ role: "system", content: "ordinary text" }))),
      "absent_from_request",
    );
    assert.equal(
      reason(decideClaude(claude({ role: "system", content: "rewritten public rule" }))),
      "absent_from_request",
    );
    const retry = decideClaude([
      ...prefix(),
      blob("rule", { role: "system", content: INNER }),
      request(),
      request("request-retry", 1001.5, [], { base: "request-one", keep: 5 }),
      response("request-retry"),
    ]);
    assert.equal(retry.fact, "shown");
    if (retry.fact === "shown" && retry.source === "claude_request_replay")
      assert.equal(retry.consumed.request, "request-retry");
  });

  it.each([
    ["betas positive", { betas: [] }, "shown"],
    ["anthropic_beta positive", { anthropic_beta: ["normal-beta"] }, "shown"],
    ["missing beta list", {}, "request_unverifiable"],
    ["create B1", { betas: ["message-threads-2026-08-12"] }, "request_unverifiable"],
    ["continue B1", { anthropic_beta: ["message-threads-2026-08-12"] }, "request_unverifiable"],
    [
      "latched threads",
      { betas: ["normal-beta", "message-threads-2026-08-12"] },
      "request_unverifiable",
    ],
    [
      "threads in second list",
      { betas: [], anthropic_beta: ["message-threads-2026-08-12"] },
      "request_unverifiable",
    ],
    [
      "threads in first list",
      { betas: ["message-threads-2026-08-12"], anthropic_beta: [] },
      "request_unverifiable",
    ],
    ["malformed second list", { betas: [], anthropic_beta: null }, "unrecognized_format"],
    ["malformed first list", { betas: [1], anthropic_beta: [] }, "unrecognized_format"],
    ["wrong list shape", { betas: "normal-beta" }, "unrecognized_format"],
    ["opaque context management", { betas: [], context_management: null }, "request_unverifiable"],
    [
      "non-thinking edit",
      { betas: [], context_management: { edits: [{ type: "clear_tool_uses_20250919" }] } },
      "request_unverifiable",
    ],
    [
      "thinking-only edit",
      {
        betas: [],
        context_management: { edits: [{ type: "clear_thinking_20251015", keep: "all" }] },
      },
      "shown",
    ],
  ])("guards %s", (_name, params, expected) => {
    assert.equal(
      reason(decideClaude(claude(undefined, request("request-one", 1001, undefined, { params })))),
      expected,
    );
  });

  it.each([
    ["withheld betas", { withheld: ["betas"] }, "request_unverifiable"],
    ["withheld anthropic_beta", { withheld: ["anthropic_beta"] }, "request_unverifiable"],
    ["withheld context management", { withheld: ["context_management"] }, "request_unverifiable"],
    ["bad withheld shape", { withheld: {} }, "unrecognized_format"],
    ["cut", { cut: true }, "request_unverifiable"],
    ["before attachment", { timestamp: at(999) }, "no_model_response"],
    ["tie with attachment", { timestamp: at(1000) }, "no_model_response"],
    ["request after response", { timestamp: at(1003) }, "unrecognized_format"],
    ["subagent request", { agentId: "agent-one" }, "final_input_unrecorded"],
    ["invalid timestamp", { timestamp: "not a date" }, "unrecognized_format"],
    ["bad keep", { keep: -1 }, "unrecognized_format"],
    ["missing prior request", { base: "missing" }, "shown"],
    ["short request", { keep: 0, tail: ["start", "rule"] }, "request_unverifiable"],
    ["input at anchor slot", { tail: ["rule", "tool-result-run", "rule"] }, "request_unverifiable"],
  ])("refuses or preserves %s", (_name, change, expected) => {
    assert.equal(
      reason(decideClaude(claude(undefined, request("request-one", 1001, undefined, change)))),
      expected,
    );
  });

  it.each([
    ["plain system text", { role: "system", content: INNER }, "shown"],
    [
      "wrapped merged text",
      { role: "user", content: `<system-reminder>\n${INNER}\n</system-reminder>\n` },
      "shown",
    ],
    [
      "tool result string",
      { role: "user", content: [{ type: "tool_result", content: `output\n${INNER}` }] },
      "shown",
    ],
    [
      "tool result blocks",
      {
        role: "user",
        content: [{ type: "tool_result", content: [{ type: "text", text: INNER }] }],
      },
      "shown",
    ],
    [
      "metadata on text",
      {
        role: "system",
        content: [{ type: "text", text: "ordinary", citations: INNER, cache_control: INNER }],
      },
      "absent_from_request",
    ],
    [
      "nontext block",
      { role: "system", content: [{ type: "image", source: INNER }] },
      "absent_from_request",
    ],
    [
      "unknown block",
      { role: "user", content: [{ type: "future_text", text: INNER }] },
      "request_unverifiable",
    ],
    [
      "wrong text type",
      { role: "user", content: [{ type: "text", text: 1 }] },
      "request_unverifiable",
    ],
    [
      "unknown nested block",
      {
        role: "user",
        content: [{ type: "tool_result", content: [{ type: "future", text: INNER }] }],
      },
      "request_unverifiable",
    ],
    [
      "wrong tool result",
      { role: "user", content: [{ type: "tool_result", content: { text: INNER } }] },
      "request_unverifiable",
    ],
    [
      "unknown message key",
      { role: "system", content: INNER, metadata: {} },
      "request_unverifiable",
    ],
    ["unknown role", { role: "future", content: INNER }, "request_unverifiable"],
    [
      "unknown role with clear_at",
      { role: "future", content: INNER, clear_at: "next_user_message" },
      "request_unverifiable",
    ],
    [
      "turn-scoped clear_at",
      { role: "system", content: INNER, clear_at: "next_user_message" },
      "absent_from_request",
    ],
    [
      "output_config only",
      { role: "user", content: "ordinary", output_config: INNER },
      "absent_from_request",
    ],
  ])("walks only recognized input: %s", (_name, message, expected) => {
    assert.equal(reason(decideClaude(claude(message))), expected);
  });

  it("excludes repeated, rewritten and restored older prefix copies, but allows content after the bound", () => {
    const p = prefix();
    const older = blob("start", { role: "user", content: INNER });
    const beforeOnly = [
      older,
      ...p.slice(1),
      blob("rule", { role: "system", content: "left out" }),
      request(),
      response(),
    ];
    assert.equal(reason(decideClaude(beforeOnly)), "absent_from_request");
    assert.equal(reason(decideClaude(claude({ role: "user", content: INNER }))), "shown");
    // Missing base positions never count. Tail input can still prove receipt.
    assert.equal(
      reason(
        decideClaude(
          claude(undefined, request("request-one", 1001, undefined, { base: "missing" })),
        ),
      ),
      "shown",
    );
    assert.equal(
      reason(
        decideClaude(
          claude(
            { role: "user", content: "no rule" },
            request("request-one", 1001, undefined, { base: "missing" }),
          ),
        ),
      ),
      "absent_from_request",
    );
  });

  it("needs an actual request link and uses a later consuming response for revised unknowns", () => {
    assert.equal(reason(decideClaude([...prefix(), response()])), "final_input_unrecorded");
    assert.equal(
      reason(decideClaude(claude(undefined, request(), response(null)))),
      "final_input_unrecorded",
    );
    assert.equal(reason(decideClaude(claude().slice(0, -1))), "no_model_response");
    assert.equal(
      reason(decideClaude(claude(undefined, request(), response("missing")))),
      "final_input_unrecorded",
    );
    const omitted = claude({ role: "system", content: "left out" });
    assert.equal(reason(decideClaude(omitted)), "absent_from_request");
    const later = [
      ...omitted,
      blob("later-rule", { role: "user", content: INNER }),
      request("later", 1003, ["response-run", "later-rule"], { keep: 5, base: "request-one" }),
      response("later", 1004),
    ];
    const shown = decideClaude(later);
    assert.equal(shown.fact, "shown");
    if (shown.fact === "shown") assert.equal(shown.consumed.at, 1004);
    // An in-flight predecessor's later block cannot certify a newer attachment.
    assert.equal(
      reason(decideClaude([...prefix(), response("prior-request", 1002)])),
      "no_model_response",
    );
    assert.equal(
      reason(
        decideClaude(
          claude(undefined, request(), response("request-one", 1002, { isApiErrorMessage: true })),
        ),
      ),
      "no_model_response",
    );
    assert.equal(
      reason(
        decideClaude(
          claude(
            undefined,
            request(),
            response("request-one", 1002, {
              message: { id: "synthetic", model: "<synthetic>", content: [] },
            }),
          ),
        ),
      ),
      "no_model_response",
    );
  });

  it("first requests count all positions, and session-start can bind the second slot", () => {
    const text = "public session rule";
    const a = attachment({
      attachment: {
        type: "hook_additional_context",
        hookEvent: "SessionStart",
        hookName: "SessionStart",
        content: ["other rule", text],
      },
      rendered: [
        {
          content:
            "<system-reminder>\nSessionStart hook additional context: other rule\npublic session rule\n</system-reminder>",
        },
      ],
    });
    const rows = [
      a,
      blob("rule", {
        role: "system",
        content: "SessionStart hook additional context: other rule\npublic session rule",
      }),
      request("request-one", 1001, ["rule"], { keep: 0, base: undefined }),
      response(),
    ];
    const verdict = decideClaude(rows, { ...delivery, event: "SessionStart", call: null, text });
    assert.equal(verdict.fact, "shown");
    if (verdict.fact === "shown") assert.equal(verdict.record.slot, 1);
    assert.equal(
      reason(
        decideClaude([
          attachment(),
          blob("rule", { role: "user", content: INNER }),
          request("request-one", 1001, ["rule"], { keep: 0, base: undefined }),
          response(),
        ]),
      ),
      "shown",
    );
  });

  it("rejects cycles, conflicting IDs and undecodable complete lines", () => {
    assert.equal(
      reason(
        decideClaude([
          ...claude(),
          request("other", 1001, [], { keep: 1, base: "request-one" }),
          request("request-one", 1001, [], { keep: 1, base: "other" }),
        ]),
      ),
      "unrecognized_format",
    );
    assert.equal(
      reason(decideClaude([...claude(), request("request-one", 1001.1)])),
      "unrecognized_format",
    );
    assert.equal(reason(decideClaude([...claude(), "{broken"])), "unrecognized_format");
    const withoutPrior = claude().filter(
      (r) => r.type !== "api-request" || !("id" in r) || r.id !== "prior-request",
    );
    assert.equal(reason(decideClaude(withoutPrior)), "request_unverifiable");
  });

  it("binds the exact event, call, session, slot and all rivals", () => {
    assert.equal(
      reason(decideClaude(claude(), delivery, [{ ...delivery, delivery: "released-rival" }])),
      "ambiguous",
    );
    assert.equal(
      reason(decideClaude([...claude(), attachment({ uuid: "second-attachment" })])),
      "ambiguous",
    );
    for (const d of [
      { ...delivery, call: "another-call" },
      { ...delivery, event: "PostToolUse:Bash" },
      { ...delivery, createdAt: 900, settledAt: 900 },
      { ...delivery, session: "different" },
    ])
      assert.notEqual(decideClaude(claude(), d).fact, "shown");
    assert.equal(
      reason(
        decideClaude(
          claude().map((r) => (r.type === "attachment" ? attachment({ isSidechain: true }) : r)),
        ),
      ),
      "not_found",
    );
    assert.equal(
      reason(
        decideClaude(
          claude().map((r) =>
            r.type === "attachment" ? attachment({ rendered: [{ content: TEXT }] }) : r,
          ),
        ),
      ),
      "unrecognized_format",
    );
  });
});

const codexDelivery = { ...delivery, call: "exec-one", turn: "turn-one" };
const item = (payload: unknown, time = 1001, extra: Record<string, unknown> = {}) => ({
  type: "response_item",
  timestamp: at(time),
  payload,
  ...extra,
});
const hook = () =>
  item(
    {
      type: "message",
      role: "developer",
      id: "hook-message",
      internal_chat_message_metadata_passthrough: {
        turn_id: "turn-one",
        content_item_kinds: ["plain", "hooks.additional_context"],
      },
      content: [
        { type: "input_text", text: "other input" },
        { type: "input_text", text: TEXT },
      ],
    },
    1000,
  );
const output = () =>
  item({ type: "message", role: "assistant", id: "consuming-output", content: [] });
const codexPrefix = () => [
  {
    type: "session_meta",
    timestamp: at(980),
    payload: { id: "public-session", cli_version: "0.162.0" },
  },
  { type: "turn_context", timestamp: at(989), payload: { turn_id: "turn-one" } },
  item({ type: "message", role: "user", id: "user-message", content: [] }, 990),
  { type: "token_usage_record", timestamp: at(991) },
  {
    type: "event_msg",
    timestamp: at(995),
    payload: { type: "item_completed", turn_id: "turn-one", item: { id: "exec-one" } },
  },
];
const decideCodex = (
  rows: ReadonlyArray<unknown>,
  d: Shown.Served = codexDelivery,
  rivals: ReadonlyArray<Shown.Served> = [],
) =>
  Shown.codexEvidence(
    rows.map((r) => Shown.decodeCodexLine(typeof r === "string" ? r : JSON.stringify(r))),
    d,
    rivals,
  );

describe("Codex typed hook receipt", () => {
  it("proves unified exec and prompt receipt with native slot, turn and consumption identity", () => {
    const rows = [...codexPrefix(), hook(), output()];
    const shown = decideCodex(rows);
    assert.equal(shown.fact, "shown");
    if (shown.fact !== "shown") return;
    assert.deepEqual(shown.record, { id: "hook-message", slot: 1, at: 1000, turn: "turn-one" });
    assert.deepEqual(shown.consumed, { id: "consuming-output", call: null, at: 1001 });
    assert.equal(
      reason(decideCodex(rows, { ...codexDelivery, event: "UserPromptSubmit", call: null })),
      "shown",
    );
    const called = decideCodex([
      ...codexPrefix(),
      hook(),
      item({ type: "function_call", id: "native-call-item", call_id: "call-next" }),
    ]);
    assert.equal(called.fact, "shown");
    if (called.fact === "shown") assert.equal(called.consumed.call, "call-next");
  });

  it.each([
    "codex:code-mode-delivery:v1:complete",
    "codex:code-mode-delivery:v1:incomplete:public partial send",
  ])("retained send %s cannot prove consumption or a sampling response in flight", (marker) => {
    // Native RetainedContextEvent::DeliveredAssistantMessage wire form, after the
    // prior sampling's usage boundary while its Code Mode cell still runs tools.
    const retained = (time = 1001) =>
      item(
        {
          type: "message",
          role: "assistant",
          id: "retained-send-call",
          internal_chat_message_metadata_passthrough: { turn_id: "turn-one" },
          content: [{ type: "output_text", text: "The synthetic job is done." }],
        },
        time,
        { metadata: { delivered_assistant_message: marker, user_input_order: 7 } },
      );
    const prefix = [...codexPrefix(), hook()];
    assert.equal(reason(decideCodex([...prefix, retained()])), "no_model_response");
    const consuming = item({ type: "reasoning", id: "actual-model-output" }, 1002);
    for (const rows of [
      [...prefix, retained(), consuming],
      [...codexPrefix(), retained(999), hook(), consuming],
    ]) {
      const shown = decideCodex(rows);
      assert.equal(shown.fact, "shown");
      if (shown.fact === "shown") assert.equal(shown.consumed.id, "actual-model-output");
    }
  });

  it.each([null, true, 1, "", "codex:code-mode-delivery:v2:complete"])(
    "malformed or unknown retained marker %s fails closed",
    (marker) => {
      const retained = item(
        { type: "message", role: "assistant", id: "not-model-output", content: [] },
        1001,
        { metadata: { delivered_assistant_message: marker } },
      );
      assert.equal(
        reason(decideCodex([...codexPrefix(), hook(), retained, output()])),
        "unrecognized_format",
      );
    },
  );

  it("open tool calls bind before their output and cannot use old in-flight output", () => {
    const prefix = codexPrefix().slice(0, -1);
    const call = item({ type: "function_call", id: "call-item", call_id: "exec-one" }, 995);
    const waiting = [...prefix, call, hook(), output()];
    assert.equal(reason(decideCodex(waiting)), "no_model_response");
    const consuming = [
      ...waiting,
      { type: "token_usage_record", timestamp: at(1002) },
      item({ type: "reasoning", id: "new-response" }, 1003),
    ];
    const shown = decideCodex(consuming);
    assert.equal(shown.fact, "shown");
    if (shown.fact === "shown") assert.equal(shown.consumed.id, "new-response");
    assert.equal(
      reason(
        decideCodex([
          ...prefix,
          call,
          item({ type: "function_call_output", call_id: "exec-one" }, 999),
          hook(),
          output(),
        ]),
      ),
      "not_found",
    );
    assert.equal(
      reason(
        decideCodex([
          ...prefix,
          item({ type: "function_call", call_id: "different", id: "other" }, 995),
          hook(),
          output(),
        ]),
      ),
      "not_found",
    );
  });

  it.each([
    [
      "compacted nested history",
      { type: "compacted", timestamp: at(1000.5), replacement_history: [output()] },
      "compacted",
    ],
    [
      "context compacted",
      { type: "event_msg", timestamp: at(1000.5), payload: { type: "context_compacted" } },
      "compacted",
    ],
    ["compaction item", item({ type: "compaction", id: "compact" }), "compacted"],
    ["compaction alias", item({ type: "compaction_summary" }), "compacted"],
    ["context compaction", item({ type: "context_compaction" }), "compacted"],
    ["compaction trigger", item({ type: "compaction_trigger" }), "compacted"],
    [
      "compaction provenance",
      item({ type: "reasoning", id: "compact-reasoning" }, 1000.5, {
        metadata: { compaction_output: true },
      }),
      "compacted",
    ],
    [
      "rollback",
      { type: "event_msg", timestamp: at(1000.5), payload: { type: "thread_rolled_back" } },
      "rolled_back",
    ],
    ["unknown response", item({ type: "future_output" }), "unrecognized_format"],
    ["unknown role", item({ type: "message", role: "tool", id: "future" }), "unrecognized_format"],
    ["future top-level", { type: "future_record", timestamp: at(1000.5) }, "unrecognized_format"],
    [
      "bad provenance",
      item({ type: "reasoning", id: "bad" }, 1000.5, { metadata: { compaction_output: "true" } }),
      "unrecognized_format",
    ],
    ["bad complete line", "{broken", "unrecognized_format"],
  ])("fails closed on %s", (_name, line, expected) => {
    assert.equal(reason(decideCodex([...codexPrefix(), hook(), line, output()])), expected);
  });

  it("requires typed aligned input, native turn, identity, event and window", () => {
    const bad = [
      {
        type: "message",
        role: "developer",
        id: "hook",
        internal_chat_message_metadata_passthrough: { turn_id: "turn-one", content_item_kinds: [] },
        content: [{ type: "input_text", text: TEXT }],
      },
      {
        type: "message",
        role: "developer",
        id: "hook",
        internal_chat_message_metadata_passthrough: {
          content_item_kinds: ["hooks.additional_context"],
        },
        content: [{ type: "input_text", text: TEXT }],
      },
      {
        type: "message",
        role: "developer",
        id: "hook",
        internal_chat_message_metadata_passthrough: {
          turn_id: "turn-one",
          content_item_kinds: ["hooks.additional_context"],
        },
        content: [{ type: "future_input", text: TEXT }],
      },
    ];
    for (const payload of bad)
      assert.equal(
        reason(decideCodex([...codexPrefix(), item(payload, 1000), output()])),
        "unrecognized_format",
      );
    for (const d of [
      { ...codexDelivery, turn: "other-turn" },
      { ...codexDelivery, call: "other-call" },
      { ...codexDelivery, createdAt: 900, settledAt: 900 },
      { ...codexDelivery, session: "other-session" },
    ])
      assert.notEqual(decideCodex([...codexPrefix(), hook(), output()], d).fact, "shown");
    assert.equal(
      reason(
        decideCodex([...codexPrefix(), hook(), output()], codexDelivery, [
          { ...codexDelivery, delivery: "reserved-rival" },
        ]),
      ),
      "ambiguous",
    );
    assert.equal(reason(decideCodex([...codexPrefix(), hook(), hook(), output()])), "ambiguous");
    assert.equal(
      reason(
        decideCodex([
          ...codexPrefix(),
          hook(),
          item({ type: "agent_message", id: "not-completed" }),
        ]),
      ),
      "no_model_response",
    );
  });

  it("unknown deciding records before input and compaction usage cannot certify", () => {
    assert.equal(
      reason(
        decideCodex([
          ...codexPrefix(),
          item({ type: "message", role: "future" }, 999),
          hook(),
          output(),
        ]),
      ),
      "unrecognized_format",
    );
    assert.equal(
      reason(
        decideCodex([
          ...codexPrefix(),
          hook(),
          item({ type: "reasoning", id: "compact" }, 1001, {
            metadata: { compaction_output: true },
          }),
          { type: "token_usage_record", timestamp: at(1002) },
          output(),
        ]),
      ),
      "compacted",
    );
  });
});

describe("shown scheduling and native paths", () => {
  it("keeps waiting unknowns revisable without a terminal horizon", () => {
    assert.equal(Shown.backoff(599), 30);
    assert.equal(Shown.backoff(600), 300);
    assert.equal(Shown.backoff(86400), 3600);
    assert.equal(
      Shown.nextCheck(Shown.unknown("codex_rollout", "no_model_response"), 864000, 100),
      3700,
    );
    assert.isNull(Shown.nextCheck(Shown.unknown("codex_rollout", "compacted"), 0, 100));
    assert.isNull(Shown.nextCheck(decideCodex([...codexPrefix(), hook(), output()]), 0, 100));
  });
  it("validates source-specific absolute transcript identities", () => {
    assert.isTrue(
      Shown.validPath(
        "claude_request_replay",
        "/tmp/public-session.jsonl",
        "public-session",
        NodePath,
      ),
    );
    assert.isTrue(
      Shown.validPath(
        "codex_rollout",
        "/tmp/rollout-date-public-session.jsonl",
        "public-session",
        NodePath,
      ),
    );
    for (const path of [
      null,
      "public-session.jsonl",
      "/tmp/other.jsonl",
      `/${"x".repeat(4096)}/public-session.jsonl`,
    ])
      assert.isFalse(Shown.validPath("claude_request_replay", path, "public-session", NodePath));
  });
});
