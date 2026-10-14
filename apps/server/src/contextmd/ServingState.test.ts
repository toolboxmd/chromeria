// @effect-diagnostics nodeBuiltinImport:off - the state core runs on node:sqlite; one test holds a write lock from another process.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";

import { assert, describe, it } from "@effect/vitest";

import type { Candidate, MomentKind, Profile, Selection } from "./Selection.ts";
import type { CueReply } from "./Cue.ts";
import {
  DENY_BUDGET_BYTES,
  SERVE_BUDGET_BYTES,
  ServingState,
  isHoldout,
  type CueMoment,
  type Reply,
  type RunJoin,
} from "./ServingState.ts";

const PROFILE: Profile = {
  harness: "claude",
  delivering: [
    "session_start",
    "user_message",
    "pre_tool",
    "tool_result",
    "tool_failure",
    "before_done",
  ],
  carry_to: null,
  limit_bytes: 2000,
  settle_within_ms: 2000,
  session_start_limit_bytes: 9500,
  new_epoch_sources: ["compact", "clear"],
  templates: {
    context: { hookSpecificOutput: { hookEventName: "{{event}}", additionalContext: "{{text}}" } },
    deny: {
      hookSpecificOutput: {
        hookEventName: "{{event}}",
        permissionDecision: "deny",
        permissionDecisionReason: "{{text}}",
      },
    },
  },
  heads: { deny: "deny-head", context: "context-head" },
};

const GROK: Profile = {
  ...PROFILE,
  harness: "grok",
  delivering: ["pre_tool", "before_done"],
  carry_to: "pre_tool",
};

const SETTLE_MS = 2000;

const entry = (id: string, text: string, extra: Partial<Candidate> = {}): Candidate => ({
  matcher: "exact_key",
  entry: id,
  version: "v1",
  key: `command:${id}`,
  text,
  must_know: false,
  rule: null,
  target: null,
  ...extra,
});

const pointer = (id: string, target: string, text = `rule ${id}`) =>
  entry(id, text, {
    must_know: true,
    rule: "pointer",
    target,
    deny_text: `${text}\nRead ${target}`,
  });

const inline = (id: string, text = `rule ${id}`) =>
  entry(id, text, { must_know: true, rule: "inline", deny_text: text });

interface Fire {
  readonly kind: MomentKind;
  readonly candidates?: Candidate[];
  readonly session?: string;
  readonly run?: string;
  readonly join?: RunJoin;
  readonly reads?: Selection["moment"]["reads"];
  readonly failed?: boolean | null;
  readonly source?: string | null;
  readonly profile?: Profile;
  readonly delivery?: string;
  readonly settle?: boolean;
  readonly readChecks?: ReadonlyArray<{ readonly toolUseId: string; readonly ok: boolean | null }>;
  readonly toolUseId?: string | null;
  readonly cue?: CueMoment;
  readonly elapsedMs?: number;
  readonly transcriptPath?: string | null;
  readonly nativeTurn?: string | null;
}

const makeHarness = (file = ":memory:") => {
  const db = new NodeSqlite.DatabaseSync(file);
  const state = new ServingState(db);
  let now = 1_000;
  let n = 0;
  const fire = (f: Fire): Reply & { delivery: string } => {
    n += 1;
    const delivery = f.delivery ?? `d${n}`;
    const profile = f.profile ?? PROFILE;
    const reply = state.decide({
      harness: profile.harness,
      delivery,
      selection: {
        moment: {
          kind: f.kind,
          event: "PreToolUse",
          session: f.session ?? "s1",
          tool_use_id: f.toolUseId !== undefined ? f.toolUseId : `t${n}`,
          tool: "Bash",
          source: f.source ?? null,
          transcript_path: f.transcriptPath ?? null,
          native_turn: f.nativeTurn ?? null,
          failed:
            f.failed !== undefined
              ? f.failed
              : f.kind === "tool_failure"
                ? true
                : f.kind === "tool_result"
                  ? false
                  : null,
          reads: f.reads ?? [],
        },
        candidates: f.candidates ?? [],
        notes: [],
        owners: { "/store": "abc123" },
        profile,
      },
      join: f.join ?? { type: "run", runId: f.run ?? "run-1", threadId: "thread-1" },
      ...(f.readChecks === undefined ? {} : { readChecks: f.readChecks }),
      ...(f.cue === undefined ? {} : { cue: f.cue }),
      now,
      startedAt: now - (f.elapsedMs ?? 0) / 1000,
    });
    if (reply.status === 200 && f.settle !== false) state.settle(delivery, now);
    return { ...reply, delivery };
  };
  const text = (r: Reply) => {
    if (r.status !== 200) return null;
    const body = JSON.parse(r.body).hookSpecificOutput;
    return (body.permissionDecisionReason ?? body.additionalContext) as string;
  };
  const decision = (r: Reply) =>
    r.status === 200
      ? (JSON.parse(r.body).hookSpecificOutput.permissionDecision ?? "context")
      : "none";
  const outcomes = (entryId: string) =>
    (
      db
        .prepare("SELECT outcome FROM candidates WHERE entry = ? ORDER BY id")
        .all(entryId) as Array<{ outcome: string }>
    ).map((r) => r.outcome);
  const advance = (ms: number) => {
    now += ms / 1000;
  };
  return { db, state, fire, text, decision, outcomes, advance, now: () => now };
};

describe("ServingState entry reads", () => {
  const self = "/repo/library/long.md";
  const long = (version: string) =>
    entry("long", `Read ${self} in full.`, {
      version,
      must_know: true,
      rule: "entry",
      target: self,
      deny_text: `Read ${self} in full, then run the same call again.`,
    });

  it("lifts after following a retargeted Library's newly emitted entry path", () => {
    const h = makeHarness();
    const movedPath = "/repo/other-library/long.md";
    const moved = {
      ...long("v1"),
      target: movedPath,
      text: `Read ${movedPath} in full.`,
      deny_text: `Read ${movedPath} in full, then run the same call again.`,
    };
    const firstDenied = h.now();
    h.fire({ kind: "pre_tool", candidates: [long("v1")] });
    h.advance(10);
    const again = h.fire({ kind: "pre_tool", candidates: [moved] });
    assert.include(h.text(again)!, movedPath);
    h.advance(10);
    h.fire({ kind: "tool_result", reads: [{ path: movedPath, kind: "shell_partial" }] });
    assert.notInclude(h.outcomes("long"), "lifted");
    h.advance(10);
    h.fire({ kind: "tool_result", reads: [{ path: movedPath, kind: "shell_full" }] });
    assert.strictEqual(h.fire({ kind: "pre_tool", candidates: [moved] }).status, 204);
    const row = h.db.prepare("SELECT first_denied_at, denies, lift_outcome FROM deny_state").get();
    assert.deepStrictEqual(
      { ...row },
      { first_denied_at: firstDenied, denies: 2, lift_outcome: "read" },
    );
    h.advance(10);
    const relinked = { ...moved, target: "/repo/third-library/long.md" };
    assert.strictEqual(h.fire({ kind: "pre_tool", candidates: [relinked] }).status, 204);
    assert.deepStrictEqual(
      { ...h.db.prepare("SELECT target, lifted, denies FROM deny_state").get() },
      { target: movedPath, lifted: 1, denies: 2 },
    );
  });

  it("requires a full read after the current entry version's first deny", () => {
    const h = makeHarness();
    h.fire({ kind: "tool_result", reads: [{ path: self, kind: "shell_full" }] });
    h.advance(10);
    assert.strictEqual(h.decision(h.fire({ kind: "pre_tool", candidates: [long("v1")] })), "deny");
    h.advance(10);
    h.fire({ kind: "tool_result", reads: [{ path: self, kind: "shell_full" }] });
    assert.strictEqual(h.fire({ kind: "pre_tool", candidates: [long("v1")] }).status, 204);
    h.advance(10);
    assert.strictEqual(h.decision(h.fire({ kind: "pre_tool", candidates: [long("v2")] })), "deny");
    h.advance(10);
    h.fire({ kind: "tool_result", reads: [{ path: self, kind: "shell_partial" }] });
    assert.strictEqual(h.decision(h.fire({ kind: "pre_tool", candidates: [long("v2")] })), "deny");
    h.advance(10);
    h.fire({ kind: "tool_result", reads: [{ path: self, kind: "read_tool" }] });
    assert.strictEqual(h.fire({ kind: "pre_tool", candidates: [long("v2")] }).status, 204);
    assert.deepStrictEqual(
      h.db
        .prepare("SELECT version, reason FROM candidates WHERE outcome = 'lifted'")
        .all()
        .map((r) => ({ ...r })),
      [
        { version: "v1", reason: "read" },
        { version: "v2", reason: "read" },
      ],
    );
  });

  it("uses a pending Codex read's original time when its exit status arrives later", () => {
    const h = makeHarness();
    h.fire({ kind: "pre_tool", candidates: [long("v1")] });
    h.advance(10);
    h.fire({
      kind: "tool_result",
      failed: null,
      toolUseId: "old-read",
      reads: [{ path: self, kind: "shell_full" }],
    });
    h.advance(10);
    assert.strictEqual(h.decision(h.fire({ kind: "pre_tool", candidates: [long("v2")] })), "deny");
    h.advance(10);
    assert.strictEqual(
      h.decision(
        h.fire({
          kind: "pre_tool",
          candidates: [long("v2")],
          readChecks: [{ toolUseId: "old-read", ok: true }],
        }),
      ),
      "deny",
    );
    h.advance(10);
    h.fire({
      kind: "tool_result",
      failed: null,
      toolUseId: "new-read",
      reads: [{ path: self, kind: "shell_full" }],
    });
    assert.strictEqual(
      h.fire({
        kind: "pre_tool",
        candidates: [long("v2")],
        readChecks: [{ toolUseId: "new-read", ok: true }],
      }).status,
      204,
    );
    assert.notInclude(h.outcomes("long"), "not_read_capped");
  });

  it("binds an armed old inline row when the new build emits an entry pointer for the same version", () => {
    const h = makeHarness(),
      profile = { ...GROK, limit_bytes: 256, deny_limit_bytes: 256 };
    const firstDenied = h.now();
    h.fire({ kind: "pre_tool", profile, candidates: [inline("long", "K".repeat(413))] });
    assert.deepStrictEqual(
      { ...h.db.prepare("SELECT rule_kind, target, denies, lifted FROM deny_state").get() },
      { rule_kind: "inline", target: null, denies: 1, lifted: 0 },
    );
    h.advance(10);
    h.fire({ kind: "pre_tool", profile, candidates: [long("v1")] });
    assert.deepStrictEqual(
      {
        ...h.db.prepare("SELECT rule_kind, target, first_denied_at, denies FROM deny_state").get(),
      },
      { rule_kind: "entry", target: self, first_denied_at: firstDenied, denies: 2 },
    );
    h.advance(10);
    h.fire({ kind: "tool_result", profile, reads: [{ path: self, kind: "shell_full" }] });
    assert.strictEqual(h.fire({ kind: "pre_tool", profile, candidates: [long("v1")] }).status, 204);
    assert.include(h.outcomes("long"), "lifted");
    assert.strictEqual(
      h.db.prepare("SELECT lift_outcome FROM deny_state").get()!.lift_outcome,
      "read",
    );
  });

  it("keeps the record as the lift target when a deny also names the entry", () => {
    const h = makeHarness(),
      target = "/repo/procedure.md";
    const rule = entry("record", "Knowledge.", {
      must_know: true,
      rule: "pointer",
      target,
      deny_text: `Read ${target} and ${self} in full, then run the same call again.`,
    });
    assert.strictEqual(h.decision(h.fire({ kind: "pre_tool", candidates: [rule] })), "deny");
    h.fire({ kind: "tool_result", reads: [{ path: self, kind: "shell_full" }] });
    assert.strictEqual(h.decision(h.fire({ kind: "pre_tool", candidates: [rule] })), "deny");
    h.fire({ kind: "tool_result", reads: [{ path: target, kind: "shell_full" }] });
    assert.strictEqual(h.fire({ kind: "pre_tool", candidates: [rule] }).status, 204);
  });

  it.each(["inline", "pointer"] as const)(
    "does not rebind an armed entry row for an incoming %s deny",
    (kind) => {
      const h = makeHarness(),
        profile = { ...GROK, limit_bytes: 256, deny_limit_bytes: 256 };
      h.fire({ kind: "pre_tool", profile, candidates: [long("v1")] });
      const original = h.db
        .prepare("SELECT rule_kind, target, first_denied_at FROM deny_state")
        .get();
      h.advance(10);
      const other =
        kind === "inline" ? inline("long", "K".repeat(413)) : pointer("long", "/other/record.md");
      assert.strictEqual(
        h.decision(h.fire({ kind: "pre_tool", profile, candidates: [other] })),
        "deny",
      );
      assert.deepStrictEqual(
        { ...h.db.prepare("SELECT rule_kind, target, first_denied_at FROM deny_state").get() },
        { ...original },
      );
    },
  );
});

describe("ServingState budgets", () => {
  it("gives each run its own serve budget and cuts at the budget", () => {
    const h = makeHarness();
    const big = (i: number) => entry(`e${i}`, `rule ${i} ${"y".repeat(1490)}`);
    const served = [0, 1, 2, 3, 4, 5, 6].map((i) =>
      h.fire({ kind: "tool_result", candidates: [big(i)] }),
    );
    const total = served.reduce((sum, r) => sum + Buffer.byteLength(h.text(r) ?? ""), 0);
    assert.isAtMost(total, SERVE_BUDGET_BYTES);
    assert.deepStrictEqual(h.outcomes("e5"), ["cut_budget"]);
    // A second user turn is a second run: a fresh budget.
    assert.strictEqual(
      h.fire({ kind: "tool_result", run: "run-2", candidates: [big(6)] }).status,
      200,
    );
  });

  it("keeps serves out of the deny budget", () => {
    const h = makeHarness();
    for (let i = 0; i < 6; i++) {
      h.fire({ kind: "tool_result", candidates: [entry(`e${i}`, "y".repeat(1500))] });
    }
    const deny = h.fire({ kind: "pre_tool", candidates: [inline("pr", "P".repeat(600))] });
    assert.strictEqual(h.decision(deny), "deny");
  });

  it("bounds one output by the harness limit in bytes, with non-ASCII text", () => {
    const h = makeHarness();
    const wide = entry("wide", "\u{1F600}".repeat(400)); // 1,600 bytes
    const second = entry("second", "\u{1F600}".repeat(400));
    const r = h.fire({ kind: "tool_result", candidates: [wide, second] });
    assert.isAtMost(Buffer.byteLength(h.text(r)!), PROFILE.limit_bytes);
    assert.deepStrictEqual(h.outcomes("second"), ["cut_budget"]);
  });

  it("serves an entry once per run and epoch", () => {
    const h = makeHarness();
    assert.strictEqual(h.fire({ kind: "tool_result", candidates: [entry("a", "A")] }).status, 200);
    assert.strictEqual(h.fire({ kind: "tool_result", candidates: [entry("a", "A")] }).status, 204);
    assert.deepStrictEqual(h.outcomes("a"), ["injected", "deduped"]);
  });

  it("serves nothing for an unjoined or ambiguous session", () => {
    const h = makeHarness();
    assert.strictEqual(
      h.fire({ kind: "pre_tool", join: { type: "unjoined" }, candidates: [inline("x")] }).status,
      204,
    );
    assert.strictEqual(
      h.fire({ kind: "pre_tool", join: { type: "ambiguous" }, candidates: [inline("x")] }).status,
      204,
    );
    assert.deepStrictEqual(h.outcomes("x"), ["unjoined", "ambiguous"]);
  });
});

describe("ServingState deny composition", () => {
  it("carries whole rules only, always the first, and defers the rest", () => {
    const h = makeHarness();
    const first = inline("first", "A".repeat(1800));
    const second = inline("second", "B".repeat(1500));
    const r = h.fire({ kind: "pre_tool", candidates: [first, second] });
    const reason = h.text(r)!;
    assert.isAtMost(Buffer.byteLength(reason), PROFILE.limit_bytes);
    assert.include(reason, "A".repeat(1800));
    assert.notInclude(reason, "B");
    assert.deepStrictEqual(h.outcomes("second"), ["deferred"]);
    // The deferred rule denies the next matching call.
    assert.include(
      h.text(h.fire({ kind: "pre_tool", candidates: [first, second] }))!,
      "B".repeat(1500),
    );
  });

  it("cuts only a first rule that alone exceeds the limit, and logs it truncated", () => {
    const h = makeHarness();
    const huge = inline("huge", "é".repeat(1500)); // 3,000 bytes
    const r = h.fire({ kind: "pre_tool", candidates: [huge] });
    assert.isAtMost(Buffer.byteLength(h.text(r)!), PROFILE.limit_bytes);
    assert.notInclude(h.text(r)!, "�");
    assert.deepStrictEqual(h.outcomes("huge"), ["truncated"]);
    // A truncated inline deny is not a read: the next call is denied again.
    assert.strictEqual(h.decision(h.fire({ kind: "pre_tool", candidates: [huge] })), "deny");
  });

  it("still denies with the first rule when the deny budget is spent", () => {
    const h = makeHarness();
    h.fire({ kind: "pre_tool", candidates: [inline("a", "A".repeat(1800))] });
    const r = h.fire({ kind: "pre_tool", candidates: [inline("b", "B".repeat(1800))] });
    assert.strictEqual(h.decision(r), "deny");
    assert.include(h.text(r)!, "B".repeat(1800));
    const used = h.db.prepare("SELECT deny_used FROM budgets").get() as { deny_used: number };
    assert.isAbove(used.deny_used, DENY_BUDGET_BYTES);
  });
});

describe("ServingState must-know rules after their action (#231)", () => {
  const target = "/repo/skills/version-control/index.md";

  it("serves no must-know rule on the result of the call its own key matched", () => {
    const h = makeHarness();
    const rule = pointer("vc", target);
    const inline = entry("inline", "Say the token first.", { must_know: true, rule: "inline" });
    const plain = entry("labels", "Label PRs with the area.");
    h.fire({ kind: "tool_result", reads: [{ path: target, kind: "read_tool" }] });
    assert.strictEqual(h.fire({ kind: "pre_tool", candidates: [rule] }).status, 204);
    const after = h.fire({ kind: "tool_result", candidates: [rule, inline, plain] });
    assert.strictEqual(after.status, 200);
    assert.notInclude(h.text(after)!, "rule vc");
    assert.notInclude(h.text(after)!, "Say the token first.");
    assert.include(h.text(after)!, "Label PRs with the area.");
    assert.include(h.outcomes("vc"), "suppressed");
    assert.include(h.outcomes("inline"), "suppressed");
    assert.strictEqual(h.fire({ kind: "tool_failure", candidates: [rule] }).status, 204);
  });

  it("keeps a must-know rule keyed on an error, which only exists after the call", () => {
    const h = makeHarness();
    const onError = pointer("err", target);
    const r = h.fire({
      kind: "tool_failure",
      candidates: [{ ...onError, key: "error:permission denied" }],
    });
    assert.strictEqual(r.status, 200);
  });

  it("carries no must-know rule on Grok after its action", () => {
    const h = makeHarness();
    h.fire({ kind: "tool_result", profile: GROK, candidates: [pointer("vc", target)] });
    assert.strictEqual(h.fire({ kind: "pre_tool", profile: GROK }).status, 204);
    assert.deepStrictEqual(h.outcomes("vc"), ["suppressed"]);
  });
});

describe("ServingState deny lifts", () => {
  const target = "/repo/skills/version-control/index.md";

  it("lifts a pointer rule only after a successful read of its target", () => {
    const h = makeHarness();
    const rule = pointer("vc", target);
    assert.strictEqual(h.decision(h.fire({ kind: "pre_tool", candidates: [rule] })), "deny");
    assert.strictEqual(h.decision(h.fire({ kind: "pre_tool", candidates: [rule] })), "deny");
    // A failed read does not lift it.
    h.fire({ kind: "tool_failure", reads: [{ path: target, kind: "shell_full" }] });
    assert.strictEqual(h.decision(h.fire({ kind: "pre_tool", candidates: [rule] })), "deny");
    assert.deepStrictEqual(h.outcomes("vc"), ["injected", "injected", "injected"]);
    const fresh = makeHarness();
    fresh.fire({ kind: "pre_tool", candidates: [rule] });
    fresh.fire({ kind: "tool_result", reads: [{ path: target, kind: "read_tool" }] });
    assert.strictEqual(fresh.fire({ kind: "pre_tool", candidates: [rule] }).status, 204);
    assert.deepStrictEqual(fresh.outcomes("vc"), ["injected", "lifted", "passed"]);
  });

  it("does not lift on a read of another file", () => {
    const h = makeHarness();
    const rule = pointer("vc", target);
    h.fire({ kind: "pre_tool", candidates: [rule] });
    h.fire({ kind: "tool_result", reads: [{ path: "/repo/README.md", kind: "read_tool" }] });
    assert.strictEqual(h.decision(h.fire({ kind: "pre_tool", candidates: [rule] })), "deny");
  });

  it("holds a read without a failure signal (Codex) until the projection shows exit code 0", () => {
    const h = makeHarness();
    const rule = pointer("vc", target);
    h.fire({ kind: "pre_tool", candidates: [rule] });
    // A read whose success is unknown (`failed: null`) does not lift yet.
    h.fire({ kind: "tool_result", failed: null, reads: [{ path: target, kind: "shell_full" }] });
    assert.strictEqual(h.decision(h.fire({ kind: "pre_tool", candidates: [rule] })), "deny");
    // That call failed (exit code 1): its read is discarded.
    const pending = h.state.pendingReads("claude", "s1");
    assert.strictEqual(pending.length, 1);
    assert.strictEqual(
      h.decision(
        h.fire({
          kind: "pre_tool",
          candidates: [rule],
          readChecks: [{ toolUseId: pending[0]!, ok: false }],
        }),
      ),
      "deny",
    );
    assert.deepStrictEqual(h.state.pendingReads("claude", "s1"), []);
    // A later read of the same file that finished with exit code 0 lifts the rule.
    h.fire({ kind: "tool_result", failed: null, reads: [{ path: target, kind: "shell_full" }] });
    const [ok] = h.state.pendingReads("claude", "s1");
    assert.strictEqual(
      h.fire({ kind: "pre_tool", candidates: [rule], readChecks: [{ toolUseId: ok!, ok: true }] })
        .status,
      204,
    );
    // The read lifted it; the cap did not (the cap logs outcome `lifted`, reason `not_read_capped`).
    const reasons = (
      h.db
        .prepare("SELECT reason FROM candidates WHERE entry = 'vc' AND outcome = 'lifted'")
        .all() as Array<{ reason: string }>
    ).map((r) => r.reason);
    assert.deepStrictEqual(reasons, ["read"]);
  });

  it("lifts on a projected full read below the cap, and never on a projected partial read", () => {
    for (const kind of ["shell_partial", "read_partial"] as const) {
      const h = makeHarness();
      const rule = pointer("vc", target);
      h.fire({ kind: "pre_tool", candidates: [rule] });
      h.fire({ kind: "tool_result", failed: null, reads: [{ path: target, kind }] });
      const [id] = h.state.pendingReads("claude", "s1");
      const next = h.fire({
        kind: "pre_tool",
        candidates: [rule],
        readChecks: [{ toolUseId: id!, ok: true }],
      });
      assert.strictEqual(h.decision(next), "deny", kind);
      assert.notInclude(h.outcomes("vc"), "lifted", kind);
    }
    const h = makeHarness();
    const rule = pointer("vc", target);
    h.fire({ kind: "pre_tool", candidates: [rule] });
    h.fire({ kind: "tool_result", failed: null, reads: [{ path: target, kind: "shell_full" }] });
    const [id] = h.state.pendingReads("claude", "s1");
    assert.strictEqual(
      h.fire({ kind: "pre_tool", candidates: [rule], readChecks: [{ toolUseId: id!, ok: true }] })
        .status,
      204,
    );
    assert.deepStrictEqual(h.outcomes("vc"), ["injected", "lifted", "passed"]);
  });

  it("does not count a read that can never be checked (no failure signal, no tool-use id)", () => {
    const h = makeHarness();
    const rule = pointer("vc", target);
    h.fire({ kind: "pre_tool", candidates: [rule] });
    h.fire({
      kind: "tool_result",
      failed: null,
      toolUseId: null,
      reads: [{ path: target, kind: "shell_full" }],
    });
    assert.deepStrictEqual(h.state.pendingReads("claude", "s1"), []);
    assert.strictEqual(h.decision(h.fire({ kind: "pre_tool", candidates: [rule] })), "deny");
  });

  it("counts a successful read whose output merely mentions a printed exit", () => {
    const h = makeHarness();
    const rule = pointer("vc", target);
    h.fire({ kind: "pre_tool", candidates: [rule] });
    // Selection routes such output as tool_failure but keeps the harness's own `failed: false`.
    h.fire({ kind: "tool_failure", failed: false, reads: [{ path: target, kind: "shell_full" }] });
    assert.strictEqual(h.fire({ kind: "pre_tool", candidates: [rule] }).status, 204);
  });

  it("lets a call pass without a deny when the target was read first", () => {
    const h = makeHarness();
    h.fire({ kind: "tool_result", reads: [{ path: target, kind: "shell_full" }] });
    assert.strictEqual(
      h.fire({ kind: "pre_tool", candidates: [pointer("vc", target)] }).status,
      204,
    );
    assert.deepStrictEqual(h.outcomes("vc"), ["lifted"]);
  });

  it("keeps a pointer armed after partial reads, before and after the deny", () => {
    const h = makeHarness();
    const rule = pointer("vc", target);
    h.fire({ kind: "tool_result", reads: [{ path: target, kind: "shell_partial" }] });
    assert.strictEqual(h.decision(h.fire({ kind: "pre_tool", candidates: [rule] })), "deny");
    h.fire({ kind: "tool_result", reads: [{ path: target, kind: "read_partial" }] });
    assert.strictEqual(h.decision(h.fire({ kind: "pre_tool", candidates: [rule] })), "deny");
    h.fire({ kind: "tool_result", reads: [{ path: target, kind: "read_tool" }] });
    assert.strictEqual(h.fire({ kind: "pre_tool", candidates: [rule] }).status, 204);
    assert.include(h.outcomes("vc"), "lifted");
  });

  it("ignores a read in another session or an earlier epoch", () => {
    const h = makeHarness();
    const rule = pointer("vc", target);
    h.fire({ kind: "tool_result", session: "other", reads: [{ path: target, kind: "read_tool" }] });
    assert.strictEqual(h.decision(h.fire({ kind: "pre_tool", candidates: [rule] })), "deny");
    h.fire({ kind: "tool_result", reads: [{ path: target, kind: "read_tool" }] });
    assert.strictEqual(h.fire({ kind: "pre_tool", candidates: [rule] }).status, 204);
    // Compaction starts a new epoch: the rule denies again until it is read again.
    h.fire({ kind: "session_start", source: "compact" });
    assert.strictEqual(h.decision(h.fire({ kind: "pre_tool", candidates: [rule] })), "deny");
  });

  it("passes after three denies without a read, logged not_read_capped", () => {
    const h = makeHarness();
    const rule = pointer("vc", target);
    for (let i = 0; i < 3; i++) {
      assert.strictEqual(h.decision(h.fire({ kind: "pre_tool", candidates: [rule] })), "deny");
    }
    assert.strictEqual(h.fire({ kind: "pre_tool", candidates: [rule] }).status, 204);
    const lifted = h.db
      .prepare(
        "SELECT reason, extra_json FROM candidates WHERE entry = 'vc' AND outcome = 'lifted'",
      )
      .get() as { reason: string; extra_json: string };
    assert.strictEqual(lifted.reason, "not_read_capped");
    assert.strictEqual(JSON.parse(lifted.extra_json).denies, 3);
  });

  it("lifts an inline rule on a settled whole delivery, not an unsettled one", () => {
    const h = makeHarness();
    const rule = inline("pr");
    const unsettled = h.fire({ kind: "pre_tool", candidates: [rule], settle: false });
    assert.strictEqual(h.decision(unsettled), "deny");
    // The hook was killed after the reply and before settle: after the release window the retry is denied.
    h.advance(SETTLE_MS + 1);
    h.state.releaseStale(h.now());
    assert.strictEqual(h.decision(h.fire({ kind: "pre_tool", candidates: [rule] })), "deny");
    assert.strictEqual(h.fire({ kind: "pre_tool", candidates: [rule] }).status, 204);
    assert.deepStrictEqual(h.outcomes("pr"), ["delivery_unverified", "injected", "passed"]);
  });

  it("denies every call of a parallel wave until a read exists", () => {
    const h = makeHarness();
    const rule = pointer("vc", target);
    const wave = [1, 2, 3, 4, 5].map(() =>
      h.fire({ kind: "pre_tool", candidates: [rule], settle: false }),
    );
    assert.deepStrictEqual(wave.map(h.decision), ["deny", "deny", "deny", "deny", "deny"]);
  });

  it("does not spend the cap on denies whose delivery was dropped", () => {
    const h = makeHarness();
    const rule = pointer("vc", target);
    for (let i = 0; i < 3; i++) {
      assert.strictEqual(
        h.decision(h.fire({ kind: "pre_tool", candidates: [rule], settle: false })),
        "deny",
      );
      h.advance(SETTLE_MS + 1);
      h.state.releaseStale(h.now());
    }
    assert.strictEqual(h.decision(h.fire({ kind: "pre_tool", candidates: [rule] })), "deny");
    assert.notInclude(h.outcomes("vc"), "lifted");
  });
});

describe("ServingState deny length (#229)", () => {
  it("keeps a Grok deny within its reason limit and cuts the rule text, not the target", () => {
    const h = makeHarness();
    const target =
      "/Users/u/dev/toolboxmd/agentsmd/skills/operations/procedures/version-control/index.md";
    const rule = entry("vc", "AgentsMD: follow the version-control procedure before committing.", {
      must_know: true,
      rule: "pointer",
      target,
      deny_text: `Read ${target} in full, then run the same call again.\nAgentsMD: follow the version-control procedure before committing. ${"More detail. ".repeat(10)}`,
    });
    const profile: Profile = {
      ...GROK,
      deny_limit_bytes: 256,
      heads: {
        deny: "ContextMD, the user's own rule store, stopped this call:",
        context: "context-head",
      },
    };
    const r = h.fire({ kind: "pre_tool", profile, candidates: [rule] });
    const reason = h.text(r)!;
    assert.isAtMost(Buffer.byteLength(reason), 256);
    assert.include(reason, `Read ${target} in full, then run the same call again.`);
    assert.deepStrictEqual(h.outcomes("vc"), ["truncated"]);
  });
});

describe("ServingState deny length, long targets (#229, Sol review)", () => {
  const profile: Profile = {
    ...GROK,
    deny_limit_bytes: 256,
    heads: { deny: "The user's own ContextMD rule stopped this call:", context: "context-head" },
  };
  const pointerTo = (target: string) =>
    entry("vc", "AgentsMD: follow the version-control procedure before committing.", {
      must_know: true,
      rule: "pointer",
      target,
      deny_text: `Read ${target} in full, then run the same call again.\nAgentsMD: follow the version-control procedure before committing.`,
    });

  it("drops the head rather than cut a target that fits without it", () => {
    const h = makeHarness();
    const target = `/Users/u/dev/${"a".repeat(180)}/procedures/version-control/index.md`; // 229 bytes
    const reason = h.text(h.fire({ kind: "pre_tool", profile, candidates: [pointerTo(target)] }))!;
    assert.isAtMost(Buffer.byteLength(reason), 256);
    assert.include(reason, `Read ${target} in full`);
  });

  it("protects the whole target even when the path itself contains ' in full' (Sol review)", () => {
    const h = makeHarness();
    const target = `/Users/u/dev/work in full/${"a".repeat(170)}/procedures/version-control/index.md`; // 232 bytes
    const reason = h.text(h.fire({ kind: "pre_tool", profile, candidates: [pointerTo(target)] }))!;
    assert.isAtMost(Buffer.byteLength(reason), 256);
    assert.include(reason, `Read ${target} in full`);
  });

  it("protects the complete two-path opening supplied by selection", () => {
    const h = makeHarness();
    const target = `/work in full/${"a".repeat(100)}/record.md`,
      self = "/repo/library/e.md";
    const opening = `Read ${target} and ${self} in full`;
    const candidate = {
      ...pointerTo(target),
      opening,
      deny_text: `${opening}, then run the same call again.\n${"Knowledge. ".repeat(100)}`,
    };
    const reason = h.text(h.fire({ kind: "pre_tool", profile, candidates: [candidate] }))!;
    assert.isAtMost(Buffer.byteLength(reason), 256);
    assert.include(reason, opening);
  });

  it("denies nothing when even the read line cannot fit, and says why", () => {
    const h = makeHarness();
    const target = `/Users/u/dev/${"a".repeat(240)}/index.md`;
    const r = h.fire({ kind: "pre_tool", profile, candidates: [pointerTo(target)] });
    assert.strictEqual(r.status, 204);
    const rows = h.db
      .prepare("SELECT outcome, reason FROM candidates WHERE entry = 'vc'")
      .all() as Array<{ outcome: string; reason: string }>;
    assert.deepStrictEqual(rows, [{ outcome: "cut_budget", reason: "target" }]);
  });
});

describe("ServingState deny evidence (two hooks deny one call)", () => {
  const CLAUDE: Profile = { ...PROFILE, deny_evidence: "projection" };
  /** The strings of a denied call's projected `output`: what the model was shown. */
  const projected = (shown: string) => [`Error: PreToolUse:Bash hook error: ${shown}`];

  it("keeps an inline rule armed until the projection shows the model saw it", () => {
    const h = makeHarness();
    const rule = inline("pr", "Commits need the Refs: WID-42 trailer.");
    assert.strictEqual(
      h.decision(h.fire({ kind: "pre_tool", profile: CLAUDE, candidates: [rule] })),
      "deny",
    );
    // Settled, but not yet proven shown: the retry is denied again.
    const [check] = h.state.pendingChecks("claude", "s1");
    assert.isDefined(check);
    h.state.applyChecks([{ claim: check!.claim, output: null }], h.now());
    assert.strictEqual(
      h.decision(h.fire({ kind: "pre_tool", profile: CLAUDE, candidates: [rule] })),
      "deny",
    );
    for (const c of h.state.pendingChecks("claude", "s1")) {
      h.state.applyChecks(
        [{ claim: c.claim, output: projected(`deny-head\n\n${rule.text}`) }],
        h.now(),
      );
    }
    assert.strictEqual(
      h.fire({ kind: "pre_tool", profile: CLAUDE, candidates: [rule] }).status,
      204,
    );
  });

  it("re-arms a deny another hook's reason replaced, without counting it toward the cap", () => {
    const h = makeHarness();
    const rule = inline("pr", "Commits need the Refs: WID-42 trailer.");
    h.fire({ kind: "pre_tool", profile: CLAUDE, candidates: [rule] });
    const [check] = h.state.pendingChecks("claude", "s1");
    h.state.applyChecks(
      [
        {
          claim: check!.claim,
          output: projected("AgentsMD: before this commit, read version-control/index.md"),
        },
      ],
      h.now(),
    );
    assert.deepStrictEqual(h.outcomes("pr"), ["hidden"]);
    const state = h.db.prepare("SELECT denies, lifted FROM deny_state").get() as {
      denies: number;
      lifted: number;
    };
    assert.deepStrictEqual({ ...state }, { denies: 0, lifted: 0 });
    // The retry is denied with the rule, and three hidden denies never trigger the cap.
    for (let i = 0; i < 3; i++) {
      const r = h.fire({ kind: "pre_tool", profile: CLAUDE, candidates: [rule] });
      assert.include(h.text(r)!, "WID-42");
      for (const c of h.state.pendingChecks("claude", "s1")) {
        h.state.applyChecks([{ claim: c.claim, output: projected("another hook") }], h.now());
      }
    }
    assert.strictEqual(
      h.decision(h.fire({ kind: "pre_tool", profile: CLAUDE, candidates: [rule] })),
      "deny",
    );
  });

  it("counts a deny Grok showed cut as shown and truncated, not hidden (#228)", () => {
    const GROK_EVIDENCE: Profile = { ...GROK, deny_evidence: "projection" };
    // Grok shows a hook's deny reason up to 256 characters, then `… [+N chars]`.
    const grokShown = (text: string) =>
      [
        {
          type: "content",
          content: {
            type: "text",
            text: `Hook denied: ${text.slice(0, 256)}… [+${text.length - 256} chars]`,
          },
        },
      ].flatMap((p) => [p.content.text]);
    const rule = inline(
      "long",
      `Commits need the Refs trailer. ${"Explain why in the body. ".repeat(12)}`,
    );
    const denies = () => {
      const r = h.fire({ kind: "pre_tool", profile: GROK_EVIDENCE, candidates: [rule] });
      for (const c of h.state.pendingChecks("grok", "s1")) {
        h.state.applyChecks(
          [{ claim: c.claim, output: grokShown(`deny-head\n\n${rule.text}`) }],
          h.now(),
        );
      }
      return h.decision(r);
    };
    const h = makeHarness();
    assert.strictEqual(denies(), "deny");
    assert.deepStrictEqual(h.outcomes("long"), ["truncated"]);
    // Cut, so not read: the inline rule stays armed, and each cut deny counts toward the cap.
    assert.strictEqual(denies(), "deny");
    assert.strictEqual(denies(), "deny");
    assert.strictEqual(
      h.fire({ kind: "pre_tool", profile: GROK_EVIDENCE, candidates: [rule] }).status,
      204,
    );
    assert.notInclude(h.outcomes("long"), "hidden");
  });

  it("needs the whole rule: a rule sharing a prefix with the shown one stays armed", () => {
    const h = makeHarness();
    // Both rules share far more than a short probe: the first 70 characters are equal.
    const base = "Commits in this repository need the Refs: WID-42 trailer line, always.";
    const short = inline("short", base);
    const long = inline("long", `${base} Also run ./check.sh before every commit.`);
    h.fire({ kind: "pre_tool", profile: CLAUDE, candidates: [short] });
    h.fire({ kind: "pre_tool", profile: CLAUDE, candidates: [long] });
    for (const c of h.state.pendingChecks("claude", "s1")) {
      h.state.applyChecks(
        [{ claim: c.claim, output: projected(`deny-head\n\n${short.text}`) }],
        h.now(),
      );
    }
    assert.strictEqual(
      h.fire({ kind: "pre_tool", profile: CLAUDE, candidates: [short] }).status,
      204,
    );
    assert.strictEqual(
      h.decision(h.fire({ kind: "pre_tool", profile: CLAUDE, candidates: [long] })),
      "deny",
    );
  });

  it("adjudicates a deny once, however many moments check it", () => {
    const h = makeHarness();
    const rule = pointer("vc", "/repo/vc.md");
    h.fire({ kind: "pre_tool", profile: CLAUDE, candidates: [rule] });
    const [check] = h.state.pendingChecks("claude", "s1");
    const shown = { claim: check!.claim, output: projected(`deny-head\n\n${rule.deny_text}`) };
    h.state.applyChecks([shown], h.now());
    h.state.applyChecks([shown], h.now());
    h.state.applyChecks([{ claim: check!.claim, output: projected("another hook") }], h.now());
    const state = h.db.prepare("SELECT denies FROM deny_state").get() as { denies: number };
    assert.strictEqual(state.denies, 1);
    assert.deepStrictEqual(h.outcomes("vc"), ["injected"]);
  });

  it("stops waiting for evidence after a minute, logs unverified and counts the deny", () => {
    const h = makeHarness();
    const rule = pointer("vc", "/repo/vc.md");
    // Twelve settled denies without any projected item: more than one lookup batch.
    for (let i = 0; i < 12; i++) {
      assert.strictEqual(
        h.decision(h.fire({ kind: "pre_tool", profile: CLAUDE, candidates: [rule] })),
        "deny",
      );
    }
    assert.deepStrictEqual([...new Set(h.outcomes("vc"))], ["awaiting_evidence"]);
    h.advance(61_000);
    h.state.releaseStale(h.now());
    assert.deepStrictEqual([...new Set(h.outcomes("vc"))], ["unverified"]);
    const state = h.db.prepare("SELECT denies FROM deny_state").get() as { denies: number };
    assert.strictEqual(state.denies, 12);
    assert.strictEqual(
      h.fire({ kind: "pre_tool", profile: CLAUDE, candidates: [rule] }).status,
      204,
    );
  });

  it("lifts an inline rule on settlement where the harness leaves no evidence", () => {
    const h = makeHarness();
    const rule = inline("pr");
    h.fire({ kind: "pre_tool", candidates: [rule] });
    assert.deepStrictEqual(h.state.pendingChecks("claude", "s1"), []);
    assert.strictEqual(h.fire({ kind: "pre_tool", candidates: [rule] }).status, 204);
  });
});

describe("ServingState settlement", () => {
  it("answers a repeated delivery id with the same reply without charging again", () => {
    const h = makeHarness();
    const first = h.fire({ kind: "tool_result", candidates: [entry("a", "A")], delivery: "same" });
    assert.deepStrictEqual(h.state.replayed("same"), {
      status: 200,
      body: (first as { body: string }).body,
    });
    const used = h.db.prepare("SELECT serve_used FROM budgets").get() as { serve_used: number };
    assert.strictEqual(used.serve_used, Buffer.byteLength(h.text(first)!));
  });

  it("releases a dropped OpenCode reservation at once and returns its bytes", () => {
    const h = makeHarness();
    const opencode: Profile = {
      ...PROFILE,
      harness: "opencode",
      settle_within_ms: 120_000,
      templates: { context: { context: "{{text}}" }, deny: { deny: "{{text}}" } },
    };
    const held = h.fire({
      kind: "tool_result",
      profile: opencode,
      candidates: [entry("a", "A".repeat(1500))],
      settle: false,
    });
    const budget = (run: string) =>
      h.db.prepare("SELECT serve_used, deny_used FROM budgets WHERE run_id = ?").get(run) as {
        serve_used: number;
        deny_used: number;
      };
    const reserved = budget("run-1").serve_used;
    assert.isAbove(reserved, 0);
    // Inside OpenCode's 120 s window the sweep does not free it.
    h.advance(10_000);
    h.state.releaseStale(h.now());
    assert.strictEqual(budget("run-1").serve_used, reserved);
    assert.strictEqual(h.state.releaseReservation(held.delivery), true);
    assert.strictEqual(budget("run-1").serve_used, 0);
    assert.deepStrictEqual(h.outcomes("a"), ["delivery_unverified"]);
    assert.strictEqual(h.state.releaseReservation(held.delivery), false);
    assert.strictEqual(h.state.settle(held.delivery, h.now()), false);
    const again = h.fire({
      kind: "tool_result",
      profile: opencode,
      candidates: [entry("a", "A".repeat(1500))],
    });
    assert.strictEqual(again.status, 200);
    // A reply the model was shown is not a drop.
    assert.strictEqual(h.state.releaseReservation(again.delivery), false);
    assert.strictEqual(budget("run-1").serve_used, reserved);
    const denied = h.fire({
      kind: "pre_tool",
      profile: opencode,
      run: "run-deny",
      candidates: [inline("pr", "R".repeat(400))],
      settle: false,
    });
    assert.isAbove(budget("run-deny").deny_used, 0);
    assert.strictEqual(h.state.releaseReservation(denied.delivery), true);
    assert.strictEqual(budget("run-deny").deny_used, 0);
    assert.deepStrictEqual(h.outcomes("pr"), ["delivery_unverified"]);
  });

  it("keeps a reply reserved for the harness's own settle window (OpenCode settles when the model reads it)", () => {
    const h = makeHarness();
    const opencode: Profile = { ...PROFILE, harness: "opencode", settle_within_ms: 120_000 };
    const r = h.fire({
      kind: "pre_tool",
      profile: opencode,
      candidates: [inline("pr")],
      settle: false,
    });
    h.advance(10_000);
    h.state.releaseStale(h.now());
    assert.isTrue(h.state.settle(r.delivery, h.now()));
    assert.deepStrictEqual(h.outcomes("pr"), ["injected"]);
  });

  it("refuses a settle that arrives after the deadline, with no sweep in between", () => {
    const h = makeHarness();
    const r = h.fire({ kind: "pre_tool", candidates: [inline("pr")], settle: false });
    h.advance(10_000);
    assert.isFalse(h.state.settle(r.delivery, h.now()));
    assert.deepStrictEqual(h.outcomes("pr"), ["delivery_unverified"]);
    const used = h.db.prepare("SELECT deny_used FROM budgets").get() as { deny_used: number };
    assert.strictEqual(used.deny_used, 0);
    assert.strictEqual(
      h.decision(h.fire({ kind: "pre_tool", candidates: [inline("pr")] })),
      "deny",
    );
  });

  it("releases an unsettled reservation's bytes and claims", () => {
    const h = makeHarness();
    h.fire({ kind: "tool_result", candidates: [entry("a", "A".repeat(1000))], settle: false });
    h.advance(SETTLE_MS + 1);
    h.state.releaseStale(h.now());
    const used = h.db.prepare("SELECT serve_used FROM budgets").get() as { serve_used: number };
    assert.strictEqual(used.serve_used, 0);
    assert.strictEqual(h.state.settle("d1", h.now()), false);
    assert.strictEqual(
      h.fire({ kind: "tool_result", candidates: [entry("a", "A".repeat(1000))] }).status,
      200,
    );
  });

  it("fails fast when another writer holds the lock", async () => {
    const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "contextmd-lock-"));
    const file = NodePath.join(dir, "contextmd.sqlite");
    const h = makeHarness(file);
    const holder = NodeChildProcess.spawn(
      process.execPath,
      [
        "-e",
        `const db = new (require("node:sqlite").DatabaseSync)(process.argv[1]); db.exec("BEGIN IMMEDIATE"); process.stdout.write("locked\\n"); setTimeout(() => {}, 5000);`,
        file,
      ],
      { stdio: ["ignore", "pipe", "ignore"] },
    );
    try {
      await new Promise<void>((resolve) => holder.stdout!.once("data", () => resolve()));
      const started = performance.now();
      assert.throws(() => h.fire({ kind: "tool_result", candidates: [entry("a", "A")] }));
      assert.isBelow(performance.now() - started, 1000);
    } finally {
      holder.kill("SIGKILL");
      NodeFS.rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe("ServingState epochs and carries", () => {
  it("re-serves the session-start pack after compaction, outside both budgets", () => {
    const h = makeHarness();
    const pack = entry("/notes/PACK.md", "Prefer small diffs.", { matcher: "session_start" });
    assert.strictEqual(
      h.text(h.fire({ kind: "session_start", source: "startup", candidates: [pack] })),
      "Prefer small diffs.",
    );
    assert.strictEqual(
      h.text(h.fire({ kind: "session_start", source: "compact", candidates: [pack] })),
      "Prefer small diffs.",
    );
    const epochs = h.db.prepare("SELECT epoch FROM moments ORDER BY id").all() as Array<{
      epoch: number;
    }>;
    assert.deepStrictEqual(
      epochs.map((e) => e.epoch),
      [0, 1],
    );
    assert.strictEqual(h.db.prepare("SELECT count(*) AS n FROM budgets").get()!.n, 0);
  });

  it("starts a new epoch on a compaction moment", () => {
    const h = makeHarness();
    h.fire({ kind: "tool_result", candidates: [entry("a", "A")] });
    h.fire({ kind: "compaction", profile: { ...PROFILE, new_epoch_sources: [] } });
    assert.strictEqual(h.fire({ kind: "tool_result", candidates: [entry("a", "A")] }).status, 200);
  });

  it("carries a Grok serve to the next PreToolUse in the same run, then expires it with the run", () => {
    const h = makeHarness();
    assert.strictEqual(
      h.fire({ kind: "tool_result", profile: GROK, candidates: [entry("a", "carry me")] }).status,
      204,
    );
    const printed = h.fire({ kind: "pre_tool", profile: GROK });
    assert.include(h.text(printed)!, "carry me");
    assert.deepStrictEqual(h.outcomes("a"), ["carried", "injected"]);
    h.fire({ kind: "tool_result", profile: GROK, candidates: [entry("b", "later")] });
    h.fire({ kind: "pre_tool", profile: GROK, run: "run-2" });
    assert.deepStrictEqual(h.outcomes("b"), ["carry_expired"]);
  });

  it("replaces a waiting carry with a newer entry version", () => {
    const h = makeHarness();
    h.fire({ kind: "tool_result", profile: GROK, candidates: [entry("a", "old")] });
    h.fire({
      kind: "tool_result",
      profile: GROK,
      candidates: [entry("a", "new", { version: "v2" })],
    });
    assert.deepStrictEqual(h.outcomes("a"), ["superseded", "carried"]);
    assert.include(h.text(h.fire({ kind: "pre_tool", profile: GROK }))!, "new");
  });
});

describe("ServingState log", () => {
  it("records ids, versions and outcomes without prompt, command or result text", () => {
    const h = makeHarness();
    h.fire({ kind: "pre_tool", candidates: [inline("pr", "Rule text.")] });
    const dump = JSON.stringify([
      h.db.prepare("SELECT * FROM moments").all(),
      h.db.prepare("SELECT * FROM candidates").all(),
    ]);
    assert.notInclude(dump, "Rule text.");
    const moment = h.db.prepare("SELECT * FROM moments").get() as Record<string, unknown>;
    assert.strictEqual(moment.run_id, "run-1");
    assert.strictEqual(moment.owners_json, JSON.stringify({ "/store": "abc123" }));
  });

  it("puts about one session in ten in the holdout arm", () => {
    let held = 0;
    for (let i = 0; i < 10_000; i++) if (isHoldout("claude", `session-${i}`)) held += 1;
    assert.isAbove(held, 900);
    assert.isBelow(held, 1100);
  });
});

const cueEntry = (id: string, text = `${id} applies when: x\nprocedure ${id}`): Candidate => ({
  matcher: "intent",
  entry: id,
  version: "c1",
  key: null,
  text,
  must_know: false,
  rule: null,
  target: null,
});

const cueOk = (scores: Record<string, number>, threshold = 0.5): CueMoment => ({
  reply: { status: "ok", ms: 340, waitMs: 340, adapter: "q4b-w8a8c", threshold, scores },
  candidates: Object.keys(scores).map((id) => cueEntry(id)),
});

/** The first session id, counting up, that the holdout hash puts in (or out of) the holdout arm. */
const sessionIn = (holdout: boolean, harness = "claude") => {
  for (let i = 0; ; i++) if (isHoldout(harness, `s${i}`) === holdout) return `s${i}`;
};

describe("ServingState Cue", () => {
  const scores = (db: NodeSqlite.DatabaseSync) =>
    db
      .prepare("SELECT entry, outcome, score FROM candidates WHERE matcher = 'intent' ORDER BY id")
      .all() as Array<{ entry: string; outcome: string; score: number }>;
  const moments = (db: NodeSqlite.DatabaseSync) =>
    db
      .prepare(
        "SELECT kind, holdout, cue_status, cue_adapter, cue_ms, cue_wait_ms FROM moments ORDER BY id",
      )
      .all() as Array<Record<string, unknown>>;

  it("serves fires at the user message and logs every score", () => {
    const h = makeHarness();
    const session = sessionIn(false);
    const r = h.fire({
      kind: "user_message",
      session,
      cue: cueOk({ research: 1.5, grilling: 0.25, specify: 0.5 }),
    });
    const text = h.text(r)!;
    assert.include(text, "procedure research");
    assert.include(text, "procedure specify");
    assert.notInclude(text, "procedure grilling");
    assert.deepStrictEqual(scores(h.db), [
      { entry: "grilling", outcome: "below_threshold", score: 0.25 },
      { entry: "research", outcome: "injected", score: 1.5 },
      { entry: "specify", outcome: "injected", score: 0.5 },
    ]);
    assert.deepStrictEqual(moments(h.db), [
      {
        kind: "user_message",
        holdout: 0,
        cue_status: "ok",
        cue_adapter: "q4b-w8a8c",
        cue_ms: 340,
        cue_wait_ms: 340,
      },
    ]);
  });

  it("logs a holdout session's would-fires without printing or charging them", () => {
    const h = makeHarness();
    const session = sessionIn(true);
    const r = h.fire({ kind: "user_message", session, cue: cueOk({ research: 1.5 }) });
    assert.strictEqual(r.status, 204);
    assert.deepStrictEqual(scores(h.db), [{ entry: "research", outcome: "holdout", score: 1.5 }]);
    assert.strictEqual(moments(h.db)[0]!["holdout"], 1);
    // The holdout cost nothing: the whole serve budget is left for exact keys.
    const big = h.fire({
      kind: "tool_result",
      session,
      candidates: [entry("e", "y".repeat(1800))],
    });
    assert.strictEqual(big.status, 200);
  });

  it("serves a collected fire beside the tool result with the exact-key matches, once per run", () => {
    const h = makeHarness();
    const session = sessionIn(false);
    const first = h.fire({
      kind: "tool_result",
      session,
      candidates: [entry("a", "rule a")],
      cue: cueOk({ research: 2 }),
    });
    assert.include(h.text(first)!, "rule a");
    assert.include(h.text(first)!, "procedure research");
    h.fire({ kind: "tool_result", session, cue: cueOk({ research: 2 }) });
    assert.deepStrictEqual(
      scores(h.db).map((c) => c.outcome),
      ["injected", "deduped"],
    );
  });

  it("carries a Grok fire from the user message to the next PreToolUse", () => {
    const h = makeHarness();
    const session = sessionIn(false, "grok");
    assert.strictEqual(
      h.fire({ kind: "user_message", session, profile: GROK, cue: cueOk({ research: 2 }) }).status,
      204,
    );
    const printed = h.fire({ kind: "pre_tool", session, profile: GROK });
    assert.include(h.text(printed)!, "procedure research");
    assert.deepStrictEqual(
      scores(h.db).map((c) => [c.outcome, c.score]),
      [
        ["carried", 2],
        ["injected", 2],
      ],
    );
  });

  it("remembers that Cue is down in the database the next process opens", () => {
    const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "cue-down-"));
    const file = NodePath.join(dir, "contextmd.sqlite");
    try {
      const first = new ServingState(new NodeSqlite.DatabaseSync(file));
      assert.isFalse(first.cueDown("http://127.0.0.1:9"));
      first.markCueDown("http://127.0.0.1:9", 10);
      first.markCueDown("http://127.0.0.1:9", 11);
      assert.isTrue(first.cueDown("http://127.0.0.1:9"));
      assert.isFalse(first.cueDown("http://127.0.0.1:10"));
      const again = new ServingState(new NodeSqlite.DatabaseSync(file));
      assert.isTrue(again.cueDown("http://127.0.0.1:9"));
      again.clearCueDown("http://127.0.0.1:9");
      assert.isFalse(
        new ServingState(new NodeSqlite.DatabaseSync(file)).cueDown("http://127.0.0.1:9"),
      );
    } finally {
      NodeFS.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("records an unreachable or slow Cue per moment and still serves exact keys", () => {
    const h = makeHarness();
    const failed = (status: CueReply["status"]): CueMoment => ({
      reply: { status, ms: status === "not_called" ? null : 12, waitMs: 0 },
      candidates: [cueEntry("research")],
    });
    h.fire({ kind: "user_message", cue: failed("connect_failed") });
    const r = h.fire({
      kind: "tool_result",
      candidates: [entry("a", "rule a")],
      cue: failed("not_called"),
    });
    assert.include(h.text(r)!, "rule a");
    h.fire({
      kind: "pre_tool",
      cue: { reply: { status: "started", ms: null, waitMs: 0 }, candidates: [] },
    });
    assert.deepStrictEqual(
      moments(h.db).map((m) => m["cue_status"]),
      ["connect_failed", "not_called", "started"],
    );
    assert.deepStrictEqual(scores(h.db), []);
  });

  it("logs server time in milliseconds", () => {
    const h = makeHarness();
    h.fire({ kind: "user_message", elapsedMs: 250 });
    h.state.logUnserved({
      harness: null,
      delivery: null,
      status: "bad_request",
      now: 1_000.5,
      startedAt: 1_000,
    });
    assert.deepStrictEqual(
      (
        h.db.prepare("SELECT server_ms FROM moments ORDER BY id").all() as Array<{
          server_ms: number;
        }>
      ).map((m) => m.server_ms),
      [250, 500],
    );
  });

  it("converts server time logged in seconds by an earlier build, once", () => {
    const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "contextmd-state-"));
    const file = NodePath.join(dir, "contextmd.sqlite");
    const old = new NodeSqlite.DatabaseSync(file);
    old.exec(`CREATE TABLE moments (id INTEGER PRIMARY KEY, received_at REAL NOT NULL, harness TEXT,
      kind TEXT, session TEXT, thread_id TEXT, run_id TEXT, tool_use_id TEXT, epoch INTEGER,
      holdout INTEGER, status TEXT NOT NULL, reason TEXT, owners_json TEXT,
      cue_adapter TEXT, server_ms REAL, delivery TEXT);
      INSERT INTO moments (received_at, status, server_ms) VALUES (1, 'ok', 0.25);`);
    old.close();
    const serverMs = () => {
      const db = new NodeSqlite.DatabaseSync(file);
      const state = new ServingState(db);
      assert.ok(state);
      const { server_ms } = db.prepare("SELECT server_ms FROM moments").get() as {
        server_ms: number;
      };
      db.close();
      return server_ms;
    };
    assert.strictEqual(serverMs(), 250);
    assert.strictEqual(serverMs(), 250);
    NodeFS.rmSync(dir, { recursive: true, force: true });
  });

  it("adds the Cue columns to a state file from an earlier build", () => {
    const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "contextmd-state-"));
    const file = NodePath.join(dir, "contextmd.sqlite");
    const old = new NodeSqlite.DatabaseSync(file);
    old.exec(`CREATE TABLE moments (id INTEGER PRIMARY KEY, received_at REAL NOT NULL, harness TEXT,
      kind TEXT, session TEXT, thread_id TEXT, run_id TEXT, tool_use_id TEXT, epoch INTEGER,
      holdout INTEGER, status TEXT NOT NULL, reason TEXT, owners_json TEXT,
      cue_adapter TEXT, server_ms REAL, delivery TEXT);
      CREATE TABLE carries (id INTEGER PRIMARY KEY, harness TEXT NOT NULL, session TEXT NOT NULL,
      run_id TEXT NOT NULL, entry TEXT NOT NULL, version TEXT NOT NULL, text TEXT NOT NULL,
      candidate_id INTEGER NOT NULL, state TEXT NOT NULL, delivery TEXT, created_at REAL NOT NULL);`);
    old.close();
    const h = makeHarness(file);
    h.fire({ kind: "user_message", session: sessionIn(false), cue: cueOk({ research: 2 }) });
    assert.strictEqual(moments(h.db)[0]!["cue_status"], "ok");
    h.db.close();
    NodeFS.rmSync(dir, { recursive: true, force: true });
  });
});

describe("ServingState shown evidence", () => {
  const facts = (db: NodeSqlite.DatabaseSync) =>
    db
      .prepare(
        "SELECT delivery, shown, shown_json, shown_next, transcript_path, native_turn FROM deliveries ORDER BY rowid",
      )
      .all() as Array<{
      delivery: string;
      shown: string | null;
      shown_json: string | null;
      shown_next: number | null;
      transcript_path: string | null;
      native_turn: string | null;
    }>;

  it("marks only settled context or carry prints, storing native fields on 200 replies", () => {
    const h = makeHarness();
    const context = h.fire({
      kind: "tool_result",
      candidates: [entry("context", "public rule")],
      transcriptPath: "/tmp/s1.jsonl",
      nativeTurn: "native-turn",
    });
    const reserved = h.fire({
      kind: "tool_result",
      candidates: [entry("reserved", "another rule")],
      settle: false,
    });
    const released = h.fire({
      kind: "tool_result",
      candidates: [entry("released", "released rule")],
      settle: false,
    });
    assert.isTrue(h.state.releaseReservation(released.delivery));
    const deny = h.fire({
      kind: "pre_tool",
      candidates: [inline("deny")],
      transcriptPath: "/tmp/s1.jsonl",
    });
    const empty = h.fire({
      kind: "user_message",
      transcriptPath: "/tmp/s1.jsonl",
      nativeTurn: "turn",
    });
    h.fire({ kind: "tool_result", profile: GROK, candidates: [entry("carry", "carried rule")] });
    const printed = h.fire({ kind: "pre_tool", profile: GROK });
    const rows = facts(h.db);
    const c = rows.find((r) => r.delivery === context.delivery)!;
    assert.equal(c.shown, "unknown");
    assert.equal(c.shown_next, 1000);
    assert.deepEqual(JSON.parse(c.shown_json!), { v: 1, source: null, reason: "not_checked" });
    assert.equal(c.transcript_path, "/tmp/s1.jsonl");
    assert.equal(c.native_turn, "native-turn");
    assert.equal(rows.find((r) => r.delivery === printed.delivery)!.shown, "unknown");
    for (const delivery of [reserved.delivery, released.delivery, deny.delivery, empty.delivery])
      assert.isNull(rows.find((r) => r.delivery === delivery)!.shown);
    assert.isNull(rows.find((r) => r.delivery === empty.delivery)!.transcript_path);
    assert.isNull(rows.find((r) => r.delivery === empty.delivery)!.native_turn);
    assert.deepEqual(
      h.state.dueShown(1000, 50).map((r) => r.delivery),
      [context.delivery, printed.delivery],
    );
    h.db.close();
  });

  it("bounds stored native fields, and no shown fact is backfilled for old rows", () => {
    const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "contextmd-shown-migration-"));
    const file = NodePath.join(dir, "contextmd.sqlite");
    const old = new NodeSqlite.DatabaseSync(file);
    old.exec(`CREATE TABLE deliveries (delivery TEXT PRIMARY KEY, harness TEXT NOT NULL, moment_id INTEGER,
      stage TEXT NOT NULL, status INTEGER NOT NULL, body TEXT, run_id TEXT, serve_bytes INTEGER NOT NULL DEFAULT 0,
      deny_bytes INTEGER NOT NULL DEFAULT 0, created_at REAL NOT NULL, expires_at REAL NOT NULL, settled_at REAL);
      INSERT INTO deliveries (delivery, harness, stage, status, created_at, expires_at)
        VALUES ('old', 'claude', 'settled', 200, 10, 20);
      PRAGMA user_version = 1;`);
    old.close();
    const h = makeHarness(file);
    assert.deepEqual(facts(h.db)[0], {
      delivery: "old",
      shown: null,
      shown_json: null,
      shown_next: null,
      transcript_path: null,
      native_turn: null,
    });
    assert.equal(
      (h.db.prepare("PRAGMA user_version").get() as { user_version: number }).user_version,
      1,
    );
    assert.ok(
      h.db
        .prepare(
          "SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'deliveries_shown_due'",
        )
        .get(),
    );
    const d = h.fire({
      kind: "tool_result",
      candidates: [entry("large", "public rule")],
      transcriptPath: "x".repeat(4097),
      nativeTurn: "",
    });
    assert.isNull(facts(h.db).find((r) => r.delivery === d.delivery)!.transcript_path);
    assert.isNull(facts(h.db).find((r) => r.delivery === d.delivery)!.native_turn);
    assert.isFalse(h.state.recordShown("old", "shown", "{}", null));
    h.state.deferShown("old", 1001);
    assert.isNull(facts(h.db)[0]!.shown_next);
    h.db.close();
    // Reopening proves the additive migration and its index are idempotent.
    const reopened = makeHarness(file);
    assert.equal(reopened.state.dueShown(1000, 50).length, 1);
    reopened.db.close();
    NodeFS.rmSync(dir, { recursive: true, force: true });
  });

  it("rotates waiting rows, limits selection and never requeues shown after a group failure", () => {
    const h = makeHarness();
    const one = h.fire({ kind: "tool_result", candidates: [entry("one", "one")], run: "run-one" });
    const two = h.fire({ kind: "tool_result", candidates: [entry("two", "two")], run: "run-two" });
    assert.deepEqual(
      h.state.dueShown(1000, 1).map((r) => r.delivery),
      [one.delivery],
    );
    assert.isTrue(
      h.state.recordShown(
        one.delivery,
        "unknown",
        '{"v":1,"reason":"no_model_response","seen":"codex-1:20:10"}',
        1030,
      ),
    );
    assert.deepEqual(
      h.state.dueShown(1000, 50).map((r) => r.delivery),
      [two.delivery],
    );
    assert.isTrue(
      h.state.recordShown(two.delivery, "shown", '{"v":1,"source":"codex_rollout"}', 5000),
    );
    h.state.deferShown(two.delivery, 1030);
    assert.isFalse(
      h.state.recordShown(two.delivery, "unknown", '{"reason":"unrecognized_format"}', 1030),
    );
    assert.isNull(facts(h.db).find((r) => r.delivery === two.delivery)!.shown_next);
    assert.deepEqual(
      h.state.dueShown(1030, 50).map((r) => r.delivery),
      [one.delivery],
    );
    h.db.close();
  });

  it("rivals include reserved and released 200 replies after their claims are removed", () => {
    const h = makeHarness();
    const settled = h.fire({
      kind: "tool_result",
      candidates: [entry("settled", "same")],
      run: "one",
    });
    const reserved = h.fire({
      kind: "tool_result",
      candidates: [entry("reserved", "same")],
      run: "two",
      settle: false,
    });
    const released = h.fire({
      kind: "tool_result",
      candidates: [entry("released", "same")],
      run: "three",
      settle: false,
    });
    h.state.releaseReservation(released.delivery);
    h.fire({ kind: "user_message" });
    h.fire({ kind: "tool_result", session: "other", candidates: [entry("other", "same")] });
    assert.deepEqual(
      h.state.shownRivals("claude", "s1").map((r) => r.delivery),
      [settled.delivery, reserved.delivery, released.delivery],
    );
    assert.equal(
      (
        h.db
          .prepare("SELECT count(*) AS n FROM claims WHERE delivery = ?")
          .get(released.delivery) as { n: number }
      ).n,
      0,
    );
    h.db.close();
  });
});
