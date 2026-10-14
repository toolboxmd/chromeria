// @effect-diagnostics nodeBuiltinImport:off - isolated filesystem and SQLite owner tests.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import * as NodeCrypto from "node:crypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import { HARNESSES } from "./Harnesses.ts";
import * as Select from "./Select.ts";
import type { Candidate, Profile, Selection } from "./Selection.ts";
import { ServingState, type Reply, type SessionInstructions } from "./ServingState.ts";
const corpus = Schema.decodeUnknownSync(
  Schema.fromJsonString(
    Schema.Array(
      Schema.Struct({
        name: Schema.String,
        value: Schema.Unknown,
        expanded: Schema.optional(Schema.String),
        valid: Schema.Boolean,
      }),
    ),
  ),
)(NodeFS.readFileSync(new URL("./great-library-path-corpus.json", import.meta.url), "utf8"));

const HEAD = "ContextMD, the user's own rule store, applies this rule from the user here:";
const LINE =
  "The Great Library holds knowledge shared across projects and personal knowledge outside any project. Read /tmp/knowledge 🧠 index.md in full.";
const TEXT = `${HEAD}\n\n${LINE}`;
const H = Buffer.byteLength(TEXT);
const pointer: Candidate = {
  matcher: "great_library",
  entry: "/tmp/knowledge 🧠 index.md",
  version: NodeCrypto.createHash("sha256").update(LINE).digest("hex").slice(0, 12),
  key: null,
  text: LINE,
  must_know: false,
  rule: null,
  target: null,
};
const ordinary: Candidate = {
  matcher: "intent",
  entry: "cue",
  version: "a",
  key: null,
  text: "An ordinary Cue rule.",
  must_know: false,
  rule: null,
  target: null,
};
const installed: SessionInstructions = {
  entry: pointer.entry,
  version: pointer.version,
  text: TEXT,
  bytes: H,
  owners: { "/tmp": "frozen-owner" },
};
const profile = (name: string): Profile => {
  const h = HARNESSES[name]!;
  return {
    harness: name,
    delivering: h.delivering,
    carry_to: h.carryTo,
    limit_bytes: h.limitBytes,
    settle_within_ms: h.settleWithinMs,
    session_start_limit_bytes: h.sessionStartLimitBytes,
    new_epoch_sources: h.newEpochSources,
    templates: h.templates,
    heads: { context: HEAD, deny: "deny" },
  };
};
const make = (file = ":memory:") => {
  const db = new NodeSqlite.DatabaseSync(file);
  const state = new ServingState(db);
  let n = 0;
  const fire = (
    harness: string,
    kind: Selection["moment"]["kind"],
    extra: {
      session?: string;
      run?: string;
      source?: string;
      pointer?: Candidate | null;
      ordinary?: Candidate[];
      pointerDelivery?: string;
      delivery?: string;
      now?: number;
      joined?: boolean;
    } = {},
  ) => {
    const delivery = extra.delivery ?? `d${++n}`;
    const sel: Selection = {
      greatLibrary: extra.pointer === undefined ? pointer : extra.pointer,
      greatLibraryBytes: H,
      moment: {
        kind,
        event: kind === "session_start" ? "SessionStart" : "UserPromptSubmit",
        session: extra.session ?? "S",
        source: extra.source ?? null,
        tool: null,
        tool_use_id: null,
        failed: null,
        reads: [],
      },
      candidates: extra.ordinary ?? [],
      notes: [],
      owners: {},
      profile: profile(harness),
    };
    const reply = state.decide({
      harness,
      delivery,
      selection: sel,
      ...(extra.pointerDelivery === undefined ? {} : { pointerDelivery: extra.pointerDelivery }),
      join:
        extra.joined === false
          ? { type: "unjoined" }
          : { type: "run", runId: extra.run ?? "R", threadId: "T" },
      now: extra.now ?? 100,
      startedAt: extra.now ?? 100,
    });
    return { ...reply, delivery };
  };
  const rows = (sql: string) =>
    db
      .prepare(sql)
      .all()
      .map((row) => ({ ...row }));
  const used = () =>
    (
      db.prepare("SELECT serve_used FROM budgets WHERE run_id = 'R'").get() as
        | { serve_used: number }
        | undefined
    )?.serve_used ?? 0;
  const text = (reply: Reply) =>
    reply.status === 200 ? JSON.parse(reply.body).hookSpecificOutput?.additionalContext : null;
  return { db, state, fire, rows, used, text };
};

describe("Great Library grammar", () => {
  it.effect.each(corpus)("$name", (fixture) =>
    Effect.gen(function* () {
      const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "great-library-"));
      try {
        const file = NodePath.join(dir, "serving.json");
        NodeFS.writeFileSync(file, JSON.stringify({ great_library: fixture.value }));
        const config = yield* Effect.result(
          Select.loadConfig(file).pipe(
            Effect.provideService(
              HostProcess.HomeDirectory,
              fixture.expanded === undefined
                ? NodeOS.homedir()
                : fixture.expanded.slice(0, -String(fixture.value).slice(1).length),
            ),
          ),
        );
        assert.strictEqual(Result.isSuccess(config), fixture.valid);
        if (Result.isSuccess(config)) {
          const expected = "expanded" in fixture ? fixture.expanded : fixture.value;
          assert.strictEqual(config.success.greatLibrary, expected);
        }
      } finally {
        NodeFS.rmSync(dir, { recursive: true });
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect.each(["~/a/../index.md", "~//space index.md", "~/a/./知識.md"])(
    "keeps Python expanduser path spelling for %s",
    (value) =>
      Effect.gen(function* () {
        const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "great-library-"));
        try {
          const file = NodePath.join(dir, "serving.json");
          NodeFS.writeFileSync(file, JSON.stringify({ great_library: value }));
          const config = yield* Select.loadConfig(file);
          assert.equal(config.greatLibrary, NodeOS.homedir() + value.slice(1));
        } finally {
          NodeFS.rmSync(dir, { recursive: true });
        }
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("requires a regular index and versions the accurate plain line", () =>
    Effect.gen(function* () {
      const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "great-library-"));
      const config: Select.ServingConfig = {
        exactKeys: [],
        cue: [],
        sessionStart: [],
        cueUrl: null,
        greatLibrary: NodePath.join(dir, "index.md"),
      };
      try {
        assert.isNull(yield* Select.prepareGreatLibrary(config));
        assert.isNull(yield* Select.prepareGreatLibrary({ ...config, greatLibrary: dir }));
        NodeFS.writeFileSync(config.greatLibrary!, "index");
        const c = yield* Select.prepareGreatLibrary(config);
        assert.strictEqual(
          c?.text,
          `The Great Library holds knowledge shared across projects and personal knowledge outside any project. Read ${config.greatLibrary} in full.`,
        );
        assert.strictEqual(
          c?.version,
          NodeCrypto.createHash("sha256").update(c!.text).digest("hex").slice(0, 12),
        );
        assert.strictEqual(
          Select.greatLibraryEntry(Select.greatLibraryText(c!.text)),
          config.greatLibrary,
        );
      } finally {
        NodeFS.rmSync(dir, { recursive: true });
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});

describe("Great Library serving ownership", () => {
  it("claims by harness/session/epoch, freezes config and releases only its reservation", () => {
    const h = make();
    try {
      const first = h.fire("claude", "user_message");
      assert.strictEqual(h.text(first), TEXT);
      assert.strictEqual(h.used(), H);
      assert.strictEqual(h.fire("claude", "user_message").status, 204);
      h.state.settle(first.delivery, 100);
      assert.strictEqual(
        h.fire("claude", "user_message", {
          pointer: { ...pointer, version: "new", text: "changed" },
        }).status,
        204,
      );
      const second = h.fire("claude", "user_message", { session: "S2" });
      assert.strictEqual(h.text(second), TEXT);
      assert.isTrue(h.state.releaseReservation(second.delivery));
      assert.isTrue(h.state.greatLibraryOwed("claude", "S2", 0));
      assert.strictEqual(h.fire("codex", "user_message").status, 200);
      assert.strictEqual(
        h.fire("claude", "user_message", { session: "unjoined", joined: false }).status,
        204,
      );
      assert.lengthOf(h.rows("SELECT * FROM candidates WHERE matcher = 'great_library'"), 3);
    } finally {
      h.db.close();
    }
  });

  it.each([
    ["compact", "post"],
    ["post", "compact"],
  ])("Codex bumps once in order %s then %s", (...order) => {
    const h = make();
    try {
      const first = h.fire("codex", "user_message");
      h.state.settle(first.delivery, 100);
      assert.strictEqual(h.fire("codex", "session_start", { source: "resume" }).status, 204);
      for (const signal of order) {
        const reply =
          signal === "compact"
            ? h.fire("codex", "session_start", { source: "compact" })
            : h.fire("codex", "compaction");
        assert.strictEqual(reply.status, signal === "compact" ? 200 : 204);
        if (signal === "compact") {
          assert.strictEqual(h.text(reply), TEXT);
          h.state.settle(reply.delivery, 100);
        }
      }
      assert.strictEqual(h.used(), H * 2);
      assert.deepStrictEqual(h.rows("SELECT epoch FROM sessions"), [{ epoch: 1 }]);
      assert.strictEqual(h.fire("codex", "user_message").status, 204);
    } finally {
      h.db.close();
    }
  });

  it("keeps the Claude compact pointer first and whole within the startup cap", () => {
    const h = make();
    try {
      h.fire("claude", "user_message");
      const files = [8000, 1100, 400].map((size, i) => ({
        ...ordinary,
        matcher: "session_start",
        entry: `file${i}`,
        text: "f".repeat(size),
      }));
      const reply = h.fire("claude", "session_start", { source: "compact", ordinary: files });
      const text = h.text(reply);
      assert.isTrue(text.startsWith(TEXT));
      assert.isAtMost(Buffer.byteLength(text), 9500);
      assert.strictEqual(h.used(), H * 2);
      assert.lengthOf(
        h.rows(
          "SELECT * FROM candidates WHERE matcher = 'session_start' AND outcome = 'cut_budget'",
        ),
        1,
      );
    } finally {
      h.db.close();
    }
  });

  it("ordinary UTF-8 text preserves pointer headroom, with no run reset or exemption", () => {
    const h = make();
    try {
      for (let i = 0; i < 10; i++)
        h.fire("codex", "tool_result", {
          ordinary: [{ ...ordinary, entry: `rule${i}`, text: "é".repeat(800) }],
        });
      assert.isAtMost(h.used(), 8000 - H);
      const first = h.fire("codex", "user_message");
      assert.strictEqual(first.status, 200);
      h.state.settle(first.delivery, 100);
      h.db.prepare("UPDATE budgets SET serve_used = 8000 WHERE run_id = 'R'").run();
      assert.strictEqual(h.fire("codex", "session_start", { source: "compact" }).status, 204);
      assert.isTrue(h.state.greatLibraryOwed("codex", "S", 1));
      assert.lengthOf(
        h.rows(
          "SELECT * FROM candidates WHERE matcher = 'great_library' AND outcome = 'cut_budget' AND reason = 'turn'",
        ),
        1,
      );
      assert.strictEqual(h.used(), 8000);
    } finally {
      h.db.close();
    }
  });

  it("mixed OpenCode receipts independently own candidates, claims, refunds and replay", () => {
    const h = make();
    try {
      const mixed = h.fire("opencode", "user_message", {
        ordinary: [ordinary],
        delivery: "D",
        pointerDelivery: "P",
      });
      assert.strictEqual(mixed.status, 200);
      if (mixed.status !== 200) return;
      assert.deepStrictEqual(JSON.parse(mixed.body), {
        context: `${HEAD}\n\n${ordinary.text}`,
        pointer: TEXT,
      });
      assert.deepStrictEqual(h.state.replayed("D", "P"), { status: 200, body: mixed.body });
      assert.isFalse(h.state.settle("P", 100));
      assert.isFalse(h.state.releaseReservation("P"));
      assert.isTrue(h.state.settle("D", 100));
      assert.deepStrictEqual(
        h.rows("SELECT stage, outcome FROM candidates WHERE matcher = 'great_library'"),
        [{ stage: "reserved", outcome: "reserved" }],
      );
      const before = h.used();
      assert.isTrue(h.state.markPointerApplying("P", 100));
      assert.isTrue(h.state.markPointerApplying("P", 101));
      h.state.releaseStale(1000);
      assert.isFalse(h.state.greatLibraryOwed("opencode", "S", 0));
      assert.strictEqual(h.used(), before);
      assert.isTrue(h.state.releasePointer("P", "save_rejected"));
      assert.isTrue(h.state.releasePointer("P", "save_rejected"));
      assert.strictEqual(h.used(), before - H);
      assert.isTrue(h.state.greatLibraryOwed("opencode", "S", 0));
      assert.deepStrictEqual(h.rows("SELECT outcome FROM candidates WHERE matcher = 'intent'"), [
        { outcome: "injected" },
      ]);
      const next = h.fire("opencode", "user_message", { pointerDelivery: "P2" });
      assert.strictEqual(next.status, 200);
      assert.deepStrictEqual(
        h.rows("SELECT stage, status, serve_bytes FROM deliveries WHERE delivery = 'P2'"),
        [{ stage: "reserved", status: 200, serve_bytes: H }],
      );
      assert.isTrue(h.state.markPointerApplying("P2", 100));
      assert.isTrue(h.state.settlePointer("P2", 1000));
      assert.isTrue(h.state.settlePointer("P2", 1001));
      assert.isFalse(h.state.releasePointer("P2", "turn_ended"));
    } finally {
      h.db.close();
    }
  });

  it("requires a pointer header and keeps ordinary-only ownership unchanged", () => {
    const h = make();
    try {
      const reply = h.fire("opencode", "user_message", { ordinary: [ordinary] });
      assert.strictEqual(reply.status, 200);
      h.state.settle(reply.delivery, 100);
      assert.deepStrictEqual(
        h.rows("SELECT delivery, outcome FROM candidates WHERE matcher = 'intent'"),
        [{ delivery: null, outcome: "injected" }],
      );
      assert.deepStrictEqual(
        h.rows("SELECT outcome FROM candidates WHERE matcher = 'great_library'"),
        [{ outcome: "client_unsupported" }],
      );
      assert.isTrue(h.state.greatLibraryOwed("opencode", "S", 0));
      assert.strictEqual(h.used(), Buffer.byteLength(`${HEAD}\n\n${ordinary.text}`));
    } finally {
      h.db.close();
    }
  });
});

describe("Grok immutable installation accounting", () => {
  it.each(["live", "expired", "changed", "removed"] as const)(
    "binds and charges once after reopening: %s",
    (mode) => {
      const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "great-library-state-"));
      const file = NodePath.join(dir, "state.sqlite");
      let h = make(file);
      try {
        h.state.recordInstallation({
          ...installed,
          session: "G",
          threadId: "T",
          site: "rollback",
          now: 10,
        });
        h.db.close();
        h = make(file);
        const current =
          mode === "removed"
            ? null
            : mode === "changed"
              ? { ...installed, version: "changed" }
              : installed;
        const reservation = h.state.stageSessionInstructions({
          threadId: "T",
          runId: "R",
          delivery: "stage",
          current,
          now: 100,
        });
        h.state.releaseStale(102);
        h.state.releaseStale(219);
        if (reservation !== null)
          assert.deepStrictEqual(h.rows("SELECT stage FROM deliveries WHERE delivery = 'stage'"), [
            { stage: "reserved" },
          ]);
        if (mode === "expired") h.state.releaseStale(221);
        h.state.attachSessionInstructions({
          session: "G",
          threadId: "T",
          runId: "R",
          reservation,
          delivery: "attached",
          current,
          now: mode === "expired" ? 221 : 119,
        });
        assert.strictEqual(h.used(), H);
        assert.deepStrictEqual(h.rows("SELECT consumed_run FROM installations"), [
          { consumed_run: "R" },
        ]);
        assert.strictEqual(
          h.state.greatLibraryOwed("grok", "G", 0),
          mode === "changed" || mode === "removed",
        );
        assert.deepStrictEqual(
          h.rows("SELECT entry, version, bytes, outcome FROM candidates WHERE stage = 'settled'"),
          [
            {
              entry: pointer.entry,
              version: pointer.version,
              bytes: H,
              outcome: mode === "changed" || mode === "removed" ? "installed_stale" : "injected",
            },
          ],
        );
        h.state.attachSessionInstructions({
          session: "G",
          threadId: "T",
          runId: "R2",
          reservation: null,
          delivery: "again",
          current,
          now: 222,
        });
        assert.strictEqual(h.used(), H);
        const compact = h.fire("grok", "compaction", {
          session: "G",
          pointer:
            mode === "removed"
              ? null
              : mode === "changed"
                ? { ...pointer, version: "changed" }
                : pointer,
        });
        assert.strictEqual(compact.status, 204);
        if (mode !== "removed")
          assert.deepStrictEqual(h.rows("SELECT outcome, bytes FROM candidates WHERE bytes = 0"), [
            { outcome: mode === "changed" ? "retained_stale" : "retained", bytes: 0 },
          ]);
      } finally {
        h.db.close();
        NodeFS.rmSync(dir, { recursive: true });
      }
    },
  );

  it("reports a missing installation once per epoch and fences an exhausted actual head", () => {
    const h = make();
    try {
      for (let n = 0; n < 2; n++)
        h.state.attachSessionInstructions({
          session: "G",
          threadId: "T",
          runId: "R",
          reservation: null,
          delivery: `missing${n}`,
          current: installed,
          now: 100,
        });
      assert.lengthOf(h.rows("SELECT * FROM candidates WHERE outcome = 'not_installed'"), 1);
      h.state.recordInstallation({
        ...installed,
        session: "G",
        threadId: "T",
        site: "open",
        now: 100,
      });
      h.db.prepare("INSERT INTO budgets (run_id, serve_used) VALUES ('R', 8000)").run();
      assert.throws(
        () =>
          h.state.attachSessionInstructions({
            session: "G",
            threadId: "T",
            runId: "R",
            reservation: null,
            delivery: "fenced",
            current: null,
            now: 100,
          }),
        "serve budget",
      );
      assert.deepStrictEqual(h.rows("SELECT consumed_run FROM installations"), [
        { consumed_run: null },
      ]);
    } finally {
      h.db.close();
    }
  });
});

describe("Native fork evidence", () => {
  const claudeCompactions: ReadonlyArray<{
    name: string;
    hooks: ReadonlyArray<{ at: number; settle?: boolean; pointer?: false; source?: string }>;
    projections: ReadonlyArray<number>;
    boundaryAt?: number;
    reverted?: ReadonlyArray<string>;
    version?: string;
    expected: string;
  }> = [
    {
      name: "compact hook settles before stream ingestion",
      hooks: [{ at: 200, settle: true }],
      projections: [210],
      expected: "inherited",
    },
    { name: "missing compact hook", hooks: [], projections: [210], expected: "compacted" },
    {
      name: "unsettled compact hook",
      hooks: [{ at: 200 }],
      projections: [210],
      expected: "compacted",
    },
    {
      name: "compact hook without a pointer",
      hooks: [{ at: 200, pointer: false }],
      projections: [210],
      expected: "compacted",
    },
    {
      name: "two compact hooks precede both ingested boundaries",
      hooks: [
        { at: 200, settle: true },
        { at: 300, settle: true },
      ],
      projections: [310, 320],
      expected: "inherited",
    },
    {
      name: "missing second compact hook",
      hooks: [{ at: 200, settle: true }],
      projections: [210, 310],
      expected: "compacted",
    },
    {
      name: "unsettled second compact hook",
      hooks: [{ at: 200, settle: true }, { at: 300 }],
      projections: [210, 310],
      expected: "compacted",
    },
    {
      name: "second compact hook without a pointer",
      hooks: [
        { at: 200, settle: true },
        { at: 300, pointer: false },
      ],
      projections: [210, 310],
      expected: "compacted",
    },
    {
      name: "a clear source is not compact correlation",
      hooks: [{ at: 200, settle: true, source: "clear" }],
      projections: [210],
      expected: "compacted",
    },
    {
      name: "failed clear cannot borrow an earlier compact epoch",
      hooks: [
        { at: 200, settle: true },
        { at: 300, pointer: false, source: "clear" },
      ],
      projections: [210],
      expected: "compacted",
    },
    {
      name: "selected boundary before the later compact",
      hooks: [{ at: 200, settle: true }],
      projections: [210],
      boundaryAt: 130,
      expected: "inherited",
    },
    {
      name: "selected boundary includes only the first of two compacts",
      hooks: [
        { at: 200, settle: true },
        { at: 300, settle: true },
      ],
      projections: [210, 310],
      boundaryAt: 250,
      expected: "inherited",
    },
    {
      name: "removed actual compact emission",
      hooks: [{ at: 200, settle: true }],
      projections: [210],
      reverted: ["A0", "A1"],
      expected: "no_delivery",
    },
    {
      name: "rolled back compaction restores the earlier evidence",
      hooks: [{ at: 200, settle: true }],
      projections: [210],
      reverted: ["A1"],
      expected: "inherited",
    },
    {
      name: "compact re-delivery has a stale version",
      hooks: [{ at: 200, settle: true }],
      projections: [210],
      version: "new",
      expected: "stale_version",
    },
  ];
  it.each(claudeCompactions)("Claude fork: $name", (fixture) => {
    const h = make();
    try {
      const first = h.fire("claude", "user_message", { session: "A", run: "A0", now: 100 });
      h.state.settle(first.delivery, 100);
      for (const hook of fixture.hooks) {
        const compact = h.fire("claude", "session_start", {
          session: "A",
          run: "A1",
          source: hook.source ?? "compact",
          now: hook.at,
          ...(hook.pointer === false ? { pointer: null } : {}),
        });
        if (hook.settle) h.state.settle(compact.delivery, hook.at);
      }
      const boundaryAt = fixture.boundaryAt ?? 400;
      const reverted = new Set(fixture.reverted ?? []);
      const compactions = fixture.projections.map((startedAt) => ({ runId: "A1", startedAt }));
      const line =
        fixture.version === undefined ? pointer : { ...pointer, version: fixture.version };
      assert.strictEqual(
        h.state.recordNativeFork({
          harness: "claude",
          source: "A",
          target: "B",
          threadId: "B",
          runId: "B1",
          line,
          boundaryAt,
          compactions,
          reverted,
          now: 410,
          delivery: "fork-B",
        }),
        fixture.expected,
      );
      assert.strictEqual(
        h.fire("claude", "user_message", {
          session: "B",
          run: "B1",
          now: 420,
          pointer: line,
        }).status,
        fixture.expected === "inherited" ? 204 : 200,
      );
    } finally {
      h.db.close();
    }
  });

  it("opens old serving data without inventing compact-source provenance", () => {
    const h = make();
    try {
      const reply = h.fire("claude", "session_start", {
        source: "compact",
        session: "A",
        run: "A1",
        now: 200,
      });
      assert.strictEqual(reply.status, 200);
      h.state.settle(reply.delivery, 200);
      h.db.exec("ALTER TABLE moments DROP COLUMN source");
      const reopened = new ServingState(h.db);
      assert.strictEqual(h.text(reopened.replayed(reply.delivery)!), TEXT);
      assert.strictEqual(
        reopened.recordNativeFork({
          harness: "claude",
          source: "A",
          target: "B",
          threadId: "B",
          runId: "B1",
          line: pointer,
          boundaryAt: 230,
          compactions: [{ runId: "A1", startedAt: 210 }],
          reverted: new Set(),
          now: 240,
          delivery: "fork-B",
        }),
        "compacted",
      );
      assert.deepStrictEqual(h.rows("SELECT source FROM moments WHERE session = 'A'"), [
        { source: null },
      ]);
    } finally {
      h.db.close();
    }
  });

  it.each(["codex", "claude"] as const)(
    "%s keeps inherited prefix evidence after receiver rollback, while filtering removed deliveries",
    (harness) => {
      const h = make();
      try {
        const first = h.fire(harness, "user_message", { session: "A", run: "A1", now: 100 });
        h.state.settle(first.delivery, 100);
        const fork = (
          source: string,
          target: string,
          runId: string,
          now: number,
          extra: {
            compactedAt?: number;
            reverted?: Set<string>;
            boundaryAt?: number;
            line?: Candidate;
          } = {},
        ) =>
          h.state.recordNativeFork({
            harness,
            source,
            target,
            runId,
            threadId: target,
            line: extra.line ?? pointer,
            boundaryAt: extra.boundaryAt ?? now - 1,
            compactions:
              extra.compactedAt === undefined
                ? []
                : [{ runId: null, startedAt: extra.compactedAt }],
            reverted: extra.reverted ?? new Set(),
            now,
            delivery: `fork-${target}`,
          });
        assert.strictEqual(fork("A", "B", "B1", 140), "inherited");
        assert.strictEqual(
          h.fire(harness, "session_start", { session: "B", source: "resume", now: 145 }).status,
          204,
        );
        assert.strictEqual(
          h.fire(harness, "user_message", { session: "B", run: "B1", now: 150 }).status,
          204,
        );
        assert.strictEqual(fork("B", "C", "C1", 200, { reverted: new Set(["B1"]) }), "inherited");
        assert.strictEqual(
          h.fire(harness, "user_message", { session: "C", run: "C1", now: 210 }).status,
          204,
        );
        assert.strictEqual(
          fork("A", "removed", "D1", 240, { reverted: new Set(["A1"]) }),
          "no_delivery",
        );
        assert.strictEqual(fork("A", "boundary", "E1", 240, { boundaryAt: 99 }), "no_delivery");
        assert.strictEqual(
          fork("B", "compacted", "F1", 240, { compactedAt: 180, reverted: new Set(["B1"]) }),
          "compacted",
        );
        assert.strictEqual(
          fork("B", "stale", "G1", 240, { line: { ...pointer, version: "new" } }),
          "stale_version",
        );
        assert.strictEqual(fork("A", "B", "B2", 250), "skipped");
        assert.strictEqual(
          h.fire(harness, "session_start", { session: "C", source: "compact", now: 300 }).status,
          200,
        );
        assert.lengthOf(
          h.rows("SELECT * FROM candidates WHERE outcome = 'inherited' AND bytes = 0"),
          2,
        );
        assert.deepStrictEqual(
          h.rows("SELECT harness, session, epoch, settled FROM claims WHERE session = 'B'"),
          [{ harness, session: "B", epoch: 0, settled: 1 }],
        );
        assert.deepStrictEqual(
          h.rows("SELECT harness, stage, serve_bytes FROM deliveries WHERE delivery = 'fork-B'"),
          [{ harness, stage: "none", serve_bytes: 0 }],
        );
      } finally {
        h.db.close();
      }
    },
  );

  it.each(["codex", "claude"] as const)(
    "%s does not adopt another harness's claims or target moments for the same native ids",
    (harness) => {
      const h = make();
      try {
        const other = harness === "codex" ? "claude" : "codex";
        for (const session of ["A", "B"]) {
          const reply = h.fire(other, "user_message", { session, run: `other-${session}` });
          h.state.settle(reply.delivery, 100);
        }
        assert.strictEqual(
          h.state.recordNativeFork({
            harness,
            source: "A",
            target: "B",
            threadId: "B",
            runId: "B1",
            line: pointer,
            boundaryAt: 130,
            compactions: [],
            reverted: new Set(),
            now: 140,
            delivery: "fork-B",
          }),
          "no_delivery",
        );
        assert.deepStrictEqual(
          h.rows("SELECT harness, session FROM moments WHERE kind = 'native_fork'"),
          [{ harness, session: "B" }],
        );
        assert.strictEqual(
          h.fire(harness, "user_message", { session: "B", run: "B1", now: 150 }).status,
          200,
        );
      } finally {
        h.db.close();
      }
    },
  );

  it.each(["codex", "claude"] as const)(
    "%s does not inherit an unsettled source delivery",
    (harness) => {
      const h = make();
      try {
        assert.strictEqual(h.fire(harness, "user_message", { session: "A", now: 100 }).status, 200);
        assert.strictEqual(
          h.state.recordNativeFork({
            harness,
            source: "A",
            target: "B",
            threadId: "B",
            runId: "B1",
            line: pointer,
            boundaryAt: 130,
            compactions: [],
            reverted: new Set(),
            now: 140,
            delivery: "fork-B",
          }),
          "no_delivery",
        );
        assert.strictEqual(
          h.fire(harness, "user_message", { session: "B", run: "B1", now: 150 }).status,
          200,
        );
      } finally {
        h.db.close();
      }
    },
  );

  it.each(["codex", "claude"] as const)(
    "%s inherits a settled re-delivery after the supplied compaction cut",
    (harness) => {
      const h = make();
      try {
        const first = h.fire(harness, "user_message", { session: "A", run: "A1", now: 100 });
        h.state.settle(first.delivery, 100);
        const afterCompact = h.fire(harness, "session_start", {
          session: "A",
          source: "compact",
          run: "A2",
          now: 200,
        });
        assert.strictEqual(afterCompact.status, 200);
        h.state.settle(afterCompact.delivery, 200);
        assert.strictEqual(
          h.state.recordNativeFork({
            harness,
            source: "A",
            target: "B",
            threadId: "B",
            runId: "B1",
            line: pointer,
            boundaryAt: 230,
            compactions: [{ runId: "A2", startedAt: 180 }],
            reverted: new Set(),
            now: 240,
            delivery: "fork-B",
          }),
          "inherited",
        );
        assert.strictEqual(
          h.fire(harness, "user_message", { session: "B", run: "B1", now: 250 }).status,
          204,
        );
        const [record] = h.rows("SELECT extra_json FROM candidates WHERE outcome = 'inherited'");
        assert.strictEqual(
          JSON.parse(String(record!.extra_json)).source_delivery,
          afterCompact.delivery,
        );
      } finally {
        h.db.close();
      }
    },
  );
});
