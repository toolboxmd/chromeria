// @effect-diagnostics nodeBuiltinImport:off - tests inspect only their isolated serving database and public synthetic files.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it, vi } from "@effect/vitest";
import * as ByteSize from "effect/ByteSize";
import * as Clock from "effect/Clock";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/sql/SqlClient";

import * as ServerConfig from "../config.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ServerActivation from "../serverActivation.ts";
import * as ContextmdServing from "./ContextmdServing.ts";
import * as Shown from "./Shown.ts";
import { ServingState } from "./ServingState.ts";

const at = (n: number) => DateTime.formatIso(DateTime.makeUnsafe(n * 1000));
const jsonl = (rows: ReadonlyArray<unknown>) =>
  rows.map((r) => JSON.stringify(r)).join("\n") + "\n";
const claudeRows = (session: string, text: string, now: number, requests = true) => {
  const inner = `PostToolUse:Bash hook additional context: ${text}`;
  return [
    {
      type: "attachment",
      uuid: "public-attachment",
      sessionId: session,
      timestamp: at(now),
      isSidechain: false,
      attachment: {
        type: "hook_additional_context",
        hookEvent: "PostToolUse",
        hookName: "PostToolUse:Bash",
        toolUseID: "tool-one",
        content: [text],
      },
      rendered: [{ content: `<system-reminder>\n${inner}\n</system-reminder>` }],
    },
    ...(requests
      ? [
          {
            type: "api-request-blob",
            sessionId: session,
            hash: "public-hash",
            message: { role: "system", content: inner },
          },
          {
            type: "api-request",
            sessionId: session,
            id: "public-request",
            version: "2.1.299",
            timestamp: at(now + 0.01),
            keep: 0,
            tail: ["public-hash"],
            params: { betas: [] },
          },
        ]
      : []),
    {
      type: "assistant",
      uuid: "public-response",
      sessionId: session,
      timestamp: at(now + 0.02),
      isSidechain: false,
      ...(requests ? { requestRef: "public-request" } : {}),
      message: {
        id: "public-message",
        model: "claude-opus-5-5",
        content: [{ type: "text", text: "done" }],
      },
    },
  ];
};
const codexRows = (session: string, text: string, now: number, consuming = true) => [
  {
    type: "session_meta",
    timestamp: at(now - 1),
    payload: { id: session, cli_version: "0.162.0" },
  },
  { type: "turn_context", timestamp: at(now - 0.5), payload: { turn_id: "native-turn" } },
  {
    type: "event_msg",
    timestamp: at(now - 0.1),
    payload: { type: "item_completed", turn_id: "native-turn", item: { id: "tool-one" } },
  },
  {
    type: "response_item",
    timestamp: at(now),
    payload: {
      type: "message",
      role: "developer",
      id: "hook-input",
      internal_chat_message_metadata_passthrough: {
        turn_id: "native-turn",
        content_item_kinds: ["hooks.additional_context"],
      },
      content: [{ type: "input_text", text }],
    },
  },
  ...(consuming
    ? [
        {
          type: "response_item",
          timestamp: at(now + 0.02),
          payload: { type: "message", role: "assistant", id: "consumed-output", content: [] },
        },
      ]
    : []),
];
const decodeBody = Schema.decodeUnknownSync(
  Schema.fromJsonString(
    Schema.Struct({
      hookSpecificOutput: Schema.Struct({ additionalContext: Schema.String }),
    }),
  ),
);
const decodeEvidence = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
);
interface Fact {
  readonly shown: string | null;
  readonly shown_json: string | null;
  readonly shown_next: number | null;
}

const makeHarness = (
  overrideFs: (fs: FileSystem.FileSystem) => FileSystem.FileSystem = (fs) => fs,
) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const cfgContext = yield* Layer.build(
      ServerConfig.layerTest(process.cwd(), { prefix: "contextmd-shown-test-" }),
    );
    const config = Context.get(cfgContext, ServerConfig.ServerConfig);
    const keys = NodePath.join(config.baseDir, "keys");
    NodeFS.mkdirSync(keys);
    NodeFS.writeFileSync(
      NodePath.join(keys, "public.md"),
      "---\nid: public-rule\ncommand: echo public-rule\n---\nA public rule.\n",
    );
    const configPath = NodePath.join(config.baseDir, "serving.json");
    NodeFS.writeFileSync(configPath, JSON.stringify({ exact_keys: [keys] }));
    const database = yield* Layer.build(SqlitePersistence.layerMemory);
    const sql = Context.get(database, SqlClient.SqlClient);
    const services = yield* Layer.build(
      ContextmdServing.layer.pipe(
        Layer.provide(Layer.succeedContext(cfgContext)),
        Layer.provide(Layer.succeedContext(database)),
        Layer.provide(Layer.succeed(FileSystem.FileSystem, overrideFs(fs))),
        Layer.provide(
          ConfigProvider.layer(
            ConfigProvider.fromUnknown({ CONTEXTMD_SERVING_CONFIG: configPath }),
          ),
        ),
        Layer.provide(Layer.succeed(ServerActivation.ServerActivation, Effect.never)),
      ),
    );
    const service = Context.get(services, ContextmdServing.ContextmdServing);
    const db = new NodeSqlite.DatabaseSync(NodePath.join(config.stateDir, "contextmd.sqlite"));
    yield* Effect.addFinalizer(() => Effect.sync(() => db.close()));
    let count = 0;
    const moment = Effect.fnUntraced(function* (
      harness = "claude",
      session = "public-session",
      extra: Record<string, unknown> = {},
    ) {
      count += 1;
      const delivery = `public-delivery-${count}`;
      yield* sql`INSERT OR IGNORE INTO orchestration_v2_projection_run_attempts (
        attempt_id, thread_id, run_id, attempt_ordinal, root_node_id, provider, provider_instance_id, provider_thread_id, status, payload_json
      ) VALUES (${`attempt-${session}`}, ${`thread-${session}`}, ${`run-${session}`}, 1, 'node', 'claudeAgent', 'claudeAgent', 'provider-thread', 'running',
        ${JSON.stringify({ nativeThreadId: session })})`;
      const raw =
        harness === "opencode"
          ? {
              hook: "tool.execute.after",
              directory: config.baseDir,
              input: { sessionID: session, callID: "tool-one", tool: "bash" },
              output: { args: { command: "echo public-rule" }, output: "", metadata: { exit: 0 } },
              ...extra,
            }
          : {
              hook_event_name: "PostToolUse",
              session_id: session,
              cwd: config.baseDir,
              tool_name: harness === "grok" ? "run_terminal_command" : "Bash",
              tool_use_id: "tool-one",
              tool_input: { command: "echo public-rule" },
              toolResult: { exit_code: 0, output_for_prompt: "" },
              ...extra,
            };
      const now = (yield* Clock.currentTimeMillis) / 1000;
      const reply = yield* service.moment({
        authorized: Effect.succeed(true),
        harness,
        delivery,
        contentLength: undefined,
        body: Effect.succeed(JSON.stringify(raw)),
      });
      return { delivery, reply, now };
    });
    const fact = (delivery: string): Fact | undefined =>
      db
        .prepare("SELECT shown, shown_json, shown_next FROM deliveries WHERE delivery = ?")
        .get(delivery) as Fact | undefined;
    const evidence = (delivery: string) => decodeEvidence(fact(delivery)!.shown_json!);
    const seed = (delivery: string, session: string, path: string, now = 0) => {
      const m = db
        .prepare(
          "INSERT INTO moments (received_at, harness, kind, session, tool_use_id, status, delivery) VALUES (?, 'claude', 'tool_result', ?, 'tool-one', 'ok', ?)",
        )
        .run(now, session, delivery);
      const body = JSON.stringify({
        hookSpecificOutput: { hookEventName: "PostToolUse", additionalContext: "A public rule." },
      });
      db.prepare(`INSERT INTO deliveries (delivery, harness, moment_id, stage, status, body, created_at, expires_at, settled_at,
        transcript_path, shown, shown_json, shown_next) VALUES (?, 'claude', ?, 'settled', 200, ?, ?, ?, ?, ?, 'unknown', ?, ?)`).run(
        delivery,
        m.lastInsertRowid,
        body,
        now,
        now + 2,
        now,
        path,
        '{"v":1,"source":null,"reason":"not_checked"}',
        now,
      );
    };
    return { service, db, config, moment, fact, evidence, seed };
  });
const withNode = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.provide(NodeServices.layer));

describe("ContextMD shown service and harness seams", () => {
  it.effect.each(["claude", "codex"])(
    "records native consumption for %s after settle, never from hook return",
    (harness) =>
      withNode(
        Effect.gen(function* () {
          const h = yield* makeHarness();
          const session = "native-positive";
          const path = NodePath.join(
            h.config.baseDir,
            harness === "claude" ? `${session}.jsonl` : `rollout-date-${session}.jsonl`,
          );
          const d = yield* h.moment(harness, session, {
            transcript_path: path,
            turn_id: "native-turn",
          });
          assert.equal(d.reply.status, 200);
          if (d.reply.status !== 200) return;
          assert.isNull(h.fact(d.delivery)!.shown);
          const text = decodeBody(d.reply.body).hookSpecificOutput.additionalContext;
          NodeFS.writeFileSync(
            path,
            jsonl(
              harness === "claude"
                ? claudeRows(session, text, d.now)
                : codexRows(session, text, d.now),
            ) + '{"type":"unfinished',
          );
          yield* h.service.settle(d.delivery);
          assert.equal(h.fact(d.delivery)!.shown, "unknown");
          yield* TestClock.adjust("1 second");
          yield* h.service.checkShown;
          assert.equal(h.fact(d.delivery)!.shown, "shown");
          assert.isNull(h.fact(d.delivery)!.shown_next);
          const ev = h.evidence(d.delivery);
          assert.equal(ev.v, 1);
          assert.equal(ev.source, harness === "claude" ? "claude_request_replay" : "codex_rollout");
          assert.isNumber(ev.checked_at);
          assert.notInclude(h.fact(d.delivery)!.shown_json!, text);
          assert.notInclude(h.fact(d.delivery)!.shown_json!, path);
        }),
      ),
  );

  it.effect("promotes a waiting Codex row after completed output is appended", () =>
    withNode(
      Effect.gen(function* () {
        const h = yield* makeHarness();
        const session = "codex-waiting";
        const path = NodePath.join(h.config.baseDir, `rollout-date-${session}.jsonl`);
        const d = yield* h.moment("codex", session, {
          transcript_path: path,
          turn_id: "native-turn",
        });
        if (d.reply.status !== 200) return assert.fail("expected context reply");
        const text = decodeBody(d.reply.body).hookSpecificOutput.additionalContext;
        NodeFS.writeFileSync(path, jsonl(codexRows(session, text, d.now, false)));
        yield* h.service.settle(d.delivery);
        yield* h.service.checkShown;
        assert.equal(h.evidence(d.delivery).reason, "no_model_response");
        NodeFS.appendFileSync(path, jsonl(codexRows(session, text, d.now).slice(-1)));
        yield* TestClock.adjust("31 seconds");
        yield* h.service.checkShown;
        assert.equal(h.fact(d.delivery)!.shown, "shown");
      }),
    ),
  );

  it.effect("a Codex send confirmation waits for actual consuming model output", () =>
    withNode(
      Effect.gen(function* () {
        const h = yield* makeHarness();
        const session = "codex-retained-send";
        const path = NodePath.join(h.config.baseDir, `rollout-date-${session}.jsonl`);
        const d = yield* h.moment("codex", session, {
          transcript_path: path,
          turn_id: "native-turn",
        });
        if (d.reply.status !== 200) return assert.fail("expected context reply");
        const text = decodeBody(d.reply.body).hookSpecificOutput.additionalContext;
        NodeFS.writeFileSync(
          path,
          jsonl([
            ...codexRows(session, text, d.now, false),
            {
              type: "response_item",
              timestamp: at(d.now + 0.02),
              payload: {
                type: "message",
                role: "assistant",
                id: "send-confirmation",
                internal_chat_message_metadata_passthrough: { turn_id: "native-turn" },
                content: [{ type: "output_text", text: "The synthetic job is done." }],
              },
              metadata: {
                delivered_assistant_message: "codex:code-mode-delivery:v1:complete",
                user_input_order: 7,
              },
            },
          ]),
        );
        yield* h.service.settle(d.delivery);
        yield* TestClock.adjust("1 second");
        yield* h.service.checkShown;
        assert.equal(h.fact(d.delivery)!.shown, "unknown");
        assert.equal(h.evidence(d.delivery).reason, "no_model_response");
        NodeFS.appendFileSync(
          path,
          jsonl([
            {
              type: "response_item",
              timestamp: at(d.now + 1.01),
              payload: { type: "reasoning", id: "actual-model-output" },
            },
          ]),
        );
        yield* TestClock.adjust("31 seconds");
        yield* h.service.checkShown;
        assert.equal(h.fact(d.delivery)!.shown, "shown");
        const ev = h.evidence(d.delivery);
        assert.deepEqual(ev.consumed, { id: "actual-model-output", call: null, at: d.now + 1.01 });
      }),
    ),
  );

  it.effect("rendered-only Claude context remains unproven without request records", () =>
    withNode(
      Effect.gen(function* () {
        const h = yield* makeHarness();
        const session = "claude-unrecorded";
        const path = NodePath.join(h.config.baseDir, `${session}.jsonl`);
        const d = yield* h.moment("claude", session, { transcript_path: path });
        if (d.reply.status !== 200) return assert.fail("expected context reply");
        const text = decodeBody(d.reply.body).hookSpecificOutput.additionalContext;
        NodeFS.writeFileSync(path, jsonl(claudeRows(session, text, d.now, false)));
        yield* h.service.settle(d.delivery);
        yield* TestClock.adjust("1 second");
        yield* h.service.checkShown;
        assert.equal(h.evidence(d.delivery).reason, "final_input_unrecorded");
        assert.equal(h.fact(d.delivery)!.shown, "unknown");
      }),
    ),
  );

  it.effect.each(["grok", "opencode"])(
    "records no_evidence_source without opening a transcript for %s",
    (harness) =>
      withNode(
        Effect.gen(function* () {
          const h = yield* makeHarness();
          let d = yield* h.moment(harness);
          if (harness === "grok") {
            assert.equal(d.reply.status, 204);
            d = yield* h.moment(harness, "public-session", { hook_event_name: "PreToolUse" });
          }
          assert.equal(d.reply.status, 200);
          yield* h.service.settle(d.delivery);
          yield* h.service.checkShown;
          assert.equal(h.fact(d.delivery)!.shown, "unknown");
          assert.equal(h.evidence(d.delivery).reason, "no_evidence_source");
          assert.isNull(h.evidence(d.delivery).source);
          assert.isNull(h.fact(d.delivery)!.shown_next);
        }),
      ),
  );

  it.effect.each(["cursor", "antigravity"])(
    "does not create a delivery for unsupported %s",
    (harness) =>
      withNode(
        Effect.gen(function* () {
          const h = yield* makeHarness();
          const d = yield* h.moment(harness);
          assert.equal(d.reply.status, 204);
          assert.isUndefined(h.fact(d.delivery));
          assert.equal(
            (
              h.db.prepare("SELECT status FROM moments ORDER BY id DESC LIMIT 1").get() as {
                status: string;
              }
            ).status,
            "bad_event",
          );
        }),
      ),
  );
});

describe("ContextMD bounded sweep and recovery", () => {
  it.effect(
    "rule failure after a complete 40 MiB read remains charged, with no reusable stamp",
    () =>
      withNode(
        Effect.gen(function* () {
          const reads: string[] = [];
          const large = 40 * 1024 * 1024;
          const h = yield* makeHarness((fs) => ({
            ...fs,
            stat: (path) =>
              fs
                .stat(path)
                .pipe(
                  Effect.map((stat) =>
                    path.endsWith(".jsonl") ? { ...stat, size: ByteSize.bytes(large) } : stat,
                  ),
                ),
            stream: (path) => {
              reads.push(NodePath.basename(path));
              const row = JSON.stringify({
                type: "other",
                sessionId: NodePath.basename(path, ".jsonl"),
              });
              const bytes = new TextEncoder().encode(
                row + " ".repeat(large - row.length - 1) + "\n",
              );
              return Stream.fromIterable(
                Array.from({ length: 40 }, (_, i) =>
                  bytes.subarray(i * 1024 * 1024, (i + 1) * 1024 * 1024),
                ),
              );
            },
          }));
          for (const session of ["rule-one", "rule-two"]) {
            const path = NodePath.join(h.config.baseDir, `${session}.jsonl`);
            NodeFS.writeFileSync(path, "public placeholder");
            h.seed(session, session, path);
          }
          const spy = vi.spyOn(Shown, "claudeEvidence").mockImplementationOnce(() => {
            throw new Error("public rule failure");
          });
          yield* Effect.addFinalizer(() => Effect.sync(() => spy.mockRestore()));
          yield* h.service.checkShown;
          assert.deepEqual(reads, ["rule-one.jsonl"]);
          assert.equal(h.evidence("rule-one").reason, "unrecognized_format");
          assert.notProperty(h.evidence("rule-one"), "seen");
          assert.equal(h.fact("rule-two")!.shown_next, 0);
          yield* h.service.checkShown;
          assert.deepEqual(reads, ["rule-one.jsonl", "rule-two.jsonl"]);
        }),
      ),
  );

  it.effect(
    "charges complete size before failed reads and rotates unread groups to the next tick",
    () =>
      withNode(
        Effect.gen(function* () {
          const reads: string[] = [];
          const large = 40 * 1024 * 1024;
          const h = yield* makeHarness((fs) => ({
            ...fs,
            stat: (path) =>
              fs
                .stat(path)
                .pipe(
                  Effect.map((stat) =>
                    path.endsWith(".jsonl") ? { ...stat, size: ByteSize.bytes(large) } : stat,
                  ),
                ),
            stream: (path) => {
              reads.push(NodePath.basename(path));
              return Stream.concat(
                Stream.fromIterable(
                  Array.from({ length: 40 }, () => new Uint8Array(1024 * 1024).fill(32)),
                ),
                Stream.die("public read failure"),
              );
            },
          }));
          for (const session of ["large-one", "large-two"]) {
            const path = NodePath.join(h.config.baseDir, `${session}.jsonl`);
            NodeFS.writeFileSync(path, "public placeholder");
            h.seed(session, session, path);
          }
          yield* h.service.checkShown;
          assert.deepEqual(reads, ["large-one.jsonl"]);
          assert.equal(h.evidence("large-one").reason, "transcript_unreadable");
          assert.notProperty(h.evidence("large-one"), "seen");
          assert.equal(h.fact("large-two")!.shown_next, 0);
          yield* h.service.checkShown;
          assert.deepEqual(reads, ["large-one.jsonl", "large-two.jsonl"]);
        }),
      ),
  );

  it.effect("a full 64 MiB reservation permits no further read even when it fails", () =>
    withNode(
      Effect.gen(function* () {
        const reads: string[] = [];
        const h = yield* makeHarness((fs) => ({
          ...fs,
          stat: (path) =>
            fs.stat(path).pipe(
              Effect.map((stat) =>
                path.endsWith(".jsonl")
                  ? {
                      ...stat,
                      size: ByteSize.bytes(path.endsWith("full.jsonl") ? 64 * 1024 * 1024 : 1),
                    }
                  : stat,
              ),
            ),
          stream: (path) => {
            reads.push(NodePath.basename(path));
            return Stream.die("public read failure");
          },
        }));
        for (const session of ["full", "next"]) {
          const path = NodePath.join(h.config.baseDir, `${session}.jsonl`);
          NodeFS.writeFileSync(path, "x");
          h.seed(session, session, path);
        }
        yield* h.service.checkShown;
        assert.deepEqual(reads, ["full.jsonl"]);
        assert.equal(h.fact("next")!.shown_next, 0);
      }),
    ),
  );

  it.effect("short reads and missing files defer without seen and do not block another group", () =>
    withNode(
      Effect.gen(function* () {
        const h = yield* makeHarness((fs) => ({
          ...fs,
          stat: (path) =>
            fs
              .stat(path)
              .pipe(
                Effect.map((stat) =>
                  path.endsWith("short.jsonl") ? { ...stat, size: ByteSize.bytes(100) } : stat,
                ),
              ),
        }));
        for (const session of ["short", "missing", "ordinary"]) {
          const path = NodePath.join(h.config.baseDir, `${session}.jsonl`);
          if (session !== "missing")
            NodeFS.writeFileSync(path, jsonl([{ type: "other", sessionId: session }]));
          h.seed(session, session, path);
        }
        yield* h.service.checkShown;
        for (const session of ["short", "missing"]) {
          assert.equal(h.evidence(session).reason, "transcript_unreadable");
          assert.notProperty(h.evidence(session), "seen");
          assert.equal(h.fact(session)!.shown_next, 30);
        }
        assert.equal(h.evidence("ordinary").reason, "not_found");
      }),
    ),
  );

  it.effect("a new row and a changed rule revision are checked even on an unchanged file", () =>
    withNode(
      Effect.gen(function* () {
        const reads: string[] = [];
        const h = yield* makeHarness((fs) => ({
          ...fs,
          stream: (path, options) => {
            reads.push(path);
            return fs.stream(path, options);
          },
        }));
        const path = NodePath.join(h.config.baseDir, "same.jsonl");
        NodeFS.writeFileSync(path, jsonl([{ type: "other", sessionId: "same" }]));
        h.seed("first", "same", path);
        yield* h.service.checkShown;
        assert.equal(reads.length, 1);
        yield* TestClock.adjust("31 seconds");
        yield* h.service.checkShown;
        assert.equal(reads.length, 1);
        h.seed("new", "same", path);
        yield* h.service.checkShown;
        assert.equal(reads.length, 2);
        const previous = h.evidence("new");
        h.db
          .prepare("UPDATE deliveries SET shown_next = 0, shown_json = ? WHERE delivery = 'new'")
          .run(
            JSON.stringify({
              ...previous,
              seen: String(previous.seen).replace("claude-1", "claude-0"),
            }),
          );
        yield* h.service.checkShown;
        assert.equal(reads.length, 3);
      }),
    ),
  );

  it.effect(
    "rule exceptions omit seen, retry unchanged files, and cannot requeue a shown row",
    () =>
      withNode(
        Effect.gen(function* () {
          const h = yield* makeHarness();
          const session = "repaired";
          const path = NodePath.join(h.config.baseDir, `${session}.jsonl`);
          NodeFS.writeFileSync(path, jsonl(claudeRows(session, "A public rule.", 0)));
          h.seed("one", session, path);
          h.seed("two", session, path);
          // Removing the competing reply makes one row provable, while a later rule throws.
          h.db.prepare("UPDATE deliveries SET body = '{}' WHERE delivery = 'two'").run();
          yield* TestClock.adjust("1 second");
          const original = Shown.claudeEvidence;
          const spy = vi.spyOn(Shown, "claudeEvidence").mockImplementationOnce(() => {
            throw new Error("public rule failure");
          });
          yield* Effect.addFinalizer(() => Effect.sync(() => spy.mockRestore()));
          yield* h.service.checkShown;
          assert.notProperty(h.evidence("one"), "seen");
          assert.equal(h.fact("one")!.shown_next, 31);
          spy.mockImplementation(original);
          yield* TestClock.adjust("31 seconds");
          yield* h.service.checkShown;
          assert.equal(h.fact("one")!.shown, "shown");
          // Group error recovery is unknown-only even if persistence fails after a positive write.
          h.seed("three", session, path);
          const record = vi.spyOn(Shown, "served").mockImplementationOnce(() => {
            throw new Error("public group failure");
          });
          yield* h.service.checkShown;
          record.mockRestore();
          assert.equal(h.fact("one")!.shown, "shown");
          assert.isNull(h.fact("one")!.shown_next);
        }),
      ),
  );

  it.effect(
    "a group failure after a positive write leaves that row terminal and clears other observations",
    () =>
      withNode(
        Effect.gen(function* () {
          const h = yield* makeHarness();
          const session = "partial-write";
          const path = NodePath.join(h.config.baseDir, `${session}.jsonl`);
          NodeFS.writeFileSync(path, jsonl(claudeRows(session, "A public rule.", 0)));
          h.seed("proven", session, path);
          h.seed("unwritten", session, path);
          h.db.prepare("UPDATE deliveries SET body = ? WHERE delivery = 'unwritten'").run(
            JSON.stringify({
              hookSpecificOutput: {
                hookEventName: "PostToolUse",
                additionalContext: "A different public rule.",
              },
            }),
          );
          const original = ServingState.prototype.recordShown;
          const spy = vi.spyOn(ServingState.prototype, "recordShown").mockImplementation(function (
            this: ServingState,
            delivery,
            fact,
            json,
            next,
          ) {
            const result = original.call(this, delivery, fact, json, next);
            if (delivery === "proven" && fact === "shown")
              throw new Error("public post-write failure");
            return result;
          });
          yield* Effect.addFinalizer(() => Effect.sync(() => spy.mockRestore()));
          yield* TestClock.adjust("1 second");
          yield* h.service.checkShown;
          assert.equal(h.fact("proven")!.shown, "shown");
          assert.isNull(h.fact("proven")!.shown_next);
          assert.equal(h.evidence("unwritten").reason, "unrecognized_format");
          assert.notProperty(h.evidence("unwritten"), "seen");
          assert.equal(h.fact("unwritten")!.shown_next, 31);
          spy.mockRestore();
          yield* TestClock.adjust("31 seconds");
          yield* h.service.checkShown;
          assert.equal(h.fact("proven")!.shown, "shown");
          assert.isNull(h.fact("proven")!.shown_next);
        }),
      ),
  );

  it.effect("50 waiting rows do not starve the next provable row", () =>
    withNode(
      Effect.gen(function* () {
        const h = yield* makeHarness();
        const waiting = NodePath.join(h.config.baseDir, "waiting.jsonl");
        NodeFS.writeFileSync(waiting, jsonl([{ type: "other", sessionId: "waiting" }]));
        for (let i = 0; i < 50; i++) h.seed(`wait-${i}`, "waiting", waiting);
        const ready = NodePath.join(h.config.baseDir, "ready.jsonl");
        NodeFS.writeFileSync(ready, jsonl(claudeRows("ready", "A public rule.", 0)));
        h.seed("ready", "ready", ready);
        yield* TestClock.adjust("1 second");
        yield* h.service.checkShown;
        assert.equal(h.evidence("ready").reason, "not_checked");
        yield* h.service.checkShown;
        assert.equal(h.fact("ready")!.shown, "shown");
      }),
    ),
  );
});
