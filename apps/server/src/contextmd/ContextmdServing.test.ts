// @effect-diagnostics nodeBuiltinImport:off - tests use isolated databases, public files and a loopback Cue fixture.
import * as NodeFS from "node:fs";
import * as NodeHttp from "node:http";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";

import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { AuthSessionId } from "@t3tools/contracts";
import { assert, describe, it, vi } from "@effect/vitest";
import * as ByteSize from "effect/ByteSize";
import * as Clock from "effect/Clock";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/sql/SqlClient";
import type * as SqlStatement from "effect/sql/Statement";

import * as ServerConfig from "../config.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as AuthSessions from "../persistence/AuthSessions.ts";
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
const decodePointerBody = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Struct({ pointer: Schema.String })),
);
const decodeEvidence = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
);
const decodeCueRequest = Schema.decodeUnknownSync(
  Schema.fromJsonString(
    Schema.Struct({
      entries: Schema.Array(Schema.Struct({ id: Schema.String, doc: Schema.String })),
    }),
  ),
);
interface Fact {
  readonly shown: string | null;
  readonly shown_json: string | null;
  readonly shown_next: number | null;
}

const makeHarness = (
  overrideFs: (fs: FileSystem.FileSystem) => FileSystem.FileSystem = (fs) => fs,
  disk = false,
  onRunLookup: (rows: ReadonlyArray<unknown>, session: string) => Effect.Effect<void> = () =>
    Effect.void,
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
    const database = yield* Layer.build(
      disk ? SqlitePersistence.layerFromPath(config.dbPath) : SqlitePersistence.layerMemory,
    );
    const sql = Context.get(database, SqlClient.SqlClient);
    const observedSql = new Proxy(sql, {
      apply(target, thisArg, args) {
        const statement: SqlStatement.Statement<unknown> = Reflect.apply(target, thisArg, args);
        return String(args[0]?.[0]).includes("SELECT DISTINCT run_id, thread_id")
          ? statement.pipe(Effect.tap((rows) => onRunLookup(rows, String(args[1]))))
          : statement;
      },
    });
    const services = yield* Layer.build(
      ContextmdServing.layer.pipe(
        Layer.provide(Layer.succeedContext(cfgContext)),
        Layer.provide(Layer.succeed(SqlClient.SqlClient, observedSql)),
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
      register = true,
    ) {
      count += 1;
      const delivery = `public-delivery-${count}`;
      if (register)
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
    return { service, db, sql, config, configPath, moment, fact, evidence, seed };
  });
const withNode = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.provide(NodeServices.layer));
const decodeDeadline = Schema.decodeUnknownSync(
  Schema.fromJsonString(
    Schema.Struct({
      phase: Schema.String,
      active_ms: Schema.Number,
      completed_ms: Schema.Record(Schema.String, Schema.Number),
    }),
  ),
);

const startupHarness = (
  overrideFs: (fs: FileSystem.FileSystem) => FileSystem.FileSystem = (fs) => fs,
) =>
  Effect.gen(function* () {
    const missing = yield* Queue.unbounded<string>();
    const lookups = new Map<string, number>();
    const h = yield* makeHarness(overrideFs, false, (rows, session) => {
      const count = (lookups.get(session) ?? 0) + 1;
      lookups.set(session, count);
      // Retention has been installed before this second, still-unmatched SQL read.
      return count === 2 && rows.length === 0
        ? Queue.offer(missing, session).pipe(Effect.asVoid)
        : Effect.void;
    });
    const start = NodePath.join(h.config.baseDir, "session-start.md");
    const index = NodePath.join(h.config.baseDir, "library-index.md");
    NodeFS.writeFileSync(start, "Public startup instructions.\n");
    NodeFS.writeFileSync(index, "# Public Library\n");
    NodeFS.writeFileSync(
      h.configPath,
      JSON.stringify({
        exact_keys: [],
        great_library: index,
        session_start: [{ path: start, max_chars: 100 }],
      }),
    );
    yield* h.sql`INSERT INTO orchestration_v2_projection_runs
    (run_id, thread_id, ordinal, provider, status, requested_at, payload_json)
    VALUES ('startup-run', 'startup-thread', 1, 'custom-instance', 'starting', 'now', '{}')`;
    const request = (
      harness: string,
      event: string,
      session = "startup-session",
      delivery = "first-startup",
      extra = {},
    ) =>
      h.service.moment({
        authorized: Effect.succeed(true),
        harness,
        delivery,
        contentLength: undefined,
        body: Effect.succeed(
          JSON.stringify({
            hook_event_name: event,
            source: "startup",
            session_id: session,
            cwd: h.config.baseDir,
            ...extra,
          }),
        ),
      });
    const persist = (
      session = "startup-session",
      runId = "startup-run",
      status = "running",
      ordinal = 1,
    ) =>
      h.sql`INSERT INTO orchestration_v2_projection_run_attempts
      (attempt_id, thread_id, run_id, attempt_ordinal, root_node_id, provider,
        provider_instance_id, provider_thread_id, status, payload_json)
      VALUES (${`attempt-${runId}-${ordinal}`}, ${`thread-${runId}`}, ${runId}, ${ordinal}, 'node', 'custom-instance',
        'custom-instance', 'provider-thread', ${status}, ${JSON.stringify({ nativeThreadId: session })})`;
    const joined = (delivery = "first-startup") =>
      h.db
        .prepare("SELECT status, run_id FROM moments WHERE delivery = ? ORDER BY id DESC LIMIT 1")
        .get(delivery);
    return { ...h, missing, request, persist, joined, index, lookups };
  });

describe("ContextMD startup run registration", () => {
  it.effect.each([
    { harness: "claude", event: "SessionStart", late: false },
    { harness: "claude", event: "UserPromptSubmit", late: false },
    { harness: "grok", event: "SessionStart", late: false },
    { harness: "claude", event: "SessionStart", late: true },
    { harness: "claude", event: "UserPromptSubmit", late: true },
    { harness: "grok", event: "SessionStart", late: true },
    { harness: "claude", event: "SessionStart", late: true, nextEvent: "PostToolUse" },
    { harness: "claude", event: "SessionStart", late: true, nextEvent: "UserPromptSubmit" },
    { harness: "claude", event: "SessionStart", late: true, nextEvent: "SessionStart" },
    { harness: "claude", event: "SessionStart", late: true, nextEvent: "Stop" },
    {
      harness: "claude",
      event: "UserPromptSubmit",
      late: true,
      nextEvent: "SessionStart",
      nextSource: "compact",
    },
  ])(
    "$harness $event joins on registration (over deadline: $late, next: $nextEvent)",
    ({ harness, event, late, nextEvent, nextSource }) =>
      withNode(
        Effect.gen(function* () {
          const h = yield* startupHarness();
          const pending = yield* Effect.forkScoped(h.request(harness, event));
          assert.equal(yield* Queue.take(h.missing), "startup-session");
          if (late) {
            yield* TestClock.adjust("1300 millis");
            assert.equal((yield* Fiber.join(pending)).status, 204);
          }
          yield* h.persist();
          yield* h.sql`UPDATE orchestration_v2_projection_runs SET status = 'running' WHERE run_id = 'startup-run'`;
          yield* h.service.nativeSessionRegistered({
            nativeSessionId: "startup-session",
            runId: "startup-run",
          });
          const reply = yield* Fiber.join(pending);
          assert.equal(reply.status, 204);
          if (harness === "claude") {
            const next = yield* h.moment(
              harness,
              "startup-session",
              { hook_event_name: nextEvent ?? "PreToolUse", source: nextSource },
              false,
            );
            assert.equal(next.reply.status, 200);
            if (next.reply.status === 200)
              assert.include(
                next.reply.body,
                event === "SessionStart" ? "Public startup instructions." : h.index,
              );
            if (nextSource === "compact" && next.reply.status === 200)
              assert.equal(
                decodeBody(next.reply.body).hookSpecificOutput.additionalContext.split(h.index)
                  .length - 1,
                1,
              );
            yield* h.service.release(next.delivery);
            const retry = yield* h.moment(
              harness,
              "startup-session",
              { hook_event_name: "PreToolUse" },
              false,
            );
            assert.equal(retry.reply.status, 200);
            yield* h.service.settle(retry.delivery);
            const again = yield* h.moment(
              harness,
              "startup-session",
              { hook_event_name: "PreToolUse" },
              false,
            );
            assert.equal(again.reply.status, 204);
          }
          if (harness === "grok") {
            // SessionStart is a non-delivering Grok seam. A later normal hook still joins.
            const next = yield* h.moment(harness, "startup-session", {}, false);
            assert.equal(next.reply.status, 204);
            const row = h.db
              .prepare("SELECT status, run_id FROM moments WHERE delivery = ?")
              .get(next.delivery);
            assert.deepEqual(row, { status: "ok", run_id: "startup-run" });
            assert.equal(h.db.prepare("SELECT count(*) AS n FROM carries").get()!.n, 0);
          }
          assert.deepEqual(h.joined(), { status: "ok", run_id: "startup-run" });
        }).pipe(Effect.scoped),
      ),
  );

  it.effect.each(["UserPromptSubmit", "PreToolUse"])(
    "delivers a retained large Claude startup file through %s at its original limit",
    (nextEvent) =>
      withNode(
        Effect.gen(function* () {
          const h = yield* startupHarness();
          const text = "p".repeat(nextEvent === "PreToolUse" ? 9490 : 8200);
          const start = NodePath.join(h.config.baseDir, "session-start.md");
          NodeFS.writeFileSync(start, text);
          NodeFS.writeFileSync(
            h.configPath,
            JSON.stringify({ exact_keys: [], session_start: [{ path: start, max_chars: 9600 }] }),
          );
          assert.equal((yield* h.request("claude", "SessionStart")).status, 204);
          yield* TestClock.adjust("1300 millis");
          yield* h.persist();
          yield* h.service.nativeSessionRegistered({
            nativeSessionId: "startup-session",
            runId: "startup-run",
          });
          const next = yield* h.moment(
            "claude",
            "startup-session",
            { hook_event_name: nextEvent },
            false,
          );
          assert.equal(next.reply.status, 200);
          if (next.reply.status === 200) assert.include(next.reply.body, text);
          assert.equal(
            h.db.prepare("SELECT coalesce(sum(serve_used), 0) AS used FROM budgets").get()!.used,
            0,
          );
          yield* h.service.release(next.delivery);
          const retry = yield* h.moment(
            "claude",
            "startup-session",
            { hook_event_name: nextEvent },
            false,
          );
          assert.equal(retry.reply.status, 200);
          yield* h.service.settle(retry.delivery);
          assert.equal(
            (yield* h.moment("claude", "startup-session", { hook_event_name: nextEvent }, false))
              .reply.status,
            204,
          );
        }).pipe(Effect.scoped),
      ),
  );

  it.effect(
    "rechecks identity when the last starting run registers before retention installation",
    () =>
      withNode(
        Effect.gen(function* () {
          const lookedUp = yield* Deferred.make<void>();
          const proceed = yield* Deferred.make<void>();
          let reads = 0;
          const h = yield* makeHarness(undefined, false, (rows) =>
            ++reads === 1 && rows.length === 0
              ? Deferred.succeed(lookedUp, undefined).pipe(Effect.andThen(Deferred.await(proceed)))
              : Effect.void,
          );
          const index = NodePath.join(h.config.baseDir, "library-index.md");
          NodeFS.writeFileSync(index, "# Public Library\n");
          NodeFS.writeFileSync(
            h.configPath,
            JSON.stringify({ exact_keys: [], great_library: index }),
          );
          yield* h.sql`INSERT INTO orchestration_v2_projection_runs
      (run_id, thread_id, ordinal, provider, status, requested_at, payload_json)
      VALUES ('startup-run', 'startup-thread', 1, 'custom-instance', 'starting', 'now', '{}')`;
          const pending = yield* Effect.forkScoped(
            h.service.moment({
              authorized: Effect.succeed(true),
              harness: "claude",
              delivery: "registration-race",
              contentLength: undefined,
              body: Effect.succeed(
                JSON.stringify({
                  hook_event_name: "UserPromptSubmit",
                  session_id: "startup-session",
                }),
              ),
            }),
          );
          yield* Deferred.await(lookedUp);
          yield* h.sql`INSERT INTO orchestration_v2_projection_run_attempts
      (attempt_id, thread_id, run_id, attempt_ordinal, root_node_id, provider,
        provider_instance_id, provider_thread_id, status, payload_json)
      VALUES ('attempt', 'startup-thread', 'startup-run', 1, 'node', 'custom-instance',
        'custom-instance', 'provider-thread', 'running', ${JSON.stringify({ nativeThreadId: "startup-session" })})`;
          yield* h.sql`UPDATE orchestration_v2_projection_runs SET status = 'running'`;
          yield* h.service.nativeSessionRegistered({
            nativeSessionId: "startup-session",
            runId: "startup-run",
          });
          yield* Deferred.succeed(proceed, undefined);
          const reply = yield* Fiber.join(pending);
          assert.equal(reply.status, 200);
          if (reply.status === 200) assert.include(reply.body, index);
        }).pipe(Effect.scoped),
      ),
  );

  it.effect(
    "returns the dispatch notification while recovery is blocked, then serves the first matched hook",
    () =>
      withNode(
        Effect.gen(function* () {
          const recovering = yield* Deferred.make<void>();
          const proceed = yield* Deferred.make<void>();
          const dispatched = yield* Deferred.make<void>();
          yield* Effect.addFinalizer(() => Deferred.succeed(proceed, undefined));
          const h = yield* startupHarness((fs) => ({
            ...fs,
            readFileString: (path, options) =>
              fs
                .readFileString(path, options)
                .pipe(
                  Effect.tap(() =>
                    path.endsWith("/session-start.md")
                      ? Deferred.succeed(recovering, undefined).pipe(
                          Effect.andThen(Deferred.await(proceed)),
                        )
                      : Effect.void,
                  ),
                ),
          }));
          const pending = yield* Effect.forkScoped(h.request("claude", "SessionStart"));
          yield* Queue.take(h.missing);
          yield* TestClock.adjust("1300 millis");
          yield* Fiber.join(pending);
          yield* h.persist();
          const notification = yield* Effect.forkScoped(
            h.service
              .nativeSessionRegistered({
                nativeSessionId: "startup-session",
                runId: "startup-run",
              })
              .pipe(Effect.andThen(Deferred.succeed(dispatched, undefined))),
          );
          yield* Deferred.await(recovering);
          // This is the real callback awaited immediately before dispatch, not an immediate mock.
          assert.isTrue(yield* Deferred.isDone(dispatched));
          const firstHook = yield* Effect.forkScoped(
            h.moment("claude", "startup-session", { hook_event_name: "PreToolUse" }, false),
          );
          yield* Deferred.succeed(proceed, undefined);
          const first = yield* Fiber.join(firstHook);
          assert.equal(first.reply.status, 200);
          if (first.reply.status === 200)
            assert.include(first.reply.body, "Public startup instructions.");
          yield* Fiber.join(notification);
        }).pipe(Effect.scoped),
      ),
  );

  it.effect.each(["external", "completed", "other-session", "ambiguous", "no-start", "tool"])(
    "rejects %s without guessing ownership",
    (scenario) =>
      withNode(
        Effect.gen(function* () {
          const h = yield* startupHarness();
          if (scenario === "no-start") yield* h.sql`DELETE FROM orchestration_v2_projection_runs`;
          if (["completed", "other-session", "ambiguous"].includes(scenario))
            yield* h.persist(
              scenario === "other-session" ? "another-session" : "startup-session",
              "old-run",
              scenario === "completed" ? "completed" : "running",
            );
          if (scenario === "ambiguous") yield* h.persist("startup-session", "other-run");
          const retained = ["external", "completed", "other-session"].includes(scenario);
          const pending = yield* Effect.forkScoped(
            h.request("claude", scenario === "tool" ? "PreToolUse" : "UserPromptSubmit"),
          );
          // Complete without advancing the clock, even while an unrelated run is starting.
          assert.equal((yield* Fiber.join(pending)).status, 204);
          if (retained) {
            yield* Queue.take(h.missing);
            // A notification without a committed exact identity cannot join this moment.
            yield* h.service.nativeSessionRegistered({
              nativeSessionId: "startup-session",
              runId: "startup-run",
            });
          }
          assert.deepEqual(h.joined(), {
            status: scenario === "ambiguous" ? "ambiguous" : "unjoined",
            run_id: null,
          });
        }).pipe(Effect.scoped),
      ),
  );

  it.effect.each([
    { count: 33, payloadBytes: 0 },
    { count: 5, payloadBytes: 900_000 },
  ])(
    "bounds retained startup payloads ($count events, $payloadBytes bytes each)",
    ({ count, payloadBytes }) =>
      withNode(
        Effect.gen(function* () {
          const h = yield* startupHarness();
          const pending = [];
          for (let n = 0; n < count; n++) {
            pending.push(
              yield* Effect.forkScoped(
                h.request("claude", "SessionStart", `session-${n}`, `delivery-${n}`, {
                  prompt: "x".repeat(payloadBytes),
                }),
              ),
            );
            assert.equal(yield* Queue.take(h.missing), `session-${n}`);
          }
          yield* TestClock.adjust("1300 millis");
          for (const fiber of pending) assert.equal((yield* Fiber.join(fiber)).status, 204);
          yield* h.persist("session-0");
          yield* h.service.nativeSessionRegistered({
            nativeSessionId: "session-0",
            runId: "startup-run",
          });
          assert.equal(h.joined("delivery-0")!.run_id, null);
          const last = `session-${count - 1}`;
          yield* h.persist(last, "startup-run", "running", 2);
          yield* h.service.nativeSessionRegistered({ nativeSessionId: last, runId: "startup-run" });
          const first = yield* h.moment(
            "claude",
            "session-0",
            { hook_event_name: "PreToolUse" },
            false,
          );
          assert.equal(first.reply.status, 204);
          const next = yield* h.moment("claude", last, { hook_event_name: "PreToolUse" }, false);
          assert.equal(next.reply.status, 200);
          if (next.reply.status === 200)
            assert.include(next.reply.body, "Public startup instructions.");
          assert.deepEqual(h.joined(`delivery-${count - 1}`), {
            status: "ok",
            run_id: "startup-run",
          });
        }).pipe(Effect.scoped),
      ),
  );

  it.effect("rejects an attempt completed while its startup context was being selected", () =>
    withNode(
      Effect.gen(function* () {
        const selecting = yield* Deferred.make<void>();
        const proceed = yield* Deferred.make<void>();
        const h = yield* startupHarness((fs) => ({
          ...fs,
          readFileString: (path, options) =>
            fs
              .readFileString(path, options)
              .pipe(
                Effect.tap(() =>
                  path.endsWith("/session-start.md")
                    ? Deferred.succeed(selecting, undefined).pipe(
                        Effect.andThen(Deferred.await(proceed)),
                      )
                    : Effect.void,
                ),
              ),
        }));
        const pending = yield* Effect.forkScoped(h.request("claude", "SessionStart"));
        yield* Queue.take(h.missing);
        yield* h.persist();
        const registration = yield* Effect.forkScoped(
          h.service.nativeSessionRegistered({
            nativeSessionId: "startup-session",
            runId: "startup-run",
          }),
        );
        yield* Deferred.await(selecting);
        yield* h.sql`UPDATE orchestration_v2_projection_run_attempts SET status = 'completed'`;
        yield* Deferred.succeed(proceed, undefined);
        assert.equal((yield* Fiber.join(pending)).status, 204);
        yield* Fiber.join(registration);
        assert.deepEqual(h.joined(), { status: "unjoined", run_id: null });
        assert.equal(h.db.prepare("SELECT count(*) AS n FROM carries").get()!.n, 0);
      }).pipe(Effect.scoped),
    ),
  );

  it.effect("recovers after the unmatched HTTP hook has finished", () =>
    withNode(
      Effect.gen(function* () {
        const h = yield* startupHarness();
        const pending = yield* Effect.forkScoped(h.request("claude", "SessionStart"));
        yield* Queue.take(h.missing);
        assert.equal((yield* Fiber.join(pending)).status, 204);
        yield* h.persist();
        yield* h.service.nativeSessionRegistered({
          nativeSessionId: "startup-session",
          runId: "startup-run",
        });
        const next = yield* h.moment(
          "claude",
          "startup-session",
          { hook_event_name: "PreToolUse" },
          false,
        );
        assert.equal(next.reply.status, 200);
        assert.deepEqual(h.joined(), { status: "ok", run_id: "startup-run" });
      }).pipe(Effect.scoped),
    ),
  );

  it.effect("expires retained payloads before a later exact registration", () =>
    withNode(
      Effect.gen(function* () {
        const h = yield* startupHarness();
        const pending = yield* Effect.forkScoped(h.request("claude", "SessionStart"));
        yield* Queue.take(h.missing);
        yield* TestClock.adjust("11 seconds");
        assert.equal((yield* Fiber.join(pending)).status, 204);
        yield* h.persist();
        yield* h.service.nativeSessionRegistered({
          nativeSessionId: "startup-session",
          runId: "startup-run",
        });
        assert.deepEqual(h.joined(), { status: "unjoined", run_id: null });
        const next = yield* h.moment(
          "claude",
          "startup-session",
          { hook_event_name: "PreToolUse" },
          false,
        );
        assert.equal(next.reply.status, 204);
      }).pipe(Effect.scoped),
    ),
  );
});

describe("ContextMD Library callback scheduling", () => {
  it.effect("serves first input when filesystem callbacks share a busy event-loop turn", () =>
    withNode(
      Effect.gen(function* () {
        const completions = yield* Queue.unbounded<void>();
        let inFlight = 0;
        let ready: Deferred.Deferred<void>[] = [];
        // The files are real and fast. Delay processing their completed callbacks together,
        // as a busy main loop does, rather than inventing slow disk or holding a SQL lock.
        const callback = <A, E>(read: Effect.Effect<A, E>) =>
          Effect.gen(function* () {
            inFlight += 1;
            const result = yield* Effect.exit(read).pipe(
              Effect.ensuring(
                Effect.sync(() => {
                  inFlight -= 1;
                }),
              ),
            );
            const release = yield* Deferred.make<void>();
            ready.push(release);
            yield* Queue.offer(completions, undefined);
            yield* Deferred.await(release);
            return yield* result;
          });
        const h = yield* makeHarness((fs) => ({
          ...fs,
          stat: (path) => callback(fs.stat(path)),
          realPath: (path) => callback(fs.realPath(path)),
          readDirectory: (path, options) => callback(fs.readDirectory(path, options)),
          readFile: (path) => callback(fs.readFile(path)),
          readFileString: (path, options) => callback(fs.readFileString(path, options)),
        }));
        const roots = ["keys", "great", "cue"].map((name) => NodePath.join(h.config.baseDir, name));
        NodeFS.unlinkSync(NodePath.join(roots[0]!, "public.md"));
        const index = NodePath.join(h.config.baseDir, "library-index.md");
        NodeFS.writeFileSync(index, "# Public Great Library\n");
        NodeFS.mkdirSync(NodePath.join(h.config.baseDir, ".git"));
        NodeFS.writeFileSync(NodePath.join(h.config.baseDir, ".git", "HEAD"), "public-owner\n");
        for (const [source, count] of [6, 1, 9].entries()) {
          const root = roots[source]!;
          NodeFS.mkdirSync(root, { recursive: true });
          for (let entry = 0; entry < count; entry++) {
            NodeFS.writeFileSync(
              NodePath.join(root, `${entry}.md`),
              source === 2
                ? `---\nid: cue-${entry}\nname: Public ${entry}\napplies: A public request.\ntarget: ${index}\n---\nUseful Cue ${entry}.\n`
                : `---\nid: exact-${source}-${entry}\ncommand: public-${entry}\n${source === 0 && entry === 0 ? "name: In exact source\napplies: A public request.\n" : ""}target: ${index}\n---\nExact knowledge.\n`,
            );
          }
        }
        const cueRequests: ReturnType<typeof decodeCueRequest>[] = [];
        const server = NodeHttp.createServer((request, response) => {
          let body = "";
          request.setEncoding("utf8");
          request.on("data", (chunk: string) => {
            body += chunk;
          });
          request.on("end", () => {
            cueRequests.push(decodeCueRequest(body));
            response.writeHead(200, { "content-type": "application/json" });
            response.end(
              JSON.stringify({ adapter: "public", threshold: 0.5, scores: { "cue-0": 1 } }),
            );
          });
        });
        yield* Effect.addFinalizer(() =>
          Effect.promise(
            () =>
              new Promise<void>((resolve) => {
                server.closeAllConnections();
                server.close(() => resolve());
              }),
          ),
        );
        yield* Effect.callback<void>((resume) => {
          server.listen(0, "127.0.0.1", () => resume(Effect.void));
        });
        const address = server.address();
        assert.isNotNull(address);
        if (address === null || typeof address === "string") return;
        NodeFS.writeFileSync(
          h.configPath,
          JSON.stringify({
            exact_keys: roots.slice(0, 2),
            cue: [roots[2]],
            cue_url: `http://127.0.0.1:${address.port}`,
            great_library: index,
          }),
        );
        const session = "busy-files-first-input";
        yield* h.sql`INSERT INTO orchestration_v2_projection_turn_items (
          turn_item_id, thread_id, ordinal, type, status, updated_at, payload_json
        ) VALUES ('busy-user', ${`thread-${session}`}, 1, 'user_message', 'completed',
          '2026-01-01T00:00:00Z', ${JSON.stringify({ text: "Public request." })})`;
        const serve = <A, E>(work: Effect.Effect<A, E>) =>
          Effect.gen(function* () {
            const pending = yield* Effect.forkScoped(work);
            while (true) {
              const next = yield* Effect.raceFirst(
                Queue.take(completions).pipe(Effect.as("callbacks" as const)),
                Fiber.await(pending).pipe(Effect.as("done" as const)),
              );
              if (next === "done") return yield* Fiber.join(pending);
              if (inFlight > 0 || ready.length === 0) continue;
              const batch = ready;
              ready = [];
              // Controlled pressure, not a callback-rate estimate from the live context switches.
              yield* TestClock.adjust("15 millis");
              yield* Effect.forEach(batch, (release) => Deferred.succeed(release, undefined));
            }
          });
        const first = yield* serve(
          h.moment("claude", session, { hook_event_name: "UserPromptSubmit" }),
        );
        const row = h.db
          .prepare(
            "SELECT status, reason, owners_json, cue_status, server_ms FROM moments WHERE delivery = ?",
          )
          .get(first.delivery);
        assert.equal(first.reply.status, 200, JSON.stringify(row));
        if (first.reply.status !== 200) return;
        const context = decodeBody(first.reply.body).hookSpecificOutput.additionalContext;
        assert.include(context, `Read ${index} in full.`);
        assert.include(context, "Useful Cue 0.");
        assert.deepEqual(
          JSON.parse(String(row!.owners_json)),
          Object.fromEntries(roots.map((root) => [root, "public-owner"])),
        );
        assert.equal(row!.cue_status, "ok");
        assert.isBelow(Number(row!.server_ms), 950);
        assert.equal(cueRequests.length, 1);
        assert.lengthOf(cueRequests[0]!.entries, 10);
        assert.include(
          cueRequests[0]!.entries.map((entry) => entry.id),
          "exact-0-0",
        );
        yield* h.service.settle(first.delivery);
        const charged = h.db.prepare("SELECT serve_used FROM budgets").get();
        assert.deepEqual(
          h.db
            .prepare(
              "SELECT bytes FROM candidates WHERE matcher = 'great_library' AND outcome = 'injected'",
            )
            .get(),
          {
            bytes: Buffer.byteLength(
              `ContextMD, the user's own rule store, applies this rule from the user here:\n\nThe Great Library holds knowledge shared across projects and personal knowledge outside any project. Read ${index} in full.`,
              "utf8",
            ),
          },
        );
        const repeat = yield* serve(
          h.moment("claude", session, { hook_event_name: "UserPromptSubmit" }),
        );
        assert.equal(repeat.reply.status, 204);
        assert.deepEqual(h.db.prepare("SELECT serve_used FROM budgets").get(), charged);
        assert.deepEqual(
          h.db
            .prepare(
              "SELECT count(*) AS n FROM candidates WHERE matcher = 'great_library' AND outcome = 'injected'",
            )
            .get(),
          { n: 1 },
        );
      }).pipe(Effect.scoped),
    ),
  );

  it.effect("serves overlapping first inputs when each filesystem turn stays busy", () =>
    withNode(
      Effect.gen(function* () {
        const completions = yield* Queue.unbounded<void>();
        let inFlight = 0;
        let ready: Deferred.Deferred<void>[] = [];
        // Same gate as the single-load test. 45ms is long enough that the
        // 6/1/9 census misses 950ms if each Library still waits for the
        // previous one, and short enough that one shared read of those
        // files fits. It is a controlled turn, not a live callback rate.
        const turn = "45 millis";
        const callback = <A, E>(read: Effect.Effect<A, E>) =>
          Effect.gen(function* () {
            inFlight += 1;
            const result = yield* Effect.exit(read).pipe(
              Effect.ensuring(
                Effect.sync(() => {
                  inFlight -= 1;
                }),
              ),
            );
            const release = yield* Deferred.make<void>();
            ready.push(release);
            yield* Queue.offer(completions, undefined);
            yield* Deferred.await(release);
            return yield* result;
          });
        const h = yield* makeHarness((fs) => ({
          ...fs,
          stat: (path) => callback(fs.stat(path)),
          realPath: (path) => callback(fs.realPath(path)),
          readDirectory: (path, options) => callback(fs.readDirectory(path, options)),
          readFile: (path) => callback(fs.readFile(path)),
          readFileString: (path, options) => callback(fs.readFileString(path, options)),
        }));
        const roots = ["keys", "great", "cue"].map((name) => NodePath.join(h.config.baseDir, name));
        NodeFS.unlinkSync(NodePath.join(roots[0]!, "public.md"));
        const index = NodePath.join(h.config.baseDir, "library-index.md");
        NodeFS.writeFileSync(index, "# Public Great Library\n");
        NodeFS.mkdirSync(NodePath.join(h.config.baseDir, ".git"));
        NodeFS.writeFileSync(NodePath.join(h.config.baseDir, ".git", "HEAD"), "public-owner\n");
        for (const [source, count] of [6, 1, 9].entries()) {
          const root = roots[source]!;
          NodeFS.mkdirSync(root, { recursive: true });
          for (let entry = 0; entry < count; entry++) {
            NodeFS.writeFileSync(
              NodePath.join(root, `${entry}.md`),
              source === 2
                ? `---\nid: cue-${entry}\nname: Public ${entry}\napplies: A public request.\ntarget: ${index}\n---\nUseful Cue ${entry}.\n`
                : `---\nid: exact-${source}-${entry}\ncommand: public-${entry}\n${source === 0 && entry === 0 ? "name: In exact source\napplies: A public request.\n" : ""}target: ${index}\n---\nExact knowledge.\n`,
            );
          }
        }
        const cueRequests: ReturnType<typeof decodeCueRequest>[] = [];
        const server = NodeHttp.createServer((request, response) => {
          let body = "";
          request.setEncoding("utf8");
          request.on("data", (chunk: string) => {
            body += chunk;
          });
          request.on("end", () => {
            cueRequests.push(decodeCueRequest(body));
            response.writeHead(200, { "content-type": "application/json" });
            response.end(
              JSON.stringify({ adapter: "public", threshold: 0.5, scores: { "cue-0": 1 } }),
            );
          });
        });
        yield* Effect.addFinalizer(() =>
          Effect.promise(
            () =>
              new Promise<void>((resolve) => {
                server.closeAllConnections();
                server.close(() => resolve());
              }),
          ),
        );
        yield* Effect.callback<void>((resume) => {
          server.listen(0, "127.0.0.1", () => resume(Effect.void));
        });
        const address = server.address();
        assert.isNotNull(address);
        if (address === null || typeof address === "string") return;
        NodeFS.writeFileSync(
          h.configPath,
          JSON.stringify({
            exact_keys: roots.slice(0, 2),
            cue: [roots[2]],
            cue_url: `http://127.0.0.1:${address.port}`,
            great_library: index,
          }),
        );
        const sessions = ["busy-overlap-claude", "busy-overlap-codex"] as const;
        for (const session of sessions) {
          yield* h.sql`INSERT INTO orchestration_v2_projection_turn_items (
            turn_item_id, thread_id, ordinal, type, status, updated_at, payload_json
          ) VALUES (${`busy-user-${session}`}, ${`thread-${session}`}, 1, 'user_message', 'completed',
            '2026-01-01T00:00:00Z', ${JSON.stringify({ text: "Public request." })})`;
        }
        const left = yield* Effect.forkScoped(
          h.moment("claude", sessions[0], { hook_event_name: "UserPromptSubmit" }),
        );
        const right = yield* Effect.forkScoped(
          h.moment("codex", sessions[1], { hook_event_name: "UserPromptSubmit" }),
        );
        let batches = 0;
        while (true) {
          const next = yield* Effect.raceFirst(
            Queue.take(completions).pipe(Effect.as("callbacks" as const)),
            Effect.all([Fiber.await(left), Fiber.await(right)], { concurrency: 2 }).pipe(
              Effect.as("done" as const),
            ),
          );
          if (next === "done") break;
          if (inFlight > 0 || ready.length === 0) continue;
          const batch = ready;
          ready = [];
          batches += 1;
          yield* TestClock.adjust(turn);
          yield* Effect.forEach(batch, (release) => Deferred.succeed(release, undefined), {
            concurrency: "unbounded",
          });
        }
        const replies = [yield* Fiber.join(left), yield* Fiber.join(right)];
        for (const reply of replies) {
          const row = h.db
            .prepare(
              "SELECT status, reason, owners_json, cue_status, server_ms FROM moments WHERE delivery = ?",
            )
            .get(reply.delivery);
          assert.equal(reply.reply.status, 200, JSON.stringify({ batches, row }));
          if (reply.reply.status !== 200) return;
          assert.include(
            decodeBody(reply.reply.body).hookSpecificOutput.additionalContext,
            `Read ${index} in full.`,
          );
          assert.deepEqual(
            JSON.parse(String(row!.owners_json)),
            Object.fromEntries(roots.map((root) => [root, "public-owner"])),
          );
          assert.equal(row!.cue_status, "ok");
          assert.isBelow(Number(row!.server_ms), 950);
        }
        assert.equal(cueRequests.length, 2);
      }).pipe(Effect.scoped),
    ),
  );
});

describe("ContextMD deadline phase diagnostics", () => {
  it.effect("records completed and active phase durations only for a deadline cut", () =>
    withNode(
      Effect.gen(function* () {
        const h = yield* makeHarness();
        const authEntered = yield* Deferred.make<void>();
        const authRelease = yield* Deferred.make<void>();
        const bodyEntered = yield* Deferred.make<void>();
        const bodyRelease = yield* Deferred.make<void>();
        const request = yield* Effect.forkScoped(
          h.service.moment({
            harness: "claude",
            delivery: "phase-body",
            contentLength: undefined,
            authorized: Deferred.succeed(authEntered, undefined).pipe(
              Effect.andThen(Deferred.await(authRelease)),
              Effect.as(true),
            ),
            body: Deferred.succeed(bodyEntered, undefined).pipe(
              Effect.andThen(Deferred.await(bodyRelease)),
              Effect.as("private body that must never enter diagnostics"),
            ),
          }),
        );
        yield* Deferred.await(authEntered);
        yield* TestClock.adjust("100 millis");
        yield* Deferred.succeed(authRelease, undefined);
        yield* Deferred.await(bodyEntered);
        yield* TestClock.adjust("850 millis");
        assert.equal((yield* Fiber.join(request)).status, 204);
        assert.deepEqual(
          h.db.prepare("SELECT status, reason FROM moments WHERE delivery = 'phase-body'").get(),
          {
            status: "over_budget",
            reason:
              '{"phase":"body","active_ms":850,"completed_ms":{"authorization":100,"off_flag":0,"request":0}}',
          },
        );
        assert.deepEqual(h.db.prepare("SELECT count(*) AS n FROM candidates").get(), { n: 0 });
        assert.deepEqual(h.db.prepare("SELECT count(*) AS n FROM budgets").get(), { n: 0 });
      }).pipe(Effect.scoped),
    ),
  );

  it.effect("keeps concurrent deadline phase observations request-local", () =>
    withNode(
      Effect.gen(function* () {
        const h = yield* makeHarness();
        const authEntered = yield* Deferred.make<void>();
        const bodyEntered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const auth = yield* Effect.forkScoped(
          h.service.moment({
            harness: "claude",
            delivery: "phase-auth",
            contentLength: undefined,
            authorized: Deferred.succeed(authEntered, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.as(true),
            ),
            body: Effect.succeed("{}"),
          }),
        );
        const body = yield* Effect.forkScoped(
          h.service.moment({
            harness: "codex",
            delivery: "phase-body-concurrent",
            contentLength: undefined,
            authorized: Effect.succeed(true),
            body: Deferred.succeed(bodyEntered, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.as("{}"),
            ),
          }),
        );
        yield* Deferred.await(authEntered);
        yield* Deferred.await(bodyEntered);
        yield* TestClock.adjust("950 millis");
        assert.equal((yield* Fiber.join(auth)).status, 204);
        assert.equal((yield* Fiber.join(body)).status, 204);
        assert.deepEqual(
          h.db.prepare("SELECT reason FROM moments WHERE delivery = 'phase-auth'").get(),
          { reason: '{"phase":"authorization","active_ms":950,"completed_ms":{}}' },
        );
        assert.deepEqual(
          h.db.prepare("SELECT reason FROM moments WHERE delivery = 'phase-body-concurrent'").get(),
          {
            reason:
              '{"phase":"body","active_ms":950,"completed_ms":{"authorization":0,"off_flag":0,"request":0}}',
          },
        );
      }).pipe(Effect.scoped),
    ),
  );

  it.effect("preserves malformed-event and unauthorized reasons", () =>
    withNode(
      Effect.gen(function* () {
        const h = yield* makeHarness();
        for (const authorized of [true, false]) {
          assert.equal(
            (yield* h.service.moment({
              harness: "claude",
              delivery: `invalid-${authorized}`,
              contentLength: undefined,
              authorized: Effect.succeed(authorized),
              body: Effect.succeed("{private malformed body"),
            })).status,
            authorized ? 204 : 401,
          );
        }
        assert.deepEqual(h.db.prepare("SELECT status, reason FROM moments ORDER BY id").all(), [
          { status: "bad_event", reason: "json" },
          { status: "unauthorized", reason: null },
        ]);
      }).pipe(Effect.scoped),
    ),
  );

  it.effect("measures elapsed time with the request's injected monotonic clock", () =>
    withNode(
      Effect.gen(function* () {
        const h = yield* makeHarness();
        const clock = yield* Clock.Clock;
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const pending = yield* Effect.forkScoped(
          h.service
            .moment({
              harness: "claude",
              delivery: "fixed-wall-clock",
              contentLength: undefined,
              authorized: Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Deferred.await(release)),
                Effect.as(true),
              ),
              body: Effect.succeed("{}"),
            })
            .pipe(
              Effect.provideService(Clock.Clock, {
                ...clock,
                currentTimeMillis: Effect.succeed(1_000_000),
                currentTimeMillisUnsafe: () => 1_000_000,
              }),
            ),
        );
        yield* Deferred.await(entered);
        yield* TestClock.adjust("950 millis");
        assert.equal((yield* Fiber.join(pending)).status, 204);
        assert.deepEqual(
          h.db
            .prepare("SELECT reason, server_ms FROM moments WHERE delivery = 'fixed-wall-clock'")
            .get(),
          {
            reason: '{"phase":"authorization","active_ms":950,"completed_ms":{}}',
            server_ms: 0,
          },
        );
      }).pipe(Effect.scoped),
    ),
  );

  it.effect.each(["config", "library_load", "selection", "pointer_prepare"])(
    "attributes a blocked %s filesystem boundary",
    (phase) =>
      withNode(
        Effect.gen(function* () {
          const entered = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          const gate = Deferred.succeed(entered, undefined).pipe(
            Effect.andThen(Deferred.await(release)),
          );
          const h = yield* makeHarness((fs) => ({
            ...fs,
            readFileString: (path, options) => {
              const read = fs.readFileString(path, options);
              return (phase === "config" && path.endsWith("/serving.json")) ||
                (phase === "selection" && path.endsWith("/session-start.md"))
                ? gate.pipe(Effect.andThen(read))
                : read;
            },
            readDirectory: (path) =>
              phase === "library_load" && path.endsWith("/keys")
                ? gate.pipe(Effect.andThen(fs.readDirectory(path)))
                : fs.readDirectory(path),
            stat: (path) =>
              phase === "pointer_prepare" && path.endsWith("/library-index.md")
                ? gate.pipe(Effect.andThen(fs.stat(path)))
                : fs.stat(path),
          }));
          const index = NodePath.join(h.config.baseDir, "library-index.md");
          const start = NodePath.join(h.config.baseDir, "session-start.md");
          NodeFS.writeFileSync(index, "# Public Library\n");
          NodeFS.writeFileSync(start, "Public session instructions.\n");
          NodeFS.writeFileSync(
            h.configPath,
            JSON.stringify({
              exact_keys: [NodePath.join(h.config.baseDir, "keys")],
              great_library: index,
              session_start: [{ path: start, max_chars: 100 }],
            }),
          );
          const pending = yield* Effect.forkScoped(
            h.moment("claude", `phase-${phase}`, {
              hook_event_name: phase === "selection" ? "SessionStart" : "UserPromptSubmit",
              source: "startup",
            }),
          );
          yield* Deferred.await(entered);
          yield* TestClock.adjust("950 millis");
          assert.equal((yield* Fiber.join(pending)).reply.status, 204);
          const row = h.db.prepare("SELECT reason FROM moments ORDER BY id DESC LIMIT 1").get();
          assert.isDefined(row);
          const diagnostic = decodeDeadline(String(row!.reason));
          assert.equal(diagnostic.phase, phase);
          assert.equal(diagnostic.active_ms, 950);
          assert.equal(diagnostic.completed_ms.authorization, 0);
          assert.notProperty(diagnostic.completed_ms, phase);
        }).pipe(Effect.scoped),
      ),
  );

  it.effect.each(["run_join", "cue_window"])(
    "attributes a blocked %s on the actual SQLite reader",
    (phase) =>
      withNode(
        Effect.gen(function* () {
          const entered = yield* Deferred.make<void>();
          const proceed = yield* Deferred.make<void>();
          const gate = Deferred.succeed(entered, undefined).pipe(
            Effect.andThen(Deferred.await(proceed)),
          );
          const h = yield* makeHarness(
            (fs) => ({
              ...fs,
              readFileString: (path, options) => {
                const read = fs.readFileString(path, options);
                return phase === "run_join" && path.endsWith("/serving.json")
                  ? read.pipe(Effect.tap(() => gate))
                  : read;
              },
              stat: (path) => {
                const stat = fs.stat(path);
                return phase === "cue_window" && path.endsWith("/library-index.md")
                  ? stat.pipe(Effect.tap(() => gate))
                  : stat;
              },
            }),
            true,
          );
          const index = NodePath.join(h.config.baseDir, "library-index.md");
          const cue = NodePath.join(h.config.baseDir, "cue");
          NodeFS.writeFileSync(index, "# Public Library\n");
          NodeFS.mkdirSync(cue);
          NodeFS.writeFileSync(
            NodePath.join(cue, "public.md"),
            `---\nid: public-cue\nname: Public\napplies: A public request.\ntarget: ${index}\n---\nPublic knowledge.\n`,
          );
          NodeFS.writeFileSync(
            h.configPath,
            JSON.stringify({
              exact_keys: [],
              great_library: index,
              cue: [cue],
              cue_url: "http://127.0.0.1:1",
            }),
          );
          const pending = yield* Effect.forkScoped(
            h.moment("claude", `phase-${phase}`, { hook_event_name: "UserPromptSubmit" }),
          );
          yield* Deferred.await(entered);
          const acquired = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          const reader = yield* Effect.forkScoped(
            h.sql
              .withTransaction(
                Deferred.succeed(acquired, undefined).pipe(Effect.andThen(Deferred.await(release))),
              )
              .pipe(NodeSqliteClient.readOnly),
          );
          yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined));
          yield* Deferred.await(acquired);
          yield* Deferred.succeed(proceed, undefined);
          yield* TestClock.adjust("950 millis");
          assert.equal((yield* Fiber.join(pending)).reply.status, 204);
          const row = h.db.prepare("SELECT reason FROM moments ORDER BY id DESC LIMIT 1").get();
          const diagnostic = decodeDeadline(String(row!.reason));
          assert.equal(diagnostic.phase, phase);
          assert.equal(diagnostic.active_ms, 950);
          assert.equal(diagnostic.completed_ms.config, 0);
          assert.notProperty(diagnostic.completed_ms, phase);
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(reader);
        }).pipe(Effect.scoped),
      ),
  );

  it.effect("distinguishes a Cue wait from building its window", () =>
    withNode(
      Effect.gen(function* () {
        const h = yield* makeHarness();
        const requested = yield* Deferred.make<NodeHttp.ServerResponse>();
        const server = NodeHttp.createServer((request, response) => {
          request.resume();
          Deferred.doneUnsafe(requested, Effect.succeed(response));
        });
        yield* Effect.addFinalizer(() =>
          Effect.promise(
            () =>
              new Promise<void>((resolve) => {
                server.closeAllConnections();
                server.close(() => resolve());
              }),
          ),
        );
        yield* Effect.callback<void>((resume) => {
          server.listen(0, "127.0.0.1", () => resume(Effect.void));
        });
        const address = server.address();
        assert.isNotNull(address);
        if (address === null || typeof address === "string") return;
        const index = NodePath.join(h.config.baseDir, "library-index.md");
        const cue = NodePath.join(h.config.baseDir, "cue");
        NodeFS.writeFileSync(index, "# Public Library\n");
        NodeFS.mkdirSync(cue);
        NodeFS.writeFileSync(
          NodePath.join(cue, "public.md"),
          `---\nid: public-cue\nname: Public\napplies: A public request.\ntarget: ${index}\n---\nPublic knowledge.\n`,
        );
        NodeFS.writeFileSync(
          h.configPath,
          JSON.stringify({
            exact_keys: [],
            cue: [cue],
            cue_url: `http://127.0.0.1:${address.port}`,
          }),
        );
        yield* h.sql`INSERT INTO orchestration_v2_projection_turn_items (
          turn_item_id, thread_id, ordinal, type, status, updated_at, payload_json
        ) VALUES ('public-user', 'thread-phase-cue-wait', 1, 'user_message', 'completed',
          '2026-01-01T00:00:00Z', ${JSON.stringify({ text: "Public request." })})`;
        const pending = yield* Effect.forkScoped(
          h.moment("claude", "phase-cue-wait", { hook_event_name: "UserPromptSubmit" }),
        );
        const response = yield* Deferred.await(requested);
        yield* TestClock.adjust("950 millis");
        assert.equal((yield* Fiber.join(pending)).reply.status, 204);
        const row = h.db.prepare("SELECT reason FROM moments ORDER BY id DESC LIMIT 1").get();
        const diagnostic = decodeDeadline(String(row!.reason));
        assert.equal(diagnostic.phase, "cue_wait");
        assert.equal(diagnostic.active_ms, 950);
        assert.equal(diagnostic.completed_ms.cue_window, 0);
        response.writeHead(503);
        response.end("{}");
      }).pipe(Effect.scoped),
    ),
  );
});

describe("ContextMD serving under database contention", () => {
  it.live.each(["claude", "codex", "opencode"])(
    "serves the first %s pointer while a writer owns the shared connection",
    (harness) =>
      withNode(
        Effect.gen(function* () {
          const h = yield* makeHarness(undefined, true);
          const session = "first-input-contention";
          // Seed a committed run without calling moment or warming its reader first.
          yield* h.sql`INSERT INTO orchestration_v2_projection_run_attempts (
            attempt_id, thread_id, run_id, attempt_ordinal, root_node_id, provider,
            provider_instance_id, provider_thread_id, status, payload_json
          ) VALUES ('first-attempt', 'first-thread', 'first-run', 1, 'node', 'codex',
            'codex', 'provider-thread', 'running', ${JSON.stringify({ nativeThreadId: session })})`;
          const index = NodePath.join(h.config.baseDir, "library-index.md");
          NodeFS.writeFileSync(index, "# Public Library\n");
          NodeFS.writeFileSync(
            h.configPath,
            JSON.stringify({ exact_keys: [], great_library: index }),
          );
          const authContext = yield* Layer.build(
            AuthSessions.layer.pipe(Layer.provide(Layer.succeed(SqlClient.SqlClient, h.sql))),
          );
          const auth = Context.get(authContext, AuthSessions.AuthSessionRepository);
          const sessionId = AuthSessionId.make("public-reader");
          // Only an isolated repository fixture, with no issued token or signing credential.
          yield* auth.create({
            sessionId,
            subject: "public-fixture",
            scopes: ["orchestration:read"],
            method: "bearer-access-token",
            client: {
              label: null,
              ipAddress: null,
              userAgent: null,
              deviceType: "unknown",
              os: null,
              browser: null,
            },
            issuedAt: DateTime.makeUnsafe("2026-01-01T00:00:00Z"),
            expiresAt: DateTime.makeUnsafe("2099-01-01T00:00:00Z"),
          });
          const acquired = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          const writer = yield* Effect.forkScoped(
            h.sql.withTransaction(
              Deferred.succeed(acquired, undefined).pipe(Effect.andThen(Deferred.await(release))),
            ),
          );
          yield* Deferred.await(acquired);
          // The actual repository lookup SessionStore.verify performs before body identification.
          const authorized = auth.getById({ sessionId }).pipe(
            Effect.map(
              (row) =>
                Option.isSome(row) &&
                row.value.revokedAt === null &&
                row.value.scopes.includes("orchestration:read"),
            ),
            Effect.orDie,
          );
          const raw =
            harness === "opencode"
              ? { hook: "chat.message", input: { sessionID: session }, output: {} }
              : {
                  hook_event_name: "UserPromptSubmit",
                  session_id: session,
                  prompt: "Public question",
                };
          const started = yield* Clock.currentTimeMillis;
          const request = (delivery: string) => ({
            harness,
            delivery,
            ...(harness === "opencode" ? { pointerDelivery: `${delivery}-pointer` } : {}),
            authorized,
            contentLength: undefined,
            body: Effect.succeed(JSON.stringify(raw)),
          });
          const reply = yield* h.service.moment(request("first-input"));
          const observation = JSON.stringify(
            h.db
              .prepare(
                "SELECT status, reason, server_ms FROM moments WHERE delivery = 'first-input'",
              )
              .get(),
          );
          assert.isBelow(
            (yield* Clock.currentTimeMillis) - started,
            ContextmdServing.MOMENT_BUDGET_MS,
            observation,
          );
          assert.equal(reply.status, 200, observation);
          if (reply.status !== 200) return;
          assert.include(reply.body, "Great Library");
          const delivery = harness === "opencode" ? "first-input-pointer" : "first-input";
          const settled = yield* harness === "opencode"
            ? h.service.pointerSettle(delivery)
            : h.service.settle(delivery);
          assert.equal(settled.status, 204);
          assert.deepEqual(
            h.db
              .prepare(
                "SELECT matcher, outcome FROM candidates WHERE moment_id = (SELECT id FROM moments WHERE delivery = 'first-input')",
              )
              .all()
              .map((r) => ({ ...r })),
            [{ matcher: "great_library", outcome: "injected" }],
          );
          const text =
            harness === "opencode"
              ? decodePointerBody(reply.body).pointer
              : decodeBody(reply.body).hookSpecificOutput.additionalContext;
          const bytes = Buffer.byteLength(text, "utf8");
          assert.deepEqual(
            h.db
              .prepare("SELECT serve_bytes FROM deliveries WHERE delivery = ?")
              .all(delivery)
              .map((r) => ({ ...r })),
            [{ serve_bytes: bytes }],
          );
          assert.isAtMost(bytes, 8000);
          assert.deepEqual(
            h.db
              .prepare("SELECT serve_used FROM budgets WHERE run_id = 'first-run'")
              .all()
              .map((r) => ({ ...r })),
            [{ serve_used: bytes }],
          );
          // Settlement/claims stay on the serving connection and suppress another same-epoch copy.
          assert.equal((yield* h.service.moment(request("same-epoch"))).status, 204);
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(writer);
          // The readOnly scope must end with the moment. A later write still succeeds, and
          // the next authorization reads the committed revocation rather than an old snapshot.
          yield* auth.revoke({ sessionId, revokedAt: DateTime.makeUnsafe("2026-10-15T00:00:00Z") });
          assert.equal((yield* h.service.moment(request("revoked"))).status, 401);
        }).pipe(Effect.scoped),
      ),
  );
});

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
