// @effect-diagnostics nodeBuiltinImport:off - the test writes a ContextMD store on disk and reads the serving log.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";

import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import { HttpClient, HttpClientRequest, HttpRouter } from "effect/http";

import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as ServerConfig from "../config.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ContextmdRoute from "./ContextmdRoute.ts";

// The store and config are ContextMD's data files; the matcher runs in this module.
const writeStore = (dir: string) => {
  const keys = NodePath.join(dir, "keys");
  NodeFS.mkdirSync(keys, { recursive: true });
  NodeFS.writeFileSync(NodePath.join(keys, "a.md"), "---\nid: a\ncommand: echo a\n---\nrule a\n");
  NodeFS.writeFileSync(
    NodePath.join(keys, "skill.md"),
    "---\nid: skill\npath: **/SKILL.md\nmust-know-before: true\n---\nrule skill\n",
  );
  NodeFS.writeFileSync(
    NodePath.join(keys, "pr.md"),
    "---\nid: pr\ncommand: gh pr create\nmust-know-before: true\n---\nrule pr\n",
  );
  const rule = NodePath.join(dir, "PUSH-RULES.md");
  NodeFS.writeFileSync(rule, "Pushes need the deploy key alias.\n");
  NodeFS.writeFileSync(
    NodePath.join(keys, "push.md"),
    `---\nid: push\ncommand: git push\nmust-know-before: true\ntarget: ${rule}\n---\nrule push\n`,
  );
  const config = NodePath.join(dir, "serving.json");
  NodeFS.writeFileSync(config, JSON.stringify({ exact_keys: [keys] }));
  return { keys, config, rule };
};

/** A Claude Code hook event, as the harness sends it. */
const claude = (event: string, session: string, command: string) => ({
  hook_event_name: event,
  session_id: session,
  cwd: "/tmp",
  tool_name: "Bash",
  tool_input: { command },
  tool_use_id: `toolu-${command}`,
});

/**
 * What Claude Code's adapter projects for a denied call: its input, and the text the model saw
 * in the item type's result field (`output` for commands, `diffStr` for file edits).
 */
const projectDeniedCall = (input: {
  readonly toolUseId: string;
  readonly shown?: string;
  readonly type?: "command_execution" | "file_change";
  readonly status?: string;
  readonly commandInput?: string;
}) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const type = input.type ?? "command_execution";
    const field = type === "file_change" ? "diffStr" : "output";
    const payload = JSON.stringify({
      type,
      nativeItemRef: { driver: "claudeAgent", nativeId: input.toolUseId, strength: "strong" },
      input: input.commandInput ?? "",
      ...(input.shown === undefined
        ? {}
        : { [field]: `Error: PreToolUse hook error: ${input.shown}` }),
    });
    yield* sql`
      INSERT INTO orchestration_v2_projection_turn_items (
        turn_item_id, thread_id, run_id, ordinal, type, status, updated_at, payload_json
      ) VALUES (
        ${`turn-item:provider:claudeAgent:native-item:${input.toolUseId}`}, 'thread-of-run-1', 'run-1',
        1, ${type}, ${input.status ?? "failed"}, '', ${payload}
      )
      ON CONFLICT (turn_item_id) DO UPDATE SET status = excluded.status, payload_json = excluded.payload_json`;
  }).pipe(Effect.orDie);

const insertAttempt = (input: {
  readonly attempt: string;
  readonly run: string;
  readonly status: string;
  readonly native?: string;
  readonly ordinal?: number;
}) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const payload = JSON.stringify({
      id: input.attempt,
      runId: input.run,
      ...(input.native === undefined ? {} : { nativeThreadId: input.native }),
      status: input.status,
    });
    yield* sql`
      INSERT INTO orchestration_v2_projection_run_attempts (
        attempt_id, thread_id, run_id, attempt_ordinal, root_node_id, provider,
        provider_instance_id, provider_thread_id, provider_turn_id, status, payload_json
      ) VALUES (
        ${input.attempt}, ${`thread-of-${input.run}`}, ${input.run}, ${input.ordinal ?? 1}, 'node', 'claudeAgent',
        'claudeAgent', 'provider-thread', NULL, ${input.status}, ${payload}
      )`;
  }).pipe(Effect.orDie);

// Environment sessions are the boundary here: "reader" holds orchestration:read, "weak" holds no scope.
const layerAuth = Layer.mock(EnvironmentAuth.EnvironmentAuth)({
  authenticateHttpRequest: (request) => {
    const token = request.headers["authorization"]?.replace(/^Bearer /u, "");
    if (token !== "reader" && token !== "weak") {
      return Effect.fail(new EnvironmentAuth.ServerAuthMissingCredentialError({}));
    }
    return Effect.succeed({
      scopes: token === "reader" ? ["orchestration:read"] : [],
    } as unknown as EnvironmentAuth.AuthenticatedSession);
  },
});

/** A command item as Codex's adapter projects it, with the call's exit code. */
const projectCommand = (input: {
  readonly toolUseId: string;
  readonly status: string;
  readonly exitCode?: number;
  readonly itemId?: string;
}) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const payload = JSON.stringify({
      type: "command_execution",
      nativeItemRef: { driver: "codex", nativeId: input.toolUseId, strength: "strong" },
      input: "cat",
      ...(input.exitCode === undefined ? {} : { exitCode: input.exitCode }),
    });
    yield* sql`
      INSERT INTO orchestration_v2_projection_turn_items (
        turn_item_id, thread_id, run_id, ordinal, type, status, updated_at, payload_json
      ) VALUES (
        ${input.itemId ?? `turn-item:provider:codex:native-item:${input.toolUseId}`}, 'thread-of-run-1', 'run-1',
        1, 'command_execution', ${input.status}, '', ${payload}
      )`;
  }).pipe(Effect.orDie);

/** Serve the contextmd routes on a test server over a migrated database prepared by `setup`. */
const serve = <R>(
  setup: Effect.Effect<void, never, R | SqlClient.SqlClient>,
  options: { readonly breakStateFile?: boolean } = {},
) =>
  Effect.gen(function* () {
    const configContext = yield* Layer.build(
      ServerConfig.layerTest(process.cwd(), { prefix: "contextmd-route-test-" }),
    );
    const config = Context.get(configContext, ServerConfig.ServerConfig);
    const store = writeStore(config.baseDir);
    // A directory where the state file belongs: SQLite cannot open it.
    if (options.breakStateFile) {
      NodeFS.mkdirSync(NodePath.join(config.stateDir, "contextmd.sqlite"), { recursive: true });
    }
    const database = yield* Layer.build(SqlitePersistence.layerMemory);
    yield* setup.pipe(Effect.provide(database));
    const services = yield* Layer.build(
      HttpRouter.serve(ContextmdRoute.layer, { disableListenLog: true }).pipe(
        Layer.provideMerge(Layer.succeedContext(database)),
        Layer.provideMerge(Layer.succeedContext(configContext)),
        Layer.provideMerge(layerAuth),
        Layer.provideMerge(
          ConfigProvider.layer(
            ConfigProvider.fromUnknown({ CONTEXTMD_SERVING_CONFIG: store.config }),
          ),
        ),
        Layer.provideMerge(NodeHttpServer.layerTest),
      ),
    );
    const client = Context.get(services, HttpClient.HttpClient);
    const token = "reader";
    let n = 0;
    const moment = (
      raw: unknown,
      options: { token?: string; delivery?: string; harness?: string } = {},
    ) =>
      Effect.gen(function* () {
        n += 1;
        const response = yield* client.execute(
          HttpClientRequest.post("/contextmd/v1/moment").pipe(
            HttpClientRequest.setHeaders({
              authorization: `Bearer ${options.token ?? token}`,
              "x-contextmd-harness": options.harness ?? "claude",
              "x-contextmd-delivery": options.delivery ?? `delivery-${n}`,
            }),
            HttpClientRequest.bodyText(JSON.stringify(raw), "application/json"),
          ),
        );
        return { status: response.status, body: yield* response.text };
      }).pipe(Effect.orDie);
    const settle = (delivery: string) =>
      client
        .execute(
          HttpClientRequest.post(`/contextmd/v1/settle/${delivery}`).pipe(
            HttpClientRequest.setHeaders({ authorization: `Bearer ${token}` }),
          ),
        )
        .pipe(
          Effect.map((r) => r.status),
          Effect.orDie,
        );
    const log = () => {
      const db = new NodeSqlite.DatabaseSync(NodePath.join(config.stateDir, "contextmd.sqlite"));
      try {
        return db
          .prepare("SELECT kind, status, reason, run_id FROM moments ORDER BY id")
          .all() as Array<{
          kind: string;
          status: string;
          reason: string | null;
          run_id: string | null;
        }>;
      } finally {
        db.close();
      }
    };
    const chunked = (bytes: number) =>
      client
        .execute(
          HttpClientRequest.post("/contextmd/v1/moment").pipe(
            HttpClientRequest.setHeaders({
              authorization: `Bearer ${token}`,
              "x-contextmd-harness": "claude",
              "x-contextmd-delivery": "chunked",
            }),
            HttpClientRequest.bodyStream(
              Stream.make(new Uint8Array(bytes / 2).fill(32), new Uint8Array(bytes / 2).fill(32)),
            ),
          ),
        )
        .pipe(
          Effect.map((r) => r.status),
          // The server may stop reading at the cap and reset the connection.
          Effect.orElseSucceed(() => "reset" as const),
        );
    const outcomes = (entry: string) => {
      const db = new NodeSqlite.DatabaseSync(NodePath.join(config.stateDir, "contextmd.sqlite"));
      try {
        return (
          db
            .prepare("SELECT outcome FROM candidates WHERE entry = ? ORDER BY id")
            .all(entry) as Array<{
            outcome: string;
          }>
        ).map((r) => r.outcome);
      } finally {
        db.close();
      }
    };
    return { moment, settle, log, store, chunked, outcomes, database };
  });

describe("contextmd routes", () => {
  it.live("joins the running attempt of a run, across a steering restart", () =>
    Effect.gen(function* () {
      const s = yield* serve(
        Effect.all([
          insertAttempt({ attempt: "a1", run: "run-1", status: "superseded", native: "S" }),
          insertAttempt({
            attempt: "a2",
            run: "run-1",
            status: "running",
            native: "S",
            ordinal: 2,
          }),
          insertAttempt({ attempt: "b1", run: "run-2", status: "running", native: "T" }),
          insertAttempt({ attempt: "b2", run: "run-3", status: "running", native: "T" }),
          insertAttempt({ attempt: "c1", run: "run-4", status: "completed", native: "U" }),
        ]).pipe(Effect.asVoid),
      );
      const first = yield* s.moment(claude("PostToolUse", "S", "echo a"));
      assert.strictEqual(first.status, 200);
      assert.include(first.body, "rule a");
      assert.strictEqual((yield* s.moment(claude("PostToolUse", "T", "echo a"))).status, 204);
      assert.strictEqual((yield* s.moment(claude("PostToolUse", "U", "echo a"))).status, 204);
      assert.deepStrictEqual(
        s.log().map((r) => [r.status, r.run_id]),
        [
          ["ok", "run-1"],
          ["ambiguous", null],
          ["unjoined", null],
        ],
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("settles a delivery once and replays a repeated delivery id", () =>
    Effect.gen(function* () {
      const s = yield* serve(
        insertAttempt({ attempt: "a", run: "run-1", status: "running", native: "S" }),
      );
      // Codex leaves no projected item for a denied call, so settlement is the delivery proof.
      const raw = claude("PreToolUse", "S", "gh pr create");
      const first = yield* s.moment(raw, { delivery: "fixed", harness: "codex" });
      assert.include(first.body, "deny");
      assert.deepStrictEqual(yield* s.moment(raw, { delivery: "fixed", harness: "codex" }), first);
      assert.strictEqual(s.log().length, 1);
      assert.strictEqual(yield* s.settle("fixed"), 204);
      assert.strictEqual(yield* s.settle("fixed"), 404);
      assert.strictEqual((yield* s.moment(raw, { harness: "codex" })).status, 204);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("rejects a session without orchestration:read and logs an oversize body", () =>
    Effect.gen(function* () {
      const s = yield* serve(
        insertAttempt({ attempt: "a", run: "run-1", status: "running", native: "S" }),
      );
      const raw = claude("PreToolUse", "S", "gh pr create");
      assert.strictEqual((yield* s.moment(raw, { token: "unknown" })).status, 401);
      assert.strictEqual((yield* s.moment(raw, { token: "weak" })).status, 401);
      const big = { ...raw, pad: "x".repeat(1_100_000) };
      assert.strictEqual((yield* s.moment(big)).status, 204);
      assert.deepStrictEqual(
        s.log().map((r) => r.status),
        ["unauthorized", "unauthorized", "oversize"],
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("logs unreadable events and a broken serving config, and serves nothing", () =>
    Effect.gen(function* () {
      const s = yield* serve(
        insertAttempt({ attempt: "a", run: "run-1", status: "running", native: "S" }),
      );
      // Grok also runs Claude-format project hooks; its snake_case events are not Claude's.
      const grokEvent = {
        ...claude("PreToolUse", "S", "gh pr create"),
        hook_event_name: "pre_tool_use",
      };
      assert.strictEqual((yield* s.moment(grokEvent)).status, 204);
      NodeFS.writeFileSync(s.store.config, "{not json");
      assert.strictEqual((yield* s.moment(claude("PreToolUse", "S", "gh pr create"))).status, 204);
      assert.deepStrictEqual(
        s.log().map((r) => r.status),
        ["bad_event", "bad_config"],
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("reads an edited entry on the next moment", () =>
    Effect.gen(function* () {
      const s = yield* serve(
        insertAttempt({ attempt: "a", run: "run-1", status: "running", native: "S" }),
      );
      assert.include((yield* s.moment(claude("PostToolUse", "S", "echo a"))).body, "rule a");
      NodeFS.writeFileSync(
        NodePath.join(s.store.keys, "a.md"),
        "---\nid: a\ncommand: echo a\n---\nrule a, edited\n",
      );
      // A new version is a new entry for dedupe within the run.
      assert.include(
        (yield* s.moment(claude("PostToolUse", "S", "echo a"))).body,
        "rule a, edited",
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("keeps serving off, without failing startup, when its state file cannot be opened", () =>
    Effect.gen(function* () {
      const s = yield* serve(
        insertAttempt({ attempt: "a", run: "run-1", status: "running", native: "S" }),
        { breakStateFile: true },
      );
      assert.strictEqual((yield* s.moment(claude("PreToolUse", "S", "gh pr create"))).status, 204);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("caps a chunked body while reading it", () =>
    Effect.gen(function* () {
      const s = yield* serve(
        insertAttempt({ attempt: "a", run: "run-1", status: "running", native: "S" }),
      );
      assert.include([204, "reset"], yield* s.chunked(2_000_000));
      assert.deepStrictEqual(
        s.log().map((r) => r.status),
        ["oversize"],
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("gives concurrent requests with one delivery id the same reply", () =>
    Effect.gen(function* () {
      const s = yield* serve(
        insertAttempt({ attempt: "a", run: "run-1", status: "running", native: "S" }),
      );
      const raw = claude("PreToolUse", "S", "gh pr create");
      const [a, b] = yield* Effect.all(
        [s.moment(raw, { delivery: "same" }), s.moment(raw, { delivery: "same" })],
        { concurrency: 2 },
      );
      assert.strictEqual(a.status, 200);
      assert.deepStrictEqual(b, a);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("does not count a rule shown only in the denied call's input as delivered", () =>
    Effect.gen(function* () {
      const s = yield* serve(
        Effect.all([
          insertAttempt({ attempt: "a", run: "run-1", status: "running", native: "S" }),
          // The command itself contains the rule text; the model saw only another hook's reason.
          projectDeniedCall({
            toolUseId: "toolu-gh pr create --body 'rule pr'",
            commandInput: "gh pr create --body 'rule pr'",
            shown: "OTHER-HOOK: no",
          }),
        ]).pipe(Effect.asVoid),
      );
      const raw = claude("PreToolUse", "S", "gh pr create --body 'rule pr'");
      assert.include((yield* s.moment(raw)).body, "rule pr");
      yield* s.settle("delivery-1");
      assert.include((yield* s.moment(raw)).body, "rule pr");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("proves an edit deny from the file item's diff, once the item is finished", () =>
    Effect.gen(function* () {
      const s = yield* serve(
        insertAttempt({ attempt: "a", run: "run-1", status: "running", native: "S" }),
      );
      const edit = {
        hook_event_name: "PreToolUse",
        session_id: "S",
        cwd: "/tmp",
        tool_name: "Edit",
        tool_input: { file_path: "/tmp/skills/ops/SKILL.md" },
        tool_use_id: "toolu-edit",
      };
      assert.include((yield* s.moment(edit)).body, "rule skill");
      yield* s.settle("delivery-1");
      // Still running, with no result yet: the deny stays pending, not hidden.
      yield* projectDeniedCall({
        toolUseId: "toolu-edit",
        type: "file_change",
        status: "running",
      }).pipe(Effect.provide(s.database));
      assert.include(
        (yield* s.moment({ ...edit, tool_use_id: "toolu-edit-2" })).body,
        "rule skill",
      );
      // Finished, and its diff field shows the rule: the inline rule lifts.
      yield* projectDeniedCall({
        toolUseId: "toolu-edit",
        type: "file_change",
        status: "failed",
        shown: "deny-head\n\nrule skill",
      }).pipe(Effect.provide(s.database));
      assert.strictEqual((yield* s.moment({ ...edit, tool_use_id: "toolu-edit-3" })).status, 204);
      assert.notInclude(s.outcomes("skill"), "hidden");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("counts a Codex read only once the projected command exited 0", () =>
    Effect.gen(function* () {
      const s = yield* serve(
        insertAttempt({ attempt: "a", run: "run-1", status: "running", native: "S" }),
      );
      const codex = (event: string, command: string, toolUseId: string, response = "") => ({
        hook_event_name: event,
        session_id: "S",
        cwd: "/tmp",
        tool_name: "Bash",
        tool_input: { command },
        tool_use_id: toolUseId,
        tool_response: response,
      });
      let calls = 0;
      const push = () => {
        calls += 1;
        return s.moment(codex("PreToolUse", "git push", `push-${calls}`), { harness: "codex" });
      };
      assert.include((yield* push()).body, "rule push");
      // Another command whose id merely ends with this one's, and that succeeded, proves nothing.
      yield* projectCommand({
        toolUseId: "prior-exec-1",
        status: "completed",
        exitCode: 0,
        itemId: "turn-item:provider:codex:native-item:prior-exec-1",
      }).pipe(Effect.provide(s.database));
      // A finished item without an exit code proves nothing either.
      yield* s.moment(
        codex("PostToolUse", `cat ${s.store.rule}`, "exec-0", "Pushes need the deploy key alias."),
        {
          harness: "codex",
        },
      );
      yield* projectCommand({ toolUseId: "exec-0", status: "completed" }).pipe(
        Effect.provide(s.database),
      );
      assert.include((yield* push()).body, "rule push");
      // A failed read (exit code 1) of the target does not lift the rule.
      yield* s.moment(
        codex("PostToolUse", `cat ${s.store.rule}`, "exec-1", "cat: Permission denied"),
        {
          harness: "codex",
        },
      );
      yield* projectCommand({ toolUseId: "exec-1", status: "failed", exitCode: 1 }).pipe(
        Effect.provide(s.database),
      );
      assert.include((yield* push()).body, "rule push");
      // A read that exited 0 does.
      yield* s.moment(
        codex("PostToolUse", `cat ${s.store.rule}`, "exec-2", "Pushes need the deploy key alias."),
        {
          harness: "codex",
        },
      );
      yield* projectCommand({ toolUseId: "exec-2", status: "completed", exitCode: 0 }).pipe(
        Effect.provide(s.database),
      );
      assert.strictEqual((yield* push()).status, 204);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("disables itself when run attempts no longer carry nativeThreadId", () =>
    Effect.gen(function* () {
      const s = yield* serve(insertAttempt({ attempt: "a", run: "run-1", status: "running" }));
      assert.strictEqual((yield* s.moment(claude("PreToolUse", "S", "gh pr create"))).status, 204);
      assert.deepStrictEqual(
        s.log().map((r) => r.status),
        ["unmapped"],
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
