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
    NodePath.join(keys, "pr.md"),
    "---\nid: pr\ncommand: gh pr create\nmust-know-before: true\n---\nrule pr\n",
  );
  const config = NodePath.join(dir, "serving.json");
  NodeFS.writeFileSync(config, JSON.stringify({ exact_keys: [keys] }));
  return { keys, config };
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

/** Serve the contextmd routes on a test server over a migrated database prepared by `setup`. */
const serve = <R>(setup: Effect.Effect<void, never, R | SqlClient.SqlClient>) =>
  Effect.gen(function* () {
    const configContext = yield* Layer.build(
      ServerConfig.layerTest(process.cwd(), { prefix: "contextmd-route-test-" }),
    );
    const config = Context.get(configContext, ServerConfig.ServerConfig);
    const store = writeStore(config.baseDir);
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
    return { moment, settle, log, store };
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
