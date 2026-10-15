// @effect-diagnostics nodeBuiltinImport:off - the test writes a ContextMD store on disk, reads the serving log and runs a fake Cue service.
import * as NodeFS from "node:fs";
import * as NodeHttp from "node:http";
import type * as NodeNet from "node:net";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";

import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it, vi } from "@effect/vitest";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import { AuthSessionId, OrchestrationV2ProviderThread } from "@t3tools/contracts";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import { HttpClient, HttpClientRequest, HttpRouter } from "effect/http";

import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as ServerConfig from "../config.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as AuthSessions from "../persistence/AuthSessions.ts";
import * as ContextmdRoute from "./ContextmdRoute.ts";
import { MOMENT_BUDGET_MS } from "./ContextmdServing.ts";
import * as ContextmdServing from "./ContextmdServing.ts";
import * as Store from "./Store.ts";
import { isHoldout } from "./ServingState.ts";
const decodeProviderThread = Schema.decodeUnknownEffect(OrchestrationV2ProviderThread);
const decodeInlineReply = Schema.decodeUnknownSync(
  Schema.fromJsonString(
    Schema.Struct({ hookSpecificOutput: Schema.Struct({ additionalContext: Schema.String }) }),
  ),
);

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

const projectThread = (thread: string, root: string, worktree: string | null = null) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const project = `project-${thread}`;
    yield* sql`INSERT INTO projection_projects (project_id, title, workspace_root, scripts_json, created_at, updated_at)
      VALUES (${project}, 'Library project', ${root}, '[]', '', '')`;
    yield* sql`INSERT INTO orchestration_v2_projection_threads (
      thread_id, project_id, title, default_provider, runtime_mode, interaction_mode, created_at, updated_at, payload_json
    ) VALUES (${thread}, ${project}, 'Library thread', 'claudeAgent', 'full-access', 'default', '', '',
      ${JSON.stringify({ worktreePath: worktree })})`;
  }).pipe(Effect.orDie);

// Environment sessions are the boundary here: "reader" holds orchestration:read, "weak" holds no scope.
/** Credentials: `reader` may read, `weak` may not; a `slow-` prefix answers the same after 1.3 s. */
const layerAuth = Layer.mock(EnvironmentAuth.EnvironmentAuth)({
  authenticateHttpRequest: (request) => {
    const header = request.headers["authorization"]?.replace(/^Bearer /u, "") ?? "";
    const token = header.replace(/^slow-/u, "");
    const answer =
      token !== "reader" && token !== "weak"
        ? Effect.fail(new EnvironmentAuth.ServerAuthMissingCredentialError({}))
        : Effect.succeed({
            scopes: token === "reader" ? ["orchestration:read"] : [],
          } as unknown as EnvironmentAuth.AuthenticatedSession);
    return header.startsWith("slow-") ? Effect.sleep(1300).pipe(Effect.andThen(answer)) : answer;
  },
});

// Keep the route and real auth repository lookup; no signing key or issued credential is needed.
const layerRepositoryAuth = Layer.unwrap(
  Effect.gen(function* () {
    const sessions = yield* AuthSessions.AuthSessionRepository;
    return Layer.mock(EnvironmentAuth.EnvironmentAuth)({
      authenticateHttpRequest: (request) =>
        Effect.gen(function* () {
          const sessionId = AuthSessionId.make(
            request.headers.authorization?.replace(/^Bearer /u, "") ?? "missing",
          );
          const row = yield* sessions.getById({ sessionId }).pipe(Effect.orDie);
          if (Option.isNone(row) || row.value.revokedAt !== null) {
            return yield* new EnvironmentAuth.ServerAuthMissingCredentialError({});
          }
          return {
            sessionId,
            subject: row.value.subject,
            method: row.value.method,
            scopes: row.value.scopes,
          };
        }),
    });
  }),
).pipe(Layer.provide(AuthSessions.layer));

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

/** A Cue entry directory beside the exact keys, with one entry pointing at a procedure file. */
const writeCue = (dir: string) => {
  const cue = NodePath.join(dir, "cue");
  NodeFS.mkdirSync(cue, { recursive: true });
  const procedure = NodePath.join(dir, "research.md");
  NodeFS.writeFileSync(procedure, "# Research\n");
  NodeFS.writeFileSync(
    NodePath.join(cue, "research.md"),
    `---\nid: agentsmd-research\nname: Research\napplies: The request asks for facts beyond what is at hand.\ntarget: ${procedure}\n---\nAgentsMD: follow the research procedure.\n`,
  );
  return { cue, procedure };
};

/** A fake Cue service: records each scoring request and answers with `scores`. */
const fakeCue = (
  scores: Record<string, number>,
  refuse?: { status: number; error: string },
  delayMs = 0,
) =>
  Effect.acquireRelease(
    Effect.promise(
      () =>
        new Promise<{ url: string; requests: Array<Record<string, any>>; close: () => void }>(
          (resolve) => {
            const requests: Array<Record<string, any>> = [];
            const server = NodeHttp.createServer((req, res) => {
              const chunks: Buffer[] = [];
              req.on("data", (c: Buffer) => chunks.push(c));
              req.on("end", () => {
                if (req.url === "/score") {
                  requests.push(JSON.parse(Buffer.concat(chunks).toString("utf8")));
                }
                if (refuse !== undefined && req.url === "/score") {
                  res.writeHead(refuse.status, { "content-type": "application/json" });
                  res.end(JSON.stringify({ error: refuse.error }));
                  return;
                }
                const answer = () => {
                  res.writeHead(200, { "content-type": "application/json" });
                  res.end(
                    JSON.stringify({ adapter: "fake-adapter", threshold: 0.5, scores, ms: 5 }),
                  );
                };
                // @effect-diagnostics-next-line globalTimers:off - a plain Node test server answering late.
                if (req.url === "/score" && delayMs > 0) setTimeout(answer, delayMs);
                else answer();
              });
            });
            server.listen(0, "127.0.0.1", () => {
              const { port } = server.address() as NodeNet.AddressInfo;
              resolve({ url: `http://127.0.0.1:${port}`, requests, close: () => server.close() });
            });
          },
        ),
    ),
    (s) => Effect.sync(() => s.close()),
  );

/** A port nothing listens on. */
const closedPort = () =>
  new Promise<string>((resolve) => {
    const server = NodeHttp.createServer();
    server.listen(0, "127.0.0.1", () => {
      const { port } = server.address() as NodeNet.AddressInfo;
      server.close(() => resolve(`http://127.0.0.1:${port}`));
    });
  });

/** The first session id that the holdout hash keeps out of the holdout arm. */
const servedSession = (() => {
  for (let i = 0; ; i++) if (!isHoldout("claude", `S${i}`)) return `S${i}`;
})();

/** Serve the contextmd routes on a test server over a migrated database prepared by `setup`. */
const serve = <R>(
  setup: Effect.Effect<void, never, R | SqlClient.SqlClient>,
  options: {
    readonly breakStateFile?: boolean;
    readonly cueUrl?: string;
    /** A serving database already marked, so this server starts as the next process would. */
    readonly seedState?: string;
    readonly greatLibrary?: boolean;
    /** Real disk/WAL reader and auth-session lookup for receipt contention regressions. */
    readonly repositoryAuth?: boolean;
  } = {},
) =>
  Effect.gen(function* () {
    const configContext = yield* Layer.build(
      ServerConfig.layerTest(process.cwd(), { prefix: "contextmd-route-test-" }),
    );
    const config = Context.get(configContext, ServerConfig.ServerConfig);
    if (options.seedState !== undefined) {
      NodeFS.copyFileSync(options.seedState, NodePath.join(config.stateDir, "contextmd.sqlite"));
    }
    const store = writeStore(config.baseDir);
    if (options.cueUrl !== undefined) {
      const { cue } = writeCue(config.baseDir);
      NodeFS.writeFileSync(
        store.config,
        JSON.stringify({ exact_keys: [store.keys], cue: [cue], cue_url: options.cueUrl }),
      );
    }
    if (options.greatLibrary) {
      const index = NodePath.join(config.baseDir, "library-index.md");
      NodeFS.writeFileSync(index, "# Isolated Great Library\n");
      NodeFS.writeFileSync(
        store.config,
        JSON.stringify({
          ...JSON.parse(NodeFS.readFileSync(store.config, "utf8")),
          great_library: index,
        }),
      );
    }
    // A directory where the state file belongs: SQLite cannot open it.
    if (options.breakStateFile) {
      NodeFS.mkdirSync(NodePath.join(config.stateDir, "contextmd.sqlite"), { recursive: true });
    }
    const database = yield* Layer.build(
      options.repositoryAuth
        ? SqlitePersistence.layerFromPath(config.dbPath)
        : SqlitePersistence.layerMemory,
    );
    yield* setup.pipe(Effect.provide(database));
    const services = yield* Layer.build(
      HttpRouter.serve(ContextmdRoute.layer, { disableListenLog: true }).pipe(
        Layer.provideMerge(ContextmdServing.layer),
        Layer.provideMerge(Layer.succeedContext(database)),
        Layer.provideMerge(Layer.succeedContext(configContext)),
        Layer.provideMerge(
          options.repositoryAuth
            ? layerRepositoryAuth.pipe(Layer.provide(Layer.succeedContext(database)))
            : layerAuth,
        ),
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
      options: {
        token?: string;
        delivery?: string;
        harness?: string;
        pointerDelivery?: string;
      } = {},
    ) =>
      Effect.gen(function* () {
        n += 1;
        const response = yield* client.execute(
          HttpClientRequest.post("/contextmd/v1/moment").pipe(
            HttpClientRequest.setHeaders({
              authorization: `Bearer ${options.token ?? token}`,
              "x-contextmd-harness": options.harness ?? "claude",
              "x-contextmd-delivery": options.delivery ?? `delivery-${n}`,
              ...(options.pointerDelivery === undefined
                ? {}
                : { "x-contextmd-pointer-delivery": options.pointerDelivery }),
            }),
            HttpClientRequest.bodyText(JSON.stringify(raw), "application/json"),
          ),
        );
        return { status: response.status, body: yield* response.text };
      }).pipe(Effect.orDie);
    const postDelivery = (
      action: "settle" | "release" | "pointer/applying" | "pointer/settle" | "pointer/release",
      delivery: string,
      credential = token,
    ) =>
      client
        .execute(
          HttpClientRequest.post(`/contextmd/v1/${action}/${delivery}`).pipe(
            HttpClientRequest.setHeaders({ authorization: `Bearer ${credential}` }),
          ),
        )
        .pipe(
          Effect.map((r) => r.status),
          Effect.orDie,
        );
    const settle = (delivery: string) => postDelivery("settle", delivery);
    const release = (delivery: string) => postDelivery("release", delivery);
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
    const identity = () => {
      const db = new NodeSqlite.DatabaseSync(NodePath.join(config.stateDir, "contextmd.sqlite"));
      try {
        return db
          .prepare("SELECT kind, status, session, tool_use_id FROM moments ORDER BY id")
          .all() as Array<{
          kind: string | null;
          status: string;
          session: string | null;
          tool_use_id: string | null;
        }>;
      } finally {
        db.close();
      }
    };
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
    const cueLog = () => {
      const db = new NodeSqlite.DatabaseSync(NodePath.join(config.stateDir, "contextmd.sqlite"));
      try {
        return {
          moments: db
            .prepare("SELECT kind, cue_status, cue_adapter FROM moments ORDER BY id")
            .all() as Array<{
            kind: string;
            cue_status: string | null;
            cue_adapter: string | null;
          }>,
          candidates: db
            .prepare(
              "SELECT entry, outcome, score FROM candidates WHERE matcher = 'intent' ORDER BY id",
            )
            .all() as Array<{ entry: string; outcome: string; score: number | null }>,
        };
      } finally {
        db.close();
      }
    };
    /** Send `raw` in two halves `stallMs` apart; the reply status, or "reset". */
    const stalled = (raw: unknown, stallMs: number) => {
      const bytes = new TextEncoder().encode(JSON.stringify(raw));
      const half = Math.floor(bytes.length / 2);
      return client
        .execute(
          HttpClientRequest.post("/contextmd/v1/moment").pipe(
            HttpClientRequest.setHeaders({
              authorization: `Bearer ${token}`,
              "x-contextmd-harness": "claude",
              "x-contextmd-delivery": "stalled",
            }),
            HttpClientRequest.bodyStream(
              Stream.concat(
                Stream.make(bytes.slice(0, half)),
                Stream.fromEffect(Effect.sleep(stallMs).pipe(Effect.as(bytes.slice(half)))),
              ),
            ),
          ),
        )
        .pipe(
          Effect.map((r) => r.status),
          Effect.orElseSucceed(() => "reset" as const),
        );
    };
    const sqlitePath = NodePath.join(config.stateDir, "contextmd.sqlite");
    return {
      serving: Context.get(services, ContextmdServing.ContextmdServing),
      postDelivery,
      config,
      moment,
      settle,
      release,
      log,
      identity,
      store,
      chunked,
      stalled,
      outcomes,
      database,
      cueLog,
      sqlitePath,
    };
  });

/** Many tool calls in the thread, each a projected command item, at consecutive ordinals. */
const projectToolCalls = (from: number, count: number) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    for (let k = 0; k < count; k++) {
      const ordinal = from + k;
      yield* sql`
        INSERT INTO orchestration_v2_projection_turn_items (
          turn_item_id, thread_id, run_id, ordinal, type, status, updated_at, payload_json
        ) VALUES (
          ${`turn-item:tool:${ordinal}`}, 'thread-of-run-1', 'run-1', ${ordinal}, 'command_execution',
          'completed', '', ${JSON.stringify({ type: "command_execution", input: `ls ${ordinal}` })}
        )`;
    }
  }).pipe(Effect.orDie);

/** A thread's user message as Chromeria projects it. */
const projectUserMessage = (text: string, ordinal = 1) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`
      INSERT INTO orchestration_v2_projection_turn_items (
        turn_item_id, thread_id, run_id, ordinal, type, status, updated_at, payload_json
      ) VALUES (
        ${`turn-item:message:${ordinal}`}, 'thread-of-run-1', 'run-1', ${ordinal}, 'user_message',
        'completed', '', ${JSON.stringify({ type: "user_message", text })}
      )`;
  }).pipe(Effect.orDie);

describe("contextmd routes", () => {
  it.live.each(["settle", "release"] as const)(
    "%s authenticates within the hook receipt deadline while a writer owns the connection",
    (action) =>
      Effect.gen(function* () {
        const session = "receipt-contention";
        const sessionId = AuthSessionId.make("reader");
        const setup = Effect.gen(function* () {
          yield* insertAttempt({ attempt: "a", run: "run-1", status: "running", native: session });
          const sessions = yield* AuthSessions.AuthSessionRepository;
          for (const [id, scopes] of [
            [sessionId, ["orchestration:read"]],
            [AuthSessionId.make("weak"), []],
          ] as const) {
            yield* sessions.create({
              sessionId: id,
              subject: "public-fixture",
              scopes,
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
          }
        }).pipe(Effect.provide(AuthSessions.layer), Effect.orDie);
        const s = yield* serve(setup, { greatLibrary: true, repositoryAuth: true });
        const sql = Context.get(s.database, SqlClient.SqlClient);
        const raw = { hook_event_name: "UserPromptSubmit", session_id: session, cwd: "/tmp" };
        const first = yield* s.moment(raw, { delivery: "first-pointer" });
        assert.equal(first.status, 200);
        assert.include(first.body, "Great Library");
        const decoded = decodeInlineReply(first.body);
        const expectedBytes = Buffer.byteLength(
          decoded.hookSpecificOutput.additionalContext,
          "utf8",
        );
        const acquired = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const writer = yield* Effect.forkScoped(
          sql.withTransaction(
            Deferred.succeed(acquired, undefined).pipe(Effect.andThen(Deferred.await(release))),
          ),
        );
        yield* Deferred.await(acquired);
        // contextmd-hook prints the reply before this request, then cuts it off at 300 ms.
        const response = yield* s
          .postDelivery(action, "first-pointer")
          .pipe(Effect.timeoutOption("300 millis"));
        assert.deepEqual(response, Option.some(204));
        assert.equal(yield* s.postDelivery(action, "first-pointer", "weak"), 401);
        const db = new NodeSqlite.DatabaseSync(s.sqlitePath, { readOnly: true });
        try {
          const query = (text: string) =>
            db
              .prepare(text)
              .all()
              .map((row) => ({ ...row }));
          assert.deepEqual(query("SELECT stage FROM deliveries WHERE delivery = 'first-pointer'"), [
            { stage: action === "settle" ? "settled" : "released" },
          ]);
          const bytes = Number(
            db.prepare("SELECT serve_bytes FROM deliveries WHERE delivery = 'first-pointer'").get()!
              .serve_bytes,
          );
          assert.equal(bytes, expectedBytes);
          assert.deepEqual(query("SELECT serve_used FROM budgets WHERE run_id = 'run-1'"), [
            { serve_used: action === "settle" ? bytes : 0 },
          ]);
          assert.deepEqual(
            query("SELECT settled FROM claims WHERE delivery = 'first-pointer'"),
            action === "settle" ? [{ settled: 1 }] : [],
          );
          const repeat = yield* s.moment(raw, { delivery: "same-epoch" });
          assert.equal(repeat.status, action === "settle" ? 204 : 200);
        } finally {
          db.close();
        }
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(writer);
        // The marker is local to auth reads. Committed revocation remains authoritative.
        const sessions = Context.get(
          yield* Layer.build(
            AuthSessions.layer.pipe(Layer.provide(Layer.succeedContext(s.database))),
          ),
          AuthSessions.AuthSessionRepository,
        );
        yield* sessions.revoke({
          sessionId,
          revokedAt: DateTime.makeUnsafe("2026-10-15T00:00:00Z"),
        });
        assert.equal(yield* s.postDelivery(action, "same-epoch"), 401);
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("separates mixed Cue/pointer receipts from shown observation and generic closures", () =>
    Effect.gen(function* () {
      let session = "mixed-0";
      for (let n = 1; isHoldout("opencode", session); n++) session = `mixed-${n}`;
      const cue = yield* fakeCue({ "agentsmd-research": 2.5 });
      const s = yield* serve(
        Effect.all([
          insertAttempt({ attempt: "a", run: "run-1", status: "running", native: session }),
          projectUserMessage("Read the projector's design."),
        ]).pipe(Effect.asVoid),
        { cueUrl: cue.url, greatLibrary: true },
      );
      const raw = { hook: "chat.message", directory: "/tmp", input: { sessionID: session } };
      const first = yield* s.moment(raw, {
        harness: "opencode",
        delivery: "D",
        pointerDelivery: "P",
      });
      assert.strictEqual(first.status, 200);
      const reply = JSON.parse(first.body);
      assert.include(reply.context, "Research applies when:");
      assert.include(
        reply.pointer,
        "knowledge shared across projects and personal knowledge outside any project",
      );
      assert.deepStrictEqual(
        yield* s.moment(raw, { harness: "opencode", delivery: "D", pointerDelivery: "P" }),
        first,
      );
      assert.strictEqual(yield* s.settle("P"), 404);
      assert.strictEqual(yield* s.release("P"), 404);
      assert.strictEqual(yield* s.settle("D"), 204);
      const db = new NodeSqlite.DatabaseSync(s.sqlitePath, { readOnly: true });
      try {
        const query = (sql: string) =>
          db
            .prepare(sql)
            .all()
            .map((row) => ({ ...row }));
        assert.deepStrictEqual(
          query("SELECT delivery, stage, outcome FROM candidates WHERE matcher = 'great_library'"),
          [{ delivery: "P", stage: "reserved", outcome: "reserved" }],
        );
        const bytes = Number(
          query("SELECT serve_bytes FROM deliveries WHERE delivery = 'D'")[0]!.serve_bytes,
        );
        assert.strictEqual(yield* s.postDelivery("pointer/applying", "P"), 204);
        assert.strictEqual(yield* s.postDelivery("pointer/applying", "P"), 204);
        const accounting = () => ({
          budgets: query("SELECT * FROM budgets ORDER BY run_id"),
          claims: query("SELECT * FROM claims ORDER BY rowid"),
          candidates: query(
            "SELECT matcher, delivery, stage, outcome, bytes, served_at FROM candidates ORDER BY id",
          ),
          sessions: query("SELECT * FROM sessions ORDER BY harness, session"),
        });
        const beforeCheck = accounting();
        yield* s.serving.checkShown;
        assert.deepStrictEqual(accounting(), beforeCheck);
        assert.deepStrictEqual(
          query("SELECT delivery, stage, shown, shown_next FROM deliveries ORDER BY delivery"),
          [
            { delivery: "D", stage: "settled", shown: "unknown", shown_next: null },
            { delivery: "P", stage: "applying", shown: null, shown_next: null },
          ],
        );
        const evidence = query("SELECT shown_json FROM deliveries WHERE delivery = 'D'");
        assert.strictEqual(
          JSON.parse(String(evidence[0]!.shown_json)).reason,
          "no_evidence_source",
        );
        assert.strictEqual(yield* s.settle("P"), 404);
        assert.strictEqual(yield* s.release("P"), 404);
        assert.deepStrictEqual(accounting(), beforeCheck);
        assert.strictEqual(yield* s.postDelivery("pointer/release", "P"), 204);
        assert.deepStrictEqual(
          query("SELECT shown_json FROM deliveries WHERE delivery = 'D'"),
          evidence,
        );
        assert.deepStrictEqual(query("SELECT serve_used FROM budgets WHERE run_id = 'run-1'"), [
          { serve_used: bytes },
        ]);
        assert.deepStrictEqual(query("SELECT outcome FROM candidates WHERE matcher = 'intent'"), [
          { outcome: "injected" },
        ]);
        const retry = yield* s.moment(raw, { harness: "opencode", pointerDelivery: "P2" });
        assert.strictEqual(retry.status, 200);
        assert.strictEqual(yield* s.postDelivery("pointer/settle", "P2"), 204);
        assert.strictEqual(yield* s.postDelivery("pointer/release", "P2"), 409);
        yield* s.serving.checkShown;
        assert.deepStrictEqual(query("SELECT stage, shown FROM deliveries WHERE delivery = 'P2'"), [
          { stage: "settled", shown: null },
        ]);
        assert.strictEqual(cue.requests.length, 2);
      } finally {
        db.close();
      }
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live.each([
    { harness: "codex", driver: "codex" },
    { harness: "claude", driver: "claudeAgent" },
  ] as const)(
    "$harness fork ownership filters projected compactions by source, boundary and rollback",
    ({ harness, driver }) =>
      Effect.gen(function* () {
        const s = yield* serve(
          insertAttempt({ attempt: "a", run: "run-1", status: "running", native: "source" }),
          { greatLibrary: true },
        );
        const reply = yield* s.moment(
          { hook_event_name: "UserPromptSubmit", session_id: "source", cwd: "/tmp" },
          { harness, delivery: "source-pointer" },
        );
        assert.equal(reply.status, 200);
        assert.equal(yield* s.settle("source-pointer"), 204);
        const db = new NodeSqlite.DatabaseSync(s.sqlitePath, { readOnly: true });
        try {
          const row = db
            .prepare("SELECT received_at FROM moments WHERE delivery = 'source-pointer'")
            .get()!;
          const received = Number(row.received_at);
          const at = (seconds: number) => DateTime.formatIso(DateTime.makeUnsafe(seconds * 1000));
          const source = yield* decodeProviderThread({
            id: "provider-source",
            driver,
            providerInstanceId: driver,
            providerSessionId: "session",
            appThreadId: "thread-of-run-1",
            ownerNodeId: null,
            nativeThreadRef: { driver, nativeId: "source", strength: "strong" },
            nativeConversationHeadRef: null,
            status: "idle",
            firstRunOrdinal: null,
            lastRunOrdinal: null,
            handoffIds: [],
            forkedFrom: null,
            createdAt: DateTime.makeUnsafe(at(received)),
            updatedAt: DateTime.makeUnsafe(at(received)),
          });
          const sql = Context.get(s.database, SqlClient.SqlClient);
          yield* sql`INSERT INTO orchestration_v2_projection_runs (run_id, thread_id, ordinal, provider, status, requested_at, payload_json) VALUES ('rolled', 'thread-of-run-1', 1, ${driver}, 'rolled_back', '', '{}')`;
          const compact = (
            id: string,
            provider: string,
            started: string | null,
            run: string | null,
          ) => sql`
        INSERT INTO orchestration_v2_projection_turn_items (turn_item_id, thread_id, run_id, provider_thread_id, ordinal, type, status, updated_at, payload_json)
        VALUES (${id}, 'thread-of-run-1', ${run}, ${provider}, 1, 'compaction', 'completed', '', ${JSON.stringify({ startedAt: started })})`;
          yield* compact("foreign", "another-provider-thread", at(received + 1), null);
          yield* compact("reverted", source.id, at(received + 2), "rolled");
          yield* compact("later", source.id, at(received + 20), null);
          yield* compact("older", source.id, at(received - 1), null);
          const fork = (nativeId: string, forkDriver: string = driver) =>
            s.serving.recordNativeFork({
              driver: forkDriver,
              threadId: "target-thread",
              runId: "target-run",
              sourceThreadId: "thread-of-run-1",
              source,
              target: {
                ...source,
                nativeThreadRef: { driver: source.driver, nativeId, strength: "strong" },
              },
              boundaryAt: DateTime.makeUnsafe(at(received + 10)),
            });
          yield* fork("target-inherited");
          const outcome = (session: string) =>
            db
              .prepare(
                "SELECT m.harness,c.outcome,c.reason,c.bytes FROM candidates c JOIN moments m ON m.id = c.moment_id WHERE m.session = ?",
              )
              .all(session)
              .map((row) => ({ ...row }));
          assert.deepEqual(outcome("target-inherited"), [
            { harness, outcome: "inherited", reason: null, bytes: 0 },
          ]);
          yield* insertAttempt({
            attempt: "b",
            run: "run-2",
            status: "running",
            native: "target-inherited",
          }).pipe(Effect.provide(s.database));
          assert.strictEqual(
            (yield* s.moment(
              {
                hook_event_name: "SessionStart",
                source: "resume",
                session_id: "target-inherited",
                cwd: "/tmp",
              },
              { harness },
            )).status,
            204,
          );
          assert.strictEqual(
            (yield* s.moment(
              { hook_event_name: "UserPromptSubmit", session_id: "target-inherited", cwd: "/tmp" },
              { harness },
            )).status,
            204,
          );
          assert.deepStrictEqual(
            db
              .prepare(
                "SELECT harness, session, epoch, settled FROM claims WHERE session = 'target-inherited'",
              )
              .all()
              .map((row) => ({ ...row })),
            [{ harness, session: "target-inherited", epoch: 0, settled: 1 }],
          );
          yield* fork("unsupported", "grok");
          assert.deepStrictEqual(outcome("unsupported"), []);
          yield* compact("unknown-start", source.id, null, null);
          yield* fork("target-compacted");
          assert.deepEqual(outcome("target-compacted"), [
            { harness, outcome: "not_inherited", reason: "compacted", bytes: 0 },
          ]);
          yield* insertAttempt({
            attempt: "c",
            run: "run-3",
            status: "running",
            native: "target-compacted",
          }).pipe(Effect.provide(s.database));
          const fresh = yield* s.moment(
            { hook_event_name: "UserPromptSubmit", session_id: "target-compacted", cwd: "/tmp" },
            { harness, delivery: "fresh-pointer" },
          );
          assert.strictEqual(fresh.status, 200);
          assert.include(fresh.body, "The Great Library holds knowledge shared across projects");
          NodeFS.writeFileSync(NodePath.join(s.config.stateDir, "contextmd", "off"), "");
          yield* fork("off");
          assert.deepStrictEqual(outcome("off"), []);
        } finally {
          db.close();
        }
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live(
    "Claude fork correlation survives delayed compaction ingestion and fences a missing re-serve",
    () =>
      Effect.gen(function* () {
        const s = yield* serve(
          insertAttempt({ attempt: "a", run: "run-1", status: "running", native: "source" }),
          { greatLibrary: true },
        );
        const hook = (delivery: string, source?: string) =>
          s.moment(
            {
              hook_event_name: source === undefined ? "UserPromptSubmit" : "SessionStart",
              session_id: "source",
              cwd: "/tmp",
              ...(source === undefined ? {} : { source }),
            },
            { harness: "claude", delivery },
          );
        assert.strictEqual((yield* hook("initial-pointer")).status, 200);
        assert.strictEqual(yield* s.settle("initial-pointer"), 204);
        assert.strictEqual((yield* hook("compact-pointer", "compact")).status, 200);
        assert.strictEqual(yield* s.settle("compact-pointer"), 204);
        const db = new NodeSqlite.DatabaseSync(s.sqlitePath, { readOnly: true });
        try {
          const received = Number(
            db.prepare("SELECT received_at FROM moments WHERE delivery = 'compact-pointer'").get()!
              .received_at,
          );
          const at = (seconds: number) => DateTime.makeUnsafe(seconds * 1000);
          const source = yield* decodeProviderThread({
            id: "provider-source",
            driver: "claudeAgent",
            providerInstanceId: "claudeAgent",
            providerSessionId: "session",
            appThreadId: "thread-of-run-1",
            ownerNodeId: null,
            nativeThreadRef: { driver: "claudeAgent", nativeId: "source", strength: "strong" },
            nativeConversationHeadRef: null,
            status: "idle",
            firstRunOrdinal: null,
            lastRunOrdinal: null,
            handoffIds: [],
            forkedFrom: null,
            createdAt: at(received),
            updatedAt: at(received),
          });
          const sql = Context.get(s.database, SqlClient.SqlClient);
          const compact = (
            id: string,
            seconds: number,
          ) => sql`INSERT INTO orchestration_v2_projection_turn_items
          (turn_item_id, thread_id, run_id, provider_thread_id, ordinal, type, status, updated_at, payload_json)
          VALUES (${id}, 'thread-of-run-1', 'run-1', ${source.id}, 1, 'compaction', 'completed', '', ${JSON.stringify({ startedAt: DateTime.formatIso(at(seconds)) })})`;
          const fork = (nativeId: string) =>
            s.serving.recordNativeFork({
              driver: "claudeAgent",
              sourceThreadId: "thread-of-run-1",
              threadId: "target-thread",
              runId: "target-run",
              source,
              target: {
                ...source,
                nativeThreadRef: { driver: source.driver, nativeId, strength: "strong" },
              },
              boundaryAt: at(received + 10),
            });
          const outcome = (session: string) =>
            db
              .prepare(
                "SELECT c.outcome, c.reason FROM candidates c JOIN moments m ON m.id = c.moment_id WHERE m.session = ?",
              )
              .all(session)
              .map((row) => ({ ...row }));
          yield* compact("first-boundary", received + 1);
          yield* fork("after-one");
          assert.deepStrictEqual(outcome("after-one"), [{ outcome: "inherited", reason: null }]);
          yield* compact("second-boundary", received + 2);
          yield* fork("missing-second");
          assert.deepStrictEqual(outcome("missing-second"), [
            { outcome: "not_inherited", reason: "compacted" },
          ]);
          assert.strictEqual((yield* hook("second-compact-pointer", "compact")).status, 200);
          assert.strictEqual(yield* s.settle("second-compact-pointer"), 204);
          yield* fork("after-two");
          assert.deepStrictEqual(outcome("after-two"), [{ outcome: "inherited", reason: null }]);
          const record = db
            .prepare(
              "SELECT c.extra_json FROM candidates c JOIN moments m ON m.id = c.moment_id WHERE m.session = 'after-two'",
            )
            .get()!;
          assert.strictEqual(
            JSON.parse(String(record.extra_json)).source_delivery,
            "second-compact-pointer",
          );
          assert.deepStrictEqual(
            db
              .prepare(
                "SELECT source, epoch FROM moments WHERE delivery = 'second-compact-pointer'",
              )
              .all()
              .map((row) => ({ ...row })),
            [{ source: "compact", epoch: 2 }],
          );
          yield* insertAttempt({
            attempt: "b",
            run: "run-2",
            status: "running",
            native: "after-two",
          }).pipe(Effect.provide(s.database));
          assert.strictEqual(
            (yield* s.moment(
              { hook_event_name: "UserPromptSubmit", session_id: "after-two", cwd: "/tmp" },
              { harness: "claude" },
            )).status,
            204,
          );
        } finally {
          db.close();
        }
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("the serving owner honors off while accounting for an immutable Grok head", () =>
    Effect.gen(function* () {
      const s = yield* serve(Effect.void, { greatLibrary: true });
      const stage = yield* s.serving.stageSessionInstructions({
        driver: "grok",
        threadId: "T",
        runId: "R",
      });
      assert.isString(stage.text);
      assert.isString(stage.reservation);
      yield* s.serving.recordInstallations(
        [{ nativeSessionId: "G", text: stage.text!, site: "rollback" }],
        { threadId: "T" },
      );
      NodeFS.writeFileSync(NodePath.join(s.config.stateDir, "contextmd", "off"), "");
      assert.isUndefined(yield* s.serving.currentInstructions("grok"));
      assert.deepStrictEqual(
        yield* s.serving.stageSessionInstructions({ driver: "grok", threadId: "T", runId: "R2" }),
        { reservation: null, text: undefined },
      );
      yield* s.serving.attachSessionInstructions({
        driver: "grok",
        threadId: "T",
        runId: "R",
        nativeSessionId: "G",
        reservation: stage.reservation,
      });
      const db = new NodeSqlite.DatabaseSync(s.sqlitePath, { readOnly: true });
      try {
        assert.deepStrictEqual(
          db
            .prepare("SELECT outcome, bytes FROM candidates WHERE outcome = 'installed_stale'")
            .all()
            .map((row) => ({ ...row })),
          [{ outcome: "installed_stale", bytes: Buffer.byteLength(stage.text!) }],
        );
        assert.deepStrictEqual(
          db
            .prepare("SELECT consumed_run FROM installations")
            .all()
            .map((row) => ({ ...row })),
          [{ consumed_run: "R" }],
        );
      } finally {
        db.close();
      }
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
  it.live(
    "discovers the joined project and worktree while ignoring another project's hook cwd",
    () =>
      Effect.gen(function* () {
        const s = yield* serve(
          Effect.all([
            insertAttempt({ attempt: "a", run: "run-1", status: "running", native: "A" }),
            insertAttempt({ attempt: "b", run: "run-2", status: "running", native: "B" }),
          ]).pipe(Effect.asVoid),
        );
        const base = NodePath.dirname(s.store.keys),
          a = NodePath.join(base, "a"),
          b = NodePath.join(base, "b");
        NodeFS.mkdirSync(NodePath.join(a, ".git"), { recursive: true });
        NodeFS.mkdirSync(NodePath.join(a, "sub"));
        NodeFS.mkdirSync(NodePath.join(a, "library"));
        NodeFS.mkdirSync(NodePath.join(b, "library"), { recursive: true });
        NodeFS.writeFileSync(NodePath.join(b, ".git"), "gitdir: /unused");
        for (const [root, id] of [
          [a, "project-a"],
          [b, "project-b"],
        ]) {
          NodeFS.writeFileSync(
            NodePath.join(root!, "library", "rule.md"),
            `---\nid: ${id}\ncommand: x\nmust-know-before: true\n---\n${"Knowledge. ".repeat(200)}`,
          );
        }
        yield* Effect.all([
          projectThread("thread-of-run-1", NodePath.join(a, "sub")),
          projectThread("thread-of-run-2", a, b),
        ]).pipe(Effect.provide(s.database));
        const first = yield* s.moment({ ...claude("PreToolUse", "A", "x"), cwd: b });
        assert.strictEqual(first.status, 200);
        assert.include(first.body, NodePath.join(a, "library", "rule.md"));
        assert.notInclude(first.body, NodePath.join(b, "library"));
        const second = yield* s.moment({ ...claude("PreToolUse", "B", "x"), cwd: a });
        assert.include(second.body, NodePath.join(b, "library", "rule.md"));
        assert.notInclude(second.body, NodePath.join(a, "library"));
        assert.deepStrictEqual(s.outcomes("project-a"), ["reserved"]);
        assert.deepStrictEqual(s.outcomes("project-b"), ["reserved"]);
        const sql = Context.get(s.database, SqlClient.SqlClient);
        yield* sql`UPDATE projection_projects SET deleted_at = 'deleted' WHERE workspace_root = ${NodePath.join(a, "sub")}`;
        assert.strictEqual(
          (yield* s.moment({ ...claude("PreToolUse", "A", "x"), cwd: b })).status,
          204,
        );
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("scores a conventional Cue entry when no Cue directory is registered", () =>
    Effect.gen(function* () {
      const cue = yield* fakeCue({ "project-cue": 2.5 });
      const s = yield* serve(
        Effect.all([
          insertAttempt({ attempt: "a", run: "run-1", status: "running", native: servedSession }),
          projectUserMessage("Look at the logs"),
        ]).pipe(Effect.asVoid),
        { cueUrl: cue.url },
      );
      const root = NodePath.join(NodePath.dirname(s.store.keys), "project");
      NodeFS.mkdirSync(NodePath.join(root, "library"), { recursive: true });
      NodeFS.writeFileSync(
        NodePath.join(root, "library", "cue.md"),
        '---\nid: project-cue\nname: Project\napplies: "When looking at logs. Not: a build."\n---\nProject knowledge.',
      );
      NodeFS.writeFileSync(
        s.store.config,
        JSON.stringify({ exact_keys: [], cue: [], cue_url: cue.url }),
      );
      yield* projectThread("thread-of-run-1", root).pipe(Effect.provide(s.database));
      const reply = yield* s.moment({
        hook_event_name: "UserPromptSubmit",
        session_id: servedSession,
        cwd: "/tmp",
      });
      assert.strictEqual(reply.status, 200);
      assert.include(reply.body, "Project knowledge.");
      assert.deepStrictEqual(cue.requests[0]!["entries"], [
        { id: "project-cue", doc: "Project: When looking at logs. Not: a build." },
      ]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

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
      // A shown reply is not a drop.
      assert.strictEqual(yield* s.release("fixed"), 404);
      assert.strictEqual((yield* s.moment(raw, { harness: "codex" })).status, 204);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("releases a reserved reply once, and a later settle finds nothing", () =>
    Effect.gen(function* () {
      const s = yield* serve(
        insertAttempt({ attempt: "a", run: "run-1", status: "running", native: "S" }),
      );
      const raw = claude("PostToolUse", "S", "echo a");
      const first = yield* s.moment(raw, { delivery: "held" });
      assert.strictEqual(first.status, 200);
      assert.strictEqual(yield* s.release("held"), 204);
      assert.strictEqual(yield* s.release("held"), 404);
      assert.strictEqual(yield* s.settle("held"), 404);
      assert.strictEqual(yield* s.release("missing"), 404);
      // The bytes are back: the same rule is served again.
      assert.strictEqual((yield* s.moment(raw)).status, 200);
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

  it.live("logs a Grok pre_tool for both PreToolUse and pre_tool_use (#273)", () =>
    Effect.gen(function* () {
      const s = yield* serve(
        insertAttempt({ attempt: "a", run: "run-1", status: "running", native: "S" }),
      );
      const call = (event: string, extra: Record<string, unknown> = {}) =>
        s.moment(
          {
            hook_event_name: event,
            session_id: "S",
            cwd: "/tmp",
            tool_name: "run_terminal_command",
            tool_input: { command: "gh pr create" },
            tool_use_id: `g-${event}`,
            ...extra,
          },
          { harness: "grok" },
        );
      // The live wire: Claude's name in hook_event_name, Grok's name beside it.
      const live = yield* call("PreToolUse", { hookEventName: "pre_tool_use" });
      assert.strictEqual(live.status, 200);
      assert.include(live.body, "PreToolUse");
      assert.include(live.body, "rule pr");
      // The name the grok map was written for, with no PascalCase field.
      const snake = yield* call("pre_tool_use");
      assert.strictEqual(snake.status, 200);
      assert.include(snake.body, "PreToolUse");
      assert.include(snake.body, "rule pr");
      const others: ReadonlyArray<readonly [string, string]> = [
        ["PostToolUse", "post_tool_use"],
        ["UserPromptSubmit", "user_prompt_submit"],
        ["Stop", "stop"],
        ["SessionStart", "session_start"],
        ["PostToolUseFailure", "post_tool_use_failure"],
        ["PostCompact", "post_compact"],
      ];
      for (const [pascal, native] of others) {
        const response = yield* s.moment(
          { hook_event_name: pascal, hookEventName: native, session_id: "S", cwd: "/tmp" },
          { harness: "grok" },
        );
        assert.notStrictEqual(response.status, 401, pascal);
      }
      const rows = s.log();
      assert.strictEqual(rows.filter((row) => row.status === "bad_event").length, 0);
      assert.strictEqual(rows.filter((row) => row.kind === "pre_tool").length, 2);
      assert.ok(rows.every((row) => row.status === "ok"));
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

  it.live("serves a Cue fire at the user message, scored on the thread's transcript", () =>
    Effect.gen(function* () {
      const cue = yield* fakeCue({ "agentsmd-research": 2.5 });
      const s = yield* serve(
        Effect.all([
          insertAttempt({ attempt: "a", run: "run-1", status: "running", native: servedSession }),
          projectUserMessage("How does the projector handle retries?"),
        ]).pipe(Effect.asVoid),
        { cueUrl: cue.url },
      );
      const r = yield* s.moment({
        hook_event_name: "UserPromptSubmit",
        session_id: servedSession,
        cwd: "/tmp",
        prompt: "How does the projector handle retries?",
      });
      assert.strictEqual(r.status, 200);
      const served = JSON.parse(r.body).hookSpecificOutput.additionalContext as string;
      assert.include(
        served,
        "Research applies when: The request asks for facts beyond what is at hand.",
      );
      assert.include(served, "research.md in full.");
      const [request] = cue.requests;
      assert.deepStrictEqual(request!["items"], [
        ["user", "How does the projector handle retries?"],
      ]);
      assert.deepStrictEqual(request!["entries"], [
        {
          id: "agentsmd-research",
          doc: "Research: The request asks for facts beyond what is at hand.",
        },
      ]);
      assert.deepStrictEqual(s.cueLog(), {
        moments: [{ kind: "user_message", cue_status: "ok", cue_adapter: "fake-adapter" }],
        candidates: [{ entry: "agentsmd-research", outcome: "reserved", score: 2.5 }],
      });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("serves a slow Cue score that fits the moment's budget (#248)", () =>
    Effect.gen(function* () {
      // A 600 ms score timed out at the old 650 ms wait once the moment's own work was added.
      const cue = yield* fakeCue({ "agentsmd-research": 2.5 }, undefined, 600);
      const s = yield* serve(
        Effect.all([
          insertAttempt({ attempt: "a", run: "run-1", status: "running", native: servedSession }),
          projectUserMessage("How does the projector handle retries?"),
        ]).pipe(Effect.asVoid),
        { cueUrl: cue.url },
      );
      const r = yield* s.moment({
        hook_event_name: "UserPromptSubmit",
        session_id: servedSession,
        cwd: "/tmp",
        prompt: "How does the projector handle retries?",
      });
      assert.strictEqual(r.status, 200);
      assert.deepStrictEqual(s.cueLog().moments, [
        { kind: "user_message", cue_status: "ok", cue_adapter: "fake-adapter" },
      ]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("replies within the moment's budget when Cue never answers a failed call (#251)", () =>
    Effect.gen(function* () {
      const cue = yield* fakeCue({ "agentsmd-research": 2.5 }, undefined, 10_000);
      const s = yield* serve(
        Effect.all([
          insertAttempt({ attempt: "a", run: "run-1", status: "running", native: servedSession }),
          projectUserMessage("Look at the logs"),
        ]).pipe(Effect.asVoid),
        { cueUrl: cue.url },
      );
      for (const event of [
        claude("PostToolUseFailure", servedSession, "echo a"),
        {
          hook_event_name: "UserPromptSubmit",
          session_id: servedSession,
          cwd: "/tmp",
          prompt: "Look at the logs",
        },
      ]) {
        const started = yield* Clock.currentTimeMillis;
        yield* s.moment(event);
        // Measured by the client: the connection, the moment's work, the Cue wait and the reply.
        assert.isBelow((yield* Clock.currentTimeMillis) - started, MOMENT_BUDGET_MS);
      }
      assert.deepStrictEqual(
        s.cueLog().moments.map((m) => [m.kind, m.cue_status]),
        [
          ["tool_failure", "timeout"],
          ["user_message", "timeout"],
        ],
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("logs what a moment cut by its budget knew of itself (#252)", () =>
    Effect.gen(function* () {
      const s = yield* serve(
        insertAttempt({ attempt: "a", run: "run-1", status: "running", native: servedSession }),
      );
      // A transaction holds the shared database, so the run lookup after parsing waits past the budget.
      const sql = Context.get(s.database, SqlClient.SqlClient);
      yield* Effect.forkScoped(sql.withTransaction(Effect.sleep(2 * MOMENT_BUDGET_MS)));
      yield* Effect.sleep(50);
      const started = yield* Clock.currentTimeMillis;
      const r = yield* s.moment(claude("PreToolUse", servedSession, "echo a"));
      assert.isBelow((yield* Clock.currentTimeMillis) - started, MOMENT_BUDGET_MS);
      assert.strictEqual(r.status, 204);
      // Cut before parsing: nothing about the call is known.
      yield* s.stalled(claude("PostToolUse", servedSession, "echo a"), 2 * MOMENT_BUDGET_MS);
      assert.deepStrictEqual(s.identity(), [
        {
          kind: "pre_tool",
          status: "over_budget",
          session: servedSession,
          tool_use_id: "toolu-echo a",
        },
        { kind: null, status: "over_budget", session: null, tool_use_id: null },
      ]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("logs the parsed call of a moment cut while selection reads its store (#252)", () =>
    Effect.gen(function* () {
      const s = yield* serve(
        insertAttempt({ attempt: "a", run: "run-1", status: "running", native: servedSession }),
      );
      // The exact-key store answers only after the budget is spent.
      const load = Store.load;
      const spy = vi
        .spyOn(Store, "load")
        .mockImplementation((roots, cache) =>
          Effect.sleep(2 * MOMENT_BUDGET_MS).pipe(Effect.andThen(load(roots, cache))),
        );
      yield* Effect.addFinalizer(() => Effect.sync(() => spy.mockRestore()));
      const r = yield* s.moment(claude("PreToolUse", servedSession, "echo a"));
      assert.strictEqual(r.status, 204);
      assert.isAbove(spy.mock.calls.length, 0);
      assert.deepStrictEqual(s.identity(), [
        {
          kind: "pre_tool",
          status: "over_budget",
          session: servedSession,
          tool_use_id: "toolu-echo a",
        },
      ]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("counts a slow authentication against the moment's budget (#251)", () =>
    Effect.gen(function* () {
      const s = yield* serve(
        insertAttempt({ attempt: "a", run: "run-1", status: "running", native: servedSession }),
      );
      // Both a credential that may read and one that may not answer only after 1.3 s.
      for (const token of ["slow-reader", "slow-unknown"]) {
        const started = yield* Clock.currentTimeMillis;
        const r = yield* s.moment(claude("PostToolUse", servedSession, "echo a"), { token });
        assert.isBelow((yield* Clock.currentTimeMillis) - started, MOMENT_BUDGET_MS);
        // Cut before the credential is known: nothing is served.
        assert.strictEqual(r.status, 204);
      }
      assert.deepStrictEqual(
        s.log().map((m) => m.status),
        ["over_budget", "over_budget"],
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("cuts a moment whose work outruns the budget and logs it over budget (#251)", () =>
    Effect.gen(function* () {
      const s = yield* serve(
        insertAttempt({ attempt: "a", run: "run-1", status: "running", native: servedSession }),
      );
      const started = yield* Clock.currentTimeMillis;
      // The body arrives too slowly for the moment to finish inside its budget.
      const r = yield* s.stalled(
        claude("PostToolUse", servedSession, "echo a"),
        2 * MOMENT_BUDGET_MS,
      );
      assert.isBelow((yield* Clock.currentTimeMillis) - started, MOMENT_BUDGET_MS);
      // The reply may come while the client is still sending, which can reset the connection.
      assert.include([204, "reset"], r);
      assert.deepStrictEqual(
        s.log().map((m) => m.status),
        ["over_budget"],
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("starts Cue before a tool call and serves its fire with the tool result", () =>
    Effect.gen(function* () {
      const cue = yield* fakeCue({ "agentsmd-research": 2.5 });
      const s = yield* serve(
        Effect.all([
          insertAttempt({ attempt: "a", run: "run-1", status: "running", native: servedSession }),
          projectUserMessage("Look at the logs"),
        ]).pipe(Effect.asVoid),
        { cueUrl: cue.url },
      );
      assert.strictEqual(
        (yield* s.moment(claude("PreToolUse", servedSession, "echo a"))).status,
        204,
      );
      const result = yield* s.moment(claude("PostToolUse", servedSession, "echo a"));
      assert.include(result.body, "rule a");
      assert.include(result.body, "Research applies when");
      assert.strictEqual(cue.requests.length, 1);
      assert.strictEqual(cue.requests[0]!["moment"]["kind"], "pre_tool");
      assert.include(cue.requests[0]!["moment"]["call"], "echo a");
      assert.deepStrictEqual(
        s.cueLog().moments.map((m) => m.cue_status),
        ["started", "ok"],
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("skips an unreachable Cue for later moments and keeps serving exact keys", () =>
    Effect.gen(function* () {
      const s = yield* serve(
        Effect.all([
          insertAttempt({ attempt: "a", run: "run-1", status: "running", native: servedSession }),
          projectUserMessage("Ship it"),
        ]).pipe(Effect.asVoid),
        { cueUrl: yield* Effect.promise(closedPort) },
      );
      const message = yield* s.moment({
        hook_event_name: "UserPromptSubmit",
        session_id: servedSession,
        cwd: "/tmp",
        prompt: "Ship it",
      });
      assert.strictEqual(message.status, 204);
      yield* s.moment(claude("PreToolUse", servedSession, "echo a"));
      const result = yield* s.moment(claude("PostToolUse", servedSession, "echo a"));
      assert.include(result.body, "rule a");
      assert.deepStrictEqual(
        s.cueLog().moments.map((m) => m.cue_status),
        ["connect_failed", "not_called", "not_called"],
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("a restarted server keeps Cue skipped until its health document is back", () =>
    Effect.gen(function* () {
      const url = yield* Effect.promise(closedPort);
      const setup = Effect.all([
        insertAttempt({ attempt: "a", run: "run-1", status: "running", native: servedSession }),
        projectUserMessage("Ship it"),
      ]).pipe(Effect.asVoid);
      const first = yield* serve(setup, { cueUrl: url });
      assert.strictEqual(
        (yield* first.moment({
          hook_event_name: "UserPromptSubmit",
          session_id: servedSession,
          cwd: "/tmp",
          prompt: "Ship it",
        })).status,
        204,
      );
      assert.strictEqual(first.cueLog().moments[0]!.cue_status, "connect_failed");
      const db = new NodeSqlite.DatabaseSync(first.sqlitePath);
      db.exec("PRAGMA wal_checkpoint(FULL)");
      db.close();
      const seed = `${first.sqlitePath}.seed`;
      NodeFS.copyFileSync(first.sqlitePath, seed);
      const second = yield* serve(setup, { cueUrl: url, seedState: seed });
      yield* second.moment(
        {
          hook_event_name: "UserPromptSubmit",
          session_id: servedSession,
          cwd: "/tmp",
          prompt: "Ship it",
        },
        { delivery: "second-user" },
      );
      yield* second.moment(claude("PreToolUse", servedSession, "echo a"), {
        delivery: "second-pre",
      });
      assert.deepStrictEqual(
        second.cueLog().moments.map((m) => m.cue_status),
        ["connect_failed", "not_called", "not_called"],
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("keeps the current user message in Cue's window after hundreds of tool calls", () =>
    Effect.gen(function* () {
      const cue = yield* fakeCue({ "agentsmd-research": 2.5 });
      const s = yield* serve(
        Effect.all([
          insertAttempt({ attempt: "a", run: "run-1", status: "running", native: servedSession }),
          projectUserMessage("Earlier request", 1),
          projectUserMessage("Find out why the projector retries", 2),
          projectToolCalls(3, 450),
        ]).pipe(Effect.asVoid),
        { cueUrl: cue.url },
      );
      yield* s.moment(claude("PreToolUse", servedSession, "echo a"));
      yield* s.moment(claude("PostToolUse", servedSession, "echo a"));
      assert.strictEqual(cue.requests.length, 1);
      const items = cue.requests[0]!["items"] as Array<[string, string]>;
      const users = items.filter(([kind]) => kind === "user").map(([, text]) => text);
      assert.deepStrictEqual(users, ["Earlier request", "Find out why the projector retries"]);
      // The tool tail is bounded and newest-last.
      const tools = items.filter(([kind]) => kind === "tool");
      assert.isAtMost(tools.length, 100);
      assert.strictEqual(tools.at(-1)![1], "command_execution: ls 452");
      assert.strictEqual(items.indexOf(items.find(([k]) => k === "tool")!), 2);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live(
    "logs a request the Cue service refuses as rejected, with its reason, and keeps exact keys",
    () =>
      Effect.gen(function* () {
        const cue = yield* fakeCue({}, { status: 400, error: "NoMessage: no user message" });
        const s = yield* serve(
          Effect.all([
            insertAttempt({ attempt: "a", run: "run-1", status: "running", native: servedSession }),
            projectUserMessage("Ship it"),
          ]).pipe(Effect.asVoid),
          { cueUrl: cue.url },
        );
        yield* s.moment(claude("PreToolUse", servedSession, "echo a"));
        const result = yield* s.moment(claude("PostToolUse", servedSession, "echo a"));
        assert.include(result.body, "rule a");
        assert.deepStrictEqual(
          s.cueLog().moments.map((m) => m.cue_status),
          ["started", "rejected"],
        );
        // A refusal is about this request: the next moment still asks Cue.
        yield* s.moment({
          hook_event_name: "UserPromptSubmit",
          session_id: servedSession,
          cwd: "/tmp",
        });
        assert.strictEqual(s.cueLog().moments.at(-1)!.cue_status, "rejected");
        assert.strictEqual(cue.requests.length, 2);
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("does not ask Cue when the thread has no user message", () =>
    Effect.gen(function* () {
      const cue = yield* fakeCue({ "agentsmd-research": 2.5 });
      const s = yield* serve(
        insertAttempt({ attempt: "a", run: "run-1", status: "running", native: servedSession }),
        { cueUrl: cue.url },
      );
      yield* s.moment(claude("PreToolUse", servedSession, "echo a"));
      assert.strictEqual(s.cueLog().moments[0]!.cue_status, "no_window");
      assert.strictEqual(cue.requests.length, 0);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
