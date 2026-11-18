import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { AgentScope } from "@t3tools/shared/AgentScope";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Redacted from "effect/Redacted";
import * as Scope from "effect/Scope";
import * as TestClock from "effect/testing/TestClock";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import { describe } from "vite-plus/test";

import * as OpenCodeRuntime from "../OpenCodeRuntime.ts";
import * as OpenCodeServerLedger from "../OpenCodeServerLedger.ts";
import * as OpenCode2Client from "./OpenCode2Client.ts";
import * as OpenCode2Server from "./OpenCode2Server.ts";

// Shapes and content types observed on `opencode serve` 2.0.18 (pid and port replaced). 1.x
// answers /api/info with its web UI: 200 text/html.
const UNAUTHORIZED_BODY = '{"_tag":"UnauthorizedError","message":"Authentication required"}';
const SPA_BODY = "<!doctype html><html><head><title>OpenCode</title></head></html>";

const layerServerReplying = (reply: {
  readonly status: number;
  readonly contentType?: string;
  readonly body: string;
}) =>
  Layer.succeed(
    HttpClient.HttpClient,
    HttpClient.make((request) =>
      Effect.succeed(
        HttpClientResponse.fromWeb(
          request,
          new Response(reply.body, {
            status: reply.status,
            headers: reply.contentType === undefined ? {} : { "content-type": reply.contentType },
          }),
        ),
      ),
    ),
  );

const verify = (httpClient: Layer.Layer<HttpClient.HttpClient>, url = "http://127.0.0.1:4096") =>
  Effect.gen(function* () {
    const opencode = yield* OpenCode2Client.OpenCode2Client;
    const { client } = yield* opencode.connect({ baseUrl: url, password: "secret" });
    return yield* OpenCode2Server.verifyServer(client);
  }).pipe(Effect.provide(OpenCode2Client.layer.pipe(Layer.provide(httpClient))));

describe("OpenCode2Server.verifyServer", () => {
  it.effect("reports a rejected password, not a wrong server", () =>
    Effect.gen(function* () {
      const error = yield* verify(
        layerServerReplying({
          status: 401,
          contentType: "application/json",
          body: UNAUTHORIZED_BODY,
        }),
      ).pipe(Effect.flip);
      assert.include(error.detail, "rejected the server password");
    }),
  );

  // 1.x rejects a wrong password with an empty 401, which the client cannot decode.
  it.effect("reports an empty-body 401 as a rejected password", () =>
    Effect.gen(function* () {
      const error = yield* verify(layerServerReplying({ status: 401, body: "" })).pipe(Effect.flip);
      assert.include(error.detail, "rejected the server password");
    }),
  );

  it.effect("reports a server error as a server error, not as unreachable", () =>
    Effect.gen(function* () {
      for (const status of [500, 502]) {
        const error = yield* verify(
          layerServerReplying({ status, contentType: "text/plain", body: "upstream failed" }),
        ).pipe(Effect.flip);
        assert.include(error.detail, `returned HTTP ${status}`);
      }
    }),
  );

  it.effect("rejects a server that answers with the web UI's HTML", () =>
    Effect.gen(function* () {
      const error = yield* verify(
        layerServerReplying({ status: 200, contentType: "text/html", body: SPA_BODY }),
      ).pipe(Effect.flip);
      assert.include(error.detail, "is not an OpenCode 2 server");
    }),
  );
});

describe("OpenCode2Server error details", () => {
  const serverUrl = "http://user:url-secret@127.0.0.1:1/?token=query-secret";
  const detailFor = (httpClient: Layer.Layer<HttpClient.HttpClient>) =>
    Effect.gen(function* () {
      const server = yield* OpenCode2Server.make({
        binaryPath: "opencode",
        serverUrl,
        serverPassword: "secret",
        directory: "/project",
        environment: {},
      });
      const error = yield* server.withConnection(() => Effect.void).pipe(Effect.flip);
      return error.detail;
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          OpenCode2Client.layer.pipe(Layer.provide(httpClient)),
          OpenCodeRuntime.layer.pipe(Layer.provide(OpenCodeServerLedger.layerTest)),
        ).pipe(Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
      ),
    );

  it.effect("never include the server URL", () =>
    Effect.gen(function* () {
      const layerHanging = Layer.succeed(
        HttpClient.HttpClient,
        HttpClient.make(() => Effect.never),
      );
      const timedOut = yield* detailFor(layerHanging).pipe(Effect.forkChild);
      yield* TestClock.adjust("10 seconds");
      const details = [
        yield* detailFor(FetchHttpClient.layer),
        yield* detailFor(layerServerReplying({ status: 401, body: "" })),
        yield* detailFor(layerServerReplying({ status: 502, contentType: "text/plain", body: "" })),
        yield* detailFor(
          layerServerReplying({ status: 200, contentType: "text/html", body: SPA_BODY }),
        ),
        yield* Fiber.join(timedOut),
      ];
      assert.deepStrictEqual(details, [
        "Could not reach the OpenCode server.",
        "The OpenCode server rejected the server password.",
        "The OpenCode server returned HTTP 502.",
        "The server is not an OpenCode 2 server.",
        "Timed out waiting for the OpenCode server.",
      ]);
      for (const detail of details) {
        assert.notInclude(detail, "url-secret");
        assert.notInclude(detail, "query-secret");
        assert.notInclude(detail, "127.0.0.1");
      }
    }).pipe(Effect.provide(TestClock.layer())),
  );
});

describe("OpenCode2Server external server", () => {
  it.effect("reverifies after forgetting, so a replaced service's version is seen", () =>
    Effect.gen(function* () {
      let version = "2.2.4";
      const layerService = Layer.succeed(
        HttpClient.HttpClient,
        HttpClient.make((request) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              new Response(JSON.stringify({ version, pid: 1, urls: [], paths: { tmp: "/tmp" } }), {
                status: 200,
                headers: { "content-type": "application/json" },
              }),
            ),
          ),
        ),
      );
      const versions = yield* Effect.gen(function* () {
        const server = yield* OpenCode2Server.make({
          binaryPath: "opencode",
          serverUrl: "http://127.0.0.1:49374",
          serverPassword: "secret",
          directory: "/project",
          environment: {},
        });
        const read = server.withConnection((connection) => Effect.succeed(connection.version));
        const before = yield* read;
        version = "2.2.5";
        const cached = yield* read;
        yield* server.forget;
        return [before, cached, yield* read];
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            OpenCode2Client.layer.pipe(Layer.provide(layerService)),
            OpenCodeRuntime.layer.pipe(Layer.provide(OpenCodeServerLedger.layerTest)),
          ).pipe(Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
        ),
      );
      assert.deepStrictEqual(versions, ["2.2.4", "2.2.4", "2.2.5"]);
    }),
  );
});

describe("OpenCode2Server passwords", () => {
  it("hands the spawned server only the T3 password", () => {
    const password = Redacted.make("t3-generated");
    const environment = OpenCode2Server.serverEnvironment(
      { PATH: "/bin", OPENCODE_SERVER_PASSWORD: "ambient", OPENCODE_PASSWORD: "ambient" },
      password,
    );
    assert.deepStrictEqual(environment, { PATH: "/bin", OPENCODE_PASSWORD: "t3-generated" });
  });
});

const FAKE_SERVER = `import { createServer } from "node:http";
const expected = "Basic " + Buffer.from("opencode:" + process.env.OPENCODE_PASSWORD).toString("base64");
const server = createServer((request, response) => {
  if (request.headers.authorization !== expected) {
    response.writeHead(401, { "content-type": "application/json", "www-authenticate": "Basic" });
    response.end(${JSON.stringify(UNAUTHORIZED_BODY)});
    return;
  }
  response.writeHead(200, { "content-type": "application/json" });
  response.end(JSON.stringify({ version: "2.0.18", pid: process.pid, urls: [], paths: { tmp: "/tmp" } }));
});
server.listen(0, "127.0.0.1", () => {
  process.stdout.write("server listening on http://127.0.0.1:" + server.address().port + "\\n");
  process.stdout.write("server password not-the-t3-password\\n");
});
`;

// OpenCode 2.3.2's output when its port is already bound, written by the first launch only.
const PORT_TAKEN_ONCE = `import { existsSync, writeFileSync } from "node:fs";
const bound = new URL("./port-taken", import.meta.url);
if (!existsSync(bound)) {
  writeFileSync(bound, "");
  process.stderr.write("ServeError:\\n  [cause]: Error: Failed to start server. Is port 4096 in use?\\n");
  process.exit(1);
}
`;

const verifyLive = (connection: OpenCode2Server.OpenCode2Connection) =>
  OpenCode2Server.verifyServer(connection.client);

const writeFakeOpenCode = (script: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const platform = yield* HostProcess.Platform;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-opencode2-fake-" });
    const isWindows = platform === "win32";
    const binaryPath = path.join(directory, isWindows ? "opencode.cmd" : "opencode");
    const scriptPath = path.join(directory, "opencode.mjs");
    yield* fs.writeFileString(scriptPath, script);
    yield* fs.writeFileString(
      binaryPath,
      isWindows
        ? `@echo off\r\n"${yield* HostProcess.ExecutablePath}" "${scriptPath}" %*\r\n`
        : `#!/bin/sh\nexec "${yield* HostProcess.ExecutablePath}" "${scriptPath}" "$@"\n`,
    );
    if (!isWindows) yield* fs.chmod(binaryPath, 0o755);
    return { binaryPath, directory };
  });

const layerSpawning = (
  wrapped?: Array<{ readonly threadId?: string; readonly shared?: boolean }>,
) =>
  Layer.mergeAll(
    OpenCode2Client.layer,
    OpenCodeRuntime.layer.pipe(
      Layer.provide(OpenCodeServerLedger.layerTest),
      Layer.provide(
        wrapped === undefined
          ? Layer.empty
          : Layer.succeed(AgentScope, {
              ...AgentScope.defaultValue(),
              wrap: ({ command, args, threadId, shared }) =>
                Effect.sync(() => {
                  wrapped.push({
                    ...(threadId === undefined ? {} : { threadId }),
                    ...(shared === undefined ? {} : { shared }),
                  });
                  return { command, args };
                }),
            }),
      ),
    ),
  ).pipe(
    Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer, FetchHttpClient.layer)),
  );

describe("OpenCode2Server spawned server", () => {
  it.live("is ready once /api/info accepts the generated password", () =>
    Effect.gen(function* () {
      const { binaryPath, directory } = yield* writeFakeOpenCode(FAKE_SERVER);
      const server = yield* OpenCode2Server.make({
        binaryPath,
        serverUrl: "",
        serverPassword: "",
        directory,
        environment: { ...(yield* HostProcess.Environment), OPENCODE_SERVER_PASSWORD: "ambient" },
      });
      const first = yield* server.withConnection((connection) => Effect.succeed(connection));
      const second = yield* server.withConnection((connection) => Effect.succeed(connection));
      assert.strictEqual(first.version, "2.0.18");
      assert.strictEqual(first.external, false);
      assert.strictEqual(second.client, first.client);
      if (first.processExit === undefined)
        return yield* Effect.die("Missing owned process lifetime");
      return first.processExit;
    }).pipe(Effect.scoped, Effect.flatten, Effect.provide(layerSpawning())),
  );

  // The launcher picks a free port before OpenCode binds it, so a concurrent spawn can take it first.
  it.live("starts again when another process took its port first", () =>
    Effect.gen(function* () {
      const { binaryPath, directory } = yield* writeFakeOpenCode(PORT_TAKEN_ONCE + FAKE_SERVER);
      const server = yield* OpenCode2Server.make({
        binaryPath,
        serverUrl: "",
        serverPassword: "",
        directory,
        environment: yield* HostProcess.Environment,
      });
      const version = yield* server.withConnection((connection) =>
        Effect.succeed(connection.version),
      );
      assert.strictEqual(version, "2.0.18");
    }).pipe(Effect.scoped, Effect.provide(layerSpawning())),
  );

  it.live("runs each session on its own process until its session or the instance closes", () => {
    const wrapped: Array<{ readonly threadId?: string; readonly shared?: boolean }> = [];
    return Effect.gen(function* () {
      const { binaryPath, directory } = yield* writeFakeOpenCode(FAKE_SERVER);
      const instance = yield* Scope.make();
      const server = yield* OpenCode2Server.make({
        binaryPath,
        serverUrl: "",
        serverPassword: "",
        directory,
        environment: yield* HostProcess.Environment,
      }).pipe(Effect.provideService(Scope.Scope, instance));
      const forSession = server.forSession;
      if (forSession === undefined) return yield* Effect.die("Sessions share the server");
      const firstScope = yield* Scope.make();
      const secondScope = yield* Scope.make();
      const first = yield* forSession("thread:first").pipe(
        Effect.provideService(Scope.Scope, firstScope),
      );
      const second = yield* forSession("thread:second").pipe(
        Effect.provideService(Scope.Scope, secondScope),
      );
      const connect = (session: OpenCode2Server.OpenCode2SessionServer) =>
        session.withConnection((connection) => Effect.succeed(connection));
      const one = yield* connect(first);
      const two = yield* connect(second);
      assert.notStrictEqual(one.url, two.url);
      // A reconnect borrows the session's running process again.
      assert.strictEqual((yield* connect(first)).url, one.url);
      // Each process runs in its thread's scope, so Force stop can reach it.
      assert.deepStrictEqual(wrapped, [
        { threadId: "thread:first" },
        { threadId: "thread:second" },
      ]);
      if (one.processExit === undefined || two.processExit === undefined)
        return yield* Effect.die("Missing owned process lifetime");

      yield* Scope.close(firstScope, Exit.void);
      yield* one.processExit;
      assert.strictEqual(
        yield* second.withConnection((connection) => verifyLive(connection)),
        "2.0.18",
      );

      // Disabling or replacing the instance stops sessions that are still open.
      yield* Scope.close(instance, Exit.void);
      yield* two.processExit;
      const error = yield* connect(second).pipe(Effect.flip);
      assert.include(error.detail, "closed");
      assert.lengthOf(wrapped, 2);
    }).pipe(Effect.scoped, Effect.provide(layerSpawning(wrapped)));
  });
});
