import * as NodeServices from "@effect/platform-node/NodeServices";
import { NodeHttpServer } from "@effect/platform-node";
import { assert, it } from "@effect/vitest";
import * as Net from "@t3tools/shared/Net";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import * as TestConsole from "effect/testing/TestConsole";
import { Command } from "effect/cli";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import { cli } from "../binCli.ts";
import { descriptorOf, servePeer } from "./PeerLinks.testkit.ts";

const run = (args: ReadonlyArray<string>) => Command.runWith(cli, { version: "test" })(args);
const output = TestConsole.logLines.pipe(
  Effect.map((lines) => lines.findLast((line): line is string => typeof line === "string") ?? ""),
);

it.effect("links, lists and unlinks headlessly; CLI revocation cuts off a permanent session", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-peer-cli-" });
      const peer = yield* servePeer(descriptorOf("environment-cli-peer", "CLI peer"), Layer.empty, {
        persisted: true,
      });
      const code = yield* peer.auth.issuePairingCredential();
      yield* run([
        "environment",
        "link",
        peer.url,
        "--pairing-code",
        code.credential,
        "--access",
        "full-access",
        "--base-dir",
        baseDir,
      ]);
      assert.include(yield* output, "Linked CLI peer");
      assert.notInclude(yield* output, code.credential);
      yield* run(["environment", "list", "--base-dir", baseDir, "--json"]);
      assert.include(yield* output, '"status":"reachable"');
      assert.notInclude(yield* output, "access_token");
      // Reopen the source CLI at the advanced clock, using its persisted link.
      yield* TestClock.adjust(Duration.days(3653));
      yield* run(["environment", "list", "--base-dir", baseDir, "--json"]);
      assert.include(yield* output, '"status":"reachable"');
      const session = (yield* peer.linkedSessions)[0]!;
      const issued = yield* peer.auth.issueMcpClientSession({
        label: "CLI revoke proof",
        access: "full-access",
        peerLink: true,
        permanent: true,
        client: { deviceType: "bot" },
      });
      const request = HttpServerRequest.fromWeb(
        new Request(`${peer.url}/mcp`, { headers: { authorization: `Bearer ${issued.token}` } }),
      );
      const direct = yield* peer.auth.authenticateMcpClient(request);
      yield* run(["auth", "session", "revoke", direct.sessionId, "--base-dir", peer.baseDir]);
      assert.isTrue(
        (yield* peer.auth.authenticateMcpClient(request).pipe(Effect.result))._tag === "Failure",
      );
      yield* run(["auth", "session", "revoke", session.sessionId, "--base-dir", peer.baseDir]);
      assert.isEmpty(yield* peer.linkedSessions);
      yield* run(["environment", "unlink", "environment-cli-peer", "--base-dir", baseDir]);
      assert.include(yield* output, "Forgot environment-cli-peer");
      yield* run(["environment", "list", "--base-dir", baseDir, "--json"]);
      assert.equal(yield* output, "[]\n");
    }),
  ).pipe(
    Effect.provide(
      Layer.mergeAll(NodeServices.layer, NodeHttpServer.layerTest, Net.layer, TestConsole.layer),
    ),
  ),
);
