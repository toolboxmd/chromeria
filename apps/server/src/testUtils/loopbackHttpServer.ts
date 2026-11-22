import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- NodeHttpServer.layer takes the node:http factory.
import * as NodeHttp from "node:http";
import * as Layer from "effect/Layer";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpServer from "effect/http/HttpServer";

/**
 * `NodeHttpServer.layerTest` bound to 127.0.0.1. `layerTest` binds the wildcard, which leaves
 * 127.0.0.1 on that port free for another process on macOS, while the peer testkit and Effect's
 * test client both dial 127.0.0.1, so a stray listener can answer the test's requests.
 */
export const layerLoopbackServerTest = HttpServer.layerTestClient.pipe(
  Layer.provide(
    Layer.fresh(FetchHttpClient.layer).pipe(
      Layer.provide(Layer.succeed(FetchHttpClient.RequestInit)({ keepalive: false })),
    ),
  ),
  Layer.provideMerge(NodeHttpServer.layer(NodeHttp.createServer, { port: 0, host: "127.0.0.1" })),
);
