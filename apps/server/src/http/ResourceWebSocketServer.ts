import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import { NodeWS } from "@effect/platform-node/NodeSocket";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import { HttpServer } from "effect/http";
import type * as HttpMiddleware from "effect/http/HttpMiddleware";
import type * as HttpServerResponse from "effect/http/HttpServerResponse";

const RESOURCE_MAX_PAYLOAD = 8 * 1024 * 1024;

// This chooses a parser, never authorization. Include malformed resource suffixes
// so they cannot reach the larger RPC parser before the actual router refuses them.
const isResourceUpgrade = (url: string | undefined) => {
  try {
    const path = decodeURIComponent(new URL(url ?? "/", "http://resource.invalid").pathname);
    return path.startsWith("/api/device-hub") || path.startsWith("/api/preview-stream");
  } catch {
    // Malformed request targets never need the larger RPC parser.
    return true;
  }
};

const make = Effect.fn("ResourceWebSocketServer.make")(function* (
  evaluate: Parameters<typeof NodeHttpServer.make>[0],
  options: NodeHttpServer.Options,
) {
  const server = evaluate();
  const native = yield* NodeHttpServer.make(() => server, options);
  const serve = <E, R>(
    app: Effect.Effect<HttpServerResponse.HttpServerResponse, E, R>,
    middleware?: HttpMiddleware.HttpMiddleware,
  ) =>
    Effect.gen(function* () {
      const serveScope = yield* Effect.scope;
      const scope = Scope.forkUnsafe(serveScope, "parallel");
      const websocketServer = (resource: boolean) =>
        Effect.acquireRelease(
          Effect.sync(
            () =>
              new NodeWS.WebSocketServer({
                ...options.websocket,
                ...(resource ? { maxPayload: RESOURCE_MAX_PAYLOAD } : {}),
                noServer: true,
              }),
          ),
          (wss) =>
            Effect.callback<void>((resume) => {
              for (const client of wss.clients) client.terminate();
              wss.close(() => resume(Effect.void));
            }),
        );
      const ordinary = yield* websocketServer(false);
      const resource = yield* websocketServer(true);
      const handler = yield* NodeHttpServer.makeHandler<
        R,
        E,
        Effect.Effect<HttpServerResponse.HttpServerResponse, E, R>
      >(app, { middleware, scope });
      const ordinaryUpgrade = yield* NodeHttpServer.makeUpgradeHandler<
        R,
        E,
        Effect.Effect<HttpServerResponse.HttpServerResponse, E, R>
      >(Effect.succeed(ordinary), app, { middleware, scope });
      const resourceUpgrade = yield* NodeHttpServer.makeUpgradeHandler<
        R,
        E,
        Effect.Effect<HttpServerResponse.HttpServerResponse, E, R>
      >(Effect.succeed(resource), app, { middleware, scope });
      const upgrade: typeof ordinaryUpgrade = (request, socket, head) =>
        (isResourceUpgrade(request.url) ? resourceUpgrade : ordinaryUpgrade)(request, socket, head);
      yield* Scope.addFinalizer(
        serveScope,
        Effect.suspend(() => {
          const shutdown = options.disablePreemptiveShutdown
            ? Effect.void
            : Effect.callback<void>((resume) => {
                if (!server.listening) return resume(Effect.void);
                server.close((error) => resume(error ? Effect.die(error) : Effect.void));
              }).pipe(
                Effect.timeoutOrElse({
                  duration: options.gracefulShutdownTimeout ?? "20 seconds",
                  orElse: () => Effect.void,
                }),
              );
          return shutdown.pipe(
            Effect.ensuring(
              Effect.sync(() => {
                server.off("request", handler);
                server.off("upgrade", upgrade);
              }),
            ),
          );
        }),
      );
      server.on("request", handler);
      server.on("upgrade", upgrade);
    });
  return HttpServer.make({ address: native.address, serve });
});

export const layer = (
  evaluate: Parameters<typeof NodeHttpServer.make>[0],
  options: NodeHttpServer.Options,
) =>
  Layer.mergeAll(
    Layer.effect(HttpServer.HttpServer, make(evaluate, options)),
    NodeHttpServer.layerHttpServices,
  );
