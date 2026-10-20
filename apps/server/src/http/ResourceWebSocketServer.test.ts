// @effect-diagnostics nodeBuiltinImport:off
import * as NodeHttp from "node:http";
import { NodeWS } from "@effect/platform-node/NodeSocket";
import { expect, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import {
  FetchHttpClient,
  HttpClient,
  HttpServer,
  HttpServerRequest,
  HttpServerResponse,
} from "effect/http";
import { guardHttpResponseWriteErrors } from "../httpResponseErrorGuard.ts";
import * as ResourceWebSocketServer from "./ResourceWebSocketServer.ts";

const fixture = Effect.gen(function* () {
  const scope = yield* Scope.make();
  const native = guardHttpResponseWriteErrors(NodeHttp.createServer());
  const context = yield* Layer.buildWithScope(
    ResourceWebSocketServer.layer(() => native, {
      host: "127.0.0.1",
      port: 0,
      gracefulShutdownTimeout: "10 millis",
      websocket: { perMessageDeflate: true },
    }),
    scope,
  );
  const pendingStarted = Promise.withResolvers<void>();
  const pendingEnded = Promise.withResolvers<void>();
  let messages = 0;
  let requests = 0;
  const app = Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    requests++;
    if (request.headers.authorization !== "Bearer finite-parser-fixture")
      return HttpServerResponse.empty({ status: 403 });
    if (request.url === "/pending") {
      yield* Effect.addFinalizer(() => Effect.sync(() => pendingEnded.resolve()));
      pendingStarted.resolve();
      return yield* Effect.interruptible(Effect.never);
    }
    if (request.headers.upgrade !== "websocket") return HttpServerResponse.text("http-ok");
    const socket = yield* request.upgrade;
    const reader = yield* socket.reader;
    const writer = yield* socket.writer;
    while (true) {
      const batch = yield* reader.pull;
      for (const chunk of batch) {
        messages++;
        yield* writer.write(chunk);
      }
    }
  });
  const server = Context.get(context, HttpServer.HttpServer);
  yield* server.serve(app).pipe(Effect.provide(context), Effect.provideService(Scope.Scope, scope));
  let closed = false;
  const close = Effect.suspend(() => {
    if (closed) return Effect.void;
    closed = true;
    return Scope.close(scope, Exit.void);
  });
  yield* Effect.addFinalizer(() => close);
  return {
    origin: HttpServer.formatAddress(server.address),
    close,
    native,
    pendingStarted: pendingStarted.promise,
    pendingEnded: pendingEnded.promise,
    messages: () => messages,
    requests: () => requests,
  };
});
const connect = (origin: string, path: string, compressed = false) =>
  Effect.acquireRelease(
    Effect.promise(
      () =>
        new Promise<NodeWS.WebSocket>((resolve, reject) => {
          const socket = new NodeWS.WebSocket(`${origin.replace("http:", "ws:")}${path}`, {
            headers: { authorization: "Bearer finite-parser-fixture" },
            perMessageDeflate: compressed,
          });
          socket.on("error", () => {});
          socket.once("open", () => resolve(socket));
          socket.once("error", reject);
        }),
    ),
    (socket) => Effect.sync(() => socket.terminate()),
  );
const next = (socket: NodeWS.WebSocket) =>
  new Promise<
    { kind: "message"; bytes: Buffer; binary: boolean } | { kind: "close"; code: number }
  >((resolve) => {
    const onMessage = (bytes: Buffer, binary: boolean) => {
      socket.off("close", onClose);
      resolve({ kind: "message", bytes, binary });
    };
    const onClose = (code: number) => {
      socket.off("message", onMessage);
      resolve({ kind: "close", code });
    };
    socket.once("message", onMessage);
    socket.once("close", onClose);
  });

it.live.each([
  ["single device", "/api/device-hub/api/devices/ws?viewerId=exact", false, false],
  ["fragmented browser", "/api/preview-stream/ws?viewerId=exact", true, false],
  ["inflated device", "/api/device-hub/api/devices/ws", false, true],
  ["encoded resource alias", "/api/%64evice-hub/api/devices/ws?target=foreign", false, false],
  ["normalized resource alias", "/ignored/../api/preview-stream/ws", false, false],
  ["malformed resource suffix", "/api/preview-stream-other?target=foreign", false, false],
] as const)("bounds %s before application dispatch", ([_name, path, fragmented, compressed]) =>
  Effect.scoped(
    Effect.gen(function* () {
      const server = yield* fixture;
      const socket = yield* connect(server.origin, path, compressed);
      const received = next(socket);
      if (fragmented) {
        socket.send(Buffer.alloc(5 * 1024 * 1024, 0x61), { fin: false, compress: false });
        socket.send(Buffer.alloc(4 * 1024 * 1024, 0x62), { fin: true, compress: false });
      } else socket.send(Buffer.alloc(9 * 1024 * 1024, 0x61), { compress: compressed });
      const observed = yield* Effect.promise(() => received);
      expect(observed.kind).toBe("close");
      if (observed.kind === "close") expect(observed.code).toBe(1009);
      expect(server.messages()).toBe(0);
      expect(server.requests()).toBe(1);
    }),
  ),
);

it.live(
  "retains exact binary frames, HTTP handling, compression and ordinary RPC above 8 MiB",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* fixture;
        for (const path of ["/api/device-hub/api/devices/ws", "/api/preview-stream/ws"]) {
          const socket = yield* connect(server.origin, path);
          const bytes = Buffer.from([0, 0, 0, 5, 1, 0xff, 0xd8, 0xff, 0xd9]);
          const received = next(socket);
          socket.send(bytes);
          expect(yield* Effect.promise(() => received)).toEqual({
            kind: "message",
            bytes,
            binary: true,
          });
          const boundaryBytes = Buffer.alloc(8 * 1024 * 1024, 0x64);
          const boundaryReply = next(socket);
          socket.send(boundaryBytes);
          const boundary = yield* Effect.promise(() => boundaryReply);
          expect(boundary.kind).toBe("message");
          if (boundary.kind === "message") expect(boundary.bytes.equals(boundaryBytes)).toBe(true);
          socket.close();
        }
        for (const compressed of [false, true]) {
          const socket = yield* connect(server.origin, "/ws", compressed);
          if (compressed) expect(socket.extensions).toBe("permessage-deflate");
          const bytes = Buffer.alloc(9 * 1024 * 1024, 0x63);
          const received = next(socket);
          socket.send(bytes, { compress: compressed });
          const response = yield* Effect.promise(() => received);
          expect(response.kind).toBe("message");
          if (response.kind === "message") expect(response.bytes.equals(bytes)).toBe(true);
          socket.close();
        }
        const denied = yield* HttpClient.get(`${server.origin}/api/preview-stream/ws`);
        expect(denied.status).toBe(403);
        const response = yield* HttpClient.get(server.origin, {
          headers: { authorization: "Bearer finite-parser-fixture" },
        });
        expect(yield* response.text).toBe("http-ok");
      }),
    ).pipe(Effect.provide(FetchHttpClient.layer)),
);

it.live("closes active resource and RPC sockets within scoped server shutdown", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const server = yield* fixture;
      const resource = yield* connect(server.origin, "/api/preview-stream/ws");
      const rpc = yield* connect(server.origin, "/ws");
      const resourceClosed = next(resource);
      const rpcClosed = next(rpc);
      yield* server.close;
      expect((yield* Effect.promise(() => resourceClosed)).kind).toBe("close");
      expect((yield* Effect.promise(() => rpcClosed)).kind).toBe("close");
      expect(server.native.listening).toBe(false);
      expect(server.native.listenerCount("upgrade")).toBe(1); // response-write guard only
      expect(server.native.listenerCount("request")).toBe(1);
    }),
  ),
);

it.live.each(["disconnect", "shutdown"] as const)(
  "interrupts an active HTTP request on %s",
  (mode) =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* fixture;
        const request = yield* Effect.acquireRelease(
          Effect.sync(() => {
            const request = NodeHttp.get(`${server.origin}/pending`, {
              headers: { authorization: "Bearer finite-parser-fixture" },
            });
            request.on("error", () => {});
            return request;
          }),
          (request) => Effect.sync(() => request.destroy()),
        );
        yield* Effect.promise(() => server.pendingStarted);
        if (mode === "disconnect") request.destroy();
        else yield* server.close;
        yield* Effect.promise(() => server.pendingEnded);
        if (mode === "disconnect") {
          const response = yield* HttpClient.get(server.origin, {
            headers: { authorization: "Bearer finite-parser-fixture" },
          });
          expect(yield* response.text).toBe("http-ok");
        } else expect(server.native.listening).toBe(false);
      }),
    ).pipe(Effect.provide(FetchHttpClient.layer)),
);
