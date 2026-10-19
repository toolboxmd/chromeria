import * as NodeHttpPlatform from "@effect/platform-node/NodeHttpPlatform";
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  AuthCookieMirrorWriteScope,
  AuthSessionId,
  CookieMirrorReply,
  EnvironmentId,
  type CookieMirrorBatch,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { HttpRouter, HttpServer } from "effect/http";

import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as BrowserCookieMirror from "./BrowserCookieMirror.ts";
import { routeLayer } from "./CookieMirrorRoute.ts";

const decode = Schema.decodeUnknownSync(Schema.fromJsonString(CookieMirrorReply));
const environmentId = EnvironmentId.make("synthetic-environment");
const batch: CookieMirrorBatch = {
  environmentId,
  sequence: 0,
  profile: { id: "default", name: "Default", kind: "persistent" },
  mutations: [
    {
      type: "set",
      cookie: {
        name: "login",
        value: "synthetic-private-value",
        domain: "example.test",
        path: "/",
        secure: true,
        httpOnly: true,
        sameSite: "unspecified",
      },
    },
  ],
};

it.effect(
  "acknowledges over the private socket only after apply, forwards context resets and closes on revocation",
  () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const applied = yield* Deferred.make<void>();
      const revoked = yield* Deferred.make<void>();
      const resets = yield* PubSub.unbounded<string>();
      const received = yield* Queue.make<ReturnType<typeof decode>>();
      const batches: CookieMirrorBatch[] = [];
      const auth = Layer.mock(EnvironmentAuth.EnvironmentAuth, {
        authenticateWebSocketUpgrade: () =>
          Effect.succeed({
            sessionId: AuthSessionId.make("synthetic-main-session"),
            subject: "synthetic",
            method: "bearer-access-token",
            scopes: [AuthCookieMirrorWriteScope],
          }),
      });
      const mirror = Layer.mock(BrowserCookieMirror.BrowserCookieMirror, {
        open: () =>
          Effect.succeed({
            invalidated: Deferred.await(revoked),
            resets: Stream.fromPubSub(resets),
            apply: (input) =>
              Deferred.succeed(started, undefined).pipe(
                Effect.andThen(Deferred.await(applied)),
                Effect.andThen(
                  Effect.sync(() => {
                    batches.push(input);
                  }),
                ),
              ),
          }),
      });
      const services = yield* Layer.build(
        HttpRouter.serve(
          routeLayer.pipe(
            Layer.provide(mirror),
            Layer.provide(auth),
            Layer.provide(
              Layer.mock(ServerEnvironment.ServerEnvironment, {
                getEnvironmentId: Effect.succeed(environmentId),
              }),
            ),
            Layer.provide(NodeHttpPlatform.layer.pipe(Layer.provideMerge(NodeServices.layer))),
          ),
          { disableListenLog: true },
        ).pipe(Layer.provideMerge(NodeHttpServer.layerTest)),
      );
      const server = Context.get(services, HttpServer.HttpServer);
      const origin = HttpServer.formatAddress(server.address).replace(/^http/, "ws");
      const socket = yield* Effect.acquireRelease(
        Effect.sync(
          () => new WebSocket(`${origin}/api/browser-cookie-mirror?wsTicket=synthetic-ticket`),
        ),
        (socket) => Effect.sync(() => socket.close()),
      );
      const closed = Promise.withResolvers<CloseEvent>();
      socket.addEventListener("close", (event) => closed.resolve(event));
      socket.addEventListener("message", (event) => {
        Queue.offerUnsafe(received, decode(event.data));
      });
      expect(yield* Queue.take(received)).toEqual({ type: "ready", environmentId });
      socket.send(JSON.stringify(batch));
      yield* Deferred.await(started);
      expect(yield* Queue.size(received)).toBe(0);
      yield* Deferred.succeed(applied, undefined);
      expect(yield* Queue.take(received)).toEqual({ type: "ack", sequence: 0 });
      expect(batches).toEqual([batch]);
      yield* PubSub.publish(resets, "default");
      expect(yield* Queue.take(received)).toEqual({ type: "resync", profileId: "default" });
      yield* Deferred.succeed(revoked, undefined);
      yield* Effect.promise(() => closed.promise);
    }).pipe(Effect.scoped),
);
