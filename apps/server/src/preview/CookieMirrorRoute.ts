import {
  AuthCookieMirrorWriteScope,
  COOKIE_MIRROR_GRANT_PATH,
  COOKIE_MIRROR_PATH,
  CookieMirrorBatch,
  CookieMirrorReply,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Layer from "effect/Layer";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";
import * as Socket from "effect/socket/Socket";

import { authenticateMediaRequest } from "../auth/http.ts";
import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as BrowserCookieMirror from "./BrowserCookieMirror.ts";

const decodeBatch = Schema.decodeUnknownEffect(Schema.fromJsonString(CookieMirrorBatch));
const encodeReply = Schema.encodeEffect(Schema.fromJsonString(CookieMirrorReply));
const rejected = () =>
  Effect.succeed(
    HttpServerResponse.empty({ status: 403, headers: { "cache-control": "no-store" } }),
  );

export const routeLayer = Layer.unwrap(
  Effect.gen(function* () {
    const mirror = yield* BrowserCookieMirror.BrowserCookieMirror;
    const auth = yield* EnvironmentAuth.EnvironmentAuth;
    const environment = yield* ServerEnvironment.ServerEnvironment;
    const environmentId = yield* environment.getEnvironmentId;
    const grantRoute = HttpRouter.add(
      "POST",
      COOKIE_MIRROR_GRANT_PATH,
      Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        const session = yield* auth.authenticateHttpRequest(request);
        const grant = yield* mirror.grant(session);
        return yield* HttpServerResponse.json(grant, { headers: { "cache-control": "no-store" } });
      }).pipe(Effect.catchCause(rejected)),
    );
    const socketRoute = HttpRouter.add(
      "GET",
      COOKIE_MIRROR_PATH,
      Effect.gen(function* () {
        const session = yield* authenticateMediaRequest(AuthCookieMirrorWriteScope).pipe(
          Effect.provideService(EnvironmentAuth.EnvironmentAuth, auth),
        );
        const stream = yield* mirror.open(session);
        const request = yield* HttpServerRequest.HttpServerRequest;
        const socket = yield* request.upgrade;
        const writer = yield* socket.writer;
        const reader = yield* socket.reader;
        yield* writer.write(yield* encodeReply({ type: "ready", environmentId }));
        yield* stream.resets.pipe(
          Stream.runForEach((profileId) =>
            encodeReply({ type: "resync", profileId }).pipe(
              Effect.flatMap((message) => writer.write(message)),
            ),
          ),
          Effect.forkScoped,
        );
        const receive = (chunk: string | Uint8Array) =>
          Effect.gen(function* () {
            // Decode failures contain their input; replace them before request instrumentation.
            if (chunk.length > 4 * 1024 * 1024) return yield* Effect.fail("invalid mirror frame");
            const batch = yield* decodeBatch(
              typeof chunk === "string" ? chunk : new TextDecoder().decode(chunk),
            ).pipe(Effect.mapError(() => "invalid mirror frame"));
            yield* stream.apply(batch);
            yield* writer.write(yield* encodeReply({ type: "ack", sequence: batch.sequence }));
          });
        yield* reader.pull.pipe(
          Effect.flatMap((chunks) => Effect.forEach(chunks, receive, { discard: true })),
          Effect.forever,
          Effect.raceFirst(stream.invalidated),
          Effect.catchCause(() =>
            writer.write(new Socket.CloseEvent(1008, "Browser mirror stopped.")),
          ),
        );
        return HttpServerResponse.empty();
      }).pipe(Effect.scoped, Effect.catchCause(rejected)),
    );
    return Layer.mergeAll(grantRoute, socketRoute);
  }),
);
