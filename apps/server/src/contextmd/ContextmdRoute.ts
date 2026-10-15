/**
 * HTTP transport for ContextMD serving: `POST /contextmd/v1/moment` takes one
 * raw harness hook event and answers with the bytes the hook prints (200) or
 * nothing (204); `POST /contextmd/v1/settle/<delivery>` confirms the hook wrote
 * that reply; `POST /contextmd/v1/release/<delivery>` frees a reply the hook
 * dropped before the model saw it.
 *
 * Hooks authenticate like any other client: a bearer session issued by this
 * environment (`t3 auth session issue`, run by `contextmd-serve install`) with
 * `orchestration:read`, checked by `EnvironmentAuth`.
 */
import * as ByteSize from "effect/ByteSize";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";
import * as HttpIncomingMessage from "effect/http/HttpIncomingMessage";

import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as ContextmdServing from "./ContextmdServing.ts";

const PREFIX = "/contextmd/v1";

export const layer = HttpRouter.use((router) =>
  Effect.gen(function* () {
    const serving = yield* ContextmdServing.ContextmdServing;
    const auth = yield* EnvironmentAuth.EnvironmentAuth;
    const authorized = (request: HttpServerRequest.HttpServerRequest) =>
      auth.authenticateHttpRequest(request).pipe(
        Effect.map((session) => session.scopes.includes("orchestration:read")),
        Effect.orElseSucceed(() => false),
        // Receipt hooks have only 300 ms after printing. Their auth lookup must not queue on writers.
        NodeSqliteClient.readOnly,
      );
    const unauthorized = (request: HttpServerRequest.HttpServerRequest) =>
      serving
        .rejected({
          harness: request.headers["x-contextmd-harness"],
          delivery: request.headers["x-contextmd-delivery"],
        })
        .pipe(Effect.as(HttpServerResponse.empty({ status: 401 })));

    yield* router.add(
      "POST",
      `${PREFIX}/moment`,
      Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        const length = request.headers["content-length"];
        const reply = yield* serving.moment({
          authorized: authorized(request),
          harness: request.headers["x-contextmd-harness"],
          delivery: request.headers["x-contextmd-delivery"],
          contentLength: length === undefined ? undefined : Number(length),
          pointerDelivery: request.headers["x-contextmd-pointer-delivery"],
          // Chunked requests carry no content-length, so the reader itself is capped.
          body: request.text.pipe(
            Effect.provideService(
              HttpIncomingMessage.MaxBodySize,
              ByteSize.bytes(ContextmdServing.MAX_BODY_BYTES),
            ),
            Effect.orElseSucceed(() => null),
          ),
        });
        return reply.status === 200
          ? HttpServerResponse.text(reply.body, { status: 200, contentType: "application/json" })
          : HttpServerResponse.empty({ status: reply.status });
      }).pipe(Effect.withTracerEnabled(false)),
    );
    yield* router.add(
      "POST",
      `${PREFIX}/settle/*`,
      Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        if (!(yield* authorized(request))) return yield* unauthorized(request);
        const url = HttpServerRequest.toURL(request);
        if (Option.isNone(url)) return HttpServerResponse.empty({ status: 400 });
        const reply = yield* serving.settle(url.value.pathname.slice(`${PREFIX}/settle/`.length));
        return HttpServerResponse.empty({ status: reply.status });
      }).pipe(Effect.withTracerEnabled(false)),
    );
    yield* router.add(
      "POST",
      `${PREFIX}/release/*`,
      Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        if (!(yield* authorized(request))) return yield* unauthorized(request);
        const url = HttpServerRequest.toURL(request);
        if (Option.isNone(url)) return HttpServerResponse.empty({ status: 400 });
        const reply = yield* serving.release(url.value.pathname.slice(`${PREFIX}/release/`.length));
        return HttpServerResponse.empty({ status: reply.status });
      }).pipe(Effect.withTracerEnabled(false)),
    );
    for (const action of ["applying", "settle", "release"] as const) {
      yield* router.add(
        "POST",
        `${PREFIX}/pointer/${action}/*`,
        Effect.gen(function* () {
          const request = yield* HttpServerRequest.HttpServerRequest;
          if (!(yield* authorized(request))) return yield* unauthorized(request);
          const url = HttpServerRequest.toURL(request);
          if (Option.isNone(url)) return HttpServerResponse.empty({ status: 400 });
          const delivery = url.value.pathname.slice(`${PREFIX}/pointer/${action}/`.length);
          const reply = yield* action === "applying"
            ? serving.pointerApplying(delivery)
            : action === "settle"
              ? serving.pointerSettle(delivery)
              : serving.pointerRelease(
                  delivery,
                  url.value.searchParams.get("reason") ?? "client_release",
                );
          return HttpServerResponse.empty({ status: reply.status });
        }).pipe(
          Effect.withTracerEnabled(false),
          Effect.catchTags({
            ContextmdPointerDeliveryError: () =>
              Effect.succeed(HttpServerResponse.empty({ status: 500 })),
          }),
        ),
      );
    }
  }),
);
