import {
  AuthCookieMirrorWriteScope,
  DEFAULT_PERSON,
  CookieMirrorError,
  type AuthSessionId,
  CookieMirrorBatch,
  type CookieMirrorGrant,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import type * as Stream from "effect/Stream";

import type { AuthenticatedSession } from "../auth/EnvironmentAuth.ts";
import * as SessionStore from "../auth/SessionStore.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ServerBrowser from "./ServerBrowser.ts";

export class BrowserCookieMirror extends Context.Service<
  BrowserCookieMirror,
  {
    readonly grant: (
      session: AuthenticatedSession,
    ) => Effect.Effect<CookieMirrorGrant, CookieMirrorError>;
    readonly open: (session: AuthenticatedSession) => Effect.Effect<
      {
        readonly apply: (batch: CookieMirrorBatch) => Effect.Effect<void, CookieMirrorError>;
        readonly invalidated: Effect.Effect<void, never>;
        readonly resets: Stream.Stream<string>;
      },
      CookieMirrorError
    >;
  }
>()("t3/preview/BrowserCookieMirror") {}

const encodeBatch = Schema.encodeEffect(Schema.fromJsonString(CookieMirrorBatch));

const make = Effect.gen(function* () {
  const sessions = yield* SessionStore.SessionStore;
  const browser = yield* ServerBrowser.ServerBrowser;
  const environment = yield* ServerEnvironment.ServerEnvironment;
  const environmentId = yield* environment.getEnvironmentId;
  const lock = yield* Semaphore.make(1);
  const parents = new Map<string, AuthSessionId>();
  let generation = 0;
  const owner = Effect.fn("BrowserCookieMirror.owner")(function* (sessionId: AuthSessionId) {
    const active = yield* sessions
      .listActive()
      .pipe(Effect.mapError(() => new CookieMirrorError({ reason: "unavailable" })));
    const client = active.find((client) => client.sessionId === sessionId);
    if (
      !client ||
      (client.person ?? DEFAULT_PERSON) !== DEFAULT_PERSON ||
      !(client.permissions ?? client.scopes).includes(AuthCookieMirrorWriteScope)
    )
      return yield* new CookieMirrorError({ reason: "forbidden" });
  });
  return BrowserCookieMirror.of({
    grant: Effect.fn("BrowserCookieMirror.grant")(function* (session) {
      if (!session.scopes.includes(AuthCookieMirrorWriteScope) || session.subject === "mcp-client")
        return yield* new CookieMirrorError({ reason: "forbidden" });
      yield* owner(session.sessionId);
      const child = yield* sessions
        .issue({
          subject: `cookie-mirror:${session.sessionId}`,
          method: "bearer-access-token",
          scopes: [AuthCookieMirrorWriteScope],
          ttl: Duration.minutes(10),
          client: { deviceType: "desktop", label: "Desktop browser cookie mirror" },
          replaceActiveForSubjectAndMethod: true,
        })
        .pipe(Effect.mapError(() => new CookieMirrorError({ reason: "unavailable" })));
      for (const [id, parent] of parents) if (parent === session.sessionId) parents.delete(id);
      parents.set(child.sessionId, session.sessionId);
      const ticket = yield* sessions
        .issueWebSocketToken(child.sessionId)
        .pipe(Effect.mapError(() => new CookieMirrorError({ reason: "unavailable" })));
      return { ticket: ticket.token, environmentId };
    }),
    open: (session) =>
      lock.withPermits(1)(
        Effect.gen(function* () {
          const parent = parents.get(session.sessionId);
          if (!parent || !session.scopes.includes(AuthCookieMirrorWriteScope))
            return yield* new CookieMirrorError({ reason: "forbidden" });
          yield* owner(parent);
          const currentGeneration = ++generation;
          let expected = 0;
          let previous: string | undefined;
          return {
            invalidated: Effect.raceFirst(
              sessions.awaitInvalidation(parent),
              sessions.awaitInvalidation(session.sessionId),
            ).pipe(Effect.ignore),
            resets: browser.mirrorResets,
            apply: (batch: CookieMirrorBatch) =>
              lock.withPermits(1)(
                Effect.gen(function* () {
                  if (currentGeneration !== generation || batch.environmentId !== environmentId)
                    return yield* new CookieMirrorError({ reason: "forbidden" });
                  yield* owner(parent);
                  const active = yield* sessions
                    .listActive()
                    .pipe(Effect.mapError(() => new CookieMirrorError({ reason: "unavailable" })));
                  if (
                    !active.some(
                      (client) =>
                        client.sessionId === session.sessionId &&
                        (client.permissions ?? client.scopes).includes(AuthCookieMirrorWriteScope),
                    )
                  )
                    return yield* new CookieMirrorError({ reason: "forbidden" });
                  const encoded = yield* encodeBatch(batch).pipe(
                    Effect.mapError(() => new CookieMirrorError({ reason: "unavailable" })),
                  );
                  if (batch.sequence === expected - 1 && encoded === previous) return;
                  if (batch.sequence !== expected)
                    return yield* new CookieMirrorError({ reason: "forbidden" });
                  yield* browser.mirrorCookies(batch);
                  previous = encoded;
                  expected += 1;
                }),
              ),
          };
        }),
      ),
  });
});
export const layer = Layer.effect(BrowserCookieMirror, make);
