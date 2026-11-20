import { describe, expect, it } from "@effect/vitest";
import {
  AuthAccessWriteScope,
  AuthCookieMirrorWriteScope,
  AuthPreviewOperateScope,
  AuthSessionId,
  CookieMirrorError,
  EnvironmentId,
  type AuthClientSession,
  type AuthEnvironmentScope,
  type CookieMirrorBatch,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";

import type { AuthenticatedSession } from "../auth/EnvironmentAuth.ts";
import * as SessionStore from "../auth/SessionStore.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ServerBrowser from "./ServerBrowser.ts";
import * as BrowserCookieMirror from "./BrowserCookieMirror.ts";

const environmentId = EnvironmentId.make("synthetic-environment");
const at = DateTime.makeUnsafe("2027-01-01T00:00:00Z");
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
const fixture = Effect.gen(function* () {
  const parent: AuthenticatedSession = {
    sessionId: AuthSessionId.make("parent"),
    subject: "owner",
    method: "bearer-access-token",
    scopes: [AuthAccessWriteScope, AuthPreviewOperateScope, AuthCookieMirrorWriteScope],
  };
  const active = new Map<string, AuthClientSession>();
  const client = (session: AuthenticatedSession, person = "Luke"): AuthClientSession => ({
    ...session,
    scopes: session.scopes,
    permissions: session.scopes,
    person,
    client: { deviceType: "desktop" },
    issuedAt: at,
    expiresAt: at,
    lastConnectedAt: at,
    connected: true,
    current: false,
  });
  active.set(parent.sessionId, client(parent));
  let child: AuthenticatedSession = {
    ...parent,
    sessionId: AuthSessionId.make("child"),
    scopes: [AuthCookieMirrorWriteScope],
  };
  let issued = 0;
  let fail = false;
  const applied: CookieMirrorBatch[] = [];
  const sessions = Layer.mock(SessionStore.SessionStore, {
    cookieName: "synthetic-session-cookie",
    legacyCookieName: "synthetic-legacy-session-cookie",
    listActive: () => Effect.sync(() => [...active.values()]),
    issue: (input) =>
      Effect.sync(() => {
        child = {
          sessionId: AuthSessionId.make(`child-${++issued}`),
          subject: input?.subject ?? "",
          scopes: input?.scopes ?? [],
          method: "bearer-access-token",
        };
        active.set(child.sessionId, client(child));
        return {
          ...child,
          token: "synthetic-not-a-real-token",
          client: { deviceType: "desktop" as const },
          expiresAt: at,
        };
      }),
    issueWebSocketToken: () =>
      Effect.succeed({ token: "synthetic-one-time-ticket", expiresAt: at }),
    awaitInvalidation: () => Effect.never,
  });
  const browser = Layer.mock(ServerBrowser.ServerBrowser, {
    mirrorResets: Stream.empty,
    mirrorCookies: (batch) =>
      Effect.suspend(() =>
        fail
          ? new CookieMirrorError({ reason: "unavailable" })
          : Effect.sync(() => {
              applied.push(batch);
            }),
      ),
  });
  const layer = BrowserCookieMirror.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        sessions,
        browser,
        Layer.mock(ServerEnvironment.ServerEnvironment, {
          getEnvironmentId: Effect.succeed(environmentId),
        }),
      ),
    ),
  );
  const mirror = yield* BrowserCookieMirror.BrowserCookieMirror.pipe(Effect.provide(layer));
  const connect = Effect.gen(function* () {
    const grant = yield* mirror.grant(parent);
    expect(grant.environmentId).toBe(environmentId);
    return yield* mirror.open(child);
  });
  return {
    mirror,
    parent,
    active,
    applied,
    connect,
    fail: (value: boolean) => {
      fail = value;
    },
    setScope: (scopes: ReadonlyArray<AuthEnvironmentScope>) =>
      active.set(parent.sessionId, client({ ...parent, scopes })),
  };
});

describe("server cookie mirror authority and ordering", () => {
  it.effect("rejects an ordinary owner credential even when it claims the desktop surface", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const ordinary = { ...f.parent, scopes: [AuthAccessWriteScope, AuthPreviewOperateScope] };
      f.setScope(ordinary.scopes);
      expect((yield* f.mirror.grant(ordinary).pipe(Effect.result))._tag).toBe("Failure");
      expect(f.applied).toEqual([]);
    }),
  );

  it.effect(
    "issues only a dedicated main-process grant and applies on the destination service",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture;
        const stream = yield* f.connect;
        yield* stream.apply(batch);
        expect(f.applied).toEqual([batch]);
        const child = [...f.active.values()].find(
          (session) => session.sessionId !== f.parent.sessionId,
        );
        expect(child?.permissions).toEqual([AuthCookieMirrorWriteScope]);
      }),
  );

  it.effect("requires dedicated credential authority and rejects ordinary preview access", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      expect((yield* f.mirror.grant(f.parent).pipe(Effect.result))._tag).toBe("Success");
      f.setScope([AuthPreviewOperateScope]);
      expect((yield* f.mirror.grant(f.parent).pipe(Effect.result))._tag).toBe("Failure");
      expect((yield* f.mirror.open(f.parent).pipe(Effect.result))._tag).toBe("Failure");
      expect(f.applied).toEqual([]);
    }),
  );

  it.effect("rejects revoked parents, a different owner and the wrong destination", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const stream = yield* f.connect;
      expect(
        (yield* stream
          .apply({ ...batch, environmentId: EnvironmentId.make("wrong-destination") })
          .pipe(Effect.result))._tag,
      ).toBe("Failure");
      const owner = f.active.get(f.parent.sessionId)!;
      f.active.set(f.parent.sessionId, { ...owner, person: "Pauli" });
      expect((yield* stream.apply(batch).pipe(Effect.result))._tag).toBe("Failure");
      f.active.delete(f.parent.sessionId);
      expect((yield* stream.apply(batch).pipe(Effect.result))._tag).toBe("Failure");
      expect(f.applied).toEqual([]);
    }),
  );

  it.effect(
    "advances only after apply succeeds and accepts an identical retry without applying twice",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture;
        const stream = yield* f.connect;
        f.fail(true);
        const failure = yield* stream.apply(batch).pipe(Effect.result);
        expect(failure._tag).toBe("Failure");
        expect(JSON.stringify(failure)).not.toContain("synthetic-private-value");
        f.fail(false);
        yield* stream.apply(batch);
        yield* stream.apply(batch);
        expect(f.applied).toHaveLength(1);
        expect((yield* stream.apply({ ...batch, mutations: [] }).pipe(Effect.result))._tag).toBe(
          "Failure",
        );
        expect((yield* stream.apply({ ...batch, sequence: 2 }).pipe(Effect.result))._tag).toBe(
          "Failure",
        );
        yield* stream.apply({ ...batch, sequence: 1 });
        expect(f.applied).toHaveLength(2);
      }),
  );

  it.effect("rejects writes from a superseded reconnect generation", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const old = yield* f.connect;
      const current = yield* f.connect;
      expect((yield* old.apply(batch).pipe(Effect.result))._tag).toBe("Failure");
      yield* current.apply(batch);
      expect(f.applied).toEqual([batch]);
    }),
  );
});
