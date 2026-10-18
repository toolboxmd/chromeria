import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import * as Context from "effect/Context";
import * as Clock from "effect/Clock";
import { HttpRouter, HttpServer, HttpServerRequest, HttpServerResponse } from "effect/http";
import { EnvironmentId } from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/sql/SqlClient";
import * as SessionStore from "./SessionStore.ts";
import * as SessionRenewal from "./SessionRenewal.ts";
import * as ServerSecretStore from "./ServerSecretStore.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as ServerConfig from "../config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as AuthSessions from "../persistence/AuthSessions.ts";

const testLayer = Layer.mergeAll(SessionStore.layer, SessionRenewal.layer).pipe(
  Layer.provideMerge(Sqlite.layerMemory),
  Layer.provide(ServerSecretStore.layer),
  Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "session-renewal-test-" })),
  Layer.provide(
    Layer.succeed(ServerEnvironment.ServerEnvironmentIdentity, {
      getEnvironmentId: Effect.succeed(EnvironmentId.make("renewal-test")),
    }),
  ),
);

it.layer(NodeServices.layer)("paired auth renewal", (it) => {
  it.effect("rejects renewal when expiry passes between validation and the guarded update", () =>
    Effect.gen(function* () {
      const sessions = yield* SessionStore.SessionStore;
      const original = yield* sessions.issue({
        method: "bearer-access-token",
        ttl: Duration.minutes(10),
        renewalCommitment: SessionRenewal.commitment("expiry-boundary"),
      });
      const clock = yield* Clock.Clock;
      let reads = 0;
      const boundaryClock = {
        ...clock,
        currentTimeMillis: Effect.sync(
          () => original.expiresAt.epochMilliseconds + (++reads === 1 ? -1 : 1),
        ),
      };
      const result = yield* sessions
        .renewSession({ secret: "expiry-boundary", next: SessionRenewal.commitment("expiry-next") })
        .pipe(Effect.provideService(Clock.Clock, boundaryClock), Effect.result);
      expect(result._tag).toBe("Failure");
      if (result._tag === "Failure") expect(result.failure._tag).toBe("RenewalRejected");
      const repository = yield* AuthSessions.AuthSessionRepository;
      const row = yield* repository.getById({ sessionId: original.sessionId });
      expect(row._tag).toBe("Some");
      if (row._tag === "Some") expect(row.value.expiresAt).toEqual(original.expiresAt);
      // The failed rotation must roll back its successor, not just the auth row.
      yield* TestClock.adjust("6 minutes");
      const retry = yield* sessions.renewSession({
        secret: "expiry-boundary",
        next: SessionRenewal.commitment("different-next"),
      });
      expect(retry.generation).toBe(1);
      expect(retry.expiresAt.epochMilliseconds).toBe(
        original.expiresAt.epochMilliseconds + 360_000,
      );
    }).pipe(Effect.provide(testLayer)),
  );
  it.effect("keeps a real socket usable past its old deadline and closes it on revocation", () =>
    Effect.gen(function* () {
      const sessions = yield* SessionStore.SessionStore;
      const original = yield* sessions.issue({
        method: "bearer-access-token",
        ttl: Duration.minutes(10),
        renewalCommitment: SessionRenewal.commitment("socket-secret"),
      });
      const clock = yield* Clock.Clock;
      const routes = HttpRouter.use((router) =>
        router.add(
          "GET",
          "/ws",
          Effect.gen(function* () {
            yield* sessions.verify(original.token);
            const request = yield* HttpServerRequest.HttpServerRequest;
            const socket = yield* request.upgrade;
            const reader = yield* socket.reader;
            const writer = yield* socket.writer;
            yield* Effect.raceFirst(
              Effect.forever(reader.pull.pipe(Effect.flatMap(writer.writeAll))),
              sessions
                .awaitInvalidation(original.sessionId)
                .pipe(Effect.provideService(Clock.Clock, clock)),
            );
            return HttpServerResponse.empty();
          }).pipe(Effect.catch(() => Effect.succeed(HttpServerResponse.empty()))),
        ),
      );
      const services = yield* Layer.build(
        HttpRouter.serve(routes, { disableListenLog: true }).pipe(
          Layer.provideMerge(NodeHttpServer.layerTest),
        ),
      );
      const server = Context.get(services, HttpServer.HttpServer);
      const origin = HttpServer.formatAddress(server.address).replace(/^http/, "ws");
      const opened = Promise.withResolvers<void>();
      const closed = Promise.withResolvers<void>();
      const echo = Promise.withResolvers<string>();
      const client = yield* Effect.acquireRelease(
        Effect.sync(() => {
          const socket = new WebSocket(`${origin}/ws`);
          socket.addEventListener("open", () => opened.resolve());
          socket.addEventListener("close", () => closed.resolve());
          socket.addEventListener("error", opened.reject);
          socket.addEventListener("message", (event) => echo.resolve(String(event.data)));
          return socket;
        }),
        (client) => Effect.sync(() => client.close()),
      );
      yield* Effect.promise(() => opened.promise);
      yield* TestClock.adjust("6 minutes");
      yield* sessions.renewSession({
        secret: "socket-secret",
        next: SessionRenewal.commitment("socket-next"),
      });
      yield* TestClock.adjust("5 minutes");
      expect(client.readyState).toBe(WebSocket.OPEN);
      client.send("still usable after original expiry");
      expect(yield* Effect.promise(() => echo.promise)).toBe("still usable after original expiry");
      yield* sessions.revoke(original.sessionId);
      yield* Effect.promise(() => closed.promise);
      expect(client.readyState).toBe(WebSocket.CLOSED);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );
  it.effect(
    "requires approval of the trusted commitment and rejects expired or revoked enrollment",
    () =>
      Effect.gen(function* () {
        const sessions = yield* SessionStore.SessionStore;
        const renewal = yield* SessionRenewal.SessionRenewal;
        const original = yield* sessions.issue({
          method: "bearer-access-token",
          ttl: Duration.minutes(10),
        });
        const request = (secret: string) =>
          renewal.requestEnrollment({
            sessionId: original.sessionId,
            commitment: SessionRenewal.commitment(secret),
            route: "rocky:3773",
            ipAddress: "100.64.0.1",
          });
        const thief = yield* request("stolen-bearer-holder");
        const device = yield* request("trusted-device");
        expect(
          (yield* renewal
            .renew({
              secret: "stolen-bearer-holder",
              next: SessionRenewal.commitment("thief-next"),
            })
            .pipe(Effect.flip))._tag,
        ).toBe("RenewalPending");
        expect(
          (yield* renewal
            .approveEnrollment(device.enrollmentId, thief.fingerprint)
            .pipe(Effect.flip))._tag,
        ).toBe("RenewalRejected");
        yield* renewal.approveEnrollment(device.enrollmentId, device.fingerprint);
        // An uncertain enrollment response remains retryable after approval.
        expect((yield* request("trusted-device")).enrollmentId).toBe(device.enrollmentId);
        const rows = yield* renewal.listEnrollments();
        expect(rows.find((row) => row.enrollmentId === device.enrollmentId)).toMatchObject({
          state: "approved",
          route: "rocky:3773",
        });
        expect(rows.find((row) => row.enrollmentId === thief.enrollmentId)?.state).toBe("rejected");
        expect(
          (yield* renewal
            .renew({
              secret: "stolen-bearer-holder",
              next: SessionRenewal.commitment("thief-next"),
            })
            .pipe(Effect.flip))._tag,
        ).toBe("RenewalRejected");
        yield* sessions.renewSession({
          secret: "trusted-device",
          next: SessionRenewal.commitment("trusted-next"),
        });
        yield* sessions.revoke(original.sessionId);
        expect(
          (yield* renewal
            .renew({ secret: "trusted-next", next: SessionRenewal.commitment("later") })
            .pipe(Effect.flip))._tag,
        ).toBe("RenewalRejected");
        const expiring = yield* sessions.issue({
          method: "browser-session-cookie",
          ttl: Duration.minutes(1),
        });
        const pending = yield* renewal.requestEnrollment({
          sessionId: expiring.sessionId,
          commitment: SessionRenewal.commitment("expired-device"),
          route: null,
          ipAddress: null,
        });
        yield* TestClock.adjust("2 minutes");
        expect(
          (yield* renewal
            .approveEnrollment(pending.enrollmentId, pending.fingerprint)
            .pipe(Effect.flip))._tag,
        ).toBe("RenewalRejected");
      }).pipe(Effect.provide(testLayer)),
  );
  it.effect("retries verified approval without replacing authority before or after rotation", () =>
    Effect.gen(function* () {
      const sessions = yield* SessionStore.SessionStore;
      const renewal = yield* SessionRenewal.SessionRenewal;
      const sql = yield* SqlClient.SqlClient;
      const original = yield* sessions.issue({
        method: "bearer-access-token",
        ttl: Duration.minutes(10),
      });
      const device = yield* renewal.requestEnrollment({
        sessionId: original.sessionId,
        commitment: SessionRenewal.commitment("trusted-device"),
        route: null,
        ipAddress: null,
      });
      yield* renewal.approveEnrollment(device.enrollmentId, device.fingerprint);
      const approved = yield* sql`SELECT * FROM auth_session_renewal`;
      yield* TestClock.adjust("1 minute");
      yield* renewal.approveEnrollment(device.enrollmentId, device.fingerprint);
      expect(yield* sql`SELECT * FROM auth_session_renewal`).toEqual(approved);
      const rotated = yield* renewal.renew({
        secret: "trusted-device",
        next: SessionRenewal.commitment("trusted-next"),
      });
      expect(rotated.generation).toBe(1);
      const authority = yield* sql`SELECT * FROM auth_session_renewal`;
      yield* TestClock.adjust("1 minute");
      yield* renewal.approveEnrollment(device.enrollmentId, device.fingerprint);
      expect(yield* sql`SELECT * FROM auth_session_renewal`).toEqual(authority);
      expect(
        yield* renewal.renew({ secret: "trusted-next", next: SessionRenewal.commitment("later") }),
      ).toEqual({ ...rotated, outcome: "current" });
    }).pipe(Effect.provide(testLayer)),
  );
  it.effect("refuses rejected enrollments and different commitments on approval retries", () =>
    Effect.gen(function* () {
      const sessions = yield* SessionStore.SessionStore;
      const renewal = yield* SessionRenewal.SessionRenewal;
      const original = yield* sessions.issue({
        method: "bearer-access-token",
        ttl: Duration.minutes(10),
      });
      const request = (secret: string) =>
        renewal.requestEnrollment({
          sessionId: original.sessionId,
          commitment: SessionRenewal.commitment(secret),
          route: null,
          ipAddress: null,
        });
      const device = yield* request("trusted-device");
      const other = yield* request("other-device");
      const rejected = yield* request("rejected-device");
      yield* renewal.rejectEnrollment(rejected.enrollmentId);
      yield* renewal.approveEnrollment(device.enrollmentId, device.fingerprint);
      for (const [id, fingerprint] of [
        [device.enrollmentId, other.fingerprint],
        [other.enrollmentId, device.fingerprint],
        [other.enrollmentId, other.fingerprint],
        [rejected.enrollmentId, rejected.fingerprint],
        ["unknown-enrollment", device.fingerprint],
      ] as const) {
        expect((yield* renewal.approveEnrollment(id, fingerprint).pipe(Effect.flip))._tag).toBe(
          "RenewalRejected",
        );
      }
    }).pipe(Effect.provide(testLayer)),
  );
  it.effect.each(["expiry", "revocation"] as const)(
    "refuses verified approval retries after session %s",
    (invalidation) =>
      Effect.gen(function* () {
        const sessions = yield* SessionStore.SessionStore;
        const renewal = yield* SessionRenewal.SessionRenewal;
        const original = yield* sessions.issue({
          method: "bearer-access-token",
          ttl: Duration.minutes(10),
        });
        const device = yield* renewal.requestEnrollment({
          sessionId: original.sessionId,
          commitment: SessionRenewal.commitment("trusted-device"),
          route: null,
          ipAddress: null,
        });
        yield* renewal.approveEnrollment(device.enrollmentId, device.fingerprint);
        if (invalidation === "expiry") yield* TestClock.adjust("11 minutes");
        else yield* sessions.revoke(original.sessionId);
        expect(
          (yield* renewal
            .approveEnrollment(device.enrollmentId, device.fingerprint)
            .pipe(Effect.flip))._tag,
        ).toBe("RenewalRejected");
      }).pipe(Effect.provide(testLayer)),
  );
  it.effect("extends a short-lived pairing without changing its identity or grant", () =>
    Effect.gen(function* () {
      const sessions = yield* SessionStore.SessionStore;
      const renewal = yield* SessionRenewal.SessionRenewal;
      const original = yield* sessions.issue({
        method: "bearer-access-token",
        subject: "one-time-token",
        ttl: Duration.minutes(10),
      });
      yield* renewal.enroll({
        sessionId: original.sessionId,
        commitment: SessionRenewal.commitment("first"),
        approved: true,
      });
      yield* TestClock.adjust("6 minutes");
      const renewed = yield* renewal.renew({
        secret: "first",
        next: SessionRenewal.commitment("second"),
      });
      expect(renewed.session.sessionId).toBe(original.sessionId);
      expect(renewed.session.scopes).toEqual(original.scopes);
      expect(
        renewed.session.expiresAt.epochMilliseconds - original.expiresAt.epochMilliseconds,
      ).toBe(360_000);
      const replay = yield* renewal.renew({
        secret: "first",
        next: SessionRenewal.commitment("second"),
      });
      expect(replay.generation).toBe(1);
      expect(replay.session.expiresAt).toEqual(renewed.session.expiresAt);
      yield* sessions.revoke(original.sessionId);
      expect(
        (yield* renewal
          .renew({ secret: "second", next: SessionRenewal.commitment("third") })
          .pipe(Effect.flip))._tag,
      ).toBe("RenewalRejected");
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("renews the token and rearms live invalidation past the original short TTL", () =>
    Effect.gen(function* () {
      const sessions = yield* SessionStore.SessionStore;
      const original = yield* sessions.issue({
        method: "bearer-access-token",
        ttl: Duration.minutes(10),
        renewalCommitment: SessionRenewal.commitment("short-ttl"),
      });
      const ended = yield* Deferred.make<void>();
      const watcher = yield* sessions
        .awaitInvalidation(original.sessionId)
        .pipe(
          Effect.andThen(Deferred.succeed(ended, undefined)),
          Effect.forkScoped({ startImmediately: true }),
        );
      yield* TestClock.adjust("6 minutes");
      const renewed = yield* sessions.renewSession({
        secret: "short-ttl",
        next: SessionRenewal.commitment("short-next"),
      });
      yield* sessions.setPerson(original.sessionId, "Owner");
      yield* sessions.markConnected(original.sessionId);
      yield* sessions.markDisconnected(original.sessionId);
      yield* TestClock.adjust("5 minutes");
      expect(yield* Deferred.isDone(ended)).toBe(false);
      expect((yield* sessions.verify(original.token).pipe(Effect.flip))._tag).toBe(
        "SessionTokenExpiredError",
      );
      expect((yield* sessions.verify(renewed.token)).sessionId).toBe(original.sessionId);
      yield* sessions.revoke(original.sessionId);
      yield* Fiber.join(watcher);
      expect((yield* sessions.verify(renewed.token).pipe(Effect.flip))._tag).toBe(
        "SessionTokenRevokedError",
      );
      expect(
        (yield* sessions
          .renewSession({ secret: "short-next", next: SessionRenewal.commitment("later") })
          .pipe(Effect.flip))._tag,
      ).toBe("RenewalRejected");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect(
    "recognizes spent-secret theft across two rotations but tolerates an exact historical duplicate",
    () =>
      Effect.gen(function* () {
        const sessions = yield* SessionStore.SessionStore;
        const renewal = yield* SessionRenewal.SessionRenewal;
        const original = yield* sessions.issue({
          method: "bearer-access-token",
          ttl: Duration.minutes(10),
        });
        yield* renewal.enroll({
          sessionId: original.sessionId,
          commitment: SessionRenewal.commitment("s0"),
          approved: true,
        });
        yield* TestClock.adjust("6 minutes");
        yield* renewal.renew({ secret: "s0", next: SessionRenewal.commitment("a1") });
        yield* TestClock.adjust("6 minutes");
        yield* renewal.renew({ secret: "a1", next: SessionRenewal.commitment("a2") });
        expect(
          (yield* renewal
            .renew({ secret: "s0", next: SessionRenewal.commitment("a1") })
            .pipe(Effect.flip))._tag,
        ).toBe("RenewalStale");
        expect(
          (yield* renewal
            .renew({ secret: "unknown", next: SessionRenewal.commitment("x") })
            .pipe(Effect.flip))._tag,
        ).toBe("RenewalRejected");
        // An independent successor proves the original holder disagrees with the thief's chain.
        expect(
          (yield* renewal
            .renew({ secret: "s0", next: SessionRenewal.commitment("device-next") })
            .pipe(Effect.flip))._tag,
        ).toBe("RenewalRejected");
        expect(
          (yield* renewal
            .renew({ secret: "a2", next: SessionRenewal.commitment("a3") })
            .pipe(Effect.flip))._tag,
        ).toBe("RenewalRejected");
      }).pipe(Effect.provide(testLayer)),
  );
});
