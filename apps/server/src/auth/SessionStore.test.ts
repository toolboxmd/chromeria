import * as Scope from "effect/Scope";
import * as Exit from "effect/Exit";
import * as DateTime from "effect/DateTime";
import { permanentPeerSessionTtl } from "../peer/permanentSession.ts";
import { REUSABLE_DEV_SESSION_EXPIRES_AT } from "./ReusableDevAuth.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import { HttpRouter, HttpServer, HttpServerRequest, HttpServerResponse } from "effect/http";
import * as Clock from "effect/Clock";
import { AuthStandardClientScopes, EnvironmentId, ProjectId, ThreadId } from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Redacted from "effect/Redacted";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/sql/SqlClient";

import * as ServerConfig from "../config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import { PersistenceSqlError } from "../persistence/Errors.ts";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as AuthSessions from "../persistence/AuthSessions.ts";
import * as SessionRenewal from "./SessionRenewal.ts";
import * as SessionStore from "./SessionStore.ts";
import { CROSS_PROCESS_REVOCATION_POLL_INTERVAL } from "./forkCrossProcessRevocation.ts";
import * as ServerSecretStore from "./ServerSecretStore.ts";
import { base64UrlDecodeUtf8, base64UrlEncode, signPayload } from "./utils.ts";

const layerServerConfig = (overrides?: Partial<ServerConfig.ServerConfig["Service"]>) =>
  Layer.effect(
    ServerConfig.ServerConfig,
    Effect.gen(function* () {
      const config = yield* ServerConfig.ServerConfig;
      return {
        ...config,
        ...overrides,
      } satisfies ServerConfig.ServerConfig["Service"];
    }),
  ).pipe(Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "t3-auth-session-test-" })));

const layerServerEnvironment = (environmentId: EnvironmentId) =>
  Layer.succeed(ServerEnvironment.ServerEnvironmentIdentity, {
    getEnvironmentId: Effect.succeed(environmentId),
  });

const layerSessionStore = (
  overrides?: Partial<ServerConfig.ServerConfig["Service"]>,
  environmentId = EnvironmentId.make("test-environment"),
) =>
  SessionStore.layer.pipe(
    Layer.provide(SqlitePersistence.layerMemory),
    Layer.provide(ServerSecretStore.layer),
    Layer.provide(layerServerEnvironment(environmentId)),
    Layer.provide(layerServerConfig(overrides)),
  );

const relaySessionInput = {
  subject: "managed-relay-bootstrap",
  method: "dpop-access-token",
  proofKeyThumbprint: "relay-proof-key",
  ttl: Duration.hours(1),
  client: { label: "Relay desktop", deviceType: "desktop" },
} as const;

const makeDiskSessionStoreLayer = Effect.fn("makeDiskSessionStoreLayer")(function* (
  baseDir: string,
  token?: string,
) {
  const devUrl = new URL("http://127.0.0.1:5173");
  const paths = yield* ServerConfig.deriveServerPaths(baseDir, devUrl, {
    baseDirIsExplicit: true,
  });
  yield* ServerConfig.ensureServerDirectories(paths);
  const layerPersistence = SqlitePersistence.layerFromPath(paths.dbPath);
  return SessionStore.layer.pipe(
    Layer.provide(layerPersistence),
    Layer.provide(ServerSecretStore.layer),
    Layer.provide(layerServerEnvironment(EnvironmentId.make(baseDir))),
    Layer.provide(
      layerServerConfig({
        ...paths,
        baseDir,
        mode: "web",
        devUrl,
        ...(token === undefined ? {} : { devAuthToken: Redacted.make(token) }),
      }),
    ),
  );
});

const repositoryFailure = new PersistenceSqlError({
  operation: "AuthSessionRepository.getById:query",
  detail: "sqlite is unavailable",
});

const layerFailingSessionLookupRepository = Layer.succeed(AuthSessions.AuthSessionRepository, {
  setPerson: () => Effect.void,
  create: () => Effect.void,
  createReplacingActive: () => Effect.succeed([]),
  createIfAbsent: () => Effect.void,
  getById: () => Effect.fail(repositoryFailure),
  listActive: () => Effect.succeed([]),
  revoke: () => Effect.fail(repositoryFailure),
  revokeAllExcept: () => Effect.fail(repositoryFailure),
  setLastConnectedAt: () => Effect.void,
  setClientConnection: () => Effect.void,
});

const layerFailingSessionLookupCredential = Layer.effect(
  SessionStore.SessionStore,
  SessionStore.make,
).pipe(
  Layer.provide(SessionRenewal.layer),
  Layer.provide(layerFailingSessionLookupRepository),
  Layer.provide(ServerSecretStore.layer),
  Layer.provide(SqlitePersistence.layerMemory),
  Layer.provide(layerServerEnvironment(EnvironmentId.make("test-environment"))),
  Layer.provide(layerServerConfig()),
);

it.layer(NodeServices.layer)("SessionStore.layer", (it) => {
  it.effect(
    "admits a fresh preview viewer after the expired socket releases its last transport",
    () =>
      Effect.gen(function* () {
        const sessions = yield* SessionStore.SessionStore;
        const owner = yield* sessions.issue({ subject: "reconnecting-viewer" });
        const input = {
          sessionId: owner.sessionId,
          resource: {
            type: "preview" as const,
            environmentId: EnvironmentId.make("B"),
            threadId: ThreadId.make("context"),
            tabId: "tab",
            serverEpoch: "epoch",
          },
          operation: "control" as const,
          generation: "generation",
        };
        const old = yield* sessions.issueViewerToken(input);
        const socketScope = yield* Scope.make();
        yield* sessions
          .admitViewerToken(old.token)
          .pipe(Effect.provideService(Scope.Scope, socketScope));
        yield* TestClock.adjust("65 seconds");
        yield* Scope.close(socketScope, Exit.void);
        expect(
          (yield* sessions
            .issueViewerToken({ ...input, viewerId: old.viewerId })
            .pipe(Effect.result))._tag,
        ).toBe("Failure");
        const fresh = yield* sessions.issueViewerToken(input);
        expect(fresh.viewerId).not.toBe(old.viewerId);
        const admitted = yield* sessions.admitViewerToken(fresh.token);
        yield* admitted.recheck;
        expect(admitted.viewerId).toBe(fresh.viewerId);
      }).pipe(Effect.scoped, Effect.provide(Layer.merge(layerSessionStore(), TestClock.layer()))),
  );

  it.effect.each(["device", "preview"] as const)(
    "admitted %s transport outlives admission but not scope or session retirement",
    (kind) =>
      Effect.gen(function* () {
        const sessions = yield* SessionStore.SessionStore;
        const owner = yield* sessions.issue({ subject: "continuous-viewer" });
        const rival = yield* sessions.issue({ subject: "rival-viewer" });
        const base = { environmentId: EnvironmentId.make("B"), threadId: ThreadId.make("context") };
        const resource: SessionStore.ViewerResource =
          kind === "device"
            ? {
                ...base,
                type: "device",
                hostId: "local",
                deviceId: "device",
                platform: "ios",
                toolSessionId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
              }
            : { ...base, type: "preview", tabId: "tab", serverEpoch: "epoch" };
        const input = {
          sessionId: owner.sessionId,
          resource,
          operation: "control" as const,
          generation: "generation",
        };
        const ticket = yield* sessions.issueViewerToken(input);
        const unused = yield* sessions.issueViewerToken(input);
        const unusedObserver = yield* sessions
          .awaitViewerInvalidation(unused.viewerId)
          .pipe(Effect.forkScoped({ startImmediately: true }));
        const scope = yield* Scope.make();
        const admitted = yield* sessions
          .admitViewerToken(ticket.token)
          .pipe(Effect.provideService(Scope.Scope, scope));
        const siblingScope = yield* Scope.make();
        const sibling = yield* sessions
          .admitViewerToken(ticket.token)
          .pipe(Effect.provideService(Scope.Scope, siblingScope));
        const observer = yield* sessions
          .awaitViewerInvalidation(ticket.viewerId)
          .pipe(Effect.forkScoped({ startImmediately: true }));
        yield* TestClock.adjust("65 seconds");
        yield* Fiber.join(unusedObserver);
        expect(observer.pollUnsafe()).toBeUndefined();
        yield* admitted.recheck;
        expect(yield* sessions.closeViewer(rival.sessionId, ticket.viewerId)).toBe(false);
        expect((yield* sessions.admitViewerToken(ticket.token).pipe(Effect.result))._tag).toBe(
          "Failure",
        );
        const refreshed = yield* sessions.issueViewerToken({ ...input, viewerId: ticket.viewerId });
        expect(refreshed.viewerId).toBe(ticket.viewerId);
        expect((yield* sessions.authenticateViewerToken(refreshed.token)).resource).toEqual(
          resource,
        );
        for (const changed of [
          { ...input, sessionId: rival.sessionId },
          { ...input, operation: "view" as const },
          { ...input, generation: "other" },
        ])
          expect(
            (yield* sessions
              .issueViewerToken({ ...changed, viewerId: ticket.viewerId })
              .pipe(Effect.result))._tag,
          ).toBe("Failure");
        expect(
          (yield* sessions
            .issueViewerToken({ ...input, viewerId: unused.viewerId })
            .pipe(Effect.result))._tag,
        ).toBe("Failure");
        // Creating another admission prunes unused expired viewers, not this retained transport.
        const next = yield* sessions.issueViewerToken(input);
        yield* admitted.recheck;
        yield* Scope.close(scope, Exit.void);
        expect((yield* admitted.recheck.pipe(Effect.result))._tag).toBe("Failure");
        yield* sibling.recheck;
        expect(observer.pollUnsafe()).toBeUndefined();
        yield* Scope.close(siblingScope, Exit.void);
        yield* Fiber.join(observer);
        expect(
          (yield* sessions
            .issueViewerToken({ ...input, viewerId: ticket.viewerId })
            .pipe(Effect.result))._tag,
        ).toBe("Failure");
        expect(
          (yield* sessions.authenticateViewerToken(refreshed.token).pipe(Effect.result))._tag,
        ).toBe("Failure");
        const second = yield* sessions.admitViewerToken(next.token);
        const revoked = yield* sessions
          .awaitViewerInvalidation(next.viewerId)
          .pipe(Effect.forkScoped({ startImmediately: true }));
        yield* TestClock.adjust("65 seconds");
        yield* second.recheck;
        yield* sessions.revoke(owner.sessionId);
        expect(
          (yield* sessions
            .issueViewerToken({ ...input, viewerId: next.viewerId })
            .pipe(Effect.result))._tag,
        ).toBe("Failure");
        yield* Fiber.join(revoked);
        expect((yield* second.recheck.pipe(Effect.result))._tag).toBe("Failure");
      }).pipe(Effect.scoped, Effect.provide(Layer.merge(layerSessionStore(), TestClock.layer()))),
  );

  it.effect("fresh viewer admission remains bounded by original session expiry", () =>
    Effect.gen(function* () {
      const sessions = yield* SessionStore.SessionStore;
      const owner = yield* sessions.issue({ subject: "short-viewer", ttl: Duration.seconds(70) });
      const input = {
        sessionId: owner.sessionId,
        resource: {
          type: "preview" as const,
          environmentId: EnvironmentId.make("B"),
          threadId: ThreadId.make("context"),
          tabId: "tab",
          serverEpoch: "epoch",
        },
        operation: "control" as const,
        generation: "generation",
      };
      const initial = yield* sessions.issueViewerToken(input);
      const admitted = yield* sessions.admitViewerToken(initial.token);
      yield* TestClock.adjust("65 seconds");
      const current = yield* sessions.issueViewerToken({ ...input, viewerId: initial.viewerId });
      expect(current.expiresAt).toEqual(owner.expiresAt);
      yield* sessions.authenticateViewerToken(current.token);
      yield* TestClock.adjust("6 seconds");
      expect(
        (yield* sessions.authenticateViewerToken(current.token).pipe(Effect.result))._tag,
      ).toBe("Failure");
      expect(
        (yield* sessions
          .issueViewerToken({ ...input, viewerId: initial.viewerId })
          .pipe(Effect.result))._tag,
      ).toBe("Failure");
      expect((yield* admitted.recheck.pipe(Effect.result))._tag).toBe("Failure");
    }).pipe(Effect.scoped, Effect.provide(Layer.merge(layerSessionStore(), TestClock.layer()))),
  );

  it.effect(
    "binds viewer credentials to an exact resource, opening, operation, generation and live source session",
    () =>
      Effect.gen(function* () {
        const sessions = yield* SessionStore.SessionStore;
        const owner = yield* sessions.issue({ subject: "source-client" });
        const rival = yield* sessions.issue({ subject: "rival-client" });
        const resource = {
          type: "device" as const,
          environmentId: EnvironmentId.make("destination-B"),
          threadId: ThreadId.make("destination-context"),
          hostId: "local",
          deviceId: "same-device-id",
          toolSessionId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
          platform: "ios" as const,
        };
        const input = {
          sessionId: owner.sessionId,
          resource,
          operation: "view" as const,
          generation: "actual-link-generation",
        };
        const ticket = yield* sessions.issueViewerToken(input);
        const verification = { ...input, token: ticket.token };
        const verified = yield* sessions.verifyViewerToken(verification);
        expect(verified.session.sessionId).toBe(owner.sessionId);
        expect(verified.session.sessionId).not.toBe(rival.sessionId);
        expect(verified.viewerId).toBe(ticket.viewerId);
        // A viewer credential is not an ordinary credential for /ws, /mcp or HTTP APIs.
        expect((yield* sessions.verify(ticket.token).pipe(Effect.result))._tag).toBe("Failure");
        expect((yield* sessions.verifyWebSocketToken(ticket.token).pipe(Effect.result))._tag).toBe(
          "Failure",
        );
        expect(
          (yield* sessions
            .verifyViewerToken({ ...verification, operation: "control" })
            .pipe(Effect.result))._tag,
        ).toBe("Failure");
        expect(
          (yield* sessions
            .verifyViewerToken({ ...verification, generation: "replacement-link" })
            .pipe(Effect.result))._tag,
        ).toBe("Failure");
        for (const replacement of [
          { ...resource, environmentId: EnvironmentId.make("another-environment") },
          { ...resource, threadId: ThreadId.make("another-context") },
          { ...resource, hostId: "another-host" },
          { ...resource, deviceId: "another-device" },
          { ...resource, toolSessionId: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb" },
          { ...resource, platform: "android" as const },
        ])
          expect(
            (yield* sessions
              .verifyViewerToken({ ...verification, resource: replacement })
              .pipe(Effect.result))._tag,
          ).toBe("Failure");
        const control = yield* sessions.issueViewerToken({ ...input, operation: "control" });
        expect(control.viewerId).not.toBe(ticket.viewerId);
        expect(
          (yield* sessions.verifyViewerToken({
            ...verification,
            token: control.token,
            operation: "control",
          })).session.sessionId,
        ).toBe(owner.sessionId);
        const observer = yield* sessions
          .awaitViewerInvalidation(ticket.viewerId)
          .pipe(Effect.forkScoped({ startImmediately: true }));
        expect(yield* sessions.closeViewer(rival.sessionId, ticket.viewerId)).toBe(false);
        expect((yield* sessions.verifyViewerToken(verification)).viewerId).toBe(ticket.viewerId);
        expect(yield* sessions.closeViewer(owner.sessionId, ticket.viewerId)).toBe(true);
        yield* Fiber.join(observer);
        expect((yield* sessions.verifyViewerToken(verification).pipe(Effect.flip))._tag).toBe(
          "UnknownWebSocketSessionError",
        );
        expect((yield* sessions.authenticateViewerToken(control.token)).viewerId).toBe(
          control.viewerId,
        );
        yield* TestClock.adjust(Duration.seconds(60));
        expect(
          (yield* sessions.authenticateViewerToken(control.token).pipe(Effect.flip))._tag,
        ).toBe("WebSocketTokenExpiredError");
        const current = yield* sessions.issueViewerToken(input);
        yield* sessions.revoke(owner.sessionId);
        expect(
          (yield* sessions
            .verifyViewerToken({ ...verification, token: current.token })
            .pipe(Effect.flip))._tag,
        ).toBe("SessionTokenRevokedError");
        expect((yield* sessions.issueViewerToken(input).pipe(Effect.flip))._tag).toBe(
          "SessionTokenRevokedError",
        );
        expect((yield* sessions.verify(rival.token)).sessionId).toBe(rival.sessionId);
      }).pipe(Effect.scoped, Effect.provide(Layer.merge(layerSessionStore(), TestClock.layer()))),
  );

  it.effect.each(["revoke", "revokeAllExcept", "replace"] as const)(
    "invalidates every connection for a session on %s and leaves other sessions active",
    (operation) =>
      Effect.gen(function* () {
        const sessions = yield* SessionStore.SessionStore;
        const owner = yield* sessions.issue({ subject: "owner" });
        const client = yield* sessions.issue(relaySessionInput);
        const ownerInvalidated = yield* Deferred.make<void>();
        yield* sessions
          .awaitInvalidation(owner.sessionId)
          .pipe(
            Effect.andThen(Deferred.succeed(ownerInvalidated, undefined)),
            Effect.forkScoped({ startImmediately: true }),
          );
        const first = yield* sessions
          .awaitInvalidation(client.sessionId)
          .pipe(Effect.forkScoped({ startImmediately: true }));
        const second = yield* sessions
          .awaitInvalidation(client.sessionId)
          .pipe(Effect.forkScoped({ startImmediately: true }));
        if (operation === "revoke") yield* sessions.revoke(client.sessionId);
        else if (operation === "revokeAllExcept") yield* sessions.revokeAllExcept(owner.sessionId);
        else yield* sessions.issue({ ...relaySessionInput, replaceSessionId: client.sessionId });
        yield* Fiber.join(first);
        yield* Fiber.join(second);
        expect(yield* Deferred.isDone(ownerInvalidated)).toBe(false);
        yield* sessions.awaitInvalidation(client.sessionId);
      }).pipe(Effect.scoped, Effect.provide(Layer.merge(layerSessionStore(), TestClock.layer()))),
  );

  it.effect(
    "caps permanent peer issuance across the two-clock boundary and retains listing and revocation",
    () =>
      Effect.gen(function* () {
        const sessions = yield* SessionStore.SessionStore;
        yield* TestClock.setTime(DateTime.makeUnsafe("2026-10-17T00:00:00.000Z").epochMilliseconds);
        const ttl = yield* permanentPeerSessionTtl;
        yield* TestClock.adjust(Duration.millis(1));
        const issued = yield* sessions.issue({
          ttl,
          subject: "mcp-client",
          peerLink: true,
          runtimeModeCeiling: "auto",
          client: { label: "T3 Code · Stepped peer", deviceType: "bot" },
        });
        expect(DateTime.formatIso(issued.expiresAt)).toBe("9999-12-31T23:59:59.999Z");
        const sql = yield* SqlClient.SqlClient;
        const persisted = yield* sql<{
          expires_at: string;
        }>`SELECT expires_at FROM auth_sessions WHERE session_id=${issued.sessionId}`;
        expect(persisted[0]?.expires_at).toBe("9999-12-31T23:59:59.999Z");
        const listed = (yield* sessions.listActive()).find(
          (session) => session.sessionId === issued.sessionId,
        );
        expect(listed?.expiresAt.epochMilliseconds).toBe(
          REUSABLE_DEV_SESSION_EXPIRES_AT.epochMilliseconds,
        );
        expect((yield* sessions.verify(issued.token)).runtimeModeCeiling).toBe("auto");
        expect((yield* sessions.verify(issued.token)).peerLink).toBe(true);
        const ordinary = yield* sessions.issue();
        expect(ordinary.expiresAt.epochMilliseconds - (yield* DateTime.now).epochMilliseconds).toBe(
          Duration.toMillis(Duration.days(30)),
        );
        const short = yield* sessions.issue({ ttl: Duration.seconds(2) });
        expect(short.expiresAt.epochMilliseconds - (yield* DateTime.now).epochMilliseconds).toBe(
          2000,
        );
        yield* TestClock.adjust(Duration.seconds(2));
        expect((yield* sessions.verify(short.token).pipe(Effect.result))._tag).toBe("Failure");
        expect(yield* sessions.revoke(issued.sessionId)).toBe(true);
        expect(
          (yield* sessions.listActive()).some((session) => session.sessionId === issued.sessionId),
        ).toBe(false);
        expect((yield* sessions.verify(issued.token).pipe(Effect.result))._tag).toBe("Failure");
        expect((yield* sessions.verify(ordinary.token)).sessionId).toBe(ordinary.sessionId);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(layerSessionStore(), SqlitePersistence.layerMemory, TestClock.layer()),
        ),
      ),
  );

  it.effect("expires a connected session at its credential deadline", () =>
    Effect.gen(function* () {
      const sessions = yield* SessionStore.SessionStore;
      const client = yield* sessions.issue(relaySessionInput);
      yield* sessions.markConnected(client.sessionId);
      const invalidated = yield* Deferred.make<void>();
      const watcher = yield* sessions
        .awaitInvalidation(client.sessionId)
        .pipe(
          Effect.andThen(Deferred.succeed(invalidated, undefined)),
          Effect.forkScoped({ startImmediately: true }),
        );
      yield* TestClock.adjust(Duration.minutes(59));
      expect(yield* Deferred.isDone(invalidated)).toBe(false);
      yield* TestClock.adjust(Duration.minutes(1));
      yield* Fiber.join(watcher);
      yield* sessions.awaitInvalidation(client.sessionId);
    }).pipe(Effect.scoped, Effect.provide(Layer.merge(layerSessionStore(), TestClock.layer()))),
  );

  it.effect("keys remote cookies by environment identity instead of state directory", () =>
    Effect.gen(function* () {
      const cookieName = (stateDir: string, environmentId: EnvironmentId) =>
        Effect.gen(function* () {
          const sessions = yield* SessionStore.SessionStore;
          return sessions.cookieName;
        }).pipe(
          Effect.provide(
            layerSessionStore({ mode: "web", host: "192.168.1.50", stateDir }, environmentId),
          ),
        );

      const original = yield* cookieName("/srv/t3-one", EnvironmentId.make("environment-one"));
      const moved = yield* cookieName("/srv/t3-moved", EnvironmentId.make("environment-one"));
      const other = yield* cookieName("/srv/t3-one", EnvironmentId.make("environment-two"));

      expect(moved).toBe(original);
      expect(other).not.toBe(original);
    }),
  );

  it.effect("closes a real socket when another process revokes its session", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-cross-process-revoke-" });
      const storeLayer = yield* makeDiskSessionStoreLayer(baseDir);
      const server = Context.get(yield* Layer.build(storeLayer), SessionStore.SessionStore);
      const cli = Context.get(yield* Layer.build(storeLayer), SessionStore.SessionStore);
      const owner = yield* server.issue({ subject: "owner" });
      const client = yield* server.issue(relaySessionInput);
      const clock = yield* Clock.Clock;
      // Mirrors the /ws route: verify at upgrade, then race the socket against invalidation.
      const routes = HttpRouter.use((router) =>
        router.add(
          "GET",
          "/ws",
          Effect.gen(function* () {
            const request = yield* HttpServerRequest.HttpServerRequest;
            const token = new URL(request.url, "http://localhost").searchParams.get("token") ?? "";
            const session = yield* server.verify(token);
            const socket = yield* request.upgrade;
            const reader = yield* socket.reader;
            const writer = yield* socket.writer;
            yield* Effect.acquireUseRelease(
              server.markConnected(session.sessionId),
              () =>
                Effect.raceFirst(
                  Effect.forever(reader.pull.pipe(Effect.flatMap(writer.writeAll))),
                  server
                    .awaitInvalidation(session.sessionId)
                    .pipe(Effect.provideService(Clock.Clock, clock)),
                ),
              () => server.markDisconnected(session.sessionId),
            );
            return HttpServerResponse.empty();
          }).pipe(Effect.catch(() => Effect.succeed(HttpServerResponse.empty()))),
        ),
      );
      const http = Context.get(
        yield* Layer.build(
          HttpRouter.serve(routes, { disableListenLog: true }).pipe(
            Layer.provideMerge(NodeHttpServer.layerTest),
          ),
        ),
        HttpServer.HttpServer,
      );
      const origin = HttpServer.formatAddress(http.address).replace(/^http/, "ws");
      const connect = (token: string) =>
        Effect.acquireRelease(
          Effect.promise(
            () =>
              new Promise<{ socket: WebSocket; closed: Promise<void> }>((resolve, reject) => {
                const socket = new WebSocket(`${origin}/ws?token=${encodeURIComponent(token)}`);
                const closed = new Promise<void>((done) =>
                  socket.addEventListener("close", () => done()),
                );
                socket.addEventListener("open", () => resolve({ socket, closed }));
                socket.addEventListener("error", reject);
              }),
          ),
          ({ socket }) => Effect.sync(() => socket.close()),
        );
      const ownerSocket = yield* connect(owner.token);
      const clientSocket = yield* connect(client.token);

      expect(yield* cli.revoke(client.sessionId)).toBe(true);
      yield* TestClock.adjust(CROSS_PROCESS_REVOCATION_POLL_INTERVAL);
      yield* Effect.promise(() => clientSocket.closed);

      expect(ownerSocket.socket.readyState).toBe(WebSocket.OPEN);
      const echo = new Promise<string>((resolve) =>
        ownerSocket.socket.addEventListener("message", (event) => resolve(String(event.data)), {
          once: true,
        }),
      );
      ownerSocket.socket.send("still connected");
      expect(yield* Effect.promise(() => echo)).toBe("still connected");
      const reconnect = yield* server.verify(client.token).pipe(Effect.flip);
      expect(reconnect._tag).toBe("SessionTokenRevokedError");
    }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
  );

  it.effect("covers a watcher whose session read precedes a cross-process revocation", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-cross-process-order-" });
      const storeLayer = yield* makeDiskSessionStoreLayer(baseDir);
      const server = Context.get(yield* Layer.build(storeLayer), SessionStore.SessionStore);
      const cli = Context.get(yield* Layer.build(storeLayer), SessionStore.SessionStore);
      const owner = yield* server.issue({ subject: "owner" });
      const client = yield* server.issue(relaySessionInput);
      yield* server.markConnected(owner.sessionId);
      // The watcher reads a valid row before the revocation commits. Connection
      // bookkeeping is the caller's, so the shared check must cover the watcher itself.
      const watcher = yield* server
        .awaitInvalidation(client.sessionId)
        .pipe(Effect.forkScoped({ startImmediately: true }));
      expect(yield* cli.revoke(client.sessionId)).toBe(true);
      yield* TestClock.adjust(CROSS_PROCESS_REVOCATION_POLL_INTERVAL);
      yield* Fiber.join(watcher);
    }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
  );

  it.effect("closes a socket that starts watching after a tick already passed its revocation", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-cross-process-late-" });
      const storeLayer = yield* makeDiskSessionStoreLayer(baseDir);
      const server = Context.get(yield* Layer.build(storeLayer), SessionStore.SessionStore);
      const cli = Context.get(yield* Layer.build(storeLayer), SessionStore.SessionStore);
      const marker = yield* server.issue({ subject: "marker" });
      const late = yield* server.issue(relaySessionInput);
      const markerWatcher = yield* server
        .awaitInvalidation(marker.sessionId)
        .pipe(Effect.forkScoped({ startImmediately: true }));

      // t0: the late socket passes its upgrade check.
      yield* server.verify(late.token);
      // t1: another process revokes it, and the marker so the tick is observable.
      yield* cli.revoke(late.sessionId);
      yield* cli.revoke(marker.sessionId);
      // t2/t3: a tick reads the new version and snapshots without the late socket.
      yield* TestClock.adjust(CROSS_PROCESS_REVOCATION_POLL_INTERVAL);
      yield* Fiber.join(markerWatcher);
      // t4: the late socket starts watching; no later commit or tick may be needed.
      yield* server.awaitInvalidation(late.sessionId);
    }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
  );

  it.effect("keeps reusable dev auth local across disk-backed stores and restarts", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const token = "reusable-dev-auth-token-that-is-long-enough";
      const baseA = yield* fs.makeTempDirectoryScoped({ prefix: "t3-dev-auth-a-" });
      const baseB = yield* fs.makeTempDirectoryScoped({ prefix: "t3-dev-auth-b-" });
      const layerA = yield* makeDiskSessionStoreLayer(baseA, token);
      const fromA = yield* Effect.gen(function* () {
        const sessions = yield* SessionStore.SessionStore;
        const dev = yield* sessions.verify(token);
        const local = yield* sessions.issue({ subject: "environment-a" });
        const ticket = yield* sessions.issueWebSocketToken(local.sessionId);
        yield* sessions.revoke(dev.sessionId);
        return { dev, local, ticket };
      }).pipe(Effect.provide(layerA), Effect.scoped);

      const layerB = yield* makeDiskSessionStoreLayer(baseB, token);
      yield* Effect.gen(function* () {
        const sessions = yield* SessionStore.SessionStore;
        const dev = yield* sessions.verify(token);
        expect(dev.sessionId).toBe(fromA.dev.sessionId);
        expect((yield* Effect.flip(sessions.verify(fromA.local.token)))._tag).toBe(
          "InvalidSessionTokenSignatureError",
        );
        expect((yield* Effect.flip(sessions.verifyWebSocketToken(fromA.ticket.token)))._tag).toBe(
          "InvalidWebSocketTokenSignatureError",
        );
      }).pipe(Effect.provide(layerB), Effect.scoped);

      const layerReopenedB = yield* makeDiskSessionStoreLayer(baseB, token);
      yield* Effect.gen(function* () {
        const sessions = yield* SessionStore.SessionStore;
        const dev = yield* sessions.verify(token);
        expect(dev.sessionId).toBe(fromA.dev.sessionId);
        const ticket = yield* sessions.issueWebSocketToken(dev.sessionId);
        expect((yield* sessions.verifyWebSocketToken(ticket.token)).sessionId).toBe(dev.sessionId);
      }).pipe(Effect.provide(layerReopenedB), Effect.scoped);

      const layerReopenedA = yield* makeDiskSessionStoreLayer(baseA, token);
      yield* Effect.gen(function* () {
        const sessions = yield* SessionStore.SessionStore;
        expect((yield* Effect.flip(sessions.verify(token)))._tag).toBe("SessionTokenRevokedError");
      }).pipe(Effect.provide(layerReopenedA), Effect.scoped);
    }),
  );

  it.effect("invalidates old dev credentials and tickets after rotation or removal", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-dev-auth-rotation-" });
      const oldToken = "old-reusable-dev-auth-token-that-is-long-enough";
      const newToken = "new-reusable-dev-auth-token-that-is-long-enough";
      const layerInitial = yield* makeDiskSessionStoreLayer(baseDir, oldToken);
      const old = yield* Effect.gen(function* () {
        const sessions = yield* SessionStore.SessionStore;
        const dev = yield* sessions.verify(oldToken);
        const ticket = yield* sessions.issueWebSocketToken(dev.sessionId);
        return { dev, ticket };
      }).pipe(Effect.provide(layerInitial), Effect.scoped);

      const layerRotated = yield* makeDiskSessionStoreLayer(baseDir, newToken);
      const rotated = yield* Effect.gen(function* () {
        const sessions = yield* SessionStore.SessionStore;
        expect((yield* Effect.flip(sessions.verify(oldToken)))._tag).toBe(
          "MalformedSessionTokenError",
        );
        expect((yield* Effect.flip(sessions.verifyWebSocketToken(old.ticket.token)))._tag).toBe(
          "UnknownWebSocketSessionError",
        );
        expect(
          (yield* sessions.listActive()).some((row) => row.sessionId === old.dev.sessionId),
        ).toBe(true);
        const dev = yield* sessions.verify(newToken);
        const ticket = yield* sessions.issueWebSocketToken(dev.sessionId);
        return { dev, ticket };
      }).pipe(Effect.provide(layerRotated), Effect.scoped);

      const layerRemoved = yield* makeDiskSessionStoreLayer(baseDir);
      yield* Effect.gen(function* () {
        const sessions = yield* SessionStore.SessionStore;
        expect((yield* Effect.flip(sessions.verify(newToken)))._tag).toBe(
          "MalformedSessionTokenError",
        );
        expect((yield* Effect.flip(sessions.verifyWebSocketToken(rotated.ticket.token)))._tag).toBe(
          "UnknownWebSocketSessionError",
        );
        expect(
          (yield* sessions.listActive()).some((row) => row.sessionId === rotated.dev.sessionId),
        ).toBe(true);
      }).pipe(Effect.provide(layerRemoved), Effect.scoped);
    }),
  );

  it.effect("keeps the reusable token active after normal sessions expire", () =>
    Effect.gen(function* () {
      const sessions = yield* SessionStore.SessionStore;
      const token = "reusable-dev-auth-token-that-is-long-enough";
      const normal = yield* sessions.issue({ subject: "normal-session" });

      yield* TestClock.adjust(Duration.days(31));

      expect((yield* sessions.verify(token)).subject).toBe("reusable-dev-token");
      expect((yield* Effect.flip(sessions.verify(normal.token)))._tag).toBe(
        "SessionTokenExpiredError",
      );
    }).pipe(
      Effect.provide(
        Layer.merge(
          layerSessionStore({
            mode: "web",
            devUrl: new URL("http://127.0.0.1:5173"),
            devAuthToken: Redacted.make("reusable-dev-auth-token-that-is-long-enough"),
          }),
          TestClock.layer(),
        ),
      ),
    ),
  );

  it.effect("issues and verifies signed browser session tokens", () =>
    Effect.gen(function* () {
      const sessions = yield* SessionStore.SessionStore;
      const issued = yield* sessions.issue({
        subject: "desktop-bootstrap",
        scopes: ["orchestration:read", "access:write"],
        client: {
          label: "Desktop app",
          deviceType: "desktop",
          os: "macOS",
          browser: "Electron",
          ipAddress: "127.0.0.1",
        },
      });
      const verified = yield* sessions.verify(issued.token);

      expect(verified.method).toBe("browser-session-cookie");
      expect(verified.subject).toBe("desktop-bootstrap");
      expect(verified.scopes).toEqual(["orchestration:read", "access:write"]);
      expect(verified.client.label).toBe("Desktop app");
      expect(verified.client.browser).toBe("Electron");
      expect(verified.expiresAt?.toString()).toBe(issued.expiresAt.toString());
    }).pipe(Effect.provide(layerSessionStore())),
  );
  it.effect("carries a runtime-mode ceiling only on sessions issued with one", () =>
    Effect.gen(function* () {
      const sessions = yield* SessionStore.SessionStore;
      const capped = yield* sessions.issue({
        subject: "mcp-client",
        method: "bearer-access-token",
        runtimeModeCeiling: "auto",
      });
      const uncapped = yield* sessions.issue({ method: "bearer-access-token" });

      expect((yield* sessions.verify(capped.token)).runtimeModeCeiling).toBe("auto");
      expect((yield* sessions.verify(uncapped.token)).runtimeModeCeiling).toBeUndefined();
    }).pipe(Effect.provide(layerSessionStore())),
  );
  it.effect("keeps recorded scopes unchanged for both token versions", () =>
    Effect.gen(function* () {
      const sessions = yield* SessionStore.SessionStore;
      const secrets = yield* ServerSecretStore.ServerSecretStore;
      const legacyScopes = ["orchestration:read", "terminal:operate", "review:write"] as const;
      const issued = yield* sessions.issue({ subject: "one-time-token", scopes: legacyScopes });
      const [encodedPayload] = issued.token.split(".");
      const currentClaims = base64UrlDecodeUtf8(encodedPayload!);
      expect(currentClaims).toContain('"v":1');
      const legacyPayload = base64UrlEncode(currentClaims.replace('"v":1', '"v":2'));
      const secret = yield* secrets.getOrCreateRandom("server-signing-key", 32);
      const legacyToken = `${legacyPayload}.${signPayload(legacyPayload, secret)}`;

      expect((yield* sessions.verify(issued.token)).scopes).toEqual(legacyScopes);
      expect((yield* sessions.verify(legacyToken)).scopes).toEqual(legacyScopes);
    }).pipe(
      Effect.provide(
        SessionStore.layer.pipe(
          Layer.provideMerge(ServerSecretStore.layer),
          Layer.provide(SqlitePersistence.layerMemory),
          Layer.provide(layerServerEnvironment(EnvironmentId.make("test-environment"))),
          Layer.provide(layerServerConfig()),
        ),
      ),
    ),
  );

  it.effect("rejects malformed session tokens", () =>
    Effect.gen(function* () {
      const sessions = yield* SessionStore.SessionStore;
      const error = yield* Effect.flip(sessions.verify("not-a-session-token"));

      expect(error._tag).toBe("MalformedSessionTokenError");
      expect(error.message).toContain("Malformed session token");
    }).pipe(Effect.provide(layerSessionStore())),
  );
  it.effect("preserves repository failures while verifying session and websocket credentials", () =>
    Effect.gen(function* () {
      const sessions = yield* SessionStore.SessionStore;
      const issued = yield* sessions.issue({
        method: "bearer-access-token",
        subject: "repository-failure",
      });
      const websocket = yield* sessions.issueWebSocketToken(issued.sessionId);

      const sessionError = yield* Effect.flip(sessions.verify(issued.token));
      const websocketError = yield* Effect.flip(sessions.verifyWebSocketToken(websocket.token));
      const revokeError = yield* Effect.flip(sessions.revoke(issued.sessionId));
      const revokeOthersError = yield* Effect.flip(sessions.revokeAllExcept(issued.sessionId));

      expect(sessionError._tag).toBe("SessionCredentialVerificationError");
      expect(websocketError._tag).toBe("WebSocketTokenVerificationError");
      expect(sessionError.cause).toBe(repositoryFailure);
      expect(websocketError.cause).toBe(repositoryFailure);
      if (sessionError._tag === "SessionCredentialVerificationError") {
        expect(sessionError.sessionId).toBe(issued.sessionId);
      }
      if (websocketError._tag === "WebSocketTokenVerificationError") {
        expect(websocketError.sessionId).toBe(issued.sessionId);
      }
      expect(revokeError).toMatchObject({
        _tag: "SessionRevocationError",
        sessionId: issued.sessionId,
        cause: repositoryFailure,
      });
      expect(revokeOthersError).toMatchObject({
        _tag: "OtherSessionsRevocationError",
        currentSessionId: issued.sessionId,
        cause: repositoryFailure,
      });
    }).pipe(Effect.provide(layerFailingSessionLookupCredential)),
  );
  it.effect("verifies session tokens against the Effect clock", () =>
    Effect.gen(function* () {
      const sessions = yield* SessionStore.SessionStore;
      const issued = yield* sessions.issue({
        method: "bearer-access-token",
        subject: "test-clock",
      });
      const verified = yield* sessions.verify(issued.token);

      expect(verified.method).toBe("bearer-access-token");
      expect(verified.subject).toBe("test-clock");
      expect(verified.scopes).toEqual(AuthStandardClientScopes);
    }).pipe(Effect.provide(Layer.merge(layerSessionStore(), TestClock.layer()))),
  );

  it.effect("atomically replaces active sessions with the same subject and method", () =>
    Effect.gen(function* () {
      const sessions = yield* SessionStore.SessionStore;
      const browser = yield* sessions.issue({
        subject: "desktop-bootstrap",
        method: "browser-session-cookie",
      });
      const [firstBearer, secondBearer] = yield* Effect.all(
        [
          sessions.issue({
            subject: "desktop-bootstrap",
            method: "bearer-access-token",
            replaceActiveForSubjectAndMethod: true,
          }),
          sessions.issue({
            subject: "desktop-bootstrap",
            method: "bearer-access-token",
            replaceActiveForSubjectAndMethod: true,
          }),
        ],
        { concurrency: "unbounded" },
      );

      const active = yield* sessions.listActive();
      const bearerVerification = yield* Effect.all([
        sessions.verify(firstBearer.token).pipe(Effect.option),
        sessions.verify(secondBearer.token).pipe(Effect.option),
      ]);

      expect(active).toHaveLength(2);
      expect(active.find((entry) => entry.sessionId === browser.sessionId)).toBeDefined();
      expect(
        active.filter(
          (entry) =>
            entry.subject === "desktop-bootstrap" && entry.method === "bearer-access-token",
        ),
      ).toHaveLength(1);
      expect(bearerVerification.filter(Option.isSome)).toHaveLength(1);
    }).pipe(Effect.provide(layerSessionStore())),
  );

  it.effect("keeps the previous desktop session valid when replacement fails", () =>
    Effect.gen(function* () {
      const sessions = yield* SessionStore.SessionStore;
      const sql = yield* SqlClient.SqlClient;
      const previous = yield* sessions.issue({
        subject: "desktop-bootstrap",
        method: "bearer-access-token",
      });
      yield* sql`
        CREATE TRIGGER reject_auth_session_insert BEFORE INSERT ON auth_sessions
        BEGIN
          SELECT RAISE(ABORT, 'simulated insert failure');
        END
      `;

      const error = yield* sessions
        .issue({
          subject: "desktop-bootstrap",
          method: "bearer-access-token",
          replaceActiveForSubjectAndMethod: true,
        })
        .pipe(Effect.flip);

      expect(error._tag).toBe("SessionCredentialIssueError");
      expect((yield* sessions.verify(previous.token)).sessionId).toBe(previous.sessionId);
      expect((yield* sessions.listActive()).map((session) => session.sessionId)).toEqual([
        previous.sessionId,
      ]);
    }).pipe(Effect.provide(Layer.mergeAll(layerSessionStore(), SqlitePersistence.layerMemory))),
  );

  it.effect.each(["insert", "revoke"] as const)(
    "keeps existing browser sessions valid when replacement cannot %s",
    (operation) =>
      Effect.gen(function* () {
        const sessions = yield* SessionStore.SessionStore;
        const sql = yield* SqlClient.SqlClient;
        const previous = yield* sessions.issue({ subject: "one-time-token" });
        const unrelated = yield* sessions.issue({ subject: "one-time-token" });
        if (operation === "insert") {
          yield* sql`
            CREATE TRIGGER reject_auth_session_insert BEFORE INSERT ON auth_sessions
            BEGIN
              SELECT RAISE(ABORT, 'simulated insert failure');
            END
          `;
        } else {
          yield* sql`
            CREATE TRIGGER reject_auth_session_revocation BEFORE UPDATE OF revoked_at ON auth_sessions
            BEGIN
              SELECT RAISE(ABORT, 'simulated revocation failure');
            END
          `;
        }

        const error = yield* sessions
          .issue({
            subject: "replacement-pairing",
            scopes: ["orchestration:read"],
            replaceSessionId: previous.sessionId,
          })
          .pipe(Effect.flip);

        expect(error._tag).toBe("SessionCredentialIssueError");
        expect((yield* sessions.verify(previous.token)).sessionId).toBe(previous.sessionId);
        expect((yield* sessions.verify(unrelated.token)).sessionId).toBe(unrelated.sessionId);
        expect((yield* sessions.listActive()).map((session) => session.sessionId).sort()).toEqual(
          [previous.sessionId, unrelated.sessionId].sort(),
        );
      }).pipe(Effect.provide(Layer.mergeAll(layerSessionStore(), SqlitePersistence.layerMemory))),
  );

  it.effect("rejects websocket tokens once the parent session has expired", () =>
    Effect.gen(function* () {
      const sessions = yield* SessionStore.SessionStore;
      const issued = yield* sessions.issue({
        method: "bearer-access-token",
        subject: "short-lived",
        ttl: Duration.seconds(1),
      });
      const websocket = yield* sessions.issueWebSocketToken(issued.sessionId);

      yield* TestClock.adjust(Duration.seconds(2));

      const error = yield* Effect.flip(sessions.verifyWebSocketToken(websocket.token));
      expect(error._tag).toBe("WebSocketSessionExpiredError");
      if (error._tag === "WebSocketSessionExpiredError") {
        expect(error.sessionId).toBe(issued.sessionId);
        expect(error.expiresAt.epochMilliseconds).toBe(issued.expiresAt.epochMilliseconds);
        expect(error.observedAt.epochMilliseconds).toBeGreaterThan(
          error.expiresAt.epochMilliseconds,
        );
      }
    }).pipe(Effect.provide(Layer.merge(layerSessionStore(), TestClock.layer()))),
  );

  it.effect("includes expiry context when session and websocket tokens expire", () =>
    Effect.gen(function* () {
      const sessions = yield* SessionStore.SessionStore;
      const issued = yield* sessions.issue({
        method: "bearer-access-token",
        subject: "short-lived-token",
        ttl: Duration.seconds(1),
      });
      const websocket = yield* sessions.issueWebSocketToken(issued.sessionId, {
        ttl: Duration.seconds(1),
      });

      yield* TestClock.adjust(Duration.seconds(2));

      const sessionError = yield* Effect.flip(sessions.verify(issued.token));
      const websocketError = yield* Effect.flip(sessions.verifyWebSocketToken(websocket.token));

      expect(sessionError._tag).toBe("SessionTokenExpiredError");
      if (sessionError._tag === "SessionTokenExpiredError") {
        expect(sessionError.sessionId).toBe(issued.sessionId);
        expect(sessionError.expiresAt.epochMilliseconds).toBe(issued.expiresAt.epochMilliseconds);
        expect(sessionError.observedAt.epochMilliseconds).toBeGreaterThan(
          sessionError.expiresAt.epochMilliseconds,
        );
      }
      expect(websocketError._tag).toBe("WebSocketTokenExpiredError");
      if (websocketError._tag === "WebSocketTokenExpiredError") {
        expect(websocketError.sessionId).toBe(issued.sessionId);
        expect(websocketError.expiresAt.epochMilliseconds).toBe(
          websocket.expiresAt.epochMilliseconds,
        );
        expect(websocketError.observedAt.epochMilliseconds).toBeGreaterThan(
          websocketError.expiresAt.epochMilliseconds,
        );
      }
    }).pipe(Effect.provide(Layer.merge(layerSessionStore(), TestClock.layer()))),
  );

  it.effect("lists active sessions, tracks connectivity, and revokes other sessions", () =>
    Effect.gen(function* () {
      const sessions = yield* SessionStore.SessionStore;
      const administrative = yield* sessions.issue({
        subject: "desktop-bootstrap",
        scopes: ["orchestration:read", "access:write"],
        client: {
          label: "Desktop app",
          deviceType: "desktop",
          os: "macOS",
          browser: "Electron",
        },
      });
      const client = yield* sessions.issue({
        subject: "one-time-token",
        scopes: ["orchestration:read"],
        client: {
          label: "Julius iPhone",
          deviceType: "mobile",
          os: "iOS",
          browser: "Safari",
          ipAddress: "192.168.1.88",
        },
      });
      const clientWebSocket = yield* sessions.issueWebSocketToken(client.sessionId);

      yield* sessions.markConnected(client.sessionId);
      const beforeRevoke = yield* sessions.listActive();
      const revokedCount = yield* sessions.revokeAllExcept(administrative.sessionId);
      const afterRevoke = yield* sessions.listActive();
      const revokedClient = yield* Effect.flip(sessions.verify(client.token));
      const revokedClientWebSocket = yield* Effect.flip(
        sessions.verifyWebSocketToken(clientWebSocket.token),
      );

      expect(beforeRevoke).toHaveLength(2);
      expect(beforeRevoke.find((entry) => entry.sessionId === client.sessionId)?.connected).toBe(
        true,
      );
      expect(beforeRevoke.find((entry) => entry.sessionId === client.sessionId)?.client.label).toBe(
        "Julius iPhone",
      );
      expect(
        beforeRevoke.find((entry) => entry.sessionId === administrative.sessionId)?.client
          .deviceType,
      ).toBe("desktop");
      expect(revokedCount).toBe(1);
      expect(afterRevoke).toHaveLength(1);
      expect(afterRevoke[0]?.sessionId).toBe(administrative.sessionId);
      expect(revokedClient._tag).toBe("SessionTokenRevokedError");
      if (revokedClient._tag === "SessionTokenRevokedError") {
        expect(revokedClient.sessionId).toBe(client.sessionId);
        expect(revokedClient.revokedAt.epochMilliseconds).toBeGreaterThanOrEqual(0);
      }
      expect(revokedClientWebSocket._tag).toBe("WebSocketSessionRevokedError");
      if (revokedClientWebSocket._tag === "WebSocketSessionRevokedError") {
        expect(revokedClientWebSocket.sessionId).toBe(client.sessionId);
        expect(revokedClientWebSocket.revokedAt.epochMilliseconds).toBeGreaterThanOrEqual(0);
      }
    }).pipe(Effect.provide(layerSessionStore())),
  );

  it.effect("persists lastConnectedAt on first connect and updates it after reconnect", () =>
    Effect.gen(function* () {
      const sessions = yield* SessionStore.SessionStore;
      const issued = yield* sessions.issue({
        subject: "reconnect-test",
        method: "bearer-access-token",
      });

      const beforeConnect = yield* sessions.listActive();
      expect(beforeConnect[0]?.lastConnectedAt).toBeNull();

      yield* TestClock.adjust(Duration.seconds(1));
      yield* sessions.markConnected(issued.sessionId);
      const firstConnect = yield* sessions.listActive();
      const firstConnectedAt = firstConnect[0]?.lastConnectedAt;

      expect(firstConnect[0]?.connected).toBe(true);
      expect(firstConnectedAt).not.toBeNull();

      yield* TestClock.adjust(Duration.seconds(1));
      yield* sessions.markConnected(issued.sessionId);
      const stillConnected = yield* sessions.listActive();

      expect(stillConnected[0]?.lastConnectedAt?.toString()).toBe(firstConnectedAt?.toString());

      yield* sessions.markDisconnected(issued.sessionId);
      yield* sessions.markDisconnected(issued.sessionId);
      const afterDisconnect = yield* sessions.listActive();

      expect(afterDisconnect[0]?.connected).toBe(false);
      expect(afterDisconnect[0]?.lastConnectedAt?.toString()).toBe(firstConnectedAt?.toString());

      yield* TestClock.adjust(Duration.seconds(1));
      yield* sessions.markConnected(issued.sessionId);
      const afterReconnect = yield* sessions.listActive();

      expect(afterReconnect[0]?.connected).toBe(true);
      expect(afterReconnect[0]?.lastConnectedAt).not.toBeNull();
      expect(afterReconnect[0]?.lastConnectedAt?.toString()).not.toBe(firstConnectedAt?.toString());
    }).pipe(Effect.provide(Layer.merge(layerSessionStore(), TestClock.layer()))),
  );

  it.effect("keeps connected relay sessions visible through expiry and HTTP renewal", () =>
    Effect.gen(function* () {
      const sessions = yield* SessionStore.SessionStore;
      const original = yield* sessions.issue(relaySessionInput);
      const websocket = yield* sessions.issueWebSocketToken(original.sessionId, {
        ttl: Duration.hours(2),
      });
      yield* sessions.verifyWebSocketToken(websocket.token);
      yield* sessions.markConnected(original.sessionId);
      const beforeExpiry = yield* sessions.listActive();
      expect(beforeExpiry).toHaveLength(1);
      expect(beforeExpiry[0]?.connected).toBe(true);

      yield* TestClock.adjust(Duration.minutes(61));

      expect(yield* sessions.listActive()).toEqual(beforeExpiry);
      expect(yield* Effect.flip(sessions.verify(original.token))).toMatchObject({
        _tag: "SessionTokenExpiredError",
      });
      expect(yield* Effect.flip(sessions.verifyWebSocketToken(websocket.token))).toMatchObject({
        _tag: "WebSocketSessionExpiredError",
      });

      const renewed = yield* sessions.issue(relaySessionInput);
      const afterRenewal = yield* sessions.listActive();
      expect(renewed.sessionId).not.toBe(original.sessionId);
      expect(afterRenewal).toHaveLength(2);
      expect(afterRenewal).toEqual(
        expect.arrayContaining([
          beforeExpiry[0],
          expect.objectContaining({
            sessionId: renewed.sessionId,
            connected: false,
            lastConnectedAt: null,
          }),
        ]),
      );
    }).pipe(Effect.provide(Layer.merge(layerSessionStore(), TestClock.layer()))),
  );

  it.effect.each([1, 2])(
    "removes an expired session from listings and updates after its last of %s sockets closes",
    (socketCount) =>
      Effect.gen(function* () {
        const sessions = yield* SessionStore.SessionStore;
        const issued = yield* sessions.issue(relaySessionInput);
        const changes = yield* Queue.unbounded<SessionStore.SessionCredentialChange>();
        yield* sessions.streamChanges.pipe(
          Stream.runForEach((change) => Queue.offer(changes, change)),
          Effect.forkScoped({ startImmediately: true }),
        );
        for (let index = 0; index < socketCount; index += 1) {
          yield* sessions.markConnected(issued.sessionId);
          expect(yield* Queue.take(changes)).toMatchObject({
            type: "clientUpserted",
            clientSession: { sessionId: issued.sessionId, connected: true },
          });
        }

        yield* TestClock.adjust(Duration.minutes(61));

        for (let remaining = socketCount - 1; remaining >= 0; remaining -= 1) {
          yield* sessions.markDisconnected(issued.sessionId);
          const change = yield* Queue.take(changes);
          const listed = yield* sessions.listActive();
          if (remaining > 0) {
            expect(change).toMatchObject({
              type: "clientUpserted",
              clientSession: { sessionId: issued.sessionId, connected: true },
            });
            expect(listed).toHaveLength(1);
            expect(listed[0]?.connected).toBe(true);
          } else {
            expect(change).toEqual({ type: "clientRemoved", sessionId: issued.sessionId });
            expect(listed).toEqual([]);
          }
        }
      }).pipe(Effect.scoped, Effect.provide(Layer.merge(layerSessionStore(), TestClock.layer()))),
  );

  it.effect.each(["revoke", "revokeAllExcept"] as const)(
    "removes expired connected sessions with %s",
    (operation) =>
      Effect.gen(function* () {
        const sessions = yield* SessionStore.SessionStore;
        const administrative = yield* sessions.issue({ subject: "desktop-bootstrap" });
        const client = yield* sessions.issue(relaySessionInput);
        yield* sessions.markConnected(client.sessionId);
        yield* TestClock.adjust(Duration.minutes(61));
        const changes = yield* Queue.unbounded<SessionStore.SessionCredentialChange>();
        yield* sessions.streamChanges.pipe(
          Stream.runForEach((change) => Queue.offer(changes, change)),
          Effect.forkScoped({ startImmediately: true }),
        );

        if (operation === "revoke") {
          expect(yield* sessions.revoke(client.sessionId)).toBe(true);
        } else {
          expect(yield* sessions.revokeAllExcept(administrative.sessionId)).toBe(1);
        }

        expect(yield* Queue.take(changes)).toEqual({
          type: "clientRemoved",
          sessionId: client.sessionId,
        });
        const listed = yield* sessions.listActive();
        expect(listed).toHaveLength(1);
        expect(listed[0]?.sessionId).toBe(administrative.sessionId);
      }).pipe(Effect.scoped, Effect.provide(Layer.merge(layerSessionStore(), TestClock.layer()))),
  );

  it.effect("records client connection metadata without clearing prior values", () =>
    Effect.gen(function* () {
      const sessions = yield* SessionStore.SessionStore;
      const sql = yield* SqlClient.SqlClient;
      const issued = yield* sessions.issue({
        subject: "client-connection-test",
        method: "bearer-access-token",
      });
      const readRow = sql<{
        readonly surface: string | null;
        readonly appVersion: string | null;
      }>`
        SELECT client_surface AS "surface", client_app_version AS "appVersion"
        FROM auth_sessions
        WHERE session_id = ${issued.sessionId}
      `;

      yield* sessions.recordClientConnection(issued.sessionId, {
        surface: "mobile",
        appVersion: "1.2.0",
      });
      expect((yield* readRow)[0]).toEqual({ surface: "mobile", appVersion: "1.2.0" });

      yield* sessions.recordClientConnection(issued.sessionId, { appVersion: "1.3.0" });
      expect((yield* readRow)[0]).toEqual({ surface: "mobile", appVersion: "1.3.0" });

      yield* sessions.recordClientConnection(issued.sessionId, {});
      expect((yield* readRow)[0]).toEqual({ surface: "mobile", appVersion: "1.3.0" });
    }).pipe(Effect.provide(Layer.mergeAll(layerSessionStore(), SqlitePersistence.layerMemory))),
  );
});

it.effect("outside OAuth renews past 30 days without widening and revokes the whole grant", () =>
  Effect.gen(function* () {
    const store = yield* SessionStore.SessionStore;
    const issued = yield* store.issue({
      subject: "mcp-client",
      method: "bearer-access-token",
      ttl: Duration.days(36500),
      scopes: ["orchestration:read", "orchestration:operate"],
      runtimeModeCeiling: "auto",
      mcpLimits: { projectId: ProjectId.make("wedding"), operations: ["read", "send"] },
    });
    const first = yield* store.issueMcpOAuthTokens(issued.token, "client-a", "https://box/mcp");
    const read = (token: string) => store.verify(token);
    expect(yield* read(first.token)).toMatchObject({
      runtimeModeCeiling: "auto",
      mcpLimits: {
        projectId: "wedding",
        operations: ["read", "send"],
      },
    });
    expect((yield* Effect.result(read(first.refreshToken)))._tag).toBe("Failure");
    yield* TestClock.adjust(Duration.days(31));
    expect((yield* Effect.result(read(first.token)))._tag).toBe("Failure");
    expect(
      (yield* Effect.result(
        store.refreshMcpOAuthToken(first.refreshToken, "other-client", "https://box/mcp"),
      ))._tag,
    ).toBe("Failure");
    expect(
      (yield* Effect.result(
        store.refreshMcpOAuthToken(first.refreshToken, "client-a", "https://other/mcp"),
      ))._tag,
    ).toBe("Failure");
    const renewed = yield* store.refreshMcpOAuthToken(
      first.refreshToken,
      "client-a",
      "https://box/mcp",
    );
    expect(renewed.refreshToken).not.toBe(first.refreshToken);
    expect(yield* read(renewed.token)).toMatchObject({
      sessionId: issued.sessionId,
      runtimeModeCeiling: "auto",
      mcpLimits: { projectId: "wedding", operations: ["read", "send"] },
    });
    yield* store.revoke(issued.sessionId);
    expect((yield* Effect.result(read(renewed.token)))._tag).toBe("Failure");
    expect(
      (yield* Effect.result(
        store.refreshMcpOAuthToken(renewed.refreshToken, "client-a", "https://box/mcp"),
      ))._tag,
    ).toBe("Failure");
  }).pipe(Effect.provide(layerSessionStore().pipe(Layer.provide(NodeServices.layer)))),
);

it.effect("outside OAuth refresh authority survives closing and reopening its database", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-mcp-restart-" });
    const firstLayer = yield* makeDiskSessionStoreLayer(baseDir);
    const first = yield* Effect.gen(function* () {
      const store = yield* SessionStore.SessionStore;
      const issued = yield* store.issue({
        subject: "mcp-client",
        method: "bearer-access-token",
        ttl: Duration.days(36500),
        runtimeModeCeiling: "approval-required",
        mcpLimits: { projectId: ProjectId.make("wedding"), operations: ["launch", "read"] },
      });
      return yield* store.issueMcpOAuthTokens(issued.token, "service", "https://box/mcp");
    }).pipe(Effect.provide(firstLayer), Effect.scoped);
    yield* TestClock.adjust(Duration.days(31));
    const reopened = yield* makeDiskSessionStoreLayer(baseDir);
    const rotated = yield* Effect.gen(function* () {
      const store = yield* SessionStore.SessionStore;
      const renewed = yield* store.refreshMcpOAuthToken(
        first.refreshToken,
        "service",
        "https://box/mcp",
      );
      expect(yield* store.verify(renewed.token)).toMatchObject({
        runtimeModeCeiling: "approval-required",
        mcpLimits: { projectId: "wedding", operations: ["launch", "read"] },
      });
      return renewed;
    }).pipe(Effect.provide(reopened), Effect.scoped);
    yield* TestClock.adjust(Duration.seconds(4));
    const reopenedAgain = yield* makeDiskSessionStoreLayer(baseDir);
    yield* Effect.gen(function* () {
      const store = yield* SessionStore.SessionStore;
      const retry = yield* store.refreshMcpOAuthToken(
        first.refreshToken,
        "service",
        "https://box/mcp",
      );
      expect(retry.refreshToken).toBe(rotated.refreshToken);
      expect((yield* store.verify(retry.token)).runtimeModeCeiling).toBe("approval-required");
      // Retrying did not slide the original five-second deadline.
      yield* TestClock.adjust(Duration.seconds(1));
      expect(
        (yield* Effect.result(
          store.refreshMcpOAuthToken(first.refreshToken, "service", "https://box/mcp"),
        ))._tag,
      ).toBe("Failure");
      expect((yield* Effect.result(store.verify(retry.token)))._tag).toBe("Failure");
    }).pipe(Effect.provide(reopenedAgain), Effect.scoped);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect(
  "keeps absolute outside-grant expiry visible and revocable at the year-9999 boundary",
  () =>
    Effect.gen(function* () {
      const store = yield* SessionStore.SessionStore;
      const deadline = DateTime.makeUnsafe("9999-12-31T23:59:59.999Z");
      yield* TestClock.adjust(1);
      const issued = yield* store.issue({
        subject: "mcp-client",
        method: "bearer-access-token",
        expiresAt: deadline,
      });
      expect(DateTime.formatIso(issued.expiresAt)).toBe("9999-12-31T23:59:59.999Z");
      expect(
        (yield* store.listActive()).some((session) => session.sessionId === issued.sessionId),
      ).toBe(true);
      expect(yield* store.revoke(issued.sessionId)).toBe(true);
      expect((yield* Effect.result(store.verify(issued.token)))._tag).toBe("Failure");
    }).pipe(Effect.provide(layerSessionStore().pipe(Layer.provide(NodeServices.layer)))),
);

it.effect("revokes outside OAuth authority when a rotated refresh token is reused", () =>
  Effect.gen(function* () {
    const store = yield* SessionStore.SessionStore;
    const issued = yield* store.issue({
      subject: "mcp-client",
      method: "bearer-access-token",
      ttl: Duration.days(36500),
    });
    const first = yield* store.issueMcpOAuthTokens(issued.token, "service", "https://box/mcp");
    const current = yield* store.refreshMcpOAuthToken(
      first.refreshToken,
      "service",
      "https://box/mcp",
    );
    expect((yield* store.verify(current.token)).sessionId).toBe(issued.sessionId);
    yield* TestClock.adjust(Duration.seconds(5));
    expect(
      (yield* Effect.result(
        store.refreshMcpOAuthToken(first.refreshToken, "service", "https://box/mcp"),
      ))._tag,
    ).toBe("Failure");
    expect((yield* Effect.result(store.verify(current.token)))._tag).toBe("Failure");
    expect(
      (yield* Effect.result(
        store.refreshMcpOAuthToken(current.refreshToken, "service", "https://box/mcp"),
      ))._tag,
    ).toBe("Failure");
    expect(
      (yield* store.listActive()).some((session) => session.sessionId === issued.sessionId),
    ).toBe(false);
  }).pipe(Effect.provide(layerSessionStore().pipe(Layer.provide(NodeServices.layer)))),
);

it.effect("concurrent outside OAuth refreshes return the same live successor", () =>
  Effect.gen(function* () {
    const store = yield* SessionStore.SessionStore;
    const issued = yield* store.issue({
      subject: "mcp-client",
      method: "bearer-access-token",
      ttl: Duration.days(36500),
      mcpLimits: { projectId: ProjectId.make("wedding"), operations: ["read"] },
    });
    const first = yield* store.issueMcpOAuthTokens(issued.token, "service", "https://box/mcp");
    const results = yield* Effect.all(
      [
        store.refreshMcpOAuthToken(first.refreshToken, "service", "https://box/mcp"),
        store.refreshMcpOAuthToken(first.refreshToken, "service", "https://box/mcp"),
      ],
      { concurrency: "unbounded" },
    );
    expect(results[0].refreshToken).toBe(results[1].refreshToken);
    for (const result of results) {
      expect(yield* store.verify(result.token)).toMatchObject({
        sessionId: issued.sessionId,
        mcpLimits: { projectId: "wedding", operations: ["read"] },
      });
    }
    const next = yield* store.refreshMcpOAuthToken(
      results[0].refreshToken,
      "service",
      "https://box/mcp",
    );
    // Even inside the grace period, a token older than the immediate predecessor is reuse.
    expect(
      (yield* Effect.result(
        store.refreshMcpOAuthToken(first.refreshToken, "service", "https://box/mcp"),
      ))._tag,
    ).toBe("Failure");
    expect((yield* Effect.result(store.verify(next.token)))._tag).toBe("Failure");
  }).pipe(Effect.provide(layerSessionStore().pipe(Layer.provide(NodeServices.layer)))),
);
