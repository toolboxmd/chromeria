import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { CookieMirrorError } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import { afterEach, vi } from "vite-plus/test";
import {
  EnvironmentId,
  AuthAccessWriteScope,
  AuthCookieMirrorWriteScope,
} from "@t3tools/contracts";
import {
  EMPTY_CONNECTION_CATALOG_DOCUMENT,
  ConnectionCatalogDocument,
} from "@t3tools/client-runtime/platform";
import {
  BearerConnectionProfile,
  BearerConnectionTarget,
  BearerConnectionCredential,
  RelayConnectionTarget,
  SshConnectionProfile,
  SshConnectionTarget,
} from "@t3tools/client-runtime/connection";
import { TokenStore } from "@t3tools/client-runtime/authorization";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as HttpClient from "effect/http/HttpClient";

import * as DesktopConnectionCatalogStore from "../app/DesktopConnectionCatalogStore.ts";
import * as DesktopBackendPool from "../backend/DesktopBackendPool.ts";
import * as DesktopSshEnvironment from "../ssh/DesktopSshEnvironment.ts";
import * as ElectronWindow from "../electron/ElectronWindow.ts";
import { makeCookieMirrorDestinationResolver } from "./CookieMirrorDestination.ts";
const encodeCatalog = Schema.encodeSync(Schema.fromJsonString(ConnectionCatalogDocument));

const environmentId = EnvironmentId.make("saved-environment");
const document: ConnectionCatalogDocument = {
  ...EMPTY_CONNECTION_CATALOG_DOCUMENT,
  targets: [new BearerConnectionTarget({ environmentId, label: "Saved", connectionId: "saved" })],
  profiles: [
    new BearerConnectionProfile({
      environmentId,
      label: "Saved",
      connectionId: "saved",
      httpBaseUrl: "https://saved.test",
      wsBaseUrl: "wss://saved.test/ws",
    }),
  ],
  credentials: [
    {
      connectionId: "saved",
      credential: new BearerConnectionCredential({ token: "synthetic-paired-token" }),
    },
  ],
};
const descriptor = {
  environmentId,
  label: "Saved",
  platform: { os: "linux", arch: "x64" },
  serverVersion: "0.1.0",
  capabilities: {},
};
const session = (scopes: ReadonlyArray<string>) => ({
  authenticated: true,
  auth: {
    policy: "remote-reachable",
    bootstrapMethods: ["one-time-token"],
    sessionMethods: ["bearer-access-token"],
    sessionCookieName: "synthetic-session",
  },
  permissions: scopes,
});
const fixture = (catalog = document) =>
  makeCookieMirrorDestinationResolver.pipe(
    Effect.provide(
      Layer.mergeAll(
        NodeCrypto.layer,
        Layer.mock(DesktopConnectionCatalogStore.DesktopConnectionCatalogStore, {
          get: Effect.succeedSome(encodeCatalog(catalog)),
        }),
        Layer.mock(DesktopBackendPool.DesktopBackendPool, { list: Effect.succeed([]) }),
        Layer.mock(DesktopSshEnvironment.DesktopSshEnvironment, {
          ensureEnvironment: () => Effect.die("Synthetic SSH host unreachable"),
        }),
        Layer.mock(ElectronWindow.ElectronWindow, { main: Effect.succeedNone }),
        Layer.succeed(
          HttpClient.HttpClient,
          HttpClient.make(() => Effect.die("Unexpected bootstrap")),
        ),
      ),
    ),
  );
afterEach(() => vi.unstubAllGlobals());

describe("main-owned mirror destination", () => {
  it.effect(
    "uses a saved route and its stored credential only after environment and scope verification",
    () =>
      Effect.gen(function* () {
        const fetch = vi.fn(async (url: string, _init?: RequestInit) =>
          Response.json(
            url.endsWith("/api/auth/session")
              ? session([AuthAccessWriteScope, AuthCookieMirrorWriteScope])
              : descriptor,
          ),
        );
        vi.stubGlobal("fetch", fetch);
        const resolve = yield* fixture();
        expect(yield* Effect.promise(() => resolve(environmentId))).toEqual({
          httpBaseUrl: "https://saved.test",
          headers: { authorization: "Bearer synthetic-paired-token" },
        });
        expect(fetch.mock.calls.map(([url]) => url)).toEqual([
          "https://saved.test/.well-known/t3/environment",
          "https://saved.test/api/auth/session",
        ]);
        expect(fetch.mock.calls[1]?.[1]).toEqual(
          expect.objectContaining({
            headers: { authorization: "Bearer synthetic-paired-token" },
            redirect: "error",
          }),
        );
      }),
  );

  it.effect("skips earlier routes without the mirror scope and uses the mirror pairing", () =>
    Effect.gen(function* () {
      const fetch = vi.fn(async (url: string, init?: RequestInit) => {
        if (!url.endsWith("/api/auth/session")) return Response.json(descriptor);
        const headers = new Headers(init?.headers);
        const authorization = headers.get("authorization");
        return Response.json(
          session(
            authorization === "Bearer synthetic-mirror-token"
              ? [AuthAccessWriteScope, AuthCookieMirrorWriteScope]
              : [AuthAccessWriteScope],
          ),
        );
      });
      vi.stubGlobal("fetch", fetch);
      const resolve = yield* fixture({
        ...document,
        targets: [
          new RelayConnectionTarget({ environmentId, label: "Saved" }),
          ...document.targets,
          new SshConnectionTarget({ environmentId, label: "Saved", connectionId: "ssh" }),
          new BearerConnectionTarget({ environmentId, label: "Saved", connectionId: "mirror" }),
        ],
        profiles: [
          ...document.profiles,
          new SshConnectionProfile({
            environmentId,
            label: "Saved",
            connectionId: "ssh",
            target: { alias: "saved", hostname: "saved.test", username: null, port: null },
          }),
          new BearerConnectionProfile({
            environmentId,
            label: "Saved",
            connectionId: "mirror",
            httpBaseUrl: "https://mirror.test",
            wsBaseUrl: "wss://mirror.test/ws",
          }),
        ],
        credentials: [
          ...document.credentials,
          {
            connectionId: "ssh",
            credential: new BearerConnectionCredential({ token: "synthetic-ssh-token" }),
          },
          {
            connectionId: "mirror",
            credential: new BearerConnectionCredential({ token: "synthetic-mirror-token" }),
          },
        ],
        remoteDpopTokens: [
          new TokenStore.RemoteDpopAccessToken({
            environmentId,
            label: "Saved",
            endpoint: {
              httpBaseUrl: "https://relay.test",
              wsBaseUrl: "wss://relay.test/ws",
              providerKind: "t3_relay",
            },
            accessToken: "synthetic-relay-token",
            expiresAtEpochMs: Number.MAX_SAFE_INTEGER,
            dpopThumbprint: "synthetic-thumbprint",
          }),
        ],
      });
      expect(yield* Effect.promise(() => resolve(environmentId))).toEqual({
        httpBaseUrl: "https://mirror.test",
        headers: { authorization: "Bearer synthetic-mirror-token" },
      });
    }),
  );

  it.effect("accepts an expiring session whose expiresAt arrives as a JSON string", () =>
    Effect.gen(function* () {
      vi.stubGlobal(
        "fetch",
        vi.fn(async (url: string, _init?: RequestInit) =>
          Response.json(
            url.endsWith("/api/auth/session")
              ? {
                  ...session([AuthCookieMirrorWriteScope]),
                  expiresAt: "2026-11-09T14:22:29.000Z",
                }
              : descriptor,
          ),
        ),
      );
      const resolve = yield* fixture();
      expect(yield* Effect.promise(() => resolve(environmentId))).toEqual({
        httpBaseUrl: "https://saved.test",
        headers: { authorization: "Bearer synthetic-paired-token" },
      });
    }),
  );

  it.effect("rejects unknown or disabled environments before making any request", () =>
    Effect.gen(function* () {
      const fetch = vi.fn();
      vi.stubGlobal("fetch", fetch);
      const resolve = yield* fixture();
      expect(
        (yield* Effect.tryPromise({
          try: () => resolve("unknown"),
          catch: () => new CookieMirrorError({ reason: "forbidden" }),
        }).pipe(Effect.result))._tag,
      ).toBe("Failure");
      const disabled = yield* fixture({ ...document, disabledEnvironmentIds: [environmentId] });
      expect(
        (yield* Effect.tryPromise({
          try: () => disabled(environmentId),
          catch: () => new CookieMirrorError({ reason: "forbidden" }),
        }).pipe(Effect.result))._tag,
      ).toBe("Failure");
      expect(fetch).not.toHaveBeenCalled();
    }),
  );

  it.effect("rejects a changed server identity and an ordinary owner credential", () =>
    Effect.gen(function* () {
      const resolve = yield* fixture();
      vi.stubGlobal(
        "fetch",
        vi.fn(async () => Response.json({ ...descriptor, environmentId: "wrong" })),
      );
      expect(
        (yield* Effect.tryPromise({
          try: () => resolve(environmentId),
          catch: () => new CookieMirrorError({ reason: "forbidden" }),
        }).pipe(Effect.result))._tag,
      ).toBe("Failure");
      vi.stubGlobal(
        "fetch",
        vi.fn(async (url: string) =>
          Response.json(
            url.endsWith("/api/auth/session") ? session([AuthAccessWriteScope]) : descriptor,
          ),
        ),
      );
      expect(
        (yield* Effect.tryPromise({
          try: () => resolve(environmentId),
          catch: () => new CookieMirrorError({ reason: "forbidden" }),
        }).pipe(Effect.result))._tag,
      ).toBe("Failure");
    }),
  );
});
