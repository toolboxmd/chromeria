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
} from "@t3tools/client-runtime/connection";
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
        Layer.mock(DesktopConnectionCatalogStore.DesktopConnectionCatalogStore, {
          get: Effect.succeedSome(encodeCatalog(catalog)),
        }),
        Layer.mock(DesktopBackendPool.DesktopBackendPool, { list: Effect.succeed([]) }),
        Layer.mock(DesktopSshEnvironment.DesktopSshEnvironment, {}),
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

  it.effect("rejects unknown or disabled environments before making any request", () =>
    Effect.gen(function* () {
      const fetch = vi.fn();
      vi.stubGlobal("fetch", fetch);
      const resolve = yield* fixture();
      expect((yield* Effect.tryPromise(() => resolve("unknown")).pipe(Effect.result))._tag).toBe(
        "Failure",
      );
      const disabled = yield* fixture({ ...document, disabledEnvironmentIds: [environmentId] });
      expect(
        (yield* Effect.tryPromise(() => disabled(environmentId)).pipe(Effect.result))._tag,
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
        (yield* Effect.tryPromise(() => resolve(environmentId)).pipe(Effect.result))._tag,
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
        (yield* Effect.tryPromise(() => resolve(environmentId)).pipe(Effect.result))._tag,
      ).toBe("Failure");
    }),
  );
});
