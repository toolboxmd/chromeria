// @effect-diagnostics globalFetch:off - Authenticates only destinations from desktop-main state.
import {
  AuthCookieMirrorWriteScope,
  AuthSessionState,
  COOKIE_MIRROR_GRANT_PATH,
  CookieMirrorError,
  ExecutionEnvironmentDescriptor,
} from "@t3tools/contracts";
import { ConnectionCatalogDocument } from "@t3tools/client-runtime/platform";
import { bootstrapRemoteBearerSession } from "@t3tools/client-runtime/authorization";
import { environmentEndpointUrl } from "@t3tools/client-runtime/environment";
import { currentDesktopBootstrapToken } from "@t3tools/shared/desktopBootstrapToken";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as HttpClient from "effect/http/HttpClient";
import * as Clock from "effect/Clock";

import * as DesktopConnectionCatalogStore from "../app/DesktopConnectionCatalogStore.ts";
import * as DesktopBackendPool from "../backend/DesktopBackendPool.ts";
import * as ElectronWindow from "../electron/ElectronWindow.ts";
import * as DesktopSshEnvironment from "../ssh/DesktopSshEnvironment.ts";
import type { CookieMirrorDestination } from "./CookieMirrorGrant.ts";
import { cookieMirrorDpopProof } from "./CookieMirrorDpop.ts";

const decodeCatalog = Schema.decodeUnknownSync(Schema.fromJsonString(ConnectionCatalogDocument));
const decodeDescriptor = Schema.decodeUnknownSync(ExecutionEnvironmentDescriptor);
const decodeSession = Schema.decodeUnknownSync(AuthSessionState);

const readJson = async (url: string, headers?: Readonly<Record<string, string>>) => {
  const response = await fetch(url, {
    ...(headers ? { headers } : {}),
    redirect: "error",
    signal: AbortSignal.timeout(10_000),
  });
  if (!response.ok) throw new Error("Environment authentication rejected.");
  return response.json();
};

/** The renderer can name a saved environment, never supply its destination or authority. */
export const makeCookieMirrorDestinationResolver = Effect.gen(function* () {
  const catalogStore = yield* DesktopConnectionCatalogStore.DesktopConnectionCatalogStore;
  const pool = yield* DesktopBackendPool.DesktopBackendPool;
  const ssh = yield* DesktopSshEnvironment.DesktopSshEnvironment;
  const windows = yield* ElectronWindow.ElectronWindow;
  const http = yield* HttpClient.HttpClient;
  const context = yield* Effect.context<never>();
  const run = Effect.runPromiseWith(context);

  const resolve = Effect.fn("CookieMirrorDestination.resolve")(
    function* (environmentId: string) {
      const now = yield* Clock.currentTimeMillis;
      let httpBaseUrl: string | undefined;
      let bearer: string | undefined;
      let dpop: ConnectionCatalogDocument["remoteDpopTokens"][number] | undefined;
      for (const instance of yield* pool.list) {
        const config = yield* instance.currentConfig;
        if (Option.isNone(config)) continue;
        const descriptor = yield* Effect.tryPromise(() =>
          readJson(
            environmentEndpointUrl(config.value.httpBaseUrl.href, "/.well-known/t3/environment"),
          ),
        ).pipe(Effect.map(decodeDescriptor), Effect.option);
        if (Option.isNone(descriptor) || descriptor.value.environmentId !== environmentId) continue;
        httpBaseUrl = config.value.httpBaseUrl.href;
        const bootstrap = config.value.bootstrap;
        const credential = bootstrap.desktopBootstrapSecret
          ? currentDesktopBootstrapToken(bootstrap.desktopBootstrapSecret, now, "cookie-mirror")
          : undefined;
        if (!credential) return yield* new CookieMirrorError({ reason: "forbidden" });
        bearer = (yield* bootstrapRemoteBearerSession({
          httpBaseUrl,
          credential,
          scopes: [AuthCookieMirrorWriteScope],
          clientMetadata: { deviceType: "desktop", label: "Desktop browser cookie mirror" },
        }).pipe(Effect.provideService(HttpClient.HttpClient, http))).access_token;
        break;
      }
      if (!httpBaseUrl) {
        const saved = yield* catalogStore.get;
        if (Option.isNone(saved)) return yield* new CookieMirrorError({ reason: "forbidden" });
        const catalog = yield* Effect.try(() => decodeCatalog(saved.value));
        if (catalog.disabledEnvironmentIds.some((id) => id === environmentId))
          return yield* new CookieMirrorError({ reason: "forbidden" });
        const target = catalog.targets.find((target) => target.environmentId === environmentId);
        if (!target) return yield* new CookieMirrorError({ reason: "forbidden" });
        dpop = catalog.remoteDpopTokens.find(
          (token) => token.environmentId === environmentId && token.expiresAtEpochMs > now,
        );
        if (target._tag === "RelayConnectionTarget") {
          httpBaseUrl = dpop?.endpoint.httpBaseUrl;
        } else {
          const profile = catalog.profiles.find(
            (profile) =>
              profile.connectionId === target.connectionId &&
              profile.environmentId === environmentId,
          );
          bearer = catalog.credentials.find(
            (credential) => credential.connectionId === target.connectionId,
          )?.credential.token;
          if (profile?._tag === "BearerConnectionProfile") httpBaseUrl = profile.httpBaseUrl;
          else if (profile?._tag === "SshConnectionProfile")
            httpBaseUrl = (yield* ssh.ensureEnvironment(profile.target, {
              issuePairingToken: false,
            })).httpBaseUrl;
        }
      }
      if (!httpBaseUrl || (!bearer && !dpop))
        return yield* new CookieMirrorError({ reason: "forbidden" });
      const base = httpBaseUrl;
      const authorize = async (
        method: string,
        url: string,
      ): Promise<CookieMirrorDestination["headers"]> => {
        if (bearer) return { authorization: `Bearer ${bearer}` };
        if (!dpop) throw new Error("Missing authenticated connection.");
        const window = await run(windows.main);
        if (Option.isNone(window)) throw new Error("Desktop window unavailable.");
        const proof = await cookieMirrorDpopProof({
          execute: (script) => window.value.webContents.executeJavaScript(script),
          method,
          url,
          token: dpop.accessToken,
          thumbprint: dpop.dpopThumbprint,
          nowEpochSeconds: (await run(Clock.currentTimeMillis)) / 1000,
        });
        return { authorization: `DPoP ${dpop.accessToken}`, dpop: proof };
      };
      const descriptor = decodeDescriptor(
        yield* Effect.tryPromise(() =>
          readJson(environmentEndpointUrl(base, "/.well-known/t3/environment")),
        ),
      );
      if (descriptor.environmentId !== environmentId)
        return yield* new CookieMirrorError({ reason: "forbidden" });
      const sessionUrl = environmentEndpointUrl(base, "/api/auth/session");
      const session = decodeSession(
        yield* Effect.tryPromise(async () =>
          readJson(sessionUrl, await authorize("GET", sessionUrl)),
        ),
      );
      const scopes = session.permissions ?? session.scopes ?? [];
      if (!session.authenticated || !scopes.includes(AuthCookieMirrorWriteScope))
        return yield* new CookieMirrorError({ reason: "forbidden" });
      return {
        httpBaseUrl: base,
        headers: yield* Effect.tryPromise(() =>
          authorize("POST", environmentEndpointUrl(base, COOKIE_MIRROR_GRANT_PATH)),
        ),
      } satisfies CookieMirrorDestination;
    },
    Effect.catchCause(() => new CookieMirrorError({ reason: "forbidden" })),
  );
  return (environmentId: string) => run(resolve(environmentId));
});
