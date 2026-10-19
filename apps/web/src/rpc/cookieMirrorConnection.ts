import { Connection } from "@t3tools/client-runtime/connection";
import { environmentEndpointUrl } from "@t3tools/client-runtime/environment";
import { COOKIE_MIRROR_GRANT_PATH, COOKIE_MIRROR_ENABLE_RPC } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schedule from "effect/Schedule";

import { adoptDesktopBrowserProfiles, ensureClientSettingsHydrated } from "../hooks/useSettings.ts";

type ReadyHook = NonNullable<Parameters<typeof Connection.layerWithOptions>[0]["onReady"]>;

/** Main obtains the write grant directly. No mirror grant or cookie enters this process. */
export const mirrorDesktopCookies: ReadyHook = (connection, client, authorizeHttp) =>
  Effect.gen(function* () {
    const bridge = window.desktopBridge?.cookieMirrorConnection;
    if (!bridge) return;
    let generation = -1;
    yield* Effect.addFinalizer(() =>
      Effect.tryPromise({
        try: () =>
          bridge({ action: "disconnect", environmentId: connection.environmentId, generation }),
        catch: () => undefined,
      }).pipe(Effect.ignore),
    );
    const synchronize = Effect.gen(function* () {
      const status = yield* Effect.tryPromise({
        try: () => bridge({ action: "status", environmentId: connection.environmentId }),
        catch: () => undefined,
      });
      yield* Effect.tryPromise({ try: ensureClientSettingsHydrated, catch: () => undefined });
      generation = status.generation;
      adoptDesktopBrowserProfiles(status.profiles);
      if (status.connected) return;
      yield* client[COOKIE_MIRROR_ENABLE_RPC]({});
      const headers = yield* authorizeHttp(
        environmentEndpointUrl(connection.httpBaseUrl, COOKIE_MIRROR_GRANT_PATH),
      );
      const connected = yield* Effect.tryPromise({
        try: () =>
          bridge({
            action: "connect",
            environmentId: connection.environmentId,
            httpBaseUrl: connection.httpBaseUrl,
            headers,
          }),
        catch: () => undefined,
      });
      generation = connected.generation;
    }).pipe(Effect.catchCause(() => Effect.void));
    yield* synchronize.pipe(Effect.repeat(Schedule.spaced("5 seconds")));
  });
