import { Connection } from "@t3tools/client-runtime/connection";
import * as Effect from "effect/Effect";
import * as Schedule from "effect/Schedule";

import { recordCookieMirrorStatus } from "./cookieMirrorStatus.ts";

type ReadyHook = NonNullable<Parameters<typeof Connection.layerWithOptions>[0]["onReady"]>;

/** Main obtains the write grant directly. No mirror grant or cookie enters this process. */
export const mirrorDesktopCookies: ReadyHook = Effect.fn(
  "cookieMirrorConnection.mirrorDesktopCookies",
)(function* (connection) {
  const bridge = window.desktopBridge?.cookieMirrorConnection;
  if (!bridge) return;
  const { adoptDesktopBrowserProfiles } = yield* Effect.promise(
    () => import("../hooks/useSettings.ts"),
  );
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
    generation = status.generation;
    recordCookieMirrorStatus(status);
    yield* Effect.tryPromise({
      try: () => adoptDesktopBrowserProfiles(status.profiles),
      catch: () => undefined,
    });
    if (status.connected) return;
    const connected = yield* Effect.tryPromise({
      try: () =>
        bridge({
          action: "connect",
          environmentId: connection.environmentId,
        }),
      catch: () => undefined,
    });
    generation = connected.generation;
    recordCookieMirrorStatus(connected);
  }).pipe(Effect.ignoreCause);
  yield* synchronize.pipe(Effect.repeat(Schedule.spaced("5 seconds")));
});
