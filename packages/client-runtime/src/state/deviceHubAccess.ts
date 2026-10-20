/**
 * Credentials for the Device panel's media requests.
 *
 * The panel reaches simulator streams through `/api/device-hub/*` on the
 * environment origin. `<img>`, `EventSource`, and `WebSocket` cannot set
 * bearer or DPoP headers, so bearer and DPoP connections mint a
 * short-lived WebSocket ticket and pass it as `wsTicket`, the same way the
 * app's own `/ws` upgrade authenticates. Cookie sessions send the cookie.
 *
 * Legacy local panels use a session ticket. Resource viewers receive an exact
 * opening ticket from the guarded device viewer command on the source connection.
 * Callers obtain fresh access when reconnecting.
 */
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import {
  EnvironmentId,
  type DeviceViewerAccess,
  type PreviewViewerAccess,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as Option from "effect/Option";
import { Atom } from "effect/reactivity";
import type { EnvironmentRegistry } from "../connection/registry.ts";
import { createEnvironmentSessionAtoms } from "./session.ts";
import type { AtomCommand } from "./runtime.ts";
import type { HttpClient } from "effect/http";

import * as RemoteEnvironmentAuthorization from "../authorization/service.ts";
import type { PreparedConnection } from "../connection/model.ts";
import { environmentEndpointUrl } from "../environment/endpoint.ts";
import * as ManagedRelay from "../relay/managedRelay.ts";
import type { RemoteEnvironmentRequestError } from "../rpc/http.ts";
import { executeAuthenticatedEnvironmentHttpRequest } from "./environmentHttpAuth.ts";
import type { DeviceHubAccess } from "../device/hubAccess.ts";

export { type DeviceHubAccess, withDeviceHubQuery } from "../device/hubAccess.ts";

const resourceViewerAccess = (
  prepared: PreparedConnection,
  access: DeviceViewerAccess | PreviewViewerAccess,
  operation: "view" | "control",
): DeviceHubAccess => {
  // Only canonical fixed resource routes are admitted, always on the source origin.
  const base = environmentEndpointUrl(
    prepared.httpBaseUrl,
    "hubBasePath" in access ? access.hubBasePath : access.streamBasePath,
  );
  return {
    httpBase: base,
    wsBase: base.replace(/^http/, "ws"),
    query: { viewerTicket: access.ticket },
    credentials: false,
    canOperate: operation === "control",
    viewer: { id: access.viewerId, expiresAt: access.expiresAt },
    expiresAt: Date.parse(access.expiresAt),
  };
};

const TICKET_TIMEOUT_MS = 8_000;

export const resolveDeviceHubAccess = Effect.fn("clientRuntime.state.resolveDeviceHubAccess")(
  function* (input: {
    readonly prepared: PreparedConnection;
    readonly hubBasePath: string;
  }): Effect.fn.Return<DeviceHubAccess, RemoteEnvironmentRequestError, HttpClient.HttpClient> {
    const httpBase = environmentEndpointUrl(input.prepared.httpBaseUrl, input.hubBasePath);
    const wsBase = httpBase.replace(/^http/, "ws");
    const signer = yield* Effect.serviceOption(ManagedRelay.ManagedRelayDpopSigner);
    const remoteAuthorization = yield* Effect.serviceOption(
      RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization,
    );
    if (input.prepared.httpAuthorization === null)
      return { httpBase, wsBase, query: {}, credentials: true };
    const ticket = yield* executeAuthenticatedEnvironmentHttpRequest({
      prepared: input.prepared,
      signer,
      remoteAuthorization,
      group: "auth",
      method: "POST",
      url: (urls) => urls.webSocketTicketFor({ params: { use: "media" } }),
      timeoutMs: TICKET_TIMEOUT_MS,
      request: ({ client, headers }) =>
        client.webSocketTicketFor({ headers, params: { use: "media" } }),
    });
    return {
      httpBase,
      wsBase,
      query: { wsTicket: ticket.ticket },
      expiresAt: DateTime.toEpochMillis(ticket.expiresAt),
      credentials: false,
    };
  },
);

/** One mounted source view owns its lease; hiding it releases only that lease. */
export function createResourceViewerAccessAtomFamily<
  R,
  ER,
  I extends { readonly operation: "view" | "control" },
  A extends DeviceViewerAccess | PreviewViewerAccess,
  E,
  EC,
>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry | R, ER>,
  options: {
    readonly input: Schema.Decoder<I>;
    readonly open: AtomCommand<{ readonly environmentId: EnvironmentId; readonly input: I }, A, E>;
    readonly renew?: AtomCommand<
      { readonly environmentId: EnvironmentId; readonly input: I },
      A,
      E
    >;
    readonly close: AtomCommand<
      { readonly environmentId: EnvironmentId; readonly input: { readonly viewerId: string } },
      boolean,
      EC
    >;
  },
) {
  const sessions = createEnvironmentSessionAtoms(runtime);
  const decodeKey = Schema.decodeUnknownSync(
    Schema.fromJsonString(Schema.Tuple([EnvironmentId, options.input, Schema.String])),
  );
  const family = Atom.family((key: string) => {
    const [environmentId, input] = decodeKey(key);
    return runtime
      .atom((get) => {
        const prepared = get(sessions.preparedConnectionValueAtom(environmentId));
        if (Option.isNone(prepared)) return Effect.never;
        const run = <V, F>(command: AtomCommand<V, A, F>, value: V) =>
          Effect.promise(() => command.run(get.registry, value)).pipe(
            Effect.flatMap((result) =>
              result._tag === "Success"
                ? Effect.succeed(result.value)
                : Effect.failCause(result.cause),
            ),
          );
        return Effect.gen(function* () {
          const access = yield* Effect.acquireRelease(
            run(options.open, { environmentId, input }),
            (access) =>
              Effect.promise(() =>
                options.close.run(get.registry, {
                  environmentId,
                  input: { viewerId: access.viewerId },
                }),
              ).pipe(Effect.timeoutOption("5 seconds"), Effect.ignore),
          );
          const resolved = resourceViewerAccess(prepared.value, access, input.operation);
          const renew = options.renew;
          if (renew === undefined) return resolved;
          return {
            ...resolved,
            requestCurrentAccess: async () => {
              const renewed = await Effect.runPromise(
                run(renew, {
                  environmentId,
                  input: { ...input, viewerId: access.viewerId },
                }),
              );
              if (renewed.viewerId !== access.viewerId)
                throw new Error("The resource viewer changed during admission.");
              return resourceViewerAccess(prepared.value, renewed, input.operation);
            },
          };
        });
      })
      .pipe(Atom.setIdleTTL(0));
  });
  return (target: {
    readonly environmentId: EnvironmentId;
    readonly input: I;
    readonly viewerKey?: string;
  }) => family(JSON.stringify([target.environmentId, target.input, target.viewerKey ?? ""]));
}
