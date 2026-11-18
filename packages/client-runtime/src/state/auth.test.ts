import { AuthSessionId, EnvironmentId } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Cause from "effect/Cause";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { Atom, AtomRegistry } from "effect/reactivity";
import { HttpClient, HttpClientResponse } from "effect/http";
import { EnvironmentRegistry } from "../connection/registry.ts";
import { EnvironmentSupervisor } from "../connection/supervisor.ts";
import type { PreparedConnection } from "../connection/model.ts";
import type { RpcSession } from "../rpc/session.ts";
import { AVAILABLE_CONNECTION_STATE, BearerConnectionTarget } from "../connection/model.ts";

import {
  applyAuthAccessStreamEvent,
  EMPTY_AUTH_ACCESS_SNAPSHOT,
  createAuthEnvironmentAtoms,
} from "./auth.ts";

describe("applyAuthAccessStreamEvent", () => {
  it("accumulates rapid pairing-link and client updates into one snapshot", () => {
    const pairingLink = {
      id: "pairing-link",
      scopes: ["orchestration:read"],
      subject: "subject",
      label: "Phone",
      createdAt: DateTime.makeUnsafe("2036-04-07T00:00:00.000Z"),
      expiresAt: DateTime.makeUnsafe("2036-04-07T00:05:00.000Z"),
    } as const;
    const clientSession = {
      sessionId: AuthSessionId.make("session-client"),
      subject: "subject",
      scopes: ["orchestration:read"],
      method: "browser-session-cookie",
      client: {
        label: "Phone",
        deviceType: "mobile",
      },
      issuedAt: DateTime.makeUnsafe("2036-04-07T00:00:00.000Z"),
      expiresAt: DateTime.makeUnsafe("2036-05-07T00:00:00.000Z"),
      lastConnectedAt: null,
      connected: true,
      current: false,
    } as const;

    const withPairingLink = applyAuthAccessStreamEvent(EMPTY_AUTH_ACCESS_SNAPSHOT, {
      version: 1,
      revision: 1,
      type: "pairingLinkUpserted",
      payload: pairingLink,
    });
    const withClient = applyAuthAccessStreamEvent(withPairingLink, {
      version: 1,
      revision: 2,
      type: "clientUpserted",
      payload: clientSession,
    });

    expect(withClient).toEqual({
      pairingLinks: [pairingLink],
      clientSessions: [clientSession],
    });
  });

  it("applies removals without disturbing unrelated access state", () => {
    const snapshot = applyAuthAccessStreamEvent(
      {
        pairingLinks: [
          {
            id: "pairing-link",
            scopes: ["orchestration:read"],
            subject: "subject",
            label: "Phone",
            createdAt: DateTime.makeUnsafe("2036-04-07T00:00:00.000Z"),
            expiresAt: DateTime.makeUnsafe("2036-04-07T00:05:00.000Z"),
          },
        ],
        clientSessions: [],
      },
      {
        version: 1,
        revision: 2,
        type: "pairingLinkRemoved",
        payload: { id: "pairing-link" },
      },
    );

    expect(snapshot).toEqual(EMPTY_AUTH_ACCESS_SNAPSHOT);
  });
});

it.effect("changes a client person using the selected environment and its credentials", () =>
  Effect.gen(function* () {
    const environmentId = EnvironmentId.make("selected-remote");
    const target = new BearerConnectionTarget({
      environmentId,
      label: "Remote",
      connectionId: "remote-connection",
    });
    const supervisor = EnvironmentSupervisor.of({
      target,
      state: yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE),
      session: yield* SubscriptionRef.make<Option.Option<RpcSession>>(Option.none()),
      connect: Effect.void,
      disconnect: Effect.void,
      retryNow: Effect.void,
      prepared: yield* SubscriptionRef.make<Option.Option<PreparedConnection>>(
        Option.some({
          environmentId,
          label: "Remote",
          target,
          httpBaseUrl: "https://remote.test",
          socketUrl: "wss://remote.test/ws",
          httpAuthorization: { _tag: "Bearer" as const, token: "remote-token" },
        }),
      ),
    });
    const requests: Array<{ url: string; authorization: string | undefined }> = [];
    let forbidden = false;
    const http = HttpClient.make((request, url) => {
      requests.push({ url: url.href, authorization: request.headers.authorization });
      return Effect.succeed(
        HttpClientResponse.fromWeb(
          request,
          forbidden
            ? Response.json(
                {
                  _tag: "EnvironmentOperationForbiddenError",
                  code: "operation_forbidden",
                  reason: "unknown_person",
                  traceId: "denied",
                },
                { status: 403 },
              )
            : Response.json({ updated: true }),
        ),
      );
    });
    const environments = {
      run: (id: EnvironmentId, effect: Effect.Effect<unknown>) => {
        expect(id).toBe(environmentId);
        return Effect.provideService(effect, EnvironmentSupervisor, supervisor);
      },
    } as unknown as EnvironmentRegistry["Service"];
    const runtime = Atom.runtime(
      Layer.mergeAll(
        Layer.succeed(EnvironmentRegistry, environments),
        Layer.succeed(HttpClient.HttpClient, http),
      ),
    );
    const registry = AtomRegistry.make();
    yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()));
    const result = yield* Effect.promise(() =>
      createAuthEnvironmentAtoms(runtime).setClientPerson.run(registry, {
        environmentId,
        input: { sessionId: AuthSessionId.make("remote-session"), person: "Luke" },
      }),
    );
    expect(result).toMatchObject({ _tag: "Success", value: { updated: true } });
    expect(requests).toEqual([
      { url: "https://remote.test/api/auth/clients/person", authorization: "Bearer remote-token" },
    ]);
    forbidden = true;
    const denied = yield* Effect.promise(() =>
      createAuthEnvironmentAtoms(runtime).setClientPerson.run(registry, {
        environmentId,
        input: { sessionId: AuthSessionId.make("remote-session"), person: "Unknown" },
      }),
    );
    expect(denied._tag).toBe("Failure");
    if (denied._tag === "Failure")
      expect(Cause.squash(denied.cause)).toMatchObject({
        _tag: "EnvironmentOperationForbiddenError",
        reason: "unknown_person",
      });
    expect(requests).toHaveLength(2);
    expect(requests[1]).toEqual(requests[0]);
  }).pipe(Effect.scoped),
);
