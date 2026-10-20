import type { DeviceHubAccess } from "../device/hubAccess.ts";
import { expect, it } from "@effect/vitest";
import { EnvironmentId, ThreadId, WS_METHODS, type AuthSessionState } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { Atom, AtomRegistry, AsyncResult } from "effect/reactivity";
import { HttpClient, HttpClientResponse } from "effect/http";
import * as EnvironmentRegistry from "../connection/registry.ts";
import * as EnvironmentSupervisor from "../connection/supervisor.ts";
import {
  AVAILABLE_CONNECTION_STATE,
  PrimaryConnectionTarget,
  type PreparedConnection,
  type SupervisorConnectionState,
} from "../connection/model.ts";
import type { RpcSession } from "../rpc/session.ts";
import type { WsRpcProtocolClient } from "../rpc/protocol.ts";
import { createDeviceEnvironmentAtoms } from "./device.ts";
import { createPreviewEnvironmentAtoms } from "./preview.ts";

it.live.each(["device", "preview"] as const)(
  "%s viewer uses guarded source commands and retires only the exact mounted lease",
  (kind) =>
    Effect.scoped(
      Effect.gen(function* () {
        const source = EnvironmentId.make("source-a");
        const destination = EnvironmentId.make("destination-b");
        const target = new PrimaryConnectionTarget({
          environmentId: source,
          label: "A",
          httpBaseUrl: "https://a.test",
          wsBaseUrl: "wss://a.test",
        });
        const prepared: PreparedConnection = {
          environmentId: source,
          label: "A",
          target,
          httpBaseUrl: "https://a.test",
          socketUrl: "wss://a.test/ws",
          httpAuthorization: null,
        };
        const scopes = ["orchestration:read"] as const;
        const session: AuthSessionState = {
          authenticated: true,
          scopes,
          permissions: scopes,
          auth: {
            policy: "remote-reachable",
            bootstrapMethods: [],
            sessionMethods: [],
            sessionCookieName: "test",
          },
        };
        const requests: Array<{ method: string; input: unknown }> = [];
        const closed = yield* Deferred.make<string>();
        const secondClosed = yield* Deferred.make<string>();
        let next = 0;
        const open = (input: unknown) =>
          Effect.sync(() => {
            if (
              typeof input === "object" &&
              input !== null &&
              "viewerId" in input &&
              typeof input.viewerId === "string"
            ) {
              requests.push({ method: "renew", input });
              return {
                viewerId: input.viewerId,
                ticket: "current-file-admission",
                expiresAt: "2036-04-07T00:04:00.000Z",
                ...(kind === "device"
                  ? { hubBasePath: "/api/device-hub" }
                  : { streamBasePath: "/api/preview-stream" }),
              };
            }
            requests.push({ method: "open", input });
            const viewerId = `00000000-0000-4000-8000-${String(++next).padStart(12, "0")}`;
            return {
              viewerId,
              ticket: `source-viewer-${next}`,
              expiresAt: "2036-04-07T00:05:00.000Z",
              ...(kind === "device"
                ? { hubBasePath: "/api/device-hub" }
                : { streamBasePath: "/api/preview-stream" }),
            };
          });
        const close = (input: { viewerId: string }) =>
          Effect.gen(function* () {
            requests.push({ method: "close", input });
            yield* Deferred.succeed(closed, input.viewerId);
            if (requests.filter((request) => request.method === "close").length === 2)
              yield* Deferred.succeed(secondClosed, input.viewerId);
            return true;
          });
        const client = {
          [WS_METHODS.deviceViewerOpen]: open,
          [WS_METHODS.deviceViewerClose]: close,
          [WS_METHODS.previewViewerOpen]: open,
          [WS_METHODS.previewViewerClose]: close,
        } as unknown as WsRpcProtocolClient;
        const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
          target,
          state: yield* SubscriptionRef.make<SupervisorConnectionState>({
            ...AVAILABLE_CONNECTION_STATE,
            phase: "connected",
          }),
          session: yield* SubscriptionRef.make(Option.some({ client } as RpcSession)),
          prepared: yield* SubscriptionRef.make(Option.some(prepared)),
          connect: Effect.void,
          disconnect: Effect.void,
          retryNow: Effect.void,
        });
        const environments = EnvironmentRegistry.EnvironmentRegistry.of({
          run: (id, effect) => {
            expect(id).toBe(source);
            return Effect.provideService(
              effect,
              EnvironmentSupervisor.EnvironmentSupervisor,
              supervisor,
            );
          },
          followStream: (id, stream) => {
            expect(id).toBe(source);
            return Stream.provideService(
              stream,
              EnvironmentSupervisor.EnvironmentSupervisor,
              supervisor,
            );
          },
        } as EnvironmentRegistry.EnvironmentRegistry["Service"]);
        const http = HttpClient.make((request, url) => {
          expect(url.origin).toBe("https://a.test");
          expect(url.pathname).toBe("/api/auth/session");
          return Effect.succeed(HttpClientResponse.fromWeb(request, Response.json(session)));
        });
        const runtime = Atom.runtime(
          Layer.mergeAll(
            Layer.succeed(EnvironmentRegistry.EnvironmentRegistry, environments),
            Layer.succeed(HttpClient.HttpClient, http),
          ),
        );
        const devices = createDeviceEnvironmentAtoms(runtime);
        const previews = createPreviewEnvironmentAtoms(runtime);
        const registry = AtomRegistry.make();
        yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()));
        const device = {
          type: "device" as const,
          environmentId: destination,
          threadId: ThreadId.make("b-thread"),
          hostId: "local",
          deviceId: "same-id",
          platform: "android" as const,
          toolSessionId: "11111111-1111-4111-8111-111111111111",
        };
        const preview = {
          type: "preview" as const,
          environmentId: destination,
          threadId: ThreadId.make("b-thread"),
          tabId: "same-id",
          serverEpoch: "epoch-b",
        };
        type ViewerError = AsyncResult.AsyncResult.Failure<
          | Atom.Type<ReturnType<typeof devices.viewerAccess>>
          | Atom.Type<ReturnType<typeof previews.viewerAccess>>
        >;
        const atom: Atom.Atom<AsyncResult.AsyncResult<DeviceHubAccess, ViewerError>> =
          kind === "device"
            ? devices.viewerAccess({
                environmentId: source,
                input: { resource: device, operation: "view" },
              })
            : previews.viewerAccess({
                environmentId: source,
                input: { resource: preview, operation: "view" },
              });
        const unmount = registry.mount(atom);
        const access = yield* AtomRegistry.getResult(registry, atom, { suspendOnWaiting: true });
        expect(access.httpBase).toBe(
          `https://a.test/api/${kind === "device" ? "device-hub" : "preview-stream"}`,
        );
        expect(access.query).toEqual({ viewerTicket: "source-viewer-1" });
        expect(access.canOperate).toBe(false);
        expect(requests).toEqual([
          {
            method: "open",
            input: { resource: kind === "device" ? device : preview, operation: "view" },
          },
        ]);
        // Read permission admits observation, never silently grants control.
        const denied =
          kind === "device"
            ? yield* Effect.promise(() =>
                devices.viewerOpen.run(registry, {
                  environmentId: source,
                  input: { resource: device, operation: "control" },
                }),
              )
            : yield* Effect.promise(() =>
                previews.viewerOpen.run(registry, {
                  environmentId: source,
                  input: { resource: preview, operation: "control" },
                }),
              );
        expect(denied._tag).toBe("Failure");
        expect(requests).toHaveLength(1);
        const second: Atom.Atom<AsyncResult.AsyncResult<DeviceHubAccess, ViewerError>> =
          kind === "device"
            ? devices.viewerAccess({
                environmentId: source,
                viewerKey: "second-surface",
                input: { resource: device, operation: "view" },
              })
            : previews.viewerAccess({
                environmentId: source,
                viewerKey: "second-surface",
                input: { resource: preview, operation: "view" },
              });
        const unmountSecond = registry.mount(second);
        const secondAccess = yield* AtomRegistry.getResult(registry, second, {
          suspendOnWaiting: true,
        });
        expect(secondAccess.viewer!.id).not.toBe(access.viewer!.id);
        {
          expect(access.requestCurrentAccess).toBeDefined();
          const current = yield* Effect.promise(() => access.requestCurrentAccess!());
          expect(current.viewer!.id).toBe(access.viewer!.id);
          expect(current.viewer!.expiresAt).toBe("2036-04-07T00:04:00.000Z");
          expect(current.expiresAt).toBe(Date.parse("2036-04-07T00:04:00.000Z"));
          expect(current.query).toEqual({ viewerTicket: "current-file-admission" });
          expect(requests.at(-1)).toEqual({
            method: "renew",
            input: {
              resource: kind === "device" ? device : preview,
              operation: "view",
              viewerId: access.viewer!.id,
            },
          });
          expect(requests.filter((request) => request.method === "close")).toHaveLength(0);
          expect(
            (yield* AtomRegistry.getResult(registry, atom, { suspendOnWaiting: true })).viewer!.id,
          ).toBe(access.viewer!.id);
        }
        unmount();
        expect(yield* Deferred.await(closed)).toBe(access.viewer!.id);
        expect(requests.at(-1)).toEqual({
          method: "close",
          input: { viewerId: access.viewer!.id },
        });
        expect(
          (yield* AtomRegistry.getResult(registry, second, { suspendOnWaiting: true })).viewer!.id,
        ).toBe(secondAccess.viewer!.id);
        unmountSecond();
        expect(yield* Deferred.await(secondClosed)).toBe(secondAccess.viewer!.id);
        expect(
          requests.some(
            (request) => request.method === "device-close" || request.method === "tab-close",
          ),
        ).toBe(false);
        const release = registry.mount(atom);
        const beforeRefresh = yield* AtomRegistry.getResult(registry, atom, {
          suspendOnWaiting: true,
        });
        registry.refresh(atom);
        const fresh = yield* AtomRegistry.getResult(registry, atom, { suspendOnWaiting: true });
        expect(fresh.viewer!.id).not.toBe(beforeRefresh.viewer!.id);
        expect(requests.at(-1)).toEqual({
          method: "open",
          input: { resource: kind === "device" ? device : preview, operation: "view" },
        });
        release();
      }),
    ),
);
