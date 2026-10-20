import { describe, expect, it } from "@effect/vitest";
import { type AssetCreateUrlResult, EnvironmentId } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Option from "effect/Option";
import * as Layer from "effect/Layer";
import { HttpClient, HttpClientResponse } from "effect/http";
import { AsyncResult, Atom, AtomRegistry } from "effect/reactivity";

import { createProjectFaviconUrlAtomFamily } from "./assets.ts";

describe("project favicon URL cache", () => {
  it("does not reuse another environment, workspace, or selected icon's cached URL", () => {
    const registry = AtomRegistry.make();
    const result = Atom.make<AsyncResult.AsyncResult<AssetCreateUrlResult, unknown>>(
      AsyncResult.success({
        expiresAt: 4_000_000_000_000,
        relativeUrl: "/api/assets/token/icon.svg",
      }),
    );
    const favicon = createProjectFaviconUrlAtomFamily({
      createUrl: () => result,
      preparedConnection: () => Atom.make(Option.some({ httpBaseUrl: "https://remote.test" })),
    });
    const target = { environmentId: EnvironmentId.make("remote"), cwd: "/workspace" };
    const unmount = registry.mount(favicon(target));
    try {
      expect(registry.get(favicon(target))).toBe("https://remote.test/api/assets/token/icon.svg");
      registry.set(result, AsyncResult.failure(Cause.die("disconnected")));
      expect(
        registry.get(favicon({ ...target, environmentId: EnvironmentId.make("other") })),
      ).toBeNull();
      expect(registry.get(favicon({ ...target, cwd: "/other" }))).toBeNull();
      expect(registry.get(favicon({ ...target, faviconPath: "brand.svg" }))).toBeNull();
      expect(registry.get(favicon({ ...target, faviconPath: null }))).toBe(
        "https://remote.test/api/assets/token/icon.svg",
      );
    } finally {
      unmount();
      registry.dispose();
    }
  });
});

it.effect.each(["cookie", "bearer", "cookie-download", "bearer-download"] as const)(
  "qualified artifact URLs keep %s access on source A",
  (mode) =>
    Effect.scoped(
      Effect.gen(function* () {
        const kind = mode.startsWith("cookie") ? "cookie" : "bearer";
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
          httpAuthorization:
            kind === "cookie" ? null : { _tag: "Bearer", token: "ISOLATED_A_TOKEN" },
        };
        const resource = mode.endsWith("download")
          ? {
              _tag: "native-browser-download" as const,
              resource: {
                type: "preview" as const,
                environmentId: destination,
                threadId: ThreadId.make("destination-thread"),
                tabId: "tab-b",
                serverEpoch: "epoch-b",
              },
              downloadId: "550e8400-e29b-41d4-a716-446655440000",
              name: "report.txt",
              mimeType: "application/octet-stream" as const,
              sizeBytes: 1024 * 1024 * 1024,
              digest: "2".repeat(64),
            }
          : {
              _tag: "peer-attachment" as const,
              threadId: ThreadId.make("destination-thread"),
              reference: {
                environmentId: destination,
                attachmentId: "destination-image",
                owner: "1".repeat(64),
                digest: "2".repeat(64),
                name: "screen.png",
                mimeType: "image/png",
                sizeBytes: 68,
              },
            };
        const rpcCalls: unknown[] = [];
        const client = {
          [WS_METHODS.assetsCreateUrl]: (input: unknown) => {
            rpcCalls.push(input);
            return Effect.succeed({
              relativeUrl: "/api/assets/source-signed/artifact",
              expiresAt: 4_000_000_000_000,
            });
          },
        } as unknown as WsRpcProtocolClient;
        const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
          target,
          state: yield* SubscriptionRef.make<SupervisorConnectionState>({
            ...AVAILABLE_CONNECTION_STATE,
            phase: "connected" as const,
          }),
          session: yield* SubscriptionRef.make(Option.some({ client } as RpcSession)),
          prepared: yield* SubscriptionRef.make(Option.some(prepared)),
          connect: Effect.void,
          disconnect: Effect.void,
          retryNow: Effect.void,
        });
        const requests: Array<{ url: string; authorization: string | undefined }> = [];
        const http = HttpClient.make((request, url) => {
          requests.push({ url: url.href, authorization: request.headers.authorization });
          return Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              Response.json({
                ticket: "source-media-ticket",
                expiresAt: "2036-04-07T00:05:00.000Z",
              }),
            ),
          );
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
          followStream: (_id, stream) =>
            Stream.provideService(stream, EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        } as EnvironmentRegistry.EnvironmentRegistry["Service"]);
        const assets = createAssetEnvironmentAtoms(
          Atom.runtime(
            Layer.mergeAll(
              Layer.succeed(EnvironmentRegistry.EnvironmentRegistry, environments),
              Layer.succeed(HttpClient.HttpClient, http),
            ),
          ),
        );
        const registry = AtomRegistry.make();
        yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()));
        const result = yield* AtomRegistry.getResult(
          registry,
          assets.createUrl({ environmentId: source, input: { resource } }),
          { suspendOnWaiting: true },
        );
        expect(rpcCalls).toEqual([{ resource }]);
        expect(requests).toEqual(
          kind === "cookie"
            ? []
            : [
                {
                  url: "https://a.test/api/auth/websocket-ticket",
                  authorization: "Bearer ISOLATED_A_TOKEN",
                },
              ],
        );
        const url = new URL(result.relativeUrl, prepared.httpBaseUrl);
        expect(url.origin).toBe("https://a.test");
        expect(url.pathname).toBe("/api/assets/source-signed/artifact");
        expect(url.searchParams.get("wsTicket")).toBe(
          kind === "cookie" ? null : "source-media-ticket",
        );
        expect(url.href).not.toContain("destination-b");
        if (kind === "bearer")
          expect(result.expiresAt).toBe(Date.parse("2036-04-07T00:05:00.000Z"));
      }),
    ),
);
