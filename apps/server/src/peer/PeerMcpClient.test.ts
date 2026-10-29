import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";
import { EnvironmentId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";

import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import { descriptorOf } from "./PeerLinks.testkit.ts";
import * as PeerLinks from "./PeerLinks.ts";
import * as PeerMcpClient from "./PeerMcpClient.ts";

const here = descriptorOf("environment-here", "Here");
const peer = EnvironmentId.make("environment-peer");
const linkedAt = DateTime.makeUnsafe("2026-10-20T00:00:00.000Z");
const link = {
  environmentId: peer,
  label: "Peer",
  urls: ["https://peer.test"],
  access: "full-access" as const,
  linkedAt,
  expiresAt: DateTime.makeUnsafe("2036-10-20T00:00:00.000Z"),
  lastReachedAt: null,
  lastError: null,
  launchIdentity: "generation",
};

// The services the server runtime owns and the client reads optionally.
const runtime = Layer.mergeAll(
  Layer.succeed(ServerEnvironment.ServerEnvironment, {
    getEnvironmentId: Effect.succeed(here.environmentId),
    getDescriptor: Effect.succeed(here),
  }),
  Layer.mock(Threads.ThreadManagementService)({}),
);

it.effect("delegation over the server runtime names this environment to the peer", () =>
  Effect.gen(function* () {
    const requests: Array<string> = [];
    const services = yield* PeerMcpClient.layerOver(runtime).pipe(
      Layer.provide(
        Layer.mock(PeerLinks.PeerLinks)({
          resolve: () => Effect.succeed({ link, url: "https://peer.test", token: "token" }),
          get: () => Effect.succeedSome(link),
          recordOutcome: () => Effect.void,
        }),
      ),
      Layer.provide(
        Layer.succeed(
          HttpClient.HttpClient,
          HttpClient.make((request) => {
            requests.push(request.url);
            return Effect.succeed(
              HttpClientResponse.fromWeb(request, new Response('{"version":1}', { status: 200 })),
            );
          }),
        ),
      ),
      Layer.provide(NodeCrypto.layer),
      Layer.build,
    );
    const peers = Context.get(services, PeerMcpClient.PeerMcpClient);

    // The first call `delegate_task` makes to a linked environment.
    yield* peers.requireTaskLifecycle!(peer, "generation");

    expect(requests).toEqual(["https://peer.test/mcp/peer-control/tasks"]);
    requests.length = 0;
    const refused = yield* peers
      .call({
        environmentId: peer,
        expectedLinkIdentity: "replaced-generation",
        tool: "t3_thread_launch",
        arguments: {},
        limits: { runtimeMode: "full-access", interactionMode: "default" },
        success: Schema.Struct({}),
      })
      .pipe(Effect.flip);
    expect(refused.code).toBe("capability_denied");
    expect(requests).toEqual([]);
  }).pipe(Effect.scoped),
);

it.effect.each(["structured", "legacy", "invalid"] as const)(
  "decodes %s tool replies while keeping presentation text separate",
  (kind) =>
    Effect.scoped(
      Effect.gen(function* () {
        const response = {
          result: {
            content: [
              {
                type: "text",
                text: kind !== "structured" ? '{"answer":42}' : "Peer image result.",
              },
            ],
            ...(kind === "legacy"
              ? {}
              : { structuredContent: { answer: kind === "invalid" ? "wrong" : 42 } }),
          },
        };
        const services = yield* PeerMcpClient.layerOver(runtime).pipe(
          Layer.provide(
            Layer.mock(PeerLinks.PeerLinks)({
              resolve: () => Effect.succeed({ link, url: "https://peer.test", token: "token" }),
              recordOutcome: () => Effect.void,
            }),
          ),
          Layer.provide(
            Layer.succeed(
              HttpClient.HttpClient,
              HttpClient.make((request) =>
                Effect.succeed(
                  HttpClientResponse.fromWeb(
                    request,
                    Response.json(response, { headers: { "mcp-session-id": "fixture-session" } }),
                  ),
                ),
              ),
            ),
          ),
          Layer.provide(NodeCrypto.layer),
          Layer.build,
        );
        const result = yield* Context.get(services, PeerMcpClient.PeerMcpClient)
          .call({
            environmentId: peer,
            tool: "read_fixture",
            arguments: {},
            limits: { runtimeMode: "full-access", interactionMode: "default" },
            success: replySchema,
          })
          .pipe(Effect.result);
        if (kind === "invalid") expect(result._tag).toBe("Failure");
        else expect(result).toMatchObject({ _tag: "Success", success: { answer: 42 } });
      }),
    ),
);
const replySchema = Schema.Struct({ answer: Schema.Number });
