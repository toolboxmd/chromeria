import { NodeHttpServer } from "@effect/platform-node";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Redacted from "effect/Redacted";
import * as Scope from "effect/Scope";
import * as TestClock from "effect/testing/TestClock";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import type * as HttpClientRequest from "effect/http/HttpClientRequest";
import { McpSchema, McpServer } from "effect/ai";

import * as Config from "../config.ts";
import * as Secrets from "../auth/ServerSecretStore.ts";
import * as McpHttpServer from "../mcp/McpHttpServer.ts";
import * as Invocation from "../mcp/McpInvocationContext.ts";
import { liveThreadShell } from "../mcp/McpToolAccess.testkit.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as PeerForwarding from "../peer/PeerForwarding.ts";
import * as PeerLinks from "../peer/PeerLinks.ts";
import * as PeerMcpClient from "../peer/PeerMcpClient.ts";
import {
  descriptorOf,
  layerLinkingEnvironment,
  linkTo,
  servePeer,
} from "../peer/PeerLinks.testkit.ts";
import * as Settings from "../serverSettings.ts";
import * as PageCache from "./PageCache.ts";
import * as PublicHttp from "./PublicHttp.ts";
import * as PublicRender from "./PublicRender.ts";
import * as Search from "./Search.ts";
import * as TinyFish from "./TinyFish.ts";
import * as WebResearch from "./WebResearch.ts";
import { HostedWebSearchResult } from "./ResearchHost.ts";

const bestia = descriptorOf("search-bestia", "Bestia");
const cavallo = descriptorOf("search-cavallo", "Cavallo");
const rocky = descriptorOf("search-rocky", "Rocky");
const client = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  clientCapabilities: {},
  clientInfo: { name: "search-host-proof", version: "1" },
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "search-host-proof", version: "1" },
  },
  getClient: Effect.die("unused"),
});
const scopeOf = (environmentId: EnvironmentId): Invocation.McpInvocationScope => ({
  environmentId,
  requestNamespace: `agent:${environmentId}`,
  issuedAt: 0,
  thread: {
    threadId: ThreadId.make(`thread:${environmentId}`),
    providerInstanceId: ProviderInstanceId.make("codex"),
    providerSessionId: `session:${environmentId}`,
  },
  client: undefined,
  capabilities: new Set(),
});
const reader = (http: HttpClient.HttpClient) =>
  WebResearch.layer.pipe(
    Layer.provide(Search.layer),
    Layer.provide(PageCache.layer),
    Layer.provide(Layer.succeed(HttpClient.HttpClient, http)),
    Layer.provide(
      Layer.mock(Secrets.ServerSecretStore)({
        get: () => Effect.succeedSome(new TextEncoder().encode("fixture-only-host-secret")),
      }),
    ),
    Layer.provide(
      Layer.succeed(TinyFish.TinyFish, { fetch: () => Effect.die("Search must not use fetch.") }),
    ),
    Layer.provide(
      Layer.succeed(PublicHttp.PublicHttp, {
        get: () => Effect.die("Search must not read pages."),
      }),
    ),
    Layer.provide(
      Layer.succeed(PublicRender.PublicRender, {
        render: () => Effect.die("Search must not render."),
      }),
    ),
    Layer.provide(Config.layerTest(process.cwd(), { prefix: "search-peer-" })),
    Layer.provide(NodeServices.layer),
    Layer.fresh,
  );
const decode = Schema.decodeUnknownSync(HostedWebSearchResult);
const decodeFailure = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Struct({ code: Schema.String, message: Schema.String })),
);
const failure = (result: McpSchema.CallToolResult) => {
  expect(result.isError).toBe(true);
  expect(result.structuredContent).toBeUndefined();
  const part = result.content[0];
  if (part?.type !== "text") throw new Error("Expected typed search failure.");
  return decodeFailure(part.text);
};
const fixture = Effect.gen(function* () {
  const requests = yield* Queue.unbounded<HttpClientRequest.HttpClientRequest>();
  const callers = yield* Queue.unbounded<Invocation.McpInvocationScope>();
  const release = yield* Deferred.make<void>();
  const hostSettings = yield* Layer.build(Settings.layerTest());
  const hostScope = yield* Effect.acquireRelease(Scope.make(), (scope) =>
    Scope.close(scope, Exit.void),
  );
  const server = yield* Layer.build(NodeHttpServer.layerTest).pipe(
    Effect.provideService(Scope.Scope, hostScope),
  );
  // Inspect caller identity at authorization, before the shared query strips per-caller context.
  const hostThreads = Layer.mock(Threads.ThreadManagementService)({});
  const research = Layer.effect(
    WebResearch.WebResearch,
    Effect.gen(function* () {
      const service = yield* WebResearch.WebResearch;
      return WebResearch.WebResearch.of({
        ...service,
        search: (input) =>
          Effect.serviceOption(Invocation.McpInvocationContext).pipe(
            Effect.flatMap((scope) =>
              Option.isSome(scope)
                ? Queue.offer(callers, scope.value)
                : Effect.die("The real peer invocation must be present."),
            ),
            Effect.andThen(service.search(input)),
          ),
      });
    }),
  ).pipe(
    Layer.provide(
      reader(
        HttpClient.make((request) =>
          Effect.gen(function* () {
            yield* Queue.offer(requests, request);
            yield* Deferred.await(release);
            const url = new URL(request.url);
            for (const [key, value] of request.urlParams)
              url.searchParams.append(
                key,
                Redacted.isRedacted(value) ? Redacted.value(value) : value,
              );
            const hosted = url.hostname === "treg.to";
            return HttpClientResponse.fromWeb(
              request,
              new Response(
                JSON.stringify({
                  query: url.searchParams.get(hosted ? "query" : "q"),
                  results: [
                    {
                      url: "https://pl.example/",
                      title: "Shared discovery",
                      snippet: "Polskie miejsca",
                      engines: ["google cse"],
                    },
                  ],
                  ...(hosted ? { total_results: 1, page: 0 } : { unresponsive_engines: [] }),
                }),
                {
                  headers: {
                    "content-type": "application/json",
                    ...(hosted
                      ? {
                          "x-treg-cost-micro": "0",
                          "x-treg-call-id": "0123456789abcdef0123456789abcdef",
                        }
                      : {}),
                  },
                },
              ),
            );
          }),
        ),
      ),
    ),
  );
  const host = yield* servePeer(
    bestia,
    McpHttpServer.layerWebResearchToolkit.pipe(
      Layer.provide(research),
      Layer.provide(Layer.succeedContext(hostSettings)),
      Layer.provide(hostThreads),
      Layer.provide(Layer.mock(PeerForwarding.PeerForwarding)({})),
      Layer.provide(Layer.mock(PeerLinks.PeerLinks)({})),
    ),
  ).pipe(Effect.provide(server), Effect.provideService(Scope.Scope, hostScope));
  const source = Effect.fnUntraced(function* (descriptor: typeof cavallo) {
    const linking = yield* Layer.build(layerLinkingEnvironment(descriptor));
    const local = yield* Queue.unbounded<string>();
    const services = yield* Layer.build(
      McpHttpServer.layerWebResearchToolkit.pipe(
        Layer.provideMerge(McpServer.McpServer.layer),
        Layer.provide(
          reader(
            HttpClient.make((request) =>
              Queue.offer(local, request.url).pipe(
                Effect.andThen(Effect.die("Configured host is authoritative.")),
              ),
            ),
          ),
        ),
        Layer.provide(PeerForwarding.layer),
        Layer.provideMerge(
          Settings.layerTest({
            linkedEnvironmentsEnabled: true,
            webResearch: { host: bestia.environmentId },
          }),
        ),
        Layer.provide(Layer.succeedContext(linking)),
        Layer.provide(
          Layer.mock(Threads.ThreadManagementService)({
            getThreadShell: (id) =>
              Effect.succeed(
                liveThreadShell(id, { runtimeMode: "approval-required", interactionMode: "plan" }),
              ),
          }),
        ),
        Layer.fresh,
      ),
    );
    const server = Context.get(services, McpServer.McpServer);
    const scope = scopeOf(descriptor.environmentId);
    return {
      links: Context.get(linking, PeerLinks.PeerLinks),
      peers: Context.get(linking, PeerMcpClient.PeerMcpClient),
      settings: Context.get(services, Settings.ServerSettingsService),
      local,
      scope,
      call: (input: Record<string, unknown>) =>
        server
          .callTool({ name: "web_search", arguments: input })
          .pipe(
            Effect.provideService(Invocation.McpInvocationContext, scope),
            Effect.provideService(McpSchema.McpServerClient, client),
          ),
    };
  });
  return {
    a: yield* source(cavallo),
    b: yield* source(rocky),
    host,
    requests,
    callers,
    release,
    settings: Context.get(hostSettings, Settings.ServerSettingsService),
    stop: Scope.close(hostScope, Exit.void),
  };
});

it.effect(
  "forwards two environments through real authenticated peers to one search and preserves caller limits",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* linkTo(f.a.links, f.host);
      yield* linkTo(f.b.links, f.host);
      const first = yield* Effect.forkScoped(
        f.a.call({ query: "site:pl.example polskie miejsca", locale: "pl-PL" }),
      );
      yield* Queue.take(f.requests);
      yield* Queue.take(f.requests);
      const a = yield* Queue.take(f.callers);
      expect(a.thread).toBeUndefined();
      expect(a.client).toMatchObject({
        linked: true,
        access: "auto",
        narrowedTo: { runtimeMode: "approval-required", interactionMode: "plan" },
      });
      const second = yield* Effect.forkScoped(
        f.b.call({ query: "site:pl.example polskie miejsca", locale: "pl-PL" }),
      );
      const b = yield* Queue.take(f.callers);
      expect(b.client?.sessionId).not.toBe(a.client?.sessionId);
      yield* TestClock.adjust(0);
      yield* Deferred.succeed(f.release, undefined);
      const one = decode((yield* Fiber.join(first)).structuredContent);
      const two = decode((yield* Fiber.join(second)).structuredContent);
      expect(one).toEqual(two);
      expect(one).toMatchObject({
        status: "ok",
        researchEnvironmentId: bestia.environmentId,
        locale: "pl-PL",
      });
      expect(one.results.map((item) => item.source)).toEqual(["searxng", "tinyfish"]);
      expect(yield* Queue.size(f.requests)).toBe(0);
      expect(yield* Queue.size(f.a.local)).toBe(0);
      expect(yield* Queue.size(f.b.local)).toBe(0);
    }).pipe(Effect.provide(NodeCrypto.layer)),
);

it.effect(
  "keeps unknown, wrong-identity, disconnected and chained research hosts authoritative",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      expect(failure(yield* f.a.call({ query: "unknown" })).code).toBe("research_host_unavailable");
      yield* linkTo(f.a.links, f.host);
      yield* Ref.set(f.host.descriptor, descriptorOf("wrong-environment", "Wrong"));
      expect(failure(yield* f.a.call({ query: "identity" })).code).toBe(
        "research_host_unavailable",
      );
      yield* Ref.set(f.host.descriptor, bestia);
      yield* f.settings.updateSettings({ webResearch: { host: cavallo.environmentId } });
      expect(failure(yield* f.a.call({ query: "loop" })).code).toBe("research_host_unavailable");
      yield* f.settings.updateSettings({ webResearch: { host: null } });
      yield* f.a.settings.updateSettings({ webResearch: { host: rocky.environmentId } });
      expect(
        failure(yield* f.a.call({ query: "bypass", environmentId: bestia.environmentId })).code,
      ).toBe("invalid_request");
      yield* f.a.settings.updateSettings({ webResearch: { host: bestia.environmentId } });
      yield* f.stop;
      expect(failure(yield* f.a.call({ query: "offline" })).code).toBe("research_host_unavailable");
      expect(yield* Queue.size(f.requests)).toBe(0);
      expect(yield* Queue.size(f.a.local)).toBe(0);
    }).pipe(Effect.provide(NodeCrypto.layer)),
);

it.effect("denies a read-only link at both source and destination without searching", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* linkTo(f.a.links, f.host, "read-only");
    expect(failure(yield* f.a.call({ query: "denied" })).code).toBe("capability_denied");
    const direct = yield* f.a.peers
      .call({
        environmentId: bestia.environmentId,
        tool: "web_search",
        arguments: { query: "denied" },
        limits: { runtimeMode: "full-access", interactionMode: "default" },
        success: HostedWebSearchResult,
      })
      .pipe(Effect.flip);
    expect(direct.code).toBe("capability_denied");
    expect(yield* Queue.size(f.requests)).toBe(0);
    expect(yield* Queue.size(f.a.local)).toBe(0);
  }).pipe(Effect.provide(NodeCrypto.layer)),
);
