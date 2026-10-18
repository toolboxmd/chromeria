import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import type * as HttpClientRequest from "effect/http/HttpClientRequest";
import { McpSchema, McpServer } from "effect/ai";

import * as Config from "../config.ts";
import * as SecretStore from "../auth/ServerSecretStore.ts";
import * as EffectWorker from "../orchestration-v2/EffectWorker.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as Registry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Replay from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as PeerForwarding from "../peer/PeerForwarding.ts";
import * as PeerLinks from "../peer/PeerLinks.ts";
import * as Settings from "../serverSettings.ts";
import * as PageCache from "../webResearch/PageCache.ts";
import * as PublicHttp from "../webResearch/PublicHttp.ts";
import * as PublicRender from "../webResearch/PublicRender.ts";
import * as TinyFish from "../webResearch/TinyFish.ts";
import * as WebResearch from "../webResearch/WebResearch.ts";
import { HostedWebReadResult } from "../webResearch/ResearchHost.ts";
import * as Invocation from "./McpInvocationContext.ts";
import * as McpHttpServer from "./McpHttpServer.ts";

const ROOT = "https://93.184.216.34/";
const TOKEN = "fixture-only-not-a-live-credential";
const shell = "<html><title>Loading</title><body><div id='app'></div></body></html>";
const wall = "<html><title>Just a moment</title><body>Checking your browser</body></html>";
const article = `<html><title>Hosted rooms</title><body><h1>Rooms</h1><p>${"Eight rooms face the garden. ".repeat(1000)}</p><table><tr><td>Family</td><td>4</td></tr></table><a href='/camere'>All rooms</a><script type='application/ld+json'>{"numberOfRooms":8}</script></body></html>`;
const response = (body: string, status = 200) => ({
  body,
  status,
  headers: { "content-type": "text/html" },
});
const hostedResponse = (request: HttpClientRequest.HttpClientRequest, body = article) =>
  HttpClientResponse.fromWeb(
    request,
    new Response(
      JSON.stringify({
        results: [{ url: ROOT, final_url: `${ROOT}rooms`, text: body, format: "html" }],
        errors: [],
      }),
      {
        headers: {
          "content-type": "application/json",
          "x-treg-cost-micro": "0",
          "x-treg-call-id": "0123456789abcdef0123456789abcdef",
        },
      },
    ),
  );
const decodeResult = Schema.decodeUnknownEffect(HostedWebReadResult);

const fixture = Effect.fnUntraced(function* (
  options: {
    http?: PublicHttp.PublicHttp["Service"]["get"];
    render?: PublicRender.PublicRender["Service"]["render"];
    hosted?: (
      request: HttpClientRequest.HttpClientRequest,
    ) => Effect.Effect<HttpClientResponse.HttpClientResponse>;
  } = {},
) {
  const requests = yield* Queue.unbounded<string>();
  const cacheReads = yield* Queue.unbounded<string>();
  const providerRequests = yield* Queue.unbounded<HttpClientRequest.HttpClientRequest>();
  const order: string[] = [];
  const base = Layer.mergeAll(NodeServices.layer, NodeCrypto.layer);
  const config = Config.layerTest(process.cwd(), { prefix: "research-tinyfish-" }).pipe(
    Layer.provide(base),
  );
  const observedCache = Layer.effect(
    PageCache.PageCache,
    Effect.gen(function* () {
      const cache = yield* PageCache.PageCache;
      return PageCache.PageCache.of({
        ...cache,
        get: (key) =>
          cache
            .get(key)
            .pipe(
              Effect.tap(() => Queue.offer(cacheReads, "url" in key ? key.url : "continuation")),
            ),
      });
    }),
  ).pipe(Layer.provide(PageCache.layer));
  const dependencies = Layer.mergeAll(observedCache, SecretStore.layer).pipe(
    Layer.provideMerge(config),
    Layer.provideMerge(base),
  );
  const hosted = TinyFish.layer.pipe(
    Layer.provide(
      Layer.succeed(
        HttpClient.HttpClient,
        HttpClient.make((request) =>
          Effect.gen(function* () {
            order.push("tinyfish");
            yield* Queue.offer(providerRequests, request);
            return options.hosted ? yield* options.hosted(request) : hostedResponse(request);
          }),
        ),
      ),
    ),
    Layer.provideMerge(dependencies),
  );
  const reader = WebResearch.layer.pipe(
    Layer.provide(
      Layer.succeed(PublicHttp.PublicHttp, {
        get: (url) =>
          Effect.gen(function* () {
            order.push(url.pathname === "/robots.txt" ? "robots" : "http");
            yield* Queue.offer(requests, url.href);
            return url.pathname === "/robots.txt"
              ? response("User-agent: *\nAllow: /")
              : options.http
                ? yield* options.http(url)
                : response(shell);
          }),
      }),
    ),
    Layer.provide(
      Layer.succeed(PublicRender.PublicRender, {
        render: (input) =>
          Effect.gen(function* () {
            order.push("render");
            return options.render
              ? yield* options.render(input)
              : { body: wall, finalUrl: input.url.href, httpStatus: 200, headers: {} };
          }),
      }),
    ),
    Layer.provideMerge(hosted),
  );
  const runtime = Replay.layerWithRegistry(
    { name: "tinyfish-read" },
    Registry.layerFromAdapters([]),
    { runEffectWorker: false },
  );
  const layer = McpHttpServer.layerWebResearchToolkit.pipe(
    Layer.provideMerge(McpServer.McpServer.layer),
    Layer.provideMerge(reader),
    Layer.provide(Settings.layerTest()),
    Layer.provide(Layer.mock(PeerForwarding.PeerForwarding)({})),
    Layer.provide(Layer.mock(PeerLinks.PeerLinks)({})),
    Layer.provideMerge(runtime),
    Layer.provide(NodeCrypto.layer),
  );
  const start = Effect.fnUntraced(function* () {
    const fiber = yield* Effect.forkScoped(call({ url: ROOT, followSubpages: false }));
    expect(yield* Queue.take(requests)).toBe(`${ROOT}robots.txt`);
    yield* TestClock.adjust(3000);
    expect(yield* Queue.take(requests)).toBe(ROOT);
    return fiber;
  });
  return { layer, requests, cacheReads, providerRequests, order, start };
});

const threadId = ThreadId.make("tinyfish-integration-thread");
const client = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  clientCapabilities: {},
  clientInfo: { name: "tinyfish-proof", version: "1" },
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "tinyfish-proof", version: "1" },
  },
  getClient: Effect.die("unused"),
});
const setup = Effect.gen(function* () {
  yield* Orchestrator.OrchestratorV2.use((o) =>
    o.dispatch({
      type: "thread.create",
      commandId: CommandId.make("tinyfish:create"),
      threadId,
      projectId: ProjectId.make("research-project"),
      title: "TinyFish proof",
      branch: null,
      worktreePath: null,
      modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "test" },
      runtimeMode: "full-access",
      interactionMode: "default",
      createdBy: "user",
      creationSource: "web",
    }),
  );
  yield* EffectWorker.OrchestrationEffectWorkerV2.use((w) => w.drain());
  yield* SecretStore.ServerSecretStore.use((s) =>
    s.create(TinyFish.TREG_SECRET_NAME, new TextEncoder().encode(TOKEN)),
  );
});
const call = Effect.fnUntraced(function* (arguments_: Record<string, unknown>) {
  yield* EffectWorker.OrchestrationEffectWorkerV2.use((w) => w.drain());
  const result = yield* McpServer.McpServer.use((server) =>
    server.callTool({ name: "web_read", arguments: arguments_ }),
  ).pipe(
    Effect.provideService(McpSchema.McpServerClient, client),
    Effect.provideService(Invocation.McpInvocationContext, {
      environmentId: EnvironmentId.make("tinyfish-environment"),
      requestNamespace: "tinyfish-agent",
      issuedAt: 0,
      thread: {
        threadId,
        providerSessionId: "tinyfish-session",
        providerInstanceId: ProviderInstanceId.make("codex"),
      },
      client: undefined,
      capabilities: new Set<Invocation.McpCapability>(),
    }),
  );
  yield* EffectWorker.OrchestrationEffectWorkerV2.use((w) => w.drain());
  return result;
});
const read = Effect.fnUntraced(function* (arguments_: Record<string, unknown>) {
  const result = yield* call(arguments_);
  expect(result.isError, JSON.stringify(result.content)).not.toBe(true);
  return yield* decodeResult(result.structuredContent);
});

it.effect(
  "MCP falls back only after local rendering and preserves hosted extraction, charge and paging",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* Effect.gen(function* () {
        yield* setup;
        const before = yield* Orchestrator.OrchestratorV2.use((o) =>
          o.getThreadProjection(threadId),
        );
        const result = yield* Fiber.join(yield* f.start());
        expect(result.isError, JSON.stringify(result.content)).not.toBe(true);
        const page = yield* decodeResult(result.structuredContent);
        expect(page).toMatchObject({
          status: "ok",
          rung: "tinyfish",
          sourceRung: "tinyfish",
          cacheStorage: "memory",
          title: "Hosted rooms",
          finalUrl: `${ROOT}rooms`,
          nextOffset: 20_000,
          providerCall: {
            endpoint: "tinyfish.web.fetch",
            callId: "0123456789abcdef0123456789abcdef",
            chargedMicro: 0,
          },
        });
        expect(page.cacheId).toMatch(/^tinyfish:[a-f0-9]{64}$/);
        const rest = yield* read({
          cacheId: page.cacheId,
          cacheEnvironmentId: page.cacheEnvironmentId,
          offset: page.nextOffset,
        });
        expect(rest).toMatchObject({
          rung: "cache",
          sourceRung: "tinyfish",
          cacheStorage: "memory",
          cacheId: page.cacheId,
          contentHash: page.contentHash,
        });
        const full = page.text + rest.text;
        expect(full).toContain("| Family | 4 |");
        expect(full).toContain("https://93.184.216.34/camere");
        expect(full).toContain('JSON-LD:\n{"numberOfRooms":8}');
        expect(f.order).toEqual(["robots", "http", "render", "tinyfish"]);
        const request = yield* Queue.take(f.providerRequests);
        expect(request.method).toBe("POST");
        expect(request.url).toBe("https://treg.to/call/tinyfish.web.fetch");
        expect(request.headers["x-treg-route-max-cost"]).toBe("0");
        expect(request.headers["x-treg-token"]).toBe(TOKEN);
        expect(JSON.stringify(result)).not.toContain(TOKEN);
        const fs = yield* FileSystem.FileSystem;
        const config = yield* Config.ServerConfig;
        expect(
          (yield* fs.readDirectory(`${config.stateDir}/web-cache`)).filter((p) =>
            p.endsWith(".gz"),
          ),
        ).toEqual([]);
        expect(
          yield* Orchestrator.OrchestratorV2.use((o) => o.getThreadProjection(threadId)),
        ).toEqual(before);
      }).pipe(Effect.provide(f.layer));
    }),
);

it.effect.each(["http", "render", "http_wall", "render_login", "blocked_render"] as const)(
  "%s stops before hosted egress when the local ladder must not escalate",
  (kind) =>
    Effect.gen(function* () {
      const f = yield* fixture({
        http: () =>
          Effect.succeed(response(kind === "http" ? article : kind === "http_wall" ? wall : shell)),
        render: (input) =>
          kind === "blocked_render"
            ? Effect.fail(
                new PublicRender.PublicRenderError({
                  status: "blocked_local",
                  httpStatus: null,
                  retryAfterMs: null,
                }),
              )
            : Effect.succeed({
                body: kind === "render_login" ? "<h1>Sign in</h1><input type='password'>" : article,
                finalUrl: input.url.href,
                httpStatus: 200,
                headers: {},
              }),
      });
      yield* Effect.gen(function* () {
        yield* setup;
        const result = yield* Fiber.join(yield* f.start());
        expect(result.isError, JSON.stringify(result.content)).not.toBe(true);
        const page = yield* decodeResult(result.structuredContent);
        expect(page.sourceRung).toBe(kind.startsWith("http") ? "http" : "render");
        expect(page.status).toBe(
          kind === "http_wall"
            ? "challenge"
            : kind === "render_login"
              ? "login"
              : kind === "blocked_render"
                ? "blocked_local"
                : "ok",
        );
        expect(yield* Queue.size(f.providerRequests)).toBe(0);
      }).pipe(Effect.provide(f.layer));
    }),
);

it.effect("a renderer transport failure can use the hosted fallback", () =>
  Effect.gen(function* () {
    const f = yield* fixture({
      render: () =>
        Effect.fail(
          new PublicRender.PublicRenderError({
            status: "provider_error",
            httpStatus: null,
            retryAfterMs: null,
          }),
        ),
    });
    yield* Effect.gen(function* () {
      yield* setup;
      const result = yield* Fiber.join(yield* f.start());
      expect(yield* decodeResult(result.structuredContent)).toMatchObject({
        status: "ok",
        sourceRung: "tinyfish",
      });
      expect(f.order).toEqual(["robots", "http", "render", "tinyfish"]);
    }).pipe(Effect.provide(f.layer));
  }),
);

it.effect.each([
  {
    body: "<html><title>Just a moment</title><body>Checking your browser</body></html>",
    status: "challenge",
  },
  {
    body: "<html><title>502 Bad Gateway</title><body>Origin is down</body></html>",
    status: "origin_error",
  },
  {
    body: "<html><title>Cookie consent</title><body>Accept all cookies to continue</body></html>",
    status: "consent",
  },
  {
    body: "<html><title>Sign in</title><body><input type='password'>Log in to continue</body></html>",
    status: "login",
  },
  { body: shell, status: "thin" },
])("hosted $status is a typed empty page, never successful extracted text", ({ body, status }) =>
  Effect.gen(function* () {
    const f = yield* fixture({ hosted: (r) => Effect.succeed(hostedResponse(r, body)) });
    yield* Effect.gen(function* () {
      yield* setup;
      const result = yield* Fiber.join(yield* f.start());
      expect(result.isError, JSON.stringify(result.content)).not.toBe(true);
      const page = yield* decodeResult(result.structuredContent);
      expect(page).toMatchObject({
        status,
        sourceRung: "tinyfish",
        text: "",
        cacheStorage: "memory",
        nextOffset: null,
      });
      const rest = yield* read({
        cacheId: page.cacheId,
        cacheEnvironmentId: page.cacheEnvironmentId,
      });
      expect(rest).toMatchObject({ status, text: "", sourceRung: "tinyfish", rung: "cache" });
    }).pipe(Effect.provide(f.layer));
  }),
);

it.effect("expired hosted continuation returns an actionable error and never starts a fetch", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    yield* Effect.gen(function* () {
      yield* setup;
      const first = yield* Fiber.join(yield* f.start());
      const page = yield* decodeResult(first.structuredContent);
      yield* TestClock.adjust(899_999);
      const rest = yield* read({
        cacheId: page.cacheId,
        cacheEnvironmentId: page.cacheEnvironmentId,
        offset: page.nextOffset,
      });
      expect(rest.cacheId).toBe(page.cacheId);
      expect(rest.cacheExpiresAt).toBe(page.cacheExpiresAt);
      yield* TestClock.adjust(1);
      const expired = yield* call({
        cacheId: page.cacheId,
        cacheEnvironmentId: page.cacheEnvironmentId,
        offset: page.nextOffset,
      });
      expect(expired.isError).toBe(true);
      expect(JSON.stringify(expired.content)).toContain("15 minutes");
      expect(JSON.stringify(expired.content)).toContain("Read the URL again");
      expect(JSON.stringify(expired.content)).toContain("invalid_request");
      expect(f.order).toEqual(["robots", "http", "render", "tinyfish"]);
    }).pipe(Effect.provide(f.layer));
  }),
);

it.effect.each(["charge", "quota"] as const)(
  "MCP exposes a typed %s failure without token or provider body leakage",
  (kind) =>
    Effect.gen(function* () {
      const f = yield* fixture({
        hosted: (r) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(
              r,
              new Response(JSON.stringify({ error: TOKEN }), {
                status: kind === "quota" ? 429 : 200,
                headers: {
                  "content-type": "application/json",
                  "x-treg-cost-micro": kind === "charge" ? "1" : "0",
                  "retry-after": "120",
                },
              }),
            ),
          ),
      });
      yield* Effect.gen(function* () {
        yield* setup;
        const result = yield* Fiber.join(yield* f.start());
        expect(result.isError).toBe(true);
        expect(result.structuredContent).toBeUndefined();
        const output = JSON.stringify(result.content);
        expect(output).not.toContain(TOKEN);
        expect(output).toContain(
          kind === "quota" ? "research_budget_exceeded" : "provider_unavailable",
        );
        expect(output).toContain(kind === "quota" ? "120 seconds" : "charge");
      }).pipe(Effect.provide(f.layer));
    }),
);

it.effect(
  "concurrent MCP readers share one hosted call and one cancellation preserves the other",
  () =>
    Effect.gen(function* () {
      const release = yield* Deferred.make<void>();
      let closed = 0;
      const f = yield* fixture({
        hosted: (r) =>
          Deferred.await(release).pipe(
            Effect.as(hostedResponse(r)),
            Effect.ensuring(
              Effect.sync(() => {
                closed++;
              }),
            ),
          ),
      });
      yield* Effect.gen(function* () {
        yield* setup;
        const first = yield* f.start();
        yield* Queue.take(f.providerRequests);
        expect(yield* Queue.take(f.cacheReads)).toBe(ROOT);
        expect(yield* Queue.take(f.cacheReads)).toBe(ROOT);
        const second = yield* Effect.forkScoped(call({ url: ROOT, followSubpages: false }));
        expect(yield* Queue.take(f.cacheReads)).toBe(ROOT);
        yield* Effect.yieldNow;
        yield* Fiber.interrupt(first);
        expect(closed).toBe(0);
        yield* Deferred.succeed(release, undefined);
        const remaining = yield* Fiber.join(second);
        expect(remaining.isError, JSON.stringify(remaining.content)).not.toBe(true);
        expect(yield* decodeResult(remaining.structuredContent)).toMatchObject({
          status: "ok",
          sourceRung: "tinyfish",
        });
        expect(f.order.filter((r) => r === "tinyfish")).toHaveLength(1);
        expect(closed).toBe(1);
        expect(yield* Queue.size(f.providerRequests)).toBe(0);
      }).pipe(Effect.provide(f.layer));
    }),
);

it.effect("the last MCP cancellation aborts hosted work without publishing partial text", () =>
  Effect.gen(function* () {
    const closed = yield* Deferred.make<void>();
    const f = yield* fixture({
      hosted: () => Effect.never.pipe(Effect.ensuring(Deferred.succeed(closed, undefined))),
    });
    yield* Effect.gen(function* () {
      yield* setup;
      const first = yield* f.start();
      yield* Queue.take(f.providerRequests);
      yield* Fiber.interrupt(first);
      yield* Deferred.await(closed);
      const cache = yield* PageCache.PageCache;
      expect(yield* cache.get({ url: ROOT })).toBeNull();
      const fs = yield* FileSystem.FileSystem;
      const config = yield* Config.ServerConfig;
      const quota = JSON.parse(
        yield* fs.readFileString(`${config.stateDir}/web-cache/tinyfish-quota.json`),
      );
      expect(quota.starts).toHaveLength(1);
      expect(
        (yield* fs.readDirectory(`${config.stateDir}/web-cache`)).filter((p) => p.endsWith(".gz")),
      ).toEqual([]);
    }).pipe(Effect.provide(f.layer));
  }),
);

it.effect(
  "a hosted child retains its own provenance and continuation beside a persistent local root",
  () =>
    Effect.gen(function* () {
      const childUrl = `${ROOT}camere`;
      const f = yield* fixture({
        http: (url) =>
          Effect.succeed(
            response(
              url.pathname === "/"
                ? "<h1>Venue</h1><p>Read our complete guest room information on the rooms page. The public homepage has useful information about the garden and restaurant.</p><a href='/camere'>Rooms</a>"
                : shell,
            ),
          ),
        hosted: (r) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(
              r,
              new Response(
                JSON.stringify({
                  results: [{ url: childUrl, final_url: childUrl, format: "html", text: article }],
                  errors: [],
                }),
                {
                  headers: {
                    "x-treg-cost-micro": "0",
                    "x-treg-call-id": "0123456789abcdef0123456789abcdef",
                  },
                },
              ),
            ),
          ),
      });
      yield* Effect.gen(function* () {
        yield* setup;
        const pending = yield* Effect.forkScoped(read({ url: ROOT }));
        expect(yield* Queue.take(f.requests)).toBe(`${ROOT}robots.txt`);
        yield* TestClock.adjust(3000);
        expect(yield* Queue.take(f.requests)).toBe(ROOT);
        yield* TestClock.adjust(3000);
        expect(yield* Queue.take(f.requests)).toBe(childUrl);
        const result = yield* Fiber.join(pending);
        expect(result).toMatchObject({
          sourceRung: "http",
          cacheStorage: "persistent",
          truncated: true,
          subpages: [
            { url: childUrl, finalUrl: childUrl, sourceRung: "tinyfish", cacheStorage: "memory" },
          ],
        });
        const child = result.subpages[0]!;
        expect(result.text.length + child.text.length).toBe(20_000);
        const rest = yield* read({
          cacheId: child.cacheId,
          cacheEnvironmentId: child.cacheEnvironmentId,
          offset: child.nextOffset,
        });
        expect(child.text + rest.text).toContain("| Family | 4 |");
        expect(rest).toMatchObject({
          sourceRung: "tinyfish",
          cacheId: child.cacheId,
          contentHash: child.contentHash,
        });
        const fs = yield* FileSystem.FileSystem;
        const config = yield* Config.ServerConfig;
        expect(
          (yield* fs.readDirectory(`${config.stateDir}/web-cache`)).filter((p) =>
            p.endsWith(".gz"),
          ),
        ).toEqual([`${result.cacheId}.gz`]);
      }).pipe(Effect.provide(f.layer));
    }),
);
