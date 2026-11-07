import * as Search from "./Search.ts";
import { NodeHttpServer } from "@effect/platform-node";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
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
import * as Scope from "effect/Scope";
import * as TestClock from "effect/testing/TestClock";
import { McpSchema, McpServer } from "effect/ai";

import * as Config from "../config.ts";
import * as McpHttpServer from "../mcp/McpHttpServer.ts";
import * as Invocation from "../mcp/McpInvocationContext.ts";
import { liveThreadShell } from "../mcp/McpToolAccess.testkit.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as PeerForwarding from "../peer/PeerForwarding.ts";
import * as PeerLinks from "../peer/PeerLinks.ts";
import * as PeerMcpClient from "../peer/PeerMcpClient.ts";
import * as PreviewBrowser from "../preview/PreviewBrowser.ts";
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
import * as TinyFish from "./TinyFish.ts";
import * as WebResearch from "./WebResearch.ts";
import { HostedWebReadResult } from "./ResearchHost.ts";
import { SKIP_WITHOUT_CHROMIUM, testChromium } from "../preview/ChromeForTesting.testkit.ts";

const bestia = descriptorOf("research-bestia", "Bestia");
const cavallo = descriptorOf("research-cavallo", "Cavallo");
const rocky = descriptorOf("research-rocky", "Rocky");
const url = "https://venue.example/rooms";
const body = `<h1>Venue rooms</h1><p>${"Eight rooms with beds and a garden for events. ".repeat(600)}</p>`;
const response = (text: string) => ({
  status: 200,
  headers: { "content-type": "text/html" },
  body: text,
});
const client = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  clientCapabilities: {},
  clientInfo: { name: "research-host-proof", version: "1" },
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "research-host-proof", version: "1" },
  },
  getClient: Effect.die("unused"),
});
const scopeOf = (environmentId: EnvironmentId): Invocation.McpInvocationScope => ({
  environmentId,
  requestNamespace: `agent:${environmentId}`,
  thread: {
    threadId: ThreadId.make(`thread:${environmentId}`),
    providerInstanceId: ProviderInstanceId.make("codex"),
    providerSessionId: `provider:${environmentId}`,
  },
  client: undefined,
  capabilities: new Set(), // Reading research is not orchestration control.
  issuedAt: 0,
});
const readerLayer = (
  get: PublicHttp.PublicHttp["Service"]["get"],
  render: PublicRender.PublicRender["Service"]["render"] = () =>
    Effect.die("A forwarding source or plain HTTP fixture must not render locally."),
) =>
  WebResearch.layer.pipe(
    Layer.provide(Layer.mock(Search.Search)({})),
    Layer.provide(
      Layer.succeed(TinyFish.TinyFish, { fetch: () => Effect.die("Unused TinyFish fetch") }),
    ),
    Layer.provide(PageCache.layer),
    Layer.provide(Layer.succeed(PublicHttp.PublicHttp, { get })),
    Layer.provide(Layer.succeed(PublicRender.PublicRender, { render })),
    Layer.provide(Config.layerTest(process.cwd(), { prefix: "research-host-cache-" })),
    Layer.provide(NodeServices.layer),
    Layer.fresh,
  );
const decodeFailure = Schema.decodeSync(
  Schema.fromJsonString(Schema.Struct({ code: Schema.String, message: Schema.String })),
);
const decodePage = Schema.decodeUnknownSync(HostedWebReadResult);
const failure = (result: McpSchema.CallToolResult) => {
  expect(result.isError, JSON.stringify(result.content)).toBe(true);
  expect(result.structuredContent).toBeUndefined();
  const part = result.content[0];
  if (part?.type !== "text") throw new Error("Expected a typed MCP failure");
  expect(part.text).toMatch(/^\s*\{/);
  return decodeFailure(part.text);
};
const page = (result: McpSchema.CallToolResult) => {
  expect(result.isError, JSON.stringify(result.content)).not.toBe(true);
  return decodePage(result.structuredContent);
};

const fixture = Effect.fnUntraced(function* (
  robots = "User-agent: *\nAllow: /",
  respond = (_target: URL) => response(body),
  render?: PublicRender.PublicRender["Service"]["render"],
) {
  const requests = yield* Queue.unbounded<string>();
  const callers = yield* Queue.unbounded<Invocation.McpInvocationScope>();
  const release = yield* Deferred.make<void>();
  const hostSettingsContext = yield* Layer.build(Settings.layerTest());
  const hostScope = yield* Effect.acquireRelease(Scope.make(), (scope) =>
    Scope.close(scope, Exit.void),
  );
  const hostServer = yield* Layer.build(NodeHttpServer.layerTest).pipe(
    Effect.provideService(Scope.Scope, hostScope),
  );
  const host = yield* servePeer(
    bestia,
    McpHttpServer.layerWebResearchToolkit.pipe(
      Layer.provide(
        readerLayer(
          (target) =>
            Effect.gen(function* () {
              yield* Queue.offer(requests, target.pathname);
              const invocation = yield* Effect.serviceOption(Invocation.McpInvocationContext);
              if (Option.isSome(invocation)) yield* Queue.offer(callers, invocation.value);
              if (target.pathname === "/robots.txt") return response(robots);
              yield* Deferred.await(release);
              return respond(target);
            }),
          render,
        ),
      ),
      Layer.provide(Layer.succeedContext(hostSettingsContext)),
      Layer.provide(Layer.mock(PeerForwarding.PeerForwarding)({})),
      Layer.provide(Layer.mock(PeerLinks.PeerLinks)({})),
      Layer.provide(Layer.mock(Threads.ThreadManagementService)({})),
    ),
  ).pipe(Effect.provide(hostServer), Effect.provideService(Scope.Scope, hostScope));
  const makeSource = Effect.fnUntraced(function* (descriptor: typeof cavallo) {
    const linking = yield* Layer.build(layerLinkingEnvironment(descriptor));
    const localRequests = yield* Queue.unbounded<string>();
    const threads = Layer.mock(Threads.ThreadManagementService)({
      getThreadShell: (id) =>
        Effect.succeed({
          ...liveThreadShell(id, { runtimeMode: "approval-required", interactionMode: "plan" }),
          ...(id === ThreadId.make("thread:linked")
            ? { linkOrigin: { sessionId: "another-peer", label: "Another peer" } }
            : {}),
        }),
    });
    const services = yield* Layer.build(
      McpHttpServer.layerWebResearchToolkit.pipe(
        Layer.provideMerge(McpServer.McpServer.layer),
        Layer.provide(
          readerLayer((target) =>
            Queue.offer(localRequests, target.pathname).pipe(
              Effect.as(
                response(target.pathname === "/robots.txt" ? "User-agent: *\nAllow: /" : body),
              ),
            ),
          ),
        ),
        Layer.provide(PeerForwarding.layer),
        Layer.provideMerge(Settings.layerTest({ webResearch: { host: bestia.environmentId } })),
        Layer.provide(Layer.succeedContext(linking)),
        Layer.provide(threads),
        Layer.fresh,
      ),
    );
    const server = Context.get(services, McpServer.McpServer);
    const scope = scopeOf(descriptor.environmentId);
    return {
      links: Context.get(linking, PeerLinks.PeerLinks),
      peers: Context.get(linking, PeerMcpClient.PeerMcpClient),
      settings: Context.get(services, Settings.ServerSettingsService),
      localRequests,
      scope,
      call: (input: Record<string, unknown>, invocation = scope) =>
        server
          .callTool({ name: "web_read", arguments: input })
          .pipe(
            Effect.provideService(Invocation.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          ),
    };
  });
  const a = yield* makeSource(cavallo);
  const b = yield* makeSource(rocky);
  return {
    a,
    b,
    host,
    requests,
    callers,
    release,
    stopHost: Scope.close(hostScope, Exit.void),
    hostSettings: Context.get(hostSettingsContext, Settings.ServerSettingsService),
  };
});
const testLayers = NodeCrypto.layer;

it.effect("returns shared subpage ownership and forwards selection through the peer", () =>
  Effect.gen(function* () {
    const root = `<h1>Guest house</h1><p>${"Visit the house and garden. ".repeat(12)}</p><a href='/conference'>Conference</a><a href='/conference-two'>Conference rates</a><a href='/camere'>Rooms</a>`;
    const f = yield* fixture(undefined, (target: URL) =>
      response(target.pathname === "/rooms" ? root : body),
    );
    yield* linkTo(f.a.links, f.host);
    yield* linkTo(f.b.links, f.host);
    yield* Deferred.succeed(f.release, undefined);
    const work = yield* Effect.forkScoped(
      f.a.call({ url, subpagePattern: "Conference", maxSubpages: 1 }),
    );
    expect(yield* Queue.take(f.requests)).toBe("/robots.txt");
    yield* TestClock.adjust(3000);
    expect(yield* Queue.take(f.requests)).toBe("/rooms");
    yield* TestClock.adjust(3000);
    expect(yield* Queue.take(f.requests)).toBe("/conference");
    const result = page(yield* Fiber.join(work));
    expect(result.subpages).toHaveLength(1);
    expect(result.pendingSubpages).toEqual(["https://venue.example/conference-two"]);
    expect(result.unvisitedSubpages).toBe(1);
    expect(result.truncationReasons).toContain("page_budget");
    const child = result.subpages[0]!;
    expect(child).toMatchObject({
      url: "https://venue.example/conference",
      status: "ok",
      cacheEnvironmentId: bestia.environmentId,
    });
    expect(child.nextOffset).not.toBeNull();
    const rest = page(
      yield* f.b.call({
        cacheId: child.cacheId,
        cacheEnvironmentId: child.cacheEnvironmentId,
        offset: child.nextOffset,
      }),
    );
    expect(rest).toMatchObject({ rung: "cache", cacheId: child.cacheId, nextOffset: null });
    expect(child.text + rest.text).toHaveLength(child.totalChars);
    expect(
      page(yield* f.b.call({ url, subpagePattern: "Conference", maxSubpages: 1 })).subpages[0],
    ).toMatchObject({
      rung: "cache",
      cacheId: child.cacheId,
      cacheEnvironmentId: bestia.environmentId,
    });
    yield* f.b.settings.updateSettings({ webResearch: { host: null } });
    expect(
      failure(
        yield* f.b.call({ cacheId: child.cacheId, cacheEnvironmentId: child.cacheEnvironmentId }),
      ).code,
    ).toBe("invalid_request");
    expect(yield* Queue.size(f.requests)).toBe(0);
    expect(yield* Queue.size(f.a.localRequests)).toBe(0);
    expect(yield* Queue.size(f.b.localRequests)).toBe(0);
  }).pipe(Effect.provide(testLayers)),
);

it.effect("preserves a host traversal deadline and its single-page retry guidance", () =>
  Effect.gen(function* () {
    const f = yield* fixture("User-agent: *\nAllow: /\nCrawl-delay: 40");
    yield* linkTo(f.a.links, f.host);
    yield* Deferred.succeed(f.release, undefined);
    const work = yield* Effect.forkScoped(f.a.call({ url }));
    expect(yield* Queue.take(f.requests)).toBe("/robots.txt");
    yield* TestClock.adjust("30 seconds");
    const error = failure(yield* Fiber.join(work));
    expect(error.code).toBe("research_budget_exceeded");
    expect(error.message).toContain("followSubpages:false");
    expect(yield* Queue.size(f.requests)).toBe(0);
    const retry = yield* Effect.forkScoped(f.a.call({ url, followSubpages: false }));
    yield* TestClock.adjust("10 seconds");
    expect(yield* Queue.take(f.requests)).toBe("/rooms");
    expect(page(yield* Fiber.join(retry))).toMatchObject({ status: "ok", rung: "http" });
    expect(yield* Queue.size(f.a.localRequests)).toBe(0);
  }).pipe(Effect.provide(testLayers)),
);

it.effect("shares successful rendered caching after an ignored iframe queue refusal", () =>
  Effect.gen(function* () {
    let renders = 0;
    const f = yield* fixture(
      undefined,
      () => ({
        ...response("<div id='app'></div>"),
        headers: { "content-type": "text/html", "retry-after": "120" },
      }),
      (input) =>
        Effect.gen(function* () {
          renders++;
          const refused = yield* input
            .loadDocument(new URL("/iframe", input.url))
            .pipe(Effect.flip, Effect.orDie);
          expect(refused).toMatchObject({ status: "challenge", retryAfterMs: 120_000 });
          return { body, finalUrl: input.url.href, httpStatus: 200, headers: {} };
        }),
    );
    yield* linkTo(f.a.links, f.host);
    yield* linkTo(f.b.links, f.host);
    yield* Deferred.succeed(f.release, undefined);
    const work = yield* Effect.forkScoped(f.a.call({ url, followSubpages: false }));
    expect(yield* Queue.take(f.requests)).toBe("/robots.txt");
    yield* TestClock.adjust(3000);
    expect(yield* Queue.take(f.requests)).toBe("/rooms");
    const first = page(yield* Fiber.join(work));
    expect(first).toMatchObject({ status: "ok", sourceRung: "render", nextOffset: 20_000 });
    const rest = page(
      yield* f.b.call({
        cacheId: first.cacheId,
        cacheEnvironmentId: first.cacheEnvironmentId,
        offset: first.nextOffset,
      }),
    );
    expect(rest).toMatchObject({ rung: "cache", sourceRung: "render", nextOffset: null });
    expect(first.text + rest.text).toHaveLength(first.totalChars);
    expect(page(yield* f.b.call({ url, followSubpages: false }))).toMatchObject({
      rung: "cache",
      cacheId: first.cacheId,
    });
    expect(renders).toBe(1);
    expect(yield* Queue.size(f.requests)).toBe(0);
    expect(yield* Queue.size(f.a.localRequests)).toBe(0);
    expect(yield* Queue.size(f.b.localRequests)).toBe(0);
  }).pipe(Effect.provide(testLayers)),
);

it.effect(
  "shares real rendered subpages, continuation and restricted navigation across peers",
  (ctx) =>
    Effect.gen(function* () {
      const executable = testChromium;
      if (!executable) return ctx.skip(SKIP_WITHOUT_CHROMIUM);
      const rendererLayer = PublicRender.layer.pipe(
        Layer.provide(
          Layer.succeed(PreviewBrowser.PreviewBrowser, {
            installed: Effect.succeedSome(executable),
            executable: Effect.die("Research must not install a browser."),
          }),
        ),
        Layer.provide(NodeServices.layer),
      );
      yield* Effect.gen(function* () {
        const renderer = yield* PublicRender.PublicRender;
        const root = `<h1>Guest house</h1><p>${"Visit the house and garden. ".repeat(12)}</p><a href='/camere'>Rooms</a><a href='/conference'>Conference</a>`;
        const scripted = (html: string) =>
          `<body><script>document.body.innerHTML=${JSON.stringify(html).replaceAll("<", "\\u003c")}</script></body>`;
        const f = yield* fixture(
          undefined,
          (target: URL) =>
            response(
              target.pathname === "/rooms"
                ? scripted(root)
                : target.pathname === "/conference"
                  ? "<script>location.href='https://foreign.example/rooms';</script>"
                  : scripted(`${body}<table><tr><td>Family</td><td>4</td></tr></table>`),
            ),
          renderer.render,
        );
        yield* linkTo(f.a.links, f.host);
        yield* linkTo(f.b.links, f.host);
        yield* Deferred.succeed(f.release, undefined);
        const work = yield* Effect.forkScoped(
          f.a.call({ url, subpagePattern: "ROOMS", maxSubpages: 1 }),
        );
        expect(yield* Queue.take(f.requests)).toBe("/robots.txt");
        yield* TestClock.adjust(3000);
        expect(yield* Queue.take(f.requests)).toBe("/rooms");
        yield* TestClock.adjust(3000);
        expect(yield* Queue.take(f.requests)).toBe("/camere");
        const first = page(yield* Fiber.join(work));
        expect(first).toMatchObject({
          status: "ok",
          sourceRung: "render",
          cacheEnvironmentId: bestia.environmentId,
        });
        expect(first.subpages).toHaveLength(1);
        const child = first.subpages[0]!;
        expect(child).toMatchObject({
          status: "ok",
          sourceRung: "render",
          cacheEnvironmentId: bestia.environmentId,
        });
        expect(first.text.length + child.text.length).toBe(20_000);
        const rest = page(
          yield* f.b.call({
            cacheId: child.cacheId,
            cacheEnvironmentId: child.cacheEnvironmentId,
            offset: child.nextOffset,
          }),
        );
        expect(rest).toMatchObject({ rung: "cache", sourceRung: "render", nextOffset: null });
        expect(child.text + rest.text).toContain("| Family | 4 |");
        expect(page(yield* f.b.call({ url, subpagePattern: "ROOMS" })).subpages[0]).toMatchObject({
          rung: "cache",
          cacheId: child.cacheId,
          sourceRung: "render",
        });
        const blocked = yield* Effect.forkScoped(f.b.call({ url, subpagePattern: "Conference" }));
        yield* TestClock.adjust(3000);
        expect(yield* Queue.take(f.requests)).toBe("/conference");
        expect(page(yield* Fiber.join(blocked)).subpages[0]).toMatchObject({
          status: "unsupported",
          sourceRung: "render",
          cacheEnvironmentId: bestia.environmentId,
          text: "",
        });
        expect(yield* Queue.size(f.requests)).toBe(0);
        expect(yield* Queue.size(f.a.localRequests)).toBe(0);
        expect(yield* Queue.size(f.b.localRequests)).toBe(0);
      }).pipe(Effect.provide(rendererLayer));
    }).pipe(Effect.provide(testLayers)),
);

it.effect(
  "two isolated environments share one authenticated host fetch, TTL hit and continuation",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* linkTo(f.a.links, f.host);
      yield* linkTo(f.b.links, f.host);
      const first = yield* Effect.forkScoped(f.a.call({ url }));
      expect(yield* Queue.take(f.requests)).toBe("/robots.txt");
      yield* TestClock.adjust(3000);
      expect(yield* Queue.take(f.requests)).toBe("/rooms");
      const second = yield* Effect.forkScoped(f.b.call({ url }));
      yield* Deferred.succeed(f.release, undefined);
      const a = page(yield* Fiber.join(first));
      const b = page(yield* Fiber.join(second));
      expect(a).toMatchObject({
        status: "ok",
        rung: "http",
        cacheEnvironmentId: bestia.environmentId,
      });
      expect(b).toMatchObject({ rung: "cache", cacheId: a.cacheId, contentHash: a.contentHash });
      expect(page(yield* f.b.call({ url }))).toMatchObject({ rung: "cache", cacheId: a.cacheId });
      const rest = page(
        yield* f.b.call({
          cacheId: a.cacheId,
          cacheEnvironmentId: a.cacheEnvironmentId,
          offset: a.nextOffset,
        }),
      );
      expect(a.text + rest.text).toHaveLength(a.totalChars);
      expect(rest).toMatchObject({ rung: "cache", nextOffset: null, cacheId: a.cacheId });
      const caller = yield* Queue.take(f.callers);
      expect(caller.thread).toBeUndefined();
      expect(caller.client).toMatchObject({
        linked: true,
        access: "auto",
        narrowedTo: { runtimeMode: "approval-required", interactionMode: "plan" },
        sessionId: (yield* f.host.linkedSessions).find((entry) =>
          entry.client.label?.includes("Cavallo"),
        )!.sessionId,
      });
      expect(yield* Queue.size(f.requests)).toBe(0);
      expect(yield* Queue.size(f.a.localRequests)).toBe(0);
      expect(yield* Queue.size(f.b.localRequests)).toBe(0);
      yield* f.a.settings.updateSettings({ webResearch: { host: null } });
      expect(
        failure(yield* f.a.call({ cacheId: a.cacheId, cacheEnvironmentId: a.cacheEnvironmentId }))
          .code,
      ).toBe("invalid_request");
      yield* f.a.settings.updateSettings({ webResearch: { host: rocky.environmentId } });
      expect(
        failure(yield* f.a.call({ cacheId: a.cacheId, cacheEnvironmentId: a.cacheEnvironmentId }))
          .code,
      ).toBe("invalid_request");
      expect(yield* Queue.size(f.a.localRequests)).toBe(0);
    }).pipe(Effect.provide(testLayers)),
);

it.effect(
  "denies outside callers, read-only links and revoked links without any origin fetch",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      for (const access of ["read-only", "auto"] as const) {
        const denied = yield* f.a.call(
          { url },
          {
            ...f.a.scope,
            thread: undefined,
            client: { sessionId: "outside", label: "Outside", access },
          },
        );
        expect(failure(denied).code).toBe("thread_credential_required");
      }
      yield* linkTo(f.a.links, f.host, "read-only");
      expect(failure(yield* f.a.call({ url })).code).toBe("capability_denied");
      // Call the destination directly to exercise its authorization without the source preflight.
      const deniedAtHost = yield* f.a.peers
        .call({
          environmentId: bestia.environmentId,
          tool: "web_read",
          arguments: { url },
          limits: { runtimeMode: "full-access", interactionMode: "default" },
          success: HostedWebReadResult,
        })
        .pipe(Effect.flip);
      expect(deniedAtHost.code).toBe("capability_denied");
      yield* linkTo(f.b.links, f.host, "auto");
      expect(
        failure(
          yield* f.b.call(
            { url },
            {
              ...f.b.scope,
              thread: { ...f.b.scope.thread!, threadId: ThreadId.make("thread:linked") },
            },
          ),
        ).code,
      ).toBe("capability_denied");
      const session = (yield* f.host.linkedSessions).find((entry) =>
        entry.client.label?.includes("Rocky"),
      );
      yield* f.host.auth.revokeSession(session!.sessionId);
      expect(failure(yield* f.b.call({ url })).code).toBe("capability_denied");
      expect(yield* Queue.size(f.requests)).toBe(0);
      expect(yield* Queue.size(f.a.localRequests)).toBe(0);
      expect(yield* Queue.size(f.b.localRequests)).toBe(0);
    }).pipe(Effect.provide(testLayers)),
);

it.effect("rejects unknown, wrong-identity and unavailable hosts with no local fallback", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    expect(failure(yield* f.a.call({ url })).code).toBe("research_host_unavailable");
    yield* linkTo(f.a.links, f.host);
    yield* Ref.set(f.host.descriptor, descriptorOf("another-environment", "Another"));
    expect(failure(yield* f.a.call({ url })).code).toBe("research_host_unavailable");
    yield* Ref.set(f.host.descriptor, bestia);
    yield* f.stopHost;
    expect(failure(yield* f.a.call({ url })).code).toBe("research_host_unavailable");
    expect(yield* Queue.size(f.requests)).toBe(0);
    expect(yield* Queue.size(f.a.localRequests)).toBe(0);
  }).pipe(Effect.provide(testLayers)),
);

it.effect("rejects configured forwarding chains before a second link is used", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    yield* linkTo(f.a.links, f.host);
    yield* f.hostSettings.updateSettings({ webResearch: { host: cavallo.environmentId } });
    expect(failure(yield* f.a.call({ url })).code).toBe("research_host_unavailable");
    expect(yield* Queue.size(f.requests)).toBe(0);
    expect(yield* Queue.size(f.a.localRequests)).toBe(0);
  }).pipe(Effect.provide(testLayers)),
);

it.effect(
  "keeps a healthy redirect read pending beyond 60 seconds across bounded queue waits",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture("User-agent: *\nAllow: /\nCrawl-delay: 40", (target: URL) =>
        target.pathname === "/rooms"
          ? {
              ...response(""),
              status: 302,
              headers: { "content-type": "text/html", location: "/final" },
            }
          : response(body),
      );
      yield* linkTo(f.a.links, f.host);
      yield* Deferred.succeed(f.release, undefined);
      const completed = yield* Deferred.make<void>();
      const read = yield* Effect.forkScoped(
        f.a
          .call({ url, followSubpages: false })
          .pipe(Effect.onExit(() => Deferred.succeed(completed, undefined))),
      );
      expect(yield* Queue.take(f.requests)).toBe("/robots.txt");
      yield* TestClock.adjust("40 seconds");
      expect(yield* Queue.take(f.requests)).toBe("/rooms");
      yield* TestClock.adjust("21 seconds");
      expect(yield* Deferred.isDone(completed)).toBe(false);
      yield* TestClock.adjust("19 seconds");
      expect(yield* Queue.take(f.requests)).toBe("/final");
      expect(page(yield* Fiber.join(read))).toMatchObject({
        status: "ok",
        delayMs: 80_000,
        finalUrl: "https://venue.example/final",
      });
      expect(yield* Queue.size(f.a.localRequests)).toBe(0);
    }).pipe(Effect.provide(testLayers)),
);

it.effect("returns the host's transient queue refusal and permits an uncached retry", () =>
  Effect.gen(function* () {
    const f = yield* fixture("User-agent: *\nAllow: /\nCrawl-delay: 75");
    yield* linkTo(f.a.links, f.host);
    const read = yield* Effect.forkScoped(f.a.call({ url, followSubpages: false }));
    expect(yield* Queue.take(f.requests)).toBe("/robots.txt");
    expect(page(yield* Fiber.join(read))).toMatchObject({
      status: "challenge",
      retryAfterMs: 75_000,
      text: "",
    });
    expect(yield* Queue.size(f.requests)).toBe(0);
    yield* TestClock.adjust("75 seconds");
    const retry = yield* Effect.forkScoped(f.a.call({ url, followSubpages: false }));
    expect(yield* Queue.take(f.requests)).toBe("/rooms");
    yield* Deferred.succeed(f.release, undefined);
    expect(page(yield* Fiber.join(retry))).toMatchObject({ status: "ok", rung: "http" });
    expect(yield* Queue.size(f.a.localRequests)).toBe(0);
  }).pipe(Effect.provide(testLayers)),
);

it.effect("does not let an explicit environmentId bypass the configured research host", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    yield* linkTo(f.a.links, f.host);
    yield* f.a.settings.updateSettings({ webResearch: { host: rocky.environmentId } });
    // Without the routing guard this reaches bestia and returns its loop error instead.
    yield* f.hostSettings.updateSettings({ webResearch: { host: cavallo.environmentId } });
    const rejected = failure(yield* f.a.call({ url, environmentId: bestia.environmentId }));
    expect(rejected).toMatchObject({ code: "invalid_request" });
    expect(rejected.message).toContain("webResearch.host");
    expect(yield* Queue.size(f.requests)).toBe(0);
    expect(yield* Queue.size(f.a.localRequests)).toBe(0);
  }).pipe(Effect.provide(testLayers)),
);

it.effect(
  "the pinned MCP SDK receives typed failures without validating them as page results",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* linkTo(f.a.links, f.host);
      yield* f.hostSettings.updateSettings({ webResearch: { host: cavallo.environmentId } });
      const resolved = yield* f.a.links.resolve(bestia.environmentId);
      const sdk = yield* Effect.acquireRelease(
        Effect.promise(async () => {
          const sdk = new Client({ name: "research-sdk-proof", version: "1" });
          await sdk.connect(
            // @ts-expect-error -- SDK 1.29.0's sessionId getter conflicts with its own Transport under exactOptionalPropertyTypes.
            new StreamableHTTPClientTransport(new URL(`${resolved.url}/mcp`), {
              requestInit: { headers: { authorization: `Bearer ${resolved.token}` } },
            }),
          );
          return sdk;
        }),
        (sdk) => Effect.promise(() => sdk.close()),
      );
      // listTools installs the SDK's outputSchema validators, as an agent harness does.
      yield* Effect.promise(() => sdk.listTools());
      const result = yield* Effect.promise(() =>
        sdk.callTool({ name: "web_read", arguments: { url } }),
      );
      expect(result.isError).toBe(true);
      expect(result.structuredContent).toBeUndefined();
      expect(result.content).toContainEqual({
        type: "text",
        text: expect.stringContaining('"code":"research_host_unavailable"'),
      });
      expect(yield* Queue.size(f.requests)).toBe(0);
    }).pipe(Effect.provide(testLayers)),
);

it.effect(
  "owns continuations across host/unset transitions and treats an explicit self host as local",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.a.settings.updateSettings({ webResearch: { host: null } });
      const readLocal = yield* Effect.forkScoped(f.a.call({ url }));
      expect(yield* Queue.take(f.a.localRequests)).toBe("/robots.txt");
      yield* TestClock.adjust(3000);
      expect(yield* Queue.take(f.a.localRequests)).toBe("/rooms");
      const local = page(yield* Fiber.join(readLocal));
      const continuation = {
        cacheId: local.cacheId,
        cacheEnvironmentId: local.cacheEnvironmentId,
        offset: 20_000,
      };
      expect(failure(yield* f.a.call({ cacheId: local.cacheId })).code).toBe("invalid_request");
      yield* f.a.settings.updateSettings({ webResearch: { host: bestia.environmentId } });
      expect(failure(yield* f.a.call(continuation)).code).toBe("invalid_request");
      yield* f.a.settings.updateSettings({ webResearch: { host: cavallo.environmentId } });
      expect(page(yield* f.a.call(continuation))).toMatchObject({
        rung: "cache",
        cacheId: local.cacheId,
      });
      expect(
        failure(
          yield* f.a.call({ cacheId: local.cacheId, cacheEnvironmentId: bestia.environmentId }),
        ).code,
      ).toBe("invalid_request");
      expect(yield* Queue.size(f.a.localRequests)).toBe(0);
      expect(yield* Queue.size(f.requests)).toBe(0);
    }).pipe(Effect.provide(testLayers)),
);
