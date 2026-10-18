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
import {
  descriptorOf,
  layerLinkingEnvironment,
  linkTo,
  servePeer,
} from "../peer/PeerLinks.testkit.ts";
import * as Settings from "../serverSettings.ts";
import * as PageCache from "./PageCache.ts";
import * as PublicHttp from "./PublicHttp.ts";
import * as WebResearch from "./WebResearch.ts";
import { HostedWebReadResult } from "./ResearchHost.ts";

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
const readerLayer = (get: PublicHttp.PublicHttp["Service"]["get"]) =>
  WebResearch.layer.pipe(
    Layer.provide(PageCache.layer),
    Layer.provide(Layer.succeed(PublicHttp.PublicHttp, { get })),
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
        readerLayer((target) =>
          Effect.gen(function* () {
            yield* Queue.offer(requests, target.pathname);
            const invocation = yield* Effect.serviceOption(Invocation.McpInvocationContext);
            if (Option.isSome(invocation)) yield* Queue.offer(callers, invocation.value);
            if (target.pathname === "/robots.txt") return response(robots);
            yield* Deferred.await(release);
            return respond(target);
          }),
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
        Layer.provide(Layer.succeedContext(linking)),
        Layer.provide(threads),
        Layer.provideMerge(Settings.layerTest({ webResearch: { host: bestia.environmentId } })),
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
        f.a.call({ url }).pipe(Effect.onExit(() => Deferred.succeed(completed, undefined))),
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
    const read = yield* Effect.forkScoped(f.a.call({ url }));
    expect(yield* Queue.take(f.requests)).toBe("/robots.txt");
    expect(page(yield* Fiber.join(read))).toMatchObject({
      status: "challenge",
      retryAfterMs: 75_000,
      text: "",
    });
    expect(yield* Queue.size(f.requests)).toBe(0);
    yield* TestClock.adjust("75 seconds");
    const retry = yield* Effect.forkScoped(f.a.call({ url }));
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
