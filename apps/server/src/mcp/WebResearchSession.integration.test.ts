import { WebResearchToolkit } from "./toolkits/webResearch/tools.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as PeerAuthority from "../peer/PeerAuthority.ts";
import * as PeerMcpClient from "../peer/PeerMcpClient.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as SqlClient from "effect/sql/SqlClient";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as RequestReceipts from "./McpRequestReceipts.ts";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { NodeHttpServer } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  PreviewTabId,
  ProviderInstanceId,
  ThreadId,
  type PreviewAutomationRequest,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { McpSchema, McpServer } from "effect/ai";

import * as Config from "../config.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as PeerForwarding from "../peer/PeerForwarding.ts";
import * as PeerLinks from "../peer/PeerLinks.ts";
import { descriptorOf, linkTo, servePeer } from "../peer/PeerLinks.testkit.ts";
import * as Settings from "../serverSettings.ts";
import * as PageCache from "../webResearch/PageCache.ts";
import * as PublicHttp from "../webResearch/PublicHttp.ts";
import * as PublicRender from "../webResearch/PublicRender.ts";
import * as SessionRead from "../webResearch/SessionRead.ts";
import * as Search from "../webResearch/Search.ts";
import * as TinyFish from "../webResearch/TinyFish.ts";
import * as ResearchHost from "../webResearch/ResearchHost.ts";
import * as WebResearch from "../webResearch/WebResearch.ts";
import * as Invocation from "./McpInvocationContext.ts";
import * as McpHttpServer from "./McpHttpServer.ts";
import * as Broker from "./PreviewAutomationBroker.ts";
import { liveThreadShell, liveThreadsLayer } from "./McpToolAccess.testkit.ts";

const local = EnvironmentId.make("session-owner");
const selected = {
  environmentId: local,
  profileId: "work",
  tabId: PreviewTabId.make("account-tab"),
};
const url = "https://account.example/reading";
const scopeOf = (environmentId = local): Invocation.McpThreadInvocationScope => ({
  environmentId,
  requestNamespace: `caller:${environmentId}`,
  issuedAt: 0,
  client: undefined,
  thread: {
    threadId: ThreadId.make(`thread:${environmentId}`),
    providerSessionId: `session:${environmentId}`,
    providerInstanceId: ProviderInstanceId.make("codex"),
  },
  capabilities: new Set(["preview"]),
});
const client = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  clientCapabilities: {},
  clientInfo: { name: "session-proof", version: "1" },
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "session-proof", version: "1" },
  },
  getClient: Effect.die("unused"),
});
const decodePage = Schema.decodeUnknownSync(ResearchHost.HostedWebReadResult);
const decodeFailure = Schema.decodeSync(
  Schema.fromJsonString(Schema.Struct({ code: Schema.String, message: Schema.String })),
);
const page = (result: McpSchema.CallToolResult) => {
  expect(result.isError, JSON.stringify(result.content)).not.toBe(true);
  return decodePage(result.structuredContent);
};
const failure = (result: McpSchema.CallToolResult) => {
  expect(result.isError).toBe(true);
  const content = result.content[0];
  if (content?.type !== "text") throw new Error("Expected a typed tool error");
  return decodeFailure(content.text);
};
const callOn = (
  server: McpServer.McpServer["Service"],
  input: Record<string, unknown>,
  scope: Invocation.McpInvocationScope = scopeOf(),
) =>
  server
    .callTool({ name: "web_read", arguments: input })
    .pipe(
      Effect.provideService(McpSchema.McpServerClient, client),
      Effect.provideService(Invocation.McpInvocationContext, scope),
    );

const reader = (requests: Queue.Queue<string>) =>
  WebResearch.layer.pipe(
    Layer.provide(Layer.mock(Search.Search)({})),
    Layer.provide(
      Layer.succeed(TinyFish.TinyFish, {
        fetch: () => Effect.die("Session reads must not contact hosted providers"),
      }),
    ),
    Layer.provide(
      Layer.succeed(PublicRender.PublicRender, {
        render: () => Effect.die("Session reads must not render public pages"),
      }),
    ),
    Layer.provide(
      Layer.succeed(PublicHttp.PublicHttp, {
        get: (target) =>
          Effect.gen(function* () {
            yield* Queue.offer(requests, target.pathname);
            return {
              status: 200,
              headers: {},
              body:
                target.pathname === "/robots.txt"
                  ? "User-agent: *\nAllow: /"
                  : `<h1>Public page</h1><p>${"Public information only. ".repeat(20)}</p>`,
            };
          }),
      }),
    ),
  );

// Only the external browser/account is simulated; real broker requests and replies cross this port.
const attachBrowser = Effect.fnUntraced(function* (
  broker: Broker.PreviewAutomationBroker["Service"],
  environmentId = local,
  connect = true,
) {
  const state = {
    html: `<title>Account reading</title><h1>Private account</h1><table><tr><td>Rooms</td><td>Eight</td></tr></table><p>${"Private account information. ".repeat(1800)}</p>`,
    httpStatus: 200,
    owner: "agent" as "agent" | "human",
    generation: 1,
    profile: "work" as string | undefined,
    profiles: ["personal", "work"],
    finalUrl: url,
    requests: [] as PreviewAutomationRequest[],
    hold: undefined as Deferred.Deferred<void> | undefined,
  };
  const connected = yield* Deferred.make<void>();
  const entered = yield* Queue.unbounded<void>();
  if (!connect) return { state, entered };
  const events = yield* broker.connect({
    clientId: "existing-browser",
    environmentId,
    supportedOperations: ["status", "readPage"],
  });
  yield* Stream.runForEach(events, (event) =>
    Effect.gen(function* () {
      if (event.type === "connected") {
        yield* Deferred.succeed(connected, undefined);
        return;
      }
      const request = event.request;
      state.requests.push(request);
      if (request.operation === "readPage") {
        yield* Queue.offer(entered, undefined);
        if (state.hold) yield* Deferred.await(state.hold);
      }
      yield* broker.respond({
        clientId: "existing-browser",
        connectionId: event.connectionId,
        requestId: request.requestId,
        ok: true,
        result:
          request.operation === "status"
            ? {
                available: true,
                visible: true,
                tabId: selected.tabId,
                url: state.finalUrl,
                title: "Account reading",
                loading: false,
                control: { owner: state.owner, ownedByCaller: true, generation: state.generation },
                profiles: state.profiles.map((id) => ({ id, name: id })),
                tabs: [
                  {
                    tabId: selected.tabId,
                    profileId: state.profile,
                    owner: state.owner,
                    url: state.finalUrl,
                  },
                ],
              }
            : {
                tabId: selected.tabId,
                profileId: state.profile ?? "default",
                generation: state.generation,
                finalUrl: state.finalUrl,
                html: state.html,
                httpStatus: state.httpStatus,
              },
      });
    }),
  ).pipe(Effect.scoped, Effect.forkScoped);
  yield* Deferred.await(connected);
  return { state, entered };
});

const fixture = Effect.fnUntraced(function* (withBrowser = true) {
  const requests = yield* Queue.unbounded<string>();
  const common = Layer.mergeAll(Broker.layer, liveThreadsLayer, PageCache.layer).pipe(
    Layer.provideMerge(Config.layerTest(process.cwd(), { prefix: "session446-" })),
    Layer.provideMerge(NodeServices.layer),
  );
  const context = yield* Layer.build(
    McpHttpServer.layerWebResearchToolkit.pipe(
      Layer.provide(SessionRead.layer),
      Layer.provide(reader(requests)),
      Layer.provide(Settings.layerTest()),
      Layer.provide(Layer.mock(PeerLinks.PeerLinks)({})),
      Layer.provide(Layer.mock(PeerForwarding.PeerForwarding)({})),
      Layer.provideMerge(common),
      Layer.provideMerge(McpServer.McpServer.layer),
      Layer.fresh,
    ),
  );
  const server = Context.get(context, McpServer.McpServer);
  const browser = yield* attachBrowser(
    Context.get(context, Broker.PreviewAutomationBroker),
    local,
    withBrowser,
  );
  return {
    ...browser,
    context,
    requests,
    call: (input: Record<string, unknown>, scope: Invocation.McpInvocationScope = scopeOf()) =>
      callOn(server, input, scope),
  };
});

it.effect(
  "absent/false opt-in never touches a profile; session and public continuations remain isolated",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const first = yield* f
        .call({ url, session: selected, followSubpages: false })
        .pipe(Effect.forkScoped);
      expect(yield* Queue.take(f.requests)).toBe("/robots.txt");
      yield* TestClock.adjust(3000);
      expect(yield* Queue.take(f.requests)).toBe("/reading");
      const publicPage = page(yield* Fiber.join(first));
      expect(publicPage.text).toContain("Public information only");
      expect(f.state.requests).toEqual([]);
      expect(page(yield* f.call({ url, useSession: false, session: selected })).cacheId).toBe(
        publicPage.cacheId,
      );
      expect(f.state.requests).toEqual([]);
      const privatePage = page(
        yield* f.call({ url, useSession: true, session: selected, maxChars: 100 }),
      );
      expect(privatePage).toMatchObject({
        rung: "session",
        sourceRung: "session",
        session: selected,
        cacheStorage: "memory",
        delayMs: 0,
        subpages: [],
      });
      expect(privatePage.text.length).toBe(100);
      expect(privatePage.text).toContain("| Rooms | Eight |");
      expect(privatePage.totalChars).toBeGreaterThan(20_000);
      expect(privatePage.cacheId).toMatch(/^session:[a-f0-9]{64}$/);
      const continued = page(
        yield* f.call({
          cacheId: privatePage.cacheId,
          cacheEnvironmentId: local,
          offset: privatePage.nextOffset,
          useSession: true,
          session: selected,
        }),
      );
      expect(continued).toMatchObject({
        rung: "cache",
        sourceRung: "session",
        contentHash: privatePage.contentHash,
        offset: 100,
      });
      const cache = Context.get(f.context, PageCache.PageCache);
      expect(yield* cache.get({ cacheId: privatePage.cacheId })).toBeNull();
      expect((yield* cache.get({ url }))?.text).toContain("Public information only");
      expect(page(yield* f.call({ url, useSession: false })).text).not.toContain("Private account");
      const count = f.state.requests.length;
      expect(
        failure(
          yield* f.call({
            cacheId: privatePage.cacheId,
            cacheEnvironmentId: local,
            session: selected,
          }),
        ).code,
      ).toBe("session_unavailable");
      expect(
        failure(
          yield* f.call(
            {
              cacheId: privatePage.cacheId,
              cacheEnvironmentId: local,
              useSession: true,
              session: selected,
            },
            { ...scopeOf(), requestNamespace: "another-caller" },
          ),
        ).code,
      ).toBe("session_unavailable");
      expect(f.state.requests).toHaveLength(count);
      expect(
        failure(
          yield* f.call(
            {
              cacheId: privatePage.cacheId,
              cacheEnvironmentId: local,
              useSession: true,
              session: selected,
            },
            {
              ...scopeOf(),
              thread: { ...scopeOf().thread, providerSessionId: "replacement-provider-session" },
            },
          ),
        ).code,
      ).toBe("session_unavailable");
      expect(f.state.requests).toHaveLength(count);
      f.state.generation++;
      expect(
        failure(
          yield* f.call({
            cacheId: privatePage.cacheId,
            cacheEnvironmentId: local,
            useSession: true,
            session: selected,
          }),
        ).code,
      ).toBe("session_unavailable");
      yield* TestClock.adjust(5 * 60_000);
      expect(
        failure(
          yield* f.call({
            cacheId: privatePage.cacheId,
            cacheEnvironmentId: local,
            useSession: true,
            session: selected,
          }),
        ).message,
      ).toContain("expired");
    }).pipe(Effect.provide(NodeCrypto.layer)),
);

it.effect(
  "session reads select an existing profile and expose no account-action or pacing API",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const start = yield* Clock.currentTimeMillis;
      for (let i = 0; i < 81; i++) {
        const result = page(
          yield* f.call({
            url,
            useSession: true,
            session: selected,
            operation: "click",
            expression: "document.forms[0].submit()",
          }),
        );
        expect(result.delayMs).toBe(0);
      }
      expect(yield* Clock.currentTimeMillis).toBe(start);
      expect(new Set(f.state.requests.map((request) => request.operation))).toEqual(
        new Set(["status", "readPage"]),
      );
      expect(f.state.requests.filter((request) => request.operation === "readPage")).toHaveLength(
        81,
      );
      expect(f.state.requests.find((request) => request.operation === "readPage")?.input).toEqual({
        profileId: "work",
        url,
      });
      expect(
        failure(
          yield* f.call({ url, useSession: true, session: { ...selected, profileId: "personal" } }),
        ).code,
      ).toBe("session_unavailable");
      f.state.profile = undefined;
      f.state.profiles = ["default"];
      expect(
        page(
          yield* f.call({ url, useSession: true, session: { ...selected, profileId: "default" } }),
        ).session?.profileId,
      ).toBe("default");
      f.state.profile = "work";
      f.state.profiles = ["work"];
      f.state.owner = "human";
      const before = f.state.requests.filter((request) => request.operation === "readPage").length;
      expect(failure(yield* f.call({ url, useSession: true, session: selected })).code).toBe(
        "session_in_use",
      );
      expect(f.state.requests.filter((request) => request.operation === "readPage")).toHaveLength(
        before,
      );
    }).pipe(Effect.provide(NodeCrypto.layer)),
);

it.effect.each([
  { html: "<title>Log in</title><input type=password>", status: 200, code: "session_expired" },
  { html: "<p>Try again later</p>", status: 200, code: "session_warning" },
  { html: "<p>Account checkpoint required</p>", status: 200, code: "session_warning" },
  { html: "<h1>Account</h1><p>Rate limited</p>", status: 429, code: "session_warning" },
] as const)("returns typed private failures for $code ($status)", ({ html, status, code }) =>
  Effect.gen(function* () {
    const f = yield* fixture();
    f.state.html = html;
    f.state.httpStatus = status;
    const result = yield* f.call({ url, useSession: true, session: selected });
    expect(failure(result).code).toBe(code);
    expect(result.structuredContent).toBeUndefined();
    expect(yield* Context.get(f.context, PageCache.PageCache).get({ url })).toBeNull();
  }).pipe(Effect.provide(NodeCrypto.layer)),
);

it.effect("missing selection/capability/profile and bounded owner timeout are actionable", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    expect(failure(yield* f.call({ url, useSession: true })).message).toContain("existing tabId");
    expect(
      failure(
        yield* f.call(
          { url, useSession: true, session: selected },
          { ...scopeOf(), capabilities: new Set() },
        ),
      ).code,
    ).toBe("capability_denied");
    expect(f.state.requests).toEqual([]);
    f.state.profiles = [];
    expect(failure(yield* f.call({ url, useSession: true, session: selected })).message).toContain(
      "profile",
    );
    f.state.profiles = ["work"];
    f.state.hold = yield* Deferred.make<void>();
    const pending = yield* f
      .call({ url, useSession: true, session: selected })
      .pipe(Effect.forkScoped);
    yield* Queue.take(f.entered);
    yield* TestClock.adjust(20_000);
    expect(failure(yield* Fiber.join(pending)).code).toBe("session_unavailable");
  }).pipe(Effect.provide(NodeCrypto.layer)),
);

it.effect("oversized HTML fails without public fallback", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    f.state.html = "x".repeat(4 * 1024 * 1024 + 1);
    expect(failure(yield* f.call({ url, useSession: true, session: selected })).message).toContain(
      "oversized",
    );
    expect(yield* Queue.size(f.requests)).toBe(0);
  }).pipe(Effect.provide(NodeCrypto.layer)),
);

it.effect("an unavailable controller fails without public fallback", () =>
  Effect.gen(function* () {
    const f = yield* fixture(false);
    const result = yield* f.call({ url, useSession: true, session: selected });
    expect(failure(result).message).toContain("session owner is unavailable");
    expect(yield* Queue.size(f.requests)).toBe(0);
  }).pipe(Effect.provide(NodeCrypto.layer)),
);

const modernPeerServices = (
  environment: ReturnType<typeof descriptorOf>,
  threadLayer: Layer.Layer<Threads.ThreadManagementService>,
) =>
  Effect.gen(function* () {
    const threadContext = yield* Layer.build(threadLayer);
    const threads = Context.get(threadContext, Threads.ThreadManagementService);
    return yield* PeerForwarding.layer.pipe(
      Layer.provideMerge(PeerMcpClient.layer),
      Layer.provideMerge(PeerAuthority.layer),
      Layer.provideMerge(PeerLinks.layer),
      Layer.provideMerge(RequestReceipts.layer),
      Layer.provideMerge(Sqlite.layerMemory),
      Layer.provide(
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getThreadShell: (id) => threads.getThreadShell(id).pipe(Effect.orDie),
        }),
      ),
      Layer.provideMerge(Layer.succeedContext(threadContext)),
      Layer.provideMerge(ServerSecretStore.layer),
      Layer.provide(FetchHttpClient.layer),
      Layer.provide(
        Layer.succeed(ServerEnvironment.ServerEnvironment, {
          getEnvironmentId: Effect.succeed(environment.environmentId),
          getDescriptor: Effect.succeed(environment),
        }),
      ),
      Layer.provide(
        Settings.layerTest({ enableAgentBrowserAccess: true, linkedEnvironmentsEnabled: true }),
      ),
      Layer.provideMerge(Config.layerTest(process.cwd(), { prefix: "session446-authority-" })),
      Layer.provideMerge(NodeServices.layer),
      Layer.fresh,
      Layer.build,
    );
  });

it.effect(
  "a bestia caller reaches only the selected owner through authenticated loopback and preserves the destination fence",
  () =>
    Effect.gen(function* () {
      const bestia = descriptorOf("public-bestia", "Bestia", { peerAuthority: 1 });
      const owner = descriptorOf(local, "Profile owner", { peerAuthority: 1 });
      const ownerScope = scopeOf();
      const linkSession = yield* Ref.make("");
      const ownerRequests = yield* Queue.unbounded<string>();
      const ownerThreads = Layer.mock(Threads.ThreadManagementService)({
        getThreadShell: (id) =>
          Ref.get(linkSession).pipe(
            Effect.map((sessionId) => ({
              ...liveThreadShell(id, { runtimeMode: "auto" }),
              ...(id === ownerScope.thread!.threadId && sessionId.length > 0
                ? { linkOrigin: { sessionId, label: "T3 Code · Bestia" } }
                : {}),
            })),
          ),
      });
      const ownerServices = yield* modernPeerServices(owner, ownerThreads);
      const ownerCommon = yield* Layer.build(
        Layer.mergeAll(Broker.layer, PageCache.layer).pipe(
          Layer.provideMerge(Layer.succeedContext(ownerServices)),
        ),
      );
      const browser = yield* attachBrowser(
        Context.get(ownerCommon, Broker.PreviewAutomationBroker),
      );
      const httpServer = yield* Layer.build(NodeHttpServer.layerTest);
      const peer = yield* servePeer(
        owner,
        McpHttpServer.layerWebResearchToolkit.pipe(
          Layer.provide(SessionRead.layer),
          Layer.provide(reader(ownerRequests)),
          Layer.provide(Settings.layerTest({ webResearch: { host: bestia.environmentId } })),
          Layer.provide(Layer.mock(PeerLinks.PeerLinks)({})),
          Layer.provide(Layer.mock(PeerForwarding.PeerForwarding)({})),
          Layer.provide(ownerThreads),
          Layer.provide(Layer.succeedContext(ownerCommon)),
        ),
        {
          authorityServices: ownerServices,
          database: Layer.succeed(
            SqlClient.SqlClient,
            Context.get(ownerServices, SqlClient.SqlClient),
          ),
          secretStore: Context.get(ownerServices, ServerSecretStore.ServerSecretStore),
          serverConfig: Context.get(ownerServices, Config.ServerConfig),
          scopeForThread: () =>
            Effect.die("Modern session reads must not borrow a provider credential"),
        },
      ).pipe(Effect.provide(httpServer));
      const sourceRequests = yield* Queue.unbounded<string>();
      const linking = yield* modernPeerServices(bestia, liveThreadsLayer);
      const links = Context.get(linking, PeerLinks.PeerLinks);
      yield* linkTo(links, peer, "auto");
      const linked = yield* peer.linkedSessions;
      yield* Ref.set(linkSession, linked[0]!.sessionId);
      const enrollment = yield* Context.get(linking, PeerForwarding.PeerForwarding)
        .call(scopeOf(bestia.environmentId), WebResearchToolkit.tools.web_read, local, { url })
        .pipe(Effect.flip);
      expect(enrollment.code).toBe("research_host_unavailable");
      const ownerAuthority = Context.get(ownerServices, PeerAuthority.PeerAuthority);
      const grant = (yield* ownerAuthority.grants)[0];
      if (grant === undefined)
        return yield* Effect.die("Missing authenticated research peer grant");
      yield* ownerAuthority.setGrant({
        ...grant,
        resourceScope: "created-work",
        preview: true,
      });
      const sourceContext = yield* Layer.build(
        McpHttpServer.layerWebResearchToolkit.pipe(
          Layer.provide(reader(sourceRequests)),
          Layer.provide(PageCache.layer),
          Layer.provide(Settings.layerTest({ webResearch: { host: bestia.environmentId } })),
          Layer.provide(liveThreadsLayer),
          Layer.provide(Layer.succeedContext(linking)),
          Layer.provide(
            Config.layerTest(process.cwd(), { prefix: "session446-source-" }).pipe(
              Layer.provide(NodeServices.layer),
            ),
          ),
          Layer.provideMerge(McpServer.McpServer.layer),
        ),
      );
      const source = Context.get(sourceContext, McpServer.McpServer);
      const sourceScope = scopeOf(bestia.environmentId);
      const session = { ...selected, contextThreadId: ownerScope.thread!.threadId };
      const read = (
        input: Record<string, unknown>,
        scope: Invocation.McpInvocationScope = sourceScope,
      ) => callOn(source, input, scope);
      const result = page(yield* read({ url, useSession: true, session, maxChars: 80 }));
      expect(result).toMatchObject({
        cacheEnvironmentId: local,
        sourceRung: "session",
        session: selected,
      });
      expect(result.text).toContain("Private account");
      expect(yield* Queue.size(sourceRequests)).toBe(0);
      expect(yield* Queue.size(ownerRequests)).toBe(0);
      expect((yield* Ref.get(peer.bearers)).length).toBeGreaterThan(0);
      expect(
        page(
          yield* read({
            cacheId: result.cacheId,
            cacheEnvironmentId: local,
            useSession: true,
            session,
            offset: result.nextOffset,
          }),
        ).sourceRung,
      ).toBe("session");
      const continuation = {
        cacheId: result.cacheId,
        cacheEnvironmentId: local,
        useSession: true,
        session,
        offset: result.nextOffset,
      };
      expect(
        page(
          yield* read(continuation, {
            ...sourceScope,
            requestNamespace: "replacement-source-credential",
            thread: { ...sourceScope.thread, providerSessionId: "replacement-source-credential" },
          }),
        ).sourceRung,
      ).toBe("session");
      const beforeOtherCaller = browser.state.requests.length;
      expect(
        failure(
          yield* read(continuation, {
            ...sourceScope,
            thread: { ...sourceScope.thread, threadId: ThreadId.make("another-source-thread") },
          }),
        ).code,
      ).toBe("session_unavailable");
      expect(browser.state.requests).toHaveLength(beforeOtherCaller);
      browser.state.httpStatus = 429;
      expect(failure(yield* read({ url, useSession: true, session })).code).toBe("session_warning");
      browser.state.httpStatus = 200;
      const before = browser.state.requests.length;
      expect(failure(yield* read({ url, useSession: true, session: selected })).message).toContain(
        "direct owner link",
      );
      expect(
        failure(
          yield* read({
            url,
            useSession: true,
            session: { ...session, contextThreadId: "user-owned-thread" },
          }),
        ).code,
      ).toBe("capability_denied");
      expect(
        failure(
          yield* read(
            { url, useSession: true, session },
            { ...sourceScope, capabilities: new Set() },
          ),
        ).code,
      ).toBe("capability_denied");
      expect(
        failure(
          yield* read(
            { url, useSession: true, session },
            {
              ...sourceScope,
              thread: undefined,
              client: { sessionId: "incoming-link", label: "Linked", linked: true, access: "auto" },
            },
          ),
        ).message,
      ).toContain("cannot forward");
      expect(browser.state.requests).toHaveLength(before);
      yield* links.unlink(local);
      expect(failure(yield* read({ url, useSession: true, session })).code).toBe(
        "session_unavailable",
      );
      yield* linkTo(links, peer, "read-only");
      expect(failure(yield* read({ url, useSession: true, session })).code).toBe(
        "capability_denied",
      );
    }).pipe(Effect.provide(NodeCrypto.layer)),
);
