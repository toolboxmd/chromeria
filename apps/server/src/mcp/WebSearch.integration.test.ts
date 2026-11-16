import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Redacted from "effect/Redacted";
import * as TestClock from "effect/testing/TestClock";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientError from "effect/http/HttpClientError";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import type * as HttpClientRequest from "effect/http/HttpClientRequest";
import { McpSchema, McpServer } from "effect/ai";

import * as Config from "../config.ts";
import * as SecretStore from "../auth/ServerSecretStore.ts";
import * as Settings from "../serverSettings.ts";
import * as PeerForwarding from "../peer/PeerForwarding.ts";
import * as PeerLinks from "../peer/PeerLinks.ts";
import * as PageCache from "../webResearch/PageCache.ts";
import * as PublicHttp from "../webResearch/PublicHttp.ts";
import * as PublicRender from "../webResearch/PublicRender.ts";
import * as Search from "../webResearch/Search.ts";
import * as TinyFish from "../webResearch/TinyFish.ts";
import * as WebResearch from "../webResearch/WebResearch.ts";
import { HostedWebSearchResult } from "../webResearch/ResearchHost.ts";
import * as Invocation from "./McpInvocationContext.ts";
import * as McpHttpServer from "./McpHttpServer.ts";
import { liveThreadsLayer } from "./McpToolAccess.testkit.ts";

const TOKEN = "fixture-only-search-secret";
const searxng = "http://search.fixture:8888/";
const base = Layer.mergeAll(NodeServices.layer, NodeCrypto.layer);
const client = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  clientCapabilities: {},
  clientInfo: { name: "search-proof", version: "1" },
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "search-proof", version: "1" },
  },
  getClient: Effect.die("unused"),
});
const scope: Invocation.McpInvocationScope = {
  environmentId: EnvironmentId.make("search-environment"),
  requestNamespace: "search-agent",
  issuedAt: 0,
  thread: {
    threadId: ThreadId.make("search-thread"),
    providerInstanceId: ProviderInstanceId.make("codex"),
    providerSessionId: "search-session",
  },
  client: undefined,
  capabilities: new Set(),
};
const decode = Schema.decodeUnknownSync(HostedWebSearchResult);
const call = (arguments_: Record<string, unknown>, invocation = scope) =>
  McpServer.McpServer.use((server) =>
    server.callTool({ name: "web_search", arguments: arguments_ }),
  ).pipe(
    Effect.provideService(McpSchema.McpServerClient, client),
    Effect.provideService(Invocation.McpInvocationContext, invocation),
  );
const search = Effect.fnUntraced(function* (input: Record<string, unknown>, invocation = scope) {
  const result = yield* call(input, invocation);
  expect(result.isError, JSON.stringify(result.content)).not.toBe(true);
  const decoded = decode(result.structuredContent);
  expect(JSON.stringify(result.structuredContent).length).toBeLessThanOrEqual(20_100);
  return decoded;
});
const resultUrl = "https://pl.example/rooms";
const requestUrl = (r: HttpClientRequest.HttpClientRequest) => {
  const url = new URL(r.url);
  for (const [key, value] of r.urlParams)
    url.searchParams.append(key, Redacted.isRedacted(value) ? Redacted.value(value) : value);
  return url;
};
const envelope = (
  request: HttpClientRequest.HttpClientRequest,
  results: ReadonlyArray<Record<string, unknown>> = [
    {
      url: resultUrl,
      title: "<b>Polskie miejsca</b>",
      snippet: "Rooms &amp; events",
      content: "Rooms &amp; events",
      engines: ["google cse"],
    },
  ],
  extra: Record<string, unknown> = {},
) => ({
  query: requestUrl(request).searchParams.get(
    request.url.startsWith("https://treg.to") ? "query" : "q",
  ),
  results,
  ...(request.url.startsWith("https://treg.to")
    ? { total_results: results.length, page: 0 }
    : { unresponsive_engines: [] }),
  ...extra,
});
const reply = (
  request: HttpClientRequest.HttpClientRequest,
  body: unknown = envelope(request),
  status = 200,
  headers: Record<string, string> = {},
) =>
  HttpClientResponse.fromWeb(
    request,
    new Response(typeof body === "string" ? body : JSON.stringify(body), {
      status,
      headers: {
        "content-type": "application/json",
        ...(request.url.startsWith("https://treg.to")
          ? { "x-treg-cost-micro": "0", "x-treg-call-id": "0123456789abcdef0123456789abcdef" }
          : {}),
        ...headers,
      },
    }),
  );

type Respond = (
  request: HttpClientRequest.HttpClientRequest,
  signal: AbortSignal,
) => Effect.Effect<HttpClientResponse.HttpClientResponse, HttpClientError.HttpClientError>;
const fixture = Effect.fnUntraced(function* (
  respond: Respond = (request) => Effect.succeed(reply(request)),
  root?: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const directory = root ?? (yield* fs.makeTempDirectoryScoped({ prefix: "chromeria-search-" }));
  const paths = yield* Config.deriveServerPaths(directory, undefined);
  const requests = yield* Queue.unbounded<{
    request: HttpClientRequest.HttpClientRequest;
    at: number;
  }>();
  const seen: Array<{ request: HttpClientRequest.HttpClientRequest; at: number }> = [];
  const http = HttpClient.make((request, _url, signal) =>
    Effect.gen(function* () {
      const entry = { request, at: yield* Clock.currentTimeMillis };
      seen.push(entry);
      yield* Queue.offer(requests, entry);
      const language = requestUrl(request).searchParams.get("language");
      // SearXNG webadapter.parse_lang rejects scripts and numeric regions with HTTP 400.
      if (
        request.url.startsWith(searxng) &&
        language !== null &&
        language !== "auto" &&
        !/^[a-z]{2,3}(-[a-zA-Z]{2})?$/.test(language)
      )
        return reply(request, "Invalid search parameter: language", 400);
      return yield* respond(request, signal);
    }),
  );
  const dependencies = Layer.mergeAll(
    SecretStore.layer,
    Settings.layerTest({ webResearch: { host: null, searxngUrl: searxng } }),
    Layer.succeed(HttpClient.HttpClient, http),
  ).pipe(Layer.provideMerge(Config.layerTest(process.cwd(), directory)), Layer.provideMerge(base));
  const research = WebResearch.layer.pipe(
    Layer.provide(Search.layer),
    Layer.provide(PageCache.layer),
    Layer.provide(
      Layer.succeed(PublicHttp.PublicHttp, {
        get: () => Effect.die("Search must never fetch public pages."),
      }),
    ),
    Layer.provide(
      Layer.succeed(PublicRender.PublicRender, {
        render: () => Effect.die("Search must never render pages."),
      }),
    ),
    Layer.provide(
      Layer.succeed(TinyFish.TinyFish, {
        fetch: () => Effect.die("Search must never use the fetch route."),
      }),
    ),
    Layer.provideMerge(dependencies),
  );
  const layer = McpHttpServer.layerWebResearchToolkit.pipe(
    Layer.provideMerge(McpServer.McpServer.layer),
    Layer.provideMerge(research),
    Layer.provide(Layer.mock(PeerForwarding.PeerForwarding)({})),
    Layer.provide(Layer.mock(PeerLinks.PeerLinks)({})),
    Layer.provide(liveThreadsLayer),
  );
  const provision = SecretStore.ServerSecretStore.use((s) =>
    s.create(TinyFish.TREG_SECRET_NAME, new TextEncoder().encode(TOKEN)),
  );
  const seed = (source: "tinyfish" | "searxng", starts: number[], blockedUntil = 0) =>
    fs
      .makeDirectory(`${paths.stateDir}/web-cache`, { recursive: true })
      .pipe(
        Effect.andThen(
          fs.writeFileString(
            `${paths.stateDir}/web-cache/search-${source}-quota.json`,
            JSON.stringify({ starts, blockedUntil }),
          ),
        ),
      );
  return { layer, requests, seen, provision, seed, fs, paths, directory };
});

it.effect(
  "searches both pinned providers with locale/site intent, provenance and actual zero charge",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* Effect.gen(function* () {
        yield* f.provision;
        const result = yield* search({
          query: "site:pl.example -site:other.example polskie miejsca",
          locale: "pl-PL",
        });
        expect(result).toMatchObject({
          status: "ok",
          locale: "pl-PL",
          researchEnvironmentId: "search-environment",
          results: [
            {
              url: resultUrl,
              title: "Polskie miejsca",
              snippet: "Rooms & events",
              source: "searxng",
              engines: ["google cse"],
            },
            { url: resultUrl, source: "tinyfish", engines: ["tinyfish"] },
          ],
        });
        expect(result.engines.find((engine) => engine.source === "tinyfish")).toMatchObject({
          status: "ok",
          providerCall: {
            endpoint: "tinyfish.web.search",
            chargedMicro: 0,
            callId: "0123456789abcdef0123456789abcdef",
          },
        });
        expect(f.seen).toHaveLength(2);
        const a = f.seen.find((entry) => entry.request.url.startsWith(searxng))!.request;
        expect(requestUrl(a).pathname).toBe("/search");
        expect(requestUrl(a).searchParams.get("format")).toBe("json");
        expect(requestUrl(a).searchParams.get("language")).toBe("pl-PL");
        expect(requestUrl(a).searchParams.get("q")).toBe(
          "site:pl.example -site:other.example polskie miejsca",
        );
        expect(requestUrl(a).searchParams.get("engines")).toBe(
          "google cse,yahoo,duckduckgo web,startpage",
        );
        expect(a.headers["x-treg-token"]).toBeUndefined();
        const b = f.seen.find((entry) => entry.request.url.startsWith("https://treg.to"))!.request;
        expect(b.method).toBe("GET");
        expect(requestUrl(b).origin + requestUrl(b).pathname).toBe(
          "https://treg.to/call/tinyfish.web.search",
        );
        expect(requestUrl(b).searchParams.get("language")).toBe("pl");
        expect(requestUrl(b).searchParams.get("location")).toBe("PL");
        expect(requestUrl(b).searchParams.get("include_domains")).toBe("pl.example");
        expect(requestUrl(b).searchParams.get("exclude_domains")).toBe("other.example");
        expect(requestUrl(b).searchParams.get("domain_type")).toBe("web");
        expect(requestUrl(b).searchParams.get("fetch")).toBeNull();
        expect(b.headers["x-treg-token"]).toBe(TOKEN);
        expect(b.headers["x-treg-route-max-cost"]).toBe("0");
        expect(b.body._tag).toBe("Empty");
        // Search metadata may persist; SERPs, snippets and queries must not.
        for (const name of yield* f.fs.readDirectory(`${f.paths.stateDir}/web-cache`)) {
          const bytes = yield* f.fs.readFile(`${f.paths.stateDir}/web-cache/${name}`);
          const text = new TextDecoder().decode(bytes);
          for (const value of [TOKEN, resultUrl, "polskie miejsca", "Rooms & events"])
            expect(text).not.toContain(value);
        }
      }).pipe(Effect.provide(f.layer));
    }).pipe(Effect.provide(base)),
);

it.effect("distinguishes a healthy empty engine from CAPTCHA, blocked and provider failures", () =>
  Effect.gen(function* () {
    const f = yield* fixture((r) =>
      Effect.succeed(
        r.url.startsWith(searxng)
          ? reply(
              r,
              envelope(r, [], {
                unresponsive_engines: [
                  ["google cse", "CAPTCHA"],
                  ["startpage", "403 forbidden"],
                  ["yahoo", "timeout"],
                ],
              }),
            )
          : reply(r, { error: { message: "upstream rejected" }, results: [] }),
      ),
    );
    yield* Effect.gen(function* () {
      yield* f.provision;
      const result = yield* search({ query: "empty versus broken" });
      expect(result.results).toEqual([]);
      expect(result.status).toBe("degraded");
      expect(result.engines).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ engine: "google cse", status: "challenge" }),
          expect.objectContaining({ engine: "startpage", status: "blocked" }),
          expect.objectContaining({ engine: "yahoo", status: "timeout" }),
          expect.objectContaining({ engine: "duckduckgo web", status: "empty" }),
          expect.objectContaining({ engine: "tinyfish", status: "provider_error" }),
        ]),
      );
    }).pipe(Effect.provide(f.layer));
  }).pipe(Effect.provide(base)),
);

it.effect.each([
  { locale: "zh-Hant-TW", searxngLanguage: "zh-TW", hostedLanguage: "zh-Hant", location: "TW" },
  { locale: "sr-Latn-RS", searxngLanguage: "sr-RS", hostedLanguage: "sr-Latn", location: "RS" },
  { locale: "es-419", searxngLanguage: "es", hostedLanguage: "es", location: "419" },
  { locale: "pl-PL", searxngLanguage: "pl-PL", hostedLanguage: "pl", location: "PL" },
  { locale: "pl", searxngLanguage: "pl", hostedLanguage: "pl", location: null },
  { locale: "en", searxngLanguage: "en", hostedLanguage: "en", location: null },
])(
  "reports healthy emptiness for $locale with provider-specific languages and no fetched results",
  ({ locale, searxngLanguage, hostedLanguage, location }) =>
    Effect.gen(function* () {
      const f = yield* fixture((r) => Effect.succeed(reply(r, envelope(r, []))));
      yield* Effect.gen(function* () {
        yield* f.provision;
        const result = yield* search({ query: "no matching results", locale });
        expect(result).toMatchObject({ status: "ok", results: [], locale });
        expect(result.engines.every((engine) => engine.status === "empty")).toBe(true);
        const local = requestUrl(
          f.seen.find((entry) => entry.request.url.startsWith(searxng))!.request,
        );
        expect(local.searchParams.get("language")).toBe(searxngLanguage);
        const hosted = requestUrl(
          f.seen.find((entry) => entry.request.url.startsWith("https://treg.to"))!.request,
        );
        expect(hosted.searchParams.get("language")).toBe(hostedLanguage);
        expect(hosted.searchParams.get("location")).toBe(location);
        expect(hosted.searchParams.get("fetch")).toBeNull();
      }).pipe(Effect.provide(f.layer));
    }).pipe(Effect.provide(base)),
);

it.effect(
  "filters engines that ignore site and exclusion constraints instead of returning off-site matches",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture((r) =>
        Effect.succeed(
          reply(
            r,
            envelope(r, [
              {
                url: "https://pl.example/rooms/one",
                title: "Included",
                snippet: "yes",
                engines: ["google cse"],
              },
              { url: "https://pl.example/other", title: "Wrong path", engines: ["yahoo"] },
              { url: "https://no.pl.example/rooms", title: "Excluded", engines: ["startpage"] },
              {
                url: "https://evilpl.example/rooms",
                title: "Wrong host",
                engines: ["duckduckgo web"],
              },
            ]),
          ),
        ),
      );
      yield* Effect.gen(function* () {
        yield* f.provision;
        const result = yield* search({ query: "site:pl.example/rooms -site:no.pl.example venues" });
        expect(result.results.map((item) => item.title)).toEqual(["Included", "Included"]);
      }).pipe(Effect.provide(f.layer));
    }).pipe(Effect.provide(base)),
);

it.effect("bounds output while retaining provider attribution and marks truncation", () =>
  Effect.gen(function* () {
    const f = yield* fixture((r) =>
      Effect.succeed(
        reply(
          r,
          envelope(
            r,
            Array.from({ length: 60 }, (_, i) => ({
              url: `https://pl.example/${i}`,
              title: "T".repeat(1000),
              snippet: "S".repeat(5000),
              engines: ["google cse"],
            })),
          ),
        ),
      ),
    );
    yield* Effect.gen(function* () {
      yield* f.provision;
      const result = yield* search({ query: "bounded" });
      expect(result.truncated).toBe(true);
      expect(result.results.length).toBeGreaterThan(2);
      expect(result.results.length).toBeLessThanOrEqual(20);
      expect(new Set(result.results.map((item) => item.source))).toEqual(
        new Set(["searxng", "tinyfish"]),
      );
      expect(
        result.results.every((item) => item.title.length <= 300 && item.snippet.length <= 1000),
      ).toBe(true);
      expect(JSON.stringify(result).length).toBeLessThanOrEqual(20_100);
    }).pipe(Effect.provide(f.layer));
  }).pipe(Effect.provide(base)),
);

it.effect(
  "shares 25-second per-engine starts across threads and never caches a completed SERP",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* Effect.gen(function* () {
        yield* f.provision;
        yield* search({ query: "first" });
        const next = yield* Effect.forkScoped(
          search(
            { query: "second" },
            {
              ...scope,
              requestNamespace: "another-thread",
              thread: { ...scope.thread!, threadId: ThreadId.make("another-search-thread") },
            },
          ),
        );
        yield* TestClock.adjust(24_999);
        expect(f.seen).toHaveLength(2);
        yield* TestClock.adjust(1);
        yield* Queue.take(f.requests);
        yield* Queue.take(f.requests);
        yield* Queue.take(f.requests);
        yield* Queue.take(f.requests);
        const result = yield* Fiber.join(next);
        expect(result.engines.every((engine) => engine.delayMs >= 25_000)).toBe(true);
        for (const source of [searxng, "https://treg.to"])
          expect(
            f.seen.filter((entry) => entry.request.url.startsWith(source)).map((entry) => entry.at),
          ).toEqual([0, 25_000]);
        const repeated = yield* Effect.forkScoped(search({ query: "second" }));
        yield* TestClock.adjust(25_000);
        yield* Fiber.join(repeated);
        expect(f.seen).toHaveLength(6);
      }).pipe(Effect.provide(f.layer));
    }).pipe(Effect.provide(base)),
);

it.effect(
  "deduplicates simultaneous queries and preserves the second subscriber when the first cancels",
  () =>
    Effect.gen(function* () {
      const release = yield* Deferred.make<void>();
      const stopped = yield* Queue.unbounded<string>();
      const f = yield* fixture((r) =>
        Deferred.await(release).pipe(
          Effect.as(reply(r)),
          Effect.ensuring(Queue.offer(stopped, r.url)),
        ),
      );
      yield* Effect.gen(function* () {
        yield* f.provision;
        const first = yield* Effect.forkScoped(search({ query: "shared", locale: "pl-pl" }));
        yield* Queue.take(f.requests);
        yield* Queue.take(f.requests);
        const second = yield* Effect.forkScoped(search({ query: " shared ", locale: "pl-PL" }));
        // TestClock settles the new subscriber at its Deferred, without wall-clock waiting.
        yield* TestClock.adjust(0);
        yield* Fiber.interrupt(first);
        expect(yield* Queue.size(stopped)).toBe(0);
        expect(f.seen).toHaveLength(2);
        yield* Deferred.succeed(release, undefined);
        expect((yield* Fiber.join(second)).status).toBe("ok");
        expect(f.seen).toHaveLength(2);
        expect(yield* Queue.size(stopped)).toBe(2);
      }).pipe(Effect.provide(f.layer));
    }).pipe(Effect.provide(base)),
);

it.effect.each(["cancel", "timeout"] as const)(
  "%s aborts providers and keeps uncertain attempts reserved",
  (kind) =>
    Effect.gen(function* () {
      const closed = yield* Queue.unbounded<string>();
      const signals: AbortSignal[] = [];
      const f = yield* fixture((r, signal) => {
        signals.push(signal);
        return Effect.never.pipe(Effect.ensuring(Queue.offer(closed, r.url)));
      });
      yield* Effect.gen(function* () {
        yield* f.provision;
        const first = yield* Effect.forkScoped(search({ query: "abandoned" }));
        yield* Queue.take(f.requests);
        yield* Queue.take(f.requests);
        if (kind === "cancel") yield* Fiber.interrupt(first);
        else {
          yield* TestClock.adjust("20 seconds");
          const result = yield* Fiber.join(first);
          expect(result).toMatchObject({ status: "unavailable", results: [] });
          expect(result.engines.every((engine) => engine.status === "timeout")).toBe(true);
        }
        yield* Queue.take(closed);
        yield* Queue.take(closed);
        expect(signals.every((signal) => signal.aborted)).toBe(true);
        for (const source of ["searxng", "tinyfish"])
          expect(
            JSON.parse(
              yield* f.fs.readFileString(
                `${f.paths.stateDir}/web-cache/search-${source}-quota.json`,
              ),
            ).starts,
          ).toEqual([0]);
        const fresh = yield* Effect.forkScoped(search({ query: "abandoned" }));
        yield* TestClock.adjust(kind === "cancel" ? 25_000 : 5000);
        expect((yield* Queue.take(f.requests)).at).toBe(25_000);
        expect((yield* Queue.take(f.requests)).at).toBe(25_000);
        yield* Fiber.interrupt(fresh);
      }).pipe(Effect.provide(f.layer));
    }).pipe(Effect.provide(base)),
);

it.effect("honours TinyFish Retry-After without blocking the SearXNG lane", () =>
  Effect.gen(function* () {
    let hosted = 0;
    const f = yield* fixture((r) =>
      Effect.succeed(
        r.url.startsWith("https://treg.to") && ++hosted === 1
          ? reply(r, {}, 429, { "retry-after": "120" })
          : reply(r),
      ),
    );
    yield* Effect.gen(function* () {
      yield* f.provision;
      const first = yield* search({ query: "limited first" });
      expect(first.engines.find((engine) => engine.source === "tinyfish")).toMatchObject({
        status: "rate_limited",
        reason: "retry_after",
        retryAfterMs: 120_000,
      });
      const second = yield* Effect.forkScoped(search({ query: "limited next" }));
      yield* TestClock.adjust(25_000);
      const result = yield* Fiber.join(second);
      expect(result.engines.find((engine) => engine.engine === "google cse")?.status).toBe("ok");
      expect(result.engines.find((engine) => engine.source === "tinyfish")).toMatchObject({
        status: "rate_limited",
        retryAfterMs: 95_000,
      });
      expect(
        f.seen.filter((entry) => entry.request.url.startsWith("https://treg.to")),
      ).toHaveLength(1);
      yield* TestClock.adjust(95_000);
      expect((yield* search({ query: "after reset" })).status).toBe("ok");
      expect(hosted).toBe(2);
    }).pipe(Effect.provide(f.layer));
  }).pipe(Effect.provide(base)),
);

it.effect.each([
  { limit: "minute", count: 30 },
  { limit: "hour", count: 500 },
])("enforces the shared TinyFish $limit budget before sending", ({ limit, count }) =>
  Effect.gen(function* () {
    const f = yield* fixture();
    yield* f.seed(
      "tinyfish",
      Array.from({ length: count }, () => 0),
    );
    yield* Effect.gen(function* () {
      yield* f.provision;
      const result = yield* search({ query: "budget" });
      expect(result.engines.find((engine) => engine.source === "tinyfish")).toMatchObject({
        status: "rate_limited",
        reason: limit,
      });
      expect(f.seen).toHaveLength(1);
      expect(f.seen[0]!.request.url.startsWith(searxng)).toBe(true);
    }).pipe(Effect.provide(f.layer));
  }).pipe(Effect.provide(base)),
);

it.effect(
  "keeps starts and Retry-After across reconstruction without retaining queries or results",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture((r) =>
        Effect.succeed(reply(r, envelope(r), 200, { "retry-after": "120" })),
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* f.provision;
          yield* search({ query: "transient-query-marker" });
        }).pipe(Effect.provide(f.layer)),
      );
      const next = yield* fixture(
        () => Effect.die("Persisted Retry-After must stop both providers."),
        f.directory,
      );
      yield* Effect.gen(function* () {
        const result = yield* search({ query: "transient-query-marker" });
        expect(result.status).toBe("unavailable");
        expect(
          result.engines.every(
            (engine) => engine.status === "rate_limited" && engine.retryAfterMs === 120_000,
          ),
        ).toBe(true);
        expect(next.seen).toHaveLength(0);
      }).pipe(Effect.provide(next.layer));
      for (const source of ["tinyfish", "searxng"]) {
        const text = yield* f.fs.readFileString(
          `${f.paths.stateDir}/web-cache/search-${source}-quota.json`,
        );
        expect(JSON.parse(text)).toEqual({ starts: [0], blockedUntil: 120_000 });
        expect(text).not.toContain("transient-query-marker");
      }
    }).pipe(Effect.provide(base)),
);

it.effect("returns a safe unconfigured state when the existing secret is absent", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    yield* Effect.gen(function* () {
      const result = yield* search({ query: "no token" });
      expect(result.engines.find((engine) => engine.source === "tinyfish")).toMatchObject({
        status: "not_configured",
        reason: "secret_unavailable",
      });
      expect(f.seen).toHaveLength(1);
    }).pipe(Effect.provide(f.layer));
  }).pipe(Effect.provide(base)),
);

it.effect.each([
  { name: "nonzero charge", headers: { "x-treg-cost-micro": "1" }, reason: "charge" },
  { name: "missing charge", headers: { "x-treg-cost-micro": "" }, reason: "charge" },
  { name: "missing call identity", headers: { "x-treg-call-id": "" }, reason: "charge" },
  { name: "unsettled call", headers: { "x-treg-cost-micro": "null" }, reason: "charge" },
  { name: "upstream failed", headers: { "x-treg-upstream-failed": "1" }, reason: "contract" },
])("rejects $name instead of claiming an empty success", ({ headers, reason }) =>
  Effect.gen(function* () {
    const f = yield* fixture((r) =>
      Effect.succeed(
        reply(r, envelope(r, []), 200, r.url.startsWith("https://treg.to") ? headers : {}),
      ),
    );
    yield* Effect.gen(function* () {
      yield* f.provision;
      const result = yield* search({ query: "validate charge" });
      expect(result.engines.find((engine) => engine.source === "tinyfish")).toMatchObject({
        status: "provider_error",
        reason,
      });
      expect(result.status).toBe("degraded");
      expect(result.results).toEqual([]);
      expect(JSON.stringify(result)).not.toContain(TOKEN);
    }).pipe(Effect.provide(f.layer));
  }).pipe(Effect.provide(base)),
);

it.effect.each(["plain", "json", "html", "transport"] as const)(
  "rejects %s credential reflection and closes the authenticated request",
  (kind) =>
    Effect.gen(function* () {
      const signals: AbortSignal[] = [];
      const f = yield* fixture((r, signal) => {
        if (!r.url.startsWith("https://treg.to")) return Effect.succeed(reply(r, envelope(r, [])));
        signals.push(signal);
        if (kind === "transport")
          return Effect.fail(
            new HttpClientError.HttpClientError({
              reason: new HttpClientError.TransportError({ request: r, cause: new Error(TOKEN) }),
            }),
          );
        const secret =
          kind === "html" ? [...TOKEN].map((char) => `&#${char.charCodeAt(0)};`).join("") : TOKEN;
        const payload = envelope(r, [{ url: resultUrl, title: "Unsafe", snippet: secret }]);
        const body =
          kind === "json"
            ? JSON.stringify(payload).replace(
                TOKEN,
                [...TOKEN]
                  .map((char) => `\\u${char.charCodeAt(0).toString(16).padStart(4, "0")}`)
                  .join(""),
              )
            : payload;
        return Effect.succeed(reply(r, body));
      });
      yield* Effect.gen(function* () {
        yield* f.provision;
        const result = yield* search({ query: "safe error" });
        expect(result.results).toEqual([]);
        expect(result.engines.find((engine) => engine.source === "tinyfish")?.status).toBe(
          "provider_error",
        );
        expect(JSON.stringify(result)).not.toContain(TOKEN);
        expect(signals.every((signal) => signal.aborted)).toBe(true);
      }).pipe(Effect.provide(f.layer));
    }).pipe(Effect.provide(base)),
);

it.effect.each([
  {
    name: "read-only outside client",
    invocation: {
      ...scope,
      thread: undefined,
      client: { sessionId: "external", label: "External fixture", access: "read-only" as const },
    },
  },
  {
    name: "read-only linked client",
    invocation: {
      ...scope,
      thread: undefined,
      client: {
        sessionId: "linked",
        label: "Linked fixture",
        access: "read-only" as const,
        linked: true,
      },
    },
  },
])("denies $name before spending provider budgets", ({ invocation }) =>
  Effect.gen(function* () {
    const f = yield* fixture(() => Effect.die("Unauthorized client must not search."));
    yield* Effect.gen(function* () {
      yield* f.provision;
      const result = yield* call({ query: "denied" }, invocation);
      expect(result.isError).toBe(true);
      expect(result.structuredContent).toBeUndefined();
      expect(JSON.stringify(result.content)).toContain(
        invocation.client.linked ? "capability_denied" : "thread_credential_required",
      );
      expect(f.seen).toHaveLength(0);
    }).pipe(Effect.provide(f.layer));
  }).pipe(Effect.provide(base)),
);

it.effect("rejects invalid parameters and whitespace without contacting providers", () =>
  Effect.gen(function* () {
    const f = yield* fixture(() => Effect.die("Invalid input must not search."));
    yield* Effect.gen(function* () {
      for (const input of [{ query: "x".repeat(2001) }, { query: "query", locale: "../../pl" }])
        expect(yield* call(input).pipe(Effect.flip)).toBeInstanceOf(McpSchema.InvalidParams);
      for (const input of [{ query: "   " }, { query: "site:user:password@pl.example places" }])
        expect((yield* call(input)).isError).toBe(true);
      expect(f.seen).toHaveLength(0);
    }).pipe(Effect.provide(f.layer));
  }).pipe(Effect.provide(base)),
);

it.effect.each([
  { name: "missing SearXNG health", kind: "sear-health" },
  { name: "unknown SearXNG engine", kind: "unknown-engine" },
  { name: "missing TinyFish search count", kind: "tinyfish-count" },
  { name: "wrong TinyFish page", kind: "tinyfish-page" },
  { name: "HTTP-200 CAPTCHA", kind: "captcha" },
  { name: "oversized provider body", kind: "oversized" },
])("keeps $name unhealthy instead of returning a false empty success", ({ kind }) =>
  Effect.gen(function* () {
    const f = yield* fixture((r) => {
      const hosted = r.url.startsWith("https://treg.to");
      const failHosted = kind.startsWith("tinyfish") || kind === "oversized";
      if (hosted !== failHosted) return Effect.succeed(reply(r, envelope(r, [])));
      if (kind === "captcha")
        return Effect.succeed(reply(r, "<html>CAPTCHA: verify you are human</html>"));
      if (kind === "oversized") return Effect.succeed(reply(r, "x".repeat(1024 * 1024 + 1)));
      if (kind === "unknown-engine")
        return Effect.succeed(
          reply(r, envelope(r, [], { unresponsive_engines: [["unexpected engine", "CAPTCHA"]] })),
        );
      if (kind === "tinyfish-page") return Effect.succeed(reply(r, envelope(r, [], { page: 1 })));
      if (kind === "tinyfish-count")
        return Effect.succeed(reply(r, { query: "contract checks", results: [], page: 0 }));
      return Effect.succeed(reply(r, { query: "contract checks", results: [] }));
    });
    yield* Effect.gen(function* () {
      yield* f.provision;
      const result = yield* search({ query: "contract checks" });
      expect(result.status).toBe("degraded");
      expect(result.results).toEqual([]);
      const source = kind.startsWith("tinyfish") || kind === "oversized" ? "tinyfish" : "searxng";
      expect(
        result.engines
          .filter((engine) => engine.source === source)
          .every(
            (engine) => engine.status === (kind === "captcha" ? "challenge" : "provider_error"),
          ),
      ).toBe(true);
    }).pipe(Effect.provide(f.layer));
  }).pipe(Effect.provide(base)),
);

it.effect("honours SearXNG's HTTP-date Retry-After while TinyFish continues independently", () =>
  Effect.gen(function* () {
    let local = 0;
    const f = yield* fixture((r) =>
      Effect.succeed(
        r.url.startsWith(searxng) && ++local === 1
          ? reply(r, {}, 429, { "retry-after": "Thu, 01 Jan 1970 00:02:00 GMT" })
          : reply(r),
      ),
    );
    yield* Effect.gen(function* () {
      yield* f.provision;
      expect(
        (yield* search({ query: "date limit" })).engines.find(
          (engine) => engine.source === "searxng",
        ),
      ).toMatchObject({ status: "rate_limited", retryAfterMs: 120_000 });
      const next = yield* Effect.forkScoped(search({ query: "other query" }));
      yield* TestClock.adjust(25_000);
      const result = yield* Fiber.join(next);
      expect(result.engines.find((engine) => engine.source === "tinyfish")?.status).toBe("ok");
      expect(local).toBe(1);
      expect(
        result.engines
          .filter((engine) => engine.source === "searxng")
          .every((engine) => engine.status === "rate_limited"),
      ).toBe(true);
    }).pipe(Effect.provide(f.layer));
  }).pipe(Effect.provide(base)),
);

it.effect(
  "refuses a late queue admission before reserving quota and reports the actual queue delay",
  () =>
    Effect.gen(function* () {
      const firstRelease = yield* Deferred.make<void>();
      const secondRelease = yield* Deferred.make<void>();
      const counts = { searxng: 0, tinyfish: 0 };
      const f = yield* fixture((request) => {
        const source = request.url.startsWith(searxng) ? "searxng" : "tinyfish";
        return Deferred.await(++counts[source] === 1 ? firstRelease : secondRelease).pipe(
          Effect.as(reply(request)),
        );
      });
      yield* Effect.gen(function* () {
        yield* f.provision;
        const first = yield* Effect.forkScoped(search({ query: "queue one" }));
        yield* Queue.take(f.requests);
        yield* Queue.take(f.requests);
        const second = yield* Effect.forkScoped(search({ query: "queue two" }));
        yield* TestClock.adjust(0);
        const third = yield* Effect.forkScoped(search({ query: "queue three" }));
        yield* TestClock.adjust(15_000);
        yield* Deferred.succeed(firstRelease, undefined);
        yield* Fiber.join(first);
        yield* TestClock.adjust(10_000);
        expect((yield* Queue.take(f.requests)).at).toBe(25_000);
        expect((yield* Queue.take(f.requests)).at).toBe(25_000);
        yield* TestClock.adjust(15_000);
        yield* Deferred.succeed(secondRelease, undefined);
        yield* Fiber.join(second);
        // Releasing the prior request does not finish the queued MCP response's fiber.
        const result = yield* Fiber.join(third);
        expect(yield* Clock.currentTimeMillis).toBe(40_000);
        expect(result.status).toBe("unavailable");
        expect(
          result.engines.every(
            (engine) =>
              engine.status === "rate_limited" &&
              engine.reason === "queue" &&
              engine.delayMs === 40_000,
          ),
        ).toBe(true);
        expect(f.seen).toHaveLength(4);
        for (const source of ["tinyfish", "searxng"])
          expect(
            JSON.parse(
              yield* f.fs.readFileString(
                `${f.paths.stateDir}/web-cache/search-${source}-quota.json`,
              ),
            ).starts,
          ).toEqual([0, 25_000]);
      }).pipe(Effect.provide(f.layer));
    }).pipe(Effect.provide(base)),
);
