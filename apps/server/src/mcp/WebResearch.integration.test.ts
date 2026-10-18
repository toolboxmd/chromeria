import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { expect, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as TestClock from "effect/testing/TestClock";
import { McpSchema, McpServer } from "effect/ai";

import * as Config from "../config.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as EffectWorker from "../orchestration-v2/EffectWorker.ts";
import * as Registry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Replay from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as WebResearch from "../webResearch/WebResearch.ts";
import * as PageCache from "../webResearch/PageCache.ts";
import * as PublicHttp from "../webResearch/PublicHttp.ts";
import * as PublicRender from "../webResearch/PublicRender.ts";
import { type CachedPage, WebReadResult } from "../webResearch/schemas.ts";
import * as McpHttpServer from "./McpHttpServer.ts";
import * as Invocation from "./McpInvocationContext.ts";

const body = `<h1>Research fixture</h1><p>${"Full text stays in the page cache. ".repeat(1000)}</p>`;
const response = (text = body) => ({
  status: 200,
  headers: { "content-type": "text/html" },
  body: text,
});
const decodeResult = Schema.decodeUnknownEffect(WebReadResult);

const memoryCache = (pages: Map<string, CachedPage>) => {
  return Layer.succeed(
    PageCache.PageCache,
    PageCache.PageCache.of({
      get: (key) =>
        Effect.sync(() =>
          "url" in key
            ? (pages.get(key.url) ?? null)
            : "cacheId" in key
              ? ([...pages.values()].find((page) => page.cacheId === key.cacheId) ?? null)
              : null,
        ),
      put: (page) =>
        Effect.sync(() => {
          pages.set(page.url, page);
        }),
    }),
  );
};

const fixture = Effect.fnUntraced(function* (
  respond: PublicHttp.PublicHttp["Service"]["get"] = () => Effect.succeed(response()),
  cacheLayer: typeof PageCache.layer = PageCache.layer,
  robots = "User-agent: *\nAllow: /",
) {
  const requests = yield* Queue.unbounded<{ url: string; at: number }>();
  const runtime = Replay.layerWithRegistry(
    { name: "research-read" },
    Registry.layerFromAdapters([]),
    { runEffectWorker: false },
  );
  const reader = WebResearch.layer.pipe(
    Layer.provide(cacheLayer),
    Layer.provide(
      Layer.succeed(PublicRender.PublicRender, {
        render: () => Effect.die("Usable HTTP pages must not render."),
      }),
    ),
    Layer.provide(
      Layer.succeed(PublicHttp.PublicHttp, {
        get: (url) =>
          Effect.gen(function* () {
            yield* Queue.offer(requests, { url: url.href, at: yield* Clock.currentTimeMillis });
            return url.pathname === "/robots.txt" ? response(robots) : yield* respond(url);
          }),
      }),
    ),
    Layer.provide(Config.layerTest(process.cwd(), { prefix: "research-integration-" })),
    Layer.provide(NodeServices.layer),
  );
  const layer = McpHttpServer.layerWebResearchToolkit.pipe(
    Layer.provideMerge(McpServer.McpServer.layer),
    Layer.provide(reader),
    Layer.provideMerge(runtime),
    Layer.provide(NodeCrypto.layer),
  );
  return { layer, requests };
});

const session = Effect.gen(function* () {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
  const server = yield* McpServer.McpServer;
  const threadId = ThreadId.make("research-integration-thread");
  yield* orchestrator.dispatch({
    type: "thread.create",
    commandId: CommandId.make("research:create"),
    threadId,
    projectId: ProjectId.make("research-project"),
    title: "Research",
    branch: null,
    worktreePath: null,
    modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "test" },
    runtimeMode: "full-access",
    interactionMode: "default",
    createdBy: "user",
    creationSource: "web",
  });
  yield* worker.drain();
  const before = yield* orchestrator.getThreadProjection(threadId);
  const client = McpSchema.McpServerClient.of({
    clientId: 1,
    protocolVersion: "2025-06-18",
    clientCapabilities: {},
    clientInfo: { name: "research-proof", version: "1" },
    initializePayload: {
      protocolVersion: "2025-06-18",
      capabilities: {},
      clientInfo: { name: "research-proof", version: "1" },
    },
    getClient: Effect.die("unused"),
  });
  const call = (args: Record<string, unknown>) =>
    server.callTool({ name: "web_read", arguments: args }).pipe(
      Effect.provideService(McpSchema.McpServerClient, client),
      Effect.provideService(Invocation.McpInvocationContext, {
        environmentId: EnvironmentId.make("research-environment"),
        requestNamespace: "research-agent",
        issuedAt: 0,
        thread: {
          threadId,
          providerSessionId: "research-session",
          providerInstanceId: ProviderInstanceId.make("codex"),
        },
        client: undefined,
        capabilities: new Set<Invocation.McpCapability>(),
      }),
    );
  const read = Effect.fnUntraced(function* (args: Record<string, unknown>) {
    const result = yield* call(args);
    expect(result.isError, JSON.stringify(result.content)).not.toBe(true);
    return yield* decodeResult(result.structuredContent);
  });
  const check = Effect.gen(function* () {
    yield* worker.drain();
    expect(yield* orchestrator.getThreadProjection(threadId)).toEqual(before);
  }).pipe(Effect.orDie);
  return { call, read, check };
});

it.effect(
  "a registered research read returns bounded content with drained effects and no research events",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* Effect.gen(function* () {
        const s = yield* session;
        yield* Effect.gen(function* () {
          const fiber = yield* Effect.forkScoped(s.call({ url: "https://research.example/" }));
          expect((yield* Queue.take(f.requests)).url).toBe("https://research.example/robots.txt");
          yield* TestClock.adjust(3000);
          expect((yield* Queue.take(f.requests)).url).toBe("https://research.example/");
          const result = yield* Fiber.join(fiber);
          expect(result.isError, JSON.stringify(result.content)).not.toBe(true);
          expect(result.structuredContent).toMatchObject({ status: "ok", nextOffset: 20_000 });
          expect(JSON.stringify(result).length).toBeLessThan(45_000);
        }).pipe(Effect.ensuring(s.check));
      }).pipe(Effect.provide(f.layer));
    }),
);

it.effect(
  "a subpage read shares bounded output with drained effects and separate continuation",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture((url) =>
        Effect.succeed(
          response(
            url.pathname === "/"
              ? '<h1>Research fixture</h1><p>Room and event facts live on separate pages. The reader preserves their provenance and shares one output budget.</p><a href="/camere">Rooms</a><a href="/eventi">Events</a>'
              : body,
          ),
        ),
      );
      yield* Effect.gen(function* () {
        const s = yield* session;
        yield* Effect.gen(function* () {
          const fiber = yield* Effect.forkScoped(
            s.call({ url: "https://subpage-integration.example/" }),
          );
          expect((yield* Queue.take(f.requests)).url).toBe(
            "https://subpage-integration.example/robots.txt",
          );
          yield* TestClock.adjust(3000);
          expect((yield* Queue.take(f.requests)).url).toBe("https://subpage-integration.example/");
          yield* TestClock.adjust(3000);
          expect((yield* Queue.take(f.requests)).url).toBe(
            "https://subpage-integration.example/camere",
          );
          const result = yield* Fiber.join(fiber);
          expect(result.isError).not.toBe(true);
          const pages = yield* decodeResult(result.structuredContent);
          expect(pages).toMatchObject({
            status: "ok",
            nextOffset: null,
            subpages: [{ url: "https://subpage-integration.example/camere", status: "ok" }],
            truncated: true,
            truncationReasons: ["output_budget"],
            pendingSubpages: ["https://subpage-integration.example/eventi"],
            unvisitedSubpages: 1,
          });
          expect(pages.text.length + pages.subpages[0]!.text.length).toBe(20_000);
          expect(pages.subpages[0]!.nextOffset).toBeGreaterThan(0);
          expect(JSON.stringify(result).length).toBeLessThan(50_000);
          expect(yield* Queue.size(f.requests)).toBe(0);
        }).pipe(Effect.ensuring(s.check));
      }).pipe(Effect.provide(f.layer));
    }),
);

it.effect("round 2: queued fast reads do not time out or cache queue failures", () =>
  Effect.gen(function* () {
    const pages = new Map<string, CachedPage>();
    const cacheLayer = memoryCache(pages);
    const f = yield* fixture(undefined, cacheLayer);
    yield* Effect.gen(function* () {
      const s = yield* session;
      yield* Effect.gen(function* () {
        const urls = Array.from({ length: 12 }, (_, i) => `https://queue.example/room-${i}`);
        const fiber = yield* Effect.forkScoped(
          Effect.forEach(urls, (url) => s.read({ url, followSubpages: false }), {
            concurrency: "unbounded",
          }),
        );
        expect((yield* Queue.take(f.requests)).url).toBe("https://queue.example/robots.txt");
        yield* TestClock.adjust("36 seconds");
        const results = yield* Fiber.join(fiber);
        expect(results.map((page) => page.status)).toEqual(Array(12).fill("ok"));
        expect(results.map((page) => page.delayMs).toSorted((a, b) => a - b)).toEqual([
          3000, 6000, 9000, 12000, 15000, 18000, 21000, 24000, 27000, 30000, 33000, 36000,
        ]);
        expect([...pages.values()].every((page) => page.status === "ok")).toBe(true);
        expect(yield* Queue.size(f.requests)).toBe(12);
        const cached = yield* Effect.forEach(urls, (url) => s.read({ url, followSubpages: false }));
        expect(cached.every((page) => page.status === "ok" && page.rung === "cache")).toBe(true);
        expect(yield* Queue.size(f.requests)).toBe(12);
      }).pipe(Effect.ensuring(s.check));
    }).pipe(Effect.provide(f.layer));
  }),
);

it.effect("round 2: only active HTTP time consumes the total redirect budget", () =>
  Effect.gen(function* () {
    const releases = yield* Queue.unbounded<Deferred.Deferred<void>>();
    const f = yield* fixture((url) =>
      Effect.gen(function* () {
        const release = yield* Deferred.make<void>();
        yield* Queue.offer(releases, release);
        yield* Deferred.await(release);
        return { ...response(""), status: 302, headers: { location: `${url.pathname}/next` } };
      }),
    );
    yield* Effect.gen(function* () {
      const s = yield* session;
      yield* Effect.gen(function* () {
        const fiber = yield* Effect.forkScoped(
          s.read({ url: "https://budget.example/start", followSubpages: false }),
        );
        yield* Queue.take(f.requests);
        yield* TestClock.adjust(3000);
        for (let hop = 0; hop < 3; hop++) {
          const release = yield* Queue.take(releases);
          yield* TestClock.adjust(8000);
          yield* Deferred.succeed(release, undefined);
        }
        yield* Queue.take(releases); // Fourth hop has six active seconds left.
        yield* TestClock.adjust(6000);
        const page = yield* Fiber.join(fiber);
        expect(page).toMatchObject({
          status: "timeout",
          delayMs: 3000,
          fetchedAt: "1970-01-01T00:00:33.000Z",
        });
        expect(yield* s.read({ url: "https://budget.example/start" })).toMatchObject({
          status: "timeout",
          rung: "cache",
        });
      }).pipe(Effect.ensuring(s.check));
    }).pipe(Effect.provide(f.layer));
  }),
);

it.effect.each(["corrupt-index", "cache-dir-is-file"] as const)(
  "round 2: %s cannot prevent MCP startup",
  (failure) =>
    Effect.gen(function* () {
      const brokenCache = Layer.unwrap(
        Effect.gen(function* () {
          const config = yield* Config.ServerConfig;
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const directory = path.join(config.stateDir, "web-cache");
          if (failure === "corrupt-index") {
            yield* fs.makeDirectory(directory, { recursive: true }).pipe(Effect.orDie);
            yield* fs
              .writeFileString(
                path.join(directory, "index.sqlite"),
                "not a SQLite database".repeat(400),
              )
              .pipe(Effect.orDie);
          } else {
            yield* fs.makeDirectory(config.stateDir, { recursive: true }).pipe(Effect.orDie);
            yield* fs
              .writeFileString(directory, "This file prevents cache directory creation.")
              .pipe(Effect.orDie);
          }
          return PageCache.layer;
        }),
      );
      const f = yield* fixture(undefined, brokenCache);
      yield* Effect.gen(function* () {
        const s = yield* session;
        yield* Effect.gen(function* () {
          const result = yield* s.call({ url: "https://cache.example/" });
          expect(result.isError).toBe(true);
          expect(JSON.stringify(result.content)).toContain("research page cache is unavailable");
          expect(JSON.stringify(result.content)).toContain("restart the server");
          expect(yield* Queue.size(f.requests)).toBe(0);
        }).pipe(Effect.ensuring(s.check));
      }).pipe(Effect.provide(f.layer));
    }),
);

it.effect("round 2: rechecks storage when a shared fetch completes after a cache miss", () =>
  Effect.gen(function* () {
    const missed = yield* Deferred.make<void>();
    const resumeMiss = yield* Deferred.make<void>();
    const releasePage = yield* Deferred.make<void>();
    let pauseMiss = false;
    let fetches = 0;
    const racingCache = Layer.effect(
      PageCache.PageCache,
      Effect.gen(function* () {
        const cache = yield* PageCache.PageCache;
        return PageCache.PageCache.of({
          get: (key) =>
            Effect.gen(function* () {
              const page = yield* cache.get(key);
              if ("url" in key && !page && pauseMiss) {
                pauseMiss = false;
                yield* Deferred.succeed(missed, undefined);
                yield* Deferred.await(resumeMiss);
              }
              return page;
            }),
          put: cache.put,
        });
      }),
    ).pipe(Layer.provide(PageCache.layer));
    const f = yield* fixture(
      () =>
        ++fetches === 1
          ? Deferred.await(releasePage).pipe(Effect.as(response()))
          : Effect.die("A completed shared fetch must not be repeated after a cache miss."),
      racingCache,
    );
    yield* Effect.gen(function* () {
      const s = yield* session;
      yield* Effect.gen(function* () {
        const first = yield* Effect.forkScoped(s.read({ url: "https://race.example/" }));
        yield* Queue.take(f.requests);
        yield* TestClock.adjust(3000);
        yield* Queue.take(f.requests);
        pauseMiss = true;
        const second = yield* Effect.forkScoped(s.read({ url: "https://race.example/" }));
        yield* Deferred.await(missed);
        yield* Deferred.succeed(releasePage, undefined);
        const a = yield* Fiber.join(first);
        yield* Deferred.succeed(resumeMiss, undefined);
        yield* TestClock.adjust(3000);
        const b = yield* Fiber.join(second);
        expect(b).toMatchObject({
          status: "ok",
          rung: "cache",
          cacheId: a.cacheId,
          contentHash: a.contentHash,
          delayMs: 0,
        });
        expect(fetches).toBe(1);
        expect(yield* Queue.size(f.requests)).toBe(0);
      }).pipe(Effect.ensuring(s.check));
    }).pipe(Effect.provide(f.layer));
  }),
);

it.effect("round 2: the storage recheck preserves forced refresh", () =>
  Effect.gen(function* () {
    let fetches = 0;
    const f = yield* fixture(() => Effect.succeed(response(`${body}<p>Capture ${++fetches}</p>`)));
    yield* Effect.gen(function* () {
      const s = yield* session;
      yield* Effect.gen(function* () {
        const first = yield* Effect.forkScoped(s.read({ url: "https://force.example/" }));
        yield* Queue.take(f.requests);
        yield* TestClock.adjust(3000);
        yield* Queue.take(f.requests);
        const a = yield* Fiber.join(first);
        const second = yield* Effect.forkScoped(
          s.read({ url: "https://force.example/", maxAgeHours: 0 }),
        );
        yield* TestClock.adjust(3000);
        yield* Queue.take(f.requests);
        const b = yield* Fiber.join(second);
        expect(b).toMatchObject({ status: "ok", rung: "http" });
        expect(b.contentHash).not.toBe(a.contentHash);
        expect(fetches).toBe(2);
      }).pipe(Effect.ensuring(s.check));
    }).pipe(Effect.provide(f.layer));
  }),
);

it.effect("round 2: ignores a non-web meta refresh and preserves readable content", () =>
  Effect.gen(function* () {
    const f = yield* fixture(() =>
      Effect.succeed(
        response(`<meta http-equiv="refresh" content="0;url=javascript:location.reload()">${body}`),
      ),
    );
    yield* Effect.gen(function* () {
      const s = yield* session;
      yield* Effect.gen(function* () {
        const fiber = yield* Effect.forkScoped(s.read({ url: "https://refresh.example/" }));
        yield* Queue.take(f.requests);
        yield* TestClock.adjust(3000);
        yield* Queue.take(f.requests);
        const page = yield* Fiber.join(fiber);
        expect(page).toMatchObject({ status: "ok", finalUrl: "https://refresh.example/" });
        expect(page.text).toContain("Full text stays in the page cache.");
        expect(yield* Queue.size(f.requests)).toBe(0);
      }).pipe(Effect.ensuring(s.check));
    }).pipe(Effect.provide(f.layer));
  }),
);

it.effect.each([600, 99999999])(
  "round 3: Retry-After %s refuses long waits without caching the refusal",
  (seconds) =>
    Effect.gen(function* () {
      const pages = new Map<string, CachedPage>();
      const f = yield* fixture(
        (url) =>
          Effect.succeed(
            url.pathname === "/first"
              ? {
                  ...response(),
                  status: seconds === 600 ? 503 : 200,
                  headers: { "retry-after": String(seconds) },
                }
              : response(),
          ),
        memoryCache(pages),
      );
      yield* Effect.gen(function* () {
        const s = yield* session;
        yield* Effect.gen(function* () {
          const first = yield* Effect.forkScoped(s.read({ url: "https://backoff.example/first" }));
          yield* Queue.take(f.requests);
          yield* TestClock.adjust(3000);
          yield* Fiber.join(first);
          yield* Queue.take(f.requests);
          const refused = yield* Effect.forkScoped(s.read({ url: "https://backoff.example/next" }));
          yield* TestClock.adjust("60 seconds");
          // A completed fiber retains its exit; an unbounded wait is interrupted and fails this assertion.
          yield* Fiber.interrupt(refused);
          const exit = yield* Fiber.await(refused);
          expect(Exit.isSuccess(exit)).toBe(true);
          if (Exit.isSuccess(exit))
            expect(exit.value).toMatchObject({
              status: "challenge",
              rung: "http",
              delayMs: 0,
              retryAfterMs: seconds * 1000,
            });
          expect(pages.has("https://backoff.example/next")).toBe(false);
          expect(yield* Queue.size(f.requests)).toBe(0);
          if (seconds === 600) {
            yield* TestClock.adjust("540 seconds");
            const recovered = yield* s.read({ url: "https://backoff.example/next" });
            expect(recovered).toMatchObject({ status: "ok", rung: "http", delayMs: 0 });
            expect((yield* Queue.take(f.requests)).url).toBe("https://backoff.example/next");
          }
        }).pipe(Effect.ensuring(s.check));
      }).pipe(Effect.provide(f.layer));
    }),
);

it.effect.each([120, 3600])(
  "round 3: Crawl-delay %s refuses long waits before fetching the page",
  (seconds) =>
    Effect.gen(function* () {
      const pages = new Map<string, CachedPage>();
      const f = yield* fixture(
        undefined,
        memoryCache(pages),
        `User-agent: *\nAllow: /\nCrawl-delay: ${seconds}`,
      );
      yield* Effect.gen(function* () {
        const s = yield* session;
        yield* Effect.gen(function* () {
          const first = yield* Effect.forkScoped(s.read({ url: "https://crawl.example/" }));
          yield* Queue.take(f.requests);
          yield* TestClock.adjust("60 seconds");
          yield* Fiber.interrupt(first);
          const exit = yield* Fiber.await(first);
          expect(Exit.isSuccess(exit)).toBe(true);
          if (Exit.isSuccess(exit))
            expect(exit.value).toMatchObject({
              status: "challenge",
              delayMs: 0,
              retryAfterMs: seconds * 1000,
            });
          expect(pages.size).toBe(0);
          expect(yield* Queue.size(f.requests)).toBe(0);
          if (seconds === 120) {
            yield* TestClock.adjust("60 seconds");
            expect(yield* s.read({ url: "https://crawl.example/" })).toMatchObject({
              status: "ok",
              rung: "http",
              delayMs: 0,
            });
            expect((yield* Queue.take(f.requests)).url).toBe("https://crawl.example/");
          }
        }).pipe(Effect.ensuring(s.check));
      }).pipe(Effect.provide(f.layer));
    }),
);

it.effect("round 3: bounds actual semaphore contention separately from active fetch time", () =>
  Effect.gen(function* () {
    const pages = new Map<string, CachedPage>();
    const f = yield* fixture(undefined, memoryCache(pages));
    yield* Effect.gen(function* () {
      const s = yield* session;
      yield* Effect.gen(function* () {
        const fiber = yield* Effect.forkScoped(
          Effect.forEach(
            Array.from({ length: 24 }, (_, i) => `https://contention.example/${i}`),
            (url) => s.read({ url, followSubpages: false }),
            { concurrency: "unbounded" },
          ),
        );
        yield* Queue.take(f.requests);
        yield* TestClock.adjust("63 seconds");
        yield* Fiber.interrupt(fiber);
        const exit = yield* Fiber.await(fiber);
        expect(Exit.isSuccess(exit)).toBe(true);
        if (Exit.isSuccess(exit)) {
          expect(exit.value.some((page) => page.status === "challenge")).toBe(true);
          expect(
            exit.value.every(
              (page) =>
                page.status === "ok" ||
                (page.status === "challenge" &&
                  page.delayMs === 60_000 &&
                  page.retryAfterMs === null),
            ),
          ).toBe(true);
        }
        expect([...pages.values()].every((page) => page.status === "ok")).toBe(true);
      }).pipe(Effect.ensuring(s.check));
    }).pipe(Effect.provide(f.layer));
  }),
);

it.effect.each([
  [55, 6],
  [60, 2],
] as const)(
  "round 4: Crawl-delay %s allows an active response lasting %s seconds across the queue deadline",
  ([seconds, activeSeconds]) =>
    Effect.gen(function* () {
      const release = yield* Deferred.make<void>();
      const pages = new Map<string, CachedPage>();
      const f = yield* fixture(
        () => Deferred.await(release).pipe(Effect.as(response())),
        memoryCache(pages),
        `User-agent: *\nAllow: /\nCrawl-delay: ${seconds}`,
      );
      yield* Effect.gen(function* () {
        const s = yield* session;
        yield* Effect.gen(function* () {
          const fiber = yield* Effect.forkScoped(
            s.read({ url: "https://active.example/", followSubpages: false }),
          );
          yield* Queue.take(f.requests);
          yield* TestClock.adjust(seconds * 1000);
          expect(yield* Queue.take(f.requests)).toEqual({
            url: "https://active.example/",
            at: seconds * 1000,
          });
          yield* TestClock.adjust(activeSeconds * 1000);
          yield* Deferred.succeed(release, undefined);
          expect(yield* Fiber.join(fiber)).toMatchObject({
            status: "ok",
            delayMs: seconds * 1000,
            httpStatus: 200,
          });
          expect(pages.get("https://active.example/")?.status).toBe("ok");
        }).pipe(Effect.ensuring(s.check));
      }).pipe(Effect.provide(f.layer));
    }),
);

it.effect("round 4: the active HTTP timeout starts after long spacing", () =>
  Effect.gen(function* () {
    const release = yield* Deferred.make<void>();
    const f = yield* fixture(
      () => Deferred.await(release).pipe(Effect.as(response())),
      memoryCache(new Map()),
      "User-agent: *\nAllow: /\nCrawl-delay: 60",
    );
    yield* Effect.gen(function* () {
      const s = yield* session;
      yield* Effect.gen(function* () {
        const fiber = yield* Effect.forkScoped(
          s.read({ url: "https://active-timeout.example/", followSubpages: false }),
        );
        yield* Queue.take(f.requests);
        yield* TestClock.adjust("60 seconds");
        yield* Queue.take(f.requests);
        yield* TestClock.adjust("10 seconds");
        expect(yield* Fiber.join(fiber)).toMatchObject({
          status: "timeout",
          delayMs: 60_000,
          fetchedAt: "1970-01-01T00:01:10.000Z",
        });
      }).pipe(Effect.ensuring(s.check));
    }).pipe(Effect.provide(f.layer));
  }),
);

it.effect("round 4: eight queued slow responses finish on their own active budgets", () =>
  Effect.gen(function* () {
    const releases = yield* Queue.unbounded<Deferred.Deferred<void>>();
    const f = yield* fixture(
      () =>
        Effect.gen(function* () {
          const release = yield* Deferred.make<void>();
          yield* Queue.offer(releases, release);
          yield* Deferred.await(release);
          return response();
        }),
      memoryCache(new Map()),
    );
    yield* Effect.gen(function* () {
      const s = yield* session;
      yield* Effect.gen(function* () {
        const fiber = yield* Effect.forkScoped(
          Effect.forEach(
            Array.from({ length: 8 }, (_, i) => `https://slow-queue.example/${i}`),
            (url) => s.read({ url, followSubpages: false }),
            { concurrency: "unbounded" },
          ),
        );
        yield* Queue.take(f.requests);
        yield* TestClock.adjust("3 seconds");
        for (let i = 0; i < 8; i++) {
          const release = yield* Queue.take(releases);
          yield* TestClock.adjust("8 seconds");
          yield* Deferred.succeed(release, undefined);
        }
        const results = yield* Fiber.join(fiber);
        expect(results.map((page) => page.status)).toEqual(Array(8).fill("ok"));
        expect(results.map((page) => page.delayMs).toSorted((a, b) => a - b)).toEqual([
          3000, 11000, 19000, 27000, 35000, 43000, 51000, 59000,
        ]);
      }).pipe(Effect.ensuring(s.check));
    }).pipe(Effect.provide(f.layer));
  }),
);

it.effect("round 4: a refused redirect hop has no inherited HTTP response status", () =>
  Effect.gen(function* () {
    const f = yield* fixture(
      () =>
        Effect.succeed({
          ...response(""),
          status: 302,
          headers: { location: "/next", "retry-after": "600" },
        }),
      memoryCache(new Map()),
    );
    yield* Effect.gen(function* () {
      const s = yield* session;
      yield* Effect.gen(function* () {
        const fiber = yield* Effect.forkScoped(s.read({ url: "https://redirect-wait.example/" }));
        yield* Queue.take(f.requests);
        yield* TestClock.adjust("3 seconds");
        yield* Queue.take(f.requests);
        expect(yield* Fiber.join(fiber)).toMatchObject({
          status: "challenge",
          finalUrl: "https://redirect-wait.example/next",
          httpStatus: null,
          delayMs: 3000,
          retryAfterMs: 600_000,
        });
        expect(yield* Queue.size(f.requests)).toBe(0);
      }).pipe(Effect.ensuring(s.check));
    }).pipe(Effect.provide(f.layer));
  }),
);
