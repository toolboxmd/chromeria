import * as Search from "../webResearch/Search.ts";
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
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import { McpSchema, McpServer } from "effect/ai";

import * as Config from "../config.ts";
import * as EffectWorker from "../orchestration-v2/EffectWorker.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as Registry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Replay from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as PreviewBrowser from "../preview/PreviewBrowser.ts";
import * as PageCache from "../webResearch/PageCache.ts";
import * as PublicHttp from "../webResearch/PublicHttp.ts";
import * as PublicRender from "../webResearch/PublicRender.ts";
import * as TinyFish from "../webResearch/TinyFish.ts";
import * as WebResearch from "../webResearch/WebResearch.ts";
import { classify, extract } from "../webResearch/extract.ts";
import { HostedWebReadResult } from "../webResearch/ResearchHost.ts";
import * as Settings from "../serverSettings.ts";
import * as PeerForwarding from "../peer/PeerForwarding.ts";
import * as PeerLinks from "../peer/PeerLinks.ts";
import * as Invocation from "./McpInvocationContext.ts";
import * as McpHttpServer from "./McpHttpServer.ts";
import { SKIP_WITHOUT_CHROMIUM, testChromium } from "../preview/ChromeForTesting.testkit.ts";

const threadId = ThreadId.make("render-integration-thread");
const client = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  clientCapabilities: {},
  clientInfo: { name: "render-proof", version: "1" },
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "render-proof", version: "1" },
  },
  getClient: Effect.die("unused"),
});
const decodeResult = Schema.decodeUnknownEffect(HostedWebReadResult);
const read = Effect.fnUntraced(function* (arguments_: Record<string, unknown>) {
  const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
  yield* worker.drain();
  const result = yield* McpServer.McpServer.use((server) =>
    server.callTool({ name: "web_read", arguments: arguments_ }),
  ).pipe(
    Effect.provideService(McpSchema.McpServerClient, client),
    Effect.provideService(Invocation.McpInvocationContext, {
      environmentId: EnvironmentId.make("render-environment"),
      requestNamespace: "render-agent",
      issuedAt: 0,
      thread: {
        threadId,
        providerSessionId: "render-session",
        providerInstanceId: ProviderInstanceId.make("codex"),
      },
      client: undefined,
      capabilities: new Set<Invocation.McpCapability>(),
    }),
  );
  yield* worker.drain();
  expect(result.isError, JSON.stringify(result.content)).not.toBe(true);
  return yield* decodeResult(result.structuredContent);
});
const shell = "<html><head><title>Loading</title></head><body><div id='app'></div></body></html>";
const article = `<html><head><title>Rendered rooms</title><script type="application/ld+json">{"numberOfRooms":8}</script></head><body><h1>Rooms</h1><p>${"Eight guest rooms and a garden for events. ".repeat(700)}</p><table><tr><td>Family</td><td>4</td></tr></table><a href='/camere'>All rooms</a></body></html>`;
const response = (body: string, status = 200) => ({
  body,
  status,
  headers: { "content-type": "text/html" },
});
const scriptedPage = (body: string) =>
  `<body><script>document.body.innerHTML=${JSON.stringify(body).replaceAll("<", "\\u003c")}</script></body>`;
const pinnedRendererLayer = (executable: string) =>
  PublicRender.layer.pipe(
    Layer.provide(
      Layer.succeed(PreviewBrowser.PreviewBrowser, {
        installed: Effect.succeedSome(executable),
        executable: Effect.die("Research must not install a browser."),
      }),
    ),
  );

const fixture = Effect.fnUntraced(function* (
  render: PublicRender.PublicRender["Service"]["render"],
  respond: PublicHttp.PublicHttp["Service"]["get"] = () => Effect.succeed(response(shell)),
  robots = "User-agent: *\nAllow: /",
) {
  const requests = yield* Queue.unbounded<{ url: string; at: number }>();
  const fallbacks: Array<string> = [];
  const runtime = Replay.layerWithRegistry(
    { name: "render-read" },
    Registry.layerFromAdapters([]),
    { runEffectWorker: false },
  );
  const reader = WebResearch.layer.pipe(
    Layer.provide(Layer.mock(Search.Search)({})),
    Layer.provide(
      Layer.succeed(TinyFish.TinyFish, {
        fetch: ({ url }) =>
          Effect.sync(() => {
            fallbacks.push(url.href);
            return undefined;
          }),
      }),
    ),
    Layer.provide(PageCache.layer),
    Layer.provide(Layer.succeed(PublicRender.PublicRender, { render })),
    Layer.provide(
      Layer.succeed(PublicHttp.PublicHttp, {
        get: (url) =>
          Effect.gen(function* () {
            yield* Queue.offer(requests, { url: url.href, at: yield* Clock.currentTimeMillis });
            return url.pathname === "/robots.txt" ? response(robots) : yield* respond(url);
          }),
      }),
    ),
    Layer.provide(Config.layerTest(process.cwd(), { prefix: "research-render-" })),
    Layer.provide(NodeServices.layer),
  );
  const layer = McpHttpServer.layerWebResearchToolkit.pipe(
    Layer.provideMerge(McpServer.McpServer.layer),
    Layer.provide(reader),
    Layer.provide(Settings.layerTest()),
    Layer.provide(Layer.mock(PeerForwarding.PeerForwarding)({})),
    Layer.provide(Layer.mock(PeerLinks.PeerLinks)({})),
    Layer.provideMerge(runtime),
    Layer.provide(NodeCrypto.layer),
  );
  const setup = Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make("render:create"),
      threadId,
      projectId: ProjectId.make("research-project"),
      title: "Render proof",
      branch: null,
      worktreePath: null,
      modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "test" },
      runtimeMode: "full-access",
      interactionMode: "default",
      createdBy: "user",
      creationSource: "web",
    });
    yield* EffectWorker.OrchestrationEffectWorkerV2.use((worker) => worker.drain());
    return yield* orchestrator.getThreadProjection(threadId);
  });
  const start = Effect.fnUntraced(function* (url = "https://render.example/") {
    const fiber = yield* Effect.forkScoped(read({ url }));
    expect((yield* Queue.take(requests)).url).toBe(new URL("/robots.txt", url).href);
    yield* TestClock.adjust(3000);
    expect((yield* Queue.take(requests)).url).toBe(url);
    return fiber;
  });
  return { layer, requests, setup, start, fallbacks };
});

it.effect.each([
  { status: "thin", body: shell },
  {
    status: "consent",
    body: "<h1>Cookie consent</h1><p>Accept all cookies to continue. Manage your preferences.</p>",
  },
])(
  "renders $status at the registered MCP seam, preserving full text, cache and provenance",
  ({ body }) =>
    Effect.gen(function* () {
      let renders = 0;
      const f = yield* fixture(
        (input) =>
          Effect.sync(() => {
            renders++;
            expect(input.initial.body).toBe(body);
            return {
              body: article,
              finalUrl: "https://render.example/rooms",
              httpStatus: 200,
              headers: {},
            };
          }),
        () => Effect.succeed(response(body)),
      );
      yield* Effect.gen(function* () {
        const before = yield* f.setup;
        const result = yield* Fiber.join(yield* f.start());
        expect(result).toMatchObject({
          status: "ok",
          rung: "render",
          sourceRung: "render",
          finalUrl: "https://render.example/rooms",
          title: "Rendered rooms",
          nextOffset: 20_000,
          delayMs: 3000,
        });
        const rest = yield* read({
          cacheId: result.cacheId,
          cacheEnvironmentId: result.cacheEnvironmentId,
          offset: result.nextOffset,
        });
        expect(result.text + rest.text).toContain("| Family | 4 |");
        expect(result.text + rest.text).toContain("https://render.example/camere");
        expect(result.text + rest.text).toContain('"numberOfRooms":8');
        expect(rest).toMatchObject({
          rung: "cache",
          sourceRung: "render",
          contentHash: result.contentHash,
        });
        expect(yield* read({ url: "https://render.example/" })).toMatchObject({
          rung: "cache",
          cacheId: result.cacheId,
        });
        expect(renders).toBe(1);
        expect(yield* Queue.size(f.requests)).toBe(0);
        expect(
          yield* Orchestrator.OrchestratorV2.use((o) => o.getThreadProjection(threadId)),
        ).toEqual(before);
      }).pipe(Effect.provide(f.layer));
    }),
);

it.effect("renders only thin/consent pages, preserving explicit HTTP walls and failures", () =>
  Effect.gen(function* () {
    const cases = [
      { body: article, status: 200, expected: "ok" },
      { body: "<h1>Sign in</h1><input type='password'>", status: 200, expected: "login" },
      {
        body: "<title>Just a moment</title>Checking your browser",
        status: 200,
        expected: "challenge",
      },
      {
        body: "<title>502 Bad Gateway</title>Origin is down",
        status: 200,
        expected: "origin_error",
      },
      { body: shell, status: 500, expected: "origin_error" },
    ];
    let renders = 0;
    const f = yield* fixture(
      () =>
        Effect.sync(() => {
          renders++;
          return {
            body: article,
            finalUrl: "https://unexpected.example/",
            httpStatus: 200,
            headers: {},
          };
        }),
      (url) => {
        const c = cases[Number(url.pathname.slice(1))]!;
        return Effect.succeed(response(c.body, c.status));
      },
    );
    yield* Effect.gen(function* () {
      yield* f.setup;
      for (const [i, c] of cases.entries()) {
        const result = yield* Fiber.join(yield* f.start(`https://case${i}.example/${i}`));
        expect(result).toMatchObject({ status: c.expected, rung: "http", sourceRung: "http" });
        if (c.expected !== "ok") expect(result.text).toBe("");
        if (c.expected === "login") expect(result.nextAction).toBe("needs_session");
      }
      expect(renders).toBe(0);
    }).pipe(Effect.provide(f.layer));
  }),
);

it.effect(
  "classifies rendered walls and provider failures instead of returning them as content",
  () =>
    Effect.gen(function* () {
      const cases = [
        { body: shell, status: 200, expected: "thin" },
        {
          body: "<h1>Cookie consent</h1><p>Accept all cookies to continue.</p>",
          status: 200,
          expected: "consent",
        },
        { body: "<h1>Sign in</h1><input type='password'>", status: 200, expected: "login" },
        {
          body: "<title>Just a moment</title>Checking your browser",
          status: 200,
          expected: "challenge",
        },
        {
          body: "<title>502 Bad Gateway</title>Origin is down",
          status: 200,
          expected: "origin_error",
        },
        { body: article, status: 503, expected: "origin_error" },
      ];
      const f = yield* fixture((input) => {
        const c = cases[Number(input.url.pathname.slice(1))];
        return c
          ? Effect.succeed({
              body: c.body,
              finalUrl: input.url.href,
              httpStatus: c.status,
              headers: {},
            })
          : Effect.fail(
              new PublicRender.PublicRenderError({
                status: "provider_error",
                httpStatus: null,
                retryAfterMs: null,
              }),
            );
      });
      yield* Effect.gen(function* () {
        yield* f.setup;
        for (let i = 0; i <= cases.length; i++) {
          const result = yield* Fiber.join(yield* f.start(`https://wall${i}.example/${i}`));
          expect(result).toMatchObject({
            status: cases[i]?.expected ?? "provider_error",
            rung: "render",
            sourceRung: "render",
            text: "",
          });
          if (result.status === "login") expect(result.nextAction).toBe("needs_session");
        }
        expect(f.fallbacks).toEqual([0, 1, 3, 4, 5, 6].map((i) => `https://wall${i}.example/${i}`));
      }).pipe(Effect.provide(f.layer));
    }),
);

it.effect("shares in-flight renders and caps concurrency at two across MCP callers", () =>
  Effect.gen(function* () {
    const entered = yield* Queue.unbounded<string>();
    const release = yield* Deferred.make<void>();
    let active = 0;
    let peak = 0;
    let renders = 0;
    const f = yield* fixture((input) =>
      Effect.gen(function* () {
        active++;
        renders++;
        peak = Math.max(peak, active);
        yield* Queue.offer(entered, input.url.href);
        yield* Deferred.await(release);
        return { body: article, finalUrl: input.url.href, httpStatus: 200, headers: {} };
      }).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            active--;
          }),
        ),
      ),
    );
    yield* Effect.gen(function* () {
      yield* f.setup;
      const a = yield* f.start("https://one.example/");
      expect(yield* Queue.take(entered)).toBe("https://one.example/");
      const duplicate = yield* Effect.forkScoped(read({ url: "https://one.example/" }));
      const b = yield* f.start("https://two.example/");
      expect(yield* Queue.take(entered)).toBe("https://two.example/");
      const c = yield* f.start("https://three.example/");
      expect(active).toBe(2);
      expect(yield* Queue.size(entered)).toBe(0);
      yield* TestClock.adjust(1000);
      yield* Deferred.succeed(release, undefined);
      expect(yield* Queue.take(entered)).toBe("https://three.example/");
      const results = yield* Effect.all([
        Fiber.join(a),
        Fiber.join(duplicate),
        Fiber.join(b),
        Fiber.join(c),
      ]);
      expect(results.every((result) => result.status === "ok")).toBe(true);
      expect(results[1]).toMatchObject({
        rung: "cache",
        cacheId: results[0]!.cacheId,
        sourceRung: "render",
      });
      expect(renders).toBe(3);
      expect(results[3]!.delayMs).toBe(4000);
      expect(peak).toBe(2);
      expect(active).toBe(0);
      expect(yield* Queue.size(f.requests)).toBe(0);
    }).pipe(Effect.provide(f.layer));
  }),
);

it.effect(
  "cancellation closes the render and releases its permit without caching partial output",
  () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const closed = yield* Deferred.make<void>();
      let renders = 0;
      const f = yield* fixture((input) =>
        Effect.gen(function* () {
          renders++;
          if (renders === 1)
            return yield* Deferred.succeed(entered, undefined).pipe(
              Effect.andThen(Effect.never),
              Effect.ensuring(Deferred.succeed(closed, undefined)),
            );
          return { body: article, finalUrl: input.url.href, httpStatus: 200, headers: {} };
        }),
      );
      yield* Effect.gen(function* () {
        yield* f.setup;
        const interrupted = yield* f.start();
        yield* Deferred.await(entered);
        yield* Fiber.interrupt(interrupted);
        yield* Deferred.await(closed);
        const next = yield* Effect.forkScoped(read({ url: "https://render.example/" }));
        yield* TestClock.adjust(3000);
        expect((yield* Queue.take(f.requests)).url).toBe("https://render.example/");
        expect(yield* Fiber.join(next)).toMatchObject({ status: "ok", rung: "render" });
        expect(renders).toBe(2);
      }).pipe(Effect.provide(f.layer));
    }),
);

it.effect("the render deadline includes queuing and closes timed-out work", () =>
  Effect.gen(function* () {
    const entered = yield* Queue.unbounded<void>();
    let active = 0;
    const f = yield* fixture(() =>
      Effect.gen(function* () {
        active++;
        yield* Queue.offer(entered, undefined);
        return yield* Effect.never;
      }).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            active--;
          }),
        ),
      ),
    );
    yield* Effect.gen(function* () {
      yield* f.setup;
      const a = yield* f.start("https://timeout1.example/");
      yield* Queue.take(entered);
      const b = yield* f.start("https://timeout2.example/");
      yield* Queue.take(entered);
      const c = yield* f.start("https://timeout3.example/");
      const queuedAt = yield* Clock.currentTimeMillis;
      yield* TestClock.adjust(20_000);
      const results = yield* Effect.all([Fiber.join(a), Fiber.join(b), Fiber.join(c)]);
      expect(
        results.every((r) => r.status === "timeout" && r.sourceRung === "render" && r.text === ""),
      ).toBe(true);
      expect((yield* Clock.currentTimeMillis) - queuedAt).toBe(20_000);
      expect(active).toBe(0);
      const cached = yield* read({ url: "https://timeout3.example/" });
      expect(cached).toMatchObject({ rung: "cache", status: "timeout", sourceRung: "render" });
      expect(f.fallbacks.toSorted()).toEqual([
        "https://timeout1.example/",
        "https://timeout2.example/",
        "https://timeout3.example/",
      ]);
    }).pipe(Effect.provide(f.layer));
  }),
);

it.effect("render document navigations reuse core robots, pacing and public-address checks", () =>
  Effect.gen(function* () {
    const f = yield* fixture(
      (input) =>
        Effect.gen(function* () {
          const next = yield* input.loadDocument(new URL("/next", input.url));
          return {
            body: next.body,
            finalUrl: "https://render.example/next",
            httpStatus: next.status,
            headers: {},
          };
        }),
      (url) =>
        url.pathname === "/next"
          ? Effect.succeed(response(article))
          : Effect.succeed(response(shell)),
    );
    yield* Effect.gen(function* () {
      yield* f.setup;
      const work = yield* f.start();
      yield* TestClock.adjust(3000);
      const navigation = yield* Queue.take(f.requests);
      expect(navigation).toMatchObject({ url: "https://render.example/next", at: 6000 });
      expect(yield* Fiber.join(work)).toMatchObject({
        status: "ok",
        sourceRung: "render",
        delayMs: 6000,
        finalUrl: navigation.url,
      });
    }).pipe(Effect.provide(f.layer));
  }),
);

it.effect("render navigations cannot bypass robots or introduce unsupported document schemes", () =>
  Effect.gen(function* () {
    const f = yield* fixture(
      (input) =>
        Effect.gen(function* () {
          const target =
            input.url.hostname === "robots.example"
              ? new URL("/denied", input.url)
              : new URL("file:///etc/passwd");
          const document = yield* input.loadDocument(target);
          return {
            body: document.body,
            finalUrl: target.href,
            httpStatus: document.status,
            headers: document.headers,
          };
        }),
      () => Effect.succeed(response(shell)),
      "User-agent: *\nDisallow: /denied",
    );
    yield* Effect.gen(function* () {
      yield* f.setup;
      expect(yield* Fiber.join(yield* f.start("https://robots.example/"))).toMatchObject({
        status: "blocked_robots",
        sourceRung: "render",
        text: "",
      });
      expect(yield* Fiber.join(yield* f.start("https://scheme.example/"))).toMatchObject({
        status: "unsupported",
        sourceRung: "render",
        text: "",
      });
      expect(yield* Queue.size(f.requests)).toBe(0);
    }).pipe(Effect.provide(f.layer));
  }),
);

it.effect(
  "the pinned renderer preserves an HTTP redirect's final error status through MCP",
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
      );
      yield* Effect.gen(function* () {
        const renderer = yield* PublicRender.PublicRender;
        const f = yield* fixture(renderer.render, (url) =>
          Effect.succeed(
            url.pathname === "/"
              ? response("<script>location.href='/it';</script>")
              : url.pathname === "/it"
                ? {
                    ...response("", 302),
                    headers: { "content-type": "text/plain", location: "/it/" },
                  }
                : response(
                    `<h1>Page not found</h1><p>${"Ordinary server response text. ".repeat(10)}</p>`,
                    404,
                  ),
          ),
        );
        yield* Effect.gen(function* () {
          const before = yield* f.setup;
          const work = yield* f.start();
          yield* TestClock.adjust(3000);
          expect((yield* Queue.take(f.requests)).url).toBe("https://render.example/it");
          yield* TestClock.adjust(3000);
          expect((yield* Queue.take(f.requests)).url).toBe("https://render.example/it/");
          const result = yield* Fiber.join(work);
          expect(result).toMatchObject({
            status: "origin_error",
            httpStatus: 404,
            sourceRung: "render",
            rung: "render",
            finalUrl: "https://render.example/it/",
            text: "",
          });
          expect(yield* read({ url: "https://render.example/" })).toMatchObject({
            status: "origin_error",
            rung: "cache",
            httpStatus: 404,
          });
          expect(
            yield* Orchestrator.OrchestratorV2.use((o) => o.getThreadProjection(threadId)),
          ).toEqual(before);
        }).pipe(Effect.provide(f.layer));
      }).pipe(Effect.provide(rendererLayer));
    }),
);

it.effect.each(["continuation", "repeat"] as const)(
  "an ignored child-frame queue refusal preserves the main page's cached %s",
  (followUp) =>
    Effect.gen(function* () {
      let renders = 0;
      const mainArticle = `<h1>Main article</h1><p>${"The main document has guest rooms and a garden for events. ".repeat(500)}</p><p>End marker: cached main document after a refused frame.</p>`;
      const f = yield* fixture(
        (input) =>
          Effect.gen(function* () {
            renders++;
            yield* input.loadDocument(new URL("/embedded-frame", input.url)).pipe(
              Effect.match({
                onFailure: (error) => {
                  expect(error).toMatchObject({
                    status: "challenge",
                    httpStatus: null,
                    retryAfterMs: 120_000,
                    cause: { _tag: "OriginQueueWaitError" },
                  });
                },
                onSuccess: () => {
                  throw new Error("The embedded document must be refused by Retry-After.");
                },
              }),
            );
            return {
              body: mainArticle,
              finalUrl: input.url.href,
              httpStatus: 200,
              headers: {},
            };
          }),
        () =>
          Effect.succeed({
            ...response(shell),
            headers: { "content-type": "text/html", "retry-after": "120" },
          }),
      );
      yield* Effect.gen(function* () {
        const before = yield* f.setup;
        const result = yield* Fiber.join(yield* f.start());
        expect(result).toMatchObject({
          status: "ok",
          rung: "render",
          sourceRung: "render",
          nextOffset: 20_000,
          delayMs: 3000,
        });
        const cached = yield* read(
          followUp === "continuation"
            ? {
                cacheId: result.cacheId,
                cacheEnvironmentId: result.cacheEnvironmentId,
                offset: result.nextOffset,
              }
            : { url: "https://render.example/" },
        );
        expect(cached).toMatchObject({
          status: "ok",
          rung: "cache",
          sourceRung: "render",
          cacheId: result.cacheId,
          contentHash: result.contentHash,
        });
        if (followUp === "continuation") {
          expect(cached.nextOffset).toBeNull();
          expect(result.text + cached.text).toContain(
            "End marker: cached main document after a refused frame.",
          );
        } else expect(cached.text).toBe(result.text);
        expect(renders).toBe(1);
        expect(yield* Queue.size(f.requests)).toBe(0);
        expect(
          yield* Orchestrator.OrchestratorV2.use((o) => o.getThreadProjection(threadId)),
        ).toEqual(before);
      }).pipe(Effect.provide(f.layer));
    }),
);

it.effect("rendered origin queue refusals stay typed and do not poison the page cache", (ctx) =>
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
    );
    yield* Effect.gen(function* () {
      const renderer = yield* PublicRender.PublicRender;
      let documents = 0;
      const f = yield* fixture(renderer.render, (url) =>
        Effect.sync(() => {
          expect(url.href).toBe("https://render.example/");
          documents++;
          return documents === 1
            ? {
                ...response("<script>location.href='/next';</script>"),
                headers: { "content-type": "text/html", "retry-after": "120" },
              }
            : response(article);
        }),
      );
      yield* Effect.gen(function* () {
        const before = yield* f.setup;
        const refused = yield* Fiber.join(yield* f.start());
        expect(refused).toMatchObject({
          status: "challenge",
          rung: "render",
          sourceRung: "render",
          httpStatus: null,
          retryAfterMs: 120_000,
          delayMs: 3000,
          text: "",
        });
        expect(yield* read({ url: "https://render.example/" })).toMatchObject({
          status: "challenge",
          rung: "http",
          retryAfterMs: 120_000,
          text: "",
        });
        expect(yield* Queue.size(f.requests)).toBe(0);
        yield* TestClock.adjust(120_000);
        const retry = yield* Effect.forkScoped(read({ url: "https://render.example/" }));
        expect((yield* Queue.take(f.requests)).url).toBe("https://render.example/");
        expect(yield* Fiber.join(retry)).toMatchObject({ status: "ok", rung: "http" });
        expect(documents).toBe(2);
        expect(yield* read({ url: "https://render.example/" })).toMatchObject({
          status: "ok",
          rung: "cache",
        });
        expect(
          yield* Orchestrator.OrchestratorV2.use((o) => o.getThreadProjection(threadId)),
        ).toEqual(before);
      }).pipe(Effect.provide(f.layer));
    }).pipe(Effect.provide(rendererLayer));
  }),
);

it.effect(
  "render/subpages: DOM links discover rendered children with shared paging and cache",
  (ctx) =>
    Effect.gen(function* () {
      const executable = testChromium;
      if (!executable) return ctx.skip(SKIP_WITHOUT_CHROMIUM);
      yield* Effect.gen(function* () {
        const renderer = yield* PublicRender.PublicRender;
        const root = `<h1>Guest house</h1><p>${"Visit our guest house and its garden. ".repeat(12)}</p><a href='/camere'>Rooms</a>`;
        const f = yield* fixture(renderer.render, (url) =>
          Effect.succeed(response(scriptedPage(url.pathname === "/" ? root : article))),
        );
        yield* Effect.gen(function* () {
          const before = yield* f.setup;
          const work = yield* f.start();
          yield* TestClock.adjust(3000);
          expect((yield* Queue.take(f.requests)).url).toBe("https://render.example/camere");
          const result = yield* Fiber.join(work);
          expect(result).toMatchObject({ status: "ok", rung: "render", sourceRung: "render" });
          expect(result.subpages).toHaveLength(1);
          const child = result.subpages[0]!;
          expect(child).toMatchObject({
            url: "https://render.example/camere",
            status: "ok",
            rung: "render",
            sourceRung: "render",
          });
          expect(result.text.length + child.text.length).toBe(20_000);
          expect(child.nextOffset).not.toBeNull();
          expect(result.truncationReasons).toContain("output_budget");
          const rest = yield* read({
            cacheId: child.cacheId,
            cacheEnvironmentId: child.cacheEnvironmentId,
            offset: child.nextOffset,
          });
          expect(rest).toMatchObject({ rung: "cache", sourceRung: "render", subpages: [] });
          expect(child.text + rest.text).toContain("| Family | 4 |");
          expect(child.text + rest.text).toContain('"numberOfRooms":8');
          const cached = yield* read({ url: "https://render.example/" });
          expect(cached.rung).toBe("cache");
          expect(cached.subpages[0]).toMatchObject({ rung: "cache", sourceRung: "render" });
          expect(yield* Queue.size(f.requests)).toBe(0);
          expect(
            yield* Orchestrator.OrchestratorV2.use((o) => o.getThreadProjection(threadId)),
          ).toEqual(before);
        }).pipe(Effect.provide(f.layer));
      }).pipe(Effect.provide(pinnedRendererLayer(executable)));
    }),
);

it.effect("render/subpages: a child browser navigation cannot leave the root origin", (ctx) =>
  Effect.gen(function* () {
    const executable = testChromium;
    if (!executable) return ctx.skip(SKIP_WITHOUT_CHROMIUM);
    yield* Effect.gen(function* () {
      const renderer = yield* PublicRender.PublicRender;
      const root = `<h1>Guest house</h1><p>${"Visit our guest house and its garden. ".repeat(12)}</p><a href='/camere'>Rooms</a>`;
      const f = yield* fixture(renderer.render, (url) =>
        Effect.succeed(
          response(
            url.pathname === "/"
              ? root
              : "<script>location.href='https://foreign.example/rooms';</script>",
          ),
        ),
      );
      yield* Effect.gen(function* () {
        const before = yield* f.setup;
        const work = yield* f.start();
        yield* TestClock.adjust(3000);
        expect((yield* Queue.take(f.requests)).url).toBe("https://render.example/camere");
        const outcome = yield* Effect.raceFirst(
          Fiber.join(work).pipe(Effect.map((result) => ({ _tag: "Done" as const, result }))),
          Queue.take(f.requests).pipe(
            Effect.map((request) => ({ _tag: "Network" as const, request })),
          ),
        );
        if (outcome._tag === "Network")
          throw new Error(`A restricted render reached ${outcome.request.url}`);
        expect(outcome.result.status).toBe("ok");
        expect(outcome.result.subpages).toHaveLength(1);
        expect(outcome.result.subpages[0]).toMatchObject({
          status: "unsupported",
          sourceRung: "render",
          text: "",
        });
        expect(yield* Queue.size(f.requests)).toBe(0);
        expect(
          yield* Orchestrator.OrchestratorV2.use((o) => o.getThreadProjection(threadId)),
        ).toEqual(before);
      }).pipe(Effect.provide(f.layer));
    }).pipe(Effect.provide(pinnedRendererLayer(executable)));
  }),
);

it.effect(
  "render/subpages: a cached redirect alias keeps rendered provenance and body age",
  (ctx) =>
    Effect.gen(function* () {
      const executable = testChromium;
      if (!executable) return ctx.skip(SKIP_WITHOUT_CHROMIUM);
      yield* Effect.gen(function* () {
        const renderer = yield* PublicRender.PublicRender;
        const releaseAlias = yield* Deferred.make<void>();
        const root = `<h1>Guest house</h1><p>${"Visit our guest house and its garden. ".repeat(12)}</p><a href='/camere'>Rooms</a><a href='/rooms-alias'>Other rooms</a>`;
        const rooms = `<h1>Rooms</h1><p>${"Eight guest rooms surround the garden. ".repeat(20)}</p>`;
        const f = yield* fixture(renderer.render, (url) =>
          url.pathname === "/rooms-alias"
            ? Deferred.await(releaseAlias).pipe(
                Effect.as({
                  ...response("", 302),
                  headers: { "content-type": "text/html", location: "/camere" },
                }),
              )
            : Effect.succeed(response(url.pathname === "/" ? root : scriptedPage(rooms))),
        );
        yield* Effect.gen(function* () {
          const before = yield* f.setup;
          const work = yield* f.start();
          yield* TestClock.adjust(3000);
          expect((yield* Queue.take(f.requests)).url).toBe("https://render.example/camere");
          yield* TestClock.adjust(3000);
          expect((yield* Queue.take(f.requests)).url).toBe("https://render.example/rooms-alias");
          yield* TestClock.adjust(1000);
          yield* Deferred.succeed(releaseAlias, undefined);
          const result = yield* Fiber.join(work);
          expect(result.subpages).toHaveLength(1);
          const child = result.subpages[0]!;
          expect(child.sourceRung).toBe("render");
          const alias = yield* read({
            url: "https://render.example/rooms-alias",
            followSubpages: false,
          });
          expect(alias).toMatchObject({
            rung: "cache",
            sourceRung: "render",
            finalUrl: "https://render.example/camere",
            fetchedAt: child.fetchedAt,
            contentHash: child.contentHash,
            text: child.text,
          });
          expect(alias.fetchedAt).not.toBe(DateTime.formatIso(yield* DateTime.now));
          expect(yield* Queue.size(f.requests)).toBe(0);
          expect(
            yield* Orchestrator.OrchestratorV2.use((o) => o.getThreadProjection(threadId)),
          ).toEqual(before);
        }).pipe(Effect.provide(f.layer));
      }).pipe(Effect.provide(pinnedRendererLayer(executable)));
    }),
);

// Observed public shell: navigation, two headings, and one enable-JavaScript line.
// Extracted text is 115 characters; compact visible text stays above 80.
const javascriptShellHtml = `<html><head><title>JS Rendering Challenge to Learn Web Scraping - ScrapingCourse.com</title></head><body><nav><a href="https://www.scrapingcourse.com/">Scraping Course</a></nav><h1>JS Rendering</h1><h2>Challenge</h2><p>Enable JavaScript to see products</p></body></html>`;
const javascriptShellUrl = "https://www.scrapingcourse.com/javascript-rendering/";
const observedShellText =
  "[Scraping Course](https://www.scrapingcourse.com/)\n\n# JS Rendering\n\n## Challenge\n\nEnable JavaScript to see products";

it("classifies an instructional JavaScript shell as thin once navigation and headings pass 80 characters", () => {
  const page = extract(javascriptShellHtml, javascriptShellUrl);
  expect(page.title).toBe("JS Rendering Challenge to Learn Web Scraping - ScrapingCourse.com");
  expect(page.text).toBe(observedShellText);
  expect(page.text.length).toBe(115);
  expect(page.visibleText.length).toBeGreaterThan(80);
  expect(classify(page, 200)).toBe("thin");
  expect(classify(page, 500)).toBe("origin_error");
  expect(
    classify(
      extract(
        javascriptShellHtml.replace(
          "<title>JS Rendering Challenge to Learn Web Scraping - ScrapingCourse.com</title>",
          "<title>Just a moment</title>",
        ),
        javascriptShellUrl,
      ),
      200,
    ),
  ).toBe("challenge");
});

it("keeps short substantive pages, JSON-LD, and articles that mention JavaScript", () => {
  const nearMiss = extract(
    javascriptShellHtml.replace(
      "<p>Enable JavaScript to see products</p>",
      "<p>Enable JavaScript to see products. The catalog already lists a ceramic mug at 12 euros and a linen towel at 18 euros.</p>",
    ),
    javascriptShellUrl,
  );
  expect(nearMiss.visibleText.length).toBeGreaterThan(80);
  expect(nearMiss.visibleText).toContain("ceramic mug");
  expect(classify(nearMiss, 200)).toBe("ok");

  const article = extract(
    "<html><head><title>Rendering notes</title></head><body><h1>Rendering notes</h1><p>This short article explains that browsers run JavaScript after HTML arrives, and the text is already here.</p></body></html>",
    "https://notes.example/javascript",
  );
  expect(article.visibleText.length).toBeGreaterThan(80);
  expect(article.text).toContain("browsers run JavaScript");
  expect(classify(article, 200)).toBe("ok");

  const structured = extract(
    `<html><head><title>Shop facts</title><script type="application/ld+json">{"name":"Scraping Course","offers":8}</script></head><body>${javascriptShellHtml.match(/<body>(.*)<\/body>/)?.[1] ?? ""}</body></html>`,
    javascriptShellUrl,
  );
  expect(structured.text).toContain("JSON-LD:");
  expect(structured.visibleText).toContain("Enable JavaScript to see products");
  expect(classify(structured, 200)).toBe("ok");

  const factsOnly = extract(
    '<html><head><title>Facts</title><script type="application/ld+json">{"numberOfRooms":8}</script></head><body><p>Eight.</p></body></html>',
    "https://facts.example/",
  );
  expect(factsOnly.visibleText.length).toBeLessThan(80);
  expect(classify(factsOnly, 200)).toBe("ok");
  expect(
    classify(extract("<title>Villa JS</title><div id='root'></div>", "https://app.example/"), 200),
  ).toBe("thin");
});

it("keeps opening hours readable when the enable-JavaScript line only follows them", () => {
  const page = extract(
    "<html><head><title>Hours</title></head><body><h1>Opening hours</h1><p>Monday to Friday, 9:00 to 17:00.</p><h2>Address</h2><p>12 Main Street, Springfield.</p><p>Enable JavaScript to view the map.</p></body></html>",
    "https://hours.example/",
  );
  expect(page.visibleText.length).toBe(123);
  expect(page.text).toContain("Monday to Friday, 9:00 to 17:00.");
  expect(page.text).toContain("12 Main Street, Springfield.");
  expect(classify(page, 200)).toBe("ok");
});

it("keeps product prices readable when the enable-JavaScript line only follows them", () => {
  const page = extract(
    "<html><head><title>Shop</title></head><body><h1>Mug</h1><p>Ceramic mug, 12 euros, ships in two days.</p><h2>Towel</h2><p>Linen towel, 18 euros, ships tomorrow.</p><p>Enable JavaScript to add items to your cart.</p></body></html>",
    "https://shop.example/",
  );
  expect(page.visibleText.length).toBe(140);
  expect(page.text).toContain("Ceramic mug, 12 euros, ships in two days.");
  expect(page.text).toContain("Linen towel, 18 euros, ships tomorrow.");
  expect(classify(page, 200)).toBe("ok");
});

it("keeps a paragraph after the enable-JavaScript line readable", () => {
  const page = extract(
    "<html><head><title>Notes</title></head><body><h1>Notes</h1><p>Enable JavaScript to see products</p><p>The ceramic mug is 12 euros and the linen towel is 18 euros, and both are already listed.</p></body></html>",
    "https://notes.example/after",
  );
  expect(page.visibleText.length).toBe(131);
  expect(page.text).toContain("The ceramic mug is 12 euros and the linen towel is 18 euros");
  expect(classify(page, 200)).toBe("ok");
});

it("does not throw when a text/plain run of nine-word headings reaches the public body limit", () => {
  const heading = "# a b c d e f g h i ";
  const text = heading.repeat(Math.floor((4 * 1024 * 1024) / heading.length));
  expect(text.length).toBeGreaterThan(3_950_000);
  expect(text.length).toBeLessThanOrEqual(4 * 1024 * 1024);
  const page = {
    title: "",
    text,
    visibleText: text,
    password: false,
    refresh: undefined,
    links: [],
  };
  expect(() => classify(page, 200)).not.toThrow();
  expect(classify(page, 200)).toBe("ok");
});

it.effect(
  "renders an instructional JavaScript shell and leaves a substantive JavaScript article on HTTP",
  () =>
    Effect.gen(function* () {
      const products =
        "<html><head><title>Products</title></head><body><h1>Products</h1><p>Ceramic mug, 12 euros. Linen towel, 18 euros. Oak board, 24 euros. The shop lists these three products after scripts run.</p></body></html>";
      const article =
        "<html><head><title>Rendering notes</title></head><body><h1>Rendering notes</h1><p>This short article explains that browsers run JavaScript after HTML arrives, and the text is already here.</p></body></html>";
      let renders = 0;
      const f = yield* fixture(
        (input) =>
          Effect.sync(() => {
            renders++;
            expect(input.url.href).toBe("https://course.example/shell");
            expect(input.initial.body).toBe(javascriptShellHtml);
            return {
              body: products,
              finalUrl: input.url.href,
              httpStatus: 200,
              headers: {},
            };
          }),
        (url) =>
          Effect.succeed(
            response(
              url.hostname === "notes.example"
                ? article
                : url.pathname === "/shell"
                  ? javascriptShellHtml
                  : products,
            ),
          ),
      );
      yield* Effect.gen(function* () {
        yield* f.setup;
        const rendered = yield* Fiber.join(yield* f.start("https://course.example/shell"));
        expect(renders).toBe(1);
        expect(rendered).toMatchObject({
          status: "ok",
          rung: "render",
          sourceRung: "render",
          finalUrl: "https://course.example/shell",
          title: "Products",
        });
        expect(rendered.text).toContain("Ceramic mug, 12 euros.");
        expect(rendered.text).not.toContain("Enable JavaScript to see products");
        const notes = yield* Fiber.join(yield* f.start("https://notes.example/article"));
        expect(notes).toMatchObject({
          status: "ok",
          rung: "http",
          sourceRung: "http",
          title: "Rendering notes",
        });
        expect(notes.text).toContain("browsers run JavaScript");
        expect(renders).toBe(1);
      }).pipe(Effect.provide(f.layer));
    }),
);
