import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import { McpSchema, McpServer } from "effect/ai";

import * as ServerConfig from "../../../config.ts";
import * as WebResearch from "../../../webResearch/WebResearch.ts";
import * as PageCache from "../../../webResearch/PageCache.ts";
import * as PublicHttp from "../../../webResearch/PublicHttp.ts";
import { WebReadResult } from "../../../webResearch/schemas.ts";
import * as McpHttpServer from "../../McpHttpServer.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as McpToolAccessTestkit from "../../McpToolAccess.testkit.ts";

const scope: McpInvocationContext.McpInvocationScope = {
  environmentId: EnvironmentId.make("research-test"),
  requestNamespace: "research-agent",
  thread: {
    threadId: ThreadId.make("research-thread"),
    providerSessionId: "research-session",
    providerInstanceId: ProviderInstanceId.make("codex"),
  },
  client: undefined,
  capabilities: new Set(),
  issuedAt: 0,
};
const client = McpSchema.McpServerClient.of({
  clientId: 1,
  clientCapabilities: {},
  clientInfo: { name: "research-test", version: "1" },
  protocolVersion: "2025-06-18",
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "research-test", version: "1" },
  },
  getClient: Effect.die("unused"),
});
const call = (arguments_: Record<string, unknown>, invocation = scope) =>
  McpServer.McpServer.use((server) =>
    server.callTool({ name: "web_read", arguments: arguments_ }),
  ).pipe(
    Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
    Effect.provideService(McpSchema.McpServerClient, client),
  );
const decodeResult = Schema.decodeUnknownEffect(WebReadResult);
const read = Effect.fnUntraced(function* (input: Record<string, unknown>, invocation = scope) {
  const result = yield* call(input, invocation);
  expect(result.isError, JSON.stringify(result.content)).not.toBe(true);
  return yield* decodeResult(result.structuredContent);
});
const html =
  "<html><head><title>Villa Example</title></head><body><h1>Welcome to Villa Example</h1><p>Our quiet farmhouse has eight guest rooms and a garden for events. Contact the owners for availability and room prices.</p></body></html>";
const response = (body = html, status = 200, headers: Record<string, string> = {}) => ({
  body,
  status,
  headers: { "content-type": "text/html; charset=utf-8", ...headers },
});

const fixture = Effect.fnUntraced(function* (
  respond: PublicHttp.PublicHttp["Service"]["get"] = () => Effect.succeed(response()),
  directory?: string,
  robots: string | PublicHttp.PublicHttp["Service"]["get"] = "User-agent: *\nAllow: /",
  cacheLayer: typeof PageCache.layer = PageCache.layer,
) {
  const requests = yield* Queue.unbounded<{ url: string; at: number }>();
  const http = PublicHttp.PublicHttp.of({
    get: (url) =>
      Effect.gen(function* () {
        yield* Queue.offer(requests, { url: url.href, at: yield* Clock.currentTimeMillis });
        return url.pathname === "/robots.txt"
          ? typeof robots === "string"
            ? response(robots, 200, { "content-type": "text/plain" })
            : yield* robots(url)
          : yield* respond(url);
      }),
  });
  const config = ServerConfig.layerTest(
    process.cwd(),
    directory ?? { prefix: "chromeria-research-" },
  ).pipe(Layer.provide(NodeServices.layer));
  const resources = Layer.mergeAll(config, NodeServices.layer);
  const reader = WebResearch.layer.pipe(
    Layer.provide(cacheLayer),
    Layer.provide(Layer.succeed(PublicHttp.PublicHttp, http)),
    Layer.provide(resources),
  );
  const layer = McpHttpServer.layerWebResearchToolkit.pipe(
    Layer.provideMerge(McpServer.McpServer.layer),
    Layer.provide(reader),
    Layer.provide(McpToolAccessTestkit.liveThreadsLayer),
  );
  return { layer, requests };
});

// Advance only after a recorded network milestone, never by waiting for wall time.
const finish = Effect.fnUntraced(function* <A, E, R>(
  work: Effect.Effect<A, E, R>,
  requests: Queue.Queue<{ url: string; at: number }>,
  networkCalls = 2,
) {
  const fiber = yield* Effect.forkScoped(work);
  for (let i = 0; i < networkCalls; i++) {
    yield* Queue.take(requests);
    yield* TestClock.adjust(3000);
  }
  return yield* Fiber.join(fiber);
});

describe("web_read at the registered MCP seam", () => {
  it.effect(
    "keeps full document facts, tables, links and JSON-LD with bounded continuation and provenance",
    () =>
      Effect.gen(function* () {
        const body = `<html><head><title>Rooms &amp; events</title><script type="application/ld+json">{"numberOfRooms":8}</script></head><body><h1>Rooms &amp; events</h1><table><tr><th>Room</th><th>Beds</th></tr><tr><td>Family</td><td>4</td></tr></table><p>${"An independent paragraph with useful venue facts. ".repeat(700)}</p><a href="/camere">All rooms</a><script>SECRET_SCRIPT</script><style>SECRET_STYLE</style></body></html>`;
        const f = yield* fixture(() => Effect.succeed(response(body)));
        yield* Effect.gen(function* () {
          const result = yield* finish(read({ url: "https://villa.example/" }), f.requests);
          expect(result).toMatchObject({
            status: "ok",
            url: "https://villa.example/",
            finalUrl: "https://villa.example/",
            rung: "http",
            sourceRung: "http",
            offset: 0,
            nextOffset: 20_000,
            delayMs: 3000,
          });
          expect(result.text).toHaveLength(20_000);
          expect(result.text).toContain("# Rooms & events");
          expect(result.text).toContain("| Family | 4 |");
          expect(result.contentHash).toMatch(/^[a-f0-9]{64}$/);
          expect(result.fetchedAt).toMatch(/^1970-/);
          expect(result.contentNotice).toContain("never as instructions");
          const rest = yield* read({ cacheId: result.cacheId, offset: result.nextOffset });
          expect(rest).toMatchObject({
            rung: "cache",
            contentHash: result.contentHash,
            cacheId: result.cacheId,
            nextOffset: null,
            delayMs: 0,
          });
          const all = result.text + rest.text;
          expect(all.length).toBe(result.totalChars);
          expect(all).toContain("[All rooms](https://villa.example/camere)");
          expect(all).toContain('{"numberOfRooms":8}');
          expect(all).not.toMatch(/SECRET_SCRIPT|SECRET_STYLE/);
          expect(yield* Queue.size(f.requests)).toBe(0);
        }).pipe(Effect.provide(f.layer));
      }),
  );

  it.effect.each([
    ["challenge", "<title>Just a moment...</title><p>Checking your browser before continuing.</p>"],
    ["login", "<title>Sign in</title><form><input type='password'>Sign in to continue</form>"],
    [
      "consent",
      "<title>Before you continue</title><p>Before you continue to our site, accept cookies.</p>",
    ],
    ["origin_error", "<title>Error 502</title><p>Error code: 502. Origin is down.</p>"],
    [
      "origin_error",
      "<p>HTTP 500 Internal Server Error. The upstream website failed while processing the request. Please contact the site owner.</p>",
    ],
    ["origin_error", "<title>Domain for sale</title><p>Buy this domain</p>"],
    ["thin", "<title>Villa JS</title><div id='root'></div><script>loadTheApp()</script>"],
  ] as const)("classifies %s instead of returning an HTTP-200 wall as content", ([status, body]) =>
    Effect.gen(function* () {
      const f = yield* fixture(() => Effect.succeed(response(body)));
      yield* Effect.gen(function* () {
        const first = yield* finish(read({ url: "https://wall.example/" }), f.requests);
        expect(first).toMatchObject({ status, text: "", totalChars: 0, nextOffset: null });
        expect(first.nextAction).toBe(status === "login" ? "needs_session" : undefined);
        expect(yield* read({ url: "https://wall.example/" })).toMatchObject({
          status,
          rung: "cache",
          text: "",
        });
      }).pipe(Effect.provide(f.layer));
    }),
  );

  it.effect("follows redirects and quoted meta refresh without losing final URL provenance", () =>
    Effect.gen(function* () {
      const f = yield* fixture((url) =>
        Effect.succeed(
          url.pathname === "/start"
            ? response("", 302, { location: "/refresh" })
            : url.pathname === "/refresh"
              ? response('<meta http-equiv="refresh" content="0; URL=\'/rooms\'">')
              : response(),
        ),
      );
      yield* Effect.gen(function* () {
        const result = yield* finish(
          read({ url: "https://redirect.example/start" }),
          f.requests,
          4,
        );
        expect(result).toMatchObject({
          status: "ok",
          url: "https://redirect.example/start",
          finalUrl: "https://redirect.example/rooms",
          delayMs: 9000,
        });
        expect(result.text).toContain("eight guest rooms");
      }).pipe(Effect.provide(f.layer));
    }),
  );

  it.effect("does not mistake an ordinary login link and cookie footer for a wall", () =>
    Effect.gen(function* () {
      const f = yield* fixture(() =>
        Effect.succeed(
          response(
            html.replace(
              "</body>",
              '<footer><a href="/login">Login</a><a href="/cookies">Accept cookies</a></footer></body>',
            ),
          ),
        ),
      );
      yield* Effect.gen(function* () {
        expect((yield* finish(read({ url: "https://footer.example/" }), f.requests)).status).toBe(
          "ok",
        );
      }).pipe(Effect.provide(f.layer));
    }),
  );

  it.effect("ignores hidden login forms when classifying a readable page", () =>
    Effect.gen(function* () {
      const f = yield* fixture(() =>
        Effect.succeed(
          response(
            html.replace(
              "</body>",
              "<form hidden><input type='password'>Log in to continue</form></body>",
            ),
          ),
        ),
      );
      yield* Effect.gen(function* () {
        const result = yield* finish(read({ url: "https://hidden-login.example/" }), f.requests);
        expect(result.status).toBe("ok");
        expect(result.text).toContain("eight guest rooms");
      }).pipe(Effect.provide(f.layer));
    }),
  );

  it.effect("keeps the document title when an SVG icon is titled Login", () =>
    Effect.gen(function* () {
      const f = yield* fixture(() =>
        Effect.succeed(response(html.replace("</body>", "<svg><title>Login</title></svg></body>"))),
      );
      yield* Effect.gen(function* () {
        const result = yield* finish(read({ url: "https://svg.example/" }), f.requests);
        expect(result).toMatchObject({ status: "ok", title: "Villa Example" });
      }).pipe(Effect.provide(f.layer));
    }),
  );

  it.effect("preserves contact details behind mail and phone links", () =>
    Effect.gen(function* () {
      const f = yield* fixture(() =>
        Effect.succeed(
          response(
            html.replace(
              "</body>",
              '<a href="mailto:bookings@villa.example">Write to us</a><a href="tel:+390612345678">Call us</a></body>',
            ),
          ),
        ),
      );
      yield* Effect.gen(function* () {
        const result = yield* finish(read({ url: "https://contact.example/" }), f.requests);
        expect(result.text).toContain("[Write to us](mailto:bookings@villa.example)");
        expect(result.text).toContain("[Call us](tel:+390612345678)");
      }).pipe(Effect.provide(f.layer));
    }),
  );

  it.effect("classifies a JavaScript shell with a noscript tracker as thin", () =>
    Effect.gen(function* () {
      const body =
        '<title>Villa JS</title><div id="root"></div><noscript><iframe src="https://www.googletagmanager.com/ns.html?id=GTM-EXAMPLE" height="0" width="0" style="display:none;visibility:hidden"></iframe></noscript>';
      const f = yield* fixture(() => Effect.succeed(response(body)));
      yield* Effect.gen(function* () {
        expect(yield* finish(read({ url: "https://noscript.example/" }), f.requests)).toMatchObject(
          {
            status: "thin",
            text: "",
            totalChars: 0,
          },
        );
      }).pipe(Effect.provide(f.layer));
    }),
  );

  it.effect.each(["0", "600"])(
    "preserves readable content with a %s-second self refresh",
    (delay) =>
      Effect.gen(function* () {
        const f = yield* fixture(() =>
          Effect.succeed(
            response(
              html.replace(
                "</head>",
                `<meta http-equiv="refresh" content="${delay};url=/"></head>`,
              ),
            ),
          ),
        );
        yield* Effect.gen(function* () {
          const result = yield* finish(read({ url: "https://refresh.example/" }), f.requests);
          expect(result.status).toBe("ok");
          expect(result.text).toContain("eight guest rooms");
          expect(yield* Queue.size(f.requests)).toBe(0);
        }).pipe(Effect.provide(f.layer));
      }),
  );

  it.effect("ignores a delayed refresh to a different page", () =>
    Effect.gen(function* () {
      const f = yield* fixture((url) =>
        url.pathname === "/"
          ? Effect.succeed(
              response(
                html.replace(
                  "</head>",
                  '<meta http-equiv="refresh" content="600;url=/later"></head>',
                ),
              ),
            )
          : Effect.die("A delayed refresh must not replace the requested content."),
      );
      yield* Effect.gen(function* () {
        const result = yield* finish(read({ url: "https://refresh.example/" }), f.requests);
        expect(result).toMatchObject({ status: "ok", finalUrl: "https://refresh.example/" });
      }).pipe(Effect.provide(f.layer));
    }),
  );

  it.effect("rejects invalid requests and non-thread clients before any network call", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* Effect.gen(function* () {
        for (const input of [
          {},
          { url: "file:///etc/passwd" },
          { url: "http://user:secret@example.com/" },
          { url: "https://example.com/", cacheId: "a".repeat(64) },
          { url: "https://example.com/", maxChars: 20_001 },
          { cacheId: "../secrets" },
        ]) {
          const exit = yield* Effect.exit(call(input));
          expect(
            Exit.isFailure(exit) || (Exit.isSuccess(exit) && exit.value.isError === true),
          ).toBe(true);
        }
        const denied = yield* call(
          { url: "https://example.com/" },
          {
            ...scope,
            thread: undefined,
            client: { sessionId: "outside", label: "Outside", access: "read-only" },
          },
        );
        expect(denied.isError).toBe(true);
        expect(yield* Queue.size(f.requests)).toBe(0);
      }).pipe(Effect.provide(f.layer));
    }),
  );

  it.effect("shares an in-flight fetch across callers and preserves the cache hash", () =>
    Effect.gen(function* () {
      const release = yield* Deferred.make<void>();
      const f = yield* fixture(() => Deferred.await(release).pipe(Effect.as(response())));
      yield* Effect.gen(function* () {
        const first = yield* Effect.forkScoped(
          read({ url: "https://shared.example/", maxAgeHours: 0 }),
        );
        yield* Queue.take(f.requests); // robots
        yield* TestClock.adjust(3000);
        yield* Queue.take(f.requests); // held page request
        const second = yield* Effect.forkScoped(
          read(
            { url: "https://shared.example/" },
            {
              ...scope,
              requestNamespace: "second-agent",
              thread: { ...scope.thread!, threadId: ThreadId.make("second-thread") },
            },
          ),
        );
        yield* Deferred.succeed(release, undefined);
        const a = yield* Fiber.join(first);
        const b = yield* Fiber.join(second);
        expect(a.rung).toBe("http");
        expect(b).toMatchObject({ rung: "cache", cacheId: a.cacheId, contentHash: a.contentHash });
        expect(yield* Queue.size(f.requests)).toBe(0);
      }).pipe(Effect.provide(f.layer));
    }),
  );

  it.effect("serializes distinct URLs on an origin and reports spacing", () =>
    Effect.gen(function* () {
      const release = yield* Deferred.make<void>();
      let active = 0;
      let peak = 0;
      const f = yield* fixture((url) =>
        Effect.gen(function* () {
          active++;
          peak = Math.max(peak, active);
          if (url.pathname === "/a") yield* Deferred.await(release);
          active--;
          return response();
        }),
      );
      yield* Effect.gen(function* () {
        const first = yield* Effect.forkScoped(read({ url: "https://limit.example/a" }));
        yield* Queue.take(f.requests);
        yield* TestClock.adjust(3000);
        const a = yield* Queue.take(f.requests);
        const second = yield* Effect.forkScoped(read({ url: "https://limit.example/b" }));
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(first);
        yield* TestClock.adjust(3000);
        const b = yield* Queue.take(f.requests);
        const result = yield* Fiber.join(second);
        expect(peak).toBe(1);
        expect(b.at - a.at).toBeGreaterThanOrEqual(3000);
        expect(result.delayMs).toBeGreaterThanOrEqual(3000);
      }).pipe(Effect.provide(f.layer));
    }),
  );

  it.effect("keeps a shared fetch alive when its first reader cancels", () =>
    Effect.gen(function* () {
      const release = yield* Deferred.make<void>();
      // Synchronous empty storage isolates cancellation after both MCP readers join the fetch.
      const memoryCache = Layer.succeed(
        PageCache.PageCache,
        PageCache.PageCache.of({
          get: () => Effect.succeed(null),
          put: () => Effect.void,
        }),
      );
      const f = yield* fixture(
        () => Deferred.await(release).pipe(Effect.as(response())),
        undefined,
        undefined,
        memoryCache,
      );
      yield* Effect.gen(function* () {
        const first = yield* Effect.forkScoped(read({ url: "https://cancel.example/" }));
        yield* Queue.take(f.requests);
        yield* TestClock.adjust(3000);
        yield* Queue.take(f.requests);
        const second = yield* Effect.forkScoped(read({ url: "https://cancel.example/" }));
        yield* TestClock.adjust(0); // Drain runnable readers until both wait on the held page.
        yield* Fiber.interrupt(first);
        yield* Deferred.succeed(release, undefined);
        const result = yield* Fiber.join(second);
        expect(result).toMatchObject({ status: "ok", rung: "cache", delayMs: 3000 });
        expect(result.text).toContain("eight guest rooms");
        expect(yield* Queue.size(f.requests)).toBe(0);
      }).pipe(Effect.provide(f.layer));
    }),
  );

  it.effect("cancels abandoned fetches and lets a later read start again", () =>
    Effect.gen(function* () {
      const stopped = yield* Deferred.make<void>();
      let attempts = 0;
      const f = yield* fixture(() =>
        ++attempts === 1
          ? Effect.never.pipe(Effect.ensuring(Deferred.succeed(stopped, undefined)))
          : Effect.succeed(response()),
      );
      yield* Effect.gen(function* () {
        const first = yield* Effect.forkScoped(read({ url: "https://abandoned.example/" }));
        yield* Queue.take(f.requests);
        yield* TestClock.adjust(3000);
        yield* Queue.take(f.requests);
        yield* Fiber.interrupt(first);
        yield* Deferred.await(stopped);
        const second = yield* Effect.forkScoped(read({ url: "https://abandoned.example/" }));
        yield* TestClock.adjust(3000);
        yield* Queue.take(f.requests);
        expect((yield* Fiber.join(second)).status).toBe("ok");
        expect(attempts).toBe(2);
        expect(yield* Queue.size(f.requests)).toBe(0);
      }).pipe(Effect.provide(f.layer));
    }),
  );

  it.effect.each([{}, { location: "/robots.txt" }])(
    "stops a robots redirect without a new target: %j",
    (headers) =>
      Effect.gen(function* () {
        let calls = 0;
        const f = yield* fixture(undefined, undefined, () =>
          ++calls === 1
            ? Effect.succeed(response("", 302, headers))
            : Effect.die("The same robots redirect must not be requested twice."),
        );
        yield* Effect.gen(function* () {
          const result = yield* finish(
            read({ url: "https://robots-loop.example/" }),
            f.requests,
            1,
          );
          expect(result).toMatchObject({ status: "provider_error", text: "" });
          expect(yield* Queue.size(f.requests)).toBe(0);
        }).pipe(Effect.provide(f.layer));
      }),
  );

  it.effect("honours a shorter freshness request and immutable cache continuation", () =>
    Effect.gen(function* () {
      let body = html;
      const f = yield* fixture(() => Effect.succeed(response(body)));
      yield* Effect.gen(function* () {
        const first = yield* finish(read({ url: "https://fresh.example/" }), f.requests);
        yield* TestClock.adjust("2 hours");
        expect((yield* read({ url: "https://fresh.example/" })).cacheId).toBe(first.cacheId);
        body = html.replace("eight guest rooms", "twelve guest rooms");
        const next = yield* finish(
          read({ url: "https://fresh.example/", maxAgeHours: 1 }),
          f.requests,
          2,
        ); // robots expired too
        expect(next.rung).toBe("http");
        expect(next.contentHash).not.toBe(first.contentHash);
        expect((yield* read({ cacheId: first.cacheId })).text).toContain("eight guest rooms");
        expect((yield* call({ cacheId: first.cacheId, maxAgeHours: 1 })).isError).toBe(true);
      }).pipe(Effect.provide(f.layer));
    }),
  );

  it.effect("returns a typed timeout without sleeps or a successful cached body", () =>
    Effect.gen(function* () {
      const f = yield* fixture(() => Effect.never);
      yield* Effect.gen(function* () {
        const fiber = yield* Effect.forkScoped(read({ url: "https://timeout.example/" }));
        yield* Queue.take(f.requests);
        yield* TestClock.adjust(3000);
        yield* Queue.take(f.requests);
        yield* TestClock.adjust("10 seconds");
        expect(yield* Fiber.join(fiber)).toMatchObject({ status: "timeout", text: "" });
        expect(yield* read({ url: "https://timeout.example/" })).toMatchObject({
          status: "timeout",
          rung: "cache",
        });
      }).pipe(Effect.provide(f.layer));
    }),
  );

  it.effect("honours robots exclusions, allow precedence and crawl delay", () =>
    Effect.gen(function* () {
      const f = yield* fixture(
        undefined,
        undefined,
        "User-agent: *\nDisallow: /private\nAllow: /private/public\nCrawl-delay: 8",
      );
      yield* Effect.gen(function* () {
        const denied = yield* finish(
          read({ url: "https://robots.example/private" }),
          f.requests,
          1,
        );
        expect(denied).toMatchObject({ status: "blocked_robots", text: "" });
        const fiber = yield* Effect.forkScoped(
          read({ url: "https://robots.example/private/public" }),
        );
        yield* TestClock.adjust(8000);
        const requested = yield* Queue.take(f.requests);
        expect(requested.url).toBe("https://robots.example/private/public");
        expect((yield* Fiber.join(fiber)).status).toBe("ok");
      }).pipe(Effect.provide(f.layer));
    }),
  );

  it.effect("returns a typed failure for excessively deep untrusted markup", () =>
    Effect.gen(function* () {
      const f = yield* fixture(() =>
        Effect.succeed(response(`${"<div>".repeat(5000)}${html}${"</div>".repeat(5000)}`)),
      );
      yield* Effect.gen(function* () {
        const result = yield* finish(read({ url: "https://deep.example/" }), f.requests);
        expect(result).toMatchObject({ status: "provider_error", text: "" });
      }).pipe(Effect.provide(f.layer));
    }),
  );

  it.effect("honours Retry-After and exposes a 429 origin circuit to other URLs", () =>
    Effect.gen(function* () {
      const f = yield* fixture(() =>
        Effect.succeed(response("Too many requests", 429, { "retry-after": "120" })),
      );
      yield* Effect.gen(function* () {
        const first = yield* finish(read({ url: "https://retry.example/one" }), f.requests);
        expect(first).toMatchObject({ status: "challenge", retryAfterMs: 120_000 });
        const blocked = yield* read({ url: "https://retry.example/two" });
        expect(blocked).toMatchObject({ status: "challenge", text: "" });
        expect(blocked.retryAfterMs).toBeGreaterThan(110_000);
        expect(yield* Queue.size(f.requests)).toBe(0);
        yield* TestClock.adjust("2 minutes");
        const again = yield* finish(
          read({ url: "https://retry.example/two", maxAgeHours: 0 }),
          f.requests,
          1,
        );
        expect(again.httpStatus).toBe(429);
      }).pipe(Effect.provide(f.layer));
    }),
  );

  it.effect("keeps local-network refusal typed after a public redirect", () =>
    Effect.gen(function* () {
      const f = yield* fixture(
        () => Effect.succeed(response("", 302, { location: "http://127.0.0.1/admin" })),
        undefined,
        (url) =>
          url.hostname === "127.0.0.1"
            ? Effect.fail(new PublicHttp.PublicNetworkBlockedError({ host: url.hostname }))
            : Effect.succeed(response("User-agent: *\nAllow: /")),
      );
      yield* Effect.gen(function* () {
        const result = yield* finish(read({ url: "https://public.example/" }), f.requests, 3);
        expect(result).toMatchObject({
          status: "blocked_local",
          text: "",
          finalUrl: "http://127.0.0.1/admin",
        });
      }).pipe(Effect.provide(f.layer));
    }),
  );

  it.effect(
    "persists the compressed cache outside the state DB and reopens it without network",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "research-restart-" });
        const first = yield* fixture(undefined, directory);
        const saved = yield* Effect.scoped(
          finish(read({ url: "https://restart.example/" }), first.requests).pipe(
            Effect.provide(first.layer),
          ),
        );
        const second = yield* fixture(
          () => Effect.die("A restart cache hit must not fetch."),
          directory,
        );
        const restored = yield* Effect.scoped(
          read({ cacheId: saved.cacheId }).pipe(Effect.provide(second.layer)),
        );
        expect(restored).toMatchObject({
          rung: "cache",
          contentHash: saved.contentHash,
          text: saved.text,
        });
        expect(yield* Queue.size(second.requests)).toBe(0);
      }).pipe(Effect.provide(NodeServices.layer)),
  );
});
