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
  const observed: Array<{ url: string; at: number }> = [];
  const http = PublicHttp.PublicHttp.of({
    get: (url) =>
      Effect.gen(function* () {
        const request = { url: url.href, at: yield* Clock.currentTimeMillis };
        observed.push(request);
        yield* Queue.offer(requests, request);
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
  return { layer, requests, observed };
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
  it.effect("includes a matching depth-one subpage with its own provenance by default", () =>
    Effect.gen(function* () {
      const f = yield* fixture((url) =>
        Effect.succeed(
          response(
            url.pathname === "/"
              ? html.replace(
                  "</body>",
                  `<a href="/camere#one">Camere</a>
                  <a href="https://SUBPAGES.example:443/a/../camere#two">Duplicate</a>
                  <a href="/eventi">Eventi</a><a href="/#rooms">Rooms home</a>
                  <a href="/about">About us</a></body>`,
                )
              : html
                  .replace("eight guest rooms", "twelve guest rooms")
                  .replace(
                    "</body>",
                    '<a href="/prezzi">Prices at depth two</a><a href="/">Home</a></body>',
                  ),
          ),
        ),
      );
      yield* Effect.gen(function* () {
        const result = yield* finish(read({ url: "https://subpages.example/" }), f.requests, 4);
        expect(result).toMatchObject({
          status: "ok",
          subpages: [
            {
              url: "https://subpages.example/camere",
              finalUrl: "https://subpages.example/camere",
              status: "ok",
              rung: "http",
              sourceRung: "http",
            },
            { url: "https://subpages.example/eventi", status: "ok" },
          ],
          truncated: false,
          pendingSubpages: [],
          unvisitedSubpages: 0,
        });
        for (const child of result.subpages) {
          expect(child.text).toContain("twelve guest rooms");
          expect(child.fetchedAt).toMatch(/^1970-/);
          expect(child.cacheId).toMatch(/^[a-f0-9]{64}$/);
          expect(child.contentHash).not.toBe(result.contentHash);
          expect(child.cacheId).not.toBe(result.cacheId);
        }
        expect(f.observed.map((request) => request.url)).toEqual([
          "https://subpages.example/robots.txt",
          "https://subpages.example/",
          "https://subpages.example/camere",
          "https://subpages.example/eventi",
        ]);
        expect(
          f.observed.slice(1).every((request, i) => request.at - f.observed[i]!.at >= 3000),
        ).toBe(true);
        const cached = yield* read({ url: "https://subpages.example/" });
        expect(cached.rung).toBe("cache");
        expect(cached.subpages.map((page) => page.cacheId)).toEqual(
          result.subpages.map((page) => page.cacheId),
        );
        expect(cached.subpages.every((page) => page.rung === "cache" && page.delayMs === 0)).toBe(
          true,
        );
        expect(f.observed).toHaveLength(4);
      }).pipe(Effect.provide(f.layer));
    }),
  );

  it.effect(
    "disables following and overrides matching without evaluating regular expressions",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture((url) =>
          Effect.succeed(
            response(
              url.pathname === "/"
                ? html.replace(
                    "</body>",
                    '<a href="/rooms">Rooms</a><a href="/conference">Conference</a></body>',
                  )
                : html,
            ),
          ),
        );
        yield* Effect.gen(function* () {
          const single = yield* finish(
            read({ url: "https://patterns.example/", followSubpages: false }),
            f.requests,
          );
          expect(single.subpages).toEqual([]);
          const custom = yield* finish(
            read({ url: "https://patterns.example/", subpagePattern: "Conference|rates" }),
            f.requests,
            1,
          );
          expect(custom.subpages.map((page) => page.url)).toEqual([
            "https://patterns.example/conference",
          ]);
          const literal = yield* read({ url: "https://patterns.example/", subpagePattern: ".*" });
          expect(literal.subpages).toEqual([]);
          const rooms = yield* finish(
            read({ url: "https://patterns.example/", subpagePattern: "ROOMS" }),
            f.requests,
            1,
          );
          expect(rooms.subpages.map((page) => page.url)).toEqual([
            "https://patterns.example/rooms",
          ]);
          expect(f.observed).toHaveLength(4);
        }).pipe(Effect.provide(f.layer));
      }),
  );

  it.effect("bounds traversal to six pages by default and reports pending page continuation", () =>
    Effect.gen(function* () {
      const f = yield* fixture((url) =>
        Effect.succeed(
          response(
            url.pathname === "/"
              ? html.replace(
                  "</body>",
                  Array.from(
                    { length: 9 },
                    (_, i) => `<a href="/rooms-${i + 1}">Rooms ${i + 1}</a>`,
                  ).join("") + "</body>",
                )
              : html,
          ),
        ),
      );
      yield* Effect.gen(function* () {
        const result = yield* finish(read({ url: "https://pages.example/" }), f.requests, 8);
        expect(result.subpages).toHaveLength(6);
        expect(result).toMatchObject({
          truncated: true,
          truncationReasons: ["page_budget"],
          unvisitedSubpages: 3,
          pendingSubpages: [
            "https://pages.example/rooms-7",
            "https://pages.example/rooms-8",
            "https://pages.example/rooms-9",
          ],
        });
        expect(f.observed).toHaveLength(8);
        const smaller = yield* read({ url: "https://pages.example/", maxSubpages: 1 });
        expect(smaller.subpages).toHaveLength(1);
        expect(smaller.unvisitedSubpages).toBe(8);
        expect(smaller.pendingSubpages).toHaveLength(6);
        expect(smaller.truncationReasons).toContain("page_budget");
        expect(f.observed).toHaveLength(8);
      }).pipe(Effect.provide(f.layer));
    }),
  );

  it.effect(
    "shares the output budget across pages and continues text and unfetched URLs separately",
    () =>
      Effect.gen(function* () {
        const fullText = "Room facts retained for continuation. ".repeat(50).trim();
        const f = yield* fixture((url) =>
          Effect.succeed(
            url.pathname === "/"
              ? response(
                  html.replace(
                    "</body>",
                    '<a href="/rooms">Rooms</a><a href="/eventi">Events</a></body>',
                  ),
                )
              : response(fullText, 200, { "content-type": "text/plain" }),
          ),
        );
        yield* Effect.gen(function* () {
          const result = yield* finish(
            read({ url: "https://output.example/", maxChars: 400 }),
            f.requests,
            3,
          );
          const child = result.subpages[0]!;
          expect(
            result.text.length + result.subpages.reduce((sum, page) => sum + page.text.length, 0),
          ).toBe(400);
          expect(child.nextOffset).toBeGreaterThan(0);
          expect(result).toMatchObject({
            truncated: true,
            truncationReasons: ["output_budget"],
            pendingSubpages: ["https://output.example/eventi"],
            unvisitedSubpages: 1,
          });
          const rest = yield* read({ cacheId: child.cacheId, offset: child.nextOffset });
          expect(child.text + rest.text).toBe(fullText);
          expect(rest).toMatchObject({ rung: "cache", nextOffset: null, subpages: [] });
          expect(f.observed).toHaveLength(3);
          const pending = yield* finish(
            read({ url: result.pendingSubpages[0], followSubpages: false }),
            f.requests,
            1,
          );
          expect(pending.text).toBe(fullText);
          expect(f.observed).toHaveLength(4);
        }).pipe(Effect.provide(f.layer));
      }),
  );

  it.effect("returns completed pages at the total deadline and cancels unfinished traversal", () =>
    Effect.gen(function* () {
      const interrupted = yield* Deferred.make<void>();
      const f = yield* fixture(
        (url) =>
          url.pathname === "/rooms-slow"
            ? Effect.never.pipe(Effect.onInterrupt(() => Deferred.succeed(interrupted, undefined)))
            : Effect.succeed(
                response(
                  url.pathname === "/"
                    ? html.replace(
                        "</body>",
                        '<a href="/rooms">Rooms</a><a href="/rooms-slow">Slow rooms</a><a href="/eventi">Events</a></body>',
                      )
                    : html,
                ),
              ),
        undefined,
        "User-agent: *\nAllow: /\nCrawl-delay: 8",
      );
      yield* Effect.gen(function* () {
        const startedAt = yield* Clock.currentTimeMillis;
        const fiber = yield* Effect.forkScoped(read({ url: "https://deadline.example/" }));
        expect((yield* Queue.take(f.requests)).url).toBe("https://deadline.example/robots.txt");
        for (const path of ["/", "/rooms", "/rooms-slow"]) {
          yield* TestClock.adjust(8000);
          expect((yield* Queue.take(f.requests)).url).toBe(`https://deadline.example${path}`);
        }
        yield* TestClock.adjust(6000);
        const result = yield* Fiber.join(fiber);
        yield* Deferred.await(interrupted);
        expect((yield* Clock.currentTimeMillis) - startedAt).toBe(30_000);
        expect(result).toMatchObject({
          status: "ok",
          subpages: [{ url: "https://deadline.example/rooms", status: "ok" }],
          truncated: true,
          truncationReasons: ["time_budget"],
          pendingSubpages: [
            "https://deadline.example/rooms-slow",
            "https://deadline.example/eventi",
          ],
          unvisitedSubpages: 2,
        });
        expect(f.observed).toHaveLength(4);
      }).pipe(Effect.provide(f.layer));
    }),
  );

  it.effect("retains successful pages and independent failure provenance", () =>
    Effect.gen(function* () {
      const f = yield* fixture((url) =>
        Effect.succeed(
          url.pathname === "/"
            ? response(
                html.replace(
                  "</body>",
                  '<a href="/rooms-error">Rooms</a><a href="/eventi">Events</a><a href="/rooms-login">Members</a></body>',
                ),
              )
            : url.pathname === "/rooms-error"
              ? response("Origin failed", 500)
              : url.pathname === "/rooms-login"
                ? response("<title>Sign in</title><input type='password'>")
                : response(),
        ),
      );
      yield* Effect.gen(function* () {
        const result = yield* finish(read({ url: "https://partial.example/" }), f.requests, 5);
        expect(result.status).toBe("ok");
        expect(result.subpages.map((page) => page.status)).toEqual(["origin_error", "ok", "login"]);
        expect(result.subpages[0]).toMatchObject({
          url: "https://partial.example/rooms-error",
          httpStatus: 500,
          text: "",
        });
        expect(result.subpages[1]!.text).toContain("eight guest rooms");
        expect(result.subpages[2]).toMatchObject({ nextAction: "needs_session", text: "" });
        expect(new Set([result.cacheId, ...result.subpages.map((page) => page.cacheId)]).size).toBe(
          4,
        );
        expect(result.truncated).toBe(false);
        expect(f.observed).toHaveLength(5);
      }).pipe(Effect.provide(f.layer));
    }),
  );

  it.effect(
    "refuses external and private links and subpage redirects while using public-network validation",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture((url) =>
          url.pathname === "/camere-rebound"
            ? Effect.fail(new PublicHttp.PublicNetworkBlockedError({ host: url.hostname }))
            : Effect.succeed(
                url.pathname === "/"
                  ? response(
                      html.replace(
                        "</body>",
                        `<a href="https://outside.example/rooms">Rooms</a>
              <a href="http://127.0.0.1/rooms">Rooms</a><a href="http://192.168.1.1/rooms">Rooms</a>
              <a href="http://[::1]/rooms">Rooms</a><a href="http://100.64.0.1/rooms">Rooms</a>
              <a href="https://user:secret@safe.example/rooms">Rooms</a><a href="javascript:rooms()">Rooms</a>
              <a href="/rooms-external">Rooms</a><a href="/eventi-private">Events</a>
              <a href="/camere-rebound">Camere</a><a href="/alloggi">Alloggi</a></body>`,
                      ),
                    )
                  : url.pathname === "/rooms-external"
                    ? response("", 302, { location: "https://outside.example/rooms" })
                    : url.pathname === "/eventi-private"
                      ? response(
                          '<meta http-equiv="refresh" content="0; url=http://127.0.0.1/rooms">',
                        )
                      : response(),
              ),
        );
        yield* Effect.gen(function* () {
          const result = yield* finish(read({ url: "https://safe.example/" }), f.requests, 6);
          expect(result.subpages.map((page) => page.status)).toEqual([
            "unsupported",
            "unsupported",
            "blocked_local",
            "ok",
          ]);
          expect(result.subpages[0]).toMatchObject({
            finalUrl: "https://outside.example/rooms",
            text: "",
          });
          expect(result.subpages[1]).toMatchObject({
            finalUrl: "http://127.0.0.1/rooms",
            text: "",
          });
          expect(f.observed.map((request) => request.url)).toEqual([
            "https://safe.example/robots.txt",
            "https://safe.example/",
            "https://safe.example/rooms-external",
            "https://safe.example/eventi-private",
            "https://safe.example/camere-rebound",
            "https://safe.example/alloggi",
          ]);
          const cached = yield* read({ url: "https://safe.example/" });
          expect(cached.subpages.map((page) => page.status)).toEqual([
            "unsupported",
            "unsupported",
            "blocked_local",
            "ok",
          ]);
          expect(cached.subpages.every((page) => page.rung === "cache")).toBe(true);
          expect(f.observed).toHaveLength(6);
          const direct = yield* finish(
            read({ url: "https://safe.example/rooms-external", followSubpages: false }),
            f.requests,
            3,
          );
          expect(direct).toMatchObject({ status: "ok", finalUrl: "https://outside.example/rooms" });
          expect(f.observed).toHaveLength(9);
        }).pipe(Effect.provide(f.layer));
      }),
  );

  it.effect(
    "resolves links from the final root URL and deduplicates redirected aliases and cycles",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture((url) =>
          Effect.succeed(
            url.pathname === "/start"
              ? response("", 302, { location: "/home/" })
              : url.pathname === "/home/"
                ? response(
                    html.replace(
                      "</body>",
                      '<a href="alias">Rooms</a><a href="camere">Camere</a><a href="/start#rooms">Rooms home</a><a href="#rooms">Rooms home</a></body>',
                    ),
                  )
                : url.pathname === "/home/alias"
                  ? response("", 302, { location: "camere" })
                  : response(
                      html.replace(
                        "</body>",
                        '<a href="/home/">Home</a><a href="/rooms-deeper">Rooms at depth two</a></body>',
                      ),
                    ),
          ),
        );
        yield* Effect.gen(function* () {
          const result = yield* finish(
            read({ url: "https://canonical.example/start" }),
            f.requests,
            5,
          );
          expect(result).toMatchObject({
            url: "https://canonical.example/start",
            finalUrl: "https://canonical.example/home/",
            subpages: [
              {
                url: "https://canonical.example/home/alias",
                finalUrl: "https://canonical.example/home/camere",
                status: "ok",
              },
            ],
            truncated: false,
            unvisitedSubpages: 0,
            pendingSubpages: [],
          });
          expect(f.observed.map((request) => new URL(request.url).pathname)).toEqual([
            "/robots.txt",
            "/start",
            "/home/",
            "/home/alias",
            "/home/camere",
          ]);
        }).pipe(Effect.provide(f.layer));
      }),
  );

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
          { url: "https://example.com/", maxSubpages: 7 },
          { url: "https://example.com/", maxSubpages: 0 },
          { url: "https://example.com/", subpagePattern: "" },
          { url: "https://example.com/", subpagePattern: "rooms|" },
          { url: "https://example.com/", subpagePattern: "x".repeat(257) },
          { url: "https://example.com/", followSubpages: "false" },
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

  it.effect(
    "shares a subpage fetch across concurrent callers and applies freshness to each page",
    () =>
      Effect.gen(function* () {
        const release = yield* Deferred.make<void>();
        let rooms = html;
        const f = yield* fixture((url) =>
          url.pathname === "/"
            ? Effect.succeed(
                response(html.replace("</body>", '<a href="/camere">Camere</a></body>')),
              )
            : Deferred.await(release).pipe(Effect.map(() => response(rooms))),
        );
        yield* Effect.gen(function* () {
          yield* finish(
            read({ url: "https://shared-pages.example/", followSubpages: false }),
            f.requests,
          );
          const first = yield* Effect.forkScoped(read({ url: "https://shared-pages.example/" }));
          expect((yield* Queue.take(f.requests)).url).toBe("https://shared-pages.example/camere");
          const second = yield* Effect.forkScoped(
            read(
              { url: "https://shared-pages.example/" },
              {
                ...scope,
                requestNamespace: "second-agent",
                thread: { ...scope.thread!, threadId: ThreadId.make("second-thread") },
              },
            ),
          );
          yield* TestClock.adjust(0);
          yield* Deferred.succeed(release, undefined);
          const a = yield* Fiber.join(first);
          const b = yield* Fiber.join(second);
          expect(a.subpages[0]!.rung).toBe("http");
          expect(b.subpages[0]).toMatchObject({
            rung: "cache",
            cacheId: a.subpages[0]!.cacheId,
            contentHash: a.subpages[0]!.contentHash,
          });
          expect(f.observed).toHaveLength(3);
          rooms = html.replace("eight guest rooms", "twelve guest rooms");
          yield* TestClock.adjust(3000);
          const fresh = yield* finish(
            read({ url: "https://shared-pages.example/", maxAgeHours: 0 }),
            f.requests,
            2,
          );
          expect(fresh.subpages[0]!.rung).toBe("http");
          expect(fresh.subpages[0]!.text).toContain("twelve guest rooms");
          expect(fresh.subpages[0]!.contentHash).not.toBe(a.subpages[0]!.contentHash);
          expect(f.observed).toHaveLength(5);
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
