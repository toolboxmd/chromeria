// @effect-diagnostics nodeBuiltinImport:off - an isolated local HTTP listener detects proxy escapes.
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as NodeHttp from "node:http";
import { vi } from "vite-plus/test";

import * as PreviewBrowser from "../preview/PreviewBrowser.ts";
import { ServerBrowserContexts } from "../preview/ServerBrowserContexts.ts";
import * as PublicHttp from "./PublicHttp.ts";
import * as PublicRender from "./PublicRender.ts";
import { classify, extract } from "./extract.ts";
import { SKIP_WITHOUT_CHROMIUM, testChromium } from "../preview/ChromeForTesting.testkit.ts";

const dns = vi.hoisted(() => ({ rebound: vi.fn() }));
vi.mock("node:dns/promises", async (original) => {
  const actual = await original<typeof import("node:dns/promises")>();
  return {
    ...actual,
    lookup: (host: string, options: { all: true }) =>
      host === "rebound.research.invalid" ? dns.rebound() : actual.lookup(host, options),
  };
});
const layer = (executable?: string) =>
  PublicRender.layer.pipe(
    Layer.provide(
      Layer.succeed(PreviewBrowser.PreviewBrowser, {
        installed: Effect.succeed(Option.fromUndefinedOr(executable)),
        executable: Effect.die("Public research must never install a browser."),
      }),
    ),
    Layer.provideMerge(NodeServices.layer),
  );
const initial = (body: string, status = 200, headers: Record<string, string> = {}) => ({
  body,
  status,
  headers,
});
const unusedDocument = () => Effect.die("This fixture must not navigate to another document.");

it.effect(
  "an unavailable installed browser remains an explicit provider failure without installing",
  () =>
    PublicRender.PublicRender.use((renderer) =>
      renderer.render({
        url: new URL("http://research.invalid/"),
        initial: initial("<div id='root'></div>"),
        loadDocument: unusedDocument,
      }),
    ).pipe(
      Effect.flip,
      Effect.tap((error) => Effect.sync(() => expect(error.status).toBe("provider_error"))),
      Effect.provide(layer()),
    ),
);

it.live(
  "the pinned shell renders JavaScript with fresh storage and blocks private/rebound subresources",
  (ctx) =>
    Effect.gen(function* () {
      const executable = testChromium;
      if (!executable) return ctx.skip(SKIP_WITHOUT_CHROMIUM);
      const requests: string[] = [];
      dns.rebound.mockResolvedValue([{ address: "127.0.0.1", family: 4 }]);
      const server = yield* Effect.acquireRelease(
        Effect.callback<NodeHttp.Server>((resume) => {
          const listener = NodeHttp.createServer((request, response) => {
            requests.push(request.url ?? "");
            response.end("LOCAL_SECRET");
          });
          listener.listen(0, "127.0.0.1", () => resume(Effect.succeed(listener)));
        }),
        (listener) =>
          Effect.callback<void>((resume) => {
            listener.close(() => resume(Effect.void));
          }),
      );
      const address = server.address();
      if (!address || typeof address === "string")
        return yield* Effect.die("Fixture did not bind a TCP port.");
      const origin = `http://127.0.0.1:${address.port}`;
      const rebound = `http://rebound.research.invalid:${address.port}`;
      const html = `<html><head><title>JS fixture</title></head><body><div id="root"></div>
    <img src="${origin}/image"><script src="${rebound}/script"></script>
    <iframe src="${origin}/frame"></iframe>
    <script type="speculationrules">{"prefetch":[{"source":"list","urls":["${origin}/prefetch"]}]}</script>
    <script>
      const prior = localStorage.getItem('render-test'); localStorage.setItem('render-test', 'written');
      document.getElementById('root').innerHTML = '<h1>' + (prior || 'fresh storage') + '</h1><p>' + 'Eight rooms and a garden for events. '.repeat(10) + '</p><table><tr><td>Family</td><td>4</td></tr></table><a href="/rooms">Rooms</a>';
      fetch('${origin}/fetch').catch(() => {}); fetch('${rebound}/fetch').catch(() => {});
      try { new WebSocket('${origin.replace("http:", "ws:")}/socket'); } catch {}
      document.body.dataset.webrtc = String(typeof RTCPeerConnection);
      document.body.dataset.plugins = String(navigator.plugins.length);
      window.open('${origin}/popup');
    </script></body></html>`;
      expect(classify(extract("<div id='root'></div>", "http://research.invalid/"), 200)).toBe(
        "thin",
      );
      yield* Effect.gen(function* () {
        const renderer = yield* PublicRender.PublicRender;
        const http = yield* PublicHttp.PublicHttp;
        const render = () =>
          renderer.render({
            url: new URL("http://research.invalid/"),
            initial: initial(html),
            // A blocked iframe may fail its own document without replacing the main page's content.
            loadDocument: (url) =>
              http.get(url).pipe(
                Effect.mapError(
                  (cause) =>
                    new PublicRender.PublicRenderError({
                      status:
                        cause._tag === "PublicNetworkBlockedError"
                          ? "blocked_local"
                          : "provider_error",
                      httpStatus: null,
                      retryAfterMs: null,
                      cause,
                    }),
                ),
              ),
          });
        const result = yield* render();
        const page = extract(result.body, result.finalUrl);
        expect(classify(page, result.httpStatus)).toBe("ok");
        expect(page.text).toContain("# fresh storage");
        expect(page.text).toContain("| Family | 4 |");
        expect(page.text).toContain("http://research.invalid/rooms");
        expect(result.body).toContain('data-webrtc="undefined"');
        // chrome-headless-shell lists no plugins; full Chrome does.
        expect(result.body).toMatch(/data-plugins="[1-9]/);
        const second = yield* render();
        expect(extract(second.body, second.finalUrl).text).toContain("# fresh storage");
        expect(dns.rebound).toHaveBeenCalled();
        expect(requests).toEqual([]);
      }).pipe(Effect.provide(Layer.mergeAll(layer(executable), PublicHttp.layer)));
    }).pipe(Effect.scoped),
);

it.live(
  "a JavaScript redirect to a private document is blocked through the real HTTP boundary",
  (ctx) =>
    Effect.gen(function* () {
      const executable = testChromium;
      if (!executable) return ctx.skip(SKIP_WITHOUT_CHROMIUM);
      yield* Effect.gen(function* () {
        const http = yield* PublicHttp.PublicHttp;
        const result = yield* PublicRender.PublicRender.use((renderer) =>
          renderer.render({
            url: new URL("http://research.invalid/"),
            initial: initial("<script>location.href='http://127.0.0.1/private';</script>"),
            loadDocument: (url) =>
              http.get(url).pipe(
                Effect.mapError(
                  (cause) =>
                    new PublicRender.PublicRenderError({
                      status:
                        cause._tag === "PublicNetworkBlockedError"
                          ? "blocked_local"
                          : "provider_error",
                      httpStatus: null,
                      retryAfterMs: null,
                      cause,
                    }),
                ),
              ),
          }),
        ).pipe(Effect.flip);
        expect(result.status).toBe("blocked_local");
      }).pipe(Effect.provide(Layer.mergeAll(layer(executable), PublicHttp.layer)));
    }),
);

it.live(
  "cancelling a render closes the real browser context and its pending document load",
  (ctx) =>
    Effect.gen(function* () {
      const executable = testChromium;
      if (!executable) return ctx.skip(SKIP_WITHOUT_CHROMIUM);
      const entered = yield* Deferred.make<void>();
      const documentClosed = yield* Deferred.make<void>();
      const closed = Promise.withResolvers<void>();
      const open = ServerBrowserContexts.publicContext;
      yield* Effect.acquireRelease(
        Effect.sync(() =>
          vi.spyOn(ServerBrowserContexts, "publicContext").mockImplementation(async (input) => {
            const browser = await open(input);
            browser.context.once("close", () => closed.resolve());
            return browser;
          }),
        ),
        (spy) => Effect.sync(() => spy.mockRestore()),
      );
      yield* Effect.gen(function* () {
        const fiber = yield* PublicRender.PublicRender.use((renderer) =>
          renderer.render({
            url: new URL("http://research.invalid/"),
            initial: initial("<script>location.href='/pending';</script>"),
            loadDocument: () =>
              Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Effect.never),
                Effect.ensuring(Deferred.succeed(documentClosed, undefined)),
              ),
          }),
        ).pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        yield* Fiber.interrupt(fiber);
        yield* Deferred.await(documentClosed);
        yield* Effect.promise(() => closed.promise);
      }).pipe(Effect.provide(layer(executable)));
    }).pipe(Effect.scoped),
);

it.live(
  "routes HTTP redirect hops through the document port and preserves the final URL/status",
  (ctx) =>
    Effect.gen(function* () {
      const executable = testChromium;
      if (!executable) return ctx.skip(SKIP_WITHOUT_CHROMIUM);
      const calls: string[] = [];
      const result = yield* PublicRender.PublicRender.use((renderer) =>
        renderer.render({
          url: new URL("http://research.invalid/"),
          initial: initial("<script>location.href='/it';</script>"),
          loadDocument: (url) =>
            Effect.sync(() => {
              calls.push(url.href);
              return url.pathname === "/it"
                ? initial("", 302, { location: "/it/" })
                : initial(
                    `<title>Page not found</title><p>${"Ordinary server response text. ".repeat(10)}</p><a href='rooms'>Rooms</a>`,
                    404,
                  );
            }),
        }),
      ).pipe(Effect.result, Effect.provide(layer(executable)));
      expect(calls).toEqual(["http://research.invalid/it", "http://research.invalid/it/"]);
      expect(result._tag).toBe("Success");
      if (result._tag === "Success") {
        expect(result.success).toMatchObject({
          finalUrl: "http://research.invalid/it/",
          httpStatus: 404,
        });
        const page = extract(result.success.body, result.success.finalUrl);
        expect(classify(page, result.success.httpStatus)).toBe("origin_error");
        expect(page.text).toContain("http://research.invalid/it/rooms");
      }
    }),
);

it.live(
  "HTTP redirects to private documents retain blocked_local instead of a browser error page",
  (ctx) =>
    Effect.gen(function* () {
      const executable = testChromium;
      if (!executable) return ctx.skip(SKIP_WITHOUT_CHROMIUM);
      const calls: string[] = [];
      yield* Effect.gen(function* () {
        const http = yield* PublicHttp.PublicHttp;
        const result = yield* PublicRender.PublicRender.use((renderer) =>
          renderer.render({
            url: new URL("http://research.invalid/"),
            initial: initial("<script>location.href='/redirect';</script>"),
            loadDocument: (url) =>
              Effect.gen(function* () {
                calls.push(url.href);
                if (url.pathname === "/redirect")
                  return initial("", 302, { location: "http://127.0.0.1/private" });
                return yield* http.get(url).pipe(
                  Effect.mapError(
                    (cause) =>
                      new PublicRender.PublicRenderError({
                        status:
                          cause._tag === "PublicNetworkBlockedError"
                            ? "blocked_local"
                            : "provider_error",
                        httpStatus: null,
                        retryAfterMs: null,
                        cause,
                      }),
                  ),
                );
              }),
          }),
        ).pipe(Effect.result);
        expect(calls).toEqual(["http://research.invalid/redirect", "http://127.0.0.1/private"]);
        expect(result._tag).toBe("Failure");
        if (result._tag === "Failure") expect(result.failure.status).toBe("blocked_local");
      }).pipe(Effect.provide(Layer.mergeAll(layer(executable), PublicHttp.layer)));
    }),
);

it.live("bounds HTTP redirect loops without releasing a hop to browser networking", (ctx) =>
  Effect.gen(function* () {
    const executable = testChromium;
    if (!executable) return ctx.skip(SKIP_WITHOUT_CHROMIUM);
    let calls = 0;
    const result = yield* PublicRender.PublicRender.use((renderer) =>
      renderer.render({
        url: new URL("http://research.invalid/"),
        initial: initial("<script>location.href='/loop';</script>"),
        loadDocument: () =>
          Effect.sync(() => {
            calls++;
            return initial("", 302, { location: "/loop" });
          }),
      }),
    ).pipe(Effect.flip, Effect.provide(layer(executable)));
    expect(result.status).toBe("origin_error");
    expect(calls).toBe(6);
  }),
);

it.live("keeps thin plain text as text rather than executing it as HTML", (ctx) =>
  Effect.gen(function* () {
    const executable = testChromium;
    if (!executable) return ctx.skip(SKIP_WITHOUT_CHROMIUM);
    const result = yield* PublicRender.PublicRender.use((renderer) =>
      renderer.render({
        url: new URL("http://research.invalid/"),
        initial: initial("<script>document.title='EXECUTED'</script>", 200, {
          "content-type": "text/plain",
        }),
        loadDocument: unusedDocument,
      }),
    ).pipe(Effect.provide(layer(executable)));
    const page = extract(result.body, result.finalUrl);
    expect(page.title).not.toBe("EXECUTED");
    expect(page.text).toContain("<script>document.title='EXECUTED'</script>");
  }),
);

it.live(
  "reports unsupported main-frame POST navigation without issuing a document request",
  (ctx) =>
    Effect.gen(function* () {
      const executable = testChromium;
      if (!executable) return ctx.skip(SKIP_WITHOUT_CHROMIUM);
      const result = yield* PublicRender.PublicRender.use((renderer) =>
        renderer.render({
          url: new URL("http://research.invalid/"),
          initial: initial(
            "<form method='post' action='/write'></form><script>document.querySelector('form').submit();</script>",
          ),
          loadDocument: unusedDocument,
        }),
      ).pipe(Effect.flip, Effect.provide(layer(executable)));
      expect(result.status).toBe("unsupported");
    }),
);
