import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type { BrowserContext, Page, Route } from "playwright-core";

import { publicProxy } from "../htmlRender/publicProxy.ts";
import * as PreviewBrowser from "../preview/PreviewBrowser.ts";
import { ServerBrowserContexts } from "../preview/ServerBrowserContexts.ts";

export class PublicRenderError extends Schema.TaggedError<PublicRenderError>()(
  "PublicRenderError",
  {
    status: Schema.Literals([
      "provider_error",
      "timeout",
      "blocked_local",
      "blocked_robots",
      "unsupported",
      "challenge",
      "origin_error",
    ]),
    httpStatus: Schema.NullOr(Schema.Int),
    retryAfterMs: Schema.NullOr(Schema.Number),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message() {
    return "The isolated public-page render did not produce a readable page.";
  }
}
const isRenderError = Schema.is(PublicRenderError);

interface DocumentResponse {
  readonly status: number;
  readonly headers: Readonly<Record<string, string>>;
  readonly body: string;
}

interface RenderInput {
  readonly url: URL;
  readonly initial: DocumentResponse;
  /** Every later document, including redirects and frames, uses the core's HTTP policy. */
  readonly loadDocument: (url: URL) => Effect.Effect<DocumentResponse, PublicRenderError>;
}

export class PublicRender extends Context.Service<
  PublicRender,
  {
    readonly render: (input: RenderInput) => Effect.Effect<
      {
        readonly body: string;
        readonly finalUrl: string;
        readonly httpStatus: number;
        readonly headers: Readonly<Record<string, string>>;
      },
      PublicRenderError
    >;
  }
>()("t3/webResearch/PublicRender") {}

const MAX_DOM_CHARS = 4 * 1024 * 1024;

/** Playwright callback boundary: document I/O stays owned by the Effect reader. */
const routeDocuments = (
  context: BrowserContext,
  page: Page,
  input: RenderInput,
  signal: AbortSignal,
) => {
  let first = true;
  let httpStatus = input.initial.status;
  let responseHeaders = input.initial.headers;
  let failure: PublicRenderError | undefined;
  let redirects = 0;
  const route = async (route: Route) => {
    const request = route.request();
    if (!/^https?:/.test(request.url()) || !["GET", "HEAD"].includes(request.method())) {
      if (request.isNavigationRequest() && request.frame() === page.mainFrame())
        failure = new PublicRenderError({
          status: "unsupported",
          httpStatus: null,
          retryAfterMs: null,
        });
      await route.abort("blockedbyclient").catch(() => {});
      return;
    }
    if (!request.isNavigationRequest()) {
      // The browser's SOCKS proxy checks and pins every subresource connection.
      await route.continue().catch(() => {});
      return;
    }
    try {
      const loaded =
        first && request.url() === input.url.href && request.frame() === page.mainFrame()
          ? { response: input.initial }
          : await Effect.runPromise(
              input.loadDocument(new URL(request.url())).pipe(
                Effect.match({
                  onSuccess: (response) => ({ response }),
                  onFailure: (error) => ({ error }),
                }),
              ),
              { signal },
            );
      if ("error" in loaded) throw loaded.error;
      const response = loaded.response;
      first = false;
      if (request.frame() === page.mainFrame()) {
        httpStatus = response.status;
        responseHeaders = response.headers;
      }
      if ([301, 302, 303, 307, 308].includes(response.status)) {
        const location = response.headers.location;
        if (++redirects >= 6 || !location || !URL.canParse(location, request.url()))
          throw new PublicRenderError({
            status: "origin_error",
            httpStatus: response.status,
            retryAfterMs: null,
          });
        const target = new URL(location, request.url());
        if (!["http:", "https:"].includes(target.protocol) || target.username || target.password)
          throw new PublicRenderError({
            status: "unsupported",
            httpStatus: response.status,
            retryAfterMs: null,
          });
        const escaped = target.href
          .replaceAll("&", "&amp;")
          .replaceAll('"', "&quot;")
          .replaceAll("<", "&lt;");
        // Playwright skips routing HTTP redirect hops. A new document navigation keeps
        // every hop in loadDocument and preserves the target URL for relative links.
        await route.fulfill({
          status: 200,
          contentType: "text/html; charset=utf-8",
          body: `<!doctype html><meta http-equiv="refresh" content="0;url=${escaped}">`,
        });
        return;
      }
      // Public HTTP decoded/decompressed the body; do not replay its cookies into browser storage.
      const headers = Object.fromEntries(
        Object.entries(response.headers).filter(
          ([name]) =>
            ![
              "content-encoding",
              "content-length",
              "transfer-encoding",
              "set-cookie",
              "content-type",
            ].includes(name.toLowerCase()),
        ),
      );
      await route.fulfill({
        status: response.status,
        headers: {
          ...headers,
          "content-type":
            response.headers["content-type"]?.split(";", 1)[0]?.trim().toLowerCase() ===
            "text/plain"
              ? "text/plain; charset=utf-8"
              : "text/html; charset=utf-8",
        },
        body: response.body,
      });
    } catch (cause) {
      if (request.frame() === page.mainFrame())
        failure = isRenderError(cause)
          ? cause
          : new PublicRenderError({
              status: "provider_error",
              httpStatus: null,
              retryAfterMs: null,
              cause,
            });
      await route.abort("blockedbyclient").catch(() => {});
    }
  };
  return {
    install: () => context.route("**/*", route),
    outcome: () => ({ httpStatus, headers: responseHeaders, failure }),
  };
};

/** The callback adapter owns its document fibers until the DOM has been read. */
const renderBrowser = async (context: BrowserContext, input: RenderInput, signal: AbortSignal) => {
  const documentsController = new AbortController();
  const abortDocuments = () => documentsController.abort();
  signal.addEventListener("abort", abortDocuments, { once: true });
  try {
    signal.throwIfAborted();
    await context.addInitScript(
      "delete window.RTCPeerConnection; delete window.webkitRTCPeerConnection;",
    );
    const page = await context.newPage();
    const documents = routeDocuments(context, page, input, documentsController.signal);
    await documents.install();
    try {
      await page.goto(input.url.href, { waitUntil: "networkidle", timeout: 0 });
      const finalUrl = page.url();
      if (!/^https?:/.test(finalUrl))
        throw new PublicRenderError({
          status: "unsupported",
          httpStatus: null,
          retryAfterMs: null,
        });
      const body = await page.evaluate<string>(
        `document.documentElement.outerHTML.slice(0, ${MAX_DOM_CHARS + 1})`,
      );
      const { httpStatus, headers, failure } = documents.outcome();
      if (failure) throw failure;
      if (body.length > MAX_DOM_CHARS)
        throw new Error("Research DOM exceeds the extraction limit.");
      return { body, finalUrl, httpStatus, headers };
    } catch (cause) {
      throw documents.outcome().failure ?? cause;
    }
  } finally {
    // Effect's signal aborts on interruption; a successful snapshot must also stop
    // a document load that raced the snapshot and would otherwise outlive the read.
    abortDocuments();
    signal.removeEventListener("abort", abortDocuments);
  }
};

const make = Effect.gen(function* () {
  const previewBrowser = yield* PreviewBrowser.PreviewBrowser;
  const env = yield* HostProcess.Environment;
  const render = Effect.fn("PublicRender.render")(function* (input: RenderInput) {
    // Playwright callbacks leave the current fiber. Keep the caller's clock and
    // service context when they enter the reader again through document I/O.
    const services = yield* Effect.context<never>();
    const documentInput = {
      ...input,
      loadDocument: (url: URL) => input.loadDocument(url).pipe(Effect.provideContext(services)),
    };
    const executable = yield* previewBrowser.installed;
    if (Option.isNone(executable))
      return yield* new PublicRenderError({
        status: "provider_error",
        httpStatus: null,
        retryAfterMs: null,
      });
    const proxyPort = yield* publicProxy.pipe(
      Effect.mapError(
        (cause) =>
          new PublicRenderError({
            status: "provider_error",
            httpStatus: null,
            retryAfterMs: null,
            cause,
          }),
      ),
    );
    const browser = yield* Effect.acquireRelease(
      Effect.tryPromise({
        try: () =>
          ServerBrowserContexts.publicContext({ executable: executable.value, proxyPort, env }),
        catch: (cause) =>
          new PublicRenderError({
            status: "provider_error",
            httpStatus: null,
            retryAfterMs: null,
            cause,
          }),
      }),
      (browser) => Effect.promise(() => browser.close()).pipe(Effect.ignore),
    );
    return yield* Effect.tryPromise({
      try: (signal) => renderBrowser(browser.context, documentInput, signal),
      catch: (cause) =>
        isRenderError(cause)
          ? cause
          : new PublicRenderError({
              status: "provider_error",
              httpStatus: null,
              retryAfterMs: null,
              cause,
            }),
    });
  }, Effect.scoped);
  return PublicRender.of({ render });
});

export const layer = Layer.effect(PublicRender, make);
