import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import { afterEach, vi } from "vite-plus/test";
import * as HttpClientError from "effect/http/HttpClientError";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import type * as HttpClientRequest from "effect/http/HttpClientRequest";

import * as Config from "../config.ts";
import * as SecretStore from "../auth/ServerSecretStore.ts";
import * as TinyFish from "./TinyFish.ts";

const dns = vi.hoisted(() => ({ lookup: vi.fn() }));
vi.mock("node:dns/promises", () => dns);
afterEach(() => dns.lookup.mockReset());

const URL_ = "https://93.184.216.34/";
const TOKEN = "fixture-only-secret-never-reflected";
const html =
  "<html><body><h1>Research</h1><p>Public facts for a complete research page, with enough text to be usable rather than a shell.</p></body></html>";
const payload = (page: Record<string, unknown> = {}, extra: Record<string, unknown> = {}) => ({
  results: [{ url: URL_, final_url: `${URL_}final`, format: "html", text: html, ...page }],
  errors: [],
  ...extra,
});
const reply = (
  request: HttpClientRequest.HttpClientRequest,
  value: unknown = payload(),
  status = 200,
  headers: Record<string, string> = {},
) =>
  HttpClientResponse.fromWeb(
    request,
    new Response(JSON.stringify(value), {
      status,
      headers: {
        "content-type": "application/json",
        "x-treg-cost-micro": "0",
        "x-treg-call-id": "0123456789abcdef0123456789abcdef",
        ...headers,
      },
    }),
  );
const base = Layer.mergeAll(NodeServices.layer, NodeCrypto.layer);
const fixture = Effect.fnUntraced(function* (
  respond: (
    request: HttpClientRequest.HttpClientRequest,
    signal: AbortSignal,
  ) => Effect.Effect<HttpClientResponse.HttpClientResponse, HttpClientError.HttpClientError> = (
    r,
  ) => Effect.succeed(reply(r)),
) {
  const fs = yield* FileSystem.FileSystem;
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "tinyfish-port-" });
  const config = Config.layerTest(process.cwd(), root);
  const paths = yield* Config.deriveServerPaths(root, undefined);
  const requests: HttpClientRequest.HttpClientRequest[] = [];
  const dependencies = SecretStore.layer.pipe(Layer.provideMerge(config), Layer.provideMerge(base));
  const layer = TinyFish.layer.pipe(
    Layer.provide(
      Layer.succeed(
        HttpClient.HttpClient,
        HttpClient.make((request, _url, signal) => {
          requests.push(request);
          return respond(request, signal);
        }),
      ),
    ),
    Layer.provideMerge(dependencies),
  );
  const provision = SecretStore.ServerSecretStore.use((s) =>
    s.create(TinyFish.TREG_SECRET_NAME, new TextEncoder().encode(TOKEN)),
  );
  const quotaFile = `${paths.stateDir}/web-cache/tinyfish-quota.json`;
  const seedQuota = (starts: number[], blockedUntil = 0) =>
    fs
      .makeDirectory(`${paths.stateDir}/web-cache`, { recursive: true })
      .pipe(
        Effect.andThen(fs.writeFileString(quotaFile, JSON.stringify({ starts, blockedUntil }))),
      );
  return { layer, requests, provision, seedQuota, quotaFile, fs, paths };
});
const fetch = (url = URL_) => TinyFish.TinyFish.use((s) => s.fetch({ url: new URL(url) }));

it.effect(
  "pins the free endpoint and request body, reads the exact secret, and records actual zero charge",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* Effect.gen(function* () {
        yield* f.provision;
        expect(yield* fetch()).toMatchObject({
          page: {
            text: "# Research\n\nPublic facts for a complete research page, with enough text to be usable rather than a shell.",
          },
          finalUrl: `${URL_}final`,
          chargedMicro: 0,
          callId: "0123456789abcdef0123456789abcdef",
        });
        expect(f.requests).toHaveLength(1);
        const r = f.requests[0]!;
        expect(r.url).toBe("https://treg.to/call/tinyfish.web.fetch");
        expect(r.method).toBe("POST");
        expect(r.headers["x-treg-token"]).toBe(TOKEN);
        expect(r.headers["x-treg-route-max-cost"]).toBe("0");
        expect(r.body._tag).toBe("Uint8Array");
        if (r.body._tag === "Uint8Array")
          expect(JSON.parse(new TextDecoder().decode(r.body.body))).toEqual({
            urls: [URL_],
            format: "html",
            ttl: 0,
            per_url_timeout_ms: 15_000,
          });
        expect(
          (yield* f.fs.stat(`${f.paths.secretsDir}/${TinyFish.TREG_SECRET_NAME}.bin`)).mode & 0o777,
        ).toBe(0o600);
        expect(yield* f.fs.readFileString(f.quotaFile)).not.toContain(TOKEN);
      }).pipe(Effect.provide(f.layer));
    }).pipe(Effect.provide(base)),
);

it.effect.each(
  (
    [
      ["wrapped provider error", payload({}, { error: { message: TOKEN } }), {}],
      ["partial errors", payload({}, { errors: [{ message: "upstream error" }] }), {}],
      ["success false", payload({}, { success: false }), {}],
      ["missing result", { results: [], errors: [] }, {}],
      ["extra result", { results: [...payload().results, ...payload().results], errors: [] }, {}],
      ["wrong original URL", payload({ url: "https://93.184.216.34/other" }), {}],
      ["relative final URL", payload({ final_url: "/private" }), {}],
      ["credential final URL", payload({ final_url: "https://name:password@example.com/" }), {}],
      ["private final URL", payload({ final_url: "http://127.0.0.1/" }), {}],
      ["tailnet final URL", payload({ final_url: "http://100.64.0.1/" }), {}],
      ["non-web final URL", payload({ final_url: "file:///etc/passwd" }), {}],
      ["wrong format", payload({ format: "markdown" }), {}],
      [
        "JSON as HTML",
        payload({ text: '{"error":"upstream unavailable and no readable page"}' }),
        {},
      ],
      ["reflected credential", payload({ text: `<p>${TOKEN}</p>` }), {}],
      ["nonzero charge", payload(), { "x-treg-cost-micro": "1" }],
      ["missing charge", payload(), { "x-treg-cost-micro": "" }],
      ["unsettled charge", payload(), { "x-treg-cost-micro": "null" }],
      ["missing call identity", payload(), { "x-treg-call-id": "" }],
      ["upstream failed header", payload(), { "x-treg-upstream-failed": TOKEN }],
    ] as const
  ).map(([name, body, headers]) => ({ name, body, headers })),
)("rejects $name without reflecting secrets or accepting provider content", ({ body, headers }) =>
  Effect.gen(function* () {
    const f = yield* fixture((r) => Effect.succeed(reply(r, body, 200, headers)));
    yield* Effect.gen(function* () {
      yield* f.provision;
      const result = yield* Effect.result(fetch());
      expect(result).toMatchObject({
        _tag: "Failure",
        failure: { _tag: "TinyFishProviderError" },
      });
      expect(JSON.stringify(result)).not.toContain(TOKEN);
      expect(f.requests).toHaveLength(1);
    }).pipe(Effect.provide(f.layer));
  }).pipe(Effect.provide(base)),
);

it.effect.each([
  "http://127.0.0.1/",
  "http://169.254.169.254/",
  "http://10.0.0.1/",
  "http://[::1]/",
  "http://100.64.0.1/",
  "https://name:password@example.com/",
  "file:///etc/passwd",
  "https://booking.com/",
])("blocks target %s before using provider quota", (url) =>
  Effect.gen(function* () {
    const f = yield* fixture((r) =>
      Effect.gen(function* () {
        const disabled = yield* HttpClient.TracerDisabledWhen;
        expect(disabled(r)).toBe(true);
        return reply(r);
      }),
    );
    yield* Effect.gen(function* () {
      yield* f.provision;
      expect(yield* Effect.result(fetch(url))).toMatchObject({
        _tag: "Failure",
        failure: { _tag: "TinyFishProviderError", reason: "blocked_target" },
      });
      expect(f.requests).toHaveLength(0);
      expect(yield* f.fs.exists(f.quotaFile)).toBe(false);
    }).pipe(Effect.provide(f.layer));
  }).pipe(Effect.provide(base)),
);

it.effect("a restricted subpage cannot accept a cross-origin hosted redirect", () =>
  Effect.gen(function* () {
    const f = yield* fixture((r) =>
      Effect.succeed(reply(r, payload({ final_url: "https://1.1.1.1/" }))),
    );
    yield* Effect.gen(function* () {
      yield* f.provision;
      expect(
        yield* Effect.result(
          TinyFish.TinyFish.use((s) =>
            s.fetch({ url: new URL(URL_), sameOrigin: "https://93.184.216.34" }),
          ),
        ),
      ).toMatchObject({ _tag: "Failure", failure: { reason: "contract" } });
    }).pipe(Effect.provide(f.layer));
  }).pipe(Effect.provide(base)),
);

it.effect("missing credentials preserve optional local reading without any hosted request", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    yield* Effect.gen(function* () {
      expect(yield* fetch()).toBeUndefined();
      expect(f.requests).toHaveLength(0);
      expect(yield* f.fs.exists(f.quotaFile)).toBe(false);
    }).pipe(Effect.provide(f.layer));
  }).pipe(Effect.provide(base)),
);

it.effect.each(["minute", "day"] as const)(
  "shares the %s quota across callers and survives reconstruction",
  (kind) =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const now = yield* Clock.currentTimeMillis;
      yield* f.seedQuota(
        Array.from({ length: kind === "minute" ? 149 : 999 }, (_, i) =>
          kind === "minute" ? now : now - 120_000 - i,
        ),
      );
      yield* Effect.gen(function* () {
        yield* f.provision;
        const results = yield* Effect.all([Effect.result(fetch()), Effect.result(fetch())], {
          concurrency: "unbounded",
        });
        expect(results.filter((r) => r._tag === "Success")).toHaveLength(1);
        expect(results.find((r) => r._tag === "Failure")).toMatchObject({
          failure: { _tag: "TinyFishBudgetError", reason: kind },
        });
        expect(f.requests).toHaveLength(1);
      }).pipe(Effect.provide(f.layer));
      yield* Effect.gen(function* () {
        expect(yield* Effect.result(fetch())).toMatchObject({
          _tag: "Failure",
          failure: { _tag: "TinyFishBudgetError", reason: kind },
        });
      }).pipe(Effect.provide(Layer.fresh(f.layer)));
      expect(f.requests).toHaveLength(1);
      yield* TestClock.adjust(kind === "minute" ? 60_000 : 86_400_000);
      yield* fetch().pipe(Effect.provide(Layer.fresh(f.layer)));
      expect(f.requests).toHaveLength(2);
    }).pipe(Effect.provide(base)),
);

it.effect("a 429 circuit respects Retry-After across callers and service reconstruction", () =>
  Effect.gen(function* () {
    let calls = 0;
    const f = yield* fixture((r) =>
      Effect.succeed(
        ++calls === 1
          ? reply(r, { error: "rate_limited" }, 429, { "retry-after": "120" })
          : reply(r),
      ),
    );
    yield* Effect.gen(function* () {
      yield* f.provision;
      expect(yield* Effect.result(fetch())).toMatchObject({
        _tag: "Failure",
        failure: { reason: "circuit", retryAfterMs: 120_000 },
      });
      expect(yield* Effect.result(fetch())).toMatchObject({
        _tag: "Failure",
        failure: { reason: "circuit", retryAfterMs: 120_000 },
      });
    }).pipe(Effect.provide(f.layer));
    yield* TestClock.adjust(119_999);
    yield* Effect.gen(function* () {
      expect(yield* Effect.result(fetch())).toMatchObject({
        _tag: "Failure",
        failure: { reason: "circuit", retryAfterMs: 1 },
      });
      yield* TestClock.adjust(1);
      expect(yield* fetch()).toMatchObject({ chargedMicro: 0 });
    }).pipe(Effect.provide(Layer.fresh(f.layer)));
    expect(f.requests).toHaveLength(2);
  }).pipe(Effect.provide(base)),
);

it.effect.each(["cancel", "timeout"] as const)(
  "%s aborts the authenticated transport and does not refund reserved quota",
  (kind) =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const closed = yield* Deferred.make<void>();
      let signal: AbortSignal | undefined;
      const f = yield* fixture((_r, abort) => {
        signal = abort;
        return Deferred.succeed(entered, undefined).pipe(
          Effect.andThen(Effect.never),
          Effect.ensuring(Deferred.succeed(closed, undefined)),
        );
      });
      yield* Effect.gen(function* () {
        yield* f.provision;
        const run = yield* Effect.forkScoped(Effect.result(fetch()));
        yield* Deferred.await(entered);
        if (kind === "cancel") yield* Fiber.interrupt(run);
        else {
          yield* TestClock.adjust(20_000);
          expect(yield* Fiber.join(run)).toMatchObject({
            _tag: "Failure",
            failure: { reason: "timeout" },
          });
        }
        yield* Deferred.await(closed);
        expect(signal?.aborted).toBe(true);
        const quota = JSON.parse(yield* f.fs.readFileString(f.quotaFile));
        expect(quota.starts).toHaveLength(1);
        expect(JSON.stringify(quota)).not.toContain(TOKEN);
      }).pipe(Effect.provide(f.layer));
    }).pipe(Effect.provide(base)),
);

it.effect("corrupted quota state fails closed before a request", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    yield* f.fs.makeDirectory(`${f.paths.stateDir}/web-cache`, { recursive: true });
    yield* f.fs.writeFileString(f.quotaFile, "not valid quota JSON");
    yield* Effect.gen(function* () {
      yield* f.provision;
      expect(yield* Effect.result(fetch())).toMatchObject({
        _tag: "Failure",
        failure: { reason: "quota_storage" },
      });
      expect(f.requests).toHaveLength(0);
    }).pipe(Effect.provide(f.layer));
  }).pipe(Effect.provide(base)),
);

it.effect("a hostname resolving to any private address is refused before the hosted request", () =>
  Effect.gen(function* () {
    dns.lookup.mockResolvedValue([
      { address: "93.184.216.34", family: 4 },
      { address: "127.0.0.1", family: 4 },
    ]);
    const f = yield* fixture();
    yield* Effect.gen(function* () {
      yield* f.provision;
      expect(yield* Effect.result(fetch("https://mixed.example/"))).toMatchObject({
        _tag: "Failure",
        failure: { reason: "blocked_target" },
      });
      expect(f.requests).toHaveLength(0);
    }).pipe(Effect.provide(f.layer));
  }).pipe(Effect.provide(base)),
);

it.effect(
  "drops transport causes containing the authenticated request and reflected credential",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture((request) =>
        Effect.fail(
          new HttpClientError.HttpClientError({
            reason: new HttpClientError.TransportError({ request, cause: new Error(TOKEN) }),
          }),
        ),
      );
      yield* Effect.gen(function* () {
        yield* f.provision;
        const result = yield* Effect.result(fetch());
        expect(result).toMatchObject({ _tag: "Failure", failure: { reason: "transport" } });
        expect(JSON.stringify(result)).not.toContain(TOKEN);
        if (result._tag === "Failure") expect("cause" in result.failure).toBe(false);
      }).pipe(Effect.provide(f.layer));
    }).pipe(Effect.provide(base)),
);

it.effect("escaped reflected credentials are rejected after response parsing", () =>
  Effect.gen(function* () {
    const f = yield* fixture((r) =>
      Effect.succeed(
        HttpClientResponse.fromWeb(
          r,
          new Response(
            JSON.stringify(payload({ text: `<p>${TOKEN}</p>` })).replaceAll(
              TOKEN,
              `\\u0066${TOKEN.slice(1)}`,
            ),
            {
              headers: {
                "x-treg-cost-micro": "0",
                "x-treg-call-id": "0123456789abcdef0123456789abcdef",
              },
            },
          ),
        ),
      ),
    );
    yield* Effect.gen(function* () {
      yield* f.provision;
      const result = yield* Effect.result(fetch());
      expect(result).toMatchObject({ _tag: "Failure", failure: { reason: "contract" } });
      expect(JSON.stringify(result)).not.toContain(TOKEN);
    }).pipe(Effect.provide(f.layer));
  }).pipe(Effect.provide(base)),
);

it.effect("a zero-cost ceiling refusal is an explicit charge error and never changes route", () =>
  Effect.gen(function* () {
    const f = yield* fixture((r) => Effect.succeed(reply(r, { error: "route_max_cost" }, 402)));
    yield* Effect.gen(function* () {
      yield* f.provision;
      expect(yield* Effect.result(fetch())).toMatchObject({
        _tag: "Failure",
        failure: { reason: "charge", httpStatus: 402 },
      });
      expect(f.requests).toHaveLength(1);
      expect(f.requests[0]!.url).toBe("https://treg.to/call/tinyfish.web.fetch");
    }).pipe(Effect.provide(f.layer));
  }).pipe(Effect.provide(base)),
);

it.effect("HTML entity encoded credentials cannot reach extracted text or provenance", () =>
  Effect.gen(function* () {
    const f = yield* fixture((r) =>
      Effect.succeed(
        reply(
          r,
          payload({
            text: `<html><title>&#102;${TOKEN.slice(1)}</title><body><p>&#102;${TOKEN.slice(1)}</p></body></html>`,
          }),
        ),
      ),
    );
    yield* Effect.gen(function* () {
      yield* f.provision;
      const result = yield* Effect.result(fetch());
      expect(result).toMatchObject({ _tag: "Failure", failure: { reason: "contract" } });
      expect(JSON.stringify(result)).not.toContain(TOKEN);
    }).pipe(Effect.provide(f.layer));
  }).pipe(Effect.provide(base)),
);

it.effect.each([200, 402, 429, 503])(
  "closes the authenticated response scope after HTTP %s, including unread errors",
  (status) =>
    Effect.gen(function* () {
      let signal: AbortSignal | undefined;
      const f = yield* fixture((r, abort) => {
        signal = abort;
        return Effect.succeed(
          reply(r, status === 200 ? payload() : { error: "unread provider error" }, status),
        );
      });
      yield* Effect.gen(function* () {
        yield* f.provision;
        yield* Effect.result(fetch());
        expect(signal?.aborted).toBe(true);
        expect(f.requests).toHaveLength(1);
      }).pipe(Effect.provide(f.layer));
    }).pipe(Effect.provide(base)),
);

it.effect.each(["resolver failure", "private answer"] as const)(
  "missing credentials skip hosted DNS even with a %s",
  (kind) =>
    Effect.gen(function* () {
      if (kind === "resolver failure") dns.lookup.mockRejectedValue(new Error(TOKEN));
      else dns.lookup.mockResolvedValue([{ address: "10.0.0.1", family: 4 }]);
      const f = yield* fixture();
      yield* Effect.gen(function* () {
        expect(yield* fetch("https://research.example/")).toBeUndefined();
        expect(dns.lookup).not.toHaveBeenCalled();
        expect(f.requests).toHaveLength(0);
        expect(yield* f.fs.exists(f.quotaFile)).toBe(false);
      }).pipe(Effect.provide(f.layer));
    }).pipe(Effect.provide(base)),
);

it.effect("resolver failures are distinct from blocked targets and drop unsafe DNS causes", () =>
  Effect.gen(function* () {
    dns.lookup.mockRejectedValue(new Error(TOKEN));
    const f = yield* fixture();
    yield* Effect.gen(function* () {
      yield* f.provision;
      const result = yield* Effect.result(fetch("https://research.example/"));
      expect(result).toMatchObject({
        _tag: "Failure",
        failure: { _tag: "TinyFishProviderError", reason: "resolve", httpStatus: null },
      });
      expect(JSON.stringify(result)).not.toContain(TOKEN);
      if (result._tag === "Failure") expect("cause" in result.failure).toBe(false);
      expect(f.requests).toHaveLength(0);
      expect(yield* f.fs.exists(f.quotaFile)).toBe(false);
    }).pipe(Effect.provide(f.layer));
  }).pipe(Effect.provide(base)),
);

it.effect("rechecks hosted deadline after DNS before reserving shared quota", () =>
  Effect.gen(function* () {
    const started = Promise.withResolvers<void>();
    const release = Promise.withResolvers<ReadonlyArray<{ address: string; family: number }>>();
    dns.lookup.mockImplementation(() => {
      started.resolve();
      return release.promise;
    });
    const url = "https://research.example/";
    const f = yield* fixture((r) => Effect.succeed(reply(r, payload({ url, final_url: URL_ }))));
    yield* Effect.gen(function* () {
      yield* f.provision;
      const pending = yield* Effect.forkScoped(
        TinyFish.TinyFish.use((s) => s.fetch({ url: new URL(url), deadline: () => 25_000 })),
      );
      yield* Effect.promise(() => started.promise);
      yield* TestClock.adjust(10_000);
      release.resolve([{ address: "93.184.216.34", family: 4 }]);
      expect(yield* Fiber.join(pending)).toEqual({ _tag: "DeadlineDeclined" });
      expect(f.requests).toHaveLength(0);
      expect(yield* f.fs.exists(f.quotaFile)).toBe(false);
    }).pipe(Effect.provide(f.layer));
  }).pipe(Effect.provide(base)),
);
