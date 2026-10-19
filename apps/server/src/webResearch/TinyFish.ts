import * as ByteSize from "effect/ByteSize";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as HttpClient from "effect/http/HttpClient";
import * as Headers from "effect/http/Headers";
import * as HttpBody from "effect/http/HttpBody";
import * as HttpIncomingMessage from "effect/http/HttpIncomingMessage";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerConfig from "../config.ts";
import * as PublicHttp from "./PublicHttp.ts";
import { extract } from "./extract.ts";

export const TREG_SECRET_NAME = "web-research-treg-token";
const ENDPOINT = "https://treg.to/call/tinyfish.web.fetch";
const FETCH_TIMEOUT_MS = 20_000;
const DAY_MS = 24 * 3600_000;
const MAX_BYTES = 8 * 1024 * 1024;
const Quota = Schema.Struct({
  starts: Schema.Array(Schema.Number.check(Schema.isFinite())),
  blockedUntil: Schema.Number.check(Schema.isFinite()),
});
const encodeQuota = Schema.encodeEffect(Schema.fromJsonString(Quota));
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeQuota = Schema.decodeUnknownEffect(Schema.fromJsonString(Quota));
const Response = Schema.Struct({
  results: Schema.Array(
    Schema.Struct({
      url: Schema.String,
      final_url: Schema.String,
      text: Schema.String.check(Schema.isMaxLength(4 * 1024 * 1024)),
      format: Schema.Literal("html"),
    }),
  ),
  errors: Schema.Array(Schema.Unknown),
  error: Schema.optional(Schema.Unknown),
  success: Schema.optional(Schema.Boolean),
});
const decodeResponse = Schema.decodeUnknownEffect(Schema.fromJsonString(Response));

export class TinyFishBudgetError extends Schema.TaggedError<TinyFishBudgetError>()(
  "TinyFishBudgetError",
  {
    reason: Schema.Literals(["minute", "day", "circuit"]),
    retryAfterMs: Schema.Number,
  },
) {
  override get message() {
    return `TinyFish fetch is paused by its shared ${this.reason} limit. Retry in ${Math.ceil(this.retryAfterMs / 1000)} seconds.`;
  }
}

export class TinyFishProviderError extends Schema.TaggedError<TinyFishProviderError>()(
  "TinyFishProviderError",
  {
    reason: Schema.Literals([
      "secret_unavailable",
      "quota_storage",
      "transport",
      "timeout",
      "http",
      "charge",
      "contract",
      "blocked_target",
      "resolve",
    ]),
    httpStatus: Schema.NullOr(Schema.Int),
  },
) {
  override get message() {
    return `TinyFish fetch failed (${this.reason}). No page content was accepted.`;
  }
}

export class TinyFish extends Context.Service<
  TinyFish,
  {
    readonly fetch: (input: {
      readonly url: URL;
      readonly sameOrigin?: string;
      // Latest deadline among active readers, rechecked before reserving quota.
      readonly deadline?: () => number;
    }) => Effect.Effect<
      | { readonly _tag: "DeadlineDeclined" }
      | {
          readonly _tag: "Fetched";
          readonly page: ReturnType<typeof extract>;
          readonly finalUrl: string;
          readonly callId: string;
          readonly chargedMicro: 0;
          readonly delayMs: number;
        }
      | undefined,
      TinyFishProviderError | TinyFishBudgetError
    >;
  }
>()("t3/webResearch/TinyFish") {}

const targetUrl = (value: string, sameOrigin?: string) => {
  const url = URL.parse(value);
  if (
    !url ||
    !["http:", "https:"].includes(url.protocol) ||
    url.username ||
    url.password ||
    url.href.length > 4096 ||
    (sameOrigin !== undefined && sameOrigin !== url.origin) ||
    /(^|\.)booking\.com$|(^|\.)airbnb\./i.test(url.hostname)
  )
    return undefined;
  url.hash = "";
  return url;
};
const retryAfter = (value: string | undefined, now: number) => {
  const seconds = value === undefined ? NaN : Number(value);
  const delay = Number.isFinite(seconds) ? seconds * 1000 : Date.parse(value ?? "") - now;
  return Number.isFinite(delay) ? Math.max(60_000, Math.min(DAY_MS, delay)) : 60_000;
};

const make = Effect.gen(function* () {
  const client = yield* HttpClient.HttpClient;
  const secrets = yield* ServerSecretStore.ServerSecretStore;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const config = yield* ServerConfig.ServerConfig;
  const directory = path.join(config.stateDir, "web-cache");
  const quotaPath = path.join(directory, "tinyfish-quota.json");
  const lock = yield* Semaphore.make(1);
  let quota: typeof Quota.Type | undefined;
  const storeQuota = Effect.fnUntraced(function* (value: typeof Quota.Type) {
    yield* fs.makeDirectory(directory, { recursive: true });
    yield* fs.writeFileString(`${quotaPath}.tmp`, yield* encodeQuota(value), { mode: 0o600 });
    yield* fs.rename(`${quotaPath}.tmp`, quotaPath);
    quota = value;
  });
  const reserve = (deadline: (() => number) | undefined) =>
    Effect.gen(function* () {
      if (quota === undefined) {
        const stored = yield* fs.readFileString(quotaPath).pipe(
          Effect.catchIf(
            (error) => error.reason._tag === "NotFound",
            () => Effect.succeed('{"starts":[],"blockedUntil":0}'),
          ),
        );
        quota = yield* decodeQuota(stored);
      }
      const now = yield* Clock.currentTimeMillis;
      // Admission uses the live subscribers' deadline after quota I/O and lock waiting.
      if (deadline !== undefined && now + FETCH_TIMEOUT_MS >= deadline()) return false;
      if (quota.blockedUntil > now)
        return yield* new TinyFishBudgetError({
          reason: "circuit",
          retryAfterMs: quota.blockedUntil - now,
        });
      const starts = quota.starts.filter((at) => at > now - DAY_MS).toSorted((a, b) => a - b);
      if (starts.length >= 1000)
        return yield* new TinyFishBudgetError({
          reason: "day",
          retryAfterMs: starts[0]! + DAY_MS - now,
        });
      const minute = starts.filter((at) => at > now - 60_000);
      if (minute.length >= 150)
        return yield* new TinyFishBudgetError({
          reason: "minute",
          retryAfterMs: minute[0]! + 60_000 - now,
        });
      // Reserve before sending; cancelled or uncertain requests still consumed shared-key capacity.
      yield* storeQuota({ starts: [...starts, now], blockedUntil: quota.blockedUntil });
      return true;
    }).pipe(
      lock.withPermits(1),
      Effect.mapError((error) =>
        error._tag === "TinyFishBudgetError"
          ? error
          : new TinyFishProviderError({ reason: "quota_storage", httpStatus: null }),
      ),
    );
  const block = (until: number) =>
    lock
      .withPermits(1)(
        Effect.gen(function* () {
          const blocked = {
            starts: quota?.starts ?? [],
            blockedUntil: Math.max(until, quota?.blockedUntil ?? 0),
          };
          quota = blocked;
          yield* storeQuota(blocked);
        }),
      )
      .pipe(
        Effect.mapError(
          () => new TinyFishProviderError({ reason: "quota_storage", httpStatus: 429 }),
        ),
      );
  // The provider documents private target/redirect rejection. Its connection pinning and
  // rendered subresources remain outside this requested/final URL admission check.
  const validate = Effect.fnUntraced(function* (url: URL) {
    yield* PublicHttp.validateDestination(url).pipe(
      Effect.mapError(
        (error) =>
          new TinyFishProviderError({
            reason: error._tag === "PublicNetworkBlockedError" ? "blocked_target" : "resolve",
            httpStatus: null,
          }),
      ),
    );
  });
  const fetch = Effect.fn("TinyFish.fetch")(
    function* (input: {
      readonly url: URL;
      readonly sameOrigin?: string;
      readonly deadline?: () => number;
    }) {
      const secret = yield* secrets
        .get(TREG_SECRET_NAME)
        .pipe(
          Effect.mapError(
            () => new TinyFishProviderError({ reason: "secret_unavailable", httpStatus: null }),
          ),
        );
      if (Option.isNone(secret)) return undefined;
      const token = new TextDecoder().decode(secret.value).trim();
      if (!/^[\x21-\x7e]{1,8192}$/.test(token))
        return yield* new TinyFishProviderError({ reason: "secret_unavailable", httpStatus: null });
      if (
        input.deadline !== undefined &&
        (yield* Clock.currentTimeMillis) + FETCH_TIMEOUT_MS >= input.deadline()
      )
        return { _tag: "DeadlineDeclined" as const };
      const url = targetUrl(input.url.href, input.sameOrigin);
      if (!url)
        return yield* new TinyFishProviderError({ reason: "blocked_target", httpStatus: null });
      yield* validate(url);
      const queuedAt = yield* Clock.currentTimeMillis;
      if (!(yield* reserve(input.deadline))) return { _tag: "DeadlineDeclined" as const };
      const delayMs = (yield* Clock.currentTimeMillis) - queuedAt;
      // Transport errors contain the authenticated request. Never retain them in domain errors.
      const response = yield* HttpClient.withScope(client)
        .post(ENDPOINT, {
          headers: {
            "X-Treg-Token": token,
            "X-Treg-Route-Max-Cost": "0",
            "content-type": "application/json",
          },
          body: HttpBody.text(
            encodeJson({
              urls: [url.href],
              format: "html",
              ttl: 0,
              per_url_timeout_ms: 15_000,
            }),
            "application/json",
          ),
        })
        .pipe(
          Effect.mapError(
            () => new TinyFishProviderError({ reason: "transport", httpStatus: null }),
          ),
        );
      const cost = response.headers["x-treg-cost-micro"];
      if (cost !== undefined && cost !== "0")
        return yield* new TinyFishProviderError({ reason: "charge", httpStatus: response.status });
      if (response.status === 429) {
        const now = yield* Clock.currentTimeMillis;
        const retryAfterMs = retryAfter(response.headers["retry-after"], now);
        yield* block(now + retryAfterMs);
        return yield* new TinyFishBudgetError({ reason: "circuit", retryAfterMs });
      }
      if (response.status !== 200)
        return yield* new TinyFishProviderError({
          reason: response.status === 402 ? "charge" : "http",
          httpStatus: response.status,
        });
      const callId = response.headers["x-treg-call-id"];
      if (
        cost !== "0" ||
        !callId ||
        !/^[A-Za-z0-9_-]{1,128}$/.test(callId) ||
        callId.includes(token)
      )
        return yield* new TinyFishProviderError({ reason: "charge", httpStatus: response.status });
      if (response.headers["x-treg-upstream-failed"] !== undefined)
        return yield* new TinyFishProviderError({
          reason: "contract",
          httpStatus: response.status,
        });
      const json = yield* response.text.pipe(
        Effect.provideService(HttpIncomingMessage.MaxBodySize, ByteSize.bytes(MAX_BYTES)),
        Effect.mapError(
          () => new TinyFishProviderError({ reason: "contract", httpStatus: response.status }),
        ),
      );
      // Reflected credentials are never extracted, retained in a cause, or returned to the tool.
      if (json.includes(token))
        return yield* new TinyFishProviderError({
          reason: "contract",
          httpStatus: response.status,
        });
      const parsed = yield* decodeResponse(json).pipe(
        Effect.mapError(
          () => new TinyFishProviderError({ reason: "contract", httpStatus: response.status }),
        ),
      );
      const page = parsed.results[0];
      const final = page && targetUrl(page.final_url, input.sameOrigin);
      if (
        parsed.errors.length !== 0 ||
        parsed.error != null ||
        parsed.success === false ||
        parsed.results.length !== 1 ||
        !page ||
        page.text.includes(token) ||
        page.final_url.includes(token) ||
        targetUrl(page.url)?.href !== url.href ||
        !final ||
        !/<(?:html|body|head|title|h[1-6]|p|div|main|article|table)\b/i.test(page.text)
      )
        return yield* new TinyFishProviderError({
          reason: "contract",
          httpStatus: response.status,
        });
      yield* validate(final);
      const document = yield* Effect.try({
        try: () => extract(page.text, final.href),
        catch: () => new TinyFishProviderError({ reason: "contract", httpStatus: response.status }),
      });
      if (document.text.includes(token) || document.title.includes(token))
        return yield* new TinyFishProviderError({
          reason: "contract",
          httpStatus: response.status,
        });
      return {
        _tag: "Fetched" as const,
        page: document,
        finalUrl: final.href,
        callId,
        chargedMicro: 0 as const,
        delayMs,
      };
    },
    Effect.scoped,
    Effect.provideServiceEffect(
      Headers.CurrentRedactedNames,
      Effect.map(Headers.CurrentRedactedNames, (names) => [...names, "x-treg-token"]),
    ),
    // Lower-level transport spans can retain unsanitized failures containing the request.
    Effect.provideService(HttpClient.TracerDisabledWhen, () => true),
    Effect.timeout(FETCH_TIMEOUT_MS),
    Effect.catchTags({
      TimeoutError: () =>
        Effect.fail(new TinyFishProviderError({ reason: "timeout", httpStatus: null })),
    }),
  );
  return TinyFish.of({ fetch });
});

export const layer = Layer.effect(TinyFish, make);
