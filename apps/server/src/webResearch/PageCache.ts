// @effect-diagnostics nodeBuiltinImport:off - compressed disposable cache bodies use Node's bounded zlib codec.
import * as NodeZlib from "node:zlib";
import * as NodeUtil from "node:util";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/sql/SqlClient";

import * as ServerConfig from "../config.ts";
import { CachedPage } from "./schemas.ts";

const gzip = NodeUtil.promisify(NodeZlib.gzip);
const gunzip = NodeUtil.promisify(NodeZlib.gunzip);
const decode = Schema.decodeUnknownEffect(Schema.fromJsonString(CachedPage));
const encode = Schema.encodeEffect(Schema.fromJsonString(CachedPage));
// JSON escapes control characters in both page text and the retained link labels.
// This limits encoded payload bytes, not total live heap used by strings and link objects.
const MAX_CACHE_JSON_BYTES = 64 * 1024 * 1024;

export const HOSTED_CONTINUATION_TTL_MS = 15 * 60_000;

export class PageContinuationExpired extends Schema.TaggedError<PageContinuationExpired>()(
  "PageContinuationExpired",
  {},
) {
  override get message() {
    return "That TinyFish continuation expired, was evicted, or was lost on restart. Hosted pages stay only in this research server’s memory for at most 15 minutes. Read the URL again.";
  }
}

type CacheKey =
  | { readonly url: string; readonly sameOrigin?: string }
  | { readonly redirectTarget: string; readonly sameOrigin: string }
  | { readonly cacheId: string };

export class PageCacheError extends Schema.TaggedError<PageCacheError>()("PageCacheError", {
  operation: Schema.Literals(["open", "read", "write"]),
  cause: Schema.Defect(),
}) {
  override get message() {
    if (this.operation === "open")
      return "The research page cache is unavailable; web_read is disabled. Repair the cache and restart the server to retry opening it.";
    return "The research page cache could not be read or written.";
  }
}

export class PageCache extends Context.Service<
  PageCache,
  {
    readonly get: (
      key: CacheKey,
    ) => Effect.Effect<CachedPage | null, PageCacheError | PageContinuationExpired>;
    readonly put: (page: CachedPage) => Effect.Effect<void, PageCacheError>;
  }
>()("t3/webResearch/PageCache") {}

const make = Effect.gen(function* () {
  const ownerScope = yield* Scope.Scope;
  const sql = yield* SqlClient.SqlClient;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const config = yield* ServerConfig.ServerConfig;
  const directory = path.join(config.stateDir, "web-cache");
  yield* Effect.gen(function* () {
    yield* sql`CREATE TABLE IF NOT EXISTS pages (id TEXT PRIMARY KEY, url TEXT NOT NULL, fetched_at REAL NOT NULL)`;
    const columns = yield* sql<{ readonly name: string }>`PRAGMA table_info(pages)`;
    // Older core caches have only URL/time metadata; their unrestricted entries stay readable.
    if (!columns.some((column) => column.name === "restriction_origin"))
      yield* sql`ALTER TABLE pages ADD COLUMN restriction_origin TEXT`;
    if (!columns.some((column) => column.name === "final_origin"))
      yield* sql`ALTER TABLE pages ADD COLUMN final_origin TEXT`;
    if (!columns.some((column) => column.name === "final_url"))
      yield* sql`ALTER TABLE pages ADD COLUMN final_url TEXT`;
    if (!columns.some((column) => column.name === "indexed_at")) {
      yield* sql`ALTER TABLE pages ADD COLUMN indexed_at REAL`;
      yield* sql`UPDATE pages SET indexed_at = fetched_at`;
    }
    yield* sql`CREATE INDEX IF NOT EXISTS pages_url ON pages(url, fetched_at DESC)`;
    yield* sql`CREATE INDEX IF NOT EXISTS pages_url_indexed ON pages(url, indexed_at DESC)`;
    yield* sql`CREATE INDEX IF NOT EXISTS pages_final_url ON pages(final_url, indexed_at DESC)`;
  }).pipe(Effect.mapError((cause) => new PageCacheError({ operation: "open", cause })));

  // Provider retention permission, including these transient buffers, remains unresolved.
  // Final admission needs that acceptance; hosted bodies never enter SQLite or compressed files.
  const continuations = new Map<string, { page: CachedPage; bytes: number }>();
  let continuationBytes = 0;
  const forget = (id: string) => {
    continuationBytes -= continuations.get(id)?.bytes ?? 0;
    continuations.delete(id);
  };
  const prune = (now: number) => {
    for (const [id, entry] of continuations) if (entry.page.expiresAt <= now) forget(id);
  };

  const expire = (id: string, expiresAt: number) =>
    Clock.currentTimeMillis.pipe(
      Effect.flatMap((now) => Effect.sleep(Math.max(0, expiresAt - now))),
      Effect.andThen(
        Effect.sync(() => {
          if (continuations.get(id)?.page.expiresAt === expiresAt) forget(id);
        }),
      ),
    );
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      continuations.clear();
      continuationBytes = 0;
    }),
  );

  const get = Effect.fn("PageCache.get")(function* (key: CacheKey) {
    prune(yield* Clock.currentTimeMillis);
    if ("cacheId" in key) {
      const buffered = continuations.get(key.cacheId);
      if (buffered) return buffered.page;
      if (key.cacheId.startsWith("tinyfish:")) return yield* new PageContinuationExpired();
    }
    const rows = yield* (
      "redirectTarget" in key
        ? sql<{ readonly id: string }>`SELECT id FROM pages
            WHERE (url = ${key.redirectTarget} OR final_url = ${key.redirectTarget})
            AND (restriction_origin = ${key.sameOrigin} OR final_origin = ${key.sameOrigin} OR final_origin IS NULL)
            ORDER BY indexed_at DESC, rowid DESC LIMIT 1`
        : "url" in key
          ? key.sameOrigin === undefined
            ? sql<{ readonly id: string }>`SELECT id FROM pages WHERE url = ${key.url}
              AND (restriction_origin IS NULL OR final_origin = ${new URL(key.url).origin})
              ORDER BY indexed_at DESC, rowid DESC LIMIT 1`
            : sql<{ readonly id: string }>`SELECT id FROM pages WHERE url = ${key.url}
              AND (restriction_origin = ${key.sameOrigin} OR final_origin = ${key.sameOrigin} OR final_origin IS NULL)
              ORDER BY indexed_at DESC, rowid DESC LIMIT 1`
          : sql<{ readonly id: string }>`SELECT id FROM pages WHERE id = ${key.cacheId}`
    ).pipe(Effect.mapError((cause) => new PageCacheError({ operation: "read", cause })));
    const id = rows[0]?.id;
    if (id === undefined) return null;
    // The index is disposable, but a corrupted row must not become a filesystem path.
    if (!/^[a-f0-9]{64}$/.test(id))
      return yield* new PageCacheError({
        operation: "read",
        cause: new Error("Invalid cache id."),
      });
    const compressed = yield* fs
      .readFile(path.join(directory, `${id}.gz`))
      .pipe(Effect.mapError((cause) => new PageCacheError({ operation: "read", cause })));
    const json = yield* Effect.tryPromise({
      try: () => gunzip(compressed, { maxOutputLength: MAX_CACHE_JSON_BYTES }),
      catch: (cause) => new PageCacheError({ operation: "read", cause }),
    });
    const page = yield* decode(json.toString("utf8")).pipe(
      Effect.mapError((cause) => new PageCacheError({ operation: "read", cause })),
    );
    return page.sourceRung === "tinyfish" ? null : page;
  });
  const put = Effect.fn("PageCache.put")(function* (page: CachedPage) {
    const json = yield* encode(page).pipe(
      Effect.mapError((cause) => new PageCacheError({ operation: "write", cause })),
    );
    if (Buffer.byteLength(json, "utf8") > MAX_CACHE_JSON_BYTES)
      return yield* new PageCacheError({
        operation: "write",
        cause: new RangeError("Research page exceeds the cache body limit."),
      });
    if (page.sourceRung === "tinyfish") {
      const now = yield* Clock.currentTimeMillis;
      prune(now);
      if (continuations.has(page.cacheId)) return;
      const expiresAt = Math.min(page.expiresAt, now + HOSTED_CONTINUATION_TTL_MS);
      if (expiresAt <= now) return;
      const bytes = Buffer.byteLength(json, "utf8");
      for (const id of continuations.keys()) {
        if (continuations.size < 32 && continuationBytes + bytes <= MAX_CACHE_JSON_BYTES) break;
        forget(id);
      }
      continuations.set(page.cacheId, {
        page: { ...page, expiresAt },
        bytes,
      });
      continuationBytes += bytes;
      // The cache owns expiry even after the request scope closes. The timer captures only identity/time.
      yield* expire(page.cacheId, expiresAt).pipe(Effect.forkIn(ownerScope));
      return;
    }
    const body = yield* Effect.tryPromise({
      try: () => gzip(json),
      catch: (cause) => new PageCacheError({ operation: "write", cause }),
    });
    yield* fs
      .writeFile(path.join(directory, `${page.cacheId}.gz`), body)
      .pipe(Effect.mapError((cause) => new PageCacheError({ operation: "write", cause })));
    // Publish the index after the body exists. Cache ids are immutable across refreshes.
    // Redirects may reuse an older target body; index order follows publication, not body age.
    const indexedAt = yield* Clock.currentTimeMillis;
    // A refused redirect never fetched its target, so only successful bodies advertise final URLs.
    const reusableFinalUrl = page.status === "ok" ? page.finalUrl : null;
    yield* sql`INSERT OR REPLACE INTO pages (id, url, fetched_at, restriction_origin, final_origin, final_url, indexed_at)
      VALUES (${page.cacheId}, ${page.url}, ${page.fetchedAt}, ${page.restrictedToOrigin ?? null}, ${new URL(page.finalUrl).origin}, ${reusableFinalUrl}, ${indexedAt})`.pipe(
      Effect.mapError((cause) => new PageCacheError({ operation: "write", cause })),
    );
  });
  return PageCache.of({ get, put });
});

export const layer = Layer.unwrap(
  Effect.gen(function* () {
    const config = yield* ServerConfig.ServerConfig;
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = path.join(config.stateDir, "web-cache");
    yield* fs
      .makeDirectory(directory, { recursive: true })
      .pipe(Effect.mapError((cause) => new PageCacheError({ operation: "open", cause })));
    return Layer.effect(PageCache, make).pipe(
      Layer.provide(NodeSqliteClient.layer({ filename: path.join(directory, "index.sqlite") })),
      Layer.catchTag("SqlError", (cause) =>
        Layer.effect(PageCache, Effect.fail(new PageCacheError({ operation: "open", cause }))),
      ),
    );
  }),
).pipe(
  Layer.catchTag("PageCacheError", (error) =>
    Layer.effect(
      PageCache,
      Effect.logWarning(error.message, error.cause).pipe(
        Effect.as(
          PageCache.of({
            get: () => Effect.fail(error),
            put: () => Effect.fail(error),
          }),
        ),
      ),
    ),
  ),
);
