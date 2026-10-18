// @effect-diagnostics nodeBuiltinImport:off - compressed disposable cache bodies use Node's bounded zlib codec.
import * as NodeZlib from "node:zlib";
import * as NodeUtil from "node:util";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import * as ServerConfig from "../config.ts";
import { CachedPage } from "./schemas.ts";

const gzip = NodeUtil.promisify(NodeZlib.gzip);
const gunzip = NodeUtil.promisify(NodeZlib.gunzip);
const decode = Schema.decodeUnknownEffect(Schema.fromJsonString(CachedPage));
const encode = Schema.encodeEffect(Schema.fromJsonString(CachedPage));

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
      key: { readonly url: string } | { readonly cacheId: string },
    ) => Effect.Effect<CachedPage | null, PageCacheError>;
    readonly put: (page: CachedPage) => Effect.Effect<void, PageCacheError>;
  }
>()("t3/webResearch/PageCache") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const config = yield* ServerConfig.ServerConfig;
  const directory = path.join(config.stateDir, "web-cache");
  yield* sql`CREATE TABLE IF NOT EXISTS pages (id TEXT PRIMARY KEY, url TEXT NOT NULL, fetched_at REAL NOT NULL)`.pipe(
    Effect.andThen(sql`CREATE INDEX IF NOT EXISTS pages_url ON pages(url, fetched_at DESC)`),
    Effect.mapError((cause) => new PageCacheError({ operation: "open", cause })),
  );

  const get = Effect.fn("PageCache.get")(function* (
    key: { readonly url: string } | { readonly cacheId: string },
  ) {
    const rows = yield* (
      "url" in key
        ? sql<{
            readonly id: string;
          }>`SELECT id FROM pages WHERE url = ${key.url} ORDER BY fetched_at DESC, rowid DESC LIMIT 1`
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
      try: () => gunzip(compressed, { maxOutputLength: 32 * 1024 * 1024 }),
      catch: (cause) => new PageCacheError({ operation: "read", cause }),
    });
    return yield* decode(json.toString("utf8")).pipe(
      Effect.mapError((cause) => new PageCacheError({ operation: "read", cause })),
    );
  });
  const put = Effect.fn("PageCache.put")(function* (page: CachedPage) {
    const json = yield* encode(page).pipe(
      Effect.mapError((cause) => new PageCacheError({ operation: "write", cause })),
    );
    const body = yield* Effect.tryPromise({
      try: () => gzip(json),
      catch: (cause) => new PageCacheError({ operation: "write", cause }),
    });
    yield* fs
      .writeFile(path.join(directory, `${page.cacheId}.gz`), body)
      .pipe(Effect.mapError((cause) => new PageCacheError({ operation: "write", cause })));
    // Publish the index after the body exists. Cache ids are immutable across refreshes.
    yield* sql`INSERT OR REPLACE INTO pages (id, url, fetched_at) VALUES (${page.cacheId}, ${page.url}, ${page.fetchedAt})`.pipe(
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
