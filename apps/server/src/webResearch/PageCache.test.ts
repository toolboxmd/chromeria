// @effect-diagnostics nodeBuiltinImport:off - fixtures build gzip bodies directly.
import * as NodeZlib from "node:zlib";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";

import * as Config from "../config.ts";
import * as PageCache from "./PageCache.ts";
import type { CachedPage } from "./schemas.ts";

const page = (
  id: number,
  sourceRung: CachedPage["sourceRung"] = "tinyfish",
  text = `HOSTED-ONLY-${id}`,
): CachedPage => ({
  cacheId: `${sourceRung === "tinyfish" ? "tinyfish:" : ""}${id.toString(16).padStart(64, "0")}`,
  url: `https://example.com/${sourceRung}`,
  finalUrl: `https://example.com/${sourceRung}`,
  fetchedAt: 0,
  expiresAt: sourceRung === "tinyfish" ? 900_000 : 604_800_000,
  sourceRung,
  status: "ok",
  httpStatus: sourceRung === "tinyfish" ? null : 200,
  title: "Fixture",
  text,
  contentHash: "b".repeat(64),
  delayMs: 0,
  retryAfterMs: null,
  links: [],
});
const setup = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "tinyfish-cache-" });
  const config = Config.layerTest(process.cwd(), root);
  const layer = PageCache.layer.pipe(Layer.provide(config));
  const paths = yield* Config.deriveServerPaths(root, undefined);
  return { fs, layer, directory: `${paths.stateDir}/web-cache` };
});

it.effect(
  "never writes hosted bodies or index rows, preserves local persistence, and loses hosted text on restart",
  () =>
    Effect.gen(function* () {
      const f = yield* setup;
      const hosted = page(1);
      const local = page(2, "http", "LOCAL-PERSISTENT-PAGE");
      yield* Effect.gen(function* () {
        const cache = yield* PageCache.PageCache;
        yield* cache.put(local);
        yield* cache.put(hosted);
        expect(yield* cache.get({ cacheId: hosted.cacheId })).toEqual(hosted);
        expect(yield* cache.get({ url: hosted.url })).toBeNull();
      }).pipe(Effect.provide(f.layer));
      const files = yield* f.fs.readDirectory(f.directory);
      expect(files.filter((p) => p.endsWith(".gz"))).toEqual([`${local.cacheId}.gz`]);
      const rows = yield* SqlClient.SqlClient.use((sql) => sql`SELECT id, url FROM pages`).pipe(
        Effect.provide(NodeSqliteClient.layer({ filename: `${f.directory}/index.sqlite` })),
      );
      expect(rows).toEqual([{ id: local.cacheId, url: local.url }]);
      for (const name of files) {
        const bytes = yield* f.fs.readFile(`${f.directory}/${name}`);
        expect(Buffer.from(bytes).includes(Buffer.from(hosted.text))).toBe(false);
      }
      yield* Effect.gen(function* () {
        const cache = yield* PageCache.PageCache;
        expect(yield* cache.get({ url: local.url })).toEqual(local);
        const missing = yield* Effect.result(cache.get({ cacheId: hosted.cacheId }));
        expect(missing).toMatchObject({
          _tag: "Failure",
          failure: { _tag: "PageContinuationExpired" },
        });
        if (missing._tag === "Failure")
          expect(missing.failure.message).toContain("Read the URL again");
      }).pipe(Effect.provide(Layer.fresh(f.layer)));
    }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "keeps original hosted text and identity stable across a refresh until exactly fifteen minutes",
  () =>
    Effect.gen(function* () {
      const f = yield* setup;
      yield* Effect.gen(function* () {
        const cache = yield* PageCache.PageCache;
        const original = page(1);
        yield* cache.put(original);
        yield* TestClock.adjust(60_000);
        yield* cache.put({
          ...original,
          text: "MUST-NOT-REPLACE-IMMUTABLE-CONTINUATION",
          expiresAt: 960_000,
        });
        yield* cache.put({ ...page(2), fetchedAt: 60_000, expiresAt: 960_000 });
        yield* TestClock.adjust(839_999);
        expect(yield* cache.get({ cacheId: original.cacheId })).toEqual(original);
        expect(yield* Clock.currentTimeMillis).toBe(899_999);
        yield* TestClock.adjust(1);
        expect(yield* Effect.result(cache.get({ cacheId: original.cacheId }))).toMatchObject({
          _tag: "Failure",
          failure: { _tag: "PageContinuationExpired" },
        });
        expect((yield* cache.get({ cacheId: page(2).cacheId }))?.text).toBe("HOSTED-ONLY-2");
      }).pipe(Effect.provide(f.layer));
    }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "bounds hosted buffers by entry count and total bytes and never resets their lifetime on access",
  () =>
    Effect.gen(function* () {
      const f = yield* setup;
      yield* Effect.gen(function* () {
        const cache = yield* PageCache.PageCache;
        for (let id = 1; id <= 33; id++) yield* cache.put(page(id));
        expect(yield* Effect.result(cache.get({ cacheId: page(1).cacheId }))).toMatchObject({
          _tag: "Failure",
          failure: { _tag: "PageContinuationExpired" },
        });
        expect(yield* cache.get({ cacheId: page(33).cacheId })).toEqual(page(33));
        const fullText = "x".repeat(4 * 1024 * 1024);
        for (let id = 34; id <= 50; id++) yield* cache.put(page(id, "tinyfish", fullText));
        expect(yield* Effect.result(cache.get({ cacheId: page(34).cacheId }))).toMatchObject({
          _tag: "Failure",
          failure: { _tag: "PageContinuationExpired" },
        });
        expect((yield* cache.get({ cacheId: page(50).cacheId }))?.text).toBe(fullText);
        yield* TestClock.adjust(899_999);
        expect((yield* cache.get({ cacheId: page(50).cacheId }))?.text).toBe(fullText);
        yield* TestClock.adjust(1);
        expect(yield* Effect.result(cache.get({ cacheId: page(50).cacheId }))).toMatchObject({
          _tag: "Failure",
          failure: { _tag: "PageContinuationExpired" },
        });
      }).pipe(Effect.provide(f.layer));
    }).pipe(Effect.provide(NodeServices.layer)),
);

const indexRows = (directory: string) =>
  SqlClient.SqlClient.use((sql) => sql<{ readonly id: string }>`SELECT id FROM pages`).pipe(
    Effect.provide(NodeSqliteClient.layer({ filename: `${directory}/index.sqlite` })),
  );

it.effect("treats a missing, empty, truncated or undecodable body as a miss and drops it", () =>
  Effect.gen(function* () {
    const f = yield* setup;
    const notAPage = NodeZlib.gzipSync(JSON.stringify({ not: "a cached page" }));
    const corruptions: ReadonlyArray<Uint8Array | null> = [
      null,
      new Uint8Array(),
      NodeZlib.gzipSync("x".repeat(4096)).subarray(0, 20),
      notAPage,
    ];
    for (const [index, body] of corruptions.entries()) {
      const local = page(index + 1, "http", `LOCAL-${index}`);
      const blob = `${f.directory}/${local.cacheId}.gz`;
      yield* Effect.gen(function* () {
        const cache = yield* PageCache.PageCache;
        yield* cache.put(local);
        if (body === null) yield* f.fs.remove(blob);
        else yield* f.fs.writeFile(blob, body);
        expect(yield* cache.get({ url: local.url })).toBeNull();
        expect(yield* indexRows(f.directory)).toEqual([]);
        expect(yield* f.fs.exists(blob)).toBe(false);
        yield* cache.put(local);
        expect(yield* cache.get({ cacheId: local.cacheId })).toEqual(local);
        yield* f.fs.writeFile(blob, new Uint8Array());
        expect(yield* cache.get({ cacheId: local.cacheId })).toBeNull();
        expect(yield* indexRows(f.directory)).toEqual([]);
      }).pipe(Effect.provide(Layer.fresh(f.layer)));
    }
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("publishes no index row or partial body when a write fails", () =>
  Effect.gen(function* () {
    const f = yield* setup;
    const local = page(1, "http");
    // A directory at the body path makes the body write fail.
    yield* f.fs.makeDirectory(`${f.directory}/${local.cacheId}.gz`, { recursive: true });
    yield* Effect.gen(function* () {
      const cache = yield* PageCache.PageCache;
      expect(yield* Effect.result(cache.put(local))).toMatchObject({
        _tag: "Failure",
        failure: { _tag: "PageCacheError", operation: "write" },
      });
    }).pipe(Effect.provide(f.layer));
    expect(yield* indexRows(f.directory)).toEqual([]);
  }).pipe(Effect.provide(NodeServices.layer)),
);
