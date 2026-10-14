// @effect-diagnostics nodeBuiltinImport:off - fixture for the copied Chromium SQLite index.
import * as NodeSqlite from "node:sqlite";
import * as NodeVM from "node:vm";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { renewalCommitment } from "@t3tools/shared/sessionRenewal";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Layer from "effect/Layer";
import * as Reader from "./BrowserRenewalCommitments.ts";

it.effect(
  "the fixed reader returns public hashes from fake IndexedDB without exposing credentials",
  () =>
    Effect.tryPromise(async () => {
      let closed = false;
      const records = [
        {
          active: {
            environmentId: "paired",
            origin: "http://paired.test",
            epoch: "one",
            current: "a".repeat(43),
            accessToken: "private-access",
          },
          provisional: [],
        },
      ];
      const request = <T>(result: T) => {
        const value: { result: T; onsuccess?: () => void } = { result };
        queueMicrotask(() => value.onsuccess?.());
        return value;
      };
      const result = await NodeVM.runInNewContext(Reader.READ_EXPRESSION, {
        indexedDB: {
          open: () =>
            request({
              transaction: () => ({ objectStore: () => ({ getAll: () => request(records) }) }),
              close: () => {
                closed = true;
              },
            }),
        },
        t3RenewalHash: (secret: string) => Promise.resolve(renewalCommitment(secret)),
      });
      expect(result).toEqual([
        {
          environmentId: "paired",
          origin: "http://paired.test",
          epoch: "one",
          commitment: "66d34fba71f8f450f7e45598853e53bfc23bbd129027cbb131a2f4ffd7878cd0",
        },
      ]);
      expect(closed).toBe(true);
    }),
);

it.effect(
  "copies only the selected origin's storage and cleans up without launching a browser",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const source = yield* fs.makeTempDirectoryScoped({ prefix: "t3-profile-fixture-" });
      for (const directory of [
        "IndexedDB/http_paired.test_0.indexeddb.leveldb",
        "WebStorage/7/IndexedDB",
        "WebStorage/8/IndexedDB",
      ]) {
        yield* fs.makeDirectory(path.join(source, directory), { recursive: true });
        yield* fs.writeFileString(path.join(source, directory, "fixture"), directory);
      }
      yield* fs.writeFileString(path.join(source, "Cookies"), "never copy cookies");
      yield* Effect.sync(() => {
        const db = new NodeSqlite.DatabaseSync(path.join(source, "WebStorage/QuotaManager"));
        try {
          db.exec("CREATE TABLE buckets (id INTEGER, storage_key TEXT)");
          db.prepare("INSERT INTO buckets VALUES (?, ?)").run(7, "http://paired.test/");
          db.prepare("INSERT INTO buckets VALUES (?, ?)").run(8, "https://unrelated.test/");
        } finally {
          db.close();
        }
      });
      let copied = "";
      const engine = Layer.succeed(Reader.BrowserRenewalEngine, {
        read: (input) =>
          Effect.gen(function* () {
            copied = input.profileDirectory;
            const profile = path.join(copied, "Default");
            expect(
              yield* fs.readFileString(
                path.join(profile, "IndexedDB/http_paired.test_0.indexeddb.leveldb/fixture"),
              ),
            ).toContain("paired.test");
            expect(yield* fs.exists(path.join(profile, "WebStorage/7/IndexedDB/fixture"))).toBe(
              true,
            );
            expect(yield* fs.exists(path.join(profile, "WebStorage/8"))).toBe(false);
            expect(yield* fs.exists(path.join(profile, "Cookies"))).toBe(false);
            return [
              {
                environmentId: "paired",
                origin: "http://paired.test",
                epoch: "device",
                commitment: renewalCommitment("a".repeat(43)),
              },
            ];
          }).pipe(Effect.mapError((cause) => new Reader.BrowserCommitmentReadError({ cause }))),
      });
      const rows = yield* Reader.read({
        executable: "/not-executed",
        profileDirectory: source,
        origin: "http://paired.test",
      }).pipe(Effect.provide(engine), Effect.scoped);
      expect(rows).toEqual([
        {
          environmentId: "paired",
          origin: "http://paired.test",
          epoch: "device",
          commitment: "66d34fba71f8f450f7e45598853e53bfc23bbd129027cbb131a2f4ffd7878cd0",
        },
      ]);
      expect(yield* fs.exists(copied)).toBe(false);
      expect(yield* fs.readFileString(path.join(source, "Cookies"))).toBe("never copy cookies");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
