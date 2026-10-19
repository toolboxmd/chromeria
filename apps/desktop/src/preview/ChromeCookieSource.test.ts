// @effect-diagnostics nodeBuiltinImport:off - Encrypts synthetic Chrome database fixtures.
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as NodeCrypto from "node:crypto";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import { ChildProcessSpawner } from "effect/process";
import { vi } from "vite-plus/test";

import {
  readChromeCookieSnapshot,
  readChromeDeviceBoundSites,
  readChromeProcessIdentity,
} from "./ChromeCookieSource.ts";

const key = Buffer.from("0123456789abcdef");
const encrypted = (host: string, value: string) => {
  const cipher = NodeCrypto.createCipheriv("aes-128-cbc", key, Buffer.alloc(16, 0x20));
  const bound = Buffer.concat([
    NodeCrypto.createHash("sha256").update(host).digest(),
    Buffer.from(value),
  ]);
  return Buffer.concat([Buffer.from("v10"), cipher.update(bound), cipher.final()]);
};
const fixture = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const dir = yield* fs.makeTempDirectoryScoped({ prefix: "chromeria-mirror-chrome-test-" });
  const database = `${dir}/Cookies`;
  return { fs, dir, database, layer: NodeSqliteClient.layer({ filename: database }) };
});
const schema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`pragma journal_mode = WAL`;
  yield* sql`pragma wal_autocheckpoint = 0`;
  yield* sql`create table meta (key text primary key, value text not null)`;
  yield* sql`insert into meta values ('version', '24')`;
  yield* sql`create table cookies (
    host_key text, name text, value text, encrypted_value blob, path text,
    expires_utc integer, is_secure integer, is_httponly integer, samesite integer,
    top_frame_site_key text, has_cross_site_ancestor integer, is_persistent integer
  )`;
});

describe("Chrome snapshot observation", () => {
  it.effect(
    "identifies the local SingletonLock process by PID and start time without trusting stale or foreign locks",
    () =>
      Effect.gen(function* () {
        let target = "synthetic-mac-42";
        let started = "Mon Oct 19 08:00:00 2026\n";
        const inspect = vi.fn<ChildProcessSpawner.ChildProcessSpawner["Service"]["string"]>(() =>
          Effect.succeed(started),
        );
        const probe = readChromeProcessIdentity("/synthetic/chrome").pipe(
          Effect.provideService(HostProcess.Hostname, "synthetic-mac"),
          Effect.provide(
            Layer.mergeAll(
              Layer.succeed(
                FileSystem.FileSystem,
                FileSystem.makeNoop({ readLink: () => Effect.succeed(target) }),
              ),
              Layer.mock(ChildProcessSpawner.ChildProcessSpawner, { string: inspect }),
            ),
          ),
        );
        const first = yield* probe;
        expect(first).toBe("synthetic-mac-42:Mon Oct 19 08:00:00 2026");
        expect(inspect).toHaveBeenCalledWith(
          expect.objectContaining({
            command: "/bin/ps",
            args: ["-p", "42", "-o", "lstart="],
          }),
        );
        started = "Mon Oct 19 09:00:00 2026\n";
        expect(yield* probe).not.toBe(first);
        started = "";
        expect(yield* probe).toBeUndefined();
        inspect.mockClear();
        for (const invalid of [
          "another-mac-42",
          "synthetic-mac-0",
          "synthetic-mac-99999999999999999999",
          "invalid",
          "synthetic-mac-42;echo",
        ]) {
          target = invalid;
          expect(yield* probe).toBeUndefined();
        }
        expect(inspect).not.toHaveBeenCalled();
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("reports each DBSC site without selecting its credential protobuf", () =>
    Effect.gen(function* () {
      const { database, layer } = yield* fixture;
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`create table dbsc_session_tbl (key text primary key, proto blob)`;
        yield* sql`insert into dbsc_session_tbl values ('https://google.com', ${Buffer.from("synthetic-private-binding")}), ('https://work.example.test', ${Buffer.from("invalid-protobuf")})`;
        expect((yield* readChromeDeviceBoundSites(database)).sort()).toEqual([
          "google.com",
          "work.example.test",
        ]);
      }).pipe(Effect.provide(layer));
    }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
  );

  it.effect(
    "reads committed WAL rows while the source remains open, preserving full keys and attributes",
    () =>
      Effect.gen(function* () {
        const { database, layer, fs } = yield* fixture;
        yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* schema;
          yield* sql`insert into cookies values ('login.example.test', '__Host-session', '', ${encrypted("login.example.test", "synthetic-session")}, '/', 0, 1, 1, -1, '', 0, 0)`;
          yield* sql`insert into cookies values ('.example.test', 'partitioned', '', ${encrypted(".example.test", "synthetic-partitioned")}, '/account', 13644473600123456, 1, 1, 0, 'https://top.test', 0, 1)`;
          expect(yield* fs.exists(`${database}-wal`)).toBe(true);
          const result = yield* readChromeCookieSnapshot(database, { cbcV10: key });
          expect(result.complete).toBe(true);
          expect([...result.cookies].sort((a, b) => a.domain.localeCompare(b.domain))).toEqual(
            [
              {
                domain: ".example.test",
                name: "partitioned",
                value: "synthetic-partitioned",
                path: "/account",
                secure: true,
                httpOnly: true,
                sameSite: "no_restriction",
                expires: 2_000_000_000.123456,
                partitionKey: { topLevelSite: "https://top.test", hasCrossSiteAncestor: false },
              },
              {
                domain: "login.example.test",
                name: "__Host-session",
                value: "synthetic-session",
                path: "/",
                secure: true,
                httpOnly: true,
                sameSite: "unspecified",
              },
            ].sort((a, b) => a.domain.localeCompare(b.domain)),
          );
          expect(result.identities).toHaveLength(2);
        }).pipe(Effect.provide(layer));
      }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
  );

  it.effect("retains unreadable row identities and rejects macOS encryption other than v10", () =>
    Effect.gen(function* () {
      const { database, layer } = yield* fixture;
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* schema;
        yield* sql`insert into cookies values ('readable.test', 'ok', 'plain-synthetic', ${new Uint8Array()}, '/', 0, 0, 0, 1, '', 0, 0)`;
        yield* sql`insert into cookies values ('unknown.test', 'unknown', '', ${Buffer.from("v20synthetic-unsupported")}, '/', 0, 1, 1, 0, 'https://top.test', 1, 0)`;
        yield* sql`insert into cookies values ('broken.test', 'bad-key', '', ${Buffer.from("v10bad-ciphertext")}, '/', 0, 1, 1, 0, '', 0, 0)`;
        const result = yield* readChromeCookieSnapshot(database, { cbcV10: key });
        expect(result.complete).toBe(false);
        expect(result.cookies.map((cookie) => cookie.name)).toEqual(["ok"]);
        expect(result.identities.map((identity) => identity.key.name).sort()).toEqual([
          "bad-key",
          "ok",
          "unknown",
        ]);
        expect(
          result.identities.find((identity) => identity.key.name === "unknown")?.key.partitionKey,
        ).toEqual({ topLevelSite: "https://top.test", hasCrossSiteAncestor: true });
      }).pipe(Effect.provide(layer));
    }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
  );

  it.effect("returns failure for a broken database rather than an empty observation", () =>
    Effect.gen(function* () {
      const { database, fs } = yield* fixture;
      yield* fs.writeFileString(database, "synthetic-invalid-sqlite");
      const result = yield* readChromeCookieSnapshot(database, { cbcV10: key }).pipe(Effect.result);
      expect(result._tag).toBe("Failure");
    }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
  );
});
