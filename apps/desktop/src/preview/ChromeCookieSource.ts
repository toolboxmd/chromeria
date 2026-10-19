import { CookieMirrorError, MirrorCookie } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import { decryptChromiumValue } from "./BrowserImport/ChromiumCookies.ts";
import { type ChromiumKeyMaterial } from "./BrowserImport/ChromiumKeys.ts";
import type { ChromeCookieSnapshot } from "./CookieMirror.ts";
import type { ManagedCookieKey } from "./CookieMirrorMetadata.ts";
import { snapshotCookieDatabase } from "./BrowserImport/CookieDatabase.ts";

const Row = Schema.Struct({
  host_key: Schema.String,
  name: Schema.String,
  value: Schema.String,
  encrypted_value: Schema.Uint8Array,
  path: Schema.String,
  expires_seconds: Schema.Number,
  is_secure: Schema.Number,
  is_httponly: Schema.Number,
  samesite: Schema.Number,
  top_frame_site_key: Schema.String,
  has_cross_site_ancestor: Schema.Number,
  is_persistent: Schema.Number,
});
const decodeRows = Schema.decodeUnknownEffect(Schema.Array(Row));
const decodeVersion = Schema.decodeUnknownEffect(
  Schema.Array(Schema.Struct({ value: Schema.Union([Schema.Number, Schema.FiniteFromString]) })),
);
const decodeCookies = Schema.decodeUnknownEffect(Schema.Array(MirrorCookie));

/** PID plus start time distinguishes a live Chrome logout from relaunch cleanup. */
export const readChromeProcessIdentity = Effect.fnUntraced(
  function* (root: string) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const host = yield* HostProcess.Hostname;
    const target = yield* fs.readLink(path.join(root, "SingletonLock"));
    const match = /^(.*)-(\d+)$/.exec(target);
    if (!match || match[1] !== host) return undefined;
    const pid = Number(match[2]);
    if (!Number.isSafeInteger(pid) || pid <= 0) return undefined;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const environment = yield* HostProcess.Environment;
    const started = (yield* spawner.string(
      ChildProcess.make("/bin/ps", ["-p", String(pid), "-o", "lstart="], {
        env: { ...environment, LC_ALL: "C" },
        stdin: "ignore",
      }),
    )).trim();
    return started ? `${target}:${started}` : undefined;
  },
  Effect.catchCause(() => Effect.succeed(undefined)),
);

/** Chrome stores the site in the key; never select the credential protobuf. */
export const readChromeDeviceBoundSites = Effect.fnUntraced(function* (source: string) {
  return yield* Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql`select key from dbsc_session_tbl`.pipe(
      Effect.flatMap(
        Schema.decodeUnknownEffect(Schema.Array(Schema.Struct({ key: Schema.String }))),
      ),
    );
    const sites = new Set<string>();
    for (const row of rows) {
      const site = yield* Effect.try(() => new URL(row.key));
      if (
        (site.protocol !== "https:" && site.protocol !== "http:") ||
        site.username ||
        site.password ||
        site.pathname !== "/" ||
        site.search ||
        site.hash
      )
        return yield* new CookieMirrorError({ reason: "unavailable" });
      sites.add(site.hostname);
    }
    return [...sites];
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: source, readonly: true })));
});

/** Unreadable rows protect their identities while unrelated absence stays observable. */
export const readChromeCookieSnapshot = Effect.fnUntraced(function* (
  source: string,
  keys: ChromiumKeyMaterial,
) {
  const snapshot = yield* snapshotCookieDatabase(source, "chromeria-cookie-mirror-");
  const cookies = yield* Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const [meta] = yield* sql`select value from meta where key = 'version'`.pipe(
      Effect.flatMap(decodeVersion),
    );
    if (!meta) return yield* Effect.fail(new CookieMirrorError({ reason: "unavailable" }));
    const columns = yield* sql<{ readonly name: string }>`pragma table_info(cookies)`;
    const hasAncestor = columns.some((column) => column.name === "has_cross_site_ancestor");
    const hasPartitions = columns.some((column) => column.name === "top_frame_site_key");
    const rows = yield* sql
      .unsafe(`select host_key, name, value, encrypted_value, path,
      (expires_utc - 11644473600000000) / 1000000.0 as expires_seconds, is_secure, is_httponly, samesite, is_persistent,
      ${hasPartitions ? "top_frame_site_key" : "'' as top_frame_site_key"},
      ${hasAncestor ? "has_cross_site_ancestor" : "0 as has_cross_site_ancestor"} from cookies`)
      .pipe(Effect.flatMap(decodeRows));
    const cookies: MirrorCookie[] = [];
    const identities: ManagedCookieKey[] = [];
    const unresolvedPartitions: NonNullable<
      ChromeCookieSnapshot["unresolvedPartitions"]
    >[number][] = [];
    let complete = true;
    for (const row of rows) {
      // Older databases cannot distinguish ancestor siblings; protect that family.
      if (row.top_frame_site_key !== "" && !hasAncestor) {
        unresolvedPartitions.push({
          domain: row.host_key,
          name: row.name,
          path: row.path,
          topLevelSite: row.top_frame_site_key,
        });
        complete = false;
        continue;
      }
      identities.push({
        key: {
          domain: row.host_key,
          name: row.name,
          path: row.path,
          ...(row.top_frame_site_key === ""
            ? {}
            : {
                partitionKey: {
                  topLevelSite: row.top_frame_site_key,
                  hasCrossSiteAncestor: row.has_cross_site_ancestor === 1,
                },
              }),
        },
        persistent: row.is_persistent === 1,
      });
      if (
        row.encrypted_value.length > 0 &&
        new TextDecoder().decode(row.encrypted_value.subarray(0, 3)) !== "v10"
      ) {
        complete = false;
        continue;
      }
      const value =
        row.encrypted_value.length === 0
          ? row.value
          : decryptChromiumValue(row.encrypted_value, keys, row.host_key, meta.value, "darwin");
      if (value === null) {
        complete = false;
        continue;
      }
      const expires = row.expires_seconds;
      cookies.push({
        domain: row.host_key,
        name: row.name,
        value,
        path: row.path,
        secure: row.is_secure === 1,
        httpOnly: row.is_httponly === 1,
        sameSite:
          row.samesite === 0
            ? "no_restriction"
            : row.samesite === 1
              ? "lax"
              : row.samesite === 2
                ? "strict"
                : "unspecified",
        ...(row.is_persistent === 1 && expires > 0 ? { expires } : {}),
        ...(row.top_frame_site_key === ""
          ? {}
          : {
              partitionKey: {
                topLevelSite: row.top_frame_site_key,
                hasCrossSiteAncestor: row.has_cross_site_ancestor === 1,
              },
            }),
      });
    }
    return {
      cookies: yield* decodeCookies(cookies),
      identities,
      complete,
      unresolvedPartitions,
    } satisfies ChromeCookieSnapshot;
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: snapshot, readonly: true })));
  return cookies;
});
