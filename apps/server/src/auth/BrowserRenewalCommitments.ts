// @effect-diagnostics nodeBuiltinImport:off - SQLite reads Chromium's copied quota index at this boundary.
import * as NodeSqlite from "node:sqlite";
import { chromium } from "playwright-core";
import { AuthRenewalCommitment } from "@t3tools/contracts";
import { renewalCommitment } from "@t3tools/shared/sessionRenewal";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

export const PublicCommitments = Schema.Array(
  Schema.Struct({
    environmentId: Schema.String,
    origin: Schema.String,
    epoch: Schema.String,
    commitment: AuthRenewalCommitment,
  }),
);
export class BrowserCommitmentReadError extends Schema.TaggedError<BrowserCommitmentReadError>()(
  "BrowserCommitmentReadError",
  { cause: Schema.Defect() },
) {
  override get message() {
    return "Could not read browser renewal storage. Verify the profile and browser version; if storage is busy, retry.";
  }
}
export class BrowserRenewalEngine extends Context.Service<
  BrowserRenewalEngine,
  {
    readonly read: (input: {
      executable: string;
      profileDirectory: string;
      origin: string;
    }) => Effect.Effect<unknown, BrowserCommitmentReadError>;
  }
>()("t3/auth/BrowserRenewalCommitments/BrowserRenewalEngine") {}

export const READ_EXPRESSION = `(async () => {
        const database = await new Promise((resolve, reject) => {
          const request = indexedDB.open("t3code:session-renewal");
          request.onupgradeneeded = () => { request.transaction?.abort(); resolve(null); };
          request.onerror = () => reject(request.error);
          request.onsuccess = () => resolve(request.result);
        });
        if (!database) return [];
        try {
          const records = await new Promise((resolve, reject) => {
            const request = database.transaction("records", "readonly").objectStore("records").getAll();
            request.onsuccess = () => resolve(request.result);
            request.onerror = () => reject(request.error);
          });
          const result = [];
          for (const raw of records) {
            if (typeof raw !== "object" || raw === null || !("provisional" in raw) || !Array.isArray(raw.provisional)) throw new Error("Invalid renewal record");
            const slots = [...raw.provisional, ...("active" in raw && raw.active ? [raw.active] : [])];
            for (const slot of slots) {
              if (typeof slot !== "object" || slot === null || typeof slot.current !== "string" || !/^[A-Za-z0-9_-]{43}$/.test(slot.current)) throw new Error("Invalid renewal slot");
              const hash = Reflect.get(globalThis, "t3RenewalHash");
              result.push({ environmentId: slot.environmentId, origin: slot.origin, epoch: slot.epoch, commitment: await hash(slot.current) });
            }
          }
          return result;
        } finally { database.close(); }
})()`;

export const layer = Layer.succeed(BrowserRenewalEngine, {
  read: (input) =>
    Effect.acquireUseRelease(
      Effect.tryPromise({
        try: () =>
          chromium.launchPersistentContext(input.profileDirectory, {
            executablePath: input.executable,
            headless: true,
            chromiumSandbox: true,
            serviceWorkers: "block",
            proxy: { server: "http://127.0.0.1:9", bypass: "<-loopback>" },
            args: [
              "--disable-background-networking",
              "--disable-component-update",
              "--no-first-run",
              "--force-webrtc-ip-handling-policy=disable_non_proxied_udp",
            ],
          }),
        catch: (cause) => new BrowserCommitmentReadError({ cause }),
      }),
      (context) =>
        Effect.tryPromise({
          try: async () => {
            await context.route("**/*", (route) =>
              route.request().isNavigationRequest() && route.request().url() === `${input.origin}/`
                ? route.fulfill({
                    status: 200,
                    contentType: "text/html",
                    body: "<!doctype html><title>Renewal storage</title>",
                  })
                : route.abort("accessdenied"),
            );
            // Hashing is confined to this process; only public projections leave the reader.
            await context.exposeFunction("t3RenewalHash", renewalCommitment);
            const page = await context.newPage();
            await page.goto(`${input.origin}/`);
            return await page.evaluate(READ_EXPRESSION);
          },
          catch: (cause) => new BrowserCommitmentReadError({ cause }),
        }),
      (context) => Effect.promise(() => context.close()).pipe(Effect.ignore),
    ),
});

/** The live profile is only copied. Chromium and SQLite open the private copy. */
export const read = Effect.fn("BrowserRenewalCommitments.read")(function* (input: {
  executable: string;
  profileDirectory: string;
  origin: string;
}) {
  const url = new URL(input.origin);
  if (
    !["http:", "https:"].includes(url.protocol) ||
    url.username ||
    url.password ||
    url.pathname !== "/" ||
    url.search ||
    url.hash
  )
    throw new Error("Expected an HTTP origin without a path or credentials.");
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const engine = yield* BrowserRenewalEngine;
  const temporary = yield* fs.makeTempDirectoryScoped({ prefix: "t3-renewal-reader-" });
  yield* fs.chmod(temporary, 0o700);
  const profile = path.join(temporary, "Default");
  yield* fs.makeDirectory(profile, { recursive: true, mode: 0o700 });
  const copy = Effect.fnUntraced(function* (relative: string) {
    const source = path.join(input.profileDirectory, relative);
    if (!(yield* fs.exists(source))) return;
    const target = path.join(profile, relative);
    yield* fs.makeDirectory(path.dirname(target), { recursive: true, mode: 0o700 });
    yield* fs.copy(source, target);
  });
  const legacy = `${url.protocol.slice(0, -1)}_${url.hostname}_${url.port || "0"}.indexeddb`;
  yield* copy(path.join("IndexedDB", `${legacy}.leveldb`));
  yield* copy(path.join("IndexedDB", `${legacy}.blob`));
  for (const suffix of ["", "-wal", "-shm", "-journal"])
    yield* copy(path.join("WebStorage", `QuotaManager${suffix}`));
  const index = path.join(profile, "WebStorage", "QuotaManager");
  if (yield* fs.exists(index)) {
    const ids = yield* Effect.try({
      try: () => {
        const db = new NodeSqlite.DatabaseSync(index);
        try {
          return db
            .prepare("SELECT id FROM buckets WHERE storage_key = ? OR storage_key = ?")
            .all(url.origin, `${url.origin}/`);
        } finally {
          db.close();
        }
      },
      catch: (cause) => new BrowserCommitmentReadError({ cause }),
    });
    for (const row of ids) {
      const { id } = yield* Schema.decodeUnknownEffect(Schema.Struct({ id: Schema.Number }))(row);
      if (!Number.isSafeInteger(id) || id < 0) throw new Error("Invalid browser storage bucket.");
      yield* copy(path.join("WebStorage", String(id), "IndexedDB"));
    }
  }
  return yield* engine
    .read({ executable: input.executable, profileDirectory: temporary, origin: url.origin })
    .pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(PublicCommitments)),
      Effect.map((rows) => rows.filter((row) => new URL(row.origin).origin === url.origin)),
      Effect.mapError((cause) => new BrowserCommitmentReadError({ cause })),
    );
});
