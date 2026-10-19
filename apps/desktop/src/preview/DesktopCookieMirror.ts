import { app } from "electron";
import {
  BrowserProfile,
  CookieMirrorError,
  DEFAULT_CLIENT_SETTINGS,
  resolveBrowserProfiles,
  type CookieMirrorConnectionInput,
  type CookieMirrorConnectionStatus,
  type CookieMirrorProfile,
  type CookieMirrorSiteLimitation,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Crypto from "effect/Crypto";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as FileSystem from "effect/FileSystem";
import type * as Path from "effect/Path";
import type * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as Result from "effect/Result";
import * as Schedule from "effect/Schedule";
import * as Semaphore from "effect/Semaphore";

import * as DesktopEnvironment from "../app/DesktopEnvironment.ts";
import * as DesktopAppSettings from "../settings/DesktopAppSettings.ts";
import * as DesktopClientSettings from "../settings/DesktopClientSettings.ts";
import * as BrowserSession from "./BrowserSession.ts";
import { resolvePartitionScope } from "./PartitionScope.ts";
import {
  BROWSER_IMPORT_SOURCES,
  chromiumSingletonLockIsHeld,
  chromiumProcessIsAlive,
  listSourceProfiles,
  resolveCookieDatabase,
  sourcePathContext,
} from "./BrowserImport/Sources.ts";
import type { ChromiumKeyMaterial } from "./BrowserImport/ChromiumKeys.ts";
import { readChromeCookieKey } from "./ChromeCookieKey.ts";
import { readChromeCookieSnapshot } from "./ChromeCookieSource.ts";
import { CookieMirror } from "./CookieMirror.ts";
import { CookieMirrorMetadataFile } from "./CookieMirrorMetadata.ts";
import { requestCookieMirrorGrant } from "./CookieMirrorGrant.ts";
import { CookieMirrorSocket } from "./CookieMirrorSocket.ts";
import { electronCookieMirrorStore } from "./ElectronCookieMirror.ts";

export class DesktopCookieMirror extends Context.Service<
  DesktopCookieMirror,
  {
    readonly connection: (
      input: CookieMirrorConnectionInput,
    ) => Effect.Effect<CookieMirrorConnectionStatus, CookieMirrorError>;
  }
>()("@t3tools/desktop/preview/DesktopCookieMirror") {}

const CHROME_POLL_MS = 30_000;
const encodeSource = Schema.encodeEffect(
  Schema.fromJsonString(Schema.Tuple([Schema.String, Schema.String])),
);
const profilesEqual = Schema.toEquivalence(Schema.Array(BrowserProfile));
const DESKTOP_FULL_SCAN_MS = 30_000;

const make = Effect.gen(function* () {
  const environment = yield* DesktopEnvironment.DesktopEnvironment;
  const settings = yield* DesktopClientSettings.DesktopClientSettings;
  const appSettings = yield* DesktopAppSettings.DesktopAppSettings;
  const sessions = yield* BrowserSession.BrowserSession;
  const crypto = yield* Crypto.Crypto;
  const lock = yield* Semaphore.make(1);
  const sourceServices = yield* Effect.context<
    FileSystem.FileSystem | Path.Path | ChildProcessSpawner.ChildProcessSpawner
  >();
  const sourceContext = yield* sourcePathContext;
  const sourceHost = yield* HostProcess.Hostname;
  const fileSystem = yield* FileSystem.FileSystem;
  const file = new CookieMirrorMetadataFile(
    environment.path.join(environment.stateDir, "browser-cookie-mirror.json"),
  );
  const loaded = yield* Effect.tryPromise({
    try: () => file.read(),
    catch: () => new CookieMirrorError({ reason: "unavailable" }),
  }).pipe(Effect.result);
  if (loaded._tag === "Failure") {
    yield* Effect.logWarning("Browser cookie mirror metadata unavailable; mirroring stopped.");
    return DesktopCookieMirror.of({
      connection: () => new CookieMirrorError({ reason: "unavailable" }),
    });
  }
  yield* appSettings.load;
  const connections = new Map<
    string,
    { readonly socket: CookieMirrorSocket; readonly generation: number }
  >();
  const generations = new Map<string, number>();
  const environments = new Set(
    loaded.success.partitions.map((partition) => partition.environmentId),
  );
  const stores = new Map<
    string,
    { readonly store: ReturnType<typeof electronCookieMirrorStore>; readonly close: () => void }
  >();
  const dirty = new Set<string>();
  const problems = new Set<string>();
  const warn = (problem: string) =>
    problems.has(problem)
      ? Effect.void
      : Effect.logWarning("Browser cookie mirror needs attention.", { problem }).pipe(
          Effect.tap(() =>
            Effect.sync(() => {
              problems.add(problem);
            }),
          ),
        );
  const mirror = new CookieMirror({
    metadata: loaded.success,
    persist: (metadata) => file.write(metadata),
    changed: (id) => connections.get(id)?.socket.flush(),
  });
  let profiles: ReadonlyArray<CookieMirrorProfile> = [];
  const limitations = new Map<string, CookieMirrorSiteLimitation>();
  let keyResult: Result.Result<ChromiumKeyMaterial, CookieMirrorError> | undefined;
  const sourceFiles = new Map<
    string,
    { readonly fingerprint: string; readonly capturedAt: number }
  >();
  const fingerprint = (database: string) =>
    Effect.forEach([database, `${database}-wal`], (path) =>
      fileSystem.stat(path).pipe(
        Effect.map(
          (stat) =>
            `${stat.size}:${Option.getOrNull(stat.mtime)?.getTime() ?? "unknown"}:${Option.getOrNull(stat.ino) ?? "unknown"}`,
        ),
        Effect.catchIf(
          (error) => error.reason._tag === "NotFound",
          () => Effect.succeed("missing"),
        ),
      ),
    ).pipe(Effect.map((parts) => parts.join("/")));
  let nextChromeRead = 0;
  let nextFullRead = 0;
  const focus = () => {
    nextChromeRead = 0;
  };
  app.on("browser-window-focus", focus);
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      app.off("browser-window-focus", focus);
      for (const connection of connections.values()) connection.socket.close();
      for (const store of stores.values()) store.close();
    }),
  );

  const synchronize = (force = false) =>
    Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis;
      const scan = force || now >= nextChromeRead;
      if (scan) {
        nextChromeRead = now + CHROME_POLL_MS;
        let current = Option.getOrElse(yield* settings.get, () => DEFAULT_CLIENT_SETTINGS);
        const chrome =
          environment.platform === "darwin"
            ? BROWSER_IMPORT_SOURCES.find((source) => source.id === "chrome")
            : undefined;
        const root = chrome?.userDataDirectory(sourceContext);
        const sourceProfiles = (
          chrome && root ? yield* listSourceProfiles(chrome, sourceContext) : []
        ).filter(
          (source) => source.directory !== "Guest Profile" && source.directory !== "System Profile",
        );
        const mapped = new Map<string, CookieMirrorProfile>();
        if (root)
          for (const source of sourceProfiles) {
            const sourceId = yield* encodeSource([root, source.directory]);
            const digest = yield* crypto.digest("SHA-256", new TextEncoder().encode(sourceId));
            const mapping = yield* Effect.tryPromise({
              try: () =>
                mirror.mapChromeProfile({
                  root,
                  directory: source.directory,
                  name: source.name,
                  proposedId: `chrome-${Hex.encode(digest).slice(0, 20)}`,
                  profiles: current.browserProfiles,
                }),
              catch: () => new CookieMirrorError({ reason: "unavailable" }),
            }).pipe(Effect.result);
            if (mapping._tag === "Failure") {
              yield* warn("profile-mapping");
              continue;
            }
            mapped.set(source.directory, mapping.success.profile);
            if (!profilesEqual(current.browserProfiles, mapping.success.profiles)) {
              current = yield* settings.update((saved) => {
                const latest = Option.getOrElse(saved, () => DEFAULT_CLIENT_SETTINGS);
                const profile = mapping.success.profile;
                const list = resolveBrowserProfiles(latest.browserProfiles);
                return {
                  ...latest,
                  browserProfiles: list.some((old) => old.id === profile.id)
                    ? list.map((old) => (old.id === profile.id ? profile : old))
                    : [...list, profile],
                };
              });
            }
          }
        profiles = resolveBrowserProfiles(current.browserProfiles).filter(
          (profile): profile is CookieMirrorProfile => profile.kind === "persistent",
        );
        for (const environmentId of environments)
          for (const profile of profiles) {
            const scope = resolvePartitionScope(environmentId, profile.id);
            const partition = yield* sessions.getPartition(
              scope.scope,
              scope.persistent,
              scope.namespace,
            );
            const existing = stores.get(partition);
            if (existing) {
              yield* Effect.tryPromise({
                try: () => mirror.addPartition(partition, environmentId, profile, existing.store),
                catch: () => new CookieMirrorError({ reason: "unavailable" }),
              });
              continue;
            }
            const session = yield* sessions.getSession(
              scope.scope,
              scope.persistent,
              scope.namespace,
            );
            const store = yield* Effect.try({
              try: () => electronCookieMirrorStore(session),
              catch: () => new CookieMirrorError({ reason: "unavailable" }),
            });
            const changed = () => {
              dirty.add(partition);
            };
            session.cookies.on("changed", changed);
            const added = yield* Effect.tryPromise({
              try: () => mirror.addPartition(partition, environmentId, profile, store),
              catch: () => new CookieMirrorError({ reason: "unavailable" }),
            }).pipe(Effect.result);
            if (added._tag === "Failure") {
              session.cookies.off("changed", changed);
              store.close();
              yield* warn("desktop-partition");
              continue;
            }
            stores.set(partition, {
              store,
              close: () => {
                session.cookies.off("changed", changed);
                store.close();
              },
            });
          }
        if (chrome && root && sourceProfiles.length > 0) {
          // Import's busy check treats unknown lock state as running. Removal
          // needs positive local ownership; an unreadable/foreign lock is unknown.
          const running = yield* fileSystem
            .readLink(sourceContext.path.join(root, "SingletonLock"))
            .pipe(
              Effect.flatMap((target) => {
                const pidText = target.slice(sourceHost.length + 1);
                const pid = Number(pidText);
                return target.startsWith(`${sourceHost}-`) &&
                  /^\d+$/.test(pidText) &&
                  Number.isSafeInteger(pid) &&
                  pid > 0
                  ? chromiumSingletonLockIsHeld(target, sourceHost, chromiumProcessIsAlive)
                  : Effect.succeed(false);
              }),
              Effect.orElseSucceed(() => false),
            );
          for (const source of sourceProfiles) {
            const profile = mapped.get(source.directory);
            const database = yield* resolveCookieDatabase(chrome, sourceContext, source.directory);
            if (!profile || !database) continue;
            const stamp = yield* fingerprint(database);
            const previous = sourceFiles.get(database);
            if (
              previous &&
              (previous.fingerprint === stamp || now - previous.capturedAt < CHROME_POLL_MS)
            )
              continue;
            const began = yield* Clock.currentTimeMillis;
            const observed = yield* Effect.gen(function* () {
              keyResult ??= yield* readChromeCookieKey(
                (yield* appSettings.get).chromeCookieKeyCommand ?? null,
              ).pipe(Effect.result);
              const keys = yield* Effect.fromResult(keyResult);
              const snapshot = yield* readChromeCookieSnapshot(database, keys).pipe(Effect.scoped);
              if (
                !limitations.has("accounts.google.com") &&
                snapshot.cookies.some(
                  (cookie) =>
                    /(^|\.)google\.com$/.test(cookie.domain) &&
                    /^(?:__Secure-[13]P)?(?:SID|PSID|APISID|PAPISID|LSID|HSID|SSID)$/.test(
                      cookie.name,
                    ),
                )
              ) {
                const limitation: CookieMirrorSiteLimitation = {
                  host: "accounts.google.com",
                  reason: "device-bound, not mirrored",
                };
                limitations.set(limitation.host, limitation);
                yield* Effect.logWarning("Browser login mirror limitation.", {
                  host: limitation.host,
                  code: limitation.reason,
                });
              }
              const sourceId = yield* encodeSource([root, source.directory]);
              const failures = yield* Effect.promise(() =>
                mirror.observeChrome(sourceId, profile, snapshot, running),
              );
              sourceFiles.set(database, {
                fingerprint: snapshot.complete ? stamp : (previous?.fingerprint ?? ""),
                capturedAt: now,
              });
              const finished = yield* Clock.currentTimeMillis;
              yield* Effect.logDebug("Chrome cookie mirror scan completed.", {
                durationMs: finished - began,
                rows: snapshot.identities.length,
                readableRows: snapshot.cookies.length,
                complete: snapshot.complete,
              });
              if (!snapshot.complete) yield* warn("incomplete-chrome-snapshot");
              else problems.delete("incomplete-chrome-snapshot");
              if (failures.length > 0) yield* warn("desktop-apply");
            }).pipe(Effect.provide(sourceServices), Effect.result);
            if (observed._tag === "Failure") yield* warn("chrome-source-or-key");
            else problems.delete("chrome-source-or-key");
          }
        }
      }
      if (force || now >= nextFullRead) {
        nextFullRead = now + DESKTOP_FULL_SCAN_MS;
        for (const partition of stores.keys()) dirty.add(partition);
      }
      // Applying a pending write can queue another notification; bound this tick.
      const observations = Array.from(dirty);
      for (const partition of observations) {
        dirty.delete(partition);
        const observed = yield* Effect.tryPromise({
          try: () => mirror.observeDesktop(partition),
          catch: () => new CookieMirrorError({ reason: "unavailable" }),
        }).pipe(Effect.result);
        if (observed._tag === "Failure") {
          dirty.add(partition);
          yield* warn("desktop-read-or-apply");
        }
      }
    }).pipe(Effect.provide(sourceServices));
  const tick = lock
    .withPermits(1)(synchronize())
    .pipe(Effect.catchCause(() => warn("desktop-observation")));
  yield* Effect.promise(() => app.whenReady()).pipe(
    Effect.andThen(tick.pipe(Effect.repeat(Schedule.spaced("250 millis")))),
    Effect.forkScoped,
  );

  return DesktopCookieMirror.of({
    connection: (input) =>
      lock.withPermits(1)(
        Effect.gen(function* () {
          const current = connections.get(input.environmentId);
          if (input.action === "disconnect" && current?.generation === input.generation) {
            current.socket.close();
            connections.delete(input.environmentId);
          } else if (input.action === "connect") {
            environments.add(input.environmentId);
            current?.socket.close();
            yield* synchronize(true).pipe(Effect.catchCause(() => warn("desktop-observation")));
            const generation = (generations.get(input.environmentId) ?? 0) + 1;
            generations.set(input.environmentId, generation);
            const socketUrl = yield* Effect.tryPromise({
              try: () => requestCookieMirrorGrant(input),
              catch: () => new CookieMirrorError({ reason: "forbidden" }),
            });
            const socket = yield* Effect.try({
              try: () => new CookieMirrorSocket(socketUrl, input.environmentId, mirror),
              catch: () => new CookieMirrorError({ reason: "unavailable" }),
            });
            connections.set(input.environmentId, { socket, generation });
          }
          return {
            connected: connections.get(input.environmentId)?.socket.connected ?? false,
            generation: generations.get(input.environmentId) ?? 0,
            profiles,
            limitations: [...limitations.values()],
          };
        }),
      ),
  });
});
export const layer = Layer.effect(DesktopCookieMirror, make);
