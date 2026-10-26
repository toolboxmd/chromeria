// @effect-diagnostics nodeBuiltinImport:off - Effect has no incremental digest or no-follow open.
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import type * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/http";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeStreamConsumers from "node:stream/consumers";
import * as NodeStreamPromises from "node:stream/promises";
import type * as Yauzl from "yauzl";

import * as ServerConfig from "../config.ts";
import { openZipArchive } from "../zipArchive.ts";

// Update version, sizes and hashes from each platform's full chrome zip in Chrome for Testing's
// known-good-versions-with-downloads.json; sites fingerprint chrome-headless-shell as a bot. Never
// pin below 156.0.8078.70: older builds lack the CRLSet that blocks compromised eMudhra intermediates.
const VERSION = "156.0.8078.70";
const MAC_EXECUTABLE = "Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing";
const ARCHIVES = {
  linux64: {
    bytes: 201_035_256,
    sha256: "baac330433bb105012b7429ad77d4ba3d4ec3768810008ce9257d5d930c1b0cd",
    executable: "chrome",
  },
  "linux-arm64": {
    bytes: 200_845_435,
    sha256: "46dce94a0790d177927044e83ee7e0f571bc22e90e6fafe21fce5c1e248bc865",
    executable: "chrome",
  },
  "mac-arm64": {
    bytes: 194_910_526,
    sha256: "1070e2acc6fec1b80aac9215c9b83a381334a8c556df941e5146efb816121adb",
    executable: MAC_EXECUTABLE,
  },
  "mac-x64": {
    bytes: 205_543_880,
    sha256: "3218f6e802588632f2f8055b90035b306cad48874daf6067330e0f012afdb7ab",
    executable: MAC_EXECUTABLE,
  },
  win32: {
    bytes: 188_579_460,
    sha256: "853955e981b07b8c44dfc77e658f6bfc806868a74176a44900aecda65ea4c26e",
    executable: "chrome.exe",
  },
  win64: {
    bytes: 209_092_934,
    sha256: "8ab65752ca6106bb8a5bf3afaa710ab4f62a57c27ed39db4977422c929fce0b3",
    executable: "chrome.exe",
  },
} as const;

/** Installs live in `<baseDir>/tools/<this>/<platform>/<version>`. */
export const PREVIEW_BROWSER_DIRECTORY = "chrome-for-testing";

const chromePlatform = (platform: NodeJS.Platform, arch: NodeJS.Architecture) => {
  switch (platform) {
    case "linux":
      return arch === "x64" ? "linux64" : arch === "arm64" ? "linux-arm64" : null;
    case "darwin":
      return arch === "arm64" ? "mac-arm64" : arch === "x64" ? "mac-x64" : null;
    case "win32":
      // There is no Windows arm64 build; Windows on Arm runs the x64 one under emulation.
      return arch === "ia32" ? "win32" : arch === "x64" || arch === "arm64" ? "win64" : null;
    default:
      return null;
  }
};

export interface PreviewBrowserRelease {
  readonly version: string;
  readonly platform: string;
  readonly url: string;
  readonly bytes: number;
  readonly sha256: string;
  /** `/`-separated path inside the archive's top directory. */
  readonly executable: string;
}

export const previewBrowserRelease = (
  platform: NodeJS.Platform,
  arch: NodeJS.Architecture,
): PreviewBrowserRelease | null => {
  const chrome = chromePlatform(platform, arch);
  return chrome === null
    ? null
    : {
        version: VERSION,
        platform: chrome,
        url: `https://storage.googleapis.com/chrome-for-testing-public/${VERSION}/${chrome}/chrome-${chrome}.zip`,
        ...ARCHIVES[chrome],
      };
};

const megabytes = (bytes: number) => Math.round(bytes / 1_000_000);

export class PreviewBrowserInstallError extends Schema.TaggedError<PreviewBrowserInstallError>()(
  "PreviewBrowserInstallError",
  { detail: Schema.String, cause: Schema.optional(Schema.Defect()) },
) {
  override get message(): string {
    return `T3 could not install its headless browser: ${this.detail} Try again.`;
  }
}
const isInstallError = Schema.is(PreviewBrowserInstallError);

export class PreviewBrowserInstallingError extends Schema.TaggedError<PreviewBrowserInstallingError>()(
  "PreviewBrowserInstallingError",
  { downloadedBytes: Schema.Number, totalBytes: Schema.Number, unpacking: Schema.Boolean },
) {
  override get message(): string {
    const progress = this.unpacking
      ? "unpacking"
      : `${megabytes(this.downloadedBytes)} of ${megabytes(this.totalBytes)} MB downloaded`;
    return `T3 is installing its headless browser (${progress}). Try again in a minute.`;
  }
}

export class PreviewBrowserUnsupportedError extends Schema.TaggedError<PreviewBrowserUnsupportedError>()(
  "PreviewBrowserUnsupportedError",
  { platform: Schema.String, arch: Schema.String },
) {
  override get message(): string {
    return `T3's headless browser is not available on ${this.platform}-${this.arch}: Chrome for Testing has no build for it.`;
  }
}

export class PreviewBrowser extends Context.Service<
  PreviewBrowser,
  {
    /**
     * Path to the installed Chrome for Testing. The first call starts the
     * install; callers wait for it up to a bound and then get its progress
     * instead, while the install keeps running.
     */
    readonly executable: Effect.Effect<
      string,
      PreviewBrowserInstallError | PreviewBrowserInstallingError | PreviewBrowserUnsupportedError
    >;
    readonly installed: Effect.Effect<Option.Option<string>>;
  }
>()("t3/preview/PreviewBrowser") {}

export interface PreviewBrowserOptions {
  readonly baseDir: string;
  readonly release?: PreviewBrowserRelease | null;
  /** How long a caller waits on an install in progress. Provider tool calls time out near 60s. */
  readonly wait?: Duration.Input;
}

const DOWNLOAD_TIMEOUT = "15 minutes";
const MAX_LINK_TARGET_BYTES = 1024;
const MAX_LINK_HOPS = 40;
const CREATE_NO_FOLLOW =
  NodeFS.constants.O_CREAT |
  NodeFS.constants.O_EXCL |
  NodeFS.constants.O_WRONLY |
  // Windows has no O_NOFOLLOW; its archives carry no links.
  (NodeFS.constants.O_NOFOLLOW ?? 0);
const STAGING_STALE_AFTER_MS = 60 * 60_000;

const wrapFailure = (detail: string) => (cause: unknown) =>
  isInstallError(cause) ? cause : new PreviewBrowserInstallError({ detail, cause });

type InstallState =
  | { readonly _tag: "idle" }
  | {
      readonly _tag: "installing";
      readonly done: Deferred.Deferred<string, PreviewBrowserInstallError>;
    }
  | { readonly _tag: "installed"; readonly executable: string }
  | { readonly _tag: "failed"; readonly error: PreviewBrowserInstallError };

export const makePreviewBrowser = Effect.fn("PreviewBrowser.make")(function* (
  options: PreviewBrowserOptions,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const http = yield* HttpClient.HttpClient;
  // Installs belong to the service, so they finish even when no caller is still waiting.
  const serviceScope = yield* Effect.scope;
  const platform = yield* HostProcess.Platform;
  const arch = yield* HostProcess.Architecture;
  const release =
    options.release === undefined ? previewBrowserRelease(platform, arch) : options.release;
  const wait = options.wait ?? "45 seconds";
  const installRoot = path.join(
    options.baseDir,
    "tools",
    PREVIEW_BROWSER_DIRECTORY,
    release?.platform ?? `${platform}-${arch}`,
  );
  const executablePath = (release: PreviewBrowserRelease) =>
    path.join(installRoot, release.version, ...release.executable.split("/"));
  const gate = yield* Semaphore.make(1);
  let state: InstallState = { _tag: "idle" };
  const progress = { downloadedBytes: 0, unpacking: false };

  const installedExecutable = (release: PreviewBrowserRelease) => {
    const executable = executablePath(release);
    return fs.stat(executable).pipe(
      Effect.map((info) =>
        info.type === "File" && (platform === "win32" || (info.mode & 0o111) !== 0)
          ? Option.some(executable)
          : Option.none<string>(),
      ),
      Effect.orElseSucceed(() => Option.none<string>()),
    );
  };

  const download = Effect.fn("PreviewBrowser.download")(
    function* (release: PreviewBrowserRelease, archivePath: string) {
      const response = yield* http
        .execute(HttpClientRequest.get(release.url))
        .pipe(Effect.flatMap(HttpClientResponse.filterStatusOk));
      const contentLength = response.headers["content-length"];
      const contentEncoding = response.headers["content-encoding"]?.trim().toLowerCase();
      if (
        (contentEncoding === undefined || contentEncoding === "identity") &&
        contentLength !== undefined &&
        Number(contentLength) !== release.bytes
      ) {
        return yield* new PreviewBrowserInstallError({
          detail: "The download size did not match the pinned release.",
        });
      }
      const hash = NodeCrypto.createHash("sha256");
      yield* response.stream.pipe(
        Stream.tap((chunk) =>
          Effect.suspend(() => {
            progress.downloadedBytes += chunk.byteLength;
            if (progress.downloadedBytes > release.bytes) {
              return Effect.fail(
                new PreviewBrowserInstallError({
                  detail: "The download was larger than the pinned release.",
                }),
              );
            }
            hash.update(chunk);
            return Effect.void;
          }),
        ),
        Stream.run(fs.sink(archivePath, { flag: "wx", mode: 0o600 })),
      );
      if (progress.downloadedBytes !== release.bytes || hash.digest("hex") !== release.sha256) {
        return yield* new PreviewBrowserInstallError({
          detail: "The download failed its size or SHA-256 check. Nothing was installed.",
        });
      }
    },
    Effect.timeout(DOWNLOAD_TIMEOUT),
    Effect.mapError(
      wrapFailure(
        "The download from storage.googleapis.com failed. Check this machine's network access.",
      ),
    ),
  );

  /**
   * Whether `relative` resolves inside `root` through existing entries,
   * following at most MAX_LINK_HOPS links and never stepping above `root`.
   */
  const resolvesInside = async (root: string, relative: string) => {
    const pending = relative.split("/");
    let resolved = root;
    let hops = 0;
    while (pending.length > 0) {
      const part = pending.shift()!;
      if (part === "" || part === ".") continue;
      if (part === "..") {
        if (resolved === root) return false;
        resolved = path.dirname(resolved);
        continue;
      }
      const candidate = path.join(resolved, part);
      const info = await NodeFS.promises.lstat(candidate).catch(() => null);
      if (info === null) return false;
      if (info.isSymbolicLink()) {
        hops += 1;
        if (hops > MAX_LINK_HOPS) return false;
        const target = await NodeFS.promises.readlink(candidate);
        if (path.isAbsolute(target)) return false;
        pending.unshift(...target.split("/"));
        continue;
      }
      if (!info.isDirectory() && !(info.isFile() && pending.length === 0)) {
        return false;
      }
      resolved = candidate;
    }
    return true;
  };

  // Every entry is checked before anything is written. Directories and files
  // come first, opened without following links, and links only after them, so
  // no write can pass through a link; the finished link graph must stay inside.
  const extract = Effect.fn("PreviewBrowser.extract")(
    function* (release: PreviewBrowserRelease, archivePath: string, destination: string) {
      const archive = yield* openZipArchive(
        archivePath,
        (detail, cause) => new PreviewBrowserInstallError({ detail, cause }),
      );
      const unsafe = new PreviewBrowserInstallError({
        detail: "The archive contains an unexpected or unsafe entry.",
      });
      const root = `chrome-${release.platform}/`;
      const directories: Array<string> = [];
      const files: Array<{ readonly relative: string; readonly entry: Yauzl.Entry }> = [];
      const links: Array<{ readonly relative: string; readonly entry: Yauzl.Entry }> = [];
      const names = new Set<string>();
      for (;;) {
        const entry = yield* archive.next;
        if (!entry) break;
        if (!entry.fileName.startsWith(root)) return yield* unsafe;
        const isDirectory = entry.fileName.endsWith("/");
        const relative = entry.fileName.slice(root.length, isDirectory ? -1 : undefined);
        if (relative === "" && isDirectory) continue;
        const unixType = (entry.externalFileAttributes >>> 16) & 0o170000;
        const kind =
          unixType === 0
            ? isDirectory
              ? "directory"
              : "file"
            : unixType === 0o040000 && isDirectory
              ? "directory"
              : unixType === 0o100000 && !isDirectory
                ? "file"
                : unixType === 0o120000 && !isDirectory
                  ? "link"
                  : null;
        if (
          kind === null ||
          relative.split("/").some((part) => ["", ".", ".."].includes(part)) ||
          /[\\\0]/.test(relative) ||
          names.has(relative) ||
          (entry.generalPurposeBitFlag & 1) !== 0 ||
          ![0, 8].includes(entry.compressionMethod) ||
          (kind === "link" && entry.uncompressedSize > MAX_LINK_TARGET_BYTES)
        ) {
          return yield* unsafe;
        }
        names.add(relative);
        if (kind === "directory") directories.push(relative);
        else (kind === "file" ? files : links).push({ relative, entry });
      }
      const linkNames = new Set(links.map((link) => link.relative));
      for (const name of names) {
        const parts = name.split("/");
        for (let depth = 1; depth < parts.length; depth++) {
          if (linkNames.has(parts.slice(0, depth).join("/"))) return yield* unsafe;
        }
      }
      if (!files.some((file) => file.relative === release.executable)) {
        return yield* new PreviewBrowserInstallError({
          detail: "The archive does not contain Chrome.",
        });
      }

      const target = (relative: string) => path.join(destination, ...relative.split("/"));
      const unpackFailure = (cause: unknown) =>
        new PreviewBrowserInstallError({ detail: "Could not unpack the browser.", cause });
      yield* fs.makeDirectory(destination);
      for (const directory of directories) {
        yield* fs.makeDirectory(target(directory), { recursive: true });
      }
      for (const { relative, entry } of files) {
        const file = target(relative);
        const unixMode = (entry.externalFileAttributes >>> 16) & 0o777;
        yield* fs.makeDirectory(path.dirname(file), { recursive: true });
        yield* Effect.gen(function* () {
          const readable = yield* archive.streamEntry(entry);
          yield* Effect.tryPromise({
            try: async () => {
              const handle = await NodeFS.promises.open(file, CREATE_NO_FOLLOW, 0o600);
              await NodeStreamPromises.pipeline(readable, handle.createWriteStream());
            },
            catch: unpackFailure,
          });
        }).pipe(Effect.scoped);
        // The binary and its shared libraries carry exec bits in the archive.
        if (platform !== "win32" && unixMode !== 0) yield* fs.chmod(file, unixMode);
      }
      for (const { relative, entry } of links) {
        const linkTarget = yield* Effect.gen(function* () {
          const readable = yield* archive.streamEntry(entry);
          return yield* Effect.tryPromise({
            try: () => NodeStreamConsumers.text(readable),
            catch: unpackFailure,
          });
        }).pipe(Effect.scoped);
        // Targets use `/` only; Windows would read a backslash as a separator the walk never checks.
        if (linkTarget === "" || /[\\\0]/.test(linkTarget) || path.isAbsolute(linkTarget)) {
          return yield* unsafe;
        }
        const link = target(relative);
        yield* fs.makeDirectory(path.dirname(link), { recursive: true });
        yield* fs.symlink(linkTarget, link);
      }
      for (const { relative } of links) {
        const inside = yield* Effect.tryPromise({
          try: () => resolvesInside(destination, relative),
          catch: unpackFailure,
        });
        if (!inside) return yield* unsafe;
      }
    },
    Effect.scoped,
    Effect.mapError(
      wrapFailure("Could not unpack the browser. Check free disk space in T3's home directory."),
    ),
  );

  const install = Effect.fn("PreviewBrowser.install")(
    function* (release: PreviewBrowserRelease) {
      yield* Effect.gen(function* () {
        yield* fs.makeDirectory(installRoot, { recursive: true });
        // Staging shares the install root's filesystem so publishing is one rename.
        const staging = yield* fs.makeTempDirectoryScoped({
          directory: installRoot,
          prefix: ".install-",
        });
        const archivePath = path.join(staging, "download.zip");
        const unpacked = path.join(staging, "browser");
        yield* download(release, archivePath);
        progress.unpacking = true;
        yield* extract(release, archivePath, unpacked);
        const destination = path.join(installRoot, release.version);
        yield* fs.remove(destination, { recursive: true, force: true });
        yield* fs.rename(unpacked, destination);
      }).pipe(Effect.scoped);
      // Shared homes may have another server installing; preserve staging directories touched within the last hour.
      const entries = yield* fs.readDirectory(installRoot);
      const now = yield* Clock.currentTimeMillis;
      yield* Effect.forEach(
        entries.filter((name) => name !== release.version),
        (name) =>
          Effect.gen(function* () {
            const target = path.join(installRoot, name);
            if (name.startsWith(".install-")) {
              const info = yield* fs.stat(target);
              const modifiedAt = Option.match(info.mtime, {
                onNone: () => now,
                onSome: (date) => date.getTime(),
              });
              if (now - modifiedAt < STAGING_STALE_AFTER_MS) return;
            }
            yield* fs.remove(target, { recursive: true, force: true });
          }).pipe(
            Effect.catch((cause) =>
              Effect.logWarning("Could not remove an old preview browser.", { name, cause }),
            ),
          ),
        { discard: true },
      );
      return executablePath(release);
    },
    Effect.mapError(wrapFailure("Could not save the browser in T3's home directory.")),
  );

  const join = (release: PreviewBrowserRelease) =>
    gate.withPermit(
      Effect.gen(function* () {
        if (state._tag === "installing") return state;
        if (state._tag === "failed") {
          const failed = state;
          state = { _tag: "idle" };
          return failed;
        }
        const installed = yield* installedExecutable(release);
        if (Option.isSome(installed)) {
          state = { _tag: "installed", executable: installed.value };
          return state;
        }
        const done = yield* Deferred.make<string, PreviewBrowserInstallError>();
        progress.downloadedBytes = 0;
        progress.unpacking = false;
        const installing: InstallState = { _tag: "installing", done };
        state = installing;
        yield* install(release).pipe(
          Effect.onExit((exit) =>
            gate
              .withPermit(
                Effect.sync(() => {
                  state = Exit.isSuccess(exit)
                    ? { _tag: "installed", executable: exit.value }
                    : {
                        _tag: "failed",
                        error: Option.getOrElse(
                          Cause.findErrorOption(exit.cause),
                          () => new PreviewBrowserInstallError({ detail: "The install stopped." }),
                        ),
                      };
                }),
              )
              .pipe(Effect.andThen(Deferred.done(done, exit))),
          ),
          Effect.ignoreCause,
          Effect.interruptible,
          Effect.forkIn(serviceScope),
        );
        return installing;
      }),
    );

  const clearFailure = (error: PreviewBrowserInstallError) =>
    gate.withPermit(
      Effect.sync(() => {
        if (state._tag === "failed" && state.error === error) state = { _tag: "idle" };
      }),
    );

  const executable = Effect.gen(function* () {
    if (release === null) {
      return yield* new PreviewBrowserUnsupportedError({ platform, arch });
    }
    const current = yield* join(release);
    switch (current._tag) {
      case "installed":
        return current.executable;
      case "failed":
        return yield* current.error;
      case "installing":
        return yield* Deferred.await(current.done).pipe(
          Effect.tapError(clearFailure),
          Effect.timeoutOrElse({
            duration: wait,
            orElse: () =>
              Effect.fail(
                new PreviewBrowserInstallingError({
                  downloadedBytes: progress.downloadedBytes,
                  totalBytes: release.bytes,
                  unpacking: progress.unpacking,
                }),
              ),
          }),
        );
    }
  }).pipe(Effect.withSpan("PreviewBrowser.executable"));

  const installed = release === null ? Effect.succeedNone : installedExecutable(release);

  return PreviewBrowser.of({ executable, installed });
});

export const layer = Layer.effect(
  PreviewBrowser,
  Effect.gen(function* () {
    const config = yield* ServerConfig.ServerConfig;
    return yield* makePreviewBrowser({ baseDir: config.baseDir });
  }),
);
