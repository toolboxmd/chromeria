// @effect-diagnostics nodeBuiltinImport:off - Effect has no lstat to compare unpacked trees.
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Crypto from "effect/Crypto";
import type * as Duration from "effect/Duration";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Stream from "effect/Stream";
import { HttpClient, HttpClientResponse } from "effect/http";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeStreamPromises from "node:stream/promises";
import * as NodeZlib from "node:zlib";

import * as PreviewBrowser from "./PreviewBrowser.ts";

// NTFS cannot enforce POSIX modes; choose the host platform.
const hostPlatform: NodeJS.Platform =
  HostProcess.Platform.defaultValue() === "win32" ? "win32" : "linux";
const executableName = "Fixture.app/Contents/MacOS/Fixture";
const ROOT = "chrome-fixture/";
const LINK = 0o120777;

const makeZip = (
  entries: ReadonlyArray<{ readonly name: string; readonly data?: string; readonly mode: number }>,
) => {
  const records: Array<Buffer> = [];
  const directory: Array<Buffer> = [];
  let offset = 0;
  for (const entry of entries) {
    const name = Buffer.from(entry.name);
    const data = Buffer.from(entry.data ?? "");
    const crc = NodeZlib.crc32(data);
    const local = Buffer.alloc(30);
    local.writeUInt32LE(0x04034b50, 0);
    local.writeUInt16LE(20, 4);
    local.writeUInt32LE(crc, 14);
    local.writeUInt32LE(data.length, 18);
    local.writeUInt32LE(data.length, 22);
    local.writeUInt16LE(name.length, 26);
    const central = Buffer.alloc(46);
    central.writeUInt32LE(0x02014b50, 0);
    central.writeUInt16LE((3 << 8) | 20, 4);
    central.writeUInt16LE(20, 6);
    central.writeUInt32LE(crc, 16);
    central.writeUInt32LE(data.length, 20);
    central.writeUInt32LE(data.length, 24);
    central.writeUInt16LE(name.length, 28);
    central.writeUInt32LE((entry.mode << 16) >>> 0, 38);
    central.writeUInt32LE(offset, 42);
    records.push(local, name, data);
    directory.push(central, name);
    offset += local.length + name.length + data.length;
  }
  const centralDirectory = Buffer.concat(directory);
  const end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50, 0);
  end.writeUInt16LE(entries.length, 8);
  end.writeUInt16LE(entries.length, 10);
  end.writeUInt32LE(centralDirectory.length, 12);
  end.writeUInt32LE(offset, 16);
  return Buffer.concat([...records, centralDirectory, end]);
};

const browserArchive = makeZip([
  { name: ROOT, mode: 0o40755 },
  { name: `${ROOT}${executableName}`, data: "#!/bin/sh\n", mode: 0o100755 },
  { name: `${ROOT}libEGL.so`, data: "library", mode: 0o100755 },
  { name: `${ROOT}locales/`, mode: 0o40755 },
  { name: `${ROOT}locales/en-US.pak`, data: "strings", mode: 0o100644 },
  // The macOS framework layout: links listed before the files they point to.
  { name: `${ROOT}Fixture.framework/Versions/Current`, data: "1", mode: LINK },
  { name: `${ROOT}Fixture.framework/Libraries`, data: "Versions/Current/Libraries", mode: LINK },
  {
    name: `${ROOT}Fixture.framework/Versions/1/Libraries/libGLES.dylib`,
    data: "gl",
    mode: 0o100755,
  },
]);

const makeHarness = Effect.fn("test.makePreviewBrowser")(function* (
  options: {
    readonly archive?: Buffer;
    readonly sha256?: string;
    readonly body?: Stream.Stream<Uint8Array>;
    readonly wait?: Duration.Input;
    readonly unsupported?: boolean;
  } = {},
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-preview-browser-test-" });
  const archive = options.archive ?? browserArchive;
  const crypto = yield* Crypto.Crypto;
  const sha256 =
    options.sha256 ?? Hex.encode(yield* crypto.digest("SHA-256", archive).pipe(Effect.orDie));
  const requests: Array<string> = [];
  const browser = yield* PreviewBrowser.makePreviewBrowser({
    baseDir,
    release: options.unsupported
      ? null
      : {
          version: "1.2.3",
          platform: "fixture",
          url: "https://storage.googleapis.com/chrome-fixture.zip",
          bytes: archive.byteLength,
          sha256,
          executable: executableName,
        },
    ...(options.wait === undefined ? {} : { wait: options.wait }),
  }).pipe(
    Effect.provideService(HostProcess.Platform, hostPlatform),
    Effect.provideService(HostProcess.Architecture, "x64"),
    Effect.provideService(
      HttpClient.HttpClient,
      HttpClient.make((request) =>
        Effect.sync(() => {
          requests.push(request.url);
          const response = HttpClientResponse.fromWeb(request, new Response(null));
          return Object.defineProperty(response, "stream", {
            value: options.body ?? Stream.make(archive.subarray(0, 40), archive.subarray(40)),
          });
        }),
      ),
    ),
  );
  const installRoot = path.join(baseDir, "tools", "chrome-for-testing", "fixture");
  return { browser, fs, path, baseDir, installRoot, requests };
});

it.layer(NodeServices.layer)("PreviewBrowser", (it) => {
  it.effect("installs a verified download with its file modes and then reuses it", () =>
    Effect.gen(function* () {
      const { browser, fs, path, installRoot, requests } = yield* makeHarness();
      yield* fs.makeDirectory(path.join(installRoot, "1.0.0"), { recursive: true });
      const active = path.join(installRoot, ".install-other");
      const abandoned = path.join(installRoot, ".install-abandoned");
      yield* fs.makeDirectory(active);
      yield* fs.makeDirectory(abandoned);
      const twoHoursAgo = DateTime.toDate(DateTime.subtract(yield* DateTime.now, { hours: 2 }));
      yield* fs.utimes(abandoned, twoHoursAgo, twoHoursAgo);
      expect(yield* browser.installed).toEqual(Option.none());

      const executable = yield* browser.executable;

      const version = path.join(installRoot, "1.2.3");
      expect(executable).toBe(path.join(version, ...executableName.split("/")));
      expect(yield* fs.readFileString(path.join(version, "locales", "en-US.pak"))).toBe("strings");
      expect(
        yield* fs.readFileString(
          path.join(version, "Fixture.framework", "Libraries", "libGLES.dylib"),
        ),
      ).toBe("gl");
      expect(
        yield* fs.readLink(path.join(version, "Fixture.framework", "Versions", "Current")),
      ).toBe("1");
      if (hostPlatform !== "win32") {
        const mode = (file: string) =>
          fs.stat(path.join(version, file)).pipe(Effect.map((info) => info.mode & 0o777));
        expect(yield* mode(executableName)).toBe(0o755);
        expect(yield* mode("libEGL.so")).toBe(0o755);
        expect(yield* mode(path.join("locales", "en-US.pak"))).toBe(0o644);
      }
      expect((yield* fs.readDirectory(installRoot)).toSorted()).toEqual([
        ".install-other",
        "1.2.3",
      ]);
      expect(yield* browser.executable).toBe(executable);
      expect(yield* browser.installed).toEqual(Option.some(executable));
      expect(requests).toHaveLength(1);
    }),
  );

  it.effect.each([
    { name: "a hash mismatch", sha256: "0".repeat(64) },
    { name: "a short download", body: Stream.make(browserArchive.subarray(0, -1)) },
    {
      name: "an oversized download",
      body: Stream.make(browserArchive, Buffer.from("extra")),
    },
  ])("rejects $name, installs nothing, and retries on the next call", (testCase) =>
    Effect.gen(function* () {
      const { browser, fs, installRoot, requests } = yield* makeHarness(testCase);

      const error = yield* browser.executable.pipe(Effect.flip);

      expect(error._tag).toBe("PreviewBrowserInstallError");
      expect(error.message).toMatch(/^T3 could not install its headless browser: .+ Try again\.$/);
      expect(yield* fs.readDirectory(installRoot)).toEqual([]);
      yield* browser.executable.pipe(Effect.flip);
      expect(requests).toHaveLength(2);
    }),
  );

  it.effect.each([
    {
      name: "a parent-directory entry",
      reader: true,
      entries: [{ name: `${ROOT}../escape`, data: "x", mode: 0o100644 }],
    },
    {
      name: "an absolute entry",
      reader: true,
      entries: [{ name: "/escape", data: "x", mode: 0o100644 }],
    },
    {
      name: "an entry outside the top directory",
      entries: [{ name: "escape", data: "x", mode: 0o100644 }],
    },
    { name: "an absolute link", entries: [{ name: `${ROOT}a`, data: "/etc", mode: LINK }] },
    {
      name: "a link escaping with ../",
      entries: [{ name: `${ROOT}a`, data: "../escape", mode: LINK }],
    },
    {
      name: "a chained link escape",
      entries: [
        { name: `${ROOT}a`, data: "b", mode: LINK },
        { name: `${ROOT}b`, data: "../escape", mode: LINK },
      ],
    },
    {
      name: "a link escape through a linked ancestor",
      entries: [
        { name: `${ROOT}a`, data: ".", mode: LINK },
        { name: `${ROOT}a/x`, data: "../escape", mode: LINK },
      ],
    },
    {
      name: "a file under a link",
      entries: [
        { name: `${ROOT}a`, data: "..", mode: LINK },
        { name: `${ROOT}a/escape`, data: "x", mode: 0o100644 },
      ],
    },
    {
      name: "a link cycle",
      entries: [
        { name: `${ROOT}a`, data: "b", mode: LINK },
        { name: `${ROOT}b`, data: "a", mode: LINK },
      ],
    },
    {
      name: "a file and a link with the same name",
      entries: [
        { name: `${ROOT}a`, data: "x", mode: 0o100644 },
        { name: `${ROOT}a`, data: "../escape", mode: LINK },
      ],
    },
    {
      name: "a Windows-style link escape",
      entries: [{ name: `${ROOT}a`, data: "..\\download.zip", mode: LINK }],
    },
    {
      name: "a link through a file",
      entries: [
        { name: `${ROOT}f`, data: "x", mode: 0o100644 },
        { name: `${ROOT}d/`, mode: 0o40755 },
        { name: `${ROOT}a`, data: "f/../d", mode: LINK },
      ],
    },
    ...["f/", "f/."].map((data) => ({
      name: `a link to ${data}`,
      entries: [
        { name: `${ROOT}f`, data: "x", mode: 0o100644 },
        { name: `${ROOT}a`, data, mode: LINK },
      ],
    })),
    { name: "a dangling link", entries: [{ name: `${ROOT}a`, data: "missing", mode: LINK }] },
    {
      name: "an oversized link target",
      entries: [{ name: `${ROOT}a`, data: "b/".repeat(513), mode: LINK }],
    },
  ])("rejects an archive with $name and writes nothing outside it", (testCase) =>
    Effect.gen(function* () {
      const { browser, fs, path, baseDir, installRoot } = yield* makeHarness({
        archive: makeZip([
          { name: `${ROOT}${executableName}`, data: "#!/bin/sh\n", mode: 0o100755 },
          ...testCase.entries,
        ]),
      });

      const error = yield* browser.executable.pipe(Effect.flip);

      expect(error).toMatchObject({
        _tag: "PreviewBrowserInstallError",
        // The zip reader refuses these names before extraction sees them.
        detail:
          "reader" in testCase
            ? "The archive could not be read."
            : "The archive contains an unexpected or unsafe entry.",
      });
      expect(yield* fs.readDirectory(installRoot)).toEqual([]);
      for (const directory of [baseDir, installRoot, path.dirname(installRoot)]) {
        expect(yield* fs.exists(path.join(directory, "escape"))).toBe(false);
      }
    }),
  );

it.live(
  "installs the pinned mac-arm64 Chrome for Testing with a valid signature",
  (ctx) =>
    Effect.gen(function* () {
      const archivePath = (yield* HostProcess.Environment).CHROMERIA_TEST_CFT_ARCHIVE;
      if (!archivePath || HostProcess.Platform.defaultValue() !== "darwin") {
        return ctx.skip("Set CHROMERIA_TEST_CFT_ARCHIVE to chrome-mac-arm64.zip on macOS.");
      }
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-preview-browser-real-" });
      const release = PreviewBrowser.previewBrowserRelease("darwin", "arm64")!;
      const browser = yield* PreviewBrowser.makePreviewBrowser({
        baseDir,
        wait: "5 minutes",
      }).pipe(
        Effect.provideService(HostProcess.Platform, "darwin"),
        Effect.provideService(HostProcess.Architecture, "arm64"),
        Effect.provideService(
          HttpClient.HttpClient,
          HttpClient.make((request) =>
            Effect.sync(() =>
              Object.defineProperty(
                HttpClientResponse.fromWeb(request, new Response(null)),
                "stream",
                { value: fs.stream(archivePath).pipe(Stream.orDie) },
              ),
            ),
          ),
        ),
      );

      const executable = yield* browser.executable;

      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const app = path.join(
        baseDir,
        "tools",
        "chrome-for-testing",
        "mac-arm64",
        release.version,
        "Google Chrome for Testing.app",
      );
      expect(executable.startsWith(`${app}${path.sep}`)).toBe(true);
      // Apple's own extractor is the reference: every path, type, mode, link
      // target and file hash must match it.
      const reference = path.join(baseDir, "reference");
      yield* spawner.exitCode(
        ChildProcess.make("ditto", ["-x", "-k", archivePath, reference], { stdin: "ignore" }),
      );
      const referenceApp = path.join(
        reference,
        "chrome-mac-arm64",
        "Google Chrome for Testing.app",
      );
      const [ours, theirs] = yield* Effect.promise(() =>
        Promise.all([describeTree(app), describeTree(referenceApp)]),
      );
      expect(ours.length).toBeGreaterThan(600);
      expect(ours).toEqual(theirs);
      // CfT is only ad-hoc linker-signed without sealed resources, so the bundle
      // never passes codesign; unpacking must not change codesign's verdict.
      const verify = (bundle: string) =>
        spawner.exitCode(
          ChildProcess.make("codesign", ["--verify", "--deep", "--strict", bundle], {
            stdin: "ignore",
            stderr: "ignore",
          }),
        );
      expect(yield* verify(app)).toBe(yield* verify(referenceApp));
      expect(
        (yield* spawner.string(
          ChildProcess.make(executable, ["--version"], { stdin: "ignore" }),
        )).trim(),
      ).toBe(`Google Chrome for Testing ${release.version}`);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  { timeout: 300_000 },
);
});
