import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";

import { resolveUserDataPath } from "./DesktopUserData.ts";

it.effect("identifies a failed source read and preserves its cause", () => {
  const sourceState = "/profiles/Chromeria/Local State";
  const cause = PlatformError.systemError({
    _tag: "PermissionDenied",
    module: "FileSystem",
    method: "readFileString",
    pathOrDescriptor: sourceState,
  });
  return Effect.gen(function* () {
    const error = yield* resolveUserDataPath({
      appDataDirectory: "/profiles",
      isDevelopment: false,
      platform: "win32",
    }).pipe(Effect.flip);
    assert.equal(error.operation, "read");
    assert.equal(error.resourcePath, sourceState);
    assert.equal(error.category, "PermissionDenied");
    assert.strictEqual(error.cause, cause);
  }).pipe(
    Effect.provideService(
      FileSystem.FileSystem,
      FileSystem.makeNoop({
        exists: (path) => Effect.succeed(path === sourceState),
        readFileString: () => Effect.fail(cause),
      }),
    ),
    Effect.provide(NodeServices.layer),
  );
});

it.effect("copies only Windows Local State from the legacy Chromeria profile", () => {
  const sourceState = "/profiles/Chromeria/Local State";
  const destinationState = "/profiles/chromeria/Local State";
  const state = '{"os_crypt":{"encrypted_key":"test-encrypted-key"}}';
  const reads: string[] = [];
  const writes: Array<{ path: string; contents: string }> = [];
  const directories: string[] = [];
  return Effect.gen(function* () {
    assert.equal(
      yield* resolveUserDataPath({
        appDataDirectory: "/profiles",
        isDevelopment: false,
        platform: "win32",
      }),
      "/profiles/chromeria",
    );
    assert.deepEqual(reads, [sourceState]);
    assert.deepEqual(writes, [{ path: destinationState, contents: state }]);
    assert.deepEqual(directories, ["/profiles/chromeria"]);
  }).pipe(
    Effect.provideService(
      FileSystem.FileSystem,
      FileSystem.makeNoop({
        exists: (path) => Effect.succeed(path === sourceState),
        readFileString: (path) =>
          Effect.sync(() => {
            reads.push(path);
            return state;
          }),
        makeDirectory: (path) =>
          Effect.sync(() => {
            directories.push(path);
          }),
        writeFileString: (path, contents) =>
          Effect.sync(() => {
            writes.push({ path, contents });
          }),
      }),
    ),
    Effect.provide(NodeServices.layer),
  );
});

it.effect("preserves the existing Chromeria profile and locked databases on any filesystem", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "chromeria-profile-" });
    const destination = path.join(directory, "chromeria");
    const statePath = path.join(destination, "Local State");
    const lockPath = path.join(destination, "IndexedDB", "LOCK");
    const state = '{"os_crypt":{"encrypted_key":"existing-key"}}';
    yield* fs.makeDirectory(path.join(destination, "IndexedDB"), { recursive: true });
    yield* fs.writeFileString(statePath, state);
    yield* fs.writeFileString(lockPath, "running profile owns this database");
    for (const platform of ["win32", "darwin", "linux"] as const) {
      assert.equal(
        yield* resolveUserDataPath({
          appDataDirectory: directory,
          isDevelopment: false,
          platform,
        }),
        destination,
      );
      assert.equal(yield* fs.readFileString(statePath), state);
      assert.equal(yield* fs.readFileString(lockPath), "running profile owns this database");
    }
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
