import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as PlatformError from "effect/PlatformError";

import * as ServerConfig from "../config.ts";
import * as ServerSecretStore from "./ServerSecretStore.ts";

const layerServerConfig = () =>
  ServerConfig.layerTest(process.cwd(), { prefix: "t3-secret-store-test-" });

const layerServerSecretStore = () => Layer.provide(ServerSecretStore.layer, layerServerConfig());

const layerPermissionDeniedFileSystem = Layer.effect(
  FileSystem.FileSystem,
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;

    return {
      ...fileSystem,
      readFile: (path) =>
        Effect.fail(
          PlatformError.systemError({
            _tag: "PermissionDenied",
            module: "FileSystem",
            method: "readFile",
            pathOrDescriptor: path,
            description: "Permission denied while reading secret file.",
          }),
        ),
    } satisfies FileSystem.FileSystem;
  }),
).pipe(Layer.provide(NodeServices.layer));

const layerPermissionDeniedSecretStore = () =>
  ServerSecretStore.layer.pipe(
    Layer.provide(layerServerConfig()),
    Layer.provideMerge(layerPermissionDeniedFileSystem),
  );

const layerRenameFailureFileSystem = Layer.effect(
  FileSystem.FileSystem,
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;

    return {
      ...fileSystem,
      rename: (from, to) =>
        Effect.fail(
          PlatformError.systemError({
            _tag: "PermissionDenied",
            module: "FileSystem",
            method: "rename",
            pathOrDescriptor: `${String(from)} -> ${String(to)}`,
            description: "Permission denied while persisting secret file.",
          }),
        ),
    } satisfies FileSystem.FileSystem;
  }),
).pipe(Layer.provide(NodeServices.layer));

const layerRenameFailureSecretStore = () =>
  ServerSecretStore.layer.pipe(
    Layer.provide(layerServerConfig()),
    Layer.provideMerge(layerRenameFailureFileSystem),
  );

const layerConcurrentReadMissFileSystem = Layer.effect(
  FileSystem.FileSystem,
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const readCountRef = yield* Ref.make(0);
    const readBarrier = yield* Deferred.make<void>();

    return {
      ...fileSystem,
      readFile: (path) =>
        /[\\/]session-signing-key\.bin$/.test(String(path))
          ? Ref.updateAndGet(readCountRef, (count) => count + 1).pipe(
              Effect.flatMap((count) => {
                if (count > 2) {
                  return fileSystem.readFile(path);
                }
                return Effect.gen(function* () {
                  if (count === 2) {
                    yield* Deferred.succeed(readBarrier, void 0);
                  }
                  yield* Deferred.await(readBarrier);
                  return yield* Effect.failCause(
                    Cause.fail(
                      PlatformError.systemError({
                        _tag: "NotFound",
                        module: "FileSystem",
                        method: "readFile",
                        pathOrDescriptor: String(path),
                        description: "Secret file does not exist yet.",
                      }),
                    ),
                  );
                });
              }),
            )
          : fileSystem.readFile(path),
    } satisfies FileSystem.FileSystem;
  }),
).pipe(Layer.provide(NodeServices.layer));

const layerConcurrentCreateSecretStore = () =>
  ServerSecretStore.layer.pipe(
    Layer.provide(layerServerConfig()),
    Layer.provideMerge(layerConcurrentReadMissFileSystem),
  );

it.layer(NodeServices.layer)("ServerSecretStore.layer", (it) => {
  it.effect("never exposes a secret before its bytes are written", () =>
    Effect.gen(function* () {
      const writing = yield* Deferred.make<void>();
      const readDone = yield* Deferred.make<void>();
      const layerPausedWriteFileSystem = Layer.effect(
        FileSystem.FileSystem,
        Effect.gen(function* () {
          const fileSystem = yield* FileSystem.FileSystem;

          return {
            ...fileSystem,
            open: (path, options) =>
              fileSystem.open(path, options).pipe(
                Effect.map((file) =>
                  Object.assign(Object.create(file) as FileSystem.File, {
                    writeAll: (buffer: Uint8Array) =>
                      Deferred.succeed(writing, void 0).pipe(
                        Effect.andThen(Deferred.await(readDone)),
                        Effect.andThen(file.writeAll(buffer)),
                      ),
                  }),
                ),
              ),
          } satisfies FileSystem.FileSystem;
        }),
      ).pipe(Layer.provide(NodeServices.layer));

      yield* Effect.gen(function* () {
        const secretStore = yield* ServerSecretStore.ServerSecretStore;
        const value = Uint8Array.from([1, 2, 3]);

        const creating = yield* Effect.forkChild(secretStore.create("session-signing-key", value));
        yield* Deferred.await(writing);
        const midWrite = yield* secretStore.get("session-signing-key");
        yield* Deferred.succeed(readDone, void 0);
        yield* Fiber.join(creating);

        assert.isTrue(Option.isNone(midWrite));
        const persisted = Option.getOrThrow(yield* secretStore.get("session-signing-key"));
        assert.deepEqual(Array.from(persisted), Array.from(value));
      }).pipe(
        Effect.provide(
          ServerSecretStore.layer.pipe(
            Layer.provide(layerServerConfig()),
            Layer.provideMerge(layerPausedWriteFileSystem),
          ),
        ),
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("reuses an existing secret instead of regenerating it", () =>
    Effect.gen(function* () {
      const secretStore = yield* ServerSecretStore.ServerSecretStore;

      const first = yield* secretStore.getOrCreateRandom("session-signing-key", 32);
      const second = yield* secretStore.getOrCreateRandom("session-signing-key", 32);

      assert.deepEqual(Array.from(second), Array.from(first));
    }).pipe(Effect.provide(layerServerSecretStore())),
  );

  it.effect("returns the persisted secret when concurrent creators race", () =>
    Effect.gen(function* () {
      const secretStore = yield* ServerSecretStore.ServerSecretStore;

      const [first, second] = yield* Effect.all(
        [
          secretStore.getOrCreateRandom("session-signing-key", 32),
          secretStore.getOrCreateRandom("session-signing-key", 32),
        ],
        { concurrency: "unbounded" },
      );
      const persisted = yield* secretStore.get("session-signing-key");
      const persistedBytes = Option.getOrThrow(persisted);

      assert.deepEqual(Array.from(first), Array.from(persistedBytes));
      assert.deepEqual(Array.from(second), Array.from(persistedBytes));
    }).pipe(Effect.provide(layerConcurrentCreateSecretStore())),
  );

  it.effect("propagates read failures other than missing-file errors", () =>
    Effect.gen(function* () {
      const secretStore = yield* ServerSecretStore.ServerSecretStore;

      const error = yield* Effect.flip(secretStore.getOrCreateRandom("session-signing-key", 32));

      assert.instanceOf(error, ServerSecretStore.SecretStoreReadError);
      assert.include(error.message, "Failed to read secret session-signing-key.");
      assert.instanceOf(error.cause, PlatformError.PlatformError);
      assert.equal((error.cause as PlatformError.PlatformError).reason._tag, "PermissionDenied");
    }).pipe(Effect.provide(layerPermissionDeniedSecretStore())),
  );

  it.effect("propagates write failures instead of treating them as success", () =>
    Effect.gen(function* () {
      const secretStore = yield* ServerSecretStore.ServerSecretStore;

      const error = yield* Effect.flip(
        secretStore.set("session-signing-key", Uint8Array.from([1, 2, 3])),
      );

      assert.instanceOf(error, ServerSecretStore.SecretStorePersistError);
      assert.include(error.message, "Failed to persist secret session-signing-key.");
      assert.instanceOf(error.cause, PlatformError.PlatformError);
      assert.equal((error.cause as PlatformError.PlatformError).reason._tag, "PermissionDenied");
    }).pipe(Effect.provide(layerRenameFailureSecretStore())),
  );
});
