import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";

import { writeFileStringAtomically } from "./atomicWrite.ts";

it.layer(NodeServices.layer)("writeFileStringAtomically", (it) => {
  it.effect("keeps a symlinked file linked and rewrites its destination", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-atomic-write-" });
      const destination = path.join(root, "dotfiles", "settings.json");
      const link = path.join(root, "home", "settings.json");
      yield* fs.makeDirectory(path.dirname(destination), { recursive: true });
      yield* fs.makeDirectory(path.dirname(link), { recursive: true });
      yield* fs.writeFileString(destination, "before");
      yield* fs.symlink(destination, link);

      yield* writeFileStringAtomically({ filePath: link, contents: "after" });

      assert.strictEqual(yield* fs.readLink(link), destination);
      assert.strictEqual(yield* fs.readFileString(destination), "after");
    }),
  );

  it.effect("keeps a dangling symlink linked and creates its destination", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-atomic-write-" });
      const destination = path.join(root, "dotfiles", "settings.json");
      const link = path.join(root, "home", "settings.json");
      yield* fs.makeDirectory(path.dirname(link), { recursive: true });
      yield* fs.symlink(destination, link);

      yield* writeFileStringAtomically({ filePath: link, contents: "fresh" });

      assert.strictEqual(yield* fs.readLink(link), destination);
      assert.strictEqual(yield* fs.readFileString(destination), "fresh");
    }),
  );

  it.effect("fails on a symlink cycle without replacing either link", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-atomic-write-" });
      const first = path.join(root, "first.json");
      const second = path.join(root, "second.json");
      yield* fs.symlink(second, first);
      yield* fs.symlink(first, second);

      const result = yield* Effect.exit(
        writeFileStringAtomically({ filePath: first, contents: "after" }),
      );

      assert.isTrue(Exit.isFailure(result));
      assert.strictEqual(yield* fs.readLink(first), second);
      assert.strictEqual(yield* fs.readLink(second), first);
    }),
  );

  it.effect("resolves a relative link through a symlinked parent directory", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-atomic-write-" });
      const destination = path.join(root, "dotfiles", "config", "settings.json");
      const linkedState = path.join(root, "dotfiles", "state");
      const home = path.join(root, "home");
      const link = path.join(home, "state", "settings.json");
      yield* fs.makeDirectory(path.dirname(destination), { recursive: true });
      yield* fs.makeDirectory(linkedState, { recursive: true });
      yield* fs.makeDirectory(home, { recursive: true });
      yield* fs.symlink(linkedState, path.join(home, "state"));
      yield* fs.writeFileString(destination, "before");
      yield* fs.symlink("../config/settings.json", link);

      yield* writeFileStringAtomically({ filePath: link, contents: "after" });

      assert.strictEqual(yield* fs.readLink(link), "../config/settings.json");
      assert.strictEqual(yield* fs.readFileString(destination), "after");
    }),
  );

  it.effect("creates a missing file and its directory", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-atomic-write-" });
      const filePath = path.join(root, "nested", "settings.json");

      yield* writeFileStringAtomically({ filePath, contents: "fresh" });

      assert.strictEqual(yield* fs.readFileString(filePath), "fresh");
    }),
  );
});

it.effect("surfaces an unreadable link instead of writing over it", () =>
  Effect.gen(function* () {
    const readLinkFailure = PlatformError.systemError({
      _tag: "Unknown",
      module: "FileSystem",
      method: "readLink",
      pathOrDescriptor: "/home/settings.json",
    });

    const result = yield* Effect.exit(
      writeFileStringAtomically({ filePath: "/home/settings.json", contents: "after" }),
    );

    assert.deepStrictEqual(result, Exit.fail(readLinkFailure));
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        Path.layer,
        FileSystem.layerNoop({
          readLink: () =>
            Effect.fail(
              PlatformError.systemError({
                _tag: "Unknown",
                module: "FileSystem",
                method: "readLink",
                pathOrDescriptor: "/home/settings.json",
              }),
            ),
          rename: () => Effect.die("an unreadable link must not be replaced"),
        }),
      ),
    ),
  ),
);

it.effect("succeeds when the write lands but its temp directory cannot be removed", () =>
  Effect.gen(function* () {
    const renamed: Array<string> = [];
    const removed: Array<string> = [];
    const fileSystem = FileSystem.layerNoop({
      readLink: (path) =>
        Effect.fail(
          PlatformError.systemError({
            _tag: "NotFound",
            module: "FileSystem",
            method: "readLink",
            pathOrDescriptor: path,
          }),
        ),
      makeDirectory: () => Effect.void,
      makeTempDirectory: () => Effect.succeed("/home/settings.json.abc123"),
      writeFileString: () => Effect.void,
      rename: (_from, to) => Effect.sync(() => void renamed.push(to)),
      remove: (path) =>
        Effect.sync(() => void removed.push(path)).pipe(
          Effect.andThen(
            Effect.fail(
              PlatformError.systemError({
                _tag: "PermissionDenied",
                module: "FileSystem",
                method: "remove",
                pathOrDescriptor: path,
              }),
            ),
          ),
        ),
    });

    const result = yield* Effect.exit(
      writeFileStringAtomically({ filePath: "/home/settings.json", contents: "after" }).pipe(
        Effect.provide(Layer.mergeAll(Path.layer, fileSystem)),
      ),
    );

    assert.deepStrictEqual(result, Exit.void);
    assert.deepStrictEqual(renamed, ["/home/settings.json"]);
    assert.deepStrictEqual(removed, ["/home/settings.json.abc123"]);
  }),
);

it.layer(NodeServices.layer)("conditional file replacement", (it) => {
  it.effect.each([
    "staged",
    "captured",
    "publishing",
    "published",
    "displaced-inode",
    "missing",
  ] as const)("retains external bytes and refuses a conflict at %s", (boundary) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-conditional-write-" });
      const target = path.join(root, "settings.json");
      if (boundary !== "missing") yield* fs.writeFileString(target, "before");
      let previous: string | undefined;
      let injected = false;
      const wrapped = FileSystem.FileSystem.of({
        ...fs,
        writeFileString: (file, value, options) =>
          fs.writeFileString(file, value, options).pipe(
            Effect.andThen(
              Effect.suspend(() => {
                if (!file.endsWith("contents.tmp") || !["staged", "missing"].includes(boundary))
                  return Effect.void;
                injected = true;
                return fs.writeFileString(target, "external");
              }),
            ),
          ),
        rename: (from, to) =>
          fs.rename(from, to).pipe(
            Effect.andThen(
              Effect.suspend(() => {
                if (from !== target || !to.endsWith("previous")) return Effect.void;
                previous = to;
                if (boundary !== "captured") return Effect.void;
                injected = true;
                return fs.writeFileString(target, "external");
              }),
            ),
          ),
        link: (from, to) =>
          Effect.gen(function* () {
            if (from.endsWith("contents.tmp") && to === target && boundary === "publishing") {
              injected = true;
              yield* fs.writeFileString(target, "external");
            }
            yield* fs.link(from, to);
            if (
              from.endsWith("contents.tmp") &&
              to === target &&
              ["published", "displaced-inode"].includes(boundary)
            ) {
              injected = true;
              yield* fs.writeFileString(
                boundary === "displaced-inode" ? previous! : target,
                "external",
              );
            }
          }),
      });
      const result = yield* writeFileStringAtomically({
        filePath: target,
        contents: "after",
        expectedContents: boundary === "missing" ? null : "before",
      }).pipe(Effect.provideService(FileSystem.FileSystem, wrapped), Effect.result);
      assert.isTrue(injected);
      assert.equal(result._tag, "Failure");
      const values: Array<string> = [];
      for (const entry of yield* fs.readDirectory(root)) {
        const file = path.join(root, entry);
        if ((yield* fs.stat(file)).type === "Directory") {
          assert.equal((yield* fs.stat(file)).mode & 0o077, 0);
          for (const name of yield* fs.readDirectory(file))
            values.push(yield* fs.readFileString(path.join(file, name)));
        } else values.push(yield* fs.readFileString(file));
      }
      assert.include(values, "external");
      if (boundary === "staged" || boundary === "missing")
        assert.equal(yield* fs.readFileString(target), "external");
      if (result._tag === "Failure") {
        assert.equal(result.failure._tag, "ConditionalWriteError");
        assert.include(result.failure.message, "Conditional file replacement");
        if (boundary !== "missing") assert.include(result.failure.message, "Retained files:");
      }
    }).pipe(Effect.scoped),
  );
});
