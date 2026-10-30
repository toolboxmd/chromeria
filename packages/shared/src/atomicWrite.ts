import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import type { PlatformError } from "effect/PlatformError";
import { resolveSymlinkTarget } from "@t3tools/shared/symlink";

export class ConditionalWriteError extends Schema.TaggedError<ConditionalWriteError>()(
  "ConditionalWriteError",
  {
    filePath: Schema.String,
    recoveryDirectory: Schema.optional(Schema.String),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message() {
    return `Conditional file replacement refused for '${this.filePath}'.${this.recoveryDirectory === undefined ? "" : ` Retained files: ${this.recoveryDirectory}`}`;
  }
}

type AtomicWriteInput = { readonly filePath: string; readonly contents: string };

/**
 * A symlinked target is resolved first so the link survives and its destination is
 * rewritten, since renaming over the link itself would swap it for a regular file.
 */
export function writeFileStringAtomically(
  input: AtomicWriteInput & { readonly expectedContents?: undefined },
): Effect.Effect<void, PlatformError, FileSystem.FileSystem | Path.Path>;
// Conditional publication briefly claims the path and keeps displaced inodes in a private recovery directory.
// null requires an absent destination; conflicts refuse replacement and report retained files.
export function writeFileStringAtomically(
  input: AtomicWriteInput & { readonly expectedContents: string | null },
): Effect.Effect<void, PlatformError | ConditionalWriteError, FileSystem.FileSystem | Path.Path>;
export function writeFileStringAtomically(
  input: AtomicWriteInput & { readonly expectedContents?: string | null | undefined },
): Effect.Effect<void, PlatformError | ConditionalWriteError, FileSystem.FileSystem | Path.Path> {
  return Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const targetPath = yield* resolveSymlinkTarget(input.filePath);
      const targetDirectory = path.dirname(targetPath);

      yield* fs.makeDirectory(targetDirectory, { recursive: true });
      // The temp directory is cleanup, not part of the write: failing to remove
      // it (a virus scanner holding it on Windows) must not fail a write that
      // already landed.
      let retained = false;
      const tempDirectory = yield* Effect.acquireRelease(
        fs.makeTempDirectory({
          directory: targetDirectory,
          prefix: `${path.basename(targetPath)}.`,
        }),
        (directory) =>
          retained
            ? Effect.void
            : fs.remove(directory, { recursive: true }).pipe(Effect.ignore({ log: true })),
      );
      const tempPath = path.join(tempDirectory, "contents.tmp");

      yield* fs.writeFileString(tempPath, input.contents);
      if (input.expectedContents !== undefined) {
        return yield* Effect.uninterruptible(
          Effect.gen(function* () {
            const previous = path.join(tempDirectory, "previous");
            let captured = false;
            let published = false;
            return yield* Effect.gen(function* () {
              if (input.expectedContents !== null) {
                // Capture the actual destination, then publish without replacing a concurrent file.
                retained = true;
                yield* Effect.logInfo("Conditional replacement retains displaced data", {
                  filePath: input.filePath,
                  recoveryDirectory: tempDirectory,
                });
                yield* fs.rename(targetPath, previous);
                captured = true;
                if ((yield* fs.readFileString(previous)) !== input.expectedContents)
                  return yield* new ConditionalWriteError({
                    filePath: input.filePath,
                    recoveryDirectory: tempDirectory,
                  });
              }
              yield* fs.link(tempPath, targetPath);
              published = true;
              if (
                (captured && (yield* fs.readFileString(previous)) !== input.expectedContents) ||
                (yield* fs.readFileString(targetPath)) !== input.contents
              )
                return yield* new ConditionalWriteError({
                  filePath: input.filePath,
                  recoveryDirectory: retained ? tempDirectory : undefined,
                });
              if (retained) {
                yield* fs.remove(tempPath).pipe(Effect.ignore({ log: true }));
              }
            }).pipe(
              Effect.catch((cause) =>
                Effect.gen(function* () {
                  const restore = yield* Effect.gen(function* () {
                    if (!captured) return;
                    if (published)
                      yield* fs.rename(targetPath, path.join(tempDirectory, "uncommitted"));
                    yield* fs.link(previous, targetPath).pipe(
                      Effect.catchIf(
                        (error) => error.reason._tag === "AlreadyExists",
                        () => Effect.void,
                      ),
                    );
                  }).pipe(Effect.result);
                  return yield* new ConditionalWriteError({
                    filePath: input.filePath,
                    recoveryDirectory: retained ? tempDirectory : undefined,
                    cause:
                      restore._tag === "Failure"
                        ? { cause, recoveryFailure: restore.failure }
                        : cause,
                  });
                }),
              ),
            );
          }),
        );
      }
      yield* fs.rename(tempPath, targetPath);
    }),
  );
}
