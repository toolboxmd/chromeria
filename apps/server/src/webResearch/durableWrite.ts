import * as Effect from "effect/Effect";
import type * as FileSystem from "effect/FileSystem";

// Synced before the rename so a crash leaves the old file or the complete new one,
// never a zero-length file under the final name.
export const writeFileDurably = Effect.fn("webResearch.writeFileDurably")(function* (
  fs: FileSystem.FileSystem,
  target: string,
  data: Uint8Array,
  mode?: number,
) {
  const temporary = `${target}.tmp`;
  yield* fs.open(temporary, { flag: "w", ...(mode === undefined ? {} : { mode }) }).pipe(
    Effect.flatMap((file) => file.writeAll(data).pipe(Effect.andThen(file.sync))),
    Effect.scoped,
    Effect.andThen(fs.rename(temporary, target)),
    Effect.tapError(() => fs.remove(temporary, { force: true }).pipe(Effect.ignore)),
  );
});
