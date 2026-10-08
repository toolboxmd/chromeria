import { OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Git from "../vcs/GitVcsDriver.ts";

/** A delegated destination uses only registered worktrees of that project. */
export const assertProjectWorktree = Effect.fn("childThreads.assertProjectWorktree")(function* (
  workspaceRoot: string,
  worktreePath: string,
) {
  const git = yield* Git.GitVcsDriver;
  const fs = yield* FileSystem.FileSystem;
  const real = (path: string) => fs.realPath(path).pipe(Effect.orElseSucceed(() => path));
  const paths = yield* git.listWorktreePaths(workspaceRoot).pipe(
    Effect.flatMap((paths) => Effect.forEach(paths, real)),
    Effect.orElseSucceed((): ReadonlyArray<string> => []),
  );
  if (!paths.includes(yield* real(worktreePath)))
    return yield* new OrchestratorMcpFailure({
      code: "invalid_request",
      message: "worktreePath must be one of the destination project's git worktrees.",
    });
});
