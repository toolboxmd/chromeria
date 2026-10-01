import { T3_PROJECT_FILE_NAME, type OrchestrationProjectShell } from "@t3tools/contracts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import { parseT3ProjectFile } from "@t3tools/shared/t3ProjectFile";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import { GitWorkflowService } from "../../../git/GitWorkflowService.ts";
import { ServerSettingsService } from "../../../serverSettings.ts";
import { ThreadsToolError } from "./tools.ts";

/** New independent work uses the destination's settings, never the caller's checkout. */
export const makeSpawnWorkspace = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const settings = yield* ServerSettingsService;
  const git = yield* GitWorkflowService;
  return Effect.fn("ThreadsToolkit.prepareSpawnWorkspace")(function* (
    project: OrchestrationProjectShell,
    random: string,
  ) {
    const file = yield* fs
      .readFileString(path.join(project.workspaceRoot, T3_PROJECT_FILE_NAME))
      .pipe(
        Effect.map(parseT3ProjectFile),
        Effect.orElseSucceed(() => null),
      );
    const resolved = resolveProjectSettings(
      yield* settings.getSettings.pipe(
        Effect.mapError((error) => new ThreadsToolError({ reason: error.message })),
      ),
      project.id,
      project,
      file,
    ).settings;
    if (resolved.defaultThreadEnvMode === "local") return { branch: null, worktreePath: null };
    const local = { branch: null, worktreePath: null };
    const repository = yield* git
      .isRepository(project.workspaceRoot)
      .pipe(Effect.mapError((error) => new ThreadsToolError({ reason: error.message })));
    if (!repository) return local;
    const hasCommit = yield* git
      .hasCommit({ cwd: project.workspaceRoot, refName: "HEAD" })
      .pipe(Effect.mapError((error) => new ThreadsToolError({ reason: error.message })));
    // Match the normal new-thread flow for non-repositories and unborn HEAD.
    if (!hasCommit) return local;
    // HEAD belongs to the destination. Detached HEAD also works, and the unique
    // branch avoids checking out a branch already owned by another thread.
    const worktree = yield* git
      .createWorktree(
        {
          cwd: project.workspaceRoot,
          refName: "HEAD",
          newRefName: `t3/spawn-${random}`,
          path: null,
        },
        { submodules: resolved.worktreeSubmodules },
      )
      .pipe(
        Effect.mapError(
          (error) =>
            new ThreadsToolError({
              reason: `Could not prepare target project worktree: ${error.message}`,
            }),
        ),
      );
    return { branch: worktree.worktree.refName, worktreePath: worktree.worktree.path };
  });
});
