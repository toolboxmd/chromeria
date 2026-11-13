import { OrchestratorMcpFailure, type ProjectId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";

import * as ServerConfig from "../config.ts";
import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import * as ManagedProjectFolders from "../project/ManagedProjectFolders.ts";
import * as Project from "../project/ProjectService.ts";

export const agentThreadCleanup =
  "Archive your own threads with t3_thread_organize instead; deleting a project deletes every thread in it.";

/**
 * Agents never delete the shared No project. Its folder decides even when the scratch probe
 * failed, so an unknown scratch state refuses instead of allowing the delete.
 */
const isSharedScratch = Effect.fn("isSharedScratch")(function* (projectId: ProjectId) {
  const folders = yield* ManagedProjectFolders.ManagedProjectFolders;
  if (yield* folders.isScratchProject(projectId)) return true;
  const project = yield* (yield* Project.ProjectService)
    .getById(projectId)
    .pipe(Effect.orElseSucceed(() => Option.none()));
  if (Option.isNone(project)) return false;
  const path = yield* Path.Path;
  const fileSystem = yield* FileSystem.FileSystem;
  const config = yield* ServerConfig.ServerConfig;
  const scratch = path.resolve(config.baseDir, "scratch");
  const scratchTarget = yield* fileSystem
    .realPath(scratch)
    .pipe(Effect.orElseSucceed(() => scratch));
  const root = path.resolve(project.value.workspaceRoot);
  return root === scratch || root === scratchTarget;
});

/** Agent deletes name their caller, so the cascade refuses threads it did not create. */
export const agentProjectDeleteRequester = Effect.fn("agentProjectDeleteRequester")(function* (
  projectId: ProjectId,
) {
  if (yield* isSharedScratch(projectId)) {
    return yield* new OrchestratorMcpFailure({
      code: "invalid_request",
      message: `Project ${projectId} is the shared No project and cannot be deleted. ${agentThreadCleanup}`,
    });
  }
  const scope = yield* McpInvocationContext.McpInvocationContext;
  return { kind: "agent" as const, threadId: scope.thread?.threadId };
});
