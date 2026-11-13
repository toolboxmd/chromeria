import { OrchestratorMcpFailure, type ProjectId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import { unavailable } from "../mcp/threadAccess.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as ManagedProjectFolders from "../project/ManagedProjectFolders.ts";

const threadCleanup =
  "Archive your own threads with t3_thread_organize instead; deleting a project deletes every thread in it.";

/**
 * Agents delete projects only when nothing else lives there: never the shared No project,
 * and never a project holding a thread other than the caller or the caller's children.
 */
export const assertAgentMayDeleteProject = Effect.fn("assertAgentMayDeleteProject")(function* (
  projectId: ProjectId,
) {
  const folders = yield* ManagedProjectFolders.ManagedProjectFolders;
  if (yield* folders.isScratchProject(projectId)) {
    return yield* new OrchestratorMcpFailure({
      code: "invalid_request",
      message: `Project ${projectId} is the shared No project and cannot be deleted. ${threadCleanup}`,
    });
  }
  const scope = yield* McpInvocationContext.McpInvocationContext;
  const callerId = scope.thread?.threadId;
  const threads = yield* ThreadManagement.ThreadManagementService;
  const snapshot = yield* threads
    .getShellSnapshot({ projectId })
    .pipe(Effect.mapError(unavailable));
  const others = [...snapshot.threads, ...snapshot.archivedThreads].filter(
    (thread) =>
      thread.deletedAt === null &&
      (callerId === undefined ||
        (thread.id !== callerId && thread.lineage.parentThreadId !== callerId)),
  );
  if (others.length > 0) {
    return yield* new OrchestratorMcpFailure({
      code: "invalid_request",
      message: `Project ${projectId} holds ${others.length} thread(s) this caller did not create, so it cannot be deleted. ${threadCleanup}`,
    });
  }
});
