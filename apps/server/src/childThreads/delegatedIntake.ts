import { OrchestratorMcpFailure, type OrchestrationV2Command } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { ProjectStoreV2 } from "../orchestration-v2/ProjectStore.ts";
import { ThreadLaunchService } from "../orchestration-v2/ThreadLaunchService.ts";
import { ThreadManagementService } from "../orchestration-v2/ThreadManagementService.ts";
import { assertProjectWorktree } from "./workspaceAccess.ts";

/** Client delegation uses the same destination access and preparation as MCP. */
export const dispatchDelegatedRequest = Effect.fn("childThreads.dispatchDelegatedRequest")(
  function* (command: Extract<OrchestrationV2Command, { type: "delegated_task.request" }>) {
    const threads = yield* ThreadManagementService;
    const projects = yield* ProjectStoreV2;
    const launches = yield* ThreadLaunchService;
    const parent = yield* threads.getThreadProjection(command.parentThreadId);
    const projectId = command.projectId ?? parent.thread.projectId;
    const destination = yield* projects.getShell(projectId).pipe(
      Effect.mapError(
        () =>
          new OrchestratorMcpFailure({
            code: "orchestration_error",
            message: "Unable to read destination project.",
          }),
      ),
    );
    if (Option.isNone(destination))
      return yield* new OrchestratorMcpFailure({
        code: "invalid_request",
        message: `Project ${projectId} was not found.`,
      });
    if (command.workspaceStrategy?.type === "existing_worktree") {
      yield* assertProjectWorktree(
        destination.value.workspaceRoot,
        command.workspaceStrategy.worktreePath,
      );
    }
    const result = yield* threads.dispatch(command);
    const child = result.storedEvents.find((stored) => stored.event.type === "thread.created");
    if (child?.event.type === "thread.created") {
      const run = result.storedEvents.find(
        (stored) =>
          stored.event.type === "run.created" && stored.event.threadId === child.event.threadId,
      );
      if (run?.event.type === "run.created")
        yield* launches.prepareDelegatedRun({
          commandId: command.commandId,
          threadId: child.event.threadId,
          runId: run.event.payload.id,
          projectId: child.event.payload.projectId,
        });
    }
    return result;
  },
);
