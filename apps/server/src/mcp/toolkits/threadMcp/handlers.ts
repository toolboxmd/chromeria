import * as Effect from "effect/Effect";
import { OrchestratorMcpFailure } from "@t3tools/contracts";
import * as ThreadLaunch from "../../../orchestration-v2/ThreadLaunchService.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import { ThreadMcpToolkit } from "./tools.ts";

export const layer = McpToolAccess.toLayer(ThreadMcpToolkit, {
  t3_thread_mcp_configure: McpToolAccess.writesThreads(
    (input) => [input.threadId],
    (input) =>
      ThreadLaunch.ThreadLaunchService.pipe(
        Effect.flatMap((launches) => launches.configureMcp(input)),
        Effect.mapError(
          (error) =>
            new OrchestratorMcpFailure({ code: "invalid_request", message: error.message }),
        ),
      ),
  ),
});
