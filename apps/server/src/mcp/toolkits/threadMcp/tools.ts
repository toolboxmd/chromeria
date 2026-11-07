import { ThreadId, ThreadMcpServers, OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/ai";
import * as ThreadLaunch from "../../../orchestration-v2/ThreadLaunchService.ts";
import * as Invocation from "../../McpInvocationContext.ts";
import * as Threads from "../../../orchestration-v2/ThreadManagementService.ts";
import * as McpRouting from "../../McpRouting.ts";

export const ThreadMcpToolkit = Toolkit.make(
  Tool.make("t3_thread_mcp_configure", {
    description:
      "Replace one idle restricted thread's private MCP configuration; servers:{} removes its attachments. Keeps its allowlist. Revoke old credentials at their issuer separately.",
    parameters: McpRouting.parameters(
      Schema.Struct({ threadId: ThreadId, servers: ThreadMcpServers }),
    ),
    success: Schema.Struct({ sequence: Schema.Number }),
    failure: OrchestratorMcpFailure,
    failureMode: "return" as const,
    dependencies: [
      ThreadLaunch.ThreadLaunchService,
      Invocation.McpInvocationContext,
      Threads.ThreadManagementService,
    ],
  }).annotate(Tool.Destructive, true),
);
