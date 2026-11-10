import { OrchestratorToolkit } from "./toolkits/orchestrator/tools.ts";
import { ThreadToolkit } from "./toolkits/thread/tools.ts";
import { ThreadMcpToolkit } from "./toolkits/threadMcp/tools.ts";
import { AttachmentToolkit } from "./toolkits/attachment/tools.ts";
import { ProjectToolkit } from "./toolkits/project/tools.ts";
import { EnvironmentToolkit } from "./toolkits/environment/tools.ts";
import { PreviewControlsToolkit } from "./toolkits/previewControls/tools.ts";
import { WorktreeToolkit } from "./toolkits/worktree/tools.ts";
import { PullRequestsToolkit } from "./toolkits/pullRequests/tools.ts";
import { IssuesToolkit } from "./toolkits/issues/tools.ts";
import { DeviceToolkit } from "./toolkits/device/tools.ts";
import { HtmlToolkit } from "./toolkits/html/tools.ts";
import { WebResearchToolkit } from "./toolkits/webResearch/tools.ts";
import { PreviewToolkit, PreviewWatchToolkit } from "./toolkits/preview/tools.ts";
import { SpectrumToolkit } from "../spectrum/mcpToolkit.ts";

const toolNames = new Set([
  ...Object.keys(OrchestratorToolkit.tools),
  ...Object.keys(ThreadToolkit.tools),
  ...Object.keys(ThreadMcpToolkit.tools),
  ...Object.keys(AttachmentToolkit.tools),
  ...Object.keys(ProjectToolkit.tools),
  ...Object.keys(EnvironmentToolkit.tools),
  ...Object.keys(PreviewControlsToolkit.tools),
  ...Object.keys(WorktreeToolkit.tools),
  ...Object.keys(PullRequestsToolkit.tools),
  ...Object.keys(IssuesToolkit.tools),
  ...Object.keys(DeviceToolkit.tools),
  ...Object.keys(HtmlToolkit.tools),
  ...Object.keys(WebResearchToolkit.tools),
  ...Object.keys(PreviewToolkit.tools),
  ...Object.keys(PreviewWatchToolkit.tools),
  ...Object.keys(SpectrumToolkit.tools),
]);

export const canonicalT3ToolName = (name: string) => {
  const canonical = name.replace(/^mcp__t3[-_]code__/, "");
  return toolNames.has(canonical) ? canonical : undefined;
};
