import {
  IssueLinkError,
  McpCapabilityUnavailableError,
  OrchestratorMcpFailure,
  PositiveInt,
  ThreadIssueLink,
  TrimmedNonEmptyString,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as Tool from "effect/ai/Tool";
import * as Toolkit from "effect/ai/Toolkit";

import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as ThreadManagementService from "../../../orchestration-v2/ThreadManagementService.ts";

// The handlers capture `IssueLinks` when the toolkit is built; the caller and its access check
// (`McpToolAccess`) are per call.
const dependencies = [
  McpInvocationContext.McpInvocationContext,
  ThreadManagementService.ThreadManagementService,
];

/** Either the Issue's URL or its number, like `link_pull_request`. */
export const IssueTargetInput = Schema.Struct({
  url: Schema.optional(
    TrimmedNonEmptyString.annotate({
      description:
        "The Issue's web URL, for example https://github.com/owner/repo/issues/12. Preferred when you have it.",
    }),
  ),
  repository: Schema.optional(
    TrimmedNonEmptyString.annotate({
      description:
        "Repository as owner/repo. Defaults to this thread's project repository when url is omitted.",
    }),
  ),
  number: Schema.optional(
    PositiveInt.annotate({ description: "Issue number. Required when url is omitted." }),
  ),
  host: Schema.optional(
    TrimmedNonEmptyString.annotate({
      description: "GitHub host, for example github.com. Defaults to the project's host.",
    }),
  ),
});

/** Includes the access gate's refusal, such as a caller outside a T3 thread. */
export const IssueToolError = Schema.Union([
  McpCapabilityUnavailableError,
  OrchestratorMcpFailure,
  IssueLinkError,
]);

const IssueIdentity = {
  host: Schema.String,
  repository: Schema.String,
  number: Schema.Int,
};

const LinkIssueTool = Tool.make("link_issue", {
  description:
    "Link a GitHub Issue to this thread so T3 Code shows it beside the thread and the Issue shows this thread. Link the Issue you are working on when it is not already linked by your branch name (<type>/<number>-<slug>) or a closing reference in your pull request. Pass the URL, or the number (repository defaults to this project's). Linking an already-linked Issue succeeds with alreadyLinked=true.",
  parameters: IssueTargetInput,
  success: Schema.Struct({
    ...IssueIdentity,
    alreadyLinked: Schema.Boolean.annotate({
      description: "True when the Issue was linked to this thread before the call.",
    }),
  }),
  failure: IssueToolError,
  dependencies,
})
  .annotate(Tool.Title, "Link Issue to thread")
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, false);

const UnlinkIssueTool = Tool.make("unlink_issue", {
  description:
    "Remove an Issue link from this thread, including one derived from the branch name or a closing reference. Pass the URL, or the number. Unlinking an Issue that is not linked succeeds with wasLinked=false.",
  parameters: IssueTargetInput,
  success: Schema.Struct({
    ...IssueIdentity,
    wasLinked: Schema.Boolean.annotate({
      description: "False when the Issue was not linked to this thread to begin with.",
    }),
  }),
  failure: IssueToolError,
  dependencies,
})
  .annotate(Tool.Title, "Unlink Issue from thread")
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, true)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, false);

const ListThreadIssuesTool = Tool.make("list_thread_issues", {
  description:
    "List the GitHub Issues linked to this thread and why each is linked: manual, agent, started (thread started from the Issue), branch, or closing-reference.",
  success: Schema.Struct({ issues: Schema.Array(ThreadIssueLink) }),
  failure: IssueToolError,
  dependencies,
})
  .annotate(Tool.Title, "List thread Issues")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, false);

export const IssuesToolkit = Toolkit.make(LinkIssueTool, UnlinkIssueTool, ListThreadIssuesTool);
