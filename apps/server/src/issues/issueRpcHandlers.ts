import {
  FEATURE_MAP_WS_METHODS,
  type FeatureMapReadInput,
  ISSUE_LINKS_WS_METHODS,
  type GitHubIssueCommentInput,
  type GitHubIssueCountsInput,
  type GitHubIssueListInput,
  type GitHubIssueRef,
  type GitHubIssueSetStateInput,
  type IssueStatesInput,
  ISSUE_WS_METHODS,
} from "@t3tools/contracts";

import type { IssueService } from "./IssueService.ts";

/** The Issues RPCs, spread into the WebSocket handler group behind its scope check. */
export function makeIssueRpcHandlers(issues: IssueService["Service"]) {
  return {
    [ISSUE_WS_METHODS.issuesList]: (input: GitHubIssueListInput) => issues.list(input),
    [ISSUE_WS_METHODS.issuesDetail]: (input: GitHubIssueRef) => issues.detail(input),
    [ISSUE_WS_METHODS.issuesStates]: (input: IssueStatesInput) => issues.states(input),
    [ISSUE_WS_METHODS.issuesCounts]: (input: GitHubIssueCountsInput) => issues.counts(input),
    [ISSUE_WS_METHODS.issuesComment]: (input: GitHubIssueCommentInput) => issues.comment(input),
    [ISSUE_WS_METHODS.issuesSetState]: (input: GitHubIssueSetStateInput) => issues.setState(input),
    [FEATURE_MAP_WS_METHODS.read]: (input: FeatureMapReadInput) => issues.readFeatureMap(input),
  };
}

/** The `rpc.aggregate` each fork Issue RPC is traced under, spread into upstream's table. */
export const ISSUE_RPC_AGGREGATES = {
  [ISSUE_WS_METHODS.issuesList]: "issues",
  [ISSUE_WS_METHODS.issuesDetail]: "issues",
  [ISSUE_WS_METHODS.issuesStates]: "issues",
  [ISSUE_WS_METHODS.issuesCounts]: "issues",
  [ISSUE_WS_METHODS.issuesComment]: "issues",
  [ISSUE_WS_METHODS.issuesSetState]: "issues",
  [FEATURE_MAP_WS_METHODS.read]: "featureMap",
  [ISSUE_LINKS_WS_METHODS.forThread]: "issueLinks",
  [ISSUE_LINKS_WS_METHODS.threadsForIssues]: "issueLinks",
  [ISSUE_LINKS_WS_METHODS.link]: "issueLinks",
  [ISSUE_LINKS_WS_METHODS.unlink]: "issueLinks",
  [ISSUE_LINKS_WS_METHODS.subscribeChanges]: "issueLinks",
} as const;
