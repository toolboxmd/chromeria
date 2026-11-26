import * as Schema from "effect/Schema";
import * as Rpc from "effect/rpc/Rpc";

import { EnvironmentAuthorizationError } from "./auth.ts";
import {
  IsoDateTime,
  NonNegativeInt,
  PositiveInt,
  ProjectId,
  TrimmedNonEmptyString,
} from "./baseSchemas.ts";
import { PullRequestActor } from "./pullRequest.ts";

/**
 * GitHub Issues from the repositories of an environment's projects, read live from GitHub and
 * never stored. GitHub only: other forges are reported as not supported.
 */

/** GitHub's own state: closed as completed is Done, as not planned or duplicate Not planned. */
export const GitHubIssueState = Schema.Literals(["open", "done", "not-planned"]);
export type GitHubIssueState = typeof GitHubIssueState.Type;

export const GitHubIssueListState = Schema.Literals(["open", "closed", "all"]);
export type GitHubIssueListState = typeof GitHubIssueListState.Type;

export const GitHubIssueListSort = Schema.Literals(["updated", "created", "number"]);
export type GitHubIssueListSort = typeof GitHubIssueListSort.Type;

/** Addresses one Issue. The server reads it with its GitHub credential for `host`, a host of one of its projects. */
export const GitHubIssueRef = Schema.Struct({
  host: TrimmedNonEmptyString,
  repository: TrimmedNonEmptyString,
  number: PositiveInt,
});
export type GitHubIssueRef = typeof GitHubIssueRef.Type;

/** A parent or sub-Issue as the list shows it, possibly in a repository outside the projects. */
export const IssueLink = Schema.Struct({
  ...GitHubIssueRef.fields,
  title: Schema.String,
  url: TrimmedNonEmptyString,
  state: GitHubIssueState,
});
export type IssueLink = typeof IssueLink.Type;

export const GitHubIssueLabel = Schema.Struct({
  name: TrimmedNonEmptyString,
  color: Schema.String,
});
export type GitHubIssueLabel = typeof GitHubIssueLabel.Type;

/**
 * The commit status `review/independent` on a pull request's head. `error` reads as a failure,
 * `expected` as pending. Clients count it only when a trusted account posted it: the account
 * behind the GitHub connection of any server that listed the Issue.
 */
export const IssueReviewMark = Schema.Literals(["pending", "success", "failure"]);
export type IssueReviewMark = typeof IssueReviewMark.Type;

export const IssueReviewStatus = Schema.Struct({
  state: IssueReviewMark,
  /** Login of the account that posted it; null for a deleted account. */
  creator: Schema.NullOr(Schema.String),
});
export type IssueReviewStatus = typeof IssueReviewStatus.Type;

/**
 * A pull request as the list's own GraphQL page reads it: one that closes the Issue
 * (`Closes #N`), or one linked to a thread of this server.
 */
export const IssuePullRequest = Schema.Struct({
  host: TrimmedNonEmptyString,
  repository: TrimmedNonEmptyString,
  number: PositiveInt,
  url: TrimmedNonEmptyString,
  state: Schema.Literals(["open", "closed", "merged"]),
  isDraft: Schema.Boolean,
  headRefName: Schema.String,
  headSha: Schema.NullOr(TrimmedNonEmptyString),
  review: Schema.NullOr(IssueReviewStatus),
});
export type IssuePullRequest = typeof IssuePullRequest.Type;

export const GitHubIssueListEntry = Schema.Struct({
  ...IssueLink.fields,
  /** The project whose repository this is; worktrees of one repository list it once. */
  projectId: ProjectId,
  projectTitle: TrimmedNonEmptyString,
  author: Schema.NullOr(TrimmedNonEmptyString),
  labels: Schema.Array(GitHubIssueLabel),
  milestone: Schema.NullOr(TrimmedNonEmptyString),
  commentCount: NonNegativeInt,
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
  parent: Schema.NullOr(IssueLink),
  subIssues: Schema.Array(IssueLink),
  /** GitHub's own count, which can exceed `subIssues` when there are more than one page. */
  subIssueCount: NonNegativeInt,
  /** Native blockers (`blocked by`) still open. */
  openBlockerCount: NonNegativeInt,
  /** Open, closed and merged alike; closed ones still derive thread links. */
  closingPullRequests: Schema.Array(IssuePullRequest),
});
export type GitHubIssueListEntry = typeof GitHubIssueListEntry.Type;

/** Opaque per-search continuation, keyed by the search it came from. */
export const GitHubIssueListCursors = Schema.Record(
  TrimmedNonEmptyString,
  TrimmedNonEmptyString.check(Schema.isMaxLength(1024)),
);
export type GitHubIssueListCursors = typeof GitHubIssueListCursors.Type;

const BoundedText = TrimmedNonEmptyString.check(Schema.isMaxLength(200));

export const GitHubIssueListInput = Schema.Struct({
  state: GitHubIssueListState,
  sort: Schema.optional(GitHubIssueListSort),
  /** Free text GitHub matches, quoted so it cannot become a qualifier. */
  query: Schema.optional(BoundedText),
  /** Every label must be present. */
  labels: Schema.optional(Schema.Array(BoundedText).check(Schema.isMaxLength(10))),
  milestone: Schema.optional(BoundedText),
  /** Only these repositories, as `<host> <owner/name>`; absent means every project repository. */
  repositories: Schema.optional(Schema.Array(BoundedText).check(Schema.isMaxLength(100))),
  /** Rows per search page. */
  limit: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 }))),
  /** Carry on from `nextCursors`; only the searches named here are read. */
  cursors: Schema.optional(GitHubIssueListCursors),
});
export type GitHubIssueListInput = typeof GitHubIssueListInput.Type;

export const IssueListRepository = Schema.Struct({
  host: TrimmedNonEmptyString,
  repository: TrimmedNonEmptyString,
  projectId: ProjectId,
  projectTitle: TrimmedNonEmptyString,
});
export type IssueListRepository = typeof IssueListRepository.Type;

export const GitHubIssueListResult = Schema.Struct({
  /** Every GitHub repository of this environment's projects, whether or not it has rows. */
  repositories: Schema.Array(IssueListRepository),
  /** Repositories on forges other than GitHub, which this view does not read; once each. */
  unsupported: Schema.Array(
    Schema.Struct({ host: TrimmedNonEmptyString, repository: TrimmedNonEmptyString }),
  ),
  /** Hosts whose search failed; the others still answer. */
  errors: Schema.Array(Schema.Struct({ host: TrimmedNonEmptyString, message: Schema.String })),
  entries: Schema.Array(GitHubIssueListEntry),
  /** The account each host's reads ran as; review marks it posted are trusted. */
  viewers: Schema.Array(Schema.Struct({ host: TrimmedNonEmptyString, login: Schema.String })),
  /**
   * Open pull requests linked to this server's threads, read on each search's first page. GitHub
   * makes closing references only for pull requests into the default branch, so a component
   * pull request into another branch reaches its Issue through its thread instead.
   */
  linkedPullRequests: Schema.Array(IssuePullRequest),
  /** Where each search carries on; absent once it has nothing more. */
  nextCursors: GitHubIssueListCursors,
  /**
   * How many Issues GitHub says each search read here matches, which can exceed what its pages
   * return. Absent from servers that predate it: the reader then cannot tell a list is complete.
   */
  matchCounts: Schema.optional(Schema.Record(TrimmedNonEmptyString, NonNegativeInt)),
});
export type GitHubIssueListResult = typeof GitHubIssueListResult.Type;

export const GitHubIssueComment = Schema.Struct({
  id: TrimmedNonEmptyString,
  author: Schema.NullOr(TrimmedNonEmptyString),
  body: Schema.String,
  createdAt: IsoDateTime,
  url: TrimmedNonEmptyString,
});
export type GitHubIssueComment = typeof GitHubIssueComment.Type;

export const GitHubIssueDetail = Schema.Struct({
  ...IssueLink.fields,
  author: Schema.NullOr(TrimmedNonEmptyString),
  body: Schema.String,
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
  /** The newest comments, oldest first. */
  comments: Schema.Array(GitHubIssueComment),
  commentCount: NonNegativeInt,
  /** Locked Issues take comments from collaborators only; GitHub decides and says why. */
  locked: Schema.Boolean,
  viewerCanClose: Schema.Boolean,
  viewerCanReopen: Schema.Boolean,
  /** As on the list's rows, so the side panel shows them however it was opened. */
  closingPullRequests: Schema.Array(IssuePullRequest),
});
export type GitHubIssueDetail = typeof GitHubIssueDetail.Type;

// Markdown is not trimmed; GitHub refuses bodies past 65536 characters.
export const GitHubIssueCommentInput = Schema.Struct({
  ...GitHubIssueRef.fields,
  body: Schema.String.check(Schema.isNonEmpty()).check(Schema.isMaxLength(65_536)),
});
export type GitHubIssueCommentInput = typeof GitHubIssueCommentInput.Type;

export const IssueStateAction = Schema.Literals(["close-completed", "close-not-planned", "reopen"]);
export type IssueStateAction = typeof IssueStateAction.Type;

export const GitHubIssueSetStateInput = Schema.Struct({
  ...GitHubIssueRef.fields,
  action: IssueStateAction,
});
export type GitHubIssueSetStateInput = typeof GitHubIssueSetStateInput.Type;

/** Issues read per `issues.states` request; the thread panel asks for its linked ones. */
export const ISSUE_STATES_MAX = 50;

export const IssueStatesInput = Schema.Struct({
  issues: Schema.Array(GitHubIssueRef).check(Schema.isMaxLength(ISSUE_STATES_MAX)),
});
export type IssueStatesInput = typeof IssueStatesInput.Type;

/**
 * An Issue's current state; null when GitHub did not return it (deleted or not visible). Title,
 * author and update time come with a returned Issue so the panel's rows read like its pull
 * requests; optional because older servers answer the state alone.
 */
export const IssueStateEntry = Schema.Struct({
  ...GitHubIssueRef.fields,
  state: Schema.NullOr(GitHubIssueState),
  title: Schema.optional(Schema.String),
  author: Schema.optional(Schema.NullOr(PullRequestActor)),
  updatedAt: Schema.optional(IsoDateTime),
});
export type IssueStateEntry = typeof IssueStateEntry.Type;

export const IssueStatesResult = Schema.Struct({ issues: Schema.Array(IssueStateEntry) });
export type IssueStatesResult = typeof IssueStatesResult.Type;

export class GitHubIssueOperationError extends Schema.TaggedError<GitHubIssueOperationError>()(
  "IssueOperationError",
  {
    operation: Schema.String,
    detail: TrimmedNonEmptyString,
  },
) {
  override get message(): string {
    return this.detail;
  }
}

export const ISSUE_WS_METHODS = {
  issuesList: "githubIssues.list",
  issuesDetail: "githubIssues.detail",
  issuesStates: "githubIssues.states",
  issuesComment: "githubIssues.comment",
  issuesSetState: "githubIssues.setState",
} as const;

const IssueRpcError = Schema.Union([GitHubIssueOperationError, EnvironmentAuthorizationError]);

export const IssueRpcs = [
  Rpc.make(ISSUE_WS_METHODS.issuesList, {
    payload: GitHubIssueListInput,
    success: GitHubIssueListResult,
    error: IssueRpcError,
  }),
  Rpc.make(ISSUE_WS_METHODS.issuesDetail, {
    payload: GitHubIssueRef,
    success: GitHubIssueDetail,
    error: IssueRpcError,
  }),
  Rpc.make(ISSUE_WS_METHODS.issuesStates, {
    payload: IssueStatesInput,
    success: IssueStatesResult,
    error: IssueRpcError,
  }),
  Rpc.make(ISSUE_WS_METHODS.issuesComment, {
    payload: GitHubIssueCommentInput,
    success: Schema.Void,
    error: IssueRpcError,
  }),
  Rpc.make(ISSUE_WS_METHODS.issuesSetState, {
    payload: GitHubIssueSetStateInput,
    success: Schema.Void,
    error: IssueRpcError,
  }),
] as const;

export * from "./issueStatus.ts";
