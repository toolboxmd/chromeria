import type { IssueCheckState, IssueReviewMark, GitHubIssueState } from "./issues.ts";

/**
 * The computed Issue status (toolboxmd/t3code#29), in the order of the status table in
 * toolboxmd/t3code#25: the first rule that matches wins. The Issues view groups by `issueGroupOf`
 * and names these statuses as the reason a row gives.
 */
const ISSUE_STATUSES = [
  "done",
  "not-planned",
  "in-review",
  "in-progress",
  "waiting-for-merge",
  "changes-requested",
  "waiting-for-review",
  "paused",
  "blocked",
  "discussion",
  "to-do",
] as const;
export type IssueStatus = (typeof ISSUE_STATUSES)[number];

export interface IssueStatusPullRequest {
  readonly state: "open" | "closed" | "merged";
  readonly isDraft: boolean;
  /** Already filtered to the trusted accounts. */
  readonly reviewMark: IssueReviewMark | null;
}

export interface IssueStatusInput {
  readonly state: GitHubIssueState;
  readonly openBlockerCount: number;
  /**
   * Pull requests that close the Issue plus those linked to its linked threads, once each. Only
   * open ones count: a merged one with the Issue still open is a component PR whose final PR
   * into the default branch closes the Issue.
   */
  readonly pullRequests: ReadonlyArray<IssueStatusPullRequest>;
  /** A linked thread's branch names the Issue (`<type>/<number>-<slug>`). */
  readonly hasTaskBranch: boolean;
  readonly linkedThreadCount: number;
  /** A linked thread, or any of its descendant child threads, is working now. */
  readonly workingNow: boolean;
}

/**
 * The status table, first match wins. An open draft counts as work in progress, not as a pull
 * request waiting for review, so with nobody working it reads as Paused.
 */
export function issueStatusOf(input: IssueStatusInput): IssueStatus {
  if (input.state === "done") return "done";
  if (input.state === "not-planned") return "not-planned";
  const open = input.pullRequests.filter((pullRequest) => pullRequest.state === "open");
  const marks = new Set(open.map((pullRequest) => pullRequest.reviewMark));
  const inProgress = input.hasTaskBranch || open.length > 0;
  if (marks.has("pending")) return "in-review";
  if (inProgress && input.workingNow) return "in-progress";
  if (marks.has("success")) return "waiting-for-merge";
  if (marks.has("failure")) return "changes-requested";
  if (open.some((pullRequest) => !pullRequest.isDraft)) return "waiting-for-review";
  if (inProgress) return "paused";
  if (input.openBlockerCount > 0) return "blocked";
  if (input.linkedThreadCount > 0) return "discussion";
  return "to-do";
}

/**
 * Whose move an Issue is, in the order the groups show. Open Issues fall into the first six; Done
 * and Not planned are GitHub's own closed states.
 */
export const ISSUE_GROUPS = [
  "your-move",
  "agents-working",
  "waiting",
  "blocked",
  "stalled",
  "nobody-on-it",
  "done",
  "not-planned",
] as const;
export type IssueGroup = (typeof ISSUE_GROUPS)[number];

/** Only the user can decide or act; an agent or the user set it on purpose. */
const ISSUE_NEEDS_USER_LABEL = "needs-user";
/** A date or a third party holds the Issue; set on purpose, like `needs-user`. */
const ISSUE_WAITING_LABEL = "waiting";

export interface IssueGroupPullRequest<PullRequest> extends IssueStatusPullRequest {
  /** What the caller opens for this pull request. */
  readonly ref: PullRequest;
  /** The head commit's combined check state; absent when it is unknown or the head has none. */
  readonly checks?: IssueCheckState | undefined;
}

export interface IssueGroupThread<Thread> {
  /** What the caller opens for this thread. */
  readonly ref: Thread;
  readonly archived: boolean;
  /** The thread holding a pending approval or question: this one or one it spawned. */
  readonly waitsOnUser: Thread | null;
}

/** The status inputs, with the linked threads and pull requests named so a row can open one. */
export interface IssueGroupInput<Thread, PullRequest> extends Omit<
  IssueStatusInput,
  "pullRequests" | "linkedThreadCount"
> {
  /** Label names as GitHub returned them; case is ignored. */
  readonly labels: ReadonlyArray<string>;
  /** Linked threads in the order a row would open them: the first that fits a rule is its target. */
  readonly threads: ReadonlyArray<IssueGroupThread<Thread>>;
  readonly pullRequests: ReadonlyArray<IssueGroupPullRequest<PullRequest>>;
}

/** Why a row sits in its group. `status` reasons show today's status name. */
export type IssueGroupReason =
  | { readonly kind: "needs-user" }
  | { readonly kind: "thread-waits-on-user" }
  | { readonly kind: "waiting" }
  /** `openBlockerCount` is above zero only for work in flight on an Issue with open blockers. */
  | { readonly kind: "status"; readonly status: IssueStatus; readonly openBlockerCount: number }
  | { readonly kind: "reviewed-checks-running" }
  | { readonly kind: "reviewed-not-merged" }
  | { readonly kind: "blocked"; readonly openBlockerCount: number }
  | { readonly kind: "last-thread-archived" };

/** What a row's action opens. `issue` is the row itself, so it needs no action of its own. */
export type IssueGroupTarget<Thread, PullRequest> =
  | { readonly kind: "issue" }
  | { readonly kind: "thread"; readonly thread: Thread }
  | { readonly kind: "pull-request"; readonly pullRequest: PullRequest };

export interface IssueGrouping<Thread, PullRequest> {
  readonly group: IssueGroup;
  /** `issueStatusOf` of the same inputs. */
  readonly status: IssueStatus;
  /** Null where the group says it all: Nobody on it, Done and Not planned. */
  readonly reason: IssueGroupReason | null;
  readonly target: IssueGroupTarget<Thread, PullRequest>;
}

/**
 * The group table, first match wins. The rule that wins supplies group, reason and target
 * together, so the three cannot disagree. A mark set on purpose (the two labels, a pending
 * request) outranks what activity suggests. A reviewed pull request is the user's move only once
 * its checks are not demonstrably running, and never while it is a draft.
 */
export function issueGroupOf<Thread, PullRequest>(
  input: IssueGroupInput<Thread, PullRequest>,
): IssueGrouping<Thread, PullRequest> {
  const status = issueStatusOf({ ...input, linkedThreadCount: input.threads.length });
  const issue = { kind: "issue" } as const;
  const statusReason = (openBlockerCount = 0) =>
    ({ kind: "status", status, openBlockerCount }) as const;
  if (status === "done" || status === "not-planned") {
    return { group: status, status, reason: null, target: issue };
  }
  const labels = new Set(input.labels.map((label) => label.trim().toLowerCase()));
  if (labels.has(ISSUE_NEEDS_USER_LABEL)) {
    return { group: "your-move", status, reason: { kind: "needs-user" }, target: issue };
  }
  const asked = input.threads.find((thread) => !thread.archived && thread.waitsOnUser !== null);
  if (asked !== undefined && asked.waitsOnUser !== null) {
    return {
      group: "your-move",
      status,
      reason: { kind: "thread-waits-on-user" },
      target: { kind: "thread", thread: asked.waitsOnUser },
    };
  }
  if (labels.has(ISSUE_WAITING_LABEL)) {
    return { group: "waiting", status, reason: { kind: "waiting" }, target: issue };
  }
  if (input.workingNow) {
    return {
      group: "agents-working",
      status,
      // Work without a branch or pull request reads as Blocked already; the count says it once.
      reason:
        status === "blocked"
          ? { kind: "blocked", openBlockerCount: input.openBlockerCount }
          : statusReason(input.openBlockerCount),
      target: issue,
    };
  }
  const open = input.pullRequests.filter((pullRequest) => pullRequest.state === "open");
  if (open.some((pullRequest) => pullRequest.reviewMark === "pending")) {
    return { group: "agents-working", status, reason: statusReason(), target: issue };
  }
  const reviewed = open.filter(
    (pullRequest) => !pullRequest.isDraft && pullRequest.reviewMark === "success",
  );
  const inChecks = reviewed.find((pullRequest) => pullRequest.checks === "pending");
  if (inChecks !== undefined) {
    return {
      group: "agents-working",
      status,
      reason: { kind: "reviewed-checks-running" },
      target: { kind: "pull-request", pullRequest: inChecks.ref },
    };
  }
  const held = reviewed[0];
  if (held !== undefined) {
    return {
      group: "your-move",
      status,
      reason: { kind: "reviewed-not-merged" },
      target: { kind: "pull-request", pullRequest: held.ref },
    };
  }
  if (input.openBlockerCount > 0) {
    return {
      group: "blocked",
      status,
      reason: { kind: "blocked", openBlockerCount: input.openBlockerCount },
      target: issue,
    };
  }
  // Stalled: something was started and nobody moves it. The pull request the status names opens
  // first, then a thread that can be resumed, then whatever is left.
  const named =
    status === "changes-requested"
      ? open.find((pullRequest) => pullRequest.reviewMark === "failure")
      : status === "waiting-for-review"
        ? open.find((pullRequest) => !pullRequest.isDraft)
        : status === "waiting-for-merge"
          ? open.find((pullRequest) => pullRequest.reviewMark === "success")
          : undefined;
  const resumable = input.threads.find((thread) => !thread.archived);
  const pullRequest = named ?? (resumable === undefined ? open[0] : undefined);
  if (pullRequest !== undefined) {
    return {
      group: "stalled",
      status,
      reason: statusReason(),
      target: { kind: "pull-request", pullRequest: pullRequest.ref },
    };
  }
  if (resumable !== undefined) {
    return {
      group: "stalled",
      status,
      reason: statusReason(),
      target: { kind: "thread", thread: resumable.ref },
    };
  }
  const archived = input.threads[0];
  if (archived !== undefined) {
    return {
      group: "stalled",
      status,
      reason: { kind: "last-thread-archived" },
      target: { kind: "thread", thread: archived.ref },
    };
  }
  return { group: "nobody-on-it", status, reason: null, target: issue };
}

/** A review mark counts only when one of the trusted accounts (logins, any case) posted it. */
export function trustedReviewMark(
  review: { readonly state: IssueReviewMark; readonly creator: string | null } | null,
  trustedLogins: ReadonlySet<string>,
): IssueReviewMark | null {
  const creator = review?.creator?.toLowerCase() ?? "";
  return review !== null && creator.length > 0 && trustedLogins.has(creator) ? review.state : null;
}
