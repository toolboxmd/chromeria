import { scopeThreadRef, scopedThreadKey } from "@t3tools/client-runtime/environment";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import {
  type EnvironmentId,
  ISSUE_LINKS_BATCH_MAX,
  ISSUE_STATUSES,
  type IssueKey,
  type IssueListEntry,
  type IssuePullRequest,
  type IssueStatus,
  type IssueStatusInput,
  type ThreadId,
  type ThreadIssueLinkSource,
  type ThreadsForIssuesInput,
  type ThreadsForIssuesResult,
  trustedReviewMark,
} from "@t3tools/contracts";
import { issueKey } from "./issueList.logic";

/** Groups that start collapsed: finished work. */
export const COLLAPSED_ISSUE_STATUSES: ReadonlySet<IssueStatus> = new Set(["done", "not-planned"]);

export type IssueLinkedFilter = "linked" | "unlinked";

export interface IssueStatusFilters {
  /** Any of these; absent or empty means every status. */
  readonly statuses?: ReadonlyArray<IssueStatus> | undefined;
  readonly linked?: IssueLinkedFilter | undefined;
}

export function matchesIssueStatusFilters(
  derived: { readonly status: IssueStatus; readonly linkedThreadCount: number },
  filters: IssueStatusFilters,
): boolean {
  if (
    filters.statuses !== undefined &&
    filters.statuses.length > 0 &&
    !filters.statuses.includes(derived.status)
  ) {
    return false;
  }
  if (filters.linked === "linked") return derived.linkedThreadCount > 0;
  if (filters.linked === "unlinked") return derived.linkedThreadCount === 0;
  return true;
}

/** Rows grouped by status in table order, keeping each group's incoming order; empty groups drop. */
export function groupIssuesByStatus<Entry>(
  entries: ReadonlyArray<Entry>,
  statusOf: (entry: Entry) => IssueStatus,
): ReadonlyArray<{ readonly status: IssueStatus; readonly entries: ReadonlyArray<Entry> }> {
  const byStatus = new Map<IssueStatus, Array<Entry>>();
  for (const entry of entries) {
    const status = statusOf(entry);
    const group = byStatus.get(status);
    if (group === undefined) byStatus.set(status, [entry]);
    else group.push(entry);
  }
  return ISSUE_STATUSES.flatMap((status) => {
    const group = byStatus.get(status);
    return group === undefined ? [] : [{ status, entries: group }];
  });
}

type WorkingShell = Pick<
  EnvironmentThreadShell,
  | "environmentId"
  | "id"
  | "archivedAt"
  | "hasPendingApprovals"
  | "hasPendingUserInput"
  | "runtime"
  | "pendingBackgroundTasks"
  | "lineage"
>;

/**
 * Working now: a run in flight, or agents and background tasks it started still running after its
 * turn (monitors only watch), unless it waits on the user. An archived thread does not work.
 */
export function isThreadShellWorking(
  shell: Omit<WorkingShell, "environmentId" | "id" | "lineage">,
) {
  if (shell.archivedAt !== null || shell.hasPendingApprovals || shell.hasPendingUserInput) {
    return false;
  }
  const status = shell.runtime?.status;
  return (
    status === "preparing" ||
    status === "starting" ||
    status === "running" ||
    status === "waiting" ||
    shell.pendingBackgroundTasks.some(
      (task) => task.kind === "subagent" || task.kind === "background_task",
    )
  );
}

/**
 * Scoped keys of threads working now, each counted also while any thread it spawned works, at any
 * depth, as one sorted string so a view can subscribe to it alone.
 */
export function workingThreadKeysOf(shells: ReadonlyArray<WorkingShell>): string {
  const byKey = new Map(
    shells.map((shell) => [scopedThreadKey(scopeThreadRef(shell.environmentId, shell.id)), shell]),
  );
  const keys = new Set<string>();
  for (const shell of shells) {
    if (!isThreadShellWorking(shell)) continue;
    // A spawned thread's work is its parent's, up an unbroken chain of spawned threads.
    for (
      let current: WorkingShell | undefined = shell;
      current !== undefined;
      current =
        current.lineage.relationshipToParent === "subagent" &&
        current.lineage.parentThreadId !== null
          ? byKey.get(
              scopedThreadKey(
                scopeThreadRef(current.environmentId, current.lineage.parentThreadId),
              ),
            )
          : undefined
    ) {
      const key = scopedThreadKey(scopeThreadRef(current.environmentId, current.id));
      if (keys.has(key)) break;
      keys.add(key);
    }
  }
  return [...keys].toSorted().join("\n");
}

export interface IssueRowThread {
  readonly environmentId: EnvironmentId;
  readonly id: ThreadId;
  readonly title: string;
  readonly archivedAt: string | null;
  readonly sources: ReadonlyArray<ThreadIssueLinkSource>;
  readonly pullRequests: ReadonlyArray<IssueKey>;
}

/**
 * The status table's inputs for one row. Its pull requests are the ones closing it plus the
 * ones linked to its linked threads, once each; a mark counts when any of `trustedLogins` posted it.
 */
export function issueStatusInputOf(
  entry: Pick<IssueListEntry, "state" | "openBlockerCount" | "closingPullRequests">,
  threads: ReadonlyArray<IssueRowThread>,
  context: {
    readonly working: ReadonlySet<string>;
    /** Thread-linked pull requests the list read, by `issueKey`. */
    readonly linkedPullRequests: ReadonlyMap<string, IssuePullRequest>;
    /** Lowercase logins. */
    readonly trustedLogins: ReadonlySet<string>;
  },
): IssueStatusInput {
  const pullRequests = new Map<string, IssuePullRequest>();
  for (const pullRequest of entry.closingPullRequests) {
    pullRequests.set(issueKey(pullRequest), pullRequest);
  }
  for (const thread of threads) {
    for (const key of thread.pullRequests) {
      const read = context.linkedPullRequests.get(issueKey(key));
      if (read !== undefined && !pullRequests.has(issueKey(key))) {
        pullRequests.set(issueKey(key), read);
      }
    }
  }
  return {
    state: entry.state,
    openBlockerCount: entry.openBlockerCount,
    pullRequests: [...pullRequests.values()].map((pullRequest) => ({
      state: pullRequest.state,
      isDraft: pullRequest.isDraft,
      reviewMark: trustedReviewMark(pullRequest.review, context.trustedLogins),
    })),
    hasTaskBranch: threads.some((thread) => thread.sources.includes("branch")),
    linkedThreadCount: threads.length,
    workingNow: threads.some((thread) =>
      context.working.has(scopedThreadKey(scopeThreadRef(thread.environmentId, thread.id))),
    ),
  };
}

/**
 * `threadsForIssues` reads in batches of 100, every row to every server that keeps Issue links,
 * so threads (and their work) on any machine count. Each read carries the closing pull requests
 * the rows already hold.
 */
export function issueThreadTargets(
  entries: ReadonlyArray<
    Pick<IssueListEntry, "host" | "repository" | "number" | "closingPullRequests">
  >,
  linkEnvironments: ReadonlySet<EnvironmentId>,
): ReadonlyArray<{ readonly environmentId: EnvironmentId; readonly input: ThreadsForIssuesInput }> {
  const issues = entries.map((entry) => ({
    host: entry.host,
    repository: entry.repository,
    number: entry.number,
    closingPullRequests: entry.closingPullRequests.map(({ repository, number }) => ({
      repository,
      number,
    })),
  }));
  return [...linkEnvironments].toSorted().flatMap((environmentId) => {
    const targets = [];
    for (let start = 0; start < issues.length; start += ISSUE_LINKS_BATCH_MAX) {
      targets.push({
        environmentId,
        input: { issues: issues.slice(start, start + ISSUE_LINKS_BATCH_MAX) },
      });
    }
    return targets;
  });
}

/** Linked threads per `issueKey`, each thread once. */
export function mergeIssueRowThreads(
  values: ReadonlyArray<readonly [EnvironmentId, ThreadsForIssuesResult]>,
): ReadonlyMap<string, ReadonlyArray<IssueRowThread>> {
  const byIssue = new Map<string, Array<IssueRowThread>>();
  const seen = new Set<string>();
  for (const [environmentId, result] of values) {
    for (const issue of result.issues) {
      const key = issueKey(issue);
      for (const thread of issue.threads) {
        const seenKey = `${key} ${scopedThreadKey(scopeThreadRef(environmentId, thread.id))}`;
        if (seen.has(seenKey)) continue;
        seen.add(seenKey);
        const threads = byIssue.get(key) ?? [];
        threads.push({ ...thread, environmentId });
        byIssue.set(key, threads);
      }
    }
  }
  return byIssue;
}

/** A pull request as the side panel lists it; `state` is null where no read has seen it yet. */
export interface IssuePanelPullRequest extends IssueKey {
  readonly url: string;
  readonly state: IssuePullRequest["state"] | null;
  readonly isDraft: boolean;
}

/**
 * The Issue's closing pull requests, then its linked threads' pull requests, once each; a
 * thread's pull request carries its state when the list read it.
 */
export function issuePanelPullRequests(
  closing: ReadonlyArray<IssuePullRequest>,
  threads: ReadonlyArray<Pick<IssueRowThread, "pullRequests">>,
  linkedPullRequests: ReadonlyMap<string, IssuePullRequest>,
): ReadonlyArray<IssuePanelPullRequest> {
  const byKey = new Map<string, IssuePanelPullRequest>();
  const add = (pullRequest: IssuePanelPullRequest) => {
    const key = issueKey(pullRequest);
    if (!byKey.has(key)) byKey.set(key, pullRequest);
  };
  for (const pullRequest of closing) add(pullRequest);
  for (const thread of threads) {
    for (const key of thread.pullRequests) {
      add(
        linkedPullRequests.get(issueKey(key)) ?? {
          ...key,
          url: `https://${key.host}/${key.repository}/pull/${key.number}`,
          state: null,
          isDraft: false,
        },
      );
    }
  }
  return [...byKey.values()];
}
