import { scopeThreadRef, scopedThreadKey } from "@t3tools/client-runtime/environment";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import {
  type EnvironmentId,
  ISSUE_GROUPS,
  ISSUE_LINKS_BATCH_MAX,
  type IssueGroup,
  type IssueGroupInput,
  type IssueGrouping,
  type IssueKey,
  type GitHubIssueListEntry,
  type IssuePullRequest,
  ThreadId,
  type ThreadGitHubIssueLinkSource,
  type ThreadsForIssuesInput,
  type ThreadsForIssuesResult,
  trustedReviewMark,
} from "@t3tools/contracts";
import { issueKey } from "./issueList.logic";

/** Groups that start collapsed: finished work. */
export const COLLAPSED_ISSUE_GROUPS: ReadonlySet<IssueGroup> = new Set(["done", "not-planned"]);

export type IssueLinkedFilter = "linked" | "unlinked";

export interface IssueGroupFilters {
  /** Any of these; absent or empty means every group. */
  readonly groups?: ReadonlyArray<IssueGroup> | undefined;
  readonly linked?: IssueLinkedFilter | undefined;
}

export function matchesIssueGroupFilters(
  derived: { readonly group: IssueGroup; readonly linkedThreadCount: number },
  filters: IssueGroupFilters,
): boolean {
  if (
    filters.groups !== undefined &&
    filters.groups.length > 0 &&
    !filters.groups.includes(derived.group)
  ) {
    return false;
  }
  if (filters.linked === "linked") return derived.linkedThreadCount > 0;
  if (filters.linked === "unlinked") return derived.linkedThreadCount === 0;
  return true;
}

/** Rows by group in the group table's order, keeping each group's incoming order; empty groups drop. */
export function groupIssues<Entry>(
  entries: ReadonlyArray<Entry>,
  groupOf: (entry: Entry) => IssueGroup,
): ReadonlyArray<{ readonly group: IssueGroup; readonly entries: ReadonlyArray<Entry> }> {
  const byGroup = new Map<IssueGroup, Array<Entry>>();
  for (const entry of entries) {
    const group = groupOf(entry);
    const held = byGroup.get(group);
    if (held === undefined) byGroup.set(group, [entry]);
    else held.push(entry);
  }
  return ISSUE_GROUPS.flatMap((group) => {
    const held = byGroup.get(group);
    return held === undefined ? [] : [{ group, entries: held }];
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
      current = spawnerOf(current, byKey)
    ) {
      const key = scopedThreadKey(scopeThreadRef(current.environmentId, current.id));
      if (keys.has(key)) break;
      keys.add(key);
    }
  }
  return [...keys].toSorted().join("\n");
}

/**
 * Threads that wait on the user, each with every thread that spawned it up an unbroken chain, as
 * sorted `<scoped key>\t<id of the waiting thread>` lines, so a view can subscribe to it alone.
 * The waiting thread is the one to open: the request is answered there. An archived thread asks
 * nothing; a thread that waits itself names itself over one it spawned.
 */
export function waitingThreadKeysOf(shells: ReadonlyArray<WorkingShell>): string {
  const byKey = new Map(
    shells.map((shell) => [scopedThreadKey(scopeThreadRef(shell.environmentId, shell.id)), shell]),
  );
  const waiting = new Map<string, ThreadId>();
  const asking = shells
    .filter(
      (shell) =>
        shell.archivedAt === null && (shell.hasPendingApprovals || shell.hasPendingUserInput),
    )
    .toSorted((left, right) => left.id.localeCompare(right.id));
  for (const shell of asking) {
    waiting.set(scopedThreadKey(scopeThreadRef(shell.environmentId, shell.id)), shell.id);
  }
  for (const shell of asking) {
    const seen = new Set<string>();
    for (
      let current = spawnerOf(shell, byKey);
      current !== undefined;
      current = spawnerOf(current, byKey)
    ) {
      const key = scopedThreadKey(scopeThreadRef(current.environmentId, current.id));
      if (seen.has(key)) break;
      seen.add(key);
      if (!waiting.has(key)) waiting.set(key, shell.id);
    }
  }
  return [...waiting]
    .map(([key, id]) => `${key}\t${id}`)
    .toSorted()
    .join("\n");
}

/** `waitingThreadKeysOf` as a lookup from a thread's scoped key to the thread that waits. */
export function waitingThreadsOf(keys: string): ReadonlyMap<string, ThreadId> {
  return new Map(
    keys
      .split("\n")
      .filter((line) => line.length > 0)
      .map((line) => {
        const [key = "", id = ""] = line.split("\t");
        return [key, ThreadId.make(id)];
      }),
  );
}

/** The thread that spawned this one, when the client holds it. */
function spawnerOf(
  shell: WorkingShell,
  byKey: ReadonlyMap<string, WorkingShell>,
): WorkingShell | undefined {
  return shell.lineage.relationshipToParent === "subagent" && shell.lineage.parentThreadId !== null
    ? byKey.get(scopedThreadKey(scopeThreadRef(shell.environmentId, shell.lineage.parentThreadId)))
    : undefined;
}

export interface IssueRowThread {
  readonly environmentId: EnvironmentId;
  readonly id: ThreadId;
  readonly title: string;
  readonly archivedAt: string | null;
  readonly sources: ReadonlyArray<ThreadGitHubIssueLinkSource>;
  readonly pullRequests: ReadonlyArray<IssueKey>;
}

/** What a row opens for a thread: enough to navigate to it. */
export interface IssueThreadRef {
  readonly environmentId: EnvironmentId;
  readonly id: ThreadId;
}

/** A row's group, reason and target, with the thread or pull request the target opens. */
export type IssueRowGrouping = IssueGrouping<IssueThreadRef, IssuePullRequest>;

/**
 * The group table's inputs for one row. Its pull requests are the ones closing it plus the ones
 * linked to its linked threads, once each; a mark counts when any of `trustedLogins` posted it.
 * A linked thread waits on the user through the client's own shells, on whichever server it lives.
 */
export function issueGroupInputOf(
  entry: Pick<
    GitHubIssueListEntry,
    "state" | "openBlockerCount" | "closingPullRequests" | "labels"
  >,
  threads: ReadonlyArray<IssueRowThread>,
  context: {
    readonly working: ReadonlySet<string>;
    /** `waitingThreadsOf`: a thread's scoped key to the thread that holds the pending request. */
    readonly waiting: ReadonlyMap<string, ThreadId>;
    /** Thread-linked pull requests the list read, by `issueKey`. */
    readonly linkedPullRequests: ReadonlyMap<string, IssuePullRequest>;
    /** Lowercase logins. */
    readonly trustedLogins: ReadonlySet<string>;
  },
): IssueGroupInput<IssueThreadRef, IssuePullRequest> {
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
  const keyOf = (thread: IssueRowThread) =>
    scopedThreadKey(scopeThreadRef(thread.environmentId, thread.id));
  return {
    state: entry.state,
    openBlockerCount: entry.openBlockerCount,
    labels: entry.labels.map((label) => label.name),
    threads: threads.map((thread) => {
      const waiting = context.waiting.get(keyOf(thread));
      return {
        ref: { environmentId: thread.environmentId, id: thread.id },
        archived: thread.archivedAt !== null,
        waitsOnUser:
          waiting === undefined ? null : { environmentId: thread.environmentId, id: waiting },
      };
    }),
    pullRequests: [...pullRequests.values()].map((pullRequest) => ({
      ref: pullRequest,
      state: pullRequest.state,
      isDraft: pullRequest.isDraft,
      reviewMark: trustedReviewMark(pullRequest.review, context.trustedLogins),
      checks: pullRequest.checks,
    })),
    hasTaskBranch: threads.some((thread) => thread.sources.includes("branch")),
    workingNow: threads.some((thread) => context.working.has(keyOf(thread))),
  };
}

/**
 * `threadsForIssues` reads in batches of 100, every row to every server that keeps Issue links,
 * so threads (and their work) on any machine count. Each read carries the closing pull requests
 * the rows already hold.
 */
export function issueThreadTargets(
  entries: ReadonlyArray<
    Pick<GitHubIssueListEntry, "host" | "repository" | "number"> & {
      readonly closingPullRequests: ReadonlyArray<Pick<IssuePullRequest, "repository" | "number">>;
    }
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
