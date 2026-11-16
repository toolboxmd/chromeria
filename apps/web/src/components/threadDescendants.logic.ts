/**
 * Roll-up of what child threads linked (toolboxmd/chromeria#75): a worker links its pull requests
 * to its own thread, so its parent's links panel adds those of every descendant thread.
 */
import type {
  OrchestrationV2ThreadShell,
  ThreadId,
  ThreadPullRequestLink,
} from "@t3tools/contracts";
import { visibleThreadPullRequests } from "@t3tools/shared/threadPullRequests";

export interface DescendantShell {
  readonly id: ThreadId;
  readonly title: string;
  readonly branch: string | null;
  readonly createdAt: string;
  readonly pullRequests: ReadonlyArray<ThreadPullRequestLink>;
  readonly lineage: Pick<
    OrchestrationV2ThreadShell["lineage"],
    "parentThreadId" | "relationshipToParent"
  >;
}

/**
 * The threads `ancestorId` spawned, directly or through other spawned threads, in spawn order.
 * Only unbroken subagent links count, as the server's roll-up: a conversation fork and anything
 * below it are not the thread's work.
 */
export function descendantThreads<T extends DescendantShell>(
  shells: ReadonlyArray<T>,
  ancestorId: string,
): ReadonlyArray<T> {
  const children = new Map<string, T[]>();
  for (const shell of shells) {
    const parent = shell.lineage.parentThreadId;
    if (shell.lineage.relationshipToParent !== "subagent" || parent === null) continue;
    children.set(parent, [...(children.get(parent) ?? []), shell]);
  }
  const descendants: T[] = [];
  const seen = new Set([ancestorId]);
  const queue = [ancestorId];
  while (queue.length > 0) {
    for (const child of children.get(queue.shift()!) ?? []) {
      if (seen.has(child.id)) continue;
      seen.add(child.id);
      descendants.push(child);
      queue.push(child.id);
    }
  }
  return descendants.toSorted(
    (left, right) =>
      left.createdAt.localeCompare(right.createdAt) || left.id.localeCompare(right.id),
  );
}

const pullRequestKey = (link: ThreadPullRequestLink) =>
  `${link.host.toLowerCase()}/${link.repository.toLowerCase()}#${link.number}`;

interface RolledUpPullRequests {
  readonly links: ReadonlyArray<ThreadPullRequestLink>;
  /** The descendant holding each rolled-up link, by host, repository and number. */
  readonly linkedBy: ReadonlyMap<string, DescendantShell>;
}

/**
 * The thread's visible pull requests, then its descendants' not already among them, each once.
 * The earliest descendant holding a link is the one shown; one the thread dismissed stays hidden.
 */
export function rollUpThreadPullRequests(
  own: ReadonlyArray<ThreadPullRequestLink>,
  descendants: ReadonlyArray<DescendantShell>,
): RolledUpPullRequests {
  const seen = new Set(own.map(pullRequestKey));
  const links = [...visibleThreadPullRequests(own)];
  const linkedBy = new Map<string, DescendantShell>();
  for (const descendant of descendants) {
    for (const link of visibleThreadPullRequests(descendant.pullRequests)) {
      const key = pullRequestKey(link);
      if (seen.has(key)) continue;
      seen.add(key);
      links.push(link);
      linkedBy.set(key, descendant);
    }
  }
  return { links, linkedBy };
}

export function linkedByOf(
  rolledUp: RolledUpPullRequests,
  link: ThreadPullRequestLink,
): DescendantShell | null {
  return rolledUp.linkedBy.get(pullRequestKey(link)) ?? null;
}
