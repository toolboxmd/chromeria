// Fork: parent and sibling links for a child thread's header (toolboxmd/chromeria#201).

interface CrumbShell<Id extends string = string> {
  readonly id: Id;
  readonly environmentId: string;
  readonly createdAt: string;
  readonly archivedAt: string | null;
  readonly lineage: {
    readonly parentThreadId: Id | null;
    readonly relationshipToParent: "fork" | "subagent" | null;
  };
}

/** The parent a child thread reports to; forks are separate conversations, not children. */
export function childParentThreadId<Id extends string>(shell: CrumbShell<Id> | null): Id | null {
  if (shell === null || shell.lineage.relationshipToParent === "fork") return null;
  return shell.lineage.parentThreadId;
}

/** Live children of `parentId` in one environment, oldest first, excluding `selfId`. */
export function siblingThreads<T extends CrumbShell>(
  shells: ReadonlyArray<T>,
  environmentId: string,
  parentId: string,
  selfId: string,
): ReadonlyArray<T> {
  return shells
    .filter(
      (shell) =>
        shell.environmentId === environmentId &&
        shell.id !== selfId &&
        shell.archivedAt === null &&
        childParentThreadId(shell) === parentId,
    )
    .toSorted((left, right) => left.createdAt.localeCompare(right.createdAt));
}
