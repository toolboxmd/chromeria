/**
 * Agents panel structure for child threads (toolboxmd/t3code#17): which
 * threads nest under a row, and the parent/sibling links for a child
 * thread's header breadcrumb.
 *
 * Pure functions over thread shells and the subagent fold, so every client
 * can reuse them and tests need no rendering.
 */
import type { RuntimeSubagentStatus } from "@t3tools/client-runtime/state/subagentRuntime";
import type { OrchestrationThreadShell } from "@t3tools/contracts";

import { parentThreadIdOf } from "./subagentThreads";

/** Threads carry their parent in the `sub.<parent>.<suffix>` id; a real field wins. */
function parentThreadIdOfShell(shell: {
  readonly id: string;
  readonly parentThreadId?: string | null;
}): string | null {
  return shell.parentThreadId ?? parentThreadIdOf(shell.id);
}

interface TreeShell {
  readonly id: string;
  readonly createdAt: string;
  readonly archivedAt: string | null;
  readonly parentThreadId?: string | null;
}

/** Live child threads per parent id, in spawn order. */
export function childThreadsByParent<T extends TreeShell>(
  shells: ReadonlyArray<T>,
): ReadonlyMap<string, ReadonlyArray<T>> {
  const byParent = new Map<string, T[]>();
  for (const shell of shells) {
    if (shell.archivedAt !== null) continue;
    const parentId = parentThreadIdOfShell(shell);
    if (parentId === null) continue;
    const siblings = byParent.get(parentId);
    if (siblings) siblings.push(shell);
    else byParent.set(parentId, [shell]);
  }
  for (const siblings of byParent.values()) {
    siblings.sort((left, right) => left.createdAt.localeCompare(right.createdAt));
  }
  return byParent;
}

/**
 * Coarse Agents-panel status for a thread known only by its shell. A live or
 * stopped session outranks its last turn, which a stop can leave unfinished.
 */
export function threadShellStatus(
  shell: Pick<OrchestrationThreadShell, "session" | "latestTurn">,
): RuntimeSubagentStatus {
  const session = shell.session?.status;
  const turn = shell.latestTurn?.state;
  if (session === "running" || session === "starting") return "running";
  if (session === "interrupted") return "interrupted";
  if (turn === "running") return "running";
  if (session === "error" || turn === "error") return "failed";
  if (turn === "interrupted") return "interrupted";
  if (turn === "completed") return "idle";
  return "pending";
}

/** Toggles one row's children; nothing else changes, so nothing opens on its own. */
export function toggleExpandedThread(
  expanded: ReadonlySet<string>,
  threadId: string,
): ReadonlySet<string> {
  const next = new Set(expanded);
  if (!next.delete(threadId)) next.add(threadId);
  return next;
}

export interface ThreadBreadcrumbLinks<T> {
  readonly parent: T;
  readonly siblings: ReadonlyArray<T>;
}

/**
 * Parent and siblings (including the thread itself) for a child thread's
 * header, or null for a thread the user started or whose parent is gone.
 */
export function threadBreadcrumbLinks<T extends TreeShell>(
  threadId: string,
  shells: ReadonlyArray<T>,
): ThreadBreadcrumbLinks<T> | null {
  const self = shells.find((shell) => shell.id === threadId);
  const parentId = self ? parentThreadIdOfShell(self) : parentThreadIdOf(threadId);
  if (parentId === null) return null;
  const parent = shells.find((shell) => shell.id === parentId);
  if (!parent) return null;
  return { parent, siblings: childThreadsByParent(shells).get(parentId) ?? [] };
}
