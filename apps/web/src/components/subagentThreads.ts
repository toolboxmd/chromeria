/**
 * Child-thread identity (toolboxmd/t3code#8): a thread spawned by another
 * thread through the `threads` MCP toolkit carries its parent in its own id,
 * `sub.<parentThreadId>.<suffix>`. It needs no contract, projector or
 * migration change, survives restarts, and lets every client classify a
 * thread from its shell alone. A real optional `parentThreadId` on
 * `thread.create` would be cleaner but is too invasive for this fork (see
 * the server copy for the full tradeoff).
 *
 * Keep in sync with apps/server/src/mcp/toolkits/threads/subagentThreadId.ts.
 */
const PREFIX = "sub.";

export function makeSubagentThreadId(parentThreadId: string, suffix: string): string {
  return `${PREFIX}${parentThreadId}.${suffix}`;
}

export function isSubagentThreadId(threadId: string): boolean {
  return threadId.startsWith(PREFIX) && threadId.lastIndexOf(".") > PREFIX.length;
}

/** The spawning thread's id, or null for a thread the user started. */
export function parentThreadIdOf(threadId: string): string | null {
  if (!isSubagentThreadId(threadId)) return null;
  return threadId.slice(PREFIX.length, threadId.lastIndexOf("."));
}

/** Whether `threadId` was spawned by `ancestorId`, directly or through other child threads. */
export function isDescendantThreadId(threadId: string, ancestorId: string): boolean {
  for (
    let parent = parentThreadIdOf(threadId);
    parent !== null;
    parent = parentThreadIdOf(parent)
  ) {
    if (parent === ancestorId) return true;
  }
  return false;
}
