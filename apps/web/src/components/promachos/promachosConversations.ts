import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import type { ScopedProjectRef } from "@t3tools/contracts";

import { firstValidTimestampMs } from "../Sidebar.logic";
import { parentThreadIdOf } from "../subagentThreads";

/**
 * The Promachos's conversations: the home's own top-level threads, newest
 * activity first. Drafters he starts as child threads stay in the Agents
 * panel of the conversation that started them.
 */
export function promachosConversations<
  T extends Pick<
    EnvironmentThreadShell,
    "id" | "environmentId" | "projectId" | "archivedAt" | "latestUserMessageAt" | "updatedAt"
  >,
>(threads: ReadonlyArray<T>, home: ScopedProjectRef): T[] {
  return threads
    .filter(
      (thread) =>
        thread.environmentId === home.environmentId &&
        thread.projectId === home.projectId &&
        thread.archivedAt === null &&
        parentThreadIdOf(thread.id) === null,
    )
    .toSorted(
      (left, right) =>
        firstValidTimestampMs(right.latestUserMessageAt, right.updatedAt) -
        firstValidTimestampMs(left.latestUserMessageAt, left.updatedAt),
    );
}
