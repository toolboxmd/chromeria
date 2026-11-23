import type { ScopedProjectRef } from "@t3tools/contracts";

import {
  isThreadVisibleInNavigation,
  type EnvironmentProject,
  type EnvironmentThreadShell,
} from "../state/models.ts";
import {
  deriveLogicalProjectKeyFromSettings,
  type ProjectGroupingSettings,
} from "../state/projectGrouping.ts";

/**
 * The projects that make up the Promachos home: the chosen home and every
 * project the sidebar merges with it, such as the same repository on another
 * machine. The Promachos runs on any of them.
 */
export function promachosHomeRefs(
  projects: ReadonlyArray<
    Pick<EnvironmentProject, "environmentId" | "id" | "workspaceRoot" | "repositoryIdentity">
  >,
  home: ScopedProjectRef,
  settings: ProjectGroupingSettings,
): ScopedProjectRef[] {
  const homeProject = projects.find(
    (project) => project.environmentId === home.environmentId && project.id === home.projectId,
  );
  if (homeProject === undefined) return [];
  const homeKey = deriveLogicalProjectKeyFromSettings(homeProject, settings);
  return projects
    .filter((project) => deriveLogicalProjectKeyFromSettings(project, settings) === homeKey)
    .map((project) => ({ environmentId: project.environmentId, projectId: project.id }));
}

export function isInPromachosHome(
  projectRef: ScopedProjectRef,
  homeRefs: ReadonlyArray<ScopedProjectRef>,
): boolean {
  return homeRefs.some(
    (ref) =>
      ref.environmentId === projectRef.environmentId && ref.projectId === projectRef.projectId,
  );
}

/**
 * The Promachos's conversations: the home's own top-level threads on every
 * machine, newest activity first. Drafters he starts as child threads stay in
 * the Lineage section of the conversation that started them.
 */
export function promachosConversations<
  T extends Pick<
    EnvironmentThreadShell,
    | "id"
    | "environmentId"
    | "projectId"
    | "archivedAt"
    | "latestUserMessageAt"
    | "updatedAt"
    | "lineage"
    | "delegatedFrom"
  >,
>(threads: ReadonlyArray<T>, homeRefs: ReadonlyArray<ScopedProjectRef>): T[] {
  return threads
    .filter((thread) => isInPromachosHome(thread, homeRefs) && isThreadVisibleInNavigation(thread))
    .sort((left, right) => lastActivityMs(right) - lastActivityMs(left));
}

function lastActivityMs(thread: {
  readonly latestUserMessageAt: string | null;
  readonly updatedAt: string;
}): number {
  for (const candidate of [thread.latestUserMessageAt, thread.updatedAt]) {
    const parsed = candidate === null ? Number.NaN : Date.parse(candidate);
    if (!Number.isNaN(parsed)) return parsed;
  }
  return 0;
}
