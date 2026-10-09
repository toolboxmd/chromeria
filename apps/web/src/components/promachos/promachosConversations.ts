import type {
  EnvironmentProject,
  EnvironmentThreadShell,
} from "@t3tools/client-runtime/state/models";
import type { ScopedProjectRef } from "@t3tools/contracts";

import {
  deriveLogicalProjectKeyFromSettings,
  type ProjectGroupingSettings,
} from "../../logicalProject";
import { firstValidTimestampMs } from "../Sidebar.logic";

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
  >,
>(threads: ReadonlyArray<T>, homeRefs: ReadonlyArray<ScopedProjectRef>): T[] {
  return threads
    .filter(
      (thread) =>
        isInPromachosHome(thread, homeRefs) &&
        thread.archivedAt === null &&
        thread.lineage.relationshipToParent !== "subagent",
    )
    .toSorted(
      (left, right) =>
        firstValidTimestampMs(right.latestUserMessageAt, right.updatedAt) -
        firstValidTimestampMs(left.latestUserMessageAt, left.updatedAt),
    );
}
