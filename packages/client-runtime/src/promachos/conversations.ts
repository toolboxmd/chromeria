import type { EnvironmentId, ScopedProjectRef, ServerSettings } from "@t3tools/contracts";

import {
  isThreadVisibleInNavigation,
  type EnvironmentProject,
  type EnvironmentThreadShell,
} from "../state/models.ts";
import {
  deriveLogicalProjectKeyFromSettings,
  type ProjectGroupingSettings,
} from "../state/projectGrouping.ts";

type HomeProject = Pick<
  EnvironmentProject,
  "environmentId" | "id" | "workspaceRoot" | "repositoryIdentity"
>;

/**
 * The homes the connected environments name in their own settings, limited to projects this client
 * knows. `preferredEnvironmentId`'s home comes first, so a new conversation starts there.
 */
export function promachosHomes(
  configs: ReadonlyMap<EnvironmentId, { readonly settings: Pick<ServerSettings, "promachosHome"> }>,
  projects: ReadonlyArray<Pick<EnvironmentProject, "environmentId" | "id">>,
  preferredEnvironmentId: EnvironmentId | null,
): ScopedProjectRef[] {
  const homes: ScopedProjectRef[] = [];
  for (const [environmentId, config] of configs) {
    const projectId = config.settings.promachosHome;
    if (projectId === null) continue;
    if (
      !projects.some(
        (project) => project.environmentId === environmentId && project.id === projectId,
      )
    )
      continue;
    const home = { environmentId, projectId };
    if (environmentId === preferredEnvironmentId) homes.unshift(home);
    else homes.push(home);
  }
  return homes;
}

/**
 * The projects that make up the Promachos home: each named home and every project the sidebar
 * merges with one, such as the same repository on a machine that names no home. The Promachos runs
 * on any of them.
 */
export function promachosHomeRefs(
  projects: ReadonlyArray<HomeProject>,
  homes: ReadonlyArray<ScopedProjectRef>,
  settings: ProjectGroupingSettings,
): ScopedProjectRef[] {
  if (homes.length === 0) return [];
  const homeKeys = new Set(
    projects
      .filter((project) =>
        isInPromachosHome({ environmentId: project.environmentId, projectId: project.id }, homes),
      )
      .map((project) => deriveLogicalProjectKeyFromSettings(project, settings)),
  );
  return projects
    .filter((project) => homeKeys.has(deriveLogicalProjectKeyFromSettings(project, settings)))
    .map((project) => ({ environmentId: project.environmentId, projectId: project.id }));
}

/**
 * Whether a thread's project is in the Promachos home. "unknown" until every connected
 * environment's settings and projects have loaded and the thread itself is known: a home named by
 * a machine that has not answered yet must not count as no home.
 */
export function promachosMembership(input: {
  readonly projectRef: ScopedProjectRef | null;
  readonly loaded: boolean;
  readonly configs: ReadonlyMap<
    EnvironmentId,
    { readonly settings: Pick<ServerSettings, "promachosHome"> }
  >;
  readonly projects: ReadonlyArray<HomeProject>;
  readonly grouping: ProjectGroupingSettings;
}): "in" | "out" | "unknown" {
  if (!input.loaded || input.projectRef === null) return "unknown";
  const homes = promachosHomes(input.configs, input.projects, null);
  const refs = promachosHomeRefs(input.projects, homes, input.grouping);
  return isInPromachosHome(input.projectRef, refs) ? "in" : "out";
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
