import type { EnvironmentId, ScopedProjectRef, ServerSettings } from "@t3tools/contracts";

import {
  isThreadVisibleInNavigation,
  type EnvironmentProject,
  type EnvironmentThreadShell,
} from "../state/models.ts";
import type { OlderSettledScope } from "../state/olderSettledThreads.ts";
import {
  deriveLogicalProjectKeyFromSettings,
  type ProjectGroupingSettings,
} from "../state/projectGrouping.ts";
import { effectiveSnoozed, type ThreadSnoozeShell } from "../state/threadSettled.ts";

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
 * Whether a thread's project is in the Promachos home. "unknown" until the client knows the thread
 * and every named home's project, and every connected environment has answered this session with
 * its settings and projects: a cached copy may predate a home another device chose, and a "no"
 * from it would wrongly drop the thread.
 */
export function promachosMembership(input: {
  readonly projectRef: ScopedProjectRef | null;
  readonly environments: ReadonlyArray<{
    readonly settingsLive: boolean;
    readonly projectsLive: boolean;
  }>;
  readonly configs: ReadonlyMap<
    EnvironmentId,
    { readonly settings: Pick<ServerSettings, "promachosHome"> }
  >;
  readonly projects: ReadonlyArray<HomeProject>;
  readonly grouping: ProjectGroupingSettings;
}): "in" | "out" | "unknown" {
  const live = input.environments.every(
    (environment) => environment.settingsLive && environment.projectsLive,
  );
  if (!live || input.projectRef === null) return "unknown";
  const homes = promachosHomes(input.configs, input.projects, null);
  // A home whose project has not reached this client yet may take the thread in.
  const named = [...input.configs.values()].filter(
    (config) => config.settings.promachosHome !== null,
  );
  if (homes.length < named.length) return "unknown";
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

/**
 * Promachos mode's list: a conversation the user snoozed or settled leaves it for its own group,
 * as in the thread list, and returns when it wakes or is un-settled. `supports` is false for an
 * environment whose server has not answered yet; its conversations stay listed.
 */
export function promachosConversationGroups<
  T extends ThreadSnoozeShell & Pick<EnvironmentThreadShell, "environmentId" | "settledOverride">,
>(
  conversations: ReadonlyArray<T>,
  options: {
    readonly now: string;
    readonly supports: (environmentId: EnvironmentId) => boolean;
  },
): { readonly active: T[]; readonly snoozed: T[]; readonly settled: T[] } {
  const groups = { active: [] as T[], snoozed: [] as T[], settled: [] as T[] };
  for (const conversation of conversations) {
    const supported = options.supports(conversation.environmentId);
    (supported && effectiveSnoozed(conversation, options)
      ? groups.snoozed
      : supported && conversation.settledOverride === "settled"
        ? groups.settled
        : groups.active
    ).push(conversation);
  }
  return groups;
}

/** The home's projects per environment: the scope its older settled conversations page in. */
export function promachosOlderSettledScopes(
  homeRefs: ReadonlyArray<ScopedProjectRef>,
): OlderSettledScope[] {
  const scopes = new Map<EnvironmentId, ScopedProjectRef["projectId"][]>();
  for (const ref of homeRefs) {
    scopes.set(ref.environmentId, [...(scopes.get(ref.environmentId) ?? []), ref.projectId]);
  }
  return [...scopes].map(([environmentId, projectIds]) => ({ environmentId, projectIds }));
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
