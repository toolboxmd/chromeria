/**
 * Promachos mode on the phone: the Promachos tab shows the home's conversations, and a thread of
 * the home renders as his bubble chat. Each environment names its own home in its server settings,
 * chosen in Settings → Promachos; the home spans every project the thread list merges with one.
 */
import { useAtomValue } from "@effect/atom-react";
import {
  isInPromachosHome,
  promachosHomeRefs,
  promachosHomes,
} from "@t3tools/client-runtime/promachos/conversations";
import { DEFAULT_SERVER_SETTINGS, type ScopedProjectRef } from "@t3tools/contracts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import { AsyncResult } from "effect/reactivity";
import { use } from "react";

import { useEnvironmentServerConfig, useProjects, useServerConfigs } from "../../state/entities";
import { mobilePreferencesAtom } from "../../state/preferences";
import { useMobileProjectGroupingSettings } from "../../state/project-grouping";
import {
  navigationCustomizationAvailable,
  resolveHiddenNavigationTabs,
} from "../layout/workspace/navigation-tab-order";
import { PromachosDraftContext } from "./promachosDraft";

/**
 * The homes the connected environments name: `start` is where a new conversation opens, null when
 * none names a project this phone knows, and `refs` every project of the home. `active: false`
 * skips the work while the Promachos tab is hidden.
 */
export function usePromachosHome(active = true): {
  readonly homes: ReadonlyArray<ScopedProjectRef>;
  readonly start: ScopedProjectRef | null;
  readonly refs: ReadonlyArray<ScopedProjectRef>;
} {
  const projects = useProjects();
  const configs = useServerConfigs();
  const grouping = useMobileProjectGroupingSettings();
  const homes = active ? promachosHomes(configs, projects, null) : [];
  return {
    homes,
    start: homes[0] ?? null,
    refs: promachosHomeRefs(projects, homes, grouping),
  };
}

/**
 * The Promachos role's models when a thread in `projectRef` renders as a Promachos chat, else
 * null. A hidden Promachos tab turns the mode off, so the app behaves as without it.
 */
export function usePromachosChatModels(
  projectRef: ScopedProjectRef | null,
): ReadonlyArray<unknown> | null {
  const preferences = useAtomValue(mobilePreferencesAtom);
  const hidden =
    navigationCustomizationAvailable() &&
    AsyncResult.isSuccess(preferences) &&
    resolveHiddenNavigationTabs(preferences.value.hiddenNavigationTabs).includes("promachos");
  const { refs: homeRefs } = usePromachosHome(!hidden);
  const serverConfig = useEnvironmentServerConfig(projectRef?.environmentId ?? null);
  if (projectRef === null || !isInPromachosHome(projectRef, homeRefs)) return null;
  return resolveProjectSettings(
    serverConfig?.settings ?? DEFAULT_SERVER_SETTINGS,
    projectRef.projectId,
  ).settings.prismRoles.promachos.models;
}

/** The homes' names for Settings, or null when no environment names one. */
export function usePromachosHomeLabel(): string | null {
  const { homes } = usePromachosHome();
  const projects = useProjects();
  const titles = new Set(
    homes.flatMap(
      (home) =>
        projects.find(
          (project) =>
            project.environmentId === home.environmentId && project.id === home.projectId,
        )?.title ?? [],
    ),
  );
  return titles.size === 0 ? null : [...titles].join(", ");
}

/**
 * The Promachos role's models for a new conversation the Promachos tab's compose button started in
 * his home, else null: any other draft stays an ordinary thread, even in the home project.
 */
export function usePromachosDraftModels(
  projectRef: ScopedProjectRef | null,
): ReadonlyArray<unknown> | null {
  const models = usePromachosChatModels(projectRef);
  return use(PromachosDraftContext) ? models : null;
}
