/**
 * Promachos mode on the phone: the Promachos tab shows the home's conversations, and a thread of
 * the home renders as his bubble chat. The home lives in this phone's preferences, naming one
 * project on one environment; it spans every project the thread list merges with it. It is chosen in
 * Settings → Promachos.
 */
import { useAtomValue } from "@effect/atom-react";
import {
  isInPromachosHome,
  promachosHomeRefs,
} from "@t3tools/client-runtime/promachos/conversations";
import {
  DEFAULT_SERVER_SETTINGS,
  EnvironmentId,
  ProjectId,
  type ScopedProjectRef,
} from "@t3tools/contracts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import { AsyncResult } from "effect/reactivity";
import { use } from "react";

import { useEnvironmentServerConfig, useProjects } from "../../state/entities";
import { mobilePreferencesAtom, saveMobilePreferences } from "../../state/preferences";
import { useMobileProjectGroupingSettings } from "../../state/project-grouping";
import { useAtomCommand } from "../../state/use-atom-command";
import {
  navigationCustomizationAvailable,
  resolveHiddenNavigationTabs,
} from "../layout/workspace/navigation-tab-order";
import { PromachosDraftContext } from "./promachosDraft";

/** The saved home and how to change it. */
export function usePromachosHome(): {
  readonly home: ScopedProjectRef | null;
  readonly setHome: (home: ScopedProjectRef) => void;
} {
  const preferences = useAtomValue(mobilePreferencesAtom);
  const save = useAtomCommand(saveMobilePreferences);
  const saved = AsyncResult.isSuccess(preferences) ? preferences.value.promachosHome : undefined;
  return {
    home:
      saved === undefined
        ? null
        : {
            environmentId: EnvironmentId.make(saved.environmentId),
            projectId: ProjectId.make(saved.projectId),
          },
    // A conversation of the old home must not reopen on the next cold start.
    setHome: (next) => void save({ promachosHome: next, promachosConversation: undefined }),
  };
}

/** Every project of the Promachos home, across the machines it is merged over. */
export function usePromachosHomeRefs(
  home: ScopedProjectRef | null,
): ReadonlyArray<ScopedProjectRef> {
  const projects = useProjects();
  const grouping = useMobileProjectGroupingSettings();
  return home === null ? [] : promachosHomeRefs(projects, home, grouping);
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
  const { home } = usePromachosHome();
  const homeRefs = usePromachosHomeRefs(hidden ? null : home);
  const serverConfig = useEnvironmentServerConfig(projectRef?.environmentId ?? null);
  if (projectRef === null || !isInPromachosHome(projectRef, homeRefs)) return null;
  return resolveProjectSettings(
    serverConfig?.settings ?? DEFAULT_SERVER_SETTINGS,
    projectRef.projectId,
  ).settings.prismRoles.promachos.models;
}

/** The home's name for Settings, or null when there is none. */
export function usePromachosHomeLabel(): string | null {
  const { home } = usePromachosHome();
  const projects = useProjects();
  if (home === null) return null;
  return (
    projects.find(
      (project) => project.environmentId === home.environmentId && project.id === home.projectId,
    )?.title ?? null
  );
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
