/**
 * Promachos mode on the phone: the Promachos tab shows the home's conversations, and a thread of
 * the home renders as his bubble chat. The home lives in this phone's preferences, naming one
 * project on one environment; it spans every project the thread list merges with it.
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

import { useEnvironmentServerConfig, useProjects } from "../../state/entities";
import { mobilePreferencesAtom, saveMobilePreferences } from "../../state/preferences";
import { useMobileProjectGroupingSettings } from "../../state/project-grouping";
import { useAtomCommand } from "../../state/use-atom-command";
import { useHiddenNavigationTabs } from "../layout/workspace/navigation-tabs";

export function usePromachosHome(): [
  ScopedProjectRef | null,
  (home: ScopedProjectRef | null) => void,
] {
  const preferences = useAtomValue(mobilePreferencesAtom);
  const save = useAtomCommand(saveMobilePreferences);
  const saved = AsyncResult.isSuccess(preferences) ? preferences.value.promachosHome : undefined;
  const home =
    saved === undefined
      ? null
      : {
          environmentId: EnvironmentId.make(saved.environmentId),
          projectId: ProjectId.make(saved.projectId),
        };
  const setHome = (next: ScopedProjectRef | null) =>
    void save({ promachosHome: next ?? undefined });
  return [home, setHome];
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
  const hidden = useHiddenNavigationTabs().includes("promachos");
  const [home] = usePromachosHome();
  const homeRefs = usePromachosHomeRefs(hidden ? null : home);
  const serverConfig = useEnvironmentServerConfig(projectRef?.environmentId ?? null);
  if (projectRef === null || !isInPromachosHome(projectRef, homeRefs)) return null;
  return resolveProjectSettings(
    serverConfig?.settings ?? DEFAULT_SERVER_SETTINGS,
    projectRef.projectId,
  ).settings.prismRoles.promachos.models;
}
