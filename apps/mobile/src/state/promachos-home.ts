import { promachosHomeRefs, promachosHomes } from "@t3tools/client-runtime/promachos/conversations";
import type { ScopedProjectRef } from "@t3tools/contracts";
import { Atom, AsyncResult } from "effect/reactivity";

import { mobilePreferencesAtom } from "./preferences";
import {
  DEFAULT_MOBILE_PROJECT_GROUPING_SETTINGS,
  resolveMobileProjectGroupingSettings,
} from "./project-grouping.logic";
import { environmentProjects } from "./projects";
import { environmentServerConfigsAtom } from "./server";

let lastRefs: ReadonlyArray<ScopedProjectRef> = [];

/**
 * Every project of the Promachos home, for the thread list's Promachos shelf. The same projects
 * keep the same array, so an unrelated preference change does not rebuild the list.
 */
export const promachosHomeRefsAtom = Atom.make((get) => {
  const projects = get(environmentProjects.projectsAtom);
  const preferences = get(mobilePreferencesAtom);
  const refs = promachosHomeRefs(
    projects,
    promachosHomes(get(environmentServerConfigsAtom), projects, null),
    AsyncResult.isSuccess(preferences)
      ? resolveMobileProjectGroupingSettings(preferences.value)
      : DEFAULT_MOBILE_PROJECT_GROUPING_SETTINGS,
  );
  const same =
    refs.length === lastRefs.length &&
    refs.every(
      (ref, index) =>
        ref.environmentId === lastRefs[index]!.environmentId &&
        ref.projectId === lastRefs[index]!.projectId,
    );
  if (!same) lastRefs = refs;
  return lastRefs;
}).pipe(Atom.withLabel("mobile-promachos-home-refs"));
