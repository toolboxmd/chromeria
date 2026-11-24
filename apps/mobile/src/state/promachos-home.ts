import { promachosHomeRefs, promachosHomes } from "@t3tools/client-runtime/promachos/conversations";
import { Atom, AsyncResult } from "effect/reactivity";

import { mobilePreferencesAtom } from "./preferences";
import {
  DEFAULT_MOBILE_PROJECT_GROUPING_SETTINGS,
  resolveMobileProjectGroupingSettings,
} from "./project-grouping.logic";
import { environmentProjects } from "./projects";
import { environmentServerConfigsAtom } from "./server";

/** Every project of the Promachos home, for the thread list's Promachos shelf. */
export const promachosHomeRefsAtom = Atom.make((get) => {
  const projects = get(environmentProjects.projectsAtom);
  const preferences = get(mobilePreferencesAtom);
  return promachosHomeRefs(
    projects,
    promachosHomes(get(environmentServerConfigsAtom), projects, null),
    AsyncResult.isSuccess(preferences)
      ? resolveMobileProjectGroupingSettings(preferences.value)
      : DEFAULT_MOBILE_PROJECT_GROUPING_SETTINGS,
  );
}).pipe(Atom.withLabel("mobile-promachos-home-refs"));
