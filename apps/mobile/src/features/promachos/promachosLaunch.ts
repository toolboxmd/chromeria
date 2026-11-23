import {
  isInPromachosHome,
  promachosHomeRefs,
  promachosHomes,
} from "@t3tools/client-runtime/promachos/conversations";
import type { EnvironmentId, ThreadId } from "@t3tools/contracts";

import type { Preferences } from "../../persistence/mobile-preferences";
import { appAtomRegistry } from "../../state/atom-registry";
import { resolveMobileProjectGroupingSettings } from "../../state/project-grouping.logic";
import { environmentProjects } from "../../state/projects";
import { environmentServerConfigsAtom } from "../../state/server";
import { environmentThreadShells } from "../../state/threads";

/**
 * Whether a cold start may reopen the remembered conversation on the Promachos tab: false once its
 * machine's settings and the thread have loaded and the thread is no longer in the home, as after
 * another device changed the home.
 */
export function promachosConversationInHome(
  conversation: { readonly environmentId: string; readonly threadId: string },
  preferences: Preferences | null,
): boolean {
  const environmentId = conversation.environmentId as EnvironmentId;
  const configs = appAtomRegistry.get(environmentServerConfigsAtom);
  const shell = appAtomRegistry.get(
    environmentThreadShells.threadShellAtom({
      environmentId,
      threadId: conversation.threadId as ThreadId,
    }),
  );
  if (!configs.has(environmentId) || shell === null) return true;
  const projects = appAtomRegistry.get(environmentProjects.projectsAtom);
  const grouping = resolveMobileProjectGroupingSettings(preferences ?? {});
  const refs = promachosHomeRefs(projects, promachosHomes(configs, projects, null), grouping);
  return isInPromachosHome({ environmentId, projectId: shell.projectId }, refs);
}
