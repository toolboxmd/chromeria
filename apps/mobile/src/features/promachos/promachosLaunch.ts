import { promachosMembership } from "@t3tools/client-runtime/promachos/conversations";
import { enabledEnvironmentIds } from "@t3tools/client-runtime/state/connections";
import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { AsyncResult, Atom } from "effect/reactivity";

import { environmentCatalog } from "../../connection/catalog";
import { mobilePreferencesAtom } from "../../state/preferences";
import { resolveMobileProjectGroupingSettings } from "../../state/project-grouping.logic";
import { environmentProjects } from "../../state/projects";
import { environmentServerConfigsAtom, serverEnvironment } from "../../state/server";
import { environmentShell } from "../../state/shell";
import { environmentThreadShells } from "../../state/threads";

const membershipAtom = Atom.family((key: string) => {
  const [environmentId, threadId] = JSON.parse(key) as [EnvironmentId, ThreadId];
  return Atom.make((get) => {
    const catalog = get(environmentCatalog.catalogValueAtom);
    if (!catalog.isReady) return "unknown" as const;
    const configs = get(environmentServerConfigsAtom);
    const environments = [...enabledEnvironmentIds(catalog)].map((id) => ({
      settingsLive: configs.has(id) && get(serverEnvironment.configLiveAtom(id)),
      projectsLive: get(environmentShell.statusAtom(id)) === "live",
    }));
    const preferences = get(mobilePreferencesAtom);
    // Grouping decides which projects join a home, so it must be the phone's own.
    if (!AsyncResult.isSuccess(preferences)) return "unknown" as const;
    const shell = get(environmentThreadShells.threadShellAtom({ environmentId, threadId }));
    return promachosMembership({
      projectRef: shell === null ? null : { environmentId, projectId: shell.projectId },
      environments,
      configs,
      projects: get(environmentProjects.projectsAtom),
      grouping: resolveMobileProjectGroupingSettings(preferences.value),
    });
  }).pipe(Atom.withLabel(`promachos-conversation-membership:${key}`));
});

/**
 * Whether the remembered Promachos conversation is still in the home, "unknown" until every
 * connected machine has answered live this session. Another device may have moved the home since
 * it was remembered, and the launch cache may predate that.
 */
export function promachosConversationMembershipAtom(conversation: {
  readonly environmentId: string;
  readonly threadId: string;
}) {
  return membershipAtom(JSON.stringify([conversation.environmentId, conversation.threadId]));
}
