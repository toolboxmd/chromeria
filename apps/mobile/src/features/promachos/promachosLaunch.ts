import { promachosMembership } from "@t3tools/client-runtime/promachos/conversations";
import { enabledEnvironmentIds } from "@t3tools/client-runtime/state/connections";
import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { AsyncResult, Atom } from "effect/reactivity";

import { environmentCatalog } from "../../connection/catalog";
import { mobilePreferencesAtom } from "../../state/preferences";
import {
  DEFAULT_MOBILE_PROJECT_GROUPING_SETTINGS,
  resolveMobileProjectGroupingSettings,
} from "../../state/project-grouping.logic";
import { environmentProjects } from "../../state/projects";
import { environmentServerConfigsAtom } from "../../state/server";
import { environmentSnapshotAtom } from "../../state/shell";
import { environmentThreadShells } from "../../state/threads";

const membershipAtom = Atom.family((key: string) => {
  const [environmentId, threadId] = JSON.parse(key) as [EnvironmentId, ThreadId];
  return Atom.make((get) => {
    const configs = get(environmentServerConfigsAtom);
    const loaded = [...enabledEnvironmentIds(get(environmentCatalog.catalogValueAtom))].every(
      (id) => configs.has(id) && get(environmentSnapshotAtom(id)) !== null,
    );
    const shell = get(environmentThreadShells.threadShellAtom({ environmentId, threadId }));
    const preferences = get(mobilePreferencesAtom);
    return promachosMembership({
      projectRef: shell === null ? null : { environmentId, projectId: shell.projectId },
      loaded,
      configs,
      projects: get(environmentProjects.projectsAtom),
      grouping: AsyncResult.isSuccess(preferences)
        ? resolveMobileProjectGroupingSettings(preferences.value)
        : DEFAULT_MOBILE_PROJECT_GROUPING_SETTINGS,
    });
  }).pipe(Atom.withLabel(`promachos-conversation-membership:${key}`));
});

/**
 * Whether the remembered Promachos conversation is still in the home, "unknown" until every
 * connected machine has loaded. Another device may have moved the home since it was remembered.
 */
export function promachosConversationMembershipAtom(conversation: {
  readonly environmentId: string;
  readonly threadId: string;
}) {
  return membershipAtom(JSON.stringify([conversation.environmentId, conversation.threadId]));
}
