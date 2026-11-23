import { useAtomValue } from "@effect/atom-react";
import { useIsFocused } from "@react-navigation/native";
import type { EnvironmentId, ProjectId, ThreadId } from "@t3tools/contracts";
import { AsyncResult } from "effect/reactivity";
import { useEffect, type ReactNode } from "react";

import { mobilePreferencesAtom, saveMobilePreferences } from "../../state/preferences";
import { useAtomCommand } from "../../state/use-atom-command";
import { useWorkspaceTabs } from "../layout/workspace/WorkspaceTabsContext";
import { PromachosChatContext } from "./PromachosBubbles";
import { usePromachosChatModels } from "./promachosHome";

/**
 * Marks a thread of the Promachos home as his chat, so its replies render as bubbles, and
 * remembers it as the conversation the Promachos tab shows while that tab opened it.
 */
export function PromachosChatScope(props: {
  readonly environmentId: EnvironmentId;
  readonly projectId: ProjectId;
  readonly threadId: ThreadId;
  readonly children: ReactNode;
}) {
  const chat =
    usePromachosChatModels({ environmentId: props.environmentId, projectId: props.projectId }) !==
    null;
  const promachosTab = useWorkspaceTabs()?.state.selectedTab === "promachos";
  const focused = useIsFocused();
  const remember = chat && promachosTab && focused;
  const preferences = useAtomValue(mobilePreferencesAtom);
  const saved = AsyncResult.isSuccess(preferences)
    ? preferences.value.promachosConversation
    : undefined;
  const save = useAtomCommand(saveMobilePreferences);
  const current = saved?.environmentId === props.environmentId && saved.threadId === props.threadId;
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- persists the shown conversation so a cold start reopens it
  useEffect(() => {
    if (!remember || current) return;
    void save({
      promachosConversation: { environmentId: props.environmentId, threadId: props.threadId },
    });
  }, [remember, current, save, props.environmentId, props.threadId]);
  return <PromachosChatContext value={chat}>{props.children}</PromachosChatContext>;
}
