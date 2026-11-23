import { useGT } from "gt-react-native";

import { useAdaptiveWorkspaceLayout } from "../adaptive-workspace-context";
import { usePromachosCompose } from "../../promachos/usePromachosCompose";
import { useWorkspaceTabs } from "./WorkspaceTabsContext";

/** What the compose button starts on the selected tab; null where the tab has nothing to start. */
export function useWorkspaceCompose() {
  const gt = useGT();
  const workspace = useWorkspaceTabs();
  const { startNewTask } = useAdaptiveWorkspaceLayout();
  // Fork: the Promachos tab starts a conversation in his home, from the same button.
  const promachos = usePromachosCompose();
  if (workspace && !workspace.settingsOpen && workspace.state.selectedTab === "promachos")
    return promachos;
  if (!workspace || workspace.settingsOpen || workspace.state.selectedTab !== "threads")
    return null;
  return { label: gt("New chat"), onPress: startNewTask } as const;
}
