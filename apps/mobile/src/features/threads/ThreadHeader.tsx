import { useGT, useMessages } from "gt-react-native";

import { AppBar } from "../../components/ui/AppBar";
import type { AppBarAction, AppBarMenu, AppBarMenuItem } from "../../components/ui/AppBar.types";
import {
  useAdaptiveWorkspaceLayout,
  useWorkspacePaneState,
} from "../layout/AdaptiveWorkspaceLayout";
import type { ThreadInspectorMode } from "./thread-inspector-navigation";
import { useWorkspaceBackNavigation } from "../layout/workspace/useAndroidWorkspaceNavigation";
import { EnvironmentId, type ProjectId } from "@t3tools/contracts";

import { usePromachosChatModels } from "../promachos/promachosHome";
import { PromachosTitlePill } from "../promachos/PromachosTitlePill";
import {
  threadTerminalMenu,
  useThreadGitMenuDefinition,
  type ThreadGitControlsProps,
} from "./ThreadGitControls";

export function ThreadHeader(props: {
  readonly title: string;
  readonly subtitle: string;
  readonly gitControls: ThreadGitControlsProps;
  readonly onReturnToThread?: () => void;
  readonly draft?: boolean;
  readonly hasThreadCwd: boolean;
  readonly inspectorMode: ThreadInspectorMode | null;
  readonly onOpenFilesInspector: () => void;
  /** Fork: a Promachos chat shows his name pill and one "…" menu (toolboxmd/chromeria#898). */
  readonly projectId?: ProjectId;
}) {
  const gt = useGT();
  const m = useMessages();
  const goBack = useWorkspaceBackNavigation();
  const { toggleAuxiliaryPane, startNewTask } = useAdaptiveWorkspaceLayout();
  const { panes } = useWorkspacePaneState();
  const gitMenu = useThreadGitMenuDefinition(props.gitControls);
  const terminalMenu = threadTerminalMenu(props.gitControls, m);
  const filesVisible = props.inspectorMode === "files" && panes.auxiliaryPaneVisible;
  const actions: AppBarAction[] = props.hasThreadCwd
    ? [
        {
          accessibilityLabel: filesVisible ? gt("Close files") : gt("Open files"),
          icon: "folder",
          selected: filesVisible,
          disabled: !props.gitControls.canOpenFiles,
          separateBackground: false,
          onPress: filesVisible ? toggleAuxiliaryPane : props.onOpenFilesInspector,
        },
      ]
    : [];
  const menus = [gitMenu, terminalMenu].filter((menu): menu is AppBarMenu => menu !== null);
  const promachos =
    usePromachosChatModels(
      props.projectId === undefined
        ? null
        : {
            environmentId: EnvironmentId.make(String(props.gitControls.environmentId)),
            projectId: props.projectId,
          },
    ) !== null;

  return (
    <AppBar
      title={props.title}
      {...(promachos
        ? { titleContent: <PromachosTitlePill title={props.title} />, centerTitleContent: true }
        : { subtitle: props.subtitle })}
      onBackToList={goBack}
      onCompose={props.draft || promachos ? undefined : startNewTask}
      backInSplitView={
        props.onReturnToThread
          ? {
              accessibilityLabel: gt("Return to chat"),
              icon: "chevron.left",
              onPress: props.onReturnToThread,
            }
          : undefined
      }
      actions={promachos ? [] : actions}
      menus={promachos ? promachosMenu(gt("More"), actions, menus) : menus}
      hideBottomBorder
    />
  );
}

/** Every header control of the standard view, folded into one "…" menu. */
function promachosMenu(
  title: string,
  actions: ReadonlyArray<AppBarAction>,
  menus: ReadonlyArray<AppBarMenu>,
): AppBarMenu[] {
  const items: AppBarMenuItem[] = [
    ...actions.map((action) => ({
      id: action.accessibilityLabel,
      title: action.accessibilityLabel,
      ...(action.disabled ? { disabled: true } : {}),
      onPress: action.onPress,
    })),
    ...menus.map((menu) => ({ id: menu.title, title: menu.title, items: menu.items })),
  ];
  return items.length === 0 ? [] : [{ title, icon: "ellipsis", items }];
}
