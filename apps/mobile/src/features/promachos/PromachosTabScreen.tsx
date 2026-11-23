import { useNavigation } from "@react-navigation/native";
import { AndroidHomeFabLayout } from "../home/AndroidHomeFab";
import { usePromachosCompose } from "./usePromachosCompose";
import { Button } from "../../components/ui/Button";
import { openSettings } from "../settings/settings-navigation";
import { useAtomValue } from "@effect/atom-react";
import { useIsFocused } from "@react-navigation/native";
import { promachosConversations } from "@t3tools/client-runtime/promachos/conversations";
import {
  createEnvironmentRpcCommand,
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import {
  DEFAULT_PROMACHOS_HOME_PATH,
  PROMACHOS_HOME_WS_METHODS,
  type EnvironmentId,
  type ProjectId,
  type ScopedProjectRef,
} from "@t3tools/contracts";
import { useGT } from "gt-react-native";
import { AsyncResult } from "effect/reactivity";
import { use, useEffect } from "react";
import { View } from "react-native";

import { AppText as Text } from "../../components/ui/AppText";
import * as Dialog from "../../components/ui/Dialog";
import { ListActionRow } from "../../components/ui/ListActionRow";
import { ListSection } from "../../components/ui/ListSection";
import { ScreenScrollView as ScrollView } from "../../components/ui/ScreenScrollView";
import { connectionAtomRuntime } from "../../connection/runtime";
import { useNowMinuteMs } from "../../lib/clock";
import { useProjects, useServerConfigs, useVisibleThreadShells } from "../../state/entities";
import { serverEnvironment } from "../../state/server";
import { mobilePreferencesAtom, saveMobilePreferences } from "../../state/preferences";
import { useAtomCommand } from "../../state/use-atom-command";
import { savedConnectionsByIdAtom } from "../../state/use-remote-environment-registry";
import { useAdaptiveWorkspaceLayout } from "../layout/adaptive-workspace-context";
import { useSelectWorkspaceDetail } from "../layout/workspace/useSelectWorkspaceDetail";
import { useWorkspaceTabs } from "../layout/workspace/WorkspaceTabsContext";
import { NativeWorkspaceTabActivityContext } from "../../native/v5-workspace-context";
import { PromachosConversationRow } from "./PromachosConversationRow";
import { usePromachosHome } from "./promachosHome";

const createHome = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "promachos:create-home",
  tag: PROMACHOS_HOME_WS_METHODS.create,
});

/** The Promachos tab: his home's conversations; the home itself is set in Settings. */
export function PromachosTabScreen() {
  const gt = useGT();
  const navigation = useNavigation();
  const { start, refs } = usePromachosHome();
  const bottomClearance = useWorkspaceTabs()?.bottomClearance ?? 0;
  return (
    <ScrollView
      contentInsetAdjustmentBehavior="automatic"
      contentContainerStyle={{ paddingTop: 4, paddingBottom: bottomClearance + 16 }}
    >
      {start !== null ? (
        <PromachosConversationList homeRefs={refs} />
      ) : (
        <View className="items-center gap-4 px-8 pt-24">
          <Text className="text-center text-foreground-secondary">
            {gt("Choose the project that is the Promachos home to see his conversations.")}
          </Text>
          {/* The button starts its own row; this box centres it. */}
          <View>
            <Button
              label={gt("Choose home")}
              onPress={() => openSettings(navigation, "SettingsPromachos")}
            />
          </View>
        </View>
      )}
    </ScrollView>
  );
}

function PromachosConversationList(props: { readonly homeRefs: ReadonlyArray<ScopedProjectRef> }) {
  const gt = useGT();
  const conversations = promachosConversations(useVisibleThreadShells(), props.homeRefs);
  const selectDetail = useSelectWorkspaceDetail();
  const nowMs = useNowMinuteMs();
  useForgetConversationOnList();
  return conversations.length === 0 ? (
    <Text className="px-4 py-3 text-foreground-secondary">{gt("No conversations yet.")}</Text>
  ) : (
    conversations.map((thread) => (
      <PromachosConversationRow
        key={`${thread.environmentId}:${thread.id}`}
        thread={thread}
        nowMs={nowMs}
        onPress={() =>
          selectDetail({
            name: "Thread",
            params: { environmentId: thread.environmentId, threadId: thread.id },
          })
        }
      />
    ))
  );
}

/** Where the list itself is on screen, a cold start reopens the list, not a conversation. */
function useForgetConversationOnList() {
  const focused = useIsFocused() && use(NativeWorkspaceTabActivityContext)?.active !== false;
  const { layout } = useAdaptiveWorkspaceLayout();
  const preferences = useAtomValue(mobilePreferencesAtom);
  const save = useAtomCommand(saveMobilePreferences);
  const remembered =
    AsyncResult.isSuccess(preferences) && preferences.value.promachosConversation !== undefined;
  const forget = focused && !layout.usesSplitView && remembered;
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- persists that the tab shows its list, so a cold start reopens the list
  useEffect(() => {
    if (forget) void save({ promachosConversation: undefined });
  }, [forget, save]);
}

/**
 * Settings → Promachos: each connected machine's projects, its home checked. The home is that
 * machine's server setting, so every client connected to it follows; tapping the home clears it.
 */
export function PromachosHomePicker() {
  const gt = useGT();
  const configs = useServerConfigs();
  const savedConnections = useAtomValue(savedConnectionsByIdAtom);
  const environmentIds = [...configs.keys()];
  if (environmentIds.length === 0) {
    return (
      <ListSection title={gt("Promachos home")}>
        <Text className="px-4 py-3 text-foreground-secondary">
          {gt("Connect an environment to choose the Promachos home.")}
        </Text>
      </ListSection>
    );
  }
  return environmentIds.map((environmentId) => (
    <PromachosEnvironmentHome
      key={environmentId}
      environmentId={environmentId}
      home={configs.get(environmentId)?.settings.promachosHome ?? null}
      title={
        environmentIds.length > 1
          ? (savedConnections[environmentId]?.environmentLabel ?? environmentId)
          : gt("Promachos home")
      }
    />
  ));
}

function PromachosEnvironmentHome(props: {
  readonly environmentId: EnvironmentId;
  readonly home: ProjectId | null;
  readonly title: string;
}) {
  const gt = useGT();
  const projects = useProjects().filter((project) => project.environmentId === props.environmentId);
  const canWrite = useAtomValue(
    serverEnvironment.updateSettings.permissionAtom(props.environmentId),
  );
  const update = useAtomCommand(serverEnvironment.updateSettings, "Promachos home update");
  const save = (promachosHome: ProjectId | null) =>
    void update({ environmentId: props.environmentId, input: { patch: { promachosHome } } });
  const missing = props.home !== null && !projects.some((project) => project.id === props.home);
  return (
    <ListSection
      title={props.title}
      footer={
        missing
          ? gt("The Promachos home is not available here. Choose it again.")
          : gt(
              "Choose the project that is the Promachos home on this machine, or create a new one. His conversations live there.",
            )
      }
    >
      {projects.map((project) => (
        <ListActionRow
          key={project.id}
          icon={project.id === props.home ? "checkmark" : "house"}
          label={project.title}
          disabled={!canWrite}
          onPress={() => save(project.id === props.home ? null : project.id)}
        />
      ))}
      <PromachosNewHomeRow
        environmentId={props.environmentId}
        label={gt("New home")}
        onCreated={(created) => save(created.projectId)}
      />
    </ListSection>
  );
}

/** Creates a starter home folder on that environment's machine (toolboxmd/chromeria#139). */
function PromachosNewHomeRow(props: {
  readonly environmentId: EnvironmentId;
  readonly label: string;
  readonly onCreated: (home: ScopedProjectRef) => void;
}) {
  const gt = useGT();
  const create = useAtomCommand(createHome, { reportFailure: false });
  const canCreate = useAtomValue(createHome.permissionAtom(props.environmentId));
  const submit = async (path: string) => {
    if (path.trim().length === 0) return;
    const result = await create({
      environmentId: props.environmentId,
      input: { path: path.trim() },
    });
    if (result._tag === "Success") {
      props.onCreated({ environmentId: props.environmentId, projectId: result.value.projectId });
      return;
    }
    if (isAtomCommandInterrupted(result)) return;
    const failure = squashAtomCommandFailure(result);
    Dialog.alert(
      gt("The home could not be created."),
      failure instanceof Error ? failure.message : undefined,
    );
  };
  return (
    <View>
      <ListActionRow
        icon="plus"
        label={props.label}
        disabled={!canCreate}
        onPress={() =>
          Dialog.prompt({
            title: props.label,
            message: gt("Folder for the new home"),
            initialValue: DEFAULT_PROMACHOS_HOME_PATH,
            confirmText: gt("Create", { $context: "button, creates a new home" }),
            onConfirm: (path) => void submit(path),
          })
        }
      />
    </View>
  );
}

/**
 * Android: the Promachos tab with the Chats tab's floating new-conversation button. It floats
 * in the sidebar too, since the split-view rail's compose starts an ordinary chat.
 */
export function AndroidPromachosTab() {
  const compose = usePromachosCompose();
  return compose === null ? (
    <PromachosTabScreen />
  ) : (
    <AndroidHomeFabLayout onStartNewTask={compose.onPress}>
      <PromachosTabScreen />
    </AndroidHomeFabLayout>
  );
}
