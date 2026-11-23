import { useNavigation } from "@react-navigation/native";
import { Button } from "../../components/ui/Button";
import { openSettings } from "../settings/settings-navigation";
import { useAtomValue } from "@effect/atom-react";
import { useIsFocused } from "@react-navigation/native";
import { promachosConversations } from "@t3tools/client-runtime/promachos/conversations";
import type { EnvironmentProject } from "@t3tools/client-runtime/state/models";
import {
  createEnvironmentRpcCommand,
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import {
  DEFAULT_PROMACHOS_HOME_PATH,
  PROMACHOS_HOME_WS_METHODS,
  type EnvironmentId,
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
import { useProjects, useVisibleThreadShells } from "../../state/entities";
import { mobilePreferencesAtom, saveMobilePreferences } from "../../state/preferences";
import { useAtomCommand } from "../../state/use-atom-command";
import { savedConnectionsByIdAtom } from "../../state/use-remote-environment-registry";
import { useAdaptiveWorkspaceLayout } from "../layout/adaptive-workspace-context";
import { useSelectWorkspaceDetail } from "../layout/workspace/useSelectWorkspaceDetail";
import { useWorkspaceTabs } from "../layout/workspace/WorkspaceTabsContext";
import { NativeWorkspaceTabActivityContext } from "../../native/v5-workspace-context";
import { PromachosConversationRow } from "./PromachosConversationRow";
import { usePromachosHome, usePromachosHomeRefs } from "./promachosHome";

const createHome = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "promachos:create-home",
  tag: PROMACHOS_HOME_WS_METHODS.create,
});

/** The Promachos tab: his home's conversations; the home itself is set in Settings. */
export function PromachosTabScreen() {
  const gt = useGT();
  const navigation = useNavigation();
  const { home } = usePromachosHome();
  const homeProject = usePromachosHomeProject(home);
  const bottomClearance = useWorkspaceTabs()?.bottomClearance ?? 0;
  return (
    <ScrollView
      contentInsetAdjustmentBehavior="automatic"
      contentContainerStyle={{ paddingTop: 4, paddingBottom: bottomClearance + 16 }}
    >
      {homeProject !== null && home !== null ? (
        <PromachosConversationList home={home} />
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

function usePromachosHomeProject(home: ScopedProjectRef | null) {
  const projects = useProjects();
  return home === null
    ? null
    : (projects.find(
        (project) => project.environmentId === home.environmentId && project.id === home.projectId,
      ) ?? null);
}

function PromachosConversationList(props: { readonly home: ScopedProjectRef }) {
  const gt = useGT();
  const homeRefs = usePromachosHomeRefs(props.home);
  const conversations = promachosConversations(useVisibleThreadShells(), homeRefs);
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

/** Settings → Promachos: the projects to choose the home from, the current one checked. */
export function PromachosHomePicker() {
  const gt = useGT();
  const { home, setHome } = usePromachosHome();
  const homeProject = usePromachosHomeProject(home);
  const projects = useProjects();
  const savedConnections = useAtomValue(savedConnectionsByIdAtom);
  const environmentIds = [...new Set(projects.map((project) => project.environmentId))];
  const multipleEnvironments = environmentIds.length > 1;
  const label = (project: EnvironmentProject) => {
    const environment = savedConnections[project.environmentId]?.environmentLabel;
    return multipleEnvironments && environment
      ? `${project.title} · ${environment}`
      : project.title;
  };
  return (
    <ListSection
      title={gt("Promachos home")}
      footer={
        home !== null && homeProject === null
          ? gt("The Promachos home is not available here. Choose it again.")
          : gt(
              "Choose the project that is the Promachos home, or create a new one. His conversations live there.",
            )
      }
    >
      {projects.map((project) => (
        <ListActionRow
          key={`${project.environmentId}:${project.id}`}
          icon={project === homeProject ? "checkmark" : "house"}
          label={label(project)}
          onPress={() => setHome({ environmentId: project.environmentId, projectId: project.id })}
        />
      ))}
      {environmentIds.map((environmentId) => (
        <PromachosNewHomeRow
          key={environmentId}
          environmentId={environmentId}
          label={
            multipleEnvironments
              ? gt("New home · {environment}", {
                  environment: savedConnections[environmentId]?.environmentLabel ?? environmentId,
                })
              : gt("New home")
          }
          onCreated={setHome}
        />
      ))}
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
