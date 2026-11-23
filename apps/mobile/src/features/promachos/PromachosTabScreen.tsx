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
import { useGT, useMessages } from "gt-react-native";
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
import { relativeTime } from "../../lib/time";
import { uuidv4 } from "../../lib/uuid";
import { useProjects, useVisibleThreadShells } from "../../state/entities";
import { mobilePreferencesAtom, saveMobilePreferences } from "../../state/preferences";
import { useAtomCommand } from "../../state/use-atom-command";
import { savedConnectionsByIdAtom } from "../../state/use-remote-environment-registry";
import { useAdaptiveWorkspaceLayout } from "../layout/adaptive-workspace-context";
import { useSelectWorkspaceDetail } from "../layout/workspace/useSelectWorkspaceDetail";
import { useWorkspaceTabs } from "../layout/workspace/WorkspaceTabsContext";
import { NativeWorkspaceTabActivityContext } from "../../native/v5-workspace-context";
import { usePromachosHome, usePromachosHomeRefs } from "./promachosHome";

const createHome = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "promachos:create-home",
  tag: PROMACHOS_HOME_WS_METHODS.create,
});

/** The Promachos tab: his home's conversations, or the picker for his home. */
export function PromachosTabScreen() {
  const [home, setHome] = usePromachosHome();
  const projects = useProjects();
  const savedConnections = useAtomValue(savedConnectionsByIdAtom);
  const multipleEnvironments = new Set(projects.map((project) => project.environmentId)).size > 1;
  const label = (project: EnvironmentProject) => {
    const environment = savedConnections[project.environmentId]?.environmentLabel;
    return multipleEnvironments && environment
      ? `${project.title} · ${environment}`
      : project.title;
  };
  const homeProject =
    home === null
      ? null
      : (projects.find(
          (project) =>
            project.environmentId === home.environmentId && project.id === home.projectId,
        ) ?? null);
  const bottomClearance = useWorkspaceTabs()?.bottomClearance ?? 0;
  return (
    <ScrollView
      contentInsetAdjustmentBehavior="automatic"
      contentContainerStyle={{ padding: 16, paddingBottom: bottomClearance + 16, gap: 24 }}
    >
      {homeProject !== null && home !== null ? (
        <PromachosConversationList
          home={home}
          homeLabel={label(homeProject)}
          onChangeHome={() => setHome(null)}
        />
      ) : (
        <PromachosHomePicker
          missing={home !== null}
          projects={projects}
          label={label}
          onPick={setHome}
        />
      )}
    </ScrollView>
  );
}

function PromachosConversationList(props: {
  readonly home: ScopedProjectRef;
  readonly homeLabel: string;
  readonly onChangeHome: () => void;
}) {
  const gt = useGT();
  const m = useMessages();
  const homeRefs = usePromachosHomeRefs(props.home);
  const conversations = promachosConversations(useVisibleThreadShells(), homeRefs);
  const selectDetail = useSelectWorkspaceDetail();
  const nowMs = useNowMinuteMs();
  useForgetConversationOnList();
  return (
    <>
      <ListSection title={gt("Promachos home")}>
        <ListActionRow
          icon="house"
          label={props.homeLabel}
          description={gt("Choose another home")}
          onPress={props.onChangeHome}
        />
        <ListActionRow
          icon="square.and.pencil"
          label={gt("New conversation")}
          onPress={() =>
            selectDetail({
              name: "Thread",
              params: {
                draft: {
                  requestId: uuidv4(),
                  environmentId: String(props.home.environmentId),
                  projectId: String(props.home.projectId),
                },
              },
            })
          }
        />
      </ListSection>
      <ListSection title={gt("Conversations")}>
        {conversations.length === 0 ? (
          <Text className="px-4 py-3 text-foreground-muted">{gt("No conversations yet.")}</Text>
        ) : (
          conversations.map((thread) => (
            <ListActionRow
              key={`${thread.environmentId}:${thread.id}`}
              icon="text.bubble"
              label={thread.title}
              detail={relativeTime(thread.latestUserMessageAt ?? thread.updatedAt, nowMs, m)}
              onPress={() =>
                selectDetail({
                  name: "Thread",
                  params: { environmentId: thread.environmentId, threadId: thread.id },
                })
              }
            />
          ))
        )}
      </ListSection>
    </>
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

function PromachosHomePicker(props: {
  readonly missing: boolean;
  readonly projects: ReadonlyArray<EnvironmentProject>;
  readonly label: (project: EnvironmentProject) => string;
  readonly onPick: (home: ScopedProjectRef) => void;
}) {
  const gt = useGT();
  const savedConnections = useAtomValue(savedConnectionsByIdAtom);
  const environmentIds = [...new Set(props.projects.map((project) => project.environmentId))];
  return (
    <ListSection
      title={gt("Promachos home")}
      footer={
        props.missing
          ? gt("The Promachos home is not available here. Choose it again.")
          : gt(
              "Choose the project that is the Promachos home, or create a new one. His conversations live there.",
            )
      }
    >
      {props.projects.map((project) => (
        <ListActionRow
          key={`${project.environmentId}:${project.id}`}
          icon="house"
          label={props.label(project)}
          onPress={() =>
            props.onPick({ environmentId: project.environmentId, projectId: project.id })
          }
        />
      ))}
      {environmentIds.map((environmentId) => (
        <PromachosNewHomeRow
          key={environmentId}
          environmentId={environmentId}
          label={
            environmentIds.length > 1
              ? gt("New home · {environment}", {
                  environment: savedConnections[environmentId]?.environmentLabel ?? environmentId,
                })
              : gt("New home")
          }
          onCreated={props.onPick}
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
