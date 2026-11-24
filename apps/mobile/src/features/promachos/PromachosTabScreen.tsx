import { useNavigation } from "@react-navigation/native";
import { AndroidHomeFabLayout } from "../home/AndroidHomeFab";
import { usePromachosCompose } from "./usePromachosCompose";
import { Button } from "../../components/ui/Button";
import { openSettings } from "../settings/settings-navigation";
import { useAtomValue } from "@effect/atom-react";
import { useIsFocused } from "@react-navigation/native";
import {
  promachosConversationGroups,
  promachosConversations,
  promachosOlderSettledScopes,
} from "@t3tools/client-runtime/promachos/conversations";
import {
  createEnvironmentRpcCommand,
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import {
  AuthSettingsWriteScope,
  DEFAULT_PROMACHOS_HOME_PATH,
  PROMACHOS_HOME_WS_METHODS,
  type EnvironmentId,
  type ProjectId,
  type ScopedProjectRef,
} from "@t3tools/contracts";
import { useGT } from "gt-react-native";
import { AsyncResult } from "effect/reactivity";
import { use, useEffect, useMemo, useState } from "react";
import { View } from "react-native";

import { AppText as Text } from "../../components/ui/AppText";
import * as Dialog from "../../components/ui/Dialog";
import { ListActionRow } from "../../components/ui/ListActionRow";
import { ListSection } from "../../components/ui/ListSection";
import { ScreenScrollView as ScrollView } from "../../components/ui/ScreenScrollView";
import { connectionAtomRuntime } from "../../connection/runtime";
import { useNowMinuteMs } from "../../lib/clock";
import {
  useProjects,
  useServerConfigs,
  useThreadShells,
  useVisibleThreadShells,
} from "../../state/entities";
import { serverEnvironment } from "../../state/server";
import { readEnvironmentScope, useEnvironmentScope } from "../../state/session";
import { mobilePreferencesAtom, saveMobilePreferences } from "../../state/preferences";
import { useOlderSettledShelf, useOlderSettledShelfGrowth } from "../../state/olderSettledShelf";
import { threadListEnvironmentsAtom } from "../../state/presentation";
import { useAtomCommand } from "../../state/use-atom-command";
import { savedConnectionsByIdAtom } from "../../state/use-remote-environment-registry";
import { useAdaptiveWorkspaceLayout } from "../layout/adaptive-workspace-context";
import { useThreadListActions } from "../home/useThreadListActions";
import {
  ThreadListV2SettledShelfHeader,
  ThreadListV2ShowMoreRow,
  ThreadListV2SnoozedShelfHeader,
} from "../threads/thread-list-v2-items";
import { THREAD_LIST_V2_SETTLED_PAGE_COUNT } from "../threads/threadListV2";
import { useThreadListNow } from "../threads/use-thread-list-now";
import { useSelectWorkspaceDetail } from "../layout/workspace/useSelectWorkspaceDetail";
import { useWorkspaceTabs } from "../layout/workspace/WorkspaceTabsContext";
import { NativeWorkspaceTabActivityContext } from "../../native/v5-workspace-context";
import { PromachosConversationRow } from "./PromachosConversationRow";
import { PromachosConversationSwipe } from "./PromachosConversationSwipe";
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
  const allThreads = useThreadShells();
  const conversations = promachosConversations(useVisibleThreadShells(), props.homeRefs);
  const selectDetail = useSelectWorkspaceDetail();
  const nowMs = useNowMinuteMs();
  // Advances at each snooze wake as well as each minute, so a woken conversation returns on time.
  const now = useThreadListNow(conversations);
  const { settlementEnvironmentIds, snoozePresetsByEnvironmentId } = useAtomValue(
    threadListEnvironmentsAtom,
  );
  const groups = promachosConversationGroups(conversations, {
    now,
    supports: (environmentId) => settlementEnvironmentIds.has(environmentId),
  });
  const actions = useThreadListActions();
  const [expanded, setExpanded] = useState({ snoozed: false, settled: false });
  // Shells leave older settled threads out; the Settled group pages them in, as the thread list does.
  const [settledVisibleCount, setSettledVisibleCount] = useState(THREAD_LIST_V2_SETTLED_PAGE_COUNT);
  const scopes = useMemo(() => promachosOlderSettledScopes(props.homeRefs), [props.homeRefs]);
  const olderSettled = useOlderSettledShelf({
    scopes,
    expanded: expanded.settled,
    visibleCount: settledVisibleCount,
  });
  const listedSettled = groups.settled.filter(olderSettled.isListed);
  useOlderSettledShelfGrowth(olderSettled, listedSettled);
  // Counted against every loaded shell: a row the tag filter hides is still loaded.
  const settledCount = groups.settled.length + olderSettled.unloadedCount(allThreads);
  const shownSettled = listedSettled.slice(0, settledVisibleCount);
  useForgetConversationOnList();
  const rows = (
    group: "active" | "snoozed" | "settled",
    list: ReadonlyArray<(typeof conversations)[number]>,
  ) =>
    list.map((thread) => (
      <PromachosConversationSwipe
        key={`${thread.environmentId}:${thread.id}`}
        thread={thread}
        group={group}
        supported={settlementEnvironmentIds.has(thread.environmentId)}
        snoozePresets={snoozePresetsByEnvironmentId.get(thread.environmentId)}
        nowMs={nowMs}
        actions={actions}
      >
        <PromachosConversationRow
          thread={thread}
          nowMs={nowMs}
          onPress={() =>
            selectDetail({
              name: "Thread",
              params: { environmentId: thread.environmentId, threadId: thread.id },
            })
          }
        />
      </PromachosConversationSwipe>
    ));
  return (
    <>
      {conversations.length === 0 && settledCount === 0 ? (
        <Text className="px-4 py-3 text-foreground-secondary">{gt("No conversations yet.")}</Text>
      ) : (
        rows("active", groups.active)
      )}
      {groups.snoozed.length === 0 ? null : (
        <View>
          <ThreadListV2SnoozedShelfHeader
            count={groups.snoozed.length}
            expanded={expanded.snoozed}
            onToggle={() => setExpanded({ ...expanded, snoozed: !expanded.snoozed })}
          />
          {expanded.snoozed ? rows("snoozed", groups.snoozed) : null}
        </View>
      )}
      {settledCount === 0 ? null : (
        <View>
          <ThreadListV2SettledShelfHeader
            count={settledCount}
            expanded={expanded.settled}
            onToggle={() => setExpanded({ ...expanded, settled: !expanded.settled })}
          />
          {expanded.settled ? rows("settled", shownSettled) : null}
          {expanded.settled && settledCount > shownSettled.length ? (
            <ThreadListV2ShowMoreRow
              hiddenCount={settledCount - shownSettled.length}
              {...(olderSettled.loading
                ? { state: "loading" as const }
                : olderSettled.failed
                  ? { state: "failed" as const }
                  : {})}
              onPress={() => {
                olderSettled.retry();
                setSettledVisibleCount(settledVisibleCount + THREAD_LIST_V2_SETTLED_PAGE_COUNT);
              }}
            />
          ) : null}
        </View>
      )}
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

/**
 * Settings → Promachos: each connected machine's projects, its home checked. The home is that
 * machine's server setting, so every client connected to it follows; "Not set" clears it.
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
  // The settings command reports no required scope, so its permission atom would always allow it.
  const canWrite = useEnvironmentScope(props.environmentId, AuthSettingsWriteScope);
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
      <ListActionRow
        icon={props.home === null ? "checkmark" : "xmark"}
        label={gt("Not set")}
        disabled={!canWrite}
        onPress={() => save(null)}
      />
      {projects.map((project) => (
        <ListActionRow
          key={project.id}
          icon={project.id === props.home ? "checkmark" : "house"}
          label={project.title}
          disabled={!canWrite}
          onPress={() => save(project.id)}
        />
      ))}
      <PromachosNewHomeRow
        environmentId={props.environmentId}
        label={gt("New home")}
        disabled={!canWrite}
        onCreated={(created) => save(created.projectId)}
      />
    </ListSection>
  );
}

/** Creates a starter home folder on that environment's machine (toolboxmd/chromeria#139). */
function PromachosNewHomeRow(props: {
  readonly environmentId: EnvironmentId;
  readonly label: string;
  /** Set when the home could be created but not saved as this machine's home. */
  readonly disabled: boolean;
  readonly onCreated: (home: ScopedProjectRef) => void;
}) {
  const gt = useGT();
  const create = useAtomCommand(createHome, { reportFailure: false });
  const canCreate = useAtomValue(createHome.permissionAtom(props.environmentId)) && !props.disabled;
  const submit = async (path: string) => {
    // The grant can change while the prompt is open; a home that cannot be saved is not created.
    if (
      path.trim().length === 0 ||
      !readEnvironmentScope(props.environmentId, AuthSettingsWriteScope)
    )
      return;
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
