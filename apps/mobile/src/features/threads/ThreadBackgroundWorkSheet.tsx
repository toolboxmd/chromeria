import { useAtomValue } from "@effect/atom-react";
import { StackActions, useNavigation, type StaticScreenProps } from "@react-navigation/native";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import {
  presentPendingBackgroundWork,
  type PendingBackgroundWorkItem,
} from "@t3tools/client-runtime/state/thread-execution";
import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import * as Haptics from "expo-haptics";
import { msg, T, useGT, useMessages } from "gt-react-native";
import { Pressable, ScrollView, View } from "react-native";
import { useSafeAreaInsets } from "react-native-safe-area-context";

import { SheetHeader } from "../../components/ui/SheetHeader";
import { SymbolView } from "../../components/ui/AppSymbol";
import { AppText as Text } from "../../components/ui/AppText";
import { ScreenContent } from "../../components/ui/ScreenContent";
import { useEnvironments } from "../../state/environments";
import { useLinkedEnvironments } from "../../state/peerLinks";
import { environmentThreadShells } from "../../state/threads";
import { backgroundWorkThreadTarget } from "./backgroundWorkTarget";
import type { SubagentThreadTarget } from "./threadAgentsPresentation";
import { useOpenLinkedThreadRead } from "./useOpenLinkedThread";

const KIND_LABELS = {
  subagent: msg("Subagent"),
  command: msg("Command", { $context: "noun, a running shell command" }),
  monitor: msg("Monitor", { $context: "noun, a background watcher task" }),
  background_task: msg("Background task"),
} as const;

type BackgroundWorkTarget = { readonly environmentId: EnvironmentId; readonly threadId: ThreadId };

export function ThreadBackgroundWorkSheet({ route }: StaticScreenProps<BackgroundWorkTarget>) {
  const navigation = useNavigation();
  const insets = useSafeAreaInsets();
  const gt = useGT();
  const m = useMessages();
  const target = route.params;
  const thread = useAtomValue(
    environmentThreadShells.threadShellAtom(scopeThreadRef(target.environmentId, target.threadId)),
  );
  const presentation = presentPendingBackgroundWork(thread?.pendingBackgroundTasks ?? [], m);
  const openLinkedRead = useOpenLinkedThreadRead();
  const { presentationById } = useEnvironments();
  const ownConnection = (environmentId: EnvironmentId) => {
    const own = presentationById.get(environmentId);
    return own && { connected: own.connection.phase === "connected" };
  };
  const links = useLinkedEnvironments(
    presentation?.items.some(
      (item) =>
        item.childEnvironmentId !== undefined &&
        ownConnection(item.childEnvironmentId)?.connected !== true,
    )
      ? target.environmentId
      : null,
  );
  const openThread = (child: SubagentThreadTarget, title: string) => {
    void Haptics.selectionAsync();
    if (child.linked !== undefined) {
      openLinkedRead(child, child.linked, title, { source: target, replace: true });
      return;
    }
    // A thread replaces the leaf sheet so workspace back navigation still works.
    navigation.dispatch(
      StackActions.replace("Thread", {
        environmentId: child.environmentId,
        threadId: child.threadId,
      }),
    );
  };
  const content = (
    <ScrollView
      nestedScrollEnabled
      className="flex-1"
      contentInsetAdjustmentBehavior="automatic"
      contentContainerClassName="px-5 pb-6"
      contentContainerStyle={{ paddingBottom: Math.max(insets.bottom, 16) + 8 }}
    >
      {presentation === null ? (
        <Text className="pt-6 text-center text-sm text-foreground-muted">
          <T>Nothing running in the background.</T>
        </Text>
      ) : (
        presentation.items.map((item) => (
          <BackgroundWorkRow
            key={item.taskId}
            item={item}
            target={backgroundWorkThreadTarget(item, target.environmentId, ownConnection, (id) =>
              links.get(id),
            )}
            onOpen={openThread}
          />
        ))
      )}
    </ScrollView>
  );
  return (
    <View collapsable={false} className="flex-1 bg-sheet">
      <SheetHeader title={gt("Background work")} onClose={() => navigation.goBack()} />
      <ScreenContent>{content}</ScreenContent>
    </View>
  );
}

function BackgroundWorkRow(props: {
  readonly item: PendingBackgroundWorkItem;
  readonly target: SubagentThreadTarget | null;
  readonly onOpen: (child: SubagentThreadTarget, title: string) => void;
}) {
  const { item, target } = props;
  const gt = useGT();
  const m = useMessages();
  const row = (
    <View className="flex-row items-center gap-3 border-b border-border py-3.5">
      <SymbolView
        name={
          item.kind === "subagent" ? "person.2" : item.kind === "monitor" ? "clock" : "terminal"
        }
        size={15}
        tintColorClassName="accent-foreground-muted"
        type="monochrome"
      />
      <View className="min-w-0 flex-1">
        <Text className="font-t3-medium text-sm text-foreground">{item.label}</Text>
        <Text className="text-xs text-foreground-muted">{m(KIND_LABELS[item.kind])}</Text>
      </View>
      {target === null ? null : (
        <SymbolView name="chevron.right" size={12} tintColorClassName="accent-icon-subtle" />
      )}
    </View>
  );
  return target === null ? (
    <View accessible>{row}</View>
  ) : (
    <Pressable
      accessibilityRole="link"
      accessibilityLabel={gt("Open subagent {label}", { label: item.label })}
      onPress={() => props.onOpen(target, item.label)}
      className="active:opacity-70"
    >
      {row}
    </Pressable>
  );
}
