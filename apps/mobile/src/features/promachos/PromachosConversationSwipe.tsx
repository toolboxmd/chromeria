import type { MenuAction } from "@react-native-menu/menu";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import { canSnooze } from "@t3tools/client-runtime/state/thread-settled";
import { AuthOrchestrationOperateScope } from "@t3tools/contracts";
import type { SnoozePresetSetting } from "@t3tools/contracts/settings";
import { useGT, useMessages } from "gt-react-native";
import { useState, type ReactNode } from "react";
import { useWindowDimensions, View } from "react-native";

import * as Dialog from "../../components/ui/Dialog";
import { useUniwindTheme } from "../../lib/useUniwindTheme";
import { useEnvironmentScope } from "../../state/session";
import { ThreadSwipeable } from "../home/thread-swipe-actions";
import type { useThreadListActions } from "../home/useThreadListActions";
import { useAdaptiveWorkspaceLayout } from "../layout/adaptive-workspace-context";
import { CustomSnoozeSheet } from "../threads/CustomSnoozeSheet";
import {
  resolveThreadListV2SnoozeMenuSelection,
  resolveThreadListV2SnoozePresets,
  resolveThreadListV2SwipeActions,
} from "../threads/threadListV2";

/**
 * The home thread list's swipe actions on a Promachos conversation: settle and snooze in the
 * list, wake or un-settle in the Snoozed and Settled groups.
 */
export function PromachosConversationSwipe(props: {
  readonly thread: EnvironmentThreadShell;
  readonly group: "active" | "snoozed" | "settled";
  /** False until the thread's server has answered; the row then has no swipe actions. */
  readonly supported: boolean;
  readonly snoozePresets: ReadonlyArray<SnoozePresetSetting> | undefined;
  readonly nowMs: number;
  readonly actions: ReturnType<typeof useThreadListActions>;
  readonly children: ReactNode;
}) {
  const { thread, actions } = props;
  const gt = useGT();
  const m = useMessages();
  const theme = useUniwindTheme();
  const { width } = useWindowDimensions();
  const { layout } = useAdaptiveWorkspaceLayout();
  const canOperate = useEnvironmentScope(thread.environmentId, AuthOrchestrationOperateScope);
  const [customSnoozeOpen, setCustomSnoozeOpen] = useState(false);
  if (!canOperate || !props.supported) return props.children;

  const now = new Date(props.nowMs).toISOString();
  const swipe = resolveThreadListV2SwipeActions({
    variant: props.group === "settled" ? "slim" : "card",
    settlementSupported: true,
    snoozeSupported: true,
    snoozable: canSnooze(thread, { now }),
    snoozed: props.group === "snoozed",
  });
  const title = thread.title;
  const primaryAction =
    swipe.primary === "unsnooze"
      ? {
          accessibilityLabel: gt("Wake {title} now", { title }),
          icon: "clock" as const,
          label: gt("Wake", { $context: "verb, thread action" }),
          onPress: () => void actions.unsnoozeThread(thread),
        }
      : swipe.primary === "unsettle"
        ? {
            accessibilityLabel: gt("Un-settle {title}", { title }),
            icon: "arrow.uturn.backward" as const,
            label: gt("Un-settle", { $context: "verb, thread action" }),
            onPress: () => void actions.unsettleThread(thread),
          }
        : {
            accessibilityLabel: gt("Settle {title}", { title }),
            icon: "checkmark" as const,
            label: gt("Settle", { $context: "verb, thread action" }),
            onPress: () => void actions.settleThread(thread),
          };
  const presets = resolveThreadListV2SnoozePresets(
    { secondary: swipe.secondary, now, presets: props.snoozePresets },
    m,
  );
  const snoozeMenu: MenuAction[] = [
    ...presets.map((preset) => ({
      id: `snooze:${preset.id}`,
      title: preset.label,
      subtitle: preset.whenLabel,
    })),
    { id: "snooze:custom", title: gt("Custom…", { $context: "custom snooze time" }) },
  ];
  const secondaryAction =
    swipe.secondary === "snooze"
      ? {
          accessibilityLabel: gt("Choose when to snooze {title}", { title }),
          icon: "clock" as const,
          label: gt("Snooze", { $context: "verb, thread action" }),
          menu: {
            actions: snoozeMenu,
            title: gt("Snooze until"),
            onPressAction: ({ nativeEvent }: { nativeEvent: { event: string } }) => {
              if (nativeEvent.event === "snooze:custom") {
                setCustomSnoozeOpen(true);
                return;
              }
              const selection = resolveThreadListV2SnoozeMenuSelection({
                event: nativeEvent.event,
                displayedPresets: presets,
                now: new Date(),
                presets: props.snoozePresets,
              });
              if (selection._tag === "selected") {
                void actions.snoozeThread(thread, selection.preset.snoozedUntil);
              } else if (selection._tag === "expired") {
                Dialog.alert(
                  gt("Could not snooze thread"),
                  gt("That snooze time has passed. Choose another time."),
                );
              }
            },
          },
          onPress: () => undefined,
        }
      : null;
  return (
    <View collapsable={false}>
      {customSnoozeOpen ? (
        <CustomSnoozeSheet
          onClose={() => setCustomSnoozeOpen(false)}
          onSnooze={(snoozedUntil) => void actions.snoozeThread(thread, snoozedUntil)}
        />
      ) : null}
      <ThreadSwipeable
        threadKey={`${thread.environmentId}:${thread.id}`}
        resetKey={`${thread.environmentId}:${thread.id}:${props.group}`}
        backgroundColor={theme[layout.usesSplitView ? "--color-drawer" : "--color-screen"]}
        enableTrackpadSwipe
        fullSwipeAction="primary"
        fullSwipeWidth={width - 32}
        onDelete={() => actions.confirmDeleteThread(thread)}
        primaryAction={primaryAction}
        secondaryAction={secondaryAction}
        threadTitle={title}
      >
        {() => props.children}
      </ThreadSwipeable>
    </View>
  );
}
