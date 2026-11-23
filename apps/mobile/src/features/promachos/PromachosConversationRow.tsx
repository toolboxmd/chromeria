import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import { msg, useLocale, useMessages } from "gt-react-native";
import { Pressable, View } from "react-native";

import { AppText as Text } from "../../components/ui/AppText";
import {
  resolveThreadListV2RowStatus,
  threadHasUnseenCompletion,
  type ThreadListV2Status,
} from "../threads/threadListV2";

// Thread shells carry no message text, so the preview line says where the conversation stands.
const PREVIEW: Record<ThreadListV2Status, string> = {
  approval: msg("Needs your approval", { $context: "Promachos conversation preview" }),
  input: msg("Waiting for your answer", { $context: "Promachos conversation preview" }),
  request: msg("A request is waiting", { $context: "Promachos conversation preview" }),
  working: msg("Working…", { $context: "Promachos conversation preview" }),
  waiting: msg("Waiting", { $context: "Promachos conversation preview" }),
  failed: msg("Something went wrong", { $context: "Promachos conversation preview" }),
  limited: msg("Reached a usage limit", { $context: "Promachos conversation preview" }),
  ready: msg("Replied", { $context: "Promachos conversation preview" }),
};

/** Today's time of day, or the date for an older conversation. */
function rowTime(input: string, nowMs: number, locale: string) {
  const at = new Date(input);
  if (Number.isNaN(at.getTime())) return "";
  return at.toDateString() === new Date(nowMs).toDateString()
    ? at.toLocaleTimeString(locale, { hour: "numeric", minute: "2-digit" })
    : at.toLocaleDateString(locale, { month: "short", day: "numeric" });
}

/** One conversation in the Promachos list: title, time, where it stands, unread dot. */
export function PromachosConversationRow(props: {
  readonly thread: EnvironmentThreadShell;
  readonly nowMs: number;
  readonly onPress: () => void;
}) {
  const m = useMessages();
  const locale = useLocale();
  const status = resolveThreadListV2RowStatus(props.thread);
  const unread =
    status !== "ready" ? status !== "working" : threadHasUnseenCompletion(props.thread);
  return (
    <Pressable
      accessibilityRole="button"
      accessibilityLabel={props.thread.title}
      className="h-20 flex-row items-center px-4 active:opacity-60"
      onPress={props.onPress}
    >
      <View className="min-w-0 flex-1 gap-0.5">
        <View className="flex-row items-baseline gap-2">
          <Text numberOfLines={1} className="min-w-0 flex-1 font-t3-medium text-xl text-foreground">
            {props.thread.title}
          </Text>
          <Text className="text-sm text-foreground-tertiary">
            {rowTime(
              props.thread.latestUserMessageAt ?? props.thread.updatedAt,
              props.nowMs,
              locale,
            )}
          </Text>
        </View>
        <View className="flex-row items-center gap-2">
          <Text numberOfLines={1} className="min-w-0 flex-1 text-base text-foreground-secondary">
            {m(PREVIEW[status])}
          </Text>
          {unread ? <View className="size-2.5 rounded-full bg-primary" /> : null}
        </View>
      </View>
    </Pressable>
  );
}
