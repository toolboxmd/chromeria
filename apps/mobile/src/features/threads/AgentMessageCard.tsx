import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { type AgentMessage, parseAgentMessage } from "@t3tools/client-runtime/agent-message";
import { type EnvironmentId, ThreadId } from "@t3tools/contracts";
import * as Haptics from "expo-haptics";
import { memo, useMemo, useState, type ReactNode } from "react";
import { type ColorValue, Pressable, View } from "react-native";
import Animated, { FadeIn, FadeOut, LinearTransition } from "react-native-reanimated";

import { SymbolView } from "../../components/AppSymbol";
import { AppText as Text } from "../../components/AppText";
import { useThreadShell } from "../../state/entities";
import { useAdaptiveWorkspaceLayout } from "../layout/AdaptiveWorkspaceLayout";
import { MarkdownImageAvailableWidthContext } from "./ThreadMarkdownImage";
import { THREAD_DISCLOSURE_TRANSITION_MS, ThreadDisclosureChevron } from "./thread-work-log";

const LAYOUT_TRANSITION = LinearTransition.duration(THREAD_DISCLOSURE_TRANSITION_MS);
// Card border and padding plus the body's indent under the icon.
const BODY_INSET = 48;

const parsedMessages = new WeakMap<{ readonly text: string }, AgentMessage | null>();

/** The parsed agent header of a feed message, cached per message object. */
export function agentMessageOf(message: { readonly text: string }): AgentMessage | null {
  let parsed = parsedMessages.get(message);
  if (parsed === undefined) {
    parsed = parseAgentMessage(message.text);
    parsedMessages.set(message, parsed);
  }
  return parsed;
}

/**
 * A report or message from another agent's thread, styled like the subagent
 * card: sender and a one-line preview, expanding to the full body. The
 * sender's title opens its thread while the thread still exists.
 */
export const AgentMessageCard = memo(function AgentMessageCard(props: {
  readonly message: AgentMessage;
  readonly environmentId: EnvironmentId;
  readonly iconSubtleColor: ColorValue;
  readonly contentWidth: number;
  readonly renderBody: (text: string) => ReactNode;
  readonly onCopy: () => void;
}) {
  const { message } = props;
  const [expanded, setExpanded] = useState(false);
  const { selectThread } = useAdaptiveWorkspaceLayout();
  const threadRef = useMemo(
    () => scopeThreadRef(props.environmentId, ThreadId.make(message.threadId)),
    [props.environmentId, message.threadId],
  );
  const thread = useThreadShell(threadRef);
  const label = message.kind === "report" ? "Report from" : "Message from";
  const hasBody = message.body.trim().length > 0;

  return (
    <Animated.View layout={LAYOUT_TRANSITION} className="-mx-1 mb-1 px-1">
      <Pressable
        accessibilityRole={hasBody ? "button" : undefined}
        accessibilityState={hasBody ? { expanded } : undefined}
        accessibilityLabel={`${label} ${message.title}, ${message.preview}`}
        accessibilityHint={
          hasBody
            ? `Double tap to ${expanded ? "hide" : "show"} the message. Long press to copy.`
            : "Long press to copy."
        }
        hitSlop={4}
        onPress={() => {
          if (!hasBody) return;
          void Haptics.selectionAsync();
          setExpanded((value) => !value);
        }}
        onLongPress={props.onCopy}
        className="rounded-xl border border-border-subtle bg-card px-2.5 py-2 active:bg-subtle"
      >
        <View className="flex-row items-center gap-2">
          <View className="h-6 w-6 shrink-0 items-center justify-center">
            <SymbolView
              name={
                message.kind === "report"
                  ? { ios: "sparkles", android: "auto_awesome" }
                  : { ios: "bubble.left", android: "chat_bubble" }
              }
              size={14}
              weight="medium"
              tintColor={props.iconSubtleColor}
              type="monochrome"
            />
          </View>
          <View className="min-w-0 flex-1 gap-0.5">
            <View className="flex-row items-center gap-1">
              <Text className="shrink-0 text-sm text-foreground-muted">{label}</Text>
              {thread ? (
                <Pressable
                  accessibilityRole="link"
                  accessibilityLabel={`Open ${message.title}`}
                  hitSlop={4}
                  onPress={() => selectThread(thread)}
                  className="min-w-0 shrink"
                >
                  <Text className="font-t3-medium text-sm text-foreground" numberOfLines={1}>
                    {message.title}
                  </Text>
                </Pressable>
              ) : (
                <Text className="min-w-0 shrink text-sm text-foreground-muted" numberOfLines={1}>
                  {message.title}
                </Text>
              )}
            </View>
            {message.preview ? (
              <Text className="text-xs text-foreground-muted" numberOfLines={1}>
                {message.preview}
              </Text>
            ) : null}
          </View>
          {hasBody ? (
            <ThreadDisclosureChevron
              expanded={expanded}
              collapsedDirection="down"
              size={11}
              tintColor={props.iconSubtleColor}
            />
          ) : null}
        </View>
        {expanded && hasBody ? (
          <Animated.View
            entering={FadeIn.duration(140)}
            exiting={FadeOut.duration(120)}
            layout={LAYOUT_TRANSITION}
            className="ml-8 mt-1.5"
          >
            <MarkdownImageAvailableWidthContext value={props.contentWidth - BODY_INSET}>
              {props.renderBody(message.body)}
            </MarkdownImageAvailableWidthContext>
          </Animated.View>
        ) : null}
      </Pressable>
    </Animated.View>
  );
});
