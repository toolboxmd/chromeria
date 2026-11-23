import { promachosStatusText } from "@t3tools/client-runtime/promachos/status";
import { useGT, useLocale } from "gt-react-native";
import { View, type ColorValue } from "react-native";

import { SymbolView } from "../../components/ui/AppSymbol";
import { AppText as Text } from "../../components/ui/AppText";
import { ShimmeringWorkContent } from "../threads/thread-work-log";

/** His avatar: Chromeria's own mark on the accent, the same in the list and the chat. */
export function PromachosAvatar(props: { readonly size: number }) {
  return (
    <View
      className="items-center justify-center rounded-full bg-primary"
      style={{ width: props.size, height: props.size }}
    >
      <SymbolView
        name="face.smiling"
        size={Math.round(props.size * 0.5)}
        tintColorClassName="accent-primary-foreground"
        type="monochrome"
      />
    </View>
  );
}

/** One line under the user's message while he works, naming his current step. */
export function PromachosStatusLine(props: {
  readonly label: string | undefined;
  readonly iconSubtleColor: ColorValue;
  /** Just before his reply: accent dots and "Typing…" in place of the avatar and step. */
  readonly typing?: boolean;
}) {
  const gt = useGT();
  return (
    <View className="min-h-9 flex-row items-center gap-2.5 py-1">
      {props.typing ? (
        <View className="flex-row items-center gap-1 px-1">
          <View className="size-1.5 rounded-full bg-primary opacity-40" />
          <View className="size-1.5 rounded-full bg-primary opacity-70" />
          <View className="size-1.5 rounded-full bg-primary" />
        </View>
      ) : (
        <PromachosAvatar size={30} />
      )}
      <ShimmeringWorkContent
        className="min-w-0 flex-1"
        textClassName="text-base text-foreground-secondary"
        icon="brain"
        iconSubtleColor={props.iconSubtleColor}
        label={props.typing ? gt("Typing…") : (promachosStatusText(props.label) ?? gt("Working"))}
        showIcon={false}
      />
    </View>
  );
}

function lineTime(input: string, locale: string, today: string, yesterday: string) {
  const at = new Date(input);
  if (Number.isNaN(at.getTime())) return "";
  const time = at.toLocaleTimeString(locale, { hour: "numeric", minute: "2-digit" });
  const now = new Date();
  const dayBefore = new Date(now);
  dayBefore.setDate(now.getDate() - 1);
  if (at.toDateString() === now.toDateString()) return `${today} ${time}`;
  if (at.toDateString() === dayBefore.toDateString()) return `${yesterday} ${time}`;
  return `${at.toLocaleDateString(locale, { month: "short", day: "numeric" })} ${time}`;
}

/** A centered gray line between groups: the time they start, or an event with its icon. */
export function PromachosLine(
  props:
    | { readonly kind: "time"; readonly at: string }
    | { readonly kind: "notice"; readonly label: string },
) {
  const gt = useGT();
  const locale = useLocale();
  return (
    <View className="flex-row items-center justify-center gap-1.5 py-2">
      {props.kind === "notice" ? (
        <SymbolView
          name="clock"
          size={13}
          tintColorClassName="accent-foreground-tertiary"
          type="monochrome"
        />
      ) : null}
      <Text numberOfLines={1} className="shrink text-sm text-foreground-tertiary">
        {props.kind === "time"
          ? lineTime(
              props.at,
              locale,
              gt("Today", { $context: "time line in a chat, before a time of day" }),
              gt("Yesterday", { $context: "time line in a chat, before a time of day" }),
            )
          : props.label}
      </Text>
    </View>
  );
}
