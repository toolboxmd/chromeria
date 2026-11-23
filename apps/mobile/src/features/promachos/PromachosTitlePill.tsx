import { View } from "react-native";

import { AppText as Text } from "../../components/ui/AppText";
import { PromachosAvatar } from "./PromachosStatusLine";

/** The chat's centered name pill: his avatar and the conversation's title. */
export function PromachosTitlePill(props: { readonly title: string }) {
  return (
    <View className="h-11 max-w-[220px] flex-row items-center gap-2 rounded-full border border-border-subtle bg-card pr-4 pl-1.5 shadow-sm">
      <PromachosAvatar size={30} />
      <Text numberOfLines={1} className="min-w-0 shrink font-t3-medium text-lg text-foreground">
        {props.title}
      </Text>
    </View>
  );
}
