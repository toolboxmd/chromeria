import { View } from "react-native";

import { AppText as Text } from "../../components/ui/AppText";

/** The chat's centered name pill: the conversation's title. */
export function PromachosTitlePill(props: { readonly title: string }) {
  return (
    <View className="h-11 max-w-[220px] flex-row items-center rounded-full border border-border-subtle bg-card px-4 shadow-sm">
      <Text numberOfLines={1} className="min-w-0 shrink font-t3-medium text-lg text-foreground">
        {props.title}
      </Text>
    </View>
  );
}
