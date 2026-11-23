import { useGT } from "gt-react-native";
import { View, type ColorValue } from "react-native";

import { SymbolView } from "../../components/ui/AppSymbol";
import { ShimmeringWorkContent } from "../threads/thread-work-log";

/** His avatar: Chromeria's own mark on the accent, the same in the list and the chat. */
function PromachosAvatar(props: { readonly size: number }) {
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
}) {
  const gt = useGT();
  return (
    <View className="min-h-9 flex-row items-center gap-2.5 py-1">
      <PromachosAvatar size={30} />
      <ShimmeringWorkContent
        className="min-w-0 flex-1"
        textClassName="text-base text-foreground-secondary"
        icon="brain"
        iconSubtleColor={props.iconSubtleColor}
        label={props.label ?? gt("Working")}
        showIcon={false}
      />
    </View>
  );
}
