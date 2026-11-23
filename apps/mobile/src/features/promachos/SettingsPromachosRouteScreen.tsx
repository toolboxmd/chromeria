import { useGT } from "gt-react-native";
import { useSafeAreaInsets } from "react-native-safe-area-context";

import { ScreenScrollView as ScrollView } from "../../components/ui/ScreenScrollView";
import { SettingsScreen } from "../settings/components/SettingsScreen";
import { PromachosHomePicker } from "./PromachosTabScreen";

/** Settings → Promachos: where the phone's Promachos home is changed. */
export function SettingsPromachosRouteScreen() {
  const gt = useGT();
  const insets = useSafeAreaInsets();
  return (
    <SettingsScreen title={gt("Promachos")}>
      <ScrollView
        contentInsetAdjustmentBehavior="automatic"
        className="flex-1"
        contentContainerClassName="gap-3 px-5 pt-4"
        contentContainerStyle={{ paddingBottom: Math.max(insets.bottom, 18) + 18 }}
      >
        <PromachosHomePicker />
      </ScrollView>
    </SettingsScreen>
  );
}
