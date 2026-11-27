import type { EnvironmentId, PreviewAdjustInput, PreviewSessionSnapshot } from "@t3tools/contracts";
import { msg, useGT } from "gt-react-native";
import { View } from "react-native";

import { Button } from "../../components/ui/Button";
import * as Dialog from "../../components/ui/Dialog";
import { Notice } from "../../components/ui/Notice";
import { m } from "../../i18n/messages";
import { previewEnvironment } from "../../state/preview";
import { useAtomCommand } from "../../state/use-atom-command";

/** Agents stopped on this site; the person passes its check in this tab, then says so. */
export function SiteHoldBanner({
  environmentId,
  tab,
}: {
  readonly environmentId: EnvironmentId;
  readonly tab: PreviewSessionSnapshot;
}) {
  const gt = useGT();
  const adjust = useAtomCommand(previewEnvironment.adjust, "browser tab change");
  const hold = tab.siteHold;
  if (!hold) return null;
  const done = async () => {
    const input: PreviewAdjustInput = {
      threadId: tab.threadId as PreviewAdjustInput["threadId"],
      tabId: tab.tabId,
      clear: "siteHold",
    };
    const result = await adjust({ environmentId, input });
    if (result._tag === "Failure") Dialog.alert(m(msg("Could not change this browser tab")));
  };
  return (
    <View className="gap-2 px-3 py-2">
      <Notice
        icon="exclamationmark.triangle"
        message={
          hold.kind === "challenge"
            ? gt(
                "{site} asks for a human check. Agents stopped acting on this site. Take control, pass the check, release control, then tell the agent to continue.",
                { site: hold.site },
              )
            : gt("{site} blocked this browser. Agents stopped acting on this site.", {
                site: hold.site,
              })
        }
      />
      <Button
        size="compact"
        label={
          hold.kind === "challenge"
            ? gt("Done", { $context: "finished a site check" })
            : gt("Clear hold")
        }
        onPress={() => void done()}
      />
    </View>
  );
}
