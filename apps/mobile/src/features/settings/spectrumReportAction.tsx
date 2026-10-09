import { useAtomValue } from "@effect/atom-react";
import type { MenuAction } from "@react-native-menu/menu";
import {
  abandonableReportRun,
  abandonReportFailureMessage,
  abandonReportNotice,
} from "@t3tools/client-runtime/spectrum-report";
import {
  createEnvironmentRpcCommand,
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import {
  type EnvironmentId,
  type ScheduledTask,
  SPECTRUM_WS_METHODS,
  type SpectrumReportAbandonInput,
} from "@t3tools/contracts";
import { Alert } from "react-native";

import { connectionAtomRuntime } from "../../connection/runtime";
import { useAtomCommand } from "../../state/use-atom-command";

const abandonReport = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "spectrum:abandon-report",
  tag: SPECTRUM_WS_METHODS.abandonReport,
});

const ACTION_ID = "abandon-report";

/**
 * Fork (toolboxmd/chromeria#176): **Abandon report** in a scheduled task row's
 * menu, offered only while a Spectrum report holds the task's run at needs-you.
 */
export function useAbandonReport(environmentId: EnvironmentId) {
  const abandon = useAtomCommand(abandonReport, { reportFailure: false });
  const canAbandon = useAtomValue(abandonReport.permissionAtom(environmentId));

  const submit = async (request: SpectrumReportAbandonInput): Promise<void> => {
    const result = await abandon({ environmentId, input: request });
    if (result._tag === "Success") {
      Alert.alert(abandonReportNotice(result.value));
    } else if (!isAtomCommandInterrupted(result)) {
      const message = abandonReportFailureMessage(squashAtomCommandFailure(result));
      Alert.alert("Could not abandon report", message, [
        { text: "Cancel", style: "cancel" },
        { text: "Retry", onPress: () => void submit(request) },
      ]);
    }
  };

  return {
    /** The menu entry for this task, or none. */
    actions: (task: ScheduledTask): MenuAction[] =>
      abandonableReportRun(task) === null
        ? []
        : [
            {
              id: ACTION_ID,
              title: "Abandon report",
              attributes: { destructive: true, disabled: !canAbandon },
            },
          ],
    /** Handles the menu's choice when it is this entry; other choices are left alone. */
    onAction: (action: string, task: ScheduledTask) => {
      const request = abandonableReportRun(task);
      if (action !== ACTION_ID || request === null || !canAbandon) return;
      Alert.alert(
        "Abandon report?",
        "Spectrum stops delivering it and the run goes on to its check.",
        [
          { text: "Cancel", style: "cancel" },
          { text: "Abandon", style: "destructive", onPress: () => void submit(request) },
        ],
      );
    },
  };
}
