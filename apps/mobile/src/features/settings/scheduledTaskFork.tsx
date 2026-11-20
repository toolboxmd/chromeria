import type { ScheduledTask, ScheduledTaskForkSchedule } from "@t3tools/contracts";
import { forkScheduleLabel, forkTaskSummary } from "@t3tools/client-runtime/scheduled-task-fork";
import { msg, T, useMessages, Var } from "gt-react-native";
import type { MessageResolver } from "../../i18n/messages";
import { Text } from "react-native";

/**
 * Fork: the mobile scheduled task screens' view of
 * one-shot triggers, fixed_time's fork fields, outcome checks and command tasks.
 * They are read-only here.
 */
export { forkScheduleLabel };
export { useAbandonReport } from "./spectrumReportAction";

/** A fork task's one-line state: its check verdict or its command's last result. */
export function ForkTaskSummaryText({ task }: { readonly task: ScheduledTask }) {
  const messages = useMessages();
  const summary = forkTaskSummary(task, messages);
  return summary === null ? null : (
    <Text className="text-sm text-foreground-muted" numberOfLines={2}>
      {summary}
    </Text>
  );
}

/** A fork trigger shown in place of the schedule picker, which it cannot express. */
export function PreservedScheduleText({
  schedule,
}: {
  readonly schedule: ScheduledTaskForkSchedule | null;
}) {
  const messages = useMessages();
  if (schedule === null) return null;
  const label = forkScheduleLabel(schedule, {}, messages);
  return (
    <Text className="px-4 py-3 text-base text-foreground-muted">
      {schedule.type === "fixed_time" ? (
        <T>
          <Var>{label}</Var> · change it on desktop or the web
        </T>
      ) : (
        <T>
          <Var>{label}</Var> · change it through an agent
        </T>
      )}
    </Text>
  );
}

/** Run now stays off while a run is reported running, including an unfinished checked run. */
export const runNowAction = (
  task: Pick<ScheduledTask, "lastRunStatus">,
  canOperate: boolean,
  resolve: MessageResolver,
) => ({
  id: "run",
  title: resolve(msg("Run now")),
  attributes: { disabled: !canOperate || task.lastRunStatus === "running" },
});
