import type { ScheduledTask, ScheduledTaskForkSchedule } from "@t3tools/contracts";
import { forkScheduleLabel, forkTaskSummary } from "@t3tools/client-runtime/scheduled-task-fork";
import { Text } from "react-native";

/**
 * Fork (toolboxmd/chromeria#174): the mobile scheduled task screens' view of
 * one-shot and weekly triggers, outcome checks and command tasks. They are
 * read-only here and edited through agent tools.
 */
export { forkScheduleLabel };
export { useAbandonReport } from "./spectrumReportAction";

/** A fork task's one-line state: its check verdict or its command's last result. */
export function ForkTaskSummaryText({ task }: { readonly task: ScheduledTask }) {
  const summary = forkTaskSummary(task);
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
  return schedule === null ? null : (
    <Text className="px-4 py-3 text-base text-foreground-muted">
      {forkScheduleLabel(schedule)} · change it through an agent
    </Text>
  );
}

/** Run now stays off while a run is reported running, including an unfinished checked run. */
export const runNowAction = (task: Pick<ScheduledTask, "lastRunStatus">, canOperate: boolean) => ({
  id: "run",
  title: "Run now",
  attributes: { disabled: !canOperate || task.lastRunStatus === "running" },
});
