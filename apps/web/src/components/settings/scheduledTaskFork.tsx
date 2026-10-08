import {
  isForkScheduledTaskSchedule,
  type ScheduledTask,
  type ScheduledTaskUpsertSchedule,
} from "@t3tools/contracts";
import { forkScheduleLabel, forkTaskSummary } from "@t3tools/client-runtime/scheduled-task-fork";

/**
 * Fork (toolboxmd/chromeria#174): the web scheduled task screens' view of
 * one-shot and weekly triggers, outcome checks and command tasks. They are
 * read-only here and edited through agent tools.
 */
export { forkScheduleLabel };

/** A fork task's one-line state: its check verdict or its command's last result. */
export function ForkTaskSummary({ task }: { readonly task: ScheduledTask }) {
  const summary = forkTaskSummary(task);
  return summary === null ? null : <span>{summary}</span>;
}

/** A fork trigger shown beside the disabled schedule picker, which cannot express it. */
export function PreservedScheduleNote({
  schedule,
}: {
  readonly schedule: ScheduledTaskUpsertSchedule | null;
}) {
  return schedule === null || !isForkScheduledTaskSchedule(schedule) ? null : (
    <span className="text-sm text-muted-foreground">
      {forkScheduleLabel(schedule)} · change it through an agent
    </span>
  );
}
