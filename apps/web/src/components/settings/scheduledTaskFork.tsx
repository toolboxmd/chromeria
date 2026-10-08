import {
  type EnvironmentId,
  isForkScheduledTaskSchedule,
  type ScheduledTask,
  type ScheduledTaskUpsertSchedule,
} from "@t3tools/contracts";
import { forkScheduleLabel, forkTaskSummary } from "@t3tools/client-runtime/scheduled-task-fork";
import { useEffect, useState } from "react";

import { useEnvironmentQuery } from "../../state/query";
import { serverEnvironment } from "../../state/server";
import { Button } from "../ui/button";

/**
 * Fork (toolboxmd/chromeria#174): the web scheduled task screens' view of
 * one-shot and weekly triggers, outcome checks and command tasks. They are
 * read-only here and edited through agent tools.
 */
export { forkScheduleLabel };

/** A fork task's one-line state: its check verdict or its command's last result. */
export function ForkTaskSummary({
  environmentId,
  task,
}: {
  readonly environmentId: EnvironmentId;
  readonly task: ScheduledTask;
}) {
  const summary = forkTaskSummary(task);
  if (summary === null) return null;
  const run = task.command?.run;
  return (
    <>
      <span>{summary}</span>
      {run === null || run === undefined ? null : (
        <CommandOutputToggle
          environmentId={environmentId}
          taskId={task.id}
          runKey={`${run.id}:${run.endedAt ?? ""}`}
        />
      )}
    </>
  );
}

/** The live list leaves command output out, so it is fetched only when asked for. */
function CommandOutputToggle(props: {
  readonly environmentId: EnvironmentId;
  readonly taskId: ScheduledTask["id"];
  readonly runKey: string;
}) {
  const [open, setOpen] = useState(false);
  return (
    <>
      <Button variant="link" size="compact" onClick={() => setOpen((value) => !value)}>
        {open ? "Hide output" : "Show output"}
      </Button>
      {open ? <CommandOutput {...props} /> : null}
    </>
  );
}

function CommandOutput({
  environmentId,
  taskId,
  runKey,
}: {
  readonly environmentId: EnvironmentId;
  readonly taskId: ScheduledTask["id"];
  readonly runKey: string;
}) {
  const query = useEnvironmentQuery(
    serverEnvironment.scheduledTasksList({ environmentId, input: {} }),
  );
  const { refresh } = query;
  // Fetched on opening, and again when the run it shows changes.
  useEffect(() => {
    refresh();
  }, [refresh, runKey]);
  if (query.data === null) return <span className="text-muted-foreground">Loading output…</span>;
  const output = query.data.tasks.find((entry) => entry.id === taskId)?.command?.run?.output;
  return output === undefined || output === "" ? (
    <span className="text-muted-foreground">No output was kept for this run.</span>
  ) : (
    <pre className="w-full max-h-64 overflow-auto whitespace-pre-wrap text-xs">{output}</pre>
  );
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
