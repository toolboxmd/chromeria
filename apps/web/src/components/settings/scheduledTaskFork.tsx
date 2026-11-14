import {
  type EnvironmentId,
  isForkScheduledTaskSchedule,
  type ScheduledTask,
  type ScheduledTaskUpsertSchedule,
} from "@t3tools/contracts";
import { forkScheduleLabel, forkTaskSummary } from "@t3tools/client-runtime/scheduled-task-fork";
import { useEffect, useState } from "react";

import { useEnvironmentQuery } from "../../state/query";
import { ChromeriaFeatureMark } from "../ChromeriaFeatureMark";
import { serverEnvironment } from "../../state/server";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import { Label } from "../ui/label";
import { Select, SelectItem, SelectPopup, SelectTrigger, SelectValue } from "../ui/select";
import { MONTHLY_DAYS, monthlyDayLabel, type MonthlyDraft } from "./scheduledTaskFork.logic";
import { AbandonReportAction } from "./spectrumReportAction";

/**
 * Fork (toolboxmd/chromeria#174): the web scheduled task screens' view of
 * one-shot, weekly and monthly triggers, outcome checks and command tasks.
 * Monthly triggers are edited here; the rest only through agent tools.
 */
/** A fork task's one-line state: its check verdict or its command's last result. */
export function ForkTaskSummary({
  environmentId,
  task,
}: {
  readonly environmentId: EnvironmentId;
  readonly task: ScheduledTask;
}) {
  const summary = forkTaskSummary(task);
  if (summary === null && !isForkScheduledTaskSchedule(task.schedule)) return null;
  const run = task.command?.run;
  return (
    <>
      <span className="inline-flex items-center gap-1.5">
        <ChromeriaFeatureMark />
        {summary === null ? null : <span>{summary}</span>}
      </span>
      <AbandonReportAction environmentId={environmentId} task={task} />
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
      {/* A new run remounts it, so it fetches the output again. */}
      {open ? (
        <CommandOutput
          key={props.runKey}
          environmentId={props.environmentId}
          taskId={props.taskId}
        />
      ) : null}
    </>
  );
}

function CommandOutput({
  environmentId,
  taskId,
}: {
  readonly environmentId: EnvironmentId;
  readonly taskId: ScheduledTask["id"];
}) {
  const query = useEnvironmentQuery(
    serverEnvironment.scheduledTasksList({ environmentId, input: {} }),
  );
  const { refresh } = query;
  // Fetched on opening.
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Refresh persisted command output when its viewer mounts.
  useEffect(() => {
    refresh();
  }, [refresh]);
  if (query.error !== null)
    return (
      <span className="text-destructive">
        Could not load the output: {query.error}{" "}
        <Button variant="link" size="compact" onClick={refresh}>
          Retry
        </Button>
      </span>
    );
  // While fetching, the query still holds the previous result: never show it as this run's.
  if (query.isPending || query.data === null)
    return <span className="text-muted-foreground">Loading output…</span>;
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
    <span className="inline-flex items-center gap-1.5 text-sm text-muted-foreground">
      <ChromeriaFeatureMark />
      <span>{forkScheduleLabel(schedule)} · change it through an agent</span>
    </span>
  );
}

/** A monthly trigger's day, times and time zone. A day a month lacks runs on its last day. */
export function MonthlyScheduleFields({
  draft,
  onChange,
}: {
  readonly draft: MonthlyDraft;
  readonly onChange: (draft: MonthlyDraft) => void;
}) {
  return (
    <div className="space-y-2">
      <div className="flex flex-wrap items-center gap-3">
        <div className="flex items-center gap-2">
          <Label htmlFor="scheduled-task-month-day">On day</Label>
          <Select
            value={draft.day}
            onValueChange={(day) => {
              if (typeof day === "string") onChange({ ...draft, day });
            }}
          >
            <SelectTrigger size="sm" id="scheduled-task-month-day">
              <SelectValue>{monthlyDayLabel(draft.day)}</SelectValue>
            </SelectTrigger>
            <SelectPopup>
              {MONTHLY_DAYS.map((day) => (
                <SelectItem key={day} value={day}>
                  {monthlyDayLabel(day)}
                </SelectItem>
              ))}
            </SelectPopup>
          </Select>
        </div>
        <div className="flex items-center gap-2">
          <Label htmlFor="scheduled-task-month-times">at</Label>
          <Input
            id="scheduled-task-month-times"
            nativeInput
            className="w-40"
            placeholder="09:00, 18:00"
            value={draft.times}
            onChange={(event) => onChange({ ...draft, times: event.target.value })}
          />
        </div>
        <div className="flex items-center gap-2">
          <Label htmlFor="scheduled-task-month-zone">in</Label>
          <Input
            id="scheduled-task-month-zone"
            nativeInput
            className="w-44"
            placeholder="Europe/Warsaw"
            value={draft.timeZone}
            onChange={(event) => onChange({ ...draft, timeZone: event.target.value })}
          />
        </div>
      </div>
      <p className="text-xs text-muted-foreground">
        A month without that day, such as the 31st in April, runs on its last day.
      </p>
    </div>
  );
}
