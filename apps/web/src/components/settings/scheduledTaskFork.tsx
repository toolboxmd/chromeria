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
import { Toggle as DayToggleBase } from "../ui/toggle";
import { Toggle, ToggleGroup } from "../ui/toggle-group";
import { MONTH_DAYS, type FixedTimeForkDraft } from "./scheduledTaskFork.logic";
import { AbandonReportAction } from "./spectrumReportAction";

/**
 * Fork: the web scheduled task screens' view of
 * one-shot triggers, fixed_time's fork fields, outcome checks and command tasks.
 * fixed_time's fields are edited here; the rest only through agent tools.
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

/** fixed_time's fork fields under upstream's time and weekdays. */
export function FixedTimeForkFields({
  draft,
  serverTimeZone,
  onChange,
}: {
  readonly draft: FixedTimeForkDraft;
  readonly serverTimeZone: string | undefined;
  readonly onChange: (draft: FixedTimeForkDraft) => void;
}) {
  return (
    <div className="w-full space-y-3">
      <div className="flex flex-wrap items-center gap-3">
        <ChromeriaFeatureMark />
        <div className="flex items-center gap-2">
          <Label htmlFor="scheduled-task-time-zone-name">Time zone</Label>
          <Input
            id="scheduled-task-time-zone-name"
            nativeInput
            className="w-44"
            placeholder={serverTimeZone ?? "Server's zone"}
            value={draft.timeZone}
            onChange={(event) => onChange({ ...draft, timeZone: event.target.value })}
          />
        </div>
        <div className="flex items-center gap-2">
          <Label htmlFor="scheduled-task-extra-times">Also at</Label>
          <Input
            id="scheduled-task-extra-times"
            nativeInput
            className="w-40"
            placeholder="13:00, 18:00"
            value={draft.extraTimes}
            onChange={(event) => onChange({ ...draft, extraTimes: event.target.value })}
          />
        </div>
        <div className="flex items-center gap-2">
          <Label htmlFor="scheduled-task-spread">Spread up to</Label>
          <Input
            type="number"
            id="scheduled-task-spread"
            nativeInput
            min={0}
            max={720}
            className="w-20"
            value={draft.windowMinutes}
            onChange={(event) => onChange({ ...draft, windowMinutes: event.target.value })}
          />
          <span className="text-xs text-muted-foreground">min</span>
        </div>
      </div>
      <div className="flex flex-wrap items-center gap-3">
        <ToggleGroup
          variant="outline"
          size="sm"
          aria-label="Run on"
          value={[draft.dayMode]}
          onValueChange={(values) => {
            const mode = values[0];
            if (mode === "week" || mode === "month") onChange({ ...draft, dayMode: mode });
          }}
        >
          <Toggle value="week">Days of the week</Toggle>
          <Toggle value="month">Days of the month</Toggle>
        </ToggleGroup>
        {draft.dayMode === "month" ? (
          <div role="group" aria-label="Days of the month" className="grid w-fit grid-cols-7 gap-1">
            {MONTH_DAYS.map((day) => (
              <DayToggle
                key={day}
                day={day}
                pressed={draft.monthDays.has(day)}
                onPressedChange={(pressed) => {
                  const monthDays = new Set(draft.monthDays);
                  if (pressed) monthDays.add(day);
                  else monthDays.delete(day);
                  onChange({ ...draft, monthDays });
                }}
              />
            ))}
          </div>
        ) : null}
      </div>
      {draft.dayMode === "month" ? (
        <p className="text-xs text-muted-foreground">
          A month without a chosen day, such as the 31st in April, runs on its last day.
        </p>
      ) : null}
    </div>
  );
}

function DayToggle(props: {
  readonly day: number;
  readonly pressed: boolean;
  readonly onPressedChange: (pressed: boolean) => void;
}) {
  return (
    <DayToggleBase
      variant="outline"
      size="sm"
      aria-label={props.day === 31 ? "Day 31 or the last day" : `Day ${props.day}`}
      pressed={props.pressed}
      onPressedChange={props.onPressedChange}
    >
      {props.day}
    </DayToggleBase>
  );
}
