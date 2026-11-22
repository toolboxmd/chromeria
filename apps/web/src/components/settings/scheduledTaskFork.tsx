import {
  type EnvironmentId,
  isForkScheduledTaskSchedule,
  type ScheduledTask,
  type ScheduledTaskUpsertSchedule,
} from "@t3tools/contracts";
import { forkTaskSummary } from "@t3tools/client-runtime/scheduled-task-fork";
import { T, useGT, Var } from "gt-react";
import { useEffect, useState } from "react";

import { useClientSettings } from "../../hooks/useSettings";
import { useEnvironmentQuery } from "../../state/query";
import { ChromeriaFeatureMark } from "../ChromeriaFeatureMark";
import { serverEnvironment } from "../../state/server";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import { Label } from "../ui/label";
import { Toggle as DayToggleBase } from "../ui/toggle";
import { Toggle, ToggleGroup } from "../ui/toggle-group";
import { MONTH_DAYS, type FixedTimeForkDraft } from "./scheduledTaskFork.logic";
import { scheduleLabel } from "./scheduledTasksSettings.logic";
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
  const gt = useGT();
  const [open, setOpen] = useState(false);
  return (
    <>
      <Button variant="link" size="compact" onClick={() => setOpen((value) => !value)}>
        {open ? gt("Hide output") : gt("Show output")}
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
        <T>
          Could not load the output: <Var>{query.error}</Var>
        </T>{" "}
        <Button variant="link" size="compact" onClick={refresh}>
          <T>Retry</T>
        </Button>
      </span>
    );
  // While fetching, the query still holds the previous result: never show it as this run's.
  if (query.isPending || query.data === null)
    return (
      <span className="text-muted-foreground">
        <T>Loading output…</T>
      </span>
    );
  const output = query.data.tasks.find((entry) => entry.id === taskId)?.command?.run?.output;
  return output === undefined || output === "" ? (
    <span className="text-muted-foreground">
      <T>No output was kept for this run.</T>
    </span>
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
  const timestampFormat = useClientSettings((settings) => settings.timestampFormat);
  return schedule === null || !isForkScheduledTaskSchedule(schedule) ? null : (
    <span className="inline-flex items-center gap-1.5 text-sm text-muted-foreground">
      <ChromeriaFeatureMark />
      <span>
        <T>
          <Var>{scheduleLabel(schedule, timestampFormat)}</Var> · change it through an agent
        </T>
      </span>
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
  const gt = useGT();
  return (
    <div className="w-full space-y-3">
      <div className="flex flex-wrap items-center gap-3">
        <ChromeriaFeatureMark />
        <div className="flex items-center gap-2">
          <Label htmlFor="scheduled-task-time-zone-name">
            <T>Time zone</T>
          </Label>
          <Input
            id="scheduled-task-time-zone-name"
            nativeInput
            className="w-44"
            placeholder={serverTimeZone ?? gt("Server's zone")}
            value={draft.timeZone}
            onChange={(event) => onChange({ ...draft, timeZone: event.target.value })}
          />
        </div>
        <div className="flex items-center gap-2">
          <Label htmlFor="scheduled-task-extra-times">
            <T context="label before extra run times">Also at</T>
          </Label>
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
          <Label htmlFor="scheduled-task-spread">
            <T context="label before a number of minutes to spread run times over">Spread up to</T>
          </Label>
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
          <span className="text-xs text-muted-foreground">
            <T context="abbreviation for minutes">min</T>
          </span>
        </div>
      </div>
      <div className="flex flex-wrap items-center gap-3">
        <ToggleGroup
          variant="outline"
          size="sm"
          aria-label={gt("Run on", { $context: "which days a scheduled task runs" })}
          value={[draft.dayMode]}
          onValueChange={(values) => {
            const mode = values[0];
            if (mode === "week" || mode === "month") onChange({ ...draft, dayMode: mode });
          }}
        >
          <Toggle value="week">
            <T>Days of the week</T>
          </Toggle>
          <Toggle value="month">
            <T>Days of the month</T>
          </Toggle>
        </ToggleGroup>
        {draft.dayMode === "month" ? (
          <div
            role="group"
            aria-label={gt("Days of the month")}
            className="grid w-fit grid-cols-7 gap-1"
          >
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
          <T>A month without a chosen day, such as the 31st in April, runs on its last day.</T>
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
  const gt = useGT();
  return (
    <DayToggleBase
      variant="outline"
      size="sm"
      aria-label={
        props.day === 31 ? gt("Day 31 or the last day") : gt("Day {day}", { day: props.day })
      }
      pressed={props.pressed}
      onPressedChange={props.onPressedChange}
    >
      {props.day}
    </DayToggleBase>
  );
}
