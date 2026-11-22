import { useAtom, useAtomValue } from "@effect/atom-react";
import { connectionStatusTitle } from "@t3tools/client-runtime/connection";
import {
  forkTaskSummary,
  isScheduledTaskHistory,
  type ScheduledTaskListEntry,
  scheduledTaskEntryKey,
  scheduledTaskSections,
} from "@t3tools/client-runtime/scheduled-task-fork";
import {
  type EnvironmentId,
  isForkScheduledTaskSchedule,
  resolveEnvironmentMachineKind,
  type ScheduledTask,
  type ScheduledTaskId,
  type ScheduledTaskUpsertSchedule,
} from "@t3tools/contracts";
import { AsyncResult, Atom } from "effect/reactivity";
import { msg, T, useGT, useMessages, Var } from "gt-react";
import { BotIcon, TerminalIcon } from "lucide-react";
import { Fragment, type ReactNode, useEffect, useState } from "react";

import { useClientSettings } from "../../hooks/useSettings";
import { environmentAtomList, environmentIdsKey } from "../../state/environmentAtomList";
import type { EnvironmentPresentation } from "../../state/environments";
import { formatEnvironmentQueryError, useEnvironmentQuery } from "../../state/query";
import { ChromeriaFeatureMark } from "../ChromeriaFeatureMark";
import { EnvironmentMachineIcon } from "../EnvironmentMachineIcon";
import { serverEnvironment } from "../../state/server";
import { Button } from "../ui/button";
import { CollapsibleSectionHeader } from "../ui/collapsible-section-header";
import { Input } from "../ui/input";
import { Label } from "../ui/label";
import { Toggle as DayToggleBase } from "../ui/toggle";
import { Toggle, ToggleGroup } from "../ui/toggle-group";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../ui/tooltip";
import { MONTH_DAYS, type FixedTimeForkDraft } from "./scheduledTaskFork.logic";
import { matchesScheduledTaskScope, scheduleLabel } from "./scheduledTasksSettings.logic";
import { SettingsGroup } from "./SettingsGroup";
import { useSettingsScope } from "./SettingsScopeContext";
import { settingsScopeEnvironmentLabel } from "./settingsScopeAxis";
import { SettingsRow } from "./settingsLayout";
import { AbandonReportAction } from "./spectrumReportAction";

/**
 * Fork: the web scheduled task screens' view of
 * one-shot triggers, fixed_time's fork fields, outcome checks and command tasks.
 * fixed_time's fields are edited here; the rest only through agent tools.
 */
export interface ScheduledTaskListRow extends ScheduledTaskListEntry {
  readonly environmentId: EnvironmentId;
  readonly timeZone: string | undefined;
  /** Kind, machine and project, shown first on the row. */
  readonly origin: ReactNode;
}

const scheduledTasksListAtom = environmentAtomList("web-scheduled-tasks-list", (environmentId) =>
  serverEnvironment.scheduledTasksLive({ environmentId, input: {} }),
);

/**
 * A link into History, carried across the remount Settings does when the editor closes and the
 * link leaves the URL. The next mount without a link takes it once; leaving the page drops it.
 */
const pendingHistoryLinkAtom = Atom.make<string | null>(null).pipe(
  Atom.keepAlive,
  Atom.withLabel("web-scheduled-tasks-pending-history-link"),
);

const SCOPE_SUMMARY_TEMPLATES = {
  notConnected: msg("Not connected: {environments}"),
  loading: msg("Loading scheduled tasks on {environments}"),
  failed: msg("Could not load scheduled tasks: {environments}"),
  empty: msg("No tasks on {environments}"),
} as const;

/**
 * Every connected environment's tasks as one list: upcoming one-time tasks, recurring
 * tasks, then the one-time tasks that already ran, collapsed until asked for.
 */
export function ScheduledTaskList({
  environmentId,
  taskId,
  renderRow,
}: {
  readonly environmentId: EnvironmentId | undefined;
  readonly taskId: ScheduledTaskId | undefined;
  readonly renderRow: (row: ScheduledTaskListRow) => ReactNode;
}) {
  const messages = useMessages();
  const gt = useGT();
  const { scope, environments, groups } = useSettingsScope();
  const [failingOnly, setFailingOnly] = useState(false);
  const [pendingHistoryLink, setPendingHistoryLink] = useAtom(pendingHistoryLinkAtom);
  const [historyOpen, setHistoryOpen] = useState(() => pendingHistoryLink !== null);
  const [shownLink, setShownLink] = useState<string | null>(null);
  const connectedIds = environments.flatMap((environment) =>
    environment.connection.phase === "connected" && environment.serverConfig !== null
      ? [environment.environmentId]
      : [],
  );
  const queries = useAtomValue(scheduledTasksListAtom(environmentIdsKey(connectedIds)));
  const states = environments.map((environment) => {
    const index = connectedIds.indexOf(environment.environmentId);
    const query = index < 0 ? null : queries[index]!;
    // A failed subscription can still hold its last list; its machine shows the error instead.
    const data =
      query === null || query._tag === "Failure" ? null : AsyncResult.getOrElse(query, () => null);
    return {
      environment,
      connected: index >= 0,
      timeZone: data?.timeZone,
      tasks: data?.tasks.filter((task) =>
        matchesScheduledTaskScope(scope, environment.environmentId, task.projectId),
      ),
      error: query?._tag === "Failure" ? formatEnvironmentQueryError(query.cause, messages) : null,
    };
  });
  const entries = states.flatMap(({ environment, tasks, timeZone }) =>
    (tasks ?? []).map((task) => ({
      environmentId: environment.environmentId,
      task,
      timeZone,
      environment,
    })),
  );
  const linkedState = states.find((state) => state.environment.environmentId === environmentId);
  const linkedTask = linkedState?.tasks?.find((task) => task.id === taskId);
  const linkedKey =
    linkedTask && environmentId !== undefined
      ? scheduledTaskEntryKey({ environmentId, task: linkedTask })
      : null;
  const historyLink =
    linkedTask !== undefined && isScheduledTaskHistory(linkedTask) ? linkedKey : null;
  // A newly loaded link shows its row: History opens and Failing stops hiding it, then the
  // user owns both.
  if (linkedKey !== shownLink) {
    setShownLink(linkedKey);
    if (historyLink !== null) setHistoryOpen(true);
    if (linkedTask !== undefined && linkedTask.lastRunStatus !== "failed") setFailingOnly(false);
  }
  const anyFailing = entries.some((entry) => entry.task.lastRunStatus === "failed");
  // The chip hides once nothing fails, so its selection must not wait for the next failure.
  if (failingOnly && !anyFailing) setFailingOnly(false);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Carries a URL link into History across the remount that drops it, in a store outside this component.
  useEffect(() => {
    if (historyLink !== null) setPendingHistoryLink(historyLink);
    else if (taskId === undefined) setPendingHistoryLink(null);
  }, [historyLink, taskId, setPendingHistoryLink]);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Leaving the page drops a carried link, so a later visit starts with History closed.
  useEffect(
    () => () => {
      if (window.location.pathname !== "/settings/scheduled-tasks") setPendingHistoryLink(null);
    },
    [setPendingHistoryLink],
  );
  const sections = scheduledTaskSections(entries, { failingOnly: failingOnly && anyFailing });

  const loaded = states.filter((state) => state.connected && state.error === null && state.tasks);
  const populated = loaded.some((state) => state.tasks && state.tasks.length > 0);
  const summaries: Record<keyof typeof SCOPE_SUMMARY_TEMPLATES, string[]> = {
    notConnected: [],
    loading: [],
    failed: [],
    empty: [],
  };
  for (const { environment, connected, tasks, error } of states) {
    const label = settingsScopeEnvironmentLabel(environment, environments);
    if (!connected) {
      summaries.notConnected.push(
        `${label} (${connectionStatusTitle(environment.connection, messages)})`,
      );
    } else if (error !== null) {
      summaries.failed.push(`${label}: ${error}`);
    } else if (!tasks) {
      summaries.loading.push(label);
    } else if (tasks.length === 0 && populated && environments.length > 1) {
      summaries.empty.push(label);
    }
  }

  const row = (entry: (typeof entries)[number]) => (
    <Fragment key={scheduledTaskEntryKey(entry)}>
      {renderRow({
        environmentId: entry.environmentId,
        task: entry.task,
        timeZone: entry.timeZone,
        origin: (
          <ScheduledTaskOrigin
            task={entry.task}
            environment={entry.environment}
            machine={settingsScopeEnvironmentLabel(entry.environment, environments)}
            project={
              groups.find((group) =>
                // Refs keep every registration of a grouped checkout, members only the newest.
                group.memberProjectRefs.some(
                  (ref) =>
                    ref.environmentId === entry.environmentId &&
                    ref.projectId === entry.task.projectId,
                ),
              )?.displayName
            }
          />
        ),
      })}
    </Fragment>
  );

  return (
    <div className="space-y-4">
      {Object.entries(summaries).map(([key, labels]) => {
        if (labels.length === 0) return null;
        const summary = messages(SCOPE_SUMMARY_TEMPLATES[key as keyof typeof summaries], {
          environments: labels.join(", "),
        });
        return (
          <Tooltip key={key}>
            <TooltipTrigger
              render={<p className="truncate px-1 text-xs text-muted-foreground" role="status" />}
            >
              {summary}
            </TooltipTrigger>
            <TooltipPopup>{summary}</TooltipPopup>
          </Tooltip>
        );
      })}
      {sections.failing > 0 ? (
        <DayToggleBase
          variant="outline"
          size="sm"
          pressed={failingOnly}
          onPressedChange={setFailingOnly}
        >
          <T>
            Failing · <Var>{sections.failing}</Var>
          </T>
        </DayToggleBase>
      ) : null}
      {taskId && linkedState?.tasks && linkedState.error === null && !linkedTask ? (
        <SettingsGroup>
          <SettingsRow
            title={gt("Task unavailable")}
            description={gt(
              "This task was deleted or belongs to a different project. Check your project filter.",
            )}
            role="status"
          />
        </SettingsGroup>
      ) : null}
      {entries.length === 0 && loaded.length > 0 ? (
        <SettingsGroup>
          <SettingsRow
            title={gt("No scheduled tasks")}
            description={gt("No tasks match this environment and project selection.")}
          />
        </SettingsGroup>
      ) : null}
      {sections.upcoming.length > 0 ? (
        <ScheduledTaskListSection title={gt("Upcoming", { $context: "scheduled tasks section" })}>
          {sections.upcoming.map(row)}
        </ScheduledTaskListSection>
      ) : null}
      {sections.recurring.length > 0 ? (
        <ScheduledTaskListSection title={gt("Recurring", { $context: "scheduled tasks section" })}>
          {sections.recurring.map(row)}
        </ScheduledTaskListSection>
      ) : null}
      {sections.history.length > 0 ? (
        <div className="space-y-1">
          <CollapsibleSectionHeader
            expanded={historyOpen}
            tone="emphasized"
            onClick={() => setHistoryOpen(!historyOpen)}
            accessory={
              <span className="text-xs text-muted-foreground">
                {sections.historyFailing > 0
                  ? gt("{count} · {failed} failed", {
                      count: sections.history.length,
                      failed: sections.historyFailing,
                    })
                  : sections.history.length}
              </span>
            }
          >
            {gt("History", { $context: "scheduled tasks section of one-time tasks that ran" })}
          </CollapsibleSectionHeader>
          {/* Hundreds of spent one-time tasks would mount here, so they wait to be opened. */}
          {historyOpen ? <SettingsGroup>{sections.history.map(row)}</SettingsGroup> : null}
        </div>
      ) : null}
    </div>
  );
}

function ScheduledTaskListSection({
  title,
  children,
}: {
  readonly title: string;
  readonly children: ReactNode;
}) {
  return (
    <section className="space-y-1" aria-label={title}>
      <h3 className="px-2 text-xs font-medium text-muted-foreground">{title}</h3>
      <SettingsGroup>{children}</SettingsGroup>
    </section>
  );
}

/** What runs, on which machine, in which project. */
function ScheduledTaskOrigin({
  task,
  environment,
  machine,
  project,
}: {
  readonly task: ScheduledTask;
  readonly environment: EnvironmentPresentation;
  readonly machine: string;
  readonly project: string | undefined;
}) {
  const gt = useGT();
  const command = task.command !== undefined;
  const kind = command ? gt("Shell command task") : gt("Agent task");
  return (
    <span className="inline-flex min-w-0 items-center gap-1.5">
      {command ? (
        <TerminalIcon className="size-3.5 shrink-0" aria-label={kind} role="img" />
      ) : (
        <BotIcon className="size-3.5 shrink-0" aria-label={kind} role="img" />
      )}
      <EnvironmentMachineIcon
        kind={resolveEnvironmentMachineKind(environment.serverConfig)}
        className="size-3.5 shrink-0"
      />
      <span className="truncate">
        {project === undefined ? machine : `${machine} · ${project}`}
      </span>
    </span>
  );
}

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
