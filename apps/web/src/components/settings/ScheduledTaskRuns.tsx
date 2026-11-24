import { useAtomValue } from "@effect/atom-react";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import {
  createScheduledTaskRunThreadStatesAtom,
  type ScheduledTaskRunMark,
  type ScheduledTaskRunView,
  type ScheduledTaskRunViews,
  scheduledTaskRunsStale,
  scheduledTaskRunThreadsKey,
  scheduledTaskRunViews,
} from "@t3tools/client-runtime/scheduled-task-fork";
import type { EnvironmentId, ScheduledTask, ScheduledTaskRunRecord } from "@t3tools/contracts";
import { formatDuration } from "@t3tools/shared/orchestrationTimingCopy";
import { Link } from "@tanstack/react-router";
import { useGT, useMessages } from "gt-react";
import { ArrowUpRightIcon, ChevronDownIcon, ChevronRightIcon } from "lucide-react";
import { useEffect, useRef, useState } from "react";

import { useNowMinute } from "../../hooks/useNowMinute";
import { useClientSettings } from "../../hooks/useSettings";
import { cn } from "../../lib/utils";
import { useEnvironmentQuery } from "../../state/query";
import { serverEnvironment } from "../../state/server";
import { environmentThreadShells } from "../../state/threads";
import { buildThreadRouteParams } from "../../threadRoutes";
import { formatDayAwareTimestamp } from "../../timestampFormat";
import { Badge } from "../ui/badge";
import { Button } from "../ui/button";

/**
 * Fork: a scheduled task's kept runs (toolboxmd/chromeria#951): a strip of one mark per run, and
 * a list of them with each thread's state and a link to it.
 */
const NO_RUNS: ReadonlyArray<ScheduledTaskRunRecord> = [];

const MARK_CLASS: Record<ScheduledTaskRunMark, string> = {
  running: "bg-info",
  failed: "bg-destructive",
  open: "bg-warning",
  succeeded: "bg-success",
};

const runThreadStatesAtom = createScheduledTaskRunThreadStatesAtom(
  environmentThreadShells.quietThreadShellAtom,
);

function useScheduledTaskRuns(environmentId: EnvironmentId, task: ScheduledTask) {
  const query = useEnvironmentQuery(
    task.lastRunStatus === "never" && task.runCount === 0
      ? null
      : serverEnvironment.scheduledTaskRuns({ environmentId, input: { id: task.id } }),
  );
  const runs = query.data?.runs ?? NO_RUNS;
  const key = scheduledTaskRunThreadsKey(environmentId, runs);
  const states = useAtomValue(runThreadStatesAtom(key));
  return { views: scheduledTaskRunViews(runs, states), runs, key, states, refresh: query.refresh };
}

/** On the task's status line: how many of its runs still wait for the user. */
export function ScheduledTaskOpenRunsBadge(props: {
  readonly environmentId: EnvironmentId;
  readonly task: ScheduledTask;
}) {
  const gt = useGT();
  const { open } = useScheduledTaskRuns(props.environmentId, props.task).views;
  return open === 0 ? null : (
    <Badge variant="warning">
      {gt("{count} open", {
        count: open,
        $context: "count of a scheduled task's runs whose threads are not settled yet",
      })}
    </Badge>
  );
}

/** Under the task row: one mark per kept run, oldest first, and the runs themselves on demand. */
export function ScheduledTaskRuns(props: {
  readonly environmentId: EnvironmentId;
  readonly task: ScheduledTask;
}) {
  const gt = useGT();
  const [expanded, setExpanded] = useState(false);
  const { views, runs, key, states, refresh } = useScheduledTaskRuns(
    props.environmentId,
    props.task,
  );
  const previousSeen = useRef({ key, states });
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Read the runs again when one of their threads starts or ends a run.
  useEffect(() => {
    const seen = { key, states };
    if (scheduledTaskRunsStale(previousSeen.current, seen, runs)) refresh();
    previousSeen.current = seen;
  }, [key, states, runs, refresh]);
  if (views.runs.length === 0) return null;
  return (
    <div className="mt-2 space-y-1.5">
      <div className="flex items-center gap-2">
        <RunStrip views={views} />
        <Button
          variant="link"
          size="compact"
          aria-expanded={expanded}
          onClick={() => setExpanded((value) => !value)}
        >
          {expanded ? <ChevronDownIcon /> : <ChevronRightIcon />}
          {gt("Runs ({count})", { count: views.runs.length })}
        </Button>
      </div>
      {expanded ? (
        <ul className="space-y-1.5 border-l border-border pl-3">
          {views.runs.map((view) => (
            <RunRow key={view.run.id} environmentId={props.environmentId} view={view} />
          ))}
        </ul>
      ) : null}
    </div>
  );
}

function RunStrip({ views }: { readonly views: ScheduledTaskRunViews }) {
  const gt = useGT();
  return (
    <span
      role="img"
      aria-label={gt("{count, plural, one {{count} run} other {{count} runs}}, {open} open", {
        count: views.runs.length,
        open: views.open,
      })}
      className="flex items-center gap-0.5"
    >
      {views.runs.toReversed().map((view) => (
        <span key={view.run.id} className={cn("h-3.5 w-1 rounded-full", MARK_CLASS[view.mark])} />
      ))}
    </span>
  );
}

function RunRow({
  environmentId,
  view,
}: {
  readonly environmentId: EnvironmentId;
  readonly view: ScheduledTaskRunView;
}) {
  const gt = useGT();
  const messages = useMessages();
  const timestampFormat = useClientSettings((settings) => settings.timestampFormat);
  const nowMs = Date.parse(`${useNowMinute()}:00.000Z`);
  const { run, thread } = view;
  const started = formatDayAwareTimestamp(
    run.startedAt,
    timestampFormat,
    nowMs,
    undefined,
    messages,
  );
  return (
    <li className="flex flex-wrap items-center gap-x-2 gap-y-1 text-xs">
      <span className="shrink-0 text-muted-foreground tabular-nums">
        {view.durationMs === null
          ? started
          : `${started} · ${formatDuration(view.durationMs, messages)}`}
      </span>
      <span className="min-w-0 flex-1 basis-40 truncate text-foreground">{run.summary}</span>
      <span className="ml-auto flex shrink-0 items-center gap-2">
        {run.status === "running" ? (
          <Badge variant="info">{gt("Running", { $context: "scheduled task run status" })}</Badge>
        ) : null}
        {run.status === "failed" ? (
          <Badge variant="error">{gt("Failed", { $context: "scheduled task run status" })}</Badge>
        ) : null}
        {thread === null ? null : thread.needsUser ? (
          <Badge variant="warning">{gt("Open · needs you")}</Badge>
        ) : thread.settled ? (
          <Badge variant="outline">
            {gt("Settled", { $context: "state of a thread the user is done with" })}
          </Badge>
        ) : null}
        {thread === null ? null : (
          <Link
            to="/$environmentId/$threadId"
            params={buildThreadRouteParams(scopeThreadRef(environmentId, thread.id))}
            className="inline-flex items-center gap-0.5 text-muted-foreground underline underline-offset-2 hover:text-foreground"
          >
            {gt("Open thread")}
            <ArrowUpRightIcon className="size-3" />
          </Link>
        )}
      </span>
    </li>
  );
}
