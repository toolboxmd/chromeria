import { useAtomValue } from "@effect/atom-react";
import {
  createScheduledTaskRunThreadStatesAtom,
  type ScheduledTaskRunMark,
  type ScheduledTaskRunView,
  scheduledTaskRunThreadsChanged,
  scheduledTaskRunThreadsKey,
  scheduledTaskRunViews,
} from "@t3tools/client-runtime/scheduled-task-fork";
import type { EnvironmentId, ScheduledTask, ScheduledTaskRunRecord } from "@t3tools/contracts";
import { formatDuration } from "@t3tools/shared/orchestrationTimingCopy";
import { useGT, useMessages } from "gt-react-native";
import { useEffect, useRef, useState } from "react";
import { Pressable, View } from "react-native";

import { SymbolView } from "../../components/ui/AppSymbol";
import { AppText } from "../../components/ui/AppText";
import { cn } from "../../lib/cn";
import { useEnvironmentQuery } from "../../state/query";
import { serverEnvironment } from "../../state/server";
import { environmentThreadShells } from "../../state/threads";
import { useSelectWorkspaceDetail } from "../layout/workspace/useSelectWorkspaceDetail";

/**
 * Fork: a scheduled task's kept runs (toolboxmd/chromeria#951): a strip of one mark per run, and
 * a list of them with each thread's state. A run's row opens its thread.
 */
const NO_RUNS: ReadonlyArray<ScheduledTaskRunRecord> = [];

const MARK_CLASS: Record<ScheduledTaskRunMark, string> = {
  running: "bg-sky-500",
  failed: "bg-red-500",
  open: "bg-amber-500",
  succeeded: "bg-emerald-500",
};

const runThreadStatesAtom = createScheduledTaskRunThreadStatesAtom(
  environmentThreadShells.quietThreadShellAtom,
);

const formatStarted = (value: string) =>
  new Date(value).toLocaleString([], {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });

/** Under the task row: one mark per kept run, oldest first, and the runs themselves on demand. */
export function ScheduledTaskRuns({
  environmentId,
  task,
}: {
  readonly environmentId: EnvironmentId;
  readonly task: ScheduledTask;
}) {
  const gt = useGT();
  const [expanded, setExpanded] = useState(false);
  const query = useEnvironmentQuery(
    task.lastRunStatus === "never" && task.runCount === 0
      ? null
      : serverEnvironment.scheduledTaskRuns({ environmentId, input: { id: task.id } }),
  );
  const runs = query.data?.runs ?? NO_RUNS;
  const states = useAtomValue(runThreadStatesAtom(scheduledTaskRunThreadsKey(environmentId, runs)));
  const views = scheduledTaskRunViews(runs, states);
  const { refresh } = query;
  const previousStates = useRef(states);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Read the runs again when one of their threads starts or ends a run.
  useEffect(() => {
    if (scheduledTaskRunThreadsChanged(previousStates.current, states)) refresh();
    previousStates.current = states;
  }, [states, refresh]);
  if (views.runs.length === 0) return null;
  return (
    <View className="-mt-1 gap-2 px-4 pb-3">
      <Pressable
        accessibilityRole="button"
        accessibilityState={{ expanded }}
        accessibilityLabel={gt("{count} runs, {open} open", {
          count: views.runs.length,
          open: views.open,
        })}
        onPress={() => setExpanded((value) => !value)}
        className="min-h-8 flex-row items-center gap-2 active:opacity-70"
      >
        <View className="shrink flex-row items-center gap-0.5 overflow-hidden">
          {/* Hermes lacks Array#toReversed. */}
          {[...views.runs].reverse().map((view) => (
            <View
              key={view.run.id}
              className={cn("h-3.5 w-1 rounded-full", MARK_CLASS[view.mark])}
            />
          ))}
        </View>
        <SymbolView
          name={expanded ? "chevron.down" : "chevron.right"}
          size={11}
          tintColorClassName="accent-icon-muted"
          type="monochrome"
        />
        <AppText className="text-sm text-foreground-muted">
          {gt("Runs ({count})", { count: views.runs.length })}
        </AppText>
        {views.open === 0 ? null : (
          <RunBadge tone="warning">
            {gt("{count} open", {
              count: views.open,
              $context: "count of a scheduled task's runs whose threads are not settled yet",
            })}
          </RunBadge>
        )}
      </Pressable>
      {expanded ? (
        <View className="gap-2.5 border-l border-border pl-3">
          {views.runs.map((view) => (
            <RunRow key={view.run.id} environmentId={environmentId} view={view} />
          ))}
        </View>
      ) : null}
    </View>
  );
}

function RunBadge(props: {
  readonly tone: "warning" | "danger" | "muted";
  readonly children: string;
}) {
  return (
    <View
      className={cn(
        "rounded-md border px-1.5 py-0.5",
        props.tone === "warning"
          ? "border-warning-border bg-warning"
          : props.tone === "danger"
            ? "border-danger-border bg-danger"
            : "border-border",
      )}
    >
      <AppText
        className={cn(
          "text-xs",
          props.tone === "warning"
            ? "text-warning-foreground"
            : props.tone === "danger"
              ? "text-danger-foreground"
              : "text-foreground-muted",
        )}
      >
        {props.children}
      </AppText>
    </View>
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
  const selectDetail = useSelectWorkspaceDetail();
  const { run, thread } = view;
  const started = formatStarted(run.startedAt);
  const when =
    view.durationMs === null
      ? started
      : `${started} · ${formatDuration(view.durationMs, messages)}`;
  const body = (
    <>
      <View className="flex-row flex-wrap items-center gap-x-2 gap-y-1">
        <AppText className="text-xs text-foreground-muted">{when}</AppText>
        {run.status === "running" ? (
          <RunBadge tone="muted">
            {gt("Running", { $context: "scheduled task run status" })}
          </RunBadge>
        ) : null}
        {run.status === "failed" ? (
          <RunBadge tone="danger">
            {gt("Failed", { $context: "scheduled task run status" })}
          </RunBadge>
        ) : null}
        {thread === null ? null : thread.needsUser ? (
          <RunBadge tone="warning">{gt("Open · needs you")}</RunBadge>
        ) : thread.settled ? (
          <RunBadge tone="muted">
            {gt("Settled", { $context: "state of a thread the user is done with" })}
          </RunBadge>
        ) : null}
      </View>
      {run.summary === null ? null : (
        <AppText className="text-sm text-foreground" numberOfLines={1}>
          {run.summary}
        </AppText>
      )}
    </>
  );
  if (thread === null) return <View className="gap-1">{body}</View>;
  return (
    <Pressable
      accessibilityRole="button"
      accessibilityLabel={gt("Open thread of the run from {when}", { when: started })}
      onPress={() =>
        selectDetail({
          name: "Thread",
          params: { environmentId: String(environmentId), threadId: String(thread.id) },
        })
      }
      className="flex-row items-center gap-2 active:opacity-70"
    >
      <View className="min-w-0 flex-1 gap-1">{body}</View>
      <SymbolView
        name="arrow.up.right"
        size={13}
        tintColorClassName="accent-icon-muted"
        type="monochrome"
      />
    </Pressable>
  );
}
