import { useAtomValue } from "@effect/atom-react";
import { connectionStatusTitle } from "@t3tools/client-runtime/connection";
import {
  forkScheduleLabel,
  forkTaskSummary,
  scheduledTaskEntryKey,
  scheduledTaskSections,
} from "@t3tools/client-runtime/scheduled-task-fork";
import {
  type EnvironmentId,
  type ProjectId,
  resolveEnvironmentMachineKind,
  type ScheduledTask,
  type ScheduledTaskForkSchedule,
} from "@t3tools/contracts";
import { AsyncResult, Atom } from "effect/reactivity";
import { msg, T, useGT, useMessages, Var } from "gt-react-native";
import { Fragment, type ReactNode, useState } from "react";
import { Platform, Pressable, Text, View } from "react-native";

import { EnvironmentMachineSymbol } from "../../components/EnvironmentMachineSymbol";
import { AppText } from "../../components/ui/AppText";
import { SymbolView } from "../../components/ui/AppSymbol";
import type { MessageResolver } from "../../i18n/messages";
import type { EnvironmentPresentation } from "../../state/environments";
import { formatEnvironmentQueryError } from "../../state/query";
import { serverEnvironment } from "../../state/server";
import { SettingsSection } from "./components/SettingsSection";

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
  formatTime,
}: {
  readonly schedule: ScheduledTaskForkSchedule | null;
  readonly formatTime: (timeOfDay: string) => string;
}) {
  const messages = useMessages();
  if (schedule === null) return null;
  const label = forkScheduleLabel(schedule, { formatTime }, messages);
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

const scheduledTasksListAtom = Atom.family((key: string) =>
  Atom.make((get) =>
    key === ""
      ? []
      : (key.split("\n") as EnvironmentId[]).map((environmentId) =>
          get(serverEnvironment.scheduledTasksLive({ environmentId, input: {} })),
        ),
  ).pipe(Atom.withLabel(`mobile-scheduled-tasks-list:${key}`)),
);

export interface ScheduledTaskListRow {
  readonly environment: EnvironmentPresentation;
  readonly task: ScheduledTask;
  readonly timeZone: string | undefined;
  /** False for a section's first row, which draws no divider. */
  readonly divided: boolean;
  /** Kind, machine and project, shown first on the row. */
  readonly origin: ReactNode;
}

const SCOPE_SUMMARY_TEMPLATES = {
  notConnected: msg("Not connected: {environments}"),
  loading: msg("Loading scheduled tasks on {environments}"),
  failed: msg("Could not load scheduled tasks: {environments}"),
} as const;

/**
 * Every selected environment's tasks as one list: upcoming one-time tasks, recurring
 * tasks, then the one-time tasks that already ran, collapsed until asked for.
 */
export function ScheduledTaskList({
  environments,
  disconnected,
  projectIds,
  projectName,
  renderRow,
}: {
  readonly environments: ReadonlyArray<EnvironmentPresentation>;
  readonly disconnected: ReadonlyArray<EnvironmentPresentation>;
  /** Null when no project is selected. */
  readonly projectIds: (environmentId: EnvironmentId) => ReadonlyArray<ProjectId> | null;
  readonly projectName: (environmentId: EnvironmentId, projectId: ProjectId) => string | undefined;
  readonly renderRow: (row: ScheduledTaskListRow) => ReactNode;
}) {
  const gt = useGT();
  const messages = useMessages();
  const [failingOnly, setFailingOnly] = useState(false);
  const [historyOpen, setHistoryOpen] = useState(false);
  const queries = useAtomValue(
    scheduledTasksListAtom(environments.map((entry) => entry.environmentId).join("\n")),
  );
  const states = environments.map((environment, index) => {
    const query = queries[index]!;
    // A failed subscription can still hold its last list; its machine shows the error instead.
    const data = query._tag === "Failure" ? null : AsyncResult.getOrElse(query, () => null);
    const selected = projectIds(environment.environmentId);
    return {
      environment,
      timeZone: data?.timeZone,
      tasks: data?.tasks.filter((task) => selected === null || selected.includes(task.projectId)),
      error: query._tag === "Failure" ? formatEnvironmentQueryError(query.cause, messages) : null,
    };
  });
  const entries = states.flatMap(({ environment, tasks, timeZone }) =>
    (tasks ?? []).map((task) => ({
      environmentId: environment.environmentId,
      environment,
      task,
      timeZone,
    })),
  );
  const anyFailing = entries.some((entry) => entry.task.lastRunStatus === "failed");
  // The chip hides once nothing fails, so its selection must not wait for the next failure.
  if (failingOnly && !anyFailing) setFailingOnly(false);
  const sections = scheduledTaskSections(entries, { failingOnly: failingOnly && anyFailing });
  const summaries = {
    notConnected: disconnected.map(
      (environment) =>
        `${environment.label} (${connectionStatusTitle(environment.connection, messages)})`,
    ),
    loading: states.flatMap((state) =>
      state.error === null && !state.tasks ? [state.environment.label] : [],
    ),
    failed: states.flatMap((state) =>
      state.error === null ? [] : [`${state.environment.label}: ${state.error}`],
    ),
  };
  const loaded = states.some((state) => state.error === null && state.tasks);

  const rows = (list: ReadonlyArray<(typeof entries)[number]>) =>
    list.map((entry, index) => (
      <Fragment key={scheduledTaskEntryKey(entry)}>
        {renderRow({
          environment: entry.environment,
          task: entry.task,
          timeZone: entry.timeZone,
          divided: index > 0,
          origin: (
            <ScheduledTaskOrigin
              task={entry.task}
              environment={entry.environment}
              project={projectName(entry.environmentId, entry.task.projectId)}
            />
          ),
        })}
      </Fragment>
    ));

  return (
    <View className="gap-5">
      {Object.entries(summaries).map(([key, labels]) =>
        labels.length === 0 ? null : (
          <AppText
            key={key}
            role="status"
            textRole="footnote"
            className="px-2 text-foreground-muted"
          >
            {messages(SCOPE_SUMMARY_TEMPLATES[key as keyof typeof summaries], {
              environments: labels.join(", "),
            })}
          </AppText>
        ),
      )}
      {sections.failing > 0 ? (
        <Pressable
          accessibilityRole="switch"
          accessibilityState={{ checked: failingOnly }}
          onPress={() => setFailingOnly((value) => !value)}
          className={
            failingOnly
              ? "self-start rounded-full border border-danger-foreground bg-danger px-3 py-1.5"
              : "self-start rounded-full border border-border px-3 py-1.5"
          }
        >
          <AppText textRole="footnote" className="text-danger-foreground">
            <T>
              Failing · <Var>{sections.failing}</Var>
            </T>
          </AppText>
        </Pressable>
      ) : null}
      {loaded && entries.length === 0 ? (
        <AppText className="px-2 text-base text-foreground-muted">
          {environments.some((entry) => projectIds(entry.environmentId) !== null)
            ? gt("No tasks in this project.")
            : gt("No scheduled tasks yet.")}
        </AppText>
      ) : null}
      {sections.upcoming.length > 0 ? (
        <SettingsSection title={gt("Upcoming", { $context: "scheduled tasks section" })}>
          {rows(sections.upcoming)}
        </SettingsSection>
      ) : null}
      {sections.recurring.length > 0 ? (
        <SettingsSection title={gt("Recurring", { $context: "scheduled tasks section" })}>
          {rows(sections.recurring)}
        </SettingsSection>
      ) : null}
      {sections.history.length > 0 ? (
        <View className="gap-2">
          <Pressable
            accessibilityRole="button"
            accessibilityState={{ expanded: historyOpen }}
            onPress={() => setHistoryOpen((value) => !value)}
            className="min-h-11 flex-row items-center gap-2 px-2 active:opacity-70"
          >
            <AppText textRole="footnote" className="flex-1 font-t3-medium text-foreground-muted">
              {gt("History", { $context: "scheduled tasks section of one-time tasks that ran" })}
            </AppText>
            <AppText textRole="footnote" className="text-foreground-muted">
              {sections.historyFailing > 0
                ? gt("{count} · {failed} failed", {
                    count: sections.history.length,
                    failed: sections.historyFailing,
                  })
                : String(sections.history.length)}
            </AppText>
            <SymbolView
              name={historyOpen ? "chevron.down" : "chevron.right"}
              size={12}
              tintColorClassName="accent-foreground-muted"
              type="monochrome"
            />
          </Pressable>
          {/* Hundreds of spent one-time tasks would mount here, so they wait to be opened. */}
          {historyOpen ? <SettingsSection>{rows(sections.history)}</SettingsSection> : null}
        </View>
      ) : null}
    </View>
  );
}

/** What runs, on which machine, in which project. */
function ScheduledTaskOrigin({
  task,
  environment,
  project,
}: {
  readonly task: ScheduledTask;
  readonly environment: EnvironmentPresentation;
  readonly project: string | undefined;
}) {
  const gt = useGT();
  const command = task.command !== undefined;
  return (
    <View className="flex-row items-center gap-1.5">
      <View accessible accessibilityLabel={command ? gt("Shell command task") : gt("Agent task")}>
        <SymbolView
          name={command ? "terminal" : "robot"}
          size={13}
          tintColorClassName="accent-foreground-muted"
          type="monochrome"
        />
      </View>
      <EnvironmentMachineSymbol
        kind={resolveEnvironmentMachineKind(environment.serverConfig)}
        size={13}
        tintColorClassName={
          Platform.OS === "android" ? "accent-primary" : "accent-foreground-muted"
        }
      />
      <AppText className="shrink text-sm text-foreground-muted" numberOfLines={1}>
        {project === undefined ? environment.label : `${environment.label} · ${project}`}
      </AppText>
    </View>
  );
}
