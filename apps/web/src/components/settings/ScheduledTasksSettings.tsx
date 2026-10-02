import {
  isCommandTask,
  PRISM_ROLE_LABELS,
  PRISM_ROLES,
  PrismLane,
  type EnvironmentId,
  type ScheduledTask,
  type ScheduledTaskView,
  type TaskCheckVersion,
  type TaskRun,
} from "@t3tools/contracts";
import { ChevronDownIcon, ChevronRightIcon, PlusIcon } from "lucide-react";
import type { AtomCommandResult } from "@t3tools/client-runtime/state/runtime";
import { type ComponentProps, type ReactNode, useMemo, useState } from "react";

import { requestConfirmDialog } from "~/confirmDialog";
import { useProjects, useThreadShells } from "../../state/entities";
import { formatEnvironmentQueryError, useEnvironmentQuery } from "../../state/query";
import {
  scheduledTaskCheckHistory,
  scheduledTaskCreate,
  scheduledTaskDelete,
  scheduledTaskEdit,
  scheduledTaskList,
  scheduledTaskPause,
  scheduledTaskRunNow,
} from "../../state/scheduler";
import { useAtomCommand } from "../../state/use-atom-command";
import { isSpectrumThreadId } from "../subagentThreads";
import { Badge } from "../ui/badge";
import { Button } from "../ui/button";
import {
  Dialog,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogPanel,
  DialogPopup,
  DialogTitle,
} from "../ui/dialog";
import { Input } from "../ui/input";
import { Label } from "../ui/label";
import { Select, SelectItem, SelectPopup, SelectTrigger, SelectValue } from "../ui/select";
import { Textarea } from "../ui/textarea";
import { toastManager } from "../ui/toast";
import { Toggle, ToggleGroup } from "../ui/toggle-group";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../ui/tooltip";
import { useSettingsScope } from "./SettingsScopeContext";
import { SettingsScopeNotice } from "./SettingsScopeNotice";
import { SettingsPageContainer, SettingsSection } from "./settingsLayout";
import {
  activeCheck,
  commandRunView,
  createPayloadFromDraft,
  DEFAULT_WINDOW_MINUTES,
  deleteBlockedReason,
  describeActor,
  describeChoice,
  describeSchedule,
  draftFromTask,
  editPayloadFromDraft,
  emptyTaskDraft,
  formatSlot,
  latestRun,
  needsYou,
  runNowBlockedReason,
  runStatusView,
  taskCreator,
  taskStatusView,
  type TaskDraft,
  WEEKDAY_LABELS,
} from "./ScheduledTasksSettings.logic";

const HISTORY_PAGE = 20;

type DialogState = { kind: "create" } | { kind: "edit"; task: ScheduledTask } | null;

/** Lookups the rows need from the environment's projects and threads. */
function useTargets(environmentId: EnvironmentId | null) {
  const projects = useProjects();
  const threads = useThreadShells();
  return useMemo(() => {
    const envProjects = projects.filter((project) => project.environmentId === environmentId);
    const envThreads = threads.filter((thread) => thread.environmentId === environmentId);
    const projectTitle = new Map<string, string>(
      envProjects.map((project) => [project.id, project.title]),
    );
    const threadTitle = new Map<string, string>(
      envThreads.map((thread) => [thread.id, thread.title]),
    );
    return {
      projects: envProjects,
      // Spectrum transcripts never run a provider turn, so they cannot be continued.
      threads: envThreads.filter(
        (thread) => thread.archivedAt === null && !isSpectrumThreadId(thread.id),
      ),
      projectTitle: (id: string) => projectTitle.get(id),
      threadTitle: (id: string) => threadTitle.get(id),
    };
  }, [environmentId, projects, threads]);
}
type Targets = ReturnType<typeof useTargets>;

export function ScheduledTasksSettings() {
  const { scope } = useSettingsScope();
  // Tasks belong to one environment; other selections would show a stand-in environment's tasks.
  const environmentId = scope.kind === "environment" ? scope.environmentId : null;
  const list = useEnvironmentQuery(
    environmentId === null ? null : scheduledTaskList({ environmentId, input: {} }),
  );
  const targets = useTargets(environmentId);
  const [dialog, setDialog] = useState<DialogState>(null);
  const tasks = useMemo(
    () =>
      (list.data ?? []).toSorted((a, b) => a.definition.title.localeCompare(b.definition.title)),
    [list.data],
  );

  if (environmentId === null)
    return (
      <SettingsScopeNotice target="environment">
        Scheduled tasks belong to one environment. Choose the environment whose tasks to manage.
      </SettingsScopeNotice>
    );

  return (
    <SettingsPageContainer>
      <SettingsSection
        id="scheduled-tasks"
        title="Scheduled tasks"
        variant="plain"
        headerAction={
          <Button size="xs" variant="outline" onClick={() => setDialog({ kind: "create" })}>
            <PlusIcon />
            New task
          </Button>
        }
      >
        <p className="px-3 text-sm text-muted-foreground sm:px-4">
          An agent run is done only when its outcome check exits 0 and no Drafter it started is
          pending. A turn that finishes with the check still failing is continued in the same thread
          until it passes. A command run starts no agent: exit 0 passes, anything else needs you.
          Fixed times run within ±{DEFAULT_WINDOW_MINUTES} minutes by default so tasks asking for
          the same time spread out.
        </p>
        {list.error && (
          <p role="alert" className="px-3 text-sm text-destructive sm:px-4">
            {list.error}
          </p>
        )}
        {list.isSuccess && tasks.length === 0 && (
          <p className="px-3 text-sm text-muted-foreground sm:px-4">
            No scheduled tasks yet. Create one here, or ask an agent to.
          </p>
        )}
        {tasks.map((task) => (
          <TaskRow
            key={task.id}
            environmentId={environmentId}
            task={task}
            targets={targets}
            onEdit={() => setDialog({ kind: "edit", task })}
          />
        ))}
      </SettingsSection>
      {dialog !== null && (
        <TaskDialog
          environmentId={environmentId}
          task={dialog.kind === "edit" ? dialog.task : null}
          targets={targets}
          onClose={() => setDialog(null)}
        />
      )}
    </SettingsPageContainer>
  );
}

function BlockedButton({
  reason,
  children,
  ...props
}: ComponentProps<typeof Button> & { reason: string | null }) {
  if (reason === null) return <Button {...props}>{children}</Button>;
  return (
    <Tooltip>
      <TooltipTrigger render={<span tabIndex={0} className="inline-flex" />}>
        <Button {...props} disabled>
          {children}
        </Button>
      </TooltipTrigger>
      <TooltipPopup>{reason}</TooltipPopup>
    </Tooltip>
  );
}

/** Where the task runs, and for an agent task the Prism role it runs in. */
function targetLabel(task: ScheduledTask, targets: Targets) {
  const { definition } = task;
  if (isCommandTask(definition))
    return `Runs in ${targets.projectTitle(definition.projectId) ?? definition.projectId}`;
  const target = definition.target;
  const where =
    target.kind === "new-thread"
      ? `New thread in ${targets.projectTitle(target.projectId) ?? target.projectId}`
      : `Continues "${targets.threadTitle(target.threadId) ?? target.threadId}"`;
  const lane = definition.role === "worker" && definition.lane ? ` (${definition.lane})` : "";
  return `${where} · ${PRISM_ROLE_LABELS[definition.role]}${lane}`;
}

function lastRunLabel(task: ScheduledTask) {
  const run = latestRun(task);
  if (run === null) return "Never ran";
  if (isCommandTask(run.definition)) {
    const view = commandRunView(run);
    return `${view.started ?? formatSlot(run.slot)}: ${view.exit}`;
  }
  return `${formatSlot(run.slot)}, judged by check v${run.checkVersion}: ${
    run.check === null ? "no verdict yet" : run.check.passed ? "passed" : "failed"
  }`;
}

function TaskRow({
  environmentId,
  task,
  targets,
  onEdit,
}: {
  environmentId: EnvironmentId;
  task: ScheduledTaskView;
  targets: Targets;
  onEdit: () => void;
}) {
  const [open, setOpen] = useState(false);
  const [pending, setPending] = useState(false);
  const pause = useAtomCommand(scheduledTaskPause, { reportFailure: false });
  const runNow = useAtomCommand(scheduledTaskRunNow, { reportFailure: false });
  const remove = useAtomCommand(scheduledTaskDelete, { reportFailure: false });
  const status = taskStatusView(task);
  // Run now on an agent task that needs you resumes the same run, thread and check.
  const resumes = needsYou(task);
  const creator = taskCreator(task);
  const { definition } = task;

  const act = async (
    title: string,
    action: () => Promise<AtomCommandResult<unknown, unknown>>,
  ): Promise<void> => {
    setPending(true);
    const result = await action();
    setPending(false);
    if (result._tag === "Failure")
      toastManager.add({
        type: "error",
        title,
        description: formatEnvironmentQueryError(result.cause),
      });
  };
  const confirmDelete = async () => {
    const confirmed = await requestConfirmDialog(
      isCommandTask(definition)
        ? `Delete "${definition.title}"?\nIt stops running and leaves this list. Its run history stays in the audit log.`
        : needsYou(task)
          ? `Delete "${definition.title}"?\nIt leaves this list. Its unfinished run is not marked done; the run and its check history stay in the audit log. The server refuses while anything the run started is still live or pending.`
          : `Delete "${definition.title}"?\nIt stops running and leaves this list. Its check history stays in the audit log.`,
      { variant: "destructive" },
    );
    if (confirmed !== true) return;
    await act("Could not delete the task", () =>
      remove({ environmentId, input: { taskId: task.id } }),
    );
  };

  return (
    <div className="space-y-2 border-b border-border/50 px-3 py-3 last:border-b-0 sm:px-4">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <button
          type="button"
          className="flex min-w-0 flex-1 items-start gap-1.5 text-left"
          aria-expanded={open}
          onClick={() => setOpen((value) => !value)}
        >
          {open ? (
            <ChevronDownIcon className="mt-0.5 size-4 shrink-0 text-muted-foreground" />
          ) : (
            <ChevronRightIcon className="mt-0.5 size-4 shrink-0 text-muted-foreground" />
          )}
          <span className="min-w-0 space-y-0.5">
            <span className="flex flex-wrap items-center gap-2">
              <span className="text-sm font-medium">{definition.title}</span>
              <Badge variant="outline">{isCommandTask(definition) ? "Command" : "Agent"}</Badge>
              <Badge variant={status.tone}>{status.label}</Badge>
              {task.paused && status.label !== "Paused" && (
                <Badge variant="secondary">Paused</Badge>
              )}
            </span>
            <span className="block text-xs text-muted-foreground">
              {describeSchedule(definition.schedule)} · {targetLabel(task, targets)}
            </span>
          </span>
        </button>
        <div className="flex flex-wrap items-center gap-1">
          <BlockedButton
            size="xs"
            variant="outline"
            reason={runNowBlockedReason(task)}
            disabled={pending}
            onClick={() =>
              void act(resumes ? "Could not resume the run" : "Could not start a run", () =>
                runNow({ environmentId, input: { taskId: task.id } }),
              )
            }
          >
            {resumes ? "Resume run" : "Run now"}
          </BlockedButton>
          <Button
            size="xs"
            variant="outline"
            disabled={pending}
            onClick={() =>
              void act(task.paused ? "Could not resume" : "Could not pause", () =>
                pause({ environmentId, input: { taskId: task.id, paused: !task.paused } }),
              )
            }
          >
            {task.paused ? "Resume" : "Pause"}
          </Button>
          <Button size="xs" variant="outline" disabled={pending} onClick={onEdit}>
            Edit
          </Button>
          <BlockedButton
            size="xs"
            variant="ghost-destructive"
            reason={deleteBlockedReason(task)}
            disabled={pending}
            onClick={() => void confirmDelete()}
          >
            Delete
          </BlockedButton>
        </div>
      </div>
      <dl className="grid gap-x-4 gap-y-1 pl-5.5 text-xs sm:grid-cols-[auto_1fr]">
        <dt className="text-muted-foreground">Next run</dt>
        <dd>
          {task.nextRunAt !== null
            ? formatSlot(task.nextRunAt)
            : task.paused
              ? "None while paused"
              : "None scheduled"}
        </dd>
        <dt className="text-muted-foreground">Last run</dt>
        <dd>{lastRunLabel(task)}</dd>
        <dt className="text-muted-foreground">Failure streak</dt>
        <dd>{task.failureStreak}</dd>
        <dt className="text-muted-foreground">Created by</dt>
        <dd>{creator === null ? "Unknown" : describeActor(creator, targets.threadTitle)}</dd>
        {task.lastError && (
          <>
            <dt className="text-muted-foreground">Last error</dt>
            <dd className="break-words text-destructive">{task.lastError}</dd>
          </>
        )}
        {task.choices.length > 0 && (
          <>
            <dt className="text-muted-foreground">Chosen minute</dt>
            <dd>{task.choices.map(describeChoice).join("; ")}</dd>
          </>
        )}
      </dl>
      {open && <TaskDetails environmentId={environmentId} task={task} targets={targets} />}
    </div>
  );
}

function TaskDetails({
  environmentId,
  task,
  targets,
}: {
  environmentId: EnvironmentId;
  task: ScheduledTask;
  targets: Targets;
}) {
  const { definition } = task;
  const check = activeCheck(task);
  const neighbours = task.choices.flatMap((choice) => choice.neighbours);
  return (
    <div className="space-y-4 pl-5.5 text-xs">
      {isCommandTask(definition) ? (
        <section className="space-y-1">
          <h3 className="font-medium">Command</h3>
          <pre className="overflow-x-auto rounded-md bg-muted/40 p-2 font-mono">
            {definition.command}
          </pre>
          <p className="text-muted-foreground">
            Runs with /bin/sh in <span className="font-mono">{task.checkCwd}</span>. Exit 0 passes;
            anything else needs you. It stops after 30 minutes. No agent starts and nothing retries:
            the next scheduled time runs it again.
          </p>
        </section>
      ) : (
        <section className="space-y-1">
          <h3 className="font-medium">Prompt</h3>
          <p className="whitespace-pre-wrap text-muted-foreground">{definition.prompt}</p>
        </section>
      )}
      {check !== null && (
        <section className="space-y-1">
          <h3 className="font-medium">Outcome check v{check.version}</h3>
          <pre className="overflow-x-auto rounded-md bg-muted/40 p-2 font-mono">
            {check.command}
          </pre>
          <p className="text-muted-foreground">
            Runs in <span className="font-mono">{task.checkCwd}</span>. Exit 0 means done.
          </p>
        </section>
      )}
      {neighbours.length > 0 && (
        <section className="space-y-1">
          <h3 className="font-medium">Tasks near the chosen minute</h3>
          <ul className="space-y-0.5 text-muted-foreground">
            {neighbours.map((neighbour) => (
              <li key={`${neighbour.taskId}:${neighbour.chosen}`}>
                {neighbour.title} at {formatSlot(neighbour.chosen)}
              </li>
            ))}
          </ul>
        </section>
      )}
      <section className="space-y-1">
        <h3 className="font-medium">Recent runs</h3>
        {task.runs.length === 0 ? (
          <p className="text-muted-foreground">No runs yet.</p>
        ) : (
          <ul className="space-y-2">
            {task.runs.toReversed().map((run) => (
              <RunEntry key={run.id} run={run} />
            ))}
          </ul>
        )}
      </section>
      {check !== null && (
        <CheckHistory
          environmentId={environmentId}
          task={task}
          targets={targets}
          active={check.version}
        />
      )}
    </div>
  );
}

function CommandRunEntry({ run }: { run: TaskRun }) {
  const status = runStatusView(run);
  const view = commandRunView(run);
  return (
    <li className="space-y-1 rounded-md border border-border/50 p-2">
      <div className="flex flex-wrap items-center gap-2">
        <span>{formatSlot(run.slot)}</span>
        <Badge variant={status.tone}>{status.label}</Badge>
        <span className="text-muted-foreground">{view.exit}</span>
      </div>
      <p className="text-muted-foreground">
        Started {view.started ?? "unknown"}
        {view.ended !== null ? ` · ended ${view.ended}` : ""}
      </p>
      {run.error && <p className="break-words text-destructive">{run.error}</p>}
      {view.output !== null ? (
        <details>
          <summary className="cursor-pointer text-muted-foreground">Output (last 4 KiB)</summary>
          <pre className="mt-1 max-h-48 overflow-auto rounded-md bg-muted/40 p-2 font-mono whitespace-pre-wrap">
            {view.output || "(no output)"}
          </pre>
        </details>
      ) : (
        view.outputNote !== null && (
          <p className="text-muted-foreground">Output: {view.outputNote}</p>
        )
      )}
    </li>
  );
}

function RunEntry({ run }: { run: TaskRun }) {
  if (isCommandTask(run.definition)) return <CommandRunEntry run={run} />;
  const status = runStatusView(run);
  return (
    <li className="space-y-1 rounded-md border border-border/50 p-2">
      <div className="flex flex-wrap items-center gap-2">
        <span>{formatSlot(run.slot)}</span>
        <Badge variant={status.tone}>{status.label}</Badge>
        <span className="text-muted-foreground">
          check v{run.checkVersion}
          {run.attempt > 0 ? ` · attempt ${run.attempt + 1}` : ""}
          {run.drafterIds.length > 0 ? ` · ${run.drafterIds.length} Drafters` : ""}
        </span>
      </div>
      {run.error && <p className="break-words text-destructive">{run.error}</p>}
      {run.check && (
        <details>
          <summary className="cursor-pointer text-muted-foreground">
            Check {run.check.passed ? "passed" : "failed"} at {formatSlot(run.check.checkedAt)}
          </summary>
          <pre className="mt-1 max-h-48 overflow-auto rounded-md bg-muted/40 p-2 font-mono whitespace-pre-wrap">
            {run.check.output || "(no output)"}
          </pre>
        </details>
      )}
    </li>
  );
}

function CheckHistory({
  environmentId,
  task,
  targets,
  active,
}: {
  environmentId: EnvironmentId;
  task: ScheduledTask;
  targets: Targets;
  active: number;
}) {
  const readHistory = useAtomCommand(scheduledTaskCheckHistory, { reportFailure: false });
  const [older, setOlder] = useState<ReadonlyArray<TaskCheckVersion>>([]);
  const [exhausted, setExhausted] = useState(false);
  const [loading, setLoading] = useState(false);
  const [reverting, setReverting] = useState<TaskCheckVersion | null>(null);
  const shown = useMemo(() => {
    const byVersion = new Map([...task.checks, ...older].map((check) => [check.version, check]));
    return [...byVersion.values()].toSorted((a, b) => b.version - a.version);
  }, [older, task.checks]);
  const oldest = shown.at(-1)?.version ?? 1;

  const loadOlder = async () => {
    setLoading(true);
    const result = await readHistory({
      environmentId,
      input: { taskId: task.id, beforeVersion: oldest, limit: HISTORY_PAGE },
    });
    setLoading(false);
    if (result._tag === "Failure") {
      toastManager.add({
        type: "error",
        title: "Could not read older check versions",
        description: formatEnvironmentQueryError(result.cause),
      });
      return;
    }
    setOlder((current) => [...current, ...result.value]);
    if (result.value.length < HISTORY_PAGE) setExhausted(true);
  };

  return (
    <section className="space-y-1">
      <h3 className="font-medium">Check history</h3>
      <p className="text-muted-foreground">
        Versions are never edited. A change or revert adds a version that applies to future runs; a
        running run keeps the check it started with.
      </p>
      <ul className="space-y-2">
        {shown.map((version) => (
          <li key={version.version} className="space-y-1 rounded-md border border-border/50 p-2">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <span>
                v{version.version}
                {version.version === active ? " (active)" : ""}
                {version.revertedFrom !== null ? `, revert of v${version.revertedFrom}` : ""} ·{" "}
                {describeActor(version.actor, targets.threadTitle)} ·{" "}
                {formatSlot(version.createdAt)}
              </span>
              {version.version !== active && (
                <Button size="xs" variant="outline" onClick={() => setReverting(version)}>
                  Revert to this
                </Button>
              )}
            </div>
            <pre className="overflow-x-auto rounded-md bg-muted/40 p-2 font-mono">
              {version.command}
            </pre>
            <p className="text-muted-foreground">{version.reason}</p>
          </li>
        ))}
      </ul>
      {oldest > 1 && !exhausted && (
        <Button size="xs" variant="ghost" disabled={loading} onClick={() => void loadOlder()}>
          Load older versions
        </Button>
      )}
      {reverting && (
        <RevertDialog
          environmentId={environmentId}
          task={task}
          version={reverting}
          onClose={() => setReverting(null)}
        />
      )}
    </section>
  );
}

function RevertDialog({
  environmentId,
  task,
  version,
  onClose,
}: {
  environmentId: EnvironmentId;
  task: ScheduledTask;
  version: TaskCheckVersion;
  onClose: () => void;
}) {
  const edit = useAtomCommand(scheduledTaskEdit, { reportFailure: false });
  const [reason, setReason] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState(false);
  const submit = async () => {
    if (reason.trim() === "") {
      setError("Say why you revert. The reason stays in the history.");
      return;
    }
    setPending(true);
    const result = await edit({
      environmentId,
      input: { taskId: task.id, revertVersion: version.version, checkReason: reason.trim() },
    });
    setPending(false);
    if (result._tag === "Failure") {
      setError(formatEnvironmentQueryError(result.cause));
      return;
    }
    onClose();
  };
  return (
    <Dialog open onOpenChange={(open) => (open ? undefined : onClose())}>
      <DialogPopup>
        <DialogHeader>
          <DialogTitle>Revert to check v{version.version}</DialogTitle>
          <DialogDescription>
            Adds a new version with this command. It applies to future runs only.
          </DialogDescription>
        </DialogHeader>
        <DialogPanel>
          <div className="space-y-3">
            <pre className="overflow-x-auto rounded-md bg-muted/40 p-2 font-mono text-xs">
              {version.command}
            </pre>
            <div className="space-y-1">
              <Label htmlFor="scheduled-task-revert-reason">Reason</Label>
              <Textarea
                id="scheduled-task-revert-reason"
                value={reason}
                onChange={(event) => setReason(event.target.value)}
              />
            </div>
            {error && (
              <p role="alert" className="text-sm text-destructive">
                {error}
              </p>
            )}
          </div>
        </DialogPanel>
        <DialogFooter>
          <Button variant="outline" onClick={onClose}>
            Cancel
          </Button>
          <Button disabled={pending} onClick={() => void submit()}>
            Revert
          </Button>
        </DialogFooter>
      </DialogPopup>
    </Dialog>
  );
}

const browserTimeZone = () => Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC";

function Field({
  id,
  label,
  help,
  children,
}: {
  id: string;
  label: string;
  help?: string;
  children: ReactNode;
}) {
  return (
    <div className="space-y-1">
      <Label htmlFor={id}>{label}</Label>
      {children}
      {help && <p className="text-xs text-muted-foreground">{help}</p>}
    </div>
  );
}

function TaskDialog({
  environmentId,
  task,
  targets,
  onClose,
}: {
  environmentId: EnvironmentId;
  task: ScheduledTask | null;
  targets: Targets;
  onClose: () => void;
}) {
  const create = useAtomCommand(scheduledTaskCreate, { reportFailure: false });
  const edit = useAtomCommand(scheduledTaskEdit, { reportFailure: false });
  const [draft, setDraft] = useState<TaskDraft>(() =>
    task ? draftFromTask(task) : emptyTaskDraft(browserTimeZone()),
  );
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState(false);
  const set = <K extends keyof TaskDraft>(key: K, value: TaskDraft[K]) =>
    setDraft((current) => ({ ...current, [key]: value }));

  const submit = async () => {
    setError(null);
    if (task === null) {
      const payload = createPayloadFromDraft(draft);
      if (!payload.ok) return setError(payload.error);
      setPending(true);
      const result = await create({ environmentId, input: payload.value });
      setPending(false);
      if (result._tag === "Failure") return setError(formatEnvironmentQueryError(result.cause));
      announcePlacement(result.value);
      return onClose();
    }
    const payload = editPayloadFromDraft(task, draft);
    if (!payload.ok) return setError(payload.error);
    if (payload.value === null) return onClose();
    setPending(true);
    const result = await edit({ environmentId, input: payload.value });
    setPending(false);
    if (result._tag === "Failure") return setError(formatEnvironmentQueryError(result.cause));
    if (payload.value.definition) announcePlacement(result.value);
    onClose();
  };

  return (
    <Dialog open onOpenChange={(open) => (open ? undefined : onClose())}>
      <DialogPopup className="max-w-xl">
        <DialogHeader>
          <DialogTitle>{task ? "Edit scheduled task" : "New scheduled task"}</DialogTitle>
          <DialogDescription>
            {draft.kind === "command"
              ? "Runs a shell command on the schedule, without an agent. Exit 0 passes; anything else needs you until a later run passes."
              : "A run is done only when the outcome check exits 0. Until then the scheduler keeps the work going in the same thread."}
          </DialogDescription>
        </DialogHeader>
        <DialogPanel>
          <div className="space-y-4">
            {task === null && (
              <div className="space-y-1">
                <Label>Kind</Label>
                <ToggleGroup
                  aria-label="Kind"
                  value={[draft.kind]}
                  onValueChange={(next) => {
                    if (next[0] === "agent" || next[0] === "command") set("kind", next[0]);
                  }}
                >
                  <Toggle value="agent">Agent</Toggle>
                  <Toggle value="command">Command</Toggle>
                </ToggleGroup>
              </div>
            )}
            <Field id="scheduled-task-title" label="Title">
              <Input
                id="scheduled-task-title"
                value={draft.title}
                onChange={(event) => set("title", event.target.value)}
              />
            </Field>
            {draft.kind === "command" ? (
              <CommandFields draft={draft} set={set} targets={targets} />
            ) : (
              <AgentFields draft={draft} set={set} targets={targets} />
            )}
            <ScheduleFields draft={draft} set={set} />
            {draft.kind === "agent" && <CheckFields draft={draft} set={set} task={task} />}
            {error && (
              <p role="alert" className="text-sm text-destructive">
                {error}
              </p>
            )}
          </div>
        </DialogPanel>
        <DialogFooter>
          <Button variant="outline" onClick={onClose}>
            Cancel
          </Button>
          <Button disabled={pending} onClick={() => void submit()}>
            {task ? "Save" : "Create task"}
          </Button>
        </DialogFooter>
      </DialogPopup>
    </Dialog>
  );
}

function AgentFields({
  draft,
  set,
  targets,
}: {
  draft: TaskDraft;
  set: SetDraft;
  targets: Targets;
}) {
  return (
    <>
      <Field id="scheduled-task-prompt" label="Prompt">
        <Textarea
          id="scheduled-task-prompt"
          value={draft.prompt}
          onChange={(event) => set("prompt", event.target.value)}
        />
      </Field>
      <TargetFields draft={draft} set={set} targets={targets} />
      <div className="flex flex-wrap gap-4">
        <Field id="scheduled-task-role" label="Prism role">
          <Select
            value={draft.role}
            onValueChange={(value) => {
              const role = PRISM_ROLES.find((candidate) => candidate === value);
              if (role) set("role", role);
            }}
          >
            <SelectTrigger id="scheduled-task-role" size="sm" className="w-44">
              <SelectValue />
            </SelectTrigger>
            <SelectPopup>
              {PRISM_ROLES.map((role) => (
                <SelectItem key={role} value={role}>
                  {PRISM_ROLE_LABELS[role]}
                </SelectItem>
              ))}
            </SelectPopup>
          </Select>
        </Field>
        {draft.role === "worker" && (
          <div className="space-y-1">
            <Label>Worker lane</Label>
            <ToggleGroup
              aria-label="Worker lane"
              value={[draft.lane]}
              onValueChange={(next) => {
                const lane = PrismLane.literals.find((candidate) => candidate === next[0]);
                if (lane) set("lane", lane);
              }}
            >
              {PrismLane.literals.map((lane) => (
                <Toggle key={lane} value={lane}>
                  {lane[0]!.toUpperCase() + lane.slice(1)}
                </Toggle>
              ))}
            </ToggleGroup>
          </div>
        )}
      </div>
    </>
  );
}

function CommandFields({
  draft,
  set,
  targets,
}: {
  draft: TaskDraft;
  set: SetDraft;
  targets: Targets;
}) {
  return (
    <>
      <div className="space-y-1">
        <Label>Project</Label>
        <ProjectSelect draft={draft} set={set} targets={targets} />
      </div>
      <Field
        id="scheduled-task-command"
        label="Command"
        help="Runs with /bin/sh in the project root, with no input, and stops after 30 minutes. Keep it in the foreground. You can use {date}, {run_id} and {task_id}."
      >
        <Textarea
          id="scheduled-task-command"
          placeholder="./scripts/backup.sh"
          value={draft.command}
          onChange={(event) => set("command", event.target.value)}
        />
      </Field>
    </>
  );
}

/** Tells where the server placed each fixed time and which tasks already run near it. */
function announcePlacement(task: ScheduledTask) {
  if (task.choices.length === 0) return;
  const neighbours = task.choices.flatMap((choice) => choice.neighbours);
  toastManager.add({
    type: "success",
    title: `"${task.definition.title}" runs at ${task.choices.map(describeChoice).join("; ")}`,
    description:
      neighbours.length === 0
        ? "No other tasks run near that minute."
        : `Nearby: ${neighbours.map((n) => `${n.title} at ${formatSlot(n.chosen)}`).join(", ")}`,
  });
}

type SetDraft = <K extends keyof TaskDraft>(key: K, value: TaskDraft[K]) => void;

function TargetFields({
  draft,
  set,
  targets,
}: {
  draft: TaskDraft;
  set: SetDraft;
  targets: Targets;
}) {
  return (
    <div className="space-y-2">
      <Label>Target</Label>
      <ToggleGroup
        aria-label="Target"
        value={[draft.targetKind]}
        onValueChange={(next) => {
          if (next[0] === "new-thread" || next[0] === "thread") set("targetKind", next[0]);
        }}
      >
        <Toggle value="new-thread">New thread in a project</Toggle>
        <Toggle value="thread">Existing thread</Toggle>
      </ToggleGroup>
      {draft.targetKind === "new-thread" ? (
        <ProjectSelect draft={draft} set={set} targets={targets} />
      ) : (
        <Select
          value={draft.threadId === "" ? null : draft.threadId}
          onValueChange={(value) => set("threadId", value ?? "")}
        >
          <SelectTrigger size="sm" aria-label="Thread">
            <SelectValue placeholder="Choose a thread" />
          </SelectTrigger>
          <SelectPopup>
            {targets.threads.map((thread) => (
              <SelectItem key={thread.id} value={thread.id}>
                {thread.title} ({targets.projectTitle(thread.projectId) ?? thread.projectId})
              </SelectItem>
            ))}
          </SelectPopup>
        </Select>
      )}
    </div>
  );
}

function ProjectSelect({
  draft,
  set,
  targets,
}: {
  draft: TaskDraft;
  set: SetDraft;
  targets: Targets;
}) {
  return (
    <Select
      value={draft.projectId === "" ? null : draft.projectId}
      onValueChange={(value) => set("projectId", value ?? "")}
    >
      <SelectTrigger size="sm" aria-label="Project">
        <SelectValue placeholder="Choose a project" />
      </SelectTrigger>
      <SelectPopup>
        {targets.projects.map((project) => (
          <SelectItem key={project.id} value={project.id}>
            {project.title}
          </SelectItem>
        ))}
      </SelectPopup>
    </Select>
  );
}

function ScheduleFields({ draft, set }: { draft: TaskDraft; set: SetDraft }) {
  return (
    <div className="space-y-3">
      <div className="space-y-1">
        <Label>Schedule</Label>
        <ToggleGroup
          aria-label="Schedule"
          value={[draft.scheduleKind]}
          onValueChange={(next) => {
            if (next[0] === "interval" || next[0] === "weekly" || next[0] === "once")
              set("scheduleKind", next[0]);
          }}
        >
          <Toggle value="weekly">Set times</Toggle>
          <Toggle value="interval">Every N minutes</Toggle>
          <Toggle value="once">Once</Toggle>
        </ToggleGroup>
      </div>
      {draft.scheduleKind === "interval" && (
        <Field id="scheduled-task-interval" label="Every (minutes)">
          <Input
            id="scheduled-task-interval"
            inputMode="numeric"
            value={draft.intervalMinutes}
            onChange={(event) => set("intervalMinutes", event.target.value)}
          />
        </Field>
      )}
      {draft.scheduleKind === "weekly" && (
        <>
          <div className="space-y-1">
            <Label>Weekdays</Label>
            <ToggleGroup
              aria-label="Weekdays"
              multiple
              value={draft.weekdays.map(String)}
              onValueChange={(next) => set("weekdays", next.map(Number))}
            >
              {WEEKDAY_LABELS.map((label, day) => (
                <Toggle key={label} value={String(day)}>
                  {label}
                </Toggle>
              ))}
            </ToggleGroup>
          </div>
          <div className="flex flex-wrap gap-4">
            <Field
              id="scheduled-task-times"
              label="Times (24-hour)"
              help="Separate several times with commas, such as 08:00, 17:30."
            >
              <Input
                id="scheduled-task-times"
                value={draft.times}
                onChange={(event) => set("times", event.target.value)}
              />
            </Field>
            <Field id="scheduled-task-time-zone" label="Time zone">
              <Input
                id="scheduled-task-time-zone"
                value={draft.timeZone}
                onChange={(event) => set("timeZone", event.target.value)}
              />
            </Field>
          </div>
        </>
      )}
      {draft.scheduleKind === "once" && (
        <Field id="scheduled-task-once" label="Run once at">
          <Input
            id="scheduled-task-once"
            type="datetime-local"
            value={draft.onceAt}
            onChange={(event) => set("onceAt", event.target.value)}
          />
        </Field>
      )}
      {draft.scheduleKind !== "interval" && (
        <Field
          id="scheduled-task-window"
          label="Window (minutes either side)"
          help={`The server picks the least busy minute within this many minutes before or after each time, so tasks asking for the same time spread out. Default ${DEFAULT_WINDOW_MINUTES}; 0 runs at the exact minute.`}
        >
          <Input
            id="scheduled-task-window"
            inputMode="numeric"
            value={draft.windowMinutes}
            onChange={(event) => set("windowMinutes", event.target.value)}
          />
        </Field>
      )}
    </div>
  );
}

function CheckFields({
  draft,
  set,
  task,
}: {
  draft: TaskDraft;
  set: SetDraft;
  task: ScheduledTask | null;
}) {
  const current = task === null ? null : activeCheck(task);
  return (
    <div className="space-y-3">
      <Field
        id="scheduled-task-check"
        label={current ? `New outcome check (optional, now v${current.version})` : "Outcome check"}
        help={
          current
            ? "Leave empty to keep the current check. A new check applies to future runs only; a running run keeps its check."
            : "A shell command the server runs after each turn, in the project root for a new thread, or the thread's worktree (the project root when it has none). Exit 0 means done. You can use {date}, {run_id} and {task_id}. It must fail now: a check that already passes is refused."
        }
      >
        <Textarea
          id="scheduled-task-check"
          placeholder={current ? current.command : "test -f reports/{date}.md"}
          value={draft.checkCommand}
          onChange={(event) => set("checkCommand", event.target.value)}
        />
      </Field>
      <Field
        id="scheduled-task-check-reason"
        label="Why this check proves the work is done"
        help="Kept with the check version in the task's history."
      >
        <Input
          id="scheduled-task-check-reason"
          value={draft.checkReason}
          onChange={(event) => set("checkReason", event.target.value)}
        />
      </Field>
    </div>
  );
}
