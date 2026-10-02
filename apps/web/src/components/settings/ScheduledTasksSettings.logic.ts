import {
  DEFAULT_PRISM_LANE,
  type CreateScheduledTask,
  type EditScheduledTask,
  type PrismLane,
  type PrismRole,
  type ProjectId,
  type ScheduledTask,
  type TaskDefinition,
  type TaskMinuteChoice,
  type TaskRun,
  type TaskSchedule,
  type ThreadId,
} from "@t3tools/contracts";

/** Fixed times move up to this many minutes either way unless the window says otherwise. */
export const DEFAULT_WINDOW_MINUTES = 30;
export const WEEKDAY_LABELS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"] as const;

export type ScheduleKind = TaskSchedule["kind"];

/** The create/edit form's editable text, before it becomes a task definition. */
export interface TaskDraft {
  readonly title: string;
  readonly prompt: string;
  readonly targetKind: "new-thread" | "thread";
  readonly projectId: string;
  readonly threadId: string;
  readonly role: PrismRole;
  readonly lane: PrismLane;
  readonly scheduleKind: ScheduleKind;
  readonly intervalMinutes: string;
  readonly weekdays: ReadonlyArray<number>;
  readonly times: string;
  readonly timeZone: string;
  /** `datetime-local` value, read in the browser's time zone. */
  readonly onceAt: string;
  readonly windowMinutes: string;
  readonly checkCommand: string;
  readonly checkReason: string;
}

export function emptyTaskDraft(timeZone: string): TaskDraft {
  return {
    title: "",
    prompt: "",
    targetKind: "new-thread",
    projectId: "",
    threadId: "",
    role: "worker",
    lane: DEFAULT_PRISM_LANE,
    scheduleKind: "weekly",
    intervalMinutes: "60",
    weekdays: [1, 2, 3, 4, 5],
    times: "08:00",
    timeZone,
    onceAt: "",
    windowMinutes: String(DEFAULT_WINDOW_MINUTES),
    checkCommand: "",
    checkReason: "",
  };
}

const pad = (value: number) => String(value).padStart(2, "0");

/** A `datetime-local` value for an instant, in the browser's time zone. */
function toLocalDateTimeInput(iso: string): string {
  const date = new Date(iso);
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

/** The form for an existing task. Check fields start empty: a check change is opt-in. */
export function draftFromTask(task: ScheduledTask): TaskDraft {
  const { definition } = task;
  const schedule = definition.schedule;
  const base = emptyTaskDraft(schedule.kind === "weekly" ? schedule.timeZone : "UTC");
  return {
    ...base,
    title: definition.title,
    prompt: definition.prompt,
    targetKind: definition.target.kind,
    projectId: definition.target.kind === "new-thread" ? definition.target.projectId : "",
    threadId: definition.target.kind === "thread" ? definition.target.threadId : "",
    role: definition.role,
    lane: definition.lane ?? DEFAULT_PRISM_LANE,
    scheduleKind: schedule.kind,
    ...(schedule.kind === "interval" ? { intervalMinutes: String(schedule.minutes) } : {}),
    ...(schedule.kind === "weekly"
      ? { weekdays: schedule.weekdays, times: schedule.times.join(", ") }
      : {}),
    ...(schedule.kind === "once" ? { onceAt: toLocalDateTimeInput(schedule.at) } : {}),
    windowMinutes: String(
      schedule.kind === "interval"
        ? DEFAULT_WINDOW_MINUTES
        : (schedule.windowMinutes ?? DEFAULT_WINDOW_MINUTES),
    ),
  };
}

type Parsed<A> =
  | { readonly ok: true; readonly value: A }
  | { readonly ok: false; readonly error: string };

const parseWholeNumber = (text: string, min: number, max: number) => {
  const trimmed = text.trim();
  if (!/^\d+$/.test(trimmed)) return null;
  const value = Number(trimmed);
  return value >= min && value <= max ? value : null;
};

function scheduleFromDraft(draft: TaskDraft): Parsed<TaskSchedule> {
  if (draft.scheduleKind === "interval") {
    const minutes = parseWholeNumber(draft.intervalMinutes, 1, Number.MAX_SAFE_INTEGER);
    return minutes === null
      ? { ok: false, error: "Interval must be a whole number of minutes, at least 1." }
      : { ok: true, value: { kind: "interval", minutes } };
  }
  const windowMinutes = parseWholeNumber(draft.windowMinutes, 0, 720);
  if (windowMinutes === null)
    return { ok: false, error: "Window must be 0 (exact) to 720 minutes." };
  if (draft.scheduleKind === "once") {
    const at = new Date(draft.onceAt);
    if (draft.onceAt.trim() === "" || Number.isNaN(at.getTime()))
      return { ok: false, error: "Choose the date and time to run once." };
    return { ok: true, value: { kind: "once", at: at.toISOString(), windowMinutes } };
  }
  const times = [
    ...new Set(
      draft.times
        .split(/[\s,]+/)
        .filter(Boolean)
        .map((time) => (/^\d:\d\d$/.test(time) ? `0${time}` : time)),
    ),
  ];
  if (times.length === 0 || !times.every((time) => /^([01]\d|2[0-3]):[0-5]\d$/.test(time)))
    return { ok: false, error: "Times must be 24-hour HH:MM, separated by commas." };
  if (draft.weekdays.length === 0) return { ok: false, error: "Choose at least one weekday." };
  if (draft.timeZone.trim() === "") return { ok: false, error: "Time zone is required." };
  return {
    ok: true,
    value: {
      kind: "weekly",
      weekdays: [...draft.weekdays].toSorted((a, b) => a - b),
      times,
      timeZone: draft.timeZone.trim(),
      windowMinutes,
    },
  };
}

function definitionFromDraft(draft: TaskDraft): Parsed<TaskDefinition> {
  const title = draft.title.trim();
  const prompt = draft.prompt.trim();
  if (title === "") return { ok: false, error: "Title is required." };
  if (prompt === "") return { ok: false, error: "Prompt is required." };
  if (draft.targetKind === "new-thread" && draft.projectId === "")
    return { ok: false, error: "Choose the project new threads start in." };
  if (draft.targetKind === "thread" && draft.threadId === "")
    return { ok: false, error: "Choose the thread to continue." };
  const schedule = scheduleFromDraft(draft);
  if (!schedule.ok) return schedule;
  return {
    ok: true,
    value: {
      title,
      prompt,
      target:
        draft.targetKind === "new-thread"
          ? { kind: "new-thread", projectId: draft.projectId as ProjectId }
          : { kind: "thread", threadId: draft.threadId as ThreadId },
      role: draft.role,
      // Prism keeps one model list per lane only for the worker.
      ...(draft.role === "worker" ? { lane: draft.lane } : {}),
      schedule: schedule.value,
    },
  };
}

export function createPayloadFromDraft(draft: TaskDraft): Parsed<CreateScheduledTask> {
  const definition = definitionFromDraft(draft);
  if (!definition.ok) return definition;
  const checkCommand = draft.checkCommand.trim();
  const checkReason = draft.checkReason.trim();
  if (checkCommand === "") return { ok: false, error: "An outcome check is required." };
  if (checkReason === "")
    return { ok: false, error: "Say why this check proves the work is done." };
  return { ok: true, value: { ...definition.value, checkCommand, checkReason } };
}

/**
 * The smallest edit for the form. The definition is sent only when it changed, because the
 * server picks new minutes for a changed definition. A check change applies to future runs.
 */
export function editPayloadFromDraft(
  task: ScheduledTask,
  draft: TaskDraft,
): Parsed<EditScheduledTask | null> {
  const definition = definitionFromDraft(draft);
  if (!definition.ok) return definition;
  const before = definitionFromDraft(draftFromTask(task));
  const definitionChanged =
    !before.ok || JSON.stringify(before.value) !== JSON.stringify(definition.value);
  const checkCommand = draft.checkCommand.trim();
  const checkReason = draft.checkReason.trim();
  const checkChanged = checkCommand !== "" && checkCommand !== activeCheck(task).command;
  if (checkChanged && checkReason === "")
    return { ok: false, error: "Say why the check changes. It applies to future runs only." };
  if (!definitionChanged && !checkChanged) return { ok: true, value: null };
  return {
    ok: true,
    value: {
      taskId: task.id,
      ...(definitionChanged ? { definition: definition.value } : {}),
      ...(checkChanged ? { checkCommand, checkReason } : {}),
    },
  };
}

export const activeCheck = (task: ScheduledTask) => task.checks.at(-1)!;
export const latestRun = (task: ScheduledTask): TaskRun | null => task.runs.at(-1) ?? null;
/** Only a verified run is settled. A needs-you run stays unfinished and keeps its thread and check. */
const unfinishedRun = (task: ScheduledTask): TaskRun | null =>
  task.runs.find((run) => run.status !== "done") ?? null;

/** The task waits on the user: its retries ran out and nothing runs until someone acts. */
export const needsYou = (task: ScheduledTask) => unfinishedRun(task)?.status === "needs-you";

/**
 * Why Run now is unavailable, matching the server's refusals; null when allowed. On a task
 * that needs you it resumes the same pinned run. The server still refuses a retired thread.
 */
export function runNowBlockedReason(task: ScheduledTask): string | null {
  if (task.paused) return "Resume this task before running it now.";
  const run = unfinishedRun(task);
  if (run !== null && run.status !== "needs-you") return "A run is still in progress.";
  return null;
}

/**
 * Why Delete is unavailable; null when the server may accept it. Work in progress always
 * refuses. A task that needs you can go only when nothing it started is still live or pending,
 * which only the server can see, so the request is left to the server.
 */
export function deleteBlockedReason(task: ScheduledTask): string | null {
  const run = unfinishedRun(task);
  return run !== null && run.status !== "needs-you"
    ? "A run is still in progress. Pause the task and wait until it is done or needs you."
    : null;
}

export type StatusTone = "success" | "warning" | "error" | "info" | "secondary";

export interface RunStatusView {
  readonly label: string;
  readonly tone: StatusTone;
}

/**
 * One run's state in words. A finished turn is not done: only a passing check makes a run
 * done, and a failing check keeps the work going in the same thread.
 */
export function runStatusView(run: TaskRun): RunStatusView {
  switch (run.status) {
    case "done":
      return { label: "Verified done", tone: "success" };
    case "needs-you":
      return { label: "Needs you", tone: "error" };
    case "usage-limit":
      return { label: "Waiting for usage reset", tone: "info" };
    case "retry":
      return { label: `Retrying (attempt ${run.attempt + 1})`, tone: "warning" };
    case "claimed":
      return { label: "Starting", tone: "info" };
    case "running":
      return run.check && !run.check.passed
        ? { label: "Turn finished, check failing: continuing", tone: "warning" }
        : { label: "Working", tone: "info" };
  }
}

/** The task's headline state for its row. Pause never hides unfinished work or a needs-you mark. */
export function taskStatusView(task: ScheduledTask): RunStatusView {
  const run = latestRun(task);
  if (task.paused && (run === null || run.status === "done"))
    return { label: "Paused", tone: "secondary" };
  if (run === null) return { label: "No runs yet", tone: "secondary" };
  return runStatusView(run);
}

const plural = (count: number, unit: string) => `${count} ${unit}${count === 1 ? "" : "s"}`;

function formatInterval(minutes: number): string {
  if (minutes % 1440 === 0) return `Every ${plural(minutes / 1440, "day")}`;
  if (minutes % 60 === 0) return `Every ${plural(minutes / 60, "hour")}`;
  return `Every ${plural(minutes, "minute")}`;
}

function formatWindow(windowMinutes: number | undefined): string {
  const window = windowMinutes ?? DEFAULT_WINDOW_MINUTES;
  return window === 0 ? "exact minute" : `within ±${window} min`;
}

const isClockTime = (value: string) => /^\d\d:\d\d$/.test(value);

/** A clock time as is; an instant in the reader's locale. */
export function formatSlot(value: string): string {
  if (isClockTime(value)) return value;
  const date = new Date(value);
  return Number.isNaN(date.getTime())
    ? value
    : date.toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" });
}

export function describeSchedule(schedule: TaskSchedule): string {
  switch (schedule.kind) {
    case "interval":
      return formatInterval(schedule.minutes);
    case "once":
      return `Once at ${formatSlot(schedule.at)}, ${formatWindow(schedule.windowMinutes)}`;
    case "weekly": {
      const days =
        schedule.weekdays.length === 7
          ? "Every day"
          : [...schedule.weekdays]
              .toSorted((a, b) => a - b)
              .map((day) => WEEKDAY_LABELS[day])
              .join(", ");
      return `${days} at ${schedule.times.join(", ")} (${schedule.timeZone}), ${formatWindow(schedule.windowMinutes)}`;
    }
  }
}

/** "08:12 (asked 08:00, +12 min)" for each fixed time the server placed. */
export function describeChoice(choice: TaskMinuteChoice): string {
  const offset = choice.offsetMinutes;
  const moved =
    offset === 0
      ? "as asked"
      : `asked ${formatSlot(choice.requested)}, ${offset > 0 ? "+" : ""}${offset} min`;
  return `${formatSlot(choice.chosen)} (${moved})`;
}

/** Who changed a check, from the server's actor id: `user:<subject>` or the agent's thread id. */
export function describeActor(actor: string, threadTitle: (id: string) => string | undefined) {
  if (actor.startsWith("user:")) return `User (${actor.slice("user:".length)})`;
  const title = threadTitle(actor);
  return title ? `Agent in "${title}"` : `Agent thread ${actor}`;
}
