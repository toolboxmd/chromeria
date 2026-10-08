import {
  isForkScheduledTaskSchedule,
  OrchestrationV2RunJson,
  ScheduledTaskError,
  ScheduledTaskId,
  type ModelSelection,
  type ScheduledTask,
  type ScheduledTaskOutcomeCheckUpdate,
  type ScheduledTaskSchedule,
  type ScheduledTaskUpsertInput,
  type ThreadId,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import { isSqlError, type SqlError } from "effect/sql/SqlError";

import { isThreadRetired } from "../childThreads/retirement.ts";
import { ProviderAdapterRegistryV2 } from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Prism from "../prism/PrismService.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as ThreadLaunchService from "../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import { ProcessRunner } from "../processRunner.ts";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import { nextScheduledRunAt } from "../scheduledTasks/Schedule.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import { COMMAND_TIMEOUT_MS, runShellCommand } from "./commandRunner.ts";
import { ScheduledTaskDispatchPolicy } from "./DispatchPolicy.ts";
import { makeCheckedRuns, ScheduledTaskCheckError, type CheckedRuns } from "./engine.ts";
import { followSend, reportsFence, ScheduledTaskSpectra } from "./handoff.ts";
import { chooseWeeklyMinutes, forkSameSchedule, occupiedMinutes } from "./schedules.ts";
import {
  CHECK_OUTPUT_BYTES,
  checkedRunStatus,
  commandSummary,
  outcomeCheckSummary,
  unfinishedRun,
  type CheckState,
} from "./state.ts";
import { ensureCheckSchema, listCheckStates } from "./store.ts";

/** The fork summaries an agent sees on a listed task. */
export const forkSummaryFields = (task: ScheduledTask) => ({
  ...(task.outcomeCheck === undefined ? {} : { outcomeCheck: task.outcomeCheck }),
  ...(task.command === undefined ? {} : { command: task.command }),
});

/** Whether an agent-tool call sets any fork field. */
export const hasOutcomeCheckFields = (fields: ScheduledTaskOutcomeCheckUpdate) =>
  fields.checkCommand !== undefined ||
  fields.checkReason !== undefined ||
  fields.revertCheckVersion !== undefined ||
  fields.role !== undefined ||
  fields.lane !== undefined ||
  fields.command !== undefined;

/** The calling agent's thread, as upstream's tool handlers load it. */
type Caller = { readonly thread: { readonly id: ThreadId } } | undefined;

/**
 * The hooks upstream's agent tools call (`schedule_task`, `update_scheduled_task`,
 * `delete_scheduled_task`), so outcome checks and command tasks never need
 * logic inside the tool handlers.
 */
export class ScheduledTaskChecks extends Context.Service<
  ScheduledTaskChecks,
  {
    /**
     * Wraps `schedule_task`'s save in place: a new task's fork fields and its
     * upstream row commit in one transaction, so the task never fires without
     * them and a failed save leaves neither.
     */
    readonly schedule: <A, E, R>(
      hook: {
        readonly input: ScheduledTaskOutcomeCheckUpdate & {
          readonly schedule: { readonly type: ScheduledTaskSchedule["type"] };
          readonly clientRequestId?: string | undefined;
        };
        readonly projectId: ScheduledTask["projectId"];
        readonly parent: Caller;
        readonly bindToCurrentThread: boolean;
        readonly scope: { readonly requestNamespace: string };
      },
      save: (input: ScheduledTaskUpsertInput) => Effect.Effect<A, E, R>,
    ) => (input: ScheduledTaskUpsertInput) => Effect.Effect<A, E | ScheduledTaskError, R>;
    /**
     * Wraps `update_scheduled_task`'s save the same way, under the task's lock
     * so no fire sees the new fork fields with the old definition. Refuses a
     * judged thread changing its own task.
     */
    readonly update: <A, E, R>(
      hook: {
        readonly input: ScheduledTaskOutcomeCheckUpdate & {
          readonly schedule?: { readonly type: ScheduledTaskSchedule["type"] } | undefined;
        };
        readonly existing: ScheduledTask;
        readonly threadId: ThreadId | null;
        readonly parent: Caller;
      },
      save: (input: ScheduledTaskUpsertInput) => Effect.Effect<A, E, R>,
    ) => (input: ScheduledTaskUpsertInput) => Effect.Effect<A, E | ScheduledTaskError, R>;
    /** Wraps `delete_scheduled_task`'s delete: a judged thread cannot delete its own task. */
    readonly delete: <I, A, E, R>(
      hook: { readonly existing: ScheduledTask; readonly parent: Caller },
      remove: (input: I) => Effect.Effect<A, E, R>,
    ) => (input: I) => Effect.Effect<A, E | ScheduledTaskError, R>;
    /** Advances every unfinished fork run one step; the shared scheduler runs it each tick. */
    readonly reconcile: Effect.Effect<void, ScheduledTaskError>;
  }
>()("t3/scheduledTaskChecks/ScheduledTaskChecks") {}

/** A fork refusal as upstream's scheduled task error, so tool handlers report it unchanged. */
const toToolError = (taskId: ScheduledTaskId) => (error: ScheduledTaskCheckError) =>
  new ScheduledTaskError({ message: error.message, taskId });

/** A transaction that cannot begin or commit is a defect, not a caller error. */
const rethrowSql = <A, E, R>(effect: Effect.Effect<A, E | SqlError, R>) =>
  effect.pipe(Effect.catchIf(isSqlError, (error) => Effect.die(error)));

const unavailableHere = () =>
  new ScheduledTaskError({
    message: "Outcome checks and command tasks are not available in this environment.",
  });

/** The hooks where the service exists; otherwise every fork field is refused, never ignored. */
export const orRefuse = (
  checks: Option.Option<ScheduledTaskChecks["Service"]>,
): ScheduledTaskChecks["Service"] =>
  Option.getOrElse(checks, () => ({
    schedule: (hook, save) => (input) =>
      hasOutcomeCheckFields(hook.input) ? Effect.fail(unavailableHere()) : save(input),
    update: (hook, save) => (input) =>
      hasOutcomeCheckFields(hook.input) ? Effect.fail(unavailableHere()) : save(input),
    delete: (_hook, remove) => remove,
    reconcile: Effect.void,
  }));

class Engine extends Context.Service<
  Engine,
  {
    readonly runs: CheckedRuns;
    readonly changes: PubSub.PubSub<void>;
    readonly sql: SqlClient.SqlClient;
  }
>()("t3/scheduledTaskChecks/ScheduledTaskChecks/Engine") {}

// Stored payloads are JSON-encoded: dates are ISO strings, so the JSON codec decodes them.
const decodeRun = Schema.decodeUnknownOption(Schema.fromJsonString(OrchestrationV2RunJson));
const isCheckError = Schema.is(ScheduledTaskCheckError);
const BUSY_STATUSES = new Set(["preparing", "queued", "starting", "running", "waiting"]);
// Server-generated values only. Inert inside single quotes, double quotes or none at all.
const SAFE_VALUE = /^[A-Za-z0-9._:-]+$/;

/**
 * Fills `{date}`, `{run_id}` and `{task_id}`; refuses, before anything runs,
 * a referenced value a shell could interpret.
 */
export const expandCheckCommand = (
  command: string,
  variables: { readonly date: string; readonly runId: string; readonly taskId: string },
): Effect.Effect<string, ScheduledTaskCheckError> => {
  const valueOf = (key: string | undefined) =>
    key === "date" ? variables.date : key === "run_id" ? variables.runId : variables.taskId;
  const placeholders = /\{(date|run_id|task_id)\}/g;
  const used = [...command.matchAll(placeholders)].map((match) => valueOf(match[1]));
  return used.every((value) => SAFE_VALUE.test(value))
    ? Effect.succeed(command.replace(placeholders, (_, key: string) => valueOf(key)))
    : Effect.fail(
        new ScheduledTaskCheckError({
          message: "Refusing to substitute a value with shell syntax.",
        }),
      );
};

const checkError = (message: string) => (cause: unknown) =>
  new ScheduledTaskCheckError({
    message: `${message}${cause instanceof Error && cause.message ? `: ${cause.message}` : ""}`,
  });

const makeEngine = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const projects = yield* ProjectStore.ProjectStoreV2;
  const threadLaunch = yield* ThreadLaunchService.ThreadLaunchService;
  const threads = yield* ThreadManagementService.ThreadManagementService;
  const processes = yield* ProcessRunner;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const prism = yield* Prism.PrismService;
  const adapters = yield* ProviderAdapterRegistryV2;
  const providers = yield* ProviderRegistry.ProviderRegistry;
  // #176 provides its Spectra here once Spectrum is wired; without it no run has any.
  const spectra = yield* ScheduledTaskSpectra;
  const changes = yield* PubSub.sliding<void>(1);
  yield* ensureCheckSchema;

  const shellOf = (threadId: ThreadId) =>
    projections.getThreadShell(threadId).pipe(Effect.mapError(checkError("Could not read thread")));

  const runs = yield* makeCheckedRuns({
    changed: PubSub.publish(changes, undefined).pipe(Effect.asVoid),
    workOpen: (run) => {
      const last = run.sends.at(-1);
      return last === undefined || run.threadId === null
        ? Effect.succeed(false)
        : followSend(run.threadId, last.messageId).pipe(
            Effect.map((end) => end.kind === "waiting"),
            Effect.mapError(checkError("Could not follow the scheduled run")),
          );
    },
    reportsFence: (schedulerRunId) =>
      reportsFence(schedulerRunId).pipe(
        Effect.provideService(ScheduledTaskSpectra, spectra),
        Effect.mapError(checkError("Could not read the run's bound reports")),
      ),
    observe: (task, run) =>
      Effect.gen(function* () {
        const threadId = run.threadId!;
        const shell = yield* shellOf(threadId);
        if (shell === null) {
          // A new-thread run's thread exists once any of its sends was accepted, so
          // missing after that it was deleted. Missing before, it was never created
          // (a rejected or failed launch) and is launched again.
          const accepted =
            run.sends.length === 0
              ? []
              : yield* sql`
                  SELECT 1 FROM orchestration_command_receipts
                  WHERE command_id IN ${sql.in(run.sends.map((send) => send.commandId))}
                    AND status = 'accepted'
                  LIMIT 1
                `.pipe(Effect.mapError(checkError("Could not read the run's receipts")));
          const neverCreated = task.threadId === null && !run.hasWork && accepted.length === 0;
          return {
            unavailable: !neverCreated,
            threadMissing: neverCreated,
            landed: false,
            started: false,
            busy: false,
            blocked: false,
            usageLimit: null,
            runError: null,
          };
        }
        const retired =
          shell.archivedAt !== null ||
          (yield* isThreadRetired(threadId).pipe(
            Effect.provideService(ProjectionStore.ProjectionStoreV2, projections),
            Effect.mapError(checkError("Could not read thread retirement")),
          ));
        const messageIds = run.sends.map((send) => send.messageId);
        const rows =
          messageIds.length === 0
            ? []
            : yield* sql<{ payload_json: string }>`
                SELECT payload_json FROM orchestration_v2_projection_runs
                WHERE thread_id = ${threadId}
                  AND json_extract(payload_json, '$.userMessageId') IN ${sql.in(messageIds)}
              `.pipe(Effect.mapError(checkError("Could not read runs")));
        const sendRuns = rows.flatMap((row) => Option.toArray(decodeRun(row.payload_json)));
        const queued = yield* sql`
          SELECT 1 FROM orchestration_v2_projection_runs
          WHERE thread_id = ${threadId} AND status = 'queued' LIMIT 1
        `.pipe(Effect.mapError(checkError("Could not read runs")));
        const last = run.sends.at(-1);
        const limited = shell.status === "failed" && shell.lastErrorClass === "usage_limit";
        // The latest send's work, followed through exact continuations and recovery facts.
        const work =
          last === undefined
            ? null
            : yield* followSend(threadId, last.messageId).pipe(
                Effect.mapError(checkError("Could not follow the scheduled run")),
              );
        return {
          unavailable: retired,
          threadMissing: false,
          landed:
            last !== undefined && sendRuns.some((entry) => entry.userMessageId === last.messageId),
          started: sendRuns.some((entry) => entry.startedAt !== null),
          busy:
            shell.activeRunId !== null ||
            BUSY_STATUSES.has(shell.status) ||
            queued.length > 0 ||
            (sendRuns.length > 0 && work?.kind === "waiting"),
          blocked: shell.pendingRuntimeRequest !== null || shell.hasActionableProposedPlan,
          usageLimit: limited
            ? {
                resetAt:
                  shell.usageLimitResetAt == null ? null : Date.parse(shell.usageLimitResetAt),
                upstreamResumes: shell.limitRecovery?.autoResume === true,
              }
            : null,
          runError: shell.status === "failed" ? (shell.lastError ?? "The run failed.") : null,
        };
      }).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
    dispatch: ({ taskId, run, send }) =>
      Effect.gen(function* () {
        const payload = send.payload;
        if (payload === undefined)
          return yield* new ScheduledTaskCheckError({
            message: "The recorded send lost its payload and cannot be repeated.",
          });
        const threadId = run.threadId!;
        // Automatic sends are server messages: they never acknowledge a retirement.
        const provenance = { createdBy: "system", creationSource: "server" } as const;
        const launch = payload.launch;
        if (launch !== null && (yield* shellOf(threadId)) === null) {
          yield* threadLaunch
            .launch({
              commandId: send.commandId,
              threadId,
              ...launch,
              initialMessage: {
                messageId: send.messageId,
                scheduledTaskId: taskId,
                text: payload.text,
                attachments: [],
              },
              ...provenance,
            })
            .pipe(Effect.mapError(checkError("Could not start the scheduled thread")));
          return;
        }
        yield* threads
          .sendToThread({
            projectId: payload.projectId,
            commandId: send.commandId,
            threadId,
            messageId: send.messageId,
            scheduledTaskId: taskId,
            text: payload.text,
            attachments: [],
            // Never interrupt the thread; its stored model and effort are kept.
            mode: "queue",
            ...provenance,
          })
          .pipe(Effect.mapError(checkError("Could not send the scheduled message")));
      }),
    runCheck: ({ command, cwd, taskId, runId, date }) =>
      expandCheckCommand(command, { taskId, runId, date }).pipe(
        Effect.flatMap((expanded) =>
          processes
            .run({
              command: "/bin/sh",
              args: ["-c", expanded],
              cwd,
              timeout: "30 seconds",
              maxOutputBytes: CHECK_OUTPUT_BYTES,
              outputMode: "truncate",
              timeoutBehavior: "timedOutResult",
            })
            .pipe(Effect.mapError(checkError("Outcome check could not run"))),
        ),
        Effect.map((result) => ({
          passed: result.code === 0 && !result.timedOut,
          output: `${result.stdout}${result.stderr}${result.timedOut ? "\nOutcome check timed out." : ""}`,
        })),
      ),
    execute: ({ command, cwd, taskId, runId, date }) =>
      expandCheckCommand(command, { taskId, runId, date }).pipe(
        Effect.flatMap((expanded) =>
          runShellCommand({ command: expanded, cwd, deadline: Effect.sleep(COMMAND_TIMEOUT_MS) }),
        ),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      ),
    workspace: ({ projectId, threadId }) =>
      Effect.gen(function* () {
        const shell = threadId === null ? null : yield* shellOf(threadId);
        if (shell?.worktreePath) return shell.worktreePath;
        const project = yield* projects
          .get(projectId)
          .pipe(Effect.mapError(checkError("Could not read project")));
        if (Option.isNone(project))
          return yield* new ScheduledTaskCheckError({ message: "Target project not found." });
        return project.value.workspaceRoot;
      }),
    role: (state, task, launching) =>
      state.role === null
        ? Effect.succeed(null)
        : prism
            .resolve({
              projectId: task.projectId,
              role: state.role,
              lane: state.lane ?? undefined,
              // A post keeps its thread's model, so only the kit is taken. A launch asks
              // Prism for a model and validates it like any unattended launch; with no
              // eligible model the run retries rather than using the task's own model.
              ...(launching
                ? {
                    validate: (selection: ModelSelection) =>
                      Prism.validateLaunchSelection(selection).pipe(
                        Effect.provideService(ProviderAdapterRegistryV2, adapters),
                        Effect.provideService(ProviderRegistry.ProviderRegistry, providers),
                      ),
                  }
                : { explicit: task.modelSelection }),
            })
            .pipe(
              Effect.map((picked) => ({
                modelSelection: picked.modelSelection,
                kitText: picked.kitText === "" ? null : picked.kitText,
              })),
              Effect.mapError(
                (error) =>
                  new ScheduledTaskCheckError({
                    message: `Prism could not start role ${state.role}: ${error.message}`,
                  }),
              ),
            ),
  });
  return { runs, changes, sql };
});

const engineLayer = Layer.effect(Engine, makeEngine);

const policyLayer = Layer.effect(
  ScheduledTaskDispatchPolicy,
  Effect.gen(function* () {
    const { runs, sql } = yield* Engine;
    return {
      decide: (input) => runs.decide(input).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
    };
  }),
);

const toTaskError = (taskId: ScheduledTaskId | undefined) => (cause: unknown) =>
  new ScheduledTaskError({
    message: isCheckError(cause) ? cause.message : "Could not read the task's fork state.",
    ...(taskId === undefined ? {} : { taskId }),
    cause,
  });

/** The read model reports a fork run's real outcome, never a bare dispatch. */
/**
 * What the live task subscription carries: everything but command output,
 * which only the list and the agent tools return. Every client holds the
 * subscription open, and none shows output from it.
 */
export function withoutCommandOutput(task: ScheduledTask): ScheduledTask {
  const run = task.command?.run;
  if (run?.output === undefined) return task;
  const { output: _output, ...compact } = run;
  return { ...task, command: { ...task.command!, run: compact } };
}

export function withForkState(task: ScheduledTask, state: CheckState | undefined): ScheduledTask {
  if (state === undefined) return task;
  const status = checkedRunStatus(state);
  return {
    ...task,
    ...(state.kind === "command"
      ? { command: commandSummary(state) }
      : { outcomeCheck: outcomeCheckSummary(state) }),
    ...(status === null ? {} : { lastRunStatus: status.status, lastRunError: status.error }),
  };
}

const DAY_MS = 86_400_000;
const MINUTE_MS = 60_000;

/** Minutes other enabled tasks fire in the next nine days, for weekly minute spreading. */
function busyMinutes(
  tasks: ReadonlyArray<ScheduledTask>,
  exceptId: string | undefined,
  nowMs: number,
) {
  const minutes = new Set<number>();
  const end = nowMs + 9 * DAY_MS;
  for (const task of tasks) {
    if (task.id === exceptId || !task.enabled) continue;
    const schedule = task.schedule;
    if (isForkScheduledTaskSchedule(schedule)) {
      for (const minute of occupiedMinutes(schedule, nowMs)) minutes.add(minute);
    } else if (schedule.type === "fixed_time") {
      let from: DateTime.DateTime = DateTime.makeUnsafe(nowMs);
      for (let step = 0; step < 16; step += 1) {
        const next = nextScheduledRunAt(schedule, from);
        if (next === null || DateTime.toEpochMillis(next) > end) break;
        minutes.add(Math.floor(DateTime.toEpochMillis(next) / MINUTE_MS));
        from = next;
      }
    } else if (schedule.type === "interval" && task.nextRunAt !== null) {
      const every = Math.max(schedule.everyMs, MINUTE_MS);
      for (let at = Date.parse(task.nextRunAt); at <= end; at += every)
        if (at >= nowMs) minutes.add(Math.floor(at / MINUTE_MS));
    }
  }
  return minutes;
}

const decoratedLayer = Layer.effect(
  ScheduledTaskService.ScheduledTaskService,
  Effect.gen(function* () {
    const inner = yield* ScheduledTaskService.ScheduledTaskService;
    const { runs, changes, sql } = yield* Engine;
    const withSql = <A, E>(effect: Effect.Effect<A, E, SqlClient.SqlClient>) =>
      Effect.provideService(effect, SqlClient.SqlClient, sql);
    const states = withSql(listCheckStates()).pipe(
      Effect.map((all) => new Map(all.map((state) => [state.taskId, state] as const))),
    );
    const attach = (task: ScheduledTask) =>
      states.pipe(
        Effect.map((byId) => withForkState(task, byId.get(task.id))),
        Effect.mapError(toTaskError(task.id)),
      );
    const attachList = (result: { readonly tasks: ReadonlyArray<ScheduledTask> }) =>
      states.pipe(
        Effect.map((byId) => ({
          tasks: result.tasks.map((task) => withForkState(task, byId.get(task.id))),
        })),
        Effect.mapError(toTaskError(undefined)),
      );
    const refused = (message: string, taskId: ScheduledTaskId | undefined) =>
      new ScheduledTaskError({ message, ...(taskId === undefined ? {} : { taskId }) });

    return ScheduledTaskService.ScheduledTaskService.of({
      ...inner,
      list: () => inner.list().pipe(Effect.flatMap(attachList)),
      subscribeList: () =>
        Stream.merge(
          inner.subscribeList(),
          Stream.fromPubSub(changes).pipe(Stream.mapEffect(() => inner.list())),
        ).pipe(
          Stream.mapEffect(attachList),
          Stream.map((result) => ({ ...result, tasks: result.tasks.map(withoutCommandOutput) })),
        ),
      upsert: (input) =>
        Effect.gen(function* () {
          const now = yield* Clock.currentTimeMillis;
          const { tasks } = yield* inner.list();
          const existing =
            input.id === undefined ? undefined : tasks.find((task) => task.id === input.id);
          const state =
            input.id === undefined
              ? undefined
              : (yield* states.pipe(Effect.mapError(toTaskError(input.id)))).get(input.id);
          if (state !== undefined && input.schedule.type === "webhook")
            return yield* refused(
              "Webhook tasks cannot have an outcome check, a Prism role or a shell command.",
              input.id,
            );
          // Only fork triggers need this; upstream handles its own schedule changes.
          const changed =
            existing === undefined ||
            !isForkScheduledTaskSchedule(input.schedule) ||
            !forkSameSchedule(existing.schedule, input.schedule);
          if (input.schedule.type === "once" && changed && Date.parse(input.schedule.at) <= now)
            return yield* refused("The one-shot time is in the past.", input.id);
          let schedule = input.schedule;
          if (schedule.type === "weekly") {
            // The server places each time; an unchanged schedule keeps its earlier picks.
            const previous = existing?.schedule.type === "weekly" ? existing.schedule : undefined;
            schedule = {
              ...schedule,
              chosen:
                !changed && previous?.chosen !== undefined
                  ? previous.chosen
                  : chooseWeeklyMinutes(schedule, busyMinutes(tasks, input.id, now), now),
            };
          }
          const result = yield* inner.upsert({ ...input, schedule });
          return { task: yield* attach(result.task) };
        }),
      setEnabled: (input) =>
        inner.setEnabled(input).pipe(
          Effect.flatMap((result) => attach(result.task)),
          Effect.map((task) => ({ task })),
        ),
      delete: (input) =>
        inner
          .delete(input)
          .pipe(
            Effect.tap(() =>
              withSql(runs.forget(input.id)).pipe(Effect.mapError(toTaskError(input.id))),
            ),
          ),
      runNow: (input) =>
        inner.runNow(input).pipe(
          Effect.flatMap((result) => attach(result.task)),
          Effect.map((task) => ({ task })),
        ),
      rotateWebhookToken: (input) =>
        inner.rotateWebhookToken(input).pipe(
          Effect.flatMap((result) => attach(result.task)),
          Effect.map((task) => ({ task })),
        ),
    });
  }),
);

const checksLayer = Layer.effect(
  ScheduledTaskChecks,
  Effect.gen(function* () {
    // The undecorated upstream service: tasks as stored, before outcome reporting.
    const inner = yield* ScheduledTaskService.ScheduledTaskService;
    const { runs, sql } = yield* Engine;
    const crypto = yield* Crypto.Crypto;
    const scheduler = yield* Scheduler.Scheduler;
    const withSql = <A, E>(effect: Effect.Effect<A, E, SqlClient.SqlClient>) =>
      Effect.provideService(effect, SqlClient.SqlClient, sql).pipe(
        Effect.mapError((error) =>
          isCheckError(error)
            ? error
            : new ScheduledTaskCheckError({ message: "Could not store the task's fork state." }),
        ),
      );
    const reconcile = Effect.gen(function* () {
      const pending = (yield* listCheckStates().pipe(
        Effect.provideService(SqlClient.SqlClient, sql),
        Effect.mapError(toTaskError(undefined)),
      )).filter((state) => unfinishedRun(state) !== undefined);
      if (pending.length === 0) return;
      const { tasks } = yield* inner.list();
      const byId = new Map(tasks.map((task) => [task.id, task] as const));
      yield* Effect.forEach(
        pending,
        (state) => {
          const task = byId.get(state.taskId);
          // A deleted task's state is removed with it; nothing drives it meanwhile.
          return task === undefined
            ? Effect.void
            : Effect.provideService(runs.drive(task), SqlClient.SqlClient, sql).pipe(
                Effect.catchCause((cause) =>
                  Effect.logWarning("Fork scheduled task step failed", { taskId: task.id, cause }),
                ),
              );
        },
        { concurrency: 1, discard: true },
      );
    });
    yield* scheduler.register("scheduled-task-checks", reconcile);
    return ScheduledTaskChecks.of({
      schedule:
        ({ input: fields, projectId, parent, bindToCurrentThread, scope }, save) =>
        (input) =>
          Effect.gen(function* () {
            if (!hasOutcomeCheckFields(fields)) return yield* save(input);
            // A retried call with the same request key reaches the same task.
            const key =
              fields.clientRequestId === undefined
                ? yield* crypto.randomUUIDv4.pipe(Effect.orDie)
                : `${scope.requestNamespace}:${fields.clientRequestId}`;
            const id = ScheduledTaskId.make(`scheduled-task:${key}`);
            return yield* runs.withTaskLock(
              id,
              sql.withTransaction(
                Effect.gen(function* () {
                  yield* withSql(
                    runs.saveUnlocked({
                      taskId: id,
                      projectId,
                      threadId:
                        bindToCurrentThread && parent !== undefined ? parent.thread.id : null,
                      schedule: fields.schedule,
                      fields,
                      actor: parent?.thread.id ?? "agent",
                      actorThreadId: null,
                    }),
                  ).pipe(Effect.mapError(toToolError(id)));
                  return yield* save({ ...input, id });
                }),
              ),
            );
          }).pipe(rethrowSql),
      update:
        ({ input: fields, existing, threadId, parent }, save) =>
        (input) =>
          runs
            .withTaskLock(
              existing.id,
              sql.withTransaction(
                Effect.gen(function* () {
                  yield* withSql(
                    runs.saveUnlocked({
                      taskId: existing.id,
                      projectId: existing.projectId,
                      threadId,
                      schedule: fields.schedule ?? existing.schedule,
                      fields,
                      actor: parent?.thread.id ?? "agent",
                      actorThreadId: parent?.thread.id ?? null,
                    }),
                  ).pipe(Effect.mapError(toToolError(existing.id)));
                  return yield* save(input);
                }),
              ),
            )
            .pipe(rethrowSql),
      delete:
        ({ existing, parent }, remove) =>
        (input) =>
          withSql(runs.assertMayChange(existing.id, parent?.thread.id ?? null)).pipe(
            Effect.mapError(toToolError(existing.id)),
            Effect.andThen(remove(input)),
          ),
      reconcile,
    });
  }),
);

/**
 * Upstream's scheduler with the fork's scheduled task extensions
 * (toolboxmd/chromeria#174): one scheduler, a dispatch policy inside its
 * `runTask`, a read model that reports real outcomes, and a due-work source
 * that continues fork runs.
 */
export const withOutcomeChecks = <E, R>(
  upstream: Layer.Layer<ScheduledTaskService.ScheduledTaskService, E, R>,
) =>
  Layer.merge(decoratedLayer, checksLayer).pipe(
    Layer.provide(upstream.pipe(Layer.provide(policyLayer))),
    Layer.provide(engineLayer),
  );
