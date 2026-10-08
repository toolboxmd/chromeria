import {
  CommandId,
  MessageId,
  ThreadId,
  type ModelSelection,
  type ScheduledTask,
  type ScheduledTaskId,
  type ScheduledTaskOutcomeCheckUpdate,
  type ScheduledTaskSchedule,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FiberSet from "effect/FiberSet";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as SqlClient from "effect/sql/SqlClient";

import {
  ScheduledTaskDispatchFailed,
  ScheduledTaskDispatchRefused,
  type ScheduledTaskDispatchDecision,
} from "./DispatchPolicy.ts";
import type { ReportFence } from "./handoff.ts";
import { forkScheduledSlot } from "./schedules.ts";
import {
  RECOVERY_DELAYS_MS,
  activeCheck,
  compact,
  type CheckResult,
  type CheckState,
  type CheckedRun,
  type RunSend,
  truncateOutput,
  unfinishedRun,
} from "./state.ts";
import {
  consumeSlot,
  deleteCheckState,
  isCurrentTask,
  readCheckState,
  writeCheckState,
} from "./store.ts";

export class ScheduledTaskCheckError extends Schema.TaggedError<ScheduledTaskCheckError>()(
  "ScheduledTaskCheckError",
  { message: Schema.String },
) {}

/** What the reactor needs to know about a run's thread, read from the v2 projection. */
export interface RunObservation {
  /** The thread is missing, archived or retired; only the user can recover it. */
  readonly unavailable: boolean;
  /**
   * A new-thread run's thread was never created: no shell, and none of its
   * sends was accepted. Its next send launches the thread again.
   */
  readonly threadMissing: boolean;
  /** The latest send produced a v2 run (matched by its message id). */
  readonly landed: boolean;
  /** A run started by one of this run's sends reached the provider. */
  readonly started: boolean;
  /**
   * A run is active or queued on the thread, or the latest send's work is still
   * continuing or awaiting a conclusive recovery decision.
   */
  readonly busy: boolean;
  /** The thread waits on the user: a pending approval, question or plan. */
  readonly blocked: boolean;
  /** The thread stopped on a usage limit. */
  readonly usageLimit: {
    readonly resetAt: number | null;
    readonly upstreamResumes: boolean;
  } | null;
  /** The error of the thread's last failed run, when it failed. */
  readonly runError: string | null;
}

/** How one command run ended. */
export interface CommandOutcome {
  readonly exitCode: number | null;
  readonly output: string;
  readonly timedOut: boolean;
  /** Why there is no exit code when the process never started or a signal stopped it. */
  readonly failure: string | null;
}

export interface CheckedRunDeps {
  readonly observe: (
    task: ScheduledTask,
    run: CheckedRun,
  ) => Effect.Effect<RunObservation, ScheduledTaskCheckError>;
  /** Launches the run's thread or posts to it, exactly as the send recorded. */
  readonly dispatch: (input: {
    readonly taskId: ScheduledTaskId;
    readonly run: CheckedRun;
    readonly send: RunSend;
  }) => Effect.Effect<void, ScheduledTaskCheckError>;
  readonly runCheck: (input: {
    readonly command: string;
    readonly cwd: string;
    readonly taskId: string;
    readonly runId: string;
    readonly date: string;
  }) => Effect.Effect<
    { readonly passed: boolean; readonly output: string },
    ScheduledTaskCheckError
  >;
  /** Runs a command task's command once; only its own timeout ends it early. */
  readonly execute: (input: {
    readonly command: string;
    readonly cwd: string;
    readonly taskId: string;
    readonly runId: string;
    readonly date: string;
  }) => Effect.Effect<CommandOutcome, ScheduledTaskCheckError>;
  /** The workspace a check runs in: the thread's worktree, else the project root. */
  readonly workspace: (input: {
    readonly projectId: ScheduledTask["projectId"];
    readonly threadId: ThreadId | null;
  }) => Effect.Effect<string, ScheduledTaskCheckError>;
  /**
   * The Prism pick for a task with a role, null without one. `launching` asks
   * for a model for a new thread; a post only uses the kit. It fails when a
   * role cannot be honored; a role never falls back to the task's model.
   */
  readonly role: (
    state: CheckState,
    task: ScheduledTask,
    launching: boolean,
  ) => Effect.Effect<
    { readonly modelSelection: ModelSelection; readonly kitText: string | null } | null,
    ScheduledTaskCheckError
  >;
  /**
   * Whether the run's latest send's work is still continuing or undecided, such
   * as an admitted continuation or pending recovery; read in the caller's transaction.
   */
  readonly workOpen: (
    run: CheckedRun,
  ) => Effect.Effect<boolean, ScheduledTaskCheckError, SqlClient.SqlClient>;
  /** Where bound Spectrum reports leave this run (the D32/D40 fence), read in the caller's transaction. */
  readonly reportsFence: (
    schedulerRunId: string,
  ) => Effect.Effect<ReportFence, ScheduledTaskCheckError, SqlClient.SqlClient>;
  readonly changed: Effect.Effect<void>;
}

const iso = (ms: number) => DateTime.formatIso(DateTime.makeUnsafe(ms));
const upstreamDecision: ScheduledTaskDispatchDecision = { _tag: "upstream" };
const skipDecision: ScheduledTaskDispatchDecision = { _tag: "skip" };
const refuse = (message: string) => new ScheduledTaskDispatchRefused({ message });
const fail = (message: string) => new ScheduledTaskCheckError({ message });

export const runThreadId = (task: ScheduledTask, runId: string) =>
  task.threadId ?? ThreadId.make(`scheduled-run:${runId}`);

export const sendIdentity = (run: CheckedRun, index: number) => ({
  commandId: CommandId.make(`scheduled-task-check:${run.id}:${index}`),
  messageId: MessageId.make(`scheduled-task-check-message:${run.id}:${index}`),
});

const runContext = (task: ScheduledTask, run: CheckedRun) =>
  `Scheduled task ${task.id}, run ${run.id}. Immutable outcome check version ${run.checkVersion}.\n`;

/** First sends carry the task prompt; continuations ask the same thread to finish. */
export function sendText(
  task: ScheduledTask,
  run: CheckedRun,
  send: Pick<RunSend, "kind">,
  kit: string | null,
): string {
  if (send.kind === "start")
    return `${runContext(task, run)}${kit === null ? "" : `${kit}\n\n`}${task.prompt}`;
  return `${runContext(task, run)}Continue in this conversation. Check what is already done, then finish. Do not repeat completed side effects.\n${run.error ?? "Recovering after a server restart."}\n${run.check?.output ?? ""}`;
}

const replaceRun = (state: CheckState, run: CheckedRun): CheckState => ({
  ...state,
  runs: state.runs.map((entry) => (entry.id === run.id ? run : entry)),
});

const ORPHANED_COMMAND =
  "This command run was interrupted before its result was recorded, for example by a Chromeria restart. It may or may not have finished, and it was not run again.";

export const makeCheckedRuns = Effect.fnUntraced(function* (deps: CheckedRunDeps) {
  const sql = yield* SqlClient.SqlClient;
  const locks = new Map<string, Semaphore.Semaphore>();
  const locked = <A, E, R>(taskId: string, body: Effect.Effect<A, E, R>) =>
    Effect.suspend(() => {
      let lock = locks.get(taskId);
      if (lock === undefined) {
        lock = Semaphore.makeUnsafe(1);
        locks.set(taskId, lock);
      }
      return lock.withPermits(1)(body);
    });
  // Command processes run outside their task lock; closing the engine's scope stops them.
  const commands = yield* FiberSet.make();
  const executing = new Set<string>();
  const store = (previous: CheckState, next: CheckState) =>
    writeCheckState(previous, compact(next)).pipe(Effect.tap(() => deps.changed));

  /** A bound report needs the user, so the run does (D40); its check is not run until it releases. */
  const reportNeedsYou = (state: CheckState, run: CheckedRun, reason: string) => {
    const error = `Spectrum report: ${reason}`;
    return store(state, {
      ...replaceRun(state, {
        ...run,
        stage: "needs-you",
        awaitingReports: true,
        retryAt: null,
        error,
      }),
      lastError: error,
    });
  };

  const retry = (state: CheckState, run: CheckedRun, error: string, now: number) => {
    const delay = RECOVERY_DELAYS_MS[run.attempt];
    return store(state, {
      ...replaceRun(state, {
        ...run,
        error,
        stage: delay === undefined ? "needs-you" : "retry",
        attempt: run.attempt + 1,
        retryAt: delay === undefined ? null : iso(now + delay),
      }),
      failureStreak: state.failureStreak + 1,
      lastError: error,
    });
  };

  /** Sends exactly the recorded send; a failure turns into a delayed retry. */
  const deliver = (taskId: ScheduledTaskId, state: CheckState, run: CheckedRun, send: RunSend) =>
    deps.dispatch({ taskId, run, send }).pipe(
      Effect.catch((error) =>
        Clock.currentTimeMillis.pipe(
          Effect.flatMap((now) => retry(state, run, error.message, now)),
          Effect.andThen(Effect.fail(error)),
        ),
      ),
    );

  /**
   * Records the next send with its whole payload before anything leaves the
   * server. Prism's pick and the text are fixed here; a role that cannot be
   * honored becomes a delayed retry and records no send.
   */
  const startSend = (
    task: ScheduledTask,
    state: CheckState,
    run: CheckedRun,
    now: number,
    threadMissing = false,
  ) =>
    Effect.gen(function* () {
      const index = run.sends.length;
      const kind = run.hasWork ? "continue" : "start";
      // A new-thread run launches until its thread exists, each time as a new send.
      const launching = task.threadId === null && (index === 0 || threadMissing);
      const picked = yield* deps.role(state, task, launching).pipe(Effect.result);
      if (picked._tag === "Failure") {
        const failed = yield* retry(state, run, picked.failure.message, now);
        return { _tag: "refused", state: failed, error: picked.failure } as const;
      }
      const role = picked.success;
      const launch = launching
        ? {
            projectId: task.projectId,
            title: task.title,
            modelSelection: role?.modelSelection ?? task.modelSelection,
            runtimeMode: task.runtimeMode,
            interactionMode: task.interactionMode,
            workspaceStrategy: task.workspaceStrategy,
          }
        : null;
      const send: RunSend = {
        index,
        ...sendIdentity(run, index),
        kind,
        createdAt: iso(now),
        payload: {
          text: sendText(task, run, { kind }, kind === "start" ? (role?.kitText ?? null) : null),
          modelSelection: launch === null ? null : launch.modelSelection,
          launch,
          projectId: task.projectId,
        },
      };
      const next: CheckedRun = {
        ...run,
        stage: "running",
        retryAt: null,
        sends: [...run.sends, send],
      };
      const stored = yield* store(state, replaceRun(state, next));
      return { _tag: "recorded", state: stored, run: next, send } as const;
    });

  /** Records a command run's result, but only for the run this process started. */
  const finishCommand = (taskId: ScheduledTaskId, runId: string, outcome: CommandOutcome) =>
    locked(
      taskId,
      Effect.gen(function* () {
        const state = yield* readCheckState(taskId);
        const run = state?.runs.find((entry) => entry.id === runId);
        if (state === null || run === undefined || run.stage !== "running") return;
        const now = yield* Clock.currentTimeMillis;
        const passed = outcome.exitCode === 0 && !outcome.timedOut;
        const error = passed
          ? null
          : outcome.timedOut
            ? "The command timed out and was stopped."
            : (outcome.failure ?? `The command exited with code ${outcome.exitCode}.`);
        yield* store(state, {
          ...replaceRun(state, {
            ...run,
            stage: passed ? "done" : "needs-you",
            error,
            commandResult: {
              exitCode: outcome.exitCode,
              timedOut: outcome.timedOut,
              endedAt: iso(now),
              output: outcome.output,
            },
          }),
          failureStreak: passed ? 0 : state.failureStreak + 1,
          lastError: error,
          ...(passed ? { lastSuccessfulRunId: run.id } : {}),
        });
      }),
    );

  /** Starts a command run's process detached, so a long command never holds upstream's poll. */
  const launchCommand = (task: ScheduledTask, state: CheckState, run: CheckedRun) =>
    Effect.gen(function* () {
      executing.add(run.id);
      yield* FiberSet.run(
        commands,
        Effect.gen(function* () {
          const cwd = yield* deps.workspace({ projectId: task.projectId, threadId: null });
          return yield* deps.execute({
            command: state.command!,
            cwd,
            taskId: task.id,
            runId: run.id,
            date: run.slot.slice(0, 10),
          });
        }).pipe(
          Effect.catch((error) =>
            Effect.succeed({ exitCode: null, output: "", timedOut: false, failure: error.message }),
          ),
          Effect.flatMap((outcome) => finishCommand(task.id, run.id, outcome)),
          Effect.catchCause((cause) =>
            Effect.logWarning("Scheduled command result was not recorded", {
              taskId: task.id,
              runId: run.id,
              cause,
            }),
          ),
          Effect.ensuring(Effect.sync(() => executing.delete(run.id))),
        ),
      );
    });

  const newRun = (state: CheckState, task: ScheduledTask, slot: string): CheckedRun => ({
    id: `${task.id}:${slot}`,
    slot,
    checkVersion: state.kind === "agent" ? activeCheck(state).version : null,
    threadId: state.kind === "agent" ? runThreadId(task, `${task.id}:${slot}`) : null,
    checkCwd: null,
    stage: "running",
    attempt: 0,
    retryAt: null,
    hasWork: false,
    sends: [],
    error: null,
    check: null,
  });

  const decide = (input: {
    readonly task: ScheduledTask;
    readonly trigger: "scheduled" | "manual" | "webhook";
    readonly startedAt: DateTime.DateTime;
  }): Effect.Effect<
    ScheduledTaskDispatchDecision,
    ScheduledTaskDispatchRefused,
    SqlClient.SqlClient
  > =>
    input.trigger === "webhook"
      ? Effect.succeed(upstreamDecision)
      : locked(
          input.task.id,
          Effect.gen(function* () {
            const { task, trigger } = input;
            const sql = yield* SqlClient.SqlClient;
            // The dispatch runs later, after upstream marks the task running.
            const fork = (
              dispatch: Effect.Effect<void, { readonly message: string }, SqlClient.SqlClient>,
            ): ScheduledTaskDispatchDecision => ({
              _tag: "fork",
              dispatch: dispatch.pipe(
                Effect.provideService(SqlClient.SqlClient, sql),
                Effect.mapError(
                  (error) => new ScheduledTaskDispatchFailed({ message: error.message }),
                ),
              ),
            });
            const state = yield* readCheckState(task.id);
            if (state === null) return upstreamDecision;
            // An edit committed since upstream read this task: act on the stored definition
            // instead. A scheduled fire is not consumed, so the next poll fires it fresh.
            if (!(yield* isCurrentTask(task))) {
              if (trigger === "scheduled") return skipDecision;
              return yield* refuse("The task changed while it was starting; try again.");
            }
            const now = DateTime.toEpochMillis(input.startedAt);
            const current = unfinishedRun(state);
            if (current !== undefined) {
              if (trigger === "scheduled") {
                yield* consumeSlot(task, input.startedAt);
                yield* deps.changed;
                return skipDecision;
              }
              if (state.kind === "command") return yield* refuse("This command is still running.");
              if (current.stage !== "needs-you")
                return yield* refuse("This task already has unfinished work.");
              // A bound report that needs you is resolved first; nothing is sent over it.
              const fence = yield* deps.reportsFence(current.id);
              if (fence.kind === "needs-you")
                return yield* refuse(`The Spectrum report still needs you: ${fence.reason}`);
              // Its next check waits on its reports: resuming reads them again and
              // never messages the agent.
              if (current.awaitingReports === true) {
                yield* store(
                  state,
                  replaceRun(state, { ...current, stage: "running", error: null }),
                );
                return skipDecision;
              }
              const observation = yield* deps.observe(task, current);
              if (observation.unavailable)
                return yield* refuse(
                  "Recover the retired or missing thread explicitly before resuming its pinned run.",
                );
              // Resuming must not queue a second turn behind work or a pending question.
              if (observation.busy || observation.blocked || observation.usageLimit !== null)
                return yield* refuse(
                  "The run's thread is busy or waiting on you; finish that before resuming.",
                );
              const resumed = yield* startSend(
                task,
                state,
                { ...current, attempt: 0, hasWork: current.hasWork || observation.started },
                now,
                observation.threadMissing,
              );
              return resumed._tag === "refused"
                ? fork(Effect.fail(resumed.error))
                : fork(locked(task.id, deliver(task.id, resumed.state, resumed.run, resumed.send)));
            }
            // A scheduled fork trigger is identified by its slot, so a replayed fire is the same run.
            const slot = forkScheduledSlot(task, trigger) ?? iso(now);
            if (state.runs.some((run) => run.id === `${task.id}:${slot}`)) {
              if (trigger === "scheduled") {
                yield* consumeSlot(task, input.startedAt);
                yield* deps.changed;
              }
              return skipDecision;
            }
            const run = newRun(state, task, slot);
            const withRun: CheckState = { ...state, runs: [...state.runs, run] };
            if (state.kind === "command") {
              // Stored as running before anything is spawned: a restart never runs it twice.
              const stored = yield* store(state, withRun);
              return fork(launchCommand(task, stored, run));
            }
            const started = yield* startSend(task, withRun, run, now);
            return started._tag === "refused"
              ? fork(Effect.fail(started.error))
              : fork(locked(task.id, deliver(task.id, started.state, started.run, started.send)));
          }),
        ).pipe(
          Effect.catchTags({
            SchemaError: () => Effect.fail(refuse("The task's fork state is unreadable.")),
            SqlError: () => Effect.fail(refuse("Could not read the task's fork state.")),
            CheckStateConflict: () =>
              Effect.fail(refuse("The task changed while it was starting; try again.")),
            ScheduledTaskCheckError: (error) => Effect.fail(refuse(error.message)),
          }),
        );

  const check = (task: ScheduledTask, state: CheckState, run: CheckedRun, now: number) =>
    Effect.gen(function* () {
      const version = state.checks.find((entry) => entry.version === run.checkVersion)!;
      const cwd =
        run.checkCwd ??
        (yield* deps.workspace({ projectId: task.projectId, threadId: run.threadId }));
      const verdict = yield* deps
        .runCheck({
          command: version.command,
          cwd,
          taskId: task.id,
          runId: run.id,
          date: run.slot.slice(0, 10),
        })
        .pipe(Effect.catch((error) => Effect.succeed({ passed: false, output: error.message })));
      const result: CheckResult = {
        version: version.version,
        passed: verdict.passed,
        output: truncateOutput(verdict.output),
        checkedAt: iso(now),
      };
      return { result, cwd };
    });

  /** One reconciliation step for a task's unfinished run, serialized per task. */
  const drive = (task: ScheduledTask) =>
    locked(
      task.id,
      Effect.gen(function* () {
        let state = yield* readCheckState(task.id);
        if (state === null) return;
        let run = unfinishedRun(state);
        if (run === undefined) return;
        if (state.kind === "command") {
          // A command run started by an earlier process, or whose result was lost, may have run.
          if (executing.has(run.id)) return;
          yield* store(state, {
            ...replaceRun(state, { ...run, stage: "needs-you", error: ORPHANED_COMMAND }),
            failureStreak: state.failureStreak + 1,
            lastError: ORPHANED_COMMAND,
          });
          return;
        }
        if (run.stage === "needs-you") return;
        const now = yield* Clock.currentTimeMillis;
        const observation = yield* deps.observe(task, run);
        if (observation.unavailable) {
          yield* store(state, {
            ...replaceRun(state, {
              ...run,
              stage: "needs-you",
              retryAt: null,
              error: "Thread is retired or unavailable; explicit recovery is required.",
            }),
            failureStreak: state.failureStreak + 1,
            lastError: "Thread is retired or unavailable; explicit recovery is required.",
          });
          return;
        }
        // A bound report that needs you makes the run need you before any check,
        // retry, resend or redelivery, whether or not a check passed before (D40).
        const fenceState = state;
        const fenceRun = run;
        const fence = yield* sql.withTransaction(
          Effect.gen(function* () {
            const fence = yield* deps.reportsFence(fenceRun.id);
            if (fence.kind === "needs-you")
              yield* reportNeedsYou(fenceState, fenceRun, fence.reason);
            return fence;
          }),
        );
        if (fence.kind === "needs-you") return;
        const last = run.sends.at(-1);
        // A send recorded before a crash or a failed dispatch is resent with its own identity.
        if (run.stage === "running" && last !== undefined && !observation.landed) {
          yield* deliver(task.id, state, run, last).pipe(Effect.ignore);
          return;
        }
        if (observation.started && !run.hasWork) {
          run = { ...run, hasWork: true };
          state = yield* store(state, replaceRun(state, run));
        }
        if (observation.blocked) return;
        if (observation.usageLimit !== null) {
          const resetAt =
            observation.usageLimit.resetAt === null ? null : iso(observation.usageLimit.resetAt);
          if (run.stage !== "usage-limit" || run.retryAt !== resetAt) {
            run = {
              ...run,
              stage: "usage-limit",
              retryAt: resetAt,
              error: observation.runError ?? "Usage limit reached",
            };
            state = yield* store(state, replaceRun(state, run));
          }
          // Upstream's recovery resumes the thread at reset; never resume it twice.
          if (
            observation.usageLimit.upstreamResumes ||
            observation.busy ||
            observation.usageLimit.resetAt === null ||
            observation.usageLimit.resetAt > now
          )
            return;
        } else if (observation.busy) {
          return;
        } else if (run.stage === "running" || run.stage === "usage-limit") {
          // A passed check waits for bound reports before it is run again.
          if (run.awaitingReports === true && fence.kind !== "released") return;
          const { result, cwd } = yield* check(task, state, run, now);
          if (result.passed) {
            const checked = state;
            const passedRun = run;
            // Settle only in one transaction that re-evaluates the run's own work,
            // which may have resumed during the check, and every bound report.
            yield* sql.withTransaction(
              Effect.gen(function* () {
                if (yield* deps.workOpen(passedRun)) return;
                const fence = yield* deps.reportsFence(passedRun.id);
                const checkedRun = { ...passedRun, checkCwd: cwd, check: result, retryAt: null };
                if (fence.kind === "needs-you")
                  return yield* reportNeedsYou(checked, checkedRun, fence.reason);
                const released = fence.kind === "released";
                yield* store(checked, {
                  ...replaceRun(checked, {
                    ...checkedRun,
                    stage: released ? "done" : passedRun.stage,
                    awaitingReports: !released,
                  }),
                  ...(released ? { failureStreak: 0, lastError: null } : {}),
                });
              }),
            );
            return;
          }
          const failedState = state;
          const failedRun = { ...run, checkCwd: cwd, check: result };
          // A report that came to need you during the check is what follows, never a retry.
          yield* sql.withTransaction(
            Effect.gen(function* () {
              const fence = yield* deps.reportsFence(failedRun.id);
              if (fence.kind === "needs-you")
                return yield* reportNeedsYou(failedState, failedRun, fence.reason);
              yield* retry(
                failedState,
                failedRun,
                observation.runError ?? `Outcome check failed:\n${result.output}`,
                now,
              );
            }),
          );
          return;
        }
        if (run.stage === "retry" && run.retryAt !== null && Date.parse(run.retryAt) > now) return;
        const started = yield* startSend(task, state, run, now, observation.threadMissing);
        if (started._tag === "recorded")
          yield* deliver(task.id, started.state, started.run, started.send).pipe(Effect.ignore);
      }),
    );

  const judgedGuard = (state: CheckState | null, actorThreadId: ThreadId | null) => {
    const run = state === null ? undefined : unfinishedRun(state);
    return actorThreadId !== null && run?.threadId === actorThreadId
      ? Effect.fail(fail("A judged thread cannot change its own scheduled task or outcome check."))
      : Effect.void;
  };

  /**
   * Agent-tool writes: refuses webhook tasks, kind changes and a judged
   * thread's own task. Callers hold the task's lock (see `withTaskLock`).
   */
  const saveUnlocked = (input: {
    readonly taskId: ScheduledTaskId;
    readonly projectId: ScheduledTask["projectId"];
    readonly threadId: ThreadId | null;
    readonly schedule: { readonly type: ScheduledTaskSchedule["type"] };
    readonly fields: ScheduledTaskOutcomeCheckUpdate;
    readonly actor: string;
    readonly actorThreadId: ThreadId | null;
  }) =>
    Effect.gen(function* () {
      const { fields } = input;
      const state = yield* readCheckState(input.taskId);
      yield* judgedGuard(state, input.actorThreadId);
      const checkFields =
        fields.checkCommand !== undefined ||
        fields.checkReason !== undefined ||
        fields.revertCheckVersion !== undefined ||
        fields.role !== undefined ||
        fields.lane !== undefined;
      const commandField = fields.command !== undefined;
      if (input.schedule.type === "webhook" && (checkFields || commandField || state !== null))
        return yield* fail(
          "Webhook tasks cannot have an outcome check, a Prism role or a shell command.",
        );
      if (checkFields && commandField)
        return yield* fail("A command task runs a shell command and has no outcome check or role.");
      if (!checkFields && !commandField) return state;
      const now = yield* Clock.currentTimeMillis;
      if (state === null) {
        if (commandField) {
          const stored = yield* writeCheckState(null, {
            version: 1,
            taskId: input.taskId,
            revision: 0,
            kind: "command",
            command: fields.command!,
            role: null,
            lane: null,
            checks: [],
            runs: [],
            failureStreak: 0,
            lastError: null,
            lastSuccessfulRunId: null,
          });
          yield* deps.changed;
          return stored;
        }
        if (fields.revertCheckVersion !== undefined)
          return yield* fail("This task has no outcome check to revert.");
        if (fields.checkCommand === undefined || fields.checkReason === undefined)
          return yield* fail("An outcome check needs both checkCommand and checkReason.");
        const cwd = yield* deps.workspace({
          projectId: input.projectId,
          threadId: input.threadId,
        });
        const tested = yield* deps.runCheck({
          command: fields.checkCommand,
          cwd,
          taskId: input.taskId,
          runId: `${input.taskId}:creation`,
          date: iso(now).slice(0, 10),
        });
        if (tested.passed)
          return yield* fail("Outcome check already passes. Task creation refused.");
        const stored = yield* writeCheckState(null, {
          version: 1,
          taskId: input.taskId,
          revision: 0,
          kind: "agent",
          command: null,
          role: fields.role ?? null,
          lane: fields.lane ?? null,
          checks: [
            {
              version: 1,
              command: fields.checkCommand,
              actor: input.actor,
              reason: fields.checkReason,
              createdAt: iso(now),
              revertedFrom: null,
            },
          ],
          runs: [],
          failureStreak: 0,
          lastError: null,
          lastSuccessfulRunId: null,
        });
        yield* deps.changed;
        return stored;
      }
      if (state.kind === "command") {
        if (checkFields)
          return yield* fail("Command tasks have no outcome check. Change the command instead.");
        return yield* store(state, { ...state, command: fields.command! });
      }
      if (commandField)
        return yield* fail("A task's kind cannot change. Create a new task instead.");
      let checks = state.checks;
      if (fields.checkCommand !== undefined || fields.revertCheckVersion !== undefined) {
        if (fields.checkReason === undefined)
          return yield* fail("Check edits and reverts require checkReason.");
        if (fields.checkCommand !== undefined && fields.revertCheckVersion !== undefined)
          return yield* fail("Choose a new check or a revert, not both.");
        const command =
          fields.checkCommand ??
          state.checks.find((entry) => entry.version === fields.revertCheckVersion)?.command;
        if (command === undefined) return yield* fail("Check version not found.");
        checks = [
          ...checks,
          {
            version: activeCheck(state).version + 1,
            command,
            actor: input.actor,
            reason: fields.checkReason,
            createdAt: iso(now),
            revertedFrom: fields.revertCheckVersion ?? null,
          },
        ];
      }
      return yield* store(state, {
        ...state,
        checks,
        role: fields.role ?? state.role,
        lane: fields.lane ?? state.lane,
      });
    });

  const save = (input: Parameters<typeof saveUnlocked>[0]) =>
    locked(input.taskId, saveUnlocked(input));

  /** Pause, delete and plain edits by an agent run the same judged-thread guard. */
  const assertMayChange = (taskId: ScheduledTaskId, actorThreadId: ThreadId | null) =>
    readCheckState(taskId).pipe(Effect.flatMap((state) => judgedGuard(state, actorThreadId)));

  const forget = (taskId: ScheduledTaskId) =>
    locked(taskId, deleteCheckState(taskId).pipe(Effect.tap(() => deps.changed)));

  /** Completes when every command this engine started has recorded its result. */
  const drainCommands = FiberSet.awaitEmpty(commands);

  /** Serializes a write with this task's fires and steps; never nest it inside `save`. */
  const withTaskLock = locked;

  return {
    decide,
    drive,
    save,
    saveUnlocked,
    withTaskLock,
    assertMayChange,
    forget,
    drainCommands,
  };
});

export type CheckedRuns = Effect.Success<ReturnType<typeof makeCheckedRuns>>;
