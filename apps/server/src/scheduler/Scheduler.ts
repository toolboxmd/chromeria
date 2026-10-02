import {
  SchedulerError,
  COMMAND_OUTPUTS_KEPT,
  DEFAULT_PERSON,
  isCommandTask,
  isSettledRun,
  threadOwner,
  ThreadId,
  type ScheduledTask,
  type ScheduledTaskView,
  type TaskDefinition,
  type TaskRun,
  type CreateScheduledTask,
  type EditScheduledTask,
  type TaskCheckResult,
  type TaskCheckVersion,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as FiberSet from "effect/FiberSet";
import * as Semaphore from "effect/Semaphore";
import * as Schema from "effect/Schema";
import {
  CreateScheduledTask as CreateSchema,
  EditScheduledTask as EditSchema,
} from "@t3tools/contracts";
import { chooseMinutes, iso, taskSlots } from "./Schedule.ts";
import type { ShellCommandOutcome } from "./CommandRunner.ts";

export const RECOVERY_DELAYS = [30_000, 60_000, 300_000, 900_000, 3_600_000] as const;
export const LEASE_MS = 120_000;
const decodeCreate = Schema.decodeUnknownEffect(CreateSchema);
const decodeEdit = Schema.decodeUnknownEffect(EditSchema);
const terminal = isSettledRun;
const fail = (detail: string) => Effect.fail(new SchedulerError({ detail }));
const present = (task: ScheduledTask, now: number): ScheduledTaskView => {
  const next = task.paused || task.deleted ? null : taskSlots(task, now).next;
  return { ...task, nextRunAt: next === null ? null : iso(next) };
};
export type RunObservation = {
  readonly active: boolean;
  readonly idle: boolean;
  readonly retired: boolean;
  readonly hasWork: boolean;
  readonly pendingDrafters: boolean;
  readonly pendingReports?: boolean;
  readonly blocked?: boolean;
  readonly drafterIds: ReadonlyArray<ThreadId>;
  readonly turnId: string | null;
  readonly error: string | null;
  readonly usageLimited: boolean;
  readonly resetAt: number | null;
  readonly stalled: boolean;
};

/** All state writes go through the engine's serialized compare-and-set and durable receipts. */
export const makeScheduler = Effect.fnUntraced(function* (deps: {
  readonly processId: string;
  readonly tasks: Effect.Effect<ReadonlyArray<ScheduledTask>>;
  readonly save: (
    previous: ScheduledTask | undefined,
    next: ScheduledTask,
  ) => Effect.Effect<void, SchedulerError>;
  readonly uuid: Effect.Effect<string>;
  readonly sequence: Effect.Effect<number>;
  readonly check: (
    command: string,
    variables: { taskId: string; runId: string; date: string; cwd: string },
  ) => Effect.Effect<{ passed: boolean; output: string }, SchedulerError>;
  /** Runs a command task's command once. Only its own timeout ends it early. */
  readonly execute: (
    command: string,
    variables: { taskId: string; runId: string; date: string; cwd: string },
  ) => Effect.Effect<ShellCommandOutcome, SchedulerError>;
  readonly historyVersion: (
    taskId: string,
    version: number,
  ) => Effect.Effect<TaskCheckVersion | undefined, SchedulerError>;
  /** The pinned working directory: the target thread's worktree or the project root. */
  readonly validateTarget: (definition: TaskDefinition) => Effect.Effect<string, SchedulerError>;
  readonly observe: (
    task: ScheduledTask,
    run: TaskRun,
  ) => Effect.Effect<RunObservation, SchedulerError>;
  readonly prepare: (task: ScheduledTask, run: TaskRun) => Effect.Effect<ThreadId, SchedulerError>;
  readonly send: (
    task: ScheduledTask,
    run: TaskRun,
    text: string,
  ) => Effect.Effect<boolean, SchedulerError>;
  readonly interrupt: (run: TaskRun) => Effect.Effect<void, SchedulerError>;
  readonly wake: Effect.Effect<void>;
}) {
  const locks = new Map<string, Semaphore.Semaphore>();
  const creationLock = yield* Semaphore.make(1);
  // Command runs execute outside their task lock; closing the scheduler's scope stops them.
  const commands = yield* FiberSet.make();
  const executing = new Set<string>();
  const locked = <A>(id: string, body: Effect.Effect<A, SchedulerError>) =>
    Effect.gen(function* () {
      let lock = locks.get(id);
      if (!lock) {
        lock = Semaphore.makeUnsafe(1);
        locks.set(id, lock);
      }
      return yield* lock.withPermits(1)(body);
    });
  const lookup = (id: string) =>
    deps.tasks.pipe(
      Effect.flatMap((tasks) => {
        const task = tasks.find((entry) => entry.id === id && !entry.deleted);
        return task ? Effect.succeed(task) : fail(`Scheduled task ${id} not found.`);
      }),
    );
  const store = (task: ScheduledTask, patch: Partial<ScheduledTask>) =>
    Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis;
      const candidate = { ...task, ...patch };
      const pinned = new Set(
        candidate.runs.filter((run) => !terminal(run)).map((run) => run.checkVersion),
      );
      const checks = candidate.checks.filter(
        (check, index) => index >= candidate.checks.length - 20 || pinned.has(check.version),
      );
      const kept = candidate.runs.filter(
        (run, index) => !terminal(run) || index >= candidate.runs.length - 20,
      );
      const runs = kept.map((run, index) => {
        if (index >= kept.length - COMMAND_OUTPUTS_KEPT || run.commandResult?.output === undefined)
          return run;
        const { output: _, ...commandResult } = run.commandResult;
        return { ...run, commandResult };
      });
      const next = { ...candidate, checks, runs, revision: task.revision + 1, updatedAt: iso(now) };
      yield* deps.save(task, next);
      return next;
    });
  const updateRun = (task: ScheduledTask, run: TaskRun, patch: Partial<TaskRun>) =>
    store(task, {
      runs: task.runs.map((entry) => (entry.id === run.id ? { ...run, ...patch } : entry)),
    });
  const claim = (task: ScheduledTask, slot: string, now: number) =>
    Effect.gen(function* () {
      if (task.runs.some((run) => !terminal(run)))
        return yield* fail("This task already has unfinished work.");
      if (task.runs.some((run) => run.id === `${task.id}:${slot}`)) return task;
      const definition = task.definition;
      const command = isCommandTask(definition);
      const run: TaskRun = {
        owner: threadOwner(task),
        definition,
        checkCwd: task.checkCwd,
        id: `${task.id}:${slot}`,
        slot,
        ...(command ? {} : { checkVersion: task.checks.at(-1)!.version }),
        threadId:
          !command && definition.target.kind === "thread" ? definition.target.threadId : null,
        // A command claim is its admission: stored as running before anything is spawned.
        status: command ? "running" : "claimed",
        processId: deps.processId,
        originSequence: yield* deps.sequence,
        sendIndex: 0,
        attempt: 0,
        hasWork: false,
        leaseUntil: now + LEASE_MS,
        retryAt: null,
        dispatchedAt: command ? iso(now) : null,
        observedTurnId: null,
        error: null,
        check: null,
        drafterIds: [],
      };
      const next = yield* store(task, { consumedSlot: slot, runs: [...task.runs, run] });
      if (isCommandTask(run.definition)) yield* launch(next.id, run, run.definition.command);
      return next;
    });
  // Settles only the run this process admitted, so a result never lands on another run.
  const finish = (taskId: string, runId: string, outcome: ShellCommandOutcome) =>
    locked(
      taskId,
      Effect.gen(function* () {
        const task = (yield* deps.tasks).find((entry) => entry.id === taskId);
        const run = task?.runs.find((entry) => entry.id === runId);
        if (!task || !run || run.status !== "running" || run.processId !== deps.processId) return;
        const now = yield* Clock.currentTimeMillis;
        const passed = outcome.exitCode === 0 && !outcome.timedOut;
        const error = passed
          ? null
          : outcome.timedOut
            ? "The command timed out and was stopped."
            : (outcome.failure ?? `The command exited with code ${outcome.exitCode}.`);
        yield* store(task, {
          failureStreak: passed ? 0 : task.failureStreak + 1,
          lastError: error,
          runs: task.runs.map((entry) =>
            entry.id !== run.id
              ? entry
              : {
                  ...run,
                  status: passed ? "done" : "needs-you",
                  error,
                  commandResult: {
                    exitCode: outcome.exitCode,
                    output: outcome.output,
                    timedOut: outcome.timedOut,
                    endedAt: iso(now),
                  },
                },
          ),
        });
      }),
    );
  const launch = (taskId: string, run: TaskRun, command: string) =>
    Effect.gen(function* () {
      const runId = run.id;
      executing.add(runId);
      yield* FiberSet.run(
        commands,
        deps
          .execute(command, {
            taskId,
            runId,
            date: run.slot.slice(0, 10),
            cwd: run.checkCwd,
          })
          .pipe(
            Effect.catch((error) =>
              Effect.succeed({
                exitCode: null,
                output: "",
                timedOut: false,
                failure: error.detail,
              }),
            ),
            Effect.flatMap((outcome) => finish(taskId, runId, outcome)),
            Effect.catch((error) =>
              Effect.logWarning("Scheduled command result was not recorded", {
                taskId,
                runId,
                error: error.detail,
              }),
            ),
            Effect.ensuring(Effect.sync(() => executing.delete(runId))),
          ),
      );
    });
  // A command run admitted by an earlier process, or whose result was lost, may have run.
  const settleOrphanedCommand = (task: ScheduledTask) => {
    const run = task.runs.find((entry) => !terminal(entry));
    if (!run || executing.has(run.id)) return Effect.succeed(task);
    const error =
      "This command run was interrupted before its result was recorded, for example by a Chromeria restart. It may or may not have finished, and it was not run again.";
    return store(task, {
      failureStreak: task.failureStreak + 1,
      lastError: error,
      runs: task.runs.map((entry) =>
        entry.id === run.id ? { ...run, status: "needs-you", error } : entry,
      ),
    });
  };
  const retry = (task: ScheduledTask, run: TaskRun, error: string, now: number) => {
    const delay = RECOVERY_DELAYS[run.attempt];
    return store(task, {
      failureStreak: task.failureStreak + 1,
      lastError: error,
      runs: task.runs.map((entry) =>
        entry.id !== run.id
          ? entry
          : {
              ...run,
              error,
              status: delay === undefined ? "needs-you" : "retry",
              attempt: run.attempt + 1,
              retryAt: delay === undefined ? null : now + delay,
              leaseUntil: now + LEASE_MS,
            },
      ),
    });
  };
  // Retry opportunity changes; accepted send identities and all pinned work context survive.
  const resume = (
    task: ScheduledTask,
    run: TaskRun,
    slot: string,
    now: number,
    observation: RunObservation,
  ) =>
    store(task, {
      consumedSlot: slot,
      runs: task.runs.map((entry) =>
        entry.id !== run.id
          ? entry
          : {
              ...run,
              status: "retry",
              attempt: 0,
              retryAt: null,
              leaseUntil: now + LEASE_MS,
              hasWork: run.hasWork || observation.hasWork || run.dispatchedAt !== null,
            },
      ),
    });
  const drive = (id: string) =>
    locked(
      id,
      Effect.gen(function* () {
        let task = yield* lookup(id);
        const now = yield* Clock.currentTimeMillis;
        const slots = taskSlots(task, now);
        const nextSlot =
          !task.paused &&
          slots.latest !== null &&
          (task.consumedSlot === null || slots.latest > Date.parse(task.consumedSlot));
        if (isCommandTask(task.definition)) {
          // Settle an ambiguous admission first, so a newer missed slot still runs once.
          task = yield* settleOrphanedCommand(task);
          if (!nextSlot) return;
          const slot = iso(slots.latest!);
          if (task.runs.some((run) => !terminal(run))) yield* store(task, { consumedSlot: slot });
          else yield* claim(task, slot, now);
          return;
        }
        if (nextSlot) {
          const slot = iso(slots.latest!);
          task = task.runs.some((run) => !terminal(run))
            ? yield* store(task, { consumedSlot: slot })
            : yield* claim(task, slot, now);
        }
        let run = task.runs.find((entry) => !terminal(entry));
        if (!run || task.paused) return;
        const definition = run.definition;
        if (isCommandTask(definition)) return;
        const observation = yield* deps.observe(task, run);
        if (observation.retired) {
          if (run.status === "needs-you") return;
          yield* retry(
            task,
            { ...run, attempt: RECOVERY_DELAYS.length },
            "Thread is retired or unavailable; explicit recovery is required.",
            now,
          );
          return;
        }
        const hasWork =
          run.hasWork ||
          observation.hasWork ||
          (run.dispatchedAt !== null &&
            (run.processId !== deps.processId ||
              (run.status === "running" && run.leaseUntil <= now && !observation.error)));
        if (
          hasWork !== run.hasWork ||
          observation.drafterIds.some((id) => !run!.drafterIds.includes(id))
        ) {
          task = yield* updateRun(task, run, {
            hasWork,
            drafterIds: [...new Set([...run.drafterIds, ...observation.drafterIds])],
          });
          run = task.runs.find((entry) => entry.id === run!.id)!;
        }
        if (run.status === "needs-you") {
          if (!nextSlot) return;
          task = yield* resume(task, run, iso(slots.latest!), now, observation);
          run = task.runs.find((entry) => entry.id === run!.id)!;
        }
        if (observation.blocked) {
          if (run.leaseUntil <= now) yield* updateRun(task, run, { leaseUntil: now + LEASE_MS });
          return;
        }
        if (observation.usageLimited) {
          if (run.status !== "usage-limit" || run.retryAt !== observation.resetAt)
            yield* updateRun(task, run, {
              status: "usage-limit",
              retryAt: observation.resetAt,
              error: observation.error ?? "Usage limit reached",
              hasWork,
              leaseUntil: now + LEASE_MS,
            });
          return;
        }
        if (observation.active && !observation.stalled) {
          if (run.leaseUntil <= now) yield* updateRun(task, run, { leaseUntil: now + LEASE_MS });
          return;
        }
        if (observation.stalled) {
          // Interrupt first. A receipt/event then proves idle before the same thread can continue.
          yield* deps.interrupt(run);
          yield* retry(
            task,
            { ...run, hasWork: true },
            "Provider stalled. Check what is already done, then finish.",
            now,
          );
          return;
        }
        if (run.status === "running") {
          if (!observation.idle && !observation.error && run.leaseUntil > now) return;
          const version = task.checks.find((version) => version.version === run!.checkVersion)!;
          const verdict = yield* deps
            .check(version.command, {
              taskId: task.id,
              runId: run.id,
              date: run.slot.slice(0, 10),
              cwd: run.checkCwd,
            })
            .pipe(Effect.catch((error) => Effect.succeed({ passed: false, output: error.detail })));
          const check: TaskCheckResult = {
            ...verdict,
            version: version.version,
            checkedAt: iso(now),
          };
          if (verdict.passed) {
            if (observation.pendingDrafters) {
              yield* updateRun(task, run, { check, hasWork, leaseUntil: now + LEASE_MS });
              return;
            }
            if (!observation.pendingReports) {
              yield* store(task, {
                failureStreak: 0,
                lastError: null,
                runs: task.runs.map((entry) =>
                  entry.id === run!.id
                    ? { ...run!, check, hasWork, status: "done", retryAt: null }
                    : entry,
                ),
              });
              return;
            }
            // Passing the judge cannot discard queued Drafter context. Deliver it through our own turn.
            task = yield* updateRun(task, run, {
              check,
              hasWork: true,
              status: "retry",
              retryAt: null,
            });
            run = task.runs.find((entry) => entry.id === run!.id)!;
          } else {
            yield* retry(
              task,
              { ...run, check, hasWork: hasWork || !observation.error },
              observation.error ?? `Outcome check failed:\n${verdict.output}`,
              now,
            );
            return;
          }
        }
        if (run.status !== "usage-limit" && run.retryAt !== null && run.retryAt > now) return;
        if (observation.pendingDrafters) return;
        if (!observation.idle && run.threadId !== null) return;
        // Persist the thread and sending intent before any provider side effect. Recovery always uses it.
        const threadId = yield* deps
          .prepare({ ...task, definition: run.definition }, run)
          .pipe(
            Effect.catch((error) => retry(task, run!, error.detail, now).pipe(Effect.as(null))),
          );
        if (threadId === null) return;
        const continuing = hasWork || run.hasWork || run.status === "usage-limit";
        task = yield* updateRun(task, run, {
          threadId,
          status: "running",
          processId: deps.processId,
          sendIndex: run.sendIndex + 1,
          dispatchedAt: iso(now),
          observedTurnId: observation.turnId,
          leaseUntil: now + LEASE_MS,
          retryAt: null,
        });
        run = task.runs.find((entry) => entry.id === run!.id)!;
        const context = `Scheduled task ${task.id}, run ${run.id}. Immutable outcome check version ${run.checkVersion}.\n`;
        const text = continuing
          ? `${context}Continue in this conversation. Check what is already done, then finish. Do not repeat completed side effects.\n${run.error ?? "Recovering after a server restart."}\n${run.check?.output ?? ""}`
          : `${context}${definition.prompt}`;
        const sent = yield* deps
          .send({ ...task, definition: run.definition }, run, text)
          .pipe(
            Effect.catch((error) => retry(task, run!, error.detail, now).pipe(Effect.as(false))),
          );
        if (!sent) {
          const fresh = yield* lookup(task.id);
          const current = fresh.runs.find((entry) => entry.id === run!.id)!;
          if (current.status === "running")
            yield* retry(
              fresh,
              current,
              "Thread admission or provider capacity changed before dispatch.",
              now,
            );
        }
      }),
    );
  const create = (raw: CreateScheduledTask, actor: string, owner: string = DEFAULT_PERSON) =>
    creationLock.withPermits(1)(
      Effect.gen(function* () {
        const input = yield* decodeCreate(raw).pipe(
          Effect.mapError(
            () =>
              new SchedulerError({
                detail:
                  "A valid schedule plus a target and nonempty outcome check, or a project and command, are required.",
              }),
          ),
        );
        const now = yield* Clock.currentTimeMillis;
        const id = `scheduled-${yield* deps.uuid}`;
        let definition: TaskDefinition;
        let checks: ScheduledTask["checks"] = [];
        if (isCommandTask(input)) definition = input;
        else {
          const { checkCommand, checkReason, ...agent } = input;
          definition = agent;
          checks = [
            {
              version: 1,
              command: checkCommand,
              actor,
              reason: checkReason,
              createdAt: iso(now),
              revertedFrom: null,
            },
          ];
        }
        const checkCwd = yield* deps.validateTarget(definition);
        if (checks[0]) {
          const tested = yield* deps.check(checks[0].command, {
            taskId: id,
            runId: `${id}:creation`,
            date: iso(now).slice(0, 10),
            cwd: checkCwd,
          });
          if (tested.passed)
            return yield* fail("Outcome check already passes. Task creation refused.");
        }
        const existing = yield* deps.tasks;
        const task = yield* Effect.try({
          try: (): ScheduledTask => {
            const task: ScheduledTask = {
              owner,
              createdBy: actor,
              checkCwd,
              id,
              revision: 1,
              definition,
              createdAt: iso(now),
              updatedAt: iso(now),
              paused: false,
              deleted: false,
              checks,
              choices: chooseMinutes(definition, existing, now),
              consumedSlot: null,
              runs: [],
              failureStreak: 0,
              lastError: null,
            };
            taskSlots(task, now);
            return task;
          },
          catch: () => new SchedulerError({ detail: "Invalid time zone or calendar schedule." }),
        });
        yield* deps.save(undefined, task);
        yield* deps.wake;
        return present(task, now);
      }),
    );
  const edit = (raw: EditScheduledTask, actor: string) =>
    locked(
      raw.taskId,
      Effect.gen(function* () {
        const input = yield* decodeEdit(raw).pipe(
          Effect.mapError(() => new SchedulerError({ detail: "Invalid scheduled task edit." })),
        );
        const task = yield* lookup(input.taskId);
        if (task.runs.some((run) => !terminal(run) && run.threadId === actor))
          return yield* fail(
            "A judged thread cannot edit its own scheduled task or outcome check.",
          );
        if (input.definition && isCommandTask(input.definition) !== isCommandTask(task.definition))
          return yield* fail("A task's kind cannot change. Create a new task instead.");
        if (
          isCommandTask(task.definition) &&
          (input.checkCommand !== undefined ||
            input.checkReason !== undefined ||
            input.revertVersion !== undefined)
        )
          return yield* fail(
            "Command tasks have no outcome check. Change the command in the definition.",
          );
        let checks = task.checks;
        if (input.checkCommand !== undefined || input.revertVersion !== undefined) {
          if (!input.checkReason) return yield* fail("Check edits and reverts require a reason.");
          if (input.checkCommand !== undefined && input.revertVersion !== undefined)
            return yield* fail("Choose a new check or a revert, not both.");
          const previous =
            input.revertVersion === undefined
              ? undefined
              : (checks.find((check) => check.version === input.revertVersion) ??
                (yield* deps.historyVersion(task.id, input.revertVersion)));
          const command = input.checkCommand ?? previous?.command;
          if (!command) return yield* fail("Check version not found.");
          const version: TaskCheckVersion = {
            version: checks.at(-1)!.version + 1,
            command,
            actor,
            reason: input.checkReason,
            createdAt: iso(yield* Clock.currentTimeMillis),
            revertedFrom: input.revertVersion ?? null,
          };
          checks = [...checks, version];
        }
        const definition = input.definition ?? task.definition;
        const checkCwd = yield* deps.validateTarget(definition);
        const now = yield* Clock.currentTimeMillis;
        const existing = (yield* deps.tasks).filter((entry) => entry.id !== task.id);
        const choices = yield* Effect.try({
          try: () => {
            const choices = input.definition
              ? chooseMinutes(definition, existing, now)
              : task.choices;
            taskSlots({ ...task, definition, choices }, now);
            return choices;
          },
          catch: () => new SchedulerError({ detail: "Invalid time zone or calendar schedule." }),
        });
        const next = yield* store(task, { definition, checks, choices, checkCwd });
        yield* deps.wake;
        return present(next, now);
      }),
    );
  const alter = (id: string, actor: string, patch: Partial<ScheduledTask>) =>
    locked(
      id,
      Effect.gen(function* () {
        const task = yield* lookup(id);
        if (task.runs.some((run) => !terminal(run) && run.threadId === actor))
          return yield* fail("A judged thread cannot pause or delete its own task.");
        if (patch.deleted) {
          for (const run of task.runs.filter((entry) => !terminal(entry))) {
            const observation = yield* deps.observe(task, run);
            if (
              run.status !== "needs-you" ||
              observation.active ||
              observation.blocked ||
              observation.pendingDrafters ||
              observation.pendingReports ||
              (!observation.idle && !observation.retired)
            )
              return yield* fail("Task has live or pending unfinished work and cannot be deleted.");
          }
        }
        const next = yield* store(task, patch);
        yield* deps.wake;
        return present(next, yield* Clock.currentTimeMillis);
      }),
    );
  const runNow = (id: string) =>
    locked(
      id,
      Effect.gen(function* () {
        const task = yield* lookup(id);
        const now = yield* Clock.currentTimeMillis;
        if (task.paused) return yield* fail("Resume this task before running it now.");
        const unfinished = task.runs.find((run) => !terminal(run));
        let next: ScheduledTask;
        if (unfinished?.status === "needs-you") {
          const observation = yield* deps.observe(task, unfinished);
          if (observation.retired)
            return yield* fail(
              "Recover the retired thread explicitly before resuming its pinned run.",
            );
          next = yield* resume(task, unfinished, iso(now), now, observation);
        } else next = yield* claim(task, iso(now), now);
        yield* deps.wake;
        return present(next, now);
      }),
    );
  const reconcile = Effect.fn("Scheduler.reconcile")(function* () {
    const tasks = yield* deps.tasks;
    yield* Effect.forEach(
      tasks.filter((task) => !task.deleted),
      (task) =>
        drive(task.id).pipe(
          Effect.catch((error) =>
            Effect.logWarning("Scheduler reconciliation failed", {
              taskId: task.id,
              error: error.detail,
            }),
          ),
        ),
      { concurrency: "unbounded", discard: true },
    );
    const now = yield* Clock.currentTimeMillis;
    const deadlines = (yield* deps.tasks)
      .filter((task) => !task.deleted && !task.paused)
      .flatMap((task) => {
        const next = taskSlots(task, now).next;
        return [
          ...(next === null ? [] : [next]),
          ...task.runs.flatMap((run) =>
            terminal(run) || run.status === "needs-you"
              ? []
              : [run.leaseUntil, ...(run.retryAt === null ? [] : [run.retryAt])],
          ),
        ];
      })
      .filter((time) => time > now);
    return deadlines.length ? Math.min(...deadlines) : null;
  });
  return {
    list: Effect.all([deps.tasks, Clock.currentTimeMillis]).pipe(
      Effect.map(([tasks, now]) =>
        tasks.filter((task) => !task.deleted).map((task) => present(task, now)),
      ),
    ),
    create,
    edit,
    pause: (id: string, paused: boolean, actor: string) => alter(id, actor, { paused }),
    delete: (id: string, actor: string) => alter(id, actor, { deleted: true }),
    runNow,
    reconcile,
    /** Completes when every command started by this process has recorded its result. */
    drainCommands: FiberSet.awaitEmpty(commands),
  };
});
