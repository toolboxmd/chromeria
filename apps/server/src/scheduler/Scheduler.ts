import {
  SchedulerError,
  ThreadId,
  type ScheduledTask,
  type TaskRun,
  type CreateScheduledTask,
  type EditScheduledTask,
  type TaskCheckResult,
  type TaskCheckVersion,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Semaphore from "effect/Semaphore";
import * as Schema from "effect/Schema";
import {
  CreateScheduledTask as CreateSchema,
  EditScheduledTask as EditSchema,
} from "@t3tools/contracts";
import { chooseMinutes, iso, taskSlots } from "./Schedule.ts";

export const RECOVERY_DELAYS = [30_000, 60_000, 300_000, 900_000, 3_600_000] as const;
export const LEASE_MS = 120_000;
const decodeCreate = Schema.decodeUnknownEffect(CreateSchema);
const decodeEdit = Schema.decodeUnknownEffect(EditSchema);
const terminal = (run: TaskRun) => run.status === "done" || run.status === "needs-you";
const fail = (detail: string) => Effect.fail(new SchedulerError({ detail }));
export type RunObservation = {
  readonly active: boolean;
  readonly idle: boolean;
  readonly retired: boolean;
  readonly hasWork: boolean;
  readonly pendingDrafters: boolean;
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
  readonly historyVersion: (
    taskId: string,
    version: number,
  ) => Effect.Effect<TaskCheckVersion | undefined, SchedulerError>;
  readonly validateTarget: (
    task: CreateScheduledTask["target"],
  ) => Effect.Effect<string, SchedulerError>;
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
      const runs = candidate.runs.filter(
        (run, index) => !terminal(run) || index >= candidate.runs.length - 20,
      );
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
      const run: TaskRun = {
        definition: task.definition,
        checkCwd: task.checkCwd,
        id: `${task.id}:${slot}`,
        slot,
        checkVersion: task.checks.at(-1)!.version,
        threadId: task.definition.target.kind === "thread" ? task.definition.target.threadId : null,
        status: "claimed",
        processId: deps.processId,
        originSequence: yield* deps.sequence,
        sendIndex: 0,
        attempt: 0,
        hasWork: false,
        leaseUntil: now + LEASE_MS,
        retryAt: null,
        dispatchedAt: null,
        observedTurnId: null,
        error: null,
        check: null,
        drafterIds: [],
      };
      return yield* store(task, { consumedSlot: slot, runs: [...task.runs, run] });
    });
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
  const drive = (id: string) =>
    locked(
      id,
      Effect.gen(function* () {
        let task = yield* lookup(id);
        const now = yield* Clock.currentTimeMillis;
        const slots = taskSlots(task, now);
        if (
          !task.paused &&
          slots.latest !== null &&
          (task.consumedSlot === null || slots.latest > Date.parse(task.consumedSlot))
        ) {
          const slot = iso(slots.latest);
          task = task.runs.some((run) => !terminal(run))
            ? yield* store(task, { consumedSlot: slot })
            : yield* claim(task, slot, now);
        }
        let run = task.runs.find((entry) => !terminal(entry));
        if (!run || task.paused) return;
        const observation = yield* deps.observe(task, run);
        if (observation.retired) {
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
          yield* retry(
            task,
            { ...run, check, hasWork: hasWork || !observation.error },
            observation.error ?? `Outcome check failed:\n${verdict.output}`,
            now,
          );
          return;
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
        const continuing = hasWork || run.status === "usage-limit";
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
          : `${context}${run.definition.prompt}`;
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
  const create = (raw: CreateScheduledTask, actor: string) =>
    creationLock.withPermits(1)(
      Effect.gen(function* () {
        const input = yield* decodeCreate(raw).pipe(
          Effect.mapError(
            () =>
              new SchedulerError({
                detail: "A valid schedule, target and nonempty outcome check are required.",
              }),
          ),
        );
        const checkCwd = yield* deps.validateTarget(input.target);
        const now = yield* Clock.currentTimeMillis;
        const id = `scheduled-${yield* deps.uuid}`;
        const tested = yield* deps.check(input.checkCommand, {
          taskId: id,
          runId: `${id}:creation`,
          date: iso(now).slice(0, 10),
          cwd: checkCwd,
        });
        if (tested.passed)
          return yield* fail("Outcome check already passes. Task creation refused.");
        const { checkCommand, checkReason, ...definition } = input;
        const existing = yield* deps.tasks;
        const task = yield* Effect.try({
          try: (): ScheduledTask => {
            const task: ScheduledTask = {
              checkCwd,
              id,
              revision: 1,
              definition,
              createdAt: iso(now),
              updatedAt: iso(now),
              paused: false,
              deleted: false,
              checks: [
                {
                  version: 1,
                  command: checkCommand,
                  actor,
                  reason: checkReason,
                  createdAt: iso(now),
                  revertedFrom: null,
                },
              ],
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
        return task;
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
        const checkCwd = yield* deps.validateTarget(definition.target);
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
        return next;
      }),
    );
  const alter = (id: string, actor: string, patch: Partial<ScheduledTask>) =>
    locked(
      id,
      Effect.gen(function* () {
        const task = yield* lookup(id);
        if (task.runs.some((run) => !terminal(run) && run.threadId === actor))
          return yield* fail("A judged thread cannot pause or delete its own task.");
        if (patch.deleted && task.runs.some((run) => !terminal(run)))
          return yield* fail(
            "Task has unfinished work. Pause it before deleting after settlement.",
          );
        const next = yield* store(task, patch);
        yield* deps.wake;
        return next;
      }),
    );
  const runNow = (id: string) =>
    locked(
      id,
      Effect.gen(function* () {
        const task = yield* lookup(id);
        const now = yield* Clock.currentTimeMillis;
        if (task.paused) return yield* fail("Resume this task before running it now.");
        const next = yield* claim(task, iso(now), now);
        yield* deps.wake;
        return next;
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
            terminal(run) ? [] : [run.leaseUntil, ...(run.retryAt === null ? [] : [run.retryAt])],
          ),
        ];
      })
      .filter((time) => time > now);
    return deadlines.length ? Math.min(...deadlines) : null;
  });
  return {
    list: deps.tasks.pipe(Effect.map((tasks) => tasks.filter((task) => !task.deleted))),
    create,
    edit,
    pause: (id: string, paused: boolean, actor: string) => alter(id, actor, { paused }),
    delete: (id: string, actor: string) => alter(id, actor, { deleted: true }),
    runNow,
    reconcile,
  };
});
