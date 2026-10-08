import { assert, it } from "@effect/vitest";
import {
  ProjectId,
  ScheduledTaskId,
  ThreadId,
  type ModelSelection,
  type ScheduledTask,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import {
  makeCheckedRuns,
  ScheduledTaskCheckError,
  type CommandOutcome,
  type RunObservation,
} from "./engine.ts";
import type { ReportFence } from "./handoff.ts";
import { checkedRunStatus, outcomeCheckSummary, type CheckState } from "./state.ts";
import { deleteCheckState, ensureCheckSchema, readCheckState, writeCheckState } from "./store.ts";

const toJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const NOW = Date.parse("2026-10-08T12:00:00.000Z");
const taskId = ScheduledTaskId.make("task:checked");
const threadId = ThreadId.make("thread:bound");
const selection: ModelSelection = { instanceId: "codex" as never, model: "gpt-5" };

const task = (overrides: Partial<ScheduledTask> = {}): ScheduledTask => ({
  id: taskId,
  title: "Checked",
  prompt: "Do the work",
  enabled: true,
  schedule: { type: "interval", everyMs: 3_600_000 },
  projectId: ProjectId.make("project"),
  threadId,
  workspaceStrategy: { type: "root" },
  modelSelection: selection,
  runtimeMode: "full-access",
  interactionMode: "default",
  createdBy: "agent",
  creationSource: "mcp",
  createdAt: "2026-10-01T00:00:00.000Z",
  updatedAt: "2026-10-01T00:00:00.000Z",
  nextRunAt: null,
  lastRunAt: null,
  lastRunStatus: "never",
  lastRunError: null,
  runCount: 0,
  ...overrides,
});

const idle: RunObservation = {
  unavailable: false,
  threadMissing: false,
  landed: true,
  started: true,
  busy: false,
  blocked: false,
  usageLimit: null,
  runError: null,
};

/** Upstream's row for a task, as the policy's freshness check reads it. */
const storeRow = (current: ScheduledTask) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO scheduled_tasks ${sql.insert({
      task_id: current.id,
      title: current.title,
      prompt: current.prompt,
      enabled: current.enabled ? 1 : 0,
      schedule_json: toJson(current.schedule),
      project_id: current.projectId,
      thread_id: current.threadId,
      workspace_strategy_json: '{"type":"root"}',
      model_selection_json: toJson(current.modelSelection),
      runtime_mode: current.runtimeMode,
      interaction_mode: current.interactionMode,
      created_by: current.createdBy,
      creation_source: current.creationSource,
      created_at: current.createdAt,
      updated_at: current.updatedAt,
      next_run_at: current.nextRunAt,
      last_run_at: null,
      last_run_status: "never",
      last_run_error: null,
      run_count: 0,
    })} ON CONFLICT (task_id) DO UPDATE SET
      schedule_json = excluded.schedule_json, prompt = excluded.prompt,
      thread_id = excluded.thread_id, created_at = excluded.created_at,
      updated_at = excluded.updated_at, next_run_at = excluded.next_run_at`;
  });

/** Real store and engine; observation, dispatch and checks are scripted. */
const harness = (options: { readonly role?: string } = {}) =>
  Effect.gen(function* () {
    yield* ensureCheckSchema;
    const sent: Array<{
      readonly commandId: string;
      readonly messageId: string;
      readonly kind: string;
      readonly text: string;
      readonly modelSelection: ModelSelection | null;
    }> = [];
    const checks: Array<string> = [];
    const creationTests: Array<string> = [];
    const executed: Array<string> = [];
    let commandDone = yield* Deferred.make<CommandOutcome>();
    let workspaceHold: {
      readonly reached: Deferred.Deferred<void>;
      readonly release: Deferred.Deferred<void>;
    } | null = null;
    let observation: RunObservation = { ...idle, landed: false, started: false };
    let passes = false;
    let dispatchFails = false;
    let fence: ReportFence = { kind: "released" };
    let workIsOpen = false;
    let reopenOnCheck = false;
    let fenceOnCheck: ReportFence | null = null;
    const runs = yield* makeCheckedRuns({
      changed: Effect.void,
      workOpen: () => Effect.sync(() => workIsOpen),
      reportsFence: () => Effect.sync(() => fence),
      observe: () => Effect.sync(() => observation),
      dispatch: ({ send }) =>
        Effect.suspend(() =>
          dispatchFails
            ? Effect.fail(new ScheduledTaskCheckError({ message: "dispatch refused" }))
            : Effect.sync(() => {
                sent.push({
                  commandId: send.commandId,
                  messageId: send.messageId,
                  kind: send.kind,
                  text: send.payload!.text,
                  modelSelection: send.payload!.modelSelection,
                });
              }),
        ),
      start: ({ command, cwd, runId }) =>
        Effect.sync(() => {
          executed.push(`${command}@${cwd}#${runId}`);
          // A command finishes only when the test releases it.
          return Effect.suspend(() => Deferred.await(commandDone));
        }),
      runCheck: ({ command, cwd, runId }) =>
        Effect.sync(() => {
          // The creation test is a separate record from run verdicts.
          (runId.endsWith(":creation") ? creationTests : checks).push(`${command}@${cwd}`);
          // Recovery may resume the run's work, or a report change, while its check runs.
          if (reopenOnCheck) workIsOpen = true;
          if (fenceOnCheck !== null) fence = fenceOnCheck;
          return { passed: passes, output: passes ? "ok" : "still missing" };
        }),
      workspace: ({ threadId: bound }) =>
        Effect.gen(function* () {
          const hold = workspaceHold;
          if (hold !== null) {
            yield* Deferred.succeed(hold.reached, undefined);
            yield* Deferred.await(hold.release);
          }
          return bound === null ? "/project" : "/worktree";
        }),
      role: (state, _task, launching) =>
        state.role === null
          ? Effect.succeed(null)
          : options.role === undefined
            ? Effect.fail(
                new ScheduledTaskCheckError({ message: `no Prism pick for ${state.role}` }),
              )
            : Effect.succeed({
                // A post only takes the kit; a launch takes Prism's model too.
                modelSelection: launching ? { ...selection, model: options.role } : selection,
                kitText: "KIT",
              }),
    });
    const save = (fields: Record<string, unknown>, actorThreadId: ThreadId | null = null) =>
      runs.save({
        taskId,
        projectId: ProjectId.make("project"),
        threadId,
        schedule: { type: "interval" },
        fields,
        actor: "agent-thread",
        actorThreadId,
      });
    // Fires act only on a task that still matches upstream's row, so each one is stored.
    const decide = (trigger: "scheduled" | "manual", at = NOW, current = task()) =>
      storeRow(current).pipe(
        Effect.andThen(runs.decide({ task: current, trigger, startedAt: DateTime.makeUnsafe(at) })),
      );
    const fire = (trigger: "scheduled" | "manual", at = NOW, current = task()) =>
      Effect.gen(function* () {
        const decision = yield* decide(trigger, at, current);
        if (decision._tag === "fork") yield* Effect.exit(decision.dispatch);
        return decision;
      });
    const state = () => readCheckState(taskId).pipe(Effect.map((value) => value!));
    return {
      runs,
      sent,
      checks,
      creationTests,
      executed,
      finishCommand: (outcome: CommandOutcome) =>
        Deferred.succeed(commandDone, outcome).pipe(
          Effect.andThen(runs.drainCommands),
          Effect.andThen(
            Deferred.make<CommandOutcome>().pipe(
              Effect.tap((next) =>
                Effect.sync(() => {
                  commandDone = next;
                }),
              ),
            ),
          ),
        ),
      save,
      decide,
      fire,
      state,
      set: (next: Partial<RunObservation>) => {
        observation = { ...observation, ...next };
      },
      pass: (value: boolean) => {
        passes = value;
      },
      failDispatch: (value: boolean) => {
        dispatchFails = value;
      },
      reports: (next: ReportFence) => {
        fence = next;
      },
      openWork: (value: boolean) => {
        workIsOpen = value;
      },
      reopenDuringCheck: (value: boolean) => {
        reopenOnCheck = value;
      },
      reportsDuringCheck: (next: ReportFence | null) => {
        fenceOnCheck = next;
      },
      /** Holds the next workspace lookups until the test releases them. */
      holdWorkspace: Effect.gen(function* () {
        const hold = {
          reached: yield* Deferred.make<void>(),
          release: yield* Deferred.make<void>(),
        };
        workspaceHold = hold;
        return hold;
      }),
    };
  });

/** Five delayed retries, then needs-you: six failed checks and five resends. */
const exhaust = (h: Effect.Success<ReturnType<typeof harness>>) =>
  Effect.gen(function* () {
    for (let step = 0; step < 11; step += 1) {
      yield* h.runs.drive(task());
      yield* TestClock.adjust(3_600_000);
    }
    assert.equal(h.checks.length, 6);
    assert.equal((yield* h.state()).runs[0]?.attempt, 6);
  });

it.effect("creation refuses a check that already passes, and webhook tasks", () =>
  Effect.gen(function* () {
    const h = yield* harness();
    h.pass(true);
    const passing = yield* Effect.exit(
      h.save({ checkCommand: "test -f done", checkReason: "done file" }),
    );
    assert.isTrue(Exit.isFailure(passing));
    assert.include(String(passing), "already passes");
    assert.isNull(yield* readCheckState(taskId));
    const webhook = yield* Effect.exit(
      h.runs.save({
        taskId,
        projectId: ProjectId.make("project"),
        threadId,
        schedule: { type: "webhook" },
        fields: { checkCommand: "x", checkReason: "y" },
        actor: "a",
        actorThreadId: null,
      }),
    );
    assert.isTrue(Exit.isFailure(webhook));
    assert.include(String(webhook), "Webhook tasks");
    h.pass(false);
    yield* h.save({ checkCommand: "test -f done", checkReason: "done file", role: "planner" });
    const stored = yield* h.state();
    assert.equal(stored.checks.length, 1);
    assert.equal(stored.role, "planner");
    // Each creation runs the new check once in the bound thread's workspace.
    assert.deepEqual(h.creationTests, ["test -f done@/worktree", "test -f done@/worktree"]);
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("records each send before dispatch and judges a run only by its pinned check", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(NOW);
    const h = yield* harness();
    yield* h.save({ checkCommand: "check-v1", checkReason: "first" });
    const decision = yield* h.decide("scheduled");
    assert.equal(decision._tag, "fork");
    // Intent is durable before anything is sent.
    const before = yield* h.state();
    assert.equal(before.runs[0]?.sends.length, 1);
    assert.equal(h.sent.length, 0);
    if (decision._tag === "fork") yield* decision.dispatch;
    assert.deepEqual(
      h.sent.map((send) => send.commandId),
      [`scheduled-task-check:${taskId}:2026-10-08T12:00:00.000Z:0`],
    );
    assert.include(h.sent[0]!.text, "Do the work");
    // A new check version does not change what this run is judged by.
    yield* h.save({ checkCommand: "check-v2", checkReason: "stricter" });
    h.set({ landed: true, started: true, busy: false });
    yield* h.runs.drive(task());
    assert.deepEqual(h.checks, ["check-v1@/worktree"]);
    const failed = yield* h.state();
    assert.equal(failed.runs[0]?.stage, "retry");
    assert.equal(failed.runs[0]?.check?.passed, false);
    // The retry continues the same thread after its delay, with the check output.
    yield* h.runs.drive(task());
    assert.equal(h.sent.length, 1);
    yield* TestClock.adjust(30_000);
    yield* h.runs.drive(task());
    assert.equal(h.sent.length, 2);
    assert.equal(h.sent[1]!.kind, "continue");
    assert.include(h.sent[1]!.text, "Continue in this conversation");
    assert.include(h.sent[1]!.text, "still missing");
    h.pass(true);
    yield* h.runs.drive(task());
    const done = yield* h.state();
    assert.equal(done.runs[0]?.stage, "done");
    assert.deepEqual(checkedRunStatus(done), { status: "succeeded", error: null });
    assert.deepEqual(h.checks, ["check-v1@/worktree", "check-v1@/worktree"]);
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("reports a dispatched but unchecked run as running, never as succeeded", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(NOW);
    const h = yield* harness();
    yield* h.save({ checkCommand: "check", checkReason: "outcome" });
    yield* h.fire("scheduled");
    const state = yield* h.state();
    assert.deepEqual(checkedRunStatus(state), { status: "running", error: null });
    assert.equal(outcomeCheckSummary(state).run?.stage, "running");
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect(
  "skips scheduled fires while a run is unfinished and resumes only an eligible needs-you run",
  () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(NOW);
      const h = yield* harness();
      yield* h.save({ checkCommand: "check", checkReason: "outcome" });
      yield* h.fire("scheduled");
      assert.equal((yield* h.decide("scheduled", NOW + 3_600_000))._tag, "skip");
      const manual = yield* Effect.exit(h.decide("manual", NOW + 1));
      assert.isTrue(Exit.isFailure(manual));
      assert.include(String(manual), "unfinished work");
      // Exhaust every retry: each attempt is a failed check, then a resend after its delay.
      h.set({ landed: true, started: true, busy: false });
      yield* exhaust(h);
      const stuck = yield* h.state();
      assert.equal(stuck.runs[0]?.stage, "needs-you");
      assert.isTrue(outcomeCheckSummary(stuck).run?.resumable === true);
      assert.equal(checkedRunStatus(stuck)?.status, "failed");
      // Scheduled fires still skip; only Run now resumes, in the same run and thread.
      assert.equal((yield* h.decide("scheduled", NOW + 99_000_000))._tag, "skip");
      h.set({ unavailable: true });
      const retired = yield* Effect.exit(h.decide("manual", NOW + 99_000_001));
      assert.isTrue(Exit.isFailure(retired));
      assert.include(String(retired), "retired or missing thread");
      h.set({ unavailable: false });
      const sentBefore = h.sent.length;
      const resumed = yield* h.fire("manual", NOW + 99_000_002);
      assert.equal(resumed._tag, "fork");
      const after = yield* h.state();
      assert.equal(after.runs.length, 1);
      assert.equal(after.runs[0]?.stage, "running");
      assert.equal(after.runs[0]?.attempt, 0);
      assert.equal(h.sent.length, sentBefore + 1);
      assert.equal(h.sent.at(-1)!.kind, "continue");
    }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("a one-shot is one run per instant: a replayed fire with another offset is skipped", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(NOW);
    const h = yield* harness();
    yield* h.save({ checkCommand: "check", checkReason: "outcome" });
    const once = task({ schedule: { type: "once", at: "2026-10-08T14:00:00+02:00" } });
    assert.equal((yield* h.fire("scheduled", NOW + 5, once))._tag, "fork");
    h.set({ landed: true, busy: false });
    h.pass(true);
    yield* h.runs.drive(once);
    assert.equal((yield* h.state()).runs[0]?.stage, "done");
    const sameInstant = task({ schedule: { type: "once", at: "2026-10-08T12:00:00Z" } });
    assert.equal((yield* h.decide("scheduled", NOW + 10_000, sameInstant))._tag, "skip");
    const state = yield* h.state();
    assert.deepEqual(
      state.runs.map((run) => run.id),
      [`${taskId}:2026-10-08T12:00:00.000Z`],
    );
    assert.equal(h.sent.length, 1);
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("resends a recorded send that never landed with its own identity, exactly once", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(NOW);
    const h = yield* harness();
    yield* h.save({ checkCommand: "check", checkReason: "outcome" });
    // The decision persisted its send; the process died before dispatching it.
    const decision = yield* h.decide("scheduled");
    assert.equal(decision._tag, "fork");
    assert.equal(h.sent.length, 0);
    h.set({ landed: false, busy: false, started: false });
    yield* h.runs.drive(task());
    h.set({ landed: true, busy: true });
    yield* h.runs.drive(task());
    yield* h.runs.drive(task());
    const recorded = (yield* h.state()).runs[0]!.sends;
    assert.equal(recorded.length, 1);
    assert.deepEqual(
      h.sent.map((send) => [send.commandId, send.messageId]),
      [[recorded[0]!.commandId, recorded[0]!.messageId]],
    );
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("waits for upstream usage-limit recovery and resumes itself only when nothing will", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(NOW);
    const h = yield* harness();
    yield* h.save({ checkCommand: "check", checkReason: "outcome" });
    yield* h.fire("scheduled");
    const resetAt = NOW + 60_000;
    h.set({ landed: true, usageLimit: { resetAt, upstreamResumes: true }, runError: "limit" });
    yield* h.runs.drive(task());
    assert.equal((yield* h.state()).runs[0]?.stage, "usage-limit");
    yield* TestClock.adjust(120_000);
    yield* h.runs.drive(task());
    assert.equal(h.sent.length, 1, "upstream recovery owns the resume");
    // After upstream resumed and the thread went idle, the pinned check decides.
    h.set({ usageLimit: null, runError: null });
    h.pass(true);
    yield* h.runs.drive(task());
    assert.equal((yield* h.state()).runs[0]?.stage, "done");
    assert.deepEqual(h.checks, ["check@/worktree"]);

    // Without upstream recovery the run resumes its own thread at the reset.
    const h2 = yield* harness();
    yield* h2.fire("manual", NOW + 200_000);
    h2.set({
      landed: true,
      started: true,
      usageLimit: { resetAt: NOW + 300_000, upstreamResumes: false },
    });
    yield* h2.runs.drive(task());
    assert.equal(h2.sent.length, 1);
    yield* TestClock.adjust(200_000);
    yield* h2.runs.drive(task());
    assert.equal(h2.sent.length, 2);
    assert.equal(h2.sent[1]!.kind, "continue");
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("waits while blocked on the user and marks an unavailable thread needs-you", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(NOW);
    const h = yield* harness();
    yield* h.save({ checkCommand: "check", checkReason: "outcome" });
    yield* h.fire("scheduled");
    h.set({ landed: true, blocked: true });
    yield* h.runs.drive(task());
    assert.deepEqual(h.checks, []);
    h.set({ blocked: false, unavailable: true });
    yield* h.runs.drive(task());
    const state = yield* h.state();
    assert.equal(state.runs[0]?.stage, "needs-you");
    assert.include(state.runs[0]?.error ?? "", "retired or unavailable");
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("a role without a Prism pick fails visibly instead of using the task's model", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(NOW);
    const unwired = yield* harness();
    yield* unwired.save({ checkCommand: "check", checkReason: "outcome", role: "planner" });
    const decision = yield* unwired.decide("scheduled");
    assert.equal(decision._tag, "fork");
    if (decision._tag === "fork") {
      const exit = yield* Effect.exit(decision.dispatch);
      assert.isTrue(Exit.isFailure(exit));
    }
    assert.equal(unwired.sent.length, 0);
    const state = yield* unwired.state();
    assert.equal(state.runs[0]?.stage, "retry");
    assert.include(state.runs[0]?.error ?? "", "no Prism pick for planner");
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("a Prism pick selects the model of a new thread only and prefixes its kit", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(NOW);
    const wired = yield* harness({ role: "picked-model" });
    yield* wired.save({ checkCommand: "check", checkReason: "outcome", role: "worker" });
    yield* wired.fire("scheduled", NOW, task({ threadId: null }));
    assert.equal(wired.sent[0]?.modelSelection?.model, "picked-model");
    assert.match(wired.sent[0]!.text, /KIT\n\nDo the work$/);
    // Posting to a bound thread keeps that thread's stored model and effort.
    const bound = yield* harness({ role: "picked-model" });
    yield* bound.runs.removeTask(taskId, null, Effect.void);
    yield* bound.save({ checkCommand: "check", checkReason: "outcome", role: "worker" });
    yield* bound.fire("manual", NOW + 1);
    assert.equal(bound.sent.length, 1);
    assert.isNull(bound.sent[0]!.modelSelection);
    assert.match(bound.sent[0]!.text, /KIT\n\nDo the work$/);
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("a resend after a crash repeats the pinned payload even after the task changed", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(NOW);
    const h = yield* harness();
    yield* h.save({ checkCommand: "check", checkReason: "outcome" });
    // Recorded, then the process died before dispatching.
    const decision = yield* h.decide("scheduled");
    assert.equal(decision._tag, "fork");
    h.set({ landed: false, busy: false });
    yield* h.runs.drive(task({ prompt: "An edited prompt", title: "Edited" }));
    assert.equal(h.sent.length, 1);
    assert.include(h.sent[0]!.text, "Do the work");
    assert.notInclude(h.sent[0]!.text, "An edited prompt");
    const recorded = (yield* h.state()).runs[0]!.sends[0]!;
    assert.equal(h.sent[0]!.commandId, recorded.commandId);
    assert.equal(recorded.payload?.text, h.sent[0]!.text);
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("Run now refuses to resume while the thread is busy, queued or waiting on the user", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(NOW);
    const h = yield* harness();
    yield* h.save({ checkCommand: "check", checkReason: "outcome" });
    yield* h.fire("scheduled");
    h.set({ landed: true, busy: false });
    yield* exhaust(h);
    assert.equal((yield* h.state()).runs[0]?.stage, "needs-you");
    const sentBefore = h.sent.length;
    for (const blocker of [
      { busy: true },
      { blocked: true },
      { usageLimit: { resetAt: null, upstreamResumes: false } },
    ] as const) {
      h.set({ busy: false, blocked: false, usageLimit: null, ...blocker });
      const refused = yield* Effect.exit(h.decide("manual", NOW + 99_000_000));
      assert.isTrue(Exit.isFailure(refused));
      assert.include(String(refused), "busy or waiting on you");
    }
    assert.equal(h.sent.length, sentBefore);
    assert.equal((yield* h.state()).runs[0]?.stage, "needs-you");
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("a judged thread cannot edit, pause or delete its own task", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(NOW);
    const h = yield* harness();
    yield* h.save({ checkCommand: "check", checkReason: "outcome" });
    yield* h.fire("scheduled");
    const edit = yield* Effect.exit(
      h.save({ checkCommand: "true", checkReason: "cheat" }, threadId),
    );
    assert.isTrue(Exit.isFailure(edit));
    yield* h.save({ checkCommand: "check-v2", checkReason: "user edit" }, ThreadId.make("other"));
    assert.equal((yield* h.state()).checks.length, 2);
    // The delete's guard refuses before upstream's delete runs; anyone else removes both.
    let upstreamDeletes = 0;
    const deleteUpstream = Effect.sync(() => {
      upstreamDeletes += 1;
    });
    const guard = yield* Effect.exit(h.runs.removeTask(taskId, threadId, deleteUpstream));
    assert.isTrue(Exit.isFailure(guard));
    assert.equal(upstreamDeletes, 0);
    assert.isNotNull(yield* readCheckState(taskId));
    yield* h.runs.removeTask(taskId, ThreadId.make("someone-else"), deleteUpstream);
    assert.equal(upstreamDeletes, 1);
    assert.isNull(yield* readCheckState(taskId));
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("a send admitted before its task was deleted sends nothing", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(NOW);
    const h = yield* harness();
    yield* h.save({ checkCommand: "check", checkReason: "outcome" });
    // Upstream runs an admitted dispatch after marking the task running; the delete lands first.
    const send = yield* h.decide("scheduled");
    assert.equal(send._tag, "fork");
    assert.equal((yield* h.state()).runs[0]?.sends.length, 1);
    yield* deleteCheckState(taskId);
    if (send._tag === "fork") yield* send.dispatch;
    assert.deepEqual(h.sent, []);
    assert.isNull(yield* readCheckState(taskId));
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("a command admitted before its task was deleted runs nothing", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(NOW);
    const h = yield* harness();
    const command = task({ threadId: null });
    yield* h.save({ command: "backup" });
    const spawn = yield* h.decide("scheduled", NOW, command);
    assert.equal(spawn._tag, "fork");
    yield* deleteCheckState(taskId);
    if (spawn._tag === "fork") yield* spawn.dispatch;
    // Any process started would finish here and be counted.
    yield* h.finishCommand({ exitCode: 0, output: "ok", timedOut: false, failure: null });
    assert.deepEqual(h.executed, []);
    assert.isNull(yield* readCheckState(taskId));
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("a report that comes to need you after a send is admitted holds the send", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(NOW);
    const h = yield* harness();
    yield* h.save({ checkCommand: "check", checkReason: "outcome" });
    const reason = "The Spectrum report run failed and will not be retried";
    // Admitted while its reports were released; one needs you before the send goes out.
    const held = yield* h.decide("scheduled");
    h.reports({ kind: "needs-you", reason });
    if (held._tag === "fork") yield* held.dispatch;
    assert.deepEqual(h.sent, []);
    const needsYou = (yield* h.state()).runs[0]!;
    assert.equal(needsYou.stage, "needs-you");
    assert.equal(needsYou.error, `Spectrum report: ${reason}`);
    assert.equal(needsYou.sends.length, 1);
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("a command whose task is deleted while its workspace is found never starts", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(NOW);
    const h = yield* harness();
    const command = task({ threadId: null });
    yield* h.save({ command: "backup" });
    const spawn = yield* h.decide("scheduled", NOW, command);
    assert.equal(spawn._tag, "fork");
    const hold = yield* h.holdWorkspace;
    // Admitted, owned and detached: its process fiber is finding the workspace.
    if (spawn._tag === "fork") yield* spawn.dispatch;
    yield* Deferred.await(hold.reached);
    const sql = yield* SqlClient.SqlClient;
    yield* h.runs.removeTask(
      taskId,
      null,
      sql`DELETE FROM scheduled_tasks WHERE task_id = ${taskId}`,
    );
    assert.isNull(yield* readCheckState(taskId));
    yield* Deferred.succeed(hold.release, undefined);
    // Any process started would finish here and be counted.
    yield* h.finishCommand({ exitCode: 0, output: "ok", timedOut: false, failure: null });
    assert.deepEqual(h.executed, []);
    assert.deepEqual(yield* sql`SELECT task_id FROM scheduled_tasks`, []);
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect(
  "a report still waiting when an admitted send's turn comes keeps the send for redelivery",
  () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(NOW);
      const h = yield* harness();
      yield* h.save({ checkCommand: "check", checkReason: "outcome" });
      // Admitted while its reports were released; one is waiting when the send's turn comes.
      const waiting = yield* h.decide("scheduled");
      h.reports({ kind: "waiting" });
      if (waiting._tag === "fork") yield* waiting.dispatch;
      assert.deepEqual(h.sent, []);
      const kept = (yield* h.state()).runs[0]!;
      assert.equal(kept.stage, "running");
      assert.equal(kept.sends.length, 1);
      h.reports({ kind: "released" });
      h.set({ landed: false, busy: false });
      yield* h.runs.drive(task());
      assert.deepEqual(
        h.sent.map((sent) => sent.commandId),
        [kept.sends[0]!.commandId],
      );
    }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("check edits require a reason and reverts copy an earlier version", () =>
  Effect.gen(function* () {
    const h = yield* harness();
    yield* h.save({ checkCommand: "check-v1", checkReason: "first" });
    assert.isTrue(Exit.isFailure(yield* Effect.exit(h.save({ checkCommand: "check-v2" }))));
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(
          h.save({ checkCommand: "x", revertCheckVersion: 1, checkReason: "both" }),
        ),
      ),
    );
    yield* h.save({ checkCommand: "check-v2", checkReason: "second" });
    yield* h.save({ revertCheckVersion: 1, checkReason: "v2 was wrong" });
    const checks = (yield* h.state()).checks;
    assert.deepEqual(
      checks.map((check) => [check.version, check.command, check.revertedFrom]),
      [
        [1, "check-v1", null],
        [2, "check-v2", null],
        [3, "check-v1", 1],
      ],
    );
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("imported runs stay inert history and Run now starts a new run", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(NOW);
    const h = yield* harness();
    const imported: CheckState = {
      version: 1,
      taskId,
      revision: 0,
      kind: "agent",
      command: null,
      role: null,
      lane: null,
      checks: [
        {
          version: 1,
          command: "check",
          actor: "user:me",
          reason: "outcome",
          createdAt: "2026-10-07T10:00:00.000Z",
          revertedFrom: null,
        },
      ],
      runs: [
        {
          id: `${taskId}:2026-10-08T08:00:00.000Z`,
          slot: "2026-10-08T08:00:00.000Z",
          checkVersion: 1,
          threadId,
          checkCwd: "/old",
          stage: "needs-you",
          attempt: 6,
          retryAt: null,
          hasWork: true,
          sends: [],
          error: "gave up",
          check: null,
          imported: { from: "v1", status: "needs-you" },
        },
      ],
      failureStreak: 6,
      lastError: "gave up",
      lastSuccessfulRunId: null,
    };
    yield* writeCheckState(null, imported);
    const summary = outcomeCheckSummary(yield* h.state());
    assert.isTrue(summary.run?.imported === true);
    assert.isFalse(summary.run?.resumable === true);
    assert.equal(checkedRunStatus(yield* h.state())?.status, "failed");
    // The reactor never drives it, and a scheduled fire is not blocked by it.
    yield* h.runs.drive(task());
    assert.equal(h.sent.length, 0);
    const decision = yield* h.fire("manual");
    assert.equal(decision._tag, "fork");
    const state = yield* h.state();
    assert.equal(state.runs.length, 2);
    assert.equal(state.runs[0]?.imported?.from, "v1");
    assert.equal(state.runs[0]?.stage, "needs-you");
    assert.equal(state.runs[1]?.stage, "running");
    assert.equal(h.sent[0]!.kind, "start");
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("a command task runs once per run, detached, and only its newest failure notifies", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(NOW);
    const h = yield* harness();
    const command = task({ threadId: null });
    yield* h.save({ command: "backup --date {date}" });
    const refused = yield* Effect.exit(h.save({ checkCommand: "x", checkReason: "y" }));
    assert.isTrue(Exit.isFailure(refused));
    assert.include(String(refused), "no outcome check");
    // The run is stored as running before its process starts, and dispatch returns at once.
    const decision = yield* h.decide("scheduled", NOW, command);
    assert.equal(decision._tag, "fork");
    const recorded = yield* h.state();
    assert.equal(recorded.runs[0]?.stage, "running");
    // Dispatch returns while the command is still running.
    if (decision._tag === "fork") yield* decision.dispatch;
    assert.deepEqual(checkedRunStatus(yield* h.state()), { status: "running", error: null });
    // While it runs, a scheduled fire skips and Run now is refused; no agent is ever sent.
    assert.equal((yield* h.decide("scheduled", NOW + 3_600_000, command))._tag, "skip");
    assert.isTrue(Exit.isFailure(yield* Effect.exit(h.decide("manual", NOW + 1, command))));
    yield* h.finishCommand({ exitCode: 2, output: "disk full", timedOut: false, failure: null });
    assert.deepEqual(h.executed, [
      `backup --date {date}@/project#${taskId}:2026-10-08T12:00:00.000Z`,
    ]);
    const failed = yield* h.state();
    assert.equal(failed.runs[0]?.stage, "needs-you");
    assert.equal(failed.failureStreak, 1);
    assert.equal(failed.runs[0]?.commandResult?.exitCode, 2);
    assert.equal(failed.runs[0]?.error, "The command exited with code 2.");
    // A failed command run is settled: the next slot starts a new run.
    yield* TestClock.adjust(3_600_000);
    const next = yield* h.decide("scheduled", NOW + 3_600_000, command);
    assert.equal(next._tag, "fork");
    if (next._tag === "fork") yield* next.dispatch;
    yield* h.finishCommand({ exitCode: 0, output: "ok", timedOut: false, failure: null });
    const passed = yield* h.state();
    assert.equal(passed.failureStreak, 0);
    assert.equal(passed.lastSuccessfulRunId, passed.runs[1]?.id ?? null);
    assert.equal(h.sent.length, 0);
    assert.deepEqual(h.checks, []);
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("a command run left running by an earlier process needs you and is not run again", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(NOW);
    const before = yield* harness();
    yield* before.save({ command: "sync" });
    const command = task({ threadId: null });
    // Recorded, then the process died before its result was stored.
    yield* before.decide("scheduled", NOW, command);
    const restarted = yield* harness();
    yield* restarted.runs.drive(command);
    const state = yield* restarted.state();
    assert.equal(state.runs[0]?.stage, "needs-you");
    assert.include(state.runs[0]?.error ?? "", "it was not run again");
    assert.equal(state.failureStreak, 1);
    assert.deepEqual(restarted.executed, []);
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("a fire that read an older definition is not acted on", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(NOW);
    const h = yield* harness();
    yield* h.save({ checkCommand: "check", checkReason: "outcome" });
    yield* storeRow(task());
    // The task was edited after upstream read it: its row has a newer updated_at.
    const stale = task({ updatedAt: "2026-09-30T00:00:00.000Z" });
    const sql = yield* SqlClient.SqlClient;
    yield* sql`UPDATE scheduled_tasks SET updated_at = ${"2026-10-02T00:00:00.000Z"}
      WHERE task_id = ${taskId}`;
    assert.equal(
      (yield* h.runs.decide({
        task: stale,
        trigger: "scheduled",
        startedAt: DateTime.makeUnsafe(NOW),
      }))._tag,
      "skip",
    );
    const manual = yield* Effect.exit(
      h.runs.decide({ task: stale, trigger: "manual", startedAt: DateTime.makeUnsafe(NOW) }),
    );
    assert.isTrue(Exit.isFailure(manual));
    assert.include(String(manual), "changed while it was starting");
    // Nothing was recorded and the slot was not consumed.
    assert.deepEqual((yield* h.state()).runs, []);
    const [row] = yield* sql<{ next_run_at: string | null }>`
      SELECT next_run_at FROM scheduled_tasks WHERE task_id = ${taskId}`;
    assert.isNull(row?.next_run_at ?? null);
    assert.equal(h.sent.length, 0);
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("a launch that failed before its thread existed is launched again as a new send", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(NOW);
    const h = yield* harness();
    const fresh = task({ threadId: null });
    yield* h.save({ checkCommand: "check", checkReason: "outcome" });
    // The first launch is refused before any thread exists.
    h.failDispatch(true);
    yield* h.fire("scheduled", NOW, fresh);
    const refused = (yield* h.state()).runs[0]!;
    assert.equal(refused.stage, "retry");
    assert.equal(refused.sends.length, 1);
    const firstLaunch = refused.sends[0]!.payload!.launch;
    assert.isNotNull(firstLaunch);
    // No shell and no accepted send: the thread was never created.
    h.set({ unavailable: false, threadMissing: true, landed: false, started: false, busy: false });
    h.failDispatch(false);
    yield* TestClock.adjust(30_000);
    yield* h.runs.drive(fresh);
    const relaunched = (yield* h.state()).runs[0]!;
    assert.equal(relaunched.stage, "running", "a missing thread is launched, not given up on");
    assert.equal(relaunched.threadId, refused.threadId, "the same deterministic thread");
    assert.equal(relaunched.sends.length, 2);
    const second = relaunched.sends[1]!;
    assert.notEqual(second.commandId, refused.sends[0]!.commandId);
    assert.notEqual(second.messageId, refused.sends[0]!.messageId);
    assert.deepEqual(
      second.payload?.launch,
      firstLaunch,
      "it launches again, with the same thread",
    );
    // Once the thread exists, a later send posts to it instead.
    h.set({ threadMissing: false, landed: true });
    h.pass(false);
    yield* h.runs.drive(fresh);
    yield* TestClock.adjust(60_000);
    yield* h.runs.drive(fresh);
    const posted = (yield* h.state()).runs[0]!.sends.at(-1)!;
    assert.equal(posted.index, 2);
    assert.isNull(posted.payload?.launch ?? null);
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("a pass never settles when the run's work resumed during its check", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(NOW);
    const h = yield* harness();
    yield* h.save({ checkCommand: "check", checkReason: "outcome" });
    yield* h.fire("scheduled");
    h.set({ landed: true, started: true, busy: false });
    h.pass(true);
    // A re-enabled reset or an admitted continuation lands while the check runs.
    h.reopenDuringCheck(true);
    yield* h.runs.drive(task());
    assert.equal(h.checks.length, 1);
    const resumed = yield* h.state();
    assert.equal(resumed.runs[0]?.stage, "running");
    assert.isNull(resumed.runs[0]?.check ?? null, "the settle transaction wrote nothing");
    h.reopenDuringCheck(false);
    // While the resumed work runs, the run waits and the check does not run.
    h.set({ busy: true });
    yield* h.runs.drive(task());
    assert.equal(h.checks.length, 1);
    h.openWork(false);
    h.set({ busy: false });
    yield* h.runs.drive(task());
    assert.equal(h.checks.length, 2, "the check reruns after the resumed work ended");
    assert.equal((yield* h.state()).runs[0]?.stage, "done");
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect(
  "a report that needs you before any pass makes the run need you, never a check or retry",
  () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(NOW);
      const h = yield* harness();
      yield* h.save({ checkCommand: "check", checkReason: "outcome" });
      yield* h.fire("scheduled");
      const sends = h.sent.length;
      // The work ended idle and its check would fail, but the report already needs you.
      h.set({ landed: true, started: true, busy: false });
      h.pass(false);
      const reason = "The Spectrum report run failed and will not be retried";
      h.reports({ kind: "needs-you", reason });
      yield* h.runs.drive(task());
      const needsYou = yield* h.state();
      assert.equal(needsYou.runs[0]?.stage, "needs-you");
      assert.equal(needsYou.runs[0]?.error, `Spectrum report: ${reason}`);
      assert.equal(needsYou.runs[0]?.attempt, 0);
      assert.equal(h.checks.length, 0, "no check runs");
      assert.equal(h.sent.length, sends, "nothing is resent");
    }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect(
  "a report that comes to need you during a failing check is what follows, not a retry",
  () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(NOW);
      const h = yield* harness();
      yield* h.save({ checkCommand: "check", checkReason: "outcome" });
      yield* h.fire("scheduled");
      const sends = h.sent.length;
      h.set({ landed: true, started: true, busy: false });
      h.pass(false);
      h.reports({ kind: "waiting" });
      const reason = "Spectrum could not deliver its report after 3 attempts";
      h.reportsDuringCheck({ kind: "needs-you", reason });
      yield* h.runs.drive(task());
      assert.equal(h.checks.length, 1);
      const needsYou = yield* h.state();
      assert.equal(needsYou.runs[0]?.stage, "needs-you");
      assert.equal(needsYou.runs[0]?.error, `Spectrum report: ${reason}`);
      assert.equal(needsYou.runs[0]?.attempt, 0, "no retry was recorded");
      yield* TestClock.adjust(3_600_000);
      yield* h.runs.drive(task());
      assert.equal(h.sent.length, sends, "nothing is resent");
    }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect(
  "a report that needs you makes the run need you, and only its release resumes the check",
  () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(NOW);
      const h = yield* harness();
      yield* h.save({ checkCommand: "check", checkReason: "outcome" });
      yield* h.fire("scheduled");
      h.set({ landed: true, started: true, busy: false });
      h.pass(true);
      // The check passes, but its report comes to need you before the run settles.
      h.reportsDuringCheck({
        kind: "needs-you",
        reason: "Spectrum could not deliver its report after 3 attempts",
      });
      yield* h.runs.drive(task());
      h.reportsDuringCheck(null);
      const needsYou = yield* h.state();
      assert.equal(needsYou.runs[0]?.stage, "needs-you");
      assert.deepEqual(checkedRunStatus(needsYou), {
        status: "failed",
        error: "Needs you: Spectrum report: Spectrum could not deliver its report after 3 attempts",
      });
      // It is never driven, so the check does not run again and nothing is sent.
      yield* h.runs.drive(task());
      assert.equal(h.checks.length, 1);
      const sends = h.sent.length;
      // Run now re-reads the reports; while they still need you it is refused.
      const refused = yield* Effect.exit(h.decide("manual", NOW + 1_000));
      assert.isTrue(Exit.isFailure(refused));
      assert.include(String(refused), "The Spectrum report still needs you");
      // After the user abandons that report, Run now resumes the run without messaging the agent.
      h.reports({ kind: "released" });
      assert.equal((yield* h.decide("manual", NOW + 2_000))._tag, "skip");
      assert.equal((yield* h.state()).runs[0]?.stage, "running");
      yield* h.runs.drive(task());
      assert.equal(h.checks.length, 2, "the pinned check decides once the reports release");
      assert.equal((yield* h.state()).runs[0]?.stage, "done");
      assert.equal(h.sent.length, sends);
    }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("a passed check waits for bound reports, then reruns before it settles", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(NOW);
    const h = yield* harness();
    yield* h.save({ checkCommand: "check", checkReason: "outcome" });
    yield* h.fire("scheduled");
    h.set({ landed: true, started: true, busy: false });
    h.pass(true);
    h.reports({ kind: "waiting" });
    yield* h.runs.drive(task());
    const held = yield* h.state();
    assert.equal(held.runs[0]?.stage, "running");
    assert.isTrue(held.runs[0]?.awaitingReports === true);
    assert.deepEqual(checkedRunStatus(held), { status: "running", error: null });
    // While reports hold, the check is not run again.
    yield* h.runs.drive(task());
    assert.equal(h.checks.length, 1);
    h.reports({ kind: "released" });
    yield* h.runs.drive(task());
    assert.equal(h.checks.length, 2, "the check reruns after the reports' turns ended");
    assert.equal((yield* h.state()).runs[0]?.stage, "done");
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);
