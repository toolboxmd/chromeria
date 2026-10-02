import { describe, expect, it } from "@effect/vitest";
import {
  SchedulerError,
  ProjectId,
  ThreadId,
  type CreateScheduledTask,
  type ScheduledTask,
  type TaskRun,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { ScheduledTask as TaskSchema } from "@t3tools/contracts";
import * as Exit from "effect/Exit";
import { TestClock } from "effect/testing";
import { makeScheduler, RECOVERY_DELAYS, LEASE_MS, type RunObservation } from "./Scheduler.ts";

const encodeTask = Schema.encodeEffect(Schema.fromJsonString(TaskSchema));
const input: CreateScheduledTask = {
  title: "Daily work",
  prompt: "Perform a side effect exactly once",
  target: { kind: "new-thread", projectId: ProjectId.make("p") },
  role: "worker",
  schedule: { kind: "interval", minutes: 1 },
  checkCommand: "check-v1",
  checkReason: "Required outcome",
};
const idle: RunObservation = {
  active: false,
  idle: true,
  retired: false,
  hasWork: false,
  pendingDrafters: false,
  drafterIds: [],
  turnId: null,
  error: null,
  usageLimited: false,
  resetAt: null,
  stalled: false,
};
const makeHarness = Effect.gen(function* () {
  let tasks: ReadonlyArray<ScheduledTask> = [];
  let observation = idle;
  const states = new Map<string, RunObservation>();
  let passed = false;
  let unavailable = false;
  let processId = "original";
  let counter = 0;
  let interrupts = 0;
  const sent: Array<{ text: string; threadId: string | null; attempt: number }> = [];
  const checked: string[] = [];
  const saved: ScheduledTask[] = [];
  const build = () =>
    makeScheduler({
      processId,
      tasks: Effect.sync(() => tasks),
      uuid: Effect.sync(() => String(++counter)),
      sequence: Effect.succeed(0),
      wake: Effect.void,
      historyVersion: (id, version) =>
        Effect.succeed(
          saved
            .filter((task) => task.id === id)
            .flatMap((task) => task.checks)
            .find((check) => check.version === version),
        ),
      validateTarget: () => Effect.succeed("/tmp/check-cwd"),
      save: (previous, next) =>
        Effect.sync(() => {
          expect(tasks.find((entry) => entry.id === next.id)?.revision ?? 0).toBe(
            previous?.revision ?? 0,
          );
          tasks = [...tasks.filter((entry) => entry.id !== next.id), next];
          saved.push(next);
        }),
      check: (command) =>
        Effect.sync(() => {
          checked.push(command);
          return { passed, output: passed ? "verified" : "missing result" };
        }),
      observe: (_, run) => Effect.sync(() => states.get(run.id) ?? observation),
      prepare: (_, run) =>
        unavailable
          ? Effect.fail(new SchedulerError({ detail: "provider down" }))
          : Effect.succeed(run.threadId ?? ThreadId.make("stable-run-thread")),
      send: (_, run, text) =>
        Effect.sync(() => {
          sent.push({ text, threadId: run.threadId, attempt: run.attempt });
          states.set(run.id, { ...idle, active: true, idle: false, turnId: "turn" });
          return true;
        }),
      interrupt: () =>
        Effect.sync(() => {
          interrupts++;
          observation = { ...idle, hasWork: true, error: "interrupted" };
        }),
    });
  const scheduler = yield* build();
  return {
    scheduler,
    get tasks() {
      return tasks;
    },
    get sent() {
      return sent;
    },
    get checked() {
      return checked;
    },
    get interrupts() {
      return interrupts;
    },
    setObservation: (value: Partial<RunObservation>) => {
      states.clear();
      observation = { ...idle, ...value };
    },
    setPassed: (value: boolean) => {
      passed = value;
    },
    setUnavailable: (value: boolean) => {
      unavailable = value;
    },
    restart: () => {
      processId = "restart";
      return build();
    },
    replaceRun: (patch: Partial<TaskRun>) => {
      tasks = tasks.map((task) => ({
        ...task,
        runs: task.runs.map((run) => ({ ...run, ...patch })),
      }));
    },
  };
});
const start = (h: Effect.Success<typeof makeHarness>) =>
  Effect.gen(function* () {
    const task = yield* h.scheduler.create(input, "creator");
    yield* h.scheduler.runNow(task.id);
    yield* h.scheduler.reconcile();
    return task.id;
  });

describe("check-gated scheduler", () => {
  it.effect("refuses already-passing checks and invalid empty checks", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness;
      h.setPassed(true);
      expect(Exit.isFailure(yield* Effect.exit(h.scheduler.create(input, "user")))).toBe(true);
      h.setPassed(false);
      expect(
        Exit.isFailure(
          yield* Effect.exit(h.scheduler.create({ ...input, checkCommand: "" }, "user")),
        ),
      ).toBe(true);
      expect(h.tasks).toHaveLength(0);
    }),
  );
  it.effect("checks gate completion and failed checks continue the same worked thread", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness;
      yield* start(h);
      h.setObservation({ hasWork: true, turnId: "turn" });
      yield* h.scheduler.reconcile();
      expect(h.tasks[0]!.runs[0]!.status).toBe("retry");
      yield* TestClock.adjust(30_000);
      yield* h.scheduler.reconcile();
      expect(h.sent[1]!.threadId).toBe(h.sent[0]!.threadId);
      expect(h.sent[1]!.text).toContain("Check what is already done, then finish");
      expect(h.sent[1]!.text).toContain("missing result");
      h.setObservation({ hasWork: true });
      h.setPassed(true);
      yield* h.scheduler.reconcile();
      expect(h.tasks[0]!.runs[0]!.status).toBe("done");
    }),
  );
  for (const mode of ["never-started", "before-output", "after-tool-work"] as const) {
    it.effect(`walks all recovery deadlines for ${mode}`, () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        if (mode === "never-started") h.setUnavailable(true);
        yield* start(h);
        if (mode !== "never-started") {
          h.setObservation({ error: "provider failure", hasWork: mode === "after-tool-work" });
          yield* h.scheduler.reconcile();
        }
        for (const [index, delay] of RECOVERY_DELAYS.entries()) {
          expect(
            h.tasks[0]!.runs[0]!.retryAt! -
              (yield* Effect.clockWith((clock) => clock.currentTimeMillis)),
          ).toBe(delay);
          yield* TestClock.adjust(delay - 1);
          yield* h.scheduler.reconcile();
          expect(h.tasks[0]!.runs[0]!.attempt).toBe(index + 1);
          yield* TestClock.adjust(1);
          yield* h.scheduler.reconcile();
          if (mode !== "never-started") {
            h.setObservation({ error: "provider failure", hasWork: mode === "after-tool-work" });
            yield* h.scheduler.reconcile();
          }
        }
        expect(h.tasks[0]!.runs[0]!.status).toBe("needs-you");
        if (mode === "after-tool-work")
          expect(
            h.sent
              .slice(1)
              .every((send) => send.text.includes("Do not repeat completed side effects")),
          ).toBe(true);
        if (mode === "before-output")
          expect(h.sent.slice(1).every((send) => send.text.includes(input.prompt))).toBe(true);
        expect(new Set(h.sent.map((send) => send.threadId)).size).toBeLessThanOrEqual(1);
      }),
    );
  }
  it.effect("stall interrupts and waits for idle before continuing the same thread", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness;
      yield* start(h);
      h.setObservation({ active: true, idle: false, stalled: true, hasWork: true });
      yield* h.scheduler.reconcile();
      expect(h.interrupts).toBe(1);
      h.setObservation({ active: true, idle: false, hasWork: true });
      yield* TestClock.adjust(30_000);
      yield* h.scheduler.reconcile();
      expect(h.sent).toHaveLength(1);
      h.setObservation({ hasWork: true, error: "interrupted" });
      yield* h.scheduler.reconcile();
      expect(h.sent[1]!.threadId).toBe("stable-run-thread");
      expect(h.sent[1]!.text).toContain("Provider stalled");
    }),
  );
  it.effect("usage limits wait for a fresh reset observation without spending ladder steps", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness;
      yield* start(h);
      h.setObservation({
        usageLimited: true,
        resetAt: 100_000,
        error: "usage limit",
        hasWork: true,
      });
      yield* h.scheduler.reconcile();
      yield* TestClock.adjust(200_000);
      yield* h.scheduler.reconcile();
      expect(h.sent).toHaveLength(1);
      expect(h.tasks[0]!.runs[0]!.attempt).toBe(0);
      h.setObservation({ hasWork: true });
      yield* h.scheduler.reconcile();
      expect(h.sent[1]!.text).toContain("Continue in this conversation");
      expect(h.sent[1]!.text).toContain("usage limit");
    }),
  );
  it.effect("restart after persisted send intent treats absent output as unknown execution", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness;
      yield* start(h);
      h.replaceRun({ hasWork: false });
      h.setObservation({ error: "session disconnected" });
      const restarted = yield* h.restart();
      yield* TestClock.adjust(LEASE_MS);
      yield* restarted.reconcile();
      expect(h.tasks[0]!.runs[0]!.hasWork).toBe(true);
      yield* TestClock.adjust(30_000);
      yield* restarted.reconcile();
      expect(h.sent[1]!.threadId).toBe(h.sent[0]!.threadId);
      expect(h.sent[1]!.text).toContain("Do not repeat completed side effects");
      expect(h.sent[1]!.text).toContain("session disconnected");
    }),
  );
  it.effect("expired unobserved send intent continues conservatively in the same process", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness;
      yield* start(h);
      h.replaceRun({ hasWork: false });
      h.setObservation({});
      yield* TestClock.adjust(LEASE_MS);
      yield* h.scheduler.reconcile();
      yield* TestClock.adjust(30_000);
      yield* h.scheduler.reconcile();
      expect(h.sent[1]!.threadId).toBe(h.sent[0]!.threadId);
      expect(h.sent[1]!.text).toContain("Continue in this conversation");
      expect(h.sent[1]!.text).not.toContain(input.prompt);
    }),
  );
  it.effect("does not finish with pending run Drafters and retains their attribution", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness;
      yield* start(h);
      h.setPassed(true);
      h.setObservation({
        hasWork: true,
        pendingDrafters: true,
        drafterIds: [ThreadId.make("child")],
      });
      yield* h.scheduler.reconcile();
      expect(h.tasks[0]!.runs[0]!.status).toBe("running");
      expect(h.tasks[0]!.runs[0]!.drafterIds).toEqual(["child"]);
      h.setObservation({ hasWork: true, pendingDrafters: false });
      yield* h.scheduler.reconcile();
      expect(h.tasks[0]!.runs[0]!.status).toBe("done");
    }),
  );
  it.effect(
    "other-author check edits/reverts are future-only and judged threads cannot manage their task",
    () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const id = yield* start(h);
        expect(
          Exit.isFailure(
            yield* Effect.exit(
              h.scheduler.edit(
                { taskId: id, checkCommand: "true", checkReason: "cheat" },
                "stable-run-thread",
              ),
            ),
          ),
        ).toBe(true);
        expect(
          Exit.isFailure(yield* Effect.exit(h.scheduler.pause(id, true, "stable-run-thread"))),
        ).toBe(true);
        yield* h.scheduler.edit(
          { taskId: id, checkCommand: "check-v2", checkReason: "fix judge" },
          "other-agent",
        );
        expect(h.tasks[0]!.runs[0]!.checkVersion).toBe(1);
        expect(h.tasks[0]!.checks.at(-1)?.actor).toBe("other-agent");
        yield* h.scheduler.edit({ taskId: id, revertVersion: 1, checkReason: "revert" }, "user");
        expect(h.tasks[0]!.checks.at(-1)?.revertedFrom).toBe(1);
        h.setObservation({ hasWork: true });
        yield* h.scheduler.reconcile();
        expect(h.checked.at(-1)).toBe("check-v1");
      }),
  );
  it.effect(
    "one-shot fires at its chosen minute only once and still requires a passing check",
    () =>
      Effect.gen(function* () {
        const h = yield* makeHarness;
        const task = yield* h.scheduler.create(
          { ...input, schedule: { kind: "once", at: "1970-01-01T00:01:00Z", windowMinutes: 0 } },
          "user",
        );
        yield* h.scheduler.reconcile();
        expect(h.sent).toHaveLength(0);
        yield* TestClock.adjust(60_000);
        yield* h.scheduler.reconcile();
        expect(h.sent).toHaveLength(1);
        h.setObservation({ hasWork: true });
        h.setPassed(true);
        yield* h.scheduler.reconcile();
        expect(h.tasks[0]!.runs[0]!.status).toBe("done");
        yield* TestClock.adjust(86400_000);
        yield* h.scheduler.reconcile();
        expect(h.sent).toHaveLength(1);
        expect(h.tasks[0]!.id).toBe(task.id);
      }),
  );
  it.effect("latest missed slot only, overlap skipping, stable claims and parallel tasks", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness;
      const a = yield* h.scheduler.create(input, "user");
      const b = yield* h.scheduler.create(input, "user");
      yield* TestClock.adjust(600_000);
      yield* Effect.all([h.scheduler.reconcile(), h.scheduler.reconcile()], {
        concurrency: "unbounded",
      });
      expect(h.sent).toHaveLength(2);
      expect(
        h.tasks.every(
          (task) => task.runs.length === 1 && task.runs[0]!.slot === "1970-01-01T00:10:00.000Z",
        ),
      ).toBe(true);
      expect(Exit.isFailure(yield* Effect.exit(h.scheduler.runNow(a.id)))).toBe(true);
      yield* TestClock.adjust(60_000);
      yield* h.scheduler.reconcile();
      expect(h.sent).toHaveLength(2);
      expect(h.tasks.find((task) => task.id === b.id)?.consumedSlot).toBe(
        "1970-01-01T00:11:00.000Z",
      );
    }),
  );
  it.effect("bounds live run/check history without losing pinned judges or audit reverts", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness;
      const task = yield* h.scheduler.create(input, "user");
      for (let index = 0; index < 80; index++) {
        h.setPassed(true);
        h.setObservation({});
        yield* TestClock.adjust(1);
        yield* h.scheduler.runNow(task.id);
        yield* h.scheduler.reconcile();
        h.setObservation({});
        yield* h.scheduler.reconcile();
        yield* h.scheduler.edit(
          { taskId: task.id, checkCommand: `check-${index}`, checkReason: "new version" },
          "user",
        );
      }
      expect(h.tasks[0]!.runs.length).toBeLessThanOrEqual(20);
      expect(h.tasks[0]!.checks.length).toBeLessThanOrEqual(20);
      yield* h.scheduler.edit(
        { taskId: task.id, revertVersion: 1, checkReason: "old audited version" },
        "user",
      );
      expect(h.tasks[0]!.checks.at(-1)?.command).toBe("check-v1");
      expect((yield* encodeTask(h.tasks[0]!)).length).toBeLessThan(25_000);
    }),
  );
});
