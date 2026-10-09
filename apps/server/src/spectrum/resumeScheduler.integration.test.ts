import { assert, it } from "@effect/vitest";
import {
  AuthOrchestrationOperateScope,
  AuthSessionId,
  ProjectId,
  ScheduledTaskId,
  type ScheduledTask,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import { makeCheckedRuns, ScheduledTaskCheckError } from "../scheduledTaskChecks/engine.ts";
import { reportsFence } from "../scheduledTaskChecks/handoff.ts";
import type { CheckState } from "../scheduledTaskChecks/state.ts";
import {
  ensureCheckSchema,
  readCheckState,
  writeCheckState,
} from "../scheduledTaskChecks/store.ts";
import * as Adapter from "./SchedulerAdapter.ts";
import * as Reports from "./ReportService.ts";
import { SessionStore } from "../auth/SessionStore.ts";
import { abandonScheduledReport } from "./humanReportRpc.ts";
import { base, input, setup } from "./controllerTestkit.ts";
import { makeInitialReport } from "./reportPolicy.ts";
import { resumeAbandonedReport } from "./resumeScheduler.ts";
import { insertSpectrum } from "./store.ts";
import { makeState, NOW } from "./testFixtures.ts";

const task: ScheduledTask = {
  id: ScheduledTaskId.make("checked:task"),
  title: "Check after report",
  prompt: "Work",
  enabled: true,
  projectId: ProjectId.make("project"),
  threadId: input.callerThreadId,
  schedule: { type: "interval", everyMs: 3600000 },
  workspaceStrategy: { type: "root" },
  modelSelection: input.colors[0]!.selection!,
  runtimeMode: "full-access",
  interactionMode: "default",
  createdBy: "agent",
  creationSource: "mcp",
  createdAt: "2026-10-08T12:00:00.000Z",
  updatedAt: "2026-10-08T12:00:00.000Z",
  nextRunAt: null,
  lastRunAt: null,
  lastRunStatus: "never",
  lastRunError: null,
  runCount: 0,
};
const runId = "checked:run";
const runtime = Layer.fresh(
  Layer.mergeAll(
    base,
    Adapter.layer,
    Reports.layer.pipe(Layer.provide(base)),
    Layer.mock(SessionStore)({
      cookieName: "test",
      legacyCookieName: undefined,
      getPerson: () => Effect.succeed("alice"),
    }),
  ),
);
const fixture = Effect.fnUntraced(function* (abandoned = true) {
  yield* setup;
  yield* ensureCheckSchema;
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO scheduled_tasks ${sql.insert({
    task_id: task.id,
    title: task.title,
    prompt: task.prompt,
    enabled: 1,
    project_id: task.projectId,
    thread_id: task.threadId,
    schedule_json: JSON.stringify(task.schedule),
    workspace_strategy_json: '{"type":"root"}',
    model_selection_json: JSON.stringify(task.modelSelection),
    runtime_mode: task.runtimeMode,
    interaction_mode: task.interactionMode,
    created_by: task.createdBy,
    creation_source: task.creationSource,
    created_at: task.createdAt,
    updated_at: task.updatedAt,
    next_run_at: null,
    last_run_at: null,
    last_run_status: "never",
    last_run_error: null,
    run_count: 0,
  })}`;
  const state: CheckState = {
    version: 1,
    taskId: task.id,
    revision: 0,
    kind: "agent",
    command: null,
    role: null,
    lane: null,
    checks: [
      {
        version: 1,
        command: "check",
        actor: "alice",
        reason: "Success",
        createdAt: task.createdAt,
        revertedFrom: null,
      },
    ],
    runs: [
      {
        id: runId,
        slot: task.createdAt,
        checkVersion: 1,
        threadId: task.threadId,
        checkCwd: null,
        stage: "needs-you",
        attempt: 0,
        retryAt: null,
        hasWork: true,
        sends: [],
        error: "Spectrum report: failed",
        check: null,
        awaitingReports: true,
      },
    ],
    failureStreak: 0,
    lastError: "Spectrum report: failed",
    lastSuccessfulRunId: null,
  };
  const expected = yield* writeCheckState(null, state);
  const report = makeInitialReport(
    makeState({ scheduledTaskId: task.id, schedulerRunId: runId }),
    "Report",
    false,
  );
  yield* insertSpectrum(
    abandoned
      ? {
          ...report,
          outbox: [],
          reportAbandonment: {
            commandId: report.report!.commandId,
            person: "alice",
            abandonedAt: NOW,
          },
        }
      : report,
  );
  return expected;
});
const checker = Effect.fnUntraced(function* (onCheck: () => void) {
  return yield* makeCheckedRuns({
    observe: () =>
      Effect.succeed({
        unavailable: false,
        threadMissing: false,
        landed: true,
        started: true,
        busy: false,
        blocked: false,
        usageLimit: null,
        runError: null,
      }),
    dispatch: () => Effect.die("A report hold resumes its check without another agent send"),
    start: () => Effect.die("Not a command task"),
    workspace: () => Effect.succeed("/fixture"),
    role: () => Effect.succeed(null),
    workOpen: () => Effect.succeed(false),
    changed: Effect.void,
    reportsFence: (id) =>
      reportsFence(id).pipe(
        Effect.mapError(() => new ScheduledTaskCheckError({ message: "Fence failed" })),
      ),
    runCheck: () =>
      Effect.sync(() => {
        onCheck();
        return { passed: true, output: "Passed" };
      }),
  });
});
it.effect(
  "abandonment resumes the pinned check once through the real scheduler/Spectrum fence",
  () =>
    Effect.gen(function* () {
      yield* fixture(false);
      const response = yield* abandonScheduledReport(
        {
          sessionId: AuthSessionId.make("human"),
          subject: "paired-device",
          method: "browser-session-cookie",
          scopes: [AuthOrchestrationOperateScope],
        },
        { scheduledTaskId: task.id, schedulerRunId: runId },
      );
      assert.strictEqual(response.abandonedCommandIds.length, 1);
      let checks = 0;
      const engine = yield* checker(() => {
        checks++;
      });
      yield* engine.drive(task);
      yield* engine.drive(task);
      assert.strictEqual(checks, 1);
      assert.strictEqual((yield* readCheckState(task.id))!.runs[0]!.stage, "done");
    }).pipe(Effect.provide(runtime)),
);
it.effect("concurrent Run now and task recreation invalidate the exact human hold", () =>
  Effect.gen(function* () {
    const expected = yield* fixture();
    const engine = yield* checker(() => {});
    const decision = yield* engine.decide({
      task,
      trigger: "manual",
      startedAt: yield* DateTime.now,
    });
    assert.strictEqual(decision._tag, "skip");
    assert.strictEqual((yield* readCheckState(task.id))!.runs[0]!.stage, "running");
    assert.isFalse(yield* resumeAbandonedReport(expected, runId, task.createdAt));
    const sql = yield* SqlClient.SqlClient;
    yield* sql`UPDATE scheduled_tasks SET created_at='2026-10-09T00:00:00.000Z' WHERE task_id=${task.id}`;
    const current = (yield* readCheckState(task.id))!;
    const recreated = yield* writeCheckState(current, expected);
    assert.isFalse(yield* resumeAbandonedReport(recreated, runId, task.createdAt));
    assert.strictEqual((yield* readCheckState(task.id))!.runs[0]!.stage, "needs-you");
  }).pipe(Effect.provide(runtime)),
);
it.effect("an active Spectrum still holds the resumed scheduler run before its check", () =>
  Effect.gen(function* () {
    const expected = yield* fixture();
    yield* insertSpectrum(
      makeState({ threadId: input.threadId, scheduledTaskId: task.id, schedulerRunId: runId }),
    );
    assert.isTrue(yield* resumeAbandonedReport(expected, runId, task.createdAt));
    let checks = 0;
    const engine = yield* checker(() => {
      checks++;
    });
    yield* engine.drive(task);
    assert.strictEqual(checks, 0);
    assert.strictEqual((yield* readCheckState(task.id))!.runs[0]!.stage, "running");
  }).pipe(Effect.provide(runtime)),
);
