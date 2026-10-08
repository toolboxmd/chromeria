import { assert, it } from "@effect/vitest";
import { CommandId, MessageId, RunId, ScheduledTaskId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as Recovery from "../prism/RecoveryHistory.ts";
import {
  followRun,
  reportsFence,
  resolveSchedulerRun,
  ScheduledTaskSpectra,
  schedulerRunOpenGuard,
} from "./handoff.ts";
import {
  bindSpectrum,
  ensureSpectraFixture,
  fixtureSpectra,
  reportReceipt,
} from "./spectra.testkit.ts";
import type { CheckState } from "./state.ts";
import { ensureCheckSchema, writeCheckState } from "./store.ts";

const toJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const threadId = ThreadId.make("thread:handoff");
let ordinal = 0;

/** A projection run row as the follower reads it; links live in its payload. */
const run = (input: {
  readonly id: string;
  readonly status: string;
  readonly userMessageId?: string;
  readonly restartOf?: string;
  readonly prismSourceOf?: string;
}) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    ordinal += 1;
    yield* sql`INSERT INTO orchestration_v2_projection_runs ${sql.insert({
      run_id: input.id,
      thread_id: threadId,
      ordinal,
      provider: "codex",
      provider_thread_id: null,
      status: input.status,
      requested_at: "2026-10-08T12:00:00.000Z",
      completed_at: null,
      payload_json: toJson({
        id: input.id,
        threadId,
        userMessageId: input.userMessageId ?? `message:${input.id}`,
        ...(input.restartOf === undefined ? {} : { restartContinuationOfRunId: input.restartOf }),
        ...(input.prismSourceOf === undefined
          ? {}
          : { forkPrismContinuationSourceRunId: input.prismSourceOf }),
      }),
    })}`;
  });

const setup = Effect.gen(function* () {
  yield* Recovery.initializeRecoveryHistory;
  yield* ensureCheckSchema;
});

it.effect("a failed run holds until its recovery decision is conclusive", () =>
  Effect.gen(function* () {
    yield* setup;
    yield* run({ id: "run:failed", status: "failed" });
    // No fact yet is undecided, never terminal.
    assert.deepEqual(yield* followRun("run:failed"), { kind: "waiting" });
    yield* Recovery.writeRecoveryOutcome({
      sourceRunId: RunId.make("run:failed"),
      threadId,
      status: "pending",
      reason: "retry",
    });
    assert.deepEqual(yield* followRun("run:failed"), { kind: "waiting" });
    yield* Recovery.writeRecoveryOutcome({
      sourceRunId: RunId.make("run:failed"),
      threadId,
      status: "decided",
      outcome: "not_retryable",
      reason: "non_retryable",
    });
    assert.deepEqual(yield* followRun("run:failed"), { kind: "ended", runId: "run:failed" });
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("an automatic abandon ends the work for the scheduler to decide", () =>
  Effect.gen(function* () {
    yield* setup;
    yield* run({ id: "run:stopped", status: "failed" });
    yield* Recovery.writeRecoveryOutcome({
      sourceRunId: RunId.make("run:stopped"),
      threadId,
      status: "decided",
      outcome: "abandoned",
      reason: "stopped_or_retired",
    });
    assert.deepEqual(yield* followRun("run:stopped"), { kind: "ended", runId: "run:stopped" });
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("a retry is followed only to the exact successor that names its source", () =>
  Effect.gen(function* () {
    yield* setup;
    yield* run({ id: "run:source", status: "failed" });
    yield* Recovery.writeRecoveryOutcome({
      sourceRunId: RunId.make("run:source"),
      threadId,
      status: "decided",
      outcome: "retried",
      successorRunId: RunId.make("run:successor"),
    });
    // The named successor is not there yet: hold.
    assert.deepEqual(yield* followRun("run:source"), { kind: "waiting" });
    // A run that does not name this source back is not its successor.
    yield* run({ id: "run:successor", status: "completed" });
    assert.deepEqual(yield* followRun("run:source"), { kind: "waiting" });
    const sql = yield* SqlClient.SqlClient;
    yield* sql`UPDATE orchestration_v2_projection_runs
      SET payload_json = json_set(payload_json, '$.forkPrismContinuationSourceRunId', 'run:source')
      WHERE run_id = 'run:successor'`;
    assert.deepEqual(yield* followRun("run:source"), {
      kind: "completed",
      runId: "run:successor",
    });
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("upstream's restart continuation is followed before any recovery fact", () =>
  Effect.gen(function* () {
    yield* setup;
    yield* run({ id: "run:cancelled", status: "cancelled" });
    yield* run({ id: "run:restart", status: "running", restartOf: "run:cancelled" });
    assert.deepEqual(yield* followRun("run:cancelled"), { kind: "waiting" });
    const sql = yield* SqlClient.SqlClient;
    yield* sql`UPDATE orchestration_v2_projection_runs SET status = 'completed'
      WHERE run_id = 'run:restart'`;
    assert.deepEqual(yield* followRun("run:cancelled"), {
      kind: "completed",
      runId: "run:restart",
    });
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

const bind = (
  schedulerRunId: string,
  input: Omit<Parameters<typeof bindSpectrum>[0], "callerThreadId" | "schedulerRunId">,
) => bindSpectrum({ callerThreadId: threadId, schedulerRunId, ...input });
const receipt = (id: string, status: "accepted" | "rejected") =>
  reportReceipt(threadId, id, status);

const fence = (schedulerRunId: string, needsYou: ReadonlyMap<string, string> = new Map()) =>
  reportsFence(schedulerRunId).pipe(
    Effect.provideService(ScheduledTaskSpectra, fixtureSpectra(needsYou)),
  );
const released = { kind: "released" } as const;
const waiting = { kind: "waiting" } as const;

it.effect("the fence waits through Spectrum, dispatch and the report's turn", () =>
  Effect.gen(function* () {
    yield* setup;
    yield* ensureSpectraFixture;
    assert.deepEqual(yield* fence("scheduler:none"), released);
    yield* bind("scheduler:active", { status: "active" });
    assert.deepEqual(yield* fence("scheduler:active"), waiting);
    // A retired Spectrum that wrote no report releases, as does a row from before reports.
    yield* bind("scheduler:retired", { status: "retired" });
    yield* bind("scheduler:retired", { status: "retired", legacy: true });
    assert.deepEqual(yield* fence("scheduler:retired"), released);
    yield* bind("scheduler:outbox", {
      status: "settled",
      report: "outbox",
      inOutbox: true,
    });
    assert.deepEqual(yield* fence("scheduler:outbox"), waiting);
    // Drained from the outbox with no receipt yet, or rejected awaiting Spectrum's next attempt.
    yield* bind("scheduler:pending", { status: "settled", report: "pending" });
    assert.deepEqual(yield* fence("scheduler:pending"), waiting);
    yield* receipt("rejected", "rejected");
    yield* bind("scheduler:rejected", { status: "settled", report: "rejected" });
    assert.deepEqual(yield* fence("scheduler:rejected"), waiting);
    // Accepted: the report's own run decides, and only its completion delivers it.
    yield* receipt("turn", "accepted");
    yield* bind("scheduler:turn", { status: "settled", report: "turn" });
    assert.deepEqual(yield* fence("scheduler:turn"), waiting);
    yield* run({ id: "run:report", status: "running", userMessageId: "report-message:turn" });
    assert.deepEqual(yield* fence("scheduler:turn"), waiting);
    const sql = yield* SqlClient.SqlClient;
    yield* sql`UPDATE orchestration_v2_projection_runs SET status = 'completed' WHERE run_id = 'run:report'`;
    assert.deepEqual(yield* fence("scheduler:turn"), released);
    // Every bound Spectrum must release: a second, still active one keeps the run waiting.
    yield* bind("scheduler:turn", { status: "active" });
    assert.deepEqual(yield* fence("scheduler:turn"), waiting);
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("a report releases only on its completed turn or the user's abandonment", () =>
  Effect.gen(function* () {
    yield* setup;
    yield* ensureSpectraFixture;
    // The report's run failed and recovery concluded: no delivery, and Spectrum may try again.
    yield* receipt("failed", "accepted");
    yield* run({
      id: "run:failed-report",
      status: "failed",
      userMessageId: "report-message:failed",
    });
    yield* Recovery.writeRecoveryOutcome({
      sourceRunId: RunId.make("run:failed-report"),
      threadId,
      status: "decided",
      outcome: "not_retryable",
      reason: "non_retryable",
    });
    yield* bind("scheduler:failed", { status: "settled", report: "failed" });
    assert.deepEqual(yield* fence("scheduler:failed"), waiting);
    // Spectrum will not try again, so the report needs you, and so does the run.
    const reason = "Spectrum could not deliver its report after 3 attempts";
    assert.deepEqual(yield* fence("scheduler:failed", new Map([["report:failed", reason]])), {
      kind: "needs-you",
      reason,
    });
    // Needs-you or an abandonment of an earlier attempt does not count for the current one.
    assert.deepEqual(
      yield* fence("scheduler:failed", new Map([["report:earlier", reason]])),
      waiting,
    );
    yield* bind("scheduler:stale", {
      status: "settled",
      report: "failed",
      abandoned: "earlier",
    });
    assert.deepEqual(yield* fence("scheduler:stale"), waiting);
    // The user abandoning this exact report releases it, even while it needs them.
    yield* bind("scheduler:abandoned", {
      status: "settled",
      report: "failed",
      abandoned: "failed",
    });
    assert.deepEqual(
      yield* fence("scheduler:abandoned", new Map([["report:failed", reason]])),
      released,
    );
    // A report that needs you outranks another Spectrum still waiting.
    yield* bind("scheduler:failed", { status: "active" });
    assert.deepEqual(yield* fence("scheduler:failed", new Map([["report:failed", reason]])), {
      kind: "needs-you",
      reason,
    });
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

const schedulerState = (stage: "running" | "done"): CheckState => ({
  version: 1,
  taskId: ScheduledTaskId.make("task:bound"),
  revision: 0,
  kind: "agent",
  command: null,
  role: null,
  lane: null,
  checks: [
    {
      version: 1,
      command: "check",
      actor: "agent",
      reason: "outcome",
      createdAt: "2026-10-08T00:00:00.000Z",
      revertedFrom: null,
    },
  ],
  runs: [
    {
      id: "task:bound:2026-10-08T12:00:00.000Z",
      slot: "2026-10-08T12:00:00.000Z",
      checkVersion: 1,
      threadId,
      checkCwd: null,
      stage,
      attempt: 0,
      retryAt: null,
      hasWork: true,
      sends: [
        {
          index: 0,
          commandId: CommandId.make("send:0"),
          messageId: MessageId.make("send-message:0"),
          kind: "start",
          createdAt: "2026-10-08T12:00:00.000Z",
        },
      ],
      error: null,
      check: null,
    },
  ],
  failureStreak: 0,
  lastError: null,
  lastSuccessfulRunId: null,
});

it.effect("Spectrum binds to the scheduler run whose send started its caller, exactly", () =>
  Effect.gen(function* () {
    yield* setup;
    yield* writeCheckState(null, schedulerState("running"));
    // The caller is a restart continuation of the run the scheduler's send started.
    yield* run({ id: "run:sent", status: "cancelled", userMessageId: "send-message:0" });
    yield* run({ id: "run:caller", status: "running", restartOf: "run:sent" });
    assert.deepEqual(
      yield* resolveSchedulerRun({
        callerThreadId: threadId,
        callerRunId: RunId.make("run:caller"),
      }),
      {
        scheduledTaskId: ScheduledTaskId.make("task:bound"),
        schedulerRunId: "task:bound:2026-10-08T12:00:00.000Z",
      },
    );
    // An unlinked wake run on the same thread is not the scheduler's work.
    yield* run({ id: "run:wake", status: "running", userMessageId: "wake-message" });
    assert.isNull(
      yield* resolveSchedulerRun({ callerThreadId: threadId, callerRunId: RunId.make("run:wake") }),
    );
    const guard = {
      scheduledTaskId: ScheduledTaskId.make("task:bound"),
      schedulerRunId: "task:bound:2026-10-08T12:00:00.000Z",
      callerThreadId: threadId,
    };
    yield* schedulerRunOpenGuard(guard);
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(
          schedulerRunOpenGuard({ ...guard, callerThreadId: ThreadId.make("other") }),
        ),
      ),
    );
    const sql = yield* SqlClient.SqlClient;
    yield* sql`DELETE FROM fork_scheduled_task_checks`;
    yield* writeCheckState(null, schedulerState("done"));
    assert.isTrue(Exit.isFailure(yield* Effect.exit(schedulerRunOpenGuard(guard))));
    assert.isNull(
      yield* resolveSchedulerRun({
        callerThreadId: threadId,
        callerRunId: RunId.make("run:caller"),
      }),
    );
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);
